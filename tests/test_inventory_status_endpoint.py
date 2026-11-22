import asyncio
import importlib.util
import json
import unittest
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from unittest import mock

from starlette.types import Message, Scope

from app.config import Settings
from app.db import DatabaseStore, ShopRecord
from app.utils import AuthorizationError, SyncProcessingError


class InventoryStatusEndpointTests(unittest.IsolatedAsyncioTestCase):
    main: ModuleType

    @classmethod
    def setUpClass(cls) -> None:
        settings = Settings(
            shopify_client_id="test-client",
            shopify_client_secret="test-client-secret",
            app_base_url="https://sync.example.test",
            app_session_secret="test-session-secret",
            credential_encryption_secret="test-encryption-secret",
            app_scopes="read_products,write_products,read_inventory",
            database_path=":memory:",
        )
        spec = importlib.util.spec_from_file_location(
            "app._inventory_status_endpoint_tests",
            Path(__file__).resolve().parents[1] / "app" / "main.py",
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("Could not load the API module for endpoint tests.")
        cls.main = importlib.util.module_from_spec(spec)
        with (
            mock.patch("app.config.get_settings", return_value=settings),
            mock.patch("app.db.DatabaseStore", return_value=mock.Mock(spec=DatabaseStore)),
        ):
            spec.loader.exec_module(cls.main)

    def setUp(self) -> None:
        self.shop = ShopRecord(shop_domain="example.myshopify.com", access_token="test-token")
        self.main.db.reset_mock(return_value=True, side_effect=True)
        self.main.db.verify_pos_credentials.return_value = self.shop
        self.main.db.get_pos_auth_record.return_value = None
        self.main.db.get_connector_sync_request.return_value = {
            "request_id": None, "requested_at": None, "completed_at": None,
        }
        self.main.app.dependency_overrides.clear()
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.archive = stack.enter_context(
            mock.patch.object(self.main.inventory_service, "archive_zero_stock_product_by_sku")
        )
        stack.enter_context(mock.patch.object(self.main, "logger"))

    async def post(
        self,
        payload: object,
        *,
        authenticated: bool = True,
        path: str = "/sync/inventory/status/reconcile",
        method: str = "POST",
        dashboard_header: bool = True,
        query_string: bytes = b"",
    ) -> tuple[int, dict[str, object]]:
        body = json.dumps(payload).encode("utf-8")
        headers = [(b"content-type", b"application/json")]
        if dashboard_header and path in {"/api/connector/force-sync", "/api/sales-board/settings"}:
            headers.append((b"x-requested-with", b"pos-bridge"))
        if authenticated:
            headers.extend([(b"x-api-key", b"test-key"), (b"x-api-secret", b"test-secret")])
        scope: Scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("utf-8"),
            "query_string": query_string,
            "root_path": "",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
        messages: list[Message] = []
        request_received = False
        response_complete = asyncio.Event()

        async def receive() -> Message:
            nonlocal request_received
            if not request_received:
                request_received = True
                return {"type": "http.request", "body": body, "more_body": False}
            await response_complete.wait()
            return {"type": "http.disconnect"}

        async def send(message: Message) -> None:
            messages.append(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                response_complete.set()

        await asyncio.wait_for(self.main.app(scope, receive, send), timeout=5)
        status = next(message["status"] for message in messages if message["type"] == "http.response.start")
        response_body = b"".join(
            message.get("body", b"") for message in messages if message["type"] == "http.response.body"
        )
        return status, json.loads(response_body)

    async def test_missing_credentials_are_rejected_before_archiving(self) -> None:
        status, response = await self.post({"skus": ["SHIRT"]}, authenticated=False)

        self.assertEqual(status, 401)
        self.assertEqual(response["error"]["code"], "authorization_error")
        self.main.db.verify_pos_credentials.assert_not_called()
        self.archive.assert_not_called()

    async def test_heartbeat_requires_credentials_and_validates_intervals(self) -> None:
        payload = {"version": "1.2", "sync_interval_seconds": 30, "customer_sync_enabled": True}
        status, _body = await self.post(payload, path="/sync/heartbeat", authenticated=False)
        self.assertEqual(status, 401)
        self.main.db.record_connector_heartbeat.assert_not_called()
        status, _body = await self.post({**payload, "sync_interval_seconds": 0}, path="/sync/heartbeat")
        self.assertEqual(status, 422)
        self.main.db.record_connector_heartbeat.assert_not_called()
        status, _body = await self.post(payload, path="/sync/heartbeat")
        self.assertEqual(status, 200)
        self.main.db.record_connector_heartbeat.assert_called_once_with(
            shop_domain=self.shop.shop_domain, channel="connector",
            metadata={**payload, "customer_sync_interval_seconds": 60},
        )

    async def test_customer_status_requires_credentials(self) -> None:
        status, _body = await self.post(None, path="/sync/customers/status", method="GET", authenticated=False)
        self.assertEqual(status, 401)
        self.main.db.record_connector_heartbeat.assert_not_called()

    async def test_board_settings_are_managed_in_dashboard_and_read_by_connector(self) -> None:
        path = "/api/sales-board/settings"
        options = {"enabled": False, "interval_seconds": 90, "monthly_goal_cents": 500000}
        status, _ = await self.post(options, path=path, method="PUT")
        self.assertEqual(status, 401)
        with mock.patch.object(self.main, "_get_session_context", return_value=(self.shop, {})):
            status, _ = await self.post(options, path=path, method="PUT", dashboard_header=False)
            self.assertEqual(status, 401)
            status, result = await self.post(options, path=path, method="PUT")
        self.assertEqual((status, result), (200, options))
        self.main.db.save_sales_board_settings.assert_called_once_with(shop_domain=self.shop.shop_domain, **options)
        self.main.db.get_sales_board_settings.return_value = options
        with mock.patch.object(self.main, "utc_now", return_value=datetime(2026, 11, 23, 2, tzinfo=timezone.utc)):
            status, result = await self.post(None, path="/sync/sales-board/settings", method="GET")
        self.assertEqual((status, result), (200, {"enabled": False, "interval_seconds": 90, "business_date": "2026-11-22"}))

    async def test_sales_receipts_require_a_dashboard_session_and_respect_pause(self) -> None:
        path = "/api/sales-board/receipts"
        query = b"employee_id=A&period=year&page=2"
        status, _ = await self.post(None, path=path, method="GET", query_string=query)
        self.assertEqual(status, 401)
        self.main.db.get_sales_board_receipts.assert_not_called()
        with mock.patch.object(self.main, "_get_session_context", return_value=(self.shop, {})):
            self.main.db.get_sales_board_settings.return_value = {}
            receipts = [{"invoice": str(i)} for i in range(31)]
            self.main.db.get_sales_board_receipts.return_value = receipts
            status, result = await self.post(None, path=path, method="GET", query_string=query)
            self.assertEqual((status, result), (200, {"available": True, "items": receipts[:30], "has_more": True}))
            self.main.db.get_sales_board_receipts.assert_called_once_with(
                shop_domain=self.shop.shop_domain, employee_id="A", period="year", offset=30, limit=31,
            )
            self.main.db.get_sales_board_settings.return_value = {"enabled": False}
            status, result = await self.post(None, path=path, method="GET", query_string=query)
        self.assertEqual((status, result), (200, {"available": False, "items": [], "has_more": False}))
        self.assertEqual(self.main.db.get_sales_board_receipts.call_count, 1)

    async def test_customer_address_review_requires_a_shop_dashboard_session(self) -> None:
        path = "/api/customer-sync-history/17/address"
        status, _ = await self.post(None, path=path, method="GET")
        self.assertEqual(status, 401)
        self.main.db.get_customer_address_review.assert_not_called()
        with mock.patch.object(self.main, "_get_session_context", return_value=(self.shop, {})):
            self.main.db.get_customer_address_review.return_value = {"addresses": [{"zip": "99999"}]}
            status, payload = await self.post(None, path=path, method="GET")
        self.assertEqual(status, 200)
        self.assertEqual(payload["addresses"], [{"zip": "99999"}])
        self.main.db.get_customer_address_review.assert_called_once_with(self.shop.shop_domain, event_id=17)

    async def test_force_sync_requires_dashboard_session_and_uses_its_shop(self) -> None:
        status, _body = await self.post({}, path="/api/connector/force-sync")
        self.assertEqual(status, 401)
        self.main.db.request_connector_sync.assert_not_called()
        queued = {"request_id": "request1", "requested_at": "2026-11-17T12:00:00Z", "completed_at": None}
        self.main.db.request_connector_sync.return_value = queued
        with mock.patch.object(self.main, '_get_session_context', return_value=(self.shop, {})):
            status, _body = await self.post({}, path="/api/connector/force-sync", dashboard_header=False)
            self.assertEqual(status, 401)
            self.main.db.request_connector_sync.assert_not_called()
            status, body = await self.post({"shop": "other.myshopify.com"}, path="/api/connector/force-sync")
        self.assertEqual(status, 200)
        self.assertEqual(body, queued)
        self.main.db.request_connector_sync.assert_called_once_with(shop_domain=self.shop.shop_domain)

    async def test_force_sync_acknowledgment_requires_pos_authentication(self) -> None:
        status, _body = await self.post({}, path="/sync/requests/request1/ack", authenticated=False)
        self.assertEqual(status, 401)
        self.main.db.complete_connector_sync.assert_not_called()
        self.main.db.complete_connector_sync.return_value = True
        status, body = await self.post({}, path="/sync/requests/request1/ack")
        self.assertEqual(status, 200)
        self.assertTrue(body['completed'])
        self.main.db.complete_connector_sync.assert_called_once_with(
            shop_domain=self.shop.shop_domain, request_id="request1",
        )

    async def test_customer_batch_omits_contact_data_from_request_and_feed_logs(self) -> None:
        with (
            mock.patch.object(self.main.shopify_client, "ensure_customer_custom_id_definition"),
            mock.patch.object(self.main.shopify_client, "get_customer_by_identifier", return_value=None),
            mock.patch.object(self.main.shopify_client, "customer_create", return_value={
                "id": "gid://shopify/Customer/1", "email": "sample@example.test",
            }),
        ):
            status, body = await self.post({"customers": [{
                "source": "Customer.dbf", "pos_customer_number": "1",
                "firstName": "Sample", "email": "sample@example.test",
            }]}, path="/sync/customers")
        self.assertEqual(status, 200)
        self.assertEqual(body['succeeded'], 1)
        self.assertEqual(self.main.db.record_request_log.call_args.kwargs['request_body'], '[customer payload omitted]')
        feed = self.main.db.record_feed_event.call_args.kwargs
        self.assertIsNone(feed['title'])
        self.assertIsNone(feed['request_payload'])
        self.assertIsNone(feed['normalized_payload'])

    async def test_invalid_credentials_are_rejected_before_archiving(self) -> None:
        self.main.db.verify_pos_credentials.side_effect = AuthorizationError("Invalid POS credentials.")

        status, response = await self.post({"skus": ["SHIRT"]})

        self.assertEqual(status, 401)
        self.assertEqual(response["error"]["code"], "authorization_error")
        self.archive.assert_not_called()

    async def test_request_uses_the_authenticated_shop(self) -> None:
        result = {"sku": "SHIRT", "success": True, "archived": True}
        self.archive.return_value = result

        status, response = await self.post({"skus": ["SHIRT"], "shop": "other.myshopify.com"})

        self.assertEqual(status, 200)
        self.main.db.verify_pos_credentials.assert_called_once_with("test-key", "test-secret")
        self.archive.assert_called_once_with(shop=self.shop, sku="SHIRT")
        self.assertEqual(response["results"], [result])
        self.assertEqual((response["total"], response["succeeded"], response["failed"]), (1, 1, 0))

    async def test_skus_must_be_a_nonempty_array_of_nonblank_strings(self) -> None:
        payloads = [
            None,
            [],
            {},
            {"skus": None},
            {"skus": "SHIRT"},
            {"skus": []},
            {"skus": [""]},
            {"skus": [" \t"]},
            {"skus": [None]},
            {"skus": [123]},
            {"skus": ["SHIRT", False]},
        ]
        for payload in payloads:
            with self.subTest(payload=payload):
                status, response = await self.post(payload)

                self.assertEqual(status, 400)
                self.assertEqual(response["error"]["code"], "invalid_status_skus")
                self.archive.assert_not_called()

    async def test_more_than_100_skus_are_rejected_even_if_duplicated(self) -> None:
        status, response = await self.post({"skus": ["SHIRT"] * 101})

        self.assertEqual(status, 400)
        self.assertEqual(response["error"]["code"], "too_many_status_skus")
        self.archive.assert_not_called()

    async def test_100_skus_are_accepted(self) -> None:
        skus = [f"SKU-{index}" for index in range(100)]
        self.archive.side_effect = [{"sku": sku, "success": True} for sku in skus]

        status, response = await self.post({"skus": skus})

        self.assertEqual(status, 200)
        self.assertEqual((response["total"], response["succeeded"], response["failed"]), (100, 100, 0))
        self.assertEqual(
            self.archive.call_args_list,
            [mock.call(shop=self.shop, sku=sku) for sku in skus],
        )

    async def test_skus_are_trimmed_and_duplicates_processed_once_in_order(self) -> None:
        self.archive.side_effect = [
            {"sku": "SHIRT", "success": True},
            {"sku": "JACKET", "success": True},
        ]

        status, response = await self.post({"skus": [" SHIRT ", "SHIRT", " JACKET", "SHIRT "]})

        self.assertEqual(status, 200)
        self.assertEqual(response["total"], 2)
        self.assertEqual(
            self.archive.call_args_list,
            [mock.call(shop=self.shop, sku="SHIRT"), mock.call(shop=self.shop, sku="JACKET")],
        )

    async def test_failed_skus_are_reported_without_stopping_remaining_items(self) -> None:
        failure = {"sku": "MISSING", "success": False, "message": "Product not found."}
        success = {"sku": "JACKET", "success": True, "archived": True}
        self.archive.side_effect = [SyncProcessingError("Stock lookup failed."), failure, success]

        status, response = await self.post({"skus": ["SHIRT", "MISSING", "JACKET"]})

        self.assertEqual(status, 200)
        self.assertEqual((response["total"], response["succeeded"], response["failed"]), (3, 1, 2))
        self.assertEqual(
            response["results"],
            [{"sku": "SHIRT", "success": False, "message": "Stock lookup failed."}, failure, success],
        )
        self.assertEqual(
            self.archive.call_args_list,
            [mock.call(shop=self.shop, sku=sku) for sku in ("SHIRT", "MISSING", "JACKET")],
        )


if __name__ == "__main__":
    unittest.main()
