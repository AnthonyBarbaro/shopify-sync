import asyncio
import importlib.util
import json
import unittest
from contextlib import ExitStack
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
    ) -> tuple[int, dict[str, object]]:
        body = json.dumps(payload).encode("utf-8")
        headers = [(b"content-type", b"application/json")]
        if authenticated:
            headers.extend([(b"x-api-key", b"test-key"), (b"x-api-secret", b"test-secret")])
        scope: Scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/sync/inventory/status/reconcile",
            "raw_path": b"/sync/inventory/status/reconcile",
            "query_string": b"",
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
