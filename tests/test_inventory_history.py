import importlib.util
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from unittest import mock

from starlette.requests import Request

from app.config import Settings
from app.db import DatabaseStore, ShopRecord


class InventoryHistoryTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = DatabaseStore(str(Path(temporary.name) / 'history.sqlite3'), 'test-secret')

    def record(
        self,
        key: str = 'event-1',
        *,
        shop_domain: str = 'one.myshopify.com',
        sku: str = '123. 1 1',
        delta: int = -1,
        quantity_after_change: int | None = 0,
    ) -> None:
        self.store.record_recent_inventory_change(
            shop_domain=shop_domain, idempotency_key=key,
            sku=sku, delta=delta, quantity_after_change=quantity_after_change,
        )

    def test_retries_do_not_duplicate_or_replace_the_original_change(self) -> None:
        self.record()
        self.record(quantity_after_change=20)
        rows = self.store.list_recent_inventory_changes(shop_domain='one.myshopify.com')
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0].delta, rows[0].quantity_after_change), (-1, 0))

    def test_history_is_bounded_and_newest_first_and_isolated_per_shop(self) -> None:
        self.record(shop_domain='two.myshopify.com', sku='other')
        for index in range(55):
            self.record(str(index), sku=str(index))
        rows = self.store.list_recent_inventory_changes(shop_domain='one.myshopify.com', limit=100)
        self.assertEqual(len(rows), 50)
        self.assertEqual([row.sku for row in rows], [str(i) for i in range(54, 4, -1)])
        other = self.store.list_recent_inventory_changes(shop_domain='two.myshopify.com')
        self.assertEqual([row.sku for row in other], ['other'])

    def test_zero_delta_is_omitted_and_unknown_quantity_is_preserved(self) -> None:
        self.record(delta=0)
        self.assertEqual(self.store.list_recent_inventory_changes(shop_domain='one.myshopify.com'), [])
        self.record(delta=2, quantity_after_change=None)
        row = self.store.list_recent_inventory_changes(shop_domain='one.myshopify.com')[0]
        self.assertIsNone(row.quantity_after_change)
        self.assertEqual(row.delta, 2)

    def test_history_survives_queue_acknowledgment_and_is_removed_on_uninstall(self) -> None:
        self.record()
        self.store.upsert_inventory_change(
            shop_domain='one.myshopify.com', inventory_item_id='item',
            location_id='location', sku='123', quantity=0,
        )
        change = self.store.list_inventory_changes(shop_domain='one.myshopify.com')[0]
        self.store.acknowledge_inventory_changes(
            shop_domain='one.myshopify.com', changes=[(change.id, change.version)],
        )
        self.assertEqual(len(self.store.list_recent_inventory_changes(shop_domain='one.myshopify.com')), 1)
        self.store.mark_shop_uninstalled('one.myshopify.com')
        self.assertEqual(self.store.list_recent_inventory_changes(shop_domain='one.myshopify.com'), [])

    def test_optional_history_database_errors_do_not_fail_adjustments(self) -> None:
        with mock.patch.object(self.store, '_connect', side_effect=sqlite3.OperationalError('locked')):
            with self.assertLogs(self.store.logger, level='ERROR'):
                self.record()


class InventoryHistoryIntegrationTests(unittest.IsolatedAsyncioTestCase):
    main: ModuleType

    @classmethod
    def setUpClass(cls) -> None:
        settings = Settings(
            shopify_client_id='test-client', shopify_client_secret='test-secret',
            app_base_url='https://sync.example.test', app_session_secret='test-session',
            credential_encryption_secret='test-encryption', app_scopes='read_inventory',
            database_path=':memory:',
        )
        spec = importlib.util.spec_from_file_location(
            'app._inventory_history_tests', Path(__file__).resolve().parents[1] / 'app/main.py',
        )
        if spec is None or spec.loader is None:
            raise RuntimeError('Could not load API module')
        cls.main = importlib.util.module_from_spec(spec)
        with (
            mock.patch('app.config.get_settings', return_value=settings),
            mock.patch('app.db.DatabaseStore', return_value=mock.Mock(spec=DatabaseStore)),
        ):
            spec.loader.exec_module(cls.main)

    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = DatabaseStore(str(Path(temporary.name) / 'history.sqlite3'), 'test-secret')
        patcher = mock.patch.object(self.main, 'db', self.store)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.shop = ShopRecord(shop_domain='one.myshopify.com', access_token='test-token')

    async def test_only_successful_nonzero_adjustments_are_recorded_once(self) -> None:
        adjustments = [
            {'sku': '123', 'delta': -1, 'idempotency_key': 'retry-key'},
            {'sku': '123', 'delta': -1, 'idempotency_key': 'retry-key'},
            {'sku': '123', 'delta': 0, 'idempotency_key': 'zero'},
            {'sku': '123', 'delta': 2, 'idempotency_key': 'failure'},
        ]
        success = {'sku': '123', 'success': True, 'inventory_item_id': '1', 'quantity_after_change': 0}
        with (
            mock.patch.object(self.main, 'parse_external_request_payload',
                              new=mock.AsyncMock(return_value={'adjustments': adjustments})),
            mock.patch.object(self.main, 'run_with_shop_retry',
                              side_effect=[success, success, success, RuntimeError('failed')]),
        ):
            response = await self.main.connector_inventory_adjustments(
                Request({'type': 'http', 'headers': []}), self.shop,
            )
        self.assertEqual(json.loads(response.body)['failed'], 1)
        rows = self.store.list_recent_inventory_changes(shop_domain=self.shop.shop_domain)
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0].sku, rows[0].delta, rows[0].quantity_after_change), ('123', -1, 0))

    async def test_dashboard_history_uses_authenticated_shop_and_latest_ten(self) -> None:
        for index in range(12):
            self.store.record_recent_inventory_change(
                shop_domain=self.shop.shop_domain, idempotency_key=str(index),
                sku=str(index), delta=1, quantity_after_change=index,
            )
        request = Request({'type': 'http', 'headers': []})
        with (
            mock.patch.object(self.main, '_get_session_context', return_value=(self.shop, {})) as session,
            mock.patch.object(self.main, 'get_granted_access_scopes', return_value=[]),
        ):
            payload = json.loads((await self.main.bridge_status(request)).body)
            session.assert_called_once_with(request)
            self.assertEqual(len(payload['inventory']['recent']), 10)
            self.assertEqual(payload['inventory']['recent'][0]['sku'], '11')
            session.return_value = (replace(self.shop, shop_domain='other.myshopify.com'), {})
            other = json.loads((await self.main.bridge_status(request)).body)
            self.assertEqual(other['inventory']['recent'], [])


if __name__ == '__main__':
    unittest.main()
