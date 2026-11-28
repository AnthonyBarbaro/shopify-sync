import importlib.util
import json
import sqlite3
import tempfile
import unittest
from datetime import timedelta
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from unittest import mock

from starlette.requests import Request

from app.config import Settings
from app.db import DatabaseStore, ShopRecord
from app.models import ConnectorHeartbeatRequest, CustomerBulkSyncResponse, CustomerSyncResult
from app.shopify import ShopifyClient
from app.utils import utc_now


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

    def test_force_requests_coalesce_are_shop_isolated_and_acknowledgments_are_versioned(self) -> None:
        first = self.store.request_connector_sync(shop_domain='one.myshopify.com')
        self.assertEqual(self.store.request_connector_sync(shop_domain='one.myshopify.com'), first)
        self.assertFalse(self.store.complete_connector_sync(shop_domain='two.myshopify.com', request_id=first['request_id']))
        self.store.record_connector_sync_progress(shop_domain='two.myshopify.com', request_id=first['request_id'], stage='reading_pos')
        self.assertIsNone(self.store.get_connector_sync_request(shop_domain='one.myshopify.com')['progress_stage'])
        self.store.record_connector_sync_progress(shop_domain='one.myshopify.com', request_id=first['request_id'], stage='reading_shopify')
        self.assertEqual(self.store.get_connector_sync_request(shop_domain='one.myshopify.com')['progress_stage'], 'reading_shopify')
        counts = {'updated': 2, 'already_matched': 3, 'skipped': 1}
        self.assertTrue(self.store.complete_connector_sync(
            shop_domain='one.myshopify.com', request_id=first['request_id'], result=counts,
        ))
        self.assertTrue(self.store.complete_connector_sync(shop_domain='one.myshopify.com', request_id=first['request_id']))
        self.assertEqual(self.store.get_connector_sync_request(shop_domain='one.myshopify.com')['result'], counts)
        self.store.record_connector_sync_progress(shop_domain='one.myshopify.com', request_id=first['request_id'], stage='retrying')
        self.assertEqual(self.store.get_connector_sync_request(shop_domain='one.myshopify.com')['progress_stage'], 'reading_shopify')
        newer = self.store.request_connector_sync(shop_domain='one.myshopify.com')
        self.assertNotEqual(newer['request_id'], first['request_id'])
        self.assertIsNone(newer['progress_stage'])
        self.assertIsNone(newer['result'])
        self.assertFalse(self.store.complete_connector_sync(shop_domain='one.myshopify.com', request_id=first['request_id']))
        self.assertIsNone(self.store.get_connector_sync_request(shop_domain='one.myshopify.com')['completed_at'])
        self.store.mark_shop_uninstalled('one.myshopify.com')
        self.assertIsNone(self.store.get_connector_sync_request(shop_domain='one.myshopify.com')['request_id'])

    def test_product_details_are_persisted_without_replacing_recorded_quantities(self) -> None:
        self.record()
        self.store.update_inventory_history_details(
            shop_domain='two.myshopify.com', sku='123. 1 1', product_title='Wrong shop', variant_title='Wrong',
        )
        self.assertIsNone(self.store.list_recent_inventory_changes(shop_domain='one.myshopify.com')[0].product_title)
        self.store.update_inventory_history_details(
            shop_domain='one.myshopify.com', sku='123. 1 1', product_title='Linen shirt', variant_title='Blue / M',
        )
        row = self.store.list_recent_inventory_changes(shop_domain='one.myshopify.com')[0]
        self.assertEqual((row.product_title, row.variant_title), ('Linen shirt', 'Blue / M'))
        self.assertEqual((row.delta, row.quantity_after_change), (-1, 0))


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
        success = {'sku': '123', 'success': True, 'inventory_item_id': '1', 'quantity_after_change': 0,
                   'product_title': 'Linen shirt', 'variant_title': 'Blue / M'}
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
        self.assertEqual((rows[0].product_title, rows[0].variant_title), ('Linen shirt', 'Blue / M'))

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

    async def test_heartbeat_does_not_make_stale_inventory_or_orders_active(self) -> None:
        self.store.record_connector_heartbeat(shop_domain=self.shop.shop_domain, channel='inventory')
        stale = (utc_now() - timedelta(minutes=40)).isoformat()
        with self.store._connect() as connection:
            connection.execute('UPDATE connector_heartbeats SET last_inventory_poll_at = ?', (stale,))
            connection.commit()
        self.main.connector_heartbeat(ConnectorHeartbeatRequest(
            version='1.2', sync_interval_seconds=30, customer_sync_enabled=True,
        ), self.shop)
        with (
            mock.patch.object(self.main, '_get_session_context', return_value=(self.shop, {})),
            mock.patch.object(self.main, 'get_granted_access_scopes', return_value={'read_customers', 'write_customers'}),
        ):
            payload = json.loads((await self.main.bridge_status(Request({'type': 'http', 'headers': []}))).body)
        self.assertTrue(payload['connector']['active'])
        self.assertEqual(payload['connector']['sync_interval_seconds'], 30)
        self.assertFalse(payload['inventory']['active'])
        self.assertFalse(payload['orders']['active'])
        self.assertTrue(payload['customers']['enabled'])
        self.assertTrue(payload['customers']['authorized'])
        self.assertFalse(payload['customers']['active'])

    async def test_customer_status_checks_scopes_and_keeps_channels_separate(self) -> None:
        for scopes, expected in [([], False), (['read_customers'], False),
                                 (['read_customers', 'write_customers'], True)]:
            with mock.patch.object(self.main, 'get_granted_access_scopes', return_value=scopes):
                result = json.loads(self.main.connector_customer_status(self.shop).body)
                self.assertEqual(result['authorized'], expected)
        heartbeat = self.store.get_connector_heartbeat(shop_domain=self.shop.shop_domain)
        self.assertIsNotNone(heartbeat['last_customer_poll_at'])
        self.assertIsNone(heartbeat['last_inventory_poll_at'])
        self.assertIsNone(heartbeat['last_order_poll_at'])
        self.assertIsNone(self.store.get_connector_heartbeat(shop_domain='other.myshopify.com')['last_seen_at'])

    async def test_customer_history_reports_outcomes_without_exposing_contact_details(self) -> None:
        now = utc_now().isoformat()
        rows = [
            CustomerSyncResult(pos_customer_number='101', success=True, message='Created',
                               timestamp=now, details={'created': True}, email='private@example.test'),
            CustomerSyncResult(pos_customer_number='102', success=True, message='Matched',
                               timestamp=now, details={'skipped_existing': True}),
            CustomerSyncResult(pos_customer_number='103', success=False, message='private@example.test',
                               timestamp=now),
        ]
        result = CustomerBulkSyncResponse(total=3, succeeded=2, failed=1, timestamp=now, results=rows)
        request = Request({'type': 'http', 'path': '/sync/customers', 'method': 'POST', 'headers': []})
        self.store.record_feed_event(
            shop_domain=self.shop.shop_domain, source='test', endpoint='/sync/customers', method='POST',
            sku='100', title=None, success=True, message='Customer synced successfully.', product_id=None,
            variant_id=None, request_payload='', normalized_payload=None,
        )
        with (
            mock.patch.object(self.main, 'parse_external_request_payload', new=mock.AsyncMock(return_value=[
                {'pos_customer_number': row.pos_customer_number} for row in rows
            ])),
            mock.patch.object(self.main, 'run_with_shop_retry', return_value=result),
            mock.patch.object(self.main, 'run_in_threadpool', side_effect=lambda fn, *args: fn(*args)),
            mock.patch.object(self.main, '_get_session_context', return_value=(self.shop, {})) as session,
            mock.patch.object(self.main, 'get_granted_access_scopes', return_value=[]),
        ):
            await self.main.sync_customers(request, self.shop)
            payload = json.loads((await self.main.bridge_status(request)).body)
            recent = payload['customers']['recent']
            self.assertEqual([(row['pos_customer_number'], row['outcome']) for row in recent], [
                ('103', 'failed'), ('102', 'matched'), ('101', 'created'), ('100', 'processed'),
            ])
            self.assertTrue(all(set(row) == {'pos_customer_number', 'outcome', 'received_at'} for row in recent))
            session.return_value = (replace(self.shop, shop_domain='other.myshopify.com'), {})
            other = json.loads((await self.main.bridge_status(request)).body)
            self.assertEqual(other['customers']['recent'], [])

    async def test_legacy_history_names_are_loaded_once_and_unknown_skus_are_throttled(self) -> None:
        for sku in ['123', 'missing']:
            self.store.record_recent_inventory_change(
                shop_domain=self.shop.shop_domain, idempotency_key=sku, sku=sku,
                delta=1, quantity_after_change=2,
            )
        with (
            mock.patch.object(self.main, '_get_session_context', return_value=(self.shop, {})),
            mock.patch.object(self.main, 'get_granted_access_scopes', return_value=['read_products']),
            mock.patch.object(self.main.shopify_client, 'get_inventory_history_details', return_value={
                '123': {'product_title': 'Linen shirt', 'variant_title': 'Blue / M'},
            }) as details,
        ):
            request = Request({'type': 'http', 'headers': []})
            payload = json.loads((await self.main.bridge_status(request)).body)
            self.assertEqual(payload['inventory']['recent'][1]['product_title'], 'Linen shirt')
            await self.main.bridge_status(request)
        details.assert_called_once()

    async def test_legacy_heartbeat_database_is_upgraded_without_losing_checkins(self) -> None:
        path = self.store.database_path.parent / 'legacy.sqlite3'
        with sqlite3.connect(path) as connection:
            connection.execute('CREATE TABLE connector_heartbeats (shop_domain TEXT PRIMARY KEY, '
                               'last_seen_at TEXT NOT NULL, last_inventory_poll_at TEXT, '
                               'last_order_poll_at TEXT, last_catalog_sync_at TEXT)')
            connection.execute('INSERT INTO connector_heartbeats VALUES (?, ?, ?, NULL, NULL)',
                               (self.shop.shop_domain, '2026-01-01', '2026-01-01'))
        store = DatabaseStore(str(path), 'test-secret')
        heartbeat = store.get_connector_heartbeat(shop_domain=self.shop.shop_domain)
        self.assertEqual(heartbeat['last_inventory_poll_at'], '2026-01-01')
        self.assertIsNone(heartbeat['last_customer_poll_at'])


class InventoryHistoryLookupTests(unittest.TestCase):
    def test_names_require_one_exact_sku_match_and_complete_results(self) -> None:
        client = ShopifyClient(mock.Mock())
        node = {'sku': 'exact', 'title': 'Blue / M', 'product': {'title': 'Linen shirt'}}
        with mock.patch.object(client, 'graphql', return_value={'data': {
            'item0': {'nodes': [dict(node, sku='exact-other'), node]},
            'item1': {'nodes': [dict(node, sku='duplicate'), dict(node, sku='duplicate')]},
            'item2': {'nodes': [dict(node, sku='truncated')], 'pageInfo': {'hasNextPage': True}},
        }}) as graphql:
            result = client.get_inventory_history_details('one.myshopify.com', 'test-token', ['exact', 'duplicate', 'truncated'])
        self.assertEqual(result, {'exact': {'product_title': 'Linen shirt', 'variant_title': 'Blue / M'}})
        self.assertEqual(graphql.call_args.kwargs['operation_name'], 'InventoryHistoryDetails')


if __name__ == '__main__':
    unittest.main()
