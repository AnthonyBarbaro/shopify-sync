import json
import logging
import tempfile
import unittest
from pathlib import Path
from threading import Event, current_thread
from unittest import mock

import requests

from windows_connector.connector import Connector
from windows_connector import connector as connector_module
from windows_connector.order_dbf import DbfField, _serialize_dbf


class ConnectorRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        directory = Path(temporary.name)
        config = directory / "connector.env"
        config.write_text("\n".join([
            f"POS_DBF_DIR={directory / 'pos'}",
            "SHOPIFY_SYNC_BASE_URL=https://sync.example",
            "SHOPIFY_SYNC_API_KEY=test-key",
            "SHOPIFY_SYNC_API_SECRET=test-secret",
            "ORDER_SYNC_ENABLED=false",
            f"CONNECTOR_DATA_DIR={directory / 'state'}",
        ]), encoding="utf-8")
        with mock.patch.dict("os.environ", {}, clear=True):
            self.connector = Connector(config_path=config)
        self.addCleanup(self.connector.session.close)
        self.connector.logger = mock.Mock()
        self.session = mock.Mock(spec=requests.Session)
        self.session.get.return_value.json.return_value = {"authorized": True}
        self.session.post.side_effect = self.success_response
        self.payloads = [self.customer("1"), self.customer("2")]
        self.real_load_customers = connector_module.dbf_pos_sync.load_customers
        patcher = mock.patch(
            "windows_connector.connector.dbf_pos_sync.load_customers",
            side_effect=lambda _args: (self.payloads, [], None),
        )
        self.loader = patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def customer(number: str) -> dict[str, object]:
        return {
            "source": "Customer.dbf", "pos_customer_number": number,
            "firstName": "Sample", "email": f"customer{number}@example.test",
        }

    @staticmethod
    def success_response(url: str, *, json: dict, timeout: int) -> mock.Mock:
        response = mock.Mock()
        response.json.return_value = {"results": [
            {"source": payload["source"], "pos_customer_number": payload["pos_customer_number"],
             "success": True, "customer_id": f"gid://shopify/Customer/{payload['pos_customer_number']}"}
            for payload in json["customers"]
        ]}
        return response

    def test_new_defaults_and_explicit_interval_override(self) -> None:
        self.assertEqual(self.connector.interval_seconds, 30)
        self.assertTrue(self.connector.customer_sync_enabled)
        self.assertEqual(self.connector.customer_sync_interval_seconds, 60)
        with mock.patch.dict("os.environ", {"SYNC_INTERVAL_SECONDS": "90"}, clear=True):
            configured = Connector(config_path=self.connector.config_path)
        self.addCleanup(configured.session.close)
        self.assertEqual(configured.interval_seconds, 90)

    def test_updater_report_is_retained_until_the_dashboard_accepts_it(self) -> None:
        reports = self.connector.state_path.parent / "updater-reports"
        reports.mkdir()
        spool = reports / "update.json"
        report = {"run_id": "test-run", "status": "failed", "stage": "download"}
        spool.write_text(json.dumps(report), encoding="utf-8-sig")
        self.session.post.side_effect = requests.Timeout()
        self.connector._flush_updater_reports(self.session)
        self.assertTrue(spool.exists())

        self.session.post.side_effect = None
        self.session.post.return_value.json.return_value = {"accepted": False}
        self.connector._flush_updater_reports(self.session)
        self.assertTrue(spool.exists())

        self.session.post.return_value.json.return_value = {"accepted": True}
        self.connector._flush_updater_reports(self.session)
        self.assertFalse(spool.exists())
        self.session.post.assert_called_with(
            "https://sync.example/sync/connector/update-report", json=report, timeout=5,
            allow_redirects=False,
        )

    def test_heartbeat_replays_quantity_issues_until_the_dashboard_accepts_them(self) -> None:
        self.connector._record_inventory_issue(
            "22270. 1 4", "pos_writeback_preview", pos_quantity=2, shopify_quantity=0, target_quantity=0,
        )
        self.session.post.side_effect = None
        self.session.post.return_value.json.return_value = {"inventory_issues_saved": False}
        self.connector._send_heartbeat(self.session)
        first = self.session.post.call_args.kwargs["json"]["inventory_issues"]
        self.assertEqual(first[0]["sku"], "22270. 1 4")
        self.assertEqual(first[0]["shopify_quantity"], 0)

        self.session.post.return_value.json.return_value = {"inventory_issues_saved": True}
        self.connector._send_heartbeat(self.session)
        self.assertEqual(self.session.post.call_args.kwargs["json"]["inventory_issues"], first)
        self.connector._send_heartbeat(self.session)
        self.assertNotIn("inventory_issues", self.session.post.call_args.kwargs["json"])

    def test_sales_board_connection_loss_logs_one_warning_and_retries(self) -> None:
        self.connector.logger = logging.getLogger("connector.sales_board.retries")
        stop = mock.Mock()
        stop.is_set.side_effect = [False, False, True]
        self.session.headers = {}
        settings = mock.Mock()
        settings.json.return_value = {"enabled": False}
        self.session.get.side_effect = [requests.ConnectionError("peer closed\nresponse"), settings]
        with (
            mock.patch.object(connector_module.requests, "Session") as factory,
            self.assertLogs(self.connector.logger, level="WARNING") as logs,
        ):
            factory.return_value.__enter__.return_value = self.session
            self.connector._sales_board_loop(stop)

        self.assertEqual(self.session.get.call_count, 2)
        self.assertEqual(logs.output, [
            "WARNING:connector.sales_board.retries:sales_board_sync_failed stage=settings reason=connection_interrupted",
        ])

    def test_customers_import_once_then_only_changed_records_across_restarts(self) -> None:
        self.connector._sync_customers(self.session)
        self.assertEqual(self.session.post.call_count, 1)
        self.connector._sync_customers(self.session)
        self.assertEqual(self.session.post.call_count, 1)
        self.payloads[0]["firstName"] = "Updated"
        self.connector._sync_customers(self.session)
        self.assertEqual(self.session.post.call_count, 2)
        self.assertEqual(self.session.post.call_args.kwargs["json"]["customers"], [self.payloads[0]])
        state_text = self.connector.customer_state_path.read_text(encoding="utf-8")
        self.assertNotIn("example.test", state_text)
        self.assertNotIn("Updated", state_text)
        self.assertEqual(len(json.loads(state_text)["customer_hashes"]), 2)
        self.assertFalse(self.connector.state_path.exists())

    def test_customer_reader_skips_unchanged_files_and_rescans_after_shipping_changes(self) -> None:
        self.connector.dbf_dir.mkdir()
        (self.connector.dbf_dir / 'Customer.dbf').write_bytes(b'customer')
        self.connector._sync_customers(self.session)
        self.connector._sync_customers(self.session)
        self.loader.assert_called_once()
        self.assertEqual(self.session.get.call_count, 2)

        (self.connector.dbf_dir / 'CustShip.dbf').write_bytes(b'new address')
        self.connector._sync_customers(self.session)
        self.connector._sync_customers(self.session, force_scan=True)
        self.assertEqual(self.loader.call_count, 3)
        self.session.post.assert_called_once()

    def test_price_reader_retries_an_unconfirmed_change_before_skipping_unchanged_file(self) -> None:
        self.connector.dbf_dir.mkdir()
        (self.connector.dbf_dir / 'pricechg.dbf').write_bytes(b'changed price')
        state = {'price_snapshot': {'101': '10.00'}}
        failure = mock.Mock(status_code=200)
        failure.json.return_value = {'results': [{'source_sku': '101', 'success': False}]}
        success = mock.Mock(status_code=200)
        success.json.return_value = {'results': [{'source_sku': '101', 'success': True}]}
        with (
            mock.patch.object(connector_module, 'read_pos_price_snapshot', return_value={'101': '12.00'}) as read,
            mock.patch.object(self.connector.session, 'post', side_effect=[failure, success]) as post,
        ):
            self.connector._sync_pos_price_changes(state)
            self.assertEqual(state['price_snapshot'], {'101': '10.00'})
            self.connector._sync_pos_price_changes(state)
            self.connector._sync_pos_price_changes(state)
        self.assertEqual(state['price_snapshot'], {'101': '12.00'})
        self.assertEqual(read.call_count, 2)
        self.assertEqual(post.call_count, 2)

    def test_reads_real_customer_dbf_and_sends_contact_payload(self) -> None:
        self.connector.dbf_dir.mkdir()
        path = self.connector.dbf_dir / 'Customer.dbf'
        path.write_bytes(_serialize_dbf([
            DbfField('CUST_NUM', 'C', 10), DbfField('FIRST_NAME', 'C', 40),
            DbfField('LAST_NAME', 'C', 40), DbfField('INTERNET', 'C', 80),
        ], [{'CUST_NUM': '123', 'FIRST_NAME': 'Sample', 'LAST_NAME': 'Customer',
             'INTERNET': 'sample@example.test'}]))
        self.loader.side_effect = self.real_load_customers
        self.connector._sync_customers(self.session)
        payload = self.session.post.call_args.kwargs['json']['customers'][0]
        self.assertEqual(payload['pos_customer_number'], '123')
        self.assertEqual(payload['email'], 'sample@example.test')
        self.connector._sync_customers(self.session)
        self.assertEqual(self.session.post.call_count, 1)

    def test_only_confirmed_successes_advance_baselines(self) -> None:
        response = mock.Mock()
        response.json.return_value = {"results": [
            {"source": "Customer.dbf", "pos_customer_number": "1", "success": True, "customer_id": "id1"},
            {"source": "Customer.dbf", "pos_customer_number": "2", "success": False},
        ]}
        self.session.post.side_effect = [response]
        self.connector._sync_customers(self.session)
        self.session.post.side_effect = self.success_response
        self.connector._sync_customers(self.session)
        self.assertEqual(self.session.post.call_args.kwargs["json"]["customers"], [self.payloads[1]])

    def test_missing_results_and_timeouts_are_retried(self) -> None:
        self.session.post.side_effect = requests.Timeout("response lost")
        with self.assertRaises(requests.Timeout):
            self.connector._sync_customers(self.session)
        self.session.post.side_effect = None
        self.session.post.return_value.json.return_value = {"results": []}
        self.connector._sync_customers(self.session)
        self.session.post.side_effect = self.success_response
        self.connector._sync_customers(self.session)
        self.assertEqual(self.session.post.call_args.kwargs["json"]["customers"], self.payloads)

    def test_disabled_dry_run_and_missing_permissions_do_not_upload(self) -> None:
        self.connector.customer_sync_enabled = False
        self.connector._sync_customers(self.session)
        self.loader.assert_not_called()
        self.session.get.assert_not_called()
        self.connector.customer_sync_enabled = True
        self.connector.dry_run = True
        self.connector._sync_customers(self.session)
        self.session.get.assert_not_called()
        self.session.post.assert_not_called()
        self.assertFalse(self.connector.customer_state_path.exists())
        self.connector.dry_run = False
        self.session.get.return_value.json.return_value = {"authorized": False}
        self.connector._sync_customers(self.session)
        self.session.post.assert_not_called()
        self.assertFalse(self.connector.customer_state_path.exists())

    def test_pass_is_bounded_and_rotates_past_failures(self) -> None:
        self.payloads = [self.customer(str(number)) for number in range(60)]
        self.session.post.side_effect = None
        self.session.post.return_value.json.return_value = {"results": []}
        self.connector._sync_customers(self.session)
        first = [payload for call in self.session.post.call_args_list
                 for payload in call.kwargs["json"]["customers"]]
        self.assertEqual(len(first), 50)
        self.assertLessEqual(max(len(call.kwargs["json"]["customers"])
                                 for call in self.session.post.call_args_list), 10)
        self.session.post.reset_mock()
        self.connector._sync_customers(self.session)
        second = self.session.post.call_args_list[0].kwargs["json"]["customers"]
        self.assertEqual(second[0]["pos_customer_number"], "50")

    def test_customers_without_ids_are_skipped_and_removed_customers_are_not_deleted(self) -> None:
        self.payloads.append({"firstName": "No stable POS identifier"})
        self.connector._sync_customers(self.session)
        self.assertEqual(len(self.session.post.call_args.kwargs["json"]["customers"]), 2)
        self.payloads = []
        self.connector._sync_customers(self.session)
        self.assertEqual(self.session.post.call_count, 1)
        self.assertEqual(json.loads(self.connector.customer_state_path.read_text())["customer_hashes"], {})

    def test_heartbeat_and_orders_continue_while_inventory_cycle_is_blocked(self) -> None:
        self.connector.customer_sync_enabled = False
        self.connector.order_sync_enabled = True
        cycle_started = Event()
        heartbeat_received = Event()
        orders_polled = Event()
        threads = []

        def send_heartbeat(session: requests.Session) -> None:
            self.assertTrue(cycle_started.wait(timeout=2))
            threads.append(current_thread())
            heartbeat_received.set()

        def poll_orders(session: requests.Session) -> None:
            self.assertTrue(cycle_started.wait(timeout=2))
            self.assertIsNot(session, self.connector.session)
            orders_polled.set()

        def blocked_cycle(*, scheduled: bool) -> None:
            self.assertTrue(scheduled)
            cycle_started.set()
            self.assertTrue(heartbeat_received.wait(timeout=2))
            self.assertTrue(orders_polled.wait(timeout=2))
            raise KeyboardInterrupt()

        with (
            mock.patch.object(self.connector, "_send_heartbeat", side_effect=send_heartbeat),
            mock.patch.object(self.connector, "_sync_order_inbox", side_effect=poll_orders),
            mock.patch.object(self.connector, "_sales_board_loop"),
            mock.patch.object(self.connector, "run_cycle", side_effect=blocked_cycle),
        ):
            with self.assertRaises(KeyboardInterrupt):
                self.connector.run_forever()
        self.assertEqual(len(threads), 1)
        self.assertIsNot(threads[0], current_thread())

    def test_heartbeat_retries_network_failures(self) -> None:
        stop = mock.Mock()
        stop.is_set.side_effect = [False, False, True]
        with mock.patch.object(self.connector, "_send_heartbeat", side_effect=[requests.Timeout(), None]) as send:
            self.connector._heartbeat_loop(stop)
        self.assertEqual(send.call_count, 2)
        self.assertEqual(stop.wait.call_args_list, [mock.call(30), mock.call(30)])

    def test_once_runs_customers_even_if_inventory_fails(self) -> None:
        with (
            mock.patch.object(self.connector, "_send_heartbeat"),
            mock.patch.object(self.connector, "run_cycle", side_effect=OSError("POS unavailable")),
            mock.patch.object(self.connector, "_sync_customers") as customers,
        ):
            self.assertEqual(self.connector.run_forever(once=True), 1)
        customers.assert_called_once_with(self.connector.session)

    def test_heartbeat_wakes_inventory_and_customers_for_pending_force_request(self) -> None:
        self.session.post.side_effect = None
        self.session.post.return_value.json.return_value = {'sync_request_id': 'request1'}
        self.connector._send_heartbeat(self.session)
        self.assertEqual(self.connector._sync_request_id, 'request1')
        self.assertTrue(self.connector._sync_wakeup.is_set())
        self.assertTrue(self.connector._customer_wakeup.is_set())
        self.connector._sync_wakeup.clear()
        self.connector._set_sync_progress('request1', 'reading_shopify')
        self.connector._send_heartbeat(self.session)
        self.assertFalse(self.connector._sync_wakeup.is_set())
        self.assertEqual(self.session.post.call_args.kwargs['json']['sync_progress'], {
            'request_id': 'request1', 'stage': 'reading_shopify',
        })

    def test_forced_scan_is_acknowledged_only_after_success_and_retries_on_failure(self) -> None:
        self.connector.customer_sync_enabled = False
        self.connector._sync_request_id = 'request1'
        with (
            mock.patch.object(self.connector, '_send_heartbeat'),
            mock.patch.object(self.connector, 'run_cycle', side_effect=OSError('POS locked')),
            mock.patch.object(self.connector, '_complete_sync_request') as complete,
        ):
            self.assertEqual(self.connector.run_forever(once=True), 1)
            complete.assert_not_called()
        self.assertEqual(self.connector._sync_progress_stage, 'retrying')
        self.connector.session = self.session
        self.session.post.side_effect = None
        self.session.post.return_value.json.return_value = {'completed': True}
        with (
            mock.patch.object(self.connector, '_send_heartbeat'),
            mock.patch.object(self.connector, 'run_cycle') as scan,
        ):
            self.assertEqual(self.connector.run_forever(once=True), 0)
        scan.assert_called_once_with(force_full=True, sync_request_id='request1')
        self.assertIsNone(self.connector._sync_request_id)
        self.assertIsNone(self.connector._sync_progress_stage)

    def test_failed_full_scan_lets_the_regular_inventory_cycle_run_before_retrying(self) -> None:
        self.connector.customer_sync_enabled = False
        self.connector.dry_run = True
        self.connector._sync_request_id = "request1"
        with (
            mock.patch.object(self.connector, "_heartbeat_loop"),
            mock.patch.object(self.connector._sync_wakeup, "wait"),
            mock.patch.object(self.connector, "run_cycle", side_effect=[requests.ReadTimeout(), None, KeyboardInterrupt()]) as scan,
            mock.patch.object(self.connector, "_complete_sync_request") as complete,
            self.assertRaises(KeyboardInterrupt),
        ):
            self.connector.run_forever()
        self.assertEqual(scan.call_args_list, [
            mock.call(force_full=True, sync_request_id="request1"),
            mock.call(scheduled=True),
            mock.call(force_full=True, sync_request_id="request1"),
        ])
        complete.assert_not_called()

    def test_inventory_snapshot_assembles_pages_and_rejects_an_inconsistent_location(self) -> None:
        connector = self.connector
        connector.session = self.session
        first_item, next_item = {"sku": "FIRST"}, {"sku": "NEXT"}
        pages = [
            {"schema_version": 2, "location_id": "loc1", "items": [first_item], "next_id": 25000},
            {"schema_version": 2, "location_id": "loc1", "items": [next_item], "next_id": None},
        ]
        for compatible in (True, False):
            with self.subTest(compatible=compatible):
                self.session.get.reset_mock()
                payloads = [pages[0], pages[1] if compatible else {**pages[1], "location_id": "loc2"}]
                self.session.get.side_effect = [mock.Mock(status_code=200, **{"json.return_value": page}) for page in payloads]
                if compatible:
                    self.assertEqual(connector._fetch_inventory_snapshot(), {"location_id": "loc1", "items": [first_item, next_item]})
                else:
                    with self.assertRaisesRegex(RuntimeError, "location changed"):
                        connector._fetch_inventory_snapshot()
                self.assertEqual(self.session.get.call_args_list, [
                    mock.call("https://sync.example/sync/inventory/page", timeout=connector.timeout),
                    mock.call("https://sync.example/sync/inventory/page", params={"after_id": 25000}, timeout=connector.timeout),
                ])

    def test_inventory_snapshot_works_with_the_previous_server_route(self) -> None:
        self.connector.session = self.session
        payload = {"schema_version": 2, "location_id": "loc1", "items": [{"sku": "FIRST"}]}
        self.session.get.side_effect = [
            mock.Mock(status_code=404), mock.Mock(status_code=200, **{"json.return_value": payload}),
        ]
        self.assertEqual(self.connector._fetch_inventory_snapshot(), {"location_id": "loc1", "items": payload["items"]})


if __name__ == "__main__":
    unittest.main()
