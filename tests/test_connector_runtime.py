import json
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

    def test_heartbeat_continues_while_inventory_cycle_is_blocked(self) -> None:
        self.connector.customer_sync_enabled = False
        cycle_started = Event()
        heartbeat_received = Event()
        threads = []

        def send_heartbeat(session: requests.Session) -> None:
            self.assertTrue(cycle_started.wait(timeout=2))
            threads.append(current_thread())
            heartbeat_received.set()

        def blocked_cycle() -> None:
            cycle_started.set()
            self.assertTrue(heartbeat_received.wait(timeout=2))
            raise KeyboardInterrupt()

        with (
            mock.patch.object(self.connector, "_send_heartbeat", side_effect=send_heartbeat),
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
        self.connector._send_heartbeat(self.session)
        self.assertFalse(self.connector._sync_wakeup.is_set())

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
        self.connector.session = self.session
        self.session.post.side_effect = None
        self.session.post.return_value.json.return_value = {'completed': True}
        with (
            mock.patch.object(self.connector, '_send_heartbeat'),
            mock.patch.object(self.connector, 'run_cycle') as scan,
        ):
            self.assertEqual(self.connector.run_forever(once=True), 0)
        scan.assert_called_once_with(force_full=True)
        self.assertIsNone(self.connector._sync_request_id)


if __name__ == "__main__":
    unittest.main()
