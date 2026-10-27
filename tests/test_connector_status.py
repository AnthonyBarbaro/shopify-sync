import json
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from windows_connector import connector as connector_module
from windows_connector.connector import Connector, save_state


class NightlyProductStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.connector = Connector.__new__(Connector)
        self.connector.dry_run = False
        self.connector.order_sync_enabled = False
        self.connector.price_sync_enabled = False
        self.connector.initial_catalog_upload = True
        self.connector.nightly_full_sync_hour = 0
        self.connector.writeback_mode = "dry-run"
        self.connector.base_url = "https://sync.example"
        self.connector.batch_size = 25
        self.connector.timeout = 30
        self.connector.state_path = Path(temporary_directory.name) / "state.json"
        self.connector.dbf_dir = Path("unused-pos-data")
        self.connector.logger = mock.Mock()
        self.connector.session = mock.Mock()
        self.connector.session.post.side_effect = self._post
        self.connector._retry_pending = mock.Mock()
        self.connector._fetch_inventory_changes = mock.Mock(return_value=[])
        self.connector._collect_pos_event_skus = mock.Mock(return_value=(set(), False))
        self.connector._collect_new_numeric_product_skus = mock.Mock(return_value=set())
        self.connector._reader_args = mock.Mock(return_value=SimpleNamespace())
        self.connector._ready_matrix_structure_repairs = mock.Mock(return_value=[])
        self.connector._update_matrix_structure_state = mock.Mock(return_value=(True, set()))
        self.connector._acknowledge_inventory_changes = mock.Mock()
        self.payloads = [SimpleNamespace(payload={"sku": "ABC", "quantity": 0})]
        self.snapshot = {
            "location_id": "gid://shopify/Location/4",
            "items": [self._snapshot_item("ABC", 1)],
        }
        self.connector._fetch_inventory_snapshot = mock.Mock(return_value=self.snapshot)
        self.state = {
            "version": 1,
            "catalog_complete": True,
            "catalog_products": ["ABC"],
            "quantities": {"ABC": {"canonical": 1, "pos_seen": 1, "shop_seen": 1}},
            "sku_bases": {"ABC": "ABC"},
            "matrix_option_schema_version": 2,
            "inventory_reconcile_schema_version": 2,
            "catalog_structure_schema_version": 1,
            "last_full_reconcile_date": "2026-09-13",
        }
        save_state(self.connector.state_path, self.state)
        patches = ExitStack()
        self.addCleanup(patches.close)
        self.now = patches.enter_context(mock.patch.object(connector_module, "datetime"))
        self.now.now.return_value = datetime(2026, 9, 14, 0, 10)
        self.load_products = patches.enter_context(
            mock.patch.object(
                connector_module.dbf_pos_sync,
                "load_products",
                return_value=(self.payloads, SimpleNamespace(skipped_non_sellable=0)),
            )
        )
        patches.enter_context(
            mock.patch.object(connector_module, "read_targeted_pos_quantities", return_value={})
        )

    @staticmethod
    def _snapshot_item(sku: str, quantity: int) -> dict:
        return {
            "sku": sku,
            "quantity": quantity,
            "location_id": "gid://shopify/Location/4",
            "available_at_location": True,
            "duplicate_sku_count": 0,
        }

    @staticmethod
    def _post(url: str, *, json: dict, timeout: int) -> mock.Mock:
        response = mock.Mock()
        if url.endswith("/adjustments"):
            results = [
                {
                    "sku": action["sku"],
                    "success": True,
                    "quantity_after_change": action["target_quantity"],
                }
                for action in json["adjustments"]
            ]
        else:
            results = [{"sku": sku, "success": True} for sku in json["skus"]]
        response.json.return_value = {"results": results}
        return response

    def _persisted(self) -> dict:
        return json.loads(self.connector.state_path.read_text(encoding="utf-8"))

    def test_nightly_archiving_runs_after_adjustments_and_once_per_day(self) -> None:
        self.connector.run_cycle()
        calls = self.connector.session.post.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[0].args[0].endswith("/inventory/adjustments"))
        self.assertTrue(calls[1].args[0].endswith("/inventory/status/reconcile"))
        self.assertEqual(calls[1].kwargs["json"], {"skus": ["ABC"]})
        self.assertEqual(self._persisted()["last_product_status_reconcile_date"], "2026-09-14")
        self.assertNotIn("pending_product_status_reconcile_date", self._persisted())

        self.connector.run_cycle()
        self.assertEqual(self.connector.session.post.call_count, 2)
        self.connector._fetch_inventory_snapshot.assert_called_once()

        self.now.now.return_value = datetime(2026, 9, 15, 0, 10)
        self.snapshot["items"][0]["quantity"] = 0
        self.connector.run_cycle()
        self.assertEqual(self.connector.session.post.call_count, 3)
        self.assertEqual(self._persisted()["last_product_status_reconcile_date"], "2026-09-15")

    def test_upgrade_and_daytime_events_wait_for_next_nightly_pass(self) -> None:
        self.state["last_full_reconcile_date"] = "2026-09-14"
        self.state["inventory_reconcile_schema_version"] = 1
        save_state(self.connector.state_path, self.state)
        self.connector.run_cycle()
        self.connector._fetch_inventory_snapshot.assert_called_once()
        self.assertEqual(self.connector.session.post.call_count, 1)
        self.assertTrue(self.connector.session.post.call_args.args[0].endswith("/adjustments"))
        self.assertNotIn("last_product_status_reconcile_date", self._persisted())

        self.connector._collect_pos_event_skus.return_value = ({"ABC"}, False)
        self.connector.run_cycle()
        self.assertEqual(self.connector.session.post.call_count, 1)
        self.assertNotIn("last_product_status_reconcile_date", self._persisted())

    def test_failed_status_result_retries_despite_completed_quantity_date(self) -> None:
        failed = mock.Mock()
        failed.json.return_value = {"results": [{"sku": "ABC", "success": False}]}
        self.connector.session.post.side_effect = [
            self._post("/adjustments", json={"adjustments": [{"sku": "ABC", "target_quantity": 0}]}, timeout=30),
            failed,
        ]
        self.connector.run_cycle()
        persisted = self._persisted()
        self.assertEqual(persisted["last_full_reconcile_date"], "2026-09-14")
        self.assertEqual(persisted["pending_product_status_reconcile_date"], "2026-09-14")
        self.assertNotIn("last_product_status_reconcile_date", persisted)

        self.snapshot["items"][0]["quantity"] = 0
        self.connector.session.post.side_effect = self._post
        self.connector.run_cycle()
        self.assertEqual(self.connector._fetch_inventory_snapshot.call_count, 2)
        self.assertEqual(self._persisted()["last_product_status_reconcile_date"], "2026-09-14")

    def test_http_failure_keeps_status_pass_pending(self) -> None:
        failed = mock.Mock()
        failed.raise_for_status.side_effect = RuntimeError("status unavailable")
        self.connector.session.post.side_effect = [
            self._post("/adjustments", json={"adjustments": [{"sku": "ABC", "target_quantity": 0}]}, timeout=30),
            failed,
        ]
        with self.assertRaisesRegex(RuntimeError, "status unavailable"):
            self.connector.run_cycle()
        self.assertEqual(self._persisted()["pending_product_status_reconcile_date"], "2026-09-14")
        self.assertEqual(self._persisted()["pending_product_status_skus"], ["ABC"])
        self.assertNotIn("last_product_status_reconcile_date", self._persisted())

    def test_pending_status_retries_after_restock_even_without_another_webhook(self) -> None:
        failed = mock.Mock()
        failed.json.return_value = {"results": [{"sku": "ABC", "success": False}]}
        self.connector.session.post.side_effect = [
            self._post("/adjustments", json={"adjustments": [{"sku": "ABC", "target_quantity": 0}]}, timeout=30),
            failed,
        ]
        self.connector.run_cycle()
        self.assertEqual(self._persisted()["pending_product_status_skus"], ["ABC"])

        # The missed restock webhook is recovered by the next full snapshot.
        self.snapshot["items"][0]["quantity"] = 2
        self.connector.session.post.side_effect = self._post
        self.connector.run_cycle()

        persisted = self._persisted()
        self.assertGreater(persisted["quantities"]["ABC"]["canonical"], 0)
        self.assertTrue(self.connector.session.post.call_args.args[0].endswith("/status/reconcile"))
        self.assertEqual(self.connector.session.post.call_args.kwargs["json"], {"skus": ["ABC"]})
        self.assertNotIn("pending_product_status_skus", persisted)
        self.assertEqual(persisted["last_product_status_reconcile_date"], "2026-09-14")

    def test_status_candidates_are_persisted_before_request_is_sent(self) -> None:
        self.state["quantities"]["ABC"] = {"canonical": 0, "pos_seen": 0, "shop_seen": 0}

        def fail_after_send(url: str, *, json: dict, timeout: int) -> mock.Mock:
            self.assertEqual(self._persisted()["pending_product_status_skus"], ["ABC"])
            raise RuntimeError("connection lost after send")

        self.connector.session.post.side_effect = fail_after_send
        with self.assertRaisesRegex(RuntimeError, "connection lost after send"):
            self.connector._reconcile_zero_stock_product_statuses(
                self.state, {"ABC": 0}, observed_skus={"ABC"},
            )
        self.assertEqual(self._persisted()["pending_product_status_skus"], ["ABC"])

    def test_pending_status_recovery_waits_for_inventory_adjustments(self) -> None:
        self.state["pending_product_status_skus"] = ["ABC"]
        self.state["quantities"]["ABC"] = {
            "canonical": 2, "pos_seen": 0, "shop_seen": 0,
            "pending_shop": {"sku": "ABC", "delta": 2},
        }
        self.assertFalse(self.connector._reconcile_zero_stock_product_statuses(
            self.state, {"ABC": 2}, observed_skus={"ABC"},
        ))
        self.connector.session.post.assert_not_called()
        self.assertEqual(self.state["pending_product_status_skus"], ["ABC"])

        self.state["quantities"]["ABC"].pop("pending_shop")
        self.state["quantities"]["ABC"].update(pos_seen=2, shop_seen=2)
        self.assertTrue(self.connector._reconcile_zero_stock_product_statuses(
            self.state, {"ABC": 2}, observed_skus={"ABC"},
        ))
        self.connector.session.post.assert_called_once()

    def test_pending_status_sku_keeps_recovery_due_after_completed_day(self) -> None:
        self.state["last_full_reconcile_date"] = "2026-09-14"
        self.state["last_product_status_reconcile_date"] = "2026-09-14"
        self.state["pending_product_status_skus"] = ["ABC"]
        self.state["quantities"]["ABC"] = {"canonical": 0, "pos_seen": 0, "shop_seen": 0}
        self.snapshot["items"][0]["quantity"] = 0
        save_state(self.connector.state_path, self.state)

        self.connector.run_cycle()

        self.connector.session.post.assert_called_once()
        self.assertTrue(self.connector.session.post.call_args.args[0].endswith("/status/reconcile"))
        self.assertNotIn("pending_product_status_skus", self._persisted())

    def test_failed_inventory_adjustment_defers_archive_until_retry_succeeds(self) -> None:
        failed = mock.Mock()
        failed.json.return_value = {"results": [{"sku": "ABC", "success": False}]}
        self.connector.session.post.side_effect = None
        self.connector.session.post.return_value = failed
        self.connector.run_cycle()
        self.connector.session.post.assert_called_once()
        self.assertTrue(self.connector.session.post.call_args.args[0].endswith("/adjustments"))
        self.assertEqual(self._persisted()["pending_product_status_reconcile_date"], "2026-09-14")
        self.assertIn("pending_shop", self._persisted()["quantities"]["ABC"])

        self.connector.session.post.side_effect = self._post
        self.connector._retry_pending = Connector._retry_pending.__get__(self.connector)
        self.snapshot["items"][0]["quantity"] = 0
        self.connector.run_cycle()
        self.assertEqual(self.connector.session.post.call_count, 3)
        self.assertTrue(self.connector.session.post.call_args.args[0].endswith("/status/reconcile"))
        self.assertEqual(self._persisted()["last_product_status_reconcile_date"], "2026-09-14")

    def test_pending_pass_waits_until_configured_hour(self) -> None:
        self.connector.nightly_full_sync_hour = 2
        self.state["pending_product_status_reconcile_date"] = "2026-09-13"
        save_state(self.connector.state_path, self.state)
        self.connector.run_cycle()
        self.connector.session.post.assert_not_called()
        self.connector._fetch_inventory_snapshot.assert_not_called()
        self.assertEqual(self._persisted()["pending_product_status_reconcile_date"], "2026-09-13")

    def test_dry_run_does_not_send_or_record_status_pass(self) -> None:
        before = self.connector.state_path.read_text(encoding="utf-8")
        self.connector.dry_run = True
        self.connector.run_cycle()
        self.connector.session.post.assert_not_called()
        self.assertEqual(self.connector.state_path.read_text(encoding="utf-8"), before)

    def test_matrix_uses_one_child_sku_and_skips_whole_unsafe_products(self) -> None:
        for unsafe in (None, "blocked", "pending_shop", "pending_pos", "structure", "stocked", "unobserved", "unknown"):
            with self.subTest(unsafe=unsafe):
                children = ["MATRIX. 1 1", "MATRIX. 1 2"]
                state = {
                    "catalog_products": [] if unsafe == "unknown" else ["MATRIX"],
                    "sku_bases": {sku: "MATRIX" for sku in children},
                    "quantities": {
                        sku: {"canonical": 0, "pos_seen": 0, "shop_seen": 0}
                        for sku in children
                    },
                }
                if unsafe == "blocked":
                    state["blocked_inventory_skus"] = [children[1]]
                elif unsafe in {"pending_shop", "pending_pos"}:
                    state["quantities"][children[1]][unsafe] = {"sku": children[1]}
                elif unsafe == "structure":
                    state["pending_matrix_structure_repairs"] = {"MATRIX": {"stage": "candidate"}}
                elif unsafe == "stocked":
                    state["quantities"][children[1]]["shop_seen"] = 1
                observed = set(children[:1] if unsafe == "unobserved" else children)
                self.connector.session.post.reset_mock()
                complete = self.connector._reconcile_zero_stock_product_statuses(
                    state,
                    dict.fromkeys(children, 0),
                    observed_skus=observed,
                )
                if unsafe is None:
                    self.assertEqual(self.connector.session.post.call_args.kwargs["json"], {"skus": [children[0]]})
                else:
                    self.connector.session.post.assert_not_called()
                self.assertEqual(complete, unsafe not in {"pending_shop", "pending_pos"})

    def test_missing_or_mismatched_response_does_not_complete_pass(self) -> None:
        self.state["quantities"]["ABC"] = {"canonical": 0, "pos_seen": 0, "shop_seen": 0}
        for results in ([], [{"sku": "OTHER", "success": True}], [{"sku": "ABC", "success": False}]):
            with self.subTest(results=results):
                response = mock.Mock()
                response.json.return_value = {"results": results}
                self.connector.session.post.side_effect = None
                self.connector.session.post.return_value = response
                self.assertFalse(
                    self.connector._reconcile_zero_stock_product_statuses(
                        self.state, {"ABC": 0}, observed_skus={"ABC"}
                    )
                )
                self.assertEqual(self._persisted()["pending_product_status_skus"], ["ABC"])

    def test_status_requests_respect_server_batch_limit(self) -> None:
        skus = [f"PRODUCT-{index}" for index in range(101)]
        state = {
            "catalog_products": skus,
            "quantities": {
                sku: {"canonical": 0, "pos_seen": 0, "shop_seen": 0}
                for sku in skus
            },
        }
        self.connector.batch_size = 250
        self.assertTrue(
            self.connector._reconcile_zero_stock_product_statuses(
                state, dict.fromkeys(skus, 0), observed_skus=set(skus)
            )
        )
        self.assertEqual(
            [len(call.kwargs["json"]["skus"]) for call in self.connector.session.post.call_args_list],
            [100, 1],
        )
