import io
import json
import sqlite3
import struct
import subprocess
import sys
import tempfile
import unittest
import zipfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from app.db import DatabaseStore
from app.db import ShopRecord
from app.inventory import InventorySyncService
from app.models import InventoryLevelSnapshot, ProductSyncRequest, VariantMapping
from app.pos_archive import save_uploaded_archive
from app.shopify import ShopifyClient
from app.utils import SyncProcessingError
from windows_connector.order_dbf import (
    DETAIL_FIELDS,
    HEADER_FIELDS,
    migrate_legacy_sqlite_database,
    order_dbf_lock,
    read_order_dbfs,
    remove_orders_from_legacy_sqlite,
    write_order_dbfs,
)
from windows_connector.connector import (
    CATALOG_STRUCTURE_SCHEMA_VERSION,
    MATRIX_OPTION_SCHEMA_VERSION,
    Connector,
    adjustment_key,
    base_sku,
    catalog_total_quantity,
    catalog_upload_priority,
    dbf_record_count,
    detect_price_changes,
    flatten_quantities,
    iter_selected_dbf_rows,
    inventory_observation_is_newer,
    inventory_observation_is_older,
    matrix_variant_sku_for_row,
    matrix_length_repair_candidates,
    merge_quantity,
    negative_catalog_money_field,
    nightly_full_sync_due,
    numeric_sku_increases,
    read_appended_dbf_rows,
    save_state,
    sku_base_mapping,
    stable_catalog_payloads,
    upsert_order_changes,
    validate_order_dbf_paths,
)


class QuantityMergeTests(unittest.TestCase):
    def test_combines_simultaneous_pos_and_shopify_sales(self):
        plan = merge_quantity(
            {"canonical": 10, "pos_seen": 10, "shop_seen": 10},
            pos_quantity=9,
            shop_quantity=9,
        )

        self.assertEqual(plan["target"], 8)
        self.assertEqual(plan["shop_adjustment"], -1)
        self.assertEqual(plan["pos_adjustment"], -1)

    def test_shopify_sale_only_updates_the_pos(self):
        plan = merge_quantity(
            {"canonical": 10, "pos_seen": 10, "shop_seen": 10},
            pos_quantity=10,
            shop_quantity=9,
        )

        self.assertEqual(plan["target"], 9)
        self.assertEqual(plan["shop_adjustment"], 0)
        self.assertEqual(plan["pos_adjustment"], -1)

    def test_pos_sale_only_updates_shopify(self):
        plan = merge_quantity(
            {"canonical": 10, "pos_seen": 10, "shop_seen": 10},
            pos_quantity=9,
            shop_quantity=10,
        )

        self.assertEqual(plan["target"], 9)
        self.assertEqual(plan["shop_adjustment"], -1)
        self.assertEqual(plan["pos_adjustment"], 0)

    def test_matrix_payload_flattens_to_variant_skus(self):
        quantities = flatten_quantities(
            [
                {
                    "sku": "21741",
                    "quantity": 3,
                    "variants": [
                        {"sku": "21741. 1 1", "quantity": 1},
                        {"sku": "21741. 1 2", "quantity": 2},
                    ],
                },
                {"sku": "ABC", "quantity": 4},
            ]
        )

        self.assertEqual(quantities, {"21741. 1 1": 1, "21741. 1 2": 2, "ABC": 4})

    def test_adjustment_key_is_stable_for_retries(self):
        first = adjustment_key("shopify", "ABC", 1, 10, 9, 10, 10, 9)
        second = adjustment_key("shopify", "ABC", 1, 10, 9, 10, 10, 9)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)

    def test_adjustment_key_changes_for_a_repeated_transition(self):
        first = adjustment_key("shopify", "ABC", 1, 10, 9, 10, 10, 9)
        later = adjustment_key("shopify", "ABC", 3, 10, 9, 10, 10, 9)
        self.assertNotEqual(first, later)

    def test_inventory_source_timestamps_reject_delayed_observations(self):
        current = "2026-08-21T12:00:00Z"
        self.assertTrue(
            inventory_observation_is_newer("2026-08-21T12:01:00+00:00", current)
        )
        self.assertFalse(
            inventory_observation_is_newer("2026-08-21T11:59:00Z", current)
        )
        self.assertTrue(
            inventory_observation_is_older("2026-08-21T11:59:00Z", current)
        )


class CatalogUploadPriorityTests(unittest.TestCase):
    def test_negative_price_products_are_not_eligible_for_shopify_upload(self):
        self.assertEqual(
            negative_catalog_money_field({"sku": "TLCP", "price": -100.0}),
            "price",
        )
        self.assertEqual(
            negative_catalog_money_field(
                {"sku": "MATRIX", "variants": [{"price": 20.0}, {"cost": -1.0}]}
            ),
            "variants[2].cost",
        )
        self.assertIsNone(negative_catalog_money_field({"sku": "ABC", "price": 0.0}))

    def test_stocked_products_sort_before_zero_stock_products(self):
        products = [
            {"sku": "ZERO-1", "quantity": 0},
            {"sku": "STOCK-1", "quantity": 3},
            {"sku": "ZERO-2", "quantity": 0},
            {"sku": "STOCK-2", "quantity": 1},
        ]

        products.sort(key=catalog_upload_priority)

        self.assertEqual(
            [product["sku"] for product in products],
            ["STOCK-1", "STOCK-2", "ZERO-1", "ZERO-2"],
        )

    def test_matrix_priority_uses_combined_variant_quantity(self):
        matrix = {
            "sku": "MATRIX",
            "quantity": 0,
            "variants": [
                {"sku": "MATRIX. 1 1", "quantity": 0},
                {"sku": "MATRIX. 1 2", "quantity": 2},
            ],
        }

        self.assertEqual(catalog_total_quantity(matrix), 2)
        self.assertEqual(catalog_upload_priority(matrix), 0)

    def test_new_product_waits_for_two_identical_complete_payloads(self):
        state = {}
        first = {"sku": "22392", "quantity": 0, "price": 195}
        finished = {"sku": "22392", "quantity": 6, "price": 195}

        self.assertEqual(stable_catalog_payloads([first], state), [])
        self.assertEqual(stable_catalog_payloads([finished], state), [])
        self.assertEqual(stable_catalog_payloads([finished], state), [finished])
        self.assertEqual(
            state["pending_catalog_products"]["22392"]["observations"],
            2,
        )

    def test_new_product_stability_uses_the_entire_payload(self):
        state = {}
        first = {"sku": "22391", "quantity": 4, "title": "Copied shirt"}
        edited = {"sku": "22391", "quantity": 4, "title": "Black shirt"}

        stable_catalog_payloads([first], state)
        self.assertEqual(stable_catalog_payloads([edited], state), [])
        self.assertEqual(stable_catalog_payloads([edited], state), [edited])

    def test_matrix_length_repair_candidates_include_variant_skus(self):
        candidates = matrix_length_repair_candidates(
            [
                {
                    "sku": "PANTS",
                    "variants": [
                        {
                            "sku": "PANTS. 1 1",
                            "option_values": {"Size": "34", "Length": "S"},
                        },
                        {
                            "sku": "PANTS. 2 1",
                            "option_values": {"Size": "34", "Length": "R"},
                        },
                    ],
                },
                {
                    "sku": "SHIRT",
                    "variants": [
                        {
                            "sku": "SHIRT. 1 1",
                            "option_values": {"Size": "M", "Color": "Blue"},
                        }
                    ],
                },
            ]
        )

        self.assertEqual(
            candidates,
            [
                {
                    "base_sku": "PANTS",
                    "variant_skus": ["PANTS. 1 1", "PANTS. 2 1"],
                }
            ],
        )


class CatalogUploadBaselineTests(unittest.TestCase):
    def test_successful_incremental_upload_persists_every_quantity_baseline(self):
        class Response:
            status_code = 200
            text = ""

            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "results": [
                        {"success": True},
                        {"success": True},
                        {"success": False, "message": "rejected"},
                    ]
                }

        with tempfile.TemporaryDirectory() as temporary_directory:
            connector = Connector.__new__(Connector)
            connector.base_url = "https://sync.example"
            connector.batch_size = 25
            connector.timeout = 30
            connector.state_path = Path(temporary_directory) / "state.json"
            connector.order_sync_enabled = False
            connector.logger = mock.Mock()
            connector.session = mock.Mock()
            connector.session.post.return_value = Response()
            state = {
                "version": 1,
                "catalog_products": [],
                "quantities": {
                    "MATRIX. 1 1": {
                        "canonical": 9,
                        "pos_seen": 9,
                        "shop_seen": 9,
                        "pending_shop": {"delta": 1},
                    }
                },
            }
            payloads = [
                {"sku": "ZERO", "quantity": 0},
                {
                    "sku": "MATRIX",
                    "variants": [
                        {"sku": "MATRIX. 1 1", "quantity": 4},
                        {"sku": "MATRIX. 1 2", "quantity": 2},
                    ],
                },
                {"sku": "FAILED", "quantity": 7},
            ]

            succeeded = connector._upload_catalog(payloads, state=state)

            self.assertEqual(succeeded, {"ZERO", "MATRIX"})
            self.assertEqual(state["catalog_products"], ["MATRIX", "ZERO"])
            self.assertEqual(
                state["quantities"]["ZERO"],
                {"canonical": 0, "pos_seen": 0, "shop_seen": 0},
            )
            self.assertEqual(
                state["quantities"]["MATRIX. 1 2"],
                {"canonical": 2, "pos_seen": 2, "shop_seen": 2},
            )
            self.assertEqual(state["quantities"]["MATRIX. 1 1"]["canonical"], 9)
            self.assertIn("pending_shop", state["quantities"]["MATRIX. 1 1"])
            self.assertNotIn("FAILED", state["quantities"])

            persisted = connector.state_path.read_text(encoding="utf-8")
            self.assertIn('"ZERO":{"canonical":0', persisted)


class MatrixStructureRepairClientTests(unittest.TestCase):
    def test_posts_full_payloads_to_structure_endpoint_in_5_item_chunks(self):
        navy = FullInventoryReconciliationTests._navy_matrix_payload()
        payloads = []
        for index in range(26):
            payload = json.loads(json.dumps(navy))
            payload["sku"] = "22392" if index == 0 else f"MATRIX-{index}"
            for variant_index, variant in enumerate(payload["variants"], start=1):
                variant["sku"] = f"{payload['sku']}. 1 {variant_index}"
                variant["barcode"] = variant["sku"]
            payloads.append(payload)

        connector = Connector.__new__(Connector)
        connector.base_url = "https://sync.example"
        connector.batch_size = 100
        connector.timeout = 30
        connector.logger = mock.Mock()
        connector.session = mock.Mock()
        exceptional_statuses = {
            "22392": "repaired",
            "MATRIX-24": "blocked",
            "MATRIX-25": "failed",
        }

        def response_for_chunk(_endpoint, *, json, timeout):
            self.assertEqual(timeout, 30)
            response = mock.Mock()
            response.json.return_value = {
                "results": [
                    {
                        "base_sku": item["sku"],
                        "status": exceptional_statuses.get(
                            item["sku"],
                            "already_correct",
                        ),
                    }
                    for item in json["items"]
                ]
            }
            return response

        connector.session.post.side_effect = response_for_chunk

        statuses = connector._repair_matrix_structures(payloads)

        self.assertEqual(connector.session.post.call_count, 6)
        calls = connector.session.post.call_args_list
        first_call = calls[0]
        self.assertEqual(
            first_call.args[0],
            "https://sync.example/sync/catalog/matrix-structure/repair",
        )
        self.assertTrue(
            all(len(call.kwargs["json"]["items"]) == 5 for call in calls[:-1])
        )
        self.assertEqual(len(calls[-1].kwargs["json"]["items"]), 1)
        self.assertEqual(first_call.kwargs["json"]["items"][0], navy)
        self.assertEqual(statuses["22392"]["status"], "repaired")
        self.assertEqual(statuses["MATRIX-24"]["status"], "blocked")
        self.assertEqual(statuses["MATRIX-25"]["status"], "failed")

    def test_pending_retries_exclude_protected_matrix_skus(self):
        connector = Connector.__new__(Connector)
        connector.logger = mock.Mock()
        connector.writeback_mode = "vfp-oledb"
        connector._apply_shopify_adjustments = mock.Mock(return_value=set())
        connector._apply_pos_adjustments = mock.Mock()
        protected_shop = {"sku": "22392. 1 1", "delta": 1}
        protected_pos = {"sku": "22392", "delta": -6}
        unrelated_shop = {"sku": "OTHER", "delta": 2}
        unrelated_pos = {"sku": "SECOND", "delta": -1}
        state = {
            "quantities": {
                "22392. 1 1": {"pending_shop": protected_shop},
                "22392": {"pending_pos": protected_pos},
                "OTHER": {"pending_shop": unrelated_shop},
                "SECOND": {"pending_pos": unrelated_pos},
            }
        }

        connector._retry_pending(
            state,
            excluded_skus={"22392", "22392. 1 1"},
        )

        connector._apply_shopify_adjustments.assert_called_once_with(
            state,
            [unrelated_shop],
        )
        connector._apply_pos_adjustments.assert_called_once_with(
            state,
            [unrelated_pos],
        )

    def test_successful_structure_response_must_confirm_the_pos_quantities(self):
        payload = FullInventoryReconciliationTests._navy_matrix_payload()
        connector = Connector.__new__(Connector)
        connector.logger = mock.Mock()
        state = {
            "pending_matrix_structure_repairs": {
                "22392": {
                    "fingerprint": "stable",
                    "observations": 2,
                    "stage": "candidate",
                }
            }
        }
        wrong_result = FullInventoryReconciliationTests._structure_result(
            payload,
            quantities={variant["sku"]: 0 for variant in payload["variants"]},
        )

        connector._record_matrix_structure_repair_results(
            state,
            [payload],
            {"22392": wrong_result},
        )

        entry = state["pending_matrix_structure_repairs"]["22392"]
        self.assertEqual(entry["result"], "failed")
        self.assertEqual(entry["stage"], "candidate")
        self.assertNotIn("confirmed_quantities", entry)

    def test_known_matrix_type_without_cached_children_schedules_a_structure_probe(self):
        connector = Connector.__new__(Connector)
        connector.dbf_dir = Path("unused-pos-data")
        connector.logger = mock.Mock()
        state = {
            "sku_bases": {"22392": "22392"},
            "pending_catalog_products": {},
            "matrix_structure_probe_not_before": {},
        }
        module = sys.modules["windows_connector.connector"].dbf_pos_sync

        with mock.patch.object(
            module,
            "find_dbf_file",
            return_value=Path("unused-pos-data/Item.dbf"),
        ), mock.patch.object(
            module,
            "iter_dbf_rows",
            return_value=iter([{"SKU": "22392", "TYPE": "M"}]),
        ):
            new_skus = connector._collect_new_numeric_product_skus(
                state,
                known_products={"22392"},
            )

        self.assertEqual(new_skus, set())
        self.assertEqual(state["matrix_structure_probe_skus"], ["22392"])

        state["sku_bases"]["22392. 1 1"] = "22392"
        with mock.patch.object(
            module,
            "find_dbf_file",
            return_value=Path("unused-pos-data/Item.dbf"),
        ), mock.patch.object(
            module,
            "iter_dbf_rows",
            return_value=iter([{"SKU": "22392", "TYPE": "M"}]),
        ):
            connector._collect_new_numeric_product_skus(
                state,
                known_products={"22392"},
            )

        self.assertEqual(state["matrix_structure_probe_skus"], [])


class PriceChangeDetectionTests(unittest.TestCase):
    def test_unchanged_prices_are_not_sent(self):
        changes = detect_price_changes(
            {"22301": "49.00"},
            {"22301": "49.00"},
            sku_bases={"22301": "22301"},
        )
        self.assertEqual(changes, [])

    def test_changed_price_is_sent_to_the_exact_sku(self):
        changes = detect_price_changes(
            {"22301": "49.00"},
            {"22301": "59"},
            sku_bases={"22301": "22301"},
        )
        self.assertEqual(
            changes,
            [
                {
                    "source_sku": "22301",
                    "target_skus": ["22301"],
                    "old_price": "49.00",
                    "new_price": "59.00",
                }
            ],
        )

    def test_matrix_base_price_targets_all_matrix_variants(self):
        changes = detect_price_changes(
            {"22001": "100.00"},
            {"22001": "110.00"},
            sku_bases={
                "22001": "22001",
                "22001. 1 1": "22001",
                "22001. 1 2": "22001",
            },
        )
        self.assertEqual(
            changes[0]["target_skus"],
            ["22001. 1 1", "22001. 1 2"],
        )

    def test_numeric_sku_high_water_ignores_legacy_six_digit_skus(self):
        candidates, high_water, digit_width = numeric_sku_increases(
            ["120312", "132223", "22289", "22290", "22301"],
            known_products={"120312", "132223", "22289", "22290"},
        )

        self.assertEqual(digit_width, 5)
        self.assertEqual(high_water, 22301)
        self.assertEqual(candidates, {"22301"})

    def test_numeric_sku_high_water_returns_only_later_unknown_skus(self):
        candidates, high_water, digit_width = numeric_sku_increases(
            ["22290", "22291", "22292", "22301"],
            known_products={"22290", "22291"},
            high_water=22291,
            digit_width=5,
        )

        self.assertEqual(candidates, {"22292", "22301"})
        self.assertEqual(high_water, 22301)
        self.assertEqual(digit_width, 5)


class IncrementalPosEventTests(unittest.TestCase):
    @staticmethod
    def _write_event_dbf(path: Path, records):
        fields = [("SKU", "C", 12), ("ITEM", "C", 1)]
        header_length = 32 + (32 * len(fields)) + 1
        record_length = 1 + sum(field[2] for field in fields)
        header = bytearray(32)
        header[0] = 0x03
        header[4:8] = struct.pack("<I", len(records))
        header[8:10] = struct.pack("<H", header_length)
        header[10:12] = struct.pack("<H", record_length)
        payload = bytearray(header)
        for name, field_type, length in fields:
            descriptor = bytearray(32)
            descriptor[: len(name)] = name.encode("ascii")
            descriptor[11] = ord(field_type)
            descriptor[16] = length
            payload.extend(descriptor)
        payload.append(0x0D)
        for deleted, sku, item in records:
            payload.extend(b"*" if deleted else b" ")
            payload.extend(str(sku).encode("latin1")[:12].ljust(12, b" "))
            payload.extend(str(item).encode("latin1")[:1].ljust(1, b" "))
        payload.append(0x1A)
        path.write_bytes(payload)

    def test_reads_only_new_physical_records_and_skips_deleted_rows(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "invdtl.dbf"
            self._write_event_dbf(
                path,
                [
                    (False, "OLD", "R"),
                    (True, "DELETED", "R"),
                    (False, "NEW", "R"),
                ],
            )

            rows, cursor, was_reset = read_appended_dbf_rows(path, 1)

            self.assertEqual([row["SKU"] for row in rows], ["NEW"])
            self.assertEqual(cursor, 3)
            self.assertFalse(was_reset)
            self.assertEqual(dbf_record_count(path), 3)
            self.assertEqual(
                list(iter_selected_dbf_rows(path, {"NEW"}, selected_fields={"SKU", "ITEM"})),
                [{"SKU": "NEW", "ITEM": "R"}],
            )

    def test_a_shorter_repacked_event_file_forces_full_reconcile(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "editvoid.dbf"
            self._write_event_dbf(path, [(False, "ABC", "Q")])

            rows, cursor, was_reset = read_appended_dbf_rows(path, 20)

            self.assertEqual(rows, [])
            self.assertEqual(cursor, 1)
            self.assertTrue(was_reset)

    def test_matrix_variants_map_back_to_the_product_sku(self):
        payloads = [
            {
                "sku": "21741",
                "variants": [
                    {"sku": "21741. 1 1"},
                    {"sku": "CUSTOM-BARCODE"},
                ],
            },
            {"sku": "ABC"},
        ]

        self.assertEqual(base_sku("21741. 1 2"), "21741")
        self.assertEqual(base_sku("ABC"), "ABC")
        self.assertEqual(
            matrix_variant_sku_for_row(
                "21741",
                {"CELL": "110", "BARCODE": None},
                known_variants={"21741. 1 9", "21741. 1 10", "21741. 2 10"},
            ),
            "21741. 1 10",
        )
        self.assertEqual(
            sku_base_mapping(payloads),
            {
                "21741": "21741",
                "21741. 1 1": "21741",
                "CUSTOM-BARCODE": "21741",
                "ABC": "ABC",
            },
        )

    def test_full_sync_runs_once_after_the_configured_local_hour(self):
        before_midnight = datetime(2026, 7, 22, 23, 59)
        after_midnight = datetime(2026, 7, 23, 0, 1)

        self.assertFalse(nightly_full_sync_due("2026-07-22", now=before_midnight, hour=0))
        self.assertTrue(nightly_full_sync_due("2026-07-22", now=after_midnight, hour=0))
        self.assertFalse(nightly_full_sync_due("2026-07-23", now=after_midnight, hour=0))
        self.assertFalse(nightly_full_sync_due("2026-07-22", now=after_midnight, hour=2))


class FullInventoryReconciliationTests(unittest.TestCase):
    @staticmethod
    def _navy_matrix_payload(*, title="Eterna Dress Shirt - Navy"):
        quantities = [1, 1, 1, 1, 1, 1, 0, 0, 0]
        return {
            "sku": "22392",
            "title": title,
            "price": 195.0,
            "cost": 59.0,
            "quantity": 6,
            "vendor": "Scott Barber",
            "variants": [
                {
                    "sku": f"22392. 1 {index}",
                    "barcode": f"22392. 1 {index}",
                    "option_values": {"Size": str(index)},
                    "price": 195.0,
                    "cost": 59.0,
                    "quantity": quantity,
                }
                for index, quantity in enumerate(quantities, start=1)
            ],
        }

    @staticmethod
    def _snapshot_item(sku, quantity, *, duplicate=0, available=True):
        return {
            "sku": sku,
            "quantity": quantity,
            "location_id": "gid://shopify/Location/4",
            "available_at_location": available,
            "duplicate_sku_count": duplicate,
        }

    @staticmethod
    def _structure_result(payload, *, status="repaired", quantities=None):
        quantities = quantities or {
            variant["sku"]: variant["quantity"]
            for variant in payload["variants"]
        }
        return {
            "base_sku": payload["sku"],
            "status": status,
            "variants": [
                {
                    "sku": variant["sku"],
                    "quantity": quantities[variant["sku"]],
                    "inventory_item_id": f"inventory-{index}",
                    "location_id": "gid://shopify/Location/4",
                }
                for index, variant in enumerate(payload["variants"], start=1)
            ],
        }

    @staticmethod
    def _matrix_connector(state_path):
        connector = Connector.__new__(Connector)
        connector.dry_run = False
        connector.order_sync_enabled = False
        connector.price_sync_enabled = False
        connector.initial_catalog_upload = True
        connector.nightly_full_sync_hour = 0
        connector.writeback_mode = "dry-run"
        connector.base_url = "https://sync.example"
        connector.batch_size = 25
        connector.timeout = 30
        connector.state_path = state_path
        connector.dbf_dir = Path("unused-pos-data")
        connector.logger = mock.Mock()
        connector._retry_pending = mock.Mock()
        connector._fetch_inventory_changes = mock.Mock(return_value=[])
        connector._collect_pos_event_skus = mock.Mock(return_value=(set(), False))
        connector._collect_new_numeric_product_skus = mock.Mock(return_value=set())
        connector._reader_args = mock.Mock(return_value=SimpleNamespace())
        connector._apply_shopify_adjustments = mock.Mock(return_value=set())
        connector._apply_pos_adjustments = mock.Mock()
        connector._acknowledge_inventory_changes = mock.Mock()
        return connector

    @staticmethod
    def _matrix_state():
        return {
            "version": 1,
            "catalog_complete": True,
            "catalog_products": ["22392"],
            "quantities": {
                "22392": {"canonical": 0, "pos_seen": 0, "shop_seen": 0}
            },
            "event_cursors": {},
            "sku_bases": {"22392": "22392"},
            "pending_catalog_products": {},
            "pending_matrix_structure_repairs": {},
            "blocked_inventory_skus": [],
            "matrix_option_schema_version": 2,
            "inventory_reconcile_schema_version": 2,
            "last_full_reconcile_date": datetime.now().date().isoformat(),
        }

    def test_nightly_snapshot_repairs_skus_missing_connector_baselines(self):
        for shopify_quantity, expected_delta in ((0, 6), (4, 2)):
            with self.subTest(shopify_quantity=shopify_quantity):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    connector = Connector.__new__(Connector)
                    connector.dry_run = False
                    connector.order_sync_enabled = False
                    connector.price_sync_enabled = False
                    connector.initial_catalog_upload = True
                    connector.nightly_full_sync_hour = 0
                    connector.writeback_mode = "dry-run"
                    connector.state_path = Path(temporary_directory) / "state.json"
                    connector.logger = mock.Mock()
                    connector._retry_pending = mock.Mock()
                    connector._fetch_inventory_changes = mock.Mock(
                        return_value=[
                            {
                                "id": 1,
                                "version": 1,
                                "sku": "22392",
                                "quantity": 1,
                                "inventory_item_id": "gid://shopify/InventoryItem/3",
                                "location_id": "gid://shopify/Location/4",
                                "source_updated_at": "2026-08-21T10:00:00Z",
                            }
                        ]
                    )
                    connector._collect_pos_event_skus = mock.Mock(return_value=(set(), False))
                    connector._collect_new_numeric_product_skus = mock.Mock(return_value=set())
                    connector._reader_args = mock.Mock(return_value=SimpleNamespace())
                    connector._fetch_inventory_snapshot = mock.Mock(
                        return_value={
                            "location_id": "gid://shopify/Location/4",
                            "items": [
                                {
                                    "sku": "22392",
                                    "quantity": shopify_quantity,
                                    "location_id": "gid://shopify/Location/4",
                                    "available_at_location": True,
                                    "duplicate_sku_count": 0,
                                    "inventory_item_id": "gid://shopify/InventoryItem/3",
                                    "inventory_level_updated_at": "2026-08-21T11:00:00Z",
                                }
                            ],
                        }
                    )
                    connector._acknowledge_inventory_changes = mock.Mock()
                    applied_actions = []

                    def apply_adjustments(state, actions):
                        applied_actions.extend(actions)
                        for action in actions:
                            entry = state["quantities"][action["sku"]]
                            entry["shop_seen"] = int(entry["shop_seen"]) + int(action["delta"])
                            entry.pop("pending_shop", None)
                        save_state(connector.state_path, state)

                    connector._apply_shopify_adjustments = apply_adjustments
                    save_state(
                        connector.state_path,
                        {
                            "version": 1,
                            "catalog_complete": True,
                            "catalog_products": ["22392"],
                            "quantities": {},
                            "event_cursors": {},
                            "sku_bases": {"22392": "22392"},
                            "pending_catalog_products": {},
                            "matrix_option_schema_version": 2,
                            "last_full_reconcile_date": datetime.now().date().isoformat(),
                        },
                    )
                    prepared = SimpleNamespace(payload={"sku": "22392", "quantity": 6})
                    stats = SimpleNamespace(skipped_non_sellable=0)

                    with mock.patch.object(
                        sys.modules["windows_connector.connector"].dbf_pos_sync,
                        "load_products",
                        return_value=([prepared], stats),
                    ):
                        connector.run_cycle()

                    self.assertEqual(len(applied_actions), 1)
                    self.assertEqual(applied_actions[0]["sku"], "22392")
                    self.assertEqual(applied_actions[0]["delta"], expected_delta)
                    persisted = json.loads(connector.state_path.read_text(encoding="utf-8"))
                    self.assertEqual(
                        {
                            key: persisted["quantities"]["22392"][key]
                            for key in ("canonical", "pos_seen", "shop_seen")
                        },
                        {"canonical": 6, "pos_seen": 6, "shop_seen": 6},
                    )
                    self.assertNotIn("pending_shop", persisted["quantities"]["22392"])
                    connector._acknowledge_inventory_changes.assert_not_called()
                    self.assertEqual(persisted["inventory_reconcile_schema_version"], 2)
                    self.assertEqual(
                        persisted["inventory_location_id"],
                        "gid://shopify/Location/4",
                    )

    def test_newer_webhook_overlays_a_stale_full_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector.writeback_mode = "vfp-oledb"
            queued_sale = {
                "id": 1,
                "version": 1,
                "sku": "ABC",
                "quantity": 5,
                "inventory_item_id": "gid://shopify/InventoryItem/3",
                "location_id": "gid://shopify/Location/4",
                "source_updated_at": "2026-08-21T12:02:00Z",
            }
            connector._fetch_inventory_changes = mock.Mock(
                side_effect=[[], [queued_sale]]
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                return_value={
                    "location_id": "gid://shopify/Location/4",
                    "items": [
                        {
                            "sku": "ABC",
                            "quantity": 6,
                            "inventory_item_id": "gid://shopify/InventoryItem/3",
                            "location_id": "gid://shopify/Location/4",
                            "inventory_level_updated_at": "2026-08-21T12:01:00Z",
                            "available_at_location": True,
                            "duplicate_sku_count": 0,
                        }
                    ],
                }
            )
            state = {
                "version": 1,
                "catalog_complete": True,
                "catalog_products": ["ABC"],
                "quantities": {
                    "ABC": {"canonical": 6, "pos_seen": 6, "shop_seen": 6}
                },
                "event_cursors": {},
                "sku_bases": {"ABC": "ABC"},
                "pending_catalog_products": {},
                "pending_matrix_structure_repairs": {},
                "blocked_inventory_skus": [],
                "matrix_option_schema_version": MATRIX_OPTION_SCHEMA_VERSION,
                "catalog_structure_schema_version": CATALOG_STRUCTURE_SCHEMA_VERSION,
                "inventory_reconcile_schema_version": 1,
                "last_full_reconcile_date": datetime.now().date().isoformat(),
            }
            save_state(state_path, state)
            prepared = SimpleNamespace(payload={"sku": "ABC", "quantity": 7})
            stats = SimpleNamespace(skipped_non_sellable=0)

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ):
                connector.run_cycle()

            self.assertEqual(connector._fetch_inventory_changes.call_count, 2)
            shop_actions = connector._apply_shopify_adjustments.call_args.args[1]
            pos_actions = connector._apply_pos_adjustments.call_args.args[1]
            self.assertEqual(
                {
                    key: shop_actions[0][key]
                    for key in ("sku", "delta", "target_quantity")
                },
                {"sku": "ABC", "delta": 1, "target_quantity": 6},
            )
            self.assertEqual(
                {
                    key: pos_actions[0][key]
                    for key in ("sku", "delta", "target_quantity", "expected_quantity")
                },
                {
                    "sku": "ABC",
                    "delta": -1,
                    "target_quantity": 6,
                    "expected_quantity": 7,
                },
            )
            connector._acknowledge_inventory_changes.assert_not_called()

    def test_full_snapshot_aborts_on_an_unverifiable_queued_quantity(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            queued_change = {
                "id": 1,
                "version": 1,
                "sku": "ABC",
                "quantity": 5,
                "inventory_item_id": "gid://shopify/InventoryItem/3",
                "location_id": "gid://shopify/Location/4",
                "source_updated_at": None,
            }
            connector._fetch_inventory_changes = mock.Mock(
                side_effect=[[], [queued_change]]
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                return_value={
                    "location_id": "gid://shopify/Location/4",
                    "items": [
                        {
                            "sku": "ABC",
                            "quantity": 6,
                            "inventory_item_id": "gid://shopify/InventoryItem/3",
                            "location_id": "gid://shopify/Location/4",
                            "inventory_level_updated_at": "2026-08-21T12:01:00Z",
                            "available_at_location": True,
                            "duplicate_sku_count": 0,
                        }
                    ],
                }
            )
            state = {
                "version": 1,
                "catalog_complete": True,
                "catalog_products": ["ABC"],
                "quantities": {
                    "ABC": {"canonical": 6, "pos_seen": 6, "shop_seen": 6}
                },
                "event_cursors": {},
                "sku_bases": {"ABC": "ABC"},
                "pending_catalog_products": {},
                "pending_matrix_structure_repairs": {},
                "blocked_inventory_skus": [],
                "matrix_option_schema_version": MATRIX_OPTION_SCHEMA_VERSION,
                "catalog_structure_schema_version": CATALOG_STRUCTURE_SCHEMA_VERSION,
                "inventory_reconcile_schema_version": 1,
                "last_full_reconcile_date": datetime.now().date().isoformat(),
            }
            save_state(state_path, state)
            prepared = SimpleNamespace(payload={"sku": "ABC", "quantity": 7})
            stats = SimpleNamespace(skipped_non_sellable=0)

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ), self.assertRaisesRegex(RuntimeError, "no comparable timestamp"):
                connector.run_cycle()

            connector._apply_shopify_adjustments.assert_not_called()
            connector._apply_pos_adjustments.assert_not_called()
            connector._acknowledge_inventory_changes.assert_not_called()
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["inventory_reconcile_schema_version"], 1)

    def test_deleted_shopify_sku_does_not_block_a_full_reconcile(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            deleted_change = {
                "id": 9,
                "version": 1,
                "sku": "DELETED",
                "quantity": 0,
                "inventory_item_id": "gid://shopify/InventoryItem/99",
                "location_id": "gid://shopify/Location/4",
                "source_updated_at": "2026-08-21T12:02:00Z",
            }
            connector._fetch_inventory_changes = mock.Mock(
                side_effect=[[deleted_change], [deleted_change], [deleted_change]]
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                return_value={
                    "location_id": "gid://shopify/Location/4",
                    "items": [
                        {
                            "sku": "ABC",
                            "quantity": 6,
                            "inventory_item_id": "gid://shopify/InventoryItem/3",
                            "location_id": "gid://shopify/Location/4",
                            "inventory_level_updated_at": "2026-08-21T12:01:00Z",
                            "available_at_location": True,
                            "duplicate_sku_count": 0,
                        }
                    ],
                }
            )
            state = {
                "version": 1,
                "catalog_complete": True,
                "catalog_products": ["ABC"],
                "quantities": {
                    "ABC": {"canonical": 6, "pos_seen": 6, "shop_seen": 6}
                },
                "event_cursors": {},
                "sku_bases": {"ABC": "ABC"},
                "pending_catalog_products": {},
                "pending_matrix_structure_repairs": {},
                "blocked_inventory_skus": [],
                "matrix_option_schema_version": MATRIX_OPTION_SCHEMA_VERSION,
                "catalog_structure_schema_version": CATALOG_STRUCTURE_SCHEMA_VERSION,
                "inventory_reconcile_schema_version": 1,
                "last_full_reconcile_date": datetime.now().date().isoformat(),
            }
            save_state(state_path, state)
            prepared = SimpleNamespace(payload={"sku": "ABC", "quantity": 6})
            stats = SimpleNamespace(skipped_non_sellable=0)

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ), mock.patch.object(
                sys.modules["windows_connector.connector"],
                "read_targeted_pos_quantities",
                return_value={},
            ):
                connector.run_cycle()
                connector._acknowledge_inventory_changes.assert_not_called()
                connector.run_cycle()

            connector._fetch_inventory_snapshot.assert_called_once()
            connector._apply_shopify_adjustments.assert_not_called()
            connector._apply_pos_adjustments.assert_not_called()
            connector._acknowledge_inventory_changes.assert_called_once_with(
                [deleted_change]
            )

    def test_structure_upgrade_repairs_scalar_navy_before_post_repair_snapshot(self):
        payload = self._navy_matrix_payload()
        prepared = SimpleNamespace(payload=payload)
        stats = SimpleNamespace(skipped_non_sellable=0)
        call_order = []
        repaired = False

        def repair_structures(items):
            nonlocal repaired
            call_order.append("repair")
            self.assertEqual(items, [payload])
            repaired = True
            return {"22392": self._structure_result(payload)}

        def inventory_snapshot():
            call_order.append("snapshot")
            if not repaired:
                items = [self._snapshot_item("22392", 0)]
            else:
                items = [
                    self._snapshot_item(variant["sku"], variant["quantity"])
                    for variant in payload["variants"]
                ]
            return {
                "location_id": "gid://shopify/Location/4",
                "items": items,
            }

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector._repair_matrix_structures = mock.Mock(
                side_effect=repair_structures
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                side_effect=inventory_snapshot
            )
            save_state(state_path, self._matrix_state())

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ) as load_products:
                connector.run_cycle()
                first_state = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertNotIn("catalog_structure_schema_version", first_state)
                pending = first_state["pending_matrix_structure_repairs"]["22392"]
                self.assertEqual(pending["observations"], 1)
                self.assertEqual(pending["stage"], "candidate")
                connector._repair_matrix_structures.assert_not_called()

                connector.run_cycle()
                connector.run_cycle()

            self.assertEqual(call_order, ["snapshot", "repair", "snapshot"])
            connector._repair_matrix_structures.assert_called_once_with([payload])
            self.assertEqual(load_products.call_count, 2)
            connector._apply_shopify_adjustments.assert_not_called()
            connector._apply_pos_adjustments.assert_not_called()

            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                persisted["catalog_structure_schema_version"],
                CATALOG_STRUCTURE_SCHEMA_VERSION,
            )
            self.assertEqual(persisted["pending_matrix_structure_repairs"], {})
            self.assertNotIn("22392", persisted["quantities"])
            self.assertEqual(persisted["blocked_inventory_skus"], [])
            self.assertEqual(
                persisted["sku_bases"],
                {
                    "22392": "22392",
                    **{
                        variant["sku"]: "22392"
                        for variant in payload["variants"]
                    },
                },
            )
            for variant in payload["variants"]:
                self.assertEqual(
                    persisted["quantities"][variant["sku"]],
                    {
                        "canonical": variant["quantity"],
                        "pos_seen": variant["quantity"],
                        "shop_seen": variant["quantity"],
                    },
                )

    def test_existing_matrix_quantity_mismatch_merges_a_newer_child_sale(self):
        payload = self._navy_matrix_payload()
        racing_sku = payload["variants"][0]["sku"]
        prepared = SimpleNamespace(payload=payload)
        stats = SimpleNamespace(skipped_non_sellable=0)
        repaired = False

        def repair_structures(items):
            nonlocal repaired
            repaired = True
            return {
                "22392": self._structure_result(
                    payload,
                    status="quantity_mismatch",
                    quantities={variant["sku"]: 0 for variant in payload["variants"]},
                )
            }

        def inventory_snapshot():
            if repaired:
                items = [
                    self._snapshot_item(
                        variant["sku"],
                        1 if variant["sku"] == racing_sku else 0,
                    )
                    for variant in payload["variants"]
                ]
                items[0]["inventory_item_id"] = "inventory-1"
                items[0]["inventory_level_updated_at"] = (
                    "2026-08-21T12:01:00Z"
                )
            else:
                items = [self._snapshot_item("22392", 0)]
            return {
                "location_id": "gid://shopify/Location/4",
                "items": items,
            }

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector.writeback_mode = "vfp-oledb"
            queued_sale = {
                "id": 7,
                "version": 1,
                "sku": racing_sku,
                "quantity": 0,
                "inventory_item_id": "inventory-1",
                "location_id": "gid://shopify/Location/4",
                "source_updated_at": "2026-08-21T12:02:00Z",
            }
            connector._fetch_inventory_changes = mock.Mock(
                side_effect=[[], [], [queued_sale], [queued_sale]]
            )
            connector._repair_matrix_structures = mock.Mock(
                side_effect=repair_structures
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                side_effect=inventory_snapshot
            )
            save_state(state_path, self._matrix_state())

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ):
                connector.run_cycle()
                connector.run_cycle()

            connector._repair_matrix_structures.assert_called_once_with([payload])
            shop_actions = connector._apply_shopify_adjustments.call_args.args[1]
            self.assertEqual(
                {
                    action["sku"]: (action["delta"], action["target_quantity"])
                    for action in shop_actions
                },
                {
                    variant["sku"]: (variant["quantity"], variant["quantity"])
                    for variant in payload["variants"]
                    if variant["quantity"] and variant["sku"] != racing_sku
                },
            )
            pos_actions = connector._apply_pos_adjustments.call_args.args[1]
            self.assertEqual(
                {
                    key: pos_actions[0][key]
                    for key in ("sku", "delta", "target_quantity", "expected_quantity")
                },
                {
                    "sku": racing_sku,
                    "delta": -1,
                    "target_quantity": 0,
                    "expected_quantity": 1,
                },
            )
            connector._acknowledge_inventory_changes.assert_not_called()
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["pending_matrix_structure_repairs"], {})
            self.assertEqual(
                persisted["catalog_structure_schema_version"],
                CATALOG_STRUCTURE_SCHEMA_VERSION,
            )

    def test_known_scalar_to_matrix_transition_triggers_a_probe_before_nightly(self):
        payload = self._navy_matrix_payload()
        prepared = SimpleNamespace(payload=payload)
        stats = SimpleNamespace(skipped_non_sellable=0)
        repaired = False

        def detect_probe(state, *, known_products):
            has_children = any(
                sku != mapped_base and mapped_base == "22392"
                for sku, mapped_base in (state.get("sku_bases") or {}).items()
            )
            state["matrix_structure_probe_skus"] = [] if has_children else ["22392"]
            return set()

        def repair_structures(items):
            nonlocal repaired
            repaired = True
            return {"22392": self._structure_result(payload)}

        def inventory_snapshot():
            items = (
                [
                    self._snapshot_item(variant["sku"], variant["quantity"])
                    for variant in payload["variants"]
                ]
                if repaired
                else [self._snapshot_item("22392", 0)]
            )
            return {
                "location_id": "gid://shopify/Location/4",
                "items": items,
            }

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector._collect_new_numeric_product_skus = mock.Mock(
                side_effect=detect_probe
            )
            connector._repair_matrix_structures = mock.Mock(
                side_effect=repair_structures
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                side_effect=inventory_snapshot
            )
            state = self._matrix_state()
            state["catalog_structure_schema_version"] = (
                CATALOG_STRUCTURE_SCHEMA_VERSION
            )
            save_state(state_path, state)

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ) as load_products:
                connector.run_cycle()
                connector.run_cycle()

            self.assertEqual(load_products.call_count, 2)
            connector._repair_matrix_structures.assert_called_once_with([payload])
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["pending_matrix_structure_repairs"], {})
            self.assertEqual(
                persisted["catalog_structure_schema_version"],
                CATALOG_STRUCTURE_SCHEMA_VERSION,
            )

    def test_structure_upgrade_accepts_an_already_correct_matrix_without_pending_state(self):
        payload = self._navy_matrix_payload()
        prepared = SimpleNamespace(payload=payload)
        stats = SimpleNamespace(skipped_non_sellable=0)

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector.writeback_mode = "vfp-oledb"
            connector._retry_pending = Connector._retry_pending.__get__(
                connector,
                Connector,
            )
            connector._apply_shopify_adjustments = mock.Mock(return_value=set())
            connector._apply_pos_adjustments = mock.Mock()
            connector._repair_matrix_structures = mock.Mock()
            connector._fetch_inventory_snapshot = mock.Mock(
                return_value={
                    "location_id": "gid://shopify/Location/4",
                    "items": [
                        self._snapshot_item(variant["sku"], variant["quantity"])
                        for variant in payload["variants"]
                    ],
                }
            )
            state = self._matrix_state()
            state["quantities"].update(
                {
                    variant["sku"]: {
                        "canonical": variant["quantity"],
                        "pos_seen": variant["quantity"],
                        "shop_seen": variant["quantity"],
                    }
                    for variant in payload["variants"]
                }
            )
            first_stocked_sku = next(
                variant["sku"]
                for variant in payload["variants"]
                if variant["quantity"]
            )
            state["quantities"][first_stocked_sku]["pending_pos"] = {
                "sku": first_stocked_sku,
                "delta": -1,
                "target_quantity": 0,
                "expected_quantity": 1,
            }
            save_state(state_path, state)

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ):
                connector.run_cycle()
                connector.run_cycle()

            connector._repair_matrix_structures.assert_not_called()
            connector._apply_shopify_adjustments.assert_not_called()
            connector._apply_pos_adjustments.assert_not_called()
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                persisted["catalog_structure_schema_version"],
                CATALOG_STRUCTURE_SCHEMA_VERSION,
            )
            self.assertEqual(persisted["pending_matrix_structure_repairs"], {})
            self.assertNotIn("22392", persisted["quantities"])
            for variant in payload["variants"]:
                self.assertEqual(
                    persisted["quantities"][variant["sku"]]["canonical"],
                    variant["quantity"],
                )
            self.assertNotIn(
                "pending_pos",
                persisted["quantities"][first_stocked_sku],
            )

    def test_structure_upgrade_never_retries_legacy_matrix_actions_and_rebases_children(self):
        payload = self._navy_matrix_payload()
        prepared = SimpleNamespace(payload=payload)
        stats = SimpleNamespace(skipped_non_sellable=0)
        snapshot_calls = 0

        def inventory_snapshot():
            nonlocal snapshot_calls
            snapshot_calls += 1
            if snapshot_calls == 1:
                items = [self._snapshot_item("22392", 0)]
            else:
                items = [
                    self._snapshot_item(variant["sku"], 0)
                    for variant in payload["variants"]
                ]
            return {
                "location_id": "gid://shopify/Location/4",
                "items": items,
            }

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector.writeback_mode = "vfp-oledb"
            connector._retry_pending = Connector._retry_pending.__get__(
                connector,
                Connector,
            )
            connector._repair_matrix_structures = mock.Mock(
                return_value={"22392": self._structure_result(payload)}
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                side_effect=inventory_snapshot
            )
            connector._apply_shopify_adjustments = mock.Mock(return_value=set())
            connector._apply_pos_adjustments = mock.Mock()
            state = self._matrix_state()
            state["quantities"] = {
                "22392": {
                    "canonical": 6,
                    "pos_seen": 6,
                    "shop_seen": 6,
                    "pending_pos": {
                        "sku": "22392",
                        "delta": -6,
                        "target_quantity": 0,
                        "expected_quantity": 6,
                    },
                },
                "OTHER": {
                    "canonical": 2,
                    "pos_seen": 2,
                    "shop_seen": 0,
                    "pending_shop": {
                        "sku": "OTHER",
                        "delta": 2,
                        "target_quantity": 2,
                    },
                },
                **{
                    variant["sku"]: {
                        "canonical": variant["quantity"],
                        "pos_seen": variant["quantity"],
                        "shop_seen": variant["quantity"],
                        "pending_pos": {
                            "sku": variant["sku"],
                            "delta": -variant["quantity"],
                            "target_quantity": 0,
                            "expected_quantity": variant["quantity"],
                        },
                    }
                    for variant in payload["variants"]
                },
            }
            save_state(state_path, state)

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ):
                connector.run_cycle()
                connector._apply_shopify_adjustments.assert_not_called()
                connector._apply_pos_adjustments.assert_not_called()
                connector.run_cycle()

            post_repair_state = json.loads(state_path.read_text(encoding="utf-8"))
            post_repair_state["quantities"]["OTHER"].pop("pending_shop", None)
            save_state(state_path, post_repair_state)
            delayed_base_change = {
                "id": 7,
                "version": 1,
                "sku": "22392",
                "quantity": 0,
                "inventory_item_id": "legacy-base-item",
                "source_updated_at": "2026-08-21T12:00:00+00:00",
            }
            connector._fetch_inventory_changes.return_value = [delayed_base_change]
            with mock.patch.object(
                sys.modules["windows_connector.connector"],
                "read_targeted_pos_quantities",
                return_value=flatten_quantities([payload]),
            ):
                connector.run_cycle()

            shop_calls = connector._apply_shopify_adjustments.call_args_list
            self.assertEqual(shop_calls[0].args[1], [state["quantities"]["OTHER"]["pending_shop"]])
            self.assertEqual(len(shop_calls), 1)
            connector._apply_pos_adjustments.assert_not_called()
            connector._acknowledge_inventory_changes.assert_called_once_with(
                [delayed_base_change]
            )
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertNotIn("22392", persisted["quantities"])
            for variant in payload["variants"]:
                entry = persisted["quantities"][variant["sku"]]
                self.assertEqual(entry["canonical"], variant["quantity"])
                self.assertEqual(entry["pos_seen"], variant["quantity"])
                self.assertEqual(entry["shop_seen"], variant["quantity"])
                self.assertNotIn("pending_shop", entry)
                self.assertNotIn("pending_pos", entry)

    def test_repaired_matrix_remains_due_until_child_snapshot_verifies(self):
        payload = self._navy_matrix_payload()
        prepared = SimpleNamespace(payload=payload)
        stats = SimpleNamespace(skipped_non_sellable=0)
        snapshot_calls = 0

        def inventory_snapshot():
            nonlocal snapshot_calls
            snapshot_calls += 1
            if snapshot_calls == 1:
                items = [self._snapshot_item("22392", 0)]
            elif snapshot_calls == 2:
                items = [
                    self._snapshot_item(variant["sku"], variant["quantity"])
                    for variant in payload["variants"][:3]
                ]
            else:
                items = [
                    self._snapshot_item(variant["sku"], variant["quantity"])
                    for variant in payload["variants"]
                ]
            return {
                "location_id": "gid://shopify/Location/4",
                "items": items,
            }

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector._repair_matrix_structures = mock.Mock(
                return_value={"22392": self._structure_result(payload)}
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                side_effect=inventory_snapshot
            )
            save_state(state_path, self._matrix_state())

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ):
                connector.run_cycle()
                connector.run_cycle()
                waiting = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertNotIn("catalog_structure_schema_version", waiting)
                self.assertEqual(
                    waiting["pending_matrix_structure_repairs"]["22392"]["stage"],
                    "verification",
                )
                connector.run_cycle()

            connector._repair_matrix_structures.assert_called_once_with([payload])
            self.assertEqual(connector._fetch_inventory_snapshot.call_count, 3)
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                persisted["catalog_structure_schema_version"],
                CATALOG_STRUCTURE_SCHEMA_VERSION,
            )
            self.assertEqual(persisted["pending_matrix_structure_repairs"], {})

    def test_structure_upgrade_requires_two_identical_full_payloads(self):
        first_payload = self._navy_matrix_payload(title="Copied shirt")
        finished_payload = self._navy_matrix_payload(
            title="Eterna Dress Shirt - Navy"
        )
        stats = SimpleNamespace(skipped_non_sellable=0)
        call_order = []
        repaired = False

        def repair_structures(items):
            nonlocal repaired
            call_order.append("repair")
            repaired = True
            return {"22392": self._structure_result(finished_payload)}

        def inventory_snapshot():
            call_order.append("snapshot")
            items = (
                [
                    self._snapshot_item(variant["sku"], variant["quantity"])
                    for variant in finished_payload["variants"]
                ]
                if repaired
                else [self._snapshot_item("22392", 0)]
            )
            return {
                "location_id": "gid://shopify/Location/4",
                "items": items,
            }

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector._repair_matrix_structures = mock.Mock(
                side_effect=repair_structures
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                side_effect=inventory_snapshot
            )
            save_state(state_path, self._matrix_state())
            load_results = [
                ([SimpleNamespace(payload=first_payload)], stats),
                ([SimpleNamespace(payload=finished_payload)], stats),
                ([SimpleNamespace(payload=finished_payload)], stats),
            ]

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                side_effect=load_results,
            ):
                connector.run_cycle()
                connector.run_cycle()
                second_state = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(
                    second_state["pending_matrix_structure_repairs"]["22392"][
                        "observations"
                    ],
                    1,
                )
                connector._repair_matrix_structures.assert_not_called()
                connector.run_cycle()

            self.assertEqual(
                call_order,
                ["snapshot", "snapshot", "repair", "snapshot"],
            )
            connector._repair_matrix_structures.assert_called_once_with(
                [finished_payload]
            )

    def test_unsafe_matrix_shapes_are_recorded_without_mutation_or_busy_retry(self):
        payload = self._navy_matrix_payload()
        first_child = payload["variants"][0]["sku"]
        unsafe_shapes = {
            "partial": [
                self._snapshot_item("22392", 0),
                self._snapshot_item(first_child, 0),
            ],
            "duplicate_base": [
                self._snapshot_item("22392", 0, duplicate=1),
            ],
            "unavailable_base": [
                self._snapshot_item("22392", 0, available=False),
            ],
            "child_without_base": [
                self._snapshot_item(first_child, 0),
            ],
        }

        for shape, snapshot_items in unsafe_shapes.items():
            with self.subTest(shape=shape), tempfile.TemporaryDirectory() as temporary_directory:
                state_path = Path(temporary_directory) / "state.json"
                connector = self._matrix_connector(state_path)
                connector._repair_matrix_structures = mock.Mock()
                connector._fetch_inventory_snapshot = mock.Mock(
                    return_value={
                        "location_id": "gid://shopify/Location/4",
                        "items": snapshot_items,
                    }
                )
                save_state(state_path, self._matrix_state())
                prepared = SimpleNamespace(payload=payload)
                stats = SimpleNamespace(skipped_non_sellable=0)

                with mock.patch.object(
                    sys.modules["windows_connector.connector"].dbf_pos_sync,
                    "load_products",
                    return_value=([prepared], stats),
                ) as load_products:
                    connector.run_cycle()
                    connector.run_cycle()

                connector._repair_matrix_structures.assert_not_called()
                connector._apply_shopify_adjustments.assert_not_called()
                connector._apply_pos_adjustments.assert_not_called()
                connector._fetch_inventory_snapshot.assert_called_once()
                self.assertEqual(load_products.call_count, 1)
                persisted = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(
                    persisted["catalog_structure_schema_version"],
                    CATALOG_STRUCTURE_SCHEMA_VERSION,
                )
                self.assertEqual(
                    persisted["pending_matrix_structure_repairs"]["22392"]["stage"],
                    "snapshot_blocked",
                )

    def test_snapshot_blocked_matrix_can_become_a_safe_scalar_candidate_later(self):
        payload = self._navy_matrix_payload()
        prepared = SimpleNamespace(payload=payload)
        stats = SimpleNamespace(skipped_non_sellable=0)
        snapshot_items = [
            self._snapshot_item("22392", 0),
            self._snapshot_item(payload["variants"][0]["sku"], 0),
        ]

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector._repair_matrix_structures = mock.Mock()
            connector._fetch_inventory_snapshot = mock.Mock(
                side_effect=lambda: {
                    "location_id": "gid://shopify/Location/4",
                    "items": snapshot_items,
                }
            )
            save_state(state_path, self._matrix_state())

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ):
                connector.run_cycle()
                blocked_state = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(
                    blocked_state["pending_matrix_structure_repairs"]["22392"]["stage"],
                    "snapshot_blocked",
                )
                blocked_state["last_full_reconcile_date"] = "2000-01-01"
                save_state(state_path, blocked_state)
                snapshot_items[:] = [self._snapshot_item("22392", 0)]
                connector.run_cycle()

            connector._repair_matrix_structures.assert_not_called()
            pending = json.loads(state_path.read_text(encoding="utf-8"))[
                "pending_matrix_structure_repairs"
            ]["22392"]
            self.assertEqual(pending["stage"], "candidate")
            self.assertEqual(pending["observations"], 1)

    def test_server_blocked_matrix_retries_on_a_later_nightly_scan_not_every_cycle(self):
        payload = self._navy_matrix_payload()
        prepared = SimpleNamespace(payload=payload)
        stats = SimpleNamespace(skipped_non_sellable=0)

        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            connector = self._matrix_connector(state_path)
            connector._repair_matrix_structures = mock.Mock(
                return_value={
                    "22392": self._structure_result(payload, status="blocked")
                }
            )
            connector._fetch_inventory_snapshot = mock.Mock(
                return_value={
                    "location_id": "gid://shopify/Location/4",
                    "items": [self._snapshot_item("22392", 0)],
                }
            )
            save_state(state_path, self._matrix_state())

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ) as load_products:
                connector.run_cycle()
                connector.run_cycle()
                connector.run_cycle()
                waiting = json.loads(state_path.read_text(encoding="utf-8"))
                waiting["last_full_reconcile_date"] = "2000-01-01"
                waiting["pending_matrix_structure_repairs"]["22392"][
                    "last_attempt_date"
                ] = "2000-01-01"
                save_state(state_path, waiting)
                connector.run_cycle()
                connector.run_cycle()

            self.assertEqual(connector._repair_matrix_structures.call_count, 2)
            self.assertEqual(connector._fetch_inventory_snapshot.call_count, 3)
            self.assertEqual(load_products.call_count, 3)
            persisted = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                persisted["catalog_structure_schema_version"],
                CATALOG_STRUCTURE_SCHEMA_VERSION,
            )
            self.assertEqual(
                persisted["pending_matrix_structure_repairs"]["22392"]["stage"],
                "server_blocked",
            )

    def test_duplicate_snapshot_sku_is_blocked_from_all_adjustments(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            connector = Connector.__new__(Connector)
            connector.dry_run = False
            connector.order_sync_enabled = False
            connector.price_sync_enabled = False
            connector.initial_catalog_upload = True
            connector.nightly_full_sync_hour = 0
            connector.writeback_mode = "vfp-oledb"
            connector.state_path = Path(temporary_directory) / "state.json"
            connector.logger = mock.Mock()
            connector._retry_pending = mock.Mock()
            connector._fetch_inventory_changes = mock.Mock(return_value=[])
            connector._collect_pos_event_skus = mock.Mock(return_value=(set(), False))
            connector._collect_new_numeric_product_skus = mock.Mock(return_value=set())
            connector._reader_args = mock.Mock(return_value=SimpleNamespace())
            connector._fetch_inventory_snapshot = mock.Mock(
                return_value={
                    "location_id": "gid://shopify/Location/4",
                    "items": [
                        {
                            "sku": "DUP",
                            "quantity": 0,
                            "location_id": "gid://shopify/Location/4",
                            "available_at_location": True,
                            "duplicate_sku_count": 1,
                        }
                    ],
                }
            )
            connector._apply_shopify_adjustments = mock.Mock()
            connector._apply_pos_adjustments = mock.Mock()
            connector._acknowledge_inventory_changes = mock.Mock()
            save_state(
                connector.state_path,
                {
                    "version": 1,
                    "catalog_complete": True,
                    "catalog_products": ["DUP"],
                    "quantities": {
                        "DUP": {"canonical": 6, "pos_seen": 6, "shop_seen": 6}
                    },
                    "event_cursors": {},
                    "sku_bases": {"DUP": "DUP"},
                    "pending_catalog_products": {},
                    "matrix_option_schema_version": 2,
                    "last_full_reconcile_date": datetime.now().date().isoformat(),
                },
            )
            prepared = SimpleNamespace(payload={"sku": "DUP", "quantity": 6})
            stats = SimpleNamespace(skipped_non_sellable=0)

            with mock.patch.object(
                sys.modules["windows_connector.connector"].dbf_pos_sync,
                "load_products",
                return_value=([prepared], stats),
            ):
                connector.run_cycle()

            connector._apply_shopify_adjustments.assert_not_called()
            connector._apply_pos_adjustments.assert_not_called()
            persisted = json.loads(connector.state_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["blocked_inventory_skus"], ["DUP"])

    def test_snapshot_client_rejects_an_old_response_schema(self):
        connector = Connector.__new__(Connector)
        connector.base_url = "https://sync.example"
        connector.timeout = 30
        connector.session = mock.Mock()
        response = mock.Mock()
        response.json.return_value = {"items": [{"sku": "ABC", "quantity": 1}]}
        connector.session.get.return_value = response

        with self.assertRaisesRegex(RuntimeError, "schema is incompatible"):
            connector._fetch_inventory_snapshot()

        response.raise_for_status.assert_called_once()

    def test_actual_shopify_result_rebases_a_stale_paired_pos_adjustment(self):
        connector = Connector.__new__(Connector)
        connector.base_url = "https://sync.example"
        connector.timeout = 30
        connector.batch_size = 25
        connector.logger = mock.Mock()
        connector.state_path = Path("unused-state.json")
        connector.session = mock.Mock()
        response = mock.Mock()
        response.json.return_value = {
            "results": [
                {
                    "success": True,
                    "sku": "ABC",
                    "quantity_after_change": 9,
                }
            ]
        }
        connector.session.post.return_value = response
        shop_action = {
            "sku": "ABC",
            "delta": -1,
            "target_quantity": 8,
            "idempotency_key": "shop-key",
        }
        pos_action = {
            "sku": "ABC",
            "delta": -1,
            "target_quantity": 8,
            "expected_quantity": 9,
            "idempotency_key": "pos-key",
        }
        state = {
            "quantities": {
                "ABC": {
                    "canonical": 8,
                    "pos_seen": 9,
                    "shop_seen": 9,
                    "pending_shop": shop_action,
                    "pending_pos": pos_action,
                }
            }
        }

        with tempfile.TemporaryDirectory() as temporary_directory:
            connector.state_path = Path(temporary_directory) / "state.json"
            deferred = connector._apply_shopify_adjustments(state, [shop_action])

        self.assertEqual(deferred, set())
        self.assertEqual(state["quantities"]["ABC"]["canonical"], 9)
        self.assertEqual(state["quantities"]["ABC"]["shop_seen"], 9)
        self.assertNotIn("pending_shop", state["quantities"]["ABC"])
        self.assertNotIn("pending_pos", state["quantities"]["ABC"])

    def test_shopify_race_synthesizes_a_guarded_pos_correction(self):
        connector = Connector.__new__(Connector)
        connector.base_url = "https://sync.example"
        connector.timeout = 30
        connector.batch_size = 25
        connector.writeback_mode = "vfp-oledb"
        connector.logger = mock.Mock()
        connector.session = mock.Mock()
        response = mock.Mock()
        response.json.return_value = {
            "results": [
                {
                    "success": True,
                    "sku": "ABC",
                    "quantity_after_change": 6,
                }
            ]
        }
        connector.session.post.return_value = response
        shop_action = {
            "sku": "ABC",
            "delta": 1,
            "target_quantity": 7,
            "idempotency_key": "shop-key",
        }
        state = {
            "quantities": {
                "ABC": {
                    "canonical": 7,
                    "pos_seen": 7,
                    "shop_seen": 6,
                    "pending_shop": shop_action,
                    "revision": 1,
                }
            }
        }

        with tempfile.TemporaryDirectory() as temporary_directory:
            connector.state_path = Path(temporary_directory) / "state.json"
            blocked = connector._apply_shopify_adjustments(state, [shop_action])

        self.assertEqual(blocked, set())
        self.assertEqual(state["quantities"]["ABC"]["canonical"], 6)
        self.assertEqual(
            {
                key: state["quantities"]["ABC"]["pending_pos"][key]
                for key in ("sku", "delta", "target_quantity", "expected_quantity")
            },
            {
                "sku": "ABC",
                "delta": -1,
                "target_quantity": 6,
                "expected_quantity": 7,
            },
        )

    def test_unconfirmed_shopify_result_defers_a_paired_pos_adjustment(self):
        connector = Connector.__new__(Connector)
        connector.base_url = "https://sync.example"
        connector.timeout = 30
        connector.batch_size = 25
        connector.logger = mock.Mock()
        connector.session = mock.Mock()
        response = mock.Mock()
        response.json.return_value = {
            "results": [
                {
                    "success": True,
                    "sku": "ABC",
                    "quantity_after_change": None,
                }
            ]
        }
        connector.session.post.return_value = response
        shop_action = {
            "sku": "ABC",
            "delta": -1,
            "target_quantity": 8,
            "idempotency_key": "shop-key",
        }
        state = {
            "quantities": {
                "ABC": {
                    "canonical": 8,
                    "pos_seen": 9,
                    "shop_seen": 9,
                    "pending_shop": shop_action,
                    "pending_pos": {
                        "sku": "ABC",
                        "delta": -1,
                        "target_quantity": 8,
                        "expected_quantity": 9,
                        "idempotency_key": "pos-key",
                    },
                }
            }
        }

        with tempfile.TemporaryDirectory() as temporary_directory:
            connector.state_path = Path(temporary_directory) / "state.json"
            deferred = connector._apply_shopify_adjustments(state, [shop_action])

        self.assertEqual(deferred, {"ABC"})
        self.assertNotIn("pending_pos", state["quantities"]["ABC"])


class DatabaseRetentionTests(unittest.TestCase):
    def test_feed_and_request_history_are_bounded(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DatabaseStore(
                str(Path(temporary_directory) / "sync.sqlite3"),
                "test-secret",
                feed_event_retention_rows=100,
                request_log_retention_rows=100,
            )
            for index in range(105):
                store.record_feed_event(
                    shop_domain="example.myshopify.com",
                    source="test",
                    endpoint="/sync",
                    method="POST",
                    sku=f"SKU-{index}",
                    title=None,
                    success=True,
                    message="ok",
                    product_id=None,
                    variant_id=None,
                    request_payload="{}",
                    normalized_payload="{}",
                )
                store.record_request_log(
                    shop_domain="example.myshopify.com",
                    api_key_preview=None,
                    method="POST",
                    path="/sync",
                    query_string=None,
                    status_code=200,
                    route_path="/sync",
                    request_body=None,
                    user_agent="test",
                    source_ip="127.0.0.1",
                    duration_ms=1,
                )

            self.assertEqual(store.feed_event_count("example.myshopify.com"), 100)
            self.assertEqual(store.request_log_count(), 100)

    def test_optional_activity_logging_is_bounded_and_non_blocking(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DatabaseStore(
                str(Path(temporary_directory) / "sync.sqlite3"),
                "test-secret",
            )
            store.record_feed_event(
                shop_domain="example.myshopify.com",
                source="test",
                endpoint="/sync/bulk",
                method="POST",
                sku="ABC",
                title="Product",
                success=True,
                message="ok",
                product_id="1",
                variant_id="2",
                request_payload="x" * 10000,
                normalized_payload="y" * 10000,
            )
            with sqlite3.connect(store.database_path) as connection:
                lengths = connection.execute(
                    "SELECT length(request_payload), length(normalized_payload) FROM feed_events"
                ).fetchone()
            self.assertEqual(lengths, (4000, 4000))

            with mock.patch.object(store.logger, "exception"), mock.patch.object(
                store, "_connect", side_effect=sqlite3.OperationalError("database is full")
            ):
                store.record_feed_event(
                    shop_domain="example.myshopify.com",
                    source="test",
                    endpoint="/sync/bulk",
                    method="POST",
                    sku="ABC",
                    title="Product",
                    success=True,
                    message="ok",
                    product_id="1",
                    variant_id="2",
                    request_payload="{}",
                    normalized_payload=None,
                )
                store.record_request_log(
                    shop_domain="example.myshopify.com",
                    api_key_preview="key",
                    method="POST",
                    path="/sync/bulk",
                    query_string=None,
                    status_code=200,
                    route_path="/sync/bulk",
                    request_body="{}",
                    user_agent="test",
                    source_ip="127.0.0.1",
                    duration_ms=1,
                )

    def test_inventory_change_ack_does_not_delete_a_newer_webhook(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DatabaseStore(str(Path(temporary_directory) / "sync.sqlite3"), "test-secret")
            store.upsert_inventory_item_sku(
                shop_domain="example.myshopify.com",
                inventory_item_id="gid://shopify/InventoryItem/1",
                sku="ABC",
            )
            self.assertEqual(
                store.get_inventory_item_sku(
                    shop_domain="example.myshopify.com",
                    inventory_item_id="gid://shopify/InventoryItem/1",
                ),
                "ABC",
            )
            store.upsert_inventory_change(
                shop_domain="example.myshopify.com",
                inventory_item_id="gid://shopify/InventoryItem/1",
                location_id="gid://shopify/Location/2",
                sku="ABC",
                quantity=9,
            )
            first = store.list_inventory_changes(shop_domain="example.myshopify.com")[0]
            store.upsert_inventory_change(
                shop_domain="example.myshopify.com",
                inventory_item_id="gid://shopify/InventoryItem/1",
                location_id="gid://shopify/Location/2",
                sku="ABC",
                quantity=8,
            )

            self.assertEqual(
                store.acknowledge_inventory_changes(
                    shop_domain="example.myshopify.com",
                    changes=[(first.id, first.version)],
                ),
                0,
            )
            latest = store.list_inventory_changes(shop_domain="example.myshopify.com")[0]
            self.assertEqual(latest.quantity, 8)
            self.assertEqual(latest.version, 2)
            self.assertEqual(
                store.acknowledge_inventory_changes(
                    shop_domain="example.myshopify.com",
                    changes=[(latest.id, latest.version)],
                ),
                1,
            )

    def test_inventory_item_quantity_observation_survives_queue_acknowledgement(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DatabaseStore(str(Path(temporary_directory) / "sync.sqlite3"), "test-secret")
            item_id = "gid://shopify/InventoryItem/1"
            store.upsert_inventory_item_quantity(
                shop_domain="example.myshopify.com",
                inventory_item_id=item_id,
                sku="ABC",
                quantity=0,
            )
            store.upsert_inventory_change(
                shop_domain="example.myshopify.com",
                inventory_item_id=item_id,
                location_id="gid://shopify/Location/2",
                sku="ABC",
                quantity=6,
            )
            queued = store.list_inventory_changes(shop_domain="example.myshopify.com")[0]
            store.acknowledge_inventory_changes(
                shop_domain="example.myshopify.com",
                changes=[(queued.id, queued.version)],
            )

            self.assertEqual(
                store.get_inventory_item_last_quantity(
                    shop_domain="example.myshopify.com",
                    inventory_item_id=item_id,
                ),
                0,
            )
            store.upsert_inventory_item_quantity(
                shop_domain="example.myshopify.com",
                inventory_item_id=item_id,
                sku="ABC",
                quantity=6,
            )
            self.assertEqual(
                store.get_inventory_item_last_quantity(
                    shop_domain="example.myshopify.com",
                    inventory_item_id=item_id,
                ),
                6,
            )

    def test_delayed_inventory_webhook_does_not_replace_a_newer_queued_value(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DatabaseStore(str(Path(temporary_directory) / "sync.sqlite3"), "test-secret")
            common = {
                "shop_domain": "example.myshopify.com",
                "inventory_item_id": "gid://shopify/InventoryItem/1",
                "location_id": "gid://shopify/Location/2",
                "sku": "ABC",
            }
            store.upsert_inventory_change(
                **common,
                quantity=6,
                source_updated_at="2026-08-21T12:00:00Z",
            )
            store.upsert_inventory_change(
                **common,
                quantity=5,
                source_updated_at="2026-08-21T12:02:00Z",
            )
            store.upsert_inventory_change(
                **common,
                quantity=4,
                source_updated_at="2026-08-21T12:01:00Z",
            )

            queued = store.list_inventory_changes(
                shop_domain="example.myshopify.com"
            )[0]
            self.assertEqual(queued.quantity, 5)
            self.assertEqual(queued.version, 2)
            self.assertEqual(queued.source_updated_at, "2026-08-21T12:02:00Z")

    def test_order_change_ack_does_not_delete_a_newer_webhook(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DatabaseStore(
                str(Path(temporary_directory) / "sync.sqlite3"),
                "test-secret",
                order_event_retention_rows=100,
            )
            store.upsert_order_change(
                shop_domain="example.myshopify.com",
                shopify_order_id="1001",
                order_name="#1001",
                event_topic="orders/create",
                payload='{"id":1001}',
            )
            self.assertEqual(store.order_change_count(shop_domain="example.myshopify.com"), 1)
            first = store.list_order_changes(shop_domain="example.myshopify.com")[0]
            store.upsert_order_change(
                shop_domain="example.myshopify.com",
                shopify_order_id="1001",
                order_name="#1001",
                event_topic="orders/updated",
                payload='{"id":1001,"total_price":"42.00"}',
            )

            self.assertEqual(
                store.acknowledge_order_changes(
                    shop_domain="example.myshopify.com",
                    changes=[(first.id, first.version)],
                ),
                0,
            )
            latest = store.list_order_changes(shop_domain="example.myshopify.com")[0]
            self.assertEqual(latest.version, 2)
            self.assertEqual(latest.event_topic, "orders/updated")

    def test_recent_order_summaries_are_small_bounded_and_show_delivery(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DatabaseStore(
                str(Path(temporary_directory) / "sync.sqlite3"),
                "test-secret",
                recent_order_retention_rows=10,
            )
            for index in range(12):
                order_id = str(1000 + index)
                store.upsert_order_change(
                    shop_domain="example.myshopify.com",
                    shopify_order_id=order_id,
                    order_name=f"#{order_id}",
                    event_topic="orders/create",
                    payload=f'{{"id":{order_id}}}',
                )
                store.upsert_recent_order_summary(
                    shop_domain="example.myshopify.com",
                    shopify_order_id=order_id,
                    order_name=f"#{order_id}",
                    total_price="42.00",
                    currency="USD",
                    financial_status="paid",
                    fulfillment_status=None,
                    order_created_at="2026-07-23T12:00:00+00:00",
                )

            recent = store.list_recent_order_summaries(
                shop_domain="example.myshopify.com",
                limit=20,
            )
            self.assertEqual(len(recent), 10)
            self.assertEqual(recent[0].delivery_status, "queued")
            queued = store.list_order_changes(shop_domain="example.myshopify.com")
            store.acknowledge_order_changes(
                shop_domain="example.myshopify.com",
                changes=[(queued[-1].id, queued[-1].version)],
            )
            recent = store.list_recent_order_summaries(
                shop_domain="example.myshopify.com",
                limit=20,
            )
            delivered = next(row for row in recent if row.shopify_order_id == queued[-1].shopify_order_id)
            self.assertEqual(delivered.delivery_status, "sent_to_pos")

    def test_connector_heartbeat_uses_one_row_per_shop(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DatabaseStore(str(Path(temporary_directory) / "sync.sqlite3"), "test-secret")
            store.record_connector_heartbeat(
                shop_domain="example.myshopify.com",
                channel="inventory",
            )
            store.record_connector_heartbeat(
                shop_domain="example.myshopify.com",
                channel="orders",
            )
            heartbeat = store.get_connector_heartbeat(shop_domain="example.myshopify.com")
            self.assertIsNotNone(heartbeat["last_seen_at"])
            self.assertIsNotNone(heartbeat["last_inventory_poll_at"])
            self.assertIsNotNone(heartbeat["last_order_poll_at"])
            with sqlite3.connect(store.database_path) as connection:
                count = connection.execute("SELECT COUNT(*) FROM connector_heartbeats").fetchone()[0]
            self.assertEqual(count, 1)

    def test_recent_price_changes_are_bounded_to_fifty_rows(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = DatabaseStore(str(Path(temporary_directory) / "sync.sqlite3"), "test-secret")
            for index in range(55):
                store.record_recent_price_change(
                    shop_domain="example.myshopify.com",
                    source_sku=str(22000 + index),
                    old_price="10.00",
                    new_price="11.00",
                    target_count=1,
                    success=True,
                    message="Updated.",
                )

            recent = store.list_recent_price_changes(
                shop_domain="example.myshopify.com",
                limit=50,
            )
            self.assertEqual(len(recent), 50)
            self.assertEqual(recent[0].source_sku, "22054")


class ShopifyScopeTests(unittest.TestCase):
    def test_live_access_scopes_are_read_from_current_app_installation(self):
        client = ShopifyClient(SimpleNamespace())
        with mock.patch.object(
            client,
            "graphql",
            return_value={
                "data": {
                    "currentAppInstallation": {
                        "accessScopes": [
                            {"handle": "read_products"},
                            {"handle": "read_orders"},
                        ]
                    }
                }
            },
        ):
            scopes = client.get_access_scopes("example.myshopify.com", "token")

        self.assertEqual(scopes, {"read_products", "read_orders"})

    def test_inventory_snapshot_paginates_and_preserves_unavailable_levels(self):
        client = ShopifyClient(SimpleNamespace())
        pages = [
            {
                "data": {
                    "inventoryItems": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "next"},
                        "nodes": [
                            {
                                "id": "gid://shopify/InventoryItem/1",
                                "sku": "ZERO",
                                "duplicateSkuCount": 0,
                                "inventoryLevel": {
                                    "updatedAt": "2026-08-21T10:00:00Z",
                                    "quantities": [{"name": "available", "quantity": 0}],
                                },
                            },
                            {
                                "id": "gid://shopify/InventoryItem/2",
                                "sku": "UNAVAILABLE",
                                "duplicateSkuCount": 0,
                                "inventoryLevel": None,
                            },
                        ],
                    }
                }
            },
            {
                "data": {
                    "inventoryItems": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            {
                                "id": "gid://shopify/InventoryItem/3",
                                "sku": "DUPLICATE",
                                "duplicateSkuCount": 1,
                                "inventoryLevel": {
                                    "updatedAt": "2026-08-21T10:01:00Z",
                                    "quantities": [{"name": "available", "quantity": 4}],
                                },
                            }
                        ],
                    }
                }
            },
        ]

        with mock.patch.object(client, "graphql", side_effect=pages) as graphql:
            rows = client.get_inventory_snapshot(
                "example.myshopify.com",
                "token",
                "4",
            )

        self.assertEqual([row["sku"] for row in rows], ["ZERO", "UNAVAILABLE", "DUPLICATE"])
        self.assertEqual(rows[0]["quantity"], 0)
        self.assertEqual(rows[0]["inventory_level_updated_at"], "2026-08-21T10:00:00Z")
        self.assertIsNone(rows[1]["quantity"])
        self.assertFalse(rows[1]["available_at_location"])
        self.assertEqual(rows[2]["duplicate_sku_count"], 1)
        self.assertIsNone(graphql.call_args_list[0].args[3]["after"])
        self.assertEqual(graphql.call_args_list[1].args[3]["after"], "next")

    def test_primary_location_query_is_cached(self):
        client = ShopifyClient(SimpleNamespace(shopify_location_id=None))
        with mock.patch.object(
            client,
            "graphql",
            return_value={
                "data": {
                    "location": {
                        "id": "gid://shopify/Location/4",
                        "name": "Primary",
                    }
                }
            },
        ) as graphql:
            first = client.get_primary_location_id("example.myshopify.com", "token")
            second = client.get_primary_location_id("example.myshopify.com", "token")

        self.assertEqual(first, "gid://shopify/Location/4")
        self.assertEqual(second, first)
        graphql.assert_called_once()
        self.assertEqual(graphql.call_args.kwargs["operation_name"], "GetPrimaryLocation")

    @staticmethod
    def _variant_node(sku: str, variant_id: str) -> dict:
        return {
            "id": variant_id,
            "sku": sku,
            "price": "195.00",
            "product": {
                "id": "gid://shopify/Product/2",
                "title": "Shirt",
                "status": "DRAFT",
                "autoArchivedZeroStock": None,
            },
            "inventoryItem": {
                "id": f"gid://shopify/InventoryItem/{variant_id.rsplit('/', 1)[-1]}",
                "unitCost": None,
                "inventoryLevels": {"nodes": []},
            },
        }

    def test_variant_lookup_uses_only_the_exact_case_sensitive_sku(self):
        client = ShopifyClient(SimpleNamespace(shopify_sku_cache_ttl_seconds=60))
        with mock.patch.object(
            client,
            "graphql",
            return_value={
                "data": {
                    "productVariants": {
                        "nodes": [
                            self._variant_node("ABC-OLD", "gid://shopify/ProductVariant/1"),
                            self._variant_node("ABC", "gid://shopify/ProductVariant/2"),
                        ]
                    }
                }
            },
        ):
            mapping = client.get_variant_by_sku("example.myshopify.com", "token", "ABC")

        self.assertEqual(mapping.variant_id, "gid://shopify/ProductVariant/2")

    def test_variant_lookup_rejects_duplicate_exact_skus(self):
        client = ShopifyClient(SimpleNamespace(shopify_sku_cache_ttl_seconds=60))
        with mock.patch.object(
            client,
            "graphql",
            return_value={
                "data": {
                    "productVariants": {
                        "nodes": [
                            self._variant_node("ABC", "gid://shopify/ProductVariant/1"),
                            self._variant_node("ABC", "gid://shopify/ProductVariant/2"),
                        ]
                    }
                }
            },
        ), self.assertRaises(SyncProcessingError) as raised:
            client.get_variant_by_sku("example.myshopify.com", "token", "ABC")

        self.assertEqual(raised.exception.code, "duplicate_shopify_sku")

    def test_inventory_adjustment_returns_shopifys_actual_resulting_quantity(self):
        client = ShopifyClient(SimpleNamespace())
        with mock.patch.object(
            client,
            "graphql",
            return_value={
                "data": {
                    "inventoryAdjustQuantities": {
                        "inventoryAdjustmentGroup": {
                            "createdAt": "2026-08-21T12:00:00Z",
                            "changes": [
                                {
                                    "name": "available",
                                    "delta": 2,
                                    "quantityAfterChange": 6,
                                }
                            ]
                        },
                        "userErrors": [],
                    }
                }
            },
        ):
            result = client.adjust_inventory(
                "example.myshopify.com",
                "token",
                "3",
                "4",
                2,
                idempotency_key="stable-key",
            )

        self.assertEqual(result["quantity_after_change"], 6)
        self.assertEqual(result["updated_at"], "2026-08-21T12:00:00Z")


class LocalOrderInboxTests(unittest.TestCase):
    def test_connector_defaults_order_dbfs_to_sibling_web_folder(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            pos_directory = directory / "ashpsdat"
            config_path = directory / "connector.env"
            config_path.write_text(
                "\n".join(
                    (
                        f"POS_DBF_DIR={pos_directory}",
                        "SHOPIFY_SYNC_BASE_URL=https://sync.example",
                        "SHOPIFY_SYNC_API_KEY=test-key",
                        "SHOPIFY_SYNC_API_SECRET=test-secret",
                        f"CONNECTOR_DATA_DIR={directory / 'connector-data'}",
                    )
                ),
                encoding="utf-8",
            )

            with mock.patch.dict("os.environ", {}, clear=True):
                connector = Connector(config_path=config_path)

            self.assertEqual(
                connector.order_header_path,
                directory / "ashpsdat_web" / "shopify-order-header.dbf",
            )
            self.assertEqual(
                connector.order_detail_path,
                directory / "ashpsdat_web" / "shopify-order-detail.dbf",
            )

    def test_connector_redirects_legacy_default_order_settings_to_web_folder(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            pos_directory = directory / "ashpsdat"
            config_path = directory / "connector.env"
            config_path.write_text(
                "\n".join(
                    (
                        f"POS_DBF_DIR={pos_directory}",
                        "SHOPIFY_SYNC_BASE_URL=https://sync.example",
                        "SHOPIFY_SYNC_API_KEY=test-key",
                        "SHOPIFY_SYNC_API_SECRET=test-secret",
                        f"SHOPIFY_ORDER_DB_PATH={pos_directory / 'shopify-orders.db'}",
                        f"SHOPIFY_ORDER_HEADER_DBF_PATH={pos_directory / 'shopify-order-header.dbf'}",
                        f"SHOPIFY_ORDER_DETAIL_DBF_PATH={pos_directory / 'shopify-order-detail.dbf'}",
                        f"CONNECTOR_DATA_DIR={directory / 'connector-data'}",
                    )
                ),
                encoding="utf-8",
            )

            with mock.patch.dict("os.environ", {}, clear=True):
                connector = Connector(config_path=config_path)

            self.assertEqual(
                connector.order_header_path,
                directory / "ashpsdat_web" / "shopify-order-header.dbf",
            )
            self.assertEqual(
                connector.order_detail_path,
                directory / "ashpsdat_web" / "shopify-order-detail.dbf",
            )
            self.assertIn(
                pos_directory / "shopify-orders.db",
                connector.legacy_order_db_paths,
            )

    def test_empty_sync_creates_missing_web_order_folder_and_dbfs(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            web_directory = Path(temporary_directory) / "ashpsdat_web"
            header_path = web_directory / "shopify-order-header.dbf"
            detail_path = web_directory / "shopify-order-detail.dbf"

            self.assertFalse(web_directory.exists())

            upsert_order_changes(header_path, detail_path, [], retention_rows=100)

            self.assertTrue(header_path.is_file())
            self.assertTrue(detail_path.is_file())
            self.assertTrue(header_path.with_suffix(".lock").is_file())

    def test_order_dbf_paths_must_be_separate_non_native_dbf_files(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            validate_order_dbf_paths(
                directory / "shopify-order-header.dbf",
                directory / "shopify-order-detail.dbf",
            )

            with self.assertRaisesRegex(ValueError, "must end in .dbf"):
                validate_order_dbf_paths(directory / "header.db", directory / "detail.dbf")
            with self.assertRaisesRegex(ValueError, "must be different"):
                validate_order_dbf_paths(directory / "orders.dbf", directory / "orders.dbf")
            with self.assertRaisesRegex(ValueError, "must use the same directory"):
                validate_order_dbf_paths(
                    directory / "header.dbf",
                    directory / "nested" / "detail.dbf",
                )
            with self.assertRaisesRegex(ValueError, "must not overwrite native"):
                validate_order_dbf_paths(directory / "Ordhdr.dbf", directory / "detail.dbf")

    def test_stable_dbf_keys_are_never_silently_truncated(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            with self.assertRaisesRegex(ValueError, "ORDER_ID.*24 bytes"):
                upsert_order_changes(
                    directory / "shopify-order-header.dbf",
                    directory / "shopify-order-detail.dbf",
                    [
                        {
                            "id": 1,
                            "version": 1,
                            "shopify_order_id": "1" * 25,
                            "event_topic": "orders/create",
                            "order": {"id": "1" * 25, "line_items": []},
                        }
                    ],
                    retention_rows=100,
                )

    def test_empty_sync_creates_genuine_header_and_detail_dbfs(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            header_path = Path(temporary_directory) / "shopify-order-header.dbf"
            detail_path = Path(temporary_directory) / "shopify-order-detail.dbf"

            upsert_order_changes(header_path, detail_path, [], retention_rows=100)

            headers, details = read_order_dbfs(header_path, detail_path)
            self.assertEqual(headers, [])
            self.assertEqual(details, [])
            for path, schema in (
                (header_path, HEADER_FIELDS),
                (detail_path, DETAIL_FIELDS),
            ):
                content = path.read_bytes()
                self.assertEqual(content[0], 0x03)
                self.assertNotEqual(content[:16], b"SQLite format 3\x00")
                self.assertEqual(struct.unpack("<I", content[4:8])[0], 0)
                self.assertEqual(
                    struct.unpack("<H", content[8:10])[0],
                    33 + (32 * len(schema)),
                )
                self.assertEqual(
                    struct.unpack("<H", content[10:12])[0],
                    1 + sum(field.length for field in schema),
                )
                self.assertEqual(content[29], 0x03)
                for index, field in enumerate(schema):
                    offset = 32 + (index * 32)
                    descriptor = content[offset : offset + 32]
                    name = descriptor[:11].split(b"\x00", 1)[0].decode("ascii")
                    self.assertEqual(name, field.name)
                    self.assertEqual(chr(descriptor[11]), field.field_type)
                    self.assertEqual(descriptor[16], field.length)
                    self.assertEqual(descriptor[17], field.decimals)
                self.assertEqual(content[32 + (32 * len(schema))], 0x0D)
                self.assertEqual(content[-1], 0x1A)
            self.assertTrue(header_path.with_suffix(".lock").exists())
            self.assertTrue(all(len(field.name) <= 10 for field in HEADER_FIELDS + DETAIL_FIELDS))

    def test_connector_acks_only_after_publishing_both_dbfs(self):
        class Response:
            def __init__(self, body):
                self.body = body

            def raise_for_status(self):
                return None

            def json(self):
                return self.body

        class Session:
            def __init__(self, change):
                self.change = change
                self.posts = []

            def get(self, url, **kwargs):
                if url.endswith("/sync/orders/status"):
                    return Response({"read_orders_authorized": True})
                return Response({"items": [self.change]})

            def post(self, url, **kwargs):
                self.posts.append((url, kwargs))
                return Response({"acknowledged": 1})

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            change = {
                "id": 17,
                "version": 4,
                "shopify_order_id": "1001",
                "event_topic": "orders/create",
                "order": {"id": "1001", "name": "#1001", "line_items": []},
            }
            connector = Connector.__new__(Connector)
            connector.dry_run = False
            connector.order_dbfs_initialized = False
            connector.order_header_path = directory / "shopify-order-header.dbf"
            connector.order_detail_path = directory / "shopify-order-detail.dbf"
            connector.legacy_order_db_paths = []
            connector.order_retention_rows = 100
            connector.order_bridge_status_checked = False
            connector.base_url = "https://sync.example"
            connector.timeout = 30
            connector.logger = mock.Mock()
            connector.session = Session(change)
            connector.last_order_poll_monotonic = 0.0

            connector._sync_order_inbox()

            headers, details = read_order_dbfs(
                connector.order_header_path,
                connector.order_detail_path,
            )
            self.assertEqual(headers[0]["ORDER_ID"], "1001")
            self.assertEqual(details, [])
            self.assertEqual(len(connector.session.posts), 1)
            self.assertEqual(
                connector.session.posts[0][1]["json"],
                {"changes": [{"id": 17, "version": 4}]},
            )

    def test_connector_dry_run_creates_no_dbfs_and_sends_no_ack(self):
        class Response:
            def __init__(self, body):
                self.body = body

            def raise_for_status(self):
                return None

            def json(self):
                return self.body

        class Session:
            def __init__(self):
                self.posts = []

            def get(self, url, **kwargs):
                if url.endswith("/sync/orders/status"):
                    return Response({})
                return Response(
                    {
                        "items": [
                            {
                                "id": 17,
                                "version": 4,
                                "shopify_order_id": "1001",
                                "order": {"id": "1001"},
                            }
                        ]
                    }
                )

            def post(self, url, **kwargs):
                self.posts.append((url, kwargs))
                return Response({})

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            connector = Connector.__new__(Connector)
            connector.dry_run = True
            connector.order_dbfs_initialized = False
            connector.order_header_path = directory / "shopify-order-header.dbf"
            connector.order_detail_path = directory / "shopify-order-detail.dbf"
            connector.legacy_order_db_paths = []
            connector.order_retention_rows = 100
            connector.order_bridge_status_checked = False
            connector.base_url = "https://sync.example"
            connector.timeout = 30
            connector.logger = mock.Mock()
            connector.session = Session()
            connector.last_order_poll_monotonic = 0.0

            connector._sync_order_inbox()

            self.assertFalse(connector.order_header_path.exists())
            self.assertFalse(connector.order_detail_path.exists())
            self.assertEqual(connector.session.posts, [])

    def test_retention_prunes_headers_and_matching_details_together(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            changes = []
            for index in range(101):
                order_id = str(1000 + index)
                changes.append(
                    {
                        "id": index + 1,
                        "version": 1,
                        "shopify_order_id": order_id,
                        "event_topic": "orders/create",
                        "order": {
                            "id": order_id,
                            "name": f"#{order_id}",
                            "created_at": "2026-01-01T00:00:00Z",
                            "line_items": [
                                {
                                    "id": 5000 + index,
                                    "sku": f"SKU-{index}",
                                    "quantity": 1,
                                    "price": "1.00",
                                }
                            ],
                        },
                    }
                )

            delivered_order_ids = upsert_order_changes(
                header_path,
                detail_path,
                changes,
                retention_rows=1,
            )

            headers, details = read_order_dbfs(header_path, detail_path)
            header_ids = {str(row["ORDER_ID"]) for row in headers}
            self.assertEqual(len(headers), 100)
            self.assertEqual(len(details), 100)
            self.assertIn("1000", header_ids)
            self.assertNotIn("1100", header_ids)
            self.assertIn("1000", delivered_order_ids)
            self.assertNotIn("1100", delivered_order_ids)
            self.assertEqual({str(row["ORDER_ID"]) for row in details}, header_ids)

            for header in headers:
                if header["ORDER_ID"] == "1000":
                    header["IMPORT_ST"] = "IMPORTED"
            write_order_dbfs(header_path, detail_path, headers, details)
            retry_delivered_ids = upsert_order_changes(
                header_path,
                detail_path,
                [changes[-1]],
                retention_rows=100,
            )
            headers, details = read_order_dbfs(header_path, detail_path)
            header_ids = {str(row["ORDER_ID"]) for row in headers}
            self.assertIn("1100", retry_delivered_ids)
            self.assertIn("1100", header_ids)
            self.assertNotIn("1000", header_ids)
            self.assertEqual({str(row["ORDER_ID"]) for row in details}, header_ids)

    def test_connector_does_not_ack_a_change_deferred_by_local_capacity(self):
        class Response:
            def __init__(self, body):
                self.body = body

            def raise_for_status(self):
                return None

            def json(self):
                return self.body

        class Session:
            def __init__(self, change):
                self.change = change
                self.posts = []

            def get(self, url, **kwargs):
                return Response({"items": [self.change]})

            def post(self, url, **kwargs):
                self.posts.append((url, kwargs))
                return Response({"acknowledged": 1})

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            existing_changes = [
                {
                    "id": index + 1,
                    "version": 1,
                    "shopify_order_id": str(1000 + index),
                    "event_topic": "orders/create",
                    "order": {
                        "id": str(1000 + index),
                        "name": f"#{1000 + index}",
                        "created_at": "2026-01-01T00:00:00Z",
                        "line_items": [],
                    },
                }
                for index in range(100)
            ]
            upsert_order_changes(
                header_path,
                detail_path,
                existing_changes,
                retention_rows=100,
            )
            deferred_change = {
                "id": 101,
                "version": 1,
                "shopify_order_id": "1100",
                "event_topic": "orders/create",
                "order": {
                    "id": "1100",
                    "name": "#1100",
                    "created_at": "2020-01-01T00:00:00Z",
                    "line_items": [],
                },
            }
            connector = Connector.__new__(Connector)
            connector.dry_run = False
            connector.order_dbfs_initialized = True
            connector.order_header_path = header_path
            connector.order_detail_path = detail_path
            connector.legacy_order_db_paths = []
            connector.order_retention_rows = 100
            connector.order_bridge_status_checked = True
            connector.base_url = "https://sync.example"
            connector.timeout = 30
            connector.logger = mock.Mock()
            connector.session = Session(deferred_change)
            connector.last_order_poll_monotonic = 0.0

            connector._sync_order_inbox()

            headers, _ = read_order_dbfs(header_path, detail_path)
            header_ids = {str(row["ORDER_ID"]) for row in headers}
            self.assertEqual(len(header_ids), 100)
            self.assertIn("1099", header_ids)
            self.assertNotIn("1100", header_ids)
            self.assertEqual(connector.session.posts, [])
            connector.logger.warning.assert_called_once_with(
                "order_inbox_capacity_deferred changes=%s retention=%s",
                1,
                100,
            )

    def test_cp1252_text_round_trips_and_unrepresentable_text_is_flagged(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            upsert_order_changes(
                header_path,
                detail_path,
                [
                    {
                        "id": 1,
                        "version": 1,
                        "shopify_order_id": "1001",
                        "event_topic": "orders/create",
                        "order": {
                            "id": "1001",
                            "name": "#1001",
                            "customer_first_name": "José",
                            "note": "Gift 🙂 " + ("x" * 247) + "  tail" + ("y" * 600),
                            "line_items": [
                                {
                                    "id": 501,
                                    "sku": "ABC",
                                    "title": "Men’s Shirt",
                                    "quantity": 1,
                                    "price": "10.00",
                                }
                            ],
                        },
                    }
                ],
                retention_rows=100,
            )

            headers, details = read_order_dbfs(header_path, detail_path)
            self.assertEqual(headers[0]["CUST_FIRST"], "José")
            self.assertEqual(details[0]["DESCRIPT"], "Men’s Shirt")
            self.assertIn("?", (headers[0]["NOTE1"] or "") + (headers[0]["NOTE2"] or ""))
            self.assertTrue((headers[0]["NOTE2"] or "").startswith("  tail"))
            self.assertTrue(headers[0]["TRUNCATED"])
            self.assertFalse(details[0]["TRUNCATED"])

    def test_header_money_rounding_is_flagged(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            upsert_order_changes(
                header_path,
                detail_path,
                [
                    {
                        "id": 1,
                        "version": 1,
                        "shopify_order_id": "1001",
                        "event_topic": "orders/create",
                        "order": {
                            "id": "1001",
                            "name": "#1001",
                            "currency": "KWD",
                            "subtotal_price": "1.005",
                            "total_price": "1.005",
                            "line_items": [],
                        },
                    }
                ],
                retention_rows=100,
            )

            headers, _ = read_order_dbfs(header_path, detail_path)
            self.assertEqual(headers[0]["SUBTOTAL"], Decimal("1.00"))
            self.assertEqual(headers[0]["TOTAL"], Decimal("1.00"))
            self.assertTrue(headers[0]["TRUNCATED"])

    def test_malformed_line_tax_is_coerced_and_flagged(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            upsert_order_changes(
                header_path,
                detail_path,
                [
                    {
                        "id": 1,
                        "version": 1,
                        "shopify_order_id": "1001",
                        "event_topic": "orders/create",
                        "order": {
                            "id": "1001",
                            "name": "#1001",
                            "line_items": [
                                {
                                    "id": "501",
                                    "quantity": 1,
                                    "price": "10.00",
                                    "tax_lines": [{"price": "invalid"}],
                                }
                            ],
                        },
                    }
                ],
                retention_rows=100,
            )

            _, details = read_order_dbfs(header_path, detail_path)
            self.assertEqual(details[0]["TAX"], Decimal("0.00"))
            self.assertTrue(details[0]["TRUNCATED"])

    def test_dbf_record_count_cannot_hide_physical_order_rows(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            upsert_order_changes(
                header_path,
                detail_path,
                [
                    {
                        "id": 1,
                        "version": 1,
                        "shopify_order_id": "1001",
                        "event_topic": "orders/create",
                        "order": {"id": "1001", "name": "#1001", "line_items": []},
                    }
                ],
                retention_rows=100,
            )
            corrupted = bytearray(header_path.read_bytes())
            corrupted[4:8] = struct.pack("<I", 0)
            header_path.write_bytes(corrupted)

            with self.assertRaisesRegex(ValueError, "record count or end marker"):
                read_order_dbfs(header_path, detail_path)

    def test_legacy_sqlite_orders_are_migrated_without_deleting_the_source(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            legacy_path = directory / "shopify-order.db"
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            with sqlite3.connect(legacy_path) as connection:
                connection.executescript(
                    """
                    CREATE TABLE orders (
                        shopify_order_id TEXT PRIMARY KEY,
                        order_name TEXT,
                        created_at TEXT,
                        total_price TEXT,
                        print_status TEXT,
                        printed_at TEXT,
                        import_status TEXT,
                        imported_at TEXT,
                        pos_order_number TEXT,
                        import_error TEXT,
                        source_event TEXT,
                        source_version INTEGER,
                        synced_at TEXT
                    );
                    CREATE TABLE order_items (
                        shopify_order_id TEXT,
                        line_key TEXT,
                        shopify_line_item_id TEXT,
                        sku TEXT,
                        quantity INTEGER,
                        price TEXT,
                        total_discount TEXT
                    );
                    INSERT INTO orders VALUES (
                        '1001', '#1001', '2026-07-22T12:00:00-07:00', '21.00',
                        'PRINTED', '2026-07-22T12:01:00-07:00',
                        'IMPORTED', '2026-07-22T12:02:00-07:00',
                        'POS-1', NULL, 'orders/create', 3, '2026-07-22T12:00:05-07:00'
                    );
                    INSERT INTO order_items VALUES (
                        '1001', '501', '501', 'ABC', 1, '21.00', '0.00'
                    );
                    INSERT INTO order_items VALUES (
                        '1001', '502', '502', 'XYZ', 2, '10.00', '1.00'
                    );
                    """
                )

            migrated = migrate_legacy_sqlite_database(
                legacy_path,
                header_path,
                detail_path,
                retention_rows=100,
            )

            headers, details = read_order_dbfs(header_path, detail_path)
            self.assertTrue(migrated)
            self.assertTrue(legacy_path.exists())
            self.assertEqual(headers[0]["ORDER_ID"], "1001")
            self.assertEqual(headers[0]["PRINT_ST"], "PRINTED")
            self.assertEqual(headers[0]["IMPORT_ST"], "IMPORTED")
            self.assertEqual(headers[0]["POS_ORD_NO"], "POS-1")
            self.assertEqual(details[0]["SKU"], "ABC")
            self.assertEqual([row["LINE_NO"] for row in details], [Decimal("1"), Decimal("2")])
            self.assertEqual(
                [row["EXTENSION"] for row in details],
                [Decimal("21.00"), Decimal("19.00")],
            )

    def test_removal_changes_also_scrub_the_legacy_sqlite_copy(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            legacy_path = directory / "shopify-order.db"
            with sqlite3.connect(legacy_path) as connection:
                connection.executescript(
                    """
                    CREATE TABLE orders (shopify_order_id TEXT PRIMARY KEY, email TEXT);
                    CREATE TABLE order_items (shopify_order_id TEXT, line_key TEXT);
                    INSERT INTO orders VALUES ('1001', 'customer@example.com');
                    INSERT INTO order_items VALUES ('1001', '501');
                    """
                )

            removed = remove_orders_from_legacy_sqlite(
                [legacy_path],
                [
                    {
                        "shopify_order_id": "1001",
                        "event_topic": "customers/redact",
                        "order": {"id": "1001", "redacted": True},
                    }
                ],
            )

            with sqlite3.connect(legacy_path) as connection:
                order_count = connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
                item_count = connection.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
            self.assertEqual(removed, 1)
            self.assertEqual(order_count, 0)
            self.assertEqual(item_count, 0)
            self.assertNotIn(b"customer@example.com", legacy_path.read_bytes())
            self.assertFalse(Path(f"{legacy_path}-wal").exists())
            self.assertFalse(Path(f"{legacy_path}-journal").exists())

    def test_custom_header_lock_blocks_a_second_process(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "orders.dbf"
            detail_path = directory / "lines.dbf"
            acquired_path = directory / "child-acquired"
            child_code = """
import sys
from pathlib import Path
from windows_connector.order_dbf import order_dbf_lock

print('started', flush=True)
with order_dbf_lock(Path(sys.argv[1]), Path(sys.argv[2])):
    Path(sys.argv[3]).write_text('acquired', encoding='utf-8')
"""
            with order_dbf_lock(header_path, detail_path) as lock_path:
                process = subprocess.Popen(
                    [
                        sys.executable,
                        "-c",
                        child_code,
                        str(header_path),
                        str(detail_path),
                        str(acquired_path),
                    ],
                    cwd=Path(__file__).resolve().parents[1],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                self.assertEqual(process.stdout.readline().strip(), "started")
                self.assertEqual(lock_path, directory / "orders.lock")
                self.assertFalse(acquired_path.exists())

            _, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertEqual(acquired_path.read_text(encoding="utf-8"), "acquired")

    def test_connector_recovers_partial_initial_publish_before_legacy_migration(self):
        import hashlib
        import json

        from windows_connector import order_dbf

        class Response:
            def __init__(self, body):
                self.body = body

            def raise_for_status(self):
                return None

            def json(self):
                return self.body

        class Session:
            def get(self, url, **kwargs):
                if url.endswith("/sync/orders/status"):
                    return Response({})
                return Response({"items": []})

            def post(self, url, **kwargs):
                raise AssertionError("an empty recovery poll must not acknowledge changes")

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            legacy_path = directory / "shopify-order.db"
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            with sqlite3.connect(legacy_path) as connection:
                connection.execute(
                    "CREATE TABLE orders (shopify_order_id TEXT PRIMARY KEY, order_name TEXT)"
                )
                connection.execute("INSERT INTO orders VALUES ('1001', '#1001')")
            self.assertTrue(
                migrate_legacy_sqlite_database(
                    legacy_path,
                    header_path,
                    detail_path,
                    retention_rows=100,
                )
            )
            headers, _ = read_order_dbfs(header_path, detail_path)
            header_content = header_path.read_bytes()
            detail_content = detail_path.read_bytes()
            _, _, journal_path = order_dbf._publish_paths(header_path, detail_path)
            header_path.unlink()
            journal_path.write_text(
                json.dumps(
                    {
                        "generation": headers[0]["GEN_ID"],
                        "header_existed": False,
                        "detail_existed": False,
                        "header_sha256": hashlib.sha256(header_content).hexdigest(),
                        "detail_sha256": hashlib.sha256(detail_content).hexdigest(),
                        "previous_header_sha256": None,
                        "previous_detail_sha256": None,
                    }
                ),
                encoding="utf-8",
            )

            connector = Connector.__new__(Connector)
            connector.dry_run = False
            connector.order_dbfs_initialized = False
            connector.order_header_path = header_path
            connector.order_detail_path = detail_path
            connector.legacy_order_db_paths = [legacy_path]
            connector.order_retention_rows = 100
            connector.order_bridge_status_checked = False
            connector.base_url = "https://sync.example"
            connector.timeout = 30
            connector.logger = mock.Mock()
            connector.session = Session()
            connector.last_order_poll_monotonic = 0.0

            connector._sync_order_inbox()

            recovered_headers, recovered_details = read_order_dbfs(header_path, detail_path)
            self.assertEqual([row["ORDER_ID"] for row in recovered_headers], ["1001"])
            self.assertEqual(recovered_details, [])
            self.assertFalse(journal_path.exists())

    def test_recovery_removes_orphan_publish_temp_files(self):
        from windows_connector import order_dbf

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            upsert_order_changes(header_path, detail_path, [], retention_rows=100)
            previous_header, previous_detail, journal_path = order_dbf._publish_paths(
                header_path,
                detail_path,
            )
            orphan_paths = [
                target.parent / f".{target.name}.abandoned.tmp"
                for target in (
                    header_path,
                    detail_path,
                    previous_header,
                    previous_detail,
                    journal_path,
                )
            ]
            for orphan_path in orphan_paths:
                orphan_path.write_bytes(b"old customer data")

            read_order_dbfs(header_path, detail_path)

            self.assertTrue(all(not path.exists() for path in orphan_paths))

    def test_failed_second_file_replace_restores_the_previous_pair(self):
        from windows_connector import order_dbf

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            base_change = {
                "id": 1,
                "version": 1,
                "shopify_order_id": "1001",
                "event_topic": "orders/create",
                "order": {
                    "id": "1001",
                    "name": "#1001",
                    "created_at": "2026-07-22T12:00:00-07:00",
                    "total_price": "10.00",
                    "line_items": [{"id": 1, "sku": "ABC", "quantity": 1, "price": "10.00"}],
                },
            }
            upsert_order_changes(
                header_path,
                detail_path,
                [base_change],
                retention_rows=100,
            )
            original_header = header_path.read_bytes()
            original_detail = detail_path.read_bytes()
            real_replace = order_dbf.os.replace
            failed = False

            def fail_header_once(source, destination):
                nonlocal failed
                if Path(destination) == header_path and Path(source).suffix == ".tmp" and not failed:
                    failed = True
                    raise OSError("simulated header publish failure")
                return real_replace(source, destination)

            updated_change = {
                **base_change,
                "version": 2,
                "order": {**base_change["order"], "total_price": "12.00"},
            }
            with mock.patch.object(order_dbf.os, "replace", side_effect=fail_header_once):
                with self.assertRaisesRegex(OSError, "simulated header publish failure"):
                    upsert_order_changes(
                        header_path,
                        detail_path,
                        [updated_change],
                        retention_rows=100,
                    )

            self.assertEqual(header_path.read_bytes(), original_header)
            self.assertEqual(detail_path.read_bytes(), original_detail)
            headers, details = read_order_dbfs(header_path, detail_path)
            self.assertEqual(headers[0]["TOTAL"], Decimal("10.00"))
            self.assertEqual(details[0]["SKU"], "ABC")

    def test_failed_detail_temp_creation_cleans_the_header_temp(self):
        from windows_connector import order_dbf

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            upsert_order_changes(header_path, detail_path, [], retention_rows=100)
            original_header = header_path.read_bytes()
            original_detail = detail_path.read_bytes()
            real_write_temp = order_dbf._write_temp_file
            calls = 0

            def fail_second_temp(path, content):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("simulated detail temp failure")
                return real_write_temp(path, content)

            with mock.patch.object(order_dbf, "_write_temp_file", side_effect=fail_second_temp):
                with self.assertRaisesRegex(OSError, "simulated detail temp failure"):
                    upsert_order_changes(
                        header_path,
                        detail_path,
                        [],
                        retention_rows=100,
                    )

            self.assertEqual(header_path.read_bytes(), original_header)
            self.assertEqual(detail_path.read_bytes(), original_detail)
            self.assertEqual(list(directory.glob(".*.tmp")), [])

    def test_recovery_keeps_a_fully_published_journal_generation(self):
        import hashlib
        import json

        from windows_connector import order_dbf

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            header_path = directory / "shopify-order-header.dbf"
            detail_path = directory / "shopify-order-detail.dbf"
            change = {
                "id": 1,
                "version": 1,
                "shopify_order_id": "1001",
                "event_topic": "orders/create",
                "order": {
                    "id": "1001",
                    "name": "#1001",
                    "total_price": "10.00",
                    "line_items": [{"id": 1, "sku": "ABC", "quantity": 1, "price": "10.00"}],
                },
            }
            upsert_order_changes(
                header_path,
                detail_path,
                [change],
                retention_rows=100,
            )
            old_header = header_path.read_bytes()
            old_detail = detail_path.read_bytes()
            upsert_order_changes(
                header_path,
                detail_path,
                [{**change, "version": 2, "order": {**change["order"], "total_price": "12.00"}}],
                retention_rows=100,
            )
            new_header = header_path.read_bytes()
            new_detail = detail_path.read_bytes()
            headers, _ = read_order_dbfs(header_path, detail_path)
            previous_header, previous_detail, journal_path = order_dbf._publish_paths(
                header_path,
                detail_path,
            )
            previous_header.write_bytes(old_header)
            previous_detail.write_bytes(old_detail)
            journal_path.write_text(
                json.dumps(
                    {
                        "generation": headers[0]["GEN_ID"],
                        "header_existed": True,
                        "detail_existed": True,
                        "header_sha256": hashlib.sha256(new_header).hexdigest(),
                        "detail_sha256": hashlib.sha256(new_detail).hexdigest(),
                    }
                ),
                encoding="utf-8",
            )

            recovered_headers, _ = read_order_dbfs(header_path, detail_path)

            self.assertEqual(recovered_headers[0]["TOTAL"], Decimal("12.00"))
            self.assertFalse(journal_path.exists())
            self.assertFalse(previous_header.exists())
            self.assertFalse(previous_detail.exists())

    def test_order_and_lines_are_upserted_without_changing_print_status(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            header_path = Path(temporary_directory) / "shopify-order-header.dbf"
            detail_path = Path(temporary_directory) / "shopify-order-detail.dbf"
            base_change = {
                "id": 1,
                "version": 1,
                "shopify_order_id": "1001",
                "order_name": "#1001",
                "event_topic": "orders/create",
                "order": {
                    "id": 1001,
                    "name": "#1001",
                    "created_at": "2026-07-22T12:00:00-07:00",
                    "financial_status": "paid",
                    "currency": "USD",
                    "subtotal_price": "42.00",
                    "total_discounts": "2.00",
                    "shipping_price": "8.00",
                    "total_tax": "4.00",
                    "total_price": "42.00",
                    "customer_first_name": "Ada",
                    "customer_last_name": "Lovelace",
                    "email": "ada@example.com",
                    "phone": "+15555550100",
                    "billing_address": {
                        "name": "Ada Lovelace",
                        "address1": "456 Billing Ave",
                        "city": "Los Angeles",
                        "province_code": "CA",
                        "zip": "90002",
                    },
                    "shipping_address": {
                        "name": "Ada Lovelace",
                        "address1": "123 Main St",
                        "city": "Los Angeles",
                        "province_code": "CA",
                        "zip": "90001",
                    },
                    "line_items": [
                        {
                            "id": 501,
                            "product_id": 601,
                            "variant_id": 701,
                            "sku": "ABC",
                            "title": "Shirt",
                            "variant_title": "Blue / Medium",
                            "quantity": 2,
                            "price": "21.00",
                            "total_discount": "2.00",
                            "tax_lines": [{"price": "3.20"}],
                        }
                    ],
                },
            }
            upsert_order_changes(
                header_path,
                detail_path,
                [base_change],
                retention_rows=100,
            )
            headers, details = read_order_dbfs(header_path, detail_path)
            headers[0].update(
                {
                    "PRINT_ST": "PRINTED",
                    "PRINTED_AT": "2026-07-22T12:01:00-07:00",
                    "IMPORT_ST": "IMPORTED",
                    "IMPORTEDAT": "2026-07-22T12:03:00-07:00",
                    "POS_ORD_NO": "POS-77",
                    "IMP_ERROR": "reviewed",
                }
            )
            write_order_dbfs(header_path, detail_path, headers, details)

            updated_change = {
                **base_change,
                "version": 2,
                "event_topic": "orders/updated",
                "order": {
                    **base_change["order"],
                    "updated_at": "2026-07-22T12:02:00-07:00",
                    "total_price": "45.00",
                },
            }
            upsert_order_changes(
                header_path,
                detail_path,
                [updated_change],
                retention_rows=100,
            )

            headers, details = read_order_dbfs(header_path, detail_path)
            header = headers[0]
            detail = details[0]
            self.assertEqual(header["TOTAL"], Decimal("45.00"))
            self.assertEqual(header["PRINT_ST"], "PRINTED")
            self.assertEqual(header["PRINTED_AT"], "2026-07-22T12:01:00-07:00")
            self.assertEqual(header["IMPORT_ST"], "IMPORTED")
            self.assertEqual(header["IMPORTEDAT"], "2026-07-22T12:03:00-07:00")
            self.assertEqual(header["POS_ORD_NO"], "POS-77")
            self.assertEqual(header["IMP_ERROR"], "reviewed")
            self.assertEqual(header["SRC_VER"], Decimal("2"))
            self.assertEqual(header["BILL_ADR1"], "456 Billing Ave")
            self.assertEqual(header["SHIP_ADR1"], "123 Main St")
            self.assertEqual(len(details), 1)
            self.assertEqual(detail["SKU"], "ABC")
            self.assertEqual(detail["EXTENSION"], Decimal("40.00"))
            self.assertEqual(detail["TAX"], Decimal("3.20"))
            self.assertEqual(header["INVOICE_NO"], "#1001")
            self.assertEqual(header["EMAIL"], "ada@example.com")
            self.assertEqual(header["SHIPPING"], Decimal("8.00"))
            self.assertEqual(detail["QTY"], Decimal("2"))
            self.assertEqual(header["GEN_ID"], detail["GEN_ID"])
            self.assertEqual(header["LINE_COUNT"], Decimal("1"))

            upsert_order_changes(
                header_path,
                detail_path,
                [
                    {
                        "id": 2,
                        "version": 3,
                        "shopify_order_id": "1001",
                        "event_topic": "orders/delete",
                        "order": {"id": 1001},
                    }
                ],
                retention_rows=100,
            )
            headers, details = read_order_dbfs(header_path, detail_path)
            self.assertEqual(headers, [])
            self.assertEqual(details, [])


class InventoryAdjustmentTests(unittest.TestCase):
    def test_adjustment_uses_the_variant_inventory_item_and_location(self):
        class FakeShopifyClient:
            def __init__(self):
                self.adjustment = None

            def get_variant_by_sku(self, shop_domain, access_token, sku, *, force_refresh=False):
                self.force_refresh = force_refresh
                return VariantMapping(
                    sku=sku,
                    variant_id="gid://shopify/ProductVariant/1",
                    product_id="gid://shopify/Product/2",
                    inventory_item_id="gid://shopify/InventoryItem/3",
                    inventory_levels=[
                        InventoryLevelSnapshot(
                            location_id="gid://shopify/Location/4",
                            location_name="Store",
                            quantity=10,
                        )
                    ],
                )

            def get_primary_location_id(self, shop_domain, access_token):
                return "gid://shopify/Location/4"

            def adjust_inventory(self, *args, **kwargs):
                self.adjustment = (args, kwargs)
                return 9

        client = FakeShopifyClient()
        service = InventorySyncService(
            client,
            SimpleNamespace(shopify_location_id=None),
            None,
        )
        result = service.adjust_inventory_quantity(
            sku="ABC",
            delta=-1,
            idempotency_key="stable-key",
            shop=ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["delta"], -1)
        self.assertTrue(client.force_refresh)
        self.assertEqual(result["quantity_after_change"], 9)
        args, kwargs = client.adjustment
        self.assertEqual(args[2], "gid://shopify/InventoryItem/3")
        self.assertEqual(args[3], "gid://shopify/Location/4")
        self.assertEqual(args[4], -1)
        self.assertEqual(kwargs["idempotency_key"], "stable-key")

    def test_positive_adjustment_restores_only_an_auto_archived_product(self):
        class FakeShopifyClient:
            def __init__(self):
                self.lookups = 0
                self.product_update = None

            def get_variant_by_sku(self, shop_domain, access_token, sku, *, force_refresh=False):
                self.lookups += 1
                return VariantMapping(
                    sku=sku,
                    variant_id="gid://shopify/ProductVariant/1",
                    product_id="gid://shopify/Product/2",
                    inventory_item_id="gid://shopify/InventoryItem/3",
                    product_status="ARCHIVED",
                    auto_archived_zero_stock=True,
                    inventory_levels=[
                        InventoryLevelSnapshot(
                            location_id="gid://shopify/Location/4",
                            location_name="Store",
                            quantity=0 if self.lookups == 1 else 6,
                        )
                    ],
                )

            def get_primary_location_id(self, shop_domain, access_token):
                return "gid://shopify/Location/4"

            def adjust_inventory(self, *args, **kwargs):
                return 6

            def update_product(self, shop_domain, access_token, *, product, media=None):
                self.product_update = product
                return {"id": product["id"], "status": product.get("status")}

        client = FakeShopifyClient()
        service = InventorySyncService(
            client,
            SimpleNamespace(shopify_location_id=None),
            None,
        )

        result = service.adjust_inventory_quantity(
            sku="22392",
            delta=6,
            idempotency_key="restock-key",
            shop=ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(client.lookups, 2)
        self.assertTrue(result["product_status_updated"])
        self.assertEqual(result["quantity_after_change"], 6)
        self.assertEqual(
            client.product_update,
            {
                "id": "gid://shopify/Product/2",
                "status": "DRAFT",
                "metafields": [
                    {
                        "namespace": "pos",
                        "key": "auto_archived_zero_stock",
                        "type": "boolean",
                        "value": "false",
                    }
                ],
            },
        )

    def test_positive_adjustment_preserves_a_manual_archive(self):
        class FakeShopifyClient:
            def __init__(self):
                self.product_updates = []

            def get_variant_by_sku(self, shop_domain, access_token, sku, *, force_refresh=False):
                return VariantMapping(
                    sku=sku,
                    variant_id="gid://shopify/ProductVariant/1",
                    product_id="gid://shopify/Product/2",
                    inventory_item_id="gid://shopify/InventoryItem/3",
                    product_status="ARCHIVED",
                    auto_archived_zero_stock=False,
                    inventory_levels=[
                        InventoryLevelSnapshot(
                            location_id="gid://shopify/Location/4",
                            location_name="Store",
                            quantity=6,
                        )
                    ],
                )

            def get_primary_location_id(self, shop_domain, access_token):
                return "gid://shopify/Location/4"

            def adjust_inventory(self, *args, **kwargs):
                return 6

            def update_product(self, *args, **kwargs):
                self.product_updates.append((args, kwargs))

        client = FakeShopifyClient()
        service = InventorySyncService(
            client,
            SimpleNamespace(shopify_location_id=None),
            None,
        )

        result = service.adjust_inventory_quantity(
            sku="MANUAL",
            delta=6,
            idempotency_key="manual-key",
            shop=ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertFalse(result["product_status_updated"])
        self.assertEqual(client.product_updates, [])

    def test_webhook_status_repair_retries_while_positive_quantity_is_not_visible(self):
        class FakeShopifyClient:
            def get_variant_by_sku(self, shop_domain, access_token, sku, *, force_refresh=False):
                return VariantMapping(
                    sku=sku,
                    variant_id="gid://shopify/ProductVariant/1",
                    product_id="gid://shopify/Product/2",
                    inventory_item_id="gid://shopify/InventoryItem/3",
                    product_status="ARCHIVED",
                    auto_archived_zero_stock=True,
                    inventory_levels=[
                        InventoryLevelSnapshot(
                            location_id="gid://shopify/Location/4",
                            location_name="Store",
                            quantity=0,
                        )
                    ],
                )

            def get_primary_location_id(self, shop_domain, access_token):
                return "gid://shopify/Location/4"

        service = InventorySyncService(
            FakeShopifyClient(),
            SimpleNamespace(shopify_location_id=None),
            None,
        )

        with self.assertRaises(SyncProcessingError) as raised:
            service.restore_auto_archived_product_by_sku(
                shop=ShopRecord(
                    shop_domain="example.myshopify.com",
                    access_token="token",
                ),
                sku="22392",
                expected_positive_quantity=True,
            )

        self.assertEqual(raised.exception.code, "inventory_quantity_not_ready")

    def test_explicit_catalog_archive_clears_existing_automatic_marker(self):
        class FakeShopifyClient:
            def __init__(self):
                self.product_updates = []

            def get_variant_by_sku(self, shop_domain, access_token, sku, *, force_refresh=False):
                return VariantMapping(
                    sku=sku,
                    variant_id="gid://shopify/ProductVariant/1",
                    product_id="gid://shopify/Product/2",
                    inventory_item_id="gid://shopify/InventoryItem/3",
                    product_status="ARCHIVED",
                    auto_archived_zero_stock=True,
                    inventory_levels=[
                        InventoryLevelSnapshot(
                            location_id="gid://shopify/Location/4",
                            location_name="Store",
                            quantity=0,
                        )
                    ],
                )

            def update_product(self, shop_domain, access_token, *, product, media=None):
                self.product_updates.append(product)
                return {"id": product["id"], "title": "Shirt", "status": "ARCHIVED"}

            def update_variant_fields(self, *args, **kwargs):
                return {}

            def get_primary_location_id(self, shop_domain, access_token):
                return "gid://shopify/Location/4"

            def update_cached_variant(self, *args, **kwargs):
                return None

        client = FakeShopifyClient()
        service = InventorySyncService(
            client,
            SimpleNamespace(shopify_location_id=None),
            None,
        )
        payload = service._apply_catalog_import_policy(
            service._normalize_payload(
                ProductSyncRequest(sku="MANUAL", quantity=0, status="archived")
            )
        )

        service._sync_catalog_product(
            payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
            manual_archive_requested=True,
        )

        self.assertEqual(client.product_updates[0]["status"], "ARCHIVED")
        self.assertEqual(
            client.product_updates[0]["metafields"],
            [
                {
                    "namespace": "pos",
                    "key": "auto_archived_zero_stock",
                    "type": "boolean",
                    "value": "false",
                }
            ],
        )

    def test_positive_catalog_sync_restores_an_automatically_archived_product(self):
        class FakeShopifyClient:
            def __init__(self):
                self.product_updates = []
                self.lookups = 0

            def get_variant_by_sku(self, shop_domain, access_token, sku, *, force_refresh=False):
                self.lookups += 1
                return VariantMapping(
                    sku=sku,
                    variant_id="gid://shopify/ProductVariant/1",
                    product_id="gid://shopify/Product/2",
                    inventory_item_id="gid://shopify/InventoryItem/3",
                    product_status="ARCHIVED",
                    auto_archived_zero_stock=True,
                    inventory_levels=[
                        InventoryLevelSnapshot(
                            location_id="gid://shopify/Location/4",
                            location_name="Store",
                            quantity=0 if self.lookups == 1 else 6,
                        )
                    ],
                )

            def update_product(self, shop_domain, access_token, *, product, media=None):
                self.product_updates.append(product)
                return {"id": product["id"], "title": "Shirt", "status": product.get("status")}

            def update_variant_fields(self, *args, **kwargs):
                return {}

            def get_primary_location_id(self, shop_domain, access_token):
                return "gid://shopify/Location/4"

            def update_inventory(self, *args, **kwargs):
                return None

            def update_cached_variant(self, *args, **kwargs):
                return None

        client = FakeShopifyClient()
        service = InventorySyncService(
            client,
            SimpleNamespace(shopify_location_id=None),
            None,
        )
        payload = service._apply_catalog_import_policy(
            service._normalize_payload(ProductSyncRequest(sku="RESTOCK", quantity=6))
        )

        result = service._sync_catalog_product(
            payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(result.details["product_status"], "DRAFT")
        self.assertEqual(
            client.product_updates[-1],
            {
                "id": "gid://shopify/Product/2",
                "status": "DRAFT",
                "metafields": [
                    {
                        "namespace": "pos",
                        "key": "auto_archived_zero_stock",
                        "type": "boolean",
                        "value": "false",
                    }
                ],
            },
        )

    def test_price_update_changes_only_variant_id_and_price(self):
        class FakeShopifyClient:
            def __init__(self):
                self.variant_update = None

            def get_variant_by_sku(self, shop_domain, access_token, sku, *, force_refresh=False):
                self.force_refresh = force_refresh
                return VariantMapping(
                    sku=sku,
                    variant_id="gid://shopify/ProductVariant/1",
                    product_id="gid://shopify/Product/2",
                    inventory_item_id="gid://shopify/InventoryItem/3",
                    current_price=49.0,
                )

            def update_variant_fields(self, *args, **kwargs):
                self.variant_update = (args, kwargs)
                return {}

        client = FakeShopifyClient()
        service = InventorySyncService(client, SimpleNamespace(), None)
        result = service.update_price_by_sku(
            sku="22301",
            price=59,
            shop=ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertTrue(result["success"])
        self.assertTrue(result["changed"])
        self.assertTrue(client.force_refresh)
        _args, kwargs = client.variant_update
        self.assertEqual(
            kwargs["variant"],
            {"id": "gid://shopify/ProductVariant/1", "price": "59.00"},
        )


class ArchiveStorageTests(unittest.TestCase):
    def test_upload_keeps_product_dbfs_and_discards_zip_and_customer_data(self):
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w") as archive:
            archive.writestr("ashpsdat/Item.dbf", b"product")
            archive.writestr("ashpsdat/Itemmqty.dbf", b"quantity")
            archive.writestr("ashpsdat/Customer.dbf", b"private")
            archive.writestr("ashpsdat/large-backup.bak", b"backup")
        payload.seek(0)
        upload = SimpleNamespace(file=payload)

        with tempfile.TemporaryDirectory() as temporary_directory:
            storage_root = Path(temporary_directory)
            root = save_uploaded_archive(upload, storage_root)

            self.assertTrue((root / "Item.dbf").exists())
            self.assertTrue((root / "Itemmqty.dbf").exists())
            self.assertFalse((root / "Customer.dbf").exists())
            self.assertFalse((root / "large-backup.bak").exists())
            self.assertFalse((storage_root / "ashpsdat.zip").exists())


if __name__ == "__main__":
    unittest.main()
