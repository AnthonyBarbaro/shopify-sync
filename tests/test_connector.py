import io
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
from app.models import InventoryLevelSnapshot, VariantMapping
from app.pos_archive import save_uploaded_archive
from app.shopify import ShopifyClient
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
    Connector,
    adjustment_key,
    base_sku,
    catalog_total_quantity,
    catalog_upload_priority,
    dbf_record_count,
    detect_price_changes,
    flatten_quantities,
    iter_selected_dbf_rows,
    matrix_variant_sku_for_row,
    matrix_length_repair_candidates,
    merge_quantity,
    negative_catalog_money_field,
    nightly_full_sync_due,
    numeric_sku_increases,
    read_appended_dbf_rows,
    sku_base_mapping,
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

            def get_variant_by_sku(self, shop_domain, access_token, sku):
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

            def adjust_inventory(self, *args, **kwargs):
                self.adjustment = (args, kwargs)

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
        args, kwargs = client.adjustment
        self.assertEqual(args[2], "gid://shopify/InventoryItem/3")
        self.assertEqual(args[3], "gid://shopify/Location/4")
        self.assertEqual(args[4], -1)
        self.assertEqual(kwargs["idempotency_key"], "stable-key")

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
