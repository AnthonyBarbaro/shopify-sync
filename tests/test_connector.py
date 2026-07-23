import io
import sqlite3
import struct
import tempfile
import unittest
import zipfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from app.db import DatabaseStore
from app.db import ShopRecord
from app.inventory import InventorySyncService
from app.models import InventoryLevelSnapshot, VariantMapping
from app.pos_archive import save_uploaded_archive
from windows_connector.connector import (
    adjustment_key,
    base_sku,
    catalog_total_quantity,
    catalog_upload_priority,
    dbf_record_count,
    flatten_quantities,
    iter_selected_dbf_rows,
    matrix_variant_sku_for_row,
    merge_quantity,
    nightly_full_sync_due,
    read_appended_dbf_rows,
    sku_base_mapping,
    upsert_order_changes,
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


class LocalOrderInboxTests(unittest.TestCase):
    def test_order_and_lines_are_upserted_without_changing_print_status(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "shopify-order.db"
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
                    "total_price": "42.00",
                    "customer_first_name": "Ada",
                    "customer_last_name": "Lovelace",
                    "shipping_address": {
                        "name": "Ada Lovelace",
                        "address1": "123 Main St",
                        "city": "Los Angeles",
                        "province_code": "CA",
                        "zip": "90001",
                    },
                    "line_items": [
                        {"id": 501, "sku": "ABC", "title": "Shirt", "quantity": 2, "price": "21.00"}
                    ],
                },
            }
            upsert_order_changes(database_path, [base_change], retention_rows=100)
            with sqlite3.connect(database_path) as connection:
                connection.execute(
                    "UPDATE orders SET print_status='PRINTED', printed_at='2026-07-22T12:01:00-07:00'"
                )
                connection.commit()

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
            upsert_order_changes(database_path, [updated_change], retention_rows=100)

            with sqlite3.connect(database_path) as connection:
                connection.row_factory = sqlite3.Row
                order = connection.execute("SELECT * FROM orders").fetchone()
                items = connection.execute("SELECT * FROM order_items").fetchall()
            self.assertEqual(order["total_price"], "45.00")
            self.assertEqual(order["print_status"], "PRINTED")
            self.assertEqual(order["source_version"], 2)
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["sku"], "ABC")

            upsert_order_changes(
                database_path,
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
            with sqlite3.connect(database_path) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM order_items").fetchone()[0], 0)


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
