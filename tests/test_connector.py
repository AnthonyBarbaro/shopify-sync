import io
import sqlite3
import struct
import tempfile
import unittest
import zipfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from app.db import DatabaseStore
from app.db import ShopRecord
from app.inventory import InventorySyncService
from app.models import InventoryLevelSnapshot, VariantMapping
from app.pos_archive import save_uploaded_archive
from app.shopify import ShopifyClient
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
    negative_catalog_money_field,
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
    def test_empty_sync_creates_header_and_detail_schema(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "shopify-orders.db"

            upsert_order_changes(database_path, [], retention_rows=100)

            with sqlite3.connect(database_path) as connection:
                objects = dict(
                    connection.execute(
                        "SELECT name, type FROM sqlite_master WHERE name IN "
                        "('orders', 'order_items', 'order_header', 'order_detail')"
                    ).fetchall()
                )
            self.assertEqual(
                objects,
                {
                    "orders": "table",
                    "order_items": "table",
                    "order_header": "view",
                    "order_detail": "view",
                },
            )

    def test_order_and_lines_are_upserted_without_changing_print_status(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "shopify-orders.db"
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
                header = connection.execute("SELECT * FROM order_header").fetchone()
                detail = connection.execute("SELECT * FROM order_detail").fetchone()
            self.assertEqual(order["total_price"], "45.00")
            self.assertEqual(order["print_status"], "PRINTED")
            self.assertEqual(order["import_status"], "PENDING")
            self.assertEqual(order["source_version"], 2)
            self.assertEqual(order["billing_address1"], "456 Billing Ave")
            self.assertEqual(order["shipping_address1"], "123 Main St")
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["sku"], "ABC")
            self.assertEqual(items[0]["line_total"], "40.00")
            self.assertEqual(items[0]["line_tax"], "3.20")
            self.assertEqual(header["invoice_no"], "#1001")
            self.assertEqual(header["email"], "ada@example.com")
            self.assertEqual(header["shipping"], "8.00")
            self.assertEqual(detail["qty"], 2)
            self.assertEqual(detail["extension"], "40.00")

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
