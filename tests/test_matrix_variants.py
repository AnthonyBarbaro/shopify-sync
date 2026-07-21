import sys
import threading
import time
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "jbarbaro_db"))

try:
    import dbf_pos_sync
except ModuleNotFoundError:
    dbf_pos_sync = None

from app.inventory import InventorySyncService
from app.db import ShopRecord
from app.models import ProductSyncRequest, SyncResult, VariantMapping
from app.utils import utc_now_iso


@unittest.skipIf(dbf_pos_sync is None, "requires the local POS connector source")
class MatrixPayloadTests(unittest.TestCase):
    def test_legacy_barcode_and_size_mapping_are_exact(self):
        definition = dbf_pos_sync.MatrixDefinition(
            row_headers=[""],
            column_headers=["30", "32", "44"],
            cells=[
                {"row": 1, "column": 1, "cell": "1 1", "quantity": 1, "barcode": "21741. 1 1"},
                {"row": 1, "column": 2, "cell": "1 2", "quantity": 0, "barcode": "21741. 1 2"},
                {"row": 1, "column": 3, "cell": "1 3", "quantity": 2, "barcode": "21741. 1 3"},
            ],
        )

        variants = dbf_pos_sync.build_matrix_variants(
            sku="21741",
            definition=definition,
            price=Decimal("145.00"),
            compare_at_price=None,
            cost=Decimal("38.00"),
        )

        self.assertEqual(variants[0]["sku"], "21741. 1 1")
        self.assertEqual(variants[0]["barcode"], "21741. 1 1")
        self.assertEqual(variants[0]["option_values"], {"Size": "30"})
        self.assertEqual(variants[0]["quantity"], 1)
        self.assertEqual(variants[2]["option_values"], {"Size": "44"})

    def test_compact_dbf_cell_for_column_ten_is_parsed(self):
        self.assertEqual(
            dbf_pos_sync.parse_matrix_cell("110", row_count=1, column_count=11),
            (1, 10),
        )
        self.assertEqual(dbf_pos_sync.format_matrix_barcode("21741", 1, 10), "21741. 1 10")


class MatrixShopifyInputTests(unittest.TestCase):
    def setUp(self):
        self.service = InventorySyncService(None, None, None)
        self.payload = ProductSyncRequest.model_validate(
            {
                "sku": "21741",
                "title": "Ballin Casual Pants Bertini M2126/018",
                "price": 145,
                "cost": 38,
                "quantity": 1,
                "variants": [
                    {
                        "sku": "21741. 1 1",
                        "barcode": "21741. 1 1",
                        "option_values": {"Size": "30"},
                        "price": 145,
                        "cost": 38,
                        "quantity": 1,
                    },
                    {
                        "sku": "21741. 1 2",
                        "barcode": "21741. 1 2",
                        "option_values": {"Size": "32"},
                        "price": 145,
                        "cost": 38,
                        "quantity": 0,
                    },
                ],
            }
        )

    def test_existing_default_variant_is_reused_for_first_matrix_cell(self):
        existing_product = {
            "id": "gid://shopify/Product/99",
            "variants": {
                "nodes": [
                    {
                        "id": "gid://shopify/ProductVariant/100",
                        "sku": "21741",
                    }
                ]
            },
        }

        product_input = self.service._build_matrix_product_set_input(
            self.payload,
            location_id="gid://shopify/Location/7",
            media_inputs=[],
            existing_product=existing_product,
        )

        self.assertEqual(
            product_input["productOptions"],
            [{"name": "Size", "position": 1, "values": [{"name": "30"}, {"name": "32"}]}],
        )
        first = product_input["variants"][0]
        self.assertEqual(first["id"], "gid://shopify/ProductVariant/100")
        self.assertEqual(first["sku"], "21741. 1 1")
        self.assertEqual(first["barcode"], "21741. 1 1")
        self.assertEqual(first["inventoryItem"]["sku"], "21741. 1 1")
        self.assertEqual(first["inventoryQuantities"][0]["quantity"], 1)
        self.assertNotIn("id", product_input["variants"][1])

    def test_existing_single_variant_product_is_converted_idempotently(self):
        class FakeShopifyClient:
            def __init__(self):
                self.product_set_input = None

            def get_variant_by_sku(self, shop_domain, access_token, sku):
                if sku != "21741":
                    raise AssertionError(f"Unexpected lookup: {sku}")
                return VariantMapping(
                    sku="21741",
                    variant_id="gid://shopify/ProductVariant/100",
                    product_id="gid://shopify/Product/99",
                    inventory_item_id="gid://shopify/InventoryItem/200",
                    current_price=145,
                    current_cost=38,
                    inventory_levels=[],
                )

            def get_primary_location_id(self, shop_domain, access_token):
                return "gid://shopify/Location/7"

            def get_product_by_id(self, shop_domain, access_token, product_id):
                return {
                    "id": "gid://shopify/Product/99",
                    "variants": {
                        "nodes": [
                            {
                                "id": "gid://shopify/ProductVariant/100",
                                "sku": "21741",
                            }
                        ]
                    },
                }

            def update_product(self, shop_domain, access_token, *, product, media=None):
                return {"id": product["id"], "title": "Ballin Casual Pants Bertini M2126/018"}

            def product_set(self, shop_domain, access_token, *, input_data, identifier=None):
                self.product_set_input = input_data
                return {
                    "id": "gid://shopify/Product/99",
                    "title": "Ballin Casual Pants Bertini M2126/018",
                    "status": "ACTIVE",
                    "variants": {
                        "nodes": [
                            {
                                "id": "gid://shopify/ProductVariant/100",
                                "sku": variant["sku"],
                                "barcode": variant["barcode"],
                                "price": str(variant.get("price") or "0"),
                                "inventoryItem": {
                                    "id": f"gid://shopify/InventoryItem/{300 + index}",
                                    "unitCost": {"amount": "38.00"},
                                    "inventoryLevels": {"nodes": []},
                                },
                            }
                            for index, variant in enumerate(input_data["variants"])
                        ]
                    },
                }

            def set_product_metafields(self, shop_domain, access_token, metafields):
                raise AssertionError("No metafields were supplied in this fixture")

        client = FakeShopifyClient()
        service = InventorySyncService(client, None, None)
        result = service._sync_matrix_catalog_product(
            self.payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertTrue(result.success)
        self.assertFalse(result.details["created"])
        self.assertEqual(result.details["matrix_variant_count"], 2)
        self.assertEqual(client.product_set_input["variants"][0]["id"], "gid://shopify/ProductVariant/100")
        self.assertEqual(client.product_set_input["variants"][0]["barcode"], "21741. 1 1")


class BulkWorkerTests(unittest.TestCase):
    def test_parallel_bulk_sync_is_capped_and_preserves_result_order(self):
        settings = SimpleNamespace(shopify_bulk_max_workers=2)
        service = InventorySyncService(None, settings, None)
        lock = threading.Lock()
        active = 0
        max_active = 0

        def fake_sync(product, shop):
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.02 if product.sku == "A" else 0.005)
            with lock:
                active -= 1
            return SyncResult(
                shop_domain=shop.shop_domain,
                sku=product.sku,
                success=True,
                message="ok",
                timestamp=utc_now_iso(),
            )

        service.sync_product = fake_sync
        products = [ProductSyncRequest(sku=sku) for sku in ("A", "B", "C", "D")]
        result = service.sync_bulk(
            products,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
            workers=99,
        )

        self.assertEqual(max_active, 2)
        self.assertEqual([row.sku for row in result.results], ["A", "B", "C", "D"])
        self.assertEqual(result.succeeded, 4)
        self.assertEqual(result.failed, 0)


if __name__ == "__main__":
    unittest.main()
