import unittest
from unittest.mock import Mock

from app.db import ShopRecord
from app.inventory import InventorySyncService
from app.models import ProductSyncRequest, ProductVariantSyncInput, VariantMapping
from jbarbaro_db.dbf_pos_sync import pos_item_weight_grams


class ProductWeightTests(unittest.TestCase):
    def setUp(self):
        self.shopify = Mock()
        self.service = InventorySyncService(self.shopify, None, None)

    def test_pos_weight_prefers_shipping_weight_and_treats_zero_as_missing(self):
        self.assertAlmostEqual(pos_item_weight_grams({"SHIPWEIGHT": "2", "WEIGHT": "1"}), 907.18474)
        self.assertAlmostEqual(pos_item_weight_grams({"SHIPWEIGHT": "0", "WEIGHT": "1"}), 453.59237)
        self.assertIsNone(pos_item_weight_grams({"SHIPWEIGHT": "", "WEIGHT": "0"}))

    def test_single_variant_uses_measured_or_estimated_weight(self):
        payload = ProductSyncRequest(sku="SHIRT", product_type="Dress Shirt", weight_grams=325)
        create = self.service._build_new_product_input(payload, [])
        self.assertEqual(create["variants"][0]["inventoryItem"]["measurement"]["weight"], {
            "value": 325, "unit": "GRAMS",
        })
        missing = payload.model_copy(update={"weight_grams": 0})
        mapping = VariantMapping(sku="SHIRT", variant_id="1", product_id="2", inventory_item_id="3")
        update = self.service._build_variant_update_input(missing, mapping)
        self.assertEqual(update["inventoryItem"]["measurement"]["weight"]["value"], 283)
        existing = mapping.model_copy(update={"has_weight": True})
        self.assertNotIn("measurement", self.service._build_variant_update_input(payload, existing)["inventoryItem"])
        digital = payload.model_copy(update={"requires_shipping": False})
        self.assertNotIn("measurement", self.service._build_new_product_input(digital, [])["variants"][0]["inventoryItem"])

    def test_matrix_uses_variant_then_product_weight_and_preserves_positive_weight(self):
        variants = [
            ProductVariantSyncInput(
                sku=size, barcode=size, option_values={"Size": size}, weight_grams=grams,
            )
            for size, grams in [("S", 400), ("M", 450), ("L", None)]
        ]
        payload = ProductSyncRequest(sku="BASE", weight_grams=500, variants=variants)
        existing_product = {"variants": {"nodes": [{
            "id": "1", "sku": "S",
            "inventoryItem": {"measurement": {"weight": {"value": 1, "unit": "POUNDS"}}},
        }]}}
        result = self.service._build_matrix_product_set_input(
            payload, location_id="1", media_inputs=[], existing_product=existing_product,
        )
        items = [variant["inventoryItem"] for variant in result["variants"]]
        self.assertNotIn("measurement", items[0])
        self.assertEqual([item["measurement"]["weight"]["value"] for item in items[1:]], [450, 500])

    def test_backfill_previews_and_updates_only_missing_shippable_variants(self):
        product = {"id": "p1", "title": "Wool", "productType": "Suit"}
        nodes = [
            {"id": sku, "sku": sku, "product": product, "inventoryItem": {
                "requiresShipping": ships, "measurement": {"weight": weight},
            }}
            for sku, ships, weight in [
                ("missing", True, None), ("zero", True, {"value": 0, "unit": "OUNCES"}),
                ("saved", True, {"value": 2, "unit": "POUNDS"}), ("digital", False, None),
            ]
        ]
        self.shopify.get_variant_weight_page.return_value = {
            "nodes": nodes, "pageInfo": {"hasNextPage": True, "endCursor": "next"},
        }
        shop = ShopRecord("example.myshopify.com", "token")
        preview = self.service.backfill_catalog_weights(shop, cursor="old")
        self.shopify.update_variants_fields.assert_not_called()
        self.assertEqual((preview["scanned"], preview["missing"], preview["updated"]), (4, 2, 0))
        self.assertEqual(preview["next_cursor"], "next")

        def update(_, __, *, product_id, variants):
            by_sku = {node["id"]: node for node in nodes}
            self.assertEqual(product_id, "p1")
            self.assertEqual([variant["id"] for variant in variants], ["missing", "zero"])
            updated = []
            for variant in variants:
                weight = variant["inventoryItem"]["measurement"]["weight"]
                self.assertEqual(weight, {"value": 1361, "unit": "GRAMS"})
                by_sku[variant["id"]]["inventoryItem"].update(variant["inventoryItem"])
                updated.append(by_sku[variant["id"]])
            return updated

        self.shopify.update_variants_fields.side_effect = update
        self.assertEqual(self.service.backfill_catalog_weights(shop, cursor="old", apply=True)["updated"], 2)
        self.assertEqual(self.service.backfill_catalog_weights(shop, cursor="old", apply=True)["missing"], 0)
        self.shopify.update_variants_fields.assert_called_once()
        self.shopify.get_variant_weight_page.assert_called_with("example.myshopify.com", "token", cursor="old")
