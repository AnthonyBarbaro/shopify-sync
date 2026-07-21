import sys
import types
import unittest

if "dotenv" not in sys.modules:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_stub

from app.db import ShopRecord
from app.inventory import InventorySyncService
from app.utils import SyncProcessingError


def product(product_id, title, status, skus, *, truncated=False, managed=True):
    return {
        "id": f"gid://shopify/Product/{product_id}",
        "title": title,
        "status": status,
        "metafield": {"value": skus[0]} if managed and skus else None,
        "variants": {
            "pageInfo": {"hasNextPage": truncated},
            "nodes": [{"sku": sku} for sku in skus],
        },
    }


class FakeShopifyClient:
    def __init__(self, products):
        self.products = products
        self.updates = []

    def get_products(self, shop_domain, access_token):
        return self.products

    def update_product(self, shop_domain, access_token, *, product, media=None):
        self.updates.append(product)
        return {"id": product["id"], "status": product["status"]}


class CatalogReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeShopifyClient(
            [
                product(1, "Missing", "ACTIVE", ["OLD-1"]),
                product(2, "Present", "ACTIVE", ["KEEP-1"]),
                product(3, "Already archived", "ARCHIVED", ["OLD-2"]),
                product(4, "No SKU", "ACTIVE", []),
                product(5, "Large variant product", "ACTIVE", ["OLD-3"], truncated=True),
                product(6, "Mixed variants", "ACTIVE", ["OLD-4", "KEEP-2"]),
                product(7, "Unmanaged Shopify product", "ACTIVE", ["MANUAL-1"], managed=False),
            ]
        )
        self.service = InventorySyncService(self.client, None, None)
        self.shop = ShopRecord(shop_domain="example.myshopify.com", access_token="token")

    def test_preview_reports_only_safe_missing_products(self):
        result = self.service.reconcile_catalog_skus(["keep-1", "KEEP-2"], self.shop)

        self.assertFalse(result["apply"])
        self.assertEqual(result["candidate_count"], 1)
        self.assertEqual(result["candidates"][0]["skus"], ["OLD-1"])
        self.assertEqual(result["matched_product_count"], 2)
        self.assertEqual(result["already_archived_count"], 1)
        self.assertEqual(result["skipped_without_sku_count"], 1)
        self.assertEqual(result["skipped_unmanaged_count"], 1)
        self.assertEqual(result["skipped_truncated_variants_count"], 1)
        self.assertEqual(self.client.updates, [])

    def test_apply_archives_candidates(self):
        result = self.service.reconcile_catalog_skus(["KEEP-1", "KEEP-2"], self.shop, apply=True)

        self.assertEqual(result["archived_count"], 1)
        self.assertEqual(result["failed_count"], 0)
        self.assertEqual(
            self.client.updates,
            [{"id": "gid://shopify/Product/1", "status": "ARCHIVED"}],
        )

    def test_empty_source_is_rejected(self):
        with self.assertRaises(SyncProcessingError):
            self.service.reconcile_catalog_skus([], self.shop)

    def test_matrix_variant_sku_matches_managed_base_sku(self):
        self.client.products = [
            product(8, "Matrix product", "ACTIVE", ["21741. 1 1", "21741. 1 2"]),
        ]
        self.client.products[0]["metafield"] = {"value": "21741"}

        result = self.service.reconcile_catalog_skus(["21741"], self.shop)

        self.assertEqual(result["matched_product_count"], 1)
        self.assertEqual(result["candidate_count"], 0)


if __name__ == "__main__":
    unittest.main()
