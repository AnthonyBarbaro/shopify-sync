import sys
import types
import unittest


if "dotenv" not in sys.modules:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_stub

from app.inventory import InventorySyncService
from app.models import ProductSyncRequest, ProductVariantSyncInput


class CatalogImportPolicyTests(unittest.TestCase):
    def setUp(self):
        self.service = InventorySyncService(None, None, None)

    def apply_policy(self, payload: ProductSyncRequest) -> ProductSyncRequest:
        normalized = self.service._normalize_payload(payload)
        return self.service._apply_catalog_import_policy(normalized)

    def test_zero_quantity_is_archived_description_is_empty_and_title_is_tagged(self):
        payload = self.apply_policy(
            ProductSyncRequest(
                sku="1001",
                title="Classic Wool Trouser",
                quantity=0,
                description_html="<p>Generated POS description</p>",
                tags=["Menswear"],
            )
        )

        self.assertEqual(payload.status, "archived")
        self.assertEqual(payload.description_html, "")
        self.assertIsNone(payload.description)
        self.assertTrue(payload.update_description)
        self.assertEqual(payload.tags, ["Menswear", "Classic Wool Trouser"])

        create_input = self.service._build_new_product_input(payload, [])
        self.assertEqual(create_input["status"], "ARCHIVED")
        self.assertNotIn("descriptionHtml", create_input)

    def test_matrix_total_quantity_controls_zero_stock_archive(self):
        payload = self.apply_policy(
            ProductSyncRequest(
                sku="21741",
                title="Matrix Trouser",
                variants=[
                    ProductVariantSyncInput(sku="21741. 1 1", barcode="21741. 1 1", quantity=0),
                    ProductVariantSyncInput(sku="21741. 1 2", barcode="21741. 1 2", quantity=0),
                ],
            )
        )

        self.assertEqual(payload.status, "archived")

    def test_in_stock_product_keeps_default_draft_status(self):
        payload = self.apply_policy(
            ProductSyncRequest(sku="1002", title="Cashmere Sweater", quantity=4)
        )

        self.assertIsNone(payload.status)
        self.assertEqual(
            self.service._build_new_product_input(payload, [])["status"],
            "DRAFT",
        )

    def test_import_can_clear_an_existing_description(self):
        payload = self.apply_policy(
            ProductSyncRequest(
                sku="1003",
                title="Cotton Shirt",
                quantity=2,
                description="Generated text",
            )
        )

        update_input = self.service._build_product_update_input(
            payload,
            "gid://shopify/Product/10",
        )
        self.assertEqual(update_input["descriptionHtml"], "")


if __name__ == "__main__":
    unittest.main()
