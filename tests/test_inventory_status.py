import unittest
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest import mock

from app.db import ShopRecord
from app.inventory import InventorySyncService
from app.models import VariantMapping
from app.shopify import ShopifyClient
from app.utils import SyncProcessingError


def stock_variant(sku: str, quantities: list[int]) -> dict[str, Any]:
    return {
        "sku": sku,
        "inventoryItem": {
            "tracked": True,
            "inventoryLevels": {
                "pageInfo": {"hasNextPage": False},
                "nodes": [
                    {"quantities": [{"name": "available", "quantity": quantity}]}
                    for quantity in quantities
                ],
            },
        },
    }


class NightlyProductStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = mock.Mock(spec=ShopifyClient)
        self.service = InventorySyncService(
            self.client, SimpleNamespace(shopify_location_id=None), None,
        )
        self.shop = ShopRecord(shop_domain="example.myshopify.com", access_token="test-token")
        self.mapping = VariantMapping(
            sku="10001. 1 1",
            variant_id="gid://shopify/ProductVariant/1",
            product_id="gid://shopify/Product/2",
            inventory_item_id="gid://shopify/InventoryItem/3",
            product_status="ACTIVE",
        )
        self.client.get_variant_by_sku.return_value = self.mapping
        self.product = {
            "id": self.mapping.product_id,
            "status": "ACTIVE",
            "managedSku": {"value": "10001"},
            "variants": {
                "pageInfo": {"hasNextPage": False},
                "nodes": [stock_variant("10001. 1 1", [0]), stock_variant("10001. 1 2", [0])],
            },
        }
        self.client.get_product_stock_for_archiving.return_value = self.product

    def archive(self) -> dict[str, Any]:
        return self.service.archive_zero_stock_product_by_sku(shop=self.shop, sku=self.mapping.sku)

    def test_archives_zero_stock_active_and_draft_products_with_restore_marker(self) -> None:
        for status in ("ACTIVE", "DRAFT"):
            with self.subTest(status=status):
                self.client.update_product.reset_mock()
                self.product["status"] = status
                result = self.archive()
                self.assertTrue(result["success"])
                self.assertTrue(result["product_status_updated"])
                self.client.update_product.assert_called_once_with(
                    self.shop.shop_domain,
                    self.shop.access_token,
                    product={
                        "id": self.mapping.product_id,
                        "status": "ARCHIVED",
                        "metafields": [{
                            "namespace": "pos",
                            "key": "auto_archived_zero_stock",
                            "type": "boolean",
                            "value": "true",
                        }],
                    },
                )
        self.client.get_variant_by_sku.assert_called_with(
            self.shop.shop_domain, self.shop.access_token, self.mapping.sku, force_refresh=True,
        )

    def test_negative_stock_does_not_cancel_positive_variant_or_location(self) -> None:
        for variants in (
            [stock_variant("10001. 1 1", [-3]), stock_variant("10001. 1 2", [1])],
            [stock_variant("10001. 1 1", [-3, 1]), stock_variant("10001. 1 2", [0])],
        ):
            with self.subTest(variants=variants):
                self.product["variants"]["nodes"] = variants
                self.assertEqual(self.archive()["reason"], "in_stock")
                self.client.update_product.assert_not_called()

    def test_all_nonpositive_stock_can_be_archived(self) -> None:
        self.product["variants"]["nodes"] = [stock_variant(self.mapping.sku, [-1, 0])]
        self.assertTrue(self.archive()["product_status_updated"])

    def test_manual_archive_and_repeated_automatic_archive_are_preserved(self) -> None:
        for marker in (None, {"value": "true"}):
            with self.subTest(marker=marker):
                self.product["status"] = "ARCHIVED"
                self.product["autoArchivedZeroStock"] = marker
                self.assertFalse(self.archive()["product_status_updated"])
                self.client.update_product.assert_not_called()

    def test_known_archived_product_needs_no_full_inventory_read(self) -> None:
        self.mapping.product_status = "ARCHIVED"
        self.assertFalse(self.archive()["product_status_updated"])
        self.client.get_product_stock_for_archiving.assert_not_called()
        self.client.update_product.assert_not_called()

    def test_auto_archived_product_is_restored_after_post_archive_read_failure(self) -> None:
        self.client.get_product_stock_for_archiving.side_effect = [
            self.product, SyncProcessingError("Stock read temporarily unavailable"),
        ]
        with self.assertRaises(SyncProcessingError):
            self.archive()
        self.client.update_product.assert_called_once()

        self.mapping.product_status = "ARCHIVED"
        self.mapping.auto_archived_zero_stock = True
        restocked = deepcopy(self.product)
        restocked["status"] = "ARCHIVED"
        restocked["autoArchivedZeroStock"] = {"value": "true"}
        restocked["variants"]["nodes"][1] = stock_variant("10001. 1 2", [0, 2])
        self.client.get_product_stock_for_archiving.side_effect = None
        self.client.get_product_stock_for_archiving.return_value = restocked

        result = self.archive()

        self.assertEqual(result["reason"], "restocked_auto_archived")
        self.assertTrue(result["product_status_updated"])
        self.assertEqual(self.client.get_product_stock_for_archiving.call_count, 3)
        restore = self.client.update_product.call_args.kwargs["product"]
        self.assertEqual(restore["status"], "DRAFT")
        self.assertEqual(restore["metafields"][0]["value"], "false")

    def test_fresh_auto_archive_after_mapping_lookup_is_restored_if_restocked(self) -> None:
        self.product["status"] = "ARCHIVED"
        self.product["autoArchivedZeroStock"] = {"value": "true"}
        self.product["variants"]["nodes"][0] = stock_variant(self.mapping.sku, [2])

        result = self.archive()

        self.assertEqual(result["reason"], "restocked_auto_archived")
        self.client.update_product.assert_called_once()
        self.assertEqual(self.client.update_product.call_args.kwargs["product"]["status"], "DRAFT")

    def test_auto_archived_product_with_zero_or_unknown_stock_is_not_restored(self) -> None:
        self.mapping.product_status = "ARCHIVED"
        self.mapping.auto_archived_zero_stock = True
        self.product["status"] = "ARCHIVED"
        self.product["autoArchivedZeroStock"] = {"value": "true"}
        unknown_variant = stock_variant("10001. 1 2", [0])
        unknown_variant["inventoryItem"]["inventoryLevels"]["nodes"][0]["quantities"][0]["quantity"] = None
        variants = [
            [stock_variant(self.mapping.sku, [0]), stock_variant("10001. 1 2", [-1])],
            [stock_variant(self.mapping.sku, [2]), unknown_variant],
        ]
        for stock, success in zip(variants, (True, False)):
            with self.subTest(stock=stock):
                self.product["variants"]["nodes"] = stock
                result = self.archive()
                self.assertFalse(result["product_status_updated"])
                self.assertEqual(result["success"], success)
                self.client.update_product.assert_not_called()

    def test_unverifiable_post_archive_stock_keeps_status_retryable(self) -> None:
        truncated = deepcopy(self.product)
        truncated["variants"]["pageInfo"]["hasNextPage"] = True
        unknown = deepcopy(self.product)
        unknown["variants"]["nodes"][0]["inventoryItem"]["inventoryLevels"]["nodes"][0]["quantities"] = []
        unknown_status = deepcopy(self.product)
        unknown_status["status"] = None
        for refreshed in (None, {"id": "different-product"}, truncated, unknown, unknown_status):
            with self.subTest(refreshed=refreshed):
                self.client.update_product.reset_mock()
                self.client.get_product_stock_for_archiving.side_effect = [self.product, refreshed]

                result = self.archive()

                self.assertFalse(result["success"])
                self.assertTrue(result["product_status_updated"])
                self.client.update_product.assert_called_once()

    def test_changed_manual_archive_marker_is_not_restored(self) -> None:
        self.mapping.product_status = "ARCHIVED"
        self.mapping.auto_archived_zero_stock = True
        self.product["status"] = "ARCHIVED"
        self.product["autoArchivedZeroStock"] = {"value": "false"}
        self.product["variants"]["nodes"][0] = stock_variant(self.mapping.sku, [2])

        self.assertFalse(self.archive()["product_status_updated"])
        self.client.update_product.assert_not_called()

    def test_restock_during_archiving_restores_draft_even_at_another_location(self) -> None:
        restocked = deepcopy(self.product)
        restocked["status"] = "ARCHIVED"
        restocked["autoArchivedZeroStock"] = {"value": "true"}
        restocked["variants"]["nodes"][1] = stock_variant("10001. 1 2", [0, 2])
        self.client.get_product_stock_for_archiving.side_effect = [self.product, restocked]

        result = self.archive()

        self.assertEqual(result["reason"], "restocked_during_archive")
        self.assertEqual(self.client.update_product.call_count, 2)
        restore = self.client.update_product.call_args.kwargs["product"]
        self.assertEqual(restore["status"], "DRAFT")
        self.assertEqual(restore["metafields"][0]["value"], "false")

    def test_concurrent_manual_archive_is_not_restored(self) -> None:
        manual = deepcopy(self.product)
        manual["status"] = "ARCHIVED"
        manual["autoArchivedZeroStock"] = None
        manual["variants"]["nodes"][0] = stock_variant(self.mapping.sku, [2])
        self.client.get_product_stock_for_archiving.side_effect = [self.product, manual]

        self.archive()

        self.client.update_product.assert_called_once()

    def test_unmanaged_or_mismatched_product_is_preserved(self) -> None:
        for marker in (None, {"value": "different-sku"}):
            with self.subTest(marker=marker):
                self.product["managedSku"] = marker
                self.assertEqual(self.archive()["reason"], "unmanaged_product")
                self.client.update_product.assert_not_called()

    def test_product_whose_sku_changed_is_preserved(self) -> None:
        self.product["variants"]["nodes"] = [stock_variant("different-sku", [0])]
        self.assertEqual(self.archive()["reason"], "unmanaged_product")
        self.client.update_product.assert_not_called()

    def test_incomplete_or_untracked_inventory_is_never_archived(self) -> None:
        incomplete: list[dict[str, Any]] = []
        product = deepcopy(self.product)
        product["variants"]["pageInfo"]["hasNextPage"] = True
        incomplete.append(product)
        product = deepcopy(self.product)
        product["variants"]["pageInfo"] = {}
        incomplete.append(product)
        product = deepcopy(self.product)
        product["variants"]["nodes"] = []
        incomplete.append(product)
        for tracked in (False, None):
            product = deepcopy(self.product)
            product["variants"]["nodes"][1]["inventoryItem"]["tracked"] = tracked
            incomplete.append(product)
        for page_info, nodes in (
            ({"hasNextPage": True}, [{"quantities": [{"name": "available", "quantity": 0}]}]),
            ({}, [{"quantities": [{"name": "available", "quantity": 0}]}]),
            ({"hasNextPage": False}, []),
            ({"hasNextPage": False}, [{"quantities": []}]),
            ({"hasNextPage": False}, [{"quantities": [{"name": "available", "quantity": None}]}]),
            ({"hasNextPage": False}, [{"quantities": [{"name": "available", "quantity": "0"}]}]),
            ({"hasNextPage": False}, [{"quantities": [{"name": "available", "quantity": False}]}]),
        ):
            product = deepcopy(self.product)
            product["variants"]["nodes"][1]["inventoryItem"]["inventoryLevels"] = {
                "pageInfo": page_info, "nodes": nodes,
            }
            incomplete.append(product)
        for product in incomplete:
            with self.subTest(product=product):
                self.client.get_product_stock_for_archiving.return_value = product
                result = self.archive()
                self.assertFalse(result["product_status_updated"])
                self.assertTrue(result["success"])
                product["status"] = "ARCHIVED"
                product["autoArchivedZeroStock"] = {"value": "true"}
                result = self.archive()
                self.assertFalse(result["product_status_updated"])
                self.assertFalse(result["success"])
                self.client.update_product.assert_not_called()

    def test_missing_product_and_lookup_failures_are_retryable(self) -> None:
        self.client.get_product_stock_for_archiving.return_value = None
        with self.assertRaises(SyncProcessingError):
            self.archive()
        self.client.get_variant_by_sku.side_effect = SyncProcessingError("Duplicate SKU")
        with self.assertRaises(SyncProcessingError):
            self.archive()
        self.client.update_product.assert_not_called()

    def test_single_variant_product_uses_its_own_managed_sku(self) -> None:
        self.mapping.sku = "SINGLE"
        self.product["managedSku"] = {"value": "SINGLE"}
        self.product["variants"]["nodes"] = [stock_variant("SINGLE", [0])]
        self.assertTrue(self.archive()["product_status_updated"])

    def test_daytime_inventory_adjustment_to_zero_does_not_archive(self) -> None:
        self.client.get_primary_location_id.return_value = "gid://shopify/Location/4"
        self.client.adjust_inventory.return_value = {"quantity_after_change": 0}

        result = self.service.adjust_inventory_quantity(
            sku=self.mapping.sku,
            delta=-1,
            idempotency_key="last-unit",
            shop=self.shop,
        )

        self.assertEqual(result["quantity_after_change"], 0)
        self.assertFalse(result["product_status_updated"])
        self.client.update_product.assert_not_called()


if __name__ == "__main__":
    unittest.main()
