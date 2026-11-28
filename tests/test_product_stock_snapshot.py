import unittest
from types import SimpleNamespace
from unittest import mock

from app.shopify import ShopifyClient
from app.utils import ShopifyAPIError


class ProductStockSnapshotTests(unittest.TestCase):
    def test_inventory_snapshot_starts_a_new_id_range_for_each_page(self) -> None:
        client = ShopifyClient(SimpleNamespace())
        location = "gid://shopify/Location/4"

        def page(rows, *, has_next):
            return {"data": {"inventoryItems": {
                "pageInfo": {"hasNextPage": has_next}, "nodes": rows,
            }}}

        first = {"id": "gid://shopify/InventoryItem/24999", "sku": "FIRST", "inventoryLevel": {
            "updatedAt": "2026-11-28T20:00:00Z", "quantities": [{"name": "available", "quantity": 0}],
        }}
        unnamed = {"id": "gid://shopify/InventoryItem/25000", "sku": ""}
        next_row = {"id": "gid://shopify/InventoryItem/25001", "sku": "NEXT", "duplicateSkuCount": 1}
        with mock.patch.object(client, "graphql", side_effect=[
            page([first, unnamed], has_next=True), page([next_row], has_next=False),
        ]) as graphql:
            rows = client.get_inventory_snapshot("example.myshopify.com", "token", location)

        self.assertEqual([call.args[3] for call in graphql.call_args_list], [
            {"filter": None, "locationId": location}, {"filter": "id:>25000", "locationId": location},
        ])
        self.assertEqual([(row["sku"], row["quantity"], row["available_at_location"], row["duplicate_sku_count"])
                          for row in rows], [("FIRST", 0, True, 0), ("NEXT", None, False, 1)])

    def test_inventory_page_rejects_an_id_that_would_repeat_the_previous_page(self) -> None:
        client = ShopifyClient(SimpleNamespace())
        with mock.patch.object(client, "graphql", return_value={"data": {"inventoryItems": {
            "pageInfo": {"hasNextPage": True}, "nodes": [{"id": "gid://shopify/InventoryItem/10", "sku": "REPEATED"}],
        }}}):
            with self.assertRaisesRegex(ShopifyAPIError, "ascending ID"):
                client.get_inventory_snapshot_page("example.myshopify.com", "token", "4", after_id=10)

    def test_snapshot_preserves_variant_stock_at_each_location(self) -> None:
        client = ShopifyClient(SimpleNamespace())
        product = {
            "id": "gid://shopify/Product/10",
            "status": "ACTIVE",
            "managedSku": {"value": "SHIRT"},
            "autoArchivedZeroStock": {"value": "false"},
            "variants": {
                "pageInfo": {"hasNextPage": False},
                "nodes": [
                    {
                        "id": "gid://shopify/ProductVariant/20",
                        "sku": "SHIRT-S",
                        "inventoryItem": {
                            "id": "gid://shopify/InventoryItem/30",
                            "tracked": True,
                            "inventoryLevels": {
                                "pageInfo": {"hasNextPage": False},
                                "nodes": [
                                    {"quantities": [{"name": "available", "quantity": -2}]},
                                    {"quantities": [{"name": "available", "quantity": 2}]},
                                ],
                            },
                        },
                    },
                    {
                        "id": "gid://shopify/ProductVariant/21",
                        "sku": "SHIRT-M",
                        "inventoryItem": {
                            "id": "gid://shopify/InventoryItem/31",
                            "tracked": False,
                            "inventoryLevels": {
                                "pageInfo": {"hasNextPage": True},
                                "nodes": [],
                            },
                        },
                    },
                ],
            },
        }
        with mock.patch.object(
            client,
            "graphql",
            return_value={"data": {"product": product}},
        ) as graphql:
            snapshot = client.get_product_stock_for_archiving(
                "example.myshopify.com", "token", "10"
            )

        self.assertEqual(snapshot, product)
        self.assertEqual(graphql.call_args.args[3], {"id": product["id"]})
        query = graphql.call_args.args[2]
        self.assertIn('managedSku: metafield(namespace: "pos", key: "sku")', query)
        self.assertIn("tracked", query)
        self.assertIn('quantities(names: ["available"])', query)
        self.assertEqual(query.count("hasNextPage"), 2)

    def test_each_snapshot_reads_fresh_stock_and_preserves_truncation(self) -> None:
        client = ShopifyClient(SimpleNamespace())
        first_product = {
            "id": "gid://shopify/Product/10",
            "variants": {"pageInfo": {"hasNextPage": True}, "nodes": []},
        }
        second_product = {
            "id": "gid://shopify/Product/10",
            "variants": {"pageInfo": {"hasNextPage": False}, "nodes": []},
        }
        with mock.patch.object(
            client,
            "graphql",
            side_effect=[
                {"data": {"product": first_product}},
                {"data": {"product": second_product}},
            ],
        ) as graphql:
            first = client.get_product_stock_for_archiving(
                "example.myshopify.com", "token", first_product["id"]
            )
            second = client.get_product_stock_for_archiving(
                "example.myshopify.com", "token", second_product["id"]
            )

        self.assertEqual(first, first_product)
        self.assertEqual(second, second_product)
        self.assertEqual(graphql.call_count, 2)

    def test_missing_product_returns_none(self) -> None:
        client = ShopifyClient(SimpleNamespace())
        with mock.patch.object(
            client, "graphql", return_value={"data": {"product": None}}
        ):
            snapshot = client.get_product_stock_for_archiving(
                "example.myshopify.com", "token", "10"
            )

        self.assertIsNone(snapshot)


if __name__ == "__main__":
    unittest.main()
