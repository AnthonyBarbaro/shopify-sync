import sys
import threading
import time
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "jbarbaro_db"))

try:
    import dbf_pos_sync
except ModuleNotFoundError:
    dbf_pos_sync = None

from app.inventory import InventorySyncService
from app.db import ShopRecord
from app.models import ProductSyncRequest, SyncResult, VariantMapping
from app.shopify import INVENTORY_QUANTITY_NAMES, ShopifyClient
from app.utils import ShopifyAPIError, SyncProcessingError, utc_now_iso


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

    def test_short_regular_long_rows_are_length_not_color(self):
        definition = dbf_pos_sync.MatrixDefinition(
            row_headers=["S", "R", "L"],
            column_headers=["34", "36"],
            cells=[
                {"row": 1, "column": 1, "cell": "1 1", "quantity": 1},
                {"row": 2, "column": 1, "cell": "2 1", "quantity": 2},
                {"row": 3, "column": 2, "cell": "3 2", "quantity": 3},
            ],
        )

        variants = dbf_pos_sync.build_matrix_variants(
            sku="PANTS",
            definition=definition,
            price=Decimal("100.00"),
            compare_at_price=None,
            cost=Decimal("40.00"),
        )

        self.assertEqual(variants[0]["option_values"], {"Size": "34", "Length": "S"})
        self.assertEqual(variants[1]["option_values"], {"Size": "34", "Length": "R"})
        self.assertEqual(variants[2]["option_values"], {"Size": "36", "Length": "L"})
        self.assertNotIn("Color", variants[0]["option_values"])

    def test_short_regular_long_columns_are_also_length(self):
        self.assertEqual(
            dbf_pos_sync.matrix_option_name(["S", "R", "L"], default="Size"),
            "Length",
        )
        self.assertEqual(
            dbf_pos_sync.matrix_option_name(["Black", "Navy"], default="Color"),
            "Color",
        )


class ShopifyInventoryLevelTests(unittest.TestCase):
    @staticmethod
    def _level(location_id: str, quantity: int) -> dict:
        return {
            "location": {
                "id": f"gid://shopify/Location/{location_id}",
                "name": f"Location {location_id}",
            },
            "quantities": [{"name": "available", "quantity": quantity}],
        }

    def test_inventory_item_levels_are_loaded_across_every_page(self):
        client = ShopifyClient(SimpleNamespace())
        client.graphql = mock.Mock(
            side_effect=[
                {
                    "data": {
                        "inventoryItem": {
                            "inventoryLevels": {
                                "pageInfo": {
                                    "hasNextPage": True,
                                    "endCursor": "page-2",
                                },
                                "nodes": [self._level("7", 0)],
                            }
                        }
                    }
                },
                {
                    "data": {
                        "inventoryItem": {
                            "inventoryLevels": {
                                "pageInfo": {
                                    "hasNextPage": False,
                                    "endCursor": None,
                                },
                                "nodes": [self._level("8", 0)],
                            }
                        }
                    }
                },
            ]
        )

        levels = client.get_inventory_item_levels(
            "example.myshopify.com",
            "token",
            "100",
        )

        self.assertEqual(
            [level["location"]["id"] for level in levels],
            ["gid://shopify/Location/7", "gid://shopify/Location/8"],
        )
        first_call, second_call = client.graphql.call_args_list
        self.assertEqual(
            first_call.args[3],
            {
                "id": "gid://shopify/InventoryItem/100",
                "first": 250,
                "after": None,
            },
        )
        self.assertEqual(second_call.args[3]["after"], "page-2")
        self.assertEqual(
            first_call.kwargs["operation_name"],
            "InventoryItemLevels",
        )
        self.assertIn("inventoryItem(id: $id)", first_call.args[2])
        self.assertNotIn("variants(first:", first_call.args[2])
        for quantity_name in INVENTORY_QUANTITY_NAMES:
            self.assertIn(f'"{quantity_name}"', first_call.args[2])

    def test_inventory_item_levels_reject_a_missing_next_page_cursor(self):
        client = ShopifyClient(SimpleNamespace())
        client.graphql = mock.Mock(
            return_value={
                "data": {
                    "inventoryItem": {
                        "inventoryLevels": {
                            "pageInfo": {
                                "hasNextPage": True,
                                "endCursor": None,
                            },
                            "nodes": [self._level("7", 0)],
                        }
                    }
                }
            }
        )

        with self.assertRaises(ShopifyAPIError):
            client.get_inventory_item_levels(
                "example.myshopify.com",
                "token",
                "gid://shopify/InventoryItem/100",
            )

    def test_product_lookup_does_not_expand_all_locations_for_every_variant(self):
        client = ShopifyClient(SimpleNamespace())
        client.graphql = mock.Mock(return_value={"data": {"node": None}})

        self.assertIsNone(
            client.get_product_by_id(
                "example.myshopify.com",
                "token",
                "99",
            )
        )

        query = client.graphql.call_args.args[2]
        self.assertIn("variants(first: 100)", query)
        self.assertIn("inventoryLevels(first: 10)", query)
        self.assertNotIn("inventoryLevels(first: 250)", query)
        self.assertNotIn("inventoryQuantity", query)


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

    @staticmethod
    def _inventory_quantities(**overrides: int) -> list[dict]:
        return [
            {
                "name": name,
                "quantity": int(overrides.get(name, 0)),
            }
            for name in INVENTORY_QUANTITY_NAMES
        ]

    @staticmethod
    def _mapping(
        sku: str,
        variant_id: str,
        *,
        product_id: str = "gid://shopify/Product/99",
        quantity: int | None = 0,
        product_status: str | None = None,
        auto_archived_zero_stock: bool = False,
    ) -> VariantMapping:
        return VariantMapping(
            sku=sku,
            variant_id=variant_id,
            product_id=product_id,
            inventory_item_id=f"gid://shopify/InventoryItem/{variant_id.rsplit('/', 1)[-1]}",
            product_status=product_status,
            auto_archived_zero_stock=auto_archived_zero_stock,
            inventory_levels=[
                {
                    "location_id": "gid://shopify/Location/7",
                    "location_name": "Primary",
                    "quantity": quantity,
                }
            ],
        )

    @staticmethod
    def _variant_node(
        sku: str,
        variant_id: str,
        *,
        quantity: int = 0,
        price: str = "199.95",
        compare_at_price: str | None = "249.95",
        cost: str | None = "55.25",
    ) -> dict:
        return {
            "id": variant_id,
            "sku": sku,
            "barcode": sku,
            "price": price,
            "compareAtPrice": compare_at_price,
            "inventoryItem": {
                "id": f"gid://shopify/InventoryItem/{variant_id.rsplit('/', 1)[-1]}",
                "unitCost": {"amount": cost} if cost is not None else None,
                "inventoryLevels": {
                    "pageInfo": {"hasNextPage": False},
                    "nodes": [
                        {
                            "location": {"id": "gid://shopify/Location/7", "name": "Primary"},
                            "quantities": [{"name": "available", "quantity": quantity}],
                        }
                    ]
                },
            },
        }

    @staticmethod
    def _product(
        variants: list[dict],
        *,
        managed_sku: str = "21741",
        has_next_page: bool = False,
        status: str = "ACTIVE",
        auto_archived_zero_stock: bool = False,
    ) -> dict:
        return {
            "id": "gid://shopify/Product/99",
            "title": "Merchant title",
            "status": status,
            "autoArchivedZeroStock": (
                {"value": "true"} if auto_archived_zero_stock else None
            ),
            "managedSku": {"value": managed_sku},
            "variants": {
                "pageInfo": {"hasNextPage": has_next_page},
                "nodes": variants,
            },
        }

    @staticmethod
    def _not_found(sku: str) -> SyncProcessingError:
        return SyncProcessingError(
            f"Shopify variant not found for SKU '{sku}'.",
            {"sku": sku},
            status_code=404,
            code="sku_not_found",
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

    def test_structure_repair_converts_only_the_managed_base_variant(self):
        client = mock.Mock()
        base_mapping = self._mapping("21741", "gid://shopify/ProductVariant/100")
        existing_product = self._product(
            [self._variant_node("21741", "gid://shopify/ProductVariant/100")]
        )

        def lookup(_shop, _token, sku, *, force_refresh=False):
            self.assertTrue(force_refresh)
            if sku == "21741":
                return base_mapping
            raise self._not_found(sku)

        def product_set(_shop, _token, *, input_data, identifier=None):
            self.assertEqual(identifier, {"id": "gid://shopify/Product/99"})
            nodes = []
            for index, variant in enumerate(input_data["variants"]):
                quantity = variant["inventoryQuantities"][0]["quantity"]
                variant_id = variant.get("id") or f"gid://shopify/ProductVariant/{101 + index}"
                nodes.append(
                    self._variant_node(
                        variant["sku"],
                        variant_id,
                        quantity=quantity,
                        price=str(variant["price"]),
                        compare_at_price=str(variant["compareAtPrice"]),
                        cost=str(variant["inventoryItem"]["cost"]),
                    )
                )
            return self._product(nodes)

        client.get_variant_by_sku.side_effect = lookup
        client.get_product_by_id.return_value = existing_product
        client.get_inventory_item_levels.return_value = [
            {
                "location": {"id": "gid://shopify/Location/7", "name": "Primary"},
                "quantities": self._inventory_quantities(),
            },
            {
                "location": {"id": "gid://shopify/Location/8", "name": "Second"},
                "quantities": self._inventory_quantities(),
            },
        ]
        client.get_primary_location_id.return_value = "gid://shopify/Location/7"
        client.product_set.side_effect = product_set
        service = InventorySyncService(client, None, None)

        result = service.repair_matrix_structure(
            self.payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(
            set(result),
            {"base_sku", "status", "message", "product_id", "variants"},
        )
        self.assertEqual(result["base_sku"], "21741")
        self.assertEqual(result["status"], "repaired")
        self.assertEqual(result["product_id"], "gid://shopify/Product/99")
        self.assertEqual(
            [(row["sku"], row["quantity"]) for row in result["variants"]],
            [("21741. 1 1", 1), ("21741. 1 2", 0)],
        )
        product_set_input = client.product_set.call_args.kwargs["input_data"]
        self.assertEqual(set(product_set_input), {"productOptions", "variants"})
        self.assertEqual(product_set_input["variants"][0]["id"], "gid://shopify/ProductVariant/100")
        self.assertNotIn("id", product_set_input["variants"][1])
        for variant in product_set_input["variants"]:
            self.assertEqual(variant["price"], 199.95)
            self.assertEqual(variant["compareAtPrice"], 249.95)
            self.assertEqual(variant["inventoryItem"]["cost"], 55.25)
        client.get_inventory_item_levels.assert_called_once_with(
            "example.myshopify.com",
            "token",
            "gid://shopify/InventoryItem/100",
        )
        client.update_product.assert_not_called()
        client.set_product_metafields.assert_not_called()

    def test_structure_repair_blocks_a_stocked_scalar_variant(self):
        client = mock.Mock()

        def lookup(_shop, _token, sku, *, force_refresh=False):
            self.assertTrue(force_refresh)
            if sku == "21741":
                return self._mapping(
                    "21741",
                    "gid://shopify/ProductVariant/100",
                    quantity=2,
                )
            raise self._not_found(sku)

        client.get_variant_by_sku.side_effect = lookup
        client.get_product_by_id.return_value = self._product(
            [
                self._variant_node(
                    "21741",
                    "gid://shopify/ProductVariant/100",
                    quantity=2,
                )
            ]
        )
        client.get_inventory_item_levels.return_value = [
            {
                "location": {"id": "gid://shopify/Location/7", "name": "Primary"},
                "quantities": self._inventory_quantities(available=2, on_hand=2),
            }
        ]
        client.get_primary_location_id.return_value = "gid://shopify/Location/7"
        service = InventorySyncService(client, None, None)

        result = service.repair_matrix_structure(
            self.payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(result["status"], "blocked")
        self.assertIn("every inventory state", result["message"])
        client.product_set.assert_not_called()

    def test_structure_repair_blocks_committed_stock_when_available_is_zero(self):
        client = mock.Mock()

        def lookup(_shop, _token, sku, *, force_refresh=False):
            self.assertTrue(force_refresh)
            if sku == "21741":
                return self._mapping(
                    "21741",
                    "gid://shopify/ProductVariant/100",
                    quantity=0,
                )
            raise self._not_found(sku)

        client.get_variant_by_sku.side_effect = lookup
        client.get_product_by_id.return_value = self._product(
            [self._variant_node("21741", "gid://shopify/ProductVariant/100")]
        )
        client.get_inventory_item_levels.return_value = [
            {
                "location": {"id": "gid://shopify/Location/7", "name": "Primary"},
                "quantities": self._inventory_quantities(
                    available=0,
                    committed=1,
                    on_hand=1,
                ),
            }
        ]
        service = InventorySyncService(client, None, None)

        result = service.repair_matrix_structure(
            self.payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(result["status"], "blocked")
        self.assertIn("every inventory state", result["message"])
        client.get_primary_location_id.assert_not_called()
        client.product_set.assert_not_called()

    def test_structure_repair_blocks_stock_at_another_shopify_location(self):
        client = mock.Mock()

        def lookup(_shop, _token, sku, *, force_refresh=False):
            self.assertTrue(force_refresh)
            if sku == "21741":
                return self._mapping(
                    "21741",
                    "gid://shopify/ProductVariant/100",
                    quantity=0,
                )
            raise self._not_found(sku)

        client.get_variant_by_sku.side_effect = lookup
        base_variant = self._variant_node(
            "21741",
            "gid://shopify/ProductVariant/100",
            quantity=0,
        )
        client.get_inventory_item_levels.return_value = [
            {
                "location": {
                    "id": "gid://shopify/Location/7",
                    "name": "Primary",
                },
                "quantities": self._inventory_quantities(),
            },
            {
                "location": {
                    "id": "gid://shopify/Location/8",
                    "name": "Second",
                },
                "quantities": self._inventory_quantities(available=2, on_hand=2),
            },
            {
                "location": {
                    "id": "gid://shopify/Location/9",
                    "name": "Third",
                },
                "quantities": self._inventory_quantities(available=-2, on_hand=-2),
            },
        ]
        client.get_product_by_id.return_value = self._product([base_variant])
        service = InventorySyncService(client, None, None)

        result = service.repair_matrix_structure(
            self.payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(result["status"], "blocked")
        self.assertIn("every Shopify location", result["message"])
        client.get_primary_location_id.assert_not_called()
        client.product_set.assert_not_called()

    def test_structure_repair_deterministically_restores_an_auto_archived_product(self):
        client = mock.Mock()
        base_mapping = self._mapping(
            "21741",
            "gid://shopify/ProductVariant/100",
            product_status="ARCHIVED",
            auto_archived_zero_stock=True,
        )
        existing_product = self._product(
            [self._variant_node("21741", "gid://shopify/ProductVariant/100")],
            status="ARCHIVED",
            auto_archived_zero_stock=True,
        )

        def lookup(_shop, _token, sku, *, force_refresh=False):
            self.assertTrue(force_refresh)
            if sku == "21741":
                return base_mapping
            raise self._not_found(sku)

        def product_set(_shop, _token, *, input_data, identifier=None):
            nodes = [
                self._variant_node(
                    variant["sku"],
                    variant.get("id")
                    or f"gid://shopify/ProductVariant/{101 + index}",
                    quantity=variant["inventoryQuantities"][0]["quantity"],
                )
                for index, variant in enumerate(input_data["variants"])
            ]
            return self._product(
                nodes,
                status="ARCHIVED",
                auto_archived_zero_stock=True,
            )

        client.get_variant_by_sku.side_effect = lookup
        client.get_product_by_id.return_value = existing_product
        client.get_inventory_item_levels.return_value = [
            {
                "location": {"id": "gid://shopify/Location/7", "name": "Primary"},
                "quantities": self._inventory_quantities(),
            }
        ]
        client.get_primary_location_id.return_value = "gid://shopify/Location/7"
        client.product_set.side_effect = product_set
        client.update_product.return_value = {"id": "gid://shopify/Product/99"}
        service = InventorySyncService(client, None, None)

        result = service.repair_matrix_structure(
            self.payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(result["status"], "repaired")
        client.update_product.assert_called_once_with(
            "example.myshopify.com",
            "token",
            product={
                "id": "gid://shopify/Product/99",
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

    def test_structure_repair_is_a_noop_when_children_are_already_exact(self):
        client = mock.Mock()
        correct_product = self._product(
            [
                self._variant_node("21741. 1 1", "gid://shopify/ProductVariant/101", quantity=1),
                self._variant_node("21741. 1 2", "gid://shopify/ProductVariant/102", quantity=0),
            ]
        )
        mappings = {
            "21741. 1 1": self._mapping("21741. 1 1", "gid://shopify/ProductVariant/101"),
            "21741. 1 2": self._mapping("21741. 1 2", "gid://shopify/ProductVariant/102"),
        }

        def lookup(_shop, _token, sku, *, force_refresh=False):
            self.assertTrue(force_refresh)
            if sku == "21741":
                raise self._not_found(sku)
            return mappings[sku]

        client.get_variant_by_sku.side_effect = lookup
        client.get_product_by_id.return_value = correct_product
        client.get_primary_location_id.return_value = "gid://shopify/Location/7"
        service = InventorySyncService(client, None, None)

        result = service.repair_matrix_structure(
            self.payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(result["status"], "already_correct")
        self.assertEqual(len(result["variants"]), 2)
        client.product_set.assert_not_called()

    def test_structure_repair_blocks_exact_children_with_unconfirmed_quantities(self):
        client = mock.Mock()
        product = self._product(
            [
                self._variant_node(
                    "21741. 1 1",
                    "gid://shopify/ProductVariant/101",
                    quantity=0,
                ),
                self._variant_node(
                    "21741. 1 2",
                    "gid://shopify/ProductVariant/102",
                    quantity=0,
                ),
            ]
        )
        mappings = {
            "21741. 1 1": self._mapping(
                "21741. 1 1",
                "gid://shopify/ProductVariant/101",
                quantity=0,
            ),
            "21741. 1 2": self._mapping(
                "21741. 1 2",
                "gid://shopify/ProductVariant/102",
                quantity=0,
            ),
        }

        def lookup(_shop, _token, sku, *, force_refresh=False):
            self.assertTrue(force_refresh)
            if sku == "21741":
                raise self._not_found(sku)
            return mappings[sku]

        client.get_variant_by_sku.side_effect = lookup
        client.get_product_by_id.return_value = product
        client.get_primary_location_id.return_value = "gid://shopify/Location/7"
        service = InventorySyncService(client, None, None)

        result = service.repair_matrix_structure(
            self.payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(result["status"], "quantity_mismatch")
        self.assertIn("quantities", result["message"])
        self.assertEqual(len(result["variants"]), 2)
        client.product_set.assert_not_called()
        client.update_product.assert_not_called()

    def test_structure_repair_blocks_unmanaged_truncated_and_nondefault_products(self):
        cases = {
            "unmanaged": self._product(
                [self._variant_node("21741", "gid://shopify/ProductVariant/100")],
                managed_sku="OTHER",
            ),
            "truncated": self._product(
                [self._variant_node("21741", "gid://shopify/ProductVariant/100")],
                has_next_page=True,
            ),
            "multiple": self._product(
                [
                    self._variant_node("21741", "gid://shopify/ProductVariant/100"),
                    self._variant_node("MERCHANT", "gid://shopify/ProductVariant/103"),
                ]
            ),
        }
        for name, product in cases.items():
            with self.subTest(name=name):
                client = mock.Mock()

                def lookup(_shop, _token, sku, *, force_refresh=False):
                    self.assertTrue(force_refresh)
                    if sku == "21741":
                        return self._mapping("21741", "gid://shopify/ProductVariant/100")
                    raise self._not_found(sku)

                client.get_variant_by_sku.side_effect = lookup
                client.get_product_by_id.return_value = product
                service = InventorySyncService(client, None, None)

                result = service.repair_matrix_structure(
                    self.payload,
                    ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
                )

                self.assertEqual(result["status"], "blocked")
                client.product_set.assert_not_called()

    def test_structure_repair_blocks_a_child_sku_owned_by_another_product(self):
        client = mock.Mock()

        def lookup(_shop, _token, sku, *, force_refresh=False):
            self.assertTrue(force_refresh)
            if sku == "21741":
                return self._mapping("21741", "gid://shopify/ProductVariant/100")
            if sku == "21741. 1 1":
                return self._mapping(
                    sku,
                    "gid://shopify/ProductVariant/900",
                    product_id="gid://shopify/Product/900",
                )
            raise self._not_found(sku)

        client.get_variant_by_sku.side_effect = lookup
        client.get_product_by_id.return_value = self._product(
            [self._variant_node("21741", "gid://shopify/ProductVariant/100")]
        )
        service = InventorySyncService(client, None, None)

        result = service.repair_matrix_structure(
            self.payload,
            ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(result["status"], "blocked")
        client.product_set.assert_not_called()

    def test_structure_repair_rejects_an_unverified_product_set_result(self):
        client = mock.Mock()

        def lookup(_shop, _token, sku, *, force_refresh=False):
            self.assertTrue(force_refresh)
            if sku == "21741":
                return self._mapping("21741", "gid://shopify/ProductVariant/100")
            raise self._not_found(sku)

        client.get_variant_by_sku.side_effect = lookup
        client.get_product_by_id.return_value = self._product(
            [self._variant_node("21741", "gid://shopify/ProductVariant/100")]
        )
        client.get_inventory_item_levels.return_value = [
            {
                "location": {"id": "gid://shopify/Location/7", "name": "Primary"},
                "quantities": self._inventory_quantities(),
            }
        ]
        client.get_primary_location_id.return_value = "gid://shopify/Location/7"
        client.product_set.return_value = self._product(
            [self._variant_node("21741. 1 1", "gid://shopify/ProductVariant/100", quantity=1)]
        )
        service = InventorySyncService(client, None, None)

        with self.assertRaises(ShopifyAPIError):
            service.repair_matrix_structure(
                self.payload,
                ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
            )

    def test_existing_srl_color_option_is_renamed_without_product_field_updates(self):
        class FakeShopifyClient:
            def __init__(self):
                self.rename = None

            def get_product_options_by_sku(self, shop_domain, access_token, sku):
                if sku != "PANTS. 1 1":
                    return None
                return {
                    "id": "gid://shopify/Product/99",
                    "options": [
                        {"id": "gid://shopify/ProductOption/1", "name": "Size", "values": ["34", "36"]},
                        {"id": "gid://shopify/ProductOption/2", "name": "Color", "values": ["S", "R", "L"]},
                    ],
                }

            def rename_product_option(
                self,
                shop_domain,
                access_token,
                *,
                product_id,
                option_id,
                name,
            ):
                self.rename = {
                    "product_id": product_id,
                    "option_id": option_id,
                    "name": name,
                }
                return {
                    "id": product_id,
                    "options": [
                        {"name": "Size", "values": ["34", "36"]},
                        {"name": "Length", "values": ["S", "R", "L"]},
                    ],
                }

        client = FakeShopifyClient()
        service = InventorySyncService(client, None, None)
        result = service.repair_matrix_length_option(
            base_sku="PANTS",
            variant_skus=["PANTS. 1 1"],
            shop=ShopRecord(shop_domain="example.myshopify.com", access_token="token"),
        )

        self.assertEqual(result["status"], "updated")
        self.assertEqual(
            client.rename,
            {
                "product_id": "gid://shopify/Product/99",
                "option_id": "gid://shopify/ProductOption/2",
                "name": "Length",
            },
        )


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
