import time
import uuid
from decimal import Decimal, ROUND_HALF_UP
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.config import Settings
from app.models import InventoryLevelSnapshot, VariantMapping
from app.utils import (
    AuthenticationError,
    ShopifyAPIError,
    SyncProcessingError,
    get_backoff_delay,
    parse_retry_after,
    setup_logging,
    utc_now,
)


class ShopifyClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.logger = setup_logging().getChild("shopify")
        self._sku_cache: Dict[str, Dict[str, Any]] = {}
        self._sku_cache_lock = Lock()
        self._location_cache: Dict[str, Tuple[str, str]] = {}
        self._location_lock = Lock()

    def get_products(self, shop_domain: str, access_token: str) -> List[Dict[str, Any]]:
        query = """
        query GetProducts($first: Int!, $after: String) {
          products(first: $first, after: $after) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              id
              title
              handle
              status
              vendor
              productType
              updatedAt
              media(first: 10) {
                nodes {
                  alt
                  mediaContentType
                  status
                  ... on MediaImage {
                    image {
                      url
                    }
                  }
                }
              }
              variants(first: 100) {
                nodes {
                  id
                  sku
                  barcode
                  price
                  inventoryItem {
                    id
                    inventoryLevels(first: 10) {
                      nodes {
                        location {
                          id
                          name
                        }
                        quantities(names: ["available"]) {
                          name
                          quantity
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        products: List[Dict[str, Any]] = []
        cursor: Optional[str] = None

        while True:
            payload = self.graphql(
                shop_domain,
                access_token,
                query,
                {"first": 25, "after": cursor},
                operation_name="GetProducts",
            )
            products_data = payload["data"]["products"]
            products.extend(products_data["nodes"])
            if not products_data["pageInfo"]["hasNextPage"]:
                return products
            cursor = products_data["pageInfo"]["endCursor"]

    def get_variant_by_sku(
        self,
        shop_domain: str,
        access_token: str,
        sku: str,
        *,
        force_refresh: bool = False,
    ) -> VariantMapping:
        normalized_sku = sku.strip()
        if not normalized_sku:
            raise SyncProcessingError("SKU is required.", {"sku": sku}, code="invalid_sku")

        if not force_refresh:
            cached = self._get_cached_variant(shop_domain, normalized_sku)
            if cached is not None:
                return cached

        query = """
        query VariantBySku($query: String!) {
          productVariants(first: 1, query: $query) {
            nodes {
              id
              sku
              price
              product {
                id
                title
              }
              inventoryItem {
                id
                inventoryLevels(first: 25) {
                  nodes {
                    location {
                      id
                      name
                    }
                    quantities(names: ["available"]) {
                      name
                      quantity
                    }
                  }
                }
              }
            }
          }
        }
        """
        payload = self.graphql(
            shop_domain,
            access_token,
            query,
            {"query": _build_sku_search_query(normalized_sku)},
            operation_name="VariantBySku",
        )
        nodes = payload["data"]["productVariants"]["nodes"]
        if not nodes:
            raise SyncProcessingError(
                f"Shopify variant not found for SKU '{normalized_sku}'.",
                {"sku": normalized_sku, "shop": shop_domain},
                status_code=404,
                code="sku_not_found",
            )

        mapping = _parse_variant_mapping(nodes[0])
        self._set_cached_variant(shop_domain, mapping)
        return mapping

    def get_product_by_handle(
        self,
        shop_domain: str,
        access_token: str,
        handle: str,
    ) -> Optional[Dict[str, Any]]:
        normalized_handle = handle.strip()
        if not normalized_handle:
            return None

        query = """
        query ProductByHandle($query: String!) {
          products(first: 1, query: $query) {
            nodes {
              id
              title
              handle
              status
              vendor
              productType
              media(first: 10) {
                nodes {
                  alt
                  mediaContentType
                  status
                  ... on MediaImage {
                    image {
                      url
                    }
                  }
                }
              }
              variants(first: 100) {
                nodes {
                  id
                  sku
                  barcode
                  price
                  inventoryItem {
                    id
                    inventoryLevels(first: 10) {
                      nodes {
                        location {
                          id
                          name
                        }
                        quantities(names: ["available"]) {
                          name
                          quantity
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        payload = self.graphql(
            shop_domain,
            access_token,
            query,
            {"query": f"handle:{normalized_handle}"},
            operation_name="ProductByHandle",
        )
        nodes = payload["data"]["products"]["nodes"]
        return nodes[0] if nodes else None

    def get_product_by_id(
        self,
        shop_domain: str,
        access_token: str,
        product_id: str | int,
    ) -> Optional[Dict[str, Any]]:
        query = """
        query ProductById($id: ID!) {
          node(id: $id) {
            ... on Product {
              id
              title
              handle
              status
              vendor
              productType
              updatedAt
              media(first: 10) {
                nodes {
                  alt
                  mediaContentType
                  status
                  ... on MediaImage {
                    image {
                      url
                    }
                  }
                }
              }
              variants(first: 100) {
                nodes {
                  id
                  sku
                  barcode
                  price
                  inventoryItem {
                    id
                    inventoryLevels(first: 10) {
                      nodes {
                        location {
                          id
                          name
                        }
                        quantities(names: ["available"]) {
                          name
                          quantity
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        payload = self.graphql(
            shop_domain,
            access_token,
            query,
            {"id": normalize_gid("Product", str(product_id))},
            operation_name="ProductById",
        )
        node = payload["data"]["node"]
        return node if node else None

    def get_primary_location_id(self, shop_domain: str, access_token: str) -> str:
        if self.settings.shopify_location_id:
            return normalize_gid("Location", self.settings.shopify_location_id)

        with self._location_lock:
            cached = self._location_cache.get(shop_domain)
            if cached is not None:
                return cached[0]

        query = """
        query GetLocations {
          locations(first: 1) {
            nodes {
              id
              name
            }
          }
        }
        """
        payload = self.graphql(shop_domain, access_token, query, operation_name="GetLocations")
        nodes = payload["data"]["locations"]["nodes"]
        if not nodes:
            raise SyncProcessingError(
                "No Shopify locations are available for inventory sync.",
                {"shop": shop_domain},
                code="location_not_found",
                status_code=404,
            )

        with self._location_lock:
            self._location_cache[shop_domain] = (nodes[0]["id"], nodes[0]["name"])
        return nodes[0]["id"]

    def get_shop_info(self, shop_domain: str, access_token: str) -> Dict[str, Any]:
        query = """
        query ShopInfo {
          shop {
            name
            myshopifyDomain
          }
        }
        """
        payload = self.graphql(shop_domain, access_token, query, operation_name="ShopInfo")
        return payload["data"]["shop"] or {}

    def product_set(
        self,
        shop_domain: str,
        access_token: str,
        *,
        input_data: Dict[str, Any],
        identifier: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        mutation = """
        mutation ProductSetSync($input: ProductSetInput!, $synchronous: Boolean!, $identifier: ProductSetIdentifiers) {
          productSet(input: $input, synchronous: $synchronous, identifier: $identifier) {
            product {
              id
              title
              handle
              status
              vendor
              productType
              media(first: 10) {
                nodes {
                  alt
                  mediaContentType
                  status
                  ... on MediaImage {
                    image {
                      url
                    }
                  }
                }
              }
              variants(first: 100) {
                nodes {
                  id
                  title
                  sku
                  barcode
                  price
                  compareAtPrice
                  inventoryItem {
                    id
                    inventoryLevels(first: 25) {
                      nodes {
                        location {
                          id
                          name
                        }
                        quantities(names: ["available"]) {
                          name
                          quantity
                        }
                      }
                    }
                  }
                }
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        payload = self.graphql(
            shop_domain,
            access_token,
            mutation,
            {
                "input": input_data,
                "synchronous": True,
                "identifier": identifier,
            },
            operation_name="ProductSetSync",
        )
        result = payload["data"]["productSet"]
        user_errors = result.get("userErrors") or []
        if user_errors:
            raise ShopifyAPIError(
                "Shopify rejected the product sync request.",
                {
                    "identifier": identifier,
                    "user_errors": user_errors,
                },
            )
        product = result.get("product")
        if not product:
            raise ShopifyAPIError(
                "Shopify did not return a product after sync.",
                {"identifier": identifier},
            )
        return product

    def update_product(
        self,
        shop_domain: str,
        access_token: str,
        *,
        product: Dict[str, Any],
        media: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        mutation = """
        mutation UpdateProduct($product: ProductUpdateInput!, $media: [CreateMediaInput!]) {
          productUpdate(product: $product, media: $media) {
            product {
              id
              title
              handle
              status
              vendor
              productType
              media(first: 10) {
                nodes {
                  alt
                  mediaContentType
                  status
                  ... on MediaImage {
                    image {
                      url
                    }
                  }
                }
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        payload = self.graphql(
            shop_domain,
            access_token,
            mutation,
            {"product": product, "media": media or []},
            operation_name="UpdateProduct",
        )
        result = payload["data"]["productUpdate"]
        user_errors = result.get("userErrors") or []
        if user_errors:
            raise ShopifyAPIError(
                "Shopify rejected the product update.",
                {"product_id": product.get("id"), "user_errors": user_errors},
            )
        updated_product = result.get("product")
        if not updated_product:
            raise ShopifyAPIError(
                "Shopify did not return a product after update.",
                {"product_id": product.get("id")},
            )
        return updated_product

    def update_variant_fields(
        self,
        shop_domain: str,
        access_token: str,
        *,
        product_id: str,
        variant: Dict[str, Any],
    ) -> Dict[str, Any]:
        mutation = """
        mutation UpdateVariantFields($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants {
              id
              sku
              barcode
              price
              compareAtPrice
              inventoryItem {
                id
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        payload = self.graphql(
            shop_domain,
            access_token,
            mutation,
            {
                "productId": normalize_gid("Product", product_id),
                "variants": [variant],
            },
            operation_name="UpdateVariantFields",
        )
        result = payload["data"]["productVariantsBulkUpdate"]
        user_errors = result.get("userErrors") or []
        if user_errors:
            raise ShopifyAPIError(
                "Shopify rejected the variant update.",
                {"product_id": product_id, "variant": variant, "user_errors": user_errors},
            )
        updated_variants = result.get("productVariants") or []
        return updated_variants[0] if updated_variants else {}

    def update_variant_price(
        self,
        shop_domain: str,
        access_token: str,
        variant_id: str,
        price: float,
        *,
        product_id: Optional[str] = None,
    ) -> None:
        normalized_variant_id = normalize_gid("ProductVariant", variant_id)
        normalized_product_id = normalize_gid("Product", product_id) if product_id else None
        formatted_price = format_price(price)

        primary_mutation = """
        mutation UpdateVariantPrice($id: ID!, $price: Money!) {
          productVariantUpdate(input: {id: $id, price: $price}) {
            productVariant {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        try:
            payload = self.graphql(
                shop_domain,
                access_token,
                primary_mutation,
                {"id": normalized_variant_id, "price": float(formatted_price)},
                operation_name="UpdateVariantPrice",
            )
            result = payload["data"]["productVariantUpdate"]
            user_errors = result.get("userErrors") or []
            if user_errors:
                raise ShopifyAPIError(
                    "Shopify rejected the variant price update.",
                    {"variant_id": normalized_variant_id, "user_errors": user_errors},
                )
            return
        except ShopifyAPIError as exc:
            if not _should_fallback_variant_price_update(exc.details):
                raise

        fallback_product_id = normalized_product_id or self._get_product_id_for_variant(
            shop_domain,
            access_token,
            normalized_variant_id,
        )
        fallback_mutation = """
        mutation UpdateVariantPriceFallback($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants {
              id
              price
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        payload = self.graphql(
            shop_domain,
            access_token,
            fallback_mutation,
            {
                "productId": fallback_product_id,
                "variants": [{"id": normalized_variant_id, "price": float(formatted_price)}],
            },
            operation_name="UpdateVariantPriceFallback",
        )
        result = payload["data"]["productVariantsBulkUpdate"]
        user_errors = result.get("userErrors") or []
        if user_errors:
            raise ShopifyAPIError(
                "Shopify rejected the variant price update fallback.",
                {
                    "product_id": fallback_product_id,
                    "variant_id": normalized_variant_id,
                    "user_errors": user_errors,
                },
            )

    def update_inventory(
        self,
        shop_domain: str,
        access_token: str,
        inventory_item_id: str,
        location_id: str,
        quantity: int,
        *,
        change_from_quantity: Optional[int] = None,
        sku: Optional[str] = None,
    ) -> None:
        normalized_inventory_item_id = normalize_gid("InventoryItem", inventory_item_id)
        normalized_location_id = normalize_gid("Location", location_id)
        inventory_input = {
            "name": "available",
            "reason": "correction",
            "referenceDocumentUri": f"gid://inventory-sync-shopify/SyncJob/{uuid.uuid4()}",
            "quantities": [
                {
                    "inventoryItemId": normalized_inventory_item_id,
                    "locationId": normalized_location_id,
                    "quantity": quantity,
                }
            ],
        }

        if _api_version_at_least(self.settings.shopify_api_version, "2026-01"):
            mutation = """
            mutation SetInventory($input: InventorySetQuantitiesInput!, $idempotencyKey: String!) {
              inventorySetQuantities(input: $input) @idempotent(key: $idempotencyKey) {
                inventoryAdjustmentGroup {
                  id
                  reason
                }
                userErrors {
                  code
                  field
                  message
                }
              }
            }
            """
            inventory_input["quantities"][0]["changeFromQuantity"] = change_from_quantity
            variables = {
                "input": inventory_input,
                "idempotencyKey": str(uuid.uuid4()),
            }
        else:
            mutation = """
            mutation SetInventory($input: InventorySetQuantitiesInput!) {
              inventorySetQuantities(input: $input) {
                inventoryAdjustmentGroup {
                  id
                  reason
                }
                userErrors {
                  code
                  field
                  message
                }
              }
            }
            """
            if change_from_quantity is None:
                inventory_input["ignoreCompareQuantity"] = True
            else:
                inventory_input["quantities"][0]["compareQuantity"] = change_from_quantity
            variables = {"input": inventory_input}

        payload = self.graphql(
            shop_domain,
            access_token,
            mutation,
            variables,
            operation_name="SetInventory",
        )
        result = payload["data"]["inventorySetQuantities"]
        user_errors = result.get("userErrors") or []
        if user_errors:
            raise ShopifyAPIError(
                "Shopify rejected the inventory update.",
                {
                    "inventory_item_id": normalized_inventory_item_id,
                    "location_id": normalized_location_id,
                    "sku": sku,
                    "user_errors": user_errors,
                },
            )

    def activate_inventory(
        self,
        shop_domain: str,
        access_token: str,
        inventory_item_id: str,
        location_id: str,
    ) -> None:
        mutation = """
        mutation ActivateInventory($inventoryItemId: ID!, $locationId: ID!) {
          inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId) {
            inventoryLevel {
              id
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        payload = self.graphql(
            shop_domain,
            access_token,
            mutation,
            {
                "inventoryItemId": normalize_gid("InventoryItem", inventory_item_id),
                "locationId": normalize_gid("Location", location_id),
            },
            operation_name="ActivateInventory",
        )
        result = payload["data"]["inventoryActivate"]
        user_errors = result.get("userErrors") or []
        if user_errors:
            raise ShopifyAPIError(
                "Shopify could not activate inventory at the target location.",
                {
                    "inventory_item_id": normalize_gid("InventoryItem", inventory_item_id),
                    "location_id": normalize_gid("Location", location_id),
                    "user_errors": user_errors,
                },
            )

    def graphql(
        self,
        shop_domain: str,
        access_token: str,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        *,
        operation_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        base_url = f"https://{shop_domain}/admin/api/{self.settings.shopify_api_version}/graphql.json"
        last_error: Optional[Exception] = None

        for attempt in range(self.settings.shopify_retry_attempts):
            try:
                response = self.session.post(
                    base_url,
                    headers={
                        "Content-Type": "application/json",
                        "X-Shopify-Access-Token": access_token,
                    },
                    json={
                        "query": query,
                        "variables": variables or {},
                        "operationName": operation_name,
                    },
                    timeout=self.settings.shopify_request_timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt == self.settings.shopify_retry_attempts - 1:
                    raise ShopifyAPIError(
                        "Failed to reach Shopify GraphQL API.",
                        {"reason": str(exc), "operation_name": operation_name, "shop": shop_domain},
                    ) from exc
                self._sleep_before_retry(attempt)
                continue

            payload = _safe_response_json(response)

            if response.status_code == 401:
                raise AuthenticationError(
                    "Shopify rejected the stored access token.",
                    {
                        "operation_name": operation_name,
                        "shop": shop_domain,
                        "status_code": response.status_code,
                        "response": payload,
                    },
                )

            if response.status_code == 429:
                last_error = ShopifyAPIError(
                    "Shopify rate limited the request.",
                    {
                        "operation_name": operation_name,
                        "shop": shop_domain,
                        "status_code": response.status_code,
                        "response": payload,
                    },
                    status_code=429,
                )
                if attempt == self.settings.shopify_retry_attempts - 1:
                    raise last_error
                self._sleep_before_retry(
                    attempt,
                    retry_after_seconds=self._get_retry_delay_seconds(response, payload),
                )
                continue

            if response.status_code >= 500:
                last_error = ShopifyAPIError(
                    "Shopify returned a server error.",
                    {
                        "operation_name": operation_name,
                        "shop": shop_domain,
                        "status_code": response.status_code,
                        "response": payload,
                    },
                    status_code=502,
                )
                if attempt == self.settings.shopify_retry_attempts - 1:
                    raise last_error
                self._sleep_before_retry(
                    attempt,
                    retry_after_seconds=self._get_retry_delay_seconds(response, payload),
                )
                continue

            if response.status_code >= 400:
                raise ShopifyAPIError(
                    "Shopify request failed.",
                    {
                        "operation_name": operation_name,
                        "shop": shop_domain,
                        "status_code": response.status_code,
                        "response": payload,
                    },
                    status_code=response.status_code,
                )

            errors = payload.get("errors") or []
            if errors:
                if _is_throttled_graphql_error(errors):
                    last_error = ShopifyAPIError(
                        "Shopify GraphQL request was throttled.",
                        {"operation_name": operation_name, "shop": shop_domain, "errors": errors},
                        status_code=429,
                    )
                    if attempt == self.settings.shopify_retry_attempts - 1:
                        raise last_error
                    self._sleep_before_retry(
                        attempt,
                        retry_after_seconds=self._get_retry_delay_seconds(response, payload),
                    )
                    continue

                if _is_access_token_graphql_error(errors):
                    raise AuthenticationError(
                        "Shopify reported an access token error.",
                        {"operation_name": operation_name, "shop": shop_domain, "errors": errors},
                    )

                raise ShopifyAPIError(
                    "Shopify GraphQL request failed.",
                    {
                        "operation_name": operation_name,
                        "shop": shop_domain,
                        "errors": errors,
                        "response": payload,
                    },
                    status_code=502,
                )

            return payload

        if isinstance(last_error, ShopifyAPIError):
            raise last_error

        raise ShopifyAPIError(
            "Shopify request failed after retries.",
            {
                "operation_name": operation_name,
                "shop": shop_domain,
                "last_error": str(last_error) if last_error else None,
            },
        )

    def update_cached_variant(
        self,
        shop_domain: str,
        *,
        sku: str,
        price: float,
        quantity: int,
        location_id: str,
    ) -> None:
        cached = self._get_cached_variant(shop_domain, sku)
        if cached is None:
            return

        updated_levels: List[InventoryLevelSnapshot] = []
        location_found = False
        for level in cached.inventory_levels:
            if normalize_gid("Location", level.location_id) == normalize_gid("Location", location_id):
                updated_levels.append(
                    InventoryLevelSnapshot(
                        location_id=normalize_gid("Location", location_id),
                        location_name=level.location_name,
                        quantity=quantity,
                    )
                )
                location_found = True
            else:
                updated_levels.append(level)

        if not location_found:
            updated_levels.append(
                InventoryLevelSnapshot(
                    location_id=normalize_gid("Location", location_id),
                    location_name="Unknown",
                    quantity=quantity,
                )
            )

        cached.current_price = float(format_price(price))
        cached.inventory_levels = updated_levels
        self._set_cached_variant(shop_domain, cached)

    def _get_product_id_for_variant(
        self,
        shop_domain: str,
        access_token: str,
        variant_id: str,
    ) -> str:
        query = """
        query ProductIdForVariant($id: ID!) {
          node(id: $id) {
            ... on ProductVariant {
              id
              product {
                id
              }
            }
          }
        }
        """
        payload = self.graphql(
            shop_domain,
            access_token,
            query,
            {"id": variant_id},
            operation_name="ProductIdForVariant",
        )
        node = payload["data"]["node"]
        if not node or not node.get("product"):
            raise ShopifyAPIError(
                "Unable to resolve the Shopify product for the variant.",
                {"variant_id": variant_id, "shop": shop_domain},
            )
        return node["product"]["id"]

    def _sleep_before_retry(
        self,
        attempt: int,
        *,
        retry_after_seconds: Optional[float] = None,
    ) -> None:
        delay = get_backoff_delay(
            attempt,
            base_seconds=self.settings.shopify_retry_backoff_seconds,
            retry_after_seconds=retry_after_seconds,
        )
        self.logger.warning("shopify_retry attempt=%s delay=%.2fs", attempt + 1, delay)
        time.sleep(delay)

    def _get_retry_delay_seconds(
        self,
        response: requests.Response,
        payload: Dict[str, Any],
    ) -> Optional[float]:
        header_delay = parse_retry_after(response.headers.get("Retry-After"))
        if header_delay is not None:
            return header_delay

        extensions = payload.get("extensions") or {}
        cost = extensions.get("cost") or {}
        throttle_status = cost.get("throttleStatus") or {}
        currently_available = throttle_status.get("currentlyAvailable")
        restore_rate = throttle_status.get("restoreRate")
        requested_cost = cost.get("requestedQueryCost") or cost.get("actualQueryCost")

        if (
            isinstance(currently_available, (int, float))
            and isinstance(restore_rate, (int, float))
            and isinstance(requested_cost, (int, float))
            and restore_rate > 0
        ):
            deficit = max(float(requested_cost) - float(currently_available), 1.0)
            return max(deficit / float(restore_rate), 1.0)

        return None

    def _cache_key(self, shop_domain: str, sku: str) -> str:
        return f"{shop_domain}:{sku.strip()}"

    def _get_cached_variant(self, shop_domain: str, sku: str) -> Optional[VariantMapping]:
        with self._sku_cache_lock:
            entry = self._sku_cache.get(self._cache_key(shop_domain, sku))
            if not entry:
                return None

            if utc_now().timestamp() >= entry["expires_at"]:
                self._sku_cache.pop(self._cache_key(shop_domain, sku), None)
                return None

            return VariantMapping(**entry["value"])

    def _set_cached_variant(self, shop_domain: str, mapping: VariantMapping) -> None:
        with self._sku_cache_lock:
            self._sku_cache[self._cache_key(shop_domain, mapping.sku)] = {
                "value": mapping.dict(),
                "expires_at": utc_now().timestamp() + self.settings.shopify_sku_cache_ttl_seconds,
            }


def normalize_gid(resource_name: str, value: Optional[str]) -> str:
    if not value:
        raise SyncProcessingError(
            f"{resource_name} identifier is required.",
            {"resource_name": resource_name},
            code="missing_identifier",
        )
    normalized = str(value).strip()
    if normalized.startswith("gid://shopify/"):
        return normalized
    return f"gid://shopify/{resource_name}/{normalized}"


def extract_numeric_shopify_id(value: Optional[str | int]) -> Optional[int]:
    if value is None:
        return None

    normalized = str(value).strip()
    if not normalized:
        return None

    tail = normalized.rsplit("/", 1)[-1]
    if tail.isdigit():
        return int(tail)

    return None


def format_price(price: float) -> Decimal:
    return Decimal(str(price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def is_auth_error(exc: Exception) -> bool:
    if isinstance(exc, AuthenticationError):
        return True
    if not isinstance(exc, ShopifyAPIError):
        return False
    status_code = exc.details.get("status_code")
    if status_code == 401:
        return True
    errors = exc.details.get("errors") or []
    return _is_access_token_graphql_error(errors)


def _safe_response_json(response: requests.Response) -> Dict[str, Any]:
    try:
        return response.json()
    except ValueError:
        return {"raw": response.text}


def _parse_variant_mapping(node: Dict[str, Any]) -> VariantMapping:
    inventory_item = node.get("inventoryItem") or {}
    levels = []
    for level in inventory_item.get("inventoryLevels", {}).get("nodes", []):
        quantities = level.get("quantities") or []
        available_quantity = None
        if quantities:
            available_quantity = quantities[0].get("quantity")
        levels.append(
            InventoryLevelSnapshot(
                location_id=level["location"]["id"],
                location_name=level["location"]["name"],
                quantity=available_quantity,
            )
        )

    return VariantMapping(
        sku=node["sku"],
        variant_id=node["id"],
        product_id=node["product"]["id"],
        inventory_item_id=inventory_item["id"],
        current_price=float(node["price"]) if node.get("price") is not None else None,
        inventory_levels=levels,
    )


def _build_sku_search_query(sku: str) -> str:
    escaped = sku.replace("\\", "\\\\").replace('"', '\\"')
    return f'sku:"{escaped}"'


def _is_throttled_graphql_error(errors: List[Dict[str, Any]]) -> bool:
    for error in errors:
        message = str(error.get("message") or "").lower()
        if "throttled" in message:
            return True
        extensions = error.get("extensions") or {}
        code = str(extensions.get("code") or "").upper()
        if code == "THROTTLED":
            return True
    return False


def _is_access_token_graphql_error(errors: List[Dict[str, Any]]) -> bool:
    for error in errors:
        message = str(error.get("message") or "").lower()
        if "access token" in message or "invalid api key" in message:
            return True
    return False


def _should_fallback_variant_price_update(details: Dict[str, Any]) -> bool:
    errors = details.get("errors") or []
    response = details.get("response") or {}
    response_errors = response.get("errors") or []
    combined = list(errors) + list(response_errors)

    for error in combined:
        message = str(error.get("message") or "").lower()
        if "productvariantupdate" in message and (
            "doesn't exist" in message
            or "does not exist" in message
            or "undefined" in message
            or "deprecated" in message
        ):
            return True
    return False


def _parse_api_version(version: str) -> Tuple[int, int]:
    try:
        year, month = version.split("-", 1)
        return int(year), int(month)
    except (TypeError, ValueError):
        return (0, 0)


def _api_version_at_least(current_version: str, minimum_version: str) -> bool:
    return _parse_api_version(current_version) >= _parse_api_version(minimum_version)
