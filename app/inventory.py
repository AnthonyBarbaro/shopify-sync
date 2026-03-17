from typing import List, Optional

from app.config import Settings
from app.db import ShopRecord
from app.models import BulkSyncResponse, ProductSyncRequest, SyncResult, VariantMapping
from app.shopify import ShopifyClient, normalize_gid
from app.state import SyncActivityStore
from app.utils import (
    AppError,
    ShopifyAPIError,
    SyncProcessingError,
    has_user_error_code,
    log_sync_event,
    setup_logging,
    utc_now_iso,
)


class InventorySyncService:
    def __init__(
        self,
        shopify_client: ShopifyClient,
        settings: Settings,
        activity_store: SyncActivityStore,
    ) -> None:
        self.shopify_client = shopify_client
        self.settings = settings
        self.activity_store = activity_store
        self.logger = setup_logging().getChild("inventory")

    def sync_product(self, payload: ProductSyncRequest, shop: ShopRecord) -> SyncResult:
        sku = payload.sku.strip()
        if not sku:
            raise SyncProcessingError("SKU is required.", code="invalid_sku")

        mapping = None
        location_id = None
        price_updated = False
        inventory_updated = False

        try:
            mapping = self.shopify_client.get_variant_by_sku(shop.shop_domain, shop.access_token, sku)
            location_id = self._resolve_location_id(shop, mapping)
            current_quantity = self._get_inventory_quantity(mapping, location_id)

            if mapping.current_price is None or float(mapping.current_price) != float(payload.price):
                self.shopify_client.update_variant_price(
                    shop.shop_domain,
                    shop.access_token,
                    mapping.variant_id,
                    payload.price,
                    product_id=mapping.product_id,
                )
                price_updated = True

            if current_quantity != payload.quantity:
                try:
                    self.shopify_client.update_inventory(
                        shop.shop_domain,
                        shop.access_token,
                        mapping.inventory_item_id,
                        location_id,
                        payload.quantity,
                        change_from_quantity=current_quantity,
                        sku=sku,
                    )
                except ShopifyAPIError as exc:
                    if not has_user_error_code(exc.details, "CHANGE_FROM_QUANTITY_STALE"):
                        raise

                    refreshed_mapping = self.shopify_client.get_variant_by_sku(
                        shop.shop_domain,
                        shop.access_token,
                        sku,
                        force_refresh=True,
                    )
                    location_id = self._resolve_location_id(shop, refreshed_mapping)
                    latest_quantity = self._get_inventory_quantity(refreshed_mapping, location_id)
                    self.shopify_client.update_inventory(
                        shop.shop_domain,
                        shop.access_token,
                        refreshed_mapping.inventory_item_id,
                        location_id,
                        payload.quantity,
                        change_from_quantity=latest_quantity,
                        sku=sku,
                    )
                    mapping = refreshed_mapping

                inventory_updated = True

            self.shopify_client.update_cached_variant(
                shop.shop_domain,
                sku=sku,
                price=payload.price,
                quantity=payload.quantity,
                location_id=location_id,
            )
        except Exception as exc:
            details = exc.details if isinstance(exc, AppError) else {"reason": str(exc)}
            failure_result = SyncResult(
                shop_domain=shop.shop_domain,
                sku=sku,
                success=False,
                message=str(exc),
                timestamp=utc_now_iso(),
                variant_id=mapping.variant_id if mapping else None,
                product_id=mapping.product_id if mapping else None,
                inventory_item_id=mapping.inventory_item_id if mapping else None,
                location_id=normalize_gid("Location", location_id) if location_id else None,
                price=payload.price,
                quantity=payload.quantity,
                price_updated=price_updated,
                inventory_updated=inventory_updated,
                details=details,
            )
            self.activity_store.record(failure_result)
            log_sync_event(
                self.logger,
                sku=sku,
                success=False,
                message="sync_failed",
                shop=shop.shop_domain,
                error=str(exc),
            )
            raise

        result = SyncResult(
            shop_domain=shop.shop_domain,
            sku=sku,
            success=True,
            message="Sync completed successfully.",
            timestamp=utc_now_iso(),
            variant_id=mapping.variant_id,
            product_id=mapping.product_id,
            inventory_item_id=mapping.inventory_item_id,
            location_id=normalize_gid("Location", location_id),
            price_updated=price_updated,
            inventory_updated=inventory_updated,
            price=payload.price,
            quantity=payload.quantity,
            details={
                "requested_price": payload.price,
                "requested_quantity": payload.quantity,
            },
        )
        self.activity_store.record(result)
        log_sync_event(
            self.logger,
            sku=sku,
            success=True,
            message="sync_completed",
            shop=shop.shop_domain,
            variant_id=mapping.variant_id,
            location_id=result.location_id,
            price_updated=price_updated,
            inventory_updated=inventory_updated,
        )
        return result

    def sync_bulk(self, products: List[ProductSyncRequest], shop: ShopRecord) -> BulkSyncResponse:
        if not products:
            raise SyncProcessingError(
                "Bulk sync request must include at least one product.",
                code="empty_bulk_request",
            )

        results: List[SyncResult] = []
        succeeded = 0

        for product in products:
            try:
                result = self.sync_product(product, shop)
                results.append(result)
                succeeded += 1
            except Exception as exc:
                details = exc.details if isinstance(exc, AppError) else {}
                results.append(
                    SyncResult(
                        shop_domain=shop.shop_domain,
                        sku=product.sku.strip(),
                        success=False,
                        message=str(exc),
                        timestamp=utc_now_iso(),
                        price=product.price,
                        quantity=product.quantity,
                        details=details,
                    )
                )

        return BulkSyncResponse(
            total=len(products),
            succeeded=succeeded,
            failed=len(products) - succeeded,
            timestamp=utc_now_iso(),
            results=results,
        )

    def _resolve_location_id(self, shop: ShopRecord, mapping: VariantMapping) -> str:
        if self.settings.shopify_location_id:
            return normalize_gid("Location", self.settings.shopify_location_id)

        if mapping.inventory_levels:
            return normalize_gid("Location", mapping.inventory_levels[0].location_id)

        return self.shopify_client.get_primary_location_id(shop.shop_domain, shop.access_token)

    @staticmethod
    def _get_inventory_quantity(mapping: VariantMapping, location_id: str) -> Optional[int]:
        normalized_location_id = normalize_gid("Location", location_id)
        for level in mapping.inventory_levels:
            if normalize_gid("Location", level.location_id) == normalized_location_id:
                return level.quantity
        return None
