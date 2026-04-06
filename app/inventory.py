from pathlib import Path
from typing import Any, Dict, List, Optional

from app.config import Settings
from app.db import ShopRecord
from app.models import (
    BulkSyncResponse,
    CatalogProductRecord,
    ProductImageInput,
    ProductSyncRequest,
    SyncResult,
    VariantMapping,
)
from app.shopify import ShopifyClient, format_price, normalize_gid
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
        normalized = self._normalize_payload(payload)
        display_sku = normalized.sku or normalized.handle or normalized.title or "unknown-product"

        try:
            result = self._sync_catalog_product(normalized, shop)
        except Exception as exc:
            details = exc.details if isinstance(exc, AppError) else {"reason": str(exc)}
            failure_result = SyncResult(
                shop_domain=shop.shop_domain,
                sku=display_sku,
                success=False,
                message=str(exc),
                timestamp=utc_now_iso(),
                price=normalized.price,
                quantity=normalized.quantity,
                details=details,
            )
            self.activity_store.record(failure_result)
            log_sync_event(
                self.logger,
                sku=display_sku,
                success=False,
                message="sync_failed",
                shop=shop.shop_domain,
                error=str(exc),
                title=normalized.title,
            )
            raise

        self.activity_store.record(result)
        log_sync_event(
            self.logger,
            sku=result.sku,
            success=True,
            message="sync_completed",
            shop=shop.shop_domain,
            variant_id=result.variant_id,
            product_id=result.product_id,
            location_id=result.location_id,
            price_updated=result.price_updated,
            inventory_updated=result.inventory_updated,
            title=result.details.get("product_title"),
            created=result.details.get("created"),
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
                normalized = self._normalize_payload(product)
                details = exc.details if isinstance(exc, AppError) else {}
                results.append(
                    SyncResult(
                        shop_domain=shop.shop_domain,
                        sku=normalized.sku or normalized.handle or normalized.title or "unknown-product",
                        success=False,
                        message=str(exc),
                        timestamp=utc_now_iso(),
                        price=normalized.price,
                        quantity=normalized.quantity,
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

    def list_catalog(self, shop: ShopRecord) -> List[CatalogProductRecord]:
        products = self.shopify_client.get_products(shop.shop_domain, shop.access_token)
        rows: List[CatalogProductRecord] = []

        for product in products:
            media_nodes = ((product.get("media") or {}).get("nodes") or [])
            primary_image = _extract_first_media_url(media_nodes)
            variant_nodes = ((product.get("variants") or {}).get("nodes") or []) or [None]

            for variant in variant_nodes:
                quantity = None
                variant_id = None
                sku = None
                barcode = None
                price = None

                if variant is not None:
                    variant_id = variant.get("id")
                    sku = variant.get("sku")
                    barcode = variant.get("barcode")
                    price = _safe_float(variant.get("price"))
                    quantity = _extract_available_quantity(variant.get("inventoryItem") or {})

                rows.append(
                    CatalogProductRecord(
                        product_id=product["id"],
                        variant_id=variant_id,
                        title=product.get("title") or "Untitled product",
                        handle=product.get("handle"),
                        status=product.get("status"),
                        sku=sku,
                        barcode=barcode,
                        price=price,
                        quantity=quantity,
                        vendor=product.get("vendor"),
                        product_type=product.get("productType"),
                        image_url=primary_image,
                        updated_at=product.get("updatedAt"),
                    )
                )

        return rows

    def list_woo_catalog(self, shop: ShopRecord) -> List[CatalogProductRecord]:
        products = self.shopify_client.get_products(shop.shop_domain, shop.access_token)
        return [self._catalog_record_from_product(product) for product in products]

    def get_woo_catalog_product(self, shop: ShopRecord, product_id: str | int) -> CatalogProductRecord:
        product = self.shopify_client.get_product_by_id(
            shop.shop_domain,
            shop.access_token,
            product_id,
        )
        if product is None:
            raise SyncProcessingError(
                "Product not found.",
                {"product_id": str(product_id)},
                status_code=404,
                code="product_not_found",
            )
        return self._catalog_record_from_product(product)

    def _sync_catalog_product(self, payload: ProductSyncRequest, shop: ShopRecord) -> SyncResult:
        existing_mapping = self._find_existing_mapping(shop, payload)
        media_inputs = self._build_media_inputs(payload)
        created = existing_mapping is None
        product_title = payload.title or payload.sku or "POS Imported Product"
        mapping = existing_mapping
        price_updated = False
        inventory_updated = False
        product_status = None

        if created:
            product = self.shopify_client.product_set(
                shop.shop_domain,
                shop.access_token,
                input_data=self._build_new_product_input(payload, media_inputs),
            )
            product_status = product.get("status") or _normalize_product_status(payload.status, default="DRAFT")
            mapping = self._mapping_from_product(product, payload.sku)
        else:
            product_update = self._build_product_update_input(payload, existing_mapping.product_id)
            if len(product_update) > 1 or media_inputs:
                updated_product = self.shopify_client.update_product(
                    shop.shop_domain,
                    shop.access_token,
                    product=product_update,
                    media=media_inputs,
                )
                product_title = updated_product.get("title") or product_title
                product_status = updated_product.get("status")

            variant_update = self._build_variant_update_input(payload, existing_mapping)
            if len(variant_update) > 1:
                self.shopify_client.update_variant_fields(
                    shop.shop_domain,
                    shop.access_token,
                    product_id=existing_mapping.product_id,
                    variant=variant_update,
                )
                price_updated = payload.price is not None and existing_mapping.current_price != float(payload.price)

        if mapping is None:
            raise SyncProcessingError(
                "Could not resolve the Shopify product variant for this sync.",
                code="variant_resolution_failed",
            )

        location_id = self._resolve_location_id(shop, mapping)
        current_quantity = self._get_inventory_quantity(mapping, location_id)
        inventory_item_id = mapping.inventory_item_id

        if payload.quantity is not None and current_quantity != payload.quantity:
            inventory_updated = self._set_inventory_with_retries(
                shop=shop,
                mapping=mapping,
                location_id=location_id,
                quantity=payload.quantity,
                change_from_quantity=current_quantity,
                sku=payload.sku,
            )

        if payload.sku:
            cached_quantity = payload.quantity if payload.quantity is not None else (current_quantity or 0)
            cached_price = payload.price if payload.price is not None else (mapping.current_price or 0.0)
            self.shopify_client.update_cached_variant(
                shop.shop_domain,
                sku=payload.sku,
                price=cached_price,
                quantity=cached_quantity,
                location_id=location_id,
            )

        return SyncResult(
            shop_domain=shop.shop_domain,
            sku=payload.sku or payload.handle or product_title,
            success=True,
            message="Product synced successfully.",
            timestamp=utc_now_iso(),
            variant_id=mapping.variant_id,
            product_id=mapping.product_id,
            inventory_item_id=inventory_item_id,
            location_id=normalize_gid("Location", location_id),
            price_updated=price_updated,
            inventory_updated=inventory_updated,
            price=payload.price,
            quantity=payload.quantity,
            details={
                "created": created,
                "product_title": product_title,
                "product_status": product_status or _normalize_product_status(payload.status, default="DRAFT"),
                "image_count": len(media_inputs),
                "requested_price": payload.price,
                "requested_quantity": payload.quantity,
            },
        )

    def _find_existing_mapping(self, shop: ShopRecord, payload: ProductSyncRequest) -> Optional[VariantMapping]:
        if payload.external_id:
            product = self.shopify_client.get_product_by_id(
                shop.shop_domain,
                shop.access_token,
                payload.external_id,
            )
            if product:
                return self._mapping_from_product(product, payload.sku)

        if payload.sku:
            try:
                return self.shopify_client.get_variant_by_sku(
                    shop.shop_domain,
                    shop.access_token,
                    payload.sku,
                )
            except SyncProcessingError as exc:
                if exc.code != "sku_not_found":
                    raise

        if payload.handle:
            product = self.shopify_client.get_product_by_handle(
                shop.shop_domain,
                shop.access_token,
                payload.handle,
            )
            if product:
                variants = ((product.get("variants") or {}).get("nodes") or [])
                if len(variants) == 1:
                    return self._mapping_from_product(product, payload.sku)
                raise SyncProcessingError(
                    "A product handle was found, but it has multiple variants. Send a SKU to update the correct variant.",
                    {"handle": payload.handle},
                    code="ambiguous_variant_update",
                )

        return None

    def _build_new_product_input(
        self,
        payload: ProductSyncRequest,
        media_inputs: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        title = payload.title or payload.sku or "POS Imported Product"
        variant_input: Dict[str, Any] = {
            "optionValues": [{"optionName": "Title", "name": "Default Title"}],
        }

        if payload.price is not None:
            variant_input["price"] = float(format_price(payload.price))
        if payload.compare_at_price is not None:
            variant_input["compareAtPrice"] = float(format_price(payload.compare_at_price))
        if payload.barcode:
            variant_input["barcode"] = payload.barcode
        if payload.sku or payload.tracked is not None:
            inventory_item: Dict[str, Any] = {}
            if payload.sku:
                inventory_item["sku"] = payload.sku
            if payload.tracked is not None:
                inventory_item["tracked"] = bool(payload.tracked)
            if inventory_item:
                variant_input["inventoryItem"] = inventory_item
        if media_inputs:
            variant_input["file"] = dict(media_inputs[0])

        product_input: Dict[str, Any] = {
            "title": title,
            "status": _normalize_product_status(payload.status, default="DRAFT"),
            "productOptions": [{"name": "Title", "values": [{"name": "Default Title"}]}],
            "variants": [variant_input],
        }
        if payload.handle:
            product_input["handle"] = payload.handle
        description_html = payload.description_html or payload.description or payload.short_description
        if description_html:
            product_input["descriptionHtml"] = description_html
        vendor = payload.vendor or payload.brand
        if vendor:
            product_input["vendor"] = vendor
        if payload.product_type:
            product_input["productType"] = payload.product_type
        if payload.tags:
            product_input["tags"] = payload.tags
        if media_inputs:
            product_input["files"] = media_inputs
        return product_input

    def _build_product_update_input(
        self,
        payload: ProductSyncRequest,
        product_id: str,
    ) -> Dict[str, Any]:
        product_update: Dict[str, Any] = {"id": normalize_gid("Product", product_id)}
        if payload.title:
            product_update["title"] = payload.title
        if payload.handle:
            product_update["handle"] = payload.handle
        description_html = payload.description_html or payload.description or payload.short_description
        if description_html:
            product_update["descriptionHtml"] = description_html
        vendor = payload.vendor or payload.brand
        if vendor:
            product_update["vendor"] = vendor
        if payload.product_type:
            product_update["productType"] = payload.product_type
        if payload.tags:
            product_update["tags"] = payload.tags
        if payload.status:
            product_update["status"] = _normalize_product_status(payload.status, default="ACTIVE")
        return product_update

    def _build_variant_update_input(
        self,
        payload: ProductSyncRequest,
        mapping: VariantMapping,
    ) -> Dict[str, Any]:
        variant_update: Dict[str, Any] = {"id": normalize_gid("ProductVariant", mapping.variant_id)}
        if payload.price is not None:
            variant_update["price"] = float(format_price(payload.price))
        if payload.compare_at_price is not None:
            variant_update["compareAtPrice"] = float(format_price(payload.compare_at_price))
        if payload.barcode:
            variant_update["barcode"] = payload.barcode

        inventory_item: Dict[str, Any] = {}
        if payload.sku:
            inventory_item["sku"] = payload.sku
        if payload.tracked is not None:
            inventory_item["tracked"] = bool(payload.tracked)
        if inventory_item:
            variant_update["inventoryItem"] = inventory_item
        return variant_update

    def _build_media_inputs(self, payload: ProductSyncRequest) -> List[Dict[str, Any]]:
        media_inputs: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        image_candidates: List[ProductImageInput] = list(payload.images)

        if payload.image_url:
            image_candidates.append(ProductImageInput(src=payload.image_url))
        for url in payload.image_urls:
            image_candidates.append(ProductImageInput(src=url))

        for image in image_candidates:
            source = (image.src or image.url or "").strip()
            if not source or source in seen_urls:
                continue
            seen_urls.add(source)

            filename = image.filename or Path(source.split("?", 1)[0]).name or f"product-{len(media_inputs) + 1}.jpg"
            media_inputs.append(
                {
                    "originalSource": source,
                    "alt": image.alt or payload.title or payload.sku or "Product image",
                    "filename": filename,
                    "contentType": (image.content_type or "IMAGE").upper(),
                }
            )
        return media_inputs

    def _set_inventory_with_retries(
        self,
        *,
        shop: ShopRecord,
        mapping: VariantMapping,
        location_id: str,
        quantity: int,
        change_from_quantity: Optional[int],
        sku: Optional[str],
    ) -> bool:
        try:
            self.shopify_client.update_inventory(
                shop.shop_domain,
                shop.access_token,
                mapping.inventory_item_id,
                location_id,
                quantity,
                change_from_quantity=change_from_quantity,
                sku=sku,
            )
            return True
        except ShopifyAPIError as exc:
            if has_user_error_code(exc.details, "CHANGE_FROM_QUANTITY_STALE"):
                refreshed_mapping = self.shopify_client.get_variant_by_sku(
                    shop.shop_domain,
                    shop.access_token,
                    sku or mapping.sku,
                    force_refresh=True,
                )
                latest_location_id = self._resolve_location_id(shop, refreshed_mapping)
                latest_quantity = self._get_inventory_quantity(refreshed_mapping, latest_location_id)
                self.shopify_client.update_inventory(
                    shop.shop_domain,
                    shop.access_token,
                    refreshed_mapping.inventory_item_id,
                    latest_location_id,
                    quantity,
                    change_from_quantity=latest_quantity,
                    sku=sku or mapping.sku,
                )
                return True

            if _inventory_needs_activation(exc.details):
                self.shopify_client.activate_inventory(
                    shop.shop_domain,
                    shop.access_token,
                    mapping.inventory_item_id,
                    location_id,
                )
                self.shopify_client.update_inventory(
                    shop.shop_domain,
                    shop.access_token,
                    mapping.inventory_item_id,
                    location_id,
                    quantity,
                    change_from_quantity=change_from_quantity,
                    sku=sku,
                )
                return True
            raise

    def _mapping_from_product(
        self,
        product: Dict[str, Any],
        sku: Optional[str],
    ) -> VariantMapping:
        variants = ((product.get("variants") or {}).get("nodes") or [])
        if not variants:
            raise SyncProcessingError(
                "Shopify returned a product without variants.",
                {"product_id": product.get("id")},
                code="missing_variant",
            )

        target_variant = None
        normalized_sku = (sku or "").strip()
        for variant in variants:
            if normalized_sku and (variant.get("sku") or "").strip() == normalized_sku:
                target_variant = variant
                break
        if target_variant is None:
            target_variant = variants[0]

        levels = []
        inventory_item = target_variant.get("inventoryItem") or {}
        inventory_levels = ((inventory_item.get("inventoryLevels") or {}).get("nodes") or [])
        for level in inventory_levels:
            quantities = level.get("quantities") or []
            quantity = quantities[0].get("quantity") if quantities else None
            levels.append(
                {
                    "location_id": level["location"]["id"],
                    "location_name": level["location"]["name"],
                    "quantity": quantity,
                }
            )

        return VariantMapping(
            sku=target_variant.get("sku") or normalized_sku or product.get("handle") or product.get("title") or "product",
            variant_id=target_variant["id"],
            product_id=product["id"],
            inventory_item_id=inventory_item["id"],
            current_price=_safe_float(target_variant.get("price")),
            inventory_levels=levels,
        )

    def _normalize_payload(self, payload: ProductSyncRequest) -> ProductSyncRequest:
        tags = [tag.strip() for tag in payload.tags if str(tag).strip()]
        title = (payload.title or payload.name or "").strip() or None
        sku = (payload.sku or "").strip() or None
        handle = (payload.handle or "").strip() or None
        barcode = (payload.barcode or "").strip() or None
        product_type = (payload.product_type or "").strip() or None
        vendor = (payload.vendor or payload.brand or "").strip() or None
        return payload.model_copy(
            update={
                "sku": sku,
                "external_id": (payload.external_id or "").strip() or None,
                "title": title,
                "handle": handle,
                "barcode": barcode,
                "product_type": product_type,
                "vendor": vendor,
                "tags": tags,
            }
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

    def _catalog_record_from_product(
        self,
        product: Dict[str, Any],
        *,
        sku: Optional[str] = None,
    ) -> CatalogProductRecord:
        media_nodes = ((product.get("media") or {}).get("nodes") or [])
        primary_image = _extract_first_media_url(media_nodes)
        variants = ((product.get("variants") or {}).get("nodes") or [])

        target_variant = None
        normalized_sku = (sku or "").strip()
        for variant in variants:
            if normalized_sku and (variant.get("sku") or "").strip() == normalized_sku:
                target_variant = variant
                break
        if target_variant is None and variants:
            target_variant = variants[0]

        quantity = None
        variant_id = None
        variant_sku = None
        barcode = None
        price = None

        if target_variant is not None:
            variant_id = target_variant.get("id")
            variant_sku = target_variant.get("sku")
            barcode = target_variant.get("barcode")
            price = _safe_float(target_variant.get("price"))
            quantity = _extract_available_quantity(target_variant.get("inventoryItem") or {})

        return CatalogProductRecord(
            product_id=product["id"],
            variant_id=variant_id,
            title=product.get("title") or "Untitled product",
            handle=product.get("handle"),
            status=product.get("status"),
            sku=variant_sku,
            barcode=barcode,
            price=price,
            quantity=quantity,
            vendor=product.get("vendor"),
            product_type=product.get("productType"),
            image_url=primary_image,
            updated_at=product.get("updatedAt"),
        )


def _normalize_product_status(value: Optional[str], *, default: str) -> str:
    normalized = (value or "").strip().upper()
    if normalized in {"ACTIVE", "ARCHIVED", "DRAFT"}:
        return normalized
    return default


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_available_quantity(inventory_item: Dict[str, Any]) -> Optional[int]:
    levels = ((inventory_item.get("inventoryLevels") or {}).get("nodes") or [])
    for level in levels:
        quantities = level.get("quantities") or []
        if quantities:
            return quantities[0].get("quantity")
    return None


def _extract_first_media_url(media_nodes: List[Dict[str, Any]]) -> Optional[str]:
    for node in media_nodes:
        image = (node.get("image") or {}) if isinstance(node, dict) else {}
        url = image.get("url")
        if url:
            return url
    return None


def _inventory_needs_activation(details: Dict[str, Any]) -> bool:
    for user_error in details.get("user_errors") or []:
        code = str(user_error.get("code") or "").upper()
        message = str(user_error.get("message") or "").lower()
        if "NOT_STOCKED" in code or "not stocked" in message:
            return True
    return False
