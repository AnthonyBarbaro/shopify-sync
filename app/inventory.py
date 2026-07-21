import json
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.config import Settings
from app.db import ShopRecord
from app.models import (
    BulkSyncResponse,
    CatalogProductRecord,
    CustomerAddressInput,
    CustomerBulkSyncResponse,
    CustomerMetafieldInput,
    CustomerSyncRequest,
    CustomerSyncResult,
    ProductImageInput,
    ProductMetafieldInput,
    ProductSyncRequest,
    SyncResult,
    VariantMapping,
)
from app.shopify import ShopifyClient, format_price, normalize_gid
from app.state import SyncActivityStore
from app.utils import (
    AppError,
    AuthenticationError,
    ShopifyAPIError,
    SyncProcessingError,
    has_user_error_code,
    log_sync_event,
    setup_logging,
    utc_now_iso,
)


CUSTOMER_CUSTOM_ID_NAMESPACE = "pos"
CUSTOMER_CUSTOM_ID_KEY = "legacy_customer_id"
MATRIX_VARIANT_SKU_RE = re.compile(r"^(.+?)\.\s*\d+\s+\d+$")


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
        self._customer_custom_id_ready: set[str] = set()

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
                cost=normalized.cost,
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
            cost_updated=result.cost_updated,
            inventory_updated=result.inventory_updated,
            title=result.details.get("product_title"),
            created=result.details.get("created"),
        )
        return result

    def sync_bulk(
        self,
        products: List[ProductSyncRequest],
        shop: ShopRecord,
        *,
        workers: int = 1,
    ) -> BulkSyncResponse:
        if not products:
            raise SyncProcessingError(
                "Bulk sync request must include at least one product.",
                code="empty_bulk_request",
            )

        configured_max = max(1, int(getattr(self.settings, "shopify_bulk_max_workers", 4)))
        worker_count = min(max(1, int(workers)), configured_max, len(products))
        if worker_count == 1:
            results = [self._sync_bulk_product(product, shop) for product in products]
        else:
            with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="shopify-sync") as executor:
                results = list(executor.map(lambda product: self._sync_bulk_product(product, shop), products))

        succeeded = sum(1 for result in results if result.success)

        return BulkSyncResponse(
            total=len(products),
            succeeded=succeeded,
            failed=len(products) - succeeded,
            timestamp=utc_now_iso(),
            results=results,
        )

    def _sync_bulk_product(self, product: ProductSyncRequest, shop: ShopRecord) -> SyncResult:
        try:
            return self.sync_product(product, shop)
        except Exception as exc:
            normalized = self._normalize_payload(product)
            details = exc.details if isinstance(exc, AppError) else {}
            return SyncResult(
                shop_domain=shop.shop_domain,
                sku=normalized.sku or normalized.handle or normalized.title or "unknown-product",
                success=False,
                message=str(exc),
                timestamp=utc_now_iso(),
                price=normalized.price,
                cost=normalized.cost,
                quantity=normalized.quantity,
                details=details,
            )

    def sync_customer(self, payload: CustomerSyncRequest, shop: ShopRecord) -> CustomerSyncResult:
        normalized = self._normalize_customer_payload(payload)
        display_name = _customer_display_name(normalized)

        try:
            result = self._sync_customer_record(normalized, shop)
        except AuthenticationError:
            raise
        except Exception as exc:
            details = exc.details if isinstance(exc, AppError) else {"reason": str(exc)}
            log_sync_event(
                self.logger,
                sku=normalized.pos_customer_number or normalized.email or normalized.phone or "unknown-customer",
                success=False,
                message="customer_sync_failed",
                shop=shop.shop_domain,
                error=str(exc),
                title=display_name,
            )
            return CustomerSyncResult(
                shop_domain=shop.shop_domain,
                pos_customer_number=normalized.pos_customer_number,
                source=normalized.source,
                success=False,
                message=str(exc),
                timestamp=utc_now_iso(),
                email=normalized.email,
                phone=normalized.phone,
                name=display_name,
                details=details,
            )

        log_sync_event(
            self.logger,
            sku=result.pos_customer_number or result.email or result.phone or "unknown-customer",
            success=True,
            message="customer_sync_completed",
            shop=shop.shop_domain,
            title=result.name,
            customer_id=result.customer_id,
        )
        return result

    def sync_customers_bulk(self, customers: List[CustomerSyncRequest], shop: ShopRecord) -> CustomerBulkSyncResponse:
        if not customers:
            raise SyncProcessingError(
                "Customer bulk sync request must include at least one customer.",
                code="empty_customer_bulk_request",
            )

        results = [self.sync_customer(customer, shop) for customer in customers]
        succeeded = sum(1 for result in results if result.success)
        return CustomerBulkSyncResponse(
            total=len(customers),
            succeeded=succeeded,
            failed=len(customers) - succeeded,
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
                cost = None

                if variant is not None:
                    variant_id = variant.get("id")
                    sku = variant.get("sku")
                    barcode = variant.get("barcode")
                    price = _safe_float(variant.get("price"))
                    inventory_item = variant.get("inventoryItem") or {}
                    cost = _extract_unit_cost(inventory_item)
                    quantity = _extract_available_quantity(inventory_item)

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
                        cost=cost,
                        quantity=quantity,
                        vendor=product.get("vendor"),
                        product_type=product.get("productType"),
                        image_url=primary_image,
                        updated_at=product.get("updatedAt"),
                    )
                )

        return rows

    def list_inventory_snapshot(self, shop: ShopRecord) -> List[Dict[str, Any]]:
        return [
            {
                "sku": row.sku,
                "quantity": int(row.quantity or 0),
            }
            for row in self.list_catalog(shop)
            if row.sku and row.quantity is not None
        ]

    def adjust_inventory_quantity(
        self,
        *,
        sku: str,
        delta: int,
        idempotency_key: str,
        shop: ShopRecord,
    ) -> Dict[str, Any]:
        normalized_sku = sku.strip()
        if not normalized_sku:
            raise SyncProcessingError("Inventory adjustment requires a SKU.", code="missing_adjustment_sku")
        if not delta:
            return {"sku": normalized_sku, "delta": 0, "success": True}

        mapping = self.shopify_client.get_variant_by_sku(
            shop.shop_domain,
            shop.access_token,
            normalized_sku,
        )
        location_id = self._resolve_location_id(shop, mapping)
        self.shopify_client.adjust_inventory(
            shop.shop_domain,
            shop.access_token,
            mapping.inventory_item_id,
            location_id,
            int(delta),
            idempotency_key=idempotency_key,
            sku=normalized_sku,
        )
        return {
            "sku": normalized_sku,
            "delta": int(delta),
            "success": True,
            "variant_id": mapping.variant_id,
            "inventory_item_id": mapping.inventory_item_id,
            "location_id": normalize_gid("Location", location_id),
        }

    def list_woo_catalog(self, shop: ShopRecord) -> List[CatalogProductRecord]:
        products = self.shopify_client.get_products(shop.shop_domain, shop.access_token)
        return [self._catalog_record_from_product(product) for product in products]

    def reconcile_catalog_skus(
        self,
        source_skus: List[str],
        shop: ShopRecord,
        *,
        apply: bool = False,
    ) -> Dict[str, Any]:
        normalized_source_skus = {
            sku.strip().casefold()
            for sku in source_skus
            if isinstance(sku, str) and sku.strip()
        }
        if not normalized_source_skus:
            raise SyncProcessingError(
                "Catalog reconciliation requires at least one source SKU.",
                code="empty_reconciliation_source",
            )

        products = self.shopify_client.get_products(shop.shop_domain, shop.access_token)
        candidates: List[Dict[str, Any]] = []
        matched_products = 0
        already_archived = 0
        skipped_without_sku = 0
        skipped_unmanaged = 0
        skipped_truncated_variants = 0

        for product in products:
            variants_connection = product.get("variants") or {}
            if (variants_connection.get("pageInfo") or {}).get("hasNextPage"):
                # Never archive when Shopify returned only part of a large variant set.
                skipped_truncated_variants += 1
                continue

            product_skus = sorted(
                {
                    str(variant.get("sku") or "").strip()
                    for variant in (variants_connection.get("nodes") or [])
                    if str(variant.get("sku") or "").strip()
                },
                key=str.casefold,
            )
            if not product_skus:
                skipped_without_sku += 1
                continue
            managed_sku = str((product.get("metafield") or {}).get("value") or "").strip()
            product_source_skus = {
                (_matrix_base_sku(sku) or sku).casefold()
                for sku in product_skus
            }
            if not managed_sku or managed_sku.casefold() not in product_source_skus:
                # Only reconcile products that a prior rich POS sync marked as managed.
                skipped_unmanaged += 1
                continue
            if managed_sku.casefold() in normalized_source_skus or any(
                sku.casefold() in normalized_source_skus for sku in product_skus
            ):
                matched_products += 1
                continue

            current_status = str(product.get("status") or "").upper()
            if current_status == "ARCHIVED":
                already_archived += 1
                continue

            candidates.append(
                {
                    "product_id": product.get("id"),
                    "title": product.get("title") or "Untitled product",
                    "status": current_status or None,
                    "skus": product_skus,
                }
            )

        archived = 0
        failures: List[Dict[str, Any]] = []
        if apply:
            for candidate in candidates:
                try:
                    self.shopify_client.update_product(
                        shop.shop_domain,
                        shop.access_token,
                        product={
                            "id": normalize_gid("Product", candidate["product_id"]),
                            "status": "ARCHIVED",
                        },
                    )
                    archived += 1
                except Exception as exc:
                    failures.append(
                        {
                            **candidate,
                            "error": str(exc),
                        }
                    )

        return {
            "shop": shop.shop_domain,
            "apply": apply,
            "source_sku_count": len(normalized_source_skus),
            "shopify_product_count": len(products),
            "matched_product_count": matched_products,
            "candidate_count": len(candidates),
            "archived_count": archived,
            "failed_count": len(failures),
            "already_archived_count": already_archived,
            "skipped_without_sku_count": skipped_without_sku,
            "skipped_unmanaged_count": skipped_unmanaged,
            "skipped_truncated_variants_count": skipped_truncated_variants,
            "candidates": candidates,
            "failures": failures,
            "timestamp": utc_now_iso(),
        }

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

    def _sync_customer_record(self, payload: CustomerSyncRequest, shop: ShopRecord) -> CustomerSyncResult:
        custom_id = _customer_custom_id_value(payload)
        if custom_id:
            self._ensure_customer_custom_id_definition(shop)

        customer_input = self._build_customer_set_input(payload)
        identifier = _customer_identifier(payload, custom_id=custom_id)
        fallback_used = False

        try:
            customer = self.shopify_client.customer_set(
                shop.shop_domain,
                shop.access_token,
                identifier=identifier,
                input_data=customer_input,
            )
        except ShopifyAPIError as exc:
            fallback_identifier = _customer_contact_identifier(payload)
            if not custom_id or fallback_identifier is None or not _is_customer_identity_conflict(exc.details):
                raise
            customer = self.shopify_client.customer_set(
                shop.shop_domain,
                shop.access_token,
                identifier=fallback_identifier,
                input_data=customer_input,
            )
            fallback_used = True

        customer_id = customer["id"]
        metafields = self._build_customer_metafields(payload, customer_id, custom_id=custom_id)
        if metafields:
            self.shopify_client.set_customer_metafields(
                shop.shop_domain,
                shop.access_token,
                metafields,
            )

        return CustomerSyncResult(
            shop_domain=shop.shop_domain,
            pos_customer_number=payload.pos_customer_number,
            source=payload.source,
            success=True,
            message="Customer synced successfully.",
            timestamp=utc_now_iso(),
            customer_id=customer_id,
            email=customer.get("email") or payload.email,
            phone=customer.get("phone") or payload.phone,
            name=customer.get("displayName") or _customer_display_name(payload),
            details={
                "identifier": identifier,
                "fallback_used": fallback_used,
                "metafield_count": len(metafields),
                "address_count": len(customer_input.get("addresses") or []),
                "tag_count": len(customer_input.get("tags") or []),
            },
        )

    def _ensure_customer_custom_id_definition(self, shop: ShopRecord) -> None:
        cache_key = shop.shop_domain
        if cache_key in self._customer_custom_id_ready:
            return
        self.shopify_client.ensure_customer_custom_id_definition(
            shop.shop_domain,
            shop.access_token,
            namespace=CUSTOMER_CUSTOM_ID_NAMESPACE,
            key=CUSTOMER_CUSTOM_ID_KEY,
        )
        self._customer_custom_id_ready.add(cache_key)

    def _build_customer_set_input(self, payload: CustomerSyncRequest) -> Dict[str, Any]:
        first_name = payload.firstName
        last_name = payload.lastName
        if not first_name and not last_name and payload.company:
            last_name = payload.company

        customer_input: Dict[str, Any] = {}
        if first_name:
            customer_input["firstName"] = first_name
        if last_name:
            customer_input["lastName"] = last_name
        if payload.email:
            customer_input["email"] = payload.email
        if payload.phone:
            customer_input["phone"] = payload.phone
        if payload.note:
            customer_input["note"] = payload.note
        if payload.tags:
            customer_input["tags"] = payload.tags
        if payload.taxExempt is not None:
            customer_input["taxExempt"] = bool(payload.taxExempt)

        addresses = [_prepare_customer_address(address) for address in payload.addresses]
        addresses = [address for address in addresses if address]
        if addresses:
            customer_input["addresses"] = addresses[:10]

        if not customer_input:
            raise SyncProcessingError(
                "Customer payload does not contain any fields Shopify can import.",
                {"pos_customer_number": payload.pos_customer_number, "source": payload.source},
                code="empty_customer_payload",
            )
        return customer_input

    def _build_customer_metafields(
        self,
        payload: CustomerSyncRequest,
        customer_id: str,
        *,
        custom_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        owner_id = normalize_gid("Customer", customer_id)
        metafields: Dict[tuple[str, str], Dict[str, Any]] = {}

        if custom_id:
            metafields[(CUSTOMER_CUSTOM_ID_NAMESPACE, CUSTOMER_CUSTOM_ID_KEY)] = {
                "ownerId": owner_id,
                "namespace": CUSTOMER_CUSTOM_ID_NAMESPACE,
                "key": CUSTOMER_CUSTOM_ID_KEY,
                "type": "id",
                "value": custom_id,
            }

        if payload.company:
            metafields[("pos", "company")] = {
                "ownerId": owner_id,
                "namespace": "pos",
                "key": "company",
                "type": "single_line_text_field",
                "value": payload.company,
            }

        for metafield in payload.metafields:
            prepared = _prepare_customer_metafield(metafield, owner_id=owner_id)
            if prepared is None:
                continue
            metafields[(prepared["namespace"], prepared["key"])] = prepared

        return list(metafields.values())

    def _sync_catalog_product(self, payload: ProductSyncRequest, shop: ShopRecord) -> SyncResult:
        if payload.variants:
            return self._sync_matrix_catalog_product(payload, shop)

        existing_mapping = self._find_existing_mapping(shop, payload)
        media_inputs = self._build_media_inputs(payload)
        created = existing_mapping is None
        product_title = payload.title or payload.sku or "POS Imported Product"
        mapping = existing_mapping
        price_updated = False
        cost_updated = False
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

        if payload.cost is not None and _money_changed(mapping.current_cost, payload.cost):
            self.shopify_client.update_inventory_item_cost(
                shop.shop_domain,
                shop.access_token,
                mapping.inventory_item_id,
                payload.cost,
            )
            cost_updated = True

        metafield_inputs = self._build_metafield_inputs(payload, mapping.product_id)
        if metafield_inputs:
            self.shopify_client.set_product_metafields(
                shop.shop_domain,
                shop.access_token,
                metafield_inputs,
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
                cost=payload.cost if payload.cost is not None else mapping.current_cost,
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
            cost_updated=cost_updated,
            inventory_updated=inventory_updated,
            price=payload.price,
            cost=payload.cost,
            quantity=payload.quantity,
            details={
                "created": created,
                "product_title": product_title,
                "product_status": product_status or _normalize_product_status(payload.status, default="DRAFT"),
                "image_count": len(media_inputs),
                "metafield_count": len(metafield_inputs),
                "requested_price": payload.price,
                "requested_cost": payload.cost,
                "requested_quantity": payload.quantity,
                "description_update_skipped": (
                    not created
                    and not payload.update_description
                    and bool(payload.description_html or payload.description or payload.short_description)
                ),
            },
        )

    def _sync_matrix_catalog_product(self, payload: ProductSyncRequest, shop: ShopRecord) -> SyncResult:
        existing_mapping = self._find_existing_mapping(shop, payload)
        media_inputs = self._build_media_inputs(payload)
        created = existing_mapping is None
        product_title = payload.title or payload.sku or "POS Imported Product"
        location_id = self.shopify_client.get_primary_location_id(shop.shop_domain, shop.access_token)
        existing_product: Optional[Dict[str, Any]] = None

        if existing_mapping is not None:
            existing_product = self.shopify_client.get_product_by_id(
                shop.shop_domain,
                shop.access_token,
                existing_mapping.product_id,
            )
            if existing_product is None:
                raise SyncProcessingError(
                    "The Shopify product for this matrix SKU could not be loaded.",
                    {"sku": payload.sku, "product_id": existing_mapping.product_id},
                    code="matrix_product_not_found",
                )
            product_update = self._build_product_update_input(payload, existing_mapping.product_id)
            if len(product_update) > 1 or media_inputs:
                updated_product = self.shopify_client.update_product(
                    shop.shop_domain,
                    shop.access_token,
                    product=product_update,
                    media=media_inputs,
                )
                product_title = updated_product.get("title") or product_title

        product_set_input = self._build_matrix_product_set_input(
            payload,
            location_id=location_id,
            media_inputs=media_inputs if created else [],
            existing_product=existing_product,
        )
        product = self.shopify_client.product_set(
            shop.shop_domain,
            shop.access_token,
            input_data=product_set_input,
            identifier={"id": normalize_gid("Product", existing_mapping.product_id)} if existing_mapping else None,
        )

        first_variant_sku = payload.variants[0].sku
        mapping = self._mapping_from_product(product, first_variant_sku)
        metafield_inputs = self._build_metafield_inputs(payload, mapping.product_id)
        if metafield_inputs:
            self.shopify_client.set_product_metafields(
                shop.shop_domain,
                shop.access_token,
                metafield_inputs,
            )

        return SyncResult(
            shop_domain=shop.shop_domain,
            sku=payload.sku or first_variant_sku,
            success=True,
            message="Matrix product and POS barcodes synced successfully.",
            timestamp=utc_now_iso(),
            variant_id=mapping.variant_id,
            product_id=mapping.product_id,
            inventory_item_id=mapping.inventory_item_id,
            location_id=normalize_gid("Location", location_id),
            price_updated=True,
            cost_updated=any(variant.cost is not None for variant in payload.variants),
            inventory_updated=any(variant.quantity is not None for variant in payload.variants),
            price=payload.price,
            cost=payload.cost,
            quantity=payload.quantity,
            details={
                "created": created,
                "product_title": product.get("title") or product_title,
                "product_status": product.get("status")
                or _normalize_product_status(payload.status, default="DRAFT" if created else "ACTIVE"),
                "image_count": len(media_inputs),
                "metafield_count": len(metafield_inputs),
                "matrix_variant_count": len(payload.variants),
                "matrix_barcode_count": len({variant.barcode for variant in payload.variants}),
                "requested_quantity": payload.quantity,
                "description_update_skipped": (
                    not created
                    and not payload.update_description
                    and bool(payload.description_html or payload.description or payload.short_description)
                ),
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

        for variant in payload.variants:
            try:
                return self.shopify_client.get_variant_by_sku(
                    shop.shop_domain,
                    shop.access_token,
                    variant.sku,
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
                if len(variants) == 1 or payload.variants:
                    target_sku = payload.variants[0].sku if payload.variants else payload.sku
                    return self._mapping_from_product(product, target_sku)
                raise SyncProcessingError(
                    "A product handle was found, but it has multiple variants. Send a SKU to update the correct variant.",
                    {"handle": payload.handle},
                    code="ambiguous_variant_update",
                )

        return None

    def _build_matrix_product_set_input(
        self,
        payload: ProductSyncRequest,
        *,
        location_id: str,
        media_inputs: List[Dict[str, Any]],
        existing_product: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        option_values_by_name: Dict[str, List[str]] = {}
        for variant in payload.variants:
            if not variant.option_values:
                raise SyncProcessingError(
                    "Every matrix variant needs at least one Shopify option value.",
                    {"base_sku": payload.sku, "variant_sku": variant.sku},
                    code="missing_matrix_option",
                )
            for name, value in variant.option_values.items():
                clean_name = str(name).strip()
                clean_value = str(value).strip()
                if not clean_name or not clean_value:
                    raise SyncProcessingError(
                        "Matrix option names and values cannot be blank.",
                        {"base_sku": payload.sku, "variant_sku": variant.sku},
                        code="invalid_matrix_option",
                    )
                values = option_values_by_name.setdefault(clean_name, [])
                if clean_value not in values:
                    values.append(clean_value)

        product_options = [
            {
                "name": name,
                "position": position,
                "values": [{"name": value} for value in values],
            }
            for position, (name, values) in enumerate(option_values_by_name.items(), start=1)
        ]

        existing_variants = ((existing_product or {}).get("variants") or {}).get("nodes") or []
        existing_by_sku = {
            str(variant.get("sku") or "").strip(): variant
            for variant in existing_variants
            if str(variant.get("sku") or "").strip()
        }
        fallback_variant = None
        if payload.sku and payload.sku in existing_by_sku:
            fallback_variant = existing_by_sku[payload.sku]
        elif len(existing_variants) == 1:
            fallback_variant = existing_variants[0]

        used_variant_ids: set[str] = set()
        variant_inputs: List[Dict[str, Any]] = []
        for index, variant in enumerate(payload.variants):
            variant_input: Dict[str, Any] = {
                "sku": variant.sku,
                "barcode": variant.barcode,
                "optionValues": [
                    {"optionName": str(name).strip(), "name": str(value).strip()}
                    for name, value in variant.option_values.items()
                ],
                "inventoryItem": {
                    "sku": variant.sku,
                    "tracked": True if variant.tracked is None else bool(variant.tracked),
                    "requiresShipping": (
                        True if variant.requires_shipping is None else bool(variant.requires_shipping)
                    ),
                },
            }
            if variant.price is not None:
                variant_input["price"] = float(format_price(variant.price))
            if variant.compare_at_price is not None:
                variant_input["compareAtPrice"] = float(format_price(variant.compare_at_price))
            if variant.cost is not None:
                variant_input["inventoryItem"]["cost"] = float(format_price(variant.cost))
            if variant.quantity is not None:
                variant_input["inventoryQuantities"] = [
                    {
                        "locationId": normalize_gid("Location", location_id),
                        "name": "available",
                        "quantity": int(variant.quantity),
                    }
                ]

            existing_variant = existing_by_sku.get(variant.sku)
            if existing_variant is None and index == 0:
                existing_variant = fallback_variant
            existing_variant_id = str((existing_variant or {}).get("id") or "")
            if existing_variant_id and existing_variant_id not in used_variant_ids:
                variant_input["id"] = normalize_gid("ProductVariant", existing_variant_id)
                used_variant_ids.add(existing_variant_id)
            variant_inputs.append(variant_input)

        if existing_product is not None:
            return {
                "productOptions": product_options,
                "variants": variant_inputs,
            }

        product_input = self._build_new_product_input(payload, media_inputs)
        product_input["productOptions"] = product_options
        product_input["variants"] = variant_inputs
        return product_input

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
        if payload.update_description and description_html:
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

    def _build_metafield_inputs(
        self,
        payload: ProductSyncRequest,
        product_id: str,
    ) -> List[Dict[str, Any]]:
        metafields: Dict[tuple[str, str], Dict[str, Any]] = {}
        owner_id = normalize_gid("Product", product_id)

        for metafield in payload.metafields:
            prepared = _prepare_product_metafield(metafield, owner_id=owner_id)
            if prepared is None:
                continue
            metafields[(prepared["namespace"], prepared["key"])] = prepared

        return list(metafields.values())

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
            current_cost=_extract_unit_cost(inventory_item),
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
        variants = []
        seen_variant_skus: set[str] = set()
        seen_barcodes: set[str] = set()
        for variant in payload.variants:
            variant_sku = variant.sku.strip()
            variant_barcode = variant.barcode.strip()
            if variant_sku in seen_variant_skus:
                raise SyncProcessingError(
                    "Matrix variant SKUs must be unique within a product.",
                    {"base_sku": sku, "variant_sku": variant_sku},
                    code="duplicate_matrix_variant_sku",
                )
            if variant_barcode in seen_barcodes:
                raise SyncProcessingError(
                    "Matrix barcodes must be unique within a product.",
                    {"base_sku": sku, "barcode": variant_barcode},
                    code="duplicate_matrix_barcode",
                )
            seen_variant_skus.add(variant_sku)
            seen_barcodes.add(variant_barcode)
            variants.append(
                variant.model_copy(
                    update={
                        "sku": variant_sku,
                        "barcode": variant_barcode,
                        "option_values": {
                            str(name).strip(): str(value).strip()
                            for name, value in variant.option_values.items()
                            if str(name).strip() and str(value).strip()
                        },
                        "pos_cell": _clean_string(variant.pos_cell),
                    }
                )
            )
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
                "variants": variants,
            }
        )

    def _normalize_customer_payload(self, payload: CustomerSyncRequest) -> CustomerSyncRequest:
        tags = [tag.strip() for tag in payload.tags if str(tag).strip()]
        addresses = [
            address.model_copy(
                update={
                    "firstName": _clean_string(address.firstName),
                    "lastName": _clean_string(address.lastName),
                    "company": _clean_string(address.company),
                    "address1": _clean_string(address.address1),
                    "address2": _clean_string(address.address2),
                    "city": _clean_string(address.city),
                    "provinceCode": _clean_string(address.provinceCode or address.province),
                    "province": None,
                    "zip": _clean_string(address.zip),
                    "countryCode": (_clean_string(address.countryCode) or "US").upper(),
                    "phone": _clean_string(address.phone),
                }
            )
            for address in payload.addresses
        ]
        return payload.model_copy(
            update={
                "source": _clean_string(payload.source),
                "pos_customer_number": _clean_string(payload.pos_customer_number),
                "firstName": _clean_string(payload.firstName),
                "lastName": _clean_string(payload.lastName),
                "email": _normalize_customer_email(payload.email),
                "phone": _clean_string(payload.phone),
                "company": _clean_string(payload.company),
                "tags": tags,
                "note": _clean_string(payload.note),
                "addresses": addresses,
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
        cost = None

        if target_variant is not None:
            variant_id = target_variant.get("id")
            variant_sku = target_variant.get("sku")
            barcode = target_variant.get("barcode")
            price = _safe_float(target_variant.get("price"))
            inventory_item = target_variant.get("inventoryItem") or {}
            cost = _extract_unit_cost(inventory_item)
            quantity = _extract_available_quantity(inventory_item)

        return CatalogProductRecord(
            product_id=product["id"],
            variant_id=variant_id,
            title=product.get("title") or "Untitled product",
            handle=product.get("handle"),
            status=product.get("status"),
            sku=variant_sku,
            barcode=barcode,
            price=price,
            cost=cost,
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


def _customer_custom_id_value(payload: CustomerSyncRequest) -> Optional[str]:
    customer_number = _clean_string(payload.pos_customer_number)
    if not customer_number:
        return None
    source = _clean_string(payload.source) or "Customer.dbf"
    return f"{source}:{customer_number}"


def _customer_identifier(payload: CustomerSyncRequest, *, custom_id: Optional[str]) -> Dict[str, Any]:
    if custom_id:
        return {
            "customId": {
                "namespace": CUSTOMER_CUSTOM_ID_NAMESPACE,
                "key": CUSTOMER_CUSTOM_ID_KEY,
                "value": custom_id,
            }
        }
    contact_identifier = _customer_contact_identifier(payload)
    if contact_identifier is not None:
        return contact_identifier
    raise SyncProcessingError(
        "Customer needs a POS customer number, email, or phone to sync safely.",
        {"source": payload.source},
        code="missing_customer_identifier",
    )


def _customer_contact_identifier(payload: CustomerSyncRequest) -> Optional[Dict[str, Any]]:
    if payload.email:
        return {"email": payload.email}
    if payload.phone:
        return {"phone": payload.phone}
    return None


def _customer_display_name(payload: CustomerSyncRequest) -> str:
    name = " ".join(part for part in (payload.firstName, payload.lastName) if part).strip()
    return name or payload.company or payload.email or payload.phone or payload.pos_customer_number or "POS customer"


def _prepare_customer_address(address: CustomerAddressInput) -> Optional[Dict[str, Any]]:
    prepared = {
        "firstName": _clean_string(address.firstName),
        "lastName": _clean_string(address.lastName),
        "company": _clean_string(address.company),
        "address1": _clean_string(address.address1),
        "address2": _clean_string(address.address2),
        "city": _clean_string(address.city),
        "provinceCode": _clean_string(address.provinceCode or address.province),
        "zip": _clean_string(address.zip),
        "countryCode": (_clean_string(address.countryCode) or "US").upper(),
        "phone": _clean_string(address.phone),
    }
    if not any(prepared.get(field) for field in ("address1", "address2", "city", "provinceCode", "zip")):
        return None
    return {key: value for key, value in prepared.items() if value not in (None, "")}


def _prepare_customer_metafield(
    metafield: CustomerMetafieldInput,
    *,
    owner_id: str,
) -> Optional[Dict[str, Any]]:
    namespace = (metafield.namespace or "pos").strip()
    key = (metafield.key or "").strip()
    metafield_type = (metafield.type or "single_line_text_field").strip()
    value = _serialize_metafield_value(metafield.value, metafield_type)

    if not namespace or not key or value is None:
        return None

    return {
        "ownerId": owner_id,
        "namespace": namespace,
        "key": key,
        "type": metafield_type,
        "value": value,
    }


def _normalize_customer_email(value: Optional[str]) -> Optional[str]:
    text = _clean_string(value)
    if not text:
        return None
    email = text.lower()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        return None
    return email


def _clean_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _is_customer_identity_conflict(details: Dict[str, Any]) -> bool:
    for error in details.get("user_errors") or []:
        code = str(error.get("code") or "").upper()
        message = str(error.get("message") or "").lower()
        if code in {"TAKEN", "CUSTOMER_ALREADY_EXISTS"}:
            return True
        if "already" in message or "taken" in message or "has already been" in message:
            return True
    return False


def _prepare_product_metafield(
    metafield: ProductMetafieldInput,
    *,
    owner_id: str,
) -> Optional[Dict[str, Any]]:
    namespace = (metafield.namespace or "custom").strip()
    key = (metafield.key or "").strip()
    metafield_type = (metafield.type or "single_line_text_field").strip()
    value = _serialize_metafield_value(metafield.value, metafield_type)

    if not namespace or not key or value is None:
        return None

    return {
        "ownerId": owner_id,
        "namespace": namespace,
        "key": key,
        "type": metafield_type,
        "value": value,
    }


def _serialize_metafield_value(value: Any, metafield_type: str) -> Optional[str]:
    if value is None:
        return None
    if metafield_type == "json":
        if isinstance(value, str):
            return value
        return json.dumps(value, default=str, ensure_ascii=True, sort_keys=True)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str, ensure_ascii=True, sort_keys=True)
    text = str(value).strip()
    return text or None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _matrix_base_sku(value: str) -> Optional[str]:
    match = MATRIX_VARIANT_SKU_RE.fullmatch((value or "").strip())
    return match.group(1).strip() if match else None


def _money_changed(current: Optional[float], requested: float) -> bool:
    if current is None:
        return True
    return format_price(current) != format_price(requested)


def _extract_unit_cost(inventory_item: Dict[str, Any]) -> Optional[float]:
    unit_cost = inventory_item.get("unitCost") or {}
    return _safe_float(unit_cost.get("amount"))


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
