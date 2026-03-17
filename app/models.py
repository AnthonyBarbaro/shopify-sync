from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ProductSyncRequest(BaseModel):
    sku: str = Field(..., min_length=1)
    price: float = Field(..., ge=0)
    quantity: int = Field(..., ge=0)


class InventoryLevelSnapshot(BaseModel):
    location_id: str
    location_name: str
    quantity: Optional[int] = None


class VariantMapping(BaseModel):
    sku: str
    variant_id: str
    product_id: str
    inventory_item_id: str
    current_price: Optional[float] = None
    inventory_levels: List[InventoryLevelSnapshot] = Field(default_factory=list)


class SyncResult(BaseModel):
    shop_domain: Optional[str] = None
    sku: str
    success: bool
    message: str
    timestamp: str
    variant_id: Optional[str] = None
    product_id: Optional[str] = None
    inventory_item_id: Optional[str] = None
    location_id: Optional[str] = None
    price_updated: bool = False
    inventory_updated: bool = False
    price: Optional[float] = None
    quantity: Optional[int] = None
    details: Dict[str, Any] = Field(default_factory=dict)


class BulkSyncResponse(BaseModel):
    total: int
    succeeded: int
    failed: int
    timestamp: str
    results: List[SyncResult]


class HealthResponse(BaseModel):
    status: str
    api_version: str
    authenticated: bool
    installed_shops: int
    shop: Optional[str] = None
    timestamp: str


class UiConfigResponse(BaseModel):
    shop: Optional[str] = None
    shop_name: Optional[str] = None
    host: Optional[str] = None
    api_version: str
    client_id: str
    embedded_app_ready: bool
    authenticated: bool
    app_base_url: str
    location_override: Optional[str] = None
    timestamp: str


class ConnectionSettingsResponse(BaseModel):
    shop: str
    base_url: str
    product_sync_path: str
    product_sync_url: str
    bulk_sync_path: str
    bulk_sync_url: str
    api_key: str
    api_secret: Optional[str] = None
    api_secret_masked: str
    secret_is_temporary: bool
    auth_modes: List[str]
    auth_header_key: str
    auth_header_secret: str
    method: str
    content_type: str
    update_price_and_quantities: bool
    product_payload_example: Dict[str, Any]
    bulk_payload_example: List[Dict[str, Any]]
    created_at: str
    rotated_at: Optional[str] = None
    last_used_at: Optional[str] = None
    timestamp: str


class ShopifyConnectionResponse(BaseModel):
    status: str
    authenticated: bool
    shop: str
    shop_name: Optional[str] = None
    myshopify_domain: Optional[str] = None
    token_expires_at: Optional[str] = None
    message: str
    timestamp: str


class SyncActivityResponse(BaseModel):
    shop: str
    total: int
    items: List[SyncResult]
    timestamp: str


class ErrorBody(BaseModel):
    code: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str


class ErrorResponse(BaseModel):
    error: ErrorBody
