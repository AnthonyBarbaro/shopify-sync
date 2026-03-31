from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ProductImageInput(BaseModel):
    src: Optional[str] = None
    url: Optional[str] = None
    alt: Optional[str] = None
    filename: Optional[str] = None
    content_type: Optional[str] = None
    position: Optional[int] = None

    model_config = ConfigDict(extra="allow")


class ProductSyncRequest(BaseModel):
    sku: Optional[str] = Field(default=None, min_length=1)
    title: Optional[str] = None
    name: Optional[str] = None
    handle: Optional[str] = None
    external_id: Optional[str] = None
    description_html: Optional[str] = None
    description: Optional[str] = None
    short_description: Optional[str] = None
    vendor: Optional[str] = None
    brand: Optional[str] = None
    product_type: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    status: Optional[str] = None
    barcode: Optional[str] = None
    price: Optional[float] = Field(default=None, ge=0)
    compare_at_price: Optional[float] = Field(default=None, ge=0)
    quantity: Optional[int] = Field(default=None, ge=0)
    tracked: Optional[bool] = True
    requires_shipping: Optional[bool] = True
    image_url: Optional[str] = None
    image_urls: List[str] = Field(default_factory=list)
    images: List[ProductImageInput] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")


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


class CatalogProductRecord(BaseModel):
    product_id: str
    variant_id: Optional[str] = None
    title: str
    handle: Optional[str] = None
    status: Optional[str] = None
    sku: Optional[str] = None
    barcode: Optional[str] = None
    price: Optional[float] = None
    quantity: Optional[int] = None
    vendor: Optional[str] = None
    product_type: Optional[str] = None
    image_url: Optional[str] = None
    updated_at: Optional[str] = None


class CatalogResponse(BaseModel):
    shop: str
    total: int
    items: List[CatalogProductRecord]
    timestamp: str


class FeedEventRecord(BaseModel):
    id: int
    source: str
    endpoint: str
    method: str
    sku: Optional[str] = None
    title: Optional[str] = None
    success: bool
    message: str
    product_id: Optional[str] = None
    variant_id: Optional[str] = None
    received_at: str


class FeedEventsResponse(BaseModel):
    shop: str
    total: int
    items: List[FeedEventRecord]
    timestamp: str


class ErrorBody(BaseModel):
    code: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str


class ErrorResponse(BaseModel):
    error: ErrorBody
