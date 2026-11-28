from datetime import date
from typing import Annotated, Any, Dict, List, Literal, Optional
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, FiniteFloat


class ProductImageInput(BaseModel):
    src: Optional[str] = None
    url: Optional[str] = None
    alt: Optional[str] = None
    filename: Optional[str] = None
    content_type: Optional[str] = None
    position: Optional[int] = None

    model_config = ConfigDict(extra="allow")


class ProductMetafieldInput(BaseModel):
    namespace: str = "custom"
    key: str = Field(min_length=1)
    value: Any
    type: str = "single_line_text_field"

    model_config = ConfigDict(extra="allow")


class ProductVariantSyncInput(BaseModel):
    sku: str = Field(min_length=1)
    barcode: str = Field(min_length=1)
    option_values: Dict[str, str] = Field(default_factory=dict)
    price: Optional[float] = Field(default=None, ge=0)
    compare_at_price: Optional[float] = Field(default=None, ge=0)
    cost: Optional[float] = Field(default=None, ge=0)
    quantity: Optional[int] = Field(default=None, ge=0)
    weight_grams: Optional[FiniteFloat] = Field(default=None, ge=0)
    tracked: Optional[bool] = True
    requires_shipping: Optional[bool] = True
    pos_cell: Optional[str] = None

    model_config = ConfigDict(extra="allow")


class CustomerAddressInput(BaseModel):
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    company: Optional[str] = None
    address1: Optional[str] = None
    address2: Optional[str] = None
    city: Optional[str] = None
    provinceCode: Optional[str] = None
    province: Optional[str] = None
    zip: Optional[str] = None
    countryCode: Optional[str] = "US"
    phone: Optional[str] = None

    model_config = ConfigDict(extra="allow")


class CustomerMetafieldInput(BaseModel):
    namespace: str = "pos"
    key: str = Field(min_length=1)
    value: Any
    type: str = "single_line_text_field"

    model_config = ConfigDict(extra="allow")


class CustomerSyncRequest(BaseModel):
    source: Optional[str] = None
    pos_customer_number: Optional[str] = None
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    company: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    taxExempt: Optional[bool] = None
    addresses: List[CustomerAddressInput] = Field(default_factory=list)
    note: Optional[str] = None
    metafields: List[CustomerMetafieldInput] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")


class ConnectorSyncProgress(BaseModel):
    request_id: str = Field(min_length=1, max_length=64)
    stage: Literal["reading_pos", "reading_shopify", "reconciling", "retrying"]


class ConnectorHeartbeatRequest(BaseModel):
    version: str = Field(min_length=1, max_length=40)
    sync_interval_seconds: int = Field(ge=30, le=86400)
    order_sync_enabled: Optional[bool] = None
    customer_sync_enabled: bool = False
    customer_sync_interval_seconds: int = Field(default=60, ge=30, le=86400)
    sync_progress: ConnectorSyncProgress | None = None


class ConnectorUpdateReport(BaseModel):
    run_id: UUID
    occurred_at: AwareDatetime
    status: Literal["succeeded", "failed"]
    stage: Literal[
        "refresh_updater", "preflight", "download", "validate_download", "stop",
        "install", "validate_install", "configure_task", "start", "complete",
    ]
    file: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9_.-]{1,80}$")
    files_updated: int | None = Field(default=None, ge=0, le=100)
    error_code: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9-]{1,80}$")


class SalesBoardProfit(BaseModel):
    gross_profit_cents: int | None = None
    gross_margin_percent: FiniteFloat | None = None
    missing_cost_lines: int | None = Field(default=None, ge=0)


class SalesBoardTotals(SalesBoardProfit):
    net_cents: int
    tickets: int = Field(ge=0)
    units: FiniteFloat
    credits_cents: int = Field(ge=0)


class SalesBoardCategory(SalesBoardTotals):
    name: str = Field(min_length=1, max_length=100)


class SalesBoardEmployee(SalesBoardTotals):
    employee_id: str = Field(min_length=1, max_length=80)
    name: str = Field(min_length=1, max_length=160)
    categories: List[SalesBoardCategory] = Field(max_length=150)


class SalesBoardTrendPoint(SalesBoardProfit):
    label: str = Field(min_length=1, max_length=24)
    net_cents: int


class SalesBoardPeriod(BaseModel):
    start: date
    store: SalesBoardTotals
    employees: List[SalesBoardEmployee] = Field(max_length=200)
    categories: List[SalesBoardCategory] = Field(max_length=150)
    trend: List[SalesBoardTrendPoint] = Field(max_length=32)


class SalesBoardPeriods(BaseModel):
    today: SalesBoardPeriod
    week: SalesBoardPeriod
    month: SalesBoardPeriod
    year: SalesBoardPeriod


class SalesBoardReceiptLine(BaseModel):
    sku: str | None = Field(default=None, min_length=1, max_length=80)
    name: str = Field(min_length=1, max_length=160)
    units: FiniteFloat
    net_cents: int
    gross_profit_cents: int | None = None
    is_alteration: bool = False


class SalesBoardReceiptDetails(SalesBoardTotals):
    lines: List[SalesBoardReceiptLine]


class SalesBoardReceipt(SalesBoardReceiptDetails):
    invoice: str = Field(min_length=1, max_length=80)
    date: date
    register_id: str = Field(max_length=80)
    time: str | None = Field(default=None, pattern=r"^(?:[01]\d|2[0-3]):[0-5]\d$")
    with_alterations: SalesBoardReceiptDetails | None = None


class SalesBoardStoreSnapshot(BaseModel):
    customer_count: int | None = None
    positive_balance_cents: int | None = None
    inventory_items: int
    in_stock: int
    negative_stock: int
    inventory_cost_cents: int
    inventory_retail_cents: int
    missing_stock_costs: int
    missing_stock_prices: int


class SalesBoardSnapshot(BaseModel):
    observed_at: AwareDatetime
    business_date: date
    first_invoice_date: Optional[date] = None
    last_invoice_date: Optional[date] = None
    periods: SalesBoardPeriods
    periods_with_alterations: SalesBoardPeriods | None = None
    receipts: Dict[str, List[SalesBoardReceipt]] | None = None
    store_snapshot: SalesBoardStoreSnapshot | None = None


class SalesReportLine(BaseModel):
    date: date
    hour: int | None = Field(default=None, ge=0, le=23)
    ticket: int = Field(ge=1)
    rep: str = Field(max_length=160)
    rep_id: str = Field(max_length=80)
    sku: str | None = Field(default=None, max_length=80)
    product: str | None = Field(default=None, max_length=160)
    brand: str | None = Field(default=None, max_length=160)
    category: str = Field(max_length=100)
    department_code: str | None = Field(default=None, max_length=40)
    department: str | None = Field(default=None, max_length=100)
    net_cents: int
    tax_cents: int = 0
    units: FiniteFloat
    cost_cents: int | None = None
    is_alteration: bool = False


class SalesReportUpload(BaseModel):
    observed_at: AwareDatetime
    months: Dict[Annotated[str, Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")], List[SalesReportLine]] = Field(min_length=1, max_length=6)


class SalesBoardSettings(BaseModel):
    enabled: bool = True
    interval_seconds: int = Field(default=60, ge=30, le=900)
    monthly_goal_cents: Optional[int] = Field(default=None, gt=0, le=100_000_000_000)


class ProductSyncRequest(BaseModel):
    sku: Optional[str] = Field(default=None, min_length=1)
    title: Optional[str] = None
    name: Optional[str] = None
    handle: Optional[str] = None
    external_id: Optional[str] = None
    description_html: Optional[str] = None
    description: Optional[str] = None
    short_description: Optional[str] = None
    update_description: bool = False
    vendor: Optional[str] = None
    brand: Optional[str] = None
    product_type: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    status: Optional[str] = None
    barcode: Optional[str] = None
    price: Optional[float] = Field(default=None, ge=0)
    compare_at_price: Optional[float] = Field(default=None, ge=0)
    cost: Optional[float] = Field(default=None, ge=0)
    quantity: Optional[int] = Field(default=None, ge=0)
    weight_grams: Optional[FiniteFloat] = Field(default=None, ge=0)
    tracked: Optional[bool] = True
    requires_shipping: Optional[bool] = True
    image_url: Optional[str] = None
    image_urls: List[str] = Field(default_factory=list)
    images: List[ProductImageInput] = Field(default_factory=list)
    metafields: List[ProductMetafieldInput] = Field(default_factory=list)
    variants: List[ProductVariantSyncInput] = Field(default_factory=list)

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
    product_title: Optional[str] = None
    product_type: Optional[str] = None
    variant_title: Optional[str] = None
    product_status: Optional[str] = None
    auto_archived_zero_stock: bool = False
    current_price: Optional[float] = None
    current_cost: Optional[float] = None
    has_weight: bool = False
    requires_shipping: Optional[bool] = None
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
    cost_updated: bool = False
    inventory_updated: bool = False
    price: Optional[float] = None
    cost: Optional[float] = None
    quantity: Optional[int] = None
    details: Dict[str, Any] = Field(default_factory=dict)


class BulkSyncResponse(BaseModel):
    total: int
    succeeded: int
    failed: int
    timestamp: str
    results: List[SyncResult]


class CustomerSyncResult(BaseModel):
    shop_domain: Optional[str] = None
    pos_customer_number: Optional[str] = None
    source: Optional[str] = None
    success: bool
    message: str
    failure_reason: Optional[str] = None
    address_review: Optional[Dict[str, Any]] = Field(default=None, exclude=True)
    timestamp: str
    customer_id: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    name: Optional[str] = None
    details: Dict[str, Any] = Field(default_factory=dict)


class CustomerBulkSyncResponse(BaseModel):
    total: int
    succeeded: int
    failed: int
    timestamp: str
    results: List[CustomerSyncResult]


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
    customer_sync_path: str
    customer_sync_url: str
    customer_bulk_sync_path: str
    customer_bulk_sync_url: str
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
    cost: Optional[float] = None
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


class PosArchiveUploadResponse(BaseModel):
    shop: str
    archive_path: str
    analysis: Dict[str, Any]
    timestamp: str


class PosArchiveAnalysisResponse(BaseModel):
    shop: str
    archive_path: str
    table_count: int
    total_records: int
    sensitive_table_count: int
    categories: List[Dict[str, Any]]
    tables: List[Dict[str, Any]]
    core_tables: List[Dict[str, Any]]
    samples: Dict[str, Any]
    product_preview: List[Dict[str, Any]]
    notes: List[str]
    timestamp: str


class PosArchiveTableSampleResponse(BaseModel):
    shop: str
    table: str
    rows: List[Dict[str, Any]]
    timestamp: str


class PosArchiveProductPreviewResponse(BaseModel):
    shop: str
    total: int
    items: List[ProductSyncRequest]
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


class RequestLogRecord(BaseModel):
    id: int
    shop_domain: Optional[str] = None
    api_key_preview: Optional[str] = None
    method: str
    path: str
    query_string: Optional[str] = None
    status_code: int
    route_path: Optional[str] = None
    request_body: Optional[str] = None
    user_agent: Optional[str] = None
    source_ip: Optional[str] = None
    duration_ms: int
    created_at: str


class RequestLogsResponse(BaseModel):
    shop: str
    total: int
    items: List[RequestLogRecord]
    timestamp: str


class ErrorBody(BaseModel):
    code: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str


class ErrorResponse(BaseModel):
    error: ErrorBody
