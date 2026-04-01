import base64
import csv
import html
import json
import secrets
import time
from datetime import timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Callable, List, Tuple, TypeVar
from urllib.parse import parse_qsl, urlencode

from fastapi import Depends, FastAPI, Header, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBasicCredentials

from app.auth import (
    AppSessionManager,
    WooNonceStore,
    build_authorize_url,
    extract_woo_query_credentials,
    exchange_authorization_code,
    extract_pos_credentials,
    has_woo_oauth_signature,
    pos_basic_security,
    refresh_access_token,
    validate_shop_domain,
    verify_woo_oauth_request,
    verify_shopify_query_hmac,
    verify_shopify_webhook_hmac,
)
from app.config import get_settings
from app.db import DatabaseStore, PosCredentialRecord, ShopRecord
from app.inventory import InventorySyncService
from app.models import (
    BulkSyncResponse,
    CatalogResponse,
    ConnectionSettingsResponse,
    ErrorResponse,
    FeedEventsResponse,
    HealthResponse,
    ProductSyncRequest,
    RequestLogsResponse,
    ShopifyConnectionResponse,
    SyncActivityResponse,
    SyncResult,
    UiConfigResponse,
)
from app.shopify import ShopifyClient, is_auth_error
from app.state import SyncActivityStore
from app.utils import (
    AppError,
    AuthorizationError,
    SyncProcessingError,
    error_payload,
    parse_iso_datetime,
    safe_json_dumps,
    setup_logging,
    utc_now,
    utc_now_iso,
)


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
INDEX_FILE = STATIC_DIR / "index.html"

settings = get_settings()
logger = setup_logging()
db = DatabaseStore(settings.database_path, settings.credential_encryption_secret)
session_manager = AppSessionManager(settings)
shopify_client = ShopifyClient(settings)
activity_store = SyncActivityStore(limit=400)
inventory_service = InventorySyncService(shopify_client, settings, activity_store)
woo_nonce_store = WooNonceStore()

app = FastAPI(
    title="Shopify Inventory Sync",
    version="2.0.0",
    description="Installable Shopify inventory sync backend for POS integrations.",
)
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")

T = TypeVar("T")
POS_AUTH_QUERY_KEYS = {
    "consumer_key",
    "consumer_secret",
    "oauth_consumer_key",
    "oauth_signature",
    "oauth_signature_method",
    "oauth_timestamp",
    "oauth_nonce",
    "oauth_version",
}
REQUEST_LOG_MASK_KEYS = {
    "consumer_secret",
    "oauth_signature",
    "x-api-secret",
}


@app.middleware("http")
async def capture_incoming_request_logs(request: Request, call_next: Callable) -> Response:
    if not _should_log_incoming_request(request.url.path):
        return await call_next(request)

    started_at = time.perf_counter()
    request_body = await request.body()

    async def receive() -> dict[str, Any]:
        return {
            "type": "http.request",
            "body": request_body,
            "more_body": False,
        }

    request = Request(request.scope, receive)
    response = await call_next(request)

    api_key = _extract_api_key_for_logging(request)
    shop_domain = _resolve_request_log_shop(api_key)
    route = request.scope.get("route")
    route_path = getattr(route, "path", None)

    content_type = (request.headers.get("content-type") or "").lower()
    body_preview: str | None = None
    if request.method.upper() in {"POST", "PUT", "PATCH"} and request_body:
        if "application/json" in content_type or "application/x-www-form-urlencoded" in content_type:
            body_preview = _truncate_text(request_body.decode("utf-8", errors="replace"))
        else:
            body_preview = _truncate_text(f"[{content_type or 'binary'} payload {len(request_body)} bytes]")

    db.record_request_log(
        shop_domain=shop_domain,
        api_key_preview=_mask_api_key(api_key),
        method=request.method,
        path=request.url.path,
        query_string=_mask_query_string(request.url.query),
        status_code=response.status_code,
        route_path=route_path,
        request_body=body_preview,
        user_agent=request.headers.get("user-agent"),
        source_ip=request.client.host if request.client else None,
        duration_ms=int((time.perf_counter() - started_at) * 1000),
    )
    return response


def render_ui_shell() -> HTMLResponse:
    html_content = INDEX_FILE.read_text(encoding="utf-8")
    html_content = html_content.replace("__SHOPIFY_CLIENT_ID__", settings.shopify_client_id)
    return HTMLResponse(content=html_content)


def render_install_page(
    *,
    initial_shop: str = "",
    error: str | None = None,
    host: str | None = None,
    embedded: str | None = None,
    return_to: str | None = None,
) -> HTMLResponse:
    error_html = (
        f'<div class="pill danger" style="margin-top:16px;">{html.escape(error)}</div>' if error else ""
    )
    hidden_fields = []
    if host:
        hidden_fields.append(f'<input type="hidden" name="host" value="{html.escape(host)}" />')
    if embedded:
        hidden_fields.append(f'<input type="hidden" name="embedded" value="{html.escape(embedded)}" />')
    if return_to:
        hidden_fields.append(f'<input type="hidden" name="return_to" value="{html.escape(return_to)}" />')
    hidden_html = "\n".join(hidden_fields)
    page = f"""
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Install Inventory Sync</title>
        <link rel="stylesheet" href="/assets/styles.css" />
      </head>
      <body>
        <div class="shell" style="min-height:100vh;display:grid;place-items:center;">
          <main class="page" style="width:min(520px,100%);">
            <section class="hero">
              <h2>Connect your Shopify store</h2>
              <p>Install the app, then copy one URL, one path, one key, and one secret into your POS.</p>
            </section>
            <form class="form-card" method="get" action="/auth/start">
              <div class="section-head">
                <div>
                  <h3>Install</h3>
                  <p>Enter the shop this app should sync for.</p>
                </div>
              </div>
              {hidden_html}
              <div class="form-grid">
                <div class="field">
                  <label for="shop">Shop Domain</label>
                  <input id="shop" name="shop" placeholder="your-store.myshopify.com" value="{html.escape(initial_shop)}" required />
                </div>
                <div class="button-row">
                  <button class="button" type="submit">Install app</button>
                </div>
              </div>
              {error_html}
            </form>
          </main>
        </div>
      </body>
    </html>
    """
    return HTMLResponse(content=page)


def render_top_level_redirect_page(target_url: str) -> HTMLResponse:
    safe_target = html.escape(target_url, quote=True)
    page = f"""
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Opening Shopify</title>
        <meta name="shopify-api-key" content="{html.escape(settings.shopify_client_id)}" />
        <link rel="stylesheet" href="/assets/styles.css" />
      </head>
      <body>
        <div class="shell" style="min-height:100vh;display:grid;place-items:center;">
          <main class="page" style="width:min(520px,100%);">
            <section class="hero">
              <h2>Opening Shopify</h2>
              <p>We’re switching to the top window so Shopify can finish installation.</p>
            </section>
            <section class="card">
              <div class="button-row">
                <a class="button" href="{safe_target}" target="_top" rel="noreferrer">Continue</a>
              </div>
            </section>
          </main>
        </div>
        <script>
          (function () {{
            var target = {json.dumps(target_url)};
            try {{
              if (window.top === window.self) {{
                window.location.replace(target);
                return;
              }}
              window.top.location.href = target;
            }} catch (_error) {{
              window.open(target, "_top");
            }}
          }})();
        </script>
      </body>
    </html>
    """
    return HTMLResponse(content=page)


def resolve_base_url(request: Request) -> str:
    configured_base_url = settings.normalized_app_base_url
    if configured_base_url:
        return configured_base_url
    return str(request.base_url).rstrip("/")


def _safe_requested_shop(value: str | None) -> str | None:
    if not value:
        return None
    try:
        return validate_shop_domain(value)
    except AuthorizationError:
        return None


def _is_embedded_request(request: Request) -> bool:
    if request.query_params.get("embedded") == "1":
        return True
    if request.headers.get("sec-fetch-dest") == "iframe" and request.query_params.get("host"):
        return True
    return False


def _build_top_level_auth_url(request: Request, *, shop: str, host: str | None, return_to: str | None) -> str:
    params = [("shop", shop)]
    if host:
        params.append(("host", host))
    if return_to:
        params.append(("return_to", return_to))
    return f"{resolve_base_url(request)}/auth/start?{urlencode(params)}"


def _decode_shopify_host(host: str | None) -> str | None:
    if not host:
        return None
    normalized = host.strip()
    if not normalized:
        return None

    padding = "=" * (-len(normalized) % 4)
    try:
        decoded = base64.urlsafe_b64decode((normalized + padding).encode("utf-8")).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None

    decoded = decoded.strip().replace("https://", "").replace("http://", "").strip("/")
    return decoded or None


def _build_embedded_app_redirect_url(
    request: Request,
    *,
    host: str | None,
    redirect_path: str,
    params: list[tuple[str, str]],
) -> str:
    decoded_host = _decode_shopify_host(host)
    if not decoded_host:
        return f"{redirect_path}?{urlencode(params)}"
    embedded_path = redirect_path
    if embedded_path.startswith("/app"):
        embedded_path = embedded_path[4:] or "/"
    return f"https://{decoded_host}/apps/{settings.shopify_client_id}{embedded_path}?{urlencode(params)}"


def _get_session_context(request: Request) -> Tuple[ShopRecord, dict]:
    session = session_manager.get_app_session(request)
    if not session or not session.get("shop"):
        raise AuthorizationError(
            "Open this app from Shopify to continue.",
            {"next_step": "install_or_reopen_app"},
        )

    shop = db.get_shop(session["shop"])
    if shop is None:
        raise AuthorizationError(
            "This shop is not installed or has been disconnected.",
            {"shop": session.get("shop")},
        )

    return ensure_fresh_shop(shop), session


def ensure_fresh_shop(shop: ShopRecord, *, force_refresh: bool = False) -> ShopRecord:
    expires_at = parse_iso_datetime(shop.access_token_expires_at)
    refresh_needed = force_refresh

    if not refresh_needed and expires_at is not None:
        refresh_needed = utc_now() + timedelta(seconds=60) >= expires_at

    if not refresh_needed:
        return shop

    if not shop.refresh_token:
        return shop

    token_payload = refresh_access_token(
        settings,
        shop=shop.shop_domain,
        refresh_token=shop.refresh_token,
    )
    refreshed = db.upsert_shop(
        shop_domain=shop.shop_domain,
        access_token=token_payload["access_token"],
        scope=token_payload.get("scope") or shop.scope,
        refresh_token=token_payload.get("refresh_token") or shop.refresh_token,
        access_token_expires_at=token_payload.get("access_token_expires_at"),
        refresh_token_expires_at=token_payload.get("refresh_token_expires_at") or shop.refresh_token_expires_at,
        shop_name=shop.shop_name,
        myshopify_domain=shop.myshopify_domain or shop.shop_domain,
    )
    logger.info(
        "shop_token_refreshed %s",
        safe_json_dumps(
            {
                "shop": refreshed.shop_domain,
                "expires_at": refreshed.access_token_expires_at,
            }
        ),
    )
    return refreshed


def run_with_shop_retry(shop: ShopRecord, operation: Callable[[ShopRecord], T]) -> T:
    active_shop = ensure_fresh_shop(shop)
    try:
        return operation(active_shop)
    except Exception as exc:
        if not is_auth_error(exc) or not active_shop.refresh_token:
            raise
        refreshed_shop = ensure_fresh_shop(active_shop, force_refresh=True)
        return operation(refreshed_shop)


def require_pos_shop(
    request: Request,
    credentials: HTTPBasicCredentials | None = Depends(pos_basic_security),
    x_api_key: str | None = Header(default=None),
    x_api_secret: str | None = Header(default=None),
) -> ShopRecord:
    api_key, api_secret = extract_pos_credentials(credentials, x_api_key, x_api_secret)
    if api_key and api_secret:
        return db.verify_pos_credentials(api_key, api_secret)

    query_key, query_secret = extract_woo_query_credentials(request)
    if query_key and query_secret:
        return db.verify_query_string_credentials(query_key, query_secret)

    oauth_key = query_key
    if oauth_key and has_woo_oauth_signature(request):
        shop, raw_secret = db.get_query_auth_secret(oauth_key)
        verify_woo_oauth_request(
            request,
            api_key=oauth_key,
            api_secret=raw_secret,
            nonce_store=woo_nonce_store,
        )
        db.mark_pos_credentials_used(oauth_key)
        return shop

    raise AuthorizationError(
        "POS API credentials are required.",
        {
            "accepted_auth": [
                "basic",
                "x-api-key/x-api-secret",
                "woo_query_string",
                "woo_oauth_signature",
            ],
            "header_names": ["X-API-Key", "X-API-Secret"],
            "query_names": ["consumer_key", "consumer_secret", "oauth_*"],
        },
    )


def build_connection_settings_response(
    request: Request,
    response: Response,
    *,
    shop: ShopRecord,
    credentials: PosCredentialRecord,
    raw_secret: str | None = None,
) -> ConnectionSettingsResponse:
    flash_secret = raw_secret or session_manager.pop_secret_flash(request, response, shop=shop.shop_domain)
    base_url = resolve_base_url(request)
    return ConnectionSettingsResponse(
        shop=shop.shop_domain,
        base_url=base_url,
        product_sync_path="/wc-api/v3/products",
        product_sync_url=f"{base_url}/wc-api/v3/products",
        bulk_sync_path="/wc-api/v3/products/batch",
        bulk_sync_url=f"{base_url}/wc-api/v3/products/batch",
        api_key=credentials.api_key,
        api_secret=flash_secret,
        api_secret_masked=credentials.api_secret_masked,
        secret_is_temporary=bool(flash_secret),
        auth_modes=["woo_query_string", "woo_oauth_signature", "basic", "x-api-key/x-api-secret"],
        auth_header_key="X-API-Key",
        auth_header_secret="X-API-Secret",
        method="GET or POST",
        content_type="application/json or query/form",
        update_price_and_quantities=True,
        product_payload_example={
            "name": "Classic Tee",
            "sku": "ABC123",
            "barcode": "012345678905",
            "regular_price": "19.99",
            "stock_quantity": 10,
            "status": "draft",
            "description": "<p>Imported from POS</p>",
            "vendor": "POS Company",
            "product_type": "Apparel",
            "images": [{"src": "https://example.com/products/classic-tee.jpg"}],
        },
        bulk_payload_example=[
            {
                "name": "Classic Tee",
                "sku": "ABC123",
                "regular_price": "19.99",
                "stock_quantity": 10,
                "status": "draft",
            },
            {
                "name": "Canvas Hat",
                "sku": "DEF456",
                "regular_price": "24.99",
                "stock_quantity": 5,
                "status": "draft",
            },
        ],
        created_at=credentials.created_at,
        rotated_at=credentials.rotated_at,
        last_used_at=credentials.last_used_at,
        timestamp=utc_now_iso(),
    )


async def parse_external_request_payload(request: Request) -> Any:
    if request.method.upper() == "GET":
        payload = _query_payload_without_auth(request)
        return payload or None

    content_type = (request.headers.get("content-type") or "").split(";", 1)[0].strip().lower()

    if content_type == "application/json":
        try:
            return await request.json()
        except json.JSONDecodeError as exc:
            raise SyncProcessingError(
                "Request body is not valid JSON.",
                {"content_type": content_type},
                code="invalid_json",
            ) from exc

    if content_type in {"application/x-www-form-urlencoded", "multipart/form-data"}:
        form = await request.form()
        return _coerce_multidict(form.multi_items())

    body = await request.body()
    if not body:
        return None

    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise SyncProcessingError(
            "Unsupported request content type. Send JSON, form data, or query parameters.",
            {"content_type": content_type or "unknown"},
            code="unsupported_content_type",
        )


def normalize_external_product_payload(raw_payload: Any) -> ProductSyncRequest:
    if isinstance(raw_payload, ProductSyncRequest):
        return raw_payload
    if not isinstance(raw_payload, dict):
        raise SyncProcessingError(
            "Product payload must be a JSON object.",
            {"received_type": type(raw_payload).__name__},
            code="invalid_product_payload",
        )

    image_inputs = _normalize_image_inputs(raw_payload)
    categories = _extract_named_values(raw_payload.get("categories"))
    tags = _normalize_tags(raw_payload.get("tags"), categories)
    product_type = _string_or_none(raw_payload.get("product_type")) or (categories[0] if categories else None)
    quantity = _as_int(raw_payload.get("quantity"))
    if quantity is None:
        quantity = _as_int(raw_payload.get("stock_quantity"))

    normalized = {
        "title": _string_or_none(raw_payload.get("title")) or _string_or_none(raw_payload.get("name")),
        "name": _string_or_none(raw_payload.get("name")),
        "handle": _string_or_none(raw_payload.get("handle")) or _string_or_none(raw_payload.get("slug")),
        "external_id": _string_or_none(raw_payload.get("external_id")) or _string_or_none(raw_payload.get("id")),
        "description_html": _string_or_none(raw_payload.get("description_html")) or _string_or_none(raw_payload.get("description")),
        "short_description": _string_or_none(raw_payload.get("short_description")),
        "vendor": _string_or_none(raw_payload.get("vendor")) or _string_or_none(raw_payload.get("brand")),
        "brand": _string_or_none(raw_payload.get("brand")),
        "product_type": product_type,
        "tags": tags,
        "status": _string_or_none(raw_payload.get("status")),
        "sku": _string_or_none(raw_payload.get("sku")),
        "barcode": _string_or_none(raw_payload.get("barcode"))
        or _string_or_none(raw_payload.get("ean"))
        or _string_or_none(raw_payload.get("upc"))
        or _string_or_none(raw_payload.get("gtin")),
        "price": _as_float(raw_payload.get("price"))
        if raw_payload.get("price") not in (None, "")
        else _as_float(raw_payload.get("regular_price")),
        "compare_at_price": _as_float(raw_payload.get("compare_at_price"))
        if raw_payload.get("compare_at_price") not in (None, "")
        else _as_float(raw_payload.get("sale_price")),
        "quantity": quantity,
        "tracked": _as_bool(raw_payload.get("tracked"))
        if raw_payload.get("tracked") not in (None, "")
        else _as_bool(raw_payload.get("manage_stock")),
        "requires_shipping": _as_bool(raw_payload.get("requires_shipping")),
        "image_url": image_inputs[0]["src"] if image_inputs else _string_or_none(raw_payload.get("image_url")),
        "image_urls": [item["src"] for item in image_inputs],
        "images": image_inputs,
    }
    return ProductSyncRequest.model_validate(normalized)


def normalize_external_bulk_payload(raw_payload: Any) -> List[ProductSyncRequest]:
    if isinstance(raw_payload, list):
        items = raw_payload
    elif isinstance(raw_payload, dict):
        if isinstance(raw_payload.get("products"), list):
            items = raw_payload["products"]
        elif isinstance(raw_payload.get("create"), list) or isinstance(raw_payload.get("update"), list):
            items = list(raw_payload.get("create") or []) + list(raw_payload.get("update") or [])
        else:
            items = [raw_payload]
    else:
        raise SyncProcessingError(
            "Bulk payload must be a JSON array or object.",
            {"received_type": type(raw_payload).__name__},
            code="invalid_bulk_payload",
        )

    return [normalize_external_product_payload(item) for item in items]


def _query_payload_without_auth(request: Request) -> dict[str, Any]:
    return _coerce_multidict(
        (key, value)
        for key, value in request.query_params.multi_items()
        if key not in POS_AUTH_QUERY_KEYS
    )


def _coerce_multidict(items: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for key, value in items:
        if key in payload:
            if not isinstance(payload[key], list):
                payload[key] = [payload[key]]
            payload[key].append(value)
            continue
        payload[key] = value
    return payload


def _should_log_incoming_request(path: str) -> bool:
    return path.startswith("/sync/") or path.startswith("/wc-api/") or path.startswith("/wp-json/wc/")


def _mask_api_key(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip()
    if len(normalized) <= 10:
        return normalized
    return f"{normalized[:8]}...{normalized[-4:]}"


def _mask_query_string(query_string: str) -> str | None:
    if not query_string:
        return None
    masked_pairs = []
    for key, value in parse_qsl(query_string, keep_blank_values=True):
        lowered = key.lower()
        if lowered in REQUEST_LOG_MASK_KEYS:
            masked_pairs.append((key, "***"))
        elif lowered in {"consumer_key", "oauth_consumer_key"}:
            masked_pairs.append((key, _mask_api_key(value) or ""))
        else:
            masked_pairs.append((key, value))
    return urlencode(masked_pairs, doseq=True)


def _truncate_text(value: str | None, *, limit: int = 4000) -> str | None:
    if not value:
        return None
    if len(value) <= limit:
        return value
    return f"{value[:limit]}... [truncated]"


def _extract_api_key_for_logging(request: Request) -> str | None:
    api_key, _api_secret = extract_woo_query_credentials(request)
    if api_key:
        return api_key
    x_api_key = request.headers.get("x-api-key")
    if x_api_key:
        return x_api_key
    authorization = request.headers.get("authorization") or ""
    if authorization.lower().startswith("basic "):
        return "basic-auth"
    return None


def _resolve_request_log_shop(api_key: str | None) -> str | None:
    if not api_key or api_key == "basic-auth":
        return None
    auth_record = db.get_pos_auth_record(api_key)
    return auth_record.shop_domain if auth_record else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise SyncProcessingError(
            "Price values must be numeric.",
            {"value": value},
            code="invalid_price",
        ) from exc


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError) as exc:
        raise SyncProcessingError(
            "Quantity values must be whole numbers.",
            {"value": value},
            code="invalid_quantity",
        ) from exc


def _as_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise SyncProcessingError(
        "Boolean fields must be true/false style values.",
        {"value": value},
        code="invalid_boolean",
    )


def _extract_named_values(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        results = []
        for item in value:
            if isinstance(item, dict):
                name = _string_or_none(item.get("name"))
                if name:
                    results.append(name)
            else:
                name = _string_or_none(item)
                if name:
                    results.append(name)
        return results
    return []


def _normalize_tags(raw_tags: Any, categories: List[str]) -> List[str]:
    tags = _extract_named_values(raw_tags)
    for category in categories:
        if category not in tags:
            tags.append(category)
    return tags


def _normalize_image_inputs(raw_payload: dict[str, Any]) -> List[dict[str, Any]]:
    images = []
    raw_images = raw_payload.get("images") or raw_payload.get("image_urls") or []
    if isinstance(raw_images, str):
        raw_images = [item.strip() for item in raw_images.split(",") if item.strip()]

    for item in raw_images:
        if isinstance(item, dict):
            src = _string_or_none(item.get("src")) or _string_or_none(item.get("url"))
            if not src:
                continue
            images.append(
                {
                    "src": src,
                    "alt": _string_or_none(item.get("alt")),
                    "filename": _string_or_none(item.get("filename")),
                    "content_type": _string_or_none(item.get("content_type")) or _string_or_none(item.get("contentType")),
                }
            )
            continue

        src = _string_or_none(item)
        if src:
            images.append({"src": src})

    single_image = _string_or_none(raw_payload.get("image_url")) or _string_or_none(raw_payload.get("image"))
    if single_image and all(item["src"] != single_image for item in images):
        images.insert(0, {"src": single_image})

    return images


def verify_webhook_request(request: Request, body: bytes) -> str:
    hmac_header = request.headers.get("x-shopify-hmac-sha256")
    if not verify_shopify_webhook_hmac(body, hmac_header, settings.shopify_client_secret):
        raise AuthorizationError("Invalid Shopify webhook signature.")

    shop_domain = request.headers.get("x-shopify-shop-domain")
    if not shop_domain:
        raise AuthorizationError("Missing Shopify shop domain header.")
    return validate_shop_domain(shop_domain)


@app.exception_handler(AppError)
async def handle_app_error(_: Request, exc: AppError) -> JSONResponse:
    logger.error(
        "app_error %s",
        safe_json_dumps(
            {
                "code": exc.code,
                "message": exc.message,
                "details": exc.details,
            }
        ),
    )
    basic_auth_header = (
        {"WWW-Authenticate": "Basic"}
        if isinstance(exc, AuthorizationError) and exc.details.get("accepted_auth")
        else None
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=error_payload(code=exc.code, message=exc.message, details=exc.details),
        headers=basic_auth_header,
    )


@app.exception_handler(RequestValidationError)
async def handle_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    details = {"errors": exc.errors()}
    logger.error("validation_error %s", safe_json_dumps(details))
    return JSONResponse(
        status_code=422,
        content=error_payload(
            code="validation_error",
            message="Request validation failed.",
            details=details,
        ),
    )


@app.exception_handler(Exception)
async def handle_unexpected_error(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("unexpected_error %s", str(exc))
    return JSONResponse(
        status_code=500,
        content=error_payload(
            code="internal_server_error",
            message="An unexpected error occurred.",
        ),
    )


@app.get("/", include_in_schema=False)
async def root(request: Request) -> Response:
    return await app_shell(request)


@app.get("/auth/start", include_in_schema=False)
async def auth_start(request: Request) -> Response:
    shop = validate_shop_domain(request.query_params.get("shop", ""))
    if request.query_params.get("hmac") and not verify_shopify_query_hmac(
        request.url.query,
        settings.shopify_client_secret,
    ):
        raise AuthorizationError("Invalid Shopify install signature.", {"shop": shop})

    host = request.query_params.get("host")
    return_to = request.query_params.get("return_to") or "/app"
    if _is_embedded_request(request):
        target_url = _build_top_level_auth_url(
            request,
            shop=shop,
            host=host,
            return_to=return_to,
        )
        return render_top_level_redirect_page(target_url)

    state = secrets.token_urlsafe(24)
    redirect = RedirectResponse(
        url=build_authorize_url(settings, shop=shop, state=state),
        status_code=307,
    )
    session_manager.set_oauth_state(
        redirect,
        state=state,
        shop=shop,
        host=host,
        return_to=return_to,
    )
    return redirect


@app.get("/auth/callback", include_in_schema=False)
async def auth_callback(request: Request) -> RedirectResponse:
    if request.query_params.get("error"):
        message = request.query_params.get("error_description") or request.query_params.get("error")
        raise AuthorizationError("Shopify installation failed.", {"reason": message})

    if not verify_shopify_query_hmac(request.url.query, settings.shopify_client_secret):
        raise AuthorizationError("Invalid Shopify OAuth callback signature.")

    oauth_state = session_manager.get_oauth_state(request)
    shop = validate_shop_domain(request.query_params.get("shop", ""))
    state = request.query_params.get("state")
    code = request.query_params.get("code")

    if not oauth_state or oauth_state.get("state") != state or oauth_state.get("shop") != shop:
        raise AuthorizationError("Shopify OAuth state could not be verified.", {"shop": shop})

    if not code:
        raise AuthorizationError("Shopify OAuth callback did not include an authorization code.", {"shop": shop})

    token_payload = exchange_authorization_code(settings, shop=shop, code=code)
    shop_info = shopify_client.get_shop_info(shop, token_payload["access_token"])
    shop_domain = validate_shop_domain(shop_info.get("myshopifyDomain") or shop)
    saved_shop = db.upsert_shop(
        shop_domain=shop_domain,
        access_token=token_payload["access_token"],
        scope=token_payload.get("scope"),
        refresh_token=token_payload.get("refresh_token"),
        access_token_expires_at=token_payload.get("access_token_expires_at"),
        refresh_token_expires_at=token_payload.get("refresh_token_expires_at"),
        shop_name=shop_info.get("name"),
        myshopify_domain=shop_info.get("myshopifyDomain"),
    )
    _, raw_secret = db.ensure_pos_credentials(saved_shop.shop_domain)

    host = request.query_params.get("host") or oauth_state.get("host")
    return_to = oauth_state.get("return_to") or "/app"
    redirect_path = "/app/settings" if raw_secret else return_to
    params = [("shop", saved_shop.shop_domain)]
    if host:
        params.append(("host", host))
    params.append(("embedded", "1"))
    redirect_url = _build_embedded_app_redirect_url(
        request,
        host=host,
        redirect_path=redirect_path,
        params=params,
    )
    redirect = RedirectResponse(url=redirect_url, status_code=303)
    session_manager.set_app_session(redirect, shop=saved_shop.shop_domain, host=host)
    session_manager.clear_oauth_state(redirect)
    if raw_secret:
        session_manager.set_secret_flash(redirect, shop=saved_shop.shop_domain, api_secret=raw_secret)
    return redirect


@app.get("/app", include_in_schema=False)
@app.get("/app/product-sync", include_in_schema=False)
@app.get("/app/bulk-sync", include_in_schema=False)
@app.get("/app/catalog", include_in_schema=False)
@app.get("/app/settings", include_in_schema=False)
async def app_shell(request: Request) -> Response:
    session = session_manager.get_app_session(request)
    session_shop = db.get_shop(session["shop"]) if session and session.get("shop") else None
    requested_shop = _safe_requested_shop(request.query_params.get("shop"))

    if requested_shop and (session_shop is None or session_shop.shop_domain != requested_shop):
        params = list(request.query_params.multi_items())
        if not any(key == "return_to" for key, _value in params):
            params.append(("return_to", request.url.path))
        if _is_embedded_request(request):
            target_url = _build_top_level_auth_url(
                request,
                shop=requested_shop,
                host=request.query_params.get("host"),
                return_to=request.url.path,
            )
            return render_top_level_redirect_page(target_url)
        return RedirectResponse(url=f"/auth/start?{urlencode(params)}", status_code=307)

    if session_shop is not None:
        return render_ui_shell()

    message = request.query_params.get("error_description") or request.query_params.get("error")
    return render_install_page(
        initial_shop=requested_shop or "",
        error=message,
        host=request.query_params.get("host"),
        embedded=request.query_params.get("embedded"),
        return_to=request.url.path,
    )


@app.get("/health", response_model=HealthResponse)
async def health(request: Request) -> HealthResponse:
    session = session_manager.get_app_session(request)
    session_shop = db.get_shop(session["shop"]) if session and session.get("shop") else None
    return HealthResponse(
        status="ok",
        api_version=settings.shopify_api_version,
        authenticated=session_shop is not None,
        installed_shops=db.shop_count(),
        shop=session_shop.shop_domain if session_shop else None,
        timestamp=utc_now_iso(),
    )


@app.get("/health/shopify", response_model=ShopifyConnectionResponse)
async def shopify_health(request: Request) -> ShopifyConnectionResponse:
    shop, _session = _get_session_context(request)

    def load(active_shop: ShopRecord) -> ShopifyConnectionResponse:
        shop_info = shopify_client.get_shop_info(active_shop.shop_domain, active_shop.access_token)
        return ShopifyConnectionResponse(
            status="ok",
            authenticated=True,
            shop=active_shop.shop_domain,
            shop_name=shop_info.get("name") or active_shop.shop_name,
            myshopify_domain=shop_info.get("myshopifyDomain") or active_shop.myshopify_domain,
            token_expires_at=active_shop.access_token_expires_at,
            message="Successfully authenticated with Shopify and loaded shop details.",
            timestamp=utc_now_iso(),
        )

    return run_with_shop_retry(shop, load)


@app.get("/api/ui/config", response_model=UiConfigResponse)
async def ui_config(request: Request) -> UiConfigResponse:
    shop, session = _get_session_context(request)
    return UiConfigResponse(
        shop=shop.shop_domain,
        shop_name=shop.shop_name,
        host=session.get("host"),
        api_version=settings.shopify_api_version,
        client_id=settings.shopify_client_id,
        embedded_app_ready=bool(settings.shopify_client_id),
        authenticated=True,
        app_base_url=resolve_base_url(request),
        location_override=settings.shopify_location_id,
        timestamp=utc_now_iso(),
    )


@app.get("/api/connection-settings", response_model=ConnectionSettingsResponse)
async def connection_settings(request: Request, response: Response) -> ConnectionSettingsResponse:
    shop, _session = _get_session_context(request)
    credentials, created_secret = db.ensure_pos_credentials(shop.shop_domain)
    return build_connection_settings_response(
        request,
        response,
        shop=shop,
        credentials=credentials,
        raw_secret=created_secret,
    )


@app.post("/api/connection-settings/rotate", response_model=ConnectionSettingsResponse)
async def rotate_connection_settings(request: Request, response: Response) -> ConnectionSettingsResponse:
    shop, _session = _get_session_context(request)
    credentials, raw_secret = db.rotate_pos_credentials(shop.shop_domain)
    return build_connection_settings_response(
        request,
        response,
        shop=shop,
        credentials=credentials,
        raw_secret=raw_secret,
    )


@app.get("/api/activity", response_model=SyncActivityResponse)
async def activity(request: Request, limit: int = 25) -> SyncActivityResponse:
    shop, _session = _get_session_context(request)
    safe_limit = max(1, min(limit, 100))
    return SyncActivityResponse(
        shop=shop.shop_domain,
        total=activity_store.total(shop_domain=shop.shop_domain),
        items=activity_store.list(limit=safe_limit, shop_domain=shop.shop_domain),
        timestamp=utc_now_iso(),
    )


@app.get("/api/catalog", response_model=CatalogResponse)
async def catalog(request: Request, limit: int = 100) -> CatalogResponse:
    shop, _session = _get_session_context(request)
    safe_limit = max(1, min(limit, 500))

    def load(active_shop: ShopRecord) -> CatalogResponse:
        items = inventory_service.list_catalog(active_shop)[:safe_limit]
        return CatalogResponse(
            shop=active_shop.shop_domain,
            total=len(items),
            items=items,
            timestamp=utc_now_iso(),
        )

    return run_with_shop_retry(shop, load)


@app.get("/api/catalog.csv")
async def catalog_csv(request: Request) -> StreamingResponse:
    shop, _session = _get_session_context(request)

    def load(active_shop: ShopRecord) -> list[Any]:
        return inventory_service.list_catalog(active_shop)

    items = run_with_shop_retry(shop, load)
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "product_id",
            "variant_id",
            "title",
            "handle",
            "status",
            "sku",
            "barcode",
            "price",
            "quantity",
            "vendor",
            "product_type",
            "image_url",
            "updated_at",
        ]
    )
    for item in items:
        writer.writerow(
            [
                item.product_id,
                item.variant_id or "",
                item.title,
                item.handle or "",
                item.status or "",
                item.sku or "",
                item.barcode or "",
                item.price if item.price is not None else "",
                item.quantity if item.quantity is not None else "",
                item.vendor or "",
                item.product_type or "",
                item.image_url or "",
                item.updated_at or "",
            ]
        )
    output.seek(0)
    headers = {
        "Content-Disposition": f'attachment; filename="{shop.shop_domain}-catalog.csv"'
    }
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers=headers)


@app.get("/api/feed", response_model=FeedEventsResponse)
async def feed(request: Request, limit: int = 50) -> FeedEventsResponse:
    shop, _session = _get_session_context(request)
    safe_limit = max(1, min(limit, 250))
    rows = db.list_feed_events(shop.shop_domain, limit=safe_limit)
    return FeedEventsResponse(
        shop=shop.shop_domain,
        total=db.feed_event_count(shop.shop_domain),
        items=[
            {
                "id": row.id,
                "source": row.source,
                "endpoint": row.endpoint,
                "method": row.method,
                "sku": row.sku,
                "title": row.title,
                "success": row.success,
                "message": row.message,
                "product_id": row.product_id,
                "variant_id": row.variant_id,
                "received_at": row.received_at,
            }
            for row in rows
        ],
        timestamp=utc_now_iso(),
    )


@app.get("/api/feed.csv")
async def feed_csv(request: Request) -> StreamingResponse:
    shop, _session = _get_session_context(request)
    rows = db.list_feed_events(shop.shop_domain, limit=1000)
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "id",
            "source",
            "endpoint",
            "method",
            "sku",
            "title",
            "success",
            "message",
            "product_id",
            "variant_id",
            "received_at",
            "request_payload",
            "normalized_payload",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.id,
                row.source,
                row.endpoint,
                row.method,
                row.sku or "",
                row.title or "",
                "true" if row.success else "false",
                row.message,
                row.product_id or "",
                row.variant_id or "",
                row.received_at,
                row.request_payload,
                row.normalized_payload or "",
            ]
        )
    output.seek(0)
    headers = {
        "Content-Disposition": f'attachment; filename="{shop.shop_domain}-feed.csv"'
    }
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers=headers)


@app.get("/api/request-logs", response_model=RequestLogsResponse)
async def request_logs(request: Request, limit: int = 100) -> RequestLogsResponse:
    shop, _session = _get_session_context(request)
    safe_limit = max(1, min(limit, 500))
    rows = db.list_request_logs(shop_domain=shop.shop_domain, limit=safe_limit)
    return RequestLogsResponse(
        shop=shop.shop_domain,
        total=db.request_log_count(shop_domain=shop.shop_domain),
        items=[
            {
                "id": row.id,
                "shop_domain": row.shop_domain,
                "api_key_preview": row.api_key_preview,
                "method": row.method,
                "path": row.path,
                "query_string": row.query_string,
                "status_code": row.status_code,
                "route_path": row.route_path,
                "request_body": row.request_body,
                "user_agent": row.user_agent,
                "source_ip": row.source_ip,
                "duration_ms": row.duration_ms,
                "created_at": row.created_at,
            }
            for row in rows
        ],
        timestamp=utc_now_iso(),
    )


@app.get("/api/request-logs.csv")
async def request_logs_csv(request: Request) -> StreamingResponse:
    shop, _session = _get_session_context(request)
    rows = db.list_request_logs(shop_domain=shop.shop_domain, limit=2000)
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "id",
            "shop_domain",
            "api_key_preview",
            "method",
            "path",
            "query_string",
            "status_code",
            "route_path",
            "request_body",
            "user_agent",
            "source_ip",
            "duration_ms",
            "created_at",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.id,
                row.shop_domain or "",
                row.api_key_preview or "",
                row.method,
                row.path,
                row.query_string or "",
                row.status_code,
                row.route_path or "",
                row.request_body or "",
                row.user_agent or "",
                row.source_ip or "",
                row.duration_ms,
                row.created_at,
            ]
        )
    output.seek(0)
    headers = {
        "Content-Disposition": f'attachment; filename="{shop.shop_domain}-request-logs.csv"'
    }
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers=headers)


@app.post(
    "/api/sync/product",
    response_model=SyncResult,
    responses={400: {"model": ErrorResponse}, 401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 502: {"model": ErrorResponse}},
)
async def ui_sync_product(request: Request, payload: ProductSyncRequest) -> SyncResult:
    shop, _session = _get_session_context(request)
    return run_with_shop_retry(shop, lambda active_shop: inventory_service.sync_product(payload, active_shop))


@app.post(
    "/api/sync/bulk",
    response_model=BulkSyncResponse,
    responses={400: {"model": ErrorResponse}, 401: {"model": ErrorResponse}, 422: {"model": ErrorResponse}, 502: {"model": ErrorResponse}},
)
async def ui_sync_bulk(request: Request, payload: List[ProductSyncRequest]) -> BulkSyncResponse:
    shop, _session = _get_session_context(request)
    return run_with_shop_retry(shop, lambda active_shop: inventory_service.sync_bulk(payload, active_shop))


def _feed_source_for_path(path: str) -> str:
    if path.startswith("/wp-json/") or path.startswith("/wc-api/"):
        return "woo_compatible"
    return "pos_direct"


def _serialize_payload(value: Any) -> str:
    return json.dumps(value, default=str, sort_keys=True)


def _build_woo_product_list(items: list[Any]) -> list[dict[str, Any]]:
    payload = []
    for item in items:
        payload.append(
            {
                "id": item.product_id,
                "name": item.title,
                "slug": item.handle,
                "status": (item.status or "draft").lower(),
                "sku": item.sku,
                "barcode": item.barcode,
                "regular_price": f"{item.price:.2f}" if item.price is not None else None,
                "stock_quantity": item.quantity,
                "vendor": item.vendor,
                "product_type": item.product_type,
                "images": [{"src": item.image_url}] if item.image_url else [],
                "updated_at": item.updated_at,
            }
        )
    return payload


def _looks_like_product_mutation(raw_payload: Any) -> bool:
    if not isinstance(raw_payload, dict):
        return False
    mutation_keys = {
        "name",
        "title",
        "description",
        "description_html",
        "price",
        "regular_price",
        "sale_price",
        "quantity",
        "stock_quantity",
        "barcode",
        "images",
        "image_url",
        "image",
    }
    return any(key in raw_payload for key in mutation_keys)


async def _handle_external_single_sync(request: Request, shop: ShopRecord) -> SyncResult | JSONResponse:
    raw_payload = await parse_external_request_payload(request)
    source = _feed_source_for_path(request.url.path)

    if request.method.upper() == "GET" and request.url.path in {"/wc-api/v3/products", "/wp-json/wc/v3/products"} and not _looks_like_product_mutation(raw_payload):
        try:
            page = max(int(request.query_params.get("page", "1")), 1)
            per_page = max(1, min(int(request.query_params.get("per_page", "100")), 250))
        except ValueError as exc:
            raise SyncProcessingError(
                "page and per_page must be whole numbers.",
                {"page": request.query_params.get("page"), "per_page": request.query_params.get("per_page")},
                code="invalid_pagination",
            ) from exc

        requested_sku = _string_or_none(request.query_params.get("sku"))
        requested_status = _string_or_none(request.query_params.get("status"))
        search = (_string_or_none(request.query_params.get("search")) or "").lower()

        def load(active_shop: ShopRecord) -> list[Any]:
            return inventory_service.list_catalog(active_shop)

        rows = run_with_shop_retry(shop, load)
        if requested_sku:
            rows = [row for row in rows if (row.sku or "").strip() == requested_sku]
        if requested_status:
            rows = [row for row in rows if (row.status or "").lower() == requested_status.lower()]
        if search:
            rows = [
                row
                for row in rows
                if search in (row.title or "").lower()
                or search in (row.sku or "").lower()
                or search in (row.handle or "").lower()
            ]
        start = (page - 1) * per_page
        end = start + per_page
        filtered_rows = rows[start:end]
        return JSONResponse(_build_woo_product_list(filtered_rows))

    if raw_payload is None:
        return JSONResponse(
            {
                "status": "ok",
                "message": "POS sync endpoint is reachable.",
                "shop": shop.shop_domain,
                "method": request.method.upper(),
                "path": request.url.path,
                "timestamp": utc_now_iso(),
            }
        )

    normalized = normalize_external_product_payload(raw_payload)
    try:
        result = run_with_shop_retry(shop, lambda active_shop: inventory_service.sync_product(normalized, active_shop))
    except Exception as exc:
        db.record_feed_event(
            shop_domain=shop.shop_domain,
            source=source,
            endpoint=request.url.path,
            method=request.method,
            sku=normalized.sku,
            title=normalized.title,
            success=False,
            message=str(exc),
            product_id=None,
            variant_id=None,
            request_payload=_serialize_payload(raw_payload),
            normalized_payload=_serialize_payload(normalized.model_dump(mode="json")),
        )
        raise

    db.record_feed_event(
        shop_domain=shop.shop_domain,
        source=source,
        endpoint=request.url.path,
        method=request.method,
        sku=result.sku,
        title=result.details.get("product_title"),
        success=True,
        message=result.message,
        product_id=result.product_id,
        variant_id=result.variant_id,
        request_payload=_serialize_payload(raw_payload),
        normalized_payload=_serialize_payload(normalized.model_dump(mode="json")),
    )
    return result


async def _handle_external_bulk_sync(request: Request, shop: ShopRecord) -> BulkSyncResponse | JSONResponse:
    raw_payload = await parse_external_request_payload(request)
    source = _feed_source_for_path(request.url.path)

    if raw_payload is None:
        return JSONResponse(
            {
                "status": "ok",
                "message": "POS bulk sync endpoint is reachable.",
                "shop": shop.shop_domain,
                "method": request.method.upper(),
                "path": request.url.path,
                "timestamp": utc_now_iso(),
            }
        )

    normalized_items = normalize_external_bulk_payload(raw_payload)
    try:
        result = run_with_shop_retry(shop, lambda active_shop: inventory_service.sync_bulk(normalized_items, active_shop))
    except Exception as exc:
        db.record_feed_event(
            shop_domain=shop.shop_domain,
            source=source,
            endpoint=request.url.path,
            method=request.method,
            sku=None,
            title=f"{len(normalized_items)} products",
            success=False,
            message=str(exc),
            product_id=None,
            variant_id=None,
            request_payload=_serialize_payload(raw_payload),
            normalized_payload=_serialize_payload([item.model_dump(mode="json") for item in normalized_items]),
        )
        raise

    for row in result.results:
        db.record_feed_event(
            shop_domain=shop.shop_domain,
            source=source,
            endpoint=request.url.path,
            method=request.method,
            sku=row.sku,
            title=row.details.get("product_title"),
            success=row.success,
            message=row.message,
            product_id=row.product_id,
            variant_id=row.variant_id,
            request_payload=_serialize_payload(raw_payload),
            normalized_payload=_serialize_payload([item.model_dump(mode="json") for item in normalized_items]),
        )
    return result


@app.api_route(
    "/sync/product",
    methods=["GET", "POST"],
    response_model=None,
    responses={400: {"model": ErrorResponse}, 401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 502: {"model": ErrorResponse}},
)
async def sync_product(
    request: Request,
    shop: ShopRecord = Depends(require_pos_shop),
) -> SyncResult | JSONResponse:
    return await _handle_external_single_sync(request, shop)


@app.api_route(
    "/sync/bulk",
    methods=["GET", "POST"],
    response_model=None,
    responses={400: {"model": ErrorResponse}, 401: {"model": ErrorResponse}, 422: {"model": ErrorResponse}, 502: {"model": ErrorResponse}},
)
async def sync_bulk(
    request: Request,
    shop: ShopRecord = Depends(require_pos_shop),
) -> BulkSyncResponse | JSONResponse:
    return await _handle_external_bulk_sync(request, shop)


@app.api_route(
    "/wc-api/v3/products",
    methods=["GET", "POST"],
    response_model=None,
    include_in_schema=False,
)
@app.api_route(
    "/wc-api/v3/products/",
    methods=["GET", "POST"],
    response_model=None,
    include_in_schema=False,
)
@app.api_route(
    "/wp-json/wc/v3/products",
    methods=["GET", "POST"],
    response_model=None,
    include_in_schema=False,
)
@app.api_route(
    "/wp-json/wc/v3/products/",
    methods=["GET", "POST"],
    response_model=None,
    include_in_schema=False,
)
async def woo_products(
    request: Request,
    shop: ShopRecord = Depends(require_pos_shop),
) -> SyncResult | JSONResponse:
    return await _handle_external_single_sync(request, shop)


@app.api_route(
    "/wc-api/v3/products/batch",
    methods=["GET", "POST"],
    response_model=None,
    include_in_schema=False,
)
@app.api_route(
    "/wc-api/v3/products/batch/",
    methods=["GET", "POST"],
    response_model=None,
    include_in_schema=False,
)
@app.api_route(
    "/wp-json/wc/v3/products/batch",
    methods=["GET", "POST"],
    response_model=None,
    include_in_schema=False,
)
@app.api_route(
    "/wp-json/wc/v3/products/batch/",
    methods=["GET", "POST"],
    response_model=None,
    include_in_schema=False,
)
async def woo_products_batch(
    request: Request,
    shop: ShopRecord = Depends(require_pos_shop),
) -> BulkSyncResponse | JSONResponse:
    return await _handle_external_bulk_sync(request, shop)


@app.post("/webhooks/app-uninstalled")
async def app_uninstalled_webhook(request: Request) -> JSONResponse:
    body = await request.body()
    shop_domain = verify_webhook_request(request, body)
    db.mark_shop_uninstalled(shop_domain)
    logger.info("shop_uninstalled %s", safe_json_dumps({"shop": shop_domain, "timestamp": utc_now_iso()}))
    return JSONResponse({"status": "ok"})


@app.post("/webhooks/customers/data_request")
@app.post("/webhooks/customers/redact")
@app.post("/webhooks/shop/redact")
async def compliance_webhooks(request: Request) -> JSONResponse:
    body = await request.body()
    shop_domain = verify_webhook_request(request, body)
    logger.info(
        "compliance_webhook %s",
        safe_json_dumps({"shop": shop_domain, "path": request.url.path, "timestamp": utc_now_iso()}),
    )
    return JSONResponse({"status": "ok"})
