import html
import secrets
from datetime import timedelta
from pathlib import Path
from typing import Callable, List, Tuple, TypeVar
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, Header, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBasicCredentials

from app.auth import (
    AppSessionManager,
    build_authorize_url,
    exchange_authorization_code,
    extract_pos_credentials,
    pos_basic_security,
    refresh_access_token,
    validate_shop_domain,
    verify_shopify_query_hmac,
    verify_shopify_webhook_hmac,
)
from app.config import get_settings
from app.db import DatabaseStore, PosCredentialRecord, ShopRecord
from app.inventory import InventorySyncService
from app.models import (
    BulkSyncResponse,
    ConnectionSettingsResponse,
    ErrorResponse,
    HealthResponse,
    ProductSyncRequest,
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
db = DatabaseStore(settings.database_path)
session_manager = AppSessionManager(settings)
shopify_client = ShopifyClient(settings)
activity_store = SyncActivityStore(limit=400)
inventory_service = InventorySyncService(shopify_client, settings, activity_store)

app = FastAPI(
    title="Shopify Inventory Sync",
    version="2.0.0",
    description="Installable Shopify inventory sync backend for POS integrations.",
)
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")

T = TypeVar("T")


def render_ui_shell() -> HTMLResponse:
    html_content = INDEX_FILE.read_text(encoding="utf-8")
    html_content = html_content.replace("__SHOPIFY_CLIENT_ID__", settings.shopify_client_id)
    return HTMLResponse(content=html_content)


def render_install_page(*, initial_shop: str = "", error: str | None = None) -> HTMLResponse:
    error_html = (
        f'<div class="pill danger" style="margin-top:16px;">{html.escape(error)}</div>' if error else ""
    )
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
    credentials: HTTPBasicCredentials | None = Depends(pos_basic_security),
    x_api_key: str | None = Header(default=None),
    x_api_secret: str | None = Header(default=None),
) -> ShopRecord:
    api_key, api_secret = extract_pos_credentials(credentials, x_api_key, x_api_secret)
    if not api_key or not api_secret:
        raise AuthorizationError(
            "POS API credentials are required.",
            {
                "accepted_auth": ["basic", "x-api-key/x-api-secret"],
                "header_names": ["X-API-Key", "X-API-Secret"],
            },
        )
    return db.verify_pos_credentials(api_key, api_secret)


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
        product_sync_path="/sync/product",
        product_sync_url=f"{base_url}/sync/product",
        bulk_sync_path="/sync/bulk",
        bulk_sync_url=f"{base_url}/sync/bulk",
        api_key=credentials.api_key,
        api_secret=flash_secret,
        api_secret_masked=credentials.api_secret_masked,
        secret_is_temporary=bool(flash_secret),
        auth_modes=["basic", "x-api-key/x-api-secret"],
        auth_header_key="X-API-Key",
        auth_header_secret="X-API-Secret",
        method="POST",
        content_type="application/json",
        update_price_and_quantities=True,
        product_payload_example={"sku": "ABC123", "price": 19.99, "quantity": 10},
        bulk_payload_example=[
            {"sku": "ABC123", "price": 19.99, "quantity": 10},
            {"sku": "DEF456", "price": 24.99, "quantity": 5},
        ],
        created_at=credentials.created_at,
        rotated_at=credentials.rotated_at,
        last_used_at=credentials.last_used_at,
        timestamp=utc_now_iso(),
    )


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
async def root() -> RedirectResponse:
    return RedirectResponse(url="/app", status_code=307)


@app.get("/auth/start", include_in_schema=False)
async def auth_start(request: Request) -> RedirectResponse:
    shop = validate_shop_domain(request.query_params.get("shop", ""))
    if request.query_params.get("hmac") and not verify_shopify_query_hmac(
        request.url.query,
        settings.shopify_client_secret,
    ):
        raise AuthorizationError("Invalid Shopify install signature.", {"shop": shop})

    host = request.query_params.get("host")
    return_to = request.query_params.get("return_to") or "/app"
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
    redirect = RedirectResponse(url=f"{redirect_path}?{urlencode(params)}", status_code=303)
    session_manager.set_app_session(redirect, shop=saved_shop.shop_domain, host=host)
    session_manager.clear_oauth_state(redirect)
    if raw_secret:
        session_manager.set_secret_flash(redirect, shop=saved_shop.shop_domain, api_secret=raw_secret)
    return redirect


@app.get("/app", include_in_schema=False)
@app.get("/app/product-sync", include_in_schema=False)
@app.get("/app/bulk-sync", include_in_schema=False)
@app.get("/app/settings", include_in_schema=False)
async def app_shell(request: Request) -> Response:
    session = session_manager.get_app_session(request)
    session_shop = db.get_shop(session["shop"]) if session and session.get("shop") else None
    requested_shop = _safe_requested_shop(request.query_params.get("shop"))

    if requested_shop and (session_shop is None or session_shop.shop_domain != requested_shop):
        params = list(request.query_params.multi_items())
        if not any(key == "return_to" for key, _value in params):
            params.append(("return_to", request.url.path))
        return RedirectResponse(url=f"/auth/start?{urlencode(params)}", status_code=307)

    if session_shop is not None:
        return render_ui_shell()

    message = request.query_params.get("error_description") or request.query_params.get("error")
    return render_install_page(initial_shop=requested_shop or "", error=message)


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


@app.post(
    "/sync/product",
    response_model=SyncResult,
    responses={400: {"model": ErrorResponse}, 401: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 502: {"model": ErrorResponse}},
)
async def sync_product(
    payload: ProductSyncRequest,
    shop: ShopRecord = Depends(require_pos_shop),
) -> SyncResult:
    return run_with_shop_retry(shop, lambda active_shop: inventory_service.sync_product(payload, active_shop))


@app.post(
    "/sync/bulk",
    response_model=BulkSyncResponse,
    responses={400: {"model": ErrorResponse}, 401: {"model": ErrorResponse}, 422: {"model": ErrorResponse}, 502: {"model": ErrorResponse}},
)
async def sync_bulk(
    payload: List[ProductSyncRequest],
    shop: ShopRecord = Depends(require_pos_shop),
) -> BulkSyncResponse:
    return run_with_shop_retry(shop, lambda active_shop: inventory_service.sync_bulk(payload, active_shop))


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
