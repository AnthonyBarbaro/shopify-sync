import os
from dataclasses import dataclass
from functools import lru_cache
from typing import List, Optional

from dotenv import load_dotenv

from app.utils import ConfigurationError


load_dotenv()


@dataclass(frozen=True)
class Settings:
    shopify_client_id: str
    shopify_client_secret: str
    app_base_url: str
    app_session_secret: str
    credential_encryption_secret: str
    app_scopes: str
    database_path: str
    shopify_api_version: str = "2026-01"
    shopify_request_timeout_seconds: int = 30
    shopify_retry_attempts: int = 3
    shopify_retry_backoff_seconds: float = 1.0
    shopify_sku_cache_ttl_seconds: int = 15 * 60
    shopify_location_id: Optional[str] = None

    @property
    def normalized_app_base_url(self) -> str:
        return self.app_base_url.rstrip("/")

    @property
    def oauth_redirect_url(self) -> str:
        return f"{self.normalized_app_base_url}/auth/callback"

    @property
    def scope_list(self) -> List[str]:
        return [scope.strip() for scope in self.app_scopes.split(",") if scope.strip()]


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ConfigurationError(f"Missing required environment variable: {name}")
    return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    client_secret = _require_env("SHOPIFY_CLIENT_SECRET")
    return Settings(
        shopify_client_id=_require_env("SHOPIFY_CLIENT_ID"),
        shopify_client_secret=client_secret,
        app_base_url=_require_env("APP_BASE_URL"),
        app_session_secret=(os.getenv("APP_SESSION_SECRET") or "").strip() or client_secret,
        credential_encryption_secret=(
            os.getenv("POS_SECRET_ENCRYPTION_SECRET") or os.getenv("APP_SESSION_SECRET") or client_secret
        ).strip(),
        app_scopes=(
            os.getenv("APP_SCOPES")
            or "read_products,write_products,read_inventory,write_inventory,read_locations"
        ).strip(),
        database_path=(os.getenv("DATABASE_PATH") or "inventory_sync.sqlite3").strip(),
        shopify_api_version=(os.getenv("SHOPIFY_API_VERSION") or "2026-01").strip(),
        shopify_location_id=(os.getenv("SHOPIFY_LOCATION_ID") or "").strip() or None,
    )
