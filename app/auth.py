import base64
import hashlib
import hmac
import json
import re
import secrets
import time
from threading import Lock
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qsl, quote, urlencode

import requests
from fastapi import Request, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.config import Settings
from app.utils import AuthenticationError, AuthorizationError, seconds_from_now_iso


pos_basic_security = HTTPBasic(auto_error=False)
SHOP_DOMAIN_PATTERN = re.compile(r"^[a-z0-9][a-z0-9\-]*\.myshopify\.com$")
WOO_AUTH_WINDOW_SECONDS = 15 * 60
WOO_SIGNATURE_HASHES = {
    "HMAC-SHA1": hashlib.sha1,
    "HMAC-SHA256": hashlib.sha256,
}


class WooNonceStore:
    def __init__(self, *, ttl_seconds: int = WOO_AUTH_WINDOW_SECONDS, max_entries: int = 5000) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._lock = Lock()
        self._entries: Dict[Tuple[str, str], int] = {}

    def register(self, api_key: str, nonce: str, timestamp: int) -> bool:
        now = int(time.time())
        cutoff = now - self.ttl_seconds

        with self._lock:
            stale_keys = [key for key, value in self._entries.items() if value < cutoff]
            for key in stale_keys:
                self._entries.pop(key, None)

            cache_key = (api_key, nonce)
            if cache_key in self._entries:
                return False

            if len(self._entries) >= self.max_entries:
                oldest_key = min(self._entries, key=self._entries.get)
                self._entries.pop(oldest_key, None)

            self._entries[cache_key] = timestamp
            return True


class AppSessionManager:
    SESSION_COOKIE = "inventory_sync_session"
    OAUTH_COOKIE = "inventory_sync_oauth"
    SECRET_FLASH_COOKIE = "inventory_sync_secret_flash"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._secret = settings.app_session_secret.encode("utf-8")
        self._secure = settings.normalized_app_base_url.startswith("https://")
        self._same_site = "none" if self._secure else "lax"

    def set_app_session(self, response: Response, *, shop: str, host: Optional[str]) -> None:
        self._set_cookie(
            response,
            self.SESSION_COOKIE,
            {"shop": shop, "host": host},
            max_age=30 * 24 * 60 * 60,
        )

    def get_app_session(self, request: Request) -> Optional[Dict[str, Any]]:
        return self._load_cookie(request.cookies.get(self.SESSION_COOKIE))

    def clear_app_session(self, response: Response) -> None:
        response.delete_cookie(self.SESSION_COOKIE, path="/")

    def set_oauth_state(
        self,
        response: Response,
        *,
        state: str,
        shop: str,
        host: Optional[str],
        return_to: Optional[str],
    ) -> None:
        self._set_cookie(
            response,
            self.OAUTH_COOKIE,
            {"state": state, "shop": shop, "host": host, "return_to": return_to},
            max_age=10 * 60,
        )

    def get_oauth_state(self, request: Request) -> Optional[Dict[str, Any]]:
        return self._load_cookie(request.cookies.get(self.OAUTH_COOKIE))

    def clear_oauth_state(self, response: Response) -> None:
        response.delete_cookie(self.OAUTH_COOKIE, path="/")

    def set_secret_flash(self, response: Response, *, shop: str, api_secret: str) -> None:
        self._set_cookie(
            response,
            self.SECRET_FLASH_COOKIE,
            {"shop": shop, "api_secret": api_secret},
            max_age=15 * 60,
        )

    def pop_secret_flash(self, request: Request, response: Response, *, shop: str) -> Optional[str]:
        payload = self._load_cookie(request.cookies.get(self.SECRET_FLASH_COOKIE))
        if not payload or payload.get("shop") != shop:
            return None
        self.clear_secret_flash(response)
        return payload.get("api_secret")

    def clear_secret_flash(self, response: Response) -> None:
        response.delete_cookie(self.SECRET_FLASH_COOKIE, path="/")

    def _set_cookie(
        self,
        response: Response,
        key: str,
        payload: Dict[str, Any],
        *,
        max_age: int,
    ) -> None:
        token = _sign_payload(self._secret, payload, max_age=max_age)
        response.set_cookie(
            key,
            token,
            max_age=max_age,
            httponly=True,
            secure=self._secure,
            samesite=self._same_site,
            path="/",
        )

    def _load_cookie(self, token: Optional[str]) -> Optional[Dict[str, Any]]:
        if not token:
            return None
        return _verify_signed_payload(self._secret, token)


def validate_shop_domain(shop: str) -> str:
    normalized = (shop or "").strip().lower()
    if not SHOP_DOMAIN_PATTERN.fullmatch(normalized):
        raise AuthorizationError(
            "A valid Shopify shop domain is required.",
            {"field": "shop", "expected_format": "store-name.myshopify.com"},
        )
    return normalized


def extract_pos_credentials(
    credentials: Optional[HTTPBasicCredentials],
    x_api_key: Optional[str],
    x_api_secret: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    if credentials:
        return credentials.username, credentials.password
    return x_api_key, x_api_secret


def extract_woo_query_credentials(request: Request) -> Tuple[Optional[str], Optional[str]]:
    api_key = request.query_params.get("consumer_key") or request.query_params.get("oauth_consumer_key")
    api_secret = request.query_params.get("consumer_secret")
    return api_key, api_secret


def has_woo_oauth_signature(request: Request) -> bool:
    return bool(request.query_params.get("oauth_signature"))


def verify_woo_oauth_request(
    request: Request,
    *,
    api_key: str,
    api_secret: str,
    nonce_store: WooNonceStore,
) -> None:
    signature = request.query_params.get("oauth_signature")
    nonce = request.query_params.get("oauth_nonce")
    timestamp_raw = request.query_params.get("oauth_timestamp")
    signature_method = (request.query_params.get("oauth_signature_method") or "HMAC-SHA256").upper()

    if not signature or not nonce or not timestamp_raw:
        raise AuthorizationError(
            "Woo-style signed requests must include oauth_nonce, oauth_timestamp, and oauth_signature.",
            {"api_key": api_key},
        )

    if signature_method not in WOO_SIGNATURE_HASHES:
        raise AuthorizationError(
            "Unsupported OAuth signature method.",
            {"signature_method": signature_method, "accepted": sorted(WOO_SIGNATURE_HASHES)},
        )

    try:
        timestamp = int(timestamp_raw)
    except (TypeError, ValueError) as exc:
        raise AuthorizationError("Invalid OAuth timestamp.", {"oauth_timestamp": timestamp_raw}) from exc

    now = int(time.time())
    if abs(now - timestamp) > WOO_AUTH_WINDOW_SECONDS:
        raise AuthorizationError(
            "OAuth timestamp is outside the accepted window.",
            {"oauth_timestamp": timestamp, "server_timestamp": now},
        )

    if not nonce_store.register(api_key, nonce, timestamp):
        raise AuthorizationError(
            "OAuth nonce has already been used.",
            {"oauth_nonce": nonce, "api_key": api_key},
        )

    provided_signature = signature.replace(" ", "+")
    expected_signature = build_woo_signature(
        request=request,
        api_secret=api_secret,
        signature_method=signature_method,
    )
    if not secrets.compare_digest(expected_signature, provided_signature):
        raise AuthorizationError(
            "Woo-style OAuth signature verification failed.",
            {
                "api_key": api_key,
                "signature_method": signature_method,
            },
        )


def build_woo_signature(
    *,
    request: Request,
    api_secret: str,
    signature_method: str,
) -> str:
    digestmod = WOO_SIGNATURE_HASHES[signature_method]
    base_url = str(request.url).split("?", 1)[0]
    params = []

    for key, value in request.query_params.multi_items():
        if key == "oauth_signature":
            continue
        params.append((key, value))

    normalized_pairs = sorted(
        (_oauth_quote(key), _oauth_quote(value))
        for key, value in params
    )
    parameter_string = "&".join(f"{key}={value}" for key, value in normalized_pairs)
    base_string = "&".join(
        [
            request.method.upper(),
            _oauth_quote(base_url),
            _oauth_quote(parameter_string),
        ]
    )
    signing_key = f"{_oauth_quote(api_secret)}&"
    digest = hmac.new(signing_key.encode("utf-8"), base_string.encode("utf-8"), digestmod).digest()
    return base64.b64encode(digest).decode("utf-8")


def _oauth_quote(value: Any) -> str:
    return quote(str(value), safe="~-._")


def verify_shopify_query_hmac(query_string: str, client_secret: str) -> bool:
    pairs = parse_qsl(query_string, keep_blank_values=True)
    provided_hmac = None
    message_parts = []

    for key, value in pairs:
        if key == "hmac":
            provided_hmac = value
            continue
        if key == "signature":
            continue
        message_parts.append(f"{key}={value}")

    if not provided_hmac:
        return False

    message = "&".join(sorted(message_parts))
    digest = hmac.new(
        client_secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return secrets.compare_digest(digest, provided_hmac)


def verify_shopify_webhook_hmac(body: bytes, hmac_header: Optional[str], client_secret: str) -> bool:
    if not hmac_header:
        return False
    digest = hmac.new(client_secret.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return secrets.compare_digest(expected, hmac_header)


def build_authorize_url(settings: Settings, *, shop: str, state: str) -> str:
    query = urlencode(
        {
            "client_id": settings.shopify_client_id,
            "scope": ",".join(settings.scope_list),
            "redirect_uri": settings.oauth_redirect_url,
            "state": state,
        }
    )
    return f"https://{shop}/admin/oauth/authorize?{query}"


def exchange_authorization_code(settings: Settings, *, shop: str, code: str) -> Dict[str, Any]:
    response = _token_request(
        settings,
        shop=shop,
        payload={
            "client_id": settings.shopify_client_id,
            "client_secret": settings.shopify_client_secret,
            "code": code,
        },
        failure_message="Shopify OAuth code exchange failed.",
    )
    return _normalize_token_payload(response)


def refresh_access_token(settings: Settings, *, shop: str, refresh_token: str) -> Dict[str, Any]:
    response = _token_request(
        settings,
        shop=shop,
        payload={
            "client_id": settings.shopify_client_id,
            "client_secret": settings.shopify_client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        failure_message="Shopify access token refresh failed.",
    )
    return _normalize_token_payload(response)


def _token_request(
    settings: Settings,
    *,
    shop: str,
    payload: Dict[str, str],
    failure_message: str,
) -> Dict[str, Any]:
    url = f"https://{shop}/admin/oauth/access_token"

    try:
        response = requests.post(
            url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=settings.shopify_request_timeout_seconds,
        )
    except requests.RequestException as exc:
        raise AuthenticationError(
            "Failed to reach Shopify OAuth endpoint.",
            {"shop": shop, "reason": str(exc)},
        ) from exc

    payload_data = _parse_response_json(response)
    if response.status_code >= 400:
        raise AuthenticationError(
            failure_message,
            {
                "shop": shop,
                "status_code": response.status_code,
                "response": payload_data,
            },
        )

    access_token = payload_data.get("access_token")
    if not access_token:
        raise AuthenticationError(
            "Shopify OAuth response did not include an access token.",
            {"shop": shop, "response": payload_data},
        )

    return payload_data


def _normalize_token_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    expires_in = _coerce_int(payload.get("expires_in"))
    refresh_expires_in = _coerce_int(payload.get("refresh_token_expires_in"))
    return {
        "access_token": payload.get("access_token"),
        "scope": payload.get("scope"),
        "refresh_token": payload.get("refresh_token"),
        "access_token_expires_at": seconds_from_now_iso(expires_in),
        "refresh_token_expires_at": seconds_from_now_iso(refresh_expires_in),
        "raw": payload,
    }


def _parse_response_json(response: requests.Response) -> Dict[str, Any]:
    try:
        return response.json()
    except ValueError:
        return {"raw": response.text}


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _sign_payload(secret: bytes, payload: Dict[str, Any], *, max_age: int) -> str:
    body = {
        "exp": int(_now_timestamp()) + int(max_age),
        "payload": payload,
    }
    raw = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    encoded = _b64encode(raw)
    signature = hmac.new(secret, encoded.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{encoded}.{signature}"


def _verify_signed_payload(secret: bytes, token: str) -> Optional[Dict[str, Any]]:
    try:
        encoded, signature = token.rsplit(".", 1)
    except ValueError:
        return None

    expected = hmac.new(secret, encoded.encode("utf-8"), hashlib.sha256).hexdigest()
    if not secrets.compare_digest(expected, signature):
        return None

    try:
        payload = json.loads(_b64decode(encoded).decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        return None

    if int(payload.get("exp") or 0) <= int(_now_timestamp()):
        return None
    return payload.get("payload")


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("utf-8").rstrip("=")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _now_timestamp() -> int:
    return int(time.time())
