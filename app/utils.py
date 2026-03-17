import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional


class AppError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        code: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code
        self.details = details or {}


class ConfigurationError(AppError):
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message,
            status_code=500,
            code="configuration_error",
            details=details,
        )


class AuthenticationError(AppError):
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message,
            status_code=502,
            code="shopify_auth_error",
            details=details,
        )


class AuthorizationError(AppError):
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(
            message,
            status_code=401,
            code="authorization_error",
            details=details,
        )


class ShopifyAPIError(AppError):
    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        *,
        status_code: int = 502,
    ) -> None:
        super().__init__(
            message,
            status_code=status_code,
            code="shopify_api_error",
            details=details,
        )


class SyncProcessingError(AppError):
    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        *,
        status_code: int = 400,
        code: str = "sync_error",
    ) -> None:
        super().__init__(
            message,
            status_code=status_code,
            code=code,
            details=details,
        )


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("inventory_sync")
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def seconds_from_now_iso(seconds: Optional[int]) -> Optional[str]:
    if seconds is None:
        return None
    return (utc_now() + timedelta(seconds=int(seconds))).isoformat()


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    if not value:
        return None

    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return None


def get_backoff_delay(
    attempt: int,
    *,
    base_seconds: float = 1.0,
    retry_after_seconds: Optional[float] = None,
) -> float:
    if retry_after_seconds is not None:
        return retry_after_seconds
    return min(base_seconds * (2 ** attempt), 8.0)


def error_payload(
    *,
    code: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
            "timestamp": utc_now_iso(),
        }
    }


def safe_json_dumps(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, default=str, sort_keys=True)


def log_sync_event(
    logger: logging.Logger,
    *,
    sku: str,
    success: bool,
    message: str,
    **fields: Any,
) -> None:
    payload = {
        "sku": sku,
        "success": success,
        "message": message,
        "timestamp": utc_now_iso(),
    }
    payload.update(fields)
    logger.info("sync_event %s", safe_json_dumps(payload))


def has_user_error_code(details: Dict[str, Any], code: str) -> bool:
    user_errors = details.get("user_errors") or []
    return any(item.get("code") == code for item in user_errors if isinstance(item, dict))
