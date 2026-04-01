import base64
import hashlib
import secrets
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Optional, Tuple

from cryptography.fernet import Fernet, InvalidToken

from app.utils import AuthorizationError, setup_logging, utc_now_iso


@dataclass(frozen=True)
class ShopRecord:
    shop_domain: str
    access_token: str
    scope: Optional[str] = None
    refresh_token: Optional[str] = None
    access_token_expires_at: Optional[str] = None
    refresh_token_expires_at: Optional[str] = None
    shop_name: Optional[str] = None
    myshopify_domain: Optional[str] = None
    installed_at: Optional[str] = None
    updated_at: Optional[str] = None
    uninstalled_at: Optional[str] = None


@dataclass(frozen=True)
class PosCredentialRecord:
    shop_domain: str
    api_key: str
    api_secret_masked: str
    created_at: str
    rotated_at: Optional[str] = None
    last_used_at: Optional[str] = None


@dataclass(frozen=True)
class PosAuthRecord:
    shop_domain: str
    api_key: str
    secret_salt: str
    secret_hash: str
    secret_ciphertext: Optional[str]


@dataclass(frozen=True)
class FeedEventRow:
    id: int
    shop_domain: str
    source: str
    endpoint: str
    method: str
    sku: Optional[str]
    title: Optional[str]
    success: bool
    message: str
    product_id: Optional[str]
    variant_id: Optional[str]
    request_payload: str
    normalized_payload: Optional[str]
    received_at: str


@dataclass(frozen=True)
class RequestLogRow:
    id: int
    shop_domain: Optional[str]
    api_key_preview: Optional[str]
    method: str
    path: str
    query_string: Optional[str]
    status_code: int
    route_path: Optional[str]
    request_body: Optional[str]
    user_agent: Optional[str]
    source_ip: Optional[str]
    duration_ms: int
    created_at: str


class DatabaseStore:
    def __init__(self, database_path: str, encryption_secret: str) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logging().getChild("db")
        self._lock = Lock()
        self._fernet = _build_fernet(encryption_secret)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS shops (
                    shop_domain TEXT PRIMARY KEY,
                    access_token TEXT NOT NULL,
                    scope TEXT,
                    refresh_token TEXT,
                    access_token_expires_at TEXT,
                    refresh_token_expires_at TEXT,
                    shop_name TEXT,
                    myshopify_domain TEXT,
                    installed_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    uninstalled_at TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS pos_credentials (
                    shop_domain TEXT PRIMARY KEY,
                    api_key TEXT UNIQUE NOT NULL,
                    secret_salt TEXT NOT NULL,
                    secret_hash TEXT NOT NULL,
                    secret_preview TEXT NOT NULL,
                    secret_ciphertext TEXT,
                    created_at TEXT NOT NULL,
                    rotated_at TEXT,
                    last_used_at TEXT,
                    FOREIGN KEY(shop_domain) REFERENCES shops(shop_domain)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS feed_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shop_domain TEXT NOT NULL,
                    source TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    method TEXT NOT NULL,
                    sku TEXT,
                    title TEXT,
                    success INTEGER NOT NULL,
                    message TEXT NOT NULL,
                    product_id TEXT,
                    variant_id TEXT,
                    request_payload TEXT NOT NULL,
                    normalized_payload TEXT,
                    received_at TEXT NOT NULL,
                    FOREIGN KEY(shop_domain) REFERENCES shops(shop_domain)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS request_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shop_domain TEXT,
                    api_key_preview TEXT,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    query_string TEXT,
                    status_code INTEGER NOT NULL,
                    route_path TEXT,
                    request_body TEXT,
                    user_agent TEXT,
                    source_ip TEXT,
                    duration_ms INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(shop_domain) REFERENCES shops(shop_domain)
                )
                """
            )
            self._ensure_column(connection, "pos_credentials", "secret_ciphertext", "TEXT")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_pos_credentials_api_key ON pos_credentials(api_key)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_feed_events_shop_created ON feed_events(shop_domain, received_at DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_request_logs_shop_created ON request_logs(shop_domain, created_at DESC)"
            )
            connection.commit()

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_definition: str,
    ) -> None:
        columns = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name in columns:
            return
        connection.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
        )

    def upsert_shop(
        self,
        *,
        shop_domain: str,
        access_token: str,
        scope: Optional[str],
        refresh_token: Optional[str],
        access_token_expires_at: Optional[str],
        refresh_token_expires_at: Optional[str],
        shop_name: Optional[str],
        myshopify_domain: Optional[str],
    ) -> ShopRecord:
        now = utc_now_iso()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO shops (
                    shop_domain,
                    access_token,
                    scope,
                    refresh_token,
                    access_token_expires_at,
                    refresh_token_expires_at,
                    shop_name,
                    myshopify_domain,
                    installed_at,
                    updated_at,
                    uninstalled_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                ON CONFLICT(shop_domain) DO UPDATE SET
                    access_token=excluded.access_token,
                    scope=excluded.scope,
                    refresh_token=excluded.refresh_token,
                    access_token_expires_at=excluded.access_token_expires_at,
                    refresh_token_expires_at=excluded.refresh_token_expires_at,
                    shop_name=excluded.shop_name,
                    myshopify_domain=excluded.myshopify_domain,
                    updated_at=excluded.updated_at,
                    uninstalled_at=NULL
                """,
                (
                    shop_domain,
                    access_token,
                    scope,
                    refresh_token,
                    access_token_expires_at,
                    refresh_token_expires_at,
                    shop_name,
                    myshopify_domain,
                    now,
                    now,
                ),
            )
            connection.commit()
        return self.get_shop(shop_domain)

    def get_shop(self, shop_domain: str) -> Optional[ShopRecord]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT *
                FROM shops
                WHERE shop_domain = ?
                  AND uninstalled_at IS NULL
                """,
                (shop_domain,),
            ).fetchone()
        return self._row_to_shop(row)

    def shop_count(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM shops WHERE uninstalled_at IS NULL"
            ).fetchone()
        return int(row["count"]) if row else 0

    def ensure_pos_credentials(
        self,
        shop_domain: str,
    ) -> Tuple[PosCredentialRecord, Optional[str]]:
        existing = self.get_pos_credentials_for_shop(shop_domain)
        if existing is not None:
            return existing, None
        return self.rotate_pos_credentials(shop_domain, is_first_issue=True)

    def rotate_pos_credentials(
        self,
        shop_domain: str,
        *,
        is_first_issue: bool = False,
    ) -> Tuple[PosCredentialRecord, str]:
        raw_secret = f"sec_{secrets.token_hex(32)}"
        api_key = f"pos_{secrets.token_hex(16)}"
        salt, secret_hash = _hash_secret(raw_secret)
        preview = _mask_secret(raw_secret)
        ciphertext = self._fernet.encrypt(raw_secret.encode("utf-8")).decode("utf-8")
        now = utc_now_iso()

        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO pos_credentials (
                        shop_domain,
                        api_key,
                        secret_salt,
                        secret_hash,
                        secret_preview,
                        secret_ciphertext,
                        created_at,
                        rotated_at,
                        last_used_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    ON CONFLICT(shop_domain) DO UPDATE SET
                        api_key=excluded.api_key,
                        secret_salt=excluded.secret_salt,
                        secret_hash=excluded.secret_hash,
                        secret_preview=excluded.secret_preview,
                        secret_ciphertext=excluded.secret_ciphertext,
                        rotated_at=excluded.rotated_at
                    """,
                    (
                        shop_domain,
                        api_key,
                        salt,
                        secret_hash,
                        preview,
                        ciphertext,
                        now,
                        None if is_first_issue else now,
                    ),
                )
                connection.commit()

        credentials = self.get_pos_credentials_for_shop(shop_domain)
        if credentials is None:
            raise AuthorizationError("Failed to generate POS credentials.")
        return credentials, raw_secret

    def get_pos_credentials_for_shop(self, shop_domain: str) -> Optional[PosCredentialRecord]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT shop_domain, api_key, secret_preview, created_at, rotated_at, last_used_at
                FROM pos_credentials
                WHERE shop_domain = ?
                """,
                (shop_domain,),
            ).fetchone()
        if row is None:
            return None
        return PosCredentialRecord(
            shop_domain=row["shop_domain"],
            api_key=row["api_key"],
            api_secret_masked=row["secret_preview"],
            created_at=row["created_at"],
            rotated_at=row["rotated_at"],
            last_used_at=row["last_used_at"],
        )

    def get_pos_auth_record(self, api_key: str) -> Optional[PosAuthRecord]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    shops.shop_domain AS shop_domain,
                    pos_credentials.api_key AS api_key,
                    pos_credentials.secret_salt AS secret_salt,
                    pos_credentials.secret_hash AS secret_hash,
                    pos_credentials.secret_ciphertext AS secret_ciphertext
                FROM pos_credentials
                JOIN shops ON shops.shop_domain = pos_credentials.shop_domain
                WHERE pos_credentials.api_key = ?
                  AND shops.uninstalled_at IS NULL
                """,
                (api_key,),
            ).fetchone()
        if row is None:
            return None
        return PosAuthRecord(
            shop_domain=row["shop_domain"],
            api_key=row["api_key"],
            secret_salt=row["secret_salt"],
            secret_hash=row["secret_hash"],
            secret_ciphertext=row["secret_ciphertext"],
        )

    def verify_pos_credentials(self, api_key: str, api_secret: str) -> ShopRecord:
        auth_record = self.get_pos_auth_record(api_key)
        if auth_record is None or not _verify_secret(
            api_secret,
            salt_hex=auth_record.secret_salt,
            secret_hash=auth_record.secret_hash,
        ):
            raise AuthorizationError(
                "Invalid POS API credentials.",
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

        with self._connect() as connection:
            connection.execute(
                """
                UPDATE pos_credentials
                SET last_used_at = ?
                WHERE api_key = ?
                """,
                (utc_now_iso(), api_key),
            )
            connection.commit()

        shop = self.get_shop(auth_record.shop_domain)
        if shop is None:
            raise AuthorizationError("This shop is not installed or has been disconnected.")
        return shop

    def verify_query_string_credentials(self, api_key: str, api_secret: str) -> ShopRecord:
        return self.verify_pos_credentials(api_key, api_secret)

    def get_query_auth_secret(self, api_key: str) -> Tuple[ShopRecord, str]:
        auth_record = self.get_pos_auth_record(api_key)
        if auth_record is None:
            raise AuthorizationError("Invalid POS API credentials.")

        if not auth_record.secret_ciphertext:
            raise AuthorizationError(
                "Rotate the POS credentials once before using Woo-style signed requests.",
                {
                    "required_action": "rotate_credentials",
                    "shop": auth_record.shop_domain,
                },
            )

        try:
            raw_secret = self._fernet.decrypt(auth_record.secret_ciphertext.encode("utf-8")).decode("utf-8")
        except (InvalidToken, ValueError) as exc:
            raise AuthorizationError(
                "Stored POS credentials could not be decrypted. Rotate the credentials and try again.",
                {
                    "required_action": "rotate_credentials",
                    "shop": auth_record.shop_domain,
                },
            ) from exc

        shop = self.get_shop(auth_record.shop_domain)
        if shop is None:
            raise AuthorizationError("This shop is not installed or has been disconnected.")
        return shop, raw_secret

    def mark_pos_credentials_used(self, api_key: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE pos_credentials
                SET last_used_at = ?
                WHERE api_key = ?
                """,
                (utc_now_iso(), api_key),
            )
            connection.commit()

    def record_feed_event(
        self,
        *,
        shop_domain: str,
        source: str,
        endpoint: str,
        method: str,
        sku: Optional[str],
        title: Optional[str],
        success: bool,
        message: str,
        product_id: Optional[str],
        variant_id: Optional[str],
        request_payload: str,
        normalized_payload: Optional[str],
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO feed_events (
                    shop_domain,
                    source,
                    endpoint,
                    method,
                    sku,
                    title,
                    success,
                    message,
                    product_id,
                    variant_id,
                    request_payload,
                    normalized_payload,
                    received_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    shop_domain,
                    source,
                    endpoint,
                    method.upper(),
                    sku,
                    title,
                    1 if success else 0,
                    message,
                    product_id,
                    variant_id,
                    request_payload,
                    normalized_payload,
                    utc_now_iso(),
                ),
            )
            connection.commit()

    def record_request_log(
        self,
        *,
        shop_domain: Optional[str],
        api_key_preview: Optional[str],
        method: str,
        path: str,
        query_string: Optional[str],
        status_code: int,
        route_path: Optional[str],
        request_body: Optional[str],
        user_agent: Optional[str],
        source_ip: Optional[str],
        duration_ms: int,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO request_logs (
                    shop_domain,
                    api_key_preview,
                    method,
                    path,
                    query_string,
                    status_code,
                    route_path,
                    request_body,
                    user_agent,
                    source_ip,
                    duration_ms,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    shop_domain,
                    api_key_preview,
                    method.upper(),
                    path,
                    query_string,
                    int(status_code),
                    route_path,
                    request_body,
                    user_agent,
                    source_ip,
                    int(duration_ms),
                    utc_now_iso(),
                ),
            )
            connection.commit()

    def list_feed_events(self, shop_domain: str, *, limit: int = 50) -> list[FeedEventRow]:
        safe_limit = max(1, min(limit, 500))
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM feed_events
                WHERE shop_domain = ?
                ORDER BY received_at DESC, id DESC
                LIMIT ?
                """,
                (shop_domain, safe_limit),
            ).fetchall()
        return [self._row_to_feed_event(row) for row in rows]

    def feed_event_count(self, shop_domain: str) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM feed_events WHERE shop_domain = ?",
                (shop_domain,),
            ).fetchone()
        return int(row["count"]) if row else 0

    def list_request_logs(
        self,
        *,
        shop_domain: Optional[str] = None,
        limit: int = 100,
    ) -> list[RequestLogRow]:
        safe_limit = max(1, min(limit, 1000))
        with self._connect() as connection:
            if shop_domain:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM request_logs
                    WHERE shop_domain = ? OR shop_domain IS NULL
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                    """,
                    (shop_domain, safe_limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT *
                    FROM request_logs
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
        return [self._row_to_request_log(row) for row in rows]

    def request_log_count(self, *, shop_domain: Optional[str] = None) -> int:
        with self._connect() as connection:
            if shop_domain:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM request_logs WHERE shop_domain = ? OR shop_domain IS NULL",
                    (shop_domain,),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM request_logs"
                ).fetchone()
        return int(row["count"]) if row else 0

    def mark_shop_uninstalled(self, shop_domain: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE shops
                SET uninstalled_at = ?, updated_at = ?
                WHERE shop_domain = ?
                """,
                (utc_now_iso(), utc_now_iso(), shop_domain),
            )
            connection.execute(
                "DELETE FROM pos_credentials WHERE shop_domain = ?",
                (shop_domain,),
            )
            connection.commit()

    @staticmethod
    def _row_to_shop(row: Optional[sqlite3.Row]) -> Optional[ShopRecord]:
        if row is None:
            return None

        return ShopRecord(
            shop_domain=row["shop_domain"],
            access_token=row["access_token"],
            scope=row["scope"],
            refresh_token=row["refresh_token"],
            access_token_expires_at=row["access_token_expires_at"],
            refresh_token_expires_at=row["refresh_token_expires_at"],
            shop_name=row["shop_name"],
            myshopify_domain=row["myshopify_domain"],
            installed_at=row["installed_at"],
            updated_at=row["updated_at"],
            uninstalled_at=row["uninstalled_at"],
        )

    @staticmethod
    def _row_to_feed_event(row: sqlite3.Row) -> FeedEventRow:
        return FeedEventRow(
            id=int(row["id"]),
            shop_domain=row["shop_domain"],
            source=row["source"],
            endpoint=row["endpoint"],
            method=row["method"],
            sku=row["sku"],
            title=row["title"],
            success=bool(row["success"]),
            message=row["message"],
            product_id=row["product_id"],
            variant_id=row["variant_id"],
            request_payload=row["request_payload"],
            normalized_payload=row["normalized_payload"],
            received_at=row["received_at"],
        )

    @staticmethod
    def _row_to_request_log(row: sqlite3.Row) -> RequestLogRow:
        return RequestLogRow(
            id=int(row["id"]),
            shop_domain=row["shop_domain"],
            api_key_preview=row["api_key_preview"],
            method=row["method"],
            path=row["path"],
            query_string=row["query_string"],
            status_code=int(row["status_code"]),
            route_path=row["route_path"],
            request_body=row["request_body"],
            user_agent=row["user_agent"],
            source_ip=row["source_ip"],
            duration_ms=int(row["duration_ms"]),
            created_at=row["created_at"],
        )


def _hash_secret(secret: str) -> Tuple[str, str]:
    salt = secrets.token_bytes(16)
    secret_hash = hashlib.scrypt(secret.encode("utf-8"), salt=salt, n=16384, r=8, p=1)
    return salt.hex(), secret_hash.hex()


def _verify_secret(secret: str, *, salt_hex: str, secret_hash: str) -> bool:
    candidate = hashlib.scrypt(
        secret.encode("utf-8"),
        salt=bytes.fromhex(salt_hex),
        n=16384,
        r=8,
        p=1,
    ).hex()
    return secrets.compare_digest(candidate, secret_hash)


def _mask_secret(secret: str) -> str:
    return f"{secret[:6]}...{secret[-4:]}"


def _build_fernet(secret: str) -> Fernet:
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    key = base64.urlsafe_b64encode(digest)
    return Fernet(key)
