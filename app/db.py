import hashlib
import secrets
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Optional, Tuple

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


class DatabaseStore:
    def __init__(self, database_path: str) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = setup_logging().getChild("db")
        self._lock = Lock()
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
                    created_at TEXT NOT NULL,
                    rotated_at TEXT,
                    last_used_at TEXT,
                    FOREIGN KEY(shop_domain) REFERENCES shops(shop_domain)
                )
                """
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_pos_credentials_api_key ON pos_credentials(api_key)"
            )
            connection.commit()

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
                        created_at,
                        rotated_at,
                        last_used_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
                    ON CONFLICT(shop_domain) DO UPDATE SET
                        api_key=excluded.api_key,
                        secret_salt=excluded.secret_salt,
                        secret_hash=excluded.secret_hash,
                        secret_preview=excluded.secret_preview,
                        rotated_at=excluded.rotated_at
                    """,
                    (
                        shop_domain,
                        api_key,
                        salt,
                        secret_hash,
                        preview,
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

    def verify_pos_credentials(self, api_key: str, api_secret: str) -> ShopRecord:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT
                    shops.*,
                    pos_credentials.api_key,
                    pos_credentials.secret_salt,
                    pos_credentials.secret_hash
                FROM pos_credentials
                JOIN shops ON shops.shop_domain = pos_credentials.shop_domain
                WHERE pos_credentials.api_key = ?
                  AND shops.uninstalled_at IS NULL
                """,
                (api_key,),
            ).fetchone()

            if row is None or not _verify_secret(
                api_secret,
                salt_hex=row["secret_salt"],
                secret_hash=row["secret_hash"],
            ):
                raise AuthorizationError(
                    "Invalid POS API credentials.",
                    {
                        "accepted_auth": ["basic", "x-api-key/x-api-secret"],
                        "header_names": ["X-API-Key", "X-API-Secret"],
                    },
                )

            connection.execute(
                """
                UPDATE pos_credentials
                SET last_used_at = ?
                WHERE api_key = ?
                """,
                (utc_now_iso(), api_key),
            )
            connection.commit()

        shop = self.get_shop(row["shop_domain"])
        if shop is None:
            raise AuthorizationError("This shop is not installed or has been disconnected.")
        return shop

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
