import hashlib
import io
import re
import zipfile
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit


CONNECTOR_ARTIFACT_VERSION = "1.2.0"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SETUP_SCRIPT = PROJECT_ROOT / "windows_connector" / "quick_install.ps1"
RUNTIME_FILES = (
    "windows_connector/connector.py",
    "windows_connector/order_dbf.py",
    "windows_connector/write_pos_quantity.ps1",
    "windows_connector/install.ps1",
    "windows_connector/uninstall.ps1",
    "windows_connector/connector.env.example",
    "windows_connector/README.md",
    "windows_connector/SHOPIFY_ORDER_DB_SCHEMA.md",
    "jbarbaro_db/dbf_pos_sync.py",
)
_SAFE_SHOP = re.compile(r"^[a-z0-9][a-z0-9.-]*\.myshopify\.com$")
_SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_-]+$")
_ZIP_TIMESTAMP = (2026, 1, 1, 0, 0, 0)


@lru_cache(maxsize=1)
def setup_script_bytes() -> bytes:
    return SETUP_SCRIPT.read_bytes()


@lru_cache(maxsize=4)
def build_runtime_archive(project_root: Path = PROJECT_ROOT) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(
        output,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        for relative_path in RUNTIME_FILES:
            source_path = project_root / relative_path
            _write_zip_member(archive, relative_path, source_path.read_bytes())
        _write_zip_member(archive, "windows_connector/requirements.txt", b"requests>=2.32,<3\n")
    return output.getvalue()


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest().upper()


def build_personalized_installer_bat(
    *,
    base_url: str,
    shop_domain: str,
    enrollment_token: str,
    setup_sha256: str,
    runtime_sha256: str,
) -> bytes:
    normalized_base_url = _validated_base_url(base_url)
    normalized_shop = str(shop_domain or "").strip().lower()
    if not _SAFE_SHOP.fullmatch(normalized_shop):
        raise ValueError("Invalid Shopify shop domain for installer.")
    if not _SAFE_TOKEN.fullmatch(str(enrollment_token or "")):
        raise ValueError("Invalid connector enrollment token.")
    for digest in (setup_sha256, runtime_sha256):
        if not re.fullmatch(r"[A-Fa-f0-9]{64}", str(digest or "")):
            raise ValueError("Invalid installer artifact digest.")

    lines = [
        "@echo off",
        "setlocal DisableDelayedExpansion",
        "title Inventory Sync POS Bridge Setup",
        "color 0A",
        "echo.",
        f"echo Installing the Cash Register Express connector for {normalized_shop}",
        "echo Keep this window open. Windows will ask for Administrator approval.",
        "echo.",
        f'set "SHOPIFY_CONNECTOR_ENROLLMENT_TOKEN={enrollment_token}"',
        f'set "SHOPIFY_CONNECTOR_EXPECTED_SHOP={normalized_shop}"',
        f'set "SHOPIFY_CONNECTOR_SETUP_URL={normalized_base_url}/api/connector/setup.ps1"',
        f'set "SHOPIFY_CONNECTOR_RUNTIME_URL={normalized_base_url}/api/connector/runtime.zip"',
        f'set "SHOPIFY_CONNECTOR_SETUP_SHA256={setup_sha256.upper()}"',
        f'set "SHOPIFY_CONNECTOR_RUNTIME_SHA256={runtime_sha256.upper()}"',
        f'set "SHOPIFY_CONNECTOR_BASE_URL={normalized_base_url}"',
        'set "SHOPIFY_CONNECTOR_SETUP_FILE=%TEMP%\\InventorySyncPosBridgeSetup-%RANDOM%-%RANDOM%.ps1"',
        "powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -Command \"$ErrorActionPreference='Stop'; $ProgressPreference='SilentlyContinue'; [Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -UseBasicParsing -Uri $env:SHOPIFY_CONNECTOR_SETUP_URL -OutFile $env:SHOPIFY_CONNECTOR_SETUP_FILE; $actual=(Get-FileHash -Algorithm SHA256 -LiteralPath $env:SHOPIFY_CONNECTOR_SETUP_FILE).Hash; if ($actual -ne $env:SHOPIFY_CONNECTOR_SETUP_SHA256) { throw 'The setup file did not pass its security check. Download a fresh installer from the Inventory Sync app.' }; & $env:SHOPIFY_CONNECTOR_SETUP_FILE; exit $LASTEXITCODE\"",
        'set "SHOPIFY_CONNECTOR_RESULT=%ERRORLEVEL%"',
        'del /Q "%SHOPIFY_CONNECTOR_SETUP_FILE%" >nul 2>&1',
        'set "SHOPIFY_CONNECTOR_ENROLLMENT_TOKEN="',
        "if not \"%SHOPIFY_CONNECTOR_RESULT%\"==\"0\" goto FAILED",
        "echo.",
        "echo Setup completed successfully.",
        "echo You can close this window.",
        "echo.",
        "pause",
        "exit /b 0",
        ":FAILED",
        "echo.",
        "echo SETUP DID NOT FINISH.",
        "echo Read the message above, then download a fresh installer from the Inventory Sync app if needed.",
        "echo.",
        "pause",
        "exit /b %SHOPIFY_CONNECTOR_RESULT%",
        "",
    ]
    return "\r\n".join(lines).encode("utf-8")


def installer_filename(shop_domain: str) -> str:
    shop_name = str(shop_domain or "shop").split(".", 1)[0]
    safe_name = re.sub(r"[^A-Za-z0-9_-]+", "-", shop_name).strip("-") or "shop"
    return f"Install-Inventory-Sync-POS-Bridge-{safe_name}.bat"


def _validated_base_url(value: str) -> str:
    normalized = str(value or "").strip().rstrip("/")
    parsed = urlsplit(normalized)
    is_local = parsed.hostname in {"127.0.0.1", "localhost"}
    if (
        parsed.scheme not in ({"http", "https"} if is_local else {"https"})
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("The connector installer requires a valid HTTPS APP_BASE_URL.")
    return normalized


def _write_zip_member(archive: zipfile.ZipFile, name: str, payload: bytes) -> None:
    if name.startswith(("/", "\\")) or ".." in Path(name).parts:
        raise ValueError(f"Unsafe runtime archive member: {name}")
    info = zipfile.ZipInfo(filename=name, date_time=_ZIP_TIMESTAMP)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = 0o100644 << 16
    archive.writestr(info, payload)
