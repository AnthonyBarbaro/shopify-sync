#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import io
import json
import math
import re
import struct
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.parse import parse_qs, urlparse, urlunparse

import requests


DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_BATCH_SIZE = 25
DEFAULT_METAFIELD_NAMESPACE = "pos"
DEFAULT_DBF_DIR = "ashpsdat"
DEFAULT_CUSTOMER_TAGS = ["POS Customer"]
EXPANDED_CUSTOMER_FILES = ("mcust.DBF", "mcust002.dbf")
SHOPIFY_CUSTOMER_CSV_MAX_BYTES = 14 * 1024 * 1024
SHOPIFY_CUSTOMER_CSV_HEADERS = [
    "First Name",
    "Last Name",
    "Email",
    "Accepts Email Marketing",
    "Default Address Company",
    "Default Address Address1",
    "Default Address Address2",
    "Default Address City",
    "Default Address Province Code",
    "Default Address Country Code",
    "Default Address Zip",
    "Default Address Phone",
    "Phone",
    "Accepts SMS Marketing",
    "Accepts WhatsApp Marketing",
    "Tags",
    "Note",
    "Tax Exempt",
]
US_STATE_CODES = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}
CORE_EXPORT_TABLES = (
    "Item",
    "Itemmqty",
    "Vendor",
    "vendors",
    "price",
    "pricechg",
    "Customer",
    "CustShip",
    "Contacts",
    "invhdr",
    "invdtl",
    "minvhdr",
    "minvdtl",
    "ordhdr",
    "orddtl",
    "giftcert",
    "BrideDAT",
)
SENSITIVE_FIELD_PATTERNS = (
    re.compile(r"card", re.IGNORECASE),
    re.compile(r"acct|account", re.IGNORECASE),
    re.compile(r"password|pass", re.IGNORECASE),
    re.compile(r"track2", re.IGNORECASE),
    re.compile(r"micr", re.IGNORECASE),
    re.compile(r"auth", re.IGNORECASE),
    re.compile(r"email|internet", re.IGNORECASE),
    re.compile(r"phone|cell|fax|pager", re.IGNORECASE),
    re.compile(r"address|addr|zip", re.IGNORECASE),
)
GENERIC_DESC2_VALUES = {"CFT", "STATUS"}
STANDARD_SIZE_VALUES = {
    "XS",
    "S",
    "M",
    "L",
    "XL",
    "XXL",
    "XXXL",
    "OVER CALF",
}
TOKEN_CASE_PRESERVE = {
    "AG",
    "BOC",
    "CS",
    "DP",
    "DS",
    "GB",
    "HB",
    "JA",
    "JV",
    "PKSQ",
    "PS",
    "RT",
    "SB",
    "SCO",
    "SP",
    "TUX",
    "UW",
    "ZA",
}
COLOR_VALUE_MAP = {
    "APH34": None,
    "BEIGE MARL": "Beige Marl",
    "BLACK": "Black",
    "BLK": "Black",
    "BLK/WHT": "Black / White",
    "BLUE": "Blue",
    "BROWN": "Brown",
    "BROWN/BLAC": "Brown / Black",
    "BURG": "Burgundy",
    "BURGUNDY": "Burgundy",
    "BUTTER": "Butter",
    "CHARCOAL": "Charcoal",
    "DK BROWN": "Dark Brown",
    "DKBRWN": "Dark Brown",
    "GRASS": "Grass",
    "GRAY": "Gray",
    "GREEN": "Green",
    "KHAKI": "Khaki",
    "L GRAY": "Light Gray",
    "LT GRAY": "Light Gray",
    "MARRONE": "Marrone",
    "MED.BROWN": "Medium Brown",
    "MUSTARD": "Mustard",
    "NAVY": "Navy",
    "OLIVE": "Olive",
    "OLIVE MIX": "Olive Mix",
    "ORN": "Orange",
    "RED": "Red",
    "TAN": "Tan",
    "WHITE": "White",
    "WHT": "White",
}
COLOR_TOKEN_MAP = {
    "BEIGE": "Beige",
    "BLACK": "Black",
    "BLK": "Black",
    "BLUE": "Blue",
    "BRN": "Brown",
    "BROWN": "Brown",
    "BRWN": "Brown",
    "BURG": "Burgundy",
    "DK": "Dark",
    "GRAY": "Gray",
    "GRY": "Gray",
    "KHAKI": "Khaki",
    "LT": "Light",
    "MED": "Medium",
    "NAVY": "Navy",
    "OLIVE": "Olive",
    "ORN": "Orange",
    "RED": "Red",
    "TAN": "Tan",
    "WHT": "White",
    "WHITE": "White",
}
GROUP_PRODUCT_TYPE_MAP = {
    "ACC": "ACCESSORIES",
    "BEL": "BELTS",
    "BOW": "BOW TIES",
    "BRA": "BRACES",
    "DE": "DENIM",
    "JERSEY": "JERSEY",
    "MC": "SPORT COAT",
    "MCP": "CASUAL PANTS",
    "MCS": "CASUAL SHIRTS",
    "MDP": "DRESS PANTS",
    "MDS": "DRESS SHIRT",
    "MKS": "KNIT SHIRT",
    "MS": "SUIT",
    "OU WE": "OUTERWEAR",
    "PS": "POCKET SQUARE",
    "SHO": "SHOES",
    "SOC": "SOCKS",
    "TIE": "TIES",
    "TUX": "TUXEDOS",
    "UND": "UNDERWEAR",
    "VES": "VEST",
}
NON_SELLABLE_GROUPS = {
    "ALT",
    "ALTCFT",
    "CMS",
    "COUPON",
    "CS",
    "GIFTCT",
    "HOLGC",
    "MSC",
    "RENTAL",
}
NON_SELLABLE_DESC_PATTERNS = (
    re.compile(r"\bACCIDENT DAMAGE\b", re.IGNORECASE),
    re.compile(r"\bALTERATIONS?\b", re.IGNORECASE),
    re.compile(r"\bGIFT CARD\b", re.IGNORECASE),
    re.compile(r"\bREWARDS?\b", re.IGNORECASE),
    re.compile(r"\bRENTAL\b", re.IGNORECASE),
    re.compile(r"\bSHIPPING\b", re.IGNORECASE),
    re.compile(r"\bSPECIAL ORDER\b", re.IGNORECASE),
)


@dataclass(frozen=True)
class DBFField:
    name: str
    field_type: str
    length: int
    decimals: int


@dataclass(frozen=True)
class PreparedProduct:
    payload: Dict[str, Any]
    audit: Dict[str, Any]


@dataclass(frozen=True)
class MatrixDefinition:
    row_headers: List[str]
    column_headers: List[str]
    cells: List[Dict[str, Any]]


@dataclass(frozen=True)
class BuildStats:
    total_rows_seen: int
    skipped_duplicate_sku: int
    skipped_zero_price: int
    skipped_zero_quantity: int
    skipped_non_sellable: int
    skipped_missing_payload: int


@dataclass(frozen=True)
class CustomerBuildStats:
    total_rows_seen: int
    skipped_deleted: int
    skipped_missing_identity: int
    skipped_duplicate: int
    source_rows_seen: Dict[str, int] = field(default_factory=dict)
    source_payloads: Dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class DBFHeaderSummary:
    path: Path
    relative_path: str
    table_name: str
    record_count: int
    header_length: int
    record_length: int
    fields: List[DBFField]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read DBF exports from a host computer and push them to the Shopify sync service."
    )
    parser.add_argument(
        "--dbf-dir",
        default=DEFAULT_DBF_DIR,
        help="Directory containing Item.dbf and the related POS export files.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Load credentials from this .env file. Default: first existing of jbarbaro_db/.env, .env.",
    )
    parser.add_argument(
        "--full-sync",
        action="store_true",
        help=(
            "Production catalog run. Enables recursive DBF discovery, rich product data, zero-stock "
            "products, a local report folder, and a live-upload confirmation requirement."
        ),
    )
    parser.add_argument(
        "--archive-missing",
        action="store_true",
        help=(
            "Compare every SKU present in Item.dbf with Shopify. Read-only runs preview products missing "
            "from the DBF; live runs archive them after all product batches succeed."
        ),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Confirm that a live full sync should send product updates to Shopify.",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Actually send payloads to the sync service. Omit this to only read/analyze ashpsdat.",
    )
    parser.add_argument(
        "--customers",
        action="store_true",
        help="Read active Customer.dbf and build Shopify customer payloads instead of product payloads.",
    )
    parser.add_argument(
        "--customer-limit",
        type=int,
        default=None,
        help="Only process the first N customer payloads after filtering.",
    )
    parser.add_argument(
        "--customer-scope",
        choices=("active", "expanded"),
        default="active",
        help=(
            "Customer source set. active reads Customer.dbf only. expanded also reads mcust.DBF "
            "and mcust002.dbf, but still skips deleted/copy archive tables."
        ),
    )
    parser.add_argument(
        "--output-customers-json",
        default=None,
        help="Write generated customer payloads to this JSON file.",
    )
    parser.add_argument(
        "--output-customers-csv",
        default=None,
        help="Write a customer audit CSV to this file.",
    )
    parser.add_argument(
        "--existing-customer-csv",
        default=None,
        help=(
            "Shopify customer CSV that already exists/imported. Customer mode writes a missing-customers.csv "
            "report for POS customers not found in this file."
        ),
    )
    parser.add_argument(
        "--output-missing-customers-csv",
        default=None,
        help="Write POS customers missing from --existing-customer-csv to this audit CSV.",
    )
    parser.add_argument(
        "--shopify-customer-csv-dir",
        default=None,
        help="Write Shopify Admin customer import CSV files to this directory. Defaults inside the customer report folder.",
    )
    parser.add_argument(
        "--no-shopify-customer-csv",
        action="store_true",
        help="Do not write Shopify Admin customer import CSV files with customer reports.",
    )
    parser.add_argument(
        "--shopify-customer-csv-max-mb",
        type=float,
        default=14.0,
        help="Maximum size per Shopify customer CSV part in MB. Default: 14.0, below Shopify's 15 MB limit.",
    )
    parser.add_argument(
        "--customer-tag",
        action="append",
        default=[],
        help="Customer tag to add. Repeat to add more tags. Defaults to POS Customer.",
    )
    parser.add_argument(
        "--stocked-only",
        action="store_true",
        help="With full product sync, only include products with positive quantity.",
    )
    parser.add_argument(
        "--matrix-variants",
        action="store_true",
        help=(
            "Build Shopify size/color variants for matrix items using Itemmrc.dbf and Itemmqty.dbf. "
            "Each variant receives the exact legacy POS barcode, such as '21741. 1 1'."
        ),
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search nested folders for the expected DBF filenames.",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="App base URL. Can also come from URL or SHOPIFY_SYNC_BASE_URL in .env.",
    )
    parser.add_argument(
        "--endpoint",
        default=None,
        help=(
            "Override the upload URL. You can pass a full batch URL, or even the products URL with "
            "consumer_key/consumer_secret and the script will switch it to /batch automatically."
        ),
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="POS API key. Can also come from Key, POS_API_KEY, or SHOPIFY_SYNC_API_KEY in .env.",
    )
    parser.add_argument(
        "--api-secret",
        default=None,
        help="POS API secret. Can also come from Secret, POS_API_SECRET, or SHOPIFY_SYNC_API_SECRET in .env.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Products per request. Default: {DEFAULT_BATCH_SIZE}",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Products processed in parallel inside each batch. Start with 2; the app caps this at 4. Default: 1",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"HTTP timeout in seconds. Default: {DEFAULT_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--start-offset",
        type=int,
        default=0,
        help="Skip the first N prepared products before uploading. Useful for manual resumes.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Stop after N upload batches. Useful for testing or staged rollouts.",
    )
    parser.add_argument(
        "--resume-file",
        default=None,
        help="JSON checkpoint file. With --full-sync, defaults inside the report folder.",
    )
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop the run when a batch returns product-level failures instead of continuing.",
    )
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Write an archive manifest, payload preview, run summary, and optional CSV exports here.",
    )
    parser.add_argument(
        "--export-core-csv",
        action="store_true",
        help="Export the most important DBF tables to CSV in the report folder. These files may contain PII.",
    )
    parser.add_argument(
        "--export-row-limit",
        type=int,
        default=100000,
        help="Maximum rows per exported core CSV. Default: 100000.",
    )
    parser.add_argument(
        "--status",
        choices=("draft", "active", "archived"),
        default=None,
        help=(
            "Force one Shopify product status for every product. Omit during full sync to automatically "
            "set in-stock products active and zero-quantity products archived."
        ),
    )
    parser.add_argument(
        "--in-stock-status",
        choices=("draft", "active", "archived"),
        default="active",
        help="Product status to send for full-sync rows with quantity greater than zero. Default: active.",
    )
    parser.add_argument(
        "--zero-quantity-status",
        choices=("draft", "active", "archived"),
        default="archived",
        help="Product status to send for full-sync rows with zero quantity. Default: archived.",
    )
    parser.add_argument(
        "--quantity-source",
        choices=("best", "item", "itemmqty"),
        default="best",
        help="Use the best available quantity from Item.dbf and Itemmqty.dbf. Default: best",
    )
    parser.add_argument(
        "--itemmqty-cell",
        default=None,
        help='When using --quantity-source itemmqty, optionally limit to one CELL value such as "1 1".',
    )
    parser.add_argument(
        "--name-mode",
        choices=("raw", "smart"),
        default="smart",
        help="Use raw DESC as the title or build a smarter name from multiple DBF fields.",
    )
    parser.add_argument(
        "--include-desc2-description",
        action="store_true",
        help="Map DESC2 into the Shopify description field. Off by default.",
    )
    parser.add_argument(
        "--include-html-description",
        action="store_true",
        help="Build a richer HTML description from the DBF metadata.",
    )
    parser.add_argument(
        "--include-tags",
        action="store_true",
        help="Send DBF-derived tags. Use carefully because this can replace existing Shopify tags on updates.",
    )
    parser.add_argument(
        "--include-metafields",
        action="store_true",
        help="Send compact POS metadata as Shopify product metafields.",
    )
    parser.add_argument(
        "--metafield-namespace",
        default=DEFAULT_METAFIELD_NAMESPACE,
        help=f"Shopify metafield namespace for DBF metadata. Default: {DEFAULT_METAFIELD_NAMESPACE}",
    )
    parser.add_argument(
        "--rich",
        action="store_true",
        help="Enable HTML descriptions, tags, and POS metafields for a fuller Shopify import.",
    )
    parser.add_argument(
        "--output-title-audit",
        default=None,
        help="Write a CSV showing raw names versus generated upload names.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N products after filtering.",
    )
    parser.add_argument(
        "--sku",
        action="append",
        default=[],
        help="Only sync a specific SKU. Repeat this flag to include more than one.",
    )
    parser.add_argument(
        "--skip-zero-price",
        action="store_true",
        help="Skip rows where PRICE is blank or zero.",
    )
    parser.add_argument(
        "--skip-zero-quantity",
        dest="skip_zero_quantity",
        action="store_true",
        default=True,
        help="Skip rows where the chosen quantity value is zero. Default: on",
    )
    parser.add_argument(
        "--include-zero-quantity",
        dest="skip_zero_quantity",
        action="store_false",
        help="Include rows where the chosen quantity value is zero.",
    )
    parser.add_argument(
        "--skip-non-sellable",
        dest="skip_non_sellable",
        action="store_true",
        default=True,
        help="Skip non-sellable POS rows like alterations, rentals, gift cards, shipping, and custom orders. Default: on",
    )
    parser.add_argument(
        "--include-non-sellable",
        dest="skip_non_sellable",
        action="store_false",
        help="Include non-sellable or internal POS rows in the payload.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build payloads without uploading them.",
    )
    parser.add_argument(
        "--output-json",
        default=None,
        help="Write the generated payload list to a JSON file.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON when using --output-json.",
    )
    parser.add_argument(
        "--verbose-preview",
        action="store_true",
        help="Print full JSON previews to the terminal. Default terminal output stays compact.",
    )
    args = parser.parse_args()
    explicit_skip_zero_quantity = "--skip-zero-quantity" in sys.argv[1:]
    load_env_files(args.env_file)
    if not args.upload:
        args.dry_run = True
    if not args.full_sync and args.limit is None and not args.sku and not args.customers:
        args.full_sync = True
    if args.archive_missing and args.customers:
        parser.error("--archive-missing is only available for product syncs.")
    if args.archive_missing and (args.limit is not None or args.sku):
        parser.error("--archive-missing cannot be combined with --limit or --sku.")
    if args.rich:
        args.include_html_description = True
        args.include_tags = True
        args.include_metafields = True
    if args.full_sync:
        args.recursive = True
        args.include_html_description = True
        args.include_tags = True
        args.include_metafields = True
        args.skip_zero_quantity = False
        if args.stocked_only or explicit_skip_zero_quantity:
            args.skip_zero_quantity = True
        if args.report_dir is None:
            args.report_dir = str(Path("jbarbaro_db") / "reports" / f"full-sync-{timestamp_for_filename()}")
        report_dir = Path(args.report_dir)
        if args.output_title_audit is None:
            args.output_title_audit = str(report_dir / "title-audit.csv")
        if args.resume_file is None:
            args.resume_file = str(report_dir / "resume.json")
    if args.customers and args.report_dir is None:
        args.report_dir = str(Path("jbarbaro_db") / "reports" / f"customers-{timestamp_for_filename()}")
    if args.customers and args.upload and args.resume_file is None:
        args.resume_file = str(Path(args.report_dir) / "resume.json")
    if args.customers and args.customer_limit is None:
        args.customer_limit = args.limit
    return args


def iter_dbf_rows(path: Path, *, encoding: str = "latin1") -> Iterator[Dict[str, Any]]:
    with path.open("rb") as handle:
        header = handle.read(32)
        if len(header) != 32:
            raise ValueError(f"{path} is not a valid DBF file.")

        number_of_records = struct.unpack("<I", header[4:8])[0]
        header_length = struct.unpack("<H", header[8:10])[0]
        record_length = struct.unpack("<H", header[10:12])[0]

        fields: List[DBFField] = []
        while True:
            descriptor = handle.read(32)
            if not descriptor:
                raise ValueError(f"{path} ended before the field list was complete.")
            if descriptor[0] == 0x0D:
                break

            name = descriptor[:11].split(b"\x00", 1)[0].decode("ascii", "ignore")
            fields.append(
                DBFField(
                    name=name,
                    field_type=chr(descriptor[11]),
                    length=descriptor[16],
                    decimals=descriptor[17],
                )
            )

        handle.seek(header_length)
        for _ in range(number_of_records):
            record = handle.read(record_length)
            if not record:
                break
            if record[0] == 0x2A:
                continue

            row: Dict[str, Any] = {}
            offset = 1
            for field in fields:
                raw_value = record[offset : offset + field.length]
                offset += field.length
                row[field.name] = _parse_dbf_value(raw_value, field, encoding=encoding)
            yield row


def _parse_dbf_value(raw_value: bytes, field: DBFField, *, encoding: str) -> Any:
    text = raw_value.decode(encoding, "ignore").strip()
    if field.field_type == "N":
        if not text:
            return None
        try:
            return Decimal(text)
        except InvalidOperation:
            return text
    if field.field_type == "L":
        if not text:
            return None
        normalized = text.upper()
        if normalized in {"T", "Y"}:
            return True
        if normalized in {"F", "N"}:
            return False
        return text
    if field.field_type == "D":
        if len(text) == 8 and text.isdigit():
            return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
        return text or None
    if field.field_type == "T":
        return text or None
    return text or None


def find_dbf_file(dbf_dir: Path, filename: str, *, recursive: bool) -> Optional[Path]:
    direct_path = dbf_dir / filename
    if direct_path.exists():
        return direct_path

    expected_name = filename.lower()
    candidates = dbf_dir.rglob("*") if recursive else dbf_dir.glob("*")
    matches = sorted(
        path
        for path in candidates
        if path.is_file() and path.suffix.lower() == ".dbf" and path.name.lower() == expected_name
    )
    if not matches:
        return None
    if len(matches) > 1:
        match_list = ", ".join(str(path) for path in matches[:10])
        raise ValueError(f"Found multiple {filename} files: {match_list}")
    return matches[0]


def discover_dbf_files(dbf_dir: Path, *, recursive: bool) -> List[Path]:
    pattern = "**/*" if recursive else "*"
    return sorted(
        (path for path in dbf_dir.glob(pattern) if path.is_file() and path.suffix.lower() == ".dbf"),
        key=lambda path: str(path).lower(),
    )


def read_dbf_header_summary(path: Path, *, root: Path) -> DBFHeaderSummary:
    with path.open("rb") as handle:
        header = handle.read(32)
        if len(header) != 32:
            raise ValueError(f"{path} is not a valid DBF file.")

        record_count = struct.unpack("<I", header[4:8])[0]
        header_length = struct.unpack("<H", header[8:10])[0]
        record_length = struct.unpack("<H", header[10:12])[0]
        fields: List[DBFField] = []
        while True:
            descriptor = handle.read(32)
            if not descriptor:
                raise ValueError(f"{path} ended before the field list was complete.")
            if descriptor[0] == 0x0D:
                break
            name = descriptor[:11].split(b"\x00", 1)[0].decode("ascii", "ignore")
            fields.append(
                DBFField(
                    name=name,
                    field_type=chr(descriptor[11]),
                    length=descriptor[16],
                    decimals=descriptor[17],
                )
            )

    try:
        relative_path = str(path.relative_to(root))
    except ValueError:
        relative_path = str(path)
    return DBFHeaderSummary(
        path=path,
        relative_path=relative_path,
        table_name=path.stem,
        record_count=record_count,
        header_length=header_length,
        record_length=record_length,
        fields=fields,
    )


def build_archive_manifest(dbf_dir: Path, *, recursive: bool) -> Dict[str, Any]:
    table_rows: List[Dict[str, Any]] = []
    total_records = 0
    sensitive_table_count = 0

    for path in discover_dbf_files(dbf_dir, recursive=recursive):
        try:
            table = read_dbf_header_summary(path, root=dbf_dir)
        except ValueError as exc:
            table_rows.append(
                {
                    "table": path.stem,
                    "path": str(path),
                    "category": "unreadable",
                    "records": 0,
                    "field_count": 0,
                    "fields": [],
                    "sensitive_fields": [],
                    "error": str(exc),
                }
            )
            continue

        sensitive_fields = detect_sensitive_fields(table.fields)
        category = classify_table(table.table_name, table.fields)
        total_records += table.record_count
        if sensitive_fields:
            sensitive_table_count += 1
        table_rows.append(
            {
                "table": table.table_name,
                "path": table.relative_path,
                "category": category,
                "records": table.record_count,
                "record_length": table.record_length,
                "field_count": len(table.fields),
                "fields": [field.name for field in table.fields],
                "sensitive_fields": sensitive_fields,
            }
        )

    table_rows.sort(key=lambda row: (row.get("records") or 0, row.get("field_count") or 0), reverse=True)
    category_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"tables": 0, "records": 0})
    for row in table_rows:
        category = str(row.get("category") or "unknown")
        category_counts[category]["tables"] += 1
        category_counts[category]["records"] += int(row.get("records") or 0)

    return {
        "dbf_dir": str(dbf_dir),
        "table_count": len(table_rows),
        "total_records": total_records,
        "sensitive_table_count": sensitive_table_count,
        "categories": [
            {"category": category, **counts}
            for category, counts in sorted(category_counts.items(), key=lambda item: item[0])
        ],
        "tables": table_rows,
        "notes": [
            "Products are uploaded from Item.dbf with quantities, prices, costs, vendor lookup data, tags, descriptions, and POS metafields.",
            "Customer, contact, payment, employee, order, and invoice tables are inventoried in this report but are not sent to Shopify products.",
            "Customer migration to Shopify requires write_customers/read_customers scopes, consent handling, and a separate reviewed importer.",
        ],
    }


def classify_table(table_name: str, fields: List[DBFField]) -> str:
    name = table_name.lower()
    field_names = {field.name.upper() for field in fields}
    if name in {"item", "mproduct", "itemmqty", "itemmrc", "prod_aux", "price", "pricechg"}:
        return "products_inventory"
    if "item" in name or "prod" in name or {"SKU", "QTY", "PRICE"} <= field_names:
        return "products_inventory"
    if "cust" in name or "contact" in name or {"CUST_NUM", "LAST_NAME"} <= field_names:
        return "customers"
    if "vend" in name or {"VENDOR_ID", "COMPANY"} <= field_names:
        return "vendors"
    if name in {"invhdr", "invdtl", "minvhdr", "minvdtl", "ordhdr", "orddtl"}:
        return "sales_orders"
    if "invoice" in field_names or name.startswith("inv") or name.startswith("ord"):
        return "sales_orders"
    if "employee" in name or "emptime" in name or "closeout" in name:
        return "operations"
    if "bride" in name or "gift" in name or "rent" in name:
        return "special_programs"
    return "system_other"


def detect_sensitive_fields(fields: List[DBFField]) -> List[str]:
    return [
        field.name
        for field in fields
        if any(pattern.search(field.name) for pattern in SENSITIVE_FIELD_PATTERNS)
    ]


def build_vendor_lookup(dbf_dir: Path, *, recursive: bool) -> Dict[str, str]:
    vendor_path = find_dbf_file(dbf_dir, "Vendor.dbf", recursive=recursive)
    if vendor_path is None:
        return {}

    lookup: Dict[str, str] = {}
    for row in iter_dbf_rows(vendor_path):
        vendor_id = clean_text(row.get("VENDOR_ID"))
        company = clean_text(row.get("COMPANY"))
        if vendor_id and company:
            lookup[vendor_id] = company
    return lookup


def build_price_change_lookup(dbf_dir: Path, *, recursive: bool) -> Dict[str, Dict[str, Any]]:
    pricechg_path = find_dbf_file(dbf_dir, "pricechg.dbf", recursive=recursive)
    if pricechg_path is None:
        return {}

    lookup: Dict[str, Dict[str, Any]] = {}
    for row in iter_dbf_rows(pricechg_path):
        sku = clean_text(row.get("SKU"))
        if sku:
            lookup[sku] = row
    return lookup


def build_vendor_item_lookup(dbf_dir: Path, *, recursive: bool) -> Dict[str, Dict[str, Any]]:
    vendors_path = find_dbf_file(dbf_dir, "vendors.dbf", recursive=recursive)
    if vendors_path is None:
        return {}

    lookup: Dict[str, Dict[str, Any]] = {}
    for row in iter_dbf_rows(vendors_path):
        sku = clean_text(row.get("SKU"))
        if not sku or sku in lookup:
            continue
        lookup[sku] = row
    return lookup


def build_quantity_lookup(
    dbf_dir: Path,
    *,
    quantity_source: str,
    itemmqty_cell: Optional[str],
    recursive: bool,
) -> Dict[str, Decimal]:
    if quantity_source not in {"itemmqty", "best"}:
        return {}

    quantity_path = find_dbf_file(dbf_dir, "Itemmqty.dbf", recursive=recursive)
    if quantity_path is None:
        return {}

    cell_filter = clean_text(itemmqty_cell)
    quantities: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in iter_dbf_rows(quantity_path):
        sku = clean_text(row.get("SKU"))
        cell = clean_text(row.get("CELL"))
        quantity = decimal_or_none(row.get("QTY"))
        if not sku or quantity is None:
            continue
        if cell_filter and cell != cell_filter:
            continue
        quantities[sku] += quantity
    return dict(quantities)


def parse_matrix_cell(value: Any, *, row_count: int, column_count: int) -> Optional[tuple[int, int]]:
    raw_cell = clean_text(value)
    if not raw_cell or row_count < 1 or column_count < 1:
        return None

    matches: List[tuple[int, int]] = []
    for row_number in range(1, row_count + 1):
        for column_number in range(1, column_count + 1):
            if raw_cell in {f"{row_number} {column_number}", f"{row_number}{column_number}"}:
                matches.append((row_number, column_number))
    return matches[0] if len(matches) == 1 else None


def format_matrix_barcode(sku: str, row_number: int, column_number: int) -> str:
    return f"{sku}. {row_number} {column_number}"


def build_matrix_lookup(dbf_dir: Path, *, recursive: bool) -> Dict[str, MatrixDefinition]:
    quantity_path = find_dbf_file(dbf_dir, "Itemmqty.dbf", recursive=recursive)
    header_path = find_dbf_file(dbf_dir, "Itemmrc.dbf", recursive=recursive)
    if quantity_path is None or header_path is None:
        return {}

    headers: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: {"R": [], "C": []})
    for row in iter_dbf_rows(header_path):
        sku = clean_text(row.get("SKU"))
        header_type = (clean_text(row.get("RC")) or "").upper()
        if not sku or header_type not in {"R", "C"}:
            continue
        headers[sku][header_type].append(clean_text(row.get("HEADER")) or "")

    quantity_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in iter_dbf_rows(quantity_path):
        sku = clean_text(row.get("SKU"))
        if sku:
            quantity_rows[sku].append(row)

    definitions: Dict[str, MatrixDefinition] = {}
    for sku, rows in quantity_rows.items():
        row_headers = headers.get(sku, {}).get("R") or []
        column_headers = headers.get(sku, {}).get("C") or []
        if not row_headers or not column_headers:
            continue

        cells: List[Dict[str, Any]] = []
        for row in rows:
            coordinates = parse_matrix_cell(
                row.get("CELL"),
                row_count=len(row_headers),
                column_count=len(column_headers),
            )
            if coordinates is None:
                raise ValueError(
                    f"Cannot map Itemmqty cell {row.get('CELL')!r} for matrix SKU {sku} "
                    f"({len(row_headers)} rows x {len(column_headers)} columns)."
                )
            row_number, column_number = coordinates
            quantity = decimal_to_quantity(decimal_or_none(row.get("QTY"))) or 0
            cells.append(
                {
                    "row": row_number,
                    "column": column_number,
                    "cell": f"{row_number} {column_number}",
                    "quantity": quantity,
                    "barcode": clean_text(row.get("BARCODE"))
                    or format_matrix_barcode(sku, row_number, column_number),
                }
            )

        definitions[sku] = MatrixDefinition(
            row_headers=row_headers,
            column_headers=column_headers,
            cells=cells,
        )
    return definitions


def _unique_matrix_header_values(headers: List[str], *, fallback_prefix: str) -> List[str]:
    normalized_counts: Dict[str, int] = defaultdict(int)
    for header in headers:
        normalized_counts[normalize_spaces(header).casefold()] += 1

    values: List[str] = []
    for index, header in enumerate(headers, start=1):
        value = normalize_spaces(header) or f"{fallback_prefix} {index}"
        if normalized_counts[normalize_spaces(header).casefold()] > 1:
            value = f"{value} ({index})"
        values.append(value)
    return values


def build_matrix_variants(
    *,
    sku: str,
    definition: MatrixDefinition,
    price: Optional[Decimal],
    compare_at_price: Optional[Decimal],
    cost: Optional[Decimal],
) -> List[Dict[str, Any]]:
    row_values = _unique_matrix_header_values(definition.row_headers, fallback_prefix="Row")
    column_values = _unique_matrix_header_values(definition.column_headers, fallback_prefix="Column")
    use_rows = len(row_values) > 1
    use_columns = len(column_values) > 1
    single_option: Optional[tuple[str, str]] = None
    if not use_rows and not use_columns:
        if definition.column_headers[0].strip():
            single_option = ("Size", column_values[0])
        elif definition.row_headers[0].strip():
            single_option = ("Color", row_values[0])
        else:
            single_option = ("Title", "Default Title")

    variants: List[Dict[str, Any]] = []
    for cell in definition.cells:
        row_number = int(cell["row"])
        column_number = int(cell["column"])
        option_values: Dict[str, str] = {}
        if use_columns:
            option_values["Size"] = column_values[column_number - 1]
        if use_rows:
            option_values["Color"] = row_values[row_number - 1]
        if single_option:
            option_values[single_option[0]] = single_option[1]

        barcode = clean_text(cell.get("barcode")) or format_matrix_barcode(sku, row_number, column_number)
        variant = {
            "sku": barcode,
            "barcode": barcode,
            "option_values": option_values,
            "quantity": int(cell.get("quantity") or 0),
            "price": decimal_to_price(price),
            "compare_at_price": decimal_to_price(compare_at_price),
            "cost": decimal_to_price(cost),
            "tracked": True,
            "requires_shipping": True,
            "pos_cell": str(cell.get("cell") or f"{row_number} {column_number}"),
        }
        variants.append(prune_empty_values(variant))
    return variants


def validate_matrix_variants(products: List[PreparedProduct]) -> tuple[int, int]:
    seen_barcodes: Dict[str, str] = {}
    matrix_product_count = 0
    matrix_variant_count = 0
    for product in products:
        variants = product.payload.get("variants") or []
        if not variants:
            continue
        matrix_product_count += 1
        matrix_variant_count += len(variants)
        seen_option_combinations: set[tuple[tuple[str, str], ...]] = set()
        for variant in variants:
            barcode = clean_text(variant.get("barcode"))
            variant_sku = clean_text(variant.get("sku"))
            if not barcode or not variant_sku:
                raise ValueError(f"Matrix product {product.payload.get('sku')} has a variant without SKU/barcode.")
            if barcode in seen_barcodes:
                raise ValueError(
                    f"Duplicate matrix barcode {barcode!r} for base SKUs "
                    f"{seen_barcodes[barcode]!r} and {product.payload.get('sku')!r}."
                )
            seen_barcodes[barcode] = str(product.payload.get("sku") or "")
            option_combination = tuple(
                (str(name), str(value))
                for name, value in (variant.get("option_values") or {}).items()
            )
            if not option_combination or option_combination in seen_option_combinations:
                raise ValueError(
                    f"Matrix product {product.payload.get('sku')} has a missing or duplicate option combination: "
                    f"{dict(option_combination)!r}."
                )
            seen_option_combinations.add(option_combination)
    return matrix_product_count, matrix_variant_count


def _item_row_quality(row: Dict[str, Any], index: int) -> tuple[Any, ...]:
    price = decimal_or_none(row.get("PRICE")) or Decimal("0")
    quantity = decimal_or_none(row.get("QTY")) or Decimal("0")
    latest_date = max(
        (clean_text(row.get(field_name)) or "" for field_name in ("LAST_ACT", "EDITDATE", "DATE_ST")),
        default="",
    )
    completeness = sum(
        bool(clean_text(row.get(field_name)))
        for field_name in ("DESC", "GROUP", "VENDOR", "VEND_ID", "ALT_SKU", "STYLE", "SIZE", "COLOR")
    )
    return (price > 0, latest_date, quantity > 0, completeness, index)


def deduplicate_item_rows(rows: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
    selected: Dict[str, tuple[int, Dict[str, Any]]] = {}
    sku_order: List[str] = []
    rows_without_sku: List[Dict[str, Any]] = []
    duplicate_count = 0
    for index, row in enumerate(rows):
        sku = clean_text(row.get("SKU"))
        if not sku:
            rows_without_sku.append(row)
            continue
        if sku not in selected:
            selected[sku] = (index, row)
            sku_order.append(sku)
            continue
        duplicate_count += 1
        current_index, current_row = selected[sku]
        if _item_row_quality(row, index) > _item_row_quality(current_row, current_index):
            selected[sku] = (index, row)
    return [selected[sku][1] for sku in sku_order] + rows_without_sku, duplicate_count


def load_products(args: argparse.Namespace) -> tuple[List[PreparedProduct], BuildStats]:
    dbf_dir = Path(args.dbf_dir).expanduser()
    item_path = find_dbf_file(dbf_dir, "Item.dbf", recursive=args.recursive)
    if item_path is None:
        raise FileNotFoundError(f"Expected Item.dbf in {dbf_dir}")

    vendor_lookup = build_vendor_lookup(dbf_dir, recursive=args.recursive)
    price_change_lookup = build_price_change_lookup(dbf_dir, recursive=args.recursive)
    vendor_item_lookup = build_vendor_item_lookup(dbf_dir, recursive=args.recursive)
    quantity_lookup = build_quantity_lookup(
        dbf_dir,
        quantity_source=args.quantity_source,
        itemmqty_cell=args.itemmqty_cell,
        recursive=args.recursive,
    )
    matrix_lookup = build_matrix_lookup(dbf_dir, recursive=args.recursive) if args.matrix_variants else {}
    sku_filter = {sku.strip() for sku in args.sku if sku and sku.strip()}

    item_rows = list(iter_dbf_rows(item_path))
    deduplicated_rows, skipped_duplicate_sku = deduplicate_item_rows(item_rows)
    prepared: List[PreparedProduct] = []
    total_rows_seen = len(item_rows)
    skipped_zero_price = 0
    skipped_zero_quantity = 0
    skipped_non_sellable = 0
    skipped_missing_payload = 0
    for row in deduplicated_rows:
        row_sku = clean_text(row.get("SKU"))
        if sku_filter and row_sku not in sku_filter:
            continue
        if args.skip_non_sellable and non_sellable_reason(row):
            skipped_non_sellable += 1
            continue
        prepared_product = build_product(
            row,
            args=args,
            vendor_lookup=vendor_lookup,
            price_change_row=price_change_lookup.get(clean_text(row.get("SKU")) or "", {}),
            vendor_item_row=vendor_item_lookup.get(clean_text(row.get("SKU")) or "", {}),
            quantity_lookup=quantity_lookup,
            matrix_definition=matrix_lookup.get(clean_text(row.get("SKU")) or ""),
        )
        if prepared_product is None:
            skipped_missing_payload += 1
            continue
        payload = prepared_product.payload
        if args.skip_zero_price and (payload.get("price") is None or payload["price"] <= 0):
            skipped_zero_price += 1
            continue
        if args.skip_zero_quantity and (payload.get("quantity") is None or payload["quantity"] <= 0):
            skipped_zero_quantity += 1
            continue
        prepared.append(prepared_product)
        if args.limit is not None and len(prepared) >= args.limit:
            break
    validate_matrix_variants(prepared)
    return prepared, BuildStats(
        total_rows_seen=total_rows_seen,
        skipped_duplicate_sku=skipped_duplicate_sku,
        skipped_zero_price=skipped_zero_price,
        skipped_zero_quantity=skipped_zero_quantity,
        skipped_non_sellable=skipped_non_sellable,
        skipped_missing_payload=skipped_missing_payload,
    )


def load_item_source_skus(dbf_dir: Path, *, recursive: bool) -> List[str]:
    """Return every non-empty SKU present in Item.dbf, independent of upload filters."""
    item_path = find_dbf_file(dbf_dir, "Item.dbf", recursive=recursive)
    if item_path is None:
        raise FileNotFoundError(f"Expected Item.dbf in {dbf_dir}")
    return sorted(
        {
            sku
            for row in iter_dbf_rows(item_path)
            if (sku := clean_text(row.get("SKU")))
        },
        key=str.casefold,
    )


def customer_source_paths(dbf_dir: Path, *, scope: str) -> List[Path]:
    customer_path = find_dbf_file(dbf_dir, "Customer.dbf", recursive=True)
    if customer_path is None:
        raise FileNotFoundError(f"Expected Customer.dbf in {dbf_dir}")

    paths = [customer_path]
    if scope == "expanded":
        for filename in EXPANDED_CUSTOMER_FILES:
            source_path = find_dbf_file(dbf_dir, filename, recursive=True)
            if source_path is not None and source_path not in paths:
                paths.append(source_path)
    return paths


def load_customers(args: argparse.Namespace) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], CustomerBuildStats]:
    dbf_dir = Path(args.dbf_dir).expanduser()
    paths = customer_source_paths(dbf_dir, scope=args.customer_scope)

    shipping_lookup = build_customer_shipping_lookup(dbf_dir, recursive=True)
    tags = args.customer_tag or DEFAULT_CUSTOMER_TAGS
    payloads: List[Dict[str, Any]] = []
    audits: List[Dict[str, Any]] = []
    seen_keys: set[str] = set()
    total_rows_seen = 0
    skipped_deleted = 0
    skipped_missing_identity = 0
    skipped_duplicate = 0
    source_rows_seen: Dict[str, int] = defaultdict(int)
    source_payloads: Dict[str, int] = defaultdict(int)

    for source_path in paths:
        source_name = source_path.name
        for row in iter_dbf_rows(source_path):
            total_rows_seen += 1
            source_rows_seen[source_name] += 1
            if is_deleted_customer(row):
                skipped_deleted += 1
                continue

            payload = build_customer_payload(
                row,
                shipping_lookup.get(clean_text(row.get("CUST_NUM")) or "", []),
                tags=tags,
                source=source_name,
            )
            if payload is None:
                skipped_missing_identity += 1
                continue

            identity_key = customer_identity_key(payload)
            if identity_key in seen_keys:
                skipped_duplicate += 1
                continue
            seen_keys.add(identity_key)

            payloads.append(payload)
            audits.append(build_customer_audit(row, payload))
            source_payloads[source_name] += 1
            if args.customer_limit is not None and len(payloads) >= args.customer_limit:
                break
        if args.customer_limit is not None and len(payloads) >= args.customer_limit:
            break

    return payloads, audits, CustomerBuildStats(
        total_rows_seen=total_rows_seen,
        skipped_deleted=skipped_deleted,
        skipped_missing_identity=skipped_missing_identity,
        skipped_duplicate=skipped_duplicate,
        source_rows_seen=dict(source_rows_seen),
        source_payloads=dict(source_payloads),
    )


def build_customer_shipping_lookup(dbf_dir: Path, *, recursive: bool) -> Dict[str, List[Dict[str, Any]]]:
    shipping_path = find_dbf_file(dbf_dir, "CustShip.dbf", recursive=recursive)
    if shipping_path is None:
        return {}
    lookup: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in iter_dbf_rows(shipping_path):
        cust_num = clean_text(row.get("CUST_NUM"))
        if not cust_num:
            continue
        address = build_customer_address(
            name=clean_text(row.get("SHIPNAME")),
            company=None,
            address1=clean_text(row.get("SHIPADD")),
            address2=clean_text(row.get("SHIPADD2")),
            city=clean_text(row.get("SHIPCITY")),
            province=clean_text(row.get("SHIPSTATE")),
            zip_code=clean_text(row.get("SHIPZIP")),
            phone=None,
        )
        if address:
            lookup[cust_num].append(address)
    return dict(lookup)


def build_customer_payload(
    row: Dict[str, Any],
    shipping_addresses: List[Dict[str, Any]],
    *,
    tags: List[str],
    source: str = "Customer.dbf",
) -> Optional[Dict[str, Any]]:
    cust_num = clean_text(row.get("CUST_NUM"))
    first_name = smart_title_case(clean_text(row.get("FIRST_NAME")) or "")
    last_name = smart_title_case(clean_text(row.get("LAST_NAME")) or "")
    company = smart_title_case(clean_text(row.get("COMPANY")) or "")
    email = normalize_email(row.get("INTERNET"))
    phone = normalize_us_phone(first_nonempty(clean_text(row.get("PHONE")), clean_text(row.get("PHONE_H")), clean_text(row.get("PHONE_W"))))

    if not any([email, phone, first_name, last_name, company]):
        return None

    primary_address = build_customer_address(
        name=first_nonempty(" ".join(part for part in (first_name, last_name) if part).strip(), clean_text(row.get("SHIPNAME"))),
        company=company,
        address1=clean_text(row.get("ADDRESS")),
        address2=clean_text(row.get("ADDRESS2")),
        city=clean_text(row.get("CITY_STATE")),
        province=clean_text(row.get("STATE")),
        zip_code=clean_text(row.get("ZIP")),
        phone=phone,
    )
    ship_address = build_customer_address(
        name=clean_text(row.get("SHIPNAME")),
        company=company,
        address1=clean_text(row.get("SHIPADD")),
        address2=clean_text(row.get("SHIPADD2")),
        city=clean_text(row.get("SHIPCITY")),
        province=clean_text(row.get("SHIPSTATE")),
        zip_code=clean_text(row.get("SHIPZIP")),
        phone=phone,
    )
    addresses = dedupe_customer_addresses([primary_address, ship_address, *shipping_addresses])
    customer_tags = dedupe_preserving_order([
        *tags,
        _customer_tag("POS Class", row.get("CLASS")),
        _customer_tag("POS Location", row.get("LOC")),
        _customer_tag("POS Price Level", row.get("PRICE_LEV")),
    ])

    payload: Dict[str, Any] = {
        "source": source,
        "pos_customer_number": cust_num,
        "firstName": first_name or None,
        "lastName": last_name or None,
        "email": email,
        "phone": phone,
        "company": company or None,
        "tags": [tag for tag in customer_tags if tag],
        "taxExempt": bool(clean_text(row.get("TAX_EXEMPT")) or str(clean_text(row.get("TAXEXEMPT")) or "").upper() in {"Y", "YES", "TRUE"}),
        "addresses": addresses,
        "note": build_customer_note(row),
        "metafields": build_customer_metafields(row, source=source),
    }
    return prune_empty_values(payload)


def build_customer_address(
    *,
    name: Optional[str],
    company: Optional[str],
    address1: Optional[str],
    address2: Optional[str],
    city: Optional[str],
    province: Optional[str],
    zip_code: Optional[str],
    phone: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not any([address1, address2, city, province, zip_code]):
        return None
    first_name, last_name = split_customer_name(name)
    return prune_empty_values(
        {
            "firstName": first_name,
            "lastName": last_name,
            "company": company,
            "address1": address1,
            "address2": address2,
            "city": smart_title_case(city or "") if city else None,
            "provinceCode": province.upper() if province else None,
            "zip": zip_code,
            "countryCode": "US",
            "phone": phone,
        }
    )


def build_customer_note(row: Dict[str, Any]) -> Optional[str]:
    pieces = []
    for label, value in (
        ("POS customer", row.get("CUST_NUM")),
        ("Company", row.get("COMPANY")),
        ("Last activity", row.get("LAST_ACT")),
        ("Total purchased", format_decimal(decimal_or_none(row.get("TOTAL_PUR")))),
        ("Class", row.get("CLASS")),
        ("Location", row.get("LOC")),
    ):
        text = clean_text(value)
        if text:
            pieces.append(f"{label}: {text}")
    return "\n".join(pieces) if pieces else None


def build_customer_metafields(row: Dict[str, Any], *, source: str = "Customer.dbf") -> List[Dict[str, Any]]:
    fields = {
        "source_file": source,
        "customer_number": row.get("CUST_NUM"),
        "legacy_customer_number": row.get("MSCUST_NUM"),
        "web_customer_number": nonzero_customer_value(row.get("WEBCUSTNUM")),
        "last_activity": row.get("LAST_ACT"),
        "start_date": row.get("START_DATE"),
        "first_contact": row.get("FIRST_CONT"),
        "edit_date": row.get("EDITDATE"),
        "customer_class": row.get("CLASS"),
        "price_level": row.get("PRICE_LEV"),
        "location": row.get("LOC"),
        "total_purchased": nonzero_decimal_string(row.get("TOTAL_PUR")),
        "ytd": nonzero_decimal_string(row.get("YTD")),
        "qtd": nonzero_decimal_string(row.get("QTD")),
        "lastq": nonzero_decimal_string(row.get("LASTQ")),
    }
    metafields = []
    for key, value in fields.items():
        prepared = build_metafield(namespace="pos", key=key, value=value, metafield_type="single_line_text_field")
        if prepared:
            metafields.append(prepared)
    return metafields


def build_customer_audit(row: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source": payload.get("source") or "",
        "cust_num": clean_text(row.get("CUST_NUM")) or "",
        "first_name": payload.get("firstName") or "",
        "last_name": payload.get("lastName") or "",
        "company": payload.get("company") or "",
        "email": payload.get("email") or "",
        "phone": payload.get("phone") or "",
        "address_count": len(payload.get("addresses") or []),
        "tag_count": len(payload.get("tags") or []),
        "metafield_count": len(payload.get("metafields") or []),
        "last_activity": clean_text(row.get("LAST_ACT")) or "",
        "total_purchased": format_decimal(decimal_or_none(row.get("TOTAL_PUR"))) or "",
    }


def is_deleted_customer(row: Dict[str, Any]) -> bool:
    return str(clean_text(row.get("DELETED")) or "").upper() in {"Y", "YES", "TRUE", "1"}


def customer_identity_key(payload: Dict[str, Any]) -> str:
    for key in ("email", "phone", "pos_customer_number"):
        value = clean_text(payload.get(key))
        if value:
            return f"{key}:{value.lower()}"
    return json.dumps(payload, sort_keys=True, default=str)


@dataclass(frozen=True)
class ExistingCustomerCsvIndex:
    row_count: int
    pos_customer_numbers: set[str]
    emails: set[str]
    phones: set[str]


def build_existing_customer_csv_index(path: Path) -> ExistingCustomerCsvIndex:
    pos_customer_numbers: set[str] = set()
    emails: set[str] = set()
    phones: set[str] = set()
    row_count = 0

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_count += 1
            for pos_id in extract_pos_customer_ids(row.get("Tags")):
                pos_customer_numbers.add(pos_id)

            email = normalize_email(row.get("Email"))
            if email:
                emails.add(email)

            for phone_key in ("Phone", "Default Address Phone"):
                phone = normalize_us_phone(clean_text(row.get(phone_key)))
                if phone:
                    phones.add(phone)

    return ExistingCustomerCsvIndex(
        row_count=row_count,
        pos_customer_numbers=pos_customer_numbers,
        emails=emails,
        phones=phones,
    )


def extract_pos_customer_ids(tags: Any) -> List[str]:
    tag_text = clean_text(tags)
    if not tag_text:
        return []

    pos_ids: List[str] = []
    for tag in tag_text.split(","):
        label, separator, value = tag.strip().partition(":")
        if separator and label.strip().lower() == "pos id":
            pos_id = clean_text(value)
            if pos_id:
                pos_ids.append(pos_id.lower())
    return pos_ids


def customer_exists_in_csv(payload: Dict[str, Any], index: ExistingCustomerCsvIndex) -> bool:
    pos_customer_number = clean_text(payload.get("pos_customer_number"))
    if pos_customer_number and pos_customer_number.lower() in index.pos_customer_numbers:
        return True

    email = normalize_email(payload.get("email"))
    if email and email in index.emails:
        return True

    phone = normalize_us_phone(clean_text(payload.get("phone")))
    if phone and phone in index.phones:
        return True

    return False


def normalize_email(value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    email = text.strip().lower()
    if email.startswith("www.") or email.startswith("http://") or email.startswith("https://"):
        return None
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        return None
    return email


def normalize_us_phone(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    digits = re.sub(r"\D+", "", value)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10 or len(set(digits)) == 1:
        return None
    return f"+1{digits}"


def nonzero_customer_value(value: Any) -> Optional[str]:
    decimal_value = decimal_or_none(value)
    if decimal_value is not None:
        if decimal_value == 0:
            return None
        return format_decimal(decimal_value)
    return clean_text(value)


def split_customer_name(value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = smart_title_case(value or "") if value else ""
    if not text:
        return None, None
    parts = text.split()
    if len(parts) == 1:
        return None, parts[0]
    return parts[0], " ".join(parts[1:])


def dedupe_customer_addresses(addresses: List[Optional[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for address in addresses:
        if not address:
            continue
        key = "|".join(str(address.get(field) or "").lower() for field in ("address1", "address2", "city", "provinceCode", "zip"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(address)
    return deduped[:10]


def _customer_tag(label: str, value: Any) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    return f"{label}:{text}"


def build_product(
    row: Dict[str, Any],
    *,
    args: argparse.Namespace,
    vendor_lookup: Dict[str, str],
    price_change_row: Dict[str, Any],
    vendor_item_row: Dict[str, Any],
    quantity_lookup: Dict[str, Decimal],
    matrix_definition: Optional[MatrixDefinition] = None,
) -> Optional[PreparedProduct]:
    sku = clean_text(row.get("SKU"))
    raw_desc = clean_text(row.get("DESC"))
    if not sku or not raw_desc:
        return None

    vendor_code = first_nonempty(
        clean_text(row.get("VENDOR")),
        clean_text(price_change_row.get("VENDOR")),
        clean_text(vendor_item_row.get("VENDOR")),
    )
    vendor_name = resolve_vendor_name(
        vendor_code=vendor_code,
        vendor_lookup=vendor_lookup,
        item_row=row,
        price_change_row=price_change_row,
    )
    department = clean_text(row.get("GROUP"))
    product_type = first_nonempty(group_product_type(department), clean_text(row.get("PFIELD1")), department)
    display_vendor = simplify_vendor_name(vendor_name, product_type)
    display_product_type = smart_title_case(product_type) if product_type else None
    size = first_nonempty(clean_text(row.get("SIZE")), clean_text(price_change_row.get("SIZE")))
    color = first_nonempty(clean_text(row.get("COLOR")), clean_text(price_change_row.get("COLOR")))
    style = first_nonempty(clean_text(row.get("STYLE")), clean_text(price_change_row.get("STYLE")))
    vendor_item = first_nonempty(
        clean_text(row.get("VEND_ID")),
        clean_text(price_change_row.get("VEND_ID")),
        clean_text(vendor_item_row.get("VEND_SKU")),
    )
    desc2 = clean_text(row.get("DESC2"))
    barcode = clean_text(row.get("ALT_SKU"))
    price = first_nonempty_decimal(decimal_or_none(row.get("PRICE")), decimal_or_none(price_change_row.get("PRICE")))
    compare_at_price = choose_compare_at_price(row=row, price_change_row=price_change_row, price=price)
    cost = first_nonempty_decimal(
        decimal_or_none(row.get("COST")),
        decimal_or_none(row.get("LAST_COST")),
        decimal_or_none(row.get("VEND_COST")),
        decimal_or_none(row.get("BASE_COST")),
        decimal_or_none(vendor_item_row.get("BASE_COST")),
    )
    quantity = select_quantity(row, quantity_lookup=quantity_lookup, quantity_source=args.quantity_source)
    matrix_variants: List[Dict[str, Any]] = []
    if args.matrix_variants and (clean_text(row.get("TYPE")) or "").upper() == "M" and matrix_definition:
        matrix_variants = build_matrix_variants(
            sku=sku,
            definition=matrix_definition,
            price=price,
            compare_at_price=compare_at_price,
            cost=cost,
        )
        quantity = sum(int(variant.get("quantity") or 0) for variant in matrix_variants)
    images = build_image_inputs(row)

    title = raw_desc if args.name_mode == "raw" else build_smart_title(
        raw_desc=raw_desc,
        vendor_name=display_vendor,
        vendor_code=vendor_code,
        product_type=display_product_type,
        size=size,
        color=color,
        desc2=desc2,
    )

    description_html = None
    if args.include_html_description:
        description_html = build_description_html(
            sku=sku,
            title=title,
            raw_desc=raw_desc,
            vendor_name=display_vendor,
            vendor_code=vendor_code,
            vendor_item=vendor_item,
            product_type=display_product_type,
            department=department,
            size=size,
            color=color,
            style=style,
            barcode=barcode,
            price=price,
            compare_at_price=compare_at_price,
            quantity=quantity,
            desc2=desc2,
            row=row,
            vendor_item_row=vendor_item_row,
        )

    payload: Dict[str, Any] = {
        "sku": sku,
        "title": title,
        "vendor": display_vendor,
        "brand": display_vendor,
        "product_type": display_product_type,
        "barcode": barcode,
        "price": decimal_to_price(price),
        "compare_at_price": decimal_to_price(compare_at_price),
        "cost": decimal_to_price(cost),
        "tracked": True,
        "requires_shipping": True,
        "images": images,
    }
    if matrix_variants:
        payload["variants"] = matrix_variants
    if quantity is not None:
        payload["quantity"] = quantity
        payload["qty"] = quantity
        payload["stock_quantity"] = quantity
    if args.status:
        payload["status"] = args.status
    elif args.full_sync and quantity is not None:
        payload["status"] = args.in_stock_status if quantity > 0 else args.zero_quantity_status
    if args.include_tags:
        payload["tags"] = build_tags(
            row=row,
            product_type=product_type,
            department=department,
            size=size,
            color=color,
            style=style,
            vendor_code=vendor_code,
            vendor_item=vendor_item,
        )
    if args.include_desc2_description and desc2:
        payload["description"] = desc2
    if description_html:
        payload["description_html"] = description_html
        payload["update_description"] = False
    if args.include_metafields:
        payload["metafields"] = build_metafields(
            namespace=args.metafield_namespace,
            row=row,
            price_change_row=price_change_row,
            vendor_item_row=vendor_item_row,
            sku=sku,
            raw_desc=raw_desc,
            title=title,
            vendor_name=display_vendor,
            vendor_code=vendor_code,
            vendor_item=vendor_item,
            product_type=display_product_type,
            department=department,
            size=size,
            color=color,
            style=style,
            barcode=barcode,
            price=price,
            compare_at_price=compare_at_price,
            quantity=quantity,
            itemmqty_quantity=quantity_lookup.get(sku),
            desc2=desc2,
        )
        if matrix_variants:
            payload["metafields"].extend(
                [
                    {
                        "namespace": args.metafield_namespace,
                        "key": "matrix_variant_count",
                        "value": str(len(matrix_variants)),
                        "type": "number_integer",
                    },
                    {
                        "namespace": args.metafield_namespace,
                        "key": "matrix_barcode_format",
                        "value": "<base SKU>. <row> <column>",
                        "type": "single_line_text_field",
                    },
                ]
            )

    payload = prune_empty_values(payload)
    audit = {
        "sku": sku,
        "raw_desc": raw_desc,
        "generated_title": title,
        "vendor_name": display_vendor or "",
        "vendor_code": vendor_code or "",
        "product_type": display_product_type or "",
        "department": department or "",
        "size": size or "",
        "color": color or "",
        "desc2": desc2 or "",
        "barcode": barcode or "",
        "price": decimal_to_price(price) or "",
        "compare_at_price": decimal_to_price(compare_at_price) or "",
        "cost": decimal_to_price(cost) or "",
        "quantity": quantity if quantity is not None else "",
        "tag_count": len(payload.get("tags") or []),
        "metafield_count": len(payload.get("metafields") or []),
        "variant_count": len(matrix_variants),
        "variant_barcode_sample": matrix_variants[0]["barcode"] if matrix_variants else "",
    }
    return PreparedProduct(payload=payload, audit=audit)


def resolve_vendor_name(
    *,
    vendor_code: Optional[str],
    vendor_lookup: Dict[str, str],
    item_row: Dict[str, Any],
    price_change_row: Dict[str, Any],
) -> Optional[str]:
    return first_nonempty(
        vendor_lookup.get(vendor_code or ""),
        clean_text(item_row.get("PFIELD2")),
        vendor_lookup.get(clean_text(price_change_row.get("VENDOR")) or ""),
        vendor_code,
    )


def select_quantity(
    row: Dict[str, Any],
    *,
    quantity_lookup: Dict[str, Decimal],
    quantity_source: str,
) -> Optional[int]:
    sku = clean_text(row.get("SKU"))
    item_quantity = decimal_or_none(row.get("QTY"))
    itemmqty_quantity = quantity_lookup.get(sku) if sku else None

    if quantity_source == "itemmqty":
        quantity = itemmqty_quantity if itemmqty_quantity is not None else item_quantity
    elif quantity_source == "best":
        available_quantities = [value for value in (item_quantity, itemmqty_quantity) if value is not None]
        quantity = max(available_quantities) if available_quantities else None
    else:
        quantity = item_quantity
    return decimal_to_quantity(quantity)


def choose_compare_at_price(
    *,
    row: Dict[str, Any],
    price_change_row: Dict[str, Any],
    price: Optional[Decimal],
) -> Optional[Decimal]:
    if price is None or price <= 0:
        return None

    candidates: List[Decimal] = []
    for field_name in ("PRICE_R", "PRICE_B", "PRICE_C", "PRICE_D", "PRICE_E"):
        for source in (row, price_change_row):
            candidate = decimal_or_none(source.get(field_name))
            if candidate is not None and candidate > price and candidate <= price * Decimal("3"):
                candidates.append(candidate)
    if not candidates:
        return None
    return max(candidates)


def group_product_type(group_code: Optional[str]) -> Optional[str]:
    return GROUP_PRODUCT_TYPE_MAP.get((group_code or "").strip().upper())


def non_sellable_reason(row: Dict[str, Any]) -> Optional[str]:
    group_code = (clean_text(row.get("GROUP")) or "").upper()
    description = clean_text(row.get("DESC")) or ""
    sku = (clean_text(row.get("SKU")) or "").upper()

    if group_code in NON_SELLABLE_GROUPS:
        return f"group:{group_code}"
    if sku == "SHIPPING":
        return "sku:SHIPPING"
    for pattern in NON_SELLABLE_DESC_PATTERNS:
        if pattern.search(description):
            return f"desc:{pattern.pattern}"
    return None


def build_image_inputs(row: Dict[str, Any]) -> List[Dict[str, str]]:
    images: List[Dict[str, str]] = []
    seen: set[str] = set()
    for field_name in ("IMAGE", "IMAGE2", "IMAGE3", "IMAGE4"):
        value = clean_text(row.get(field_name))
        if not value:
            continue
        lowered = value.lower()
        if not (lowered.startswith("http://") or lowered.startswith("https://")):
            continue
        if value in seen:
            continue
        seen.add(value)
        images.append({"src": value})
    return images


def build_tags(
    *,
    row: Dict[str, Any],
    product_type: Optional[str],
    department: Optional[str],
    size: Optional[str],
    color: Optional[str],
    style: Optional[str],
    vendor_code: Optional[str],
    vendor_item: Optional[str],
) -> List[str]:
    tags: List[str] = []
    for label, value in (
        ("Department", department),
        ("Product Type", product_type),
        ("Style", style),
        ("Size", size),
        ("Color", color),
        ("Vendor Code", vendor_code),
        ("Vendor Item", vendor_item),
        ("PField1", clean_text(row.get("PFIELD1"))),
        ("PField2", clean_text(row.get("PFIELD2"))),
        ("PField3", clean_text(row.get("PFIELD3"))),
        ("PField4", clean_text(row.get("PFIELD4"))),
        ("PField5", clean_text(row.get("PFIELD5"))),
    ):
        if value:
            tags.append(f"{label}:{value}")
    return dedupe_preserving_order(tags)


def build_metafields(
    *,
    namespace: str,
    row: Dict[str, Any],
    price_change_row: Dict[str, Any],
    vendor_item_row: Dict[str, Any],
    sku: str,
    raw_desc: str,
    title: str,
    vendor_name: Optional[str],
    vendor_code: Optional[str],
    vendor_item: Optional[str],
    product_type: Optional[str],
    department: Optional[str],
    size: Optional[str],
    color: Optional[str],
    style: Optional[str],
    barcode: Optional[str],
    price: Optional[Decimal],
    compare_at_price: Optional[Decimal],
    quantity: Optional[int],
    itemmqty_quantity: Optional[Decimal],
    desc2: Optional[str],
) -> List[Dict[str, Any]]:
    metafields: List[Dict[str, Any]] = []

    def add(key: str, value: Any, metafield_type: str = "single_line_text_field") -> None:
        metafield = build_metafield(
            namespace=namespace,
            key=key,
            value=value,
            metafield_type=metafield_type,
        )
        if metafield:
            metafields.append(metafield)

    add("sku", sku)
    add("raw_description", raw_desc, "multi_line_text_field")
    add("generated_title", title)
    add("desc2", desc2, "multi_line_text_field")
    add("vendor_name", vendor_name)
    add("vendor_code", vendor_code)
    add("vendor_item", vendor_item)
    add("department_code", department)
    add("product_type", product_type)
    add("style", style)
    add("size", size)
    add("color", color)
    add("barcode", barcode)
    add("pos_type", clean_text(row.get("TYPE")))
    add("sellweb", row.get("SELLWEB"), "boolean")
    add("web_product_id", clean_text(row.get("WEBPRODID")))
    add("web_variant_id", clean_text(row.get("VARIANTID")))
    add("bin", first_nonempty(clean_text(row.get("BIN_NUM")), clean_text(row.get("BIN"))))
    add("last_ordered", clean_text(row.get("LAST_ORD")), "date")
    add("last_activity", clean_text(row.get("LAST_ACT")), "date")
    add("last_updated", clean_text(row.get("L_UPDATE")), "date")
    add("edit_date", clean_text(row.get("EDITDATE")), "date")
    add("price", format_decimal(price), "number_decimal")
    add("compare_at_price", format_decimal(compare_at_price), "number_decimal")
    add("cost", format_decimal(decimal_or_none(row.get("COST"))), "number_decimal")
    add("last_cost", format_decimal(decimal_or_none(row.get("LAST_COST"))), "number_decimal")
    add("vendor_cost", format_decimal(decimal_or_none(row.get("VEND_COST"))), "number_decimal")
    add("base_cost", format_decimal(decimal_or_none(row.get("BASE_COST"))), "number_decimal")
    add("case_cost", format_decimal(decimal_or_none(row.get("CASE_COST"))), "number_decimal")
    add("selected_quantity", quantity, "number_integer")
    add("item_quantity", decimal_to_quantity(decimal_or_none(row.get("QTY"))), "number_integer")
    add("itemmqty_quantity", decimal_to_quantity(itemmqty_quantity), "number_integer")
    add("committed", format_decimal(decimal_or_none(row.get("COMMITTED"))), "number_decimal")
    add("on_order", format_decimal(decimal_or_none(row.get("ON_ORDER"))), "number_decimal")
    add("reorder_level", format_decimal(decimal_or_none(row.get("REORDER"))), "number_decimal")
    add("stock_level", format_decimal(decimal_or_none(row.get("STK_LEVEL"))), "number_decimal")
    add("taxable", clean_text(row.get("TAXABLE")))
    add("item_data", compact_dbf_row(row), "json")
    add("price_change_data", compact_dbf_row(price_change_row), "json")
    add("vendor_item_data", compact_dbf_row(vendor_item_row), "json")

    return metafields


def build_metafield(
    *,
    namespace: str,
    key: str,
    value: Any,
    metafield_type: str,
) -> Optional[Dict[str, Any]]:
    normalized_value = metafield_value_to_string(value, metafield_type)
    if normalized_value is None:
        return None
    return {
        "namespace": clean_metafield_namespace(namespace),
        "key": key,
        "value": normalized_value,
        "type": metafield_type,
    }


def clean_metafield_namespace(namespace: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "_", (namespace or "").strip())
    return normalized.strip("_") or DEFAULT_METAFIELD_NAMESPACE


def metafield_value_to_string(value: Any, metafield_type: str) -> Optional[str]:
    if value is None:
        return None
    if metafield_type == "json":
        if not value:
            return None
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format_decimal(value)
    if isinstance(value, (dict, list)):
        if not value:
            return None
        return json.dumps(value, default=str, ensure_ascii=True, sort_keys=True)
    text = clean_text(value)
    if text is None:
        return None
    if "\x00" in text:
        return None
    return text


def compact_dbf_row(row: Dict[str, Any]) -> Dict[str, Any]:
    compact: Dict[str, Any] = {}
    for key, value in row.items():
        normalized = compact_json_value(value)
        if normalized is None:
            continue
        compact[key] = normalized
    return compact


def compact_json_value(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value if value else None
    if isinstance(value, Decimal):
        if value == 0:
            return None
        return format(value, "f")
    if isinstance(value, str):
        text = clean_text(value)
        if not text or "\x00" in text:
            return None
        return text
    return value


def build_smart_title(
    *,
    raw_desc: str,
    vendor_name: Optional[str],
    vendor_code: Optional[str],
    product_type: Optional[str],
    size: Optional[str],
    color: Optional[str],
    desc2: Optional[str],
) -> str:
    desc_without_vendor_prefix = strip_vendor_prefix(raw_desc, vendor_code=vendor_code)
    pretty_desc = smart_title_case(desc_without_vendor_prefix)
    pretty_vendor = simplify_vendor_name(vendor_name, product_type)
    pretty_type = smart_title_case(product_type) if product_type else None
    qualifier = human_desc2_value(desc2)
    brand_in_desc = bool(pretty_vendor and text_contains_phrase(pretty_desc, pretty_vendor))

    title_parts: List[str] = []
    if pretty_vendor and not brand_in_desc:
        title_parts.append(pretty_vendor)
    if (
        pretty_type
        and looks_code_heavy(desc_without_vendor_prefix)
        and not text_contains_phrase(pretty_desc, pretty_type)
        and not text_mentions_type(pretty_desc, pretty_type)
        and not brand_in_desc
    ):
        title_parts.append(pretty_type)
    title_parts.append(pretty_desc)

    title = " ".join(part for part in title_parts if part)
    if qualifier and looks_code_heavy(desc_without_vendor_prefix) and not text_contains_phrase(title, qualifier):
        title = f"{title} - {qualifier}"

    extras: List[str] = []
    display_color = color_for_title(color)
    if display_color and not text_contains_phrase(title, display_color):
        extras.append(display_color)

    display_size = size_for_title(size)
    if display_size and not text_contains_phrase(title, display_size):
        extras.append(display_size)

    if extras:
        title = f"{title} - {' / '.join(extras)}"
    return normalize_spaces(title)


def build_description_html(
    *,
    sku: str,
    title: str,
    raw_desc: str,
    vendor_name: Optional[str],
    vendor_code: Optional[str],
    vendor_item: Optional[str],
    product_type: Optional[str],
    department: Optional[str],
    size: Optional[str],
    color: Optional[str],
    style: Optional[str],
    barcode: Optional[str],
    price: Optional[Decimal],
    compare_at_price: Optional[Decimal],
    quantity: Optional[int],
    desc2: Optional[str],
    row: Dict[str, Any],
    vendor_item_row: Dict[str, Any],
) -> str:
    summary_parts: List[str] = []
    human_desc2 = human_desc2_value(desc2)
    if raw_desc and normalize_compare_text(raw_desc) != normalize_compare_text(title):
        summary_parts.append(f"POS name: {raw_desc}")
    if human_desc2:
        summary_parts.append(human_desc2)

    details: List[tuple[str, Any]] = [
        ("SKU", sku),
        ("Vendor", vendor_name),
        ("Vendor Code", vendor_code),
        ("Vendor Item", vendor_item),
        ("Product Type", product_type),
        ("Department", department),
        ("Style", style),
        ("Size", size),
        ("Color", color),
        ("Barcode", barcode),
        ("Price", format_decimal(price)),
        ("Compare At Price", format_decimal(compare_at_price)),
        ("Quantity", quantity),
        ("Committed", nonzero_decimal_string(row.get("COMMITTED"))),
        ("On Order", nonzero_decimal_string(row.get("ON_ORDER"))),
        ("Reorder Level", nonzero_decimal_string(row.get("REORDER"))),
        ("Stock Level", nonzero_decimal_string(row.get("STK_LEVEL"))),
        ("Cost", format_decimal(decimal_or_none(row.get("COST")))),
        ("Last Cost", format_decimal(decimal_or_none(row.get("LAST_COST")))),
        ("Vendor Cost", format_decimal(decimal_or_none(row.get("VEND_COST")))),
        ("Base Cost", format_decimal(decimal_or_none(vendor_item_row.get("BASE_COST")))),
        ("Case Cost", format_decimal(decimal_or_none(vendor_item_row.get("CASE_COST")))),
        ("Bin", first_nonempty(clean_text(row.get("BIN_NUM")), clean_text(row.get("BIN")))),
        ("Last Ordered", clean_text(row.get("LAST_ORD"))),
        ("Last Activity", clean_text(row.get("LAST_ACT"))),
    ]

    body_parts: List[str] = [f"<p>{html.escape(title)}</p>"]
    if summary_parts:
        body_parts.append("<p>" + "<br>".join(html.escape(part) for part in summary_parts) + "</p>")

    detail_items = [
        f"<li><strong>{html.escape(str(label))}:</strong> {html.escape(str(value))}</li>"
        for label, value in details
        if value not in (None, "", 0, "0.00")
    ]
    if detail_items:
        body_parts.append("<ul>" + "".join(detail_items) + "</ul>")
    return "".join(body_parts)


def strip_vendor_prefix(raw_desc: str, *, vendor_code: Optional[str]) -> str:
    desc = normalize_spaces(raw_desc)
    if not vendor_code:
        return desc
    pattern = re.compile(rf"^{re.escape(vendor_code)}([\s/-]+)", re.IGNORECASE)
    if not pattern.search(desc):
        return desc
    stripped = pattern.sub("", desc, count=1).strip()
    return stripped or desc


def looks_code_heavy(value: str) -> bool:
    tokens = re.findall(r"[A-Za-z0-9./'-]+", value.upper())
    if not tokens:
        return False
    score = 0
    for token in tokens:
        if any(character.isdigit() for character in token):
            score += 1
            continue
        if "/" in token or "-" in token:
            score += 1
    if len(tokens) == 1 and len(tokens[0]) >= 5 and tokens[0].isupper():
        score += 1
    return score >= max(1, len(tokens) // 2)


def human_desc2_value(value: Optional[str]) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    upper = text.upper()
    if upper in GENERIC_DESC2_VALUES:
        return None
    if any(character.isdigit() for character in text) and not re.search(r"[A-Za-z]{4,}", text):
        return None
    if re.fullmatch(r"[A-Z0-9./' -]+", upper) and not re.search(r"[A-Za-z]{4,}", text):
        return None
    return smart_title_case(text)


def smart_title_case(value: str) -> str:
    tokens = normalize_spaces(value).split(" ")
    return " ".join(_smart_case_token(token) for token in tokens if token)


def _smart_case_token(token: str) -> str:
    parts = re.split(r"([/&-])", token)
    return "".join(_smart_case_part(part) if part not in {"/", "&", "-"} else part for part in parts)


def _smart_case_part(part: str) -> str:
    match = re.fullmatch(r"([^A-Za-z0-9']*)([A-Za-z0-9'.]+)([^A-Za-z0-9']*)", part)
    if not match:
        return part
    prefix, core, suffix = match.groups()
    cased = _smart_case_core(core)
    return f"{prefix}{cased}{suffix}"


def _smart_case_core(core: str) -> str:
    upper = core.upper()
    if any(character.isdigit() for character in core):
        return upper
    if upper in TOKEN_CASE_PRESERVE:
        return upper
    if len(core) <= 2 and core.isalpha():
        return upper
    if "." in core:
        dot_parts = core.split(".")
        return ".".join(_smart_case_core(part) for part in dot_parts if part)
    if "'" in core:
        apostrophe_parts = core.split("'")
        return "'".join(_smart_case_core(part) for part in apostrophe_parts)
    return core[:1].upper() + core[1:].lower()


def color_for_title(value: Optional[str]) -> Optional[str]:
    raw = clean_text(value)
    if not raw:
        return None
    upper = normalize_spaces(raw).upper()
    if upper in COLOR_VALUE_MAP:
        return COLOR_VALUE_MAP[upper]
    if any(character.isdigit() for character in upper):
        return None
    if re.fullmatch(r"[A-Z]{1,4}", upper):
        return None

    pieces = re.split(r"([/ ])", upper.replace(".", " "))
    converted: List[str] = []
    for piece in pieces:
        if piece in {"", " ", "/"}:
            converted.append(piece)
            continue
        converted.append(COLOR_TOKEN_MAP.get(piece, _smart_case_core(piece)))
    result = "".join(converted).strip()
    return normalize_spaces(result) or None


def size_for_title(value: Optional[str]) -> Optional[str]:
    raw = clean_text(value)
    if not raw:
        return None
    upper = normalize_spaces(raw).upper()
    if upper in STANDARD_SIZE_VALUES:
        return upper.title() if upper == "OVER CALF" else upper
    if re.fullmatch(r"\d{1,3}/\d{1,3}", upper):
        return upper
    if re.fullmatch(r"\d{1,3}(\.\d)?[A-Z]{0,2}", upper):
        return upper
    if upper in {"XS", "S", "M", "L", "XL", "XXL", "XXXL"}:
        return upper
    return None


def text_contains_phrase(text: str, phrase: str) -> bool:
    return normalize_compare_text(phrase) in normalize_compare_text(text)


def text_mentions_type(text: str, phrase: str) -> bool:
    text_words = {normalize_compare_word(word) for word in re.findall(r"[A-Za-z]+", text)}
    phrase_words = [normalize_compare_word(word) for word in re.findall(r"[A-Za-z]+", phrase)]
    return bool(phrase_words) and all(word in text_words for word in phrase_words)


def simplify_vendor_name(vendor_name: Optional[str], product_type: Optional[str]) -> Optional[str]:
    pretty_vendor = smart_title_case(vendor_name) if vendor_name else None
    pretty_type = smart_title_case(product_type) if product_type else None
    if not pretty_vendor or not pretty_type:
        return pretty_vendor

    vendor_words = pretty_vendor.split()
    type_words = pretty_type.split()
    if len(vendor_words) > len(type_words) and [normalize_compare_word(word) for word in vendor_words[-len(type_words):]] == [
        normalize_compare_word(word) for word in type_words
    ]:
        return " ".join(vendor_words[:-len(type_words)])
    return pretty_vendor


def normalize_compare_text(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def normalize_compare_word(value: str) -> str:
    word = value.lower()
    if word.endswith("ies") and len(word) > 4:
        return word[:-3] + "y"
    if word.endswith("s") and len(word) > 3:
        return word[:-1]
    return word


def dedupe_preserving_order(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    deduped: List[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = normalize_spaces(str(value))
    return text or None


def normalize_spaces(value: str) -> str:
    return " ".join(str(value).strip().split())


def decimal_or_none(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def first_nonempty(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def first_nonempty_decimal(*values: Optional[Decimal]) -> Optional[Decimal]:
    for value in values:
        if value is not None:
            return value
    return None


def decimal_to_price(value: Optional[Decimal]) -> Optional[float]:
    if value is None:
        return None
    return float(value.quantize(Decimal("0.01")))


def decimal_to_quantity(value: Optional[Decimal]) -> Optional[int]:
    if value is None:
        return None
    return max(0, math.floor(value))


def format_decimal(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    return f"{value.quantize(Decimal('0.01'))}"


def nonzero_decimal_string(value: Any) -> Optional[str]:
    decimal_value = decimal_or_none(value)
    if decimal_value is None or decimal_value == 0:
        return None
    return format_decimal(decimal_value)


def prune_empty_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if value == "":
            continue
        if isinstance(value, list) and not value:
            continue
        cleaned[key] = value
    return cleaned


def resolve_endpoint(args: argparse.Namespace) -> str:
    default_path = "/wc-api/v3/customers/batch" if args.customers else "/wc-api/v3/products/batch"
    if args.endpoint:
        endpoint = args.endpoint.strip()
    else:
        base_url = args.base_url or _env_any("SHOPIFY_SYNC_BASE_URL", "URL", "APP_BASE_URL")
        if not base_url:
            raise ValueError("Put URL=... in .env, or pass --base-url or --endpoint.")
        if args.customers:
            endpoint_path = _env_any("SHOPIFY_SYNC_CUSTOMER_PATH") or default_path
        else:
            endpoint_path = _env_any("SHOPIFY_SYNC_PATH", "POS_SYNC_PATH", "Path") or default_path
        endpoint = f"{base_url.rstrip('/')}/{endpoint_path.lstrip('/')}"
    return normalize_batch_endpoint(endpoint, customers=args.customers)


def normalize_batch_endpoint(endpoint: str, *, customers: bool = False) -> str:
    parsed = urlparse(endpoint)
    path = parsed.path.rstrip("/")
    collection_paths = (
        ("/wc-api/v3/customers", "/wp-json/wc/v3/customers")
        if customers
        else ("/wc-api/v3/products", "/wp-json/wc/v3/products")
    )
    if path.endswith(collection_paths):
        path = f"{path}/batch"
    normalized = parsed._replace(path=path)
    return urlunparse(normalized)


def normalize_reconcile_endpoint(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    path = parsed.path.rstrip("/")
    for suffix in ("/wc-api/v3/products/batch", "/wp-json/wc/v3/products/batch"):
        if path.endswith(suffix):
            path = f"{path[:-len('/batch')]}/reconcile"
            break
    else:
        if path.endswith("/sync/bulk"):
            path = f"{path[:-len('/sync/bulk')]}/sync/catalog/reconcile"
        else:
            raise ValueError(
                "Cannot derive the catalog reconciliation URL from the configured product batch endpoint."
            )
    return urlunparse(parsed._replace(path=path))


def endpoint_has_query_auth(endpoint: str) -> bool:
    query = parse_qs(urlparse(endpoint).query)
    return bool(query.get("consumer_key") and query.get("consumer_secret"))


def resolve_sync_auth(args: argparse.Namespace) -> tuple[str, Optional[str], Optional[str]]:
    endpoint = resolve_endpoint(args)
    api_key = args.api_key or _env_any("SHOPIFY_SYNC_API_KEY", "POS_API_KEY", "Key")
    api_secret = args.api_secret or _env_any("SHOPIFY_SYNC_API_SECRET", "POS_API_SECRET", "Secret")
    if (not api_key or not api_secret) and not endpoint_has_query_auth(endpoint):
        raise ValueError(
            "Missing API credentials. Pass --api-key/--api-secret, set SHOPIFY_SYNC_API_KEY and "
            "SHOPIFY_SYNC_API_SECRET, or use an --endpoint URL with consumer_key and consumer_secret."
        )
    return endpoint, api_key, api_secret


def reconcile_missing_products(
    *,
    endpoint: str,
    api_key: Optional[str],
    api_secret: Optional[str],
    source_skus: List[str],
    apply: bool,
    timeout: int,
) -> Dict[str, Any]:
    if len(source_skus) < 100:
        raise ValueError(
            f"Refusing catalog reconciliation with only {len(source_skus)} source SKUs; expected a complete Item.dbf."
        )

    session = requests.Session()
    session.headers.update(
        {
            "Content-Type": "application/json",
            "User-Agent": "dbf-pos-sync/2.0",
        }
    )
    if api_key and api_secret:
        session.headers.update({"X-API-Key": api_key, "X-API-Secret": api_secret})

    reconcile_endpoint = normalize_reconcile_endpoint(endpoint)
    payload: Dict[str, Any] = {
        "source_skus": source_skus,
        "apply": apply,
    }
    if apply:
        payload["confirmation"] = "ARCHIVE_MISSING_PRODUCTS"

    action = "Applying" if apply else "Previewing"
    print(f"{action} missing-product reconciliation with {len(source_skus)} Item.dbf SKUs...")
    response = session.post(reconcile_endpoint, json=payload, timeout=max(1, timeout))
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict):
        raise ValueError("The reconciliation endpoint returned an unexpected response.")

    print(
        "Reconciliation: "
        f"Shopify products={int_or_zero(result.get('shopify_product_count'))}, "
        f"missing candidates={int_or_zero(result.get('candidate_count'))}, "
        f"archived={int_or_zero(result.get('archived_count'))}, "
        f"failed={int_or_zero(result.get('failed_count'))}"
    )
    candidates = result.get("candidates") or []
    for candidate in candidates[:10]:
        skus = ", ".join(candidate.get("skus") or [])
        print(f"  Missing: {skus or '[no SKU]'} - {candidate.get('title') or 'Untitled product'}")
    if len(candidates) > 10:
        print(f"  ...and {len(candidates) - 10} more (see the reconciliation report).")
    return result


def write_payloads(path: Path, payloads: List[Dict[str, Any]], *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payloads, handle, indent=2 if pretty else None, ensure_ascii=True)
        handle.write("\n")


def print_product_preview(payloads: List[Dict[str, Any]], *, verbose: bool) -> None:
    if not payloads:
        return
    if verbose:
        print(json.dumps(payloads[:2], indent=2, ensure_ascii=True))
        return

    compact_preview: List[Dict[str, Any]] = []
    for payload in payloads[:2]:
        item = {
            key: payload.get(key)
            for key in (
                "sku",
                "title",
                "vendor",
                "brand",
                "product_type",
                "barcode",
                "price",
                "cost",
                "quantity",
                "tracked",
                "status",
                "update_description",
            )
            if payload.get(key) not in (None, "", [])
        }
        tags = payload.get("tags") or []
        metafields = payload.get("metafields") or []
        variants = payload.get("variants") or []
        description = payload.get("description_html") or payload.get("description") or ""
        if tags:
            item["tag_count"] = len(tags)
            item["tag_sample"] = tags[:5]
        if metafields:
            item["metafield_count"] = len(metafields)
        if variants:
            item["variant_count"] = len(variants)
            item["variant_sample"] = [
                {
                    key: variant.get(key)
                    for key in ("sku", "barcode", "option_values", "quantity")
                    if variant.get(key) not in (None, "", [])
                }
                for variant in variants[:3]
            ]
        if description:
            item["description_chars"] = len(str(description))
        compact_preview.append(item)

    print("Sample product payloads (compact; use --verbose-preview for full JSON):")
    print(json.dumps(compact_preview, indent=2, ensure_ascii=True))


def print_customer_preview(payloads: List[Dict[str, Any]], *, verbose: bool) -> None:
    if not payloads:
        return
    if verbose:
        print(json.dumps(payloads[:2], indent=2, ensure_ascii=True))
        return

    compact_preview: List[Dict[str, Any]] = []
    for payload in payloads[:2]:
        item = {
            key: payload.get(key)
            for key in (
                "pos_customer_number",
                "firstName",
                "lastName",
                "company",
                "email",
                "phone",
                "taxExempt",
            )
            if payload.get(key) not in (None, "", [])
        }
        addresses = payload.get("addresses") or []
        tags = payload.get("tags") or []
        metafields = payload.get("metafields") or []
        if addresses:
            item["address_count"] = len(addresses)
        if tags:
            item["tag_count"] = len(tags)
            item["tag_sample"] = tags[:5]
        if metafields:
            item["metafield_count"] = len(metafields)
        compact_preview.append(item)

    print("Sample customer payloads (compact; use --verbose-preview for full JSON):")
    print(json.dumps(compact_preview, indent=2, ensure_ascii=True))


def write_title_audit(path: Path, audits: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "sku",
        "raw_desc",
        "generated_title",
        "vendor_name",
        "vendor_code",
        "product_type",
        "department",
        "size",
        "color",
        "desc2",
        "barcode",
        "price",
        "compare_at_price",
        "cost",
        "quantity",
        "tag_count",
        "metafield_count",
        "variant_count",
        "variant_barcode_sample",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for audit in audits:
            writer.writerow(audit)


def write_matrix_variant_audit(path: Path, payloads: List[Dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "base_sku",
        "product_title",
        "variant_sku",
        "barcode",
        "size",
        "color",
        "option_title",
        "pos_cell",
        "quantity",
        "price",
        "cost",
    ]
    row_count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for payload in payloads:
            for variant in payload.get("variants") or []:
                options = variant.get("option_values") or {}
                writer.writerow(
                    {
                        "base_sku": payload.get("sku") or "",
                        "product_title": payload.get("title") or "",
                        "variant_sku": variant.get("sku") or "",
                        "barcode": variant.get("barcode") or "",
                        "size": options.get("Size") or "",
                        "color": options.get("Color") or "",
                        "option_title": options.get("Title") or "",
                        "pos_cell": variant.get("pos_cell") or "",
                        "quantity": variant.get("quantity") if variant.get("quantity") is not None else "",
                        "price": variant.get("price") if variant.get("price") is not None else "",
                        "cost": variant.get("cost") if variant.get("cost") is not None else "",
                    }
                )
                row_count += 1
    return row_count


def write_customer_audit(path: Path, audits: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "source",
        "cust_num",
        "first_name",
        "last_name",
        "company",
        "email",
        "phone",
        "address_count",
        "tag_count",
        "metafield_count",
        "last_activity",
        "total_purchased",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for audit in audits:
            writer.writerow(audit)


def write_shopify_customer_csvs(
    output_dir: Path,
    payloads: List[Dict[str, Any]],
    *,
    max_bytes: int = SHOPIFY_CUSTOMER_CSV_MAX_BYTES,
) -> List[Dict[str, Any]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    header_line = render_shopify_customer_csv_header()
    header_size = len(header_line.encode("utf-8"))
    safe_max_bytes = max(max_bytes, header_size + 1024)
    files: List[Dict[str, Any]] = []
    current_lines: List[str] = []
    current_size = header_size
    part_number = 1
    seen_emails: set[str] = set()
    seen_phones: set[str] = set()
    duplicate_emails_cleared = 0
    duplicate_phones_cleared = 0

    def flush() -> None:
        nonlocal part_number, current_lines, current_size
        if not current_lines:
            return
        path = output_dir / f"shopify-customers-{part_number:03d}.csv"
        text = header_line + "".join(current_lines)
        path.write_text(text, encoding="utf-8")
        files.append(
            {
                "path": str(path),
                "rows": len(current_lines),
                "bytes": len(text.encode("utf-8")),
            }
        )
        part_number += 1
        current_lines = []
        current_size = header_size

    for payload in payloads:
        row = shopify_customer_csv_row(payload)
        email_key = row["Email"].lower()
        if email_key:
            if email_key in seen_emails:
                row["Email"] = ""
                duplicate_emails_cleared += 1
            else:
                seen_emails.add(email_key)

        phone_key = row["Phone"]
        if phone_key:
            if phone_key in seen_phones:
                row["Phone"] = ""
                if row["Default Address Phone"] == phone_key:
                    row["Default Address Phone"] = ""
                duplicate_phones_cleared += 1
            else:
                seen_phones.add(phone_key)

        row_line = render_shopify_customer_csv_row(row)
        row_size = len(row_line.encode("utf-8"))
        if current_lines and current_size + row_size > safe_max_bytes:
            flush()
        current_lines.append(row_line)
        current_size += row_size

    flush()
    if files:
        files[0]["duplicate_email_values_cleared"] = duplicate_emails_cleared
        files[0]["duplicate_phone_values_cleared"] = duplicate_phones_cleared
    return files


def render_shopify_customer_csv_header() -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=SHOPIFY_CUSTOMER_CSV_HEADERS, lineterminator="\n")
    writer.writeheader()
    return buffer.getvalue()


def render_shopify_customer_csv_row(row: Dict[str, str]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=SHOPIFY_CUSTOMER_CSV_HEADERS, lineterminator="\n")
    writer.writerow(row)
    return buffer.getvalue()


def shopify_customer_csv_row(payload: Dict[str, Any]) -> Dict[str, str]:
    address = first_customer_address(payload)
    country_code = normalize_country_code(address.get("countryCode"))
    province_code = normalize_province_code(address.get("provinceCode"), country_code=country_code)
    note = shopify_csv_text(payload.get("note"))
    tags = [shopify_csv_text(tag) for tag in payload.get("tags") or [] if shopify_csv_text(tag)]
    source = shopify_csv_text(payload.get("source"))
    pos_customer_number = shopify_csv_text(payload.get("pos_customer_number"))
    if pos_customer_number and f"POS ID:{pos_customer_number}" not in tags:
        tags.append(f"POS ID:{pos_customer_number}")
    if source and f"POS Source:{source}" not in tags:
        tags.append(f"POS Source:{source}")

    return {
        "First Name": shopify_csv_text(payload.get("firstName")),
        "Last Name": shopify_csv_text(payload.get("lastName") or payload.get("company")),
        "Email": shopify_csv_text(payload.get("email")),
        "Accepts Email Marketing": "no",
        "Default Address Company": shopify_csv_text(address.get("company") or payload.get("company")),
        "Default Address Address1": shopify_csv_text(address.get("address1")),
        "Default Address Address2": shopify_csv_text(address.get("address2")),
        "Default Address City": shopify_csv_text(address.get("city")),
        "Default Address Province Code": province_code,
        "Default Address Country Code": country_code,
        "Default Address Zip": shopify_csv_text(address.get("zip")),
        "Default Address Phone": shopify_csv_text(address.get("phone") or payload.get("phone")),
        "Phone": shopify_csv_text(payload.get("phone")),
        "Accepts SMS Marketing": "no",
        "Accepts WhatsApp Marketing": "no",
        "Tags": ", ".join(tag for tag in tags if tag),
        "Note": note,
        "Tax Exempt": "yes" if payload.get("taxExempt") else "no",
    }


def first_customer_address(payload: Dict[str, Any]) -> Dict[str, Any]:
    addresses = payload.get("addresses") or []
    if isinstance(addresses, list) and addresses and isinstance(addresses[0], dict):
        return addresses[0]
    return {}


def normalize_country_code(value: Any) -> str:
    text = shopify_csv_text(value).upper()
    return text if len(text) == 2 else "US"


def normalize_province_code(value: Any, *, country_code: str) -> str:
    text = shopify_csv_text(value).upper()
    if not text:
        return ""
    if country_code == "US":
        if len(text) == 2:
            return text
        return US_STATE_CODES.get(text, "")
    return text if len(text) <= 3 else ""


def shopify_csv_text(value: Any) -> str:
    text = clean_text(value) or ""
    text = (
        text.replace("\x00", "")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2018", "'")
        .replace("\u2019", "'")
    )
    text = re.sub(r"[\r\n]+", " | ", text)
    return text.strip()


def write_customer_summary(
    path: Path,
    *,
    args: argparse.Namespace,
    stats: CustomerBuildStats,
    payload_count: int,
    upload_summary: Optional[Dict[str, Any]] = None,
    shopify_csv_files: Optional[List[Dict[str, Any]]] = None,
    missing_customer_report: Optional[Dict[str, Any]] = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "command": sanitize_args_for_report(args),
        "payload_count": payload_count,
        "build_stats": {
            "total_rows_seen": stats.total_rows_seen,
            "skipped_deleted": stats.skipped_deleted,
            "skipped_missing_identity": stats.skipped_missing_identity,
            "skipped_duplicate": stats.skipped_duplicate,
            "source_rows_seen": stats.source_rows_seen,
            "source_payloads": stats.source_payloads,
        },
        "upload_enabled": bool(args.upload),
        "upload_note": (
            "Customer upload was requested. Shopify must grant read_customers/write_customers scopes and protected customer data access."
            if args.upload
            else "Customer upload was not requested. Add --upload --yes only after Shopify customer scopes and protected customer data access are granted."
        ),
        "upload_summary": upload_summary,
        "shopify_customer_csv_files": shopify_csv_files or [],
        "missing_customer_report": missing_customer_report,
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def write_archive_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def write_archive_manifest_csv(path: Path, manifest: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "table",
        "path",
        "category",
        "records",
        "record_length",
        "field_count",
        "sensitive_fields",
        "fields",
        "error",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for table in manifest.get("tables", []):
            row = dict(table)
            row["fields"] = ";".join(row.get("fields") or [])
            row["sensitive_fields"] = ";".join(row.get("sensitive_fields") or [])
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def export_core_table_csvs(
    *,
    dbf_dir: Path,
    report_dir: Path,
    recursive: bool,
    row_limit: int,
) -> List[Dict[str, Any]]:
    export_dir = report_dir / "core-table-csv"
    export_dir.mkdir(parents=True, exist_ok=True)
    exported: List[Dict[str, Any]] = []
    safe_limit = max(1, row_limit)

    for table_name in CORE_EXPORT_TABLES:
        table_path = find_dbf_file(dbf_dir, f"{table_name}.dbf", recursive=recursive)
        if table_path is None:
            continue
        table = read_dbf_header_summary(table_path, root=dbf_dir)
        output_path = export_dir / f"{table.table_name}.csv"
        fieldnames = [field.name for field in table.fields]
        row_count = 0
        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in iter_dbf_rows(table_path):
                writer.writerow({field: value if value is not None else "" for field, value in row.items()})
                row_count += 1
                if row_count >= safe_limit:
                    break
        exported.append(
            {
                "table": table.table_name,
                "path": str(output_path),
                "rows_written": row_count,
                "records_in_dbf": table.record_count,
                "truncated": row_count < table.record_count,
                "sensitive_fields": detect_sensitive_fields(table.fields),
            }
        )
    return exported


def write_run_summary(
    path: Path,
    *,
    args: argparse.Namespace,
    stats: BuildStats,
    payload_count: int,
    upload_summary: Optional[Dict[str, Any]],
    manifest: Optional[Dict[str, Any]],
    exported_tables: Optional[List[Dict[str, Any]]] = None,
    reconciliation_summary: Optional[Dict[str, Any]] = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "command": sanitize_args_for_report(args),
        "payload_count": payload_count,
        "build_stats": {
            "total_rows_seen": stats.total_rows_seen,
            "skipped_duplicate_sku": stats.skipped_duplicate_sku,
            "skipped_zero_price": stats.skipped_zero_price,
            "skipped_zero_quantity": stats.skipped_zero_quantity,
            "skipped_non_sellable": stats.skipped_non_sellable,
            "skipped_missing_payload": stats.skipped_missing_payload,
        },
        "upload_summary": upload_summary,
        "reconciliation_summary": reconciliation_summary,
        "archive": {
            "table_count": manifest.get("table_count") if manifest else None,
            "total_records": manifest.get("total_records") if manifest else None,
            "sensitive_table_count": manifest.get("sensitive_table_count") if manifest else None,
            "categories": manifest.get("categories") if manifest else None,
            "notes": manifest.get("notes") if manifest else None,
        },
        "exported_tables": exported_tables or [],
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def write_reconciliation_reports(report_dir: Path, summary: Dict[str, Any]) -> None:
    write_payloads(report_dir / "missing-products.json", summary, pretty=True)
    csv_path = report_dir / "missing-products.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("product_id", "title", "status", "skus"),
        )
        writer.writeheader()
        for candidate in summary.get("candidates") or []:
            writer.writerow(
                {
                    "product_id": candidate.get("product_id") or "",
                    "title": candidate.get("title") or "",
                    "status": candidate.get("status") or "",
                    "skus": ";".join(candidate.get("skus") or []),
                }
            )


def sanitize_args_for_report(args: argparse.Namespace) -> Dict[str, Any]:
    data = dict(vars(args))
    if data.get("api_secret"):
        data["api_secret"] = "***"
    if data.get("api_key"):
        data["api_key"] = mask_secret(str(data["api_key"]))
    if data.get("endpoint"):
        data["endpoint"] = mask_endpoint_secret(str(data["endpoint"]))
    return data


def mask_secret(value: str) -> str:
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def mask_endpoint_secret(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    query = parse_qs(parsed.query, keep_blank_values=True)
    masked_pairs = []
    for key, values in query.items():
        lowered = key.lower()
        for value in values:
            if lowered in {"consumer_key", "oauth_consumer_key"}:
                masked_pairs.append((key, mask_secret(value)))
            elif lowered in {"consumer_secret", "oauth_signature"}:
                masked_pairs.append((key, "***"))
            else:
                masked_pairs.append((key, value))
    query_string = "&".join(f"{key}={value}" for key, value in masked_pairs)
    return urlunparse(parsed._replace(query=query_string))


def timestamp_for_filename() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def upload_payloads(
    *,
    endpoint: str,
    api_key: Optional[str],
    api_secret: Optional[str],
    payloads: List[Dict[str, Any]],
    batch_size: int,
    workers: int,
    timeout: int,
    start_offset: int = 0,
    max_batches: Optional[int] = None,
    resume_file: Optional[Path] = None,
    stop_on_failure: bool = False,
) -> Dict[str, Any]:
    session = requests.Session()
    session.headers.update(
        {
            "Content-Type": "application/json",
            "User-Agent": "dbf-pos-sync/2.0",
        }
    )
    if api_key and api_secret:
        session.headers.update(
            {
                "X-API-Key": api_key,
                "X-API-Secret": api_secret,
            }
        )
    session.headers["X-Sync-Workers"] = str(max(1, workers))

    total = len(payloads)
    effective_start = max(0, start_offset)
    checkpoint = read_resume_checkpoint(resume_file)
    if checkpoint is not None:
        checkpoint_next = int(checkpoint.get("next_index") or 0)
        if checkpoint_next > effective_start:
            effective_start = checkpoint_next
            print(f"Resuming from checkpoint index {effective_start}")

    summary = {
        "endpoint": mask_endpoint_secret(endpoint),
        "total_payloads": total,
        "start_offset": effective_start,
        "batch_size": batch_size,
        "workers": max(1, workers),
        "batches_attempted": 0,
        "rows_attempted": 0,
        "succeeded": 0,
        "failed": 0,
        "next_index": effective_start,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "batch_failures": [],
    }

    if effective_start >= total:
        print(f"Resume offset {effective_start} is at or beyond the {total} prepared products. Nothing to upload.")
        summary["finished_at"] = datetime.now(timezone.utc).isoformat()
        return summary

    for start in range(effective_start, total, batch_size):
        if max_batches is not None and summary["batches_attempted"] >= max(0, max_batches):
            print(f"Stopped after --max-batches={max_batches}")
            break

        chunk = payloads[start : start + batch_size]
        print(
            f"Uploading batch {start // batch_size + 1}: "
            f"rows {start + 1}-{start + len(chunk)} of {total} "
            f"(timeout={timeout}s, workers={max(1, workers)})"
        )
        response = session.post(endpoint, json=chunk, timeout=timeout)
        response.raise_for_status()
        body = response.json()
        succeeded = int_or_zero(body.get("succeeded"))
        failed = int_or_zero(body.get("failed"))
        summary["batches_attempted"] += 1
        summary["rows_attempted"] += len(chunk)
        summary["succeeded"] += succeeded
        summary["failed"] += failed
        summary["next_index"] = start + len(chunk)
        print(
            f"Uploaded batch {start // batch_size + 1}: "
            f"{len(chunk)} rows, succeeded={succeeded}, failed={failed}"
        )
        if failed:
            print(json.dumps(body, indent=2, ensure_ascii=True))
            summary["batch_failures"].append(
                {
                    "start_index": start,
                    "row_count": len(chunk),
                    "response": body,
                }
            )
            if stop_on_failure:
                write_resume_checkpoint(resume_file, summary)
                raise RuntimeError(f"Stopping because batch {start // batch_size + 1} had {failed} product failures.")
        write_resume_checkpoint(resume_file, summary)

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_resume_checkpoint(resume_file, summary)
    return summary


def int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def read_resume_checkpoint(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if path is None or not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def write_resume_checkpoint(path: Optional[Path], summary: Dict[str, Any]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, ensure_ascii=True)
        handle.write("\n")


def main() -> int:
    args = parse_args()
    if args.customers:
        return run_customer_mode(args)

    dbf_dir = Path(args.dbf_dir).expanduser()
    report_dir = Path(args.report_dir).expanduser() if args.report_dir else None
    manifest: Optional[Dict[str, Any]] = None
    exported_tables: List[Dict[str, Any]] = []
    upload_summary: Optional[Dict[str, Any]] = None
    reconciliation_summary: Optional[Dict[str, Any]] = None
    source_skus: List[str] = []

    try:
        prepared_products, stats = load_products(args)
    except Exception as exc:
        print(f"Failed to build payloads: {exc}", file=sys.stderr)
        return 1

    payloads = [item.payload for item in prepared_products]
    audits = [item.audit for item in prepared_products]
    matrix_product_count, matrix_variant_count = validate_matrix_variants(prepared_products)
    if args.archive_missing:
        try:
            source_skus = load_item_source_skus(dbf_dir, recursive=args.recursive)
        except Exception as exc:
            print(f"Failed to read the complete Item.dbf SKU list: {exc}", file=sys.stderr)
            return 1
        print(f"Loaded {len(source_skus)} unique Item.dbf SKUs for missing-product reconciliation")
    print(f"Built {len(payloads)} product payloads from {args.dbf_dir}")
    if args.dry_run:
        print("Read-only mode: no Shopify upload will run. Add --upload --yes when you are ready to send data.")
    if args.full_sync:
        print("Full-sync mode: rich product descriptions, tags, POS metafields, and zero-stock products are enabled.")
    if args.matrix_variants:
        print(
            "Matrix-variant mode: "
            f"{matrix_product_count} products, {matrix_variant_count} exact POS barcodes, no duplicates."
        )
    if stats.skipped_duplicate_sku:
        print(f"Deduplicated {stats.skipped_duplicate_sku} repeated Item.dbf SKU rows")
    if stats.skipped_non_sellable:
        print(f"Skipped {stats.skipped_non_sellable} internal or non-sellable POS rows")
    if stats.skipped_zero_quantity:
        print(f"Skipped {stats.skipped_zero_quantity} rows with zero quantity")
    if stats.skipped_zero_price:
        print(f"Skipped {stats.skipped_zero_price} rows with zero price")
    if audits:
        changed_samples = [audit for audit in audits if audit["raw_desc"] != audit["generated_title"]][:5]
        if changed_samples:
            print("Sample title changes:")
            for audit in changed_samples:
                print(f"  {audit['sku']}: {audit['raw_desc']} -> {audit['generated_title']}")

    print_product_preview(payloads, verbose=args.verbose_preview)

    if report_dir:
        report_dir.mkdir(parents=True, exist_ok=True)
        try:
            manifest = build_archive_manifest(dbf_dir, recursive=args.recursive)
            write_archive_manifest(report_dir / "archive-manifest.json", manifest)
            write_archive_manifest_csv(report_dir / "archive-manifest.csv", manifest)
            write_payloads(report_dir / "payload-preview.json", payloads[:25], pretty=True)
            if args.matrix_variants:
                audit_count = write_matrix_variant_audit(report_dir / "matrix-variant-audit.csv", payloads)
                print(f"Wrote {audit_count} matrix variants to {report_dir / 'matrix-variant-audit.csv'}")
            print(
                f"Wrote archive manifest to {report_dir} "
                f"({manifest['table_count']} tables, {manifest['total_records']} DBF records)"
            )
            if manifest.get("sensitive_table_count"):
                print(
                    f"Sensitive tables found: {manifest['sensitive_table_count']}. "
                    "They are recorded in the manifest but not uploaded as Shopify product data."
                )
        except Exception as exc:
            print(f"Failed to write archive report: {exc}", file=sys.stderr)
            return 1

        if args.export_core_csv:
            try:
                exported_tables = export_core_table_csvs(
                    dbf_dir=dbf_dir,
                    report_dir=report_dir,
                    recursive=args.recursive,
                    row_limit=args.export_row_limit,
                )
                print(f"Exported {len(exported_tables)} core DBF tables to {report_dir / 'core-table-csv'}")
            except Exception as exc:
                print(f"Failed to export core table CSVs: {exc}", file=sys.stderr)
                return 1

    if args.output_json:
        output_path = Path(args.output_json).expanduser()
        write_payloads(output_path, payloads, pretty=args.pretty)
        print(f"Wrote payloads to {output_path}")

    if args.output_title_audit:
        audit_path = Path(args.output_title_audit).expanduser()
        write_title_audit(audit_path, audits)
        print(f"Wrote title audit to {audit_path}")

    if args.dry_run:
        if args.archive_missing:
            try:
                endpoint, api_key, api_secret = resolve_sync_auth(args)
                reconciliation_summary = reconcile_missing_products(
                    endpoint=endpoint,
                    api_key=api_key,
                    api_secret=api_secret,
                    source_skus=source_skus,
                    apply=False,
                    timeout=max(1, args.timeout),
                )
                if report_dir:
                    write_reconciliation_reports(report_dir, reconciliation_summary)
                    print(f"Wrote reconciliation preview to {report_dir / 'missing-products.csv'}")
            except requests.HTTPError as exc:
                response = exc.response
                print(
                    f"Reconciliation preview failed with HTTP {response.status_code}: {response.text}",
                    file=sys.stderr,
                )
                return 1
            except Exception as exc:
                print(f"Reconciliation preview failed: {exc}", file=sys.stderr)
                return 1
        if report_dir:
            write_run_summary(
                report_dir / "run-summary.json",
                args=args,
                stats=stats,
                payload_count=len(payloads),
                upload_summary=None,
                manifest=manifest,
                exported_tables=exported_tables,
                reconciliation_summary=reconciliation_summary,
            )
        return 0

    if not payloads and not args.archive_missing:
        print("No payloads matched the selected filters.")
        return 0

    if args.upload and not args.yes:
        print(
            "Refusing live upload without --yes. Run once in read-only mode first, then add --upload --yes "
            "when you are ready to update Shopify.",
            file=sys.stderr,
        )
        return 1

    try:
        endpoint, api_key, api_secret = resolve_sync_auth(args)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    try:
        if payloads:
            upload_summary = upload_payloads(
                endpoint=endpoint,
                api_key=api_key,
                api_secret=api_secret,
                payloads=payloads,
                batch_size=max(1, args.batch_size),
                workers=max(1, args.workers),
                timeout=max(1, args.timeout),
                start_offset=max(0, args.start_offset),
                max_batches=args.max_batches,
                resume_file=Path(args.resume_file).expanduser() if args.resume_file else None,
                stop_on_failure=args.stop_on_failure,
            )

        if args.archive_missing:
            if upload_summary and (
                int_or_zero(upload_summary.get("failed")) > 0
                or int_or_zero(upload_summary.get("next_index")) < len(payloads)
            ):
                raise RuntimeError(
                    "Refusing to archive missing products because the product upload did not complete successfully."
                )
            reconciliation_summary = reconcile_missing_products(
                endpoint=endpoint,
                api_key=api_key,
                api_secret=api_secret,
                source_skus=source_skus,
                apply=True,
                timeout=max(1, args.timeout),
            )
            if report_dir:
                write_reconciliation_reports(report_dir, reconciliation_summary)
                print(f"Wrote reconciliation results to {report_dir / 'missing-products.csv'}")
    except requests.HTTPError as exc:
        response = exc.response
        print(
            f"Upload failed with HTTP {response.status_code}: {response.text}",
            file=sys.stderr,
        )
        return 1
    except requests.ReadTimeout:
        print(
            f"Upload timed out after {args.timeout} seconds while waiting for the batch response. "
            "The sync app processes each batch synchronously, so larger batches can take a while. "
            "Try a smaller --batch-size such as 10 or 25, or a larger --timeout such as 300.",
            file=sys.stderr,
        )
        return 1
    except requests.RequestException as exc:
        print(f"Upload failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Upload failed: {exc}", file=sys.stderr)
        return 1

    if report_dir:
        write_run_summary(
            report_dir / "run-summary.json",
            args=args,
            stats=stats,
            payload_count=len(payloads),
            upload_summary=upload_summary,
            manifest=manifest,
            exported_tables=exported_tables,
            reconciliation_summary=reconciliation_summary,
        )
        print(f"Wrote run summary to {report_dir / 'run-summary.json'}")

    if reconciliation_summary and int_or_zero(reconciliation_summary.get("failed_count")):
        return 1
    return 0


def run_customer_mode(args: argparse.Namespace) -> int:
    report_dir = Path(args.report_dir).expanduser() if args.report_dir else None
    upload_summary: Optional[Dict[str, Any]] = None
    shopify_csv_files: List[Dict[str, Any]] = []
    missing_customer_report: Optional[Dict[str, Any]] = None
    missing_shopify_csv_files: List[Dict[str, Any]] = []
    try:
        payloads, audits, stats = load_customers(args)
    except Exception as exc:
        print(f"Failed to build customer payloads: {exc}", file=sys.stderr)
        return 1

    print(f"Built {len(payloads)} customer payloads from {args.dbf_dir} (scope={args.customer_scope})")
    if args.dry_run:
        print("Customer read-only mode: no Shopify upload will run. Add --customers --upload --yes only after Shopify customer access is ready.")
    else:
        print("Customer upload mode: sending customers to Shopify. Requires read_customers/write_customers scopes and protected customer data access.")
    if stats.source_payloads:
        print("Customer source payloads:")
        for source, count in stats.source_payloads.items():
            print(f"  {source}: {count}")
    if stats.skipped_deleted:
        print(f"Skipped {stats.skipped_deleted} deleted customer rows")
    if stats.skipped_missing_identity:
        print(f"Skipped {stats.skipped_missing_identity} customers without usable name, company, email, or phone")
    if stats.skipped_duplicate:
        print(f"Skipped {stats.skipped_duplicate} duplicate customer identities")

    print_customer_preview(payloads, verbose=args.verbose_preview)

    if args.existing_customer_csv:
        existing_customer_csv = Path(args.existing_customer_csv).expanduser()
        try:
            existing_customer_index = build_existing_customer_csv_index(existing_customer_csv)
        except Exception as exc:
            print(f"Failed to read existing customer CSV: {exc}", file=sys.stderr)
            return 1

        missing_pairs = [
            (payload, audit)
            for payload, audit in zip(payloads, audits)
            if not customer_exists_in_csv(payload, existing_customer_index)
        ]
        missing_payloads = [payload for payload, _audit in missing_pairs]
        missing_audits = [audit for _payload, audit in missing_pairs]
        missing_audit_path = (
            Path(args.output_missing_customers_csv).expanduser()
            if args.output_missing_customers_csv
            else (report_dir / "missing-customers.csv" if report_dir else None)
        )

        if missing_audit_path:
            write_customer_audit(missing_audit_path, missing_audits)
            print(f"Wrote {len(missing_audits)} missing customer row(s) to {missing_audit_path}")
        else:
            print(f"Found {len(missing_audits)} customer row(s) missing from {existing_customer_csv}")

        if report_dir and not args.no_shopify_customer_csv:
            csv_dir = report_dir / "missing-shopify-customer-csv"
            max_bytes = max(1024 * 1024, int(args.shopify_customer_csv_max_mb * 1024 * 1024))
            missing_shopify_csv_files = write_shopify_customer_csvs(csv_dir, missing_payloads, max_bytes=max_bytes)
            print(f"Wrote {len(missing_shopify_csv_files)} missing-customer Shopify CSV file(s) to {csv_dir}")

        missing_customer_report = {
            "reference_csv": str(existing_customer_csv),
            "reference_rows": existing_customer_index.row_count,
            "reference_pos_ids": len(existing_customer_index.pos_customer_numbers),
            "reference_emails": len(existing_customer_index.emails),
            "reference_phones": len(existing_customer_index.phones),
            "generated_payloads": len(payloads),
            "missing_count": len(missing_audits),
            "missing_audit_csv": str(missing_audit_path) if missing_audit_path else None,
            "missing_shopify_customer_csv_files": missing_shopify_csv_files,
        }

    if report_dir:
        report_dir.mkdir(parents=True, exist_ok=True)
        write_payloads(report_dir / "customer-payload-preview.json", payloads[:100], pretty=True)
        write_customer_audit(report_dir / "customer-audit.csv", audits)
        if not args.no_shopify_customer_csv:
            csv_dir = Path(args.shopify_customer_csv_dir).expanduser() if args.shopify_customer_csv_dir else report_dir / "shopify-customer-csv"
            max_bytes = max(1024 * 1024, int(args.shopify_customer_csv_max_mb * 1024 * 1024))
            shopify_csv_files = write_shopify_customer_csvs(csv_dir, payloads, max_bytes=max_bytes)
            print(f"Wrote {len(shopify_csv_files)} Shopify customer CSV file(s) to {csv_dir}")
        write_customer_summary(
            report_dir / "customer-run-summary.json",
            args=args,
            stats=stats,
            payload_count=len(payloads),
            upload_summary=None,
            shopify_csv_files=shopify_csv_files,
            missing_customer_report=missing_customer_report,
        )
        print(f"Wrote customer reports to {report_dir}")
    elif args.shopify_customer_csv_dir and not args.no_shopify_customer_csv:
        csv_dir = Path(args.shopify_customer_csv_dir).expanduser()
        max_bytes = max(1024 * 1024, int(args.shopify_customer_csv_max_mb * 1024 * 1024))
        shopify_csv_files = write_shopify_customer_csvs(csv_dir, payloads, max_bytes=max_bytes)
        print(f"Wrote {len(shopify_csv_files)} Shopify customer CSV file(s) to {csv_dir}")

    if args.output_customers_json:
        output_path = Path(args.output_customers_json).expanduser()
        write_payloads(output_path, payloads, pretty=args.pretty)
        print(f"Wrote customer payloads to {output_path}")

    if args.output_customers_csv:
        audit_path = Path(args.output_customers_csv).expanduser()
        write_customer_audit(audit_path, audits)
        print(f"Wrote customer audit to {audit_path}")

    if args.dry_run:
        return 0

    if not payloads:
        print("No customer payloads matched the selected filters.")
        return 0

    if args.upload and not args.yes:
        print(
            "Refusing live customer upload without --yes. Run once in read-only mode first, then add --customers --upload --yes "
            "when you are ready to update Shopify customers.",
            file=sys.stderr,
        )
        return 1

    endpoint = resolve_endpoint(args)
    api_key = args.api_key or _env_any("SHOPIFY_SYNC_API_KEY", "POS_API_KEY", "Key")
    api_secret = args.api_secret or _env_any("SHOPIFY_SYNC_API_SECRET", "POS_API_SECRET", "Secret")
    if (not api_key or not api_secret) and not endpoint_has_query_auth(endpoint):
        print(
            "Missing API credentials. Pass --api-key/--api-secret, set "
            "SHOPIFY_SYNC_API_KEY and SHOPIFY_SYNC_API_SECRET, or use an --endpoint URL "
            "that already contains consumer_key and consumer_secret.",
            file=sys.stderr,
        )
        return 1

    try:
        upload_summary = upload_payloads(
            endpoint=endpoint,
            api_key=api_key,
            api_secret=api_secret,
            payloads=payloads,
            batch_size=max(1, args.batch_size),
            workers=max(1, args.workers),
            timeout=max(1, args.timeout),
            start_offset=max(0, args.start_offset),
            max_batches=args.max_batches,
            resume_file=Path(args.resume_file).expanduser() if args.resume_file else None,
            stop_on_failure=args.stop_on_failure,
        )
    except requests.HTTPError as exc:
        response = exc.response
        print(
            f"Customer upload failed with HTTP {response.status_code}: {response.text}",
            file=sys.stderr,
        )
        return 1
    except requests.ReadTimeout:
        print(
            f"Customer upload timed out after {args.timeout} seconds while waiting for the batch response. "
            "Try a smaller --batch-size such as 10 or a larger --timeout such as 300.",
            file=sys.stderr,
        )
        return 1
    except requests.RequestException as exc:
        print(f"Customer upload failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Customer upload failed: {exc}", file=sys.stderr)
        return 1

    if report_dir:
        write_customer_summary(
            report_dir / "customer-run-summary.json",
            args=args,
            stats=stats,
            payload_count=len(payloads),
            upload_summary=upload_summary,
            shopify_csv_files=shopify_csv_files,
            missing_customer_report=missing_customer_report,
        )
        print(f"Wrote customer run summary to {report_dir / 'customer-run-summary.json'}")

    return 0


def _env(name: str) -> Optional[str]:
    import os

    value = os.environ.get(name)
    return value.strip() if value and value.strip() else None


def _env_any(*names: str) -> Optional[str]:
    for name in names:
        value = _env(name)
        if value:
            return value
    return None


def load_env_files(explicit_path: Optional[str]) -> None:
    candidates = [Path(explicit_path).expanduser()] if explicit_path else [Path("jbarbaro_db/.env"), Path(".env")]
    for path in candidates:
        if path.exists():
            load_env_file(path)
            print(f"Loaded environment from {path}")
            return


def load_env_file(path: Path) -> None:
    import os

    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = strip_env_value(value.strip())
        if key and key not in os.environ:
            os.environ[key] = value


def strip_env_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


if __name__ == "__main__":
    raise SystemExit(main())
