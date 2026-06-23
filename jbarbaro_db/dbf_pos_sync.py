#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
import math
import re
import struct
import sys
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional
from urllib.parse import parse_qs, urlparse, urlunparse

import requests


DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_BATCH_SIZE = 25
DEFAULT_METAFIELD_NAMESPACE = "pos"
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
class BuildStats:
    total_rows_seen: int
    skipped_zero_price: int
    skipped_zero_quantity: int
    skipped_non_sellable: int
    skipped_missing_payload: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read DBF exports from a host computer and push them to the Shopify sync service."
    )
    parser.add_argument(
        "--dbf-dir",
        required=True,
        help="Directory containing Item.dbf and the related POS export files.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search nested folders for the expected DBF filenames.",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="App base URL, for example https://shopify-sync.example.com",
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
        help="POS API key. Can also come from SHOPIFY_SYNC_API_KEY.",
    )
    parser.add_argument(
        "--api-secret",
        default=None,
        help="POS API secret. Can also come from SHOPIFY_SYNC_API_SECRET.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Products per request. Default: {DEFAULT_BATCH_SIZE}",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"HTTP timeout in seconds. Default: {DEFAULT_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--status",
        choices=("draft", "active"),
        default=None,
        help="Force a Shopify product status. Omit for the safest repeated sync behavior.",
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
    args = parser.parse_args()
    if args.rich:
        args.include_html_description = True
        args.include_tags = True
        args.include_metafields = True
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
    candidates = dbf_dir.rglob("*.dbf") if recursive else dbf_dir.glob("*.dbf")
    matches = sorted(path for path in candidates if path.name.lower() == expected_name)
    if not matches:
        return None
    if len(matches) > 1:
        match_list = ", ".join(str(path) for path in matches[:10])
        raise ValueError(f"Found multiple {filename} files: {match_list}")
    return matches[0]


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
    sku_filter = {sku.strip() for sku in args.sku if sku and sku.strip()}

    prepared: List[PreparedProduct] = []
    total_rows_seen = 0
    skipped_zero_price = 0
    skipped_zero_quantity = 0
    skipped_non_sellable = 0
    skipped_missing_payload = 0
    for row in iter_dbf_rows(item_path):
        total_rows_seen += 1
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
    return prepared, BuildStats(
        total_rows_seen=total_rows_seen,
        skipped_zero_price=skipped_zero_price,
        skipped_zero_quantity=skipped_zero_quantity,
        skipped_non_sellable=skipped_non_sellable,
        skipped_missing_payload=skipped_missing_payload,
    )


def build_product(
    row: Dict[str, Any],
    *,
    args: argparse.Namespace,
    vendor_lookup: Dict[str, str],
    price_change_row: Dict[str, Any],
    vendor_item_row: Dict[str, Any],
    quantity_lookup: Dict[str, Decimal],
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
    quantity = select_quantity(row, quantity_lookup=quantity_lookup, quantity_source=args.quantity_source)
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
        "tracked": True,
        "requires_shipping": True,
        "images": images,
    }
    if quantity is not None:
        payload["quantity"] = quantity
        payload["qty"] = quantity
        payload["stock_quantity"] = quantity
    if args.status:
        payload["status"] = args.status
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
        "quantity": quantity if quantity is not None else "",
        "tag_count": len(payload.get("tags") or []),
        "metafield_count": len(payload.get("metafields") or []),
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
    if args.endpoint:
        endpoint = args.endpoint.strip()
    else:
        base_url = args.base_url or _env("SHOPIFY_SYNC_BASE_URL")
        if not base_url:
            raise ValueError("Pass --base-url or --endpoint.")
        endpoint = f"{base_url.rstrip('/')}/wc-api/v3/products/batch"
    return normalize_batch_endpoint(endpoint)


def normalize_batch_endpoint(endpoint: str) -> str:
    parsed = urlparse(endpoint)
    path = parsed.path.rstrip("/")
    if path.endswith("/wc-api/v3/products") or path.endswith("/wp-json/wc/v3/products"):
        path = f"{path}/batch"
    normalized = parsed._replace(path=path)
    return urlunparse(normalized)


def endpoint_has_query_auth(endpoint: str) -> bool:
    query = parse_qs(urlparse(endpoint).query)
    return bool(query.get("consumer_key") and query.get("consumer_secret"))


def write_payloads(path: Path, payloads: List[Dict[str, Any]], *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payloads, handle, indent=2 if pretty else None, ensure_ascii=True)
        handle.write("\n")


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
        "quantity",
        "tag_count",
        "metafield_count",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for audit in audits:
            writer.writerow(audit)


def upload_payloads(
    *,
    endpoint: str,
    api_key: Optional[str],
    api_secret: Optional[str],
    payloads: List[Dict[str, Any]],
    batch_size: int,
    timeout: int,
) -> None:
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

    total = len(payloads)
    for start in range(0, total, batch_size):
        chunk = payloads[start : start + batch_size]
        print(
            f"Uploading batch {start // batch_size + 1}: "
            f"{len(chunk)} rows (timeout={timeout}s)"
        )
        response = session.post(endpoint, json=chunk, timeout=timeout)
        response.raise_for_status()
        body = response.json()
        succeeded = body.get("succeeded")
        failed = body.get("failed")
        print(
            f"Uploaded batch {start // batch_size + 1}: "
            f"{len(chunk)} rows, succeeded={succeeded}, failed={failed}"
        )
        if failed:
            print(json.dumps(body, indent=2, ensure_ascii=True))


def main() -> int:
    args = parse_args()
    try:
        prepared_products, stats = load_products(args)
    except Exception as exc:
        print(f"Failed to build payloads: {exc}", file=sys.stderr)
        return 1

    payloads = [item.payload for item in prepared_products]
    audits = [item.audit for item in prepared_products]
    print(f"Built {len(payloads)} product payloads from {args.dbf_dir}")
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

    if payloads:
        preview = json.dumps(payloads[:2], indent=2, ensure_ascii=True)
        print(preview)

    if args.output_json:
        output_path = Path(args.output_json).expanduser()
        write_payloads(output_path, payloads, pretty=args.pretty)
        print(f"Wrote payloads to {output_path}")

    if args.output_title_audit:
        audit_path = Path(args.output_title_audit).expanduser()
        write_title_audit(audit_path, audits)
        print(f"Wrote title audit to {audit_path}")

    if args.dry_run:
        return 0

    if not payloads:
        print("No payloads matched the selected filters.")
        return 0

    endpoint = resolve_endpoint(args)
    api_key = args.api_key or _env("SHOPIFY_SYNC_API_KEY")
    api_secret = args.api_secret or _env("SHOPIFY_SYNC_API_SECRET")
    if (not api_key or not api_secret) and not endpoint_has_query_auth(endpoint):
        print(
            "Missing API credentials. Pass --api-key/--api-secret, set "
            "SHOPIFY_SYNC_API_KEY and SHOPIFY_SYNC_API_SECRET, or use an --endpoint URL "
            "that already contains consumer_key and consumer_secret.",
            file=sys.stderr,
        )
        return 1

    try:
        upload_payloads(
            endpoint=endpoint,
            api_key=api_key,
            api_secret=api_secret,
            payloads=payloads,
            batch_size=max(1, args.batch_size),
            timeout=max(1, args.timeout),
        )
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

    return 0


def _env(name: str) -> Optional[str]:
    import os

    value = os.environ.get(name)
    return value.strip() if value and value.strip() else None


if __name__ == "__main__":
    raise SystemExit(main())
