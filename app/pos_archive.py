from __future__ import annotations

import html
import json
import math
import re
import shutil
import struct
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional


MAX_SAMPLE_ROWS = 25
MAX_TABLES_IN_SUMMARY = 250
MAX_UPLOAD_BYTES = 1024 * 1024 * 1024
MAX_ZIP_MEMBERS = 12000
MAX_UNZIPPED_BYTES = 6 * 1024 * 1024 * 1024
CORE_TABLE_NAMES = {
    "item",
    "itemmqty",
    "itemmrc",
    "prod_aux",
    "price",
    "pricechg",
    "vendor",
    "vendors",
    "vendprefs",
    "vendrange",
    "customer",
    "custship",
    "contacts",
    "notes",
    "invhdr",
    "invdtl",
    "minvhdr",
    "minvdtl",
    "ordhdr",
    "orddtl",
    "giftcert",
    "bridedat",
}
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
GROUP_PRODUCT_TYPE_MAP = {
    "ACC": "Accessories",
    "BEL": "Belts",
    "BOW": "Bow Ties",
    "BRA": "Braces",
    "DE": "Denim",
    "JERSEY": "Jersey",
    "MC": "Sport Coat",
    "MCP": "Casual Pants",
    "MCS": "Casual Shirts",
    "MDP": "Dress Pants",
    "MDS": "Dress Shirt",
    "MKS": "Knit Shirt",
    "MS": "Suit",
    "OU WE": "Outerwear",
    "PS": "Pocket Square",
    "SHO": "Shoes",
    "SOC": "Socks",
    "TIE": "Ties",
    "TUX": "Tuxedos",
    "UND": "Underwear",
    "VES": "Vest",
}


@dataclass(frozen=True)
class DBFField:
    name: str
    field_type: str
    length: int
    decimals: int


@dataclass(frozen=True)
class DBFTable:
    path: Path
    relative_path: str
    table_name: str
    record_count: int
    header_length: int
    record_length: int
    fields: List[DBFField]


def archive_storage_root(database_path: str, shop_domain: str) -> Path:
    safe_shop = re.sub(r"[^a-zA-Z0-9_.-]+", "_", shop_domain).strip("_") or "shop"
    return Path(database_path).expanduser().resolve().parent / "pos_archives" / safe_shop


def default_archive_root(storage_root: Path) -> Path:
    uploaded_root = storage_root / "current" / "ashpsdat"
    if uploaded_root.exists():
        return uploaded_root
    current_root = storage_root / "current"
    if current_root.exists():
        return current_root
    local_root = Path("ashpsdat")
    if local_root.exists():
        return local_root
    return current_root


def save_uploaded_archive(upload_file: Any, storage_root: Path) -> Path:
    storage_root.mkdir(parents=True, exist_ok=True)
    current_root = storage_root / "current"
    upload_path = storage_root / "ashpsdat.zip"
    if current_root.exists():
        shutil.rmtree(current_root)
    current_root.mkdir(parents=True, exist_ok=True)

    bytes_written = 0
    with upload_path.open("wb") as handle:
        while True:
            chunk = upload_file.file.read(1024 * 1024)
            if not chunk:
                break
            bytes_written += len(chunk)
            if bytes_written > MAX_UPLOAD_BYTES:
                raise ValueError("Archive upload is larger than the configured 1GB limit.")
            handle.write(chunk)

    extract_zip_safely(upload_path, current_root)
    extracted_ashpsdat = current_root / "ashpsdat"
    return extracted_ashpsdat if extracted_ashpsdat.exists() else current_root


def extract_zip_safely(zip_path: Path, destination: Path) -> None:
    destination = destination.resolve()
    with zipfile.ZipFile(zip_path) as archive:
        members = archive.infolist()
        if len(members) > MAX_ZIP_MEMBERS:
            raise ValueError(f"Archive contains too many files: {len(members)}")
        total_uncompressed = sum(member.file_size for member in members)
        if total_uncompressed > MAX_UNZIPPED_BYTES:
            raise ValueError("Archive expands beyond the configured 6GB limit.")
        for member in members:
            target = (destination / member.filename).resolve()
            if destination not in target.parents and target != destination:
                raise ValueError(f"Refusing unsafe zip path: {member.filename}")
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output)


def analyze_archive(root: Path, *, sample_tables: Optional[List[str]] = None) -> Dict[str, Any]:
    root = root.expanduser()
    tables = list_dbf_tables(root)
    category_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {"tables": 0, "records": 0})
    total_records = 0
    sensitive_tables = 0

    table_summaries: List[Dict[str, Any]] = []
    for table in tables:
        category = classify_table(table.table_name, table.fields)
        sensitive_fields = detect_sensitive_fields(table.fields)
        total_records += table.record_count
        category_counts[category]["tables"] += 1
        category_counts[category]["records"] += table.record_count
        if sensitive_fields:
            sensitive_tables += 1
        table_summaries.append(
            {
                "name": table.table_name,
                "path": table.relative_path,
                "category": category,
                "records": table.record_count,
                "record_length": table.record_length,
                "field_count": len(table.fields),
                "fields": [field.name for field in table.fields],
                "sensitive_fields": sensitive_fields,
            }
        )

    table_summaries.sort(key=lambda item: (item["records"], item["field_count"]), reverse=True)
    categories = [
        {"category": category, **counts}
        for category, counts in sorted(category_counts.items(), key=lambda item: item[0])
    ]

    selected_samples = sample_tables or [
        "Item",
        "Itemmqty",
        "Customer",
        "Vendor",
        "vendors",
        "invhdr",
        "invdtl",
        "Ordhdr",
        "Orddtl",
        "BrideDAT",
    ]

    samples = {
        table_name: sample_table(root, table_name, limit=3)
        for table_name in selected_samples
        if find_table(root, table_name) is not None
    }

    product_preview = build_product_payloads(root, limit=5, include_zero_quantity=False, rich=True)

    return {
        "archive_path": str(root),
        "table_count": len(tables),
        "total_records": total_records,
        "sensitive_table_count": sensitive_tables,
        "categories": categories,
        "tables": table_summaries[:MAX_TABLES_IN_SUMMARY],
        "core_tables": [item for item in table_summaries if item["name"].lower() in CORE_TABLE_NAMES],
        "samples": samples,
        "product_preview": product_preview,
        "notes": build_archive_notes(table_summaries),
    }


def list_dbf_tables(root: Path) -> List[DBFTable]:
    if not root.exists():
        return []

    tables: List[DBFTable] = []
    dbf_paths = [path for path in root.rglob("*") if path.is_file() and path.suffix.lower() == ".dbf"]
    for path in sorted(dbf_paths, key=lambda item: str(item).lower()):
        try:
            tables.append(read_dbf_header(path, root=root))
        except ValueError:
            continue
    return tables


def read_dbf_header(path: Path, *, root: Optional[Path] = None) -> DBFTable:
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

    relative_path = str(path.relative_to(root)) if root and path.is_relative_to(root) else str(path)
    return DBFTable(
        path=path,
        relative_path=relative_path,
        table_name=path.stem,
        record_count=record_count,
        header_length=header_length,
        record_length=record_length,
        fields=fields,
    )


def iter_dbf_rows(path: Path, *, encoding: str = "latin1", limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    table = read_dbf_header(path)
    with path.open("rb") as handle:
        handle.seek(table.header_length)
        emitted = 0
        for _ in range(table.record_count):
            record = handle.read(table.record_length)
            if not record:
                break
            if record[0] == 0x2A:
                continue

            row: Dict[str, Any] = {}
            offset = 1
            for field in table.fields:
                raw_value = record[offset : offset + field.length]
                offset += field.length
                row[field.name] = parse_dbf_value(raw_value, field, encoding=encoding)
            yield row
            emitted += 1
            if limit is not None and emitted >= limit:
                return


def parse_dbf_value(raw_value: bytes, field: DBFField, *, encoding: str) -> Any:
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
    return text or None


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
    if "invoice" in field_names or "invoice_no" in name or name.startswith("inv") or name.startswith("ord"):
        return "sales_orders"
    if "employee" in name or "emptime" in name or "closeout" in name:
        return "operations"
    if "bride" in name or "gift" in name or "rent" in name:
        return "special_programs"
    return "system_other"


def detect_sensitive_fields(fields: List[DBFField]) -> List[str]:
    sensitive: List[str] = []
    for field in fields:
        if any(pattern.search(field.name) for pattern in SENSITIVE_FIELD_PATTERNS):
            sensitive.append(field.name)
    return sensitive


def sample_table(root: Path, table_name: str, *, limit: int = 5) -> List[Dict[str, Any]]:
    table_path = find_table(root, table_name)
    if table_path is None:
        return []
    return [compact_row(row) for row in iter_dbf_rows(table_path, limit=max(1, min(limit, MAX_SAMPLE_ROWS)))]


def find_table(root: Path, table_name: str) -> Optional[Path]:
    normalized = table_name.lower().removesuffix(".dbf")
    matches = [
        path for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() == ".dbf" and path.stem.lower() == normalized
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda path: len(path.parts))[0]


def build_product_payloads(
    root: Path,
    *,
    limit: int = 25,
    offset: int = 0,
    include_zero_quantity: bool = False,
    rich: bool = True,
) -> List[Dict[str, Any]]:
    item_path = find_table(root, "Item")
    if item_path is None:
        return []

    vendor_lookup = build_vendor_lookup(root)
    vendor_item_lookup = build_vendor_item_lookup(root)
    quantity_lookup = build_quantity_lookup(root)
    payloads: List[Dict[str, Any]] = []
    matched_rows = 0

    for row in iter_dbf_rows(item_path):
        payload = build_product_payload(
            row,
            vendor_lookup=vendor_lookup,
            vendor_item_row=vendor_item_lookup.get(clean_text(row.get("SKU")) or "", {}),
            quantity_lookup=quantity_lookup,
            rich=rich,
        )
        if payload is None:
            continue
        if not include_zero_quantity and int(payload.get("quantity") or 0) <= 0:
            continue
        if matched_rows < offset:
            matched_rows += 1
            continue
        payloads.append(payload)
        matched_rows += 1
        if len(payloads) >= limit:
            break
    return payloads


def build_vendor_lookup(root: Path) -> Dict[str, str]:
    vendor_path = find_table(root, "Vendor")
    if vendor_path is None:
        return {}
    lookup: Dict[str, str] = {}
    for row in iter_dbf_rows(vendor_path):
        vendor_id = clean_text(row.get("VENDOR_ID"))
        company = clean_text(row.get("COMPANY"))
        if vendor_id and company:
            lookup[vendor_id] = title_case(company)
    return lookup


def build_vendor_item_lookup(root: Path) -> Dict[str, Dict[str, Any]]:
    vendors_path = find_table(root, "vendors")
    if vendors_path is None:
        return {}
    lookup: Dict[str, Dict[str, Any]] = {}
    for row in iter_dbf_rows(vendors_path):
        sku = clean_text(row.get("SKU"))
        if sku and sku not in lookup:
            lookup[sku] = row
    return lookup


def build_quantity_lookup(root: Path) -> Dict[str, Decimal]:
    quantity_path = find_table(root, "Itemmqty")
    if quantity_path is None:
        return {}
    quantities: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in iter_dbf_rows(quantity_path):
        sku = clean_text(row.get("SKU"))
        quantity = decimal_or_none(row.get("QTY"))
        if sku and quantity is not None:
            quantities[sku] += quantity
    return dict(quantities)


def build_product_payload(
    row: Dict[str, Any],
    *,
    vendor_lookup: Dict[str, str],
    vendor_item_row: Dict[str, Any],
    quantity_lookup: Dict[str, Decimal],
    rich: bool,
) -> Optional[Dict[str, Any]]:
    sku = clean_text(row.get("SKU"))
    raw_desc = clean_text(row.get("DESC"))
    if not sku or not raw_desc:
        return None

    vendor_code = clean_text(row.get("VENDOR")) or clean_text(vendor_item_row.get("VENDOR"))
    vendor_name = vendor_lookup.get(vendor_code or "") or title_case(clean_text(row.get("PFIELD2")) or vendor_code or "")
    department = clean_text(row.get("GROUP"))
    product_type = GROUP_PRODUCT_TYPE_MAP.get((department or "").upper()) or title_case(clean_text(row.get("PFIELD1")) or department or "")
    price = decimal_or_none(row.get("PRICE"))
    compare_at = choose_compare_at_price(row, price)
    cost = first_decimal(row.get("COST"), row.get("LAST_COST"), row.get("VEND_COST"), row.get("BASE_COST"), vendor_item_row.get("BASE_COST"))
    item_quantity = decimal_or_none(row.get("QTY"))
    itemmqty_quantity = quantity_lookup.get(sku)
    quantity = decimal_to_quantity(max([value for value in (item_quantity, itemmqty_quantity) if value is not None], default=Decimal("0")))
    barcode = clean_text(row.get("ALT_SKU")) or clean_text(row.get("BARCODE"))
    title = build_title(raw_desc=raw_desc, vendor_name=vendor_name, product_type=product_type, size=clean_text(row.get("SIZE")), color=clean_text(row.get("COLOR")))

    payload: Dict[str, Any] = {
        "sku": sku,
        "title": title,
        "vendor": vendor_name,
        "brand": vendor_name,
        "product_type": product_type,
        "barcode": barcode,
        "price": decimal_to_price(price),
        "compare_at_price": decimal_to_price(compare_at),
        "cost": decimal_to_price(cost),
        "quantity": quantity,
        "qty": quantity,
        "stock_quantity": quantity,
        "tracked": True,
        "requires_shipping": True,
    }
    if rich:
        payload["description_html"] = build_description_html(row, title=title, vendor_name=vendor_name)
        payload["tags"] = build_tags(row, product_type=product_type, vendor_code=vendor_code)
        payload["metafields"] = build_metafields(
            row,
            vendor_item_row=vendor_item_row,
            vendor_name=vendor_name,
            vendor_code=vendor_code,
            item_quantity=item_quantity,
            itemmqty_quantity=itemmqty_quantity,
        )
    return prune_empty(payload)


def choose_compare_at_price(row: Dict[str, Any], price: Optional[Decimal]) -> Optional[Decimal]:
    if price is None or price <= 0:
        return None
    candidates = []
    for field_name in ("PRICE_R", "PRICE_B", "PRICE_C", "PRICE_D", "PRICE_E", "PRICE_W"):
        value = decimal_or_none(row.get(field_name))
        if value is not None and price < value <= price * Decimal("3"):
            candidates.append(value)
    return max(candidates) if candidates else None


def first_decimal(*values: Any) -> Optional[Decimal]:
    for value in values:
        decimal_value = decimal_or_none(value)
        if decimal_value is not None:
            return decimal_value
    return None


def build_title(*, raw_desc: str, vendor_name: Optional[str], product_type: Optional[str], size: Optional[str], color: Optional[str]) -> str:
    title = title_case(raw_desc)
    if vendor_name and vendor_name.lower() not in title.lower():
        title = f"{vendor_name} {title}"
    extras = [title_case(value) for value in (color, size) if value]
    if extras:
        title = f"{title} - {' / '.join(extras)}"
    return normalize_spaces(title)


def build_description_html(row: Dict[str, Any], *, title: str, vendor_name: Optional[str]) -> str:
    details = [
        ("SKU", row.get("SKU")),
        ("POS name", row.get("DESC")),
        ("Vendor", vendor_name),
        ("Department", row.get("GROUP")),
        ("Style", row.get("STYLE")),
        ("Size", row.get("SIZE")),
        ("Color", row.get("COLOR")),
        ("Vendor Item", row.get("VEND_ID")),
        ("Last Ordered", row.get("LAST_ORD")),
        ("Last Activity", row.get("LAST_ACT")),
    ]
    items = [
        f"<li><strong>{html.escape(label)}:</strong> {html.escape(str(clean_text(value)))}</li>"
        for label, value in details
        if clean_text(value)
    ]
    return f"<p>{html.escape(title)}</p>" + ("<ul>" + "".join(items) + "</ul>" if items else "")


def build_tags(row: Dict[str, Any], *, product_type: Optional[str], vendor_code: Optional[str]) -> List[str]:
    tags = []
    for label, value in (
        ("Department", row.get("GROUP")),
        ("Product Type", product_type),
        ("Style", row.get("STYLE")),
        ("Size", row.get("SIZE")),
        ("Color", row.get("COLOR")),
        ("Vendor Code", vendor_code),
        ("Vendor Item", row.get("VEND_ID")),
        ("PField1", row.get("PFIELD1")),
        ("PField2", row.get("PFIELD2")),
        ("PField3", row.get("PFIELD3")),
        ("PField4", row.get("PFIELD4")),
        ("PField5", row.get("PFIELD5")),
    ):
        text = clean_text(value)
        if text:
            tags.append(f"{label}:{text}")
    return list(dict.fromkeys(tags))


def build_metafields(
    row: Dict[str, Any],
    *,
    vendor_item_row: Dict[str, Any],
    vendor_name: Optional[str],
    vendor_code: Optional[str],
    item_quantity: Optional[Decimal],
    itemmqty_quantity: Optional[Decimal],
) -> List[Dict[str, Any]]:
    values = {
        "sku": row.get("SKU"),
        "raw_description": row.get("DESC"),
        "vendor_name": vendor_name,
        "vendor_code": vendor_code,
        "department_code": row.get("GROUP"),
        "style": row.get("STYLE"),
        "size": row.get("SIZE"),
        "color": row.get("COLOR"),
        "vendor_item": row.get("VEND_ID"),
        "pos_type": row.get("TYPE"),
        "sellweb": row.get("SELLWEB"),
        "web_product_id": row.get("WEBPRODID"),
        "web_variant_id": row.get("VARIANTID"),
        "cost": row.get("COST"),
        "last_cost": row.get("LAST_COST"),
        "vendor_cost": row.get("VEND_COST"),
        "base_cost": row.get("BASE_COST"),
        "item_quantity": decimal_to_quantity(item_quantity),
        "itemmqty_quantity": decimal_to_quantity(itemmqty_quantity),
        "item_data": compact_row(row),
        "vendor_item_data": compact_row(vendor_item_row),
    }

    metafields = []
    for key, value in values.items():
        prepared = metafield("pos", key, value)
        if prepared:
            metafields.append(prepared)
    return metafields


def metafield(namespace: str, key: str, value: Any) -> Optional[Dict[str, Any]]:
    if value in (None, "", {}, []):
        return None
    if isinstance(value, dict):
        return {"namespace": namespace, "key": key, "value": json.dumps(value, default=str, ensure_ascii=True, sort_keys=True), "type": "json"}
    if isinstance(value, bool):
        return {"namespace": namespace, "key": key, "value": "true" if value else "false", "type": "boolean"}
    if isinstance(value, Decimal):
        return {"namespace": namespace, "key": key, "value": format_decimal(value), "type": "number_decimal"}
    if isinstance(value, int):
        return {"namespace": namespace, "key": key, "value": str(value), "type": "number_integer"}
    text = clean_text(value)
    if not text or "\x00" in text:
        return None
    field_type = "multi_line_text_field" if len(text) > 120 else "single_line_text_field"
    return {"namespace": namespace, "key": key, "value": text, "type": field_type}


def build_archive_notes(table_summaries: List[Dict[str, Any]]) -> List[str]:
    notes = [
        "Products can be synced to Shopify from Item.dbf with Itemmqty.dbf quantities and Vendor.dbf name lookups.",
        "Customer, payment, and contact tables include private data; keep those as archive/search data unless Shopify customer scopes and consent rules are added.",
        "Invoice/order tables can be used for reporting and purchase history but should not be recreated as Shopify orders without a separate migration plan.",
    ]
    if any(table["name"].lower() == "merc_trans" for table in table_summaries):
        notes.append("Merc_trans contains payment references and must never be sent to product metafields or public exports.")
    return notes


def compact_row(row: Dict[str, Any]) -> Dict[str, Any]:
    compact: Dict[str, Any] = {}
    for key, value in row.items():
        normalized = compact_value(value)
        if normalized is not None:
            compact[key] = normalized
    return compact


def compact_value(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        if value == 0:
            return None
        return format(value, "f")
    if isinstance(value, bool):
        return value if value else None
    if isinstance(value, str):
        text = clean_text(value)
        if not text or "\x00" in text:
            return None
        return text
    return value


def prune_empty(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if value not in (None, "", [], {})
    }


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = normalize_spaces(str(value))
    return text or None


def normalize_spaces(value: str) -> str:
    return " ".join(str(value).strip().split())


def title_case(value: Optional[str]) -> Optional[str]:
    text = clean_text(value)
    if not text:
        return None
    return " ".join(part[:1].upper() + part[1:].lower() if not any(char.isdigit() for char in part) else part.upper() for part in text.split())


def decimal_or_none(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def decimal_to_price(value: Optional[Decimal]) -> Optional[float]:
    if value is None:
        return None
    return float(value.quantize(Decimal("0.01")))


def decimal_to_quantity(value: Optional[Decimal]) -> int:
    if value is None:
        return 0
    return max(0, math.floor(value))


def format_decimal(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    return f"{value.quantize(Decimal('0.01'))}"
