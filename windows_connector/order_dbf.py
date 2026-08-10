from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import struct
import tempfile
import uuid
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple


DBF_VERSION = 0x03
DBF_ENCODING = "cp1252"
DBF_LANGUAGE_DRIVER = 0x03


@dataclass(frozen=True)
class DbfField:
    name: str
    field_type: str
    length: int
    decimals: int = 0


HEADER_FIELDS: Tuple[DbfField, ...] = (
    DbfField("GEN_ID", "C", 32),
    DbfField("ORDER_ID", "C", 24),
    DbfField("INVOICE_NO", "C", 32),
    DbfField("ORDER_NUM", "C", 24),
    DbfField("CONFIRM_NO", "C", 64),
    DbfField("ORDER_AT", "C", 35),
    DbfField("UPDATED_AT", "C", 35),
    DbfField("PROC_AT", "C", 35),
    DbfField("CANCEL_AT", "C", 35),
    DbfField("CLOSED_AT", "C", 35),
    DbfField("FIN_STATUS", "C", 24),
    DbfField("FUL_STATUS", "C", 24),
    DbfField("CURRENCY", "C", 3),
    DbfField("CUST_NAME", "C", 80),
    DbfField("CUST_FIRST", "C", 40),
    DbfField("CUST_LAST", "C", 40),
    DbfField("EMAIL", "C", 254),
    DbfField("PHONE", "C", 30),
    DbfField("BILL_NAME", "C", 80),
    DbfField("BILL_FIRST", "C", 40),
    DbfField("BILL_LAST", "C", 40),
    DbfField("BILL_COMP", "C", 80),
    DbfField("BILL_ADR1", "C", 100),
    DbfField("BILL_ADR2", "C", 100),
    DbfField("BILL_CITY", "C", 50),
    DbfField("BILL_PROV", "C", 40),
    DbfField("BILL_PCODE", "C", 10),
    DbfField("BILL_CTRY", "C", 40),
    DbfField("BILL_CCODE", "C", 2),
    DbfField("BILL_ZIP", "C", 20),
    DbfField("BILL_PHONE", "C", 30),
    DbfField("SHIP_NAME", "C", 80),
    DbfField("SHIP_FIRST", "C", 40),
    DbfField("SHIP_LAST", "C", 40),
    DbfField("SHIP_COMP", "C", 80),
    DbfField("SHIP_ADR1", "C", 100),
    DbfField("SHIP_ADR2", "C", 100),
    DbfField("SHIP_CITY", "C", 50),
    DbfField("SHIP_PROV", "C", 40),
    DbfField("SHIP_PCODE", "C", 10),
    DbfField("SHIP_CTRY", "C", 40),
    DbfField("SHIP_CCODE", "C", 2),
    DbfField("SHIP_ZIP", "C", 20),
    DbfField("SHIP_PHONE", "C", 30),
    DbfField("SHIP_METH", "C", 80),
    DbfField("SUBTOTAL", "N", 18, 2),
    DbfField("DISCOUNT", "N", 18, 2),
    DbfField("SHIPPING", "N", 18, 2),
    DbfField("HANDLING", "N", 18, 2),
    DbfField("TAX", "N", 18, 2),
    DbfField("TOTAL", "N", 18, 2),
    DbfField("NOTE1", "C", 254),
    DbfField("NOTE2", "C", 254),
    DbfField("TAGS", "C", 200),
    DbfField("PRINT_ST", "C", 12),
    DbfField("PRINTED_AT", "C", 35),
    DbfField("IMPORT_ST", "C", 12),
    DbfField("IMPORTEDAT", "C", 35),
    DbfField("POS_ORD_NO", "C", 32),
    DbfField("IMP_ERROR", "C", 200),
    DbfField("SRC_EVENT", "C", 24),
    DbfField("SRC_VER", "N", 10, 0),
    DbfField("SYNCED_AT", "C", 35),
    DbfField("LINE_COUNT", "N", 6, 0),
    DbfField("TRUNCATED", "L", 1),
)

DETAIL_FIELDS: Tuple[DbfField, ...] = (
    DbfField("GEN_ID", "C", 32),
    DbfField("ORDER_ID", "C", 24),
    DbfField("INVOICE_NO", "C", 32),
    DbfField("LINE_NO", "N", 6, 0),
    DbfField("LINE_KEY", "C", 32),
    DbfField("LINE_ID", "C", 24),
    DbfField("PRODUCT_ID", "C", 24),
    DbfField("VARIANT_ID", "C", 24),
    DbfField("SKU", "C", 64),
    DbfField("QTY", "N", 10, 0),
    DbfField("CURR_QTY", "N", 10, 0),
    DbfField("UNIT_PRICE", "N", 18, 2),
    DbfField("DISCOUNT", "N", 18, 2),
    DbfField("TAX", "N", 18, 2),
    DbfField("EXTENSION", "N", 18, 2),
    DbfField("DESCRIPT", "C", 254),
    DbfField("VAR_TITLE", "C", 254),
    DbfField("VENDOR", "C", 100),
    DbfField("GRAMS", "N", 12, 0),
    DbfField("REQ_SHIP", "L", 1),
    DbfField("FUL_STATUS", "C", 24),
    DbfField("SRC_VER", "N", 10, 0),
    DbfField("TRUNCATED", "L", 1),
)

POS_MANAGED_HEADER_FIELDS = (
    "PRINT_ST",
    "PRINTED_AT",
    "IMPORT_ST",
    "IMPORTEDAT",
    "POS_ORD_NO",
    "IMP_ERROR",
)


def _validate_schema(schema: Sequence[DbfField]) -> None:
    names = [field.name for field in schema]
    if len(names) != len(set(names)):
        raise ValueError("DBF field names must be unique")
    for field in schema:
        if not field.name or len(field.name) > 10 or not field.name.isascii():
            raise ValueError(f"Invalid dBASE III field name: {field.name!r}")
        if field.field_type not in {"C", "N", "L", "D"}:
            raise ValueError(f"Unsupported DBF field type: {field.field_type}")
        if not 1 <= field.length <= 254:
            raise ValueError(f"Invalid DBF field width for {field.name}: {field.length}")
        if field.field_type == "L" and field.length != 1:
            raise ValueError(f"Logical DBF field {field.name} must have width 1")
        if field.field_type == "D" and field.length != 8:
            raise ValueError(f"Date DBF field {field.name} must have width 8")
        if field.field_type == "N" and field.length > 19:
            raise ValueError(f"Numeric DBF field {field.name} exceeds dBASE III width 19")
        if field.field_type == "N" and not 0 <= field.decimals < field.length:
            raise ValueError(f"Invalid DBF decimal count for {field.name}")
        if field.field_type != "N" and field.decimals:
            raise ValueError(f"Only numeric DBF fields may declare decimals: {field.name}")
    record_length = 1 + sum(field.length for field in schema)
    header_length = 33 + (32 * len(schema))
    if len(schema) > 128 or record_length > 4000 or header_length > 65535:
        raise ValueError("DBF schema exceeds dBASE III limits")


_validate_schema(HEADER_FIELDS)
_validate_schema(DETAIL_FIELDS)


def money_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return f"{Decimal(str(value)).quantize(Decimal('0.01'))}"
    except (InvalidOperation, ValueError):
        return str(value)


def _numeric_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)


def _parse_dbf_value(raw_value: bytes, field: DbfField) -> Any:
    decoded = raw_value.decode(DBF_ENCODING, "replace")
    if field.field_type == "C":
        return decoded.rstrip(" ") or None
    text = decoded.strip()
    if field.field_type == "N":
        if not text:
            return None
        numeric_pattern = r"-?\d+" if field.decimals == 0 else rf"-?\d+(?:\.\d{{1,{field.decimals}}})?"
        if re.fullmatch(numeric_pattern, text) is None:
            raise ValueError(f"Invalid numeric value in DBF field {field.name}: {text!r}")
        try:
            number = Decimal(text)
        except InvalidOperation:
            raise ValueError(f"Invalid numeric value in DBF field {field.name}: {text!r}")
        if not number.is_finite():
            raise ValueError(f"Invalid numeric value in DBF field {field.name}: {text!r}")
        return number
    if field.field_type == "L":
        if not text or text == "?":
            return None
        if text.upper() in {"T", "Y"}:
            return True
        if text.upper() in {"F", "N"}:
            return False
        raise ValueError(f"Invalid logical value in DBF field {field.name}: {text!r}")
    if field.field_type == "D":
        if not text:
            return None
        if len(text) != 8 or not text.isdigit():
            raise ValueError(f"Invalid date value in DBF field {field.name}: {text!r}")
        try:
            return datetime.strptime(text, "%Y%m%d").date().isoformat()
        except ValueError:
            raise ValueError(f"Invalid date value in DBF field {field.name}: {text!r}")
    return text or None


def _read_dbf_table(path: Path, expected_schema: Sequence[DbfField]) -> List[Dict[str, Any]]:
    with path.open("rb") as handle:
        header = handle.read(32)
        if len(header) != 32 or header[0] != DBF_VERSION:
            raise ValueError(f"{path} is not a supported dBASE III file")
        record_count = struct.unpack("<I", header[4:8])[0]
        header_length = struct.unpack("<H", header[8:10])[0]
        record_length = struct.unpack("<H", header[10:12])[0]
        actual_schema: List[DbfField] = []
        while True:
            descriptor = handle.read(32)
            if not descriptor:
                raise ValueError(f"{path} ended before the DBF field list was complete")
            if descriptor[0] == 0x0D:
                break
            if len(descriptor) != 32:
                raise ValueError(f"{path} ended in the middle of a DBF field descriptor")
            name = descriptor[:11].split(b"\x00", 1)[0].decode("ascii", "strict")
            actual_schema.append(
                DbfField(name, chr(descriptor[11]), descriptor[16], descriptor[17])
            )
        if tuple(actual_schema) != tuple(expected_schema):
            raise ValueError(f"{path} does not have the expected Shopify order DBF schema")
        expected_header_length = 33 + (32 * len(expected_schema))
        expected_record_length = 1 + sum(field.length for field in expected_schema)
        if header_length != expected_header_length or record_length != expected_record_length:
            raise ValueError(f"{path} has inconsistent DBF header lengths")
        handle.seek(header_length)
        rows: List[Dict[str, Any]] = []
        for _ in range(record_count):
            record = handle.read(record_length)
            if len(record) != record_length:
                raise ValueError(f"{path} ended in the middle of a DBF record")
            if record[0] == 0x2A:
                continue
            if record[0] != 0x20:
                raise ValueError(f"{path} contains an invalid DBF record marker")
            row: Dict[str, Any] = {}
            offset = 1
            for field in expected_schema:
                raw_value = record[offset : offset + field.length]
                offset += field.length
                row[field.name] = _parse_dbf_value(raw_value, field)
            rows.append(row)
        if handle.read(1) != b"\x1a" or handle.read(1):
            raise ValueError(f"{path} has an invalid DBF record count or end marker")
        return rows


def _encode_character(value: Any, length: int) -> Tuple[bytes, bool]:
    text = "" if value is None else str(value)
    replaced = False
    try:
        encoded = text.encode(DBF_ENCODING, "strict")
    except UnicodeEncodeError:
        encoded = text.encode(DBF_ENCODING, "replace")
        replaced = True
    truncated = len(encoded) > length
    return encoded[:length].ljust(length, b" "), replaced or truncated


def _encode_numeric(value: Any, field: DbfField) -> Tuple[bytes, bool]:
    if value in (None, ""):
        return b" " * field.length, False
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return b" " * field.length, True
    if not number.is_finite():
        return b" " * field.length, True
    original_number = number
    if field.decimals:
        number = number.quantize(Decimal(1).scaleb(-field.decimals))
        text = f"{number:.{field.decimals}f}"
    else:
        number = number.quantize(Decimal("1"))
        text = f"{number:.0f}"
    encoded = text.encode("ascii")
    if len(encoded) > field.length:
        raise ValueError(f"Numeric value {text} exceeds DBF field {field.name}")
    return encoded.rjust(field.length, b" "), number != original_number


def _encode_date(value: Any, field: DbfField) -> Tuple[bytes, bool]:
    if value in (None, ""):
        return b" " * field.length, False
    if isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    else:
        text = str(value).strip()
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            return b" " * field.length, True
    return parsed.strftime("%Y%m%d").encode("ascii"), False


def _encode_field(value: Any, field: DbfField) -> Tuple[bytes, bool]:
    if field.field_type == "C":
        return _encode_character(value, field.length)
    if field.field_type == "N":
        return _encode_numeric(value, field)
    if field.field_type == "D":
        return _encode_date(value, field)
    if value is None:
        return b"?", False
    if isinstance(value, bool):
        return (b"T" if value else b"F"), False
    if isinstance(value, int) and value in {0, 1}:
        return (b"T" if value else b"F"), False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"", "?"}:
            return b"?", False
        if normalized in {"true", "t", "yes", "y", "1"}:
            return b"T", False
        if normalized in {"false", "f", "no", "n", "0"}:
            return b"F", False
    return b"?", True


def _serialize_dbf(schema: Sequence[DbfField], rows: Iterable[Dict[str, Any]]) -> bytes:
    row_list = [dict(row) for row in rows]
    today = date.today()
    header_length = 33 + (32 * len(schema))
    record_length = 1 + sum(field.length for field in schema)
    header = bytearray(32)
    header[0] = DBF_VERSION
    header[1:4] = bytes((today.year - 1900, today.month, today.day))
    header[4:8] = struct.pack("<I", len(row_list))
    header[8:10] = struct.pack("<H", header_length)
    header[10:12] = struct.pack("<H", record_length)
    header[29] = DBF_LANGUAGE_DRIVER
    output = bytearray(header)
    for field in schema:
        descriptor = bytearray(32)
        encoded_name = field.name.encode("ascii")
        descriptor[: len(encoded_name)] = encoded_name
        descriptor[11] = ord(field.field_type)
        descriptor[16] = field.length
        descriptor[17] = field.decimals
        output.extend(descriptor)
    output.append(0x0D)
    for row in row_list:
        encoded_values: Dict[str, bytes] = {}
        truncated = bool(row.get("TRUNCATED"))
        for field in schema:
            if field.name == "TRUNCATED":
                continue
            encoded, field_truncated = _encode_field(row.get(field.name), field)
            encoded_values[field.name] = encoded
            truncated = truncated or field_truncated
        output.append(0x20)
        for field in schema:
            if field.name == "TRUNCATED":
                output.extend(b"T" if truncated else b"F")
            else:
                output.extend(encoded_values[field.name])
    output.append(0x1A)
    return bytes(output)


def _write_temp_file(path: Path, content: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return temporary_path


def _replace_with_content(path: Path, content: bytes) -> None:
    temporary_path = _write_temp_file(path, content)
    try:
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _publish_paths(header_path: Path, detail_path: Path) -> Tuple[Path, Path, Path]:
    if header_path.parent != detail_path.parent:
        raise ValueError("Shopify order header and detail DBFs must use the same directory")
    token = f".{header_path.stem}-{detail_path.stem}"
    return (
        header_path.with_suffix(header_path.suffix + ".previous"),
        detail_path.with_suffix(detail_path.suffix + ".previous"),
        header_path.parent / f"{token}.update.json",
    )


def _remove_orphan_publish_temps(header_path: Path, detail_path: Path) -> None:
    previous_header, previous_detail, journal_path = _publish_paths(header_path, detail_path)
    targets = (header_path, detail_path, previous_header, previous_detail, journal_path)
    for target in targets:
        prefix = f".{target.name}."
        for candidate in target.parent.iterdir():
            if candidate.name.startswith(prefix) and candidate.name.endswith(".tmp"):
                candidate.unlink(missing_ok=True)


@contextmanager
def order_dbf_lock(header_path: Path, detail_path: Path) -> Iterator[Path]:
    _publish_paths(header_path, detail_path)
    lock_path = header_path.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\x00")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield lock_path
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield lock_path
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _recover_interrupted_publish(header_path: Path, detail_path: Path) -> None:
    previous_header, previous_detail, journal_path = _publish_paths(header_path, detail_path)
    _remove_orphan_publish_temps(header_path, detail_path)
    if not journal_path.exists():
        # A process can stop after removing the journal but before removing old
        # recovery files. They are no longer needed once no publish is pending.
        previous_header.unlink(missing_ok=True)
        previous_detail.unlink(missing_ok=True)
        return
    recovered = False
    try:
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        if header_path.exists() and detail_path.exists():
            try:
                current_headers = _read_dbf_table(header_path, HEADER_FIELDS)
                current_details = _read_dbf_table(detail_path, DETAIL_FIELDS)
                _validate_rows(current_headers, current_details)
                current_generations = {
                    str(row.get("GEN_ID") or "")
                    for row in current_headers + current_details
                }
                generation_matches = current_generations == {str(journal.get("generation") or "")}
                empty_snapshot_matches = (
                    not current_headers
                    and not current_details
                    and hashlib.sha256(header_path.read_bytes()).hexdigest()
                    == journal.get("header_sha256")
                    and hashlib.sha256(detail_path.read_bytes()).hexdigest()
                    == journal.get("detail_sha256")
                )
                if generation_matches or empty_snapshot_matches:
                    recovered = True
                    return
            except (OSError, ValueError):
                pass
        previous_header_hash = journal.get("previous_header_sha256")
        if previous_header_hash is None and previous_header.exists():
            previous_header_hash = hashlib.sha256(previous_header.read_bytes()).hexdigest()
        previous_detail_hash = journal.get("previous_detail_sha256")
        if previous_detail_hash is None and previous_detail.exists():
            previous_detail_hash = hashlib.sha256(previous_detail.read_bytes()).hexdigest()
        header_already_restored = (
            header_path.is_file()
            and bool(journal.get("header_existed"))
            and bool(previous_header_hash)
            and hashlib.sha256(header_path.read_bytes()).hexdigest() == previous_header_hash
        ) or (not bool(journal.get("header_existed")) and not header_path.exists())
        detail_already_restored = (
            detail_path.is_file()
            and bool(journal.get("detail_existed"))
            and bool(previous_detail_hash)
            and hashlib.sha256(detail_path.read_bytes()).hexdigest() == previous_detail_hash
        ) or (not bool(journal.get("detail_existed")) and not detail_path.exists())
        if header_already_restored and detail_already_restored:
            recovered = True
            return
        for path, previous, existed_key in (
            (header_path, previous_header, "header_existed"),
            (detail_path, previous_detail, "detail_existed"),
        ):
            if bool(journal.get(existed_key)):
                if not previous.exists():
                    raise ValueError(f"Missing DBF recovery file: {previous}")
                _replace_with_content(path, previous.read_bytes())
            else:
                path.unlink(missing_ok=True)
                previous.unlink(missing_ok=True)
        recovered = True
    finally:
        if recovered:
            previous_header.unlink(missing_ok=True)
            previous_detail.unlink(missing_ok=True)
            # The journal is the commit/recovery marker, so remove it last.
            journal_path.unlink(missing_ok=True)


def _validate_rows(headers: Sequence[Dict[str, Any]], details: Sequence[Dict[str, Any]]) -> None:
    header_ids = [str(row.get("ORDER_ID") or "").strip() for row in headers]
    if not all(header_ids) or len(header_ids) != len(set(header_ids)):
        raise ValueError("Shopify order header DBF contains missing or duplicate ORDER_ID values")
    if any(not order_id.isascii() or len(order_id.encode("ascii")) > 24 for order_id in header_ids):
        raise ValueError("Shopify ORDER_ID values must be ASCII and no more than 24 bytes")
    header_id_set = set(header_ids)
    detail_keys: set[Tuple[str, str]] = set()
    detail_counts: Counter[str] = Counter()
    for row in details:
        order_id = str(row.get("ORDER_ID") or "").strip()
        line_key = str(row.get("LINE_KEY") or "").strip()
        if order_id not in header_id_set:
            raise ValueError(f"Shopify order detail DBF contains orphan ORDER_ID {order_id!r}")
        if not line_key.isascii() or len(line_key.encode("ascii")) > 32:
            raise ValueError("Shopify LINE_KEY values must be ASCII and no more than 32 bytes")
        key = (order_id, line_key)
        if not line_key or key in detail_keys:
            raise ValueError(f"Shopify order detail DBF contains a missing or duplicate LINE_KEY for {order_id}")
        detail_keys.add(key)
        detail_counts[order_id] += 1
    generations = {
        str(row.get("GEN_ID") or "").strip()
        for row in list(headers) + list(details)
    }
    if len(generations) > 1 or (generations and "" in generations):
        raise ValueError("Shopify order DBF generations do not match")
    for header in headers:
        order_id = str(header["ORDER_ID"])
        line_count = int(header.get("LINE_COUNT") or 0)
        if line_count != detail_counts[order_id]:
            raise ValueError(f"Shopify order {order_id} has a DBF line-count mismatch")


def _read_order_dbfs_unlocked(
    header_path: Path,
    detail_path: Path,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    _recover_interrupted_publish(header_path, detail_path)
    if header_path.exists() != detail_path.exists():
        raise ValueError("Shopify order header and detail DBFs must either both exist or both be absent")
    if not header_path.exists():
        return [], []
    headers = _read_dbf_table(header_path, HEADER_FIELDS)
    details = _read_dbf_table(detail_path, DETAIL_FIELDS)
    _validate_rows(headers, details)
    return headers, details


def read_order_dbfs(
    header_path: Path,
    detail_path: Path,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    with order_dbf_lock(header_path, detail_path):
        return _read_order_dbfs_unlocked(header_path, detail_path)


def _write_order_dbfs_unlocked(
    header_path: Path,
    detail_path: Path,
    headers: Iterable[Dict[str, Any]],
    details: Iterable[Dict[str, Any]],
) -> None:
    _recover_interrupted_publish(header_path, detail_path)
    if header_path.exists() != detail_path.exists():
        raise ValueError("Shopify order header and detail DBFs must either both exist or both be absent")
    generation = uuid.uuid4().hex
    header_rows = [dict(row) for row in headers]
    detail_rows = [dict(row) for row in details]
    counts = Counter(str(row.get("ORDER_ID") or "").strip() for row in detail_rows)
    for row in header_rows:
        order_id = str(row.get("ORDER_ID") or "").strip()
        row["GEN_ID"] = generation
        row["LINE_COUNT"] = counts[order_id]
    source_versions = {
        str(row.get("ORDER_ID") or "").strip(): row.get("SRC_VER") for row in header_rows
    }
    for row in detail_rows:
        order_id = str(row.get("ORDER_ID") or "").strip()
        row["GEN_ID"] = generation
        row.setdefault("SRC_VER", source_versions.get(order_id))
    header_rows.sort(key=lambda row: (str(row.get("ORDER_AT") or ""), str(row.get("ORDER_ID") or "")))
    detail_rows.sort(
        key=lambda row: (
            str(row.get("ORDER_ID") or ""),
            int(row.get("LINE_NO") or 0),
            str(row.get("LINE_KEY") or ""),
        )
    )
    _validate_rows(header_rows, detail_rows)
    header_content = _serialize_dbf(HEADER_FIELDS, header_rows)
    detail_content = _serialize_dbf(DETAIL_FIELDS, detail_rows)
    old_header = header_path.read_bytes() if header_path.exists() else None
    old_detail = detail_path.read_bytes() if detail_path.exists() else None
    previous_header, previous_detail, journal_path = _publish_paths(header_path, detail_path)
    header_temp: Optional[Path] = None
    detail_temp: Optional[Path] = None
    try:
        header_temp = _write_temp_file(header_path, header_content)
        detail_temp = _write_temp_file(detail_path, detail_content)
        if old_header is not None:
            _replace_with_content(previous_header, old_header)
        else:
            previous_header.unlink(missing_ok=True)
        if old_detail is not None:
            _replace_with_content(previous_detail, old_detail)
        else:
            previous_detail.unlink(missing_ok=True)
        journal_content = json.dumps(
            {
                "generation": generation,
                "header_existed": old_header is not None,
                "detail_existed": old_detail is not None,
                "header_sha256": hashlib.sha256(header_content).hexdigest(),
                "detail_sha256": hashlib.sha256(detail_content).hexdigest(),
                "previous_header_sha256": hashlib.sha256(old_header).hexdigest()
                if old_header is not None
                else None,
                "previous_detail_sha256": hashlib.sha256(old_detail).hexdigest()
                if old_detail is not None
                else None,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        _replace_with_content(journal_path, journal_content)
        os.replace(detail_temp, detail_path)
        os.replace(header_temp, header_path)
        published_headers = _read_dbf_table(header_path, HEADER_FIELDS)
        published_details = _read_dbf_table(detail_path, DETAIL_FIELDS)
        _validate_rows(published_headers, published_details)
        published_generations = {
            str(row.get("GEN_ID") or "")
            for row in published_headers + published_details
        }
        if published_generations and published_generations != {generation}:
            raise ValueError("Published Shopify order DBF generation could not be verified")
    except Exception:
        if journal_path.exists():
            _recover_interrupted_publish(header_path, detail_path)
        else:
            previous_header.unlink(missing_ok=True)
            previous_detail.unlink(missing_ok=True)
        raise
    else:
        previous_header.unlink(missing_ok=True)
        previous_detail.unlink(missing_ok=True)
        # Removing the marker last makes interrupted cleanup safe to resume.
        journal_path.unlink(missing_ok=True)
    finally:
        if header_temp is not None:
            header_temp.unlink(missing_ok=True)
        if detail_temp is not None:
            detail_temp.unlink(missing_ok=True)


def write_order_dbfs(
    header_path: Path,
    detail_path: Path,
    headers: Iterable[Dict[str, Any]],
    details: Iterable[Dict[str, Any]],
) -> None:
    with order_dbf_lock(header_path, detail_path):
        _write_order_dbfs_unlocked(header_path, detail_path, headers, details)


def _address_name(address: Dict[str, Any]) -> str:
    explicit_name = str(address.get("name") or "").strip()
    if explicit_name:
        return explicit_name
    return " ".join(
        value
        for value in (
            str(address.get("first_name") or "").strip(),
            str(address.get("last_name") or "").strip(),
        )
        if value
    )


def _address_values(prefix: str, address: Dict[str, Any], name: str) -> Dict[str, Any]:
    return {
        f"{prefix}_NAME": name or None,
        f"{prefix}_FIRST": str(address.get("first_name") or "").strip() or None,
        f"{prefix}_LAST": str(address.get("last_name") or "").strip() or None,
        f"{prefix}_COMP": address.get("company"),
        f"{prefix}_ADR1": address.get("address1"),
        f"{prefix}_ADR2": address.get("address2"),
        f"{prefix}_CITY": address.get("city"),
        f"{prefix}_PROV": address.get("province"),
        f"{prefix}_PCODE": address.get("province_code"),
        f"{prefix}_CTRY": address.get("country"),
        f"{prefix}_CCODE": address.get("country_code"),
        f"{prefix}_ZIP": address.get("zip"),
        f"{prefix}_PHONE": address.get("phone"),
    }


def _line_tax(item: Dict[str, Any]) -> Tuple[str, bool]:
    total = Decimal("0")
    coerced = False
    for tax_line in item.get("tax_lines") or []:
        if not isinstance(tax_line, dict):
            coerced = True
            continue
        value = tax_line.get("price")
        if value in (None, ""):
            price_set = tax_line.get("price_set") or {}
            shop_money = price_set.get("shop_money") if isinstance(price_set, dict) else {}
            value = shop_money.get("amount") if isinstance(shop_money, dict) else None
        try:
            amount = Decimal(str(value))
        except InvalidOperation:
            coerced = True
            continue
        if not amount.is_finite():
            coerced = True
            continue
        total += amount
    rounded = total.quantize(Decimal("0.01"))
    return f"{rounded}", coerced or rounded != total


def _line_total(price: Optional[str], quantity: int, discount: Optional[str]) -> Optional[str]:
    if price is None:
        return None
    try:
        total = (Decimal(price) * int(quantity)) - Decimal(discount or "0")
    except (InvalidOperation, ValueError):
        return None
    return f"{total.quantize(Decimal('0.01'))}"


def _split_note(value: Any) -> Tuple[Optional[str], Optional[str], bool]:
    if value in (None, ""):
        return None, None, False
    text = str(value)
    try:
        encoded = text.encode(DBF_ENCODING, "strict")
        replaced = False
    except UnicodeEncodeError:
        encoded = text.encode(DBF_ENCODING, "replace")
        replaced = True
    return (
        encoded[:254].decode(DBF_ENCODING, "replace") or None,
        encoded[254:508].decode(DBF_ENCODING, "replace") or None,
        replaced or len(encoded) > 508,
    )


def _header_from_change(change: Dict[str, Any], existing: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    order = change.get("order") or {}
    order_id = str(change.get("shopify_order_id") or order.get("id") or "").strip()
    if not order_id:
        raise ValueError("Order change is missing shopify_order_id")
    billing_address = order.get("billing_address") or {}
    shipping_address = order.get("shipping_address") or {}
    customer_first = str(order.get("customer_first_name") or "").strip()
    customer_last = str(order.get("customer_last_name") or "").strip()
    customer_name = " ".join(value for value in (customer_first, customer_last) if value)
    billing_name = _address_name(billing_address)
    shipping_name = _address_name(shipping_address)
    shipping_method = ", ".join(
        str(line.get("title") or line.get("code") or "").strip()
        for line in (order.get("shipping_lines") or [])
        if str(line.get("title") or line.get("code") or "").strip()
    )
    note1, note2, note_truncated = _split_note(order.get("note"))
    row: Dict[str, Any] = {
        "ORDER_ID": order_id,
        "INVOICE_NO": order.get("name") or change.get("order_name"),
        "ORDER_NUM": str(order.get("order_number") or "") or None,
        "CONFIRM_NO": order.get("confirmation_number"),
        "ORDER_AT": order.get("created_at"),
        "UPDATED_AT": order.get("updated_at"),
        "PROC_AT": order.get("processed_at"),
        "CANCEL_AT": order.get("cancelled_at"),
        "CLOSED_AT": order.get("closed_at"),
        "FIN_STATUS": order.get("financial_status"),
        "FUL_STATUS": order.get("fulfillment_status"),
        "CURRENCY": order.get("currency"),
        "CUST_NAME": customer_name or billing_name or shipping_name or None,
        "CUST_FIRST": customer_first or None,
        "CUST_LAST": customer_last or None,
        "EMAIL": order.get("email"),
        "PHONE": order.get("phone"),
        **_address_values("BILL", billing_address, billing_name),
        **_address_values("SHIP", shipping_address, shipping_name),
        "SHIP_METH": shipping_method or None,
        "SUBTOTAL": _numeric_text(order.get("subtotal_price")),
        "DISCOUNT": _numeric_text(order.get("total_discounts")),
        "SHIPPING": _numeric_text(order.get("shipping_price")),
        "HANDLING": "0.00",
        "TAX": _numeric_text(order.get("total_tax")),
        "TOTAL": _numeric_text(order.get("total_price")),
        "NOTE1": note1,
        "NOTE2": note2,
        "TAGS": order.get("tags"),
        "PRINT_ST": "PENDING",
        "PRINTED_AT": None,
        "IMPORT_ST": "PENDING",
        "IMPORTEDAT": None,
        "POS_ORD_NO": None,
        "IMP_ERROR": None,
        "SRC_EVENT": change.get("event_topic") or "orders/updated",
        "SRC_VER": int(change.get("version") or 1),
        "SYNCED_AT": datetime.now().astimezone().isoformat(),
        "TRUNCATED": note_truncated,
    }
    if existing:
        for field_name in POS_MANAGED_HEADER_FIELDS:
            row[field_name] = existing.get(field_name)
    return row


def _details_from_change(change: Dict[str, Any], header: Dict[str, Any]) -> List[Dict[str, Any]]:
    order = change.get("order") or {}
    rows: List[Dict[str, Any]] = []
    for index, item in enumerate(order.get("line_items") or [], start=1):
        line_id = str(item.get("id") or "").strip()
        quantity = int(item.get("quantity") or 0)
        price = money_text(item.get("price"))
        discount = money_text(item.get("total_discount"))
        line_tax, tax_coerced = _line_tax(item)
        rows.append(
            {
                "ORDER_ID": header["ORDER_ID"],
                "INVOICE_NO": header.get("INVOICE_NO"),
                "LINE_NO": index,
                "LINE_KEY": line_id or f"line-{index}",
                "LINE_ID": line_id or None,
                "PRODUCT_ID": str(item.get("product_id") or "") or None,
                "VARIANT_ID": str(item.get("variant_id") or "") or None,
                "SKU": str(item.get("sku") or "").strip() or None,
                "QTY": quantity,
                "CURR_QTY": int(item["current_quantity"])
                if item.get("current_quantity") is not None
                else None,
                "UNIT_PRICE": price,
                "DISCOUNT": discount,
                "TAX": line_tax,
                "EXTENSION": _line_total(price, quantity, discount),
                "DESCRIPT": item.get("title"),
                "VAR_TITLE": item.get("variant_title"),
                "VENDOR": item.get("vendor"),
                "GRAMS": int(item["grams"]) if item.get("grams") is not None else None,
                "REQ_SHIP": bool(item["requires_shipping"])
                if item.get("requires_shipping") is not None
                else None,
                "FUL_STATUS": item.get("fulfillment_status"),
                "SRC_VER": header.get("SRC_VER"),
                "TRUNCATED": tax_coerced,
            }
        )
    return rows


def _removed_order_id(change: Dict[str, Any]) -> Optional[str]:
    order = change.get("order") or {}
    if change.get("event_topic") not in {"orders/delete", "customers/redact"} and not order.get(
        "redacted"
    ):
        return None
    order_id = str(change.get("shopify_order_id") or order.get("id") or "").strip()
    if not order_id:
        raise ValueError("Order removal change is missing shopify_order_id")
    return order_id


def _apply_retention(
    headers: Iterable[Dict[str, Any]],
    details: Iterable[Dict[str, Any]],
    retention_rows: int,
    *,
    protected_order_ids: Optional[set[str]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    limit = max(100, int(retention_rows))
    protected_ids = protected_order_ids or set()
    header_rows = [dict(row) for row in headers]

    def order_key(row: Dict[str, Any]) -> Tuple[str, str]:
        return (
            str(row.get("ORDER_AT") or row.get("SYNCED_AT") or ""),
            str(row.get("ORDER_ID") or ""),
        )

    pending_headers = sorted(
        (
            row
            for row in header_rows
            if str(row.get("IMPORT_ST") or "").strip().upper() != "IMPORTED"
        ),
        key=order_key,
    )
    protected_pending = [
        row for row in pending_headers if str(row.get("ORDER_ID") or "") in protected_ids
    ]
    unprotected_pending = [
        row for row in pending_headers if str(row.get("ORDER_ID") or "") not in protected_ids
    ]
    imported_headers = sorted(
        (
            row
            for row in header_rows
            if str(row.get("IMPORT_ST") or "").strip().upper() == "IMPORTED"
        ),
        key=order_key,
        reverse=True,
    )
    # Never evict a previously published, unimported order: Railway may already
    # have acknowledged it, so there is no source from which to fetch it again.
    retained_headers = protected_pending
    remaining_capacity = max(0, limit - len(retained_headers))
    retained_headers.extend(unprotected_pending[:remaining_capacity])
    if len(retained_headers) < limit:
        retained_headers.extend(imported_headers[: limit - len(retained_headers)])
    retained_ids = {str(row.get("ORDER_ID") or "") for row in retained_headers}
    retained_details = [
        dict(row) for row in details if str(row.get("ORDER_ID") or "") in retained_ids
    ]
    return retained_headers, retained_details


def _upsert_order_changes_unlocked(
    header_path: Path,
    detail_path: Path,
    changes: List[Dict[str, Any]],
    *,
    retention_rows: int,
) -> set[str]:
    headers, details = _read_order_dbfs_unlocked(header_path, detail_path)
    protected_order_ids = {
        str(row["ORDER_ID"])
        for row in headers
        if str(row.get("IMPORT_ST") or "").strip().upper() != "IMPORTED"
    }
    starting_generations = {
        str(row.get("GEN_ID") or "") for row in headers + details
    }
    headers_by_id = {str(row["ORDER_ID"]): dict(row) for row in headers}
    details_by_id: Dict[str, List[Dict[str, Any]]] = {}
    removed_ids: set[str] = set()
    changed_ids: set[str] = set()
    for row in details:
        details_by_id.setdefault(str(row["ORDER_ID"]), []).append(dict(row))
    for change in changes:
        order = change.get("order") or {}
        order_id = str(change.get("shopify_order_id") or order.get("id") or "").strip()
        if not order_id:
            raise ValueError("Order change is missing shopify_order_id")
        changed_ids.add(order_id)
        if _removed_order_id(change) is not None:
            headers_by_id.pop(order_id, None)
            details_by_id.pop(order_id, None)
            removed_ids.add(order_id)
            continue
        removed_ids.discard(order_id)
        header = _header_from_change(change, headers_by_id.get(order_id))
        headers_by_id[order_id] = header
        details_by_id[order_id] = _details_from_change(change, header)
    merged_details = [row for rows in details_by_id.values() for row in rows]
    retained_headers, retained_details = _apply_retention(
        headers_by_id.values(),
        merged_details,
        retention_rows,
        protected_order_ids=protected_order_ids,
    )
    latest_headers, latest_details = _read_order_dbfs_unlocked(header_path, detail_path)
    latest_generations = {
        str(row.get("GEN_ID") or "") for row in latest_headers + latest_details
    }
    if latest_generations != starting_generations:
        raise RuntimeError("Shopify order DBFs changed while an update was being prepared")
    latest_by_id = {str(row["ORDER_ID"]): row for row in latest_headers}
    for row in retained_headers:
        latest = latest_by_id.get(str(row["ORDER_ID"]))
        if latest:
            for field_name in POS_MANAGED_HEADER_FIELDS:
                row[field_name] = latest.get(field_name)
    _write_order_dbfs_unlocked(header_path, detail_path, retained_headers, retained_details)
    retained_ids = {str(row["ORDER_ID"]) for row in retained_headers}
    imported_change_ids = {
        order_id
        for order_id in changed_ids
        if order_id in headers_by_id
        and str(headers_by_id[order_id].get("IMPORT_ST") or "").strip().upper() == "IMPORTED"
    }
    return retained_ids | removed_ids | imported_change_ids


def upsert_order_changes(
    header_path: Path,
    detail_path: Path,
    changes: List[Dict[str, Any]],
    *,
    retention_rows: int,
) -> set[str]:
    with order_dbf_lock(header_path, detail_path):
        return _upsert_order_changes_unlocked(
            header_path,
            detail_path,
            changes,
            retention_rows=retention_rows,
        )


LEGACY_HEADER_MAP = {
    "ORDER_ID": "shopify_order_id",
    "INVOICE_NO": "order_name",
    "ORDER_NUM": "order_number",
    "CONFIRM_NO": "confirmation_number",
    "ORDER_AT": "created_at",
    "UPDATED_AT": "updated_at",
    "PROC_AT": "processed_at",
    "CANCEL_AT": "cancelled_at",
    "CLOSED_AT": "closed_at",
    "FIN_STATUS": "financial_status",
    "FUL_STATUS": "fulfillment_status",
    "CURRENCY": "currency",
    "CUST_NAME": "customer_name",
    "CUST_FIRST": "customer_first_name",
    "CUST_LAST": "customer_last_name",
    "EMAIL": "email",
    "PHONE": "phone",
    "BILL_NAME": "billing_name",
    "BILL_FIRST": "billing_first_name",
    "BILL_LAST": "billing_last_name",
    "BILL_COMP": "billing_company",
    "BILL_ADR1": "billing_address1",
    "BILL_ADR2": "billing_address2",
    "BILL_CITY": "billing_city",
    "BILL_PROV": "billing_province",
    "BILL_PCODE": "billing_province_code",
    "BILL_CTRY": "billing_country",
    "BILL_CCODE": "billing_country_code",
    "BILL_ZIP": "billing_zip",
    "BILL_PHONE": "billing_phone",
    "SHIP_NAME": "shipping_name",
    "SHIP_FIRST": "shipping_first_name",
    "SHIP_LAST": "shipping_last_name",
    "SHIP_COMP": "shipping_company",
    "SHIP_ADR1": "shipping_address1",
    "SHIP_ADR2": "shipping_address2",
    "SHIP_CITY": "shipping_city",
    "SHIP_PROV": "shipping_province",
    "SHIP_PCODE": "shipping_province_code",
    "SHIP_CTRY": "shipping_country",
    "SHIP_CCODE": "shipping_country_code",
    "SHIP_ZIP": "shipping_zip",
    "SHIP_PHONE": "shipping_phone",
    "SHIP_METH": "shipping_method",
    "SUBTOTAL": "subtotal_price",
    "DISCOUNT": "total_discounts",
    "SHIPPING": "shipping_price",
    "TAX": "total_tax",
    "TOTAL": "total_price",
    "TAGS": "tags",
    "PRINT_ST": "print_status",
    "PRINTED_AT": "printed_at",
    "IMPORT_ST": "import_status",
    "IMPORTEDAT": "imported_at",
    "POS_ORD_NO": "pos_order_number",
    "IMP_ERROR": "import_error",
    "SRC_EVENT": "source_event",
    "SRC_VER": "source_version",
    "SYNCED_AT": "synced_at",
}

LEGACY_DETAIL_MAP = {
    "ORDER_ID": "shopify_order_id",
    "LINE_KEY": "line_key",
    "LINE_ID": "shopify_line_item_id",
    "LINE_NO": "line_number",
    "PRODUCT_ID": "product_id",
    "VARIANT_ID": "variant_id",
    "SKU": "sku",
    "QTY": "quantity",
    "CURR_QTY": "current_quantity",
    "UNIT_PRICE": "price",
    "DISCOUNT": "total_discount",
    "TAX": "line_tax",
    "EXTENSION": "line_total",
    "DESCRIPT": "title",
    "VAR_TITLE": "variant_title",
    "VENDOR": "vendor",
    "GRAMS": "grams",
    "REQ_SHIP": "requires_shipping",
    "FUL_STATUS": "fulfillment_status",
}


def _legacy_value(row: sqlite3.Row, column: str, default: Any = None) -> Any:
    return row[column] if column in row.keys() else default


def remove_orders_from_legacy_sqlite(
    legacy_paths: Iterable[Path],
    changes: Iterable[Dict[str, Any]],
) -> int:
    """Apply DBF deletion/redaction changes to retained legacy SQLite copies."""
    order_ids = {
        order_id
        for change in changes
        if (order_id := _removed_order_id(change)) is not None
    }
    if not order_ids:
        return 0
    removed = 0
    seen_paths: set[Path] = set()
    for raw_path in legacy_paths:
        legacy_path = raw_path.resolve()
        if legacy_path in seen_paths or not legacy_path.is_file():
            continue
        seen_paths.add(legacy_path)
        with legacy_path.open("rb") as handle:
            if handle.read(16) != b"SQLite format 3\x00":
                continue
        connection = sqlite3.connect(str(legacy_path))
        path_removed = 0
        try:
            connection.execute("PRAGMA secure_delete=ON")
            table_names = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "orders" not in table_names:
                continue
            with connection:
                for order_id in order_ids:
                    if "order_items" in table_names:
                        connection.execute(
                            "DELETE FROM order_items WHERE shopify_order_id = ?",
                            (order_id,),
                        )
                    cursor = connection.execute(
                        "DELETE FROM orders WHERE shopify_order_id = ?",
                        (order_id,),
                    )
                    path_removed += max(0, cursor.rowcount)
            # Always finish the privacy cleanup. A previous attempt may have
            # committed its DELETE and stopped before clearing free pages/WAL.
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
            connection.execute("PRAGMA journal_mode=DELETE").fetchone()
            connection.execute("VACUUM")
            removed += path_removed
        finally:
            connection.close()
        for suffix in ("-wal", "-journal"):
            sidecar = Path(f"{legacy_path}{suffix}")
            if sidecar.exists() and sidecar.stat().st_size:
                raise RuntimeError(f"Legacy SQLite privacy sidecar could not be cleared: {sidecar}")
            sidecar.unlink(missing_ok=True)
        Path(f"{legacy_path}-shm").unlink(missing_ok=True)
    return removed


def migrate_legacy_sqlite_database(
    legacy_path: Path,
    header_path: Path,
    detail_path: Path,
    *,
    retention_rows: int,
) -> bool:
    if header_path.exists() or detail_path.exists() or not legacy_path.is_file():
        return False
    with legacy_path.open("rb") as handle:
        if handle.read(16) != b"SQLite format 3\x00":
            return False
    connection = sqlite3.connect(str(legacy_path))
    connection.row_factory = sqlite3.Row
    try:
        table_names = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "orders" not in table_names:
            raise ValueError(f"Legacy Shopify order database has no orders table: {legacy_path}")
        legacy_headers = connection.execute("SELECT * FROM orders").fetchall()
        legacy_details = (
            connection.execute("SELECT * FROM order_items ORDER BY rowid").fetchall()
            if "order_items" in table_names
            else []
        )
    finally:
        connection.close()
    headers: List[Dict[str, Any]] = []
    for source in legacy_headers:
        row = {target: _legacy_value(source, column) for target, column in LEGACY_HEADER_MAP.items()}
        note1, note2, note_truncated = _split_note(_legacy_value(source, "note"))
        row.update(
            {
                "HANDLING": "0.00",
                "NOTE1": note1,
                "NOTE2": note2,
                "PRINT_ST": row.get("PRINT_ST") or "PENDING",
                "IMPORT_ST": row.get("IMPORT_ST") or "PENDING",
                "TRUNCATED": note_truncated,
            }
        )
        headers.append(row)
    headers_by_id = {str(row.get("ORDER_ID") or ""): row for row in headers}
    details: List[Dict[str, Any]] = []
    line_counts: Counter[str] = Counter()
    for source in legacy_details:
        row = {target: _legacy_value(source, column) for target, column in LEGACY_DETAIL_MAP.items()}
        order_id = str(row.get("ORDER_ID") or "")
        line_counts[order_id] += 1
        line_number = line_counts[order_id]
        header = headers_by_id.get(order_id, {})
        extension = row.get("EXTENSION")
        if extension in (None, ""):
            extension = _line_total(
                money_text(row.get("UNIT_PRICE")),
                int(row.get("QTY") or 0),
                money_text(row.get("DISCOUNT")),
            )
        row.update(
            {
                "INVOICE_NO": header.get("INVOICE_NO"),
                "LINE_NO": line_number,
                "LINE_KEY": str(row.get("LINE_KEY") or "").strip() or f"line-{line_number}",
                "EXTENSION": extension,
                "SRC_VER": header.get("SRC_VER"),
                "TRUNCATED": False,
            }
        )
        details.append(row)
    protected_order_ids = {
        str(row.get("ORDER_ID") or "")
        for row in headers
        if str(row.get("IMPORT_ST") or "").strip().upper() != "IMPORTED"
    }
    retained_headers, retained_details = _apply_retention(
        headers,
        details,
        retention_rows,
        protected_order_ids=protected_order_ids,
    )
    write_order_dbfs(header_path, detail_path, retained_headers, retained_details)
    return True
