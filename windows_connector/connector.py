#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import struct
import subprocess
import sys
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional

import requests


CONNECTOR_DIR = Path(__file__).resolve().parent
PROJECT_DIR = CONNECTOR_DIR.parent
POS_READER_DIR = PROJECT_DIR / "jbarbaro_db"
sys.path.insert(0, str(POS_READER_DIR))

import dbf_pos_sync  # noqa: E402


STATE_VERSION = 1
POS_EVENT_FILES = ("invdtl.dbf", "editvoid.dbf")
MATRIX_VARIANT_SKU = re.compile(r"^(.+?)\.\s*\d+\s+\d+$")


def parse_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuous Windows POS and Shopify inventory connector.")
    parser.add_argument("--config", default=str(CONNECTOR_DIR / "connector.env"))
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    parser.add_argument("--dry-run", action="store_true", help="Read and compare without changing either system.")
    return parser.parse_args()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    value = int((os.getenv(name) or str(default)).strip())
    return max(minimum, min(maximum, value))


class Connector:
    def __init__(self, *, config_path: Path, dry_run: bool = False) -> None:
        if not config_path.exists():
            raise FileNotFoundError(f"Connector config not found: {config_path}")
        dbf_pos_sync.load_env_file(config_path)

        self.config_path = config_path
        self.dbf_dir = Path(self._required("POS_DBF_DIR")).expanduser().resolve()
        self.base_url = self._required("SHOPIFY_SYNC_BASE_URL").rstrip("/")
        self.api_key = self._required("SHOPIFY_SYNC_API_KEY")
        self.api_secret = self._required("SHOPIFY_SYNC_API_SECRET")
        self.interval_seconds = env_int("SYNC_INTERVAL_SECONDS", 180, minimum=30, maximum=86400)
        self.nightly_full_sync_hour = env_int("NIGHTLY_FULL_SYNC_HOUR", 0, minimum=0, maximum=23)
        self.batch_size = env_int("CONNECTOR_BATCH_SIZE", 25, minimum=1, maximum=100)
        self.workers = env_int("CONNECTOR_WORKERS", 2, minimum=1, maximum=4)
        self.timeout = env_int("CONNECTOR_TIMEOUT_SECONDS", 300, minimum=30, maximum=1800)
        self.initial_catalog_upload = env_bool("INITIAL_CATALOG_UPLOAD", True)
        self.order_sync_enabled = env_bool("ORDER_SYNC_ENABLED", True)
        self.order_db_path = Path(
            os.getenv("SHOPIFY_ORDER_DB_PATH") or (self.dbf_dir / "shopify-orders.db")
        ).expanduser().resolve()
        self.order_retention_rows = env_int("ORDER_DB_RETENTION_ROWS", 10000, minimum=100, maximum=100000)
        self.order_database_initialized = False
        self.order_bridge_status_checked = False
        self.last_order_poll_monotonic = 0.0
        self.writeback_mode = (os.getenv("POS_WRITEBACK_MODE") or "disabled").strip().lower()
        if self.writeback_mode not in {"disabled", "dry-run", "vfp-oledb"}:
            raise ValueError("POS_WRITEBACK_MODE must be disabled, dry-run, or vfp-oledb")
        self.dry_run = dry_run

        data_dir = Path(os.getenv("CONNECTOR_DATA_DIR") or (CONNECTOR_DIR / "data")).expanduser().resolve()
        data_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = data_dir / "state.json"
        self.log_path = data_dir / "connector.log"
        self.writer_script = Path(
            os.getenv("POS_WRITEBACK_SCRIPT") or (CONNECTOR_DIR / "write_pos_quantity.ps1")
        ).expanduser().resolve()
        self.logger = configure_logging(
            self.log_path,
            max_bytes=env_int("CONNECTOR_LOG_MAX_BYTES", 5 * 1024 * 1024, minimum=100000, maximum=50000000),
            backup_count=env_int("CONNECTOR_LOG_BACKUPS", 3, minimum=1, maximum=10),
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "User-Agent": "shopify-pos-windows-connector/1.0",
                "X-API-Key": self.api_key,
                "X-API-Secret": self.api_secret,
                "X-Sync-Workers": str(self.workers),
            }
        )

    @staticmethod
    def _required(name: str) -> str:
        value = (os.getenv(name) or "").strip()
        if not value:
            raise ValueError(f"Missing required connector setting: {name}")
        return value

    def run_forever(self, *, once: bool = False) -> int:
        self.logger.info(
            "connector_started dbf_dir=%s interval=%ss writeback=%s dry_run=%s",
            self.dbf_dir,
            self.interval_seconds,
            self.writeback_mode,
            self.dry_run,
        )
        while True:
            started = time.monotonic()
            try:
                self.run_cycle()
            except Exception:
                self.logger.exception("connector_cycle_failed")
                if once:
                    return 1
            if once:
                return 0
            elapsed = time.monotonic() - started
            time.sleep(max(1.0, self.interval_seconds - elapsed))

    def run_cycle(self) -> None:
        state = load_state(self.state_path)
        now = datetime.now()
        if not self.dry_run:
            self._retry_pending(state)

        if self.order_sync_enabled:
            self._sync_order_inbox()

        inventory_changes = self._fetch_inventory_changes()
        event_skus, event_file_reset = self._collect_pos_event_skus(state)
        sku_bases: Dict[str, str] = state.setdefault("sku_bases", {})
        shopify_skus = {
            sku_bases.get(
                str(change.get("sku") or "").strip(),
                base_sku(str(change.get("sku") or "").strip()),
            )
            for change in inventory_changes
            if str(change.get("sku") or "").strip()
        }
        affected_base_skus = event_skus | shopify_skus

        catalog_incomplete = not bool(state.get("catalog_complete"))
        nightly_due = nightly_full_sync_due(
            state.get("last_full_reconcile_date"),
            now=now,
            hour=self.nightly_full_sync_hour,
        )
        full_reconcile = catalog_incomplete or nightly_due or event_file_reset
        if full_reconcile:
            read_mode = "initial" if catalog_incomplete else "nightly"
        elif affected_base_skus:
            read_mode = "events"
        else:
            read_mode = "idle"

        payloads: List[Dict[str, Any]] = []
        skipped_non_sellable = 0
        if full_reconcile:
            prepared_products, stats = dbf_pos_sync.load_products(self._reader_args())
            for prepared in prepared_products:
                invalid_field = negative_catalog_money_field(prepared.payload)
                if invalid_field:
                    self.logger.warning(
                        "catalog_product_skipped_negative_money sku=%s field=%s",
                        prepared.payload.get("sku"),
                        invalid_field,
                    )
                    continue
                payloads.append(prepared.payload)
            skipped_non_sellable = stats.skipped_non_sellable
            # Python's sort is stable, so products keep their POS order within each
            # group while every stocked product is uploaded before zero-stock rows.
            payloads.sort(key=catalog_upload_priority)

        payload_by_base = {str(payload["sku"]): payload for payload in payloads}
        discovered_sku_bases = sku_base_mapping(payloads)
        if full_reconcile:
            state["sku_bases"] = discovered_sku_bases
        else:
            sku_bases.update(discovered_sku_bases)
        if full_reconcile:
            local_quantities = flatten_quantities(payloads)
        else:
            local_quantities = read_targeted_pos_quantities(
                self.dbf_dir,
                affected_base_skus,
                sku_bases=sku_bases,
            )
        self.logger.info(
            "pos_read mode=%s requested_skus=%s products=%s quantities=%s skipped_non_sellable=%s",
            read_mode,
            len(affected_base_skus),
            len(payloads) if full_reconcile else len(affected_base_skus),
            len(local_quantities),
            skipped_non_sellable,
        )

        known_products = set(state.get("catalog_products") or [])
        new_base_skus = [sku for sku in payload_by_base if sku not in known_products]
        if catalog_incomplete and new_base_skus and self.initial_catalog_upload:
            new_payloads = [payload_by_base[sku] for sku in new_base_skus]
            if self.dry_run:
                self.logger.info("catalog_upload_dry_run products=%s", len(new_payloads))
            else:
                succeeded = self._upload_catalog(new_payloads, state=state)
                known_products.update(succeeded)
                state["catalog_products"] = sorted(known_products, key=str.casefold)
                state["catalog_complete"] = len(known_products) == len(payload_by_base)
                if state["catalog_complete"]:
                    quantity_entries = state.setdefault("quantities", {})
                    for sku, quantity in local_quantities.items():
                        quantity_entries.setdefault(
                            sku,
                            {"canonical": quantity, "pos_seen": quantity, "shop_seen": quantity},
                        )
                save_state(self.state_path, state)
                self.logger.info(
                    "catalog_upload_complete attempted=%s succeeded=%s remaining=%s",
                    len(new_payloads),
                    len(succeeded),
                    len(payload_by_base) - len(known_products),
                )
        elif catalog_incomplete and new_base_skus and not self.initial_catalog_upload:
            known_products.update(new_base_skus)
            state["catalog_products"] = sorted(known_products, key=str.casefold)
            state["catalog_complete"] = True
        elif catalog_incomplete and not new_base_skus:
            state["catalog_complete"] = True

        entries: Dict[str, Dict[str, Any]] = state.setdefault("quantities", {})
        remote_quantities = {
            sku: int(entry.get("shop_seen") or 0)
            for sku, entry in entries.items()
        }
        for change in inventory_changes:
            sku = str(change.get("sku") or "").strip()
            if sku:
                remote_quantities[sku] = int(change.get("quantity") or 0)
        initialized = 0
        for sku in sorted(local_quantities.keys() & remote_quantities.keys(), key=str.casefold):
            if sku in entries:
                continue
            pos_quantity = local_quantities[sku]
            shop_quantity = remote_quantities[sku]
            if pos_quantity == shop_quantity:
                canonical = pos_quantity
            else:
                canonical = pos_quantity
            entries[sku] = {
                "canonical": canonical,
                "pos_seen": pos_quantity,
                "shop_seen": shop_quantity,
            }
            initialized += 1

        planned_shop: List[Dict[str, Any]] = []
        planned_pos: List[Dict[str, Any]] = []
        for sku in sorted(local_quantities.keys() & remote_quantities.keys(), key=str.casefold):
            entry = entries[sku]
            if entry.get("pending_shop") or entry.get("pending_pos"):
                continue
            pos_quantity = local_quantities[sku]
            shop_quantity = remote_quantities[sku]
            plan = merge_quantity(entry, pos_quantity=pos_quantity, shop_quantity=shop_quantity)
            previous_pos = plan["previous_pos"]
            previous_shop = plan["previous_shop"]
            target = plan["target"]

            entry["canonical"] = target
            entry["pos_seen"] = pos_quantity
            entry["shop_seen"] = shop_quantity
            shop_adjustment = plan["shop_adjustment"]
            pos_adjustment = plan["pos_adjustment"]
            revision = int(entry.get("revision") or 0)
            if shop_adjustment or pos_adjustment:
                revision += 1
                entry["revision"] = revision

            if shop_adjustment:
                action = {
                    "sku": sku,
                    "delta": shop_adjustment,
                    "idempotency_key": adjustment_key(
                        "shopify", sku, revision, previous_pos, pos_quantity, previous_shop, shop_quantity, target
                    ),
                }
                entry["pending_shop"] = action
                planned_shop.append(action)
            if pos_adjustment:
                action = {
                    "sku": sku,
                    "delta": pos_adjustment,
                    "expected_quantity": pos_quantity,
                    "idempotency_key": adjustment_key(
                        "pos", sku, revision, previous_pos, pos_quantity, previous_shop, shop_quantity, target
                    ),
                }
                if self.writeback_mode == "vfp-oledb":
                    entry["pending_pos"] = action
                planned_pos.append(action)

        if self.dry_run:
            self.logger.info(
                "reconcile_dry_run initialized=%s shopify_adjustments=%s pos_adjustments=%s",
                initialized,
                len(planned_shop),
                len(planned_pos),
            )
            return

        if full_reconcile:
            state["last_full_reconcile_date"] = now.date().isoformat()
        save_state(self.state_path, state)
        if planned_shop:
            self._apply_shopify_adjustments(state, planned_shop)
        if planned_pos:
            if self.writeback_mode == "vfp-oledb":
                self._apply_pos_adjustments(state, planned_pos)
            else:
                self.logger.warning(
                    "pos_writeback_not_applied mode=%s adjustments=%s",
                    self.writeback_mode,
                    len(planned_pos),
                )
        state["last_cycle_epoch"] = int(time.time())
        save_state(self.state_path, state)
        if inventory_changes:
            self._acknowledge_inventory_changes(inventory_changes)
        self.logger.info(
            "connector_cycle_complete initialized=%s webhook_changes=%s shopify_adjustments=%s pos_adjustments=%s",
            initialized,
            len(inventory_changes),
            len(planned_shop),
            len(planned_pos),
        )

    def _reader_args(self, skus: Optional[Iterable[str]] = None) -> SimpleNamespace:
        return SimpleNamespace(
            dbf_dir=str(self.dbf_dir),
            recursive=True,
            matrix_variants=True,
            quantity_source="item",
            itemmqty_cell=None,
            sku=sorted(set(skus or []), key=str.casefold),
            skip_non_sellable=True,
            skip_zero_price=False,
            skip_zero_quantity=False,
            limit=None,
            name_mode="smart",
            # Railway deliberately creates products with an empty description so
            # storefront copy can be managed in Shopify after the first import.
            include_html_description=False,
            include_tags=True,
            include_desc2_description=False,
            include_metafields=True,
            metafield_namespace="pos",
            full_sync=False,
            status=None,
            in_stock_status="active",
            zero_quantity_status="archived",
        )

    def _collect_pos_event_skus(self, state: Dict[str, Any]) -> tuple[set[str], bool]:
        cursors: Dict[str, int] = state.setdefault("event_cursors", {})
        affected: set[str] = set()
        force_full_reconcile = False
        for filename in POS_EVENT_FILES:
            path = dbf_pos_sync.find_dbf_file(self.dbf_dir, filename, recursive=True)
            if path is None:
                self.logger.warning("pos_event_file_missing file=%s", filename)
                continue

            previous_cursor = cursors.get(filename)
            if previous_cursor is None:
                record_count = dbf_record_count(path)
                cursors[filename] = record_count
                self.logger.info("pos_event_cursor_initialized file=%s records=%s", filename, record_count)
                continue

            rows, record_count, was_reset = read_appended_dbf_rows(path, int(previous_cursor))
            cursors[filename] = record_count
            if was_reset:
                # A packed/replaced master event table may no longer preserve record
                # positions. The nightly-style full read is safer than replaying its
                # entire history as if every old row were a new sale.
                force_full_reconcile = True
                self.logger.warning(
                    "pos_event_file_reset file=%s previous_records=%s current_records=%s",
                    filename,
                    previous_cursor,
                    record_count,
                )
                continue
            for row in rows:
                sku = str(row.get("SKU") or "").strip()
                if sku:
                    affected.add(base_sku(sku))
            if rows:
                self.logger.info(
                    "pos_events_read file=%s new_records=%s affected_skus=%s",
                    filename,
                    len(rows),
                    len({base_sku(str(row.get('SKU') or '').strip()) for row in rows if row.get('SKU')}),
                )
        return affected, force_full_reconcile

    def _upload_catalog(self, payloads: List[Dict[str, Any]], *, state: Dict[str, Any]) -> set[str]:
        succeeded: set[str] = set()
        known_products = set(state.get("catalog_products") or [])
        endpoint = f"{self.base_url}/wc-api/v3/products/batch"
        total_products = len(payloads)
        processed_products = 0
        for chunk in chunks(payloads, self.batch_size):
            response = self.session.post(endpoint, json=chunk, timeout=self.timeout)
            if response.status_code >= 400:
                self.logger.error(
                    "catalog_batch_http_error status=%s skus=%s response=%s",
                    response.status_code,
                    ",".join(str(payload.get("sku") or "") for payload in chunk),
                    response.text[:2000],
                )
            response.raise_for_status()
            body = response.json()
            results = body.get("results") or []
            for payload, result in zip(chunk, results):
                if result.get("success"):
                    base_sku = str(payload["sku"])
                    succeeded.add(base_sku)
                    known_products.add(base_sku)
                else:
                    self.logger.error("catalog_product_failed sku=%s message=%s", payload.get("sku"), result.get("message"))
            state["catalog_products"] = sorted(known_products, key=str.casefold)
            save_state(self.state_path, state)
            processed_products += len(chunk)
            self.logger.info(
                "catalog_upload_progress processed=%s total=%s succeeded=%s",
                processed_products,
                total_products,
                len(succeeded),
            )
            if (
                self.order_sync_enabled
                and time.monotonic() - self.last_order_poll_monotonic >= self.interval_seconds
            ):
                self._sync_order_inbox()
        return succeeded

    def _fetch_inventory_changes(self) -> List[Dict[str, Any]]:
        response = self.session.get(f"{self.base_url}/sync/inventory/changes?limit=5000", timeout=self.timeout)
        response.raise_for_status()
        return list(response.json().get("items") or [])

    def _sync_order_inbox(self) -> None:
        try:
            if not self.dry_run and not self.order_database_initialized:
                # Create/migrate the local header/detail schema even when Shopify
                # has not delivered the first order yet.
                upsert_order_changes(
                    self.order_db_path,
                    [],
                    retention_rows=self.order_retention_rows,
                )
                self.order_database_initialized = True
            if not self.order_bridge_status_checked:
                try:
                    status_response = self.session.get(
                        f"{self.base_url}/sync/orders/status",
                        timeout=self.timeout,
                    )
                    status_response.raise_for_status()
                    status = status_response.json()
                    self.logger.info(
                        "order_bridge_status read_orders=%s webhooks=%s queued=%s error=%s",
                        status.get("read_orders_authorized"),
                        status.get("webhook_status"),
                        status.get("queued_orders"),
                        status.get("webhook_error"),
                    )
                except Exception:
                    self.logger.exception("order_bridge_status_failed")
                finally:
                    self.order_bridge_status_checked = True
            response = self.session.get(f"{self.base_url}/sync/orders/changes?limit=250", timeout=self.timeout)
            response.raise_for_status()
            changes = list(response.json().get("items") or [])
            self.logger.info("order_inbox_checked changes=%s path=%s", len(changes), self.order_db_path)
            if not changes:
                return
            if self.dry_run:
                self.logger.info("order_inbox_dry_run changes=%s path=%s", len(changes), self.order_db_path)
                return
            upsert_order_changes(
                self.order_db_path,
                changes,
                retention_rows=self.order_retention_rows,
            )
            payload = {
                "changes": [
                    {"id": int(change["id"]), "version": int(change["version"])}
                    for change in changes
                ]
            }
            ack = self.session.post(
                f"{self.base_url}/sync/orders/changes/ack",
                json=payload,
                timeout=self.timeout,
            )
            ack.raise_for_status()
            self.logger.info(
                "order_inbox_updated changes=%s acknowledged=%s path=%s",
                len(changes),
                int(ack.json().get("acknowledged") or 0),
                self.order_db_path,
            )
        except Exception:
            # Order intake must not prevent inventory reconciliation. Railway keeps
            # unacknowledged changes for a later retry.
            self.logger.exception("order_inbox_sync_failed")
        finally:
            self.last_order_poll_monotonic = time.monotonic()

    def _acknowledge_inventory_changes(self, changes: List[Dict[str, Any]]) -> None:
        payload = {
            "changes": [
                {"id": int(change["id"]), "version": int(change["version"])}
                for change in changes
            ]
        }
        response = self.session.post(
            f"{self.base_url}/sync/inventory/changes/ack",
            json=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()

    def _retry_pending(self, state: Dict[str, Any]) -> None:
        entries = state.get("quantities") or {}
        shop_actions = [entry["pending_shop"] for entry in entries.values() if entry.get("pending_shop")]
        pos_actions = [entry["pending_pos"] for entry in entries.values() if entry.get("pending_pos")]
        if shop_actions:
            self.logger.warning("retrying_pending_shopify_adjustments count=%s", len(shop_actions))
            self._apply_shopify_adjustments(state, shop_actions)
        if pos_actions and self.writeback_mode == "vfp-oledb":
            self.logger.warning("retrying_pending_pos_adjustments count=%s", len(pos_actions))
            self._apply_pos_adjustments(state, pos_actions)

    def _apply_shopify_adjustments(self, state: Dict[str, Any], actions: List[Dict[str, Any]]) -> None:
        endpoint = f"{self.base_url}/sync/inventory/adjustments"
        entries = state["quantities"]
        for chunk in chunks(actions, min(self.batch_size, 250)):
            response = self.session.post(endpoint, json={"adjustments": chunk}, timeout=self.timeout)
            response.raise_for_status()
            results = response.json().get("results") or []
            for action, result in zip(chunk, results):
                if not result.get("success"):
                    self.logger.error(
                        "shopify_adjustment_failed sku=%s delta=%s message=%s",
                        action["sku"],
                        action["delta"],
                        result.get("message"),
                    )
                    continue
                entry = entries[action["sku"]]
                entry["shop_seen"] = int(entry["shop_seen"]) + int(action["delta"])
                entry.pop("pending_shop", None)
            save_state(self.state_path, state)

    def _apply_pos_adjustments(self, state: Dict[str, Any], actions: List[Dict[str, Any]]) -> None:
        if not self.writer_script.exists():
            raise FileNotFoundError(f"POS write-back script not found: {self.writer_script}")
        powershell = find_powershell()
        process = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(self.writer_script),
                "-DbfDirectory",
                str(self.dbf_dir),
                "-AdjustmentsJson",
                json.dumps(actions, separators=(",", ":")),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=self.timeout,
        )
        if process.returncode != 0:
            raise RuntimeError(f"POS write-back failed: {(process.stderr or process.stdout).strip()}")
        results = json.loads(process.stdout or "[]")
        entries = state["quantities"]
        for action, result in zip(actions, results):
            if not result.get("success"):
                self.logger.error(
                    "pos_adjustment_failed sku=%s delta=%s message=%s",
                    action["sku"],
                    action["delta"],
                    result.get("message"),
                )
                continue
            entry = entries[action["sku"]]
            entry["pos_seen"] = int(action["expected_quantity"]) + int(action["delta"])
            entry.pop("pending_pos", None)
        save_state(self.state_path, state)


def base_sku(sku: str) -> str:
    normalized = str(sku or "").strip()
    match = MATRIX_VARIANT_SKU.match(normalized)
    return match.group(1).strip() if match else normalized


def nightly_full_sync_due(last_date: Any, *, now: datetime, hour: int) -> bool:
    if now.hour < max(0, min(23, int(hour))):
        return False
    return str(last_date or "") != now.date().isoformat()


def dbf_record_count(path: Path) -> int:
    with path.open("rb") as handle:
        header = handle.read(32)
    if len(header) != 32:
        raise ValueError(f"{path} is not a valid DBF file.")
    return struct.unpack("<I", header[4:8])[0]


def read_appended_dbf_rows(
    path: Path,
    start_record: int,
    *,
    encoding: str = "latin1",
) -> tuple[List[Dict[str, Any]], int, bool]:
    """Read only physical DBF records appended after a saved record position."""
    with path.open("rb") as handle:
        header = handle.read(32)
        if len(header) != 32:
            raise ValueError(f"{path} is not a valid DBF file.")
        record_count = struct.unpack("<I", header[4:8])[0]
        header_length = struct.unpack("<H", header[8:10])[0]
        record_length = struct.unpack("<H", header[10:12])[0]
        if record_length < 2:
            raise ValueError(f"{path} has an invalid DBF record length.")

        fields: List[Any] = []
        while True:
            descriptor = handle.read(32)
            if not descriptor:
                raise ValueError(f"{path} ended before the DBF field list was complete.")
            if descriptor[0] == 0x0D:
                break
            fields.append(
                dbf_pos_sync.DBFField(
                    name=descriptor[:11].split(b"\x00", 1)[0].decode("ascii", "ignore"),
                    field_type=chr(descriptor[11]),
                    length=descriptor[16],
                    decimals=descriptor[17],
                )
            )

        cursor = max(0, int(start_record))
        if record_count < cursor:
            return [], record_count, True

        rows: List[Dict[str, Any]] = []
        handle.seek(header_length + (cursor * record_length))
        for record_index in range(cursor, record_count):
            record = handle.read(record_length)
            if len(record) != record_length:
                # Do not advance beyond a record that the POS is still writing.
                break
            cursor = record_index + 1
            if record[0] == 0x2A:
                continue
            row: Dict[str, Any] = {}
            offset = 1
            for field in fields:
                raw_value = record[offset : offset + field.length]
                offset += field.length
                row[field.name] = dbf_pos_sync._parse_dbf_value(raw_value, field, encoding=encoding)
            rows.append(row)
        return rows, cursor, False


def iter_selected_dbf_rows(
    path: Path,
    key_values: set[str],
    *,
    selected_fields: set[str],
    key_field: str = "SKU",
    encoding: str = "latin1",
) -> Iterable[Dict[str, Any]]:
    """Scan a DBF but decode only selected fields for matching keys."""
    with path.open("rb") as handle:
        header = handle.read(32)
        if len(header) != 32:
            raise ValueError(f"{path} is not a valid DBF file.")
        record_count = struct.unpack("<I", header[4:8])[0]
        header_length = struct.unpack("<H", header[8:10])[0]
        record_length = struct.unpack("<H", header[10:12])[0]

        fields: List[tuple[Any, int]] = []
        offset = 1
        while True:
            descriptor = handle.read(32)
            if not descriptor:
                raise ValueError(f"{path} ended before the DBF field list was complete.")
            if descriptor[0] == 0x0D:
                break
            field = dbf_pos_sync.DBFField(
                name=descriptor[:11].split(b"\x00", 1)[0].decode("ascii", "ignore"),
                field_type=chr(descriptor[11]),
                length=descriptor[16],
                decimals=descriptor[17],
            )
            fields.append((field, offset))
            offset += field.length

        field_lookup = {field.name.upper(): (field, offset) for field, offset in fields}
        key_definition = field_lookup.get(key_field.upper())
        if key_definition is None:
            raise ValueError(f"{path} does not contain the DBF key field {key_field}.")
        key_descriptor, key_offset = key_definition
        wanted_fields = {
            name.upper(): field_lookup[name.upper()]
            for name in selected_fields | {key_field}
            if name.upper() in field_lookup
        }

        handle.seek(header_length)
        for _ in range(record_count):
            record = handle.read(record_length)
            if len(record) != record_length:
                break
            if record[0] == 0x2A:
                continue
            raw_key = record[key_offset : key_offset + key_descriptor.length]
            key = raw_key.decode(encoding, "ignore").strip()
            if key not in key_values:
                continue
            row: Dict[str, Any] = {}
            for _, (field, field_offset) in wanted_fields.items():
                raw_value = record[field_offset : field_offset + field.length]
                row[field.name] = dbf_pos_sync._parse_dbf_value(raw_value, field, encoding=encoding)
            yield row


def sku_base_mapping(payloads: Iterable[Dict[str, Any]]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for payload in payloads:
        product_sku = str(payload.get("sku") or "").strip()
        if not product_sku:
            continue
        mapping[product_sku] = product_sku
        for variant in payload.get("variants") or []:
            variant_sku = str(variant.get("sku") or "").strip()
            if variant_sku:
                mapping[variant_sku] = product_sku
    return mapping


def read_targeted_pos_quantities(
    dbf_dir: Path,
    base_skus: Iterable[str],
    *,
    sku_bases: Dict[str, str],
) -> Dict[str, int]:
    """Read current quantities only, without rebuilding catalog metadata."""
    targets = {str(sku).strip() for sku in base_skus if str(sku).strip()}
    if not targets:
        return {}

    variants_by_base: Dict[str, set[str]] = {sku: set() for sku in targets}
    for variant_sku, product_sku in sku_bases.items():
        if product_sku in targets and variant_sku != product_sku:
            variants_by_base[product_sku].add(variant_sku)

    quantities: Dict[str, int] = {}
    item_path = dbf_pos_sync.find_dbf_file(dbf_dir, "Item.dbf", recursive=True)
    if item_path is None:
        raise FileNotFoundError(f"Expected Item.dbf in {dbf_dir}")
    for row in iter_selected_dbf_rows(item_path, targets, selected_fields={"SKU", "QTY"}):
        sku = str(row.get("SKU") or "").strip()
        if sku not in targets or variants_by_base.get(sku):
            continue
        quantity = dbf_pos_sync.decimal_to_quantity(dbf_pos_sync.decimal_or_none(row.get("QTY")))
        quantities[sku] = int(quantity or 0)

    matrix_targets = {sku for sku, variants in variants_by_base.items() if variants}
    if not matrix_targets:
        return quantities
    quantity_path = dbf_pos_sync.find_dbf_file(dbf_dir, "Itemmqty.dbf", recursive=True)
    if quantity_path is None:
        raise FileNotFoundError(f"Expected Itemmqty.dbf in {dbf_dir} for matrix inventory")

    # Start known matrix variants at zero so a removed/cleared quantity row cannot
    # leave stale stock online. Existing variants are the only rows eligible for an
    # unattended quantity update; new product structure remains a first-import job.
    for sku in matrix_targets:
        for variant_sku in variants_by_base[sku]:
            quantities[variant_sku] = 0

    for row in iter_selected_dbf_rows(
        quantity_path,
        matrix_targets,
        selected_fields={"SKU", "CELL", "BARCODE", "QTY"},
    ):
        product_sku = str(row.get("SKU") or "").strip()
        if product_sku not in matrix_targets:
            continue
        variant_sku = matrix_variant_sku_for_row(
            product_sku,
            row,
            known_variants=variants_by_base[product_sku],
        )
        if not variant_sku:
            continue
        quantity = dbf_pos_sync.decimal_to_quantity(dbf_pos_sync.decimal_or_none(row.get("QTY")))
        quantities[variant_sku] = int(quantity or 0)
    return quantities


def matrix_variant_sku_for_row(
    product_sku: str,
    row: Dict[str, Any],
    *,
    known_variants: set[str],
) -> Optional[str]:
    barcode = str(row.get("BARCODE") or "").strip()
    if barcode in known_variants:
        return barcode

    cell = str(row.get("CELL") or "").strip()
    coordinates = re.findall(r"\d+", cell)
    if len(coordinates) == 2:
        candidate = f"{product_sku}. {int(coordinates[0])} {int(coordinates[1])}"
        if candidate in known_variants:
            return candidate

    compact_cell = "".join(coordinates)
    compact_matches = []
    for variant_sku in known_variants:
        match = MATRIX_VARIANT_SKU.match(variant_sku)
        if match and "".join(re.findall(r"\d+", variant_sku[len(match.group(1)) :])) == compact_cell:
            compact_matches.append(variant_sku)
    return compact_matches[0] if len(compact_matches) == 1 else None


def flatten_quantities(payloads: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    quantities: Dict[str, int] = {}
    for payload in payloads:
        variants = payload.get("variants") or []
        if variants:
            for variant in variants:
                sku = str(variant.get("sku") or "").strip()
                if sku:
                    quantities[sku] = int(variant.get("quantity") or 0)
            continue
        sku = str(payload.get("sku") or "").strip()
        if sku:
            quantities[sku] = int(payload.get("quantity") or 0)
    return quantities


def catalog_total_quantity(payload: Dict[str, Any]) -> int:
    variants = payload.get("variants") or []
    if variants:
        return sum(int(variant.get("quantity") or 0) for variant in variants)
    return int(payload.get("quantity") or 0)


def catalog_upload_priority(payload: Dict[str, Any]) -> int:
    return 0 if catalog_total_quantity(payload) > 0 else 1


def negative_catalog_money_field(payload: Dict[str, Any]) -> Optional[str]:
    for field in ("price", "compare_at_price", "cost"):
        value = payload.get(field)
        if value is not None and float(value) < 0:
            return field
    for index, variant in enumerate(payload.get("variants") or [], start=1):
        for field in ("price", "compare_at_price", "cost"):
            value = variant.get(field)
            if value is not None and float(value) < 0:
                return f"variants[{index}].{field}"
    return None


def ensure_local_order_schema(connection: sqlite3.Connection) -> None:
    additions = {
        "orders": {
            "customer_first_name": "TEXT",
            "customer_last_name": "TEXT",
            "billing_name": "TEXT",
            "billing_first_name": "TEXT",
            "billing_last_name": "TEXT",
            "billing_company": "TEXT",
            "billing_address1": "TEXT",
            "billing_address2": "TEXT",
            "billing_city": "TEXT",
            "billing_province": "TEXT",
            "billing_province_code": "TEXT",
            "billing_country": "TEXT",
            "billing_country_code": "TEXT",
            "billing_zip": "TEXT",
            "billing_phone": "TEXT",
            "shipping_first_name": "TEXT",
            "shipping_last_name": "TEXT",
            "import_status": "TEXT NOT NULL DEFAULT 'PENDING'",
            "imported_at": "TEXT",
            "pos_order_number": "TEXT",
            "import_error": "TEXT",
        },
        "order_items": {
            "line_number": "INTEGER NOT NULL DEFAULT 0",
            "product_id": "TEXT",
            "vendor": "TEXT",
            "line_tax": "TEXT",
            "line_total": "TEXT",
        },
    }
    for table, columns in additions.items():
        existing = {
            str(row[1]).lower()
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for column, definition in columns.items():
            if column.lower() not in existing:
                connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    connection.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_orders_import_status
            ON orders(import_status, created_at);
        DROP VIEW IF EXISTS order_header;
        CREATE VIEW order_header AS
        SELECT
            shopify_order_id AS order_id,
            order_name AS invoice_no,
            order_number,
            confirmation_number,
            created_at AS order_date,
            processed_at,
            financial_status,
            fulfillment_status,
            currency,
            customer_name,
            customer_first_name,
            customer_last_name,
            email,
            phone,
            billing_name,
            billing_first_name,
            billing_last_name,
            billing_company,
            billing_address1,
            billing_address2,
            billing_city,
            billing_province,
            billing_province_code,
            billing_country,
            billing_country_code,
            billing_zip,
            billing_phone,
            shipping_name,
            shipping_first_name,
            shipping_last_name,
            shipping_company,
            shipping_address1,
            shipping_address2,
            shipping_city,
            shipping_province,
            shipping_province_code,
            shipping_country,
            shipping_country_code,
            shipping_zip,
            shipping_phone,
            shipping_method,
            subtotal_price AS subtotal,
            total_discounts AS discount,
            shipping_price AS shipping,
            '0.00' AS handling,
            total_tax AS tax,
            total_price AS total,
            note,
            tags,
            cancelled_at,
            closed_at,
            print_status,
            printed_at,
            import_status,
            imported_at,
            pos_order_number,
            import_error,
            source_event,
            source_version,
            synced_at
        FROM orders;

        DROP VIEW IF EXISTS order_detail;
        CREATE VIEW order_detail AS
        SELECT
            i.shopify_order_id AS order_id,
            o.order_name AS invoice_no,
            i.line_number,
            i.shopify_line_item_id AS line_item_id,
            i.product_id,
            i.variant_id,
            i.sku,
            i.quantity AS qty,
            i.current_quantity,
            i.price,
            i.total_discount AS discount,
            i.line_tax AS tax,
            i.line_total AS extension,
            i.title AS description,
            i.variant_title,
            i.vendor,
            i.grams,
            i.requires_shipping,
            i.fulfillment_status
        FROM order_items AS i
        JOIN orders AS o ON o.shopify_order_id = i.shopify_order_id;
        """
    )


def local_order_address_name(address: Dict[str, Any]) -> str:
    return str(address.get("name") or "").strip() or " ".join(
        part
        for part in (
            str(address.get("first_name") or "").strip(),
            str(address.get("last_name") or "").strip(),
        )
        if part
    )


def local_order_address_columns(prefix: str, address: Dict[str, Any], name: str) -> Dict[str, Any]:
    values = {
        f"{prefix}_name": name or None,
        f"{prefix}_first_name": str(address.get("first_name") or "").strip() or None,
        f"{prefix}_last_name": str(address.get("last_name") or "").strip() or None,
        f"{prefix}_company": address.get("company"),
        f"{prefix}_address1": address.get("address1"),
        f"{prefix}_address2": address.get("address2"),
        f"{prefix}_city": address.get("city"),
        f"{prefix}_province": address.get("province"),
        f"{prefix}_province_code": address.get("province_code"),
        f"{prefix}_country": address.get("country"),
        f"{prefix}_country_code": address.get("country_code"),
        f"{prefix}_zip": address.get("zip"),
        f"{prefix}_phone": address.get("phone"),
    }
    return values


def money_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        return f"{Decimal(str(value)).quantize(Decimal('0.01'))}"
    except (InvalidOperation, ValueError):
        return str(value)


def order_line_tax(item: Dict[str, Any]) -> str:
    total = Decimal("0")
    for tax_line in item.get("tax_lines") or []:
        if not isinstance(tax_line, dict):
            continue
        value = tax_line.get("price")
        if value in (None, ""):
            price_set = tax_line.get("price_set") or {}
            shop_money = price_set.get("shop_money") if isinstance(price_set, dict) else {}
            value = shop_money.get("amount") if isinstance(shop_money, dict) else None
        try:
            total += Decimal(str(value or "0"))
        except InvalidOperation:
            continue
    return f"{total.quantize(Decimal('0.01'))}"


def calculate_line_total(price: Optional[str], quantity: int, discount: Optional[str]) -> Optional[str]:
    if price is None:
        return None
    try:
        total = (Decimal(price) * int(quantity)) - Decimal(discount or "0")
    except (InvalidOperation, ValueError):
        return None
    return f"{total.quantize(Decimal('0.01'))}"


def upsert_order_changes(path: Path, changes: List[Dict[str, Any]], *, retention_rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=30)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                shopify_order_id TEXT PRIMARY KEY,
                order_name TEXT,
                order_number TEXT,
                confirmation_number TEXT,
                created_at TEXT,
                updated_at TEXT,
                processed_at TEXT,
                cancelled_at TEXT,
                closed_at TEXT,
                financial_status TEXT,
                fulfillment_status TEXT,
                currency TEXT,
                subtotal_price TEXT,
                total_discounts TEXT,
                shipping_price TEXT,
                total_tax TEXT,
                total_price TEXT,
                customer_name TEXT,
                customer_first_name TEXT,
                customer_last_name TEXT,
                email TEXT,
                phone TEXT,
                billing_name TEXT,
                billing_first_name TEXT,
                billing_last_name TEXT,
                billing_company TEXT,
                billing_address1 TEXT,
                billing_address2 TEXT,
                billing_city TEXT,
                billing_province TEXT,
                billing_province_code TEXT,
                billing_country TEXT,
                billing_country_code TEXT,
                billing_zip TEXT,
                billing_phone TEXT,
                shipping_name TEXT,
                shipping_first_name TEXT,
                shipping_last_name TEXT,
                shipping_company TEXT,
                shipping_address1 TEXT,
                shipping_address2 TEXT,
                shipping_city TEXT,
                shipping_province TEXT,
                shipping_province_code TEXT,
                shipping_country TEXT,
                shipping_country_code TEXT,
                shipping_zip TEXT,
                shipping_phone TEXT,
                shipping_method TEXT,
                note TEXT,
                tags TEXT,
                source_event TEXT NOT NULL,
                source_version INTEGER NOT NULL,
                print_status TEXT NOT NULL DEFAULT 'PENDING',
                printed_at TEXT,
                import_status TEXT NOT NULL DEFAULT 'PENDING',
                imported_at TEXT,
                pos_order_number TEXT,
                import_error TEXT,
                synced_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS order_items (
                shopify_order_id TEXT NOT NULL,
                line_key TEXT NOT NULL,
                shopify_line_item_id TEXT,
                line_number INTEGER NOT NULL DEFAULT 0,
                product_id TEXT,
                variant_id TEXT,
                sku TEXT,
                title TEXT,
                variant_title TEXT,
                vendor TEXT,
                quantity INTEGER NOT NULL DEFAULT 0,
                current_quantity INTEGER,
                price TEXT,
                total_discount TEXT,
                line_tax TEXT,
                line_total TEXT,
                grams INTEGER,
                requires_shipping INTEGER,
                fulfillment_status TEXT,
                PRIMARY KEY(shopify_order_id, line_key),
                FOREIGN KEY(shopify_order_id) REFERENCES orders(shopify_order_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_orders_print_status ON orders(print_status, created_at);
            CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);
            """
        )
        ensure_local_order_schema(connection)
        with connection:
            for change in changes:
                order = change.get("order") or {}
                order_id = str(change.get("shopify_order_id") or order.get("id") or "").strip()
                if not order_id:
                    raise ValueError("Order change is missing shopify_order_id")
                if change.get("event_topic") in {"orders/delete", "customers/redact"} or order.get("redacted"):
                    connection.execute("DELETE FROM orders WHERE shopify_order_id = ?", (order_id,))
                    continue
                billing_address = order.get("billing_address") or {}
                shipping_address = order.get("shipping_address") or {}
                customer_first_name = str(order.get("customer_first_name") or "").strip()
                customer_last_name = str(order.get("customer_last_name") or "").strip()
                customer_name = " ".join(
                    part
                    for part in (customer_first_name, customer_last_name)
                    if part
                )
                billing_name = local_order_address_name(billing_address)
                shipping_name = local_order_address_name(shipping_address)
                shipping_method = ", ".join(
                    str(line.get("title") or line.get("code") or "").strip()
                    for line in (order.get("shipping_lines") or [])
                    if str(line.get("title") or line.get("code") or "").strip()
                )
                header_values = {
                    "shopify_order_id": order_id,
                    "order_name": order.get("name") or change.get("order_name"),
                    "order_number": str(order.get("order_number") or "") or None,
                    "confirmation_number": order.get("confirmation_number"),
                    "created_at": order.get("created_at"),
                    "updated_at": order.get("updated_at"),
                    "processed_at": order.get("processed_at"),
                    "cancelled_at": order.get("cancelled_at"),
                    "closed_at": order.get("closed_at"),
                    "financial_status": order.get("financial_status"),
                    "fulfillment_status": order.get("fulfillment_status"),
                    "currency": order.get("currency"),
                    "subtotal_price": order.get("subtotal_price"),
                    "total_discounts": order.get("total_discounts"),
                    "shipping_price": order.get("shipping_price"),
                    "total_tax": order.get("total_tax"),
                    "total_price": order.get("total_price"),
                    "customer_name": customer_name or billing_name or shipping_name or None,
                    "customer_first_name": customer_first_name or None,
                    "customer_last_name": customer_last_name or None,
                    "email": order.get("email"),
                    "phone": order.get("phone"),
                    **local_order_address_columns("billing", billing_address, billing_name),
                    **local_order_address_columns("shipping", shipping_address, shipping_name),
                    "shipping_method": shipping_method or None,
                    "note": order.get("note"),
                    "tags": order.get("tags"),
                    "source_event": change.get("event_topic") or "orders/updated",
                    "source_version": int(change.get("version") or 1),
                    "synced_at": datetime.now().astimezone().isoformat(),
                }
                columns = list(header_values)
                updates = [column for column in columns if column != "shopify_order_id"]
                connection.execute(
                    f"""
                    INSERT INTO orders ({', '.join(columns)})
                    VALUES ({', '.join('?' for _ in columns)})
                    ON CONFLICT(shopify_order_id) DO UPDATE SET
                        {', '.join(f'{column}=excluded.{column}' for column in updates)}
                    """,
                    tuple(header_values[column] for column in columns),
                )
                connection.execute("DELETE FROM order_items WHERE shopify_order_id = ?", (order_id,))
                for index, item in enumerate(order.get("line_items") or [], start=1):
                    line_id = str(item.get("id") or "").strip()
                    line_key = line_id or f"line-{index}"
                    quantity = int(item.get("quantity") or 0)
                    price = money_text(item.get("price"))
                    discount = money_text(item.get("total_discount"))
                    line_values = {
                        "shopify_order_id": order_id,
                        "line_key": line_key,
                        "shopify_line_item_id": line_id or None,
                        "line_number": index,
                        "product_id": str(item.get("product_id") or "") or None,
                        "variant_id": str(item.get("variant_id") or "") or None,
                        "sku": str(item.get("sku") or "").strip() or None,
                        "title": item.get("title"),
                        "variant_title": item.get("variant_title"),
                        "vendor": item.get("vendor"),
                        "quantity": quantity,
                        "current_quantity": int(item["current_quantity"])
                        if item.get("current_quantity") is not None
                        else None,
                        "price": price,
                        "total_discount": discount,
                        "line_tax": order_line_tax(item),
                        "line_total": calculate_line_total(price, quantity, discount),
                        "grams": int(item["grams"]) if item.get("grams") is not None else None,
                        "requires_shipping": int(bool(item["requires_shipping"]))
                        if item.get("requires_shipping") is not None
                        else None,
                        "fulfillment_status": item.get("fulfillment_status"),
                    }
                    line_columns = list(line_values)
                    connection.execute(
                        f"INSERT INTO order_items ({', '.join(line_columns)}) "
                        f"VALUES ({', '.join('?' for _ in line_columns)})",
                        tuple(line_values[column] for column in line_columns),
                    )
            connection.execute(
                """
                DELETE FROM orders
                WHERE shopify_order_id IN (
                    SELECT shopify_order_id
                    FROM orders
                    ORDER BY COALESCE(created_at, synced_at) DESC, shopify_order_id DESC
                    LIMIT -1 OFFSET ?
                )
                """,
                (max(100, int(retention_rows)),),
            )
    finally:
        connection.close()


def merge_quantity(
    entry: Dict[str, Any],
    *,
    pos_quantity: int,
    shop_quantity: int,
) -> Dict[str, int]:
    previous_pos = int(entry.get("pos_seen", pos_quantity))
    previous_shop = int(entry.get("shop_seen", shop_quantity))
    previous_canonical = int(entry.get("canonical", previous_pos))
    pos_delta = int(pos_quantity) - previous_pos
    shop_delta = int(shop_quantity) - previous_shop
    target = max(0, previous_canonical + pos_delta + shop_delta)
    return {
        "previous_pos": previous_pos,
        "previous_shop": previous_shop,
        "previous_canonical": previous_canonical,
        "pos_delta": pos_delta,
        "shop_delta": shop_delta,
        "target": target,
        "shop_adjustment": target - int(shop_quantity),
        "pos_adjustment": target - int(pos_quantity),
    }


def adjustment_key(system: str, sku: str, *values: int) -> str:
    raw = "|".join([system, sku, *(str(value) for value in values)])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def chunks(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for start in range(0, len(items), max(1, size)):
        yield items[start : start + max(1, size)]


def load_state(path: Path) -> Dict[str, Any]:
    for candidate in (path, path.with_suffix(".bak")):
        if not candidate.exists():
            continue
        try:
            data = json.loads(candidate.read_text(encoding="utf-8"))
            if isinstance(data, dict) and int(data.get("version") or 0) == STATE_VERSION:
                data.setdefault("catalog_products", [])
                data.setdefault("quantities", {})
                data.setdefault("event_cursors", {})
                data.setdefault("sku_bases", {})
                return data
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return {
        "version": STATE_VERSION,
        "catalog_complete": False,
        "catalog_products": [],
        "quantities": {},
        "event_cursors": {},
        "sku_bases": {},
    }


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    backup = path.with_suffix(".bak")
    temporary.write_text(
        json.dumps(state, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if path.exists():
        shutil.copy2(path, backup)
    os.replace(temporary, path)


def configure_logging(path: Path, *, max_bytes: int, backup_count: int) -> logging.Logger:
    logger = logging.getLogger("windows_connector")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def find_powershell() -> str:
    system_root = Path(os.getenv("SystemRoot") or r"C:\Windows")
    candidates = [
        system_root / "SysWOW64" / "WindowsPowerShell" / "v1.0" / "powershell.exe",
        system_root / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return "powershell.exe"


def main() -> int:
    args = parse_cli()
    try:
        connector = Connector(config_path=Path(args.config).expanduser().resolve(), dry_run=args.dry_run)
        return connector.run_forever(once=args.once)
    except Exception as exc:
        print(f"Connector failed to start: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
