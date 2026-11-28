#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import struct
import subprocess
import sys
import time
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Event, Lock, Thread
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional

import requests


CONNECTOR_DIR = Path(__file__).resolve().parent
PROJECT_DIR = CONNECTOR_DIR.parent
POS_READER_DIR = PROJECT_DIR / "jbarbaro_db"
sys.path.insert(0, str(POS_READER_DIR))

import dbf_pos_sync  # noqa: E402

if __package__:
    from .sales_board import build_sales_board, sales_paths, sales_source_signature
    from .order_dbf import (
        migrate_legacy_sqlite_database,
        money_text,
        read_order_dbfs,
        remove_orders_from_legacy_sqlite,
        upsert_order_changes,
    )
else:
    from sales_board import build_sales_board, sales_paths, sales_source_signature
    from order_dbf import (
        migrate_legacy_sqlite_database,
        money_text,
        read_order_dbfs,
        remove_orders_from_legacy_sqlite,
        upsert_order_changes,
    )


STATE_VERSION = 1
CONNECTOR_VERSION = "1.4.6"
MATRIX_OPTION_SCHEMA_VERSION = 2
NEW_PRODUCT_STABLE_OBSERVATIONS = 2
CATALOG_STRUCTURE_SCHEMA_VERSION = 1
MATRIX_STRUCTURE_PROBE_COOLDOWN_SECONDS = 900
INVENTORY_RECONCILE_SCHEMA_VERSION = 2
INVENTORY_SNAPSHOT_SCHEMA_VERSION = 2
POS_EVENT_FILES = ("invdtl.dbf", "editvoid.dbf")
POS_INVENTORY_QUIET_SECONDS = 300
INVENTORY_MAINTENANCE_MAX_WAIT_SECONDS = 1800
MATRIX_VARIANT_SKU = re.compile(r"^(.+?)\.\s*\d+\s+\d+$")
ACTIVE_MATRIX_STRUCTURE_STAGES = {"candidate", "verification"}


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


def has_active_matrix_structure_repairs(state: Dict[str, Any]) -> bool:
    pending = state.get("pending_matrix_structure_repairs") or {}
    return any(
        isinstance(entry, dict)
        and entry.get("stage") in ACTIVE_MATRIX_STRUCTURE_STAGES
        for entry in pending.values()
    )


def matrix_structure_entry_skus(
    base: str,
    entry: Optional[Dict[str, Any]],
) -> set[str]:
    skus = {str(base or "").strip()}
    if not isinstance(entry, dict):
        return {sku for sku in skus if sku}
    for field in ("expected_child_skus", "protected_skus"):
        values = entry.get(field) or []
        if isinstance(values, list):
            skus.update(str(value or "").strip() for value in values)
    return {sku for sku in skus if sku}


def pending_matrix_structure_skus(state: Dict[str, Any]) -> set[str]:
    pending = state.get("pending_matrix_structure_repairs") or {}
    protected: set[str] = set()
    for base, entry in pending.items():
        if isinstance(entry, dict):
            protected.update(matrix_structure_entry_skus(str(base), entry))
    return protected


def snapshot_overlay_ignored_matrix_skus(state: Dict[str, Any]) -> set[str]:
    """Keep obsolete matrix identities out of snapshot/webhook comparisons."""
    pending = state.get("pending_matrix_structure_repairs") or {}
    ignored: set[str] = set()
    for base, entry in pending.items():
        if not isinstance(entry, dict):
            continue
        related = matrix_structure_entry_skus(str(base), entry)
        if entry.get("stage") == "quantity_reconciliation":
            expected_children = {
                str(sku or "").strip()
                for sku in entry.get("expected_child_skus") or []
                if str(sku or "").strip()
            }
            ignored.update(related - expected_children)
        else:
            ignored.update(related)
    return ignored


def validate_order_dbf_paths(header_path: Path, detail_path: Path) -> None:
    paths = (header_path, detail_path)
    if any(path.suffix.lower() != ".dbf" for path in paths):
        raise ValueError("Shopify order header and detail paths must end in .dbf")
    if header_path == detail_path:
        raise ValueError("Shopify order header and detail paths must be different files")
    if header_path.parent != detail_path.parent:
        raise ValueError("Shopify order header and detail paths must use the same directory")
    native_order_files = {"ordhdr.dbf", "orddtl.dbf"}
    if any(path.name.lower() in native_order_files for path in paths):
        raise ValueError("Shopify order DBFs must not overwrite native Ordhdr.dbf or Orddtl.dbf")


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
        self.interval_seconds = env_int("SYNC_INTERVAL_SECONDS", 30, minimum=30, maximum=86400)
        self.customer_sync_enabled = env_bool("CUSTOMER_SYNC_ENABLED", True)
        self.customer_sync_interval_seconds = env_int(
            "CUSTOMER_SYNC_INTERVAL_SECONDS", 60, minimum=30, maximum=86400,
        )
        self._sync_request_lock = Lock()
        self._sync_request_id: Optional[str] = None
        self._sync_progress_stage: Optional[str] = None
        self._sync_wakeup = Event()
        self._customer_wakeup = Event()
        self._customer_source_stamp: tuple[str, str | None] | None = None
        self._automatic_inventory_retry_at = 0.0
        self.nightly_full_sync_hour = env_int("NIGHTLY_FULL_SYNC_HOUR", 0, minimum=0, maximum=23)
        self.batch_size = env_int("CONNECTOR_BATCH_SIZE", 25, minimum=1, maximum=100)
        self.workers = env_int("CONNECTOR_WORKERS", 2, minimum=1, maximum=4)
        self.timeout = env_int("CONNECTOR_TIMEOUT_SECONDS", 300, minimum=30, maximum=1800)
        self.initial_catalog_upload = env_bool("INITIAL_CATALOG_UPLOAD", True)
        self.price_sync_enabled = env_bool("PRICE_SYNC_ENABLED", True)
        self.order_sync_enabled = env_bool("ORDER_SYNC_ENABLED", True)
        legacy_order_setting = (os.getenv("SHOPIFY_ORDER_DB_PATH") or "").strip()
        legacy_order_path = (
            Path(legacy_order_setting).expanduser().resolve()
            if legacy_order_setting
            else self.dbf_dir / "shopify-orders.db"
        )
        order_dbf_parent = self.dbf_dir.with_name(f"{self.dbf_dir.name}_web")
        header_setting = (os.getenv("SHOPIFY_ORDER_HEADER_DBF_PATH") or "").strip()
        detail_setting = (os.getenv("SHOPIFY_ORDER_DETAIL_DBF_PATH") or "").strip()
        legacy_header_path = (self.dbf_dir / "shopify-order-header.dbf").resolve()
        legacy_detail_path = (self.dbf_dir / "shopify-order-detail.dbf").resolve()
        if header_setting and Path(header_setting).expanduser().resolve() == legacy_header_path:
            header_setting = ""
        if detail_setting and Path(detail_setting).expanduser().resolve() == legacy_detail_path:
            detail_setting = ""
        if header_setting and not detail_setting:
            order_dbf_parent = Path(header_setting).expanduser().resolve().parent
        elif detail_setting and not header_setting:
            order_dbf_parent = Path(detail_setting).expanduser().resolve().parent
        self.order_header_path = Path(
            header_setting or (order_dbf_parent / "shopify-order-header.dbf")
        ).expanduser().resolve()
        self.order_detail_path = Path(
            detail_setting or (order_dbf_parent / "shopify-order-detail.dbf")
        ).expanduser().resolve()
        validate_order_dbf_paths(self.order_header_path, self.order_detail_path)
        legacy_candidates = [
            legacy_order_path,
            self.dbf_dir / "shopify-orders.db",
            self.dbf_dir / "shopify-order.db",
        ]
        self.legacy_order_db_paths = list(dict.fromkeys(path.resolve() for path in legacy_candidates))
        self.order_retention_rows = env_int("ORDER_DB_RETENTION_ROWS", 250, minimum=100, maximum=500)
        self.order_dbfs_initialized = False
        self.order_bridge_status_checked = False
        self.writeback_mode = (os.getenv("POS_WRITEBACK_MODE") or "disabled").strip().lower()
        if self.writeback_mode not in {"disabled", "dry-run", "vfp-oledb"}:
            raise ValueError("POS_WRITEBACK_MODE must be disabled, dry-run, or vfp-oledb")
        self.dry_run = dry_run

        data_dir = Path(os.getenv("CONNECTOR_DATA_DIR") or (CONNECTOR_DIR / "data")).expanduser().resolve()
        data_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = data_dir / "state.json"
        self.customer_state_path = data_dir / "customers.json"
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
                "User-Agent": f"shopify-pos-windows-connector/{CONNECTOR_VERSION}",
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
            "connector_started version=%s dbf_dir=%s interval=%ss writeback=%s price_sync=%s dry_run=%s",
            CONNECTOR_VERSION,
            self.dbf_dir,
            self.interval_seconds,
            self.writeback_mode,
            self.price_sync_enabled,
            self.dry_run,
        )
        stop = Event()
        workers: List[Thread] = []
        if not once:
            workers.append(Thread(target=self._heartbeat_loop, args=(stop,), daemon=True))
            if self.order_sync_enabled:
                workers.append(Thread(target=self._order_loop, args=(stop,), daemon=True))
            if self.customer_sync_enabled:
                workers.append(Thread(target=self._customer_loop, args=(stop,), daemon=True))
            if not self.dry_run:
                workers.append(Thread(target=self._sales_board_loop, args=(stop,), daemon=True))
            for worker in workers:
                worker.start()
        try:
            while True:
                self._sync_wakeup.clear()
                started = time.monotonic()
                if once:
                    try:
                        self._send_heartbeat(self.session)
                    except requests.RequestException:
                        self.logger.warning("connector_heartbeat_failed")
                    if self.order_sync_enabled:
                        self._sync_order_inbox()
                failed = False
                with self._sync_request_lock:
                    request_id = self._sync_request_id
                try:
                    if request_id:
                        self.run_cycle(force_full=True, sync_request_id=request_id)
                        self._complete_sync_request(request_id)
                    else:
                        self.run_cycle(scheduled=not once)
                except Exception:
                    self.logger.exception("connector_cycle_failed")
                    if request_id:
                        self._set_sync_progress(request_id, "retrying")
                    failed = True
                if once:
                    if self.customer_sync_enabled:
                        try:
                            self._sync_customers(self.session)
                        except Exception:
                            self.logger.warning("customer_sync_failed; check customer access and POS files")
                            failed = True
                    return 1 if failed else 0
                elapsed = time.monotonic() - started
                self._sync_wakeup.wait(max(1.0, self.interval_seconds - elapsed))
        finally:
            stop.set()
            self._customer_wakeup.set()
            for worker in workers:
                worker.join(timeout=1)

    def _send_heartbeat(self, session: requests.Session) -> None:
        payload: Dict[str, Any] = {
            "version": CONNECTOR_VERSION,
            "sync_interval_seconds": self.interval_seconds,
            "order_sync_enabled": self.order_sync_enabled,
            "customer_sync_enabled": self.customer_sync_enabled and not self.dry_run,
            "customer_sync_interval_seconds": self.customer_sync_interval_seconds,
        }
        with self._sync_request_lock:
            if self._sync_request_id and self._sync_progress_stage:
                payload["sync_progress"] = {
                    "request_id": self._sync_request_id,
                    "stage": self._sync_progress_stage,
                }
        response = session.post(
            f"{self.base_url}/sync/heartbeat",
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        request_id = response.json().get("sync_request_id")
        if isinstance(request_id, str) and request_id:
            with self._sync_request_lock:
                if request_id != self._sync_request_id:
                    self._sync_request_id = request_id
                    self._sync_progress_stage = None
                    self._sync_wakeup.set()
                    self._customer_wakeup.set()
        self._flush_updater_reports(session)

    def _flush_updater_reports(self, session: requests.Session) -> None:
        directory = self.state_path.parent / "updater-reports"
        try:
            reports = sorted(directory.glob("*.json"))[:3]
        except OSError:
            self.logger.warning("updater_report_delivery_pending")
            return
        for path in reports:
            try:
                report = json.loads(path.read_text(encoding="utf-8-sig"))
                response = session.post(
                    f"{self.base_url}/sync/connector/update-report",
                    json=report,
                    timeout=5,
                    allow_redirects=False,
                )
                response.raise_for_status()
                result = response.json()
                if not isinstance(result, dict) or result.get("accepted") is not True:
                    raise ValueError("Updater report was not accepted")
                path.unlink(missing_ok=True)
            except FileNotFoundError:
                # The updater may have sent and removed this report concurrently.
                continue
            except (OSError, ValueError, requests.RequestException):
                self.logger.warning("updater_report_delivery_pending")
                break

    def _complete_sync_request(self, request_id: str) -> None:
        response = self.session.post(
            f"{self.base_url}/sync/requests/{request_id}/ack", timeout=30,
        )
        response.raise_for_status()
        if response.json().get("completed") is True:
            with self._sync_request_lock:
                if self._sync_request_id == request_id:
                    self._sync_request_id = None
                    self._sync_progress_stage = None
            self.logger.info("forced_inventory_scan_completed")

    def _set_sync_progress(self, request_id: str, stage: str) -> None:
        with self._sync_request_lock:
            if self._sync_request_id == request_id:
                self._sync_progress_stage = stage

    def _heartbeat_loop(self, stop: Event) -> None:
        # Requests sessions are owned by one thread; long imports cannot delay check-ins.
        with requests.Session() as session:
            session.headers.update(self.session.headers)
            while not stop.is_set():
                try:
                    self._send_heartbeat(session)
                except requests.RequestException:
                    self.logger.warning("connector_heartbeat_failed")
                stop.wait(30)

    def _customer_loop(self, stop: Event) -> None:
        with requests.Session() as session:
            session.headers.update(self.session.headers)
            while not stop.is_set():
                with self._sync_request_lock:
                    force_scan = self._customer_wakeup.is_set()
                    self._customer_wakeup.clear()
                try:
                    self._sync_customers(session, force_scan=force_scan)
                except Exception:
                    # Responses can contain customer contact data; do not log them.
                    self.logger.warning("customer_sync_failed; check customer access and POS files")
                self._customer_wakeup.wait(self.customer_sync_interval_seconds)

    def _order_loop(self, stop: Event) -> None:
        with requests.Session() as session:
            session.headers.update(self.session.headers)
            while not stop.is_set():
                self._sync_order_inbox(session)
                stop.wait(self.interval_seconds)

    def _sales_board_loop(self, stop: Event) -> None:
        snapshot = None
        signature = None
        last_sent = None
        history_built_on = None
        sent_report_months: dict[str, str] = {}
        pending_report_months: dict[str, tuple[str, list]] = {}
        with requests.Session() as session:
            session.headers.update(self.session.headers)
            while not stop.is_set():
                interval = 60
                try:
                    config = session.get(f"{self.base_url}/sync/sales-board/settings", timeout=15)
                    config.raise_for_status()
                    remote = config.json()
                    if not remote["enabled"]:
                        last_sent = None
                        stop.wait(interval)
                        continue
                    interval = min(900, max(30, int(remote["interval_seconds"])))
                    today = date.fromisoformat(remote["business_date"])
                    paths = sales_paths(self.dbf_dir)
                    vendor_path = dbf_pos_sync.find_dbf_file(self.dbf_dir, "Vendor.dbf", recursive=True)
                    customers_path = dbf_pos_sync.find_dbf_file(self.dbf_dir, "Customer.dbf", recursive=True)
                    sources = (*paths, *(path for path in (vendor_path, customers_path) if path is not None))
                    current = (today, sales_source_signature(sources))
                    if current != signature:
                        report = build_sales_board(
                            paths, today=today, vendors_path=vendor_path, customers_path=customers_path,
                            history_start=date.min if history_built_on != today else today.replace(day=1),
                        )
                        if sales_source_signature(sources) != current[1]:
                            raise OSError("Sales DBFs changed while reporting; retrying next check.")
                        for month, lines in report.pop("history").items():
                            digest = hashlib.sha256(json.dumps(lines, separators=(",", ":")).encode()).hexdigest()
                            if sent_report_months.get(month) == digest:
                                pending_report_months.pop(month, None)
                            else:
                                pending_report_months[month] = (digest, lines)
                        history_built_on = today
                        snapshot = report
                        signature = current
                        last_sent = None
                    if last_sent is None or time.monotonic() - last_sent >= 300:
                        snapshot["observed_at"] = datetime.now(timezone.utc).isoformat()
                        response = session.post(f"{self.base_url}/sync/sales-board", json=snapshot, timeout=30)
                        response.raise_for_status()
                        if last_sent is None and response.json().get("saved") is True:
                            self.logger.info("sales_board_refreshed department_file=%s", paths[-1].name)
                        last_sent = time.monotonic()
                    while pending_report_months and not stop.is_set():
                        months = sorted(pending_report_months, reverse=True)[:6]
                        response = session.post(
                            f"{self.base_url}/sync/sales-reports",
                            json={"observed_at": snapshot["observed_at"],
                                  "months": {month: pending_report_months[month][1] for month in months}},
                            timeout=60,
                        )
                        response.raise_for_status()
                        if not response.json().get("accepted"):
                            break
                        for month in months:
                            sent_report_months[month], _ = pending_report_months.pop(month)
                except Exception:
                    self.logger.exception("sales_board_sync_failed")
                stop.wait(interval)

    def _sync_customers(self, session: requests.Session, *, force_scan: bool = False) -> None:
        if not self.customer_sync_enabled:
            return
        if not self.dry_run:
            response = session.get(f"{self.base_url}/sync/customers/status", timeout=30)
            response.raise_for_status()
            if response.json().get("authorized") is not True:
                self.logger.warning("customer_sync_needs_read_customers_and_write_customers; reauthorize Shopify app")
                return

        customer_stamp = dbf_file_stamp(dbf_pos_sync.find_dbf_file(self.dbf_dir, "Customer.dbf", recursive=True))
        shipping_stamp = dbf_file_stamp(dbf_pos_sync.find_dbf_file(self.dbf_dir, "CustShip.dbf", recursive=True))
        source_stamp = (customer_stamp, shipping_stamp) if customer_stamp else None
        if not self.dry_run and not force_scan and source_stamp and source_stamp == self._customer_source_stamp:
            return
        self._customer_source_stamp = None

        payloads, _audits, _stats = dbf_pos_sync.load_customers(SimpleNamespace(
            dbf_dir=str(self.dbf_dir), customer_scope="primary",
            customer_tag=None, customer_limit=None,
        ))
        state = load_state(self.customer_state_path)
        fingerprints = state.setdefault("customer_hashes", {})
        observed: set[str] = set()
        changed: List[Dict[str, Any]] = []
        offset = int(state.get("next_offset") or 0) % max(1, len(payloads))
        ordered = payloads[offset:] + payloads[:offset]
        scanned = 0
        for payload in ordered:
            scanned += 1
            number = str(payload.get("pos_customer_number") or "").strip()
            if not number:
                continue
            identity = hashlib.sha256(
                f"{payload.get('source') or 'Customer.dbf'}:{number}".encode("utf-8")
            ).hexdigest()
            observed.add(identity)
            fingerprint = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            if fingerprints.get(identity) != fingerprint:
                changed.append({"identity": identity, "fingerprint": fingerprint, "payload": payload})
            # Bound each pass while rotating past failed records to avoid starvation.
            if len(changed) == 50:
                break
        if self.dry_run:
            self.logger.info("customer_sync_dry_run pending_in_batch=%s", len(changed))
            return

        state["next_offset"] = (offset + scanned) % max(1, len(payloads))
        if scanned == len(payloads):
            # A complete scan can discard hashes for removed POS records without deleting Shopify customers.
            state["customer_hashes"] = fingerprints = {
                key: value for key, value in fingerprints.items() if key in observed
            }
        save_state(self.customer_state_path, state)
        succeeded = 0
        failure_reasons: Counter[str] = Counter()
        for batch in chunks(changed, min(10, self.batch_size)):
            response = session.post(
                f"{self.base_url}/sync/customers",
                json={"customers": [entry["payload"] for entry in batch]},
                timeout=min(self.timeout, 120),
            )
            response.raise_for_status()
            results = response.json().get("results") or []
            failure_reasons.update(
                str(row.get("failure_reason") or "Customer import rejected")
                for row in results if isinstance(row, dict) and row.get("success") is False
            )
            for entry in batch:
                payload = entry["payload"]
                confirmed = any(
                    row.get("success") is True and row.get("customer_id")
                    and row.get("pos_customer_number") == payload["pos_customer_number"]
                    and (row.get("source") or "Customer.dbf") == (payload.get("source") or "Customer.dbf")
                    for row in results if isinstance(row, dict)
                )
                if confirmed:
                    fingerprints[entry["identity"]] = entry["fingerprint"]
                    succeeded += 1
            save_state(self.customer_state_path, state)
        if scanned == len(payloads) and succeeded == len(changed):
            self._customer_source_stamp = source_stamp
        self.logger.info("customer_sync_checked pending_in_batch=%s succeeded=%s", len(changed), succeeded)
        if failure_reasons:
            self.logger.warning("customer_sync_failures %s", dict(failure_reasons))

    def run_cycle(
        self, *, force_full: bool = False, scheduled: bool = False, sync_request_id: Optional[str] = None,
    ) -> None:
        if sync_request_id:
            self._set_sync_progress(sync_request_id, "reading_pos")
        state = load_state(self.state_path)
        now = datetime.now()
        now_epoch = int(time.time())
        protected_pending_skus = pending_matrix_structure_skus(state)
        structure_schema_outdated = (
            int(state.get("catalog_structure_schema_version") or 0)
            < CATALOG_STRUCTURE_SCHEMA_VERSION
        )
        structure_scan_required = structure_schema_outdated and not protected_pending_skus
        if not self.dry_run:
            if structure_scan_required:
                # The first structure-upgrade scan has not identified matrix SKUs
                # yet. Defer every legacy action once instead of risking a stale
                # zero write before the full POS payload and Shopify shape are read.
                self.logger.warning("matrix_structure_pending_retries_deferred")
            else:
                self._retry_pending(
                    state,
                    excluded_skus=protected_pending_skus,
                )

        inventory_changes = self._fetch_inventory_changes()
        # Catalog and price work can save state before stock changes are planned.
        # Commit these cursors only when the inventory changes are saved as well.
        sku_bases: Dict[str, str] = state.setdefault("sku_bases", {})
        event_scan_state = {
            "event_cursors": state["event_cursors"].copy(),
            "sku_bases": sku_bases,
        }
        new_event_skus, event_file_reset = self._collect_pos_event_skus(event_scan_state)
        event_skus = new_event_skus | set(state.get("deferred_pos_event_skus") or [])
        if event_file_reset:
            state["pos_event_reconcile_pending"] = True
        if new_event_skus:
            state["last_pos_inventory_activity_epoch"] = now_epoch
        known_products = set(state.get("catalog_products") or [])
        new_numeric_skus: set[str] = set()
        verification_skus = set(state.get("inventory_webhook_verification_skus") or [])
        waiting_for_pos_skus = set(state.get("inventory_webhook_waiting_pos_skus") or [])
        queued_shopify_skus = {
            sku
            for change in inventory_changes
            if (sku := str(change.get("sku") or "").strip())
        }
        shopify_skus = {sku_bases.get(sku, base_sku(sku)) for sku in queued_shopify_skus}
        fresh_shopify_skus = queued_shopify_skus - verification_skus - waiting_for_pos_skus
        prioritize_inventory = scheduled and bool(event_skus or fresh_shopify_skus)
        if state.get("catalog_complete") and not prioritize_inventory:
            new_numeric_skus = self._collect_new_numeric_product_skus(
                state,
                known_products=known_products,
            )
        structure_probe_skus = set(state.get("matrix_structure_probe_skus") or [])
        affected_base_skus = event_skus | shopify_skus | new_numeric_skus

        catalog_incomplete = not bool(state.get("catalog_complete"))
        matrix_option_repair_due = (
            int(state.get("matrix_option_schema_version") or 0)
            < MATRIX_OPTION_SCHEMA_VERSION
        )
        nightly_due = nightly_full_sync_due(
            state.get("last_full_reconcile_date"),
            now=now,
            hour=self.nightly_full_sync_hour,
        )
        status_reconcile_due = (
            now.hour >= self.nightly_full_sync_hour
            and (
                state.get("last_product_status_reconcile_date") != now.date().isoformat()
                or bool(state.get("pending_product_status_skus"))
            )
            and (
                nightly_due
                or bool(state.get("pending_product_status_reconcile_date"))
                or bool(state.get("pending_product_status_skus"))
            )
        )
        inventory_reconcile_upgrade_due = (
            int(state.get("inventory_reconcile_schema_version") or 0)
            < INVENTORY_RECONCILE_SCHEMA_VERSION
        )
        catalog_structure_upgrade_due = (
            int(state.get("catalog_structure_schema_version") or 0)
            < CATALOG_STRUCTURE_SCHEMA_VERSION
            or has_active_matrix_structure_repairs(state)
            or bool(structure_probe_skus)
        )
        full_reconcile = (
            force_full
            or catalog_incomplete
            or nightly_due
            or bool(state.get("pos_event_reconcile_pending"))
            or bool(verification_skus)
            or matrix_option_repair_due
            or inventory_reconcile_upgrade_due
            or catalog_structure_upgrade_due
            or status_reconcile_due
        )
        last_activity = min(
            now_epoch, int(state.get("last_pos_inventory_activity_epoch") or 0),
        )
        pos_quiet = now_epoch - last_activity >= POS_INVENTORY_QUIET_SECONDS
        scheduled_maintenance = (
            scheduled and not force_full and not catalog_incomplete and not structure_scan_required
        )
        if scheduled_maintenance and full_reconcile:
            due_since = min(now_epoch, int(state.get("inventory_maintenance_due_since") or now_epoch))
            state["inventory_maintenance_due_since"] = due_since
            overdue = now_epoch - due_since >= INVENTORY_MAINTENANCE_MAX_WAIT_SECONDS
            can_start_scan = overdue or (pos_quiet and not prioritize_inventory)
            full_reconcile = (
                can_start_scan
                and time.monotonic() >= self._automatic_inventory_retry_at
            )
            if full_reconcile:
                # Give the event path time to run if a long Shopify scan fails.
                self._automatic_inventory_retry_at = time.monotonic() + POS_INVENTORY_QUIET_SECONDS
        elif not full_reconcile:
            state.pop("inventory_maintenance_due_since", None)
        if full_reconcile:
            if force_full:
                read_mode = "manual"
            elif catalog_incomplete:
                read_mode = "initial"
            elif catalog_structure_upgrade_due:
                read_mode = "matrix-structure-repair"
            elif matrix_option_repair_due:
                read_mode = "matrix-option-repair"
            elif inventory_reconcile_upgrade_due:
                read_mode = "inventory-reconcile-upgrade"
            elif verification_skus:
                read_mode = "webhook-verification"
            else:
                read_mode = "nightly"
        elif affected_base_skus:
            read_mode = "new-products" if new_numeric_skus else "events"
        else:
            read_mode = "idle"

        quantity_source_stamp = None
        completed_quantity_scan_stamp = None
        if (
            scheduled and not full_reconcile and not prioritize_inventory and pos_quiet
            and not structure_schema_outdated
        ):
            quantity_source_stamp = self._pos_quantity_source_stamp()
            if quantity_source_stamp and quantity_source_stamp != state.get("pos_quantity_source_stamp"):
                read_mode = "idle-quantities"

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
        elif new_numeric_skus:
            prepared_products, stats = dbf_pos_sync.load_products(
                self._reader_args(new_numeric_skus)
            )
            for prepared in prepared_products:
                invalid_field = negative_catalog_money_field(prepared.payload)
                if invalid_field:
                    self.logger.warning(
                        "new_product_skipped_negative_money sku=%s field=%s",
                        prepared.payload.get("sku"),
                        invalid_field,
                    )
                    continue
                payloads.append(prepared.payload)
            skipped_non_sellable = stats.skipped_non_sellable
            payloads.sort(key=catalog_upload_priority)

        payload_by_base = {str(payload["sku"]): payload for payload in payloads}
        discovered_sku_bases = sku_base_mapping(payloads)
        if full_reconcile:
            state["sku_bases"] = discovered_sku_bases
            sku_bases = discovered_sku_bases
        else:
            sku_bases.update(discovered_sku_bases)
        if full_reconcile:
            local_quantities = flatten_quantities(payloads)
        elif read_mode == "idle-quantities":
            local_quantities = read_targeted_pos_quantities(
                self.dbf_dir,
                known_products,
                sku_bases=sku_bases,
                include_missing_matrix_rows=False,
            )
            if quantity_source_stamp == self._pos_quantity_source_stamp():
                completed_quantity_scan_stamp = quantity_source_stamp
            else:
                local_quantities = {}
                self.logger.info("pos_idle_quantity_scan_deferred reason=files_changed_during_read")
        else:
            local_quantities = read_targeted_pos_quantities(
                self.dbf_dir,
                affected_base_skus,
                sku_bases=sku_bases,
                include_missing_matrix_rows=False,
            )
        self.logger.info(
            "pos_read mode=%s requested_skus=%s products=%s quantities=%s skipped_non_sellable=%s",
            read_mode,
            len(known_products) if read_mode == "idle-quantities" else len(affected_base_skus),
            len(payloads) if full_reconcile else len(affected_base_skus),
            len(local_quantities),
            skipped_non_sellable,
        )

        new_payloads = [
            payload
            for sku, payload in payload_by_base.items()
            if sku not in known_products
        ]
        if not catalog_incomplete and new_numeric_skus:
            numeric_payloads = [
                payload
                for payload in new_payloads
                if str(payload.get("sku") or "") in new_numeric_skus
            ]
            stable_numeric_payloads = stable_catalog_payloads(numeric_payloads, state)
            stable_numeric_skus = {
                str(payload.get("sku") or "")
                for payload in stable_numeric_payloads
            }
            waiting_count = len(numeric_payloads) - len(stable_numeric_payloads)
            new_payloads = [
                payload
                for payload in new_payloads
                if str(payload.get("sku") or "") not in new_numeric_skus
                or str(payload.get("sku") or "") in stable_numeric_skus
            ]
            if waiting_count:
                self.logger.info(
                    "catalog_upload_waiting_for_stable products=%s required_observations=%s",
                    waiting_count,
                    NEW_PRODUCT_STABLE_OBSERVATIONS,
                )
        new_base_skus = [str(payload["sku"]) for payload in new_payloads]
        uploaded_inventory_skus: set[str] = set()
        if new_base_skus and self.initial_catalog_upload:
            if self.dry_run:
                self.logger.info(
                    "catalog_upload_dry_run mode=%s products=%s",
                    "initial" if catalog_incomplete else "new-products",
                    len(new_payloads),
                )
            else:
                succeeded = self._upload_catalog(new_payloads, state=state)
                uploaded_inventory_skus = set(
                    flatten_quantities(
                        payload
                        for payload in new_payloads
                        if str(payload.get("sku") or "") in succeeded
                    )
                )
                known_products.update(succeeded)
                pending_products = state.setdefault("pending_catalog_products", {})
                for sku in succeeded:
                    pending_products.pop(sku, None)
                state["catalog_products"] = sorted(known_products, key=str.casefold)
                if catalog_incomplete:
                    state["catalog_complete"] = len(known_products) >= len(payload_by_base)
                save_state(self.state_path, state)
                self.logger.info(
                    "catalog_upload_complete mode=%s attempted=%s succeeded=%s remaining=%s",
                    "initial" if catalog_incomplete else "new-products",
                    len(new_payloads),
                    len(succeeded),
                    max(0, len(new_payloads) - len(succeeded)),
                )
        elif catalog_incomplete and new_base_skus and not self.initial_catalog_upload:
            known_products.update(new_base_skus)
            state["catalog_products"] = sorted(known_products, key=str.casefold)
            state["catalog_complete"] = True
        elif catalog_incomplete and not new_base_skus:
            state["catalog_complete"] = True

        if matrix_option_repair_due:
            repair_items = matrix_length_repair_candidates(payloads)
            if self.dry_run:
                self.logger.info(
                    "matrix_option_repair_dry_run candidates=%s",
                    len(repair_items),
                )
            else:
                repair_summary = self._repair_matrix_options(repair_items)
                state["matrix_option_schema_version"] = MATRIX_OPTION_SCHEMA_VERSION
                save_state(self.state_path, state)
                self.logger.info(
                    "matrix_option_repair_complete candidates=%s updated=%s already_correct=%s failed=%s",
                    len(repair_items),
                    repair_summary["updated"],
                    repair_summary["already_correct"],
                    repair_summary["failed"],
                )

        if self.price_sync_enabled and (full_reconcile or not prioritize_inventory):
            self._sync_pos_price_changes(state, force_scan=full_reconcile)

        ready_structure_repairs: List[Dict[str, Any]] = []
        if (
            full_reconcile
            and not catalog_incomplete
            and bool(state.get("catalog_complete"))
        ):
            ready_structure_repairs = self._ready_matrix_structure_repairs(
                state,
                payloads,
                known_products=known_products,
            )
            if ready_structure_repairs:
                if self.dry_run:
                    self.logger.info(
                        "matrix_structure_repair_dry_run candidates=%s",
                        len(ready_structure_repairs),
                    )
                else:
                    structure_statuses = self._repair_matrix_structures(
                        ready_structure_repairs,
                    )
                    self._record_matrix_structure_repair_results(
                        state,
                        ready_structure_repairs,
                        structure_statuses,
                    )

        inventory_snapshot: Optional[Dict[str, Any]] = None
        verified_webhook_skus: set[str] = set()
        deferred_webhook_skus: set[str] = set()
        if (
            full_reconcile
            and not catalog_incomplete
            and bool(state.get("catalog_complete"))
        ):
            versions_before_snapshot = {(change["id"], change["version"]) for change in inventory_changes}
            if sync_request_id:
                self._set_sync_progress(sync_request_id, "reading_shopify")
            inventory_snapshot = self._fetch_inventory_snapshot()
            # Re-read the versioned queue after the snapshot. A Shopify sale can
            # arrive while the paginated snapshot is loading, and the later
            # observation must take precedence over an older snapshot row.
            inventory_changes = self._fetch_inventory_changes()

        if sync_request_id:
            self._set_sync_progress(sync_request_id, "reconciling")
        entries: Dict[str, Dict[str, Any]] = state.setdefault("quantities", {})
        remote_quantities = (
            {}
            if inventory_snapshot is not None
            else {
                sku: int(entries[sku].get("shop_seen") or 0)
                for sku in local_quantities
                if sku in entries
            }
        )
        remote_updated_at: Dict[str, str] = {}
        snapshot_overlay_deltas: Dict[str, int] = {}
        blocked_inventory_skus = set(state.get("blocked_inventory_skus") or [])
        catalog_structure_scan_complete = False
        structure_rebased_skus: set[str] = set()
        if inventory_snapshot is None:
            waiting_for_pos_skus = (queued_shopify_skus & entries.keys()) - local_quantities.keys()
            deferred_webhook_skus.update(waiting_for_pos_skus)
            blocked_inventory_skus.update(protected_pending_skus)
            inventory_items_by_sku: Dict[str, set[str]] = {}
            for change in inventory_changes:
                sku = str(change.get("sku") or "").strip()
                inventory_item_id = str(change.get("inventory_item_id") or "").strip()
                if sku and inventory_item_id:
                    inventory_items_by_sku.setdefault(sku, set()).add(inventory_item_id)
            newly_ambiguous_skus = {
                sku
                for sku, inventory_item_ids in inventory_items_by_sku.items()
                if len(inventory_item_ids) > 1
            }
            if newly_ambiguous_skus:
                blocked_inventory_skus.update(newly_ambiguous_skus)
                state["blocked_inventory_skus"] = sorted(
                    blocked_inventory_skus,
                    key=str.casefold,
                )
                for sku in sorted(newly_ambiguous_skus, key=str.casefold):
                    self.logger.error("inventory_webhook_duplicate_sku sku=%s", sku)
            for sku in blocked_inventory_skus:
                remote_quantities.pop(sku, None)
            for change in inventory_changes:
                sku = str(change.get("sku") or "").strip()
                if not sku or sku in blocked_inventory_skus:
                    continue
                if sku in verification_skus:
                    deferred_webhook_skus.add(sku)
                    remote_quantities.pop(sku, None)
                    continue
                if sku in waiting_for_pos_skus:
                    self.logger.debug("inventory_webhook_pos_row_unavailable sku=%s", sku)
                    continue
                quantity = int(change.get("quantity") or 0)
                source_updated_at = str(change.get("source_updated_at") or "").strip()
                previous_updated_at = remote_updated_at.get(sku) or str(
                    (entries.get(sku) or {}).get("shop_seen_at") or ""
                )
                if source_updated_at and not inventory_observation_is_newer(
                    source_updated_at,
                    previous_updated_at,
                ):
                    if (
                        not inventory_observation_is_older(source_updated_at, previous_updated_at)
                        and quantity != remote_quantities.get(sku)
                    ):
                        self.logger.info("inventory_webhook_verification_queued sku=%s", sku)
                        verification_skus.add(sku)
                        deferred_webhook_skus.add(sku)
                        remote_quantities.pop(sku, None)
                        continue
                    self.logger.debug(
                        "inventory_webhook_stale sku=%s source_updated_at=%s current_updated_at=%s",
                        sku,
                        source_updated_at,
                        previous_updated_at,
                    )
                    continue
                remote_quantities[sku] = quantity
                if source_updated_at:
                    remote_updated_at[sku] = source_updated_at
        if inventory_snapshot is not None:
            waiting_for_pos_skus.difference_update(local_quantities)
            snapshot_items = inventory_snapshot["items"]
            snapshot_location_id = str(inventory_snapshot["location_id"])
            previous_location_id = str(state.get("inventory_location_id") or "")
            if previous_location_id and previous_location_id != snapshot_location_id:
                raise RuntimeError(
                    "Shopify inventory location changed from "
                    f"{previous_location_id} to {snapshot_location_id}; "
                    "review SHOPIFY_LOCATION_ID before reconciling."
                )
            blocked_snapshot_skus: set[str] = set()
            snapshot_skus: set[str] = set()
            snapshot_items_by_sku: Dict[str, Dict[str, Any]] = {}
            for item in snapshot_items:
                sku = str(item.get("sku") or "").strip()
                if not sku:
                    continue
                snapshot_skus.add(sku)
                snapshot_items_by_sku[sku] = item
                if str(item.get("location_id") or "") != snapshot_location_id:
                    raise RuntimeError(
                        f"Inventory snapshot returned SKU {sku} for an unexpected location."
                    )
                if int(item.get("duplicate_sku_count") or 0) > 0:
                    blocked_snapshot_skus.add(sku)
                    remote_quantities.pop(sku, None)
                    remote_updated_at.pop(sku, None)
                    self.logger.error(
                        "inventory_snapshot_duplicate_sku sku=%s count=%s",
                        sku,
                        item.get("duplicate_sku_count"),
                    )
                    continue
                if not item.get("available_at_location") or item.get("quantity") is None:
                    blocked_snapshot_skus.add(sku)
                    remote_quantities.pop(sku, None)
                    remote_updated_at.pop(sku, None)
                    self.logger.error(
                        "inventory_snapshot_location_unavailable sku=%s location_id=%s",
                        sku,
                        item.get("location_id"),
                    )
                    continue
                if sku in uploaded_inventory_skus:
                    continue
                if sku not in blocked_snapshot_skus:
                    snapshot_updated_at = str(
                        item.get("inventory_level_updated_at") or ""
                    ).strip()
                    existing_entry = entries.get(sku) or {}
                    existing_updated_at = str(
                        existing_entry.get("shop_seen_at") or ""
                    ).strip()
                    if inventory_observation_is_older(
                        snapshot_updated_at,
                        existing_updated_at,
                    ):
                        remote_quantities[sku] = int(
                            existing_entry.get("shop_seen") or 0
                        )
                        remote_updated_at[sku] = existing_updated_at
                        self.logger.warning(
                            "inventory_snapshot_stale sku=%s snapshot_updated_at=%s current_updated_at=%s",
                            sku,
                            snapshot_updated_at,
                            existing_updated_at,
                        )
                        continue
                    remote_quantities[sku] = int(item["quantity"])
                    if snapshot_updated_at:
                        remote_updated_at[sku] = snapshot_updated_at
            (
                snapshot_overlay_deltas, verified_webhook_skus, verification_skus,
            ) = self._overlay_inventory_changes_on_snapshot(
                inventory_changes,
                snapshot_items_by_sku=snapshot_items_by_sku,
                location_id=snapshot_location_id,
                versions_before_snapshot=versions_before_snapshot,
                remote_quantities=remote_quantities,
                remote_updated_at=remote_updated_at,
                already_blocked=blocked_snapshot_skus,
                relevant_skus=set(local_quantities),
                ignored_skus=(
                    uploaded_inventory_skus
                    | snapshot_overlay_ignored_matrix_skus(state)
                ),
            )
            deferred_webhook_skus.update(verification_skus)
            expected_snapshot_skus = {
                sku
                for sku in local_quantities
                if sku_bases.get(sku, base_sku(sku)) in known_products
            }
            missing_snapshot_skus = expected_snapshot_skus - snapshot_skus
            blocked_snapshot_skus.update(missing_snapshot_skus)
            for sku in sorted(missing_snapshot_skus, key=str.casefold):
                self.logger.error("inventory_snapshot_missing_sku sku=%s", sku)
            state["blocked_inventory_skus"] = sorted(
                blocked_snapshot_skus,
                key=str.casefold,
            )
            state["inventory_location_id"] = snapshot_location_id
            self.logger.info(
                "shopify_inventory_snapshot items=%s usable=%s blocked=%s",
                len(snapshot_items),
                len(remote_quantities),
                len(blocked_snapshot_skus),
            )
            (
                catalog_structure_scan_complete,
                structure_rebased_skus,
            ) = self._update_matrix_structure_state(
                state,
                payloads,
                known_products=known_products,
                snapshot_items=snapshot_items,
                location_id=snapshot_location_id,
                remote_quantities=remote_quantities,
            )
        initialized = 0
        verified_webhook_skus.intersection_update(local_quantities, remote_quantities)
        verified_webhook_skus.difference_update(structure_rebased_skus)
        for sku in sorted(local_quantities.keys() & remote_quantities.keys(), key=str.casefold):
            if sku in entries:
                continue
            pos_quantity = local_quantities[sku]
            shop_quantity = remote_quantities[sku]
            canonical = (
                shop_quantity
                if sku in structure_rebased_skus
                else max(
                    0,
                    pos_quantity + int(snapshot_overlay_deltas.get(sku) or 0),
                )
            )
            entries[sku] = {
                "canonical": canonical,
                "pos_seen": pos_quantity,
                "shop_seen": shop_quantity,
            }
            if remote_updated_at.get(sku):
                entries[sku]["shop_seen_at"] = remote_updated_at[sku]
            initialized += 1

        planned_shop: List[Dict[str, Any]] = []
        planned_pos: List[Dict[str, Any]] = []
        # Check a fresh POS event once more on the next cycle in case its stock
        # row was still being written when the receipt became visible.
        deferred_pos_event_skus = set(new_event_skus) if scheduled else set()
        for sku in sorted(local_quantities.keys() & remote_quantities.keys(), key=str.casefold):
            entry = entries[sku]
            if sku in structure_rebased_skus:
                # ProductSet just replaced the old inventory identities. Preserve
                # this post-mutation snapshot as the new baseline and let queued
                # versioned webhooks (or the next snapshot) carry any racing sale.
                continue
            if entry.get("pending_shop") or entry.get("pending_pos"):
                deferred_webhook_skus.add(sku)
                base = sku_bases.get(sku, base_sku(sku))
                if base in event_skus:
                    deferred_pos_event_skus.add(base)
                completed_quantity_scan_stamp = None
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
            if remote_updated_at.get(sku):
                entry["shop_seen_at"] = remote_updated_at[sku]
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
                    "target_quantity": target,
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
                    "target_quantity": target,
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

        if inventory_snapshot is not None:
            state["last_full_reconcile_date"] = now.date().isoformat()
            state.pop("pos_event_reconcile_pending", None)
            if not has_active_matrix_structure_repairs(state) and not verification_skus:
                state.pop("inventory_maintenance_due_since", None)
            state["inventory_reconcile_schema_version"] = INVENTORY_RECONCILE_SCHEMA_VERSION
            if catalog_structure_scan_complete:
                state["catalog_structure_schema_version"] = CATALOG_STRUCTURE_SCHEMA_VERSION
            if status_reconcile_due:
                # Save the unfinished pass before adjustments; failures must retry
                # even though the quantity reconciliation is already dated today.
                state["pending_product_status_reconcile_date"] = now.date().isoformat()
        if full_reconcile and structure_probe_skus:
            probe_not_before = state.setdefault("matrix_structure_probe_not_before", {})
            waiting = sorted(
                (sku for sku in structure_probe_skus if not payload_by_base.get(sku, {}).get("variants")),
                key=str.casefold,
            )
            retry_at = int(time.time()) + MATRIX_STRUCTURE_PROBE_COOLDOWN_SECONDS
            for sku in structure_probe_skus:
                probe_not_before.pop(sku, None)
            probe_not_before.update(dict.fromkeys(waiting, retry_at))
            state["matrix_structure_probe_skus"] = []
            if waiting:
                self.logger.info(
                    "matrix_structure_probe_waiting skus=%s retry_seconds=%s",
                    ",".join(waiting), MATRIX_STRUCTURE_PROBE_COOLDOWN_SECONDS,
                )
        state["inventory_webhook_verification_skus"] = sorted(verification_skus, key=str.casefold)
        state["inventory_webhook_waiting_pos_skus"] = sorted(waiting_for_pos_skus, key=str.casefold)
        state["event_cursors"] = event_scan_state["event_cursors"]
        state["deferred_pos_event_skus"] = sorted(deferred_pos_event_skus, key=str.casefold)
        if completed_quantity_scan_stamp is not None:
            state["pos_quantity_source_stamp"] = completed_quantity_scan_stamp
        save_state(self.state_path, state)
        blocked_pos_skus: set[str] = set()
        if planned_shop:
            blocked_pos_skus = self._apply_shopify_adjustments(state, planned_shop) or set()
        if planned_pos or planned_shop:
            if self.writeback_mode == "vfp-oledb":
                pos_candidate_skus = {
                    action["sku"]
                    for action in [*planned_pos, *planned_shop]
                }
                current_pos_actions = [
                    state["quantities"][sku]["pending_pos"]
                    for sku in sorted(pos_candidate_skus, key=str.casefold)
                    if sku not in blocked_pos_skus
                    and (state["quantities"].get(sku) or {}).get("pending_pos")
                ]
                if current_pos_actions:
                    self._apply_pos_adjustments(state, current_pos_actions)
            elif planned_pos:
                self.logger.warning(
                    "pos_writeback_not_applied mode=%s adjustments=%s",
                    self.writeback_mode,
                    len(planned_pos),
                )
        if inventory_snapshot is not None and status_reconcile_due:
            if self._reconcile_zero_stock_product_statuses(
                state,
                local_quantities,
                observed_skus=local_quantities.keys() & remote_quantities.keys(),
            ):
                state["last_product_status_reconcile_date"] = now.date().isoformat()
                state.pop("pending_product_status_reconcile_date", None)
        state["last_cycle_epoch"] = int(time.time())
        save_state(self.state_path, state)
        if inventory_changes:
            processed_changes = []
            for change in inventory_changes:
                sku = str(change.get("sku") or "").strip()
                if sku not in deferred_webhook_skus and (inventory_snapshot is None or sku in verified_webhook_skus):
                    processed_changes.append(change)
            if processed_changes:
                self._acknowledge_inventory_changes(processed_changes)
            if inventory_snapshot is not None and len(processed_changes) < len(inventory_changes):
                # A full snapshot can race a newer queued webhook. Acknowledge only
                # the conflicting versions explicitly verified by the later read.
                self.logger.info(
                    "inventory_webhook_ack_deferred_after_snapshot changes=%s",
                    len(inventory_changes) - len(processed_changes),
                )
        self.logger.info(
            "connector_cycle_complete mode=%s initialized=%s webhook_changes=%s shopify_adjustments=%s pos_adjustments=%s",
            read_mode,
            initialized,
            len(inventory_changes),
            len(planned_shop),
            len(planned_pos),
        )
        if scheduled_maintenance and inventory_snapshot is not None and not verification_skus:
            self._automatic_inventory_retry_at = 0.0
        if self.price_sync_enabled and prioritize_inventory and not full_reconcile:
            self._sync_pos_price_changes(state, force_scan=False)

    def _pos_quantity_source_stamp(self) -> list[str | None] | None:
        item_path = dbf_pos_sync.find_dbf_file(self.dbf_dir, "Item.dbf", recursive=True)
        item_stamp = dbf_file_stamp(item_path)
        if item_stamp is None:
            return None
        matrix_path = dbf_pos_sync.find_dbf_file(self.dbf_dir, "Itemmqty.dbf", recursive=True)
        return [item_stamp, dbf_file_stamp(matrix_path)]

    def _reconcile_zero_stock_product_statuses(
        self,
        state: Dict[str, Any],
        local_quantities: Dict[str, int],
        *,
        observed_skus: set[str],
    ) -> bool:
        if self.dry_run:
            return False
        known_products = set(state.get("catalog_products") or [])
        sku_bases = state.get("sku_bases") or {}
        entries = state.get("quantities") or {}
        blocked_skus = set(state.get("blocked_inventory_skus") or [])
        blocked_skus.update(pending_matrix_structure_skus(state))
        blocked_bases = {
            sku_bases.get(sku, base_sku(sku)) for sku in blocked_skus
        }
        pending_bases = {
            sku_bases.get(sku, base_sku(sku))
            for sku, entry in entries.items()
            if entry.get("pending_shop") or entry.get("pending_pos")
        }
        product_skus: Dict[str, List[str]] = {}
        for sku in local_quantities:
            base = sku_bases.get(sku, base_sku(sku))
            if base in known_products:
                product_skus.setdefault(base, []).append(sku)

        pending_status_skus = set(state.get("pending_product_status_skus") or [])
        candidates: List[str] = []
        for sku in sorted(pending_status_skus, key=str.casefold):
            base = sku_bases.get(sku, base_sku(sku))
            skus = product_skus.get(base) or []
            if (
                skus
                and base not in blocked_bases
                and base not in pending_bases
                and sku in observed_skus
                and all(child in observed_skus for child in skus)
            ):
                # A previous archive may need restock recovery even when this
                # snapshot is positive. The backend freshly verifies its stock.
                candidates.append(sku)
        deferred = False
        for base, skus in product_skus.items():
            if base in blocked_bases or any(sku not in observed_skus for sku in skus):
                continue
            if any(
                (entries.get(sku) or {}).get("canonical") is None
                or int(entries[sku]["canonical"]) > 0
                for sku in skus
            ):
                continue
            if base in pending_bases:
                deferred = True
                continue
            if any(
                any(
                    (entries.get(sku) or {}).get(field) is None
                    or int(entries[sku][field]) > 0
                    for field in ("pos_seen", "shop_seen")
                )
                for sku in skus
            ):
                continue
            # Matrix base SKUs need not exist as Shopify variants. Send one
            # observed child; the backend verifies every variant and location.
            candidate = sorted(skus, key=str.casefold)[0]
            if candidate not in candidates:
                candidates.append(candidate)

        succeeded = 0
        batch_size = max(1, min(self.batch_size, 100)) if candidates else 1
        for start in range(0, len(candidates), batch_size):
            batch = candidates[start : start + batch_size]
            pending_status_skus.update(batch)
            state["pending_product_status_skus"] = sorted(pending_status_skus, key=str.casefold)
            # Persist before sending: a timeout may follow a completed archive.
            save_state(self.state_path, state)
            response = self.session.post(
                f"{self.base_url}/sync/inventory/status/reconcile",
                json={"skus": batch},
                timeout=self.timeout,
            )
            response.raise_for_status()
            results = response.json().get("results") or []
            for sku in batch:
                matches = [result for result in results if result.get("sku") == sku]
                if len(matches) == 1 and matches[0].get("success") is True:
                    succeeded += 1
                    pending_status_skus.discard(sku)
                else:
                    self.logger.error("product_status_reconcile_failed sku=%s", sku)
            if pending_status_skus:
                state["pending_product_status_skus"] = sorted(pending_status_skus, key=str.casefold)
            else:
                state.pop("pending_product_status_skus", None)
            save_state(self.state_path, state)
        self.logger.info(
            "product_status_reconcile candidates=%s succeeded=%s failed=%s pending_inventory=%s",
            len(candidates),
            succeeded,
            len(candidates) - succeeded,
            deferred,
        )
        return succeeded == len(candidates) and not deferred and not pending_status_skus

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

    def _collect_new_numeric_product_skus(
        self,
        state: Dict[str, Any],
        *,
        known_products: set[str],
    ) -> set[str]:
        item_path = dbf_pos_sync.find_dbf_file(
            self.dbf_dir,
            "Item.dbf",
            recursive=True,
        )
        if item_path is None:
            self.logger.warning("new_product_scan_missing file=Item.dbf")
            return set()

        source_stamp = dbf_file_stamp(item_path)
        now_epoch = int(time.time())
        due_probe = any(
            now_epoch >= int(value or 0)
            for value in (state.get("matrix_structure_probe_not_before") or {}).values()
        )
        if (
            source_stamp and source_stamp == state.get("catalog_source_stamp")
            and not state.get("pending_catalog_products")
            and not state.get("matrix_structure_probe_skus")
            and not due_probe
        ):
            return set()

        item_rows = list(dbf_pos_sync.iter_dbf_rows(item_path))
        item_skus = [
            str(row.get("SKU") or "").strip()
            for row in item_rows
            if str(row.get("SKU") or "").strip()
        ]
        item_sku_set = set(item_skus)
        sku_bases = state.get("sku_bases") or {}
        bases_with_known_children = {
            str(mapped_base or "").strip()
            for sku, mapped_base in sku_bases.items()
            if str(sku or "").strip()
            and str(mapped_base or "").strip()
            and str(sku or "").strip() != str(mapped_base or "").strip()
        }
        matrix_bases_without_children = {
            str(row.get("SKU") or "").strip()
            for row in item_rows
            if str(row.get("SKU") or "").strip() in known_products
            and str(row.get("TYPE") or "").strip().upper() == "M"
            and str(row.get("SKU") or "").strip() not in bases_with_known_children
        }
        probe_not_before: Dict[str, int] = state.setdefault(
            "matrix_structure_probe_not_before",
            {},
        )
        for sku in list(probe_not_before):
            if sku not in matrix_bases_without_children:
                probe_not_before.pop(sku, None)
        structure_probe_skus = {
            sku
            for sku in matrix_bases_without_children
            if now_epoch >= int(probe_not_before.get(sku) or 0)
        }
        state["matrix_structure_probe_skus"] = sorted(
            structure_probe_skus,
            key=str.casefold,
        )
        if structure_probe_skus:
            self.logger.info(
                "matrix_structure_probe_detected skus=%s",
                ",".join(sorted(structure_probe_skus, key=str.casefold)),
            )
        pending_products: Dict[str, Dict[str, Any]] = state.setdefault(
            "pending_catalog_products",
            {},
        )
        for pending_sku in list(pending_products):
            if pending_sku in known_products or pending_sku not in item_sku_set:
                pending_products.pop(pending_sku, None)
        candidates, high_water, digit_width = numeric_sku_increases(
            item_skus,
            known_products=known_products,
            high_water=state.get("numeric_sku_high_water"),
            digit_width=state.get("numeric_sku_digit_width"),
        )
        state["numeric_sku_high_water"] = high_water
        state["numeric_sku_digit_width"] = digit_width
        detected_candidates = set(candidates)
        for candidate in detected_candidates:
            pending_products.setdefault(candidate, {})
        candidates.update(pending_products)
        if detected_candidates:
            self.logger.info(
                "new_numeric_products_detected new_high=%s skus=%s",
                high_water,
                ",".join(sorted(detected_candidates, key=int)),
            )
        state["catalog_source_stamp"] = source_stamp
        return candidates

    def _collect_pos_event_skus(self, state: Dict[str, Any]) -> tuple[set[str], bool]:
        cursors: Dict[str, int] = state.setdefault("event_cursors", {})
        sku_bases = state.get("sku_bases") or {}
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
            file_skus = {
                sku_bases.get(sku, base_sku(sku))
                for row in rows
                if (sku := str(row.get("SKU") or "").strip())
            }
            affected.update(file_skus)
            if rows:
                self.logger.info(
                    "pos_events_read file=%s new_records=%s affected_skus=%s",
                    filename,
                    len(rows),
                    len(file_skus),
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
                    quantity_entries = state.setdefault("quantities", {})
                    for inventory_sku, quantity in flatten_quantities([payload]).items():
                        quantity_entries.setdefault(
                            inventory_sku,
                            {
                                "canonical": quantity,
                                "pos_seen": quantity,
                                "shop_seen": quantity,
                            },
                        )
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
        return succeeded

    def _fetch_inventory_changes(self) -> List[Dict[str, Any]]:
        response = self.session.get(f"{self.base_url}/sync/inventory/changes?limit=5000", timeout=self.timeout)
        response.raise_for_status()
        return list(response.json().get("items") or [])

    def _fetch_inventory_snapshot(self) -> Dict[str, Any]:
        response = self.session.get(f"{self.base_url}/sync/inventory", timeout=self.timeout)
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict):
            raise RuntimeError("Inventory snapshot response must be a JSON object.")
        if int(body.get("schema_version") or 0) != INVENTORY_SNAPSHOT_SCHEMA_VERSION:
            raise RuntimeError(
                "Inventory snapshot API schema is incompatible with this connector."
            )
        location_id = str(body.get("location_id") or "").strip()
        items = body.get("items")
        if not location_id or not isinstance(items, list):
            raise RuntimeError("Inventory snapshot response is missing its location or items.")
        if any(not isinstance(item, dict) for item in items):
            raise RuntimeError("Inventory snapshot contains an invalid item row.")
        return {"location_id": location_id, "items": items}

    def _overlay_inventory_changes_on_snapshot(
        self,
        changes: List[Dict[str, Any]],
        *,
        snapshot_items_by_sku: Dict[str, Dict[str, Any]],
        location_id: str,
        versions_before_snapshot: set[tuple[int, int]],
        remote_quantities: Dict[str, int],
        remote_updated_at: Dict[str, str],
        already_blocked: set[str],
        relevant_skus: set[str],
        ignored_skus: set[str],
    ) -> tuple[Dict[str, int], set[str], set[str]]:
        """Overlay newer changes and verify timestamp ties against the live snapshot."""
        changes_by_sku: Dict[str, List[Dict[str, Any]]] = {}
        for change in changes:
            sku = str(change.get("sku") or "").strip()
            if sku:
                changes_by_sku.setdefault(sku, []).append(change)
        overlaid_deltas: Dict[str, int] = {}
        verified_skus: set[str] = set()
        deferred_skus: set[str] = set()

        def conflict(sku: str, reason: str) -> None:
            raise RuntimeError(
                "Cannot safely reconcile Shopify's full inventory snapshot with "
                f"queued inventory for SKU {sku}: {reason}."
            )

        for sku, sku_changes in changes_by_sku.items():
            if sku not in relevant_skus:
                continue
            if sku in already_blocked or sku in ignored_skus:
                continue
            if len(sku_changes) != 1:
                conflict(sku, "multiple queued inventory items")
            change = sku_changes[0]
            snapshot_item = snapshot_items_by_sku.get(sku)
            if snapshot_item is None or sku not in remote_quantities:
                conflict(sku, "the snapshot has no usable matching row")
            if str(change.get("location_id") or "").strip() != location_id:
                conflict(sku, "the inventory location does not match")
            snapshot_inventory_item_id = str(
                snapshot_item.get("inventory_item_id") or ""
            ).strip()
            change_inventory_item_id = str(
                change.get("inventory_item_id") or ""
            ).strip()
            if (
                not snapshot_inventory_item_id
                or not change_inventory_item_id
                or snapshot_inventory_item_id != change_inventory_item_id
            ):
                conflict(sku, "the inventory item identity does not match")
            try:
                change_quantity = int(change["quantity"])
            except (KeyError, TypeError, ValueError):
                conflict(sku, "the queued quantity is invalid")

            current_quantity = int(remote_quantities[sku])
            source_updated_at = str(
                change.get("source_updated_at") or ""
            ).strip()
            current_updated_at = str(remote_updated_at.get(sku) or "").strip()
            source_time = parse_inventory_observation_timestamp(source_updated_at)
            current_time = parse_inventory_observation_timestamp(current_updated_at)

            if change_quantity == current_quantity:
                if (
                    source_time is not None
                    and (current_time is None or source_time > current_time)
                ):
                    remote_updated_at[sku] = source_updated_at
                continue
            if source_time is None or current_time is None:
                conflict(sku, "the differing observations have no comparable timestamp")
            if source_time > current_time:
                remote_quantities[sku] = change_quantity
                remote_updated_at[sku] = source_updated_at
                overlaid_deltas[sku] = change_quantity - current_quantity
                self.logger.info(
                    "inventory_snapshot_webhook_overlaid sku=%s snapshot_quantity=%s webhook_quantity=%s",
                    sku,
                    current_quantity,
                    change_quantity,
                )
                continue
            if source_time == current_time:
                snapshot_time = parse_inventory_observation_timestamp(
                    snapshot_item.get("inventory_level_updated_at")
                )
                # A queue version observed before the snapshot is older than
                # the live read, even when their source timestamps tie.
                if (change["id"], change["version"]) in versions_before_snapshot and snapshot_time == current_time:
                    verified_skus.add(sku)
                    self.logger.info(
                        "inventory_webhook_verified sku=%s snapshot_quantity=%s webhook_quantity=%s",
                        sku, current_quantity, change_quantity,
                    )
                else:
                    deferred_skus.add(sku)
                    remote_quantities.pop(sku, None)
                    remote_updated_at.pop(sku, None)
                    self.logger.info("inventory_webhook_verification_deferred sku=%s", sku)

        return overlaid_deltas, verified_skus, deferred_skus

    def _repair_matrix_options(self, items: List[Dict[str, Any]]) -> Dict[str, int]:
        summary = {"updated": 0, "already_correct": 0, "failed": 0}
        endpoint = f"{self.base_url}/sync/catalog/matrix-options/repair"
        processed = 0
        for chunk in chunks(items, min(self.batch_size, 100)):
            response = self.session.post(
                endpoint,
                json={"items": chunk},
                timeout=self.timeout,
            )
            response.raise_for_status()
            body = response.json()
            summary["updated"] += int(body.get("updated") or 0)
            summary["already_correct"] += int(body.get("already_correct") or 0)
            summary["failed"] += int(body.get("failed") or 0)
            processed += len(chunk)
            self.logger.info(
                "matrix_option_repair_progress processed=%s total=%s updated=%s",
                processed,
                len(items),
                summary["updated"],
            )
            for result in body.get("results") or []:
                if result.get("status") in {"failed", "not_found", "not_applicable"}:
                    self.logger.warning(
                        "matrix_option_repair_skipped sku=%s status=%s message=%s",
                        result.get("base_sku"),
                        result.get("status"),
                        result.get("message"),
                    )
        return summary

    def _ready_matrix_structure_repairs(
        self,
        state: Dict[str, Any],
        payloads: List[Dict[str, Any]],
        *,
        known_products: set[str],
    ) -> List[Dict[str, Any]]:
        candidates = matrix_structure_payloads(
            payloads,
            known_products=known_products,
        )
        candidate_by_base = {
            str(payload.get("sku") or "").strip(): payload
            for payload in candidates
        }
        pending: Dict[str, Dict[str, Any]] = state.setdefault(
            "pending_matrix_structure_repairs",
            {},
        )
        for base in list(pending):
            if base not in candidate_by_base:
                self._discard_matrix_quantity_state(
                    state,
                    matrix_structure_entry_skus(base, pending[base]),
                )
                pending.pop(base, None)

        ready: List[Dict[str, Any]] = []
        for base, payload in candidate_by_base.items():
            entry = pending.get(base)
            if not isinstance(entry, dict):
                continue
            fingerprint = catalog_payload_fingerprint(payload)
            if entry.get("fingerprint") != fingerprint:
                self._discard_matrix_quantity_state(
                    state,
                    matrix_structure_entry_skus(base, entry),
                )
                pending.pop(base, None)
                continue
            stage = entry.get("stage")
            if stage == "server_blocked":
                if entry.get("last_attempt_date") == datetime.now().date().isoformat():
                    continue
                entry["observations"] = int(entry.get("observations") or 0) + 1
                ready.append(payload)
                continue
            if stage != "candidate":
                continue
            entry["observations"] = int(entry.get("observations") or 0) + 1
            if int(entry["observations"]) >= NEW_PRODUCT_STABLE_OBSERVATIONS:
                ready.append(payload)
        return ready

    def _repair_matrix_structures(
        self,
        payloads: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        allowed_statuses = {
            "repaired",
            "already_correct",
            "quantity_mismatch",
            "blocked",
            "failed",
        }
        results_by_base: Dict[str, Dict[str, Any]] = {
            str(payload.get("sku") or "").strip(): {
                "base_sku": str(payload.get("sku") or "").strip(),
                "status": "failed",
                "variants": [],
            }
            for payload in payloads
            if str(payload.get("sku") or "").strip()
        }
        endpoint = f"{self.base_url}/sync/catalog/matrix-structure/repair"
        processed = 0
        for chunk in chunks(payloads, min(self.batch_size, 5)):
            response = self.session.post(
                endpoint,
                json={"items": chunk},
                timeout=self.timeout,
            )
            response.raise_for_status()
            body = response.json()
            results = body.get("results") if isinstance(body, dict) else None
            if not isinstance(results, list):
                results = []
            chunk_bases = {
                str(payload.get("sku") or "").strip()
                for payload in chunk
                if str(payload.get("sku") or "").strip()
            }
            for result in results:
                if not isinstance(result, dict):
                    continue
                base = str(result.get("base_sku") or "").strip()
                status = str(result.get("status") or "").strip()
                if base in chunk_bases and status in allowed_statuses:
                    results_by_base[base] = result
            processed += len(chunk)
            self.logger.info(
                "matrix_structure_repair_progress processed=%s total=%s",
                processed,
                len(payloads),
            )
        return results_by_base

    def _record_matrix_structure_repair_results(
        self,
        state: Dict[str, Any],
        payloads: List[Dict[str, Any]],
        results_by_base: Dict[str, Dict[str, Any]],
    ) -> None:
        pending: Dict[str, Dict[str, Any]] = state.setdefault(
            "pending_matrix_structure_repairs",
            {},
        )
        for payload in payloads:
            base = str(payload.get("sku") or "").strip()
            entry = pending.get(base)
            if not base or not isinstance(entry, dict):
                continue
            repair_result = results_by_base.get(base) or {}
            status = str(repair_result.get("status") or "failed")
            expected_child_skus = matrix_variant_skus(payload)
            expected_quantities = flatten_quantities([payload])
            confirmed_quantities: Dict[str, int] = {}
            confirmed_updated_at: Dict[str, str] = {}
            if status in {"repaired", "already_correct"}:
                for variant in repair_result.get("variants") or []:
                    if not isinstance(variant, dict):
                        continue
                    sku = str(variant.get("sku") or "").strip()
                    quantity = variant.get("quantity")
                    if sku not in expected_child_skus or quantity is None:
                        continue
                    confirmed_quantities[sku] = int(quantity)
                    updated_at = str(
                        variant.get("inventory_level_updated_at") or ""
                    ).strip()
                    if updated_at:
                        confirmed_updated_at[sku] = updated_at
                if (
                    set(confirmed_quantities) != expected_child_skus
                    or confirmed_quantities
                    != {
                        sku: expected_quantities[sku]
                        for sku in expected_child_skus
                    }
                ):
                    status = "failed"
                    self.logger.error(
                        "matrix_structure_repair_unconfirmed_inventory sku=%s expected=%s returned=%s",
                        base,
                        len(expected_child_skus),
                        len(confirmed_quantities),
                    )
            entry["result"] = status
            entry["last_attempt_date"] = datetime.now().date().isoformat()
            if status in {"repaired", "already_correct"}:
                entry["stage"] = "verification"
                entry["confirmed_quantities"] = confirmed_quantities
                entry["confirmed_updated_at"] = confirmed_updated_at
            elif status == "quantity_mismatch":
                entry["stage"] = "quantity_reconciliation"
                entry.pop("confirmed_quantities", None)
                entry.pop("confirmed_updated_at", None)
            elif status == "blocked":
                entry["stage"] = "server_blocked"
                entry.pop("confirmed_quantities", None)
                entry.pop("confirmed_updated_at", None)
            else:
                entry["stage"] = "candidate"
                entry.pop("confirmed_quantities", None)
                entry.pop("confirmed_updated_at", None)
            if status in {"quantity_mismatch", "blocked", "failed"}:
                self.logger.warning(
                    "matrix_structure_repair_skipped sku=%s status=%s",
                    base,
                    status,
                )

    def _update_matrix_structure_state(
        self,
        state: Dict[str, Any],
        payloads: List[Dict[str, Any]],
        *,
        known_products: set[str],
        snapshot_items: List[Dict[str, Any]],
        location_id: str,
        remote_quantities: Dict[str, int],
    ) -> tuple[bool, set[str]]:
        started = time.monotonic()
        candidates = matrix_structure_payloads(
            payloads,
            known_products=known_products,
        )
        candidate_bases = {
            str(payload.get("sku") or "").strip()
            for payload in candidates
        }
        pending: Dict[str, Dict[str, Any]] = state.setdefault(
            "pending_matrix_structure_repairs",
            {},
        )
        entries: Dict[str, Dict[str, Any]] = state.setdefault("quantities", {})
        migration_scan = (
            int(state.get("catalog_structure_schema_version") or 0)
            < CATALOG_STRUCTURE_SCHEMA_VERSION
        )
        for base in list(pending):
            if base not in candidate_bases:
                self._discard_matrix_quantity_state(
                    state,
                    matrix_structure_entry_skus(base, pending[base]),
                )
                pending.pop(base, None)

        verified = 0
        scalar_candidates = 0
        blocked = 0
        newly_blocked_skus: set[str] = set()
        rebased_skus: set[str] = set()
        rows_by_sku, children_by_base = index_matrix_snapshot(snapshot_items) if candidates else ({}, {})
        for payload in candidates:
            base = str(payload.get("sku") or "").strip()
            fingerprint = catalog_payload_fingerprint(payload)
            expected_child_skus = matrix_variant_skus(payload)
            observed_children = children_by_base.get(base, set())
            shape = matrix_snapshot_structure(
                base,
                expected_child_skus,
                rows_by_sku,
                observed_children,
                location_id=location_id,
            )
            previous = pending.get(base)
            same_payload = (
                isinstance(previous, dict)
                and previous.get("fingerprint") == fingerprint
            )
            protected_skus = {base} | expected_child_skus | observed_children
            if shape == "correct":
                previous_stage = previous.get("stage") if same_payload else None
                confirmed_quantities = (
                    previous.get("confirmed_quantities")
                    if same_payload and isinstance(previous, dict)
                    else None
                )
                payload_quantities = flatten_quantities([payload])
                confirmed_exact = (
                    isinstance(confirmed_quantities, dict)
                    and set(confirmed_quantities) == expected_child_skus
                    and all(
                        isinstance(quantity, int)
                        for quantity in confirmed_quantities.values()
                    )
                    and confirmed_quantities
                    == {
                        sku: payload_quantities[sku]
                        for sku in expected_child_skus
                    }
                )
                if previous_stage in ACTIVE_MATRIX_STRUCTURE_STAGES and not confirmed_exact:
                    for sku in expected_child_skus:
                        remote_quantities.pop(sku, None)
                    continue
                if previous_stage == "quantity_reconciliation":
                    self._discard_legacy_matrix_pending_actions(
                        state,
                        payload,
                        remote_quantities=remote_quantities,
                    )
                    self._discard_matrix_quantity_state(state, {base})
                    pending.pop(base, None)
                    verified += 1
                    continue
                if previous_stage == "server_blocked":
                    for sku in expected_child_skus:
                        remote_quantities.pop(sku, None)
                    continue

                discarded_skus = {base} if migration_scan or same_payload else set()
                if previous_stage == "verification" and confirmed_exact:
                    discarded_skus.update(protected_skus)
                    discarded_skus.update(matrix_structure_entry_skus(base, previous))
                    confirmed_updated_at = previous.get("confirmed_updated_at") or {}
                    self._discard_matrix_quantity_state(state, discarded_skus)
                    for sku in expected_child_skus:
                        confirmed_quantity = int(confirmed_quantities[sku])
                        entries[sku] = {
                            "canonical": confirmed_quantity,
                            "pos_seen": int(
                                payload_quantities.get(sku, confirmed_quantity)
                            ),
                            "shop_seen": confirmed_quantity,
                        }
                        updated_at = str(
                            confirmed_updated_at.get(sku) or ""
                        ).strip()
                        if updated_at:
                            entries[sku]["shop_seen_at"] = updated_at
                    rebased_skus.update(expected_child_skus)
                    discarded_skus = set()
                elif migration_scan or same_payload:
                    self._discard_legacy_matrix_pending_actions(
                        state,
                        payload,
                        remote_quantities=remote_quantities,
                    )
                if discarded_skus:
                    self._discard_matrix_quantity_state(
                        state,
                        discarded_skus,
                    )
                pending.pop(base, None)
                verified += 1
                continue
            if shape == "scalar":
                scalar_candidates += 1
                if same_payload and previous.get("stage") in {
                    "candidate",
                    "verification",
                    "server_blocked",
                }:
                    continue
                pending[base] = {
                    "fingerprint": fingerprint,
                    "observations": 1,
                    "stage": "candidate",
                    "expected_child_skus": sorted(
                        expected_child_skus,
                        key=str.casefold,
                    ),
                    "protected_skus": sorted(
                        protected_skus,
                        key=str.casefold,
                    ),
                }
                continue
            blocked += 1
            for sku in expected_child_skus:
                remote_quantities.pop(sku, None)
            newly_blocked_skus.update(expected_child_skus)
            if same_payload and previous.get("stage") in {
                "verification",
                "quantity_reconciliation",
            }:
                previous["protected_skus"] = sorted(
                    matrix_structure_entry_skus(base, previous) | protected_skus,
                    key=str.casefold,
                )
                continue
            pending[base] = {
                "fingerprint": fingerprint,
                "observations": (
                    int(previous.get("observations") or 0)
                    if same_payload
                    else 1
                ),
                "stage": "snapshot_blocked",
                "expected_child_skus": sorted(
                    expected_child_skus,
                    key=str.casefold,
                ),
                "protected_skus": sorted(
                    protected_skus,
                    key=str.casefold,
                ),
            }

        if newly_blocked_skus:
            state["blocked_inventory_skus"] = sorted(
                set(state.get("blocked_inventory_skus") or []) | newly_blocked_skus,
                key=str.casefold,
            )
        self.logger.info(
            "matrix_structure_scan matrices=%s verified=%s scalar_candidates=%s blocked=%s pending=%s elapsed_ms=%s",
            len(candidates),
            verified,
            scalar_candidates,
            blocked,
            len(pending),
            round((time.monotonic() - started) * 1000),
        )
        return not has_active_matrix_structure_repairs(state), rebased_skus

    def _discard_matrix_quantity_state(
        self,
        state: Dict[str, Any],
        skus: set[str],
    ) -> None:
        entries: Dict[str, Dict[str, Any]] = state.setdefault("quantities", {})
        discarded = [sku for sku in skus if entries.pop(sku, None) is not None]
        if discarded:
            self.logger.warning(
                "matrix_structure_legacy_quantity_state_discarded skus=%s",
                ",".join(sorted(discarded, key=str.casefold)),
            )

    def _discard_legacy_matrix_pending_actions(
        self,
        state: Dict[str, Any],
        payload: Dict[str, Any],
        *,
        remote_quantities: Dict[str, int],
    ) -> None:
        entries: Dict[str, Dict[str, Any]] = state.setdefault("quantities", {})
        local_quantities = flatten_quantities([payload])
        cleared: List[str] = []
        for sku in matrix_variant_skus(payload):
            entry = entries.get(sku)
            if not isinstance(entry, dict):
                continue
            pending_shop = entry.pop("pending_shop", None)
            pending_pos = entry.pop("pending_pos", None)
            if isinstance(pending_shop, dict):
                target = pending_shop.get("target_quantity")
                if target is not None and remote_quantities.get(sku) == int(target):
                    entry["shop_seen"] = int(target)
            if isinstance(pending_pos, dict):
                target = pending_pos.get("target_quantity")
                if target is not None and local_quantities.get(sku) == int(target):
                    entry["pos_seen"] = int(target)
            if pending_shop is not None or pending_pos is not None:
                cleared.append(sku)
        if cleared:
            self.logger.warning(
                "matrix_structure_legacy_pending_actions_discarded skus=%s",
                ",".join(sorted(cleared, key=str.casefold)),
            )

    def _sync_pos_price_changes(self, state: Dict[str, Any], *, force_scan: bool = False) -> None:
        price_path = dbf_pos_sync.find_dbf_file(self.dbf_dir, "pricechg.dbf", recursive=True)
        source_stamp = dbf_file_stamp(price_path)
        if not force_scan and source_stamp and source_stamp == state.get("price_source_stamp"):
            return
        state.pop("price_source_stamp", None)
        current_snapshot = read_pos_price_snapshot(self.dbf_dir)
        previous_snapshot = state.get("price_snapshot")
        if not isinstance(previous_snapshot, dict):
            state["price_snapshot"] = current_snapshot
            state["price_source_stamp"] = source_stamp
            if not self.dry_run:
                save_state(self.state_path, state)
            self.logger.info(
                "price_tracker_initialized rows=%s historical_changes_replayed=0",
                len(current_snapshot),
            )
            return

        changes = detect_price_changes(
            previous_snapshot,
            current_snapshot,
            sku_bases=state.get("sku_bases") or {},
        )
        if not changes:
            # Keep the snapshot aligned if the POS purges old price-change rows.
            state["price_snapshot"] = current_snapshot
            state["price_source_stamp"] = source_stamp
            return

        self.logger.info(
            "price_changes_detected changes=%s source_skus=%s",
            len(changes),
            ",".join(change["source_sku"] for change in changes),
        )
        if self.dry_run:
            for change in changes:
                self.logger.info(
                    "price_change_dry_run sku=%s old_price=%s new_price=%s targets=%s",
                    change["source_sku"],
                    change["old_price"],
                    change["new_price"],
                    ",".join(change["target_skus"]),
                )
            return

        results: List[Dict[str, Any]] = []
        for chunk in chunks(changes, min(self.batch_size, 100)):
            try:
                response = self.session.post(
                    f"{self.base_url}/sync/prices",
                    json={"changes": chunk},
                    timeout=self.timeout,
                )
                if response.status_code >= 400:
                    self.logger.error(
                        "price_sync_http_error status=%s response=%s",
                        response.status_code,
                        response.text[:2000],
                    )
                response.raise_for_status()
                results.extend(response.json().get("results") or [])
            except Exception:
                # Price tracking must not interrupt inventory or order processing.
                # Missing results retain the old baseline and retry next cycle.
                self.logger.exception("price_sync_chunk_failed changes=%s", len(chunk))

        results_by_sku = {
            str(result.get("source_sku") or "").strip(): result
            for result in results
        }
        retry_snapshot = dict(current_snapshot)
        for change in changes:
            source_sku = change["source_sku"]
            result = results_by_sku.get(source_sku) or {}
            if result.get("success"):
                self.logger.info(
                    "price_change_applied sku=%s old_price=%s new_price=%s targets=%s",
                    source_sku,
                    change["old_price"],
                    change["new_price"],
                    len(change["target_skus"]),
                )
                continue
            if source_sku in previous_snapshot:
                retry_snapshot[source_sku] = previous_snapshot[source_sku]
            else:
                retry_snapshot.pop(source_sku, None)
            self.logger.error(
                "price_change_failed sku=%s old_price=%s new_price=%s message=%s",
                source_sku,
                change["old_price"],
                change["new_price"],
                result.get("message") or "No result returned.",
            )
        state["price_snapshot"] = retry_snapshot
        if retry_snapshot == current_snapshot:
            state["price_source_stamp"] = source_stamp
        save_state(self.state_path, state)

    def _sync_order_inbox(self, session: requests.Session | None = None) -> None:
        session = self.session if session is None else session
        try:
            if not self.dry_run and not self.order_dbfs_initialized:
                migrated_from: Optional[Path] = None
                # Resolve any interrupted two-file publish before deciding whether
                # a retained SQLite inbox still needs to be migrated.
                read_order_dbfs(self.order_header_path, self.order_detail_path)
                if not self.order_header_path.exists() and not self.order_detail_path.exists():
                    for legacy_path in self.legacy_order_db_paths:
                        if migrate_legacy_sqlite_database(
                            legacy_path,
                            self.order_header_path,
                            self.order_detail_path,
                            retention_rows=self.order_retention_rows,
                        ):
                            migrated_from = legacy_path
                            break
                upsert_order_changes(
                    self.order_header_path,
                    self.order_detail_path,
                    [],
                    retention_rows=self.order_retention_rows,
                )
                self.order_dbfs_initialized = True
                self.logger.info(
                    "order_dbfs_initialized header=%s detail=%s migrated_from=%s",
                    self.order_header_path,
                    self.order_detail_path,
                    migrated_from or "none",
                )
            if not self.order_bridge_status_checked:
                try:
                    status_response = session.get(
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
            response = session.get(f"{self.base_url}/sync/orders/changes?limit=250", timeout=self.timeout)
            response.raise_for_status()
            changes = list(response.json().get("items") or [])
            self.logger.info(
                "order_inbox_checked changes=%s header=%s detail=%s",
                len(changes),
                self.order_header_path,
                self.order_detail_path,
            )
            if not changes:
                return
            if self.dry_run:
                self.logger.info(
                    "order_inbox_dry_run changes=%s header=%s detail=%s",
                    len(changes),
                    self.order_header_path,
                    self.order_detail_path,
                )
                return
            delivered_order_ids = upsert_order_changes(
                self.order_header_path,
                self.order_detail_path,
                changes,
                retention_rows=self.order_retention_rows,
            )
            remove_orders_from_legacy_sqlite(self.legacy_order_db_paths, changes)
            delivered_changes = [
                change
                for change in changes
                if str(
                    change.get("shopify_order_id")
                    or (change.get("order") or {}).get("id")
                    or ""
                ).strip()
                in delivered_order_ids
            ]
            deferred_count = len(changes) - len(delivered_changes)
            if deferred_count:
                self.logger.warning(
                    "order_inbox_capacity_deferred changes=%s retention=%s",
                    deferred_count,
                    self.order_retention_rows,
                )
            if not delivered_changes:
                return
            payload = {
                "changes": [
                    {"id": int(change["id"]), "version": int(change["version"])}
                    for change in delivered_changes
                ]
            }
            ack = session.post(
                f"{self.base_url}/sync/orders/changes/ack",
                json=payload,
                timeout=self.timeout,
            )
            ack.raise_for_status()
            self.logger.info(
                "order_inbox_updated changes=%s acknowledged=%s header=%s detail=%s",
                len(delivered_changes),
                int(ack.json().get("acknowledged") or 0),
                self.order_header_path,
                self.order_detail_path,
            )
        except Exception:
            # Railway keeps unacknowledged changes for a later retry.
            self.logger.exception("order_inbox_sync_failed")

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

    def _retry_pending(
        self,
        state: Dict[str, Any],
        *,
        excluded_skus: Optional[set[str]] = None,
    ) -> None:
        entries = state.get("quantities") or {}
        excluded = excluded_skus or set()
        shop_actions = [
            entry["pending_shop"]
            for sku, entry in entries.items()
            if sku not in excluded
            and str((entry.get("pending_shop") or {}).get("sku") or "").strip()
            not in excluded
            and entry.get("pending_shop")
        ]
        if shop_actions:
            self.logger.warning("retrying_pending_shopify_adjustments count=%s", len(shop_actions))
            self._apply_shopify_adjustments(state, shop_actions)
        pos_actions = [
            entry["pending_pos"]
            for sku, entry in entries.items()
            if sku not in excluded
            and str((entry.get("pending_pos") or {}).get("sku") or "").strip()
            not in excluded
            and entry.get("pending_pos")
        ]
        if pos_actions and self.writeback_mode == "vfp-oledb":
            self.logger.warning("retrying_pending_pos_adjustments count=%s", len(pos_actions))
            self._apply_pos_adjustments(state, pos_actions)

    def _apply_shopify_adjustments(
        self,
        state: Dict[str, Any],
        actions: List[Dict[str, Any]],
    ) -> set[str]:
        endpoint = f"{self.base_url}/sync/inventory/adjustments"
        entries = state["quantities"]
        blocked_pos_skus = {str(action["sku"]) for action in actions}
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
                quantity_after_change = result.get("quantity_after_change")
                inventory_updated_at = str(result.get("inventory_updated_at") or "").strip()
                if inventory_updated_at:
                    entry["shop_seen_at"] = inventory_updated_at
                if quantity_after_change is None:
                    entry["shop_seen"] = int(entry["shop_seen"]) + int(action["delta"])
                    entry.pop("pending_pos", None)
                    self.logger.warning(
                        "pos_adjustment_deferred_after_unconfirmed_shopify_result sku=%s",
                        action["sku"],
                    )
                else:
                    actual_quantity = int(quantity_after_change)
                    entry["shop_seen"] = actual_quantity
                    entry["canonical"] = actual_quantity
                    target_quantity = action.get("target_quantity")
                    if (
                        target_quantity is not None
                        and actual_quantity != int(target_quantity)
                    ):
                        entry.pop("pending_pos", None)
                        pos_quantity = int(entry.get("pos_seen") or 0)
                        if (
                            getattr(self, "writeback_mode", "disabled") == "vfp-oledb"
                            and pos_quantity != actual_quantity
                        ):
                            revision = int(entry.get("revision") or 0) + 1
                            entry["revision"] = revision
                            entry["pending_pos"] = {
                                "sku": action["sku"],
                                "delta": actual_quantity - pos_quantity,
                                "target_quantity": actual_quantity,
                                "expected_quantity": pos_quantity,
                                "idempotency_key": adjustment_key(
                                    "pos",
                                    action["sku"],
                                    revision,
                                    pos_quantity,
                                    actual_quantity,
                                    int(target_quantity),
                                ),
                            }
                        blocked_pos_skus.discard(action["sku"])
                        self.logger.warning(
                            "pos_adjustment_replanned_after_shopify_race sku=%s planned=%s actual=%s",
                            action["sku"],
                            target_quantity,
                            actual_quantity,
                        )
                    elif target_quantity is not None:
                        blocked_pos_skus.discard(action["sku"])
                entry.pop("pending_shop", None)
            save_state(self.state_path, state)
        return blocked_pos_skus

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
        if isinstance(results, dict):
            # PowerShell's JSON pipeline emits a single object for one SKU.
            results = [results]
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


def read_pos_price_snapshot(dbf_dir: Path) -> Dict[str, str]:
    snapshot: Dict[str, str] = {}
    for sku, row in dbf_pos_sync.build_price_change_lookup(
        dbf_dir,
        recursive=True,
    ).items():
        normalized_sku = str(sku or "").strip()
        price = money_text(row.get("PRICE"))
        if not normalized_sku or price is None:
            continue
        try:
            if Decimal(price) < 0:
                continue
        except InvalidOperation:
            continue
        snapshot[normalized_sku] = price
    return snapshot


def detect_price_changes(
    previous_snapshot: Dict[str, Any],
    current_snapshot: Dict[str, Any],
    *,
    sku_bases: Dict[str, str],
) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []
    for source_sku in sorted(current_snapshot, key=str.casefold):
        new_price = money_text(current_snapshot[source_sku])
        old_price = money_text(previous_snapshot.get(source_sku))
        if new_price is None or old_price == new_price:
            continue

        if MATRIX_VARIANT_SKU.match(source_sku):
            target_skus = [source_sku]
        else:
            target_skus = sorted(
                {
                    variant_sku
                    for variant_sku, product_sku in sku_bases.items()
                    if product_sku == source_sku
                    and variant_sku != source_sku
                    and MATRIX_VARIANT_SKU.match(variant_sku)
                },
                key=str.casefold,
            )
            if not target_skus:
                target_skus = [source_sku]

        changes.append(
            {
                "source_sku": source_sku,
                "target_skus": target_skus,
                "old_price": old_price,
                "new_price": new_price,
            }
        )
    return changes


def numeric_sku_increases(
    item_skus: Iterable[str],
    *,
    known_products: set[str],
    high_water: Any = None,
    digit_width: Any = None,
) -> tuple[set[str], int, int]:
    numeric_skus = [
        str(sku).strip()
        for sku in item_skus
        if str(sku).strip().isdigit()
    ]
    if not numeric_skus:
        return set(), int(high_water or 0), int(digit_width or 0)

    width = int(digit_width or len(numeric_skus[-1]))
    sequence_skus = [sku for sku in numeric_skus if len(sku) == width]
    if not sequence_skus:
        return set(), int(high_water or 0), width

    known_sequence_values = [
        int(str(sku).strip())
        for sku in known_products
        if str(sku).strip().isdigit() and len(str(sku).strip()) == width
    ]
    previous_high = int(high_water) if high_water not in (None, "") else max(
        known_sequence_values,
        default=0,
    )
    observed_high = max(int(sku) for sku in sequence_skus)
    candidates = {
        sku
        for sku in sequence_skus
        if int(sku) > previous_high and sku not in known_products
    }
    return candidates, max(previous_high, observed_high), width


def nightly_full_sync_due(last_date: Any, *, now: datetime, hour: int) -> bool:
    if now.hour < max(0, min(23, int(hour))):
        return False
    return str(last_date or "") != now.date().isoformat()


def dbf_file_stamp(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        stat = path.stat()
    except OSError:
        # Without reliable metadata, let the existing reader check the file.
        return None
    return f"{datetime.now().date()}:{path}:{stat.st_ino}:{stat.st_size}:{stat.st_mtime_ns}:{stat.st_ctime_ns}"


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
    include_missing_matrix_rows: bool = True,
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
    for row in iter_selected_dbf_rows(item_path, targets, selected_fields={"SKU", "QTY", "TYPE"}):
        sku = str(row.get("SKU") or "").strip()
        if sku not in targets or variants_by_base.get(sku) or str(row.get("TYPE") or "").upper() == "M":
            continue
        quantity = dbf_pos_sync.decimal_to_quantity(dbf_pos_sync.decimal_or_none(row.get("QTY")))
        if quantity is not None or include_missing_matrix_rows:
            quantities[sku] = int(quantity or 0)

    matrix_targets = {sku for sku, variants in variants_by_base.items() if variants}
    if not matrix_targets:
        return quantities
    quantity_path = dbf_pos_sync.find_dbf_file(dbf_dir, "Itemmqty.dbf", recursive=True)
    if quantity_path is None:
        raise FileNotFoundError(f"Expected Itemmqty.dbf in {dbf_dir} for matrix inventory")

    # A live read may see a temporarily missing row while the register writes.
    # On scheduled checks, leave missing rows for the full reconciliation.
    if include_missing_matrix_rows:
        for sku in matrix_targets:
            quantities.update(dict.fromkeys(variants_by_base[sku], 0))

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
        if quantity is not None or include_missing_matrix_rows:
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


def stable_catalog_payloads(
    payloads: Iterable[Dict[str, Any]],
    state: Dict[str, Any],
    *,
    required_observations: int = NEW_PRODUCT_STABLE_OBSERVATIONS,
) -> List[Dict[str, Any]]:
    """Delay newly copied products until their complete payload is stable."""
    pending_products: Dict[str, Dict[str, Any]] = state.setdefault(
        "pending_catalog_products",
        {},
    )
    ready: List[Dict[str, Any]] = []
    for payload in payloads:
        sku = str(payload.get("sku") or "").strip()
        if not sku:
            continue
        fingerprint = catalog_payload_fingerprint(payload)
        previous = pending_products.get(sku) or {}
        observations = (
            int(previous.get("observations") or 0) + 1
            if previous.get("fingerprint") == fingerprint
            else 1
        )
        pending_products[sku] = {
            "fingerprint": fingerprint,
            "observations": observations,
        }
        if observations >= max(1, int(required_observations)):
            ready.append(payload)
    return ready


def catalog_payload_fingerprint(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            default=str,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


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


def parse_inventory_observation_timestamp(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def inventory_observation_is_newer(candidate: str, current: str) -> bool:
    if not current:
        return True
    candidate_time = parse_inventory_observation_timestamp(candidate)
    current_time = parse_inventory_observation_timestamp(current)
    if candidate_time is not None and current_time is not None:
        return candidate_time > current_time
    return candidate > current


def inventory_observation_is_older(candidate: str, current: str) -> bool:
    if not candidate or not current:
        return False
    candidate_time = parse_inventory_observation_timestamp(candidate)
    current_time = parse_inventory_observation_timestamp(current)
    if candidate_time is not None and current_time is not None:
        return candidate_time < current_time
    return candidate < current


def adjustment_key(system: str, sku: str, *values: int) -> str:
    raw = "|".join([system, sku, *(str(value) for value in values)])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def matrix_length_repair_candidates(
    payloads: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for payload in payloads:
        variants = payload.get("variants") or []
        if not any(
            "Length" in (variant.get("option_values") or {})
            for variant in variants
            if isinstance(variant, dict)
        ):
            continue
        base_sku_value = str(payload.get("sku") or "").strip()
        variant_skus = [
            str(variant.get("sku") or "").strip()
            for variant in variants
            if isinstance(variant, dict) and str(variant.get("sku") or "").strip()
        ]
        if base_sku_value or variant_skus:
            candidates.append(
                {
                    "base_sku": base_sku_value,
                    "variant_skus": variant_skus[:10],
                }
            )
    return candidates


def matrix_structure_payloads(
    payloads: Iterable[Dict[str, Any]],
    *,
    known_products: set[str],
) -> List[Dict[str, Any]]:
    return [
        payload
        for payload in payloads
        if str(payload.get("sku") or "").strip() in known_products
        and bool(payload.get("variants"))
    ]


def matrix_variant_skus(payload: Dict[str, Any]) -> set[str]:
    return {
        str(variant.get("sku") or "").strip()
        for variant in payload.get("variants") or []
        if isinstance(variant, dict) and str(variant.get("sku") or "").strip()
    }


def index_matrix_snapshot(
    snapshot_items: Iterable[Dict[str, Any]],
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, set[str]]]:
    rows_by_sku: Dict[str, List[Dict[str, Any]]] = {}
    children_by_base: Dict[str, set[str]] = {}
    for item in snapshot_items:
        sku = str(item.get("sku") or "").strip()
        if not sku:
            continue
        rows_by_sku.setdefault(sku, []).append(item)
        base = base_sku(sku)
        if base != sku:
            children_by_base.setdefault(base, set()).add(sku)
    return rows_by_sku, children_by_base


def matrix_snapshot_structure(
    base: str,
    expected_children: set[str],
    rows_by_sku: Dict[str, List[Dict[str, Any]]],
    observed_children: set[str],
    *,
    location_id: str,
) -> str:
    if not base or not expected_children or base in expected_children:
        return "blocked"

    def usable(sku: str) -> bool:
        rows = rows_by_sku.get(sku) or []
        if len(rows) != 1:
            return False
        item = rows[0]
        return (
            str(item.get("location_id") or "") == location_id
            and int(item.get("duplicate_sku_count") or 0) == 0
            and bool(item.get("available_at_location"))
            and item.get("quantity") is not None
        )

    observed_related_children = observed_children | {sku for sku in expected_children if sku in rows_by_sku}
    base_present = base in rows_by_sku
    if (
        not base_present
        and observed_related_children == expected_children
        and all(usable(sku) for sku in expected_children)
    ):
        return "correct"
    if base_present and usable(base) and not observed_related_children:
        return "scalar"
    return "blocked"


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
                data.setdefault("pending_catalog_products", {})
                data.setdefault("pending_matrix_structure_repairs", {})
                data.setdefault("matrix_structure_probe_skus", [])
                data.setdefault("matrix_structure_probe_not_before", {})
                data.setdefault("blocked_inventory_skus", [])
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
        "pending_catalog_products": {},
        "pending_matrix_structure_repairs": {},
        "matrix_structure_probe_skus": [],
        "matrix_structure_probe_not_before": {},
        "blocked_inventory_skus": [],
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
