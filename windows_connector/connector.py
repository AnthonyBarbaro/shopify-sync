#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import time
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
        self.batch_size = env_int("CONNECTOR_BATCH_SIZE", 25, minimum=1, maximum=100)
        self.workers = env_int("CONNECTOR_WORKERS", 2, minimum=1, maximum=4)
        self.timeout = env_int("CONNECTOR_TIMEOUT_SECONDS", 300, minimum=30, maximum=1800)
        self.initial_catalog_upload = env_bool("INITIAL_CATALOG_UPLOAD", True)
        self.order_sync_enabled = env_bool("ORDER_SYNC_ENABLED", True)
        self.order_db_path = Path(
            os.getenv("SHOPIFY_ORDER_DB_PATH") or (self.dbf_dir / "shopify-order.db")
        ).expanduser().resolve()
        self.order_retention_rows = env_int("ORDER_DB_RETENTION_ROWS", 10000, minimum=100, maximum=100000)
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
        if not self.dry_run:
            self._retry_pending(state)

        if self.order_sync_enabled:
            self._sync_order_inbox()

        prepared_products, stats = dbf_pos_sync.load_products(self._reader_args())
        payloads = [prepared.payload for prepared in prepared_products]
        # Python's sort is stable, so products keep their POS order within each
        # group while every stocked product is uploaded before zero-stock rows.
        payloads.sort(key=catalog_upload_priority)
        payload_by_base = {str(payload["sku"]): payload for payload in payloads}
        local_quantities = flatten_quantities(payloads)
        self.logger.info(
            "pos_read products=%s quantities=%s skipped_non_sellable=%s",
            len(payloads),
            len(local_quantities),
            stats.skipped_non_sellable,
        )

        known_products = set(state.get("catalog_products") or [])
        new_base_skus = [sku for sku in payload_by_base if sku not in known_products]
        if new_base_skus and self.initial_catalog_upload:
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
        elif new_base_skus and not self.initial_catalog_upload:
            known_products.update(new_base_skus)
            state["catalog_products"] = sorted(known_products, key=str.casefold)

        entries: Dict[str, Dict[str, Any]] = state.setdefault("quantities", {})
        inventory_changes = self._fetch_inventory_changes()
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

    def _reader_args(self) -> SimpleNamespace:
        return SimpleNamespace(
            dbf_dir=str(self.dbf_dir),
            recursive=True,
            matrix_variants=True,
            quantity_source="item",
            itemmqty_cell=None,
            sku=[],
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

    def _upload_catalog(self, payloads: List[Dict[str, Any]], *, state: Dict[str, Any]) -> set[str]:
        succeeded: set[str] = set()
        known_products = set(state.get("catalog_products") or [])
        endpoint = f"{self.base_url}/wc-api/v3/products/batch"
        for chunk in chunks(payloads, self.batch_size):
            response = self.session.post(endpoint, json=chunk, timeout=self.timeout)
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
        return succeeded

    def _fetch_inventory_changes(self) -> List[Dict[str, Any]]:
        response = self.session.get(f"{self.base_url}/sync/inventory/changes?limit=5000", timeout=self.timeout)
        response.raise_for_status()
        return list(response.json().get("items") or [])

    def _sync_order_inbox(self) -> None:
        try:
            response = self.session.get(f"{self.base_url}/sync/orders/changes?limit=250", timeout=self.timeout)
            response.raise_for_status()
            changes = list(response.json().get("items") or [])
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
                email TEXT,
                phone TEXT,
                shipping_name TEXT,
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
                synced_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS order_items (
                shopify_order_id TEXT NOT NULL,
                line_key TEXT NOT NULL,
                shopify_line_item_id TEXT,
                variant_id TEXT,
                sku TEXT,
                title TEXT,
                variant_title TEXT,
                quantity INTEGER NOT NULL DEFAULT 0,
                current_quantity INTEGER,
                price TEXT,
                total_discount TEXT,
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
        with connection:
            for change in changes:
                order = change.get("order") or {}
                order_id = str(change.get("shopify_order_id") or order.get("id") or "").strip()
                if not order_id:
                    raise ValueError("Order change is missing shopify_order_id")
                if change.get("event_topic") in {"orders/delete", "customers/redact"} or order.get("redacted"):
                    connection.execute("DELETE FROM orders WHERE shopify_order_id = ?", (order_id,))
                    continue
                address = order.get("shipping_address") or {}
                customer_name = " ".join(
                    part
                    for part in (
                        str(order.get("customer_first_name") or "").strip(),
                        str(order.get("customer_last_name") or "").strip(),
                    )
                    if part
                )
                shipping_name = str(address.get("name") or "").strip() or " ".join(
                    part
                    for part in (
                        str(address.get("first_name") or "").strip(),
                        str(address.get("last_name") or "").strip(),
                    )
                    if part
                )
                shipping_method = ", ".join(
                    str(line.get("title") or line.get("code") or "").strip()
                    for line in (order.get("shipping_lines") or [])
                    if str(line.get("title") or line.get("code") or "").strip()
                )
                connection.execute(
                    """
                    INSERT INTO orders (
                        shopify_order_id, order_name, order_number, confirmation_number,
                        created_at, updated_at, processed_at, cancelled_at, closed_at,
                        financial_status, fulfillment_status, currency, subtotal_price,
                        total_discounts, shipping_price, total_tax, total_price,
                        customer_name, email, phone, shipping_name, shipping_company,
                        shipping_address1, shipping_address2, shipping_city, shipping_province,
                        shipping_province_code, shipping_country, shipping_country_code,
                        shipping_zip, shipping_phone, shipping_method, note, tags,
                        source_event, source_version, print_status, synced_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', datetime('now')
                    )
                    ON CONFLICT(shopify_order_id) DO UPDATE SET
                        order_name=excluded.order_name,
                        order_number=excluded.order_number,
                        confirmation_number=excluded.confirmation_number,
                        created_at=excluded.created_at,
                        updated_at=excluded.updated_at,
                        processed_at=excluded.processed_at,
                        cancelled_at=excluded.cancelled_at,
                        closed_at=excluded.closed_at,
                        financial_status=excluded.financial_status,
                        fulfillment_status=excluded.fulfillment_status,
                        currency=excluded.currency,
                        subtotal_price=excluded.subtotal_price,
                        total_discounts=excluded.total_discounts,
                        shipping_price=excluded.shipping_price,
                        total_tax=excluded.total_tax,
                        total_price=excluded.total_price,
                        customer_name=excluded.customer_name,
                        email=excluded.email,
                        phone=excluded.phone,
                        shipping_name=excluded.shipping_name,
                        shipping_company=excluded.shipping_company,
                        shipping_address1=excluded.shipping_address1,
                        shipping_address2=excluded.shipping_address2,
                        shipping_city=excluded.shipping_city,
                        shipping_province=excluded.shipping_province,
                        shipping_province_code=excluded.shipping_province_code,
                        shipping_country=excluded.shipping_country,
                        shipping_country_code=excluded.shipping_country_code,
                        shipping_zip=excluded.shipping_zip,
                        shipping_phone=excluded.shipping_phone,
                        shipping_method=excluded.shipping_method,
                        note=excluded.note,
                        tags=excluded.tags,
                        source_event=excluded.source_event,
                        source_version=excluded.source_version,
                        synced_at=excluded.synced_at
                    """,
                    (
                        order_id,
                        order.get("name") or change.get("order_name"),
                        str(order.get("order_number") or "") or None,
                        order.get("confirmation_number"),
                        order.get("created_at"),
                        order.get("updated_at"),
                        order.get("processed_at"),
                        order.get("cancelled_at"),
                        order.get("closed_at"),
                        order.get("financial_status"),
                        order.get("fulfillment_status"),
                        order.get("currency"),
                        order.get("subtotal_price"),
                        order.get("total_discounts"),
                        order.get("shipping_price"),
                        order.get("total_tax"),
                        order.get("total_price"),
                        customer_name or None,
                        order.get("email"),
                        order.get("phone"),
                        shipping_name or None,
                        address.get("company"),
                        address.get("address1"),
                        address.get("address2"),
                        address.get("city"),
                        address.get("province"),
                        address.get("province_code"),
                        address.get("country"),
                        address.get("country_code"),
                        address.get("zip"),
                        address.get("phone"),
                        shipping_method or None,
                        order.get("note"),
                        order.get("tags"),
                        change.get("event_topic") or "orders/updated",
                        int(change.get("version") or 1),
                    ),
                )
                connection.execute("DELETE FROM order_items WHERE shopify_order_id = ?", (order_id,))
                for index, item in enumerate(order.get("line_items") or [], start=1):
                    line_id = str(item.get("id") or "").strip()
                    line_key = line_id or f"line-{index}"
                    connection.execute(
                        """
                        INSERT INTO order_items (
                            shopify_order_id, line_key, shopify_line_item_id, variant_id, sku,
                            title, variant_title, quantity, current_quantity, price, total_discount,
                            grams, requires_shipping, fulfillment_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            order_id,
                            line_key,
                            line_id or None,
                            str(item.get("variant_id") or "") or None,
                            str(item.get("sku") or "").strip() or None,
                            item.get("title"),
                            item.get("variant_title"),
                            int(item.get("quantity") or 0),
                            int(item["current_quantity"]) if item.get("current_quantity") is not None else None,
                            item.get("price"),
                            item.get("total_discount"),
                            int(item["grams"]) if item.get("grams") is not None else None,
                            int(bool(item["requires_shipping"])) if item.get("requires_shipping") is not None else None,
                            item.get("fulfillment_status"),
                        ),
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
                return data
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return {"version": STATE_VERSION, "catalog_complete": False, "catalog_products": [], "quantities": {}}


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
