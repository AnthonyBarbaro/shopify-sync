from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

if __package__:
    from jbarbaro_db.dbf_pos_sync import find_dbf_file, iter_dbf_rows
else:
    from dbf_pos_sync import find_dbf_file, iter_dbf_rows


ALTERATION_CODES = {"ALT", "ALTCFT"}
FINANCIAL_CODES = {"GC", "GIFTCT", "HOLGC", "COUPON", "COUPON2"}
ALTERATION = re.compile(r"\b(?:alterations?|alterati[o0]ns?|hemming|rehems?|rehemming)\b", re.I)
MATRIX_SKU = re.compile(r"^(.+?)\.\s*\d+\s+\d+$")
RECEIPT_TIME = re.compile(r"(\d{1,2}):(\d{2})(?::\d{2})?")
OPEN_HOUR, CLOSE_HOUR = 9, 21
SOURCES = ("invhdr.dbf", "invdtl.dbf", "Employee.dbf", "Item.dbf")
DEPARTMENT_SOURCES = ("depart.dbf", "MDEPT.DBF", "Dept.dbf")


def sales_paths(directory: Path) -> tuple[Path, ...]:
    paths = []
    for name in SOURCES:
        path = find_dbf_file(directory, name, recursive=True)
        if path is None:
            raise FileNotFoundError(f"Missing sales source {name}")
        paths.append(path)
    for name in DEPARTMENT_SOURCES:
        path = find_dbf_file(directory, name, recursive=True)
        if path is not None:
            return (*paths, path)
    raise FileNotFoundError(f"Missing sales department table; looked for {', '.join(DEPARTMENT_SOURCES)}")


def sales_source_signature(paths: tuple[Path, ...]) -> tuple[tuple[Path, int, int, int], ...]:
    stamps = []
    for path in paths:
        stat = path.stat()
        stamps.append((path, stat.st_ino, stat.st_size, stat.st_mtime_ns))
    return tuple(stamps)


def text(value: object) -> str:
    return str(value or "").strip()


def ticket_key(row: dict, invoice_field: str) -> tuple[str, str, str]:
    return text(row[invoice_field]), text(row["DATE"]), text(row["REG_ID"]).upper()


def cents(value: Decimal) -> int:
    return int((value * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def receipt_time(value: object) -> str | None:
    match = RECEIPT_TIME.fullmatch(text(value))
    if match:
        hour, minute = map(int, match.groups())
        if 0 <= hour < 24 and 0 <= minute < 60:
            return f"{hour:02}:{minute:02}"
    return None


class SalesTotals:
    def __init__(self) -> None:
        self.net_cents = 0
        self.units = Decimal(0)
        self.tickets: set[tuple] = set()
        self.credits_cents = 0
        self.costed_net_cents = 0
        self.cost_cents = 0
        self.costed_lines = 0
        self.missing_cost_lines = 0

    def add(self, ticket: tuple, net_cents: int, units: Decimal, cost_cents: int | None) -> None:
        self.net_cents += net_cents
        self.units += units
        if net_cents > 0 and units >= 0:
            self.tickets.add(ticket)
        if net_cents < 0:
            self.credits_cents -= net_cents
        if cost_cents is None:
            self.missing_cost_lines += 1
        else:
            self.costed_lines += 1
            self.costed_net_cents += net_cents
            self.cost_cents += cost_cents

    def export_profit(self) -> dict:
        profit = self.costed_net_cents - self.cost_cents
        margin = (
            (Decimal(profit) * 100 / self.costed_net_cents).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            if self.costed_net_cents > 0 else None
        )
        return {
            "gross_profit_cents": profit if self.costed_lines or not self.missing_cost_lines else None,
            "gross_margin_percent": float(margin) if margin is not None else None,
            "missing_cost_lines": self.missing_cost_lines,
        }

    def export(self) -> dict:
        return {
            "net_cents": self.net_cents, "tickets": len(self.tickets),
            "units": float(self.units), "credits_cents": self.credits_cents,
            **self.export_profit(),
        }


class SalesReceipt(SalesTotals):
    def __init__(self) -> None:
        super().__init__()
        self.lines: list[dict] = []


def category_rows(categories: dict[str, SalesTotals]) -> list[dict]:
    return [{"name": name, **totals.export()} for name, totals in sorted(
        categories.items(), key=lambda entry: (-entry[1].net_cents, entry[0]),
    )]


class SalesPeriod:
    def __init__(self, start: date, today: date, period: str) -> None:
        self.start = start.isoformat()
        self.period = period
        self.store = SalesTotals()
        self.employees: dict[str, SalesTotals] = defaultdict(SalesTotals)
        self.categories: dict[str, SalesTotals] = defaultdict(SalesTotals)
        self.employee_categories: dict[str, dict[str, SalesTotals]] = defaultdict(lambda: defaultdict(SalesTotals))
        if period == "today":
            self.trend = {f"{hour:02}:00": SalesTotals() for hour in range(OPEN_HOUR, CLOSE_HOUR)}
        elif period == "year":
            self.trend = {f"{today.year}-{month:02}": SalesTotals() for month in range(1, today.month + 1)}
        else:
            self.trend = {
                (start + timedelta(days=i)).isoformat(): SalesTotals()
                for i in range((today - start).days + 1)
            }

    def add(
        self, ticket: tuple, time: str | None, employee: str, category: str,
        amount: int, units: Decimal, cost_cents: int | None,
    ) -> None:
        for totals in (self.store, self.employees[employee], self.categories[category], self.employee_categories[employee][category]):
            totals.add(ticket, amount, units, cost_cents)
        day = ticket[1]
        if self.period == "today":
            bucket = f"{time[:2]}:00" if time else ""
        else:
            bucket = day[:7] if self.period == "year" else day
        if bucket in self.trend:
            self.trend[bucket].add(ticket, amount, units, cost_cents)

    def export(self, employee_names: dict[str, str]) -> dict:
        return {
            "start": self.start,
            "store": self.store.export(),
            "employees": [
                {"employee_id": employee, "name": employee_names.get(employee, "Unassigned"),
                 **totals.export(), "categories": category_rows(self.employee_categories[employee])}
                for employee, totals in sorted(self.employees.items(), key=lambda entry: (-entry[1].net_cents, entry[0]))
            ],
            "categories": category_rows(self.categories),
            "trend": [
                {"label": label, "net_cents": totals.net_cents, **totals.export_profit()}
                for label, totals in self.trend.items()
            ],
        }


def build_sales_board(paths: tuple[Path, ...], *, today: date) -> dict:
    headers_path, details_path, employees_path, items_path, departments_path = paths
    departments = {
        text(row["DEPTCODE"]).upper(): re.sub(r"\s+", " ", text(row["DESCRIPT"])).title()
        for row in iter_dbf_rows(departments_path, required_fields={"DEPTCODE", "DESCRIPT"})
        if row["DEPTCODE"] and row["DESCRIPT"]
    }
    if not departments:
        raise ValueError("The POS department list is empty.")
    alteration_groups = ALTERATION_CODES | {code for code, label in departments.items() if ALTERATION.search(label)}
    items: dict[str, set[str]] = defaultdict(set)
    product_names: dict[str, str] = {}
    alteration_skus = set()
    for row in iter_dbf_rows(items_path, required_fields={"SKU", "GROUP", "DESC"}):
        sku, group = text(row["SKU"]).upper(), text(row["GROUP"]).upper()
        if sku and group:
            items[sku].add(group)
        if sku and row["DESC"]:
            product_names[sku] = text(row["DESC"])
        if sku and (group in alteration_groups or ALTERATION.search(text(row["DESC"]))):
            alteration_skus.add(sku)
    names = {}
    for row in iter_dbf_rows(employees_path, required_fields={"ID_NO", "FIRST_NAME", "LAST_NAME"}):
        code = text(row["ID_NO"]).upper()
        name = " ".join(filter(None, (text(row["FIRST_NAME"]), text(row["LAST_NAME"]))))
        if code and name:
            names[code] = name

    periods = {period: SalesPeriod(start, today, period) for period, start in (
        ("today", today), ("week", today - timedelta(days=today.weekday())),
        ("month", today.replace(day=1)), ("year", today.replace(month=1, day=1)),
    )}
    cutoff = min(period.start for period in periods.values())
    latest = today.isoformat()
    dates = set()
    headers = Counter()
    times = {}
    for row in iter_dbf_rows(headers_path, required_fields={"INVOICE", "DATE", "REG_ID", "STATUS", "TIME"}):
        key = ticket_key(row, "INVOICE")
        if not key[0] or row["STATUS"] or not key[1] or key[1] > latest:
            continue
        try:
            day = date.fromisoformat(key[1])
        except ValueError:
            continue
        dates.add(day)
        if key[1] >= cutoff:
            headers[key] += 1
            times[key] = receipt_time(row["TIME"])

    receipts: dict[str, dict[tuple[str, str, str], SalesReceipt]] = defaultdict(lambda: defaultdict(SalesReceipt))
    for row in iter_dbf_rows(details_path, required_fields={
        "INVOICE_NO", "DATE", "REG_ID", "CLERK", "GROUP", "SKU", "DESC", "EXTENSION", "QTY",
    }, optional_fields={"COST"}):
        day = text(row["DATE"])
        if not cutoff <= day <= latest:
            continue
        key = ticket_key(row, "INVOICE_NO")
        if headers[key] != 1:
            continue
        sku, group = text(row["SKU"]).upper(), text(row["GROUP"]).upper()
        matrix = MATRIX_SKU.fullmatch(sku)
        base = matrix.group(1) if matrix else sku
        if (not sku or sku in alteration_skus or base in alteration_skus
                or group in alteration_groups or ALTERATION.search(text(row["DESC"]))):
            continue
        item_groups = items.get(sku) or items.get(base) or set()
        if not group and len(item_groups) == 1:
            group = next(iter(item_groups))
        if group not in departments or group in FINANCIAL_CODES or item_groups & FINANCIAL_CODES:
            continue
        amount, units = row["EXTENSION"], row["QTY"]
        if not isinstance(amount, Decimal) or not isinstance(units, Decimal) or not amount.is_finite() or not units.is_finite():
            continue
        if not amount and not units:
            continue
        clerk = text(row["CLERK"]).upper()
        employee = clerk if clerk in names else "unassigned"
        units = -abs(units) if amount < 0 else units
        net_cents = cents(amount)
        unit_cost = row.get("COST")
        if not units:
            cost_cents = 0
        elif isinstance(unit_cost, Decimal) and unit_cost.is_finite() and unit_cost > 0:
            cost_cents = cents(unit_cost * units)
        else:
            cost_cents = None
        receipt = receipts[employee][key]
        receipt.add(key, net_cents, units, cost_cents)
        receipt.lines.append({
            "sku": text(row["SKU"]),
            "name": product_names.get(sku) or product_names.get(base) or departments[group],
            "units": float(units),
            "net_cents": net_cents,
            "gross_profit_cents": net_cents - cost_cents if cost_cents is not None else None,
        })
        for period in periods.values():
            if day >= period.start:
                period.add(key, times[key], employee, departments[group], net_cents, units, cost_cents)
    return {
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "business_date": latest,
        "first_invoice_date": min(dates).isoformat() if dates else None,
        "last_invoice_date": max(dates).isoformat() if dates else None,
        "periods": {period: data.export(names) for period, data in periods.items()},
        "receipts": {
            employee: [
                {
                    "invoice": key[0], "date": key[1], "register_id": key[2], "time": times[key],
                    **receipt.export(), "lines": receipt.lines,
                }
                for key, receipt in sorted(
                    entries.items(), key=lambda item: (
                        item[0][1], times[item[0]] or "", item[0][0], item[0][2],
                    ), reverse=True,
                )
            ]
            for employee, entries in receipts.items()
        },
    }
