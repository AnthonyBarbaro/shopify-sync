import calendar
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable


class ReportTotals:
    def __init__(self) -> None:
        self.net_cents = 0
        self.tax_cents = 0
        self.units = Decimal(0)
        self.credits_cents = 0
        self.cost_cents = 0
        self.costed_net_cents = 0
        self.missing_cost_lines = 0
        self.costed_lines = 0
        self.tickets: set[tuple[str, int]] = set()

    def add(self, line: dict, ticket: tuple[str, int], units: Decimal) -> None:
        net = line["net_cents"]
        self.net_cents += net
        self.tax_cents += line.get("tax_cents", 0)
        self.units += units
        if net > 0 and units >= 0:
            self.tickets.add(ticket)
        if net < 0:
            self.credits_cents -= net
        if line["cost_cents"] is None:
            self.missing_cost_lines += 1
        else:
            self.costed_lines += 1
            self.costed_net_cents += net
            self.cost_cents += line["cost_cents"]

    def export(self) -> dict:
        profit = self.costed_net_cents - self.cost_cents
        margin = (
            (Decimal(profit) * 100 / self.costed_net_cents).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            if self.costed_net_cents > 0 else None
        )
        tickets = len(self.tickets)
        return {
            "net_cents": self.net_cents,
            "sales_with_tax_cents": self.net_cents + self.tax_cents,
            "units": float(self.units),
            "tickets": tickets,
            "credits_cents": self.credits_cents,
            "gross_profit_cents": profit if self.costed_lines or not self.missing_cost_lines else None,
            "gross_margin_percent": float(margin) if margin is not None else None,
            "missing_cost_lines": self.missing_cost_lines,
            "average_ticket_cents": self.net_cents / tickets if tickets else None,
        }


def trend_dates(start: date, end: date, daily: bool) -> list[str]:
    if daily:
        return [(start + timedelta(days=index)).isoformat() for index in range((end - start).days + 1)]
    first_month, last_month = start.year * 12 + start.month - 1, end.year * 12 + end.month - 1
    return [f"{index // 12:04}-{index % 12 + 1:02}" for index in range(first_month, last_month + 1)]


def summarize_sales_report(
    lines: Iterable[dict], *, start: date, end: date, include_alterations: bool, breakdowns: bool = True,
) -> dict:
    first_day, last_day = start.isoformat(), end.isoformat()
    daily = (end - start).days < 31
    totals = ReportTotals()
    trend = defaultdict(ReportTotals)
    hours = defaultdict(ReportTotals)
    weekdays = defaultdict(ReportTotals)
    dimensions = {key: defaultdict(ReportTotals) for key in ("products", "brands", "categories", "reps")}
    line_count = 0
    for line in lines:
        day = line["date"]
        if not first_day <= day <= last_day or (line["is_alteration"] and not include_alterations):
            continue
        units = Decimal(str(line["units"]))
        line_count += 1
        ticket = (day, line["ticket"])
        totals.add(line, ticket, units)
        trend[day if daily else day[:7]].add(line, ticket, units)
        if not breakdowns:
            continue
        hour = line["hour"]
        if hour is not None and 9 <= hour < 21:
            hours[hour].add(line, ticket, units)
        weekdays[date.fromisoformat(day).weekday()].add(line, ticket, units)
        sku = line["sku"]
        product = line["product"] or (f"SKU {sku}" if sku else line["category"])
        groups = (
            ("products", (sku or line["category"], product)),
            ("brands", ("", line["brand"] or "Unassigned brand")),
            ("categories", ("", line["category"])),
            ("reps", (line["rep_id"], line["rep"])),
        )
        for dimension, group in groups:
            dimensions[dimension][group].add(line, ticket, units)

    report = {
        "start": first_day, "end": last_day, "totals": totals.export(), "line_count": line_count,
        "trend_kind": "day" if daily else "month",
        "trend": [{"label": label, **trend[label].export()} for label in trend_dates(start, end, daily)],
    }
    if breakdowns:
        report["hours"] = [{"label": f"{hour:02}:00", **hours[hour].export()} for hour in range(9, 21)]
        report["weekdays"] = [{"label": calendar.day_abbr[index], **weekdays[index].export()} for index in range(7)]
        report["rankings"] = {
            dimension: [
                {"key": key, "name": name, **group.export()}
                for (key, name), group in sorted(groups.items(), key=lambda item: (-item[1].net_cents, item[0][1]))
            ]
            for dimension, groups in dimensions.items()
        }
    return report


def year_over_year_ranges(through: date) -> dict[str, tuple[date, date]]:
    yesterday = through - timedelta(days=1)
    january = through.replace(month=1, day=1)
    return {
        "today": (through, through),
        "yesterday": (yesterday, yesterday),
        "week_yesterday": (yesterday - timedelta(days=(yesterday.weekday() + 1) % 7), yesterday),
        "week": (through - timedelta(days=(through.weekday() + 1) % 7), through),
        "month_yesterday": (through.replace(day=1), yesterday),
        "month": (through.replace(day=1), through),
        "year_yesterday": (january, yesterday),
        "year": (january, through),
    }


def summarize_year_over_year(
    lines: Iterable[dict], *, through: date, include_alterations: bool, department_period: str,
) -> dict:
    ranges = {key: (first.isoformat(), last.isoformat()) for key, (first, last) in year_over_year_ranges(through).items()}
    periods = {key: ReportTotals() for key in ranges}
    departments: dict[tuple[str, str], ReportTotals] = defaultdict(ReportTotals)
    through_day = through.isoformat()
    department_start, department_end = ranges[department_period]
    for line in lines:
        day = line["date"]
        if day > through_day or (line["is_alteration"] and not include_alterations):
            continue
        ticket = (day, line["ticket"])
        units = Decimal(str(line["units"]))
        for key, (first, last) in ranges.items():
            if first <= day <= last:
                periods[key].add(line, ticket, units)
        if department_start <= day <= department_end:
            department = line.get("department") or line["category"]
            code = line.get("department_code") or department
            departments[(code, department)].add(line, ticket, units)
    return {
        "through": through_day,
        "periods": {key: values.export() for key, values in periods.items()},
        "departments": [
            {"code": code, "name": name, **values.export()}
            for (code, name), values in departments.items()
        ],
    }
