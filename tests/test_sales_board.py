import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

from app.db import CustomerHistoryEvent, DatabaseStore
from app.models import SalesBoardSnapshot
from app.sales_reports import summarize_sales_report, summarize_year_over_year, year_over_year_ranges
from app.utils import utc_now
from windows_connector.order_dbf import DbfField, _serialize_dbf
from windows_connector.sales_board import build_sales_board, sales_paths


class SalesBoardTests(unittest.TestCase):
    def test_mixed_receipts_use_signed_merchandise_lines_and_exclude_alterations_everywhere(self):
        day = date(2026, 11, 21)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prior = day.replace(year=2025)

            def write(name, fields, rows):
                schema = [DbfField(field, "D", 8) if field == "DATE" else
                          DbfField(field, "N", 12, 2) if field in {"EXTENSION", "QTY", "COST", "PRICE", "TAX", "BAL_DUE"} else
                          DbfField(field, "C", 30) for field in fields]
                (root / name).write_bytes(_serialize_dbf(schema, (dict(zip(fields, row)) for row in rows)))

            write("invhdr.dbf", "INVOICE DATE REG_ID STATUS TIME".split(), [
                ("101", day, "R1", "", "13:45"), ("102", day, "R1", "", "14:25"),
                ("103", day, "R1", "VOID", "15:00"), ("104", day, "R1", "", "16:00"),
                ("105", day, "R1", "", "08:59"), ("106", day, "R1", "", "21:00"),
                ("107", day, "R1", "", "09:00"),
                ("older", prior, "R1", "", "11:30"),
            ])
            write("invdtl.dbf", "INVOICE_NO DATE REG_ID CLERK GROUP SKU DESC EXTENSION QTY COST VENDOR TAX".split(), [
                ("101", day, "R1", "A", "MS", "suit", "Suit", 120, 2, 30, "V1", 6),
                ("101", day, "R1", "B", "MDS", "shirt", "Shirt", 30, 1, 0),
                ("101", day, "R1", "B", "ALT", "hem", "Hem", 25, 1, 5),
                ("101", day, "R1", "B", "ALTCFT", "sew", "Sew", 20, 1, 5),
                ("101", day, "R1", "B", "MS", "hem", "Adjust", 18, 1, 5),
                ("101", day, "R1", "B", "MDS", "repair", "Alteration", 12, 1, 5),
                ("102", day, "R1", "A", "MS", "suit", "Suit", -50, 1, 30),
                ("102", day, "R1", "A", "ALTER", "jacket", "Adjust", -15, -1, 5),
                ("103", day, "R1", "A", "MS", "suit", "Suit", 250, 1, 5),
                ("104", day, "R1", "B", "ALT", "", "Hem", 50, 1, 5),
                ("105", day, "R1", "A", "MS", "suit", "Suit", 10, 1, 4),
                ("106", day, "R1", "A", "MS", "suit", "Suit", 15, 1, 8),
                ("107", day, "R1", "A", "MS", "suit", "Suit", 20, 1, 7),
                ("missing", day, "R1", "A", "MS", "suit", "Suit", 300, 1, 5),
                ("older", prior, "R1", "A", "MS", "suit", "Suit", 300, 1, 60, "V1"),
            ])
            write("Employee.dbf", "ID_NO FIRST_NAME LAST_NAME".split(), [("A", "Alex", "A"), ("B", "Bailey", "B")])
            write("Item.dbf", "SKU GROUP DESC TYPE QTY COST PRICE".split(), [
                ("hem", "ALT", "Hem", "S", 7, 5, 10), ("suit", "MS", "Navy suit", "M", 3, 30, 60)
            ])
            write("depart.dbf", "DEPTCODE DESCRIPT".split(), [
                ("MS", "Suit"), ("MDS", "Dress Shirt"), ("ALT", "Store Alteration"),
                ("ALTCFT", "Outside Alterations"), ("ALTER", "Custom Alteration"),
            ])
            write("Vendor.dbf", "VENDOR_ID COMPANY".split(), [("V1", "Test tailoring brand")])
            write("Customer.dbf", "CUST_NUM BAL_DUE".split(), [("A", 15), ("B", -10)])
            paths = sales_paths(root)
            self.assertEqual(paths[-1].name, "depart.dbf")
            payload = build_sales_board(paths, today=day, history_start=date.min, vendors_path=root / "Vendor.dbf",
                                        customers_path=root / "Customer.dbf")
            report = SalesBoardSnapshot.model_validate(payload)
            (root / "depart.dbf").rename(root / "MDEPT.DBF")
            self.assertEqual(sales_paths(root)[-1].name, "MDEPT.DBF")

        for key, period in report.periods:
            self.assertEqual(period.store.net_cents, 14500)
            self.assertEqual(period.store.tickets, 4)
            self.assertEqual(period.store.units, 5)
            self.assertEqual(period.store.credits_cents, 5000)
            self.assertEqual(period.store.gross_profit_cents, 6600)
            self.assertEqual(period.store.gross_margin_percent, 57.4)
            self.assertEqual(period.store.missing_cost_lines, 1)
            self.assertEqual([(e.name, e.net_cents, e.tickets) for e in period.employees],
                             [("Alex A", 11500, 4), ("Bailey B", 3000, 1)])
            self.assertIsNone(period.employees[1].gross_profit_cents)
            self.assertEqual([c.name for c in period.categories], ["Suit", "Dress Shirt"])
            self.assertEqual(sum(point.net_cents for point in period.trend), 12000 if key == "today" else 14500)
        self.assertEqual([point.label for point in report.periods.today.trend], [
            f"{hour:02}:00" for hour in range(9, 21)
        ])
        self.assertEqual([point.label for point in report.periods.month.trend], [
            f"2026-11-{day_of_month:02}" for day_of_month in range(1, 22)
        ])
        alex_sales = report.receipts["A"]
        self.assertEqual([sale.invoice for sale in alex_sales], ["106", "102", "101", "107", "105"])
        self.assertEqual(alex_sales[1].gross_profit_cents, -2000)
        self.assertEqual(alex_sales[1].lines[0].units, -1)
        self.assertEqual([(line.name, line.gross_profit_cents) for line in alex_sales[2].lines], [("Navy suit", 6000)])
        self.assertEqual([(sale.invoice, sale.net_cents) for sale in report.receipts["B"]], [("104", 0), ("101", 3000)])
        self.assertEqual(report.receipts["B"][0].with_alterations.net_cents, 5000)
        self.assertIsNone(report.receipts["B"][0].with_alterations.lines[0].sku)
        self.assertEqual(report.receipts["B"][1].with_alterations.net_cents, 10500)
        for key, period in report.periods_with_alterations:
            self.assertEqual(period.store.net_cents, 25500)
            self.assertEqual(period.store.tickets, 5)
            self.assertEqual(period.store.gross_profit_cents, 15600)
            self.assertEqual(period.store.gross_margin_percent, 69.3)
            self.assertEqual(sum(point.net_cents for point in period.trend), 23000 if key == "today" else 25500)
        self.assertEqual(
            summarize_sales_report(payload["history"]["2026-11"], start=day, end=day, include_alterations=True)["totals"]["net_cents"],
            25500,
        )
        history = summarize_sales_report(payload["history"]["2025-11"], start=prior.replace(month=1, day=1),
                                         end=prior, include_alterations=False)
        self.assertEqual(history["totals"]["net_cents"], 30000)
        self.assertEqual(history["totals"]["gross_profit_cents"], 24000)
        self.assertEqual(history["rankings"]["brands"][0]["name"], "Test tailoring brand")
        self.assertEqual(history["rankings"]["products"][0]["key"], "SUIT")
        self.assertEqual(history["trend"][-1]["label"], "2025-11")
        year = summarize_year_over_year(payload["history"]["2026-11"], through=day,
                                        include_alterations=True, department_period="year")
        self.assertEqual(year["periods"]["year"]["sales_with_tax_cents"], 26100)
        self.assertEqual(year["periods"]["year"]["gross_profit_cents"], 15600)
        self.assertEqual({row["code"] for row in year["departments"]}, {"MS", "MDS", "ALT", "ALTCFT", "ALTER", "ALTERATIONS"})
        self.assertEqual(report.store_snapshot.customer_count, 2)
        self.assertEqual(report.store_snapshot.positive_balance_cents, 1500)
        self.assertEqual(report.store_snapshot.inventory_cost_cents, 9000)
        self.assertEqual(report.store_snapshot.inventory_retail_cents, 18000)

    def test_year_over_year_periods_use_sunday_weeks_and_previous_day_cutoffs(self):
        sunday = date(2026, 11, 22)
        current = year_over_year_ranges(sunday)
        prior = year_over_year_ranges(sunday.replace(year=2025))
        self.assertEqual(current["week"], (sunday, sunday))
        self.assertEqual(current["week_yesterday"], (date(2026, 11, 15), date(2026, 11, 21)))
        self.assertEqual(prior["week"], (date(2025, 11, 16), date(2025, 11, 22)))
        self.assertEqual(current["year_yesterday"], (date(2026, 1, 1), date(2026, 11, 21)))
        self.assertEqual(current["month"], (date(2026, 11, 1), sunday))
        self.assertEqual(current["month_yesterday"], (date(2026, 11, 1), date(2026, 11, 21)))

    def test_board_and_settings_are_shop_scoped_and_older_snapshots_cannot_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            db = DatabaseStore(str(Path(directory) / "test.sqlite"), "test")
            db.save_sales_board_settings(shop_domain="one", enabled=False, interval_seconds=120, monthly_goal_cents=90)
            db.save_sales_board(shop_domain="one", observed_at="2026-11-21", payload={"new": True})
            self.assertFalse(db.save_sales_board(shop_domain="one", observed_at="2026-11-20", payload={"new": False}))
            self.assertEqual(db.get_sales_board(shop_domain="one"), {"new": True})
            self.assertEqual(db.get_sales_board_settings(shop_domain="one")["interval_seconds"], 120)
            self.assertIsNone(db.get_sales_board(shop_domain="two"))
            self.assertEqual(db.get_sales_board_settings(shop_domain="two"), {})

    def test_rep_receipts_are_shop_scoped_filtered_by_period_and_saved_with_the_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            db = DatabaseStore(str(Path(directory) / "test.sqlite"), "test")
            periods = {"month": {"start": "2026-11-01"}}
            snapshot = {"business_date": "2026-11-22", "periods": periods, "periods_with_alterations": periods}
            line = {"name": "Suit", "net_cents": 5000}
            alteration = {"name": "Hem", "net_cents": 1500, "is_alteration": True}
            receipts = {"A": [
                {"invoice": "4", "date": "2026-11-22", "lines": [], "net_cents": 0,
                 "with_alterations": {"net_cents": 1500, "lines": [alteration]}},
                {"invoice": "3", "date": "2026-11-22", "lines": [line], "net_cents": 5000,
                 "with_alterations": {"net_cents": 6500, "lines": [alteration]}},
                {"invoice": "2", "date": "2026-11-02", "lines": [line], "net_cents": 5000},
                {"invoice": "1", "date": "2026-10-29", "lines": [line], "net_cents": 5000},
            ], "B": [{"invoice": "4", "date": "2026-11-22", "lines": [line], "net_cents": 5000}]}
            db.save_sales_board(shop_domain="one", observed_at="2026-11-22", payload=snapshot, receipts=receipts)
            self.assertFalse(db.save_sales_board(shop_domain="one", observed_at="2026-11-21", payload={}))
            self.assertEqual(db.get_sales_board(shop_domain="one"), snapshot)
            params = {"employee_id": "A", "period": "month", "offset": 1, "limit": 2}
            self.assertEqual(db.get_sales_board_receipts(shop_domain="one", **params), [receipts["A"][2]])
            params["offset"] = 0
            combined = db.get_sales_board_receipts(shop_domain="one", include_alterations=True, **params)
            self.assertEqual([(row["invoice"], row["net_cents"], row["lines"]) for row in combined],
                             [("4", 1500, [alteration]), ("3", 6500, [line, alteration])])
            default = db.get_sales_board_receipts(shop_domain="one", **params)
            self.assertEqual(default[0]["lines"], [line])
            self.assertNotIn("with_alterations", default[0])
            self.assertIsNone(db.get_sales_board_receipts(shop_domain="two", **params))
            db.save_sales_board(shop_domain="one", observed_at="2026-11-23", payload={})
            self.assertIsNone(db.get_sales_board_receipts(shop_domain="one", **params))

    def test_historical_months_are_replaced_atomically_per_shop(self):
        with tempfile.TemporaryDirectory() as directory:
            db = DatabaseStore(str(Path(directory) / "test.sqlite"), "test")
            db.save_sales_report_months(shop_domain="one", observed_at="2026-11-22", months={
                "2025-11": [{"net_cents": 30}], "2026-11": [{"net_cents": 40}],
            })
            db.save_sales_report_months(shop_domain="one", observed_at="2026-11-21", months={"2025-11": []})
            db.save_sales_report_months(shop_domain="two", observed_at="2026-11-22", months={"2025-11": [{"net_cents": 50}]})
            self.assertEqual(db.list_sales_report_months(shop_domain="one"), ["2025-11", "2026-11"])
            rows = db.read_sales_report_lines(shop_domain="one", first_month="2025-01", last_month="2025-12")
            self.assertEqual(list(rows), [{"net_cents": 30}])

    def test_customer_log_retains_creation_and_deduplicates_consecutive_failure_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            db = DatabaseStore(str(Path(directory) / "test.sqlite"), "test")
            rows = [CustomerHistoryEvent("1", "created"), CustomerHistoryEvent("2", "failed", "Missing permission"),
                    CustomerHistoryEvent("2", "failed", "Missing permission"), CustomerHistoryEvent("2", "matched")]
            db.record_customer_sync_history("one", rows)
            newest = db.list_customer_sync_history("one", before=2**63 - 1, outcome=None)
            self.assertEqual([row["outcome"] for row in newest], ["matched", "failed", "created"])
            older = db.list_customer_sync_history("one", before=newest[0]["id"], outcome="failed")
            self.assertEqual([(row["pos_customer_number"], row["reason"]) for row in older], [("2", "Missing permission")])
            self.assertEqual(db.list_customer_sync_history("two", before=2**63 - 1, outcome=None), [])

    def test_customer_address_review_is_encrypted_scoped_and_expires(self):
        with tempfile.TemporaryDirectory() as directory:
            db = DatabaseStore(str(Path(directory) / "test.sqlite"), "test")
            address = {"addresses": [{"address1": "123 Private Street"}]}
            db.record_customer_sync_history("one", [CustomerHistoryEvent("1", "failed", "Address rejected")])
            db.record_customer_sync_history("one", [CustomerHistoryEvent("1", "failed", "Address rejected", address)])
            rows = db.list_customer_sync_history("one", before=2**63 - 1, outcome=None)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertTrue(row["has_address_review"])
            self.assertNotIn("Private Street", str(row))
            with db._connect() as connection:
                data = connection.execute("SELECT address_review_ciphertext FROM customer_sync_history").fetchone()[0]
                self.assertNotIn("Private Street", data)
            self.assertIsNone(db.get_customer_address_review("two", event_id=row["id"]))
            self.assertEqual(db.get_customer_address_review("one", event_id=row["id"])["addresses"], address["addresses"])
            with mock.patch("app.db.utc_now", return_value=utc_now() + timedelta(hours=25)):
                self.assertIsNone(db.get_customer_address_review("one", event_id=row["id"]))
            db.record_customer_sync_history("one", [CustomerHistoryEvent("1", "created")])
            self.assertIsNone(db.get_customer_address_review("one", event_id=row["id"]))
