import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest import mock

from app.db import CustomerHistoryEvent, DatabaseStore
from app.models import SalesBoardSnapshot
from app.utils import utc_now
from windows_connector.order_dbf import DbfField, _serialize_dbf
from windows_connector.sales_board import build_sales_board, sales_paths


class SalesBoardTests(unittest.TestCase):
    def test_mixed_receipts_use_signed_merchandise_lines_and_exclude_alterations_everywhere(self):
        day = date(2026, 11, 21)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def write(name, fields, rows):
                schema = [DbfField(field, "D", 8) if field == "DATE" else
                          DbfField(field, "N", 12, 2) if field in {"EXTENSION", "QTY", "COST"} else
                          DbfField(field, "C", 30) for field in fields]
                (root / name).write_bytes(_serialize_dbf(schema, (dict(zip(fields, row)) for row in rows)))

            write("invhdr.dbf", "INVOICE DATE REG_ID STATUS TIME".split(), [
                ("101", day, "R1", "", "13:45"), ("102", day, "R1", "", "14:25"),
                ("103", day, "R1", "VOID", "15:00"), ("104", day, "R1", "", "16:00"),
                ("105", day, "R1", "", "08:59"), ("106", day, "R1", "", "21:00"),
                ("107", day, "R1", "", "09:00"),
            ])
            write("invdtl.dbf", "INVOICE_NO DATE REG_ID CLERK GROUP SKU DESC EXTENSION QTY COST".split(), [
                ("101", day, "R1", "A", "MS", "suit", "Suit", 120, 2, 30),
                ("101", day, "R1", "B", "MDS", "shirt", "Shirt", 30, 1, 0),
                ("101", day, "R1", "B", "ALT", "hem", "Hem", 25, 1, 5),
                ("101", day, "R1", "B", "ALTCFT", "sew", "Sew", 20, 1, 5),
                ("101", day, "R1", "B", "MS", "hem", "Adjust", 18, 1, 5),
                ("101", day, "R1", "B", "MDS", "repair", "Alteration", 12, 1, 5),
                ("102", day, "R1", "A", "MS", "suit", "Suit", -50, 1, 30),
                ("102", day, "R1", "A", "ALTER", "jacket", "Adjust", -15, -1, 5),
                ("103", day, "R1", "A", "MS", "suit", "Suit", 250, 1, 5),
                ("104", day, "R1", "B", "ALT", "hem", "Hem", 50, 1, 5),
                ("105", day, "R1", "A", "MS", "suit", "Suit", 10, 1, 4),
                ("106", day, "R1", "A", "MS", "suit", "Suit", 15, 1, 8),
                ("107", day, "R1", "A", "MS", "suit", "Suit", 20, 1, 7),
                ("missing", day, "R1", "A", "MS", "suit", "Suit", 300, 1, 5),
            ])
            write("Employee.dbf", "ID_NO FIRST_NAME LAST_NAME".split(), [("A", "Alex", "A"), ("B", "Bailey", "B")])
            write("Item.dbf", "SKU GROUP DESC".split(), [("hem", "ALT", "Hem"), ("suit", "MS", "Navy suit")])
            write("depart.dbf", "DEPTCODE DESCRIPT".split(), [
                ("MS", "Suit"), ("MDS", "Dress Shirt"), ("ALT", "Store Alteration"),
                ("ALTCFT", "Outside Alterations"), ("ALTER", "Custom Alteration"),
            ])
            paths = sales_paths(root)
            self.assertEqual(paths[-1].name, "depart.dbf")
            report = SalesBoardSnapshot.model_validate(build_sales_board(paths, today=day))
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
        self.assertEqual([(sale.invoice, sale.net_cents) for sale in report.receipts["B"]], [("101", 3000)])

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
            snapshot = {"business_date": "2026-11-22", "periods": {"month": {"start": "2026-11-01"}}}
            receipts = {"A": [
                {"invoice": "3", "date": "2026-11-22"},
                {"invoice": "2", "date": "2026-11-02"},
                {"invoice": "1", "date": "2026-10-29"},
            ], "B": [{"invoice": "4", "date": "2026-11-22"}]}
            db.save_sales_board(shop_domain="one", observed_at="2026-11-22", payload=snapshot, receipts=receipts)
            self.assertFalse(db.save_sales_board(shop_domain="one", observed_at="2026-11-21", payload={}))
            self.assertEqual(db.get_sales_board(shop_domain="one"), snapshot)
            params = {"employee_id": "A", "period": "month", "offset": 1, "limit": 2}
            self.assertEqual(db.get_sales_board_receipts(shop_domain="one", **params), [receipts["A"][1]])
            self.assertIsNone(db.get_sales_board_receipts(shop_domain="two", **params))
            db.save_sales_board(shop_domain="one", observed_at="2026-11-23", payload={})
            self.assertIsNone(db.get_sales_board_receipts(shop_domain="one", **params))

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
