import tempfile
import unittest
from datetime import date
from pathlib import Path

from app.db import DatabaseStore
from app.models import SalesBoardSnapshot
from windows_connector.order_dbf import DbfField, _serialize_dbf
from windows_connector.sales_board import build_sales_board, sales_paths


class SalesBoardTests(unittest.TestCase):
    def test_mixed_receipts_use_signed_merchandise_lines_and_exclude_alterations_everywhere(self):
        day = date(2026, 11, 21)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def write(name, fields, rows):
                schema = [DbfField(field, "D", 8) if field == "DATE" else
                          DbfField(field, "N", 12, 2) if field in {"EXTENSION", "QTY"} else
                          DbfField(field, "C", 30) for field in fields]
                (root / name).write_bytes(_serialize_dbf(schema, (dict(zip(fields, row)) for row in rows)))

            write("invhdr.dbf", "INVOICE DATE REG_ID STATUS TIME".split(), [
                ("101", day, "R1", "", "13:45"), ("102", day, "R1", "", "14:25"),
                ("103", day, "R1", "VOID", "15:00"), ("104", day, "R1", "", "16:00"),
            ])
            write("invdtl.dbf", "INVOICE_NO DATE REG_ID CLERK GROUP SKU DESC EXTENSION QTY".split(), [
                ("101", day, "R1", "A", "MS", "suit", "Suit", 120, 1),
                ("101", day, "R1", "B", "MDS", "shirt", "Shirt", 30, 1),
                ("101", day, "R1", "B", "ALT", "hem", "Hem", 25, 1),
                ("101", day, "R1", "B", "ALTCFT", "sew", "Sew", 20, 1),
                ("101", day, "R1", "B", "MS", "hem", "Adjust", 18, 1),
                ("101", day, "R1", "B", "MDS", "repair", "Alteration", 12, 1),
                ("102", day, "R1", "A", "MS", "suit", "Suit", -50, -1),
                ("102", day, "R1", "A", "ALTER", "jacket", "Adjust", -15, -1),
                ("103", day, "R1", "A", "MS", "suit", "Suit", 250, 1),
                ("104", day, "R1", "B", "ALT", "hem", "Hem", 50, 1),
                ("missing", day, "R1", "A", "MS", "suit", "Suit", 300, 1),
            ])
            write("Employee.dbf", "ID_NO FIRST_NAME LAST_NAME".split(), [("A", "Alex", "A"), ("B", "Bailey", "B")])
            write("Item.dbf", "SKU GROUP DESC".split(), [("hem", "ALT", "Hem")])
            write("MDEPT.DBF", "DEPTCODE DESCRIPT".split(), [
                ("MS", "Suit"), ("MDS", "Dress Shirt"), ("ALT", "Store Alteration"),
                ("ALTCFT", "Outside Alterations"), ("ALTER", "Custom Alteration"),
            ])
            report = SalesBoardSnapshot.model_validate(build_sales_board(sales_paths(root), today=day))

        for _, period in report.periods:
            self.assertEqual(period.store.net_cents, 10000)
            self.assertEqual(period.store.tickets, 1)
            self.assertEqual(period.store.units, 1)
            self.assertEqual(period.store.credits_cents, 5000)
            self.assertEqual([(e.name, e.net_cents, e.tickets) for e in period.employees],
                             [("Alex A", 7000, 1), ("Bailey B", 3000, 1)])
            self.assertEqual([c.name for c in period.categories], ["Suit", "Dress Shirt"])
            self.assertEqual(sum(point.net_cents for point in period.trend), 10000)

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

    def test_customer_log_retains_creation_and_deduplicates_consecutive_failure_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            db = DatabaseStore(str(Path(directory) / "test.sqlite"), "test")
            rows = [("1", "created", None), ("2", "failed", "Missing permission"),
                    ("2", "failed", "Missing permission"), ("2", "matched", None)]
            db.record_customer_sync_history("one", rows)
            newest = db.list_customer_sync_history("one", before=2**63 - 1, outcome=None)
            self.assertEqual([row["outcome"] for row in newest], ["matched", "failed", "created"])
            older = db.list_customer_sync_history("one", before=newest[0]["id"], outcome="failed")
            self.assertEqual([(row["pos_customer_number"], row["reason"]) for row in older], [("2", "Missing permission")])
            self.assertEqual(db.list_customer_sync_history("two", before=2**63 - 1, outcome=None), [])
