import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "sku_sort", Path(__file__).resolve().parents[1] / "ops/sku-sort/sort_skus.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class SkuSortTests(unittest.TestCase):
    def test_new_skus_and_price_changes_sort_without_touching_other_products(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "shop.db")
            with sqlite3.connect(path) as db:
                db.execute(
                    "CREATE TABLE product_skus (id INTEGER PRIMARY KEY, product_id INTEGER, price_amount TEXT, sort_order INTEGER, is_active INTEGER, deleted_at TEXT)"
                )
                db.executemany(
                    "INSERT INTO product_skus VALUES (?,?,?,?,?,?)",
                    [
                        (1, 107, "5.99", 9, 1, None),
                        (2, 107, "0.81", 3, 1, None),
                        (3, 107, "1.41", 0, 1, None),
                        (4, 108, "0.10", 42, 1, None),
                        (5, 107, "0.01", 40, 0, None),
                        (6, 107, "0.02", 41, 1, "deleted"),
                    ],
                )
            self.assertEqual(module.sort_skus(path, 107), 3)
            self.assertEqual(module.sort_skus(path, 107), 0)
            with sqlite3.connect(path) as db:
                self.assertEqual(
                    db.execute(
                        "SELECT id FROM product_skus WHERE product_id=107 AND is_active=1 AND deleted_at IS NULL ORDER BY sort_order DESC"
                    ).fetchall(),
                    [(2,), (3,), (1,)],
                )
                self.assertEqual(
                    db.execute("SELECT sort_order FROM product_skus WHERE id IN (4,5,6) ORDER BY id").fetchall(),
                    [(42,), (40,), (41,)],
                )
                db.execute("UPDATE product_skus SET price_amount='0.50' WHERE id=1")
            module.sort_skus(path, 107)
            with sqlite3.connect(path) as db:
                self.assertEqual(
                    db.execute(
                        "SELECT id FROM product_skus WHERE product_id=107 AND is_active=1 AND deleted_at IS NULL ORDER BY sort_order DESC"
                    ).fetchall(),
                    [(1,), (2,), (3,)],
                )

    def test_native_sync_writes_are_sorted_immediately_and_rollback_together(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "shop.db")
            with sqlite3.connect(path) as db:
                db.execute(
                    "CREATE TABLE product_skus (id INTEGER PRIMARY KEY, product_id INTEGER, price_amount NUMERIC, sort_order INTEGER, is_active INTEGER, deleted_at TEXT)"
                )
                db.executemany(
                    "INSERT INTO product_skus VALUES (?,?,?,?,?,?)",
                    [(1, 107, 5.99, 1, 1, None), (2, 107, 0.81, 0, 1, None), (3, 108, 0.10, 42, 1, None)],
                )
            module.install_sort_triggers(path, 107)
            module.install_sort_triggers(path, 107)  # Installation is repeatable.
            with sqlite3.connect(path) as db:
                db.execute("PRAGMA recursive_triggers=ON")
                db.execute("INSERT INTO product_skus VALUES (4,107,1.41,0,1,NULL)")

                def order():
                    return [
                        r[0]
                        for r in db.execute(
                            "SELECT id FROM product_skus WHERE product_id=107 AND is_active=1 AND deleted_at IS NULL ORDER BY sort_order DESC"
                        )
                    ]

                self.assertEqual(order(), [2, 4, 1])
                db.execute("UPDATE product_skus SET price_amount=0.50 WHERE id=1")
                self.assertEqual(order(), [1, 2, 4])
                db.commit()
                db.execute("UPDATE product_skus SET price_amount=9.99 WHERE id=1")
                self.assertEqual(order(), [2, 4, 1])
                db.rollback()
                self.assertEqual(order(), [1, 2, 4])
                db.execute("UPDATE product_skus SET is_active=0 WHERE id=2")
                self.assertEqual(order(), [1, 4])
                db.execute("DELETE FROM product_skus WHERE id=1")
                self.assertEqual(order(), [4])
                self.assertEqual(db.execute("SELECT sort_order FROM product_skus WHERE id=3").fetchone()[0], 42)

    def test_missing_database_is_not_created(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing.db"
            with self.assertRaises(sqlite3.OperationalError):
                module.sort_skus(str(path), 107)
            self.assertFalse(path.exists())
