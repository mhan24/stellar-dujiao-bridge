"""Schema migration, delivery payload retention and structured logging."""

import json
import logging
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from app import main as bridge

LEGACY_SCHEMA = """
    CREATE TABLE bridge_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE plans (id INTEGER PRIMARY KEY AUTOINCREMENT, stellar_plan_id TEXT NOT NULL UNIQUE,
        sku_code TEXT NOT NULL, name TEXT NOT NULL, slug TEXT NOT NULL, destination TEXT NOT NULL,
        country_code TEXT NOT NULL, data_json TEXT NOT NULL, duration_json TEXT NOT NULL,
        coverage_json TEXT NOT NULL, activation_json TEXT NOT NULL, topup_json TEXT NOT NULL,
        fair_use_json TEXT, speed TEXT, validity_days INTEGER, price_amount TEXT NOT NULL,
        price_billing_unit TEXT, currency TEXT NOT NULL, available INTEGER NOT NULL DEFAULT 0,
        rechargeable INTEGER NOT NULL DEFAULT 0, updated_at TEXT, raw_json TEXT NOT NULL);
    CREATE TABLE orders (id INTEGER PRIMARY KEY AUTOINCREMENT, order_no TEXT NOT NULL UNIQUE,
        downstream_order_no TEXT UNIQUE, callback_url TEXT, plan_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL, days INTEGER, idempotency_key TEXT NOT NULL UNIQUE,
        stellar_order_id TEXT, status TEXT NOT NULL, amount TEXT NOT NULL DEFAULT "0.00",
        currency TEXT NOT NULL DEFAULT "EUR", fulfillment_json TEXT, last_error TEXT,
        poll_attempts INTEGER NOT NULL DEFAULT 0, next_poll_at TEXT, created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL);
    CREATE TABLE webhook_events (event_id TEXT PRIMARY KEY, received_at TEXT NOT NULL);
    CREATE TABLE callback_outbox (id INTEGER PRIMARY KEY AUTOINCREMENT,
        notification_key TEXT NOT NULL UNIQUE, order_id INTEGER NOT NULL,
        callback_url TEXT NOT NULL, payload_json TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT NOT NULL,
        delivered_at TEXT, last_error TEXT);
"""

RAW_PLAN = {
    "id": "11111111-1111-4111-8111-111111111111",
    "sku": "S",
    "name": "China 10GB 30Days",
    "destination": "China",
    "coverage": {"codes": ["CN"]},
    "available": True,
    "data": {"megabytes": 10240},
    "validity_days": 30,
    "price": {"amount": "10.00", "currency": "EUR"},
}


class SchemaMigrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patcher = patch.object(bridge.settings, "DB_PATH", str(Path(self.tmp.name) / "legacy.db"))
        patcher.start()
        self.addCleanup(patcher.stop)

    def create_legacy_database(self):
        with bridge.db_connect() as db:
            db.executescript(LEGACY_SCHEMA)
            db.execute(
                "INSERT INTO plans (stellar_plan_id,sku_code,name,slug,destination,country_code,"
                "data_json,duration_json,coverage_json,activation_json,topup_json,price_amount,"
                "currency,available,raw_json) VALUES ('p','S','China 10GB 30Days','S','China','CN',"
                "'{}','{}','{}','{}','{}','10.00','EUR',1,?)",
                (json.dumps(RAW_PLAN),),
            )
            db.execute(
                "INSERT INTO orders (order_no,plan_id,quantity,idempotency_key,status,stellar_order_id,"
                "created_at,updated_at) VALUES ('ord-1',1,1,'k-1','creating','up-1','x','x')"
            )

    async def test_legacy_database_is_upgraded_in_place(self):
        self.create_legacy_database()
        with bridge.db_connect() as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 0)
        bridge.init_db()
        with bridge.db_connect() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            plan_columns = {row[1] for row in db.execute("PRAGMA table_info(plans)")}
            order_columns = {row[1] for row in db.execute("PRAGMA table_info(orders)")}
            event_columns = {row[1] for row in db.execute("PRAGMA table_info(webhook_events)")}
            outbox_columns = {row[1] for row in db.execute("PRAGMA table_info(callback_outbox)")}
            indexes = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='index'")}
            # Existing rows are preserved and backfilled.
            self.assertEqual(db.execute("SELECT purchase_attempted FROM orders").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT published FROM plans").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT upstream_active FROM plans").fetchone()[0], 1)
        self.assertEqual(version, len(bridge.util.MIGRATIONS))
        self.assertTrue({"published", "upstream_active"} <= plan_columns)
        self.assertTrue({"unit_cost", "cancel_results_json", "refunded_amount"} <= order_columns)
        self.assertIn("failed_at", event_columns)
        self.assertIn("failed_at", outbox_columns)
        self.assertIn("idx_orders_poll", indexes)
        # A second startup is a no-op.
        bridge.init_db()
        with bridge.db_connect() as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], version)

    async def test_partially_migrated_database_still_starts(self):
        """A database that already has some new columns must not fail on ALTER."""
        self.create_legacy_database()
        with bridge.db_connect() as db:
            db.execute("ALTER TABLE plans ADD COLUMN published INTEGER NOT NULL DEFAULT 0")
            db.execute("ALTER TABLE orders ADD COLUMN unit_cost TEXT")
        bridge.init_db()
        with bridge.db_connect() as db:
            columns = {row[1] for row in db.execute("PRAGMA table_info(orders)")}
        self.assertIn("refunded_amount", columns)

    async def test_delivery_payloads_expire_only_when_configured(self):
        bridge.init_db()
        bridge.upsert_plans([RAW_PLAN])
        plan = bridge.available_plan_rows()[0]
        payload = json.dumps({"delivery_data": {"esims": [{"sim_id": "SIM-1", "customer_link": "https://x"}]}})
        old = bridge.iso(bridge.now() - timedelta(days=400))
        with bridge.db_connect() as db:
            for suffix, status, updated in (
                ("old", "delivered", old),
                ("new", "delivered", bridge.iso()),
                ("live", "fulfilling", old),
            ):
                db.execute(
                    "INSERT INTO orders (order_no,plan_id,quantity,idempotency_key,status,currency,"
                    "created_at,updated_at,fulfillment_json) VALUES (?,?,?,?,?,?,?,?,?)",
                    (f"ord-{suffix}", plan["id"], 1, f"k-{suffix}", status, "EUR", updated, updated, payload),
                )
        with patch.object(bridge.settings, "FULFILLMENT_RETENTION_DAYS", 0):
            self.assertEqual(bridge.prune_durable_queues()[2], 0)
        with patch.object(bridge.settings, "FULFILLMENT_RETENTION_DAYS", 90):
            self.assertEqual(bridge.prune_durable_queues()[2], 1)
        with bridge.db_connect() as db:
            kept = {row[0] for row in db.execute("SELECT order_no FROM orders WHERE fulfillment_json IS NOT NULL")}
        self.assertEqual(kept, {"ord-new", "ord-live"})


class LoggingTests(unittest.TestCase):
    def test_json_formatter_keeps_structured_fields(self):
        record = logging.LogRecord(
            "stellar-dujiao-bridge",
            logging.INFO,
            __file__,
            1,
            "order %s: %s -> %s",
            (7, "creating", "fulfilling"),
            None,
        )
        record.order_id, record.to_status = 7, "fulfilling"
        payload = json.loads(bridge.logs.JsonFormatter().format(record))
        self.assertEqual(payload["message"], "order 7: creating -> fulfilling")
        self.assertEqual(payload["level"], "INFO")
        self.assertEqual(payload["order_id"], 7)
        self.assertEqual(payload["to_status"], "fulfilling")
        self.assertIn("ts", payload)

    def test_json_formatter_records_tracebacks(self):
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            import sys

            record = logging.LogRecord(
                "stellar-dujiao-bridge", logging.ERROR, __file__, 1, "failed", None, sys.exc_info()
            )
        payload = json.loads(bridge.logs.JsonFormatter().format(record))
        self.assertIn("RuntimeError: boom", payload["exception"])


if __name__ == "__main__":
    unittest.main()
