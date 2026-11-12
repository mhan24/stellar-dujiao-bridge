"""Clock, JSON and SQLite helpers shared by every layer."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from . import settings

log = logging.getLogger("stellar-dujiao-bridge")


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or now()).isoformat()


def parse_iso(value: str | None) -> datetime:
    if not value:
        return now()
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def json_load(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return fallback


def row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row else None


@contextmanager
def db_connect() -> Iterator[sqlite3.Connection]:
    """Open a short-lived connection; the block commits and always closes it."""
    connection = sqlite3.connect(settings.DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        with connection:
            yield connection
    finally:
        connection.close()


SCHEMA_TABLES = """
            CREATE TABLE IF NOT EXISTS bridge_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stellar_plan_id TEXT NOT NULL UNIQUE,
                sku_code TEXT NOT NULL,
                name TEXT NOT NULL,
                slug TEXT NOT NULL,
                destination TEXT NOT NULL,
                country_code TEXT NOT NULL,
                data_json TEXT NOT NULL,
                duration_json TEXT NOT NULL,
                coverage_json TEXT NOT NULL,
                activation_json TEXT NOT NULL,
                topup_json TEXT NOT NULL,
                fair_use_json TEXT,
                speed TEXT,
                validity_days INTEGER,
                price_amount TEXT NOT NULL,
                price_billing_unit TEXT,
                currency TEXT NOT NULL,
                available INTEGER NOT NULL DEFAULT 0,
                rechargeable INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT,
                raw_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_no TEXT NOT NULL UNIQUE,
                downstream_order_no TEXT UNIQUE,
                callback_url TEXT,
                plan_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                days INTEGER,
                idempotency_key TEXT NOT NULL UNIQUE,
                stellar_order_id TEXT,
                status TEXT NOT NULL,
                amount TEXT NOT NULL DEFAULT "0.00",
                currency TEXT NOT NULL DEFAULT "EUR",
                fulfillment_json TEXT,
                last_error TEXT,
                poll_attempts INTEGER NOT NULL DEFAULT 0,
                next_poll_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(plan_id) REFERENCES plans(id)
            );

            CREATE TABLE IF NOT EXISTS webhook_events (
                event_id TEXT PRIMARY KEY,
                received_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS callback_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_key TEXT NOT NULL UNIQUE,
                order_id INTEGER NOT NULL,
                callback_url TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                delivered_at TEXT,
                last_error TEXT
            );
            """


def _migrate_plans_selection(db: sqlite3.Connection) -> None:
    """Track which plans are published and still offered upstream."""
    columns = {row[1] for row in db.execute("PRAGMA table_info(plans)")}
    if "published" not in columns:
        db.execute("ALTER TABLE plans ADD COLUMN published INTEGER NOT NULL DEFAULT 0")
        db.execute("UPDATE plans SET published = 1 WHERE available = 1")
    if "upstream_active" not in columns:
        db.execute("ALTER TABLE plans ADD COLUMN upstream_active INTEGER NOT NULL DEFAULT 0")
        for row in db.execute("SELECT id,raw_json FROM plans").fetchall():
            db.execute(
                "UPDATE plans SET upstream_active=? WHERE id=?",
                (int(bool(json_load(row["raw_json"], {}).get("available"))), row["id"]),
            )


def _migrate_purchase_columns(db: sqlite3.Connection) -> None:
    """Quote protection, frozen purchase request and cancellation bookkeeping."""
    columns = {row[1] for row in db.execute("PRAGMA table_info(orders)")}
    for name, definition in {
        "unit_cost": "TEXT",
        "purchase_attempted": "INTEGER NOT NULL DEFAULT 0",
        "purchase_payload_json": "TEXT",
        "plan_snapshot_json": "TEXT",
        "cancel_requested": "INTEGER NOT NULL DEFAULT 0",
        "cancel_results_json": "TEXT NOT NULL DEFAULT '{}'",
        "refunded_amount": "TEXT NOT NULL DEFAULT '0.00'",
        "cancel_error": "TEXT",
    }.items():
        if name not in columns:
            db.execute(f"ALTER TABLE orders ADD COLUMN {name} {definition}")
    if "purchase_attempted" not in columns:
        # Existing uncertain purchases must never be treated as unsubmitted.
        db.execute("UPDATE orders SET purchase_attempted=1 WHERE stellar_order_id IS NOT NULL OR status='creating'")


def _migrate_queue_columns(db: sqlite3.Connection) -> None:
    """Durable webhook payloads, retry state and dead-letter markers."""
    columns = {row[1] for row in db.execute("PRAGMA table_info(webhook_events)")}
    for name, definition in {
        "payload_json": "TEXT",
        "processed_at": "TEXT",
        "attempts": "INTEGER NOT NULL DEFAULT 0",
        "next_attempt_at": "TEXT",
        "last_error": "TEXT",
        "failed_at": "TEXT",
    }.items():
        if name not in columns:
            db.execute(f"ALTER TABLE webhook_events ADD COLUMN {name} {definition}")
    db.execute("UPDATE webhook_events SET processed_at=received_at WHERE payload_json IS NULL")
    outbox_columns = {row[1] for row in db.execute("PRAGMA table_info(callback_outbox)")}
    if "failed_at" not in outbox_columns:
        db.execute("ALTER TABLE callback_outbox ADD COLUMN failed_at TEXT")


def _migrate_hot_path_indexes(db: sqlite3.Connection) -> None:
    """Indexes for the polling, queue and selection queries."""
    for statement in (
        "CREATE INDEX IF NOT EXISTS idx_orders_poll ON orders(status, next_poll_at)",
        "CREATE INDEX IF NOT EXISTS idx_orders_stellar ON orders(stellar_order_id)",
        "CREATE INDEX IF NOT EXISTS idx_orders_downstream ON orders(downstream_order_no)",
        "CREATE INDEX IF NOT EXISTS idx_plans_selection ON plans(available, published)",
        "CREATE INDEX IF NOT EXISTS idx_outbox_due ON callback_outbox(delivered_at, failed_at, next_attempt_at)",
        "CREATE INDEX IF NOT EXISTS idx_webhook_due ON webhook_events(processed_at, failed_at, next_attempt_at)",
    ):
        db.execute(statement)


# Numbered, ordered migrations. Each step is idempotent so databases created by
# earlier releases (user_version = 0 but partially migrated) upgrade in place.
MIGRATIONS = (
    ("plans selection columns", _migrate_plans_selection),
    ("order purchase and cancellation columns", _migrate_purchase_columns),
    ("queue retry and dead-letter columns", _migrate_queue_columns),
    ("hot path indexes", _migrate_hot_path_indexes),
)


def init_db() -> None:
    os.makedirs(os.path.dirname(settings.DB_PATH) or ".", exist_ok=True)
    with db_connect() as db:
        db.executescript(SCHEMA_TABLES)
        version = int(db.execute("PRAGMA user_version").fetchone()[0])
        for index, (name, migrate) in enumerate(MIGRATIONS, start=1):
            if version >= index:
                continue
            migrate(db)
            db.execute(f"PRAGMA user_version = {index}")
            log.info("applied schema migration %s: %s", index, name)
