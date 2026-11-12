"""Read/write helpers for the local plans and orders tables."""

from __future__ import annotations

from typing import Any

from .util import db_connect, row_dict


def get_plan(local_id: int) -> dict[str, Any] | None:
    with db_connect() as db:
        return row_dict(db.execute("SELECT * FROM plans WHERE id = ?", (local_id,)).fetchone())


def get_order(local_id: int) -> dict[str, Any] | None:
    with db_connect() as db:
        return row_dict(db.execute("SELECT * FROM orders WHERE id = ?", (local_id,)).fetchone())


def find_order_by_stellar_id(stellar_order_id: str) -> dict[str, Any] | None:
    with db_connect() as db:
        return row_dict(db.execute("SELECT * FROM orders WHERE stellar_order_id = ?", (stellar_order_id,)).fetchone())


def find_order_by_downstream(downstream_order_no: str) -> dict[str, Any] | None:
    if not downstream_order_no:
        return None
    with db_connect() as db:
        return row_dict(
            db.execute("SELECT * FROM orders WHERE downstream_order_no = ?", (downstream_order_no,)).fetchone()
        )


def insert_order(
    *,
    order_no: str,
    downstream_order_no: str | None,
    callback_url: str,
    plan_id: int,
    quantity: int,
    days: int | None,
    idempotency_key: str,
    currency: str,
    unit_cost: str,
    plan_snapshot_json: str,
    created_at: str,
) -> int:
    """Persist a pending order row and return its local id."""
    with db_connect() as db:
        cursor = db.execute(
            """
            INSERT INTO orders (
                order_no, downstream_order_no, callback_url, plan_id, quantity, days,
                idempotency_key, status, currency, next_poll_at, created_at, updated_at,
                unit_cost, plan_snapshot_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'creating', ?, ?, ?, ?, ?, ?)
            """,
            (
                order_no,
                downstream_order_no,
                callback_url,
                plan_id,
                quantity,
                days,
                idempotency_key,
                currency,
                created_at,
                created_at,
                created_at,
                unit_cost,
                plan_snapshot_json,
            ),
        )
        return int(cursor.lastrowid or 0)


def get_state(key: str) -> str | None:
    with db_connect() as db:
        row = db.execute("SELECT value FROM bridge_state WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row else None


def in_flight_order_ids(limit: int) -> list[int]:
    """Orders whose fulfillment is still being resolved, newest first."""
    if limit <= 0:
        return []
    with db_connect() as db:
        rows = db.execute(
            "SELECT id FROM orders WHERE status IN ('creating', 'fulfilling') ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [int(row[0]) for row in rows]


def find_order_by_sim_id(sim_id: str) -> dict[str, Any] | None:
    """Locate a delivered order by eSIM id without scanning every row in Python."""
    if not sim_id:
        return None
    with db_connect() as db:
        return row_dict(
            db.execute(
                """
            SELECT o.* FROM orders o, json_each(o.fulfillment_json, '$.delivery_data.esims') AS esim
            WHERE o.fulfillment_json IS NOT NULL
              AND json_extract(esim.value, '$.sim_id') = ?
            LIMIT 1
            """,
                (sim_id,),
            ).fetchone()
        )


def metrics() -> dict[str, int]:
    """Cheap counters for the operator status endpoint."""
    queries = {
        "plans_total": "SELECT count(*) FROM plans",
        "plans_active": "SELECT count(*) FROM plans WHERE available = 1",
        "plans_published": "SELECT count(*) FROM plans WHERE published = 1",
        "plans_upstream_active": "SELECT count(*) FROM plans WHERE upstream_active = 1",
        "orders_in_flight": "SELECT count(*) FROM orders WHERE status IN ('creating', 'fulfilling')",
        "orders_manual_review": "SELECT count(*) FROM orders WHERE status = 'manual_review'",
        "orders_delivered": "SELECT count(*) FROM orders WHERE status = 'delivered'",
        "callbacks_pending": "SELECT count(*) FROM callback_outbox WHERE delivered_at IS NULL AND failed_at IS NULL",
        "callbacks_failed": "SELECT count(*) FROM callback_outbox WHERE failed_at IS NOT NULL",
        "webhooks_pending": "SELECT count(*) FROM webhook_events WHERE processed_at IS NULL AND failed_at IS NULL",
        "webhooks_failed": "SELECT count(*) FROM webhook_events WHERE failed_at IS NOT NULL",
    }
    with db_connect() as db:
        return {name: int(db.execute(sql).fetchone()[0]) for name, sql in queries.items()}
