"""Maintain ascending retail prices on Dujiao's descending sort_order UI."""

from __future__ import annotations

import os
import sqlite3
from decimal import Decimal
from pathlib import Path


def sort_skus(db_path: str, product_id: int) -> int:
    if product_id <= 0:
        raise ValueError("A positive product ID is required")
    # mode=rw refuses to create a new database if the configured path is wrong.

    uri = Path(db_path).resolve().as_uri() + "?mode=rw"
    db = sqlite3.connect(uri, uri=True, timeout=10)
    try:
        with db:
            # Serialize with the native sync: price reads and sorting share one transaction.
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                """SELECT id,price_amount,sort_order FROM product_skus
                WHERE product_id=? AND is_active=1 AND deleted_at IS NULL""",
                (product_id,),
            ).fetchall()

            def key(row):
                price = Decimal(str(row[1]))
                if not price.is_finite() or price < 0:
                    raise ValueError("Invalid SKU retail price")
                return price, row[0]

            rows.sort(key=key)
            updates = [
                (len(rows) - 1 - index, row[0]) for index, row in enumerate(rows) if row[2] != len(rows) - 1 - index
            ]
            db.executemany(
                "UPDATE product_skus SET sort_order=? WHERE id=? AND product_id=?",
                [(order, sku_id, product_id) for order, sku_id in updates],
            )
            return len(updates)
    finally:
        db.close()


def install_sort_triggers(db_path: str, product_id: int) -> None:
    """Reorder in the native write transaction; the timer remains a repair fallback."""
    if product_id <= 0:
        raise ValueError("A positive product ID is required")
    uri = Path(db_path).resolve().as_uri() + "?mode=rw"
    db = sqlite3.connect(uri, uri=True, timeout=10)
    # product_id is validated as an integer by the CLI; names are product scoped.
    product_id = int(product_id)
    reorder = f"""UPDATE product_skus SET sort_order=(
        SELECT count(*) FROM product_skus AS peer
        WHERE peer.product_id={product_id} AND peer.is_active=1 AND peer.deleted_at IS NULL
          AND (CAST(peer.price_amount AS NUMERIC)>CAST(product_skus.price_amount AS NUMERIC)
            OR (CAST(peer.price_amount AS NUMERIC)=CAST(product_skus.price_amount AS NUMERIC)
                AND peer.id>product_skus.id)))
        WHERE product_id={product_id} AND is_active=1 AND deleted_at IS NULL;"""
    try:
        with db:
            db.execute("BEGIN IMMEDIATE")
            for event, suffix, condition in [
                ("INSERT", "insert", f"NEW.product_id={product_id}"),
                (
                    "UPDATE OF price_amount,is_active,deleted_at,product_id",
                    "update",
                    f"NEW.product_id={product_id} OR OLD.product_id={product_id}",
                ),
                ("DELETE", "delete", f"OLD.product_id={product_id}"),
            ]:
                name = f"bridge_sku_price_sort_{product_id}_{suffix}"
                db.execute(f"DROP TRIGGER IF EXISTS {name}")
                db.execute(f"CREATE TRIGGER {name} AFTER {event} ON product_skus WHEN {condition} BEGIN {reorder} END")
    finally:
        db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--install-triggers", action="store_true")
    args = parser.parse_args()
    db_path = os.environ["DUJIAO_DB_PATH"]
    product_id = int(os.environ["SORT_PRODUCT_ID"])
    if args.install_triggers:
        install_sort_triggers(db_path, product_id)
        print("Installed transaction-level SKU sorting triggers", flush=True)
    count = sort_skus(db_path, product_id)
    if count:
        print(f"Updated sort_order for {count} active SKUs", flush=True)
