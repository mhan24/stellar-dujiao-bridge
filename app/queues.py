"""Durable notification queue: order transitions, callbacks and webhooks."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
import time
from datetime import timedelta
from typing import Any
from urllib.parse import urlsplit

import httpx

from . import catalogue, presentation, settings, store
from .security import callback_url_error, dujiao_signature
from .util import db_connect, iso, json_load, now

# Circular dependency with orders is resolved at call time.

log = logging.getLogger("stellar-dujiao-bridge")


outbox_lock = asyncio.Lock()
webhook_lock = asyncio.Lock()


def update_order(local_id: int, **fields: Any) -> None:
    fields["updated_at"] = iso()
    columns = ", ".join(f"{key} = ?" for key in fields)
    values = list(fields.values()) + [local_id]
    with db_connect() as db:
        previous = db.execute("SELECT * FROM orders WHERE id=?", (local_id,)).fetchone()
        db.execute(f"UPDATE orders SET {columns} WHERE id = ?", values)
        if previous and fields.get("status") and previous["status"] != fields["status"]:
            changed = dict(db.execute("SELECT * FROM orders WHERE id=?", (local_id,)).fetchone())
            # Only notify when the status the downstream can see actually changes;
            # internal transitions such as creating -> fulfilling are one state
            # downstream and would otherwise resend the same event.
            visible_change = presentation.protocol_status(previous["status"]) != presentation.protocol_status(
                changed["status"]
            )
            if visible_change or changed.get("fulfillment_json"):
                enqueue_callback(db, changed, changed["status"], json_load(changed.get("fulfillment_json"), None))
            log.info(
                "order %s: %s -> %s",
                local_id,
                previous["status"],
                changed["status"],
                extra={
                    "order_id": local_id,
                    "from_status": previous["status"],
                    "to_status": changed["status"],
                    "notified": visible_change or bool(changed.get("fulfillment_json")),
                },
            )


def enqueue_callback(
    db: sqlite3.Connection, order: dict[str, Any], callback_status: str, fulfillment: dict[str, Any] | None
) -> None:
    callback_url = order.get("callback_url")
    if not callback_url:
        return
    # Defence in depth: historical rows may predate callback URL validation.
    problem = callback_url_error(str(callback_url))
    if problem:
        log.warning("Callback suppressed for order %s: %s", order.get("id"), problem)
        return
    payload = {
        "event": "order.fulfilled" if callback_status == "delivered" else "order.status",
        "order_id": order["id"],
        "order_no": order["order_no"],
        "downstream_order_no": order.get("downstream_order_no") or order["order_no"],
        "status": presentation.protocol_status(callback_status),
    }
    if fulfillment:
        payload["fulfillment"] = fulfillment
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    key = hashlib.sha256((str(callback_url) + body).encode()).hexdigest()
    db.execute(
        """INSERT OR IGNORE INTO callback_outbox
               (notification_key,order_id,callback_url,payload_json,next_attempt_at) VALUES (?,?,?,?,?)""",
        (key, order["id"], callback_url, body, iso()),
    )


async def process_callback_outbox() -> None:
    if outbox_lock.locked():
        return
    async with outbox_lock:
        with db_connect() as db:
            jobs = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM callback_outbox WHERE delivered_at IS NULL AND failed_at IS NULL "
                    "AND next_attempt_at<=? ORDER BY id LIMIT 10",
                    (iso(),),
                )
            ]
        async with httpx.AsyncClient(timeout=min(settings.HTTP_TIMEOUT_SECONDS, 10)) as client:
            for job in jobs:
                timestamp = int(time.time())
                payload = json.loads(job["payload_json"])
                payload["timestamp"] = timestamp
                body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
                path = urlsplit(job["callback_url"]).path or "/"
                headers = {
                    "Content-Type": "application/json",
                    "Dujiao-Next-Api-Key": settings.DUJIAO_API_KEY,
                    "Dujiao-Next-Timestamp": str(timestamp),
                    "Dujiao-Next-Signature": dujiao_signature(
                        settings.DUJIAO_API_SECRET, "POST", path, timestamp, body
                    ),
                }
                error = None
                try:
                    response = await client.post(job["callback_url"], content=body, headers=headers)
                    if not 200 <= response.status_code < 300:
                        error = f"HTTP {response.status_code}"
                except httpx.HTTPError as exc:
                    error = type(exc).__name__
                attempts = job["attempts"] + 1
                with db_connect() as db:
                    if not error:
                        db.execute(
                            "UPDATE callback_outbox SET attempts=?,delivered_at=?,last_error=NULL WHERE id=?",
                            (attempts, iso(), job["id"]),
                        )
                    elif attempts >= settings.CALLBACK_MAX_ATTEMPTS:
                        # Stop retrying but keep the row so operators can inspect it.
                        db.execute(
                            "UPDATE callback_outbox SET attempts=?,last_error=?,failed_at=? WHERE id=?",
                            (attempts, error, iso(), job["id"]),
                        )
                        log.error(
                            "Callback abandoned after %s attempts: order=%s url=%s error=%s",
                            attempts,
                            job["order_id"],
                            job["callback_url"],
                            error,
                        )
                    else:
                        delay = min(5 * 2 ** min(attempts - 1, 6), 300)
                        db.execute(
                            "UPDATE callback_outbox SET attempts=?,last_error=?,next_attempt_at=? WHERE id=?",
                            (attempts, error, iso(now() + timedelta(seconds=delay)), job["id"]),
                        )


# Rate limit for the "unknown eSIM" fallback sweep below.
_esim_sweep_at = 0.0


async def refresh_orders_for_esim_event(data: dict[str, Any]) -> int:
    """Turn an `esim.*` webhook into an order refresh instead of ignoring it.

    The event carries a sim_id but usually not our order, so resolve the owner
    first and fall back to a capped, rate-limited sweep of in-flight orders.
    """
    from . import orders

    order: dict[str, Any] | None = None
    upstream_order_id = data.get("order_id")
    if upstream_order_id:
        order = store.find_order_by_stellar_id(str(upstream_order_id))
    sim_id = str(data.get("sim_id") or "")
    if order is None and sim_id:
        order = store.find_order_by_sim_id(sim_id)
    if order is not None:
        await orders.refresh_order(order["id"], force=True)
        return 1
    global _esim_sweep_at
    now_monotonic = time.monotonic()
    if (
        settings.ESIM_EVENT_SWEEP_COOLDOWN_SECONDS
        and now_monotonic - _esim_sweep_at < settings.ESIM_EVENT_SWEEP_COOLDOWN_SECONDS
    ):
        log.info("eSIM event sweep skipped (cooldown): sim_id=%s", sim_id or "unknown")
        return 0
    _esim_sweep_at = now_monotonic
    refreshed = 0
    for local_id in store.in_flight_order_ids(settings.ESIM_EVENT_SWEEP_LIMIT):
        await orders.refresh_order(local_id, force=True)
        refreshed += 1
    log.info("eSIM event for unmapped sim_id=%s refreshed %s in-flight order(s)", sim_id or "unknown", refreshed)
    return refreshed


async def process_webhook_events() -> None:
    if webhook_lock.locked():
        return
    async with webhook_lock:
        with db_connect() as db:
            events = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM webhook_events WHERE processed_at IS NULL AND failed_at IS NULL "
                    "AND (next_attempt_at IS NULL OR next_attempt_at<=?) ORDER BY received_at LIMIT 10",
                    (iso(),),
                )
            ]
        for saved in events:
            try:
                event = json.loads(saved["payload_json"])
                event_type = str(event.get("type") or "")
                data = event.get("data") or {}
                if event_type == "catalogue.updated":
                    await catalogue.reconcile_catalogue_event(event, saved["received_at"])
                elif event_type.startswith("esim."):
                    await refresh_orders_for_esim_event(data)
                else:
                    stellar_id = data.get("order_id") or (data.get("id") if event_type.startswith("order.") else None)
                    order = store.find_order_by_stellar_id(str(stellar_id or ""))
                    if order:
                        from . import orders

                        await orders.refresh_order(order["id"], force=True)
                with db_connect() as db:
                    db.execute(
                        "UPDATE webhook_events SET processed_at=?,last_error=NULL WHERE event_id=?",
                        (iso(), saved["event_id"]),
                    )
            except Exception as exc:
                attempts = saved["attempts"] + 1
                with db_connect() as db:
                    if attempts >= settings.WEBHOOK_MAX_ATTEMPTS:
                        db.execute(
                            "UPDATE webhook_events SET attempts=?,last_error=?,failed_at=? WHERE event_id=?",
                            (attempts, type(exc).__name__, iso(), saved["event_id"]),
                        )
                    else:
                        db.execute(
                            "UPDATE webhook_events SET attempts=?,last_error=?,next_attempt_at=? WHERE event_id=?",
                            (
                                attempts,
                                type(exc).__name__,
                                iso(now() + timedelta(seconds=min(5 * 2 ** min(attempts - 1, 6), 300))),
                                saved["event_id"],
                            ),
                        )
                if attempts >= settings.WEBHOOK_MAX_ATTEMPTS:
                    log.error(
                        "Webhook event abandoned after %s attempts: %s (%s)",
                        attempts,
                        saved["event_id"],
                        type(exc).__name__,
                    )
                else:
                    log.warning("Webhook processing deferred: %s (%s)", saved["event_id"], type(exc).__name__)


def prune_durable_queues() -> tuple[int, int, int]:
    """Drop finished queue rows and expired delivery payloads; report the counts."""
    cutoff = iso(now() - timedelta(days=settings.QUEUE_RETENTION_DAYS))
    purged = 0
    with db_connect() as db:
        callbacks = db.execute(
            "DELETE FROM callback_outbox WHERE COALESCE(delivered_at, failed_at) IS NOT NULL "
            "AND COALESCE(delivered_at, failed_at) < ?",
            (cutoff,),
        ).rowcount
        events = db.execute(
            "DELETE FROM webhook_events WHERE COALESCE(processed_at, failed_at) IS NOT NULL "
            "AND COALESCE(processed_at, failed_at) < ?",
            (cutoff,),
        ).rowcount
        if settings.FULFILLMENT_RETENTION_DAYS > 0:
            # QR codes and activation codes are only kept for post-sale support.
            purged = db.execute(
                "UPDATE orders SET fulfillment_json = NULL WHERE fulfillment_json IS NOT NULL "
                "AND status IN ('delivered', 'canceled', 'failed') AND updated_at < ?",
                (iso(now() - timedelta(days=settings.FULFILLMENT_RETENTION_DAYS)),),
            ).rowcount
    if callbacks or events or purged:
        log.info("Pruned: %s callbacks, %s webhook events, %s delivery payloads", callbacks, events, purged)
    return callbacks, events, purged


async def prune_loop() -> None:
    while True:
        try:
            prune_durable_queues()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Durable queue pruning failed")
        await asyncio.sleep(max(settings.QUEUE_PRUNE_INTERVAL_SECONDS, 60))


async def maintenance_loop() -> None:
    # Independent loops keep rate-limited catalogue events from delaying delivery callbacks.
    async def loop(processor: Any) -> None:
        while True:
            try:
                await processor()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Durable queue processing failed")
            await asyncio.sleep(2)

    await asyncio.gather(loop(process_callback_outbox), loop(process_webhook_events), loop(prune_loop))
