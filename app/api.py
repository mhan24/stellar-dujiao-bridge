"""FastAPI application: authentication, HTTP routes and background scheduling."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from . import catalogue, logs, orders, presentation, queues, settings, store, util, wallet_balance
from .security import callback_url_error, verify_dujiao_signature, verify_stellar_signature
from .stellar_client import stellar
from .util import iso

log = logging.getLogger("stellar-dujiao-bridge")
logs.configure(settings.LOG_LEVEL, settings.LOG_FORMAT)


# Every background task is tracked so shutdown can cancel and await it.
background_tasks: set[asyncio.Task[Any]] = set()


def _int_field(payload: dict[str, Any], key: str) -> int:
    """Parse an integer payload field, rejecting anything else as a 400."""
    value = payload.get(key)
    if value is None:
        raise HTTPException(400, "sku_id and quantity must be integers")
    try:
        return int(value)
    except (TypeError, ValueError):
        raise HTTPException(400, "sku_id and quantity must be integers") from None


async def require_dujiao(request: Request) -> None:
    if not settings.DUJIAO_API_KEY or not settings.DUJIAO_API_SECRET:
        raise HTTPException(503, "Dujiao credentials are not configured")
    body = await request.body()
    valid = verify_dujiao_signature(
        settings.DUJIAO_API_SECRET,
        request.method,
        request.url.path,
        request.headers.get("Dujiao-Next-Timestamp", ""),
        request.headers.get("Dujiao-Next-Signature", ""),
        body,
    )
    if not valid or request.headers.get("Dujiao-Next-Api-Key") != settings.DUJIAO_API_KEY:
        raise HTTPException(401, "invalid Dujiao credentials")


def spawn(coro: Any, name: str) -> asyncio.Task[Any]:
    """Track every background task so shutdown can cancel and await it."""
    task = asyncio.create_task(coro, name=name)
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    return task


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    # Bad settings raise here, before the process starts serving requests.
    for message in settings.validate():
        log.warning("%s", message)
    util.init_db()
    # Serve cached catalogue immediately; stale data has zero sellable stock.
    spawn(catalogue.sync_catalogue(), "catalogue-initial-sync")
    spawn(worker_loop(), "order-worker")
    spawn(queues.maintenance_loop(), "durable-queues")
    try:
        yield
    finally:
        pending = list(background_tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await stellar.close()


async def worker_loop() -> None:
    # lifespan() performs the initial sync; do not immediately start a second
    # paginated request burst and trip Stellar's account rate limit.
    last_sync = time.time()
    while True:
        try:
            if time.time() - last_sync >= settings.SYNC_INTERVAL_SECONDS:
                last_sync = time.time()
                # Catalogue pagination must not block order fulfillment polling.
                spawn(catalogue.sync_catalogue(), "catalogue-sync")
            with util.db_connect() as db:
                rows = db.execute(
                    "SELECT id FROM orders WHERE status NOT IN ('delivered','failed','canceled') "
                    "AND (next_poll_at IS NULL OR next_poll_at <= ?)",
                    (iso(),),
                ).fetchall()
            for row in rows[:20]:
                try:
                    await orders.refresh_order(int(row["id"]))
                except Exception:
                    log.exception("order refresh failed: %s", row["id"])
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("worker loop error")
            await asyncio.sleep(5)


app = FastAPI(title="Stellar Wholesale eSIM Bridge", version="1.0.0", lifespan=lifespan)


class BodyLimitMiddleware:
    """Reject oversized request bodies before they are verified or buffered."""

    def __init__(self, app: Any, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http" or self.max_bytes <= 0:
            await self.app(scope, receive, send)
            return
        declared = dict(scope.get("headers") or []).get(b"content-length")
        if declared and declared.isdigit() and int(declared) > self.max_bytes:
            await self._reject(scope, receive, send)
            return
        buffered: list[dict[str, Any]] = []
        received = 0
        while True:
            message = await receive()
            if message.get("type") != "http.request":
                buffered.append(message)
                break
            received += len(message.get("body") or b"")
            if received > self.max_bytes:
                await self._reject(scope, receive, send)
                return
            buffered.append(message)
            if not message.get("more_body"):
                break

        async def replay() -> dict[str, Any]:
            if buffered:
                return buffered.pop(0)
            return {"type": "http.request", "body": b"", "more_body": False}

        await self.app(scope, replay, send)

    async def _reject(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        log.warning(
            "Rejected %s %s: request body exceeds %s bytes", scope.get("method"), scope.get("path"), self.max_bytes
        )
        await JSONResponse(
            status_code=413,
            content={"ok": False, "error_code": "request_too_large", "error_message": "request body too large"},
        )(scope, receive, send)


app.add_middleware(BodyLimitMiddleware, max_bytes=settings.MAX_REQUEST_BODY_BYTES)


# Dujiao HTTP API


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "service": "stellar-dujiao-bridge", "time": iso()}


@app.post("/api/v1/upstream/ping", dependencies=[Depends(require_dujiao)])
async def ping() -> dict[str, Any]:
    wallet = await wallet_balance.wallet_snapshot(force=True)
    balance = wallet.get("available_balance")
    return {
        "ok": True,
        "site_name": settings.SITE_NAME,
        "protocol_version": "1.0",
        "user_id": 1,
        "balance": f"{balance:.2f}" if isinstance(balance, Decimal) else "0.00",
        "currency": str(wallet.get("currency") or "EUR"),
        "member_level": None,
        "balance_checked_at": wallet.get("checked_at_iso"),
        "balance_error": wallet.get("error"),
    }


@app.get("/api/v1/upstream/status", dependencies=[Depends(require_dujiao)])
async def status() -> dict[str, Any]:
    """Operational snapshot: catalogue age, order backlog, queue backlog, wallet cache."""
    counts = store.metrics()
    synced_at = store.get_state("catalogue_synced_at")
    age_seconds = int((datetime.now(timezone.utc) - util.parse_iso(synced_at)).total_seconds()) if synced_at else None
    cached_wallet = wallet_balance.cached_snapshot()
    balance = cached_wallet.get("available_balance")
    return {
        "ok": True,
        "catalogue": {
            "synced_at": synced_at,
            "age_seconds": age_seconds,
            "fresh": catalogue.catalogue_is_fresh(),
            "selection_model": settings.PLAN_SELECTION_MODEL,
            "top_plan_limit": settings.TOP_PLAN_LIMIT,
            **{key: value for key, value in counts.items() if key.startswith("plans_")},
        },
        "orders": {key: value for key, value in counts.items() if key.startswith("orders_")},
        "queues": {
            **{
                key: value
                for key, value in counts.items()
                if key.startswith("callbacks_") or key.startswith("webhooks_")
            },
            "callback_max_attempts": settings.CALLBACK_MAX_ATTEMPTS,
            "webhook_max_attempts": settings.WEBHOOK_MAX_ATTEMPTS,
            "retention_days": settings.QUEUE_RETENTION_DAYS,
        },
        "wallet": {
            "currency": cached_wallet.get("currency"),
            "available_balance": f"{balance:.2f}" if isinstance(balance, Decimal) else None,
            "checked_at": cached_wallet.get("checked_at_iso"),
            "error": cached_wallet.get("error"),
        },
        "background_tasks": sorted(task.get_name() for task in background_tasks),
    }


@app.get("/api/v1/upstream/categories", dependencies=[Depends(require_dujiao)])
async def categories() -> dict[str, Any]:
    return {
        "ok": True,
        "categories": [
            {
                "id": 1,
                "parent_id": 0,
                "slug": "esim",
                "name": {"zh-CN": "eSIM", "en": "eSIM"},
                "icon": "",
                "sort_order": 100,
            }
        ],
    }


@app.get("/api/v1/upstream/products", dependencies=[Depends(require_dujiao)])
async def products(page: int = 1, page_size: int = 20, include_inactive: bool = False) -> dict[str, Any]:
    page = max(1, page)
    page_size = min(100, max(1, page_size))
    plans = presentation.available_plan_rows()
    wallet = await catalogue.product_wallet_snapshot()
    reserved_cost = catalogue.reserved_wholesale_cost()
    visible = bool(plans) and (include_inactive or any(bool(plan["available"]) for plan in plans))
    bundle = (
        presentation.product_bundle(plans, wallet, reserved_cost, wallet.get("catalogue_fresh"))
        if visible and page == 1
        else None
    )
    return {
        "ok": True,
        "items": [bundle] if bundle else [],
        "total": 1 if visible else 0,
        "includes_inactive": include_inactive,
        "page": page,
        "page_size": page_size,
    }


@app.get("/api/v1/upstream/products/{product_id}", dependencies=[Depends(require_dujiao)])
async def product(product_id: int) -> Any:
    if product_id == settings.PRODUCT_BUNDLE_ID:
        plans = presentation.available_plan_rows()
        if plans:
            wallet = await catalogue.product_wallet_snapshot()
            return {
                "ok": True,
                "product": presentation.product_bundle(
                    plans, wallet, catalogue.reserved_wholesale_cost(), wallet.get("catalogue_fresh")
                ),
            }
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error_code": "product_not_found", "error_message": "product not found"},
        )
    plan = store.get_plan(product_id)
    if not plan or not plan["available"]:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error_code": "product_not_found", "error_message": "product not found"},
        )
    wallet = await catalogue.product_wallet_snapshot()
    stock_quantity = catalogue.wallet_capacity_for_plan(
        plan, wallet, catalogue.reserved_wholesale_cost(), wallet.get("catalogue_fresh")
    )
    return {"ok": True, "product": presentation.product_from_plan(plan, stock_quantity)}


@app.post("/api/v1/upstream/orders", dependencies=[Depends(require_dujiao)])
async def create_order(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(400, "invalid request body") from exc
    if not isinstance(payload, dict):
        raise HTTPException(400, "invalid request body")
    local_sku_id = _int_field(payload, "sku_id")
    quantity = _int_field(payload, "quantity")
    if local_sku_id <= 0 or quantity <= 0:
        raise HTTPException(400, "sku_id and quantity are required")
    if quantity > 10:
        raise HTTPException(400, "当前上游每个套餐最多采购 10 张")
    downstream = str(payload.get("downstream_order_no") or "").strip() or None
    if not downstream and settings.REQUIRE_DOWNSTREAM_ORDER_NO:
        # Without a downstream number there is no durable idempotency key, and a
        # retried request would purchase twice at the supplier.
        raise HTTPException(400, "downstream_order_no is required for idempotent ordering")
    callback_url = str(payload.get("callback_url") or "").strip()
    if callback_url:
        problem = callback_url_error(callback_url)
        if problem:
            log.warning("Rejected callback URL from downstream order: %s", problem)
            return JSONResponse(
                status_code=400,
                content={
                    "ok": False,
                    "error_code": "invalid_callback_url",
                    "error_message": f"callback_url not accepted: {problem}",
                },
            )
    existing = store.find_order_by_downstream(downstream or "")
    if existing:
        if existing["plan_id"] != local_sku_id or existing["quantity"] != quantity:
            raise HTTPException(409, "同一订单号不能修改套餐或数量")
        if existing["status"] == "creating":
            spawn(orders.refresh_order(existing["id"], force=True), f"order-refresh-{existing['id']}")
        return JSONResponse(status_code=200, content=presentation.order_response(existing, include_items=False))
    async with orders.order_create_lock:
        # Concurrent downstream retries must not create a second reservation.
        existing = store.find_order_by_downstream(downstream or "")
        if existing:
            if existing["plan_id"] != local_sku_id or existing["quantity"] != quantity:
                raise HTTPException(409, "同一订单号不能修改套餐或数量")
            return JSONResponse(status_code=200, content=presentation.order_response(existing, include_items=False))
        plan = store.get_plan(local_sku_id)
        if not plan or not plan["available"]:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error_code": "sku_unavailable", "error_message": "eSIM plan unavailable"},
            )
        wallet = await wallet_balance.wallet_snapshot(force=True)
        capacity = catalogue.wallet_capacity_for_plan(plan, wallet, catalogue.reserved_wholesale_cost())
        if wallet.get("available_balance") is None:
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error_code": "wholesale_balance_unavailable",
                    "error_message": "批发余额暂时无法读取，为避免采购失败已暂停购买",
                },
            )
        if capacity < quantity:
            currency = str(wallet.get("currency") or plan.get("currency") or "").upper()
            return JSONResponse(
                status_code=409,
                content={
                    "ok": False,
                    "error_code": "insufficient_wholesale_balance",
                    "error_message": f"批发余额不足，当前该 SKU 最多可购买 {capacity} 件（余额币种：{currency}）",
                },
            )
        local_id = orders.create_local_order(plan, quantity, downstream, callback_url)
    order = await orders.refresh_order(local_id, force=True)
    if not order:
        raise HTTPException(500, "failed to create local order")
    if order["status"] == "failed":
        return JSONResponse(
            status_code=409,
            content={
                "ok": False,
                "order_id": order["id"],
                "order_no": order["order_no"],
                "status": "canceled",
                "error_code": "purchase_failed",
                "error_message": order.get("last_error") or "Stellar purchase failed",
            },
        )
    return JSONResponse(
        status_code=200,
        content={
            "ok": True,
            "order_id": order["id"],
            "order_no": order["order_no"],
            "status": "paid",
            "amount": order["amount"],
            "currency": order["currency"],
        },
    )


@app.get("/api/v1/upstream/orders/{order_id}", dependencies=[Depends(require_dujiao)])
async def get_upstream_order(order_id: int) -> Any:
    order = await orders.refresh_order(order_id)
    if not order:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error_code": "order_not_found", "error_message": "order not found"},
        )
    return presentation.order_response(order)


@app.post("/api/v1/upstream/orders/{order_id}/cancel", dependencies=[Depends(require_dujiao)])
async def cancel_upstream_order(order_id: int) -> JSONResponse:
    async with orders.order_lock(order_id):
        order = store.get_order(order_id)
        if order and order["status"] != "canceled":
            queues.update_order(order_id, cancel_error=None)
        return await orders._cancel_order(order_id)


# Stellar webhook ingestion


@app.post("/webhooks/stellar")
async def stellar_webhook(request: Request) -> dict[str, Any]:
    raw_body = await request.body()
    if not settings.STELLAR_WEBHOOK_SECRET or not verify_stellar_signature(
        settings.STELLAR_WEBHOOK_SECRET,
        request.headers.get("Stellar-Signature", ""),
        raw_body,
    ):
        raise HTTPException(401, "invalid Stellar webhook signature")
    try:
        event = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise HTTPException(400, "invalid JSON") from exc
    event_id = str(event.get("id") or request.headers.get("Stellar-Event-Id") or "")
    if not event_id:
        raise HTTPException(400, "missing event id")
    event["type"] = str(event.get("type") or request.headers.get("Stellar-Event-Type") or "")
    with util.db_connect() as db:
        inserted = db.execute(
            "INSERT OR IGNORE INTO webhook_events(event_id,received_at,payload_json) VALUES (?,?,?)",
            (event_id, iso(), json.dumps(event, ensure_ascii=False)),
        ).rowcount
    return {"ok": True, "duplicate": not bool(inserted)}
