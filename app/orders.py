"""Procurement, fulfillment polling and cancellation reconciliation."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from fastapi.responses import JSONResponse

from . import catalogue, presentation, queues, store, wallet_balance
from .stellar_client import StellarError, stellar
from .util import iso, json_load, now, parse_iso

log = logging.getLogger("stellar-dujiao-bridge")


order_create_lock = asyncio.Lock()
order_locks: dict[int, OrderLock] = {}


class OrderLock:
    """Per-order mutex with a user count so idle entries can be dropped."""

    __slots__ = ("lock", "users")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.users = 0


@asynccontextmanager
async def order_lock(order_id: int) -> AsyncIterator[None]:
    """Serialize work for one order without leaking a lock per order forever."""
    entry = order_locks.get(order_id)
    if entry is None:
        entry = order_locks[order_id] = OrderLock()
    entry.users += 1
    try:
        async with entry.lock:
            yield
    finally:
        entry.users -= 1
        if not entry.users and order_locks.get(order_id) is entry:
            del order_locks[order_id]


def create_local_order(plan: dict[str, Any], quantity: int, downstream_order_no: str | None, callback_url: str) -> int:
    """Insert the pending order row (status 'creating'); purchasing happens later."""
    raw = json_load(plan["raw_json"], {})
    order_no = f"STL{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:8].upper()}"
    return store.insert_order(
        order_no=order_no,
        downstream_order_no=downstream_order_no,
        callback_url=callback_url,
        plan_id=int(plan["id"]),
        quantity=quantity,
        days=catalogue.plan_duration_days(raw) if catalogue.is_daily_configurable(raw) else None,
        # Without a downstream number the generated order number keeps the key unique.
        idempotency_key=f"dujiao-{downstream_order_no or order_no}",
        currency=str(plan["currency"]),
        unit_cost=catalogue.product_price(raw),
        plan_snapshot_json=str(plan["raw_json"]),
        created_at=iso(),
    )


async def refresh_order(local_id: int, force: bool = False) -> dict[str, Any] | None:
    async with order_lock(local_id):
        return await _refresh_order(local_id, force)


async def validate_purchase(
    plan: dict[str, Any], quantity: int, expected_cost: str, reserved_cost: Decimal, expected_days: int | None = None
) -> dict[str, Any]:
    response = await stellar.get_plan(plan["stellar_plan_id"])
    raw = response.get("data")
    if not isinstance(raw, dict) or str(raw.get("id")) != plan["stellar_plan_id"]:
        raise StellarError(503, "套餐校验响应不完整，暂缓采购")
    if not raw.get("available") or not catalogue.is_mainland_china_plan(raw):
        raise StellarError(404, "套餐已停售或目的地发生变化")
    price = Decimal(catalogue.product_price(raw))
    if not price.is_finite() or price <= 0 or str((raw.get("price") or {}).get("currency", "")).upper() != "EUR":
        raise StellarError(503, "套餐价格或币种无法校验，暂缓采购")
    if price > Decimal(expected_cost):
        raise StellarError(422, "套餐采购价已上涨，已停止采购以避免超出原报价")
    old_raw = json_load(plan["raw_json"], {})
    if (
        Decimal(str((raw.get("data") or {}).get("megabytes") or 0))
        != Decimal(str((old_raw.get("data") or {}).get("megabytes") or 0))
        or catalogue.plan_duration_days(raw) != catalogue.plan_duration_days(old_raw)
        or catalogue.plan_route_label(raw) != catalogue.plan_route_label(old_raw)
        or catalogue.is_daily_configurable(raw) != catalogue.is_daily_configurable(old_raw)
    ):
        raise StellarError(422, "套餐规格已变化，已停止采购")
    if expected_days is not None and catalogue.plan_duration_days(raw) != expected_days:
        raise StellarError(422, "套餐销售天数已变化，已停止采购")
    wallet = await wallet_balance.wallet_snapshot(force=True)
    balance = wallet.get("available_balance")
    if not isinstance(balance, Decimal) or not balance.is_finite() or wallet.get("currency") != "EUR":
        raise StellarError(503, "批发余额暂时无法读取，暂缓采购")
    if max(Decimal(0), balance - reserved_cost) < price * quantity:
        raise StellarError(402, "最新批发余额不足，已停止采购")
    return raw


async def _refresh_order(local_id: int, force: bool = False) -> dict[str, Any] | None:
    order = store.get_order(local_id)
    if not order:
        return None
    if not force and order.get("next_poll_at") and parse_iso(order["next_poll_at"]) > now():
        return order
    if order.get("cancel_requested") and order["status"] != "canceled":
        if order.get("cancel_error"):
            queues.update_order(local_id, next_poll_at=iso(now() + timedelta(seconds=300)))
        else:
            await _cancel_order(local_id)
        return store.get_order(local_id)
    if order["status"] in {"delivered", "canceled", "failed"}:
        return order

    if not order.get("stellar_order_id"):
        if order.get("cancel_requested"):
            return order
        plan = store.get_plan(order["plan_id"])
        if not plan:
            queues.update_order(local_id, status="failed", last_error="plan mapping not found")
            return store.get_order(local_id)
        try:
            async with order_create_lock:
                if not order.get("purchase_attempted"):
                    expected_cost = order.get("unit_cost") or catalogue.product_price(json_load(plan["raw_json"], {}))
                    own_reservation = Decimal(expected_cost) * order["quantity"]
                    quote_plan = dict(plan, raw_json=order.get("plan_snapshot_json") or plan["raw_json"])
                    await validate_purchase(
                        quote_plan,
                        order["quantity"],
                        expected_cost,
                        max(Decimal(0), catalogue.reserved_wholesale_cost() - own_reservation),
                        order.get("days"),
                    )
                    purchase = {
                        "plan_id": plan["stellar_plan_id"],
                        "quantity": order["quantity"],
                        "days": order.get("days"),
                    }
                    queues.update_order(local_id, purchase_attempted=1, purchase_payload_json=json.dumps(purchase))
                else:
                    # An uncertain response may already have charged the wallet.
                    # Recover only using the same frozen request and idempotency key.
                    purchase = json_load(order.get("purchase_payload_json"), None)
                    if not purchase:
                        raw = json_load(plan["raw_json"], {})
                        purchase = {
                            "plan_id": plan["stellar_plan_id"],
                            "quantity": order["quantity"],
                            "days": order.get("days") if catalogue.is_daily_configurable(raw) else None,
                        }
                        queues.update_order(local_id, purchase_payload_json=json.dumps(purchase))
                response = await stellar.create_order(
                    purchase["plan_id"], purchase["quantity"], purchase["days"], order["idempotency_key"]
                )
            data = response.get("data") or {}
            if not data.get("id"):
                raise StellarError(503, "采购响应缺少订单号，将使用原请求核对")
            queues.update_order(
                local_id,
                stellar_order_id=data.get("id"),
                status="fulfilling",
                amount=str(data.get("total") or data.get("subtotal") or "0.00"),
                currency=str(data.get("currency") or plan["currency"]),
                next_poll_at=iso(),
                last_error=None,
            )
        except StellarError as exc:
            error_code = str((exc.body or {}).get("error", {}).get("code", "")) if isinstance(exc.body, dict) else ""
            permanent = exc.status_code in {400, 402, 403, 404, 422} or (
                exc.status_code == 409 and error_code in {"idempotency_conflict", "idempotency_key_conflict"}
            )
            if permanent:
                queues.update_order(local_id, status="failed", last_error=exc.message, next_poll_at=None)
            else:
                queues.update_order(
                    local_id,
                    status="creating",
                    last_error=exc.message,
                    next_poll_at=iso(now() + timedelta(seconds=30)),
                )
            return store.get_order(local_id)
        order = store.get_order(local_id)
        if not order:
            return None

    try:
        response = await stellar.get_order(order["stellar_order_id"])
    except StellarError as exc:
        attempts = int(order.get("poll_attempts") or 0) + 1
        queues.update_order(
            local_id,
            poll_attempts=attempts,
            last_error=exc.message,
            next_poll_at=iso(now() + timedelta(seconds=30)),
        )
        return store.get_order(local_id)

    data = response.get("data") or {}
    stellar_status = str(data.get("status") or data.get("order_status") or "processing")
    new_status = presentation.status_for_dujiao(stellar_status)
    if stellar_status in {"fulfilled", "vpn_failed"}:
        for esim in data.get("esims") or []:
            if (
                esim.get("status") == "fulfilled"
                and esim.get("sim_id")
                and not (
                    esim.get("customer_link")
                    or (esim.get("installation") or {}).get("qr_code_url")
                    or (esim.get("installation") or {}).get("activation_code")
                )
            ):
                try:
                    detail = (await stellar.get_esim(str(esim["sim_id"]))).get("data") or {}
                except StellarError as exc:
                    queues.update_order(
                        local_id, last_error=exc.message, next_poll_at=iso(now() + timedelta(seconds=30))
                    )
                    return store.get_order(local_id)
                esim.update({key: detail[key] for key in ("customer_link", "installation", "vpn") if key in detail})
        fulfillment = presentation.fulfillment_from_stellar(data, order["quantity"])
        if stellar_status == "vpn_failed":
            new_status = "delivered" if fulfillment else "manual_review"
    else:
        fulfillment = None
    if new_status == "delivered" and fulfillment is None:
        new_status = "fulfilling"
    attempts = int(order.get("poll_attempts") or 0) + 1
    age = (now() - parse_iso(order["created_at"])).total_seconds()
    interval = 300 if new_status == "manual_review" else 5 if age <= 60 else 15 if age <= 300 else 30
    queues.update_order(
        local_id,
        status=new_status,
        amount=str(data.get("total") or data.get("subtotal") or order["amount"]),
        currency=str(data.get("currency") or order["currency"]),
        fulfillment_json=json.dumps(fulfillment, ensure_ascii=False) if fulfillment else None,
        poll_attempts=attempts,
        next_poll_at=None
        if new_status in {"delivered", "failed", "canceled"}
        else iso(now() + timedelta(seconds=interval)),
        last_error=None if new_status not in {"failed", "manual_review"} else f"Stellar status: {stellar_status}",
    )
    refreshed = store.get_order(local_id)
    return refreshed


def cancel_response(order: dict[str, Any] | None, status_code: int = 200, message: str | None = None) -> JSONResponse:
    if not order:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error_code": "order_not_found", "error_message": "order not found"},
        )
    body = {
        "ok": status_code == 200,
        "order_id": order["id"],
        "order_no": order["order_no"],
        "status": presentation.protocol_status(order["status"]),
        "refunded_amount": order.get("refunded_amount") or "0.00",
    }
    if message:
        body.update(
            error_code="cancellation_pending" if status_code >= 500 else "cancel_not_allowed", error_message=message
        )
    return JSONResponse(status_code=status_code, content=body)


def save_cancel_results(order_id: int, results: dict[str, Any]) -> Decimal:
    refunded = sum(
        (
            Decimal(str(result.get("refund_amount_cents") or 0)) / 100
            for result in results.values()
            if result.get("refunded")
        ),
        Decimal(0),
    )
    queues.update_order(order_id, cancel_results_json=json.dumps(results), refunded_amount=f"{refunded:.2f}")
    return refunded


async def _cancel_order(order_id: int) -> JSONResponse:
    order = store.get_order(order_id)
    if not order:
        return JSONResponse(status_code=404, content={"ok": False, "error_code": "order_not_found"})
    if order["status"] == "canceled":
        return cancel_response(order)
    queues.update_order(order_id, cancel_requested=1)
    if not order.get("stellar_order_id"):
        if order.get("purchase_attempted"):
            message = "采购请求结果尚未确认，已停止自动采购；需要核对上游订单后处理取消"
            queues.update_order(
                order_id,
                status="manual_review",
                last_error=message,
                cancel_error=message,
                next_poll_at=iso(now() + timedelta(seconds=300)),
            )
            return cancel_response(store.get_order(order_id), 409, message)
        queues.update_order(order_id, status="canceled", next_poll_at=None)
        return cancel_response(store.get_order(order_id))
    results = json_load(order.get("cancel_results_json"), {})

    def completed(result: dict[str, Any]) -> bool:
        return bool(result.get("provider_cancelled") and result.get("refunded"))

    if len([result for result in results.values() if completed(result)]) == order["quantity"]:
        queues.update_order(order_id, status="canceled", fulfillment_json=None, next_poll_at=None, cancel_error=None)
        return cancel_response(store.get_order(order_id))
    try:
        detail = await stellar.get_order(order["stellar_order_id"])
        data = detail.get("data") or {}
        if data.get("status") == "refunded":
            queues.update_order(
                order_id,
                status="canceled",
                fulfillment_json=None,
                next_poll_at=None,
                refunded_amount=str(data.get("total") or order["amount"]),
                cancel_error=None,
            )
            return cancel_response(store.get_order(order_id))
        esims = data.get("esims") or []
        if len(esims) != order["quantity"] or any(not item.get("sim_id") for item in esims):
            raise StellarError(503, "上游尚未返回完整 eSIM 列表，稍后继续取消")
        for item in esims:
            sim_id = str(item["sim_id"])
            if completed(results.get(sim_id, {})):
                continue
            if item.get("status") != "fulfilled":
                raise StellarError(503, "eSIM 尚未交付，稍后继续取消")
            failure = None
            try:
                response = await stellar.cancel_esim(sim_id)
            except StellarError as exc:
                failure = exc
                response = exc.body if isinstance(exc.body, dict) else {}
            result = response.get("data") or {}
            if result:
                if result.get("currency") != order["currency"]:
                    raise StellarError(503, "取消退款币种无法确认，需要核对")
                if result.get("refunded"):
                    try:
                        cents = Decimal(str(result.get("refund_amount_cents")))
                        valid_cents = cents.is_finite() and cents >= 0 and cents == cents.to_integral_value()
                    except InvalidOperation:
                        valid_cents = False
                    if not valid_cents:
                        raise StellarError(503, "退款金额尚未确认，需要继续核对")
                results[sim_id] = {
                    key: result.get(key)
                    for key in ("provider_cancelled", "refunded", "refund_amount_cents", "currency")
                }
                save_cancel_results(order_id, results)
            if not completed(results.get(sim_id, {})):
                if failure:
                    raise failure
                raise StellarError(503, "上游尚未确认取消及退款，将继续核对")
        queues.update_order(order_id, status="canceled", fulfillment_json=None, next_poll_at=None, cancel_error=None)
        return cancel_response(store.get_order(order_id))
    except StellarError as exc:
        permanent = exc.status_code in {400, 403, 404, 409, 422}
        message = exc.message
        queues.update_order(
            order_id,
            status="manual_review",
            last_error=message,
            cancel_error=message if permanent else None,
            next_poll_at=iso(now() + timedelta(seconds=300 if permanent else 60)),
        )
        return cancel_response(store.get_order(order_id), 409 if permanent else 503, message)
