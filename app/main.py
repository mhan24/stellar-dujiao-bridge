from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlsplit

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .security import dujiao_signature, verify_dujiao_signature, verify_stellar_signature


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("stellar-dujiao-bridge")


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


STELLAR_BASE_URL = env("STELLAR_BASE_URL", "https://wholesale.stellarsecurity.com/api/v1").rstrip("/") + "/"
STELLAR_API_KEY = env("STELLAR_API_KEY")
STELLAR_WEBHOOK_SECRET = env("STELLAR_WEBHOOK_SECRET")
DUJIAO_API_KEY = env("DUJIAO_API_KEY")
DUJIAO_API_SECRET = env("DUJIAO_API_SECRET")
DB_PATH = env("DB_PATH", "/data/bridge.db")
SITE_NAME = env("SITE_NAME", "Stellar Wholesale eSIM")
SYNC_INTERVAL_SECONDS = int(env("SYNC_INTERVAL_SECONDS", "900"))
HTTP_TIMEOUT_SECONDS = float(env("HTTP_TIMEOUT_SECONDS", "30"))
STELLAR_PAGINATION_DELAY_SECONDS = float(env("STELLAR_PAGINATION_DELAY_SECONDS", "1.1"))
TOP_PLAN_LIMIT = int(env("TOP_PLAN_LIMIT", "5"))
WALLET_CACHE_SECONDS = float(env("WALLET_CACHE_SECONDS", "30"))
PRODUCT_BUNDLE_ID = 900000001


def now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or now()).isoformat()


def parse_iso(value: str | None) -> datetime:
    if not value:
        return now()
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def db_connect() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with db_connect() as db:
        db.executescript(
            """
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
            """
        )


class StellarError(Exception):
    def __init__(self, status_code: int, message: str, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.body = body


class StellarClient:
    def __init__(self) -> None:
        self.client = httpx.AsyncClient(
            base_url=STELLAR_BASE_URL,
            timeout=HTTP_TIMEOUT_SECONDS,
            headers={"Authorization": f"Bearer {STELLAR_API_KEY}", "Accept": "application/json"},
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Any = None,
        payload: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        headers: dict[str, str] = {}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        response = None
        for attempt in range(4):
            try:
                response = await self.client.request(
                    method, path.lstrip("/"), params=params, json=payload, headers=headers
                )
            except httpx.HTTPError as exc:
                raise StellarError(503, f"Stellar request failed: {exc}") from exc
            if response.status_code != 429 or attempt == 3:
                break
            retry_after = response.headers.get("retry-after")
            try:
                delay = float(retry_after) if retry_after else 60.0
            except ValueError:
                delay = 60.0
            delay = min(max(delay, 1.0), 300.0)
            log.warning("Stellar rate limit reached; retrying in %.1fs", delay)
            await asyncio.sleep(delay)
        assert response is not None
        try:
            body = response.json() if response.content else {}
        except ValueError:
            body = {"raw": response.text}
        if response.status_code < 200 or response.status_code >= 300:
            error = body.get("error", {}) if isinstance(body, dict) else {}
            message = error.get("message") or response.text or "Stellar request failed"
            raise StellarError(response.status_code, message, body)
        return body

    async def wallet(self) -> dict[str, Any]:
        return await self.request("GET", "/wallet")

    async def list_plans(self, page: int = 1, per_page: int = 100) -> dict[str, Any]:
        return await self.request("GET", "/plans", params={"page": page, "per_page": per_page})

    async def create_order(
        self, plan_id: str, quantity: int, days: int | None, idempotency_key: str
    ) -> dict[str, Any]:
        item: dict[str, Any] = {"plan_id": plan_id, "quantity": quantity}
        if days is not None:
            item["days"] = days
        return await self.request(
            "POST",
            "/orders",
            payload={"plans": [item]},
            idempotency_key=idempotency_key,
        )

    async def get_order(self, order_id: str) -> dict[str, Any]:
        return await self.request("GET", f"/orders/{order_id}")

    async def cancel_esim(self, sim_id: str) -> dict[str, Any]:
        return await self.request("POST", f"/esims/{sim_id}/cancel")


stellar = StellarClient()
worker_task: asyncio.Task[None] | None = None
wallet_lock = asyncio.Lock()
order_create_lock = asyncio.Lock()
wallet_cache: dict[str, Any] = {
    "available_balance": None,
    "currency": "",
    "checked_at": 0.0,
    "checked_at_iso": None,
    "error": None,
}


async def wallet_snapshot(force: bool = False) -> dict[str, Any]:
    now_monotonic = time.monotonic()
    if (
        not force
        and wallet_cache["checked_at"]
        and now_monotonic - float(wallet_cache["checked_at"]) < WALLET_CACHE_SECONDS
    ):
        return dict(wallet_cache)
    async with wallet_lock:
        now_monotonic = time.monotonic()
        if (
            not force
            and wallet_cache["checked_at"]
            and now_monotonic - float(wallet_cache["checked_at"]) < WALLET_CACHE_SECONDS
        ):
            return dict(wallet_cache)
        try:
            response = await stellar.wallet()
            data = response.get("data") or {}
            raw_balance = data.get("available_balance")
            if raw_balance is None:
                raw_balance = data.get("balance")
            balance = Decimal(str(raw_balance or "0"))
            currency = str(data.get("currency") or response.get("currency") or "").upper()
            checked_at = iso()
            wallet_cache.update(
                {
                    "available_balance": balance,
                    "currency": currency,
                    "checked_at": time.monotonic(),
                    "checked_at_iso": checked_at,
                    "error": None,
                }
            )
        except (StellarError, InvalidOperation, TypeError, ValueError) as exc:
            log.warning("Stellar wallet balance unavailable: %s", exc)
            wallet_cache.update(
                {
                    "available_balance": None,
                    "currency": "",
                    "checked_at": time.monotonic(),
                    "checked_at_iso": iso(),
                    "error": str(exc),
                }
            )
        return dict(wallet_cache)


def json_load(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row else None


def get_plan(local_id: int) -> dict[str, Any] | None:
    with db_connect() as db:
        return row_dict(db.execute("SELECT * FROM plans WHERE id = ?", (local_id,)).fetchone())


def get_order(local_id: int) -> dict[str, Any] | None:
    with db_connect() as db:
        return row_dict(db.execute("SELECT * FROM orders WHERE id = ?", (local_id,)).fetchone())


def find_order_by_stellar_id(stellar_order_id: str) -> dict[str, Any] | None:
    with db_connect() as db:
        return row_dict(
            db.execute("SELECT * FROM orders WHERE stellar_order_id = ?", (stellar_order_id,)).fetchone()
        )


def find_order_by_downstream(downstream_order_no: str) -> dict[str, Any] | None:
    if not downstream_order_no:
        return None
    with db_connect() as db:
        return row_dict(
            db.execute(
                "SELECT * FROM orders WHERE downstream_order_no = ?", (downstream_order_no,)
            ).fetchone()
        )


def update_order(local_id: int, **fields: Any) -> None:
    fields["updated_at"] = iso()
    columns = ", ".join(f"{key} = ?" for key in fields)
    values = list(fields.values()) + [local_id]
    with db_connect() as db:
        db.execute(f"UPDATE orders SET {columns} WHERE id = ?", values)


def is_daily_configurable(raw: dict[str, Any]) -> bool:
    data = raw.get("data") or {}
    duration = raw.get("duration") or {}
    return data.get("type") == "daily" and bool(duration.get("configurable"))


def plan_duration_days(raw: dict[str, Any]) -> int | None:
    duration = raw.get("duration") or {}
    if is_daily_configurable(raw):
        return int(duration.get("default_days") or 1)
    return int(raw.get("validity_days") or duration.get("default_days") or 1)


def product_price(raw: dict[str, Any]) -> str:
    price = raw.get("price") or {}
    amount = Decimal(str(price.get("amount") or "0"))
    if price.get("billing_unit") == "day":
        amount *= Decimal(plan_duration_days(raw) or 1)
    return f"{amount:.2f}"


def reserved_wholesale_cost() -> Decimal:
    with db_connect() as db:
        rows = db.execute(
            """
            SELECT p.raw_json, o.quantity
            FROM orders o
            JOIN plans p ON p.id = o.plan_id
            WHERE o.status NOT IN ('delivered', 'failed', 'canceled')
            """
        ).fetchall()
    total = Decimal("0")
    for row in rows:
        raw = json_load(row["raw_json"], {})
        total += Decimal(product_price(raw)) * Decimal(str(max(int(row["quantity"] or 0), 0)))
    return total


def wallet_capacity_for_plan(
    plan: dict[str, Any], wallet: dict[str, Any], reserved_cost: Decimal
) -> int:
    balance = wallet.get("available_balance")
    if not isinstance(balance, Decimal):
        return 0
    wallet_currency = str(wallet.get("currency") or "").upper()
    plan_currency = str(plan.get("currency") or "").upper()
    if not wallet_currency or wallet_currency != plan_currency:
        return 0
    unit_cost = Decimal(product_price(json_load(plan.get("raw_json"), {})))
    if unit_cost <= 0:
        return 0
    remaining = max(balance - reserved_cost, Decimal("0"))
    return max(int(remaining // unit_cost), 0)


def stock_status_for_quantity(quantity: int) -> str:
    if quantity <= 0:
        return "out_of_stock"
    if quantity <= 5:
        return "low_stock"
    return "in_stock"


def product_description(raw: dict[str, Any]) -> str:
    data = raw.get("data") or {}
    duration = raw.get("duration") or {}
    speed = raw.get("speed") or ""
    bits = [
        f"目的地：{raw.get('destination') or ''}",
        f"流量：{data.get('label') or data.get('megabytes') or ''}",
        f"有效期：{plan_duration_days(raw) or ''} 天",
    ]
    if speed:
        bits.append(f"网络：{speed}")
    if duration.get("configurable"):
        bits.append("按默认时长销售")
    return "；".join(bit for bit in bits if bit)


def is_mainland_china_plan(raw: dict[str, Any]) -> bool:
    """Keep only plans whose coverage is exclusively mainland China."""
    coverage = raw.get("coverage") or {}
    codes = {str(code).upper() for code in (coverage.get("codes") or []) if code}
    if codes:
        return codes == {"CN"}
    return str(raw.get("country_code") or "").upper() == "CN"


def plan_price_per_gb(raw: dict[str, Any]) -> Decimal:
    """Return the plan price per GB; non-metered plans sort last."""
    data = raw.get("data") or {}
    try:
        megabytes = Decimal(str(data.get("megabytes") or "0"))
        if megabytes <= 0:
            return Decimal("Infinity")
        return Decimal(product_price(raw)) / (megabytes / Decimal(1024))
    except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
        return Decimal("Infinity")


def best_mainland_plan_ids(plans: list[dict[str, Any]]) -> set[str]:
    """Keep the cheapest mainland-China plans by price per GB."""
    candidates = [
        plan
        for plan in plans
        if plan.get("available", False) and is_mainland_china_plan(plan)
    ]
    # Python's sort is stable, so equal price/GB plans retain Stellar's order.
    candidates.sort(key=plan_price_per_gb)
    return {
        str(plan.get("id"))
        for plan in candidates[: max(0, TOP_PLAN_LIMIT)]
        if plan.get("id")
    }


def upsert_plans(plans: list[dict[str, Any]]) -> None:
    seen_ids = {str(plan.get("id")) for plan in plans if plan.get("id")}
    best_ids = best_mainland_plan_ids(plans)
    with db_connect() as db:
        for raw in plans:
            stellar_id = str(raw.get("id") or "")
            if not stellar_id:
                continue
            data = raw.get("data") or {}
            duration = raw.get("duration") or {}
            price = raw.get("price") or {}
            db.execute(
                """
                INSERT INTO plans (
                    stellar_plan_id, sku_code, name, slug, destination, country_code,
                    data_json, duration_json, coverage_json, activation_json, topup_json,
                    fair_use_json, speed, validity_days, price_amount, price_billing_unit,
                    currency, available, rechargeable, updated_at, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(stellar_plan_id) DO UPDATE SET
                    sku_code=excluded.sku_code, name=excluded.name, slug=excluded.slug,
                    destination=excluded.destination, country_code=excluded.country_code,
                    data_json=excluded.data_json, duration_json=excluded.duration_json,
                    coverage_json=excluded.coverage_json, activation_json=excluded.activation_json,
                    topup_json=excluded.topup_json, fair_use_json=excluded.fair_use_json,
                    speed=excluded.speed, validity_days=excluded.validity_days,
                    price_amount=excluded.price_amount, price_billing_unit=excluded.price_billing_unit,
                    currency=excluded.currency, available=excluded.available,
                    rechargeable=excluded.rechargeable, updated_at=excluded.updated_at,
                    raw_json=excluded.raw_json
                """,
                (
                    stellar_id,
                    str(raw.get("sku") or f"STELLAR-{stellar_id[:8]}"),
                    str(raw.get("name") or stellar_id),
                    str(raw.get("slug") or stellar_id),
                    str(raw.get("destination") or ""),
                    str(raw.get("country_code") or ""),
                    json.dumps(data, ensure_ascii=False),
                    json.dumps(duration, ensure_ascii=False),
                    json.dumps(raw.get("coverage") or {}, ensure_ascii=False),
                    json.dumps(raw.get("activation") or {}, ensure_ascii=False),
                    json.dumps(raw.get("topup") or {}, ensure_ascii=False),
                    json.dumps(raw.get("fair_use"), ensure_ascii=False)
                    if raw.get("fair_use") is not None
                    else None,
                    str(raw.get("speed") or ""),
                    int(raw.get("validity_days") or duration.get("default_days") or 0),
                    str(price.get("amount") or "0"),
                    str(price.get("billing_unit") or ""),
                    str((price.get("currency") or "EUR")).upper(),
                    1
                    if raw.get("available", False)
                    and str(raw.get("id")) in best_ids
                    else 0,
                    1 if raw.get("rechargeable", False) else 0,
                    str(raw.get("updated_at") or iso()),
                    json.dumps(raw, ensure_ascii=False),
                ),
            )
        if seen_ids:
            placeholders = ",".join("?" for _ in seen_ids)
            db.execute(
                f"UPDATE plans SET available = 0 WHERE stellar_plan_id NOT IN ({placeholders})",
                tuple(seen_ids),
            )
        db.commit()


async def sync_catalogue() -> int:
    all_plans: list[dict[str, Any]] = []
    page = 1
    while True:
        if page > 1 and STELLAR_PAGINATION_DELAY_SECONDS > 0:
            await asyncio.sleep(STELLAR_PAGINATION_DELAY_SECONDS)
        response = await stellar.list_plans(page=page, per_page=100)
        batch = response.get("data") or []
        all_plans.extend(batch)
        meta = response.get("meta") or {}
        if page >= int(meta.get("last_page") or page) or not batch:
            break
        page += 1
    upsert_plans(all_plans)
    best_ids = best_mainland_plan_ids(all_plans)
    log.info(
        "catalogue sync complete: %s plans; best mainland China plans: %s",
        len(all_plans),
        len(best_ids),
    )
    return len(all_plans)


def display_data_label(raw: dict[str, Any]) -> str:
    data = raw.get("data") or {}
    label = str(data.get("label") or "").strip()
    if label:
        label = re.sub(r"\s+", "", label)
        label = re.sub(r"(?i)gb$", "GB", label)
        return label
    megabytes = data.get("megabytes")
    if megabytes:
        try:
            gigabytes = Decimal(str(megabytes)) / Decimal("1024")
            return f"{gigabytes.normalize()}GB"
        except (InvalidOperation, ValueError):
            return str(megabytes)
    return "流量"


def product_from_plan(plan: dict[str, Any], stock_quantity: int | None = None) -> dict[str, Any]:
    raw = json_load(plan["raw_json"], {})
    duration_days = plan_duration_days(raw)
    data_label = display_data_label(raw)
    title = {
        "zh-CN": f"{plan['destination']} eSIM {raw.get('data', {}).get('label', '')}".strip(),
        "en": plan["name"],
    }
    description = product_description(raw)
    data = raw.get("data") or {}
    spec_values = {
        "zh-CN": f"中国大陆 {data_label} {duration_days}天",
        "zh-TW": f"中國大陸 {data_label} {duration_days}天",
        "en-US": f"China Mainland {data_label} {duration_days} days",
    }
    # Dujiao renders product content as localized rich-text strings.  Keeping
    # structured objects here makes the public product detail page call
    # `.replace()` on an object and fail with `t.replace is not a function`.
    content = {
        "zh-CN": (
            f"<p>{description}</p>"
            "<p>共 5 个 SKU，按价格/GB 从低到高排序，请选择对应流量和有效期。</p>"
        ),
        "zh-TW": (
            f"<p>{description}</p>"
            "<p>共 5 個 SKU，按價格/GB 從低到高排序，請選擇對應流量和有效期。</p>"
        ),
        "en-US": (
            f"<p>{description}</p>"
            "<p>Five SKUs are sorted from lowest to highest price per GB. Select the data and validity you need.</p>"
        ),
    }
    if stock_quantity is None:
        stock_quantity = -1 if plan["available"] else 0
    stock_quantity = max(int(stock_quantity), 0) if stock_quantity >= 0 else -1
    sku = {
        "id": plan["id"],
        "sku_code": plan["sku_code"],
        "spec_values": spec_values,
        "price_amount": product_price(raw),
        "original_price": product_price(raw),
        "member_price": product_price(raw),
        "stock_status": "unlimited" if stock_quantity < 0 else stock_status_for_quantity(stock_quantity),
        "stock_quantity": stock_quantity,
        "is_active": bool(plan["available"]),
    }
    return {
        "id": plan["id"],
        "slug": f"stellar-esim-{plan['id']}",
        "title": title,
        "description": {"zh-CN": description, "en": description},
        "content": content,
        "seo_meta": {},
        "images": [],
        "tags": ["esim", plan["destination"]],
        "price_amount": product_price(raw),
        "original_price": product_price(raw),
        "member_price": product_price(raw),
        "currency": plan["currency"],
        "fulfillment_type": "auto",
        "manual_form_schema": None,
        "is_active": bool(plan["available"]),
        "category_id": 1,
        "skus": [sku],
        "created_at": plan["updated_at"],
        "updated_at": plan["updated_at"],
    }


def available_plan_rows(limit: int | None = None) -> list[dict[str, Any]]:
    with db_connect() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM plans WHERE available = 1").fetchall()]
    rows.sort(key=lambda plan: plan_price_per_gb(json_load(plan["raw_json"], {})))
    return rows[:limit] if limit is not None else rows


def product_bundle(
    plans: list[dict[str, Any]],
    wallet: dict[str, Any] | None = None,
    reserved_cost: Decimal | None = None,
) -> dict[str, Any]:
    if not plans:
        raise ValueError("at least one plan is required")
    if wallet is not None:
        reserved_cost = reserved_cost if reserved_cost is not None else Decimal("0")
        stock_quantities = {
            int(plan["id"]): wallet_capacity_for_plan(plan, wallet, reserved_cost)
            for plan in plans
        }
    else:
        stock_quantities = {int(plan["id"]): -1 for plan in plans}
    skus = [product_from_plan(plan, stock_quantities[int(plan["id"])])["skus"][0] for plan in plans]
    prices = [Decimal(str(sku["price_amount"])) for sku in skus]
    updated_at = max(str(plan["updated_at"] or "") for plan in plans)
    return {
        "id": PRODUCT_BUNDLE_ID,
        "slug": "stellar-china-mainland-esim-best-value",
        "title": {
            "zh-CN": "中国大陆 eSIM 优选套餐",
            "en": "China Mainland eSIM - Best Value",
        },
        "description": {
            "zh-CN": "按价格/GB筛选的中国大陆 eSIM 套餐，请在 SKU 中选择流量和有效期。",
            "en": "China mainland eSIM plans ranked by price per GB. Select a data and validity SKU.",
        },
        "content": {
            "zh-CN": "<p>5 个 SKU，按价格/GB 从低到高排序，请选择对应流量和有效期。</p>",
            "zh-TW": "<p>5 個 SKU，按價格/GB 從低到高排序，請選擇對應流量和有效期。</p>",
            "en-US": "<p>Five SKUs sorted from lowest to highest price per GB. Select the data and validity you need.</p>",
        },
        "seo_meta": {},
        "images": [],
        "tags": ["esim", "China", "mainland"],
        "price_amount": f"{min(prices):.2f}",
        "original_price": f"{min(prices):.2f}",
        "member_price": f"{min(prices):.2f}",
        "currency": plans[0]["currency"],
        "fulfillment_type": "auto",
        "manual_form_schema": None,
        "is_active": True,
        "category_id": 1,
        "skus": skus,
        "created_at": min(str(plan["updated_at"] or "") for plan in plans),
        "updated_at": updated_at,
    }


async def require_dujiao(request: Request) -> None:
    if not DUJIAO_API_KEY or not DUJIAO_API_SECRET:
        raise HTTPException(503, "Dujiao credentials are not configured")
    body = await request.body()
    valid = verify_dujiao_signature(
        DUJIAO_API_SECRET,
        request.method,
        request.url.path,
        request.headers.get("Dujiao-Next-Timestamp", ""),
        request.headers.get("Dujiao-Next-Signature", ""),
        body,
    )
    if not valid or request.headers.get("Dujiao-Next-Api-Key") != DUJIAO_API_KEY:
        raise HTTPException(401, "invalid Dujiao credentials")


def status_for_dujiao(stellar_status: str) -> str:
    return {
        "processing": "fulfilling",
        "vpn_processing": "fulfilling",
        "fulfilled": "delivered",
        "failed": "failed",
        "manual_review": "manual_review",
        "refunded": "canceled",
    }.get(stellar_status, stellar_status or "fulfilling")


def fulfillment_from_stellar(order_data: dict[str, Any]) -> dict[str, Any] | None:
    esims = order_data.get("esims") or []
    if not esims:
        return None
    delivered = [item for item in esims if item.get("status") == "fulfilled"]
    if not delivered:
        return None
    delivery_items: list[dict[str, Any]] = []
    payload_lines: list[str] = []
    for esim in delivered:
        installation = esim.get("installation") or {}
        customer_link = esim.get("customer_link") or installation.get("qr_code_url")
        if customer_link:
            payload_lines.append(str(customer_link))
        delivery_items.append(
            {
                "sim_id": esim.get("sim_id"),
                "customer_link": customer_link,
                "qr_code_url": installation.get("qr_code_url"),
                "activation_code": installation.get("activation_code"),
                "apn": installation.get("apn"),
                "vpn": esim.get("vpn"),
            }
        )
    return {
        "type": "auto",
        "status": "delivered",
        "payload": "\n".join(payload_lines),
        "delivery_data": {"esims": delivery_items},
        "delivered_at": order_data.get("updated_at") or iso(),
    }


async def send_callback(order: dict[str, Any], callback_status: str, fulfillment: dict[str, Any] | None) -> None:
    callback_url = order.get("callback_url")
    if not callback_url:
        return
    payload: dict[str, Any] = {
        "event": "order.fulfilled" if callback_status == "delivered" else "order.status",
        "order_id": order["id"],
        "order_no": order["order_no"],
        "downstream_order_no": order.get("downstream_order_no") or order["order_no"],
        "status": callback_status,
        "timestamp": int(time.time()),
    }
    if fulfillment:
        payload["fulfillment"] = fulfillment
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    parsed = urlsplit(callback_url)
    timestamp = int(time.time())
    signature = dujiao_signature(
        DUJIAO_API_SECRET, "POST", parsed.path or "/", timestamp, body
    )
    headers = {
        "Content-Type": "application/json",
        "Dujiao-Next-Api-Key": DUJIAO_API_KEY,
        "Dujiao-Next-Timestamp": str(timestamp),
        "Dujiao-Next-Signature": signature,
    }
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_SECONDS) as client:
            response = await client.post(callback_url, content=body, headers=headers)
            if response.status_code >= 300:
                log.warning("Dujiao callback failed: %s %s", response.status_code, response.text[:500])
    except httpx.HTTPError as exc:
        log.warning("Dujiao callback error: %s", exc)


async def refresh_order(local_id: int, force: bool = False) -> dict[str, Any] | None:
    order = get_order(local_id)
    if not order:
        return None
    if order["status"] in {"delivered", "canceled", "failed"} and not force:
        return order
    if not force and order.get("next_poll_at") and parse_iso(order["next_poll_at"]) > now():
        return order

    if not order.get("stellar_order_id"):
        plan = get_plan(order["plan_id"])
        if not plan:
            update_order(local_id, status="failed", last_error="plan mapping not found")
            return get_order(local_id)
        raw = json_load(plan["raw_json"], {})
        days = plan_duration_days(raw) if is_daily_configurable(raw) else None
        try:
            response = await stellar.create_order(
                plan["stellar_plan_id"], order["quantity"], days, order["idempotency_key"]
            )
            data = response.get("data") or {}
            update_order(
                local_id,
                stellar_order_id=data.get("id"),
                status="fulfilling",
                amount=str(data.get("total") or data.get("subtotal") or "0.00"),
                currency=str(data.get("currency") or plan["currency"]),
                next_poll_at=iso(),
                last_error=None,
            )
        except StellarError as exc:
            if exc.status_code in {400, 402, 403, 404, 409, 422}:
                update_order(local_id, status="failed", last_error=exc.message, next_poll_at=None)
            else:
                update_order(
                    local_id,
                    status="creating",
                    last_error=exc.message,
                    next_poll_at=iso(now() + timedelta(seconds=30)),
                )
            return get_order(local_id)
        order = get_order(local_id)
        if not order:
            return None

    try:
        response = await stellar.get_order(order["stellar_order_id"])
    except StellarError as exc:
        attempts = int(order.get("poll_attempts") or 0) + 1
        update_order(
            local_id,
            poll_attempts=attempts,
            last_error=exc.message,
            next_poll_at=iso(now() + timedelta(seconds=30)),
        )
        return get_order(local_id)

    data = response.get("data") or {}
    old_status = order["status"]
    stellar_status = str(data.get("status") or data.get("order_status") or "processing")
    new_status = status_for_dujiao(stellar_status)
    fulfillment = fulfillment_from_stellar(data) if new_status == "delivered" else None
    if new_status == "delivered" and fulfillment is None:
        new_status = "fulfilling"
    attempts = int(order.get("poll_attempts") or 0) + 1
    age = (now() - parse_iso(order["created_at"])).total_seconds()
    interval = 5 if age <= 60 else 15 if age <= 300 else 30
    update_order(
        local_id,
        status=new_status,
        amount=str(data.get("total") or data.get("subtotal") or order["amount"]),
        currency=str(data.get("currency") or order["currency"]),
        fulfillment_json=json.dumps(fulfillment, ensure_ascii=False) if fulfillment else None,
        poll_attempts=attempts,
        next_poll_at=None if new_status in {"delivered", "failed", "canceled"} else iso(now() + timedelta(seconds=interval)),
        last_error=None if new_status not in {"failed", "manual_review"} else json.dumps(data, ensure_ascii=False),
    )
    refreshed = get_order(local_id)
    if refreshed and refreshed["status"] != old_status:
        await send_callback(refreshed, refreshed["status"], fulfillment)
    return refreshed


def order_response(order: dict[str, Any], include_items: bool = True) -> dict[str, Any]:
    plan = get_plan(order["plan_id"])
    product = product_bundle([plan], wallet_cache, reserved_wholesale_cost()) if plan else None
    response: dict[str, Any] = {
        "ok": True,
        "order_id": order["id"],
        "order_no": order["order_no"],
        "status": order["status"],
        "amount": order["amount"],
        "refunded_amount": "0.00",
        "currency": order["currency"],
    }
    if include_items and product:
        response["items"] = [
            {
                "product_id": product["id"],
                "sku_id": product["skus"][0]["id"],
                "title": product["title"],
                "quantity": order["quantity"],
                "unit_price": product["price_amount"],
                "total_price": order["amount"],
                "fulfillment_type": "auto",
            }
        ]
    fulfillment = json_load(order.get("fulfillment_json"), None)
    if fulfillment:
        response["fulfillment"] = fulfillment
    return response


async def worker_loop() -> None:
    # startup() performs the initial sync; do not immediately start a second
    # 61-page request burst and trip Stellar's account rate limit.
    last_sync = time.time()
    while True:
        try:
            if time.time() - last_sync >= SYNC_INTERVAL_SECONDS:
                last_sync = time.time()
                try:
                    await sync_catalogue()
                    await wallet_snapshot(force=True)
                except Exception:
                    log.exception("periodic catalogue sync failed")
            with db_connect() as db:
                rows = db.execute(
                    "SELECT id FROM orders WHERE status NOT IN ('delivered','failed','canceled') "
                    "AND (next_poll_at IS NULL OR next_poll_at <= ?)",
                    (iso(),),
                ).fetchall()
            for row in rows[:20]:
                try:
                    await refresh_order(int(row["id"]))
                except Exception:
                    log.exception("order refresh failed: %s", row["id"])
            await asyncio.sleep(2)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("worker loop error")
            await asyncio.sleep(5)


app = FastAPI(title="Stellar Wholesale eSIM Bridge", version="1.0.0")


@app.on_event("startup")
async def startup() -> None:
    global worker_task
    init_db()
    if not STELLAR_API_KEY:
        log.warning("STELLAR_API_KEY is not configured")
    if not STELLAR_WEBHOOK_SECRET:
        log.warning("STELLAR_WEBHOOK_SECRET is not configured; webhook verification will fail")
    try:
        await sync_catalogue()
        await wallet_snapshot(force=True)
    except Exception:
        log.exception("initial catalogue sync failed; worker will retry")
    worker_task = asyncio.create_task(worker_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    global worker_task
    if worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
    await stellar.close()


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "service": "stellar-dujiao-bridge", "time": iso()}


@app.post("/api/v1/upstream/ping", dependencies=[Depends(require_dujiao)])
async def ping() -> dict[str, Any]:
    wallet = await wallet_snapshot(force=True)
    balance = wallet.get("available_balance")
    return {
        "ok": True,
        "site_name": SITE_NAME,
        "protocol_version": "1.0",
        "user_id": 1,
        "balance": f"{balance:.2f}" if isinstance(balance, Decimal) else "0.00",
        "currency": str(wallet.get("currency") or "EUR"),
        "member_level": None,
        "balance_checked_at": wallet.get("checked_at_iso"),
        "balance_error": wallet.get("error"),
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
async def products(page: int = 1, page_size: int = 20) -> dict[str, Any]:
    page = max(1, page)
    page_size = min(100, max(1, page_size))
    plans = available_plan_rows(limit=TOP_PLAN_LIMIT)
    wallet = await wallet_snapshot()
    reserved_cost = reserved_wholesale_cost()
    bundle = product_bundle(plans, wallet, reserved_cost) if plans and page == 1 else None
    return {
        "ok": True,
        "items": [bundle] if bundle else [],
        "total": 1 if plans else 0,
        "page": page,
        "page_size": page_size,
    }


@app.get("/api/v1/upstream/products/{product_id}", dependencies=[Depends(require_dujiao)])
async def product(product_id: int) -> dict[str, Any]:
    if product_id == PRODUCT_BUNDLE_ID:
        plans = available_plan_rows(limit=TOP_PLAN_LIMIT)
        if plans:
            wallet = await wallet_snapshot()
            return {
                "ok": True,
                "product": product_bundle(plans, wallet, reserved_wholesale_cost()),
            }
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error_code": "product_not_found", "error_message": "product not found"},
        )
    plan = get_plan(product_id)
    if not plan or not plan["available"]:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error_code": "product_not_found", "error_message": "product not found"},
        )
    wallet = await wallet_snapshot()
    stock_quantity = wallet_capacity_for_plan(plan, wallet, reserved_wholesale_cost())
    return {"ok": True, "product": product_from_plan(plan, stock_quantity)}


@app.post("/api/v1/upstream/orders", dependencies=[Depends(require_dujiao)])
async def create_order(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(400, "invalid request body") from exc
    local_sku_id = int(payload.get("sku_id") or 0)
    quantity = int(payload.get("quantity") or 0)
    if local_sku_id <= 0 or quantity <= 0:
        raise HTTPException(400, "sku_id and quantity are required")
    downstream = str(payload.get("downstream_order_no") or "").strip() or None
    existing = find_order_by_downstream(downstream or "")
    if existing:
        if existing["status"] == "creating":
            asyncio.create_task(refresh_order(existing["id"], force=True))
        return JSONResponse(status_code=200, content=order_response(existing, include_items=False))
    async with order_create_lock:
        plan = get_plan(local_sku_id)
        if not plan or not plan["available"]:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error_code": "sku_unavailable", "error_message": "eSIM plan unavailable"},
            )
        wallet = await wallet_snapshot(force=True)
        capacity = wallet_capacity_for_plan(plan, wallet, reserved_wholesale_cost())
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
        order_no = f"STL{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:8].upper()}"
        idem = f"dujiao-{downstream or order_no}"
        created = iso()
        with db_connect() as db:
            cursor = db.execute(
                """
                INSERT INTO orders (
                    order_no, downstream_order_no, callback_url, plan_id, quantity, days,
                    idempotency_key, status, currency, next_poll_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'creating', ?, ?, ?, ?)
                """,
                (
                    order_no,
                    downstream,
                    str(payload.get("callback_url") or ""),
                    plan["id"],
                    quantity,
                    plan_duration_days(json_load(plan["raw_json"], {}))
                    if is_daily_configurable(json_load(plan["raw_json"], {}))
                    else None,
                    idem,
                    plan["currency"],
                    created,
                    created,
                    created,
                ),
            )
            local_id = int(cursor.lastrowid)
    order = await refresh_order(local_id, force=True)
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
async def get_upstream_order(order_id: int) -> dict[str, Any]:
    order = await refresh_order(order_id)
    if not order:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error_code": "order_not_found", "error_message": "order not found"},
        )
    return order_response(order)


@app.post("/api/v1/upstream/orders/{order_id}/cancel", dependencies=[Depends(require_dujiao)])
async def cancel_upstream_order(order_id: int) -> JSONResponse:
    order = await refresh_order(order_id, force=True)
    if not order:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error_code": "order_not_found", "error_message": "order not found"},
        )
    if not order.get("stellar_order_id"):
        update_order(order_id, status="canceled", next_poll_at=None)
        return JSONResponse(status_code=200, content={"ok": True, "order_id": order_id, "order_no": order["order_no"], "status": "canceled"})
    try:
        detail = await stellar.get_order(order["stellar_order_id"])
        data = detail.get("data") or {}
        esims = data.get("esims") or []
        if not esims or any(item.get("status") != "fulfilled" for item in esims):
            return JSONResponse(
                status_code=409,
                content={"ok": False, "error_code": "cancel_not_allowed", "error_message": "eSIM is not yet cancellable"},
            )
        for item in esims:
            sim_id = item.get("sim_id")
            if not sim_id:
                return JSONResponse(
                    status_code=409,
                    content={"ok": False, "error_code": "cancel_not_allowed", "error_message": "missing sim_id"},
                )
            await stellar.cancel_esim(str(sim_id))
    except StellarError as exc:
        return JSONResponse(
            status_code=409 if exc.status_code in {409, 422} else exc.status_code,
            content={"ok": False, "error_code": "cancel_not_allowed", "error_message": exc.message},
        )
    update_order(order_id, status="canceled", fulfillment_json=None, next_poll_at=None)
    refreshed = get_order(order_id)
    await send_callback(refreshed or order, "canceled", None)
    return JSONResponse(
        status_code=200,
        content={"ok": True, "order_id": order_id, "order_no": order["order_no"], "status": "canceled"},
    )


@app.post("/webhooks/stellar")
async def stellar_webhook(request: Request) -> dict[str, Any]:
    raw_body = await request.body()
    if not STELLAR_WEBHOOK_SECRET or not verify_stellar_signature(
        STELLAR_WEBHOOK_SECRET,
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
    with db_connect() as db:
        inserted = db.execute(
            "INSERT OR IGNORE INTO webhook_events(event_id, received_at) VALUES (?, ?)",
            (event_id, iso()),
        ).rowcount
    if not inserted:
        return {"ok": True, "duplicate": True}
    event_type = str(event.get("type") or request.headers.get("Stellar-Event-Type") or "")
    if event_type == "catalogue.updated":
        asyncio.create_task(sync_catalogue())
    else:
        data = event.get("data") or {}
        stellar_order_id = str(data.get("order_id") or "")
        if stellar_order_id:
            order = find_order_by_stellar_id(stellar_order_id)
            if order:
                asyncio.create_task(refresh_order(order["id"], force=True))
    return {"ok": True}
