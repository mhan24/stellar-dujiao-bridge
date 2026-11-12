"""Catalogue selection, stock maths and Stellar catalogue synchronization."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any

from fastapi import HTTPException

from . import settings, wallet_balance
from .stellar_client import stellar
from .util import db_connect, iso, json_load, now, parse_iso

log = logging.getLogger("stellar-dujiao-bridge")


catalogue_lock = asyncio.Lock()
catalogue_last_attempt = 0.0


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
            SELECT p.raw_json, o.quantity, o.unit_cost
            FROM orders o
            JOIN plans p ON p.id = o.plan_id
            WHERE o.status NOT IN ('delivered', 'failed', 'canceled')
            AND o.stellar_order_id IS NULL
            """
        ).fetchall()
    total = Decimal("0")
    for row in rows:
        raw = json_load(row["raw_json"], {})
        total += Decimal(row["unit_cost"] or product_price(raw)) * Decimal(str(max(int(row["quantity"] or 0), 0)))
    return total


def catalogue_is_fresh() -> bool:
    with db_connect() as db:
        row = db.execute("SELECT value FROM bridge_state WHERE key='catalogue_synced_at'").fetchone()
    return bool(row) and (now() - parse_iso(row[0])).total_seconds() <= max(
        2 * settings.SYNC_INTERVAL_SECONDS + 120, 300
    )


def catalogue_active_plan_count() -> int:
    with db_connect() as db:
        return int(db.execute("SELECT count(*) FROM plans WHERE upstream_active = 1").fetchone()[0] or 0)


async def product_wallet_snapshot() -> dict[str, Any]:
    if not catalogue_is_fresh():
        return {"available_balance": None, "currency": "", "error": "catalogue_stale", "catalogue_fresh": False}
    try:
        wallet = await asyncio.wait_for(
            wallet_balance.wallet_snapshot(), timeout=settings.PRODUCT_WALLET_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        log.warning("Inventory sync deferred: wallet query timed out")
        raise HTTPException(503, "Wallet temporarily unavailable; retry inventory sync") from None
    balance = wallet.get("available_balance")
    if (
        not isinstance(balance, Decimal)
        or not balance.is_finite()
        or wallet.get("currency") != "EUR"
        or wallet.get("error")
    ):
        log.warning("Inventory sync deferred: no valid EUR wallet snapshot")
        raise HTTPException(503, "Wallet temporarily unavailable; retry inventory sync")
    return {**wallet, "catalogue_fresh": True}


def wallet_capacity_for_plan(
    plan: dict[str, Any],
    wallet: dict[str, Any],
    reserved_cost: Decimal,
    catalogue_fresh: bool | None = None,
) -> int:
    if catalogue_fresh is None:
        catalogue_fresh = catalogue_is_fresh()
    if not plan.get("available") or not catalogue_fresh:
        return 0
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


def plan_route_label(raw: dict[str, Any], language: str = "zh-CN") -> str:
    breakout = str((raw.get("coverage") or {}).get("breakout_ip_country_code") or "").upper().strip()
    if breakout and breakout not in {"HK", "SG"}:
        return {"zh-CN": "非香港", "zh-TW": "非香港", "en-US": "Non-Hong Kong"}.get(language, "非香港")
    marker = " ".join(str(raw.get(key) or "") for key in ("name", "sku", "slug")).lower()
    singapore = breakout == "SG" if breakout else "nonhkip" in marker
    labels = {"zh-CN": ("新加坡", "香港"), "zh-TW": ("新加坡", "香港"), "en-US": ("Singapore", "Hong Kong")}
    return labels.get(language, labels["zh-CN"])[0 if singapore else 1]


def is_mainland_china_plan(raw: dict[str, Any]) -> bool:
    """Keep the exact China destination with CN-only coverage."""
    if str(raw.get("destination") or "").strip().casefold() != "china":
        return False
    coverage = raw.get("coverage") or {}
    codes = {str(code).upper() for code in (coverage.get("codes") or []) if code}
    if codes:
        return codes == {"CN"}
    return False


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


def best_mainland_plan_ids(plans: list[dict[str, Any]], spendable_balance: Decimal | None = None) -> set[str]:
    """Fixed portfolio: low unit cost, bounded validity bonus, and route/time coverage."""
    candidates = [
        plan
        for plan in plans
        if plan.get("available", False)
        and plan.get("id")
        and is_mainland_china_plan(plan)
        and str((plan.get("price") or {}).get("currency") or "EUR").upper() == "EUR"
        and plan_price_per_gb(plan).is_finite()
        and Decimal(product_price(plan)) > 0
        and (spendable_balance is None or Decimal(product_price(plan)) <= spendable_balance)
    ]

    def stable_key(plan: dict[str, Any]) -> tuple:
        return (
            plan_price_per_gb(plan),
            Decimal(product_price(plan)),
            -(plan_duration_days(plan) or 0),
            str(plan.get("sku") or ""),
            str(plan["id"]),
        )

    def value_key(plan: dict[str, Any]) -> tuple:
        days = min(max(plan_duration_days(plan) or 0, 0), settings.PLAN_SELECTION_TIME_CAP_DAYS)
        bonus = 1 + settings.PLAN_SELECTION_TIME_WEIGHT * Decimal(days) / settings.PLAN_SELECTION_TIME_CAP_DAYS
        return (plan_price_per_gb(plan) / bonus, *stable_key(plan))

    def choose(pool: list[dict[str, Any]], cheap: bool = False) -> dict[str, Any]:
        # A candidate outside the 5% band cannot win by having a long validity.
        metric = (lambda p: Decimal(product_price(p))) if cheap else plan_price_per_gb
        floor = min(metric(p) for p in pool)
        return min((p for p in pool if metric(p) <= floor * (1 + settings.PLAN_SELECTION_PRICE_BAND)), key=value_key)

    candidates.sort(key=stable_key)

    def comparable_profile(plan: dict[str, Any]) -> str:
        coverage = plan.get("coverage") or {}
        networks = sorted(json.dumps(network, sort_keys=True) for network in coverage.get("networks", []))
        return json.dumps(
            {
                "route": plan_route_label(plan),
                "breakout": coverage.get("breakout_ip_country_code"),
                "networks": networks,
                "type": (plan.get("data") or {}).get("type", "fixed"),
                "speed": plan.get("speed"),
                "activation": plan.get("activation"),
                "topup": plan.get("topup"),
                "tethering": plan.get("tethering_allowed"),
                "fair_use": plan.get("fair_use"),
            },
            sort_keys=True,
        )

    profiles = {str(plan.get("id")): comparable_profile(plan) for plan in candidates}
    unique: list[dict[str, Any]] = []
    seen_specs: set[tuple[Decimal, int | None, str]] = set()
    for plan in candidates:
        if not plan.get("id"):
            continue
        spec = (Decimal(str(plan["data"]["megabytes"])), plan_duration_days(plan), profiles[str(plan["id"])])
        if spec in seen_specs:
            continue
        seen_specs.add(spec)
        unique.append(plan)

    # Prefer a strictly better allowance/validity within a small price premium.
    # Only compare equivalent network and service conditions, within the wallet budget.
    def dominated(plan: dict[str, Any]) -> bool:
        days = plan_duration_days(plan)
        if days is None:
            return False
        size = Decimal(str(plan["data"]["megabytes"]))
        price = Decimal(product_price(plan))
        for other in unique:
            if other is plan or profiles[str(other["id"])] != profiles[str(plan["id"])]:
                continue
            other_days = plan_duration_days(other)
            if other_days is None:
                continue
            other_size = Decimal(str(other["data"]["megabytes"]))
            other_price = Decimal(product_price(other))
            if (
                other_size >= size
                and other_days >= days
                and other_price <= price * (1 + max(Decimal(0), settings.PLAN_UPGRADE_PRICE_TOLERANCE))
                and (other_size > size or other_days > days)
            ):
                return True
        return False

    efficient = [plan for plan in unique if not dominated(plan)]
    if settings.TOP_PLAN_LIMIT <= 0:
        return {str(plan["id"]) for plan in efficient}

    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()

    def add(plan: dict[str, Any]) -> None:
        if len(selected) < settings.TOP_PLAN_LIMIT and str(plan["id"]) not in selected_ids:
            selected.append(plan)
            selected_ids.add(str(plan["id"]))

    def route_group(plan: dict[str, Any]) -> str:
        return "HK" if plan_route_label(plan) == "香港" else "NON_HK"

    # Reserve only four slots; the other six remain available for lowest unit cost.
    # Short anchors deliberately survive dominance by a longer near-price package.
    for long_term in (False, True):
        for group in ("HK", "NON_HK"):
            pool = [
                p
                for p in unique
                if route_group(p) == group
                and (
                    (plan_duration_days(p) or 0) >= settings.PLAN_SELECTION_LONG_DAYS
                    if long_term
                    else 0 < (plan_duration_days(p) or 0) <= settings.PLAN_SELECTION_SHORT_DAYS
                )
            ]
            if pool:
                add(choose(pool, cheap=not long_term))
    # If a route has no short/long option, still retain that route when available.
    for group in ("HK", "NON_HK"):
        if not any(route_group(p) == group for p in selected):
            pool = [p for p in efficient if route_group(p) == group]
            if pool:
                add(choose(pool))
    pool = [p for p in efficient if str(p["id"]) not in selected_ids]
    while pool and len(selected) < settings.TOP_PLAN_LIMIT:
        winner = choose(pool)
        add(winner)
        pool = [p for p in pool if str(p["id"]) not in selected_ids]
    return {str(plan["id"]) for plan in selected}


def upsert_plans(plans: list[dict[str, Any]], spendable_balance: Decimal | None = None) -> None:
    seen_ids = {str(plan.get("id")) for plan in plans if plan.get("id")}
    best_ids = best_mainland_plan_ids(plans, spendable_balance)
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
                    json.dumps(raw.get("fair_use"), ensure_ascii=False) if raw.get("fair_use") is not None else None,
                    str(raw.get("speed") or ""),
                    int(raw.get("validity_days") or duration.get("default_days") or 0),
                    str(price.get("amount") or "0"),
                    str(price.get("billing_unit") or ""),
                    str(price.get("currency") or "EUR").upper(),
                    1 if raw.get("available", False) and str(raw.get("id")) in best_ids else 0,
                    1 if raw.get("rechargeable", False) else 0,
                    str(raw.get("updated_at") or iso()),
                    json.dumps(raw, ensure_ascii=False),
                ),
            )
            db.execute(
                "UPDATE plans SET upstream_active=? WHERE stellar_plan_id=?",
                (int(bool(raw.get("available"))), stellar_id),
            )
        db.execute("UPDATE plans SET published = 1 WHERE available = 1")
        if seen_ids:
            placeholders = ",".join("?" for _ in seen_ids)
            db.execute(
                f"UPDATE plans SET available = 0, upstream_active = 0 WHERE stellar_plan_id NOT IN ({placeholders})",
                tuple(seen_ids),
            )
        else:
            db.execute("UPDATE plans SET available = 0, upstream_active = 0")
        db.commit()


async def sync_catalogue() -> int:
    """Coalesce catalogue events and keep them within the periodic sync budget."""
    global catalogue_last_attempt
    if catalogue_lock.locked():
        return 0
    async with catalogue_lock:
        if catalogue_last_attempt and time.monotonic() - catalogue_last_attempt < settings.SYNC_INTERVAL_SECONDS:
            return 0
        catalogue_last_attempt = time.monotonic()
        try:
            return await fetch_catalogue()
        except Exception:
            log.exception("catalogue sync failed; retry on next interval")
            return 0


async def fetch_catalogue() -> int:
    all_plans: list[dict[str, Any]] = []
    page = 1
    while True:
        if page > 1 and settings.STELLAR_PAGINATION_DELAY_SECONDS > 0:
            await asyncio.sleep(settings.STELLAR_PAGINATION_DELAY_SECONDS)
        response = await stellar.list_plans(page=page, per_page=100)
        batch = response.get("data")
        meta = response.get("meta")
        if not isinstance(batch, list):
            raise ValueError("Incomplete catalogue response: data is not a list")
        if not isinstance(meta, dict):
            if page > 1 or not batch:
                raise ValueError("Incomplete catalogue response: missing pagination meta")
            meta = {}
        try:
            last_page = int(meta.get("last_page") or page)
        except (TypeError, ValueError):
            raise ValueError("Invalid catalogue pagination meta") from None
        if last_page < page or last_page > settings.CATALOGUE_MAX_PAGES:
            raise ValueError(f"Invalid catalogue pagination: page={page} last_page={last_page}")
        if not batch and page < last_page:
            raise ValueError(f"Incomplete catalogue pagination: empty page {page} of {last_page}")
        all_plans.extend(batch)
        if page >= last_page:
            break
        page += 1
    # Guard: an empty or truncated successful response must not zero out the
    # published catalogue, so the previous snapshot keeps serving stock.
    if not all_plans:
        raise ValueError("Catalogue response contained no plans; keeping the previous catalogue")
    active_count = sum(1 for plan in all_plans if plan.get("available"))
    previous_active = catalogue_active_plan_count()
    if previous_active and Decimal(active_count) < Decimal(previous_active) * settings.CATALOGUE_MIN_ACTIVE_RATIO:
        raise ValueError(
            f"Catalogue response kept only {active_count} of {previous_active} previously active plans; "
            "keeping the previous catalogue"
        )
    wallet = await wallet_balance.wallet_snapshot(force=True)
    balance = wallet.get("available_balance")
    if not isinstance(balance, Decimal) or not balance.is_finite() or wallet.get("currency") != "EUR":
        raise ValueError("Cannot select affordable plans without a valid EUR wallet balance")
    spendable_balance = max(Decimal("0"), balance - reserved_wholesale_cost())
    upsert_plans(all_plans, spendable_balance)
    with db_connect() as db:
        db.execute("INSERT OR REPLACE INTO bridge_state(key,value) VALUES ('catalogue_synced_at',?)", (iso(),))
    best_ids = best_mainland_plan_ids(all_plans, spendable_balance)
    log.info(
        "catalogue sync complete (country_code=CN, model="
        + settings.PLAN_SELECTION_MODEL
        + "): %s plans; best mainland China plans: %s",
        len(all_plans),
        len(best_ids),
    )
    return len(all_plans)


async def reconcile_catalogue_event(event: dict[str, Any], received_at: str) -> None:
    data = event.get("data") or {}
    ids = data.get("plan_ids")
    async with catalogue_lock:
        if data.get("full_refresh_required") or not isinstance(ids, list) or not ids:
            with db_connect() as db:
                full = db.execute("SELECT value FROM bridge_state WHERE key='catalogue_synced_at'").fetchone()
            if not full or parse_iso(full[0]) < parse_iso(received_at):
                await fetch_catalogue()
            return
        changed = await stellar.plans_by_ids([str(plan_id) for plan_id in ids])
        with db_connect() as db:
            cached = {}
            for row in db.execute("SELECT stellar_plan_id,raw_json,upstream_active FROM plans"):
                raw = json_load(row["raw_json"], {})
                raw["available"] = bool(row["upstream_active"])
                cached[row["stellar_plan_id"]] = raw
        # Only a successful complete ID response can deactivate absent IDs.
        returned = {str(plan["id"]) for plan in changed}
        for plan_id in ids:
            if plan_id in cached and plan_id not in returned:
                cached[plan_id]["available"] = False
        cached.update({str(plan["id"]): plan for plan in changed})
        wallet = await wallet_balance.wallet_snapshot(force=True)
        balance = wallet.get("available_balance")
        if not isinstance(balance, Decimal) or not balance.is_finite() or wallet.get("currency") != "EUR":
            raise ValueError("Wallet unavailable during catalogue reconciliation")
        upsert_plans(list(cached.values()), max(Decimal(0), balance - reserved_wholesale_cost()))
        # Delta updates do not extend the freshness of the last full scan.
