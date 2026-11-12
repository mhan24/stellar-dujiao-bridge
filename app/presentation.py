"""Dujiao product/SKU rendering and fulfillment payload shaping."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from . import catalogue, settings, store
from .util import db_connect, iso, json_load


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
        f"有效期：{catalogue.plan_duration_days(raw) or ''} 天",
        f"线路标注：{catalogue.plan_route_label(raw)}",
    ]
    if speed:
        bits.append(f"网络：{speed}")
    if duration.get("configurable"):
        bits.append("按默认时长销售")
    return "；".join(bit for bit in bits if bit)


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
        except (InvalidOperation, ValueError):
            return str(megabytes)
        if not gigabytes.is_finite() or gigabytes <= 0:
            return str(megabytes)
        if gigabytes == gigabytes.to_integral_value():
            return f"{int(gigabytes)}GB"
        # ``normalize()`` alone renders large values in scientific notation.
        return f"{gigabytes.normalize():f}GB"
    return "流量"


def product_from_plan(plan: dict[str, Any], stock_quantity: int | None = None) -> dict[str, Any]:
    raw = json_load(plan["raw_json"], {})
    duration_days = catalogue.plan_duration_days(raw)
    data_label = display_data_label(raw)
    title = {
        "zh-CN": f"{plan['destination']} eSIM {raw.get('data', {}).get('label', '')}".strip(),
        "en": plan["name"],
    }
    description = product_description(raw)
    spec_values = {
        "zh-CN": f"中国大陆 {data_label} {duration_days}天（{catalogue.plan_route_label(raw)}）",
        "zh-TW": f"中國大陸 {data_label} {duration_days}天（{catalogue.plan_route_label(raw, 'zh-TW')}）",
        "en-US": f"China Mainland {data_label} {duration_days} days ({catalogue.plan_route_label(raw, 'en-US')})",
    }
    # Dujiao renders product content as localized rich-text strings.  Keeping
    # structured objects here makes the public product detail page call
    # `.replace()` on an object and fail with `t.replace is not a function`.
    content = {
        "zh-CN": (f"<p>{description}</p><p>按套餐总价从低到高排序，请选择对应流量和有效期。</p>"),
        "zh-TW": (f"<p>{description}</p><p>按套餐總價從低到高排序，請選擇對應流量和有效期。</p>"),
        "en-US": (
            f"<p>{description}</p>"
            "<p>SKUs are sorted from lowest to highest total price. Select the data and validity you need.</p>"
        ),
    }
    if stock_quantity is None:
        stock_quantity = -1 if plan["available"] else 0
    stock_quantity = max(int(stock_quantity), 0) if stock_quantity >= 0 else -1
    sku = {
        "id": plan["id"],
        "sku_code": plan["sku_code"],
        "spec_values": spec_values,
        "price_amount": catalogue.product_price(raw),
        "original_price": catalogue.product_price(raw),
        "member_price": catalogue.product_price(raw),
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
        "price_amount": catalogue.product_price(raw),
        "original_price": catalogue.product_price(raw),
        "member_price": catalogue.product_price(raw),
        "currency": plan["currency"],
        "fulfillment_type": "auto",
        "manual_form_schema": None,
        "is_active": bool(plan["available"]),
        "category_id": 1,
        "skus": [sku],
        "created_at": plan["updated_at"],
        "updated_at": plan["updated_at"],
    }


def available_plan_rows() -> list[dict[str, Any]]:
    with db_connect() as db:
        rows = [dict(row) for row in db.execute("SELECT * FROM plans WHERE available = 1 OR published = 1").fetchall()]
    rows.sort(
        key=lambda plan: (
            Decimal(catalogue.product_price(json_load(plan["raw_json"], {}))),
            0 if catalogue.plan_route_label(json_load(plan["raw_json"], {})) == "新加坡" else 1,
        )
    )
    return rows


def product_bundle(
    plans: list[dict[str, Any]],
    wallet: dict[str, Any] | None = None,
    reserved_cost: Decimal | None = None,
    catalogue_fresh: bool | None = None,
) -> dict[str, Any]:
    if not plans:
        raise ValueError("at least one plan is required")
    if wallet is not None:
        reserved_cost = reserved_cost if reserved_cost is not None else Decimal("0")
        fresh = catalogue.catalogue_is_fresh() if catalogue_fresh is None else catalogue_fresh
        stock_quantities = {
            int(plan["id"]): catalogue.wallet_capacity_for_plan(plan, wallet, reserved_cost, fresh) for plan in plans
        }
    else:
        stock_quantities = {int(plan["id"]): -1 for plan in plans}
    skus = [product_from_plan(plan, stock_quantities[int(plan["id"])])["skus"][0] for plan in plans]
    active_sku_count = sum(bool(sku["is_active"]) for sku in skus)
    prices = [Decimal(str(sku["price_amount"])) for sku in skus]
    updated_at = max(str(plan["updated_at"] or "") for plan in plans)
    return {
        "id": settings.PRODUCT_BUNDLE_ID,
        "slug": "stellar-china-mainland-esim-best-value",
        "title": {
            "zh-CN": "中国大陆 eSIM 优选套餐",
            "en": "China Mainland eSIM - Best Value",
        },
        "description": {
            "zh-CN": "按价格/GB筛选的中国大陆 eSIM 套餐，请在 SKU 中选择流量和有效期。",
            "en": "China mainland eSIM plans selected by price per GB and sorted by total price. Select a data and validity SKU.",
        },
        "content": {
            "zh-CN": f"<p>{active_sku_count} 个 SKU，按套餐总价从低到高排序，请选择对应流量和有效期。</p>",
            "zh-TW": f"<p>{active_sku_count} 個 SKU，按套餐總價從低到高排序，請選擇對應流量和有效期。</p>",
            "en-US": f"<p>{active_sku_count} SKUs sorted from lowest to highest total price. Select the data and validity you need.</p>",
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
        "is_active": any(bool(plan["available"]) for plan in plans),
        "category_id": 1,
        "skus": skus,
        "created_at": min(str(plan["updated_at"] or "") for plan in plans),
        "updated_at": updated_at,
    }


# Internal bookkeeping states that must never reach the downstream protocol.
PROTOCOL_STATUS_ALIASES = {"creating": "fulfilling", "": "fulfilling"}


def protocol_status(status: str | None) -> str:
    """Collapse internal order states into the Dujiao upstream status set."""
    return PROTOCOL_STATUS_ALIASES.get(status or "", status or "fulfilling")


def status_for_dujiao(stellar_status: str) -> str:
    return {
        "processing": "fulfilling",
        "vpn_processing": "fulfilling",
        "fulfilled": "delivered",
        "failed": "failed",
        "manual_review": "manual_review",
        "refunded": "canceled",
    }.get(stellar_status, stellar_status or "fulfilling")


def fulfillment_from_stellar(order_data: dict[str, Any], expected_quantity: int | None = None) -> dict[str, Any] | None:
    esims = order_data.get("esims") or []
    if not esims:
        return None
    delivered = [item for item in esims if item.get("status") == "fulfilled"]
    expected = expected_quantity or int(order_data.get("quantity") or len(esims))
    if len(delivered) != expected or any(
        not (
            item.get("customer_link")
            or (item.get("installation") or {}).get("qr_code_url")
            or (item.get("installation") or {}).get("activation_code")
        )
        for item in delivered
    ):
        return None
    delivery_items: list[dict[str, Any]] = []
    payload_lines: list[str] = []
    for esim in delivered:
        installation = esim.get("installation") or {}
        customer_link = esim.get("customer_link") or installation.get("qr_code_url")
        if customer_link:
            payload_lines.append(str(customer_link))
        elif installation.get("activation_code"):
            payload_lines.append(str(installation["activation_code"]))
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


def order_response(order: dict[str, Any], include_items: bool = True) -> dict[str, Any]:
    response: dict[str, Any] = {
        "ok": True,
        "order_id": order["id"],
        "order_no": order["order_no"],
        "status": protocol_status(order["status"]),
        "amount": order["amount"],
        "refunded_amount": order.get("refunded_amount") or "0.00",
        "currency": order["currency"],
    }
    plan = store.get_plan(order["plan_id"]) if include_items else None
    if plan:
        plan_product = product_from_plan(plan)
        response["items"] = [
            {
                "product_id": plan_product["id"],
                "sku_id": plan_product["skus"][0]["id"],
                "title": plan_product["title"],
                "quantity": order["quantity"],
                # Report the quoted purchase price, not the current catalogue price.
                "unit_price": order.get("unit_cost") or plan_product["price_amount"],
                "total_price": order["amount"],
                "fulfillment_type": "auto",
            }
        ]
    fulfillment = json_load(order.get("fulfillment_json"), None)
    if fulfillment:
        response["fulfillment"] = fulfillment
    return response
