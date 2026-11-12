"""Minimal Stellar Wholesale API client."""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

import httpx

from . import settings

log = logging.getLogger("stellar-dujiao-bridge")


class StellarError(Exception):
    def __init__(self, status_code: int, message: str, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.body = body


class StellarClient:
    def __init__(self) -> None:
        self.client = httpx.AsyncClient(
            base_url=settings.STELLAR_BASE_URL,
            timeout=settings.HTTP_TIMEOUT_SECONDS,
            headers={"Authorization": f"Bearer {settings.STELLAR_API_KEY}", "Accept": "application/json"},
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
                if method.upper() == "GET" and attempt == 0:
                    log.warning("Stellar read failed (%s); retrying once", type(exc).__name__)
                    await asyncio.sleep(1)
                    continue
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
        if response.status_code >= 400:
            log.warning(
                "Stellar response: method=%s status=%s request_id=%s",
                method,
                response.status_code,
                response.headers.get("X-Request-ID", "unknown"),
            )
        if response.status_code < 200 or response.status_code >= 300:
            error = body.get("error", {}) if isinstance(body, dict) else {}
            message = error.get("message") or response.text or "Stellar request failed"
            raise StellarError(response.status_code, message, body)
        return body

    async def wallet(self) -> dict[str, Any]:
        return await self.request("GET", "/wallet")

    async def list_plans(self, page: int = 1, per_page: int = 100) -> dict[str, Any]:
        return await self.request(
            "GET",
            "/plans",
            params={"page": page, "per_page": per_page, "country_code": "CN", "destination": "China"},
        )

    async def get_plan(self, plan_id: str) -> dict[str, Any]:
        return await self.request("GET", f"/plans/{plan_id}")

    async def get_esim(self, sim_id: str) -> dict[str, Any]:
        return await self.request("GET", f"/esims/{sim_id}")

    async def plans_by_ids(self, plan_ids: list[str]) -> list[dict[str, Any]]:
        if not 1 <= len(plan_ids) <= 100 or len(set(plan_ids)) != len(plan_ids):
            raise ValueError("A catalogue batch must contain 1–100 distinct IDs")
        for plan_id in plan_ids:
            uuid.UUID(plan_id)
        plans: list[dict[str, Any]] = []
        page = 1
        while True:
            params = [("ids[]", plan_id) for plan_id in plan_ids] + [("per_page", "100"), ("page", str(page))]
            response = await self.request("GET", "/plans", params=params)
            batch = response.get("data")
            if not isinstance(batch, list) or not isinstance(response.get("meta"), dict):
                raise ValueError("Incomplete catalogue batch response")
            if any(str(plan.get("id")) not in plan_ids for plan in batch):
                raise ValueError("Catalogue batch returned an unexpected plan")
            plans.extend(batch)
            last_page = int(response["meta"].get("last_page") or 0)
            if last_page < page or last_page > 100 or (not batch and page < last_page):
                raise ValueError("Incomplete catalogue batch pagination")
            if page == last_page:
                return plans
            page += 1

    async def create_order(self, plan_id: str, quantity: int, days: int | None, idempotency_key: str) -> dict[str, Any]:
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
