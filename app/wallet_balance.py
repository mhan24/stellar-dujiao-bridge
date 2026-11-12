"""Wallet balance cache shared by catalogue sync and order purchasing."""

from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any

from . import settings
from .stellar_client import StellarError, stellar
from .util import iso

log = logging.getLogger("stellar-dujiao-bridge")


wallet_lock = asyncio.Lock()
wallet_cache: dict[str, Any] = {
    "available_balance": None,
    "currency": "",
    "checked_at": 0.0,
    "checked_at_iso": None,
    "error": None,
}


def cached_snapshot() -> dict[str, Any]:
    """Copy of the last wallet snapshot, for read-only reporting."""
    return dict(wallet_cache)


async def wallet_snapshot(force: bool = False) -> dict[str, Any]:
    now_monotonic = time.monotonic()
    if (
        not force
        and wallet_cache["checked_at"]
        and now_monotonic - float(wallet_cache["checked_at"]) < settings.WALLET_CACHE_SECONDS
    ):
        return dict(wallet_cache)
    async with wallet_lock:
        now_monotonic = time.monotonic()
        if (
            not force
            and wallet_cache["checked_at"]
            and now_monotonic - float(wallet_cache["checked_at"]) < settings.WALLET_CACHE_SECONDS
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
