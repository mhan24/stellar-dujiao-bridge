"""Process configuration, read once from the environment at import time."""

from __future__ import annotations

import os
from decimal import Decimal, InvalidOperation


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_flag(name: str, default: bool = False) -> bool:
    raw = env(name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _number(name: str, default: str, kind: type) -> int | float:
    raw = env(name, default)
    try:
        return kind(raw)
    except ValueError:
        raise RuntimeError(f"{name} must be a number, got {raw!r}") from None


def _int(name: str, default: str) -> int:
    return int(_number(name, default, int))


def _float(name: str, default: str) -> float:
    return float(_number(name, default, float))


def _decimal(name: str, default: str) -> Decimal:
    raw = env(name, default)
    try:
        value = Decimal(raw)
    except InvalidOperation:
        raise RuntimeError(f"{name} must be a decimal number, got {raw!r}") from None
    if not value.is_finite():
        raise RuntimeError(f"{name} must be a finite decimal number, got {raw!r}")
    return value


# Stellar Wholesale upstream
STELLAR_BASE_URL = env("STELLAR_BASE_URL", "https://wholesale.stellarsecurity.com/api/v1").rstrip("/") + "/"
STELLAR_API_KEY = env("STELLAR_API_KEY")
STELLAR_WEBHOOK_SECRET = env("STELLAR_WEBHOOK_SECRET")
HTTP_TIMEOUT_SECONDS = _float("HTTP_TIMEOUT_SECONDS", "30")
STELLAR_PAGINATION_DELAY_SECONDS = _float("STELLAR_PAGINATION_DELAY_SECONDS", "1.5")

# Dujiao-Next downstream
DUJIAO_API_KEY = env("DUJIAO_API_KEY")
DUJIAO_API_SECRET = env("DUJIAO_API_SECRET")
SITE_NAME = env("SITE_NAME", "Stellar Wholesale eSIM")
LOG_LEVEL = env("LOG_LEVEL", "INFO")
LOG_FORMAT = env("LOG_FORMAT", "text").lower()
REQUIRE_DOWNSTREAM_ORDER_NO = env_flag("REQUIRE_DOWNSTREAM_ORDER_NO", True)
MAX_REQUEST_BODY_BYTES = _int("MAX_REQUEST_BODY_BYTES", "1048576")

# Local storage and catalogue scheduling
DB_PATH = env("DB_PATH", "/data/bridge.db")
SYNC_INTERVAL_SECONDS = _int("SYNC_INTERVAL_SECONDS", "60")
WALLET_CACHE_SECONDS = _float("WALLET_CACHE_SECONDS", "30")
PRODUCT_WALLET_TIMEOUT_SECONDS = _float("PRODUCT_WALLET_TIMEOUT_SECONDS", "15")
# A successful but empty or truncated catalogue response must never wipe stock.
CATALOGUE_MIN_ACTIVE_RATIO = _decimal("CATALOGUE_MIN_ACTIVE_RATIO", "0.5")
CATALOGUE_MAX_PAGES = _int("CATALOGUE_MAX_PAGES", "50")

# Durable queues give up after a bounded number of attempts and are pruned, so a
# misconfigured callback cannot grow SQLite indefinitely.
CALLBACK_MAX_ATTEMPTS = _int("CALLBACK_MAX_ATTEMPTS", "20")
WEBHOOK_MAX_ATTEMPTS = _int("WEBHOOK_MAX_ATTEMPTS", "20")
QUEUE_RETENTION_DAYS = _int("QUEUE_RETENTION_DAYS", "30")
# Delivery payloads contain QR codes and activation codes; 0 keeps them forever.
FULFILLMENT_RETENTION_DAYS = _int("FULFILLMENT_RETENTION_DAYS", "0")
QUEUE_PRUNE_INTERVAL_SECONDS = _int("QUEUE_PRUNE_INTERVAL_SECONDS", "3600")
# `esim.*` webhooks whose sim_id is not mapped yet refresh in-flight orders,
# capped and rate limited so an event burst cannot hammer the upstream.
ESIM_EVENT_SWEEP_LIMIT = _int("ESIM_EVENT_SWEEP_LIMIT", "20")
ESIM_EVENT_SWEEP_COOLDOWN_SECONDS = _float("ESIM_EVENT_SWEEP_COOLDOWN_SECONDS", "5")

# Product selection model (portfolio-v1)
PRODUCT_BUNDLE_ID = 900000001
TOP_PLAN_LIMIT = _int("TOP_PLAN_LIMIT", "10")
PLAN_UPGRADE_PRICE_TOLERANCE = _decimal("PLAN_UPGRADE_PRICE_TOLERANCE", "0.01")
PLAN_SELECTION_MODEL = "portfolio-v1"
PLAN_SELECTION_PRICE_BAND = Decimal("0.05")
PLAN_SELECTION_TIME_WEIGHT = Decimal("0.15")
PLAN_SELECTION_SHORT_DAYS = 7
PLAN_SELECTION_LONG_DAYS = 30
PLAN_SELECTION_TIME_CAP_DAYS = 90


def validate() -> list[str]:
    """Return non-fatal warnings; raise RuntimeError on unusable settings."""
    warnings = []
    if not STELLAR_API_KEY:
        warnings.append("STELLAR_API_KEY is not configured")
    if not STELLAR_WEBHOOK_SECRET:
        warnings.append("STELLAR_WEBHOOK_SECRET is not configured; webhook verification will fail")
    if not DUJIAO_API_KEY or not DUJIAO_API_SECRET:
        warnings.append("DUJIAO_API_KEY/DUJIAO_API_SECRET are not configured; every API call will fail")
    if SYNC_INTERVAL_SECONDS <= 0:
        raise RuntimeError("SYNC_INTERVAL_SECONDS must be greater than zero")
    if WALLET_CACHE_SECONDS < 0:
        raise RuntimeError("WALLET_CACHE_SECONDS must not be negative")
    if PRODUCT_WALLET_TIMEOUT_SECONDS <= 0:
        raise RuntimeError("PRODUCT_WALLET_TIMEOUT_SECONDS must be greater than zero")
    if QUEUE_RETENTION_DAYS < 1:
        raise RuntimeError("QUEUE_RETENTION_DAYS must be at least 1")
    if FULFILLMENT_RETENTION_DAYS < 0:
        raise RuntimeError("FULFILLMENT_RETENTION_DAYS must not be negative")
    if ESIM_EVENT_SWEEP_LIMIT < 0:
        raise RuntimeError("ESIM_EVENT_SWEEP_LIMIT must not be negative")
    if ESIM_EVENT_SWEEP_COOLDOWN_SECONDS < 0:
        raise RuntimeError("ESIM_EVENT_SWEEP_COOLDOWN_SECONDS must not be negative")
    if TOP_PLAN_LIMIT < 0:
        warnings.append("TOP_PLAN_LIMIT is negative; every qualifying plan will be published")
    if not 0 <= CATALOGUE_MIN_ACTIVE_RATIO <= 1:
        raise RuntimeError("CATALOGUE_MIN_ACTIVE_RATIO must be between 0 and 1")
    if PLAN_UPGRADE_PRICE_TOLERANCE < 0:
        raise RuntimeError("PLAN_UPGRADE_PRICE_TOLERANCE must not be negative")
    if LOG_FORMAT not in {"text", "json"}:
        raise RuntimeError(f"LOG_FORMAT must be 'text' or 'json', got {LOG_FORMAT!r}")
    if LOG_LEVEL.upper() not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise RuntimeError(f"LOG_LEVEL is not a known level: {LOG_LEVEL!r}")
    if not DB_PATH:
        raise RuntimeError("DB_PATH must not be empty")
    return warnings
