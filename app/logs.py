"""Logging setup: plain text by default, opt-in JSON lines for log shippers."""

from __future__ import annotations

import json
import logging
import sys

# Fields that are copied from ``record.extra`` into structured log lines.
STRUCTURED_FIELDS = (
    "order_id",
    "plan_id",
    "event_id",
    "sim_id",
    "from_status",
    "to_status",
    "notified",
    "request_id",
)


class JsonFormatter(logging.Formatter):
    """Render one JSON object per line, keeping exception details."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in STRUCTURED_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure(level: str, log_format: str) -> None:
    """Install a single stderr handler on our logger; safe to call once at import."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        JsonFormatter()
        if log_format == "json"
        else logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    logger = logging.getLogger("stellar-dujiao-bridge")
    for existing in list(logger.handlers):
        logger.removeHandler(existing)
    logger.addHandler(handler)
    logger.setLevel(level.upper() or "INFO")
    logger.propagate = False
    logging.getLogger("uvicorn.error").setLevel(level.upper() or "INFO")
