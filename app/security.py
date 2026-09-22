from __future__ import annotations

import hashlib
import hmac
import time
from urllib.parse import urlsplit


EMPTY_BODY_MD5 = hashlib.md5(b"").hexdigest()


def body_md5(body: bytes) -> str:
    return hashlib.md5(body).hexdigest()


def dujiao_signature(secret: str, method: str, path: str, timestamp: int, body: bytes) -> str:
    sign_string = "\n".join(
        [method.upper(), urlsplit(path).path, str(timestamp), body_md5(body)]
    )
    return hmac.new(secret.encode(), sign_string.encode(), hashlib.sha256).hexdigest()


def verify_dujiao_signature(
    secret: str,
    method: str,
    path: str,
    timestamp_header: str,
    signature: str,
    body: bytes,
    tolerance_seconds: int = 60,
) -> bool:
    try:
        timestamp = int(timestamp_header)
    except (TypeError, ValueError):
        return False
    if abs(int(time.time()) - timestamp) > tolerance_seconds:
        return False
    expected = dujiao_signature(secret, method, path, timestamp, body)
    return hmac.compare_digest(expected, signature or "")


def stellar_signature(secret: str, timestamp: int, raw_body: bytes) -> str:
    message = f"{timestamp}.".encode() + raw_body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def verify_stellar_signature(
    secret: str,
    signature_header: str,
    raw_body: bytes,
    tolerance_seconds: int = 300,
) -> bool:
    values: dict[str, str] = {}
    for part in (signature_header or "").split(","):
        key, separator, value = part.partition("=")
        if separator:
            values[key.strip()] = value.strip()
    try:
        timestamp = int(values["t"])
    except (KeyError, TypeError, ValueError):
        return False
    if abs(int(time.time()) - timestamp) > tolerance_seconds:
        return False
    expected = stellar_signature(secret, timestamp, raw_body)
    return hmac.compare_digest(expected, values.get("v1", ""))
