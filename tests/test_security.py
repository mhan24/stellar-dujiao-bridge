import hashlib
import hmac
import time

from app.security import dujiao_signature, stellar_signature, verify_dujiao_signature, verify_stellar_signature


def test_dujiao_signature_round_trip():
    secret = "secret"
    body = b'{"sku_id":1}'
    timestamp = int(time.time())
    signature = dujiao_signature(secret, "POST", "/api/v1/upstream/orders", timestamp, body)
    assert verify_dujiao_signature(
        secret,
        "POST",
        "/api/v1/upstream/orders",
        str(timestamp),
        signature,
        body,
    )


def test_stellar_signature_round_trip():
    secret = "whsec_test"
    body = b'{"id":"evt_1"}'
    timestamp = int(time.time())
    signature = stellar_signature(secret, timestamp, body)
    assert verify_stellar_signature(secret, f"t={timestamp},v1={signature}", body)
