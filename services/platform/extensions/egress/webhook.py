"""services/platform/extensions/egress/webhook.py — HMAC signing for push delivery.

The opt-in webhook pusher signs each POST body so the developer-hosted extension
can verify it came from Fyralis (and wasn't tampered with). The SDK mirrors
``verify_signature`` in ``fyralis_ext``. Signing key = the extension's client
secret (shared secret), HMAC-SHA256 over the exact request body.
"""
from __future__ import annotations

import hashlib
import hmac

SIGNATURE_HEADER = "X-Fyralis-Signature"
TIMESTAMP_HEADER = "X-Fyralis-Timestamp"


def sign(body: bytes, secret: str) -> str:
    """Return the ``sha256=<hex>`` signature for a webhook body."""
    mac = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"sha256={mac}"


def verify_signature(body: bytes, signature: str, secret: str) -> bool:
    """Constant-time check that ``signature`` matches ``sign(body, secret)``."""
    if not signature:
        return False
    return hmac.compare_digest(sign(body, secret), signature)


__all__ = ["sign", "verify_signature", "SIGNATURE_HEADER", "TIMESTAMP_HEADER"]
