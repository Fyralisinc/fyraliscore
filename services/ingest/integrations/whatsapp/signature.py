"""Meta `X-Hub-Signature-256` HMAC for WhatsApp Cloud API webhooks.

Meta signs each webhook POST with `HMAC-SHA256(app_secret, raw_request_body)`
and sends it in the `X-Hub-Signature-256: sha256=<hex>` header. The signature
MUST be computed over the EXACT raw bytes received (not a re-serialised JSON
form). Single source of truth for both the receiver
(`services/app/gateway/whatsapp_router.py`) and the local simulator
(`scripts/whatsapp_simulate.py`).
"""
from __future__ import annotations

import hashlib
import hmac


def sign_payload(app_secret: str, body: bytes) -> str:
    """Return the `sha256=<hex>` header value Meta would send for `body`."""
    digest = hmac.new(app_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify_signature(app_secret: str, body: bytes, signature_header: str | None) -> bool:
    """Constant-time check of an inbound `X-Hub-Signature-256` header.

    Returns False (never raises) on a missing/malformed header or mismatch.
    """
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    expected = sign_payload(app_secret, body)
    return hmac.compare_digest(expected, signature_header)


__all__ = ["sign_payload", "verify_signature"]
