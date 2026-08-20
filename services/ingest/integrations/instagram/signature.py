"""Meta `X-Hub-Signature-256` HMAC helpers for Instagram webhooks."""
from __future__ import annotations

import hashlib
import hmac


def sign_payload(app_secret: str, body: bytes) -> str:
    digest = hmac.new(app_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify_signature(
    app_secret: str,
    body: bytes,
    signature_header: str | None,
) -> bool:
    if not signature_header or not signature_header.startswith("sha256="):
        return False
    return hmac.compare_digest(sign_payload(app_secret, body), signature_header)


__all__ = ["sign_payload", "verify_signature"]
