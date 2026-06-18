"""services/platform/extensions/marketplace/signing.py — host signature over a listing (E4.2).

Public listings are signed by the host over the canonical manifest JSON so an install
can verify the published artifact wasn't altered. HMAC-SHA256 with a host marketplace
key (``MARKETPLACE_SIGNING_KEY``; SHA-256-derived so any length is safe). Ephemeral +
warned if unset (dev only).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import secrets
from typing import Any

log = logging.getLogger("extensions.marketplace.signing")
_ephemeral: str | None = None


def _key() -> bytes:
    global _ephemeral
    s = os.environ.get("MARKETPLACE_SIGNING_KEY")
    if not s:
        if _ephemeral is None:
            _ephemeral = secrets.token_urlsafe(48)
            log.warning("MARKETPLACE_SIGNING_KEY unset — using an EPHEMERAL key (dev only).")
        s = _ephemeral
    return hashlib.sha256(s.encode()).digest()


def canonical(manifest: dict[str, Any]) -> bytes:
    """Stable byte encoding of a manifest for signing (sorted keys, no whitespace)."""
    return json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()


def sign(manifest: dict[str, Any]) -> str:
    return "v1=" + hmac.new(_key(), canonical(manifest), hashlib.sha256).hexdigest()


def verify(manifest: dict[str, Any], signature: str | None) -> bool:
    if not signature:
        return False
    return hmac.compare_digest(sign(manifest), signature)


__all__ = ["sign", "verify", "canonical"]
