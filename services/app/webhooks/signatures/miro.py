"""services/app/webhooks/signatures/miro.py — Miro webhook verifier (LEGACY).

============================================================================
CONFIRMED (developers.miro.com/changelog/removed-experimental-webhooks-support):
Miro DISCONTINUED its experimental webhooks on 2025-12-05 — event transmission
stopped, the subscription endpoints were removed, and there is NO replacement.
Miro's webhooks NEVER had an HMAC signature scheme; the only authenticity check
was a challenge-response handshake (echo the `challenge` body field) over HTTPS.
So the HMAC-SHA256 verifier below is a SYNTHETIC-GATE stand-in only — it does not
correspond to any real Miro scheme.

PRODUCTION (TODO/architectural follow-up): Miro must be a POLL-ONLY source (like
Carta) — re-list boards (`GET /v2/boards`, offset pagination) and board items
(`GET /v2/boards/{id}/items`, cursor pagination) on a cadence; there is no live
webhook edge to verify. Until that conversion lands, this verifier keeps the gate
green by exercising the webhook→202→handler plumbing with an HMAC stand-in.
============================================================================

The per-tenant signing secret(s) are resolved by
`services/app/webhooks/secrets.py::load_secrets` from the `provider_installations`
row (provider='miro'). The verifier loops over ALL active secrets so a rotation
(two valid secrets in flight) verifies.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Mapping, Sequence

from services.app.webhooks.verifier import (
    Secret,
    VerifiedContext,
    WebhookVerificationError,
    constant_time_str_eq,
    require_header,
    require_secrets,
)


# --- Synthetic-gate HMAC stand-in (Miro has NO real webhook/HMAC scheme; see
# the module header — webhooks were discontinued 2025-12-05). Retained only so
# the gate can drive miro's webhook→202→handler plumbing. ---
_HEADER_NAME = "X-Miro-Signature"   # synthetic-only header
_PREFIX = "sha256="                 # synthetic-only prefix
_DIGEST_ENCODING = "hex"            # synthetic-only encoding


def _encode_digest(mac: "hmac.HMAC") -> str:
    if _DIGEST_ENCODING == "base64":
        return base64.b64encode(mac.digest()).decode("ascii")
    return mac.hexdigest()


class MiroVerifier:
    provider = "miro"

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        secrets: Sequence[Secret],
        now: float | None = None,
    ) -> VerifiedContext:
        require_secrets(secrets, provider=self.provider)
        signature = require_header(
            headers, _HEADER_NAME, provider=self.provider
        )
        if _PREFIX and not signature.startswith(_PREFIX):
            raise WebhookVerificationError(
                "malformed_signature_header",
                f"{_HEADER_NAME} must be prefixed with {_PREFIX!r}",
                provider=self.provider,
            )

        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(secret.value.encode("utf-8"), body, hashlib.sha256)
            expected = _PREFIX + _encode_digest(mac)
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "miro signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = MiroVerifier()


__all__ = ["MiroVerifier", "verifier"]
