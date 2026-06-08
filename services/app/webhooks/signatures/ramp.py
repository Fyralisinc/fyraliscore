"""services/app/webhooks/signatures/ramp.py — Ramp HMAC webhook verifier.

Cloned from the QuickBooks archetype. The Ramp webhook signature scheme is
UNVERIFIED (blueprint §5 #1), so the scheme is kept CONFIGURABLE via module
constants below and defaults to the archetype's safe HMAC-SHA256 scheme:
  - header name           : _SIGNATURE_HEADER
  - digest encoding        : _DIGEST_ENCODING ("base64" | "hex")
  - optional digest prefix : _SIGNATURE_PREFIX (e.g. "sha256=" for hex schemes)

TODO(human): confirm Ramp webhook signature (scheme + header name + base64 vs hex
+ any prefix). Default below = HMAC-SHA256 over the raw body, base64, header
`x-ramp-signature`. If Ramp uses the GitHub/Jira hex-with-`sha256=` shape, set
_DIGEST_ENCODING="hex" and _SIGNATURE_PREFIX="sha256=" — no other change needed.

The per-tenant verifier token is resolved by
`services/app/webhooks/secrets.py::load_secrets` from the `provider_installations`
row (provider='ramp') the seed/onboarding step registers. The verifier loops over
ALL active secrets so a verifier-token rotation never drops a delivery.

Like GitHub/Jira, the digest is over the body alone (no timestamp envelope);
idempotency is enforced at the ingestion layer via the versioned `external_id`.
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


# --- CONFIGURABLE signature scheme (UNVERIFIED — see module TODO) ---
# TODO(human): confirm Ramp webhook signature scheme and adjust these three.
_SIGNATURE_HEADER = "x-ramp-signature"
_DIGEST_ENCODING = "base64"  # "base64" | "hex"
_SIGNATURE_PREFIX = ""        # e.g. "sha256=" for a GitHub/Jira-style hex header


def _encode_digest(mac: "hmac.HMAC") -> str:
    if _DIGEST_ENCODING == "hex":
        return f"{_SIGNATURE_PREFIX}{mac.hexdigest()}"
    return f"{_SIGNATURE_PREFIX}{base64.b64encode(mac.digest()).decode('ascii')}"


class RampVerifier:
    provider = "ramp"

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
            headers, _SIGNATURE_HEADER, provider=self.provider
        )

        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(secret.value.encode("utf-8"), body, hashlib.sha256)
            expected = _encode_digest(mac)
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "ramp signature does not match any active verifier token",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = RampVerifier()


__all__ = ["RampVerifier", "verifier"]
