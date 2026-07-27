"""services/app/webhooks/signatures/ramp.py — Ramp HMAC webhook verifier.

`X-Ramp-Signature` is HMAC-SHA256 over the raw body, but Ramp's docs do NOT
specify whether the digest is presented as **hex** or **base64** (Phase-2
finding #35 flagged this as the one upstream unknown). Rather than guess one and
risk silently dropping every live delivery if the assumption is wrong, this
verifier evaluates BOTH encodings (a dual-conditional parser) and accepts the
delivery if the presented signature constant-time-matches the hex OR the base64
digest. An optional `sha256=` prefix (some HMAC schemes prepend it) is tolerated.
This keeps the edge resilient regardless of which encoding Ramp ships — and if
they ever switch, no code change is needed.

The per-tenant verifier token is resolved by
`services/app/webhooks/secrets.py::load_installation_secrets` from the `provider_installations`
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


_SIGNATURE_HEADER = "x-ramp-signature"
# Some HMAC schemes prepend the algorithm; strip it before the encoding compare
# so "sha256=<hex>" and a bare "<hex>" both verify.
_OPTIONAL_PREFIX = "sha256="


def _candidate_digests(secret_value: str, body: bytes) -> tuple[str, str]:
    """Return (hex, base64) HMAC-SHA256 digests of `body` keyed by the secret."""
    mac = hmac.new(secret_value.encode("utf-8"), body, hashlib.sha256)
    return mac.hexdigest(), base64.b64encode(mac.digest()).decode("ascii")


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
        raw_signature = require_header(
            headers, _SIGNATURE_HEADER, provider=self.provider
        )
        # Tolerate an optional "sha256=" prefix on either encoding.
        presented = (
            raw_signature[len(_OPTIONAL_PREFIX):]
            if raw_signature.startswith(_OPTIONAL_PREFIX)
            else raw_signature
        )

        matched: Secret | None = None
        for secret in secrets:
            hex_digest, b64_digest = _candidate_digests(secret.value, body)
            # Dual-encoding: accept whichever encoding Ramp actually sends. Both
            # comparisons are constant-time; the OR short-circuits only on a
            # genuine match (success), so it leaks no useful timing.
            if (
                constant_time_str_eq(presented, hex_digest)
                or constant_time_str_eq(presented, b64_digest)
            ):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "ramp signature does not match any active verifier token "
                "(checked both hex and base64 HMAC-SHA256 encodings)",
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
