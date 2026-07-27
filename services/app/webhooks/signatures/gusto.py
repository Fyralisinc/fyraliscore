"""services/app/webhooks/signatures/gusto.py — Gusto HMAC verifier.

Gusto signs webhook deliveries with an HMAC-SHA256 over the raw request body,
keyed by the per-subscription `verification_token`, and presents the digest as a
lowercase **hex** string in the **`X-Gusto-Signature`** header.

VERIFIED against official Gusto docs (Phase-2 contract research): the
`Gusto/gusto.github.io` developer doc states the signature is a "hex digest of
HMAC-SHA256 computed from the payload and the secret key" and ships an official
Ruby `OpenSSL::HMAC.hexdigest` verification sample with a 64-char hex
`X-Gusto-Signature` example; the current docs.gusto.com/embedded-payroll/docs/
webhooks page describes the same algorithm. There is NO `sha256=` prefix and NO
timestamp/replay envelope (the body `timestamp` is informational only). The
three knobs are module constants; per the docs hex is compared
case-insensitively for robustness. The verifier loops over ALL active secrets to
support per-subscription secret rotation.

The per-tenant signing secret is resolved by
`services/app/webhooks/secrets.py::load_installation_secrets` from the `provider_installations`
row (provider='gusto') the seed/onboarding step registers.

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


# --- Signature scheme (VERIFIED from official Gusto docs) ---
_SIGNATURE_HEADER = "X-Gusto-Signature"  # verbatim per docs.gusto.com webhooks
_DIGEST_ENCODING = "hex"                 # lowercase hex digest (Ruby OpenSSL::HMAC.hexdigest)
_SIGNATURE_PREFIX = ""                   # no prefix


def _encode_digest(mac: "hmac.HMAC") -> str:
    if _DIGEST_ENCODING == "hex":
        return _SIGNATURE_PREFIX + mac.hexdigest()
    return _SIGNATURE_PREFIX + base64.b64encode(mac.digest()).decode("ascii")


class GustoVerifier:
    provider = "gusto"

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        secrets: Sequence[Secret],
        now: float | None = None,
    ) -> VerifiedContext:
        require_secrets(secrets, provider=self.provider)
        # Hex is case-insensitive; normalise to lowercase so an upper/mixed-case
        # digest still verifies (this verifier is Gusto-only and always hex).
        signature = require_header(
            headers, _SIGNATURE_HEADER, provider=self.provider
        ).strip().lower()

        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(secret.value.encode("utf-8"), body, hashlib.sha256)
            expected = _encode_digest(mac).lower()
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "gusto signature does not match any active verifier token",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = GustoVerifier()


__all__ = ["GustoVerifier", "verifier"]
