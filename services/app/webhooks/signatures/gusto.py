"""services/app/webhooks/signatures/gusto.py — Gusto HMAC verifier.

Gusto signs webhook deliveries with an HMAC over the raw request body using a
per-subscription signing secret. The digest is presented in a signature header.

TODO(human): confirm Gusto webhook signature scheme — the exact header NAME,
    the HMAC digest algorithm, and the digest ENCODING (base64 vs hex, and any
    `sha256=` prefix). UNVERIFIED. This verifier defaults to the QuickBooks/Intuit
    archetype (HMAC-SHA256 over the raw body, base64-encoded, in the
    `intuit-signature` header). All three knobs are module constants below
    (`_SIGNATURE_HEADER`, `_DIGEST_ENCODING`, `_SIGNATURE_PREFIX`) so confirming
    the real scheme is a one-line change. The verifier loops over ALL active
    secrets to support per-subscription secret rotation.

The per-tenant signing secret is resolved by
`services/app/webhooks/secrets.py::load_secrets` from the `provider_installations`
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


# --- Configurable signature scheme (see TODO atop module — UNVERIFIED) ---
# Defaults clone the QuickBooks/Intuit archetype. Change these three constants
# once the real Gusto scheme is confirmed.
_SIGNATURE_HEADER = "Gusto-Signature"    # TODO(human): confirm Gusto webhook header name
_DIGEST_ENCODING = "base64"              # "base64" | "hex"
_SIGNATURE_PREFIX = ""                   # e.g. "sha256=" for the GitHub/Jira scheme


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
