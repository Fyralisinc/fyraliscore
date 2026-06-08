"""services/app/webhooks/signatures/brex.py — Brex HMAC webhook verifier.

TODO(human): confirm Brex webhook signature scheme (HMAC algorithm, digest
encoding hex-vs-base64, header name, and signature prefix). This is UNVERIFIED
(blueprint §5 #1). The SAFE default below clones the Mercury contract: HMAC-
SHA256 over the raw request body, presented as `sha256=<hex>` in a
`Brex-Signature` header (GitHub-style). The three unverified knobs are exposed as
the module constants `_HEADER_NAME`, `_PREFIX`, and `_DIGEST_ENCODING` so that
confirming the real scheme is a one-line edit per knob, not a rewrite.

The per-tenant signing secret(s) are resolved by
`services/app/webhooks/secrets.py::load_secrets` from the `provider_installations`
row (provider='brex') the seed/onboarding step registers. The verifier loops over
ALL active secrets so a rotation (two valid secrets in flight) verifies.

Like GitHub/Jira, the digest is over the body alone (no timestamp envelope), so
there is no replay window here; idempotency is enforced at the ingestion layer
via the versioned `external_id`.
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


# --- UNVERIFIED scheme knobs (blueprint §5 #1) — default to Mercury's scheme. ---
# TODO(human): confirm each against Brex webhook docs.
_HEADER_NAME = "Brex-Signature"   # header carrying the signature
_PREFIX = "sha256="               # prefix on the header value ("" if none)
_DIGEST_ENCODING = "hex"          # "hex" or "base64"


def _encode_digest(mac: "hmac.HMAC") -> str:
    if _DIGEST_ENCODING == "base64":
        return base64.b64encode(mac.digest()).decode("ascii")
    return mac.hexdigest()


class BrexVerifier:
    provider = "brex"

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
                "brex signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = BrexVerifier()


__all__ = ["BrexVerifier", "verifier"]
