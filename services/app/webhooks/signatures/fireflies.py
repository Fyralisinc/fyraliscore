"""services/app/webhooks/signatures/fireflies.py — Fireflies HMAC webhook verifier.

CONFIRMED (docs.fireflies.ai/graphql-api/webhooks): Fireflies signs the webhook
payload with HMAC-SHA256 and presents it in the `x-hub-signature` header (NOT a
Fireflies-branded header). The digest ENCODING (hex vs base64) and whether the
value carries a `sha256=` prefix are NOT spelled out in the docs, so those two
knobs (`_PREFIX`, `_DIGEST_ENCODING`) keep the GitHub-style default
(`sha256=`+hex) and remain TODO(human) to confirm empirically against a real
delivery. The header name is confirmed.

The per-tenant signing secret(s) are resolved by
`services/app/webhooks/secrets.py::load_secrets` from the `provider_installations`
row (provider='fireflies') the seed/onboarding step registers. The verifier
loops over ALL active secrets so a rotation (two valid secrets in flight)
verifies.

Like GitHub/Brex, the digest is over the body alone (no timestamp envelope), so
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


# Header name CONFIRMED (x-hub-signature); prefix + digest encoding stay
# TODO(human) to confirm empirically (the docs don't specify them).
_HEADER_NAME = "x-hub-signature"        # CONFIRMED header carrying the signature
_PREFIX = "sha256="                     # TODO(human): confirm prefix ("" if none)
_DIGEST_ENCODING = "hex"                # TODO(human): confirm "hex" vs "base64"


def _encode_digest(mac: "hmac.HMAC") -> str:
    if _DIGEST_ENCODING == "base64":
        return base64.b64encode(mac.digest()).decode("ascii")
    return mac.hexdigest()


class FirefliesVerifier:
    provider = "fireflies"

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
                "fireflies signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = FirefliesVerifier()


__all__ = ["FirefliesVerifier", "verifier"]
