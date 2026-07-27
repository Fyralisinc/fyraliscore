"""services/app/webhooks/signatures/deel.py — Deel HMAC SHA-256 verifier.

TODO(human): confirm Deel webhook signature scheme (HMAC algo, digest encoding
hex vs base64, header name, and prefix) against the Deel docs. UNVERIFIED — the
default below mirrors the Mercury archetype (HMAC-SHA256 over the raw body, hex,
`sha256=` prefix, `Deel-Signature` header). The header name and prefix are
module constants (`_HEADER`, `_PREFIX`) so the verified scheme can be dropped in
without touching the verify loop, which already iterates ALL active secrets for
rotation.

The per-tenant signing secret is resolved by
`services/app/webhooks/secrets.py::load_installation_secrets` from the `provider_installations`
row (provider='deel') the seed/onboarding step registers.

Like GitHub/Jira, the digest is assumed to be over the body alone (no timestamp
envelope), so there is no replay window here; idempotency is enforced at the
ingestion layer via the versioned `external_id`.
"""
from __future__ import annotations

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


# UNVERIFIED — see module TODO(human). Defaults follow the Mercury archetype.
_HEADER = "Deel-Signature"
_PREFIX = "sha256="


class DeelVerifier:
    provider = "deel"

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
            headers, _HEADER, provider=self.provider
        )
        if not signature.startswith(_PREFIX):
            raise WebhookVerificationError(
                "malformed_signature_header",
                f"{_HEADER} must be prefixed with {_PREFIX!r}",
                provider=self.provider,
            )

        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(secret.value.encode("utf-8"), body, hashlib.sha256)
            expected = _PREFIX + mac.hexdigest()
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "deel signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = DeelVerifier()


__all__ = ["DeelVerifier", "verifier"]
