"""services/app/webhooks/signatures/mercury.py — Mercury HMAC SHA-256 verifier.

Mercury signs webhook deliveries with HMAC-SHA256 over the raw request body and
presents the digest in the `Mercury-Signature` header formatted as
`sha256=<hex>` (GitHub-style). The per-tenant signing secret is resolved by
`services/app/webhooks/secrets.py::load_secrets` from the `provider_installations`
row (provider='mercury') the seed/onboarding step registers.

Like GitHub/Jira, the digest is over the body alone (no timestamp envelope), so
there is no replay window here; idempotency is enforced at the ingestion layer
via the versioned `external_id`.
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


_PREFIX = "sha256="


class MercuryVerifier:
    provider = "mercury"

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
            headers, "Mercury-Signature", provider=self.provider
        )
        if not signature.startswith(_PREFIX):
            raise WebhookVerificationError(
                "malformed_signature_header",
                f"Mercury-Signature must be prefixed with {_PREFIX!r}",
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
                "mercury signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = MercuryVerifier()


__all__ = ["MercuryVerifier", "verifier"]
