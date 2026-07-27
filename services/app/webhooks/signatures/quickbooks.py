"""services/app/webhooks/signatures/quickbooks.py — QuickBooks HMAC-SHA256 verifier.

Intuit signs webhook deliveries with HMAC-SHA256 over the raw request body using
the app's **verifier token** as the key, and presents the digest in the
`intuit-signature` header as a **base64** string (NOT hex — this is the one
difference from the GitHub/Jira hex-with-`sha256=` scheme).

The per-tenant verifier token is resolved by
`services/app/webhooks/secrets.py::load_installation_secrets` from the `provider_installations`
row (provider='quickbooks') the seed/onboarding step registers.

Like GitHub/Jira, the digest is over the body alone (no timestamp envelope);
idempotency is enforced at the ingestion layer via the versioned `external_id`.

NOTE (2026 migration): Intuit is moving webhook payloads to the CloudEvents
format; the signature scheme (HMAC-SHA256 over the raw body, base64 in
`intuit-signature`) is unchanged, so this verifier is forward-compatible. The
handler parses whichever payload shape arrives.
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


class QuickBooksVerifier:
    provider = "quickbooks"

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
            headers, "intuit-signature", provider=self.provider
        )

        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(secret.value.encode("utf-8"), body, hashlib.sha256)
            expected = base64.b64encode(mac.digest()).decode("ascii")
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "quickbooks signature does not match any active verifier token",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = QuickBooksVerifier()


__all__ = ["QuickBooksVerifier", "verifier"]
