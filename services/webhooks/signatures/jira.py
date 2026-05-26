"""services/webhooks/signatures/jira.py — Jira HMAC SHA-256 verifier (IN-17).

Jira Cloud system/admin webhooks (Settings → System → Webhooks) can be
registered with a **Secret**; Jira then signs the raw request body with
HMAC-SHA256 and presents the digest in the `X-Hub-Signature` header formatted
as `sha256=<hex>` (the same scheme GitHub uses, but under the un-suffixed
`X-Hub-Signature` header name).

Per the IN-17 design doc (§6): we deliberately use HMAC signing rather than a
URL-embedded token because the admin webhook secret fits the existing Verifier
contract cleanly. The per-tenant secret is resolved by
`services/webhooks/secrets.py::load_secrets` from the `provider_installations`
row (provider='jira') the seed/onboarding step registers.

Like GitHub, Jira's digest is over the body alone (no timestamp envelope), so
there is no replay window here; idempotency is enforced at the ingestion layer
via the versioned `external_id`.
"""
from __future__ import annotations

import hashlib
import hmac
from typing import Mapping, Sequence

from services.webhooks.verifier import (
    Secret,
    VerifiedContext,
    WebhookVerificationError,
    constant_time_str_eq,
    require_header,
    require_secrets,
)


_PREFIX = "sha256="


class JiraVerifier:
    provider = "jira"

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
            headers, "X-Hub-Signature", provider=self.provider
        )
        if not signature.startswith(_PREFIX):
            raise WebhookVerificationError(
                "malformed_signature_header",
                f"X-Hub-Signature must be prefixed with {_PREFIX!r}",
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
                "jira signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = JiraVerifier()


__all__ = ["JiraVerifier", "verifier"]
