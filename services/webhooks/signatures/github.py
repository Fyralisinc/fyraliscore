"""services/webhooks/signatures/github.py — GitHub HMAC SHA-256 verifier.

GitHub signs delivery payloads with HMAC-SHA256 over the raw body
and presents the digest in `X-Hub-Signature-256` formatted as
`sha256=<hex>`. The deprecated SHA-1 variant `X-Hub-Signature` is NOT
accepted (security regression).

GitHub does not document a replay window — the digest is over the
body alone, no timestamp envelope. The body's `delivery_id` and
GitHub's at-least-once retry semantics produce idempotency at the
ingestion layer (via `external_id`), not here.

Header reference:
    https://docs.github.com/en/webhooks/webhook-events-and-payloads
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


class GitHubVerifier:
    provider = "github"

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
            headers, "X-Hub-Signature-256", provider=self.provider
        )
        if not signature.startswith(_PREFIX):
            raise WebhookVerificationError(
                "malformed_signature_header",
                f"X-Hub-Signature-256 must be prefixed with {_PREFIX!r}",
                provider=self.provider,
            )

        # Try every secret in turn. Each comparison is constant-time;
        # the loop iteration count is bounded by the number of active
        # secrets (1 or 2 in rotation), which is uncorrelated with the
        # candidate signature value.
        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(
                secret.value.encode("utf-8"), body, hashlib.sha256
            )
            expected = _PREFIX + mac.hexdigest()
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "github signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = GitHubVerifier()


__all__ = ["GitHubVerifier", "verifier"]
