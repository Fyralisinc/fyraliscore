"""services/webhooks/signatures/notion.py — Notion HMAC SHA-256 verifier (IN-14 webhooks).

Notion signs each webhook delivery with HMAC-SHA256 over the raw request
body, keyed by the subscription's `verification_token`, and presents the
digest in the `X-Notion-Signature` header. Notion formats the header as
`sha256=<hex>`; we accept the bare hex too for forward-compat.

The `verification_token` is an APP-LEVEL secret (one subscription per
integration; events from every workspace that installed the integration
arrive on the one endpoint signed with the one token) — exactly GitHub's
App-level webhook-secret shape. It is loaded from
`NOTION_WEBHOOK_VERIFICATION_TOKEN` (see services/webhooks/secrets.py),
NOT from a per-tenant `provider_installations.secret_ref` (that column
holds the bot token).

The one-time verification POST that DELIVERS the token is unsigned (there
is no token yet to sign with); it is intercepted in the router BEFORE
verification — see services/integrations/notion/webhook.py::is_verification_handshake.
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


class NotionVerifier:
    provider = "notion"

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
            headers, "X-Notion-Signature", provider=self.provider,
        )

        # Try every active token (current + previous during rotation).
        # Each comparison is constant-time; loop count is bounded by the
        # number of active tokens (1–2), uncorrelated with the candidate.
        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(secret.value.encode("utf-8"), body, hashlib.sha256)
            expected_hex = mac.hexdigest()
            # Accept both `sha256=<hex>` (Notion's documented format) and
            # the bare hex, for resilience to header-format drift.
            if constant_time_str_eq(
                _PREFIX + expected_hex, signature
            ) or constant_time_str_eq(expected_hex, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "notion signature does not match any active verification token",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,  # Notion's HMAC is over the body alone.
        )


verifier = NotionVerifier()


__all__ = ["NotionVerifier", "verifier"]
