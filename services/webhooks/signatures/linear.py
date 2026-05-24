"""services/webhooks/signatures/linear.py — Linear HMAC SHA-256 verifier.

Linear signs webhook bodies with HMAC-SHA256 over the raw body and
presents the digest as the hex value of `Linear-Signature` (no
prefix). The webhook payload itself carries `webhookTimestamp`
(milliseconds since epoch) which Linear documents as the canonical
replay-protection value; if absent we accept the body without a
timestamp check (matching Linear's older deliveries).

Header reference:
    https://developers.linear.app/docs/graphql/webhooks
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Mapping, Sequence

from services.webhooks.verifier import (
    Secret,
    VerifiedContext,
    WebhookVerificationError,
    constant_time_str_eq,
    require_header,
    require_secrets,
)


_DEFAULT_MAX_AGE_S = int(os.environ.get("LINEAR_MAX_TIMESTAMP_AGE_S", "300"))


class LinearVerifier:
    provider = "linear"

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
            headers, "Linear-Signature", provider=self.provider
        )

        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(
                secret.value.encode("utf-8"), body, hashlib.sha256
            )
            expected = mac.hexdigest()
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "linear signature does not match any active secret",
                provider=self.provider,
            )

        # Replay-window check using `webhookTimestamp` if present in the
        # body. Linear docs say this is a defense against replay; we
        # enforce it but only when the body carries the field.
        ts_seconds: int | None = None
        try:
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                wts = parsed.get("webhookTimestamp")
                if isinstance(wts, (int, float)):
                    ts_seconds = int(wts / 1000)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

        if ts_seconds is not None:
            now_s = int(now if now is not None else time.time())
            if abs(now_s - ts_seconds) > _DEFAULT_MAX_AGE_S:
                raise WebhookVerificationError(
                    "expired_timestamp",
                    "linear webhookTimestamp outside replay window",
                    provider=self.provider,
                    max_age_s=_DEFAULT_MAX_AGE_S,
                )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=ts_seconds,
        )


verifier = LinearVerifier()


__all__ = ["LinearVerifier", "verifier"]
