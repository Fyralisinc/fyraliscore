"""services/webhooks/signatures/slack.py — Slack v0 HMAC verifier.

Delegates the cryptographic work to the existing
`services.ingestion.handlers.slack.verify_slack_signature` so we
preserve a single source of truth for Slack's signing semantics
(constitution Principle X — no premature abstraction; one
implementation, two callers).

Slack v0 protocol:
    basestring = f"v0:{X-Slack-Request-Timestamp}:{body}"
    expected   = "v0=" + hex(hmac_sha256(secret, basestring))
    compare against X-Slack-Signature in constant time.

Replay window: 300s by default (Slack's documented window).
Configurable via `SLACK_MAX_TIMESTAMP_AGE_S` env var (read by the
underlying verifier) or `max_age_s` kwarg.
"""
from __future__ import annotations

import time
from typing import Mapping, Sequence

from services.ingestion.handlers.slack import (
    SlackSignatureError,
    verify_slack_signature,
)
from services.webhooks.verifier import (
    Secret,
    VerifiedContext,
    WebhookVerificationError,
    require_header,
    require_secrets,
)


_DEFAULT_MAX_AGE_S = 300


class SlackVerifier:
    provider = "slack"

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        secrets: Sequence[Secret],
        now: float | None = None,
    ) -> VerifiedContext:
        require_secrets(secrets, provider=self.provider)
        timestamp = require_header(
            headers, "X-Slack-Request-Timestamp", provider=self.provider
        )
        signature = require_header(
            headers, "X-Slack-Signature", provider=self.provider
        )

        # The shared verifier raises `SlackSignatureError` for any of:
        # missing/malformed timestamp, replay window violation, or
        # signature mismatch. We translate each into the canonical
        # `WebhookVerificationError` reason.
        last_err: SlackSignatureError | None = None
        matched_label: str | None = None
        for secret in secrets:
            try:
                verify_slack_signature(
                    body,
                    timestamp,
                    signature,
                    secret.value,
                    max_age_s=_DEFAULT_MAX_AGE_S,
                    now=now,
                )
                matched_label = secret.label
                last_err = None
                break
            except SlackSignatureError as e:
                last_err = e
                # If the error is "too old" or "malformed timestamp",
                # short-circuit — re-trying with a different secret
                # would not help and we want to surface the canonical
                # reason. We distinguish via the exception's message
                # context.
                if "too old" in e.message or "not integer" in e.message:
                    break
                continue

        if last_err is not None:
            reason = _classify_slack_error(last_err)
            raise WebhookVerificationError(
                reason,
                last_err.message,
                provider=self.provider,
            )

        try:
            ts_int = int(timestamp)
        except ValueError:
            # Should not happen — the underlying verifier would have
            # raised. Defensive only.
            ts_int = int(now if now is not None else time.time())

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched_label,
            signed_timestamp=ts_int,
        )


def _classify_slack_error(err: SlackSignatureError) -> str:
    msg = err.message
    if "too old" in msg:
        return "expired_timestamp"
    if "not integer" in msg or "missing" in msg:
        return "malformed_signature_header"
    return "signature_mismatch"


verifier = SlackVerifier()


__all__ = ["SlackVerifier", "verifier"]
