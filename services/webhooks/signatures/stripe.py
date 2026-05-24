"""services/webhooks/signatures/stripe.py — Stripe HMAC SHA-256 verifier.

Stripe signs webhook deliveries with HMAC-SHA256 over the envelope
`{timestamp}.{body}` and packages multiple values into a single
`Stripe-Signature` header:

    t=<unix_seconds>,v1=<hex>[,v1=<hex>...][,v0=<legacy>]

Multiple `v1` values may appear when Stripe rotates a webhook
endpoint's signing secret on their side. We accept the request when
ANY `v1` value matches ANY active local secret (the cross product).
We do NOT accept `v0` (Stripe's deprecated unhashed scheme).

Replay window: 300s (Stripe's documented default).

Header reference:
    https://stripe.com/docs/webhooks/signatures
"""
from __future__ import annotations

import hashlib
import hmac
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


_DEFAULT_MAX_AGE_S = int(os.environ.get("STRIPE_MAX_TIMESTAMP_AGE_S", "300"))


def _parse_signature_header(value: str) -> tuple[int | None, list[str]]:
    """Parse a Stripe-Signature header into (timestamp, [v1 values]).

    Returns (None, []) on malformed input. Order is preserved so the
    timing of comparisons does not depend on header layout.
    """
    timestamp: int | None = None
    v1s: list[str] = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        k, v = chunk.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k == "t":
            try:
                timestamp = int(v)
            except ValueError:
                return None, []
        elif k == "v1":
            v1s.append(v)
    return timestamp, v1s


class StripeVerifier:
    provider = "stripe"

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        secrets: Sequence[Secret],
        now: float | None = None,
    ) -> VerifiedContext:
        require_secrets(secrets, provider=self.provider)
        header = require_header(
            headers, "Stripe-Signature", provider=self.provider
        )
        timestamp, v1_values = _parse_signature_header(header)
        if timestamp is None or not v1_values:
            raise WebhookVerificationError(
                "malformed_signature_header",
                "Stripe-Signature must include t=<unix> and at least one v1=<hex>",
                provider=self.provider,
            )

        # Replay window enforced before signature compute. A stale
        # request never reaches the constant-time compare, which both
        # saves CPU and matches Stripe's own server-side behavior.
        now_s = int(now if now is not None else time.time())
        if abs(now_s - timestamp) > _DEFAULT_MAX_AGE_S:
            raise WebhookVerificationError(
                "expired_timestamp",
                "stripe signature timestamp outside replay window",
                provider=self.provider,
                max_age_s=_DEFAULT_MAX_AGE_S,
            )

        signed_payload = f"{timestamp}.".encode("utf-8") + body

        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(
                secret.value.encode("utf-8"),
                signed_payload,
                hashlib.sha256,
            )
            expected = mac.hexdigest()
            for v1 in v1_values:
                if constant_time_str_eq(expected, v1):
                    matched = secret
                    break
            if matched is not None:
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "stripe signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=timestamp,
        )


verifier = StripeVerifier()


__all__ = ["StripeVerifier", "verifier"]
