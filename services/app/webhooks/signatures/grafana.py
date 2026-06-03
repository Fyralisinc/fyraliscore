"""services/app/webhooks/signatures/grafana.py — Grafana Alerting HMAC verifier.

A Grafana Alerting **webhook contact point** (Grafana 12.0+, May 2025) can be
configured with an HMAC shared secret; Grafana then signs the delivery with
HMAC-SHA256 and presents the digest as a **hex string (no prefix)** in the
`X-Grafana-Alerting-Signature` header.

Signed bytes:
  - default: the raw request body alone.
  - if a timestamp header is configured on the contact point, Grafana signs
    `"{unix_ts}:" + body` and sends the timestamp in that header. We support this
    when `GRAFANA_WEBHOOK_TIMESTAMP_HEADER` names the header (off by default).

The per-tenant signing secret is resolved by
`services/app/webhooks/secrets.py::load_secrets` from the `provider_installations`
row (provider='grafana') the seed/onboarding step registers (webhook_secret_ref).

Unlike GitHub/Jira/Mercury (which prefix `sha256=`), Grafana's header value is the
bare lowercase hex digest. Like them, there is no replay window beyond the optional
timestamp; idempotency is enforced at the ingestion layer via the `external_id`.

VERSION NOTE: HMAC signing requires Grafana 12.0+. Older self-hosted instances
should instead set a static `Authorization: Bearer <secret>` header on the contact
point — supporting that mode is a documented follow-up; v1 verifies HMAC.
"""
from __future__ import annotations

import hashlib
import hmac
import os
from typing import Mapping, Sequence

from services.app.webhooks.verifier import (
    Secret,
    VerifiedContext,
    WebhookVerificationError,
    constant_time_str_eq,
    require_header,
    require_secrets,
)


_SIGNATURE_HEADER = "X-Grafana-Alerting-Signature"


def _timestamp_header_name() -> str | None:
    name = os.environ.get("GRAFANA_WEBHOOK_TIMESTAMP_HEADER", "").strip()
    return name or None


class GrafanaVerifier:
    provider = "grafana"

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
            headers, _SIGNATURE_HEADER, provider=self.provider
        ).strip().lower()

        # Optional timestamp-in-signature mode (contact-point configurable).
        ts_header = _timestamp_header_name()
        signed_ts: str | None = None
        signed_bytes = body
        if ts_header is not None:
            ts_value = headers.get(ts_header) or headers.get(ts_header.lower())
            if ts_value:
                signed_ts = str(ts_value).strip()
                signed_bytes = f"{signed_ts}:".encode("utf-8") + body

        matched: Secret | None = None
        for secret in secrets:
            mac = hmac.new(
                secret.value.encode("utf-8"), signed_bytes, hashlib.sha256,
            )
            expected = mac.hexdigest()
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "grafana signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=signed_ts,
        )


verifier = GrafanaVerifier()


__all__ = ["GrafanaVerifier", "verifier"]
