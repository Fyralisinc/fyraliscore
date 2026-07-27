"""services/app/webhooks/signatures/ashby.py — Ashby HMAC webhook verifier.

Ashby signs webhooks with HMAC over the RAW unparsed request body. The scheme is
CONFIRMED from Ashby's first-party webhook docs:

  - algorithm: HMAC-SHA256
  - digest encoding: lowercase HEX
  - header: ``Ashby-Signature``
  - format: ``sha256=<hex>`` (GitHub-style prefix)

The digest is computed over the body bytes EXACTLY as received (no re-serialize),
so verification must run before any JSON parse — the router passes the raw body
through unchanged.

The per-tenant signing secret(s) are resolved by
`services/app/webhooks/secrets.py::load_installation_secrets` from the `provider_installations`
row (provider='ashby') the seed/onboarding step registers. The verifier loops
over ALL active secrets so a rotation (two valid secrets in flight) verifies.

Like GitHub/Brex, the digest is over the body alone (no timestamp envelope), so
there is no replay window here; idempotency is enforced at the ingestion layer
via the `external_id`.
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


# --- CONFIRMED Ashby scheme (first-party webhook docs). ---
_HEADER_NAME = "Ashby-Signature"   # header carrying the signature
_PREFIX = "sha256="                # prefix on the header value
_DIGEST_ENCODING = "hex"           # "hex" or "base64"


def _encode_digest(mac: "hmac.HMAC") -> str:
    if _DIGEST_ENCODING == "base64":
        return base64.b64encode(mac.digest()).decode("ascii")
    return mac.hexdigest()


class AshbyVerifier:
    provider = "ashby"

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
            headers, _HEADER_NAME, provider=self.provider
        )
        if _PREFIX and not signature.startswith(_PREFIX):
            raise WebhookVerificationError(
                "malformed_signature_header",
                f"{_HEADER_NAME} must be prefixed with {_PREFIX!r}",
                provider=self.provider,
            )

        matched: Secret | None = None
        for secret in secrets:
            # HMAC-SHA256 over the RAW body bytes (no re-serialize).
            mac = hmac.new(secret.value.encode("utf-8"), body, hashlib.sha256)
            expected = _PREFIX + _encode_digest(mac)
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "ashby signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = AshbyVerifier()


__all__ = ["AshbyVerifier", "verifier"]
