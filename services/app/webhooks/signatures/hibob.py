"""services/app/webhooks/signatures/hibob.py — HiBob ("Bob") HMAC webhook verifier.

HiBob signs webhook deliveries with an HMAC over the raw request body. The
CONFIRMED scheme (from the CONTRACT / first-party docs):

  - algorithm : HMAC-**SHA512**
  - digest    : **base64**-encoded (NOT hex)
  - header    : ``Bob-Signature``
  - prefix    : NONE (the header value is the bare base64 digest, no "sha512=")

The three scheme knobs are exposed as module constants (`_HEADER_NAME`,
`_PREFIX`, `_DIGEST_ENCODING`) plus the hash factory `_HASH` so the surface
matches the Brex template this was cloned from.

The per-tenant signing secret(s) are resolved by
`services/app/webhooks/secrets.py::load_installation_secrets` from the `provider_installations`
row (provider='hibob') the seed/onboarding step registers. The verifier loops
over ALL active secrets so a rotation (two valid secrets in flight) verifies.

Like GitHub/Brex, the digest is over the body alone (no timestamp envelope), so
there is no replay window here; idempotency is enforced at the ingestion layer
via the versioned `external_id`.
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


# --- CONFIRMED scheme knobs (per the CONTRACT / first-party docs). ---
_HEADER_NAME = "Bob-Signature"   # header carrying the signature
_PREFIX = ""                     # bare digest — no "sha512=" prefix
_DIGEST_ENCODING = "base64"      # "hex" or "base64"
_HASH = hashlib.sha512           # HMAC-SHA512


def _encode_digest(mac: "hmac.HMAC") -> str:
    if _DIGEST_ENCODING == "base64":
        return base64.b64encode(mac.digest()).decode("ascii")
    return mac.hexdigest()


class HibobVerifier:
    provider = "hibob"

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
            mac = hmac.new(secret.value.encode("utf-8"), body, _HASH)
            expected = _PREFIX + _encode_digest(mac)
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "hibob signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = HibobVerifier()


__all__ = ["HibobVerifier", "verifier"]
