"""services/app/webhooks/signatures/figma.py — Figma webhook verifier.

============================================================================
Figma Webhooks V2 authenticate the callback with a PASSCODE-IN-BODY scheme (NOT
an HMAC signature header), CONFIRMED against developers.figma.com/docs/rest-api/
webhooks-security: a shared `passcode` (set at webhook-creation time, max 100
chars) is echoed as a top-level JSON field in every delivered event, and the
receiver verifies by constant-time-comparing `body["passcode"]` to the stored
secret (respond 400 on mismatch). There is NO signature header — the third-party
`figma-signature`/HMAC schemes some blogs describe do NOT exist in the official
docs. `_USE_PASSCODE_IN_BODY = True` selects this real scheme; the legacy
HMAC-header branch is retained only as a fallback for the synthetic gate's older
shape. The synthetic `HmacWebhookGenerator` drives figma by embedding the
passcode in the body (a wrong passcode is its tamper-rejection probe).
============================================================================

The per-tenant secret(s) are resolved by
`services/app/webhooks/secrets.py::load_secrets` from the `provider_installations`
row (provider='figma') the seed/onboarding step registers. The verifier loops
over ALL active secrets so a rotation (two valid secrets in flight) verifies.

Like GitHub/Jira/Brex, the (synthetic) digest is over the body alone (no
timestamp envelope), so there is no replay window here; idempotency is enforced
at the ingestion layer via the versioned `external_id`.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Mapping, Sequence

from services.app.webhooks.verifier import (
    Secret,
    VerifiedContext,
    WebhookVerificationError,
    constant_time_str_eq,
    require_header,
    require_secrets,
)


# The real Figma scheme: a plaintext `passcode` echoed in the JSON body. The body
# field name is CONFIRMED (developers.figma.com).
_PASSCODE_FIELD = "passcode"

# --- Legacy HMAC-header knobs (fallback only; not Figma's real scheme). ---
_HEADER_NAME = "Figma-Signature"   # header carrying the signature
_PREFIX = "sha256="                # prefix on the header value ("" if none)
_DIGEST_ENCODING = "hex"           # "hex" or "base64"

# True = the real passcode-in-body scheme (CONFIRMED). False falls back to the
# legacy HMAC-header path retained for the synthetic gate's older shape.
_USE_PASSCODE_IN_BODY = True


def _encode_digest(mac: "hmac.HMAC") -> str:
    if _DIGEST_ENCODING == "base64":
        return base64.b64encode(mac.digest()).decode("ascii")
    return mac.hexdigest()


class FigmaVerifier:
    provider = "figma"

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        secrets: Sequence[Secret],
        now: float | None = None,
    ) -> VerifiedContext:
        require_secrets(secrets, provider=self.provider)

        # Real Figma scheme: a plaintext passcode echoed in the JSON body; no
        # signature header. Constant-time-compare against each active secret
        # (rotation-safe), respond 400-class (WebhookVerificationError) on
        # mismatch — mirrors developers.figma.com's recommended verification.
        if _USE_PASSCODE_IN_BODY:
            try:
                parsed = json.loads(body or b"{}")
            except (ValueError, TypeError) as exc:
                raise WebhookVerificationError(
                    "malformed_body",
                    "figma webhook body is not valid JSON",
                    provider=self.provider,
                ) from exc
            presented = (
                parsed.get(_PASSCODE_FIELD) if isinstance(parsed, dict) else None
            )
            if not isinstance(presented, str) or not presented:
                raise WebhookVerificationError(
                    "missing_passcode",
                    f"figma webhook body missing '{_PASSCODE_FIELD}'",
                    provider=self.provider,
                )
            matched_pc: Secret | None = None
            for secret in secrets:
                if constant_time_str_eq(secret.value, presented):
                    matched_pc = secret
                    break
            if matched_pc is None:
                raise WebhookVerificationError(
                    "passcode_mismatch",
                    "figma passcode does not match any active secret",
                    provider=self.provider,
                )
            return VerifiedContext(
                provider=self.provider,
                body=body,
                secret_label=matched_pc.label,
                signed_timestamp=None,
            )

        # Legacy HMAC-header fallback (NOT Figma's real scheme).
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
            mac = hmac.new(secret.value.encode("utf-8"), body, hashlib.sha256)
            expected = _PREFIX + _encode_digest(mac)
            if constant_time_str_eq(expected, signature):
                matched = secret
                break

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "figma signature does not match any active secret",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=None,
        )


verifier = FigmaVerifier()


__all__ = ["FigmaVerifier", "verifier"]
