"""services/app/webhooks/signatures/figma.py — Figma webhook verifier.

============================================================================
IMPORTANT DIVERGENCE — real Figma uses a PASSCODE-IN-BODY scheme, NOT an HMAC
header. This module implements an HMAC-SHA256 HEADER verifier (Brex-shaped) so
the synthetic gate's shared ``HmacWebhookGenerator`` can drive it and its
tamper-rejection probe passes uniformly with every other HMAC provider.

# TODO(human): real Figma uses a passcode-in-body verifier, not an HMAC header —
# reconcile before production. Figma Webhooks V2 authenticate the callback by
# embedding a shared PASSCODE (set at webhook-creation time) inside the request
# JSON body (`body["passcode"]`); there is NO signature header. The production
# verifier must JSON-parse the body, constant-time-compare `body["passcode"]`
# against the per-tenant secret, and ignore the header entirely. Flip
# `_USE_PASSCODE_IN_BODY` to True (and implement the body branch below) once the
# real scheme is wired; the HMAC-header path stays for the synthetic gate.
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
from typing import Mapping, Sequence

from services.app.webhooks.verifier import (
    Secret,
    VerifiedContext,
    WebhookVerificationError,
    constant_time_str_eq,
    require_header,
    require_secrets,
)


# --- Synthetic-gate HMAC scheme knobs (Brex archetype defaults). ---
# TODO(human): these only apply to the HMAC-header stand-in; the real scheme is
# passcode-in-body (see the module header).
_HEADER_NAME = "Figma-Signature"   # header carrying the signature
_PREFIX = "sha256="                # prefix on the header value ("" if none)
_DIGEST_ENCODING = "hex"           # "hex" or "base64"

# Configurable switch for the real scheme. Default False keeps the HMAC-header
# path the synthetic gate drives; set True once the passcode-in-body branch is
# implemented and wired against live Figma payloads.
_USE_PASSCODE_IN_BODY = False


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

        # TODO(human): real Figma path — when _USE_PASSCODE_IN_BODY is True,
        # JSON-parse `body`, read `body["passcode"]`, and constant-time-compare
        # against each secret value (no header). Not implemented yet so the
        # synthetic gate's HMAC-header tamper probe stays the source of truth.
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
