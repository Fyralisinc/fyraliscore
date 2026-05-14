"""services/webhooks/signatures/discord.py — Discord ed25519 verifier.

Discord signs interaction payloads with ed25519 over
`X-Signature-Timestamp || body` and presents the hex-encoded
signature in `X-Signature-Ed25519`. The "secret" here is the
application's PUBLIC key (Discord publishes a public key per app and
signs with the matching private key), so verification is asymmetric.

We use `pynacl` (NaCl bindings) for the ed25519 primitive. Constant-
time verification is provided by libsodium under the hood; pynacl's
`VerifyKey.verify` either returns the message bytes on success or
raises `BadSignatureError` on failure.

Replay window: Discord does not document a hard window but does sign
a timestamp. We enforce a 300s default window so a captured request
cannot be replayed against the application after the fact.

Header reference:
    https://discord.com/developers/docs/interactions/receiving-and-responding#security-and-authorization
"""
from __future__ import annotations

import os
import time
from typing import Mapping, Sequence

from services.webhooks.verifier import (
    Secret,
    VerifiedContext,
    WebhookVerificationError,
    require_header,
    require_secrets,
)


_DEFAULT_MAX_AGE_S = int(os.environ.get("DISCORD_MAX_TIMESTAMP_AGE_S", "300"))


def _import_nacl() -> tuple[type, type]:
    """Import pynacl lazily so the module is importable without it
    installed (the rest of the webhook stack should not fail to load
    just because Discord isn't configured in this deployment)."""
    try:
        from nacl.exceptions import BadSignatureError
        from nacl.signing import VerifyKey
    except ImportError as e:  # pragma: no cover — env without pynacl
        raise WebhookVerificationError(
            "secret_not_configured",
            "pynacl is not installed; cannot verify Discord signatures",
            provider="discord",
        ) from e
    return VerifyKey, BadSignatureError


class DiscordVerifier:
    provider = "discord"

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
            headers, "X-Signature-Ed25519", provider=self.provider
        )
        timestamp = require_header(
            headers, "X-Signature-Timestamp", provider=self.provider
        )

        try:
            ts_int = int(timestamp)
        except ValueError:
            raise WebhookVerificationError(
                "malformed_signature_header",
                f"X-Signature-Timestamp not integer: {timestamp!r}",
                provider=self.provider,
            )

        now_s = int(now if now is not None else time.time())
        if abs(now_s - ts_int) > _DEFAULT_MAX_AGE_S:
            raise WebhookVerificationError(
                "expired_timestamp",
                "discord signature timestamp outside replay window",
                provider=self.provider,
                max_age_s=_DEFAULT_MAX_AGE_S,
            )

        try:
            sig_bytes = bytes.fromhex(signature)
        except ValueError:
            raise WebhookVerificationError(
                "malformed_signature_header",
                "X-Signature-Ed25519 is not valid hex",
                provider=self.provider,
            )

        VerifyKey, BadSignatureError = _import_nacl()
        message = timestamp.encode("utf-8") + body

        matched: Secret | None = None
        for secret in secrets:
            try:
                key_bytes = bytes.fromhex(secret.value)
            except ValueError:
                # An operator-configured key that isn't hex is a
                # configuration error, not an attack. We continue
                # trying other secrets and, if none match, raise
                # signature_mismatch. (We do NOT raise here because
                # that would short-circuit the rotation overlap.)
                continue
            try:
                VerifyKey(key_bytes).verify(message, sig_bytes)
                matched = secret
                break
            except BadSignatureError:
                continue
            except Exception:
                # Length mismatch, etc. — treat as no-match for this
                # secret and continue.
                continue

        if matched is None:
            raise WebhookVerificationError(
                "signature_mismatch",
                "discord signature does not verify against any active public key",
                provider=self.provider,
            )

        return VerifiedContext(
            provider=self.provider,
            body=body,
            secret_label=matched.label,
            signed_timestamp=ts_int,
        )


verifier = DiscordVerifier()


__all__ = ["DiscordVerifier", "verifier"]
