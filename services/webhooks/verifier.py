"""services/webhooks/verifier.py — Verifier Protocol + error taxonomy.

The Verifier Protocol is the contract every per-provider signature
module satisfies. Adding a new provider (Twilio, Shopify, …) means
adding a new function under `services/webhooks/signatures/` that
satisfies this Protocol and is registered in
`services/webhooks/signatures/__init__.py::VERIFIERS`.

Verification operates on literal bytes — the router captures the
request body before any JSON decode so providers that sign whitespace
or key ordering are not surprised. Constant-time comparison is the
caller's responsibility (the helpers in this module exist so each
verifier can reuse the same primitive).

Failure reasons (per spec FR-005) are a closed set so observability
and operator dashboards can distinguish misconfiguration from active
attack. The reason is carried as a typed field on the exception, not
inferred from the message string.
"""
from __future__ import annotations

import hmac
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Protocol, Sequence

from lib.shared.errors import CompanyOSError


VerificationReason = Literal[
    "missing_signature_header",
    "malformed_signature_header",
    "expired_timestamp",
    "signature_mismatch",
    "secret_not_configured",
    "tenant_not_resolved",
]


@dataclass(frozen=True)
class Secret:
    """A single (provider, tenant) credential.

    Multiple Secret rows may be active simultaneously for the same
    (provider, tenant) during a rotation overlap window (see spec
    User Story 5 / FR-010). The verifier MUST try every active
    secret in turn and accept the first match.

    `tenant_id` may be None for verifiers whose tenant resolution
    happens after the signature check (e.g. when the signed payload
    itself names the tenant). Such verifiers are passed every secret
    that matches the provider and resolve tenant from the verified
    payload.
    """

    provider: str
    value: str
    tenant_id: str | None = None
    label: str | None = None  # opaque tag for rotation observability


@dataclass(frozen=True)
class VerifiedContext:
    """Result returned by a successful Verifier call.

    The router consumes this to drive downstream ingestion.

    Attributes:
        provider: Canonical provider name (`slack`, `github`, …).
        body: The literal bytes that were verified. Re-handed to the
            ingestion pipeline as-is; the pipeline is responsible for
            JSON-decoding into a payload dict.
        secret_label: The `Secret.label` of whichever secret matched —
            None when the verifier did not record a label. Used by
            rotation observability tests to confirm the new secret
            took the request.
        signed_timestamp: The provider-supplied timestamp (Unix
            seconds), if the provider's envelope included one.
            None for providers that do not sign timestamps.
        tenant_hint: When the signed payload names the originating
            tenant (Slack `team_id`, GitHub `installation.id`, …),
            verifiers MAY surface that here so the router can resolve
            tenant without re-parsing the body. Optional; the router
            will fall back to provider-specific tenant resolution
            if absent.
    """

    provider: str
    body: bytes
    secret_label: str | None = None
    signed_timestamp: int | None = None
    tenant_hint: dict[str, Any] = field(default_factory=dict)


class WebhookVerificationError(CompanyOSError):
    """Verification failed. Carries a typed `reason` so callers can
    branch on the failure mode without string-matching the message.

    Conforms to `CompanyOSError.to_dict()` shape: `{code, message,
    context}` with `context['provider']` and `context['reason']`
    always populated.
    """

    default_code = "webhook_verification_failed"

    def __init__(
        self,
        reason: VerificationReason,
        message: str,
        *,
        provider: str,
        **context: Any,
    ) -> None:
        super().__init__(
            message,
            provider=provider,
            reason=reason,
            **context,
        )
        self.reason: VerificationReason = reason
        self.provider = provider


class Verifier(Protocol):
    """Per-provider signature verifier.

    Implementations live under `services/webhooks/signatures/` and
    are registered in `services/webhooks/signatures/__init__.py::VERIFIERS`.

    Contract:

    - MUST raise `WebhookVerificationError` (with a typed `reason`)
      on any failure mode. MUST NOT return a truthy/falsy value to
      indicate failure.
    - MUST use `hmac.compare_digest` (or an equivalent timing-resistant
      compare) when comparing signature material.
    - MUST verify against the literal `body` bytes, not against any
      re-serialized form.
    - MUST honor a per-provider replay window for providers whose
      signed envelope includes a timestamp. Default 300s, configurable
      via the `max_age_s` kwarg.
    - MUST try every secret in `secrets` and accept the first match.
      MUST NOT short-circuit on length mismatch in a way that leaks
      timing about which secret matched.
    - MUST NOT include the body or candidate signature in any log it
      emits (callers may log a structured event from the raised
      exception's context, which is safe by construction).
    """

    provider: str

    async def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        secrets: Sequence[Secret],
        now: float | None = None,
    ) -> VerifiedContext:
        ...


# ---------------------------------------------------------------------
# Shared helpers used by verifier implementations.
# ---------------------------------------------------------------------


def constant_time_str_eq(a: str, b: str) -> bool:
    """Constant-time string comparison via hmac.compare_digest.

    Centralised here so callers do not have to remember the byte
    encoding nuance themselves. Empty inputs return False (the
    underlying compare_digest treats unequal-length inputs as not-
    equal in constant time, which is the desired behavior).
    """
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def constant_time_bytes_eq(a: bytes, b: bytes) -> bool:
    return hmac.compare_digest(a, b)


def require_header(
    headers: Mapping[str, str],
    name: str,
    *,
    provider: str,
) -> str:
    """Fetch a required header or raise the canonical missing-header
    error. Case-insensitive match.
    """
    # Mapping[str, str] from Starlette/FastAPI is case-insensitive in
    # practice (Headers class), but a plain dict in tests is not.
    # Normalize.
    value = headers.get(name)
    if value is None:
        lower = name.lower()
        for k, v in headers.items():
            if k.lower() == lower:
                value = v
                break
    if not value:
        raise WebhookVerificationError(
            "missing_signature_header",
            f"missing {name} header",
            provider=provider,
            header=name,
        )
    return value


def require_secrets(
    secrets: Sequence[Secret],
    *,
    provider: str,
) -> Sequence[Secret]:
    """Reject early when no secret is configured. A 401 with the
    `secret_not_configured` reason distinguishes operator misconfig
    from spoofing in dashboards (per FR-005)."""
    if not secrets:
        raise WebhookVerificationError(
            "secret_not_configured",
            f"no signing secret configured for provider {provider!r}",
            provider=provider,
        )
    return secrets


__all__ = [
    "Verifier",
    "VerificationReason",
    "VerifiedContext",
    "WebhookVerificationError",
    "Secret",
    "constant_time_str_eq",
    "constant_time_bytes_eq",
    "require_header",
    "require_secrets",
]
