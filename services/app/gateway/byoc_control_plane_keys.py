"""BYOC control-plane signing key resolution."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol

from fastapi.datastructures import State

from lib.shared.errors import SecretStoreError
from lib.shared.secrets import load_app_secret_text_from_env
from services.app.gateway.settings import GatewaySettings


ByocControlPlaneKeyPurpose = Literal["evidence_package_submission", "receipt_read"]


@dataclass(frozen=True, slots=True)
class ResolvedByocControlPlaneKey:
    key_ref: str
    secret: str = field(repr=False)
    source: Literal["managed_app_secret", "static_test_secret"]
    secret_ref: str | None = None


class ByocControlPlaneSigningKeyResolver(Protocol):
    async def resolve(
        self,
        *,
        purpose: ByocControlPlaneKeyPurpose,
        key_ref: str,
    ) -> ResolvedByocControlPlaneKey | None:
        """Return signing material for an allowed key reference."""
        ...


@dataclass(frozen=True, slots=True)
class StaticByocControlPlaneSigningKeyResolver:
    """Small local/test resolver for injected app-state secrets."""

    intake_key_ref: str
    intake_secret: str = field(repr=False)
    read_key_ref: str
    read_secret: str = field(repr=False)

    async def resolve(
        self,
        *,
        purpose: ByocControlPlaneKeyPurpose,
        key_ref: str,
    ) -> ResolvedByocControlPlaneKey | None:
        if purpose == "evidence_package_submission":
            expected_key_ref = self.intake_key_ref
            secret = self.intake_secret
        else:
            expected_key_ref = self.read_key_ref
            secret = self.read_secret
        if key_ref != expected_key_ref:
            return None
        return ResolvedByocControlPlaneKey(
            key_ref=expected_key_ref,
            secret=secret,
            source="static_test_secret",
        )


@dataclass(frozen=True, slots=True)
class ManagedEnvByocControlPlaneSigningKeyResolver:
    """Resolve BYOC signing keys through managed app-secret references.

    The provider lookup reuses the app-secret provider contract used by other
    gateway app-level secrets. Reads are deliberately uncached so rotating the
    value behind a stable managed secret reference takes effect without a
    process restart.
    """

    intake_key_ref: str
    read_key_ref: str
    production: bool
    intake_secret_ref: str | None = None
    read_secret_ref: str | None = None

    async def resolve(
        self,
        *,
        purpose: ByocControlPlaneKeyPurpose,
        key_ref: str,
    ) -> ResolvedByocControlPlaneKey | None:
        if purpose == "evidence_package_submission":
            expected_key_ref = self.intake_key_ref
            secret_name = "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY"
            secret_ref = self.intake_secret_ref
        else:
            expected_key_ref = self.read_key_ref
            secret_name = "FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY"
            secret_ref = self.read_secret_ref
        if key_ref != expected_key_ref:
            return None
        secret = load_app_secret_text_from_env(
            secret_name,
            production=self.production,
        ).strip()
        if not secret:
            raise SecretStoreError(
                f"{secret_name}_SECRET_REF did not resolve signing material",
                reason="missing_byoc_control_plane_key",
            )
        return ResolvedByocControlPlaneKey(
            key_ref=expected_key_ref,
            secret=secret,
            source="managed_app_secret",
            secret_ref=secret_ref,
        )


def resolver_from_gateway_settings(
    settings: GatewaySettings,
) -> ManagedEnvByocControlPlaneSigningKeyResolver | None:
    if (
        not settings.byoc_evidence_intake_key_ref
        or not settings.byoc_evidence_read_key_ref
    ):
        return None
    return ManagedEnvByocControlPlaneSigningKeyResolver(
        intake_key_ref=settings.byoc_evidence_intake_key_ref,
        read_key_ref=settings.byoc_evidence_read_key_ref,
        production=settings.is_production,
        intake_secret_ref=settings.byoc_evidence_intake_signing_key_secret_ref,
        read_secret_ref=settings.byoc_evidence_read_signing_key_secret_ref,
    )


def resolver_from_legacy_app_state(
    state: State,
    *,
    production: bool,
) -> StaticByocControlPlaneSigningKeyResolver | None:
    if production:
        return None
    intake_secret = getattr(state, "byoc_evidence_intake_secret", None)
    intake_key_ref = getattr(state, "byoc_evidence_intake_key_ref", None)
    read_secret = getattr(state, "byoc_evidence_read_secret", None) or intake_secret
    read_key_ref = getattr(state, "byoc_evidence_read_key_ref", None) or intake_key_ref
    if not intake_secret or not intake_key_ref or not read_secret or not read_key_ref:
        return None
    return StaticByocControlPlaneSigningKeyResolver(
        intake_key_ref=str(intake_key_ref),
        intake_secret=str(intake_secret),
        read_key_ref=str(read_key_ref),
        read_secret=str(read_secret),
    )


def resolver_from_app_state(
    state: State,
) -> ByocControlPlaneSigningKeyResolver | None:
    existing = getattr(state, "byoc_control_plane_key_resolver", None)
    if existing is not None:
        return existing
    settings = getattr(state, "gateway_settings", None)
    production = bool(getattr(settings, "is_production", False))
    if isinstance(settings, GatewaySettings):
        managed = resolver_from_gateway_settings(settings)
        if managed is not None:
            state.byoc_control_plane_key_resolver = managed
            return managed
    legacy = resolver_from_legacy_app_state(state, production=production)
    if legacy is not None:
        state.byoc_control_plane_key_resolver = legacy
        return legacy
    return None


__all__ = [
    "ByocControlPlaneKeyPurpose",
    "ByocControlPlaneSigningKeyResolver",
    "ManagedEnvByocControlPlaneSigningKeyResolver",
    "ResolvedByocControlPlaneKey",
    "StaticByocControlPlaneSigningKeyResolver",
    "resolver_from_app_state",
    "resolver_from_gateway_settings",
    "resolver_from_legacy_app_state",
]
