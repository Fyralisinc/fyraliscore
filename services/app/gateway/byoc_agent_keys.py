"""BYOC data-plane agent install-token resolution."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from fastapi.datastructures import State

from lib.shared.errors import SecretStoreError
from lib.shared.secrets.provider_contract import load_secret_text_from_config
from services.app.gateway.settings import GatewaySettings


@dataclass(frozen=True, slots=True)
class ResolvedByocAgentInstallToken:
    key_ref: str
    secret: str = field(repr=False)
    source: str
    secret_ref: str | None = None


class ByocAgentInstallTokenResolver(Protocol):
    async def resolve(
        self,
        *,
        key_ref: str,
    ) -> ResolvedByocAgentInstallToken | None:
        """Return install-token material for an allowed key reference."""
        ...


@dataclass(frozen=True, slots=True)
class StaticByocAgentInstallTokenResolver:
    """Small local/test resolver for injected app-state token material."""

    install_token_secret_ref: str
    install_token: str = field(repr=False)

    async def resolve(
        self,
        *,
        key_ref: str,
    ) -> ResolvedByocAgentInstallToken | None:
        if key_ref != self.install_token_secret_ref:
            return None
        return ResolvedByocAgentInstallToken(
            key_ref=self.install_token_secret_ref,
            secret=self.install_token,
            source="static_test_secret",
            secret_ref=self.install_token_secret_ref,
        )


@dataclass(frozen=True, slots=True)
class ManagedByocAgentInstallTokenResolver:
    """Resolve BYOC agent install tokens through managed secret references."""

    install_token_secret_ref: str

    async def resolve(
        self,
        *,
        key_ref: str,
    ) -> ResolvedByocAgentInstallToken | None:
        if key_ref != self.install_token_secret_ref:
            return None
        secret = load_secret_text_from_config(self.install_token_secret_ref).strip()
        if not secret:
            raise SecretStoreError(
                "FYRALIS_DATA_PLANE_AGENT_INSTALL_TOKEN_SECRET_REF did not "
                "resolve install-token material",
                reason="missing_byoc_agent_install_token",
            )
        return ResolvedByocAgentInstallToken(
            key_ref=self.install_token_secret_ref,
            secret=secret,
            source="managed_app_secret",
            secret_ref=self.install_token_secret_ref,
        )


def resolver_from_gateway_settings(
    settings: GatewaySettings,
) -> ManagedByocAgentInstallTokenResolver | None:
    if not settings.data_plane_agent_install_token_secret_ref:
        return None
    return ManagedByocAgentInstallTokenResolver(
        install_token_secret_ref=settings.data_plane_agent_install_token_secret_ref,
    )


def resolver_from_legacy_app_state(
    state: State,
    *,
    production: bool,
) -> StaticByocAgentInstallTokenResolver | None:
    if production:
        return None
    install_token = getattr(state, "byoc_agent_install_token", None)
    secret_ref = getattr(state, "byoc_agent_install_token_secret_ref", None)
    if not install_token or not secret_ref:
        return None
    return StaticByocAgentInstallTokenResolver(
        install_token_secret_ref=str(secret_ref),
        install_token=str(install_token),
    )


def resolver_from_app_state(
    state: State,
) -> ByocAgentInstallTokenResolver | None:
    existing = getattr(state, "byoc_agent_install_token_resolver", None)
    if existing is not None:
        return existing
    settings = getattr(state, "gateway_settings", None)
    production = bool(getattr(settings, "is_production", False))
    if isinstance(settings, GatewaySettings):
        managed = resolver_from_gateway_settings(settings)
        if managed is not None:
            state.byoc_agent_install_token_resolver = managed
            return managed
    legacy = resolver_from_legacy_app_state(state, production=production)
    if legacy is not None:
        state.byoc_agent_install_token_resolver = legacy
        return legacy
    return None


__all__ = [
    "ByocAgentInstallTokenResolver",
    "ManagedByocAgentInstallTokenResolver",
    "ResolvedByocAgentInstallToken",
    "StaticByocAgentInstallTokenResolver",
    "resolver_from_app_state",
    "resolver_from_gateway_settings",
    "resolver_from_legacy_app_state",
]
