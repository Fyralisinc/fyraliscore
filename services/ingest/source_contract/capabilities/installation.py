"""Installation and credential capability interfaces."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import Field

from services.ingest.source_contract.connector import OperationContext
from services.ingest.source_contract.host_services import SecretCandidate
from services.ingest.source_contract.models import ContractModel


class ConfigurationIssue(ContractModel):
    field: str
    code: str
    message: str


class ConfigurationValidation(ContractModel):
    valid: bool
    issues: tuple[ConfigurationIssue, ...] = ()


class OAuthBeginRequest(ContractModel):
    redirect_uri: str
    state: str = Field(min_length=16)
    requested_scopes: tuple[str, ...] = ()


class AuthorizationRedirect(ContractModel):
    url: str
    expires_at_epoch_seconds: int | None = None


class OAuthCompleteRequest(ContractModel):
    code: str = Field(min_length=1)
    redirect_uri: str
    callback_parameters: dict[str, str] = Field(default_factory=dict)


class OAuthResult(ContractModel):
    external_installation_id: str = Field(min_length=1)
    granted_scopes: tuple[str, ...] = ()
    metadata: dict[str, Any] = Field(default_factory=dict)


class OAuthRefreshRequest(ContractModel):
    operation_id: str = Field(min_length=1)
    requested_scopes: tuple[str, ...] = ()


class OAuthRefreshResult(ContractModel):
    granted_scopes: tuple[str, ...] = ()
    rotated: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class OAuthRevokeRequest(ContractModel):
    operation_id: str = Field(min_length=1)
    revoke_remote: bool = True


class OAuthRevocation(ContractModel):
    complete: bool
    remote_revoked: bool = False
    reason_code: str = "complete"


class SecretRotationRequest(ContractModel):
    slot: str
    candidate_handle: str


class SecretRotationVerification(ContractModel):
    valid: bool
    reason_code: str
    message: str = ""


@runtime_checkable
class ConfigurationCapability(Protocol):
    async def validate_configuration(
        self,
        configuration: dict[str, Any],
        context: OperationContext,
    ) -> ConfigurationValidation: ...


@runtime_checkable
class OAuth2Capability(Protocol):
    async def begin(
        self,
        request: OAuthBeginRequest,
        context: OperationContext,
    ) -> AuthorizationRedirect: ...

    async def complete(
        self,
        request: OAuthCompleteRequest,
        context: OperationContext,
    ) -> tuple[OAuthResult, tuple[SecretCandidate, ...]]: ...


@runtime_checkable
class OAuth2LifecycleCapability(Protocol):
    """Additive OAuth lifecycle facet; OAuth2 v1 remains unchanged."""

    async def refresh(
        self,
        request: OAuthRefreshRequest,
        context: OperationContext,
    ) -> tuple[OAuthRefreshResult, tuple[SecretCandidate, ...]]: ...

    async def revoke(
        self,
        request: OAuthRevokeRequest,
        context: OperationContext,
    ) -> OAuthRevocation: ...


@runtime_checkable
class SecretRotationCapability(Protocol):
    async def verify_candidate(
        self,
        request: SecretRotationRequest,
        context: OperationContext,
    ) -> SecretRotationVerification: ...


__all__ = [
    "AuthorizationRedirect",
    "ConfigurationCapability",
    "ConfigurationIssue",
    "ConfigurationValidation",
    "OAuth2Capability",
    "OAuth2LifecycleCapability",
    "OAuthBeginRequest",
    "OAuthCompleteRequest",
    "OAuthResult",
    "OAuthRefreshRequest",
    "OAuthRefreshResult",
    "OAuthRevokeRequest",
    "OAuthRevocation",
    "SecretRotationCapability",
    "SecretRotationRequest",
    "SecretRotationVerification",
]
