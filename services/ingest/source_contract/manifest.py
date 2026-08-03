"""Declarative, side-effect-free Source Connector manifest model."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from services.ingest.source_contract.identity import (
    CapabilityId,
    ConnectorId,
    SlotId,
    SourceId,
)
from services.ingest.source_contract.versioning import SemanticVersion, VersionRange


MANIFEST_API_VERSION = "sources.fyralis.io/v1alpha1"
MANIFEST_KIND = "SourceConnector"


class ManifestModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=None,
        extra="forbid",
        frozen=True,
        populate_by_name=True,
    )


class Maturity(StrEnum):
    EXPERIMENTAL = "experimental"
    PREVIEW = "preview"
    STABLE = "stable"
    DEPRECATED = "deprecated"


class IsolationMode(StrEnum):
    IN_PROCESS_TRUSTED = "in_process_trusted"
    RPC_ISOLATED = "rpc_isolated"


class CapabilityRef(ManifestModel):
    id: CapabilityId
    version: int = Field(ge=1)

    def __hash__(self) -> int:
        return hash((self.id, self.version))


class CapabilityConstraint(ManifestModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]{0,62}$")
    value: str | int | float | bool


class CapabilityDeclaration(ManifestModel):
    id: CapabilityId
    version: int = Field(ge=1)
    required: bool = True
    maturity: Maturity = Maturity.STABLE
    constraints: tuple[CapabilityConstraint, ...] = ()

    @property
    def ref(self) -> CapabilityRef:
        return CapabilityRef(id=self.id, version=self.version)

    @model_validator(mode="after")
    def unique_constraints(self) -> "CapabilityDeclaration":
        names = [constraint.name for constraint in self.constraints]
        if len(names) != len(set(names)):
            raise ValueError("capability constraint names must be unique")
        return self


class ConnectorMetadata(ManifestModel):
    id: ConnectorId
    source: SourceId
    display_name: str = Field(alias="displayName", min_length=1, max_length=120)
    version: str
    owner: str = Field(min_length=1, max_length=120)

    @field_validator("version")
    @classmethod
    def valid_semantic_version(cls, value: str) -> str:
        SemanticVersion.parse(value)
        return value

    @property
    def semantic_version(self) -> SemanticVersion:
        return SemanticVersion.parse(self.version)


class PermissionRequest(ManifestModel):
    secret_slots: tuple[SlotId, ...] = Field(default=(), alias="secretSlots")
    outbound_hosts: tuple[str, ...] = Field(default=(), alias="outboundHosts")
    requested_scopes: tuple[str, ...] = Field(default=(), alias="requestedScopes")

    @field_validator("outbound_hosts")
    @classmethod
    def valid_hosts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for host in value:
            if (
                not host
                or "://" in host
                or "/" in host
                or host.startswith(".")
                or host.endswith(".")
            ):
                raise ValueError(f"outbound host must be a bare DNS name: {host!r}")
        if len(value) != len(set(value)):
            raise ValueError("outbound hosts must be unique")
        return value

    @model_validator(mode="after")
    def unique_values(self) -> "PermissionRequest":
        if len(self.secret_slots) != len(set(self.secret_slots)):
            raise ValueError("secret slots must be unique")
        if len(self.requested_scopes) != len(set(self.requested_scopes)):
            raise ValueError("requested scopes must be unique")
        return self


class TrustDeclaration(ManifestModel):
    maximum_tier: str = Field(alias="maximumTier", min_length=1)


class RuntimeProfile(ManifestModel):
    isolation: IsolationMode = IsolationMode.IN_PROCESS_TRUSTED
    network_profile: str | None = Field(default=None, alias="networkProfile")
    resource_class: str = Field(default="io_standard", alias="resourceClass")


class ConnectorSpec(ManifestModel):
    contract: str
    implementation: str = Field(
        pattern=r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*$"
    )
    maturity: Maturity = Maturity.PREVIEW
    capabilities: tuple[CapabilityDeclaration, ...]
    ingress_kinds: tuple[
        Literal["webhook", "gateway", "pubsub", "backfill", "poll"], ...
    ] = Field(default=(), alias="ingressKinds")
    permissions: PermissionRequest = Field(default_factory=PermissionRequest)
    trust: TrustDeclaration
    runtime: RuntimeProfile = Field(default_factory=RuntimeProfile)

    @field_validator("contract")
    @classmethod
    def valid_contract_range(cls, value: str) -> str:
        VersionRange.parse(value)
        return value

    @model_validator(mode="after")
    def unique_declarations(self) -> "ConnectorSpec":
        refs = [declaration.ref for declaration in self.capabilities]
        if len(refs) != len(set(refs)):
            raise ValueError("capability declarations must be unique")
        if len(self.ingress_kinds) != len(set(self.ingress_kinds)):
            raise ValueError("ingress kinds must be unique")
        return self

    @property
    def contract_range(self) -> VersionRange:
        return VersionRange.parse(self.contract)


class ConnectorManifest(ManifestModel):
    api_version: Literal["sources.fyralis.io/v1alpha1"] = Field(
        alias="apiVersion"
    )
    kind: Literal["SourceConnector"]
    metadata: ConnectorMetadata
    spec: ConnectorSpec

    @property
    def connector_id(self) -> str:
        return self.metadata.id

    @property
    def source(self) -> str:
        return self.metadata.source

    @property
    def capability_refs(self) -> tuple[CapabilityRef, ...]:
        return tuple(declaration.ref for declaration in self.spec.capabilities)


__all__ = [
    "CapabilityConstraint",
    "CapabilityDeclaration",
    "CapabilityRef",
    "ConnectorManifest",
    "ConnectorMetadata",
    "ConnectorSpec",
    "IsolationMode",
    "MANIFEST_API_VERSION",
    "MANIFEST_KIND",
    "ManifestModel",
    "Maturity",
    "PermissionRequest",
    "RuntimeProfile",
    "TrustDeclaration",
]
