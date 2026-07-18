"""Pure contracts for one exact accepted-memory learning transaction."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .kernel import canonical_sha256
from .truth_admission import AdmitModelCommand, ModelTruthLifecycle, TruthCandidateKind
from .truth_evidence import TruthEvidenceKind, TruthEvidenceReference, TruthEvidenceRole


class _LearningContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, str_strip_whitespace=True, use_enum_values=False,
    )


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _sorted_unique(values: tuple[UUID, ...], *, field_name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must be unique")
    if values != tuple(sorted(values, key=str)):
        raise ValueError(f"{field_name} must be deterministically sorted")


class AcceptedHeadRef(_LearningContract):
    tenant_id: UUID
    model_id: UUID
    version_id: UUID
    version: int = Field(ge=1)
    semantic_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    lifecycle: ModelTruthLifecycle
    canonical_scope_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def is_current_accepted_and_deterministic(self) -> Self:
        if self.lifecycle is not ModelTruthLifecycle.ACTIVE:
            raise ValueError("accepted snapshot Model heads must be active")
        if len(self.canonical_scope_refs) != len(set(self.canonical_scope_refs)):
            raise ValueError("canonical scope refs must be unique")
        if self.canonical_scope_refs != tuple(sorted(self.canonical_scope_refs)):
            raise ValueError("canonical scope refs must be deterministically sorted")
        if any(":" not in value or value.startswith("batch:") for value in self.canonical_scope_refs):
            raise ValueError("canonical scope refs must be typed non-batch coordinates")
        return self


class AcceptedRelationHeadRef(_LearningContract):
    tenant_id: UUID
    relation_id: UUID
    relation_version_id: UUID
    version: int = Field(ge=1)
    semantic_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    lifecycle: Literal["active"] = "active"


class AcceptedMemorySnapshot(_LearningContract):
    snapshot_id: UUID
    tenant_id: UUID
    cutoff_at: datetime
    model_heads: tuple[AcceptedHeadRef, ...] = ()
    relation_heads: tuple[AcceptedRelationHeadRef, ...] = ()
    retrieval_receipt_ids: tuple[UUID, ...] = ()

    @field_validator("cutoff_at")
    @classmethod
    def cutoff_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="cutoff_at")

    @model_validator(mode="after")
    def is_tenant_scoped_and_deterministic(self) -> Self:
        if any(item.tenant_id != self.tenant_id for item in (*self.model_heads, *self.relation_heads)):
            raise ValueError("accepted memory snapshot crosses tenant boundaries")
        model_ids = tuple(item.model_id for item in self.model_heads)
        relation_ids = tuple(item.relation_id for item in self.relation_heads)
        _sorted_unique(model_ids, field_name="snapshot Model heads")
        _sorted_unique(relation_ids, field_name="snapshot relation heads")
        _sorted_unique(self.retrieval_receipt_ids, field_name="retrieval receipt IDs")
        return self

    @property
    def snapshot_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class EvidenceManifest(_LearningContract):
    tenant_id: UUID
    cutoff_at: datetime
    direct: tuple[TruthEvidenceReference, ...] = ()
    model_derivation: tuple[TruthEvidenceReference, ...] = ()
    contradiction: tuple[TruthEvidenceReference, ...] = ()
    relation: tuple[TruthEvidenceReference, ...] = ()
    grounding: tuple[TruthEvidenceReference, ...] = ()
    auxiliary: tuple[TruthEvidenceReference, ...] = ()

    @field_validator("cutoff_at")
    @classmethod
    def cutoff_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="cutoff_at")

    def _partitions(self) -> tuple[tuple[str, tuple[TruthEvidenceReference, ...]], ...]:
        return tuple((name, getattr(self, name)) for name in (
            "direct", "model_derivation", "contradiction", "relation", "grounding", "auxiliary",
        ))

    @model_validator(mode="after")
    def roles_are_disjoint_tenant_scoped_and_deterministic(self) -> Self:
        allowed = {
            "direct": {TruthEvidenceRole.SUPPORT},
            "model_derivation": {TruthEvidenceRole.DERIVATION},
            "contradiction": {TruthEvidenceRole.COUNTEREVIDENCE},
            "relation": {TruthEvidenceRole.SUPPORT, TruthEvidenceRole.DERIVATION},
            "grounding": {TruthEvidenceRole.AUTHORITY},
            "auxiliary": {TruthEvidenceRole.CONTEXT},
        }
        ids: list[UUID] = []
        for name, items in self._partitions():
            item_ids = tuple(item.reference_id for item in items)
            _sorted_unique(item_ids, field_name=f"{name} evidence")
            ids.extend(item_ids)
            if any(item.tenant_id != self.tenant_id for item in items):
                raise ValueError("evidence manifest crosses tenant boundaries")
            if any(item.cutoff_at != self.cutoff_at for item in items):
                raise ValueError("evidence manifest items must bind the exact manifest cutoff")
            if any(item.role not in allowed[name] for item in items):
                raise ValueError(f"{name} evidence has an incompatible role")
        if len(ids) != len(set(ids)):
            raise ValueError("evidence references cannot appear in multiple partitions")
        if any(item.kind is not TruthEvidenceKind.OBSERVATION for item in self.direct):
            raise ValueError("direct evidence must reference observations")
        if any(item.kind is not TruthEvidenceKind.MODEL_VERSION for item in self.model_derivation):
            raise ValueError("model derivation evidence must reference Model versions")
        return self

    @property
    def canonical_evidence(self) -> tuple[TruthEvidenceReference, ...]:
        return (*self.direct, *self.model_derivation, *self.contradiction, *self.relation, *self.grounding)

    @property
    def manifest_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class CompositeRelationTemplate(_LearningContract):
    relation_id: UUID
    relation_kind: Literal[
        "causal_influence", "dependency_constraint", "enablement", "predictive_indicator",
    ]
    source_model_id: UUID
    source_model_version_id: UUID
    target_model_id: UUID
    target_model_version_id: UUID
    evidence_reference_ids: tuple[UUID, ...] = Field(min_length=1)
    rationale: str = Field(min_length=1)
    mechanism: str = Field(min_length=3)

    @model_validator(mode="after")
    def is_distinct_and_deterministic(self) -> Self:
        if self.source_model_id == self.target_model_id:
            raise ValueError("relation endpoints must be distinct")
        _sorted_unique(self.evidence_reference_ids, field_name="relation evidence references")
        return self

    @property
    def template_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class AdmitCompositeRelationCommand(_LearningContract):
    command_id: UUID
    idempotency_key: str = Field(min_length=1)
    tenant_id: UUID
    snapshot: AcceptedMemorySnapshot
    evidence_manifest: EvidenceManifest
    composite: AdmitModelCommand
    relation: CompositeRelationTemplate
    expected_member_heads: tuple[AcceptedHeadRef, ...] = Field(min_length=2)
    issued_at: datetime

    @field_validator("issued_at")
    @classmethod
    def issued_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="issued_at")

    def _identity_body(self) -> dict[str, object]:
        return {
            "command_id": str(self.command_id),
            "tenant_id": str(self.tenant_id),
            "snapshot_digest": self.snapshot.snapshot_digest,
            "evidence_manifest_digest": self.evidence_manifest.manifest_digest,
            "composite_request_digest": self.composite.request_digest,
            "relation_template_digest": self.relation.template_digest,
            "expected_member_heads": [item.model_dump(mode="json") for item in self.expected_member_heads],
        }

    @property
    def idempotency_digest(self) -> str:
        return canonical_sha256(self._identity_body())

    @property
    def expected_idempotency_key(self) -> str:
        return f"composite-relation:v1:{self.tenant_id}:{self.idempotency_digest}"

    @model_validator(mode="after")
    def binds_one_exact_atomic_transition(self) -> Self:
        if any(value != self.tenant_id for value in (
            self.snapshot.tenant_id, self.evidence_manifest.tenant_id, self.composite.tenant_id,
        )):
            raise ValueError("composite relation command crosses tenant boundaries")
        if self.composite.candidate.kind is not TruthCandidateKind.SYNTHESIS:
            raise ValueError("composite relation command requires a synthesis admission")
        member_ids = tuple(item.model_id for item in self.expected_member_heads)
        _sorted_unique(member_ids, field_name="expected member heads")
        snapshot_heads = {item.model_id: item for item in self.snapshot.model_heads}
        if any(snapshot_heads.get(item.model_id) != item for item in self.expected_member_heads):
            raise ValueError("expected member heads must equal exact snapshot heads")
        endpoints = {
            self.relation.source_model_id: self.relation.source_model_version_id,
            self.relation.target_model_id: self.relation.target_model_version_id,
        }
        if any(snapshot_heads.get(model_id) is None or snapshot_heads[model_id].version_id != version_id
               for model_id, version_id in endpoints.items()):
            raise ValueError("relation endpoints must bind exact expected snapshot versions")
        if not set(endpoints) <= set(member_ids):
            raise ValueError("relation endpoints must be declared composite members")
        if set(self.composite.candidate.supporting_model_ids) != set(member_ids):
            raise ValueError("composite supporting Models must equal expected member heads")
        derivation_versions = {item.evidence_id for item in self.evidence_manifest.model_derivation}
        if derivation_versions != {str(item.version_id) for item in self.expected_member_heads}:
            raise ValueError("Model derivation evidence must bind every exact member version")
        composite_evidence_ids = {
            item.reference_id for item in self.composite.candidate.proposed_evidence
        }
        canonical_manifest_ids = {
            item.reference_id for item in self.evidence_manifest.canonical_evidence
        }
        if not composite_evidence_ids <= canonical_manifest_ids:
            raise ValueError("composite evidence must come from canonical manifest partitions")
        relation_ids = {item.reference_id for item in self.evidence_manifest.relation}
        if not set(self.relation.evidence_reference_ids) <= relation_ids:
            raise ValueError("relation evidence must come from the relation manifest partition")
        if self.idempotency_key != self.expected_idempotency_key:
            raise ValueError("idempotency key must bind the exact command identity")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


__all__ = [
    "AcceptedHeadRef", "AcceptedMemorySnapshot", "AcceptedRelationHeadRef",
    "AdmitCompositeRelationCommand", "CompositeRelationTemplate", "EvidenceManifest",
]
