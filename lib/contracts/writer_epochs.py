"""Durable single-writer ownership and cutover contracts.

``WriterScopeEpoch`` in :mod:`lib.contracts.kernel` is the compact value carried
by a semantic command.  This module defines the canonical registry whose
current version makes that value authoritative.  Partition membership is an
explicit finite set so overlap, split conservation, and merge conservation are
mechanically decidable rather than inferred from opaque wildcard strings.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.agency import AgencyWriteContext
from lib.contracts.kernel import (
    WatermarkVector,
    WriterCutoverState,
    WriterScopeEpoch,
    canonical_sha256,
)


class _WriterEpochContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=False,
    )


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class WriterScopeProofKind(StrEnum):
    BOOTSTRAP_MANIFEST = "bootstrap_manifest"
    PARTITION_COVERAGE = "partition_coverage"
    ADAPTER_COMPATIBILITY = "adapter_compatibility"
    BACKFILL_MANIFEST = "backfill_manifest"
    CATCH_UP_COMPLETE = "catch_up_complete"
    SEMANTIC_EQUIVALENCE = "semantic_equivalence"
    AUTHORITY_EQUIVALENCE = "authority_equivalence"
    REPRESENTABILITY = "representability"
    FENCE_ACKNOWLEDGED = "fence_acknowledged"
    ROLLBACK = "rollback"
    CONSUMER_DRAIN = "consumer_drain"
    REPAIR_RESIDUE_CLOSED = "repair_residue_closed"


class WriterCutoverProof(_WriterEpochContract):
    proof_id: UUID
    kind: WriterScopeProofKind
    artifact_ref: str = Field(min_length=1)
    artifact_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_at: datetime

    @field_validator("observed_at")
    @classmethod
    def observed_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="observed_at")


def _proof_kinds(proofs: tuple[WriterCutoverProof, ...]) -> set[WriterScopeProofKind]:
    ids = [proof.proof_id for proof in proofs]
    if len(ids) != len(set(ids)):
        raise ValueError("writer cutover proofs must have unique proof IDs")
    return {proof.kind for proof in proofs}


def _require_proofs(
    proofs: tuple[WriterCutoverProof, ...],
    *required: WriterScopeProofKind,
) -> None:
    missing = set(required) - _proof_kinds(proofs)
    if missing:
        names = ", ".join(sorted(item.value for item in missing))
        raise ValueError(f"writer-scope command is missing required proofs: {names}")


class WriterScopeVersion(_WriterEpochContract):
    scope_id: UUID
    tenant_id: UUID
    semantic_responsibility: str = Field(min_length=1)
    source_partitions: tuple[str, ...] = Field(min_length=1)
    writer_owner: str = Field(min_length=1)
    pending_writer_owner: str | None = None
    epoch: int = Field(ge=1)
    aggregate_version: int = Field(ge=1)
    state: WriterCutoverState
    parent_scope_ids: tuple[UUID, ...] = ()
    high_water: WatermarkVector | None = None
    change_authority_ref: str = Field(min_length=1)
    transition_proof_ids: tuple[UUID, ...] = Field(min_length=1)
    recorded_at: datetime

    @field_validator("recorded_at")
    @classmethod
    def recorded_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="recorded_at")

    @model_validator(mode="after")
    def scope_shape_is_unambiguous(self) -> Self:
        if tuple(sorted(self.source_partitions)) != self.source_partitions:
            raise ValueError("writer scope partitions must be sorted")
        if len(self.source_partitions) != len(set(self.source_partitions)):
            raise ValueError("writer scope partitions must be unique")
        if len(self.parent_scope_ids) != len(set(self.parent_scope_ids)):
            raise ValueError("writer scope parent IDs must be unique")
        if len(self.transition_proof_ids) != len(set(self.transition_proof_ids)):
            raise ValueError("writer scope transition proof IDs must be unique")
        if self.state is WriterCutoverState.WRITER_FENCED:
            if not self.pending_writer_owner:
                raise ValueError("writer-fenced scope requires its pending owner")
            if self.pending_writer_owner == self.writer_owner:
                raise ValueError("writer transfer must name a different pending owner")
        elif self.pending_writer_owner is not None:
            raise ValueError("pending writer owner is only valid while writer-fenced")
        if self.state in {
            WriterCutoverState.CATCH_UP,
            WriterCutoverState.VERIFIED,
            WriterCutoverState.WRITER_FENCED,
            WriterCutoverState.NEW_CANONICAL,
        } and self.high_water is None:
            raise ValueError(f"{self.state.value} scope requires a high-water vector")
        return self

    @property
    def version_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def embedded_epoch(self, *, source_partition: str) -> WriterScopeEpoch:
        if source_partition not in self.source_partitions:
            raise ValueError("source partition is outside this writer scope")
        return WriterScopeEpoch(
            scope_id=str(self.scope_id),
            tenant_id=self.tenant_id,
            semantic_responsibility=self.semantic_responsibility,
            source_partition=source_partition,
            writer_owner=self.writer_owner,
            epoch=self.epoch,
            state=self.state,
            parent_scope_id=(
                str(self.parent_scope_ids[0]) if len(self.parent_scope_ids) == 1 else None
            ),
            high_water=self.high_water,
        )


class WriterScopeHeadExpectation(_WriterEpochContract):
    scope_id: UUID
    expected_epoch: int = Field(ge=1)
    expected_aggregate_version: int = Field(ge=1)
    expected_state: WriterCutoverState
    expected_writer_owner: str = Field(min_length=1)


class WriterScopeChildSpec(_WriterEpochContract):
    scope_id: UUID
    source_partitions: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def partitions_are_canonical(self) -> Self:
        if tuple(sorted(self.source_partitions)) != self.source_partitions:
            raise ValueError("child writer-scope partitions must be sorted")
        if len(self.source_partitions) != len(set(self.source_partitions)):
            raise ValueError("child writer-scope partitions must be unique")
        return self


class _WriterEpochCommand(_WriterEpochContract):
    context: AgencyWriteContext
    change_authority_ref: str = Field(min_length=1)
    proofs: tuple[WriterCutoverProof, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def command_uses_registry_writer(self) -> Self:
        self.context.require_writer(
            owner="WriterEpochApplier",
            responsibility="writer_scope_epoch",
        )
        _proof_kinds(self.proofs)
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class RegisterWriterScopeCommand(_WriterEpochCommand):
    scope_id: UUID
    semantic_responsibility: str = Field(min_length=1)
    source_partitions: tuple[str, ...] = Field(min_length=1)
    writer_owner: str = Field(min_length=1)
    initial_state: WriterCutoverState = WriterCutoverState.LEGACY
    initial_high_water: WatermarkVector | None = None
    bootstrap_root: bool = False

    @model_validator(mode="after")
    def registration_is_safe(self) -> Self:
        if tuple(sorted(self.source_partitions)) != self.source_partitions:
            raise ValueError("writer scope partitions must be sorted")
        if len(self.source_partitions) != len(set(self.source_partitions)):
            raise ValueError("writer scope partitions must be unique")
        if self.bootstrap_root:
            _require_proofs(self.proofs, WriterScopeProofKind.BOOTSTRAP_MANIFEST)
            if (
                self.semantic_responsibility != "writer_scope_epoch"
                or self.writer_owner != "WriterEpochApplier"
                or self.initial_state is not WriterCutoverState.NEW_CANONICAL
                or self.source_partitions != (str(self.context.tenant_id),)
                or str(self.scope_id) != self.context.writer_scope_epoch.scope_id
                or self.initial_high_water is None
            ):
                raise ValueError("bootstrap root must be the exact self-governing tenant scope")
        else:
            _require_proofs(self.proofs, WriterScopeProofKind.PARTITION_COVERAGE)
            if self.initial_state is not WriterCutoverState.LEGACY:
                raise ValueError("non-root writer scopes register in legacy state")
            if self.initial_high_water is not None:
                raise ValueError("legacy writer-scope registration cannot claim high water")
        return self


_ADVANCE_TRANSITIONS: dict[WriterCutoverState, frozenset[WriterCutoverState]] = {
    WriterCutoverState.LEGACY: frozenset({WriterCutoverState.ADAPTER_ENFORCED}),
    WriterCutoverState.ADAPTER_ENFORCED: frozenset(
        {WriterCutoverState.BACKFILLING, WriterCutoverState.LEGACY}
    ),
    WriterCutoverState.BACKFILLING: frozenset(
        {WriterCutoverState.CATCH_UP, WriterCutoverState.LEGACY}
    ),
    WriterCutoverState.CATCH_UP: frozenset(
        {WriterCutoverState.VERIFIED, WriterCutoverState.LEGACY}
    ),
    WriterCutoverState.VERIFIED: frozenset({WriterCutoverState.LEGACY}),
}


class AdvanceWriterScopeCommand(_WriterEpochCommand):
    expected: WriterScopeHeadExpectation
    to_state: WriterCutoverState
    high_water: WatermarkVector | None = None

    @model_validator(mode="after")
    def transition_has_its_typed_proof(self) -> Self:
        if self.to_state not in _ADVANCE_TRANSITIONS.get(self.expected.expected_state, frozenset()):
            raise ValueError("illegal ordinary writer-scope cutover transition")
        required = {
            WriterCutoverState.ADAPTER_ENFORCED: (
                WriterScopeProofKind.ADAPTER_COMPATIBILITY,
            ),
            WriterCutoverState.BACKFILLING: (
                WriterScopeProofKind.BACKFILL_MANIFEST,
            ),
            WriterCutoverState.CATCH_UP: (WriterScopeProofKind.CATCH_UP_COMPLETE,),
            WriterCutoverState.VERIFIED: (
                WriterScopeProofKind.SEMANTIC_EQUIVALENCE,
                WriterScopeProofKind.AUTHORITY_EQUIVALENCE,
            ),
            WriterCutoverState.LEGACY: (WriterScopeProofKind.ROLLBACK,),
        }[self.to_state]
        _require_proofs(self.proofs, *required)
        if self.to_state in {
            WriterCutoverState.CATCH_UP,
            WriterCutoverState.VERIFIED,
        } and self.high_water is None:
            raise ValueError("catch-up and verification require a high-water vector")
        return self


class FenceWriterTransferCommand(_WriterEpochCommand):
    expected: WriterScopeHeadExpectation
    pending_writer_owner: str = Field(min_length=1)
    high_water: WatermarkVector

    @model_validator(mode="after")
    def fence_has_complete_preconditions(self) -> Self:
        if self.expected.expected_state is not WriterCutoverState.VERIFIED:
            raise ValueError("writer transfer may fence only a verified scope")
        if self.pending_writer_owner == self.expected.expected_writer_owner:
            raise ValueError("writer transfer requires a different owner")
        _require_proofs(
            self.proofs,
            WriterScopeProofKind.CATCH_UP_COMPLETE,
            WriterScopeProofKind.SEMANTIC_EQUIVALENCE,
            WriterScopeProofKind.AUTHORITY_EQUIVALENCE,
            WriterScopeProofKind.REPRESENTABILITY,
        )
        return self


class ActivateWriterTransferCommand(_WriterEpochCommand):
    expected: WriterScopeHeadExpectation
    pending_writer_owner: str = Field(min_length=1)
    high_water: WatermarkVector

    @model_validator(mode="after")
    def activation_binds_the_fence(self) -> Self:
        if self.expected.expected_state is not WriterCutoverState.WRITER_FENCED:
            raise ValueError("new canonical writer requires a fenced scope")
        _require_proofs(self.proofs, WriterScopeProofKind.FENCE_ACKNOWLEDGED)
        return self


class SplitWriterScopeCommand(_WriterEpochCommand):
    expected_parent: WriterScopeHeadExpectation
    children: tuple[WriterScopeChildSpec, ...] = Field(min_length=2)

    @model_validator(mode="after")
    def children_are_structurally_disjoint(self) -> Self:
        _require_proofs(self.proofs, WriterScopeProofKind.PARTITION_COVERAGE)
        ids = [child.scope_id for child in self.children]
        if len(ids) != len(set(ids)):
            raise ValueError("writer-scope split child IDs must be unique")
        flattened = [part for child in self.children for part in child.source_partitions]
        if len(flattened) != len(set(flattened)):
            raise ValueError("writer-scope split children must be disjoint")
        return self


class MergeWriterScopesCommand(_WriterEpochCommand):
    expected_parents: tuple[WriterScopeHeadExpectation, ...] = Field(min_length=2)
    merged_scope_id: UUID

    @model_validator(mode="after")
    def parents_are_unique(self) -> Self:
        _require_proofs(self.proofs, WriterScopeProofKind.PARTITION_COVERAGE)
        ids = [parent.scope_id for parent in self.expected_parents]
        if len(ids) != len(set(ids)):
            raise ValueError("writer-scope merge parents must be unique")
        if self.merged_scope_id in set(ids):
            raise ValueError("merged writer scope requires a new scope ID")
        return self


class RetireWriterScopeCommand(_WriterEpochCommand):
    expected: WriterScopeHeadExpectation

    @model_validator(mode="after")
    def retirement_has_closure_proofs(self) -> Self:
        if self.expected.expected_state is not WriterCutoverState.NEW_CANONICAL:
            raise ValueError("only a new-canonical scope may retire normally")
        _require_proofs(
            self.proofs,
            WriterScopeProofKind.CONSUMER_DRAIN,
            WriterScopeProofKind.SEMANTIC_EQUIVALENCE,
            WriterScopeProofKind.REPAIR_RESIDUE_CLOSED,
        )
        return self


def writer_scope_advance_allowed(
    from_state: WriterCutoverState,
    to_state: WriterCutoverState,
) -> bool:
    return to_state in _ADVANCE_TRANSITIONS.get(from_state, frozenset())


__all__ = [
    "ActivateWriterTransferCommand",
    "AdvanceWriterScopeCommand",
    "FenceWriterTransferCommand",
    "MergeWriterScopesCommand",
    "RegisterWriterScopeCommand",
    "RetireWriterScopeCommand",
    "SplitWriterScopeCommand",
    "WriterCutoverProof",
    "WriterScopeChildSpec",
    "WriterScopeHeadExpectation",
    "WriterScopeProofKind",
    "WriterScopeVersion",
    "writer_scope_advance_allowed",
]
