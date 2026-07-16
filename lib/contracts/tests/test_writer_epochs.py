from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from lib.contracts import (
    ActivateWriterTransferCommand,
    AdvanceWriterScopeCommand,
    AgencyWriteContext,
    EventPosition,
    FenceWriterTransferCommand,
    ProcessingAuthorityContext,
    RegisterWriterScopeCommand,
    RestrictionSet,
    RetireWriterScopeCommand,
    SplitWriterScopeCommand,
    WatermarkVector,
    WriterCutoverProof,
    WriterCutoverState,
    WriterScopeChildSpec,
    WriterScopeEpoch,
    WriterScopeHeadExpectation,
    WriterScopeProofKind,
    WriterScopeVersion,
)


NOW = datetime(2026, 7, 16, tzinfo=timezone.utc)
TENANT = uuid4()
ROOT_SCOPE = uuid4()
TARGET_SCOPE = uuid4()


def _watermark(offset: int = 10) -> WatermarkVector:
    return WatermarkVector(
        positions=(
            EventPosition(
                log_id="canonical-events",
                partition_epoch=1,
                partition_id="p0",
                offset=offset,
            ),
        ),
        database_snapshot_token=f"snapshot:{offset}",
        captured_at=NOW,
    )


def _context(*, key: str = "writer-scope:test") -> AgencyWriteContext:
    authority = ProcessingAuthorityContext(
        tenant_id=TENANT,
        principal_or_service_id="cutover-coordinator",
        purpose="writer_cutover",
        operation="govern_writer_scope",
        object_types=RestrictionSet.only("writer_scope_epoch"),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("architecture-registry"),
        authority_basis_refs=frozenset({"grant:writer-cutover"}),
        policy_version="writer-cutover-v1",
        authority_epoch=1,
        decision_time=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(hours=2),
    )
    return AgencyWriteContext(
        command_id=uuid4(),
        tenant_id=TENANT,
        processing_authority=authority,
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=str(ROOT_SCOPE),
            tenant_id=TENANT,
            semantic_responsibility="writer_scope_epoch",
            source_partition=str(TENANT),
            writer_owner="WriterEpochApplier",
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
            high_water=_watermark(),
        ),
        idempotency_key=key,
        issued_at=NOW,
        expires_at=NOW + timedelta(hours=1),
    )


def _proof(kind: WriterScopeProofKind, suffix: str | None = None) -> WriterCutoverProof:
    return WriterCutoverProof(
        proof_id=uuid4(),
        kind=kind,
        artifact_ref=f"proof:{suffix or kind.value}",
        artifact_digest="a" * 64,
        observed_at=NOW,
    )


def _expected(state: WriterCutoverState, *, version: int = 1, epoch: int = 1):
    return WriterScopeHeadExpectation(
        scope_id=TARGET_SCOPE,
        expected_epoch=epoch,
        expected_aggregate_version=version,
        expected_state=state,
        expected_writer_owner="LegacyWriter",
    )


def test_root_bootstrap_is_exact_self_governance() -> None:
    command = RegisterWriterScopeCommand(
        context=_context(),
        change_authority_ref="constitution:writer-root",
        proofs=(_proof(WriterScopeProofKind.BOOTSTRAP_MANIFEST),),
        scope_id=ROOT_SCOPE,
        semantic_responsibility="writer_scope_epoch",
        source_partitions=(str(TENANT),),
        writer_owner="WriterEpochApplier",
        initial_state=WriterCutoverState.NEW_CANONICAL,
        initial_high_water=_watermark(),
        bootstrap_root=True,
    )
    assert command.request_digest

    with pytest.raises(ValidationError, match="exact self-governing"):
        RegisterWriterScopeCommand(
            **{
                **command.model_dump(mode="python"),
                "source_partitions": ("wrong",),
            }
        )


def test_nonroot_registration_requires_partition_proof_and_legacy_state() -> None:
    command = RegisterWriterScopeCommand(
        context=_context(),
        change_authority_ref="authorization:scope-register",
        proofs=(_proof(WriterScopeProofKind.PARTITION_COVERAGE),),
        scope_id=TARGET_SCOPE,
        semantic_responsibility="evidence_append",
        source_partitions=("jira:project-a", "slack:workspace-a"),
        writer_owner="LegacyWriter",
    )
    assert command.initial_state is WriterCutoverState.LEGACY

    with pytest.raises(ValidationError, match="register in legacy"):
        RegisterWriterScopeCommand(
            **{
                **command.model_dump(mode="python"),
                "initial_state": WriterCutoverState.ADAPTER_ENFORCED,
            }
        )


def test_ordinary_lifecycle_cannot_skip_or_fake_proof_kind() -> None:
    valid = AdvanceWriterScopeCommand(
        context=_context(),
        change_authority_ref="authorization:adapter",
        proofs=(_proof(WriterScopeProofKind.ADAPTER_COMPATIBILITY),),
        expected=_expected(WriterCutoverState.LEGACY),
        to_state=WriterCutoverState.ADAPTER_ENFORCED,
    )
    assert valid.to_state is WriterCutoverState.ADAPTER_ENFORCED

    with pytest.raises(ValidationError, match="illegal ordinary"):
        AdvanceWriterScopeCommand(
            **{
                **valid.model_dump(mode="python"),
                "to_state": WriterCutoverState.VERIFIED,
            }
        )
    with pytest.raises(ValidationError, match="missing required proofs"):
        AdvanceWriterScopeCommand(
            **{
                **valid.model_dump(mode="python"),
                "proofs": (_proof(WriterScopeProofKind.ROLLBACK),),
            }
        )


def test_transfer_has_a_no_writer_fence_and_exact_activation() -> None:
    fence = FenceWriterTransferCommand(
        context=_context(),
        change_authority_ref="authorization:transfer",
        proofs=tuple(
            _proof(kind)
            for kind in (
                WriterScopeProofKind.CATCH_UP_COMPLETE,
                WriterScopeProofKind.SEMANTIC_EQUIVALENCE,
                WriterScopeProofKind.AUTHORITY_EQUIVALENCE,
                WriterScopeProofKind.REPRESENTABILITY,
            )
        ),
        expected=_expected(WriterCutoverState.VERIFIED, version=5),
        pending_writer_owner="NewWriter",
        high_water=_watermark(20),
    )
    assert fence.pending_writer_owner == "NewWriter"

    activation = ActivateWriterTransferCommand(
        context=_context(key="writer-scope:activate"),
        change_authority_ref="authorization:activate",
        proofs=(_proof(WriterScopeProofKind.FENCE_ACKNOWLEDGED),),
        expected=_expected(WriterCutoverState.WRITER_FENCED, version=6, epoch=2),
        pending_writer_owner="NewWriter",
        high_water=_watermark(20),
    )
    assert activation.expected.expected_epoch == 2


def test_writer_scope_version_rejects_ambiguous_fence_and_missing_watermark() -> None:
    base = dict(
        scope_id=TARGET_SCOPE,
        tenant_id=TENANT,
        semantic_responsibility="evidence_append",
        source_partitions=("jira:a",),
        writer_owner="LegacyWriter",
        epoch=2,
        aggregate_version=6,
        state=WriterCutoverState.WRITER_FENCED,
        parent_scope_ids=(),
        high_water=_watermark(),
        change_authority_ref="authorization:transfer",
        transition_proof_ids=(uuid4(),),
        recorded_at=NOW,
    )
    with pytest.raises(ValidationError, match="pending owner"):
        WriterScopeVersion(**base)
    with pytest.raises(ValidationError, match="high-water"):
        WriterScopeVersion(
            **{
                **base,
                "state": WriterCutoverState.VERIFIED,
                "high_water": None,
            }
        )


def test_split_is_disjoint_and_retirement_requires_all_closure_proofs() -> None:
    child_a = WriterScopeChildSpec(
        scope_id=uuid4(), source_partitions=("jira:a",)
    )
    child_b = WriterScopeChildSpec(
        scope_id=uuid4(), source_partitions=("slack:a",)
    )
    split = SplitWriterScopeCommand(
        context=_context(),
        change_authority_ref="authorization:split",
        proofs=(_proof(WriterScopeProofKind.PARTITION_COVERAGE),),
        expected_parent=_expected(WriterCutoverState.LEGACY),
        children=(child_a, child_b),
    )
    assert len(split.children) == 2
    with pytest.raises(ValidationError, match="must be disjoint"):
        SplitWriterScopeCommand(
            **{
                **split.model_dump(mode="python"),
                "children": (
                    child_a,
                    WriterScopeChildSpec(
                        scope_id=uuid4(), source_partitions=("jira:a",)
                    ),
                ),
            }
        )

    with pytest.raises(ValidationError, match="missing required proofs"):
        RetireWriterScopeCommand(
            context=_context(),
            change_authority_ref="authorization:retire",
            proofs=(_proof(WriterScopeProofKind.CONSUMER_DRAIN),),
            expected=_expected(WriterCutoverState.NEW_CANONICAL, version=7, epoch=2),
        )
