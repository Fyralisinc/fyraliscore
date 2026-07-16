from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from lib.contracts import (
    AgencyWriteContext,
    DependencyFenceClass,
    EventPosition,
    InvalidationKind,
    ProcessingAuthorityContext,
    RepairCoverageBasis,
    RepairEpisode,
    RepairEpisodeState,
    RepairObligation,
    RepairObligationCommand,
    RepairObligationState,
    RestrictionSet,
    WatermarkVector,
    WriterCutoverState,
    WriterScopeEpoch,
    repair_episode_transition_allowed,
    repair_obligation_transition_allowed,
)


TENANT = UUID("00000000-0000-4000-8000-000000000220")
NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _context():
    return AgencyWriteContext(
        command_id=uuid4(),
        tenant_id=TENANT,
        processing_authority=ProcessingAuthorityContext(
            tenant_id=TENANT,
            principal_or_service_id="service:repair-ledger",
            purpose="correction_closure",
            operation="apply_repair",
            object_types=RestrictionSet.unrestricted(),
            object_ids=RestrictionSet.unrestricted(),
            fields=RestrictionSet.unrestricted(),
            source_labels=RestrictionSet.unrestricted(),
            authority_basis_refs=frozenset({"platform:repair"}),
            policy_version="repair-processing-v1",
            authority_epoch=1,
            decision_time=NOW - timedelta(minutes=1),
            expires_at=NOW + timedelta(hours=2),
        ),
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=f"repair_obligation:{TENANT}",
            tenant_id=TENANT,
            semantic_responsibility="repair_obligation",
            source_partition=str(TENANT),
            writer_owner="RepairLedgerApplier",
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key=f"repair:{uuid4()}",
        issued_at=NOW,
        expires_at=NOW + timedelta(hours=1),
    )


def _watermark(offset: int) -> WatermarkVector:
    return WatermarkVector(
        positions=(
            EventPosition(
                log_id="agency-events",
                partition_epoch=2,
                partition_id="tenant-partition-1",
                offset=offset,
            ),
        ),
        database_snapshot_token=f"snapshot:{offset}",
        captured_at=NOW + timedelta(seconds=offset),
    )


def _episode(**updates):
    values = dict(
        episode_id=uuid4(),
        tenant_id=TENANT,
        invalidation_request_id=uuid4(),
        invalidation_epoch=3,
        kind=InvalidationKind.CORRECTION,
        state=RepairEpisodeState.CONVERGED,
        coverage_basis=RepairCoverageBasis.ORACLE_COMPLETE,
        known_material_dependency_count=2,
        known_covered_dependency_count=2,
        oracle_material_dependency_count=2,
        oracle_covered_dependency_count=2,
        current_tail_fate_counts={"repaired": 2},
        historical_generation_count=2,
        adjudicated_residue_count=0,
        source_fence_active=True,
        snapshot_watermark=_watermark(5),
        catchup_watermark=_watermark(7),
        stable_scan_count=2,
        reason="all oracle-known dependents repaired and vector drained",
        created_at=NOW,
        updated_at=NOW + timedelta(minutes=10),
    )
    values.update(updates)
    return RepairEpisode(**values)


def _obligation(**updates):
    values = dict(
        obligation_id=uuid4(),
        lineage_id=uuid4(),
        tenant_id=TENANT,
        generation=1,
        invalidation_request_id=uuid4(),
        invalidation_epoch=3,
        source_object_type="canonical_referent",
        source_object_id=uuid4(),
        source_generation=4,
        dependent_object_type="belief_assertion",
        dependent_object_id=uuid4(),
        dependent_object_version=2,
        dependency_kind="grounding_identity",
        fence_class=DependencyFenceClass.READ_REJECT,
        required_dependent_writer_id="EpistemicApplier",
        required_dependent_transition="supersede_stale_grounding",
        expected_target_version=3,
        maximum_attempts=3,
        deadline=NOW + timedelta(days=1),
        residue_policy_ref="repair-residue-policy:v1",
        state=RepairObligationState.OPEN,
        reason="canonical referent split invalidates dependent grounding",
        created_at=NOW,
        updated_at=NOW,
    )
    values.update(updates)
    return RepairObligation(**values)


def test_known_edges_alone_or_failed_tail_cannot_claim_convergence():
    assert _episode().episode_digest

    with pytest.raises(ValidationError, match="known-edge-only"):
        _episode(coverage_basis=RepairCoverageBasis.KNOWN_EDGE_ONLY)

    with pytest.raises(ValidationError, match="failed or nonterminal"):
        _episode(current_tail_fate_counts={"repaired": 1, "exhausted": 1})

    with pytest.raises(ValidationError, match="watermark catch-up"):
        _episode(catchup_watermark=_watermark(3))


def test_repair_obligation_requires_proof_for_noop_and_residue():
    obligation = _obligation()
    command = RepairObligationCommand(
        context=_context(), expected_version=0, obligation=obligation
    )
    assert len(command.request_digest) == 64

    with pytest.raises(ValidationError, match="no-op requires proof"):
        _obligation(state=RepairObligationState.NO_OP)

    with pytest.raises(ValidationError, match="declaration, authority, fence"):
        _obligation(state=RepairObligationState.ADJUDICATED_RESIDUE)


def test_repair_reducers_are_closed_and_terminal_states_absorb():
    assert repair_obligation_transition_allowed(
        None, RepairObligationState.OPEN
    )
    assert repair_obligation_transition_allowed(
        RepairObligationState.RECEIPT_PENDING,
        RepairObligationState.REPAIRED,
    )
    assert not repair_obligation_transition_allowed(
        RepairObligationState.EXHAUSTED,
        RepairObligationState.REPAIRED,
    )
    assert repair_episode_transition_allowed(
        RepairEpisodeState.REPAIRING,
        RepairEpisodeState.CONVERGED,
    )
    assert not repair_episode_transition_allowed(
        RepairEpisodeState.CONVERGED,
        RepairEpisodeState.REPAIRING,
    )
