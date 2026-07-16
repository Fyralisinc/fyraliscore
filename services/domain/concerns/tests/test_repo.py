from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from lib.contracts.agency import (
    AttentionGovernanceBinding,
    AttentionSourceKind,
    ConcernCriterionState,
    ConcernDisposition,
    ConcernEvaluationCommand,
    ConcernIdentity,
    ConcernIdentityCorrectionCommand,
    ConcernState,
    CriterionImpact,
    CriterionWorkEligibility,
    derive_concern_id,
)
from lib.contracts.kernel import (
    ConsumptionAuthorityContext,
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
)
from lib.evaluation.concern import ConcernEvaluationScope, evaluate_concern_state
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.concerns.repo import (
    AttentionGovernanceBindingRegistry,
    ConcernApplier,
)


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _authority(*, tenant_id: UUID, processing: bool, capabilities: frozenset[str]):
    cls = ProcessingAuthorityContext if processing else ConsumptionAuthorityContext
    return cls(
        tenant_id=tenant_id,
        principal_or_service_id=(
            "service:concern-evaluator" if processing else "actor:concern-owner"
        ),
        purpose="concern_evaluation",
        operation="evaluate",
        object_types=RestrictionSet.only("concern"),
        object_ids=RestrictionSet.unrestricted(),
        fields=RestrictionSet.unrestricted(),
        source_labels=RestrictionSet.only("intent", "brain", "physics"),
        authority_basis_refs=capabilities or frozenset({"grant:concern-evaluator"}),
        policy_version="concern-authority-v1",
        authority_epoch=2,
        decision_time=NOW - timedelta(minutes=5),
        expires_at=NOW + timedelta(hours=2),
    )


def _binding(
    *,
    source_ref: str,
    work_budget: float = 10.0,
    capability: str | None = None,
) -> AttentionGovernanceBinding:
    return AttentionGovernanceBinding(
        binding_id=uuid7(),
        binding_version=1,
        attention_source_ref=source_ref,
        attention_source_kind=AttentionSourceKind.GOAL,
        work_budget_units=work_budget,
        interruption_budget_count=2,
        interruption_budget_minutes=10,
        maximum_duration_seconds=3600,
        satisfaction_rule=f"{source_ref}:satisfied",
        expiry_rule=f"{source_ref}:expired",
        review_rule=f"{source_ref}:daily-review",
        stop_rule=f"{source_ref}:stop-at-low-value",
        permitted_priority_modifier_fields=frozenset({"order"}),
        disposition_capability_refs=(
            {ConcernDisposition.ACCEPTED_RISK: capability} if capability else {}
        ),
        nonwaivable_fields=frozenset({"source_identity"}),
        valid_from=NOW - timedelta(hours=1),
        valid_until=NOW + timedelta(days=1),
    )


def _criterion(
    *,
    ref: str,
    binding: AttentionGovernanceBinding,
    impact: CriterionImpact,
    disposition: ConcernDisposition | None = None,
    capability: str | None = None,
) -> ConcernCriterionState:
    return ConcernCriterionState(
        criterion_ref=ref,
        attention_source_ref=binding.attention_source_ref,
        attention_binding_ref=binding.binding_ref,
        applicable=True,
        impact=impact,
        disposition=disposition,
        disposition_capability_ref=capability if disposition else None,
        disposition_expires_at=NOW + timedelta(hours=1) if disposition else None,
        work_eligibility=CriterionWorkEligibility.ACTIONABLE,
    )


def _identity(*, tenant_id: UUID, dimension: str = "renewal_risk") -> ConcernIdentity:
    return ConcernIdentity(
        tenant_id=tenant_id,
        affected_object_or_scope="customer:atlas",
        state_dimension_or_missing_proposition=dimension,
        valid_time_window="2026-Q3",
        gap_identity_policy_version="gap-v1",
    )


def _command(
    *,
    tenant_id: UUID,
    identity: ConcernIdentity,
    expected_version: int,
    criteria: tuple[ConcernCriterionState, ...],
    idempotency_key: str,
    capabilities: frozenset[str] = frozenset(),
) -> ConcernEvaluationCommand:
    return ConcernEvaluationCommand(
        command_id=uuid7(),
        tenant_id=tenant_id,
        concern_id=derive_concern_id(identity),
        expected_version=expected_version,
        identity=identity,
        criteria=criteria,
        current_state_estimate={"renewal_risk": 0.8},
        materiality=0.9,
        uncertainty=0.3,
        consequence=0.9,
        urgency=0.7,
        actionability=0.8,
        evidence_cutoff=NOW,
        validity_deadline=NOW + timedelta(days=30),
        next_review_at=NOW + timedelta(days=1),
        transition_cause=idempotency_key,
        originating_attention_source_ref=criteria[0].attention_source_ref,
        processing_authority=_authority(
            tenant_id=tenant_id,
            processing=True,
            capabilities=frozenset({"grant:concern-evaluator"}),
        ),
        consumption_authority=_authority(
            tenant_id=tenant_id,
            processing=False,
            capabilities=capabilities or frozenset({"grant:concern-owner"}),
        ),
        writer_scope_epoch=WriterScopeEpoch(
            scope_id=f"concern:{tenant_id}",
            tenant_id=tenant_id,
            semantic_responsibility="concern",
            source_partition=str(tenant_id),
            writer_owner="ConcernApplier",
            epoch=1,
            state=WriterCutoverState.NEW_CANONICAL,
        ),
        idempotency_key=idempotency_key,
        issued_at=NOW,
        expires_at=NOW + timedelta(minutes=15),
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concern_apply_is_idempotent_cas_safe_and_preserves_plural_sources(
    fresh_db,
):
    evaluation_start = datetime.now(timezone.utc) - timedelta(seconds=1)
    tenant = uuid4()
    first_binding = _binding(source_ref="goal:retain-atlas", work_budget=10)
    risk_capability = "capability:accept-renewal-risk"
    second_binding = _binding(
        source_ref="goal:protect-margin",
        work_budget=4,
        capability=risk_capability,
    )
    identity = _identity(tenant_id=tenant)
    candidate = _command(
        tenant_id=tenant,
        identity=identity,
        expected_version=0,
        criteria=(
            _criterion(
                ref="criterion:retention",
                binding=first_binding,
                impact=CriterionImpact.UNKNOWN,
            ),
        ),
        idempotency_key="concern:atlas:candidate",
    )
    applier = ConcernApplier()
    registry = AttentionGovernanceBindingRegistry()
    async with fresh_db.acquire() as conn, conn.transaction():
        first, inserted = await registry.register(
            conn=conn,
            tenant_id=tenant,
            binding=first_binding,
            registered_by_ref="IntentApplier:goal:retain-atlas",
        )
        repeated, inserted_again = await registry.register(
            conn=conn,
            tenant_id=tenant,
            binding=first_binding,
            registered_by_ref="IntentApplier:goal:retain-atlas",
        )
        await registry.register(
            conn=conn,
            tenant_id=tenant,
            binding=second_binding,
            registered_by_ref="IntentApplier:goal:protect-margin",
        )
        initial = await applier.apply_evaluation(conn=conn, command=candidate, now=NOW)
        duplicate = await applier.apply_evaluation(conn=conn, command=candidate, now=NOW)
        open_command = _command(
            tenant_id=tenant,
            identity=identity,
            expected_version=1,
            criteria=(
                _criterion(
                    ref="criterion:retention",
                    binding=first_binding,
                    impact=CriterionImpact.MATERIAL_GAP,
                ),
            ),
            idempotency_key="concern:atlas:material",
        )
        opened = await applier.apply_evaluation(conn=conn, command=open_command, now=NOW)
        plural_command = _command(
            tenant_id=tenant,
            identity=identity,
            expected_version=2,
            criteria=(
                _criterion(
                    ref="criterion:margin",
                    binding=second_binding,
                    impact=CriterionImpact.MATERIAL_GAP,
                    disposition=ConcernDisposition.ACCEPTED_RISK,
                    capability=risk_capability,
                ),
            ),
            idempotency_key="concern:atlas:add-margin",
            capabilities=frozenset({risk_capability}),
        )
        plural = await applier.apply_evaluation(conn=conn, command=plural_command, now=NOW)
        snapshot = await applier.get_snapshot(
            conn=conn,
            tenant_id=tenant,
            concern_id=derive_concern_id(identity),
        )
        envelope = await conn.fetchval(
            """
            SELECT effective_binding_envelope
            FROM concern_versions
            WHERE tenant_id = $1 AND concern_id = $2 AND aggregate_version = 3
            """,
            tenant,
            derive_concern_id(identity),
        )
        counts = {
            table: await conn.fetchval(
                f"SELECT count(*) FROM {table} WHERE tenant_id = $1", tenant
            )
            for table in (
                "attention_governance_bindings",
                "concern_heads",
                "concern_command_results",
                "concern_versions",
                "concern_transitions",
                "concern_canonical_events",
                "concern_outbox_records",
            )
        }
        with pytest.raises(InvariantViolation, match="expected version"):
            await applier.apply_evaluation(
                conn=conn,
                command=open_command.model_copy(
                    update={
                        "command_id": uuid7(),
                        "idempotency_key": "concern:atlas:stale",
                    }
                ),
                now=NOW,
            )
        evaluation = await evaluate_concern_state(
            conn,
            scope=ConcernEvaluationScope(
                tenant_id=tenant,
                start=evaluation_start,
                end=datetime.now(timezone.utc) + timedelta(seconds=1),
                run_id="concern-component",
            ),
            artifact_refs=("pytest:concern-component",),
        )
    assert first == repeated == first_binding
    assert inserted is True and inserted_again is False
    assert initial.state is ConcernState.CANDIDATE
    assert duplicate.duplicate is True
    assert duplicate.command_result_id == initial.command_result_id
    assert opened.state is ConcernState.OPEN
    assert plural.state is ConcernState.OPEN
    assert snapshot is not None
    assert len(snapshot.criteria) == 2
    assert snapshot.contributing_attention_source_refs == frozenset(
        {first_binding.attention_source_ref, second_binding.attention_source_ref}
    )
    envelope_payload = json.loads(envelope) if isinstance(envelope, str) else envelope
    assert envelope_payload["work_budget_units"] == 4
    assert counts == {
        "attention_governance_bindings": 2,
        "concern_heads": 1,
        "concern_command_results": 3,
        "concern_versions": 3,
        "concern_transitions": 3,
        "concern_canonical_events": 3,
        "concern_outbox_records": 3,
    }
    assert evaluation.incident_counts == {}
    assert evaluation.reducer_conformance_rate == 1.0
    assert evaluation.binding_envelope_conformance_rate == 1.0
    assert evaluation.command_reconstructability_rate == 1.0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gap_identity_correction_is_one_atomic_reciprocal_successor_plan(
    fresh_db,
):
    evaluation_start = datetime.now(timezone.utc) - timedelta(seconds=1)
    tenant = uuid4()
    binding = _binding(source_ref="goal:retain-atlas")
    predecessor_identity = _identity(tenant_id=tenant, dimension="renewal_risk")
    successor_identity = _identity(tenant_id=tenant, dimension="renewal_probability")
    initial = _command(
        tenant_id=tenant,
        identity=predecessor_identity,
        expected_version=0,
        criteria=(
            _criterion(
                ref="criterion:retention",
                binding=binding,
                impact=CriterionImpact.UNKNOWN,
            ),
        ),
        idempotency_key="concern:predecessor:candidate",
    )
    successor = _command(
        tenant_id=tenant,
        identity=successor_identity,
        expected_version=0,
        criteria=(
            _criterion(
                ref="criterion:retention",
                binding=binding,
                impact=CriterionImpact.UNKNOWN,
            ),
        ),
        idempotency_key="concern:successor:candidate",
    )
    correction = ConcernIdentityCorrectionCommand(
        command_id=uuid7(),
        tenant_id=tenant,
        predecessor_concern_id=derive_concern_id(predecessor_identity),
        expected_predecessor_version=1,
        successor=successor,
        correction_epoch=1,
        correction_reason="state dimension was underspecified",
        idempotency_key="concern:identity-correction:1",
    )
    applier = ConcernApplier()
    async with fresh_db.acquire() as conn, conn.transaction():
        await AttentionGovernanceBindingRegistry().register(
            conn=conn,
            tenant_id=tenant,
            binding=binding,
            registered_by_ref="IntentApplier:goal:retain-atlas",
        )
        await applier.apply_evaluation(conn=conn, command=initial, now=NOW)
        result = await applier.correct_identity(conn=conn, command=correction, now=NOW)
        duplicate = await applier.correct_identity(conn=conn, command=correction, now=NOW)
        heads = await conn.fetch(
            """
            SELECT concern_id, current_version, current_state,
                   predecessor_concern_id, successor_concern_id
            FROM concern_heads WHERE tenant_id = $1 ORDER BY concern_id
            """,
            tenant,
        )
        correction_count = await conn.fetchval(
            "SELECT count(*) FROM concern_identity_corrections WHERE tenant_id = $1",
            tenant,
        )
        correction_version_count = await conn.fetchval(
            """
            SELECT count(*) FROM concern_versions
            WHERE tenant_id = $1 AND command_result_id = $2
            """,
            tenant,
            result.command_result_id,
        )
        correction_event_count = await conn.fetchval(
            """
            SELECT count(*) FROM concern_canonical_events
            WHERE tenant_id = $1 AND command_result_id = $2
            """,
            tenant,
            result.command_result_id,
        )
        evaluation = await evaluate_concern_state(
            conn,
            scope=ConcernEvaluationScope(
                tenant_id=tenant,
                start=evaluation_start,
                end=datetime.now(timezone.utc) + timedelta(seconds=1),
                run_id="concern-correction-component",
            ),
            artifact_refs=("pytest:concern-correction",),
        )
    by_id = {row["concern_id"]: row for row in heads}
    predecessor_id = derive_concern_id(predecessor_identity)
    successor_id = derive_concern_id(successor_identity)
    assert duplicate.duplicate is True
    assert duplicate.command_result_id == result.command_result_id
    assert by_id[predecessor_id]["current_state"] == "invalidated"
    assert by_id[predecessor_id]["successor_concern_id"] == successor_id
    assert by_id[successor_id]["current_state"] == "candidate"
    assert by_id[successor_id]["predecessor_concern_id"] == predecessor_id
    assert correction_count == 1
    assert correction_version_count == 2
    assert correction_event_count == 2
    assert evaluation.incident_counts == {}
    assert evaluation.identity_correction_reciprocity_rate == 1.0
