from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import ValidationError

from lib.contracts import (
    BootstrapPolicy,
    EconomicOperatingEnvelope,
    EconomicUsage,
    LearnedArtifactIsolationClass,
    LearnedArtifactKind,
    LearnedArtifactManifest,
    LearnedArtifactStatus,
    ProcessingClass,
    ProcessingClassPolicy,
    ProcessingFactors,
    RepresentationAdmissionScope,
    RepresentationScopeKind,
    TenantInfluenceDisposition,
    TenantInfluenceLineage,
    UsefulSafeFate,
    UsefulSafeFateKind,
    select_processing_class,
)


TENANT_A = UUID("00000000-0000-4000-8000-000000000001")
TENANT_B = UUID("00000000-0000-4000-8000-000000000002")


def _envelope(**overrides: int | float | str | None) -> EconomicOperatingEnvelope:
    values = {
        "policy_version": "economics-v1",
        "max_model_calls": 4,
        "max_model_tokens": 8_000,
        "max_latency_ms": 10_000,
    }
    values.update(overrides)
    return EconomicOperatingEnvelope(**values)


def test_processing_class_selection_is_monotone_in_consequence() -> None:
    policy = ProcessingClassPolicy(version="rigor-v1")
    selected = []
    for consequence in (0.0, 0.2, 0.4, 0.6, 0.8, 1.0):
        decision = select_processing_class(
            factors=ProcessingFactors(
                consequence=consequence,
                uncertainty=0.5,
                irreversibility=0.5,
                authority_sensitivity=0.5,
                expected_value=0.5,
            ),
            policy=policy,
            economic_envelope=_envelope(),
        )
        selected.append(decision.selected.rank)

    assert selected == sorted(selected)


@pytest.mark.parametrize(
    ("factor", "expected"),
    [
        ({"durable_output": True}, ProcessingClass.R3_DURABLE_UNDERSTANDING),
        (
            {"consequential_decision": True},
            ProcessingClass.R4_CONSEQUENTIAL_DECISION_SUPPORT,
        ),
        ({"external_effect": True}, ProcessingClass.R5_EXTERNAL_AGENCY),
    ],
)
def test_semantic_floor_cannot_be_weakened_by_a_low_score(
    factor: dict[str, bool],
    expected: ProcessingClass,
) -> None:
    decision = select_processing_class(
        factors=ProcessingFactors(**factor),
        policy=ProcessingClassPolicy(version="rigor-v1"),
        economic_envelope=_envelope(),
    )
    assert decision.selected is expected
    assert "semantic_floor" in decision.reason_codes


def test_policy_ceiling_cannot_make_external_action_cheap() -> None:
    with pytest.raises(ValueError, match="ceiling"):
        select_processing_class(
            factors=ProcessingFactors(external_effect=True),
            policy=ProcessingClassPolicy(
                version="rigor-v1",
                ceiling=ProcessingClass.R4_CONSEQUENTIAL_DECISION_SUPPORT,
            ),
            economic_envelope=_envelope(),
        )


def test_economic_envelope_reports_every_exceeded_native_resource() -> None:
    envelope = _envelope(max_model_calls=1, max_latency_ms=50)
    usage = EconomicUsage(model_calls=2, model_tokens=500, latency_ms=51)

    assert envelope.violations(usage) == ("model_calls", "latency_ms")
    assert not envelope.permits(usage)


def test_economic_usage_composes_without_losing_native_units() -> None:
    total = EconomicUsage(model_calls=1, model_tokens=100).plus(
        EconomicUsage(model_calls=2, model_tokens=250, repair_fanout=3)
    )
    assert total.model_calls == 3
    assert total.model_tokens == 350
    assert total.repair_fanout == 3


def test_deferred_fate_requires_a_wake_condition() -> None:
    with pytest.raises(ValidationError, match="wake condition"):
        UsefulSafeFate(
            kind=UsefulSafeFateKind.DEFERRED,
            processing_class=ProcessingClass.R2_PROVISIONAL_GROUNDING,
            stop_reason="budget_exhausted",
            material_unresolved=True,
        )


def test_non_interruption_is_explicit_but_not_a_useful_result() -> None:
    fate = UsefulSafeFate(
        kind=UsefulSafeFateKind.NON_INTERRUPTION,
        processing_class=ProcessingClass.R1_MINIMAL_INTERPRETATION,
        stop_reason="interruption_value_nonpositive",
    )
    assert fate.is_justified_no_result
    assert not fate.is_useful_result
    assert fate.is_safe


def test_complete_fate_cannot_hide_unknowns() -> None:
    with pytest.raises(ValidationError, match="cannot hide"):
        UsefulSafeFate(
            kind=UsefulSafeFateKind.COMPLETE,
            processing_class=ProcessingClass.R3_DURABLE_UNDERSTANDING,
            result_summary="done",
            usefulness_ceiling=1.0,
            unknowns=("owner",),
            stop_reason="complete",
        )


def test_bootstrap_policy_cannot_self_promote() -> None:
    with pytest.raises(ValidationError, match="independent evidence"):
        BootstrapPolicy(
            adaptive_family="entity_grounding",
            version="v1",
            governed_prior="source_ids",
            cold_start_behavior="preserve unresolved",
            shadow_behavior="score only",
            minimum_independent_evidence=20,
            promotion_metric_id="entity.brier",
            minimum_effect=0.03,
            maximum_harm_rate=0.01,
            frozen_fallback="source IDs and unresolved state",
            rollback_trigger="harm rate exceeded",
            expiry_behavior="remain shadow",
            independent_evidence_required=False,
        )


def _scope(
    *,
    kind: RepresentationScopeKind,
    candidate_id: str | None = None,
    consumer: str = "ask",
    cohort: str = "saas-50-200",
) -> RepresentationAdmissionScope:
    return RepresentationAdmissionScope(
        scope_id=f"scope:{candidate_id or cohort}",
        version="v1",
        kind=kind,
        relation_family="blocks",
        consumer=consumer,
        risk_class="decision_support",
        domain="customer_success",
        organization_cohort=cohort,
        membership_version="members-v1",
        candidate_id=candidate_id,
    )


def test_family_scope_contains_only_matching_members_and_consumer() -> None:
    family = _scope(kind=RepresentationScopeKind.FAMILY_COHORT)
    member = _scope(kind=RepresentationScopeKind.CANDIDATE, candidate_id="edge-1")
    other_consumer = _scope(
        kind=RepresentationScopeKind.CANDIDATE,
        candidate_id="edge-1",
        consumer="brief",
    )

    assert family.contains(member)
    assert member.is_no_broader_than(family)
    assert not family.contains(other_consumer)


def test_candidate_scope_cannot_masquerade_as_family_scope() -> None:
    with pytest.raises(ValidationError, match="cannot carry candidate_id"):
        _scope(
            kind=RepresentationScopeKind.FAMILY_COHORT,
            candidate_id="edge-1",
        )


def _lineage(tenant_id: UUID = TENANT_A) -> TenantInfluenceLineage:
    return TenantInfluenceLineage(
        lineage_id=f"lineage:{tenant_id}",
        tenant_id=tenant_id,
        purpose="entity_calibration",
        source_artifact_ids=("label-set-1",),
        contribution_class="calibration",
        authority_basis="tenant_training_contract-v1",
        permitted_from=datetime.now(UTC),
    )


def test_tenant_isolated_artifact_allows_only_declared_use() -> None:
    manifest = LearnedArtifactManifest(
        artifact_id="entity-calibrator",
        version="v1",
        kind=LearnedArtifactKind.CALIBRATION,
        isolation_class=LearnedArtifactIsolationClass.TENANT_ISOLATED,
        status=LearnedArtifactStatus.ACTIVE,
        permitted_tenant_ids=frozenset({TENANT_A}),
        permitted_purposes=frozenset({"entity_calibration"}),
        lineage=(_lineage(),),
        training_procedure_ref="procedure:v1",
        deletion_contract="retrain_and_replace",
    )

    assert manifest.allows_use(
        tenant_id=TENANT_A,
        purpose="entity_calibration",
    )
    assert not manifest.allows_use(
        tenant_id=TENANT_B,
        purpose="entity_calibration",
    )


def test_governed_shared_learning_requires_policy_and_leakage_evidence() -> None:
    with pytest.raises(ValidationError, match="requires a policy"):
        LearnedArtifactManifest(
            artifact_id="shared-router",
            version="v1",
            kind=LearnedArtifactKind.POLICY,
            isolation_class=LearnedArtifactIsolationClass.GOVERNED_SHARED,
            status=LearnedArtifactStatus.SHADOW,
            permitted_tenant_ids=frozenset({TENANT_A, TENANT_B}),
            permitted_purposes=frozenset({"routing"}),
            lineage=(_lineage(TENANT_A), _lineage(TENANT_B)),
            training_procedure_ref="procedure:v1",
            deletion_contract="retrain_and_replace",
        )


def test_unknown_provider_state_cannot_claim_noninterference() -> None:
    with pytest.raises(ValidationError, match="cannot claim"):
        LearnedArtifactManifest(
            artifact_id="opaque-provider-model",
            version="v1",
            kind=LearnedArtifactKind.MODEL,
            isolation_class=LearnedArtifactIsolationClass.UNKNOWN_UNBOUNDED,
            status=LearnedArtifactStatus.ACTIVE,
            permitted_tenant_ids=frozenset({TENANT_A}),
            permitted_purposes=frozenset({"reasoning"}),
            lineage=(_lineage(),),
            training_procedure_ref="provider:unknown",
            deletion_contract="provider_unknown",
            supported_guarantees=frozenset({"tenant_noninterference"}),
        )


def test_terminal_tenant_influence_disposition_requires_evidence() -> None:
    with pytest.raises(ValidationError, match="requires evidence"):
        TenantInfluenceLineage(
            lineage_id="lineage-1",
            tenant_id=TENANT_A,
            purpose="calibration",
            source_artifact_ids=("labels",),
            contribution_class="calibration",
            authority_basis="contract",
            permitted_from=datetime.now(UTC) - timedelta(days=2),
            permitted_until=datetime.now(UTC) - timedelta(days=1),
            disposition=TenantInfluenceDisposition.UNLEARNED,
        )
