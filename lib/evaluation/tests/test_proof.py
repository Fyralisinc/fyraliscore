from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.proof import (
    CANONICAL_COMPONENT_PARTITION_DIMENSION,
    CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    EvidenceAggregationMode,
    EvidenceTier,
    FateDenominatorRecord,
    IncidentObservation,
    IncidentStatus,
    InvariantEvidenceManifest,
    InvariantRunEvidence,
    MetricObservation,
    SubstantiationState,
    aggregate_invariant_evidence_manifests,
    compile_invariant_proof_matrix,
    render_invariant_proof_markdown,
)


ROOT = Path(__file__).resolve().parents[3]
REGISTRY = load_architecture_registry(ROOT / "architecture" / "registry.yaml")


def _denominator(
    *,
    eligible: int = 1,
    attempted: int = 1,
    terminal: int = 1,
    excluded: int = 0,
    unknown: int = 0,
) -> FateDenominatorRecord:
    return FateDenominatorRecord(
        denominator_id="registry-items-v1",
        denominator_version="v1",
        population_definition_version="registry-item-projection-v1",
        query_or_manifest_hash="sha256:test-fixture",
        source_or_oracle_population=eligible,
        production_accepted=eligible,
        eligible=eligible,
        attempted_or_committed=attempted,
        terminal_fates={"equivalent": terminal} if terminal else {},
        nonterminal_fates={},
        excluded_by_preregistered_reason=excluded,
        unknown_or_untraced=unknown,
        successor_lineages=0,
        effective_heads=0,
        report_cutoff="2026-07-16T00:00:00Z",
    )


def _complete_evidence(
    invariant_id: str = "INV-42",
    *,
    incidents: tuple[IncidentObservation, ...] = (),
    violation_count: int = 0,
) -> InvariantRunEvidence:
    invariant = next(
        item for item in REGISTRY.invariants if item.invariant_id == invariant_id
    )
    assert invariant.proof is not None
    metric_id = invariant.proof.continuous_metric_ids[0]
    return InvariantRunEvidence(
        invariant_id=invariant_id,
        applicable_exposures=1,
        observed_trace_facts=frozenset(invariant.proof.mandatory_trace_facts),
        executed_scenario_ids=frozenset(invariant.proof.suite_and_scenario_ids),
        metric_observations=(
            MetricObservation(
                metric_id=metric_id,
                metric_version="v1",
                raw_numerator=1.0,
                raw_denominator=1.0,
                point_estimate=1.0,
                violation_count=violation_count,
                severity_mass=float(violation_count),
                artifact_refs=("artifact://metric",),
            ),
        ),
        incidents=incidents,
        achieved_evidence_tier=EvidenceTier(invariant.evidence_floor),
        denominator=_denominator(),
        artifact_refs=("artifact://run",),
    )


def _record(report, invariant_id: str):
    return next(row for row in report.records if row.invariant_id == invariant_id)


def _manifest(
    evidence: tuple[InvariantRunEvidence, ...],
    *,
    manifest_version: str,
    run_id: str = "component-run",
    system_version: str = "system-v1",
    experiment_manifest_ref: str = "experiment://sealed-v1",
) -> InvariantEvidenceManifest:
    return InvariantEvidenceManifest(
        manifest_version=manifest_version,
        run_id=run_id,
        architecture_digest=REGISTRY.digest,
        system_version=system_version,
        created_at="2026-07-16T00:00:00Z",
        experiment_manifest_ref=experiment_manifest_ref,
        evidence=evidence,
        artifact_refs=(f"artifact://{manifest_version}",),
    )


def _partitioned_evidence(
    invariant_id: str,
    *,
    component: str,
    denominator_id: str,
    achieved_tier: EvidenceTier | None = None,
) -> InvariantRunEvidence:
    evidence = _complete_evidence(invariant_id)
    denominator = evidence.denominator.model_copy(
        update={
            "denominator_id": denominator_id,
            "population_partition_dimension": (
                CANONICAL_COMPONENT_PARTITION_DIMENSION
            ),
            "population_partition_value": component,
            "population_partition_proof_ref": (
                CANONICAL_COMPONENT_PARTITION_PROOF_REF
            ),
        }
    )
    updates = {
        "denominator": denominator,
        "artifact_refs": (f"artifact://{component}",),
    }
    if achieved_tier is not None:
        updates["achieved_evidence_tier"] = achieved_tier
    return evidence.model_copy(update=updates)


def test_empty_run_preserves_all_rows_as_insufficient() -> None:
    report = compile_invariant_proof_matrix(REGISTRY, run_id="empty")

    assert len(report.records) == 42
    assert report.coverage.registered_row_coverage == 1.0
    assert report.coverage.executable_definition_rows == 42
    assert report.coverage.observed_rows == 0
    assert report.coverage.state_counts == {"insufficient": 42}
    assert not report.production_freeze_ready
    assert _record(report, "INV-01").proof_gaps == ("missing run evidence",)


def test_complete_row_is_substantiated_without_compensating_for_other_rows() -> None:
    report = compile_invariant_proof_matrix(
        REGISTRY,
        run_id="one-complete-row",
        evidence=(_complete_evidence(),),
    )

    row = _record(report, "INV-42")
    assert row.substantiation_state is SubstantiationState.SUBSTANTIATED
    assert row.required_trace_fact_coverage == 1.0
    assert row.denominator_coverage == 1.0
    assert row.scenario_execution_coverage == 1.0
    assert row.metric_observation_coverage == 1.0
    assert report.coverage.state_counts == {
        "insufficient": 41,
        "substantiated": 1,
    }
    assert not report.production_freeze_ready


def test_confirmed_incident_contradicts_even_with_complete_positive_evidence() -> None:
    incident = IncidentObservation(
        incident_id="incident-1",
        incident_class="architecture_duplicate_authority_or_drift",
        status=IncidentStatus.CONFIRMED,
        severity=5,
        summary="A checked projection silently diverged.",
        artifact_refs=("artifact://incident",),
    )
    report = compile_invariant_proof_matrix(
        REGISTRY,
        run_id="contradiction",
        evidence=(_complete_evidence(incidents=(incident,)),),
    )

    row = _record(report, "INV-42")
    assert row.substantiation_state is SubstantiationState.CONTRADICTED
    assert row.confirmed_incident_count == 1
    assert report.coverage.confirmed_incident_count == 1


def test_metric_violation_contradicts_without_incident_averaging() -> None:
    report = compile_invariant_proof_matrix(
        REGISTRY,
        run_id="metric-violation",
        evidence=(_complete_evidence(violation_count=1),),
    )
    assert _record(report, "INV-42").substantiation_state is SubstantiationState.CONTRADICTED
    assert report.coverage.violation_count == 1


def test_incomplete_survivor_denominator_is_insufficient_and_continuous() -> None:
    complete = _complete_evidence()
    incomplete = complete.model_copy(
        update={
            "applicable_exposures": 2,
            "denominator": _denominator(eligible=2, attempted=1, terminal=1),
        }
    )
    report = compile_invariant_proof_matrix(
        REGISTRY,
        run_id="survivors-only",
        evidence=(incomplete,),
    )

    row = _record(report, "INV-42")
    assert row.denominator_coverage == 0.5
    assert not row.denominator_complete
    assert row.substantiation_state is SubstantiationState.INSUFFICIENT
    assert report.coverage.exposure_coverage == 0.5


def test_zero_exposure_not_applicable_requires_explicit_closed_population() -> None:
    invariant = next(item for item in REGISTRY.invariants if item.invariant_id == "INV-31")
    assert invariant.proof is not None
    evidence = InvariantRunEvidence(
        invariant_id="INV-31",
        applicable_exposures=0,
        achieved_evidence_tier=EvidenceTier.E4,
        denominator=_denominator(eligible=0, attempted=0, terminal=0),
        not_applicable_reason="No processing opportunities in the sealed scope.",
        artifact_refs=("artifact://zero-population-manifest",),
    )
    report = compile_invariant_proof_matrix(
        REGISTRY,
        run_id="not-applicable",
        evidence=(evidence,),
    )
    assert _record(report, "INV-31").substantiation_state is SubstantiationState.NOT_APPLICABLE


def test_denominator_rejects_impossible_funnel() -> None:
    with pytest.raises(ValidationError, match="monotonically non-increasing"):
        FateDenominatorRecord(
            denominator_id="bad",
            denominator_version="v1",
            population_definition_version="v1",
            query_or_manifest_hash="hash",
            source_or_oracle_population=1,
            production_accepted=2,
            eligible=2,
            attempted_or_committed=1,
            terminal_fates={"done": 1},
            report_cutoff="2026-07-16T00:00:00Z",
        )


def test_duplicate_or_unregistered_run_evidence_is_rejected() -> None:
    evidence = _complete_evidence()
    with pytest.raises(ValueError, match="unique by invariant_id"):
        compile_invariant_proof_matrix(
            REGISTRY,
            run_id="duplicate",
            evidence=(evidence, evidence),
        )

    unknown = evidence.model_copy(update={"invariant_id": "INV-99"})
    with pytest.raises(ValueError, match="unregistered invariants"):
        compile_invariant_proof_matrix(
            REGISTRY,
            run_id="unknown",
            evidence=(unknown,),
        )


def test_component_manifests_are_aggregated_as_a_traced_disjoint_union() -> None:
    intent = _partitioned_evidence(
        "INV-16", component="intent", denominator_id="intent-commands"
    )
    agency = _partitioned_evidence(
        "INV-16",
        component="agency",
        denominator_id="agency-commands",
        achieved_tier=EvidenceTier.E3,
    )
    bundle = aggregate_invariant_evidence_manifests(
        (
            _manifest((intent,), manifest_version="intent-v1"),
            _manifest((agency,), manifest_version="agency-v1"),
        )
    )

    assert len(bundle.evidence) == 1
    combined = bundle.evidence[0]
    assert combined.invariant_id == "INV-16"
    assert combined.applicable_exposures == 2
    assert combined.denominator.eligible == 2
    assert combined.denominator.attempted_or_committed == 2
    assert combined.denominator.terminal_fates == {"equivalent": 2}
    assert combined.metric_observations[0].raw_numerator == 2
    assert combined.metric_observations[0].raw_denominator == 2
    assert combined.metric_observations[0].point_estimate == 1.0
    assert combined.achieved_evidence_tier is EvidenceTier.E3
    assert "does not inspect raw population member identities" in combined.uncertainty[-1]
    aggregation = bundle.aggregation[0]
    assert aggregation.mode is EvidenceAggregationMode.DECLARED_DISJOINT_PARTITION_UNION
    assert aggregation.source_denominator_ids == (
        "agency-commands",
        "intent-commands",
    ) or aggregation.source_denominator_ids == (
        "intent-commands",
        "agency-commands",
    )
    assert set(aggregation.population_partition_values) == {"intent", "agency"}

    report = compile_invariant_proof_matrix(
        REGISTRY,
        run_id=bundle.run_id,
        evidence=bundle.evidence,
    )
    assert _record(report, "INV-16").applicable_exposures == 2
    assert report.coverage.observed_exposures == 2


def test_aggregation_rejects_duplicate_or_incompatible_manifests() -> None:
    evidence = _partitioned_evidence(
        "INV-16", component="intent", denominator_id="intent-commands"
    )
    manifest = _manifest((evidence,), manifest_version="intent-v1")
    with pytest.raises(ValueError, match="duplicate evidence manifests"):
        aggregate_invariant_evidence_manifests((manifest, manifest))

    mismatched = _manifest(
        (evidence,),
        manifest_version="intent-v2",
        system_version="system-v2",
    )
    with pytest.raises(ValueError, match="mismatched system_version"):
        aggregate_invariant_evidence_manifests((manifest, mismatched))

    wrong_experiment = _manifest(
        (evidence,),
        manifest_version="intent-v3",
        experiment_manifest_ref="experiment://different",
    )
    with pytest.raises(ValueError, match="mismatched experiment_manifest_ref"):
        aggregate_invariant_evidence_manifests((manifest, wrong_experiment))


def test_aggregation_refuses_unpartitioned_or_non_disjoint_overlapping_rows() -> None:
    unpartitioned_a = _complete_evidence("INV-16")
    unpartitioned_b = unpartitioned_a.model_copy(
        update={
            "denominator": unpartitioned_a.denominator.model_copy(
                update={"denominator_id": "other"}
            ),
            "artifact_refs": ("artifact://other",),
        }
    )
    with pytest.raises(ValueError, match="without a declared mutually exclusive"):
        aggregate_invariant_evidence_manifests(
            (
                _manifest((unpartitioned_a,), manifest_version="one"),
                _manifest((unpartitioned_b,), manifest_version="two"),
            )
        )

    intent_a = _partitioned_evidence(
        "INV-16", component="intent", denominator_id="intent-a"
    )
    intent_b = _partitioned_evidence(
        "INV-16", component="intent", denominator_id="intent-b"
    )
    with pytest.raises(ValueError, match="repeats population partition values"):
        aggregate_invariant_evidence_manifests(
            (
                _manifest((intent_a,), manifest_version="one"),
                _manifest((intent_b,), manifest_version="two"),
            )
        )


def test_aggregation_rejects_reused_denominator_and_misaligned_cutoff() -> None:
    intent = _partitioned_evidence(
        "INV-16", component="intent", denominator_id="shared-denominator"
    )
    agency = _partitioned_evidence(
        "INV-16", component="agency", denominator_id="shared-denominator"
    )
    with pytest.raises(ValueError, match="same population may not be counted twice"):
        aggregate_invariant_evidence_manifests(
            (
                _manifest((intent,), manifest_version="one"),
                _manifest((agency,), manifest_version="two"),
            )
        )

    agency = agency.model_copy(
        update={
            "denominator": agency.denominator.model_copy(
                update={
                    "denominator_id": "agency-denominator",
                    "report_cutoff": "2026-07-17T00:00:00Z",
                }
            )
        }
    )
    with pytest.raises(ValueError, match="different report cutoffs"):
        aggregate_invariant_evidence_manifests(
            (
                _manifest((intent,), manifest_version="one"),
                _manifest((agency,), manifest_version="two"),
            )
        )


def test_markdown_report_keeps_coverage_and_row_gaps_visible() -> None:
    report = compile_invariant_proof_matrix(REGISTRY, run_id="rendered")
    rendered = render_invariant_proof_markdown(report)

    assert "Production-freeze ready: **no**" in rendered
    assert "Executable proof definitions | 42 | 42 | 100.0%" in rendered
    assert "| INV-01 | insufficient | E0/E4 |" in rendered
    assert "| 0.0% | missing run evidence |" in rendered
    assert "This report has no compensatory overall score" in rendered
