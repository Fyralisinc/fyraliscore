from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from uuid import uuid4

import pytest

from lib.architecture_registry import load_architecture_registry
from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    ArmLineageRefs,
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    CorrectiveMemoryArmResult,
    CorrectiveMemoryExperimentSpec,
    PairedRecurrenceResult,
    RecurrenceCaseKind,
    SealedArmExpectation,
    SealedRecurrenceCase,
    evaluate_corrective_memory_experiment,
)
from lib.evaluation.company_learning_assurance import (
    ActiveSurfacesAssurance,
    CompanyLearningAssuranceSummary,
    CorrectionAssurance,
    NegativeAssurance,
    PopulationAssurance,
    PositiveAssurance,
    RetentionAssurance,
    SlackAssurance,
)
from lib.evaluation.company_learning_active_surfaces import (
    ActiveLearningSurfacesEvidence,
    SEALED_ACTIVE_SURFACE_CLAIMS,
    SourceSalienceObservation,
    StructuredIdentitySurfaceObservation,
    evaluate_active_learning_surfaces,
)
from lib.evaluation.correction_assurance import (
    CorrectionRuntimeEvidence,
    build_correction_assurance,
)
from lib.evaluation.company_learning_population import (
    HeldOutPairObservation,
    build_exact_alias_heldout_population,
    evaluate_heldout_population,
)
from lib.evaluation.company_learning_retention import (
    CompanyLearningRetentionReport,
    RetentionBehavior,
    RetentionCaseSpec,
    RetentionHorizon,
    RetentionObservation,
    RetentionRunSpec,
    evaluate_company_learning_retention,
)
from lib.evaluation.tests.test_company_learning_assurance_contract import (
    _collision_assurance_from_evidence,
    _collision_evidence,
    _lifecycle_assurance_from_evidence,
    _lifecycle_evidence,
    _variant_assurance_from_evidence,
    _variant_evidence,
)
from lib.evaluation.proof import (
    CANONICAL_COMPONENT_PARTITION_DIMENSION,
    CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    EvidenceAggregationMode,
    EvidenceTier,
    FateDenominatorRecord,
    InvariantEvidenceManifest,
    InvariantRunEvidence,
)
from lib.evaluation.slack_reconstruction_gold import SlackGoldFamily
from scripts.company_vitals import (
    _collect_company_learning_evaluation,
    _company_learning_evidence_bundle,
    _executed_scenario_ids,
    _load_artifact_bundle,
    apply_db_trace_to_signal_rows,
    build_vitals_from_report_dir,
    write_vitals_artifacts,
)
from scripts.run_company_vitals_harness import main as vitals_main


def test_company_vitals_writes_artifacts_from_e2e_report(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path)

    result = write_vitals_artifacts(report_dir)

    assert result.output_dir == report_dir / "vitals"
    assert (result.output_dir / "vitals_scorecard.json").exists()
    assert (result.output_dir / "vitals_summary.md").exists()
    assert (result.output_dir / "signal_metabolism.jsonl").exists()
    assert (result.output_dir / "residual_trace.jsonl").exists()
    assert (result.output_dir / "coherence_repair_candidates.jsonl").exists()
    assert (result.output_dir / "retrieval_outcome_learning.jsonl").exists()
    assert (result.output_dir / "latent_gap_candidates.jsonl").exists()
    assert (result.output_dir / "graph_coherence.json").exists()
    assert (result.output_dir / "company_learning_evaluation.json").exists()

    scorecard = json.loads((result.output_dir / "vitals_scorecard.json").read_text())
    assert scorecard["status"] == "ok"
    assert scorecard["hard_failures"] == []
    assert scorecard["vitals"]["control_plane_health"]["score"] == 1.0
    assert (
        scorecard["vitals"]["retrieval_roi"]["metrics"]["useful_context_ratio"] == 0.75
    )
    assert scorecard["vitals"]["authority_safety"]["status"] == "not_observed"
    assert scorecard["company_physics"]["status"] == "not_observed"
    assert scorecard["company_physics"]["noncompensatory"] is True

    signal_rows = [
        json.loads(line)
        for line in (result.output_dir / "signal_metabolism.jsonl")
        .read_text()
        .splitlines()
    ]
    assert len(signal_rows) == 2
    assert signal_rows[0]["final_fate"] == "trace_unresolved"
    assert "artifact_only_trace_unresolved" in signal_rows[0]["leak_flags"]


def test_company_vitals_reports_hard_gate_failure(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path, pending_triggers=3)

    scorecard = build_vitals_from_report_dir(report_dir)

    assert scorecard["status"] == "fail"
    assert any(
        "trigger queue did not drain" in item for item in scorecard["hard_failures"]
    )
    assert scorecard["vitals"]["control_plane_health"]["status"] == "watch"


def test_company_physics_is_precise_and_does_not_change_overall_score(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    baseline = build_vitals_from_report_dir(report_dir)
    db_trace = {
        "available": True,
        "tenant_id": "tenant-1",
        "observation_count": 2,
        "by_observation": {},
        "company_learning_evaluation": {
            "available": True,
            "status": "substantiated",
            "state": {
                "status": "substantiated",
                "scope": {
                    "run_id": "synthetic-vitals",
                    "tenant_id": "tenant-1",
                },
                "learning_loop": {
                    "governed_alias_replay_exposures": 2,
                    "governed_alias_replays_resolved": 2,
                    "governed_alias_replay_resolution_rate": 1.0,
                    "grounding_to_interpretation_coverage": 1.0,
                    "one_model_cardinality_rate": 1.0,
                },
                "conversation_context": {"selection_count": 2},
                "entity_grounding": {"alias_replay_exposure_count": 2},
                "source_semantics": {"eligible_grounding_count": 2},
                "incident_counts": {},
                "proof_gaps": [],
                "artifact_refs": ["pytest://company-physics"],
            },
        },
    }

    scorecard = build_vitals_from_report_dir(report_dir, db_trace=db_trace)

    assert scorecard["vitals"].keys() == baseline["vitals"].keys()
    assert scorecard["overall_score"] == baseline["overall_score"]
    assert scorecard["scored_vitals"] == baseline["scored_vitals"]
    assert scorecard["total_vitals"] == baseline["total_vitals"]
    assert scorecard["score_coverage"] == baseline["score_coverage"]
    assert "company_physics" not in scorecard["vitals"]
    assert scorecard["company_physics"]["status"] == "substantiated"
    assert (
        scorecard["company_physics"]["learning_loop"][
            "governed_alias_replay_resolution_rate"
        ]
        == 1.0
    )
    assert (
        scorecard["company_physics"]["components"]["entity_grounding"][
            "alias_replay_exposure_count"
        ]
        == 2
    )


def test_company_physics_incidents_are_noncompensatory_hard_failures(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "by_observation": {},
        "company_learning_evaluation": {
            "available": True,
            "status": "contradicted",
            "state": {
                "status": "contradicted",
                "scope": {"run_id": "synthetic-vitals"},
                "learning_loop": {},
                "conversation_context": {},
                "entity_grounding": {},
                "source_semantics": {},
                "incident_counts": {"entity_grounding.unsafe_governed_alias_replay": 1},
                "proof_gaps": [],
                "artifact_refs": ["pytest://company-physics-incident"],
            },
        },
    }

    scorecard = build_vitals_from_report_dir(report_dir, db_trace=db_trace)

    assert scorecard["status"] == "fail"
    assert any(
        "unsafe_governed_alias_replay" in failure
        for failure in scorecard["hard_failures"]
    )


def test_combined_assurance_is_non_scoring_and_persists_for_rerender(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    baseline = build_vitals_from_report_dir(report_dir)
    source = _write_company_learning_assurance(report_dir)

    first = write_vitals_artifacts(report_dir)
    assurance = first.scorecard["company_physics"]["assurance_suite"]

    assert first.scorecard["overall_score"] == baseline["overall_score"]
    assert first.scorecard["scored_vitals"] == baseline["scored_vitals"]
    assert first.scorecard["vitals"].keys() == baseline["vitals"].keys()
    assert assurance["available"] is True
    assert assurance["status"] == "working"
    assert assurance["positive"]["adaptive_minus_frozen_correctness"] == 1.0
    assert assurance["negative"]["safety_incident_count"] == 0
    assert assurance["slack"]["metrics"]["correct_case_rate"] == 1.0
    assert assurance["population"]["runtime_support_rate"] == 0.25
    assert assurance["variant_population"]["status"] == "observed"
    assert assurance["variant_population"]["registry_pair_count"] == 24
    assert assurance["variant_population"]["observed_pair_count"] == 24
    assert (
        assurance["variant_population"]["mechanism_metrics"][
            "candidate_memory_mediated_success_rate"
        ]
        == 1.0
    )
    assert assurance["variant_collision"]["status"] == "observed"
    assert assurance["variant_collision"]["registry_pair_count"] == 16
    assert assurance["variant_collision"]["observed_pair_count"] == 16
    assert assurance["variant_collision"]["unsupported_case_count"] == 0
    assert assurance["variant_collision"]["source_native_observed_case_count"] == 2
    assert assurance["variant_collision"]["source_native_unsupported_case_count"] == 0
    assert (
        assurance["variant_collision"]["adaptive_safe_containment_rate"][
            "point_estimate"
        ]
        == 1.0
    )
    assert assurance["customer_lifecycle"]["status"] == "observed"
    assert assurance["customer_lifecycle"]["case_count"] == 8
    assert assurance["customer_lifecycle"]["observed_case_count"] == 8
    assert assurance["customer_lifecycle"]["violating_case_count"] == 0
    assert (
        assurance["customer_lifecycle"]["alias_interval_non_overlap_rate"][
            "point_estimate"
        ]
        == 1.0
    )
    assert assurance["active_surfaces"]["status"] == "observed"
    identity = assurance["active_surfaces"]["structured_identity"]
    assert identity["status"] == "observed"
    assert identity["observed_case_count"] == 6
    assert identity["violating_case_count"] == 0
    assert identity["governed_attachment_rate"]["point_estimate"] == 1.0
    salience = assurance["active_surfaces"]["source_salience"]
    assert salience["status"] == "observed"
    assert salience["observed_case_count"] == 5
    assert salience["violating_case_count"] == 0
    assert salience["salience_direction_rate"]["point_estimate"] == 1.0
    assert assurance["retention"]["status"] == "observed"
    assert assurance["retention"]["observed_observation_count"] == 14
    assert assurance["retention"]["overall_positive_retention_rate"] == 1.0
    assert assurance["retention"]["overall_forgetting_rate"] == 0.0
    assert assurance["retention"]["restart_survival_rate"] == 1.0
    assert assurance["retention"]["negative_control_safety_rate"] == 1.0
    assert assurance["retention"]["collision_control_safety_rate"] == 1.0
    assert assurance["retention"]["source_immutability_rate"] == 1.0
    assert assurance["retention"]["hard_safety_incident_rate"] == 0.0
    assert "scorecard" not in assurance["positive"]["component_digests"]
    persisted = first.output_dir / "company_learning_assurance_summary.json"
    assert persisted.exists()
    assert (
        "Combined assurance: working"
        in (first.output_dir / "vitals_summary.md").read_text()
    )
    assert (
        "Customer identity lifecycle: 8/8"
        in (first.output_dir / "vitals_summary.md").read_text()
    )
    assert (
        "Active structured identity: observed, 6/6"
        in (first.output_dir / "vitals_summary.md").read_text()
    )
    assert (
        "Active source salience: observed, 5/5"
        in (first.output_dir / "vitals_summary.md").read_text()
    )
    assert (
        "Learning retention: observed, overall=1.0000 "
        "(forgetting=0.0000, restart survival=1.0000)"
        in (first.output_dir / "vitals_summary.md").read_text()
    )
    assert (
        "Retention families: exact=1.0000, variant=1.0000, corrected=1.0000"
        in (first.output_dir / "vitals_summary.md").read_text()
    )
    assert (
        "Retention safety and integrity: negative=1.0000, collision=1.0000"
        in (first.output_dir / "vitals_summary.md").read_text()
    )

    source.unlink()
    rerender = write_vitals_artifacts(report_dir)
    assert (
        rerender.scorecard["company_physics"]["assurance_suite"]["summary_digest"]
        == assurance["summary_digest"]
    )


def test_tampered_combined_assurance_fails_closed_noncompensatorily(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    artifact = _write_company_learning_assurance(report_dir)
    payload = json.loads(artifact.read_text())
    payload["negative"]["adaptive_unsafe_count"] = 1
    payload["summary_digest"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "summary_digest"}
    )
    _write_json(artifact, payload)

    scorecard = build_vitals_from_report_dir(report_dir)
    assurance = scorecard["company_physics"]["assurance_suite"]

    assert scorecard["status"] == "fail"
    assert assurance["available"] is False
    assert assurance["status"] == "invalid"
    assert assurance["error"] == "assurance_artifact_invalid"
    assert any(
        "assurance artifact failed validation" in failure
        for failure in scorecard["hard_failures"]
    )


def test_stale_combined_assurance_component_fails_closed(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    artifact = _write_company_learning_assurance(report_dir)
    summary_payload = json.loads(artifact.read_text())
    negative_path = Path(summary_payload["artifact_paths"]["negative_evidence"])
    negative = json.loads(negative_path.read_text())
    negative["report"]["metrics"]["adaptive_unsafe_count"] = 1
    _write_json(negative_path, negative)

    scorecard = build_vitals_from_report_dir(report_dir)

    assert scorecard["status"] == "fail"
    assurance = scorecard["company_physics"]["assurance_suite"]
    assert assurance["status"] == "invalid"
    assert "digest" in assurance["detail"].lower()


def test_inconsistent_population_accounting_fails_even_with_new_digest(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    artifact = _write_company_learning_assurance(report_dir)
    payload = json.loads(artifact.read_text())
    payload["population"]["unsupported_case_count"] = 44
    payload["summary_digest"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "summary_digest"}
    )
    _write_json(artifact, payload)

    scorecard = build_vitals_from_report_dir(report_dir)

    assert scorecard["status"] == "fail"
    assurance = scorecard["company_physics"]["assurance_suite"]
    assert assurance["status"] == "invalid"
    assert "partition" in assurance["detail"].lower()


def test_resealed_population_raw_result_mismatch_fails_closed(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    artifact = _write_company_learning_assurance(report_dir)
    summary = json.loads(artifact.read_text())
    population_path = Path(summary["artifact_paths"]["population_evidence"])
    population = json.loads(population_path.read_text())
    observed = next(
        row
        for row in population["observations"]
        if row["execution_status"] == "observed"
    )
    observed["adaptive_correct"] = False
    population["evidence_digest"] = canonical_sha256(
        {key: value for key, value in population.items() if key != "evidence_digest"}
    )
    _write_json(population_path, population)
    new_evidence_digest = population["evidence_digest"]
    summary["population"]["component_digests"]["evidence"] = new_evidence_digest
    summary["component_digests"]["population_evidence"] = new_evidence_digest
    summary["summary_digest"] = canonical_sha256(
        {key: value for key, value in summary.items() if key != "summary_digest"}
    )
    _write_json(artifact, summary)

    scorecard = build_vitals_from_report_dir(report_dir)

    assert scorecard["status"] == "fail"
    assurance = scorecard["company_physics"]["assurance_suite"]
    assert assurance["status"] == "invalid"
    assert "raw pair assessments" in assurance["detail"]


def test_failed_combined_assurance_is_a_noncompensatory_hard_failure(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    _write_company_learning_assurance(
        report_dir,
        status="failed",
        blocking_failures=("negative safety incident: sealed-case",),
    )

    scorecard = build_vitals_from_report_dir(report_dir)

    assert scorecard["status"] == "fail"
    assert any(
        "negative safety incident: sealed-case" in failure
        for failure in scorecard["hard_failures"]
    )


def test_valid_corrective_memory_experiment_is_non_scoring_and_credits_scenarios(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    _write_json(
        report_dir / "run_config.json",
        {
            "system_version": "pytest-system-v1",
            "executed_scenario_ids": ["FORGED-SCENARIO"],
        },
    )
    baseline = build_vitals_from_report_dir(report_dir)
    _write_corrective_memory_experiment(
        report_dir,
        system_version="pytest-system-v1",
    )

    scorecard = build_vitals_from_report_dir(report_dir)
    experiment = scorecard["company_physics"]["experiments"][
        "corrective_memory_recurrence"
    ]

    assert scorecard["overall_score"] == baseline["overall_score"]
    assert scorecard["vitals"].keys() == baseline["vitals"].keys()
    assert experiment["available"] is True
    assert experiment["status"] == "observed"
    assert experiment["metrics"]["adaptive_correctness_rate"] == 1.0
    assert experiment["metrics"]["frozen_correctness_rate"] == 0.0
    assert experiment["metrics"]["adaptive_minus_frozen_correctness"] == 1.0
    assert experiment["hard_safety_incident_count"] == 0
    bundle = _load_artifact_bundle(report_dir)
    assert _executed_scenario_ids(bundle) == frozenset(
        {"ENTITY-CORRECTIVE-MEMORY-PAIR"}
    )
    assert "FORGED-SCENARIO" not in _executed_scenario_ids(bundle)


def test_corrective_memory_experiment_aggregates_into_canonical_proof(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    _write_json(
        report_dir / "run_config.json",
        {"system_version": "pytest-system-v1"},
    )
    _write_corrective_memory_experiment(
        report_dir,
        system_version="pytest-system-v1",
    )
    report_cutoff = "2026-07-16T00:00:00+00:00"
    base_manifest = InvariantEvidenceManifest(
        manifest_version="company-learning-evidence-v1",
        run_id="synthetic-vitals",
        architecture_digest="a" * 64,
        system_version="pytest-system-v1",
        created_at=report_cutoff,
        experiment_manifest_ref="pytest://experiment-manifest",
        evidence=(
            InvariantRunEvidence(
                invariant_id="INV-05",
                applicable_exposures=1,
                achieved_evidence_tier=EvidenceTier.E3,
                denominator=FateDenominatorRecord(
                    denominator_id="pytest:INV-05:entity-grounding",
                    denominator_version="pytest-v1",
                    population_definition_version="pytest-v1",
                    query_or_manifest_hash=canonical_sha256(
                        "entity-grounding-population"
                    ),
                    source_or_oracle_population=1,
                    production_accepted=1,
                    eligible=1,
                    attempted_or_committed=1,
                    terminal_fates={"resolved_existing": 1},
                    report_cutoff=report_cutoff,
                    population_partition_dimension=(
                        CANONICAL_COMPONENT_PARTITION_DIMENSION
                    ),
                    population_partition_value="entity_grounding",
                    population_partition_proof_ref=(
                        CANONICAL_COMPONENT_PARTITION_PROOF_REF
                    ),
                ),
                artifact_refs=("pytest://base-manifest",),
            ),
        ),
        artifact_refs=("pytest://base-manifest",),
    )

    evidence_bundle = _company_learning_evidence_bundle(
        base_manifest,
        artifact_bundle=_load_artifact_bundle(report_dir),
        report_cutoff=report_cutoff,
    )

    assert evidence_bundle is not None
    assert len(evidence_bundle.evidence) == 1
    evidence = evidence_bundle.evidence[0]
    assert evidence.executed_scenario_ids == frozenset(
        {"ENTITY-CORRECTIVE-MEMORY-PAIR"}
    )
    assert {metric.metric_id for metric in evidence.metric_observations} == {
        "inv.entity_corrective_memory_lift"
    }
    assert evidence.applicable_exposures == 3
    aggregation = evidence_bundle.aggregation[0]
    assert aggregation.mode is EvidenceAggregationMode.DECLARED_DISJOINT_PARTITION_UNION
    assert set(aggregation.population_partition_values) == {
        "entity_grounding",
        "corrective_memory_pair_experiment",
    }


def test_tampered_corrective_memory_experiment_fails_closed(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    _write_json(
        report_dir / "run_config.json",
        {"system_version": "pytest-system-v1"},
    )
    artifact_path = _write_corrective_memory_experiment(
        report_dir,
        system_version="pytest-system-v1",
    )
    payload = json.loads(artifact_path.read_text())
    payload["report"]["metrics"]["adaptive_correctness_rate"] = 0.0
    _write_json(artifact_path, payload)

    scorecard = build_vitals_from_report_dir(report_dir)
    experiment = scorecard["company_physics"]["experiments"][
        "corrective_memory_recurrence"
    ]

    assert experiment["available"] is False
    assert experiment["status"] == "invalid"
    assert experiment["scenario_ids"] == []
    assert experiment["error"] == "experiment_artifact_invalid"
    assert _executed_scenario_ids(_load_artifact_bundle(report_dir)) == frozenset()
    assert any(
        "failed validation" in gap for gap in scorecard["company_physics"]["proof_gaps"]
    )


def test_corrective_memory_experiment_persists_for_artifact_only_rerender(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    _write_json(
        report_dir / "run_config.json",
        {"system_version": "pytest-system-v1"},
    )
    source = _write_corrective_memory_experiment(
        report_dir,
        system_version="pytest-system-v1",
    )

    first = write_vitals_artifacts(report_dir)
    persisted = first.output_dir / "company_learning_scenario_evidence.json"
    assert persisted.exists()
    source.unlink()

    rerender = write_vitals_artifacts(report_dir)
    experiment = rerender.scorecard["company_physics"]["experiments"][
        "corrective_memory_recurrence"
    ]

    assert experiment["available"] is True
    assert experiment["metrics"]["adaptive_minus_frozen_correctness"] == 1.0
    assert _executed_scenario_ids(_load_artifact_bundle(report_dir)) == frozenset(
        {"ENTITY-CORRECTIVE-MEMORY-PAIR"}
    )


def test_artifact_only_rerender_rejects_invalid_saved_company_learning_evidence(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "by_observation": {},
        "company_learning_evaluation": {
            "available": True,
            "status": "insufficient",
            "observed_slice_health": "healthy",
            "evaluation_cutoff": "2026-07-16T01:00:00+00:00",
            "state": {
                "status": "insufficient",
                "observed_slice_health": "healthy",
                "scope": {"run_id": "synthetic-vitals"},
                "learning_loop": {
                    "governed_alias_replay_exposures": 1,
                    "governed_alias_replays_resolved": 1,
                },
                "conversation_context": {"selection_count": 1},
                "entity_grounding": {"alias_replay_exposure_count": 1},
                "source_semantics": {"eligible_grounding_count": 1},
                "incident_counts": {},
                "proof_gaps": ["E4 scenarios remain unexecuted."],
                "artifact_refs": ["pytest://frozen-company-learning"],
            },
            "evidence_manifest": {
                "manifest_version": "company-learning-evidence-v1",
                "run_id": "synthetic-vitals",
            },
            "invariant_proof": {"records": []},
        },
    }

    first = write_vitals_artifacts(report_dir, db_trace=db_trace)
    assert (first.output_dir / "company_learning_evidence_manifest.json").exists()
    second = write_vitals_artifacts(report_dir)
    rerendered = json.loads(
        (second.output_dir / "company_learning_evaluation.json").read_text()
    )

    assert rerendered["available"] is False
    assert rerendered["status"] == "unavailable"
    assert rerendered["error"] == "persisted_evaluation_invalid"
    assert second.scorecard["company_physics"]["status"] == "unavailable"
    assert not (second.output_dir / "company_learning_evidence_manifest.json").exists()


def test_custom_output_dir_is_the_persisted_evaluation_source(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    output_dir = tmp_path / "custom-vitals"
    unavailable = {
        "available": False,
        "status": "unavailable",
        "error": "sealed-test-unavailable",
        "proof_gaps": ["custom output marker"],
    }

    write_vitals_artifacts(
        report_dir,
        output_dir=output_dir,
        db_trace={
            "available": True,
            "by_observation": {},
            "company_learning_evaluation": unavailable,
        },
    )
    rerender = write_vitals_artifacts(
        report_dir,
        output_dir=output_dir,
    )

    persisted = json.loads(
        (output_dir / "company_learning_evaluation.json").read_text()
    )
    assert persisted == unavailable
    assert rerender.scorecard["company_physics"]["error"] == ("sealed-test-unavailable")
    assert "custom output marker" in rerender.scorecard["proof_gaps"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing_table",
    (
        "agency_command_results",
        "agency_canonical_events",
        "agency_outbox_records",
        "entity_aliases",
        "clarification_requests",
    ),
)
async def test_company_learning_preflight_reports_each_required_table(
    tmp_path: Path,
    monkeypatch,
    missing_table: str,
) -> None:
    async def table_exists(_conn, table: str) -> bool:
        return table != missing_table

    monkeypatch.setattr("scripts.company_vitals._table_exists", table_exists)

    result = await _collect_company_learning_evaluation(
        object(),
        report_path=tmp_path,
        bundle={
            "benchmark_summary": {},
            "storyline_scores": {},
            "run_summary": {"run_id": "preflight-run"},
            "run_config": {},
        },
        tenant_id=uuid4(),
        observation_ids=(uuid4(),),
    )

    assert result["available"] is False
    assert result["status"] == "unavailable"
    assert result["error"] == "required_tables_missing"
    assert result["missing_tables"] == [missing_table]


def test_company_vitals_projection_metabolism_flags_missing_entity_surfaces(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(
        tmp_path,
        projection_metabolism={
            "available": True,
            "status": "watch",
            "entity_projection_coverage_ratio": 0.5,
            "missing_entity_projection_families": ["commitments", "decisions"],
            "refresh_job_count": 12,
            "pending_refresh_jobs": 0,
            "failed_refresh_jobs": 0,
            "jobs_to_snapshots_ratio": 2.4,
        },
    )

    scorecard = build_vitals_from_report_dir(report_dir)

    projection = scorecard["vitals"]["projection_freshness"]
    assert projection["status"] == "watch"
    assert projection["metrics"]["entity_projection_coverage_ratio"] == 0.5
    assert projection["metrics"]["missing_entity_projection_families"] == [
        "commitments",
        "decisions",
    ]
    assert any(
        "Missing first-class projection surfaces" in gap
        for gap in projection["proof_gaps"]
    )


def test_company_vitals_db_trace_resolves_signal_metabolism(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "tenant_id": "tenant-1",
        "observation_count": 2,
        "table_presence": {"think_trigger_queue": True, "models": True},
        "summary": {"trace_coverage": 1.0},
        "by_observation": {
            "obs-1": {
                "triggers": [
                    {
                        "id": "trigger-1",
                        "observation_id": "obs-1",
                        "trigger_kind": "T1",
                        "completed_at": "2026-07-01T00:00:00+00:00",
                    }
                ],
                "think_runs": [
                    {
                        "id": "run-1",
                        "trigger_id": "trigger-1",
                        "status": "success",
                        "ops_applied": {
                            "context_use": {
                                "selected_model_ids": ["prior-model"],
                                "selected_observation_ids": ["obs-1"],
                                "referenced_model_ids": ["model-1"],
                                "referenced_observation_ids": ["obs-1"],
                            }
                        },
                    }
                ],
                "models": [
                    {
                        "id": "model-1",
                        "born_from_event_id": "obs-1",
                        "provenance": "born_from_event",
                    }
                ],
                "model_edges": [{"id": "edge-1", "created_by_event_id": "obs-1"}],
                "model_events": [{"id": "event-1", "model_id": "model-1"}],
                "projection_snapshots": [
                    {
                        "projection_name": "customer_health",
                        "subject_key": "atlas",
                        "source_model_ids": ["model-1"],
                    }
                ],
                "inquiry_outcome_events": [
                    {"id": "outcome-1", "event_type": "recommendation_acted_on"}
                ],
            },
            "obs-2": {
                "triggers": [
                    {
                        "id": "trigger-2",
                        "observation_id": "obs-2",
                        "trigger_kind": "T1",
                        "completed_at": "2026-07-01T00:00:00+00:00",
                    }
                ],
                "think_runs": [
                    {
                        "id": "run-2",
                        "trigger_id": "trigger-2",
                        "status": "success",
                    }
                ],
                "model_signal_readings": [
                    {
                        "id": "reading-1",
                        "model_id": "model-1",
                        "source_event_id": "obs-2",
                        "reading_kind": "confirm",
                    }
                ],
            },
        },
    }

    scorecard = build_vitals_from_report_dir(report_dir, db_trace=db_trace)

    metabolism = scorecard["vitals"]["metabolism_yield"]
    assert metabolism["status"] == "ok"
    assert metabolism["metrics"]["trace_resolved_signals"] == 2
    assert metabolism["metrics"]["fate_counts"]["decision_outcome_recorded"] == 1
    assert metabolism["metrics"]["fate_counts"]["evidence_attached"] == 1

    rows = apply_db_trace_to_signal_rows(
        [
            {"observation_id": "obs-1", "final_fate": "trace_unresolved"},
            {"observation_id": "obs-2", "final_fate": "trace_unresolved"},
        ],
        db_trace,
    )
    assert rows[0]["final_fate"] == "decision_outcome_recorded"
    assert rows[0]["projection_subjects"] == ["customer_health:atlas"]
    assert rows[1]["final_fate"] == "evidence_attached"


def test_company_vitals_emits_residual_repair_retrieval_and_latent_traces(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "tenant_id": "tenant-1",
        "observation_count": 2,
        "table_presence": {"think_trigger_queue": True, "think_runs": True},
        "summary": {"trace_coverage": 1.0},
        "by_observation": {
            "obs-1": {"triggers": []},
            "obs-2": {
                "triggers": [
                    {
                        "id": "trigger-2",
                        "observation_id": "obs-2",
                        "trigger_kind": "T1",
                        "completed_at": "2026-07-01T00:00:00+00:00",
                    }
                ],
                "think_runs": [
                    {
                        "id": "run-2",
                        "trigger_id": "trigger-2",
                        "status": "success",
                        "ops_applied": {
                            "context_use": {
                                "selected_model_ids": ["model-prior"],
                                "selected_observation_ids": ["obs-2"],
                                "referenced_model_ids": [],
                                "referenced_observation_ids": [],
                            }
                        },
                    }
                ],
                "omitted_evidence": [
                    {
                        "id": "omit-1",
                        "inquiry_session_id": "session-1",
                        "omission_reason": "budget",
                    }
                ],
            },
        },
    }

    result = write_vitals_artifacts(report_dir, db_trace=db_trace)

    residuals = _read_jsonl(result.output_dir / "residual_trace.jsonl")
    residual_kinds = {row["residual_kind"] for row in residuals}
    assert residual_kinds == {"valuable_unmodeled", "counterevidence_unattached"}

    repairs = _read_jsonl(result.output_dir / "coherence_repair_candidates.jsonl")
    repair_kinds = {row["candidate_kind"] for row in repairs}
    assert "skipped_metabolism_repair" in repair_kinds
    assert "residual_counterevidence_unattached" in repair_kinds

    retrieval = _read_jsonl(result.output_dir / "retrieval_outcome_learning.jsonl")
    obs2 = next(row for row in retrieval if row["observation_id"] == "obs-2")
    assert obs2["outcome_class"] == "no_durable_fate"
    assert obs2["learning_signal"] == "penalize_omission_or_packet_selection"

    latent = _read_jsonl(result.output_dir / "latent_gap_candidates.jsonl")
    assert all(row["supporting_residual_ids"] for row in latent)
    assert all(row["falsifier"] for row in latent)
    assert all(row["next_evidence_needed"] for row in latent)

    scorecard = json.loads((result.output_dir / "vitals_scorecard.json").read_text())
    assert (
        scorecard["vitals"]["residual_channel"]["metrics"]["residual_candidate_count"]
        == 2
    )
    assert (
        scorecard["vitals"]["retrieval_outcome_learning"]["metrics"][
            "learnable_retrieval_decisions"
        ]
        == 1
    )


def test_company_vitals_prefers_persisted_residual_lifecycle_rows(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "tenant_id": "tenant-1",
        "observation_count": 2,
        "table_presence": {
            "model_residual_evidence": True,
            "sage_latent_gap_hypotheses": True,
        },
        "summary": {"trace_coverage": 1.0},
        "by_observation": {
            "obs-1": {
                "triggers": [
                    {
                        "id": "trigger-1",
                        "observation_id": "obs-1",
                        "trigger_kind": "T1",
                    }
                ],
                "model_residual_evidence": [
                    {
                        "id": "residual-1",
                        "source_observation_id": "obs-1",
                        "residual_kind": "valuable_unmodeled",
                        "compact_summary": "Atlas blocker was not modeled.",
                        "reason": "model compression missed the blocker",
                        "status": "absorbed",
                        "absorption_object_kind": "model",
                        "absorption_object_id": "model-1",
                        "metadata": {"absorbed_by": "test"},
                    }
                ],
            },
            "obs-2": {
                "triggers": [
                    {
                        "id": "trigger-2",
                        "observation_id": "obs-2",
                        "trigger_kind": "T1",
                    }
                ],
                "model_residual_evidence": [
                    {
                        "id": "residual-2",
                        "source_observation_id": "obs-2",
                        "residual_kind": "counterevidence_unattached",
                        "compact_summary": "Future validation needs attachment.",
                        "reason": "counterevidence did not attach",
                        "status": "open",
                        "metadata": {},
                    }
                ],
                "sage_latent_gap_hypotheses": [
                    {
                        "id": "gap-1",
                        "gap_kind": "counterevidence_unattached",
                        "status": "candidate",
                        "residual_cluster_hash": "cluster-1",
                        "supporting_residual_ids": ["residual-2"],
                        "supporting_observation_ids": ["obs-2"],
                        "missing_evidence_statement": "Counterevidence lacks target.",
                        "falsifier": "A later trace attaches it.",
                        "next_evidence_needed": "Find target model.",
                        "confidence": 0.45,
                        "hypothesis_text": "Latent counterevidence target gap.",
                        "metadata": {"source": "test"},
                    }
                ],
            },
        },
    }

    result = write_vitals_artifacts(report_dir, db_trace=db_trace)

    signal_rows = _read_jsonl(result.output_dir / "signal_metabolism.jsonl")
    obs1 = next(row for row in signal_rows if row["observation_id"] == "obs-1")
    assert obs1["persisted_residual_ids"] == ["residual-1"]
    assert obs1["persisted_residual_statuses"] == ["absorbed"]

    residuals = _read_jsonl(result.output_dir / "residual_trace.jsonl")
    assert [row["residual_id"] for row in residuals] == ["residual-1", "residual-2"]
    absorbed = residuals[0]
    assert absorbed["status"] == "absorbed"
    assert absorbed["source"] == "model_residual_evidence"
    assert absorbed["absorption_object_kind"] == "model"

    repairs = _read_jsonl(result.output_dir / "coherence_repair_candidates.jsonl")
    repair_kinds = {row["candidate_kind"] for row in repairs}
    assert "residual_counterevidence_unattached" in repair_kinds
    assert "residual_valuable_unmodeled" not in repair_kinds

    latent = _read_jsonl(result.output_dir / "latent_gap_candidates.jsonl")
    persisted = next(row for row in latent if row["candidate_id"] == "gap-1")
    assert persisted["source"] == "sage_latent_gap_hypotheses"
    assert persisted["status"] == "candidate"
    assert persisted["supporting_residual_ids"] == ["residual-2"]
    assert persisted["status_semantics"] == "non_canonical_until_confirmed"


def test_company_vitals_cli_can_fail_on_hard_gates(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path, pending_triggers=1)

    exit_code = vitals_main(["--report-dir", str(report_dir), "--fail-on-hard-gates"])

    assert exit_code == 1
    assert (report_dir / "vitals" / "vitals_scorecard.json").exists()


def _write_report_dir(
    tmp_path: Path,
    *,
    pending_triggers: int = 0,
    projection_metabolism: dict | None = None,
) -> Path:
    report_dir = tmp_path / "synthetic-e2e-report"
    report_dir.mkdir()
    _write_json(
        report_dir / "run_summary.json",
        {
            "run_id": "synthetic-vitals",
            "tenant_id": "tenant-1",
            "signal_count": 2,
            "observation_count": 2,
            "active_models": 2,
            "pending_triggers": pending_triggers,
            "pending_post_commit_actions": 0,
            "dead_lettered_post_commit_actions": 0,
            "think_runs_success": 4,
            "think_runs_failed": 0,
            "context_use_distribution": {
                "model_context_used": 2,
                "graph_context_used": 1,
                "no_selected_context": 1,
            },
            "context_use_relation_contract": {
                "context_use_runs": 4,
                "graph_relation_contract_failed_runs": 0,
            },
            "post_commit_status": {"processed": 2, "dead_lettered": 0},
            "topology_optimizer_status": {"status": "drained", "failed": 0},
            "projection_metabolism": projection_metabolism or {},
            "graph_health": {
                "active_model_count": 2,
                "active_edge_count": 1,
                "isolated_model_ratio": 0.0,
                "largest_component_ratio": 1.0,
                "duplicate_directed_edge_count": 0,
                "exact_duplicate_natural_groups": 0,
                "orphan_edge_count": 0,
                "self_edge_count": 0,
            },
        },
    )
    _write_json(
        report_dir / "benchmark_summary.json",
        {
            "run_id": "synthetic-vitals",
            "tenant_id": "tenant-1",
            "status": "passed",
            "required_run_failures": [],
            "run_amplification": {
                "pending_triggers": pending_triggers,
                "think_runs_failed": 0,
                "think_runs_success": 4,
                "validation_error_count": 0,
            },
            "company_intelligence_scorecard": {
                "overall_score": 0.9,
                "interpretation": "synthetic healthy run",
                "proof_gaps": [],
                "dimensions": {
                    "adaptive_lifecycle": {"score": 0.9, "metrics": {}},
                    "compression": {
                        "score": 1.0,
                        "metrics": {"model_updates": 1, "model_inserts": 1},
                    },
                    "edge_intelligence": {"score": 1.0, "metrics": {}},
                    "efficiency": {
                        "score": 0.9,
                        "metrics": {"cost_per_signal_usd": 0.01},
                    },
                    "retrieval_usefulness": {
                        "score": 0.9,
                        "metrics": {"model_or_graph_context_use_score": 0.75},
                    },
                    "temporal_improvement": {
                        "score": 0.8,
                        "metrics": {
                            "future_validation_events": 2,
                            "future_validation_memory_touch_ops": 1,
                            "future_validation_model_or_graph_context_use_score": 1.0,
                        },
                    },
                },
                "product_value_evals": {
                    "overall_score": 0.88,
                    "proof_gaps": [],
                    "evals": {
                        "decision_impact": {"score": 0.8, "metrics": {}},
                        "experience_metabolism": {"score": 0.9, "metrics": {}},
                        "negative_learning": {"score": 0.9, "metrics": {}},
                        "prediction_lifecycle": {"score": 0.8, "metrics": {}},
                        "question_policy": {"score": 0.9, "metrics": {}},
                    },
                },
            },
        },
    )
    _write_jsonl(
        report_dir / "planned_signals.jsonl",
        [
            {
                "index": 0,
                "storyline_id": "atlas",
                "sequence": "atlas_wave_001",
                "family": "customer_escalation",
                "content": "Atlas renewal evidence is blocked.",
            },
            {
                "index": 1,
                "storyline_id": "atlas",
                "sequence": "atlas_future_validation_wave_002",
                "family": "future_validation",
                "content": "Atlas later confirms the renewal blocker.",
            },
        ],
    )
    _write_jsonl(
        report_dir / "signal_manifest.jsonl",
        [
            {
                "index": 0,
                "channel": "slack:customer-success",
                "family": "customer_escalation",
                "observation_id": "obs-1",
            },
            {
                "index": 1,
                "channel": "email:customer-success",
                "family": "future_validation",
                "observation_id": "obs-2",
            },
        ],
    )
    _write_jsonl(
        report_dir / "models.jsonl",
        [
            {
                "id": "model-1",
                "status": "active",
                "natural": "Atlas renewal blocker is evidence readiness.",
                "confidence": 0.8,
                "proposition_kind": "belief",
                "claim_role": "situation",
                "supporting_event_ids": ["obs-1"],
                "falsifier": {"kind": "explicit_contestation"},
            },
            {
                "id": "model-2",
                "status": "active",
                "natural": "Atlas blocker was later confirmed.",
                "confidence": 0.7,
                "proposition_kind": "belief",
                "claim_role": "fact",
                "supporting_event_ids": ["obs-2"],
                "falsifier": {"kind": "explicit_contestation"},
            },
        ],
    )
    _write_jsonl(
        report_dir / "model_edges.jsonl",
        [
            {
                "id": "edge-1",
                "status": "active",
                "edge_kind": "supports",
                "source_model_id": "model-2",
                "target_model_id": "model-1",
            }
        ],
    )
    return report_dir


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def _active_surfaces_evidence(
    *,
    run_id: str,
    system_version: str,
) -> ActiveLearningSurfacesEvidence:
    identity_observations = tuple(
        StructuredIdentitySurfaceObservation(
            case_id=case_id,
            expected_claims=SEALED_ACTIVE_SURFACE_CLAIMS[case_id],
            observed_claims=SEALED_ACTIVE_SURFACE_CLAIMS[case_id],
            claim_emitted=True,
            claim_preserved=True,
            preexisting_binding_attached=True,
            handler_created_authority=False,
            ingest_created_authority=False,
            forged_text_resolved=False,
            missing_binding_authoritative=False,
            cross_source_leak=False,
            cross_tenant_leak=False,
            source_observation_immutable=True,
            artifact_refs=(f"pytest://active-surfaces/{case_id}",),
        )
        for case_id in (
            "jira_project",
            "linear_issue_bundle",
            "google_drive_file",
            "google_drive_comment",
            "google_drive_revision",
            "gmail_thread",
        )
    )
    salience_values = {
        "settled_useful": (1.0, 2.0, True),
        "corrected": (2.0, 1.0, False),
        "pending": (1.0, 1.0, False),
        "foreign_tenant": (1.0, 1.0, False),
        "profile_load": (1.0, 1.0, False),
    }
    salience_observations = tuple(
        SourceSalienceObservation(
            case_id=case_id,
            baseline_salience=baseline,
            learned_salience=learned,
            credit_observed=credit_observed,
            foreign_tenant_learned=False,
            canonical_truth_immutable=True,
            grounding_truth_immutable=True,
            artifact_refs=(f"pytest://active-surfaces/{case_id}",),
        )
        for case_id, (baseline, learned, credit_observed) in salience_values.items()
    )
    return ActiveLearningSurfacesEvidence(
        run_id=run_id,
        system_version=system_version,
        created_at="2026-07-16T00:00:00+00:00",
        identity_observations=identity_observations,
        salience_observations=salience_observations,
        report=evaluate_active_learning_surfaces(
            identity_observations=identity_observations,
            salience_observations=salience_observations,
        ),
        artifact_refs=("pytest://active-surfaces/evidence",),
    )


def _retention_evidence(
    *,
    run_id: str,
    system_version: str,
) -> tuple[dict, CompanyLearningRetentionReport]:
    horizons = (
        RetentionHorizon(cycle_count=0, restart_count=0),
        RetentionHorizon(cycle_count=4, restart_count=1),
        RetentionHorizon(cycle_count=16, restart_count=2),
    )
    exact_ref = CanonicalEntityRef(type="customer", id="retention-exact")
    variant_ref = CanonicalEntityRef(type="customer", id="retention-variant")
    corrected_ref = CanonicalEntityRef(type="customer", id="retention-corrected")
    cases = (
        RetentionCaseSpec(
            case_id="retention-exact",
            behavior=RetentionBehavior.EXACT_ALIAS,
            family="exact_alias_positive",
            expected_ref=exact_ref,
            horizons=horizons,
            allowed_terminal_fates=(ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,),
        ),
        RetentionCaseSpec(
            case_id="retention-variant",
            behavior=RetentionBehavior.VARIANT_ALIAS,
            family="acronym_from_long_form",
            expected_ref=variant_ref,
            horizons=horizons,
            allowed_terminal_fates=(ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,),
            candidate_authorization_required=True,
        ),
        RetentionCaseSpec(
            case_id="retention-correction",
            behavior=RetentionBehavior.CORRECTED_ALIAS,
            family="authoritative_exact_correction",
            expected_ref=corrected_ref,
            horizons=(horizons[-1],),
            allowed_terminal_fates=(ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,),
            correction_authority_required=True,
        ),
        *(
            RetentionCaseSpec(
                case_id=f"retention-negative:{case_id}",
                behavior=RetentionBehavior.NEGATIVE_CONTROL,
                family=family,
                horizons=(horizons[-1],),
                allowed_terminal_fates=(ConsumerTerminalFate.REVIEW,),
            )
            for case_id, family in (
                ("contextual-non-entity", "contextual_phrase_negative"),
                ("unrelated-alias", "unrelated_negative_control"),
                ("same-surface-homonym", "homonym_local_association"),
                ("conflicting-source-hint", "conflicting_source_hint"),
            )
        ),
        *(
            RetentionCaseSpec(
                case_id=f"retention-collision:{case_id}",
                behavior=RetentionBehavior.COLLISION_CONTROL,
                family=family,
                horizons=(horizons[-1],),
                allowed_terminal_fates=(ConsumerTerminalFate.REVIEW,),
            )
            for case_id, family in (
                (
                    "heldout-variant-collision-00",
                    "same_type_acronym_collision",
                ),
                (
                    "heldout-variant-collision-06",
                    "punctuation_unicode_normalization_collision",
                ),
                (
                    "heldout-variant-collision-08",
                    "contextual_channel_local_nickname",
                ),
            )
        ),
    )
    spec = RetentionRunSpec(
        run_id=run_id,
        system_version=system_version,
        created_at="2026-07-16T00:00:00+00:00",
        cases=cases,
        artifact_refs=("pytest://retention/spec",),
    )
    observations = tuple(
        RetentionObservation(
            case_id=case.case_id,
            horizon=horizon,
            intervening_learning_count=horizon.cycle_count,
            consumer_fate=(
                ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
                if case.expected_ref is not None
                else ConsumerTerminalFate.REVIEW
            ),
            observed_ref=case.expected_ref,
            candidate_authorized=(
                True
                if case.behavior is RetentionBehavior.VARIANT_ALIAS
                else None
            ),
            correction_authoritative=(
                True
                if case.behavior is RetentionBehavior.CORRECTED_ALIAS
                else None
            ),
            source_observation_immutable=True,
            models_consistent=True,
            evidence_lineage_consistent=True,
            artifact_refs=(
                f"pytest://retention/{case.case_id}/{horizon.cycle_count}",
            ),
        )
        for case in cases
        for horizon in case.horizons
    )
    report = evaluate_company_learning_retention(
        spec=spec,
        observations=observations,
        artifact_refs=("pytest://retention/report",),
    )
    payload = {
        "spec": spec.model_dump(mode="json"),
        "observations": [
            observation.model_dump(mode="json") for observation in observations
        ],
        "report": report.model_dump(mode="json"),
        "report_digest": report.digest,
    }
    return payload, report


def _write_company_learning_assurance(
    report_dir: Path,
    *,
    status: Literal["working", "failed"] = "working",
    blocking_failures: tuple[str, ...] = (),
) -> Path:
    system_version = "unreported-system-version"
    repo_root = Path(__file__).resolve().parents[2]
    architecture_digest = load_architecture_registry(
        repo_root / "architecture" / "registry.yaml"
    ).digest
    implementation_plan_digest = hashlib.sha256(
        (
            repo_root
            / "docs"
            / "plans"
            / "revised-reality-belief-intent-system-implementation.md"
        ).read_bytes()
    ).hexdigest()
    positive_report = {
        "run_id": "synthetic-vitals:positive",
        "system_version": system_version,
        "status": "observed",
        "metrics": {
            "pair_count": 3,
            "adaptive_correctness_rate": 1.0,
            "frozen_correctness_rate": 0.0,
            "adaptive_minus_frozen_correctness": 1.0,
        },
        "incidents": [],
    }
    positive_report_digest = canonical_sha256(positive_report)
    positive_path = report_dir / "pytest-positive.json"
    _write_json(
        positive_path,
        {
            "report": positive_report,
            "report_digest": positive_report_digest,
        },
    )
    positive_evaluation = {
        "available": True,
        "status": "substantiated",
        "state": {
            "scope": {"run_id": "synthetic-vitals:positive"},
        },
        "evidence_manifest": {
            "run_id": "synthetic-vitals:positive",
            "system_version": system_version,
            "architecture_digest": architecture_digest,
        },
        "evidence_bundle": {
            "run_id": "synthetic-vitals:positive",
            "system_version": system_version,
            "architecture_digest": architecture_digest,
        },
    }
    positive_evaluation_path = report_dir / "pytest-positive-evaluation.json"
    _write_json(positive_evaluation_path, positive_evaluation)
    positive_bundle = {
        "bundle_version": "pytest-v1",
        "run_id": "synthetic-vitals:positive",
        "system_version": system_version,
        "architecture_digest": architecture_digest,
        "evidence": [],
    }
    positive_bundle_path = report_dir / "pytest-positive-bundle.json"
    _write_json(positive_bundle_path, positive_bundle)

    negative_report = {
        "run_id": "synthetic-vitals:negative",
        "system_version": system_version,
        "status": "observed",
        "metrics": {
            "pair_count": 4,
            "adaptive_unsafe_count": 0,
            "frozen_unsafe_count": 0,
        },
        "incidents": [],
    }
    negative_payload = {
        "plan_digest": "b" * 64,
        "report": negative_report,
    }
    negative_payload["evidence_digest"] = canonical_sha256(negative_payload)
    negative_path = report_dir / "pytest-negative.json"
    _write_json(negative_path, negative_payload)

    population = build_exact_alias_heldout_population(size=60)
    population_assignments = []
    population_cases = []
    population_pairs = []
    population_observations = []
    for case in population.cases:
        adaptive_tenant_id = uuid4()
        frozen_tenant_id = uuid4()
        adaptive_target_id = uuid4()
        frozen_target_id = uuid4()
        supported = case.entity_type == "customer"
        population_assignments.append(
            {
                "case_id": case.case_id,
                "logical_entity_type": case.entity_type,
                "runtime_entity_type": ("customer" if supported else None),
                "adaptive_tenant_id": str(adaptive_tenant_id),
                "frozen_tenant_id": str(frozen_tenant_id),
                "adaptive_target_id": str(adaptive_target_id),
                "frozen_target_id": str(frozen_target_id),
                "unsupported_reason": (
                    None if supported else "unsupported non-customer runtime"
                ),
            }
        )
        if not supported:
            population_observations.append(
                HeldOutPairObservation(
                    case_id=case.case_id,
                    execution_status="unsupported",
                    unsupported_reason="unsupported non-customer runtime",
                )
            )
            continue
        adaptive_ref = CanonicalEntityRef(
            type="customer",
            id=str(adaptive_target_id),
        )
        frozen_ref = CanonicalEntityRef(
            type="customer",
            id=str(frozen_target_id),
        )
        population_cases.append(
            SealedRecurrenceCase(
                case_id=case.case_id,
                case_version=case.case_version,
                kind=RecurrenceCaseKind.EXACT_ALIAS_POSITIVE,
                alias_surface=case.alias_surface,
                source_text_digest=canonical_sha256(case.recurrence_text),
                context_digest=case.digest,
                adaptive_expectation=SealedArmExpectation(
                    tenant_id=adaptive_tenant_id,
                    allowed_consumer_fates=(
                        ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
                    ),
                    expected_entity_ref=adaptive_ref,
                    expected_model_count=0,
                    autonomous_resolution_permitted=True,
                ),
                frozen_expectation=SealedArmExpectation(
                    tenant_id=frozen_tenant_id,
                    allowed_consumer_fates=(ConsumerTerminalFate.REVIEW,),
                    expected_entity_ref=frozen_ref,
                    expected_model_count=0,
                    autonomous_resolution_permitted=False,
                ),
                artifact_refs=(f"pytest://population/{case.case_id}",),
            )
        )
        pair = PairedRecurrenceResult(
            case_id=case.case_id,
            adaptive=CorrectiveMemoryArmResult(
                case_id=case.case_id,
                arm=CorrectiveMemoryArm.ADAPTIVE,
                tenant_id=adaptive_tenant_id,
                consumer_fate=ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
                resolved_entity_ref=adaptive_ref,
                decision_source="governed_exact_alias_replay",
                llm_call_count=0,
                latency_ms=10.0,
                estimated_cost_usd=0.0,
                source_semantic_admitted=False,
                lineage=ArmLineageRefs(
                    training_observation_id=uuid4(),
                    recurrence_observation_id=uuid4(),
                    clarification_request_id=uuid4(),
                    clarification_answer_digest=canonical_sha256(
                        {"case_id": case.case_id, "arm": "adaptive"}
                    ),
                    adjudicated_alias_id=uuid4(),
                    artifact_refs=("pytest://population/adaptive",),
                ),
            ),
            frozen=CorrectiveMemoryArmResult(
                case_id=case.case_id,
                arm=CorrectiveMemoryArm.FROZEN,
                tenant_id=frozen_tenant_id,
                consumer_fate=ConsumerTerminalFate.REVIEW,
                resolved_entity_ref=None,
                decision_source="llm",
                llm_call_count=1,
                latency_ms=20.0,
                estimated_cost_usd=0.001,
                source_semantic_admitted=False,
                lineage=ArmLineageRefs(
                    training_observation_id=uuid4(),
                    recurrence_observation_id=uuid4(),
                    clarification_request_id=uuid4(),
                    clarification_answer_digest=canonical_sha256(
                        {"case_id": case.case_id, "arm": "frozen"}
                    ),
                    adjudicated_alias_id=uuid4(),
                    artifact_refs=("pytest://population/frozen",),
                ),
            ),
            artifact_refs=(f"pytest://population/{case.case_id}",),
        )
        population_pairs.append(pair)
        population_observations.append(
            HeldOutPairObservation(
                case_id=case.case_id,
                adaptive_correct=True,
                frozen_correct=False,
                adaptive_unsafe=False,
                frozen_unsafe=False,
                adaptive_llm_calls=0,
                frozen_llm_calls=1,
                adaptive_latency_ms=10.0,
                frozen_latency_ms=20.0,
            )
        )
    population_spec = CorrectiveMemoryExperimentSpec(
        experiment_id="pytest-heldout-population",
        run_id="synthetic-vitals:population",
        system_version=system_version,
        created_at="2026-07-16T00:00:00+00:00",
        scenario_ids=("ENTITY-CORRECTIVE-MEMORY-HELDOUT-POPULATION",),
        company_foundation_digest=canonical_sha256(population_assignments),
        provider_behavior_digest=canonical_sha256("pytest-provider"),
        cases=tuple(population_cases),
        artifact_refs=("pytest://population/spec",),
    )
    population_experiment = evaluate_corrective_memory_experiment(
        spec=population_spec,
        pairs=tuple(population_pairs),
        artifact_refs=("pytest://population/report",),
    )
    population_report = evaluate_heldout_population(
        population=population,
        observations=tuple(population_observations),
        bootstrap_samples=200,
    )
    population_report_payload = population_report.model_dump(mode="json")
    execution_population = population.model_dump(mode="json")
    population_payload = {
        "run_id": "synthetic-vitals:population",
        "system_version": system_version,
        "registry_population_digest": population.digest,
        "execution_population": execution_population,
        "selected_case_ids": [case.case_id for case in population.cases],
        "assignments": population_assignments,
        "raw_pairs": [pair.model_dump(mode="json") for pair in population_pairs],
        "observations": [
            observation.model_dump(mode="json")
            for observation in population_observations
        ],
        "population_report": population_report_payload,
        "experiment_report": population_experiment.model_dump(mode="json"),
    }
    population_payload["evidence_digest"] = canonical_sha256(population_payload)
    population_path = report_dir / "pytest-population.json"
    _write_json(population_path, population_payload)

    slack_observations = [{"case_id": "pytest-slack"}]
    slack_observations_path = report_dir / "pytest-slack-observations.jsonl"
    _write_jsonl(slack_observations_path, slack_observations)
    slack_metrics = {
        "case_count": 9,
        "supported_case_count": 9,
        "supported_case_rate": 1.0,
        "correct_case_count": 9,
        "correct_case_rate": 1.0,
        "mean_sufficient_set_recall": 1.0,
        "complete_sufficient_set_rate": 1.0,
        "selected_context_precision": 1.0,
        "contamination_rate": 0.0,
        "reconstructability_rate": 1.0,
        "mean_topology_recall": 1.0,
        "edit_delete_correctness_rate": 1.0,
        "long_range_recall": 1.0,
        "cross_channel_recall": 1.0,
        "budget_adherence_rate": 1.0,
        "abstention_under_insufficiency_rate": 1.0,
        "family_metrics": {
            family.value: {
                "case_count": 1,
                "correct_case_rate": 1.0,
                "contamination_rate": 0.0,
                "mean_sufficient_set_recall": 1.0,
            }
            for family in SlackGoldFamily
        },
    }
    slack_report = {
        "run_id": "synthetic-vitals:slack",
        "system_version": system_version,
        "status": "observed",
        "gold_manifest_digest": "c" * 64,
        "observation_digest": canonical_sha256(slack_observations),
        "metrics": slack_metrics,
        "assessments": [],
        "proof_gaps": ("Synthetic Slack gold is not open-world evidence.",),
        "artifact_refs": ("pytest://slack",),
    }
    slack_report_path = report_dir / "pytest-slack-report.json"
    _write_json(
        slack_report_path,
        {
            "report": slack_report,
            "report_digest": canonical_sha256(slack_report),
        },
    )
    correction_runtime_evidence = CorrectionRuntimeEvidence(
        expected_dependency_refs=("model:corrected",),
        discovered_dependency_refs=("model:corrected",),
        expected_immediate_fence_refs=("model:dependent",),
        immediate_fence_refs=("model:dependent",),
        expected_direct_repair_refs=("model:dependent",),
        direct_repair_refs=("model:dependent",),
        expected_recursive_repair_refs=("model:recursive",),
        recursive_repair_refs=("model:recursive",),
        expected_relation_retirement_refs=("relation:corrected",),
        relation_retirement_refs=("relation:corrected",),
        expected_projection_invalidation_refs=("projection:stale",),
        projection_invalidation_refs=("projection:stale",),
        expected_projection_rebuild_refs=("projection:fresh",),
        projection_rebuild_refs=("projection:fresh",),
        source_before_digest="d" * 64,
        source_after_digest="d" * 64,
        artifact_refs=("pytest://correction",),
    )
    correction_artifact = build_correction_assurance(
        run_id="synthetic-vitals:correction",
        system_version=system_version,
        created_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
        runtime_evidence=correction_runtime_evidence,
        artifact_refs=("pytest://correction",),
    )
    correction_path = report_dir / "pytest-correction.json"
    _write_json(correction_path, correction_artifact.artifact_payload())
    variant_evidence = _variant_evidence(
        run_id="synthetic-vitals:variant",
        system_version=system_version,
    )
    variant_path = report_dir / "pytest-variant-population.json"
    _write_json(variant_path, variant_evidence.artifact_payload())
    collision_evidence = _collision_evidence(
        run_id="synthetic-vitals:collision",
        system_version=system_version,
    )
    collision_path = report_dir / "pytest-variant-collision.json"
    _write_json(collision_path, collision_evidence.artifact_payload())
    lifecycle_evidence = _lifecycle_evidence(
        run_id="synthetic-vitals:customer-lifecycle",
        system_version=system_version,
    )
    lifecycle_path = report_dir / "pytest-customer-lifecycle.json"
    _write_json(lifecycle_path, lifecycle_evidence.artifact_payload())
    active_surfaces_evidence = _active_surfaces_evidence(
        run_id="synthetic-vitals:active-surfaces",
        system_version=system_version,
    )
    active_surfaces_path = report_dir / "pytest-active-surfaces.json"
    _write_json(
        active_surfaces_path,
        active_surfaces_evidence.artifact_payload(),
    )
    retention_payload, retention_report = _retention_evidence(
        run_id="synthetic-vitals:retention",
        system_version=system_version,
    )
    retention_path = report_dir / "pytest-retention.json"
    _write_json(retention_path, retention_payload)
    artifact_paths = {
        "positive_pair": str(positive_path),
        "positive_company_learning_evaluation": str(positive_evaluation_path),
        "positive_company_learning_evidence_bundle": str(positive_bundle_path),
        "negative_evidence": str(negative_path),
        "population_evidence": str(population_path),
        "correction_evidence": str(correction_path),
        "variant_population_evidence": str(variant_path),
        "variant_collision_evidence": str(collision_path),
        "customer_lifecycle_evidence": str(lifecycle_path),
        "active_surfaces_evidence": str(active_surfaces_path),
        "retention_evidence": str(retention_path),
        "slack_observations": str(slack_observations_path),
        "slack_report": str(slack_report_path),
    }
    variant_assurance = _variant_assurance_from_evidence(
        variant_evidence,
        path=artifact_paths["variant_population_evidence"],
    )
    collision_assurance = _collision_assurance_from_evidence(
        collision_evidence,
        path=artifact_paths["variant_collision_evidence"],
    )
    lifecycle_assurance = _lifecycle_assurance_from_evidence(
        lifecycle_evidence,
        path=artifact_paths["customer_lifecycle_evidence"],
    )
    active_surface_component_digests = {
        "evidence": active_surfaces_evidence.digest,
        "report": active_surfaces_evidence.report.digest,
        "structured_identity_report": (
            active_surfaces_evidence.report.structured_identity.digest
        ),
        "source_salience_report": (
            active_surfaces_evidence.report.source_salience.digest
        ),
        "identity_observations": canonical_sha256(
            [
                row.model_dump(mode="json")
                for row in active_surfaces_evidence.identity_observations
            ]
        ),
        "salience_observations": canonical_sha256(
            [
                row.model_dump(mode="json")
                for row in active_surfaces_evidence.salience_observations
            ]
        ),
    }
    active_surfaces_assurance = ActiveSurfacesAssurance(
        status="observed",
        evidence_tier=EvidenceTier.E4,
        structured_identity=active_surfaces_evidence.report.structured_identity,
        source_salience=active_surfaces_evidence.report.source_salience,
        artifact_paths={
            "active_surfaces_evidence": artifact_paths[
                "active_surfaces_evidence"
            ]
        },
        component_digests=active_surface_component_digests,
    )
    retention_component_digests = {
        "artifact": canonical_sha256(retention_payload),
        "spec": retention_report.spec_digest,
        "report": retention_report.digest,
        "observations": retention_report.observation_digest,
    }
    retention_assurance = RetentionAssurance(
        status="observed",
        evidence_tier=EvidenceTier.E4,
        expected_observation_count=retention_report.expected_observation_count,
        observed_observation_count=retention_report.observed_observation_count,
        exact_retention_rate=retention_report.exact_retention_rate,
        variant_retention_rate=retention_report.variant_retention_rate,
        corrected_retention_rate=retention_report.corrected_retention_rate,
        overall_positive_retention_rate=(
            retention_report.overall_positive_retention_rate
        ),
        overall_forgetting_rate=retention_report.overall_forgetting_rate,
        restart_survival_rate=retention_report.restart_survival_rate,
        correction_authority_rate=retention_report.correction_authority_rate,
        unsafe_globalization_rate=retention_report.unsafe_globalization_rate,
        negative_control_safety_rate=(
            retention_report.negative_control_safety_rate
        ),
        collision_control_safety_rate=(
            retention_report.collision_control_safety_rate
        ),
        source_immutability_rate=retention_report.source_immutability_rate,
        model_consistency_rate=retention_report.model_consistency_rate,
        evidence_lineage_consistency_rate=(
            retention_report.evidence_lineage_consistency_rate
        ),
        hard_safety_incident_rate=retention_report.hard_safety_incident_rate,
        retention_horizon_auc=retention_report.retention_horizon_auc,
        horizon_metrics=retention_report.horizon_metrics,
        family_counts=retention_report.family_counts,
        artifact_paths={
            "retention_evidence": artifact_paths["retention_evidence"]
        },
        component_digests=retention_component_digests,
    )
    summary = CompanyLearningAssuranceSummary(
        run_id="synthetic-vitals",
        system_version=system_version,
        architecture_digest=architecture_digest,
        implementation_plan_digest=implementation_plan_digest,
        created_at="2026-07-16T00:00:00+00:00",
        status=status,
        positive=PositiveAssurance(
            status="observed",
            pair_count=3,
            adaptive_correctness_rate=1.0,
            frozen_correctness_rate=0.0,
            adaptive_minus_frozen_correctness=1.0,
            hard_failures=(),
            artifact_paths={
                key: value
                for key, value in artifact_paths.items()
                if key.startswith("positive_")
            },
            component_digests={
                "report": positive_report_digest,
                "company_learning_evaluation": canonical_sha256(positive_evaluation),
                "company_learning_evidence_bundle": canonical_sha256(positive_bundle),
            },
        ),
        negative=NegativeAssurance(
            status="observed",
            pair_count=4,
            safety_incident_count=0,
            adaptive_unsafe_count=0,
            frozen_unsafe_count=0,
            artifact_paths={"negative_evidence": artifact_paths["negative_evidence"]},
            component_digests={
                "evidence": negative_payload["evidence_digest"],
                "report": canonical_sha256(negative_report),
                "plan": negative_payload["plan_digest"],
            },
        ),
        slack=SlackAssurance(
            status="observed",
            metrics=slack_metrics,
            evidence_tier=EvidenceTier.E4,
            scope_complete=True,
            open_world_complete=False,
            blocking_for_active_slice=True,
            artifact_paths={
                "slack_observations": artifact_paths["slack_observations"],
                "slack_report": artifact_paths["slack_report"],
            },
            component_digests={
                "report": canonical_sha256(slack_report),
                "gold_manifest": slack_report["gold_manifest_digest"],
                "observations": canonical_sha256(slack_observations),
            },
        ),
        correction=CorrectionAssurance(
            status=correction_artifact.status,
            evidence_tier=EvidenceTier.E3,
            expected_dependency_count=(
                correction_artifact.metrics.expected_dependency_count
            ),
            discovered_dependency_count=(
                correction_artifact.metrics.discovered_dependency_count
            ),
            dependency_discovery_rate=(
                correction_artifact.metrics.dependency_discovery_rate
            ),
            immediate_fence_rate=(correction_artifact.metrics.immediate_fence_rate),
            direct_repair_rate=correction_artifact.metrics.direct_repair_rate,
            recursive_repair_rate=(correction_artifact.metrics.recursive_repair_rate),
            relation_retirement_rate=(
                correction_artifact.metrics.relation_retirement_rate
            ),
            projection_invalidation_rate=(
                correction_artifact.metrics.projection_invalidation_rate
            ),
            projection_rebuild_rate=(
                correction_artifact.metrics.projection_rebuild_rate
            ),
            residual_unsafe_debt_count=(
                correction_artifact.metrics.residual_unsafe_debt_count
            ),
            convergence_ratio=correction_artifact.metrics.convergence_ratio,
            replay_idempotent=correction_artifact.metrics.replay_idempotent,
            source_immutable=correction_artifact.metrics.source_immutable,
            tenant_isolated=correction_artifact.metrics.tenant_isolated,
            converged=correction_artifact.metrics.converged,
            incidents=correction_artifact.incidents,
            artifact_paths={
                "correction_evidence": artifact_paths["correction_evidence"]
            },
            component_digests={
                "artifact": correction_artifact.digest,
                **correction_artifact.component_digests,
            },
        ),
        variant_population=variant_assurance,
        variant_collision=collision_assurance,
        customer_lifecycle=lifecycle_assurance,
        active_surfaces=active_surfaces_assurance,
        retention=retention_assurance,
        population=PopulationAssurance(
            status="observed_with_gaps",
            registry_pair_count=60,
            observed_pair_count=15,
            unsupported_case_count=45,
            runtime_support_rate=0.25,
            metrics={
                **population_report_payload,
                "safety_incident_count": 0,
            },
            unsupported_strata_counts=population_report_payload[
                "unsupported_strata_counts"
            ],
            unsupported_reason_counts=population_report_payload[
                "unsupported_reason_counts"
            ],
            artifact_paths={
                "population_evidence": artifact_paths["population_evidence"]
            },
            component_digests={
                "evidence": population_payload["evidence_digest"],
                "registry": population_payload["registry_population_digest"],
                "report": canonical_sha256(population_report_payload),
            },
        ),
        proof_gaps=("pytest assurance remains synthetic E4 evidence.",),
        blocking_failures=blocking_failures,
        component_digests={
            "positive_report": positive_report_digest,
            "positive_company_learning_evaluation": canonical_sha256(
                positive_evaluation
            ),
            "positive_company_learning_evidence_bundle": canonical_sha256(
                positive_bundle
            ),
            "negative_evidence": negative_payload["evidence_digest"],
            "negative_report": canonical_sha256(negative_report),
            "negative_plan": negative_payload["plan_digest"],
            "slack_report": canonical_sha256(slack_report),
            "slack_gold_manifest": slack_report["gold_manifest_digest"],
            "slack_observations": canonical_sha256(slack_observations),
            **{
                f"correction_{key}": value
                for key, value in {
                    "artifact": correction_artifact.digest,
                    **correction_artifact.component_digests,
                }.items()
            },
            **{
                f"variant_population_{key}": value
                for key, value in variant_assurance.component_digests.items()
            },
            **{
                f"variant_collision_{key}": value
                for key, value in collision_assurance.component_digests.items()
            },
            **{
                f"customer_lifecycle_{key}": value
                for key, value in lifecycle_assurance.component_digests.items()
            },
            **{
                f"active_surfaces_{key}": value
                for key, value in active_surfaces_assurance.component_digests.items()
            },
            **{
                f"retention_{key}": value
                for key, value in retention_assurance.component_digests.items()
            },
            "population_evidence": population_payload["evidence_digest"],
            "population_registry": population_payload["registry_population_digest"],
            "population_report": canonical_sha256(population_report_payload),
        },
        artifact_paths=artifact_paths,
    )
    artifact_path = report_dir / "company_learning_assurance_summary.json"
    _write_json(artifact_path, summary.artifact_payload())
    return artifact_path


def _write_corrective_memory_experiment(
    report_dir: Path,
    *,
    system_version: str,
) -> Path:
    adaptive_tenant_id = uuid4()
    frozen_tenant_id = uuid4()
    answer_digest = canonical_sha256({"answer": "accept-candidate"})
    adaptive_ref = CanonicalEntityRef(
        type="customer",
        id="nimbus-bank-adaptive",
    )
    frozen_ref = CanonicalEntityRef(
        type="customer",
        id="nimbus-bank-frozen",
    )
    case = SealedRecurrenceCase(
        case_id="held-out-renewal",
        case_version="v1",
        kind=RecurrenceCaseKind.EXACT_ALIAS_POSITIVE,
        alias_surface="NBI",
        source_text_digest=canonical_sha256("NBI renewal is delayed"),
        context_digest=canonical_sha256(
            {"channel": "C-RENEWAL", "thread": "T-HELD-OUT"}
        ),
        adaptive_expectation=SealedArmExpectation(
            tenant_id=adaptive_tenant_id,
            allowed_consumer_fates=(ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,),
            expected_entity_ref=adaptive_ref,
            expected_model_count=1,
            autonomous_resolution_permitted=True,
        ),
        frozen_expectation=SealedArmExpectation(
            tenant_id=frozen_tenant_id,
            allowed_consumer_fates=(ConsumerTerminalFate.REVIEW,),
            expected_entity_ref=frozen_ref,
            expected_model_count=0,
            autonomous_resolution_permitted=False,
        ),
        artifact_refs=("pytest://case/held-out-renewal",),
    )
    spec = CorrectiveMemoryExperimentSpec(
        experiment_id="pytest-corrective-memory-pair",
        run_id="synthetic-vitals",
        system_version=system_version,
        created_at="2026-07-16T00:00:00+00:00",
        scenario_ids=("ENTITY-CORRECTIVE-MEMORY-PAIR",),
        company_foundation_digest=canonical_sha256({"company": "pytest-foundation"}),
        provider_behavior_digest=canonical_sha256({"provider": "pytest-scripted"}),
        cases=(case,),
        artifact_refs=("pytest://experiment-spec",),
    )
    adaptive_model_id = uuid4()
    pair = PairedRecurrenceResult(
        case_id=case.case_id,
        adaptive=CorrectiveMemoryArmResult(
            case_id=case.case_id,
            arm=CorrectiveMemoryArm.ADAPTIVE,
            tenant_id=adaptive_tenant_id,
            consumer_fate=ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,
            resolved_entity_ref=adaptive_ref,
            decision_source="governed_exact_alias_replay",
            llm_call_count=0,
            latency_ms=5.0,
            estimated_cost_usd=0.0,
            source_semantic_admitted=True,
            lineage=ArmLineageRefs(
                training_observation_id=uuid4(),
                recurrence_observation_id=uuid4(),
                clarification_request_id=uuid4(),
                clarification_answer_digest=answer_digest,
                adjudicated_alias_id=uuid4(),
                grounding_trace_id=uuid4(),
                source_semantic_interpretation_id=uuid4(),
                source_semantic_admission_id=uuid4(),
                model_ids=(adaptive_model_id,),
                artifact_refs=("pytest://adaptive-lineage",),
            ),
        ),
        frozen=CorrectiveMemoryArmResult(
            case_id=case.case_id,
            arm=CorrectiveMemoryArm.FROZEN,
            tenant_id=frozen_tenant_id,
            consumer_fate=ConsumerTerminalFate.REVIEW,
            resolved_entity_ref=None,
            decision_source="llm",
            llm_call_count=1,
            latency_ms=20.0,
            estimated_cost_usd=0.01,
            source_semantic_admitted=False,
            lineage=ArmLineageRefs(
                training_observation_id=uuid4(),
                recurrence_observation_id=uuid4(),
                clarification_request_id=uuid4(),
                clarification_answer_digest=answer_digest,
                adjudicated_alias_id=uuid4(),
                grounding_trace_id=uuid4(),
                artifact_refs=("pytest://frozen-lineage",),
            ),
        ),
        artifact_refs=("pytest://pair/held-out-renewal",),
    )
    report = evaluate_corrective_memory_experiment(
        spec=spec,
        pairs=(pair,),
        artifact_refs=("pytest://corrective-memory-report",),
    )
    artifact_path = report_dir / "company_learning_scenario_evidence.json"
    _write_json(
        artifact_path,
        {
            "spec": spec.model_dump(mode="json"),
            "report": report.model_dump(mode="json"),
            "report_digest": report.digest,
        },
    )
    return artifact_path
