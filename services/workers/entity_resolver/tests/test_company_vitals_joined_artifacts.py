from __future__ import annotations

import json
import os
from pathlib import Path

import asyncpg
import pytest

from lib.evaluation.company_learning import CompanyLearningEvaluationState
from lib.evaluation.proof import (
    EvidenceAggregationMode,
    EvidenceTier,
    InvariantEvidenceBundle,
    InvariantEvidenceManifest,
    InvariantProofMatrixReport,
)
from scripts.company_vitals import (
    build_vitals_from_report_dir,
    collect_db_trace_for_report_dir,
    write_vitals_artifacts,
)
from scripts.run_company_learning_pair_harness import run_pair_experiment
from scripts.run_company_learning_vitals_harness import (
    write_company_learning_report_shell,
)


pytestmark = pytest.mark.integration


async def test_real_company_vitals_joins_db_e3_and_typed_e4_artifacts(
    resolver_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    report_dir = tmp_path / "joined-company-vitals"
    run_id = "pytest-joined-company-vitals"
    system_version = "pytest-joined-system"
    payload = await run_pair_experiment(
        pool=resolver_db,
        output_dir=report_dir,
        run_id=run_id,
        system_version=system_version,
        llm_call_cost_usd=0.001,
    )
    selected = payload["report"]["pairs"][0]["adaptive"]
    tenant_id = selected["tenant_id"]
    observation_ids = (
        selected["lineage"]["training_observation_id"],
        selected["lineage"]["recurrence_observation_id"],
    )
    write_company_learning_report_shell(
        report_dir,
        run_id=run_id,
        system_version=system_version,
        tenant_id=tenant_id,
        observation_ids=observation_ids,
    )

    experiment_path = report_dir / "company_learning_scenario_evidence.json"
    sealed_path = tmp_path / "company_learning_scenario_evidence.sealed.json"
    experiment_path.replace(sealed_path)
    database_url = os.environ["DATABASE_URL"]
    baseline_trace = await collect_db_trace_for_report_dir(
        report_dir,
        database_url=database_url,
    )
    baseline = build_vitals_from_report_dir(
        report_dir,
        db_trace=baseline_trace,
    )
    sealed_path.replace(experiment_path)

    joined_trace = await collect_db_trace_for_report_dir(
        report_dir,
        database_url=database_url,
    )
    live = write_vitals_artifacts(
        report_dir,
        db_trace=joined_trace,
    )

    assert live.scorecard["vitals_measurement_profile"] == (
        "company_learning_only"
    )
    assert live.scorecard["overall_score"] is None
    assert live.scorecard["scored_vitals"] == 0
    assert live.scorecard["overall_score"] == baseline["overall_score"]
    assert live.scorecard["scored_vitals"] == baseline["scored_vitals"]
    assert live.scorecard["vitals"] == baseline["vitals"]

    evaluation_path = live.output_dir / "company_learning_evaluation.json"
    bundle_path = live.output_dir / "company_learning_evidence_bundle.json"
    assert evaluation_path.is_file()
    assert bundle_path.is_file()
    assert (
        live.output_dir / "company_learning_scenario_evidence.json"
    ).is_file()

    live_evaluation = json.loads(evaluation_path.read_text())
    base_manifest = InvariantEvidenceManifest.model_validate(
        live_evaluation["evidence_manifest"]
    )
    evidence_bundle = InvariantEvidenceBundle.model_validate(
        json.loads(bundle_path.read_text())
    )
    base_inv05 = next(
        row for row in base_manifest.evidence if row.invariant_id == "INV-05"
    )
    joined_inv05 = next(
        row for row in evidence_bundle.evidence if row.invariant_id == "INV-05"
    )
    inv05_aggregation = next(
        row
        for row in evidence_bundle.aggregation
        if row.invariant_id == "INV-05"
    )

    assert base_inv05.achieved_evidence_tier is EvidenceTier.E3
    assert base_inv05.executed_scenario_ids == frozenset()
    assert "inv.entity_corrective_memory_lift" not in {
        metric.metric_id for metric in base_inv05.metric_observations
    }
    assert joined_inv05.achieved_evidence_tier is EvidenceTier.E3
    assert joined_inv05.executed_scenario_ids == frozenset(
        {"ENTITY-CORRECTIVE-MEMORY-PAIR"}
    )
    assert "inv.entity_corrective_memory_lift" in {
        metric.metric_id for metric in joined_inv05.metric_observations
    }
    assert (
        inv05_aggregation.mode
        is EvidenceAggregationMode.DECLARED_DISJOINT_PARTITION_UNION
    )
    assert set(inv05_aggregation.population_partition_values) == {
        "entity_grounding",
        "corrective_memory_pair_experiment",
    }

    live_company_physics = live.scorecard["company_physics"]
    live_bundle_bytes = bundle_path.read_bytes()

    rerender = write_vitals_artifacts(report_dir)
    rerendered_evaluation = json.loads(evaluation_path.read_text())

    assert CompanyLearningEvaluationState.model_validate(
        rerendered_evaluation["state"]
    ) == CompanyLearningEvaluationState.model_validate(
        live_evaluation["state"]
    )
    assert InvariantEvidenceManifest.model_validate(
        rerendered_evaluation["evidence_manifest"]
    ) == InvariantEvidenceManifest.model_validate(
        live_evaluation["evidence_manifest"]
    )
    assert InvariantEvidenceBundle.model_validate(
        rerendered_evaluation["evidence_bundle"]
    ) == InvariantEvidenceBundle.model_validate(
        live_evaluation["evidence_bundle"]
    )
    assert InvariantProofMatrixReport.model_validate(
        rerendered_evaluation["invariant_proof"]
    ) == InvariantProofMatrixReport.model_validate(
        live_evaluation["invariant_proof"]
    )
    assert bundle_path.read_bytes() == live_bundle_bytes
    assert rerender.scorecard["company_physics"] == live_company_physics
