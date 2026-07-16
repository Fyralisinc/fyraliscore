from __future__ import annotations

import json
import sys
from pathlib import Path

from lib.architecture_registry import load_architecture_registry
from lib.evaluation.proof import (
    CANONICAL_COMPONENT_PARTITION_DIMENSION,
    CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    EvidenceTier,
    FateDenominatorRecord,
    InvariantEvidenceManifest,
    InvariantRunEvidence,
    MetricObservation,
)
from scripts import report_objective_evaluation_state


ROOT = Path(__file__).resolve().parents[2]
REGISTRY = load_architecture_registry(ROOT / "architecture/registry.yaml")


def _manifest(component: str) -> InvariantEvidenceManifest:
    invariant = next(
        item for item in REGISTRY.invariants if item.invariant_id == "INV-16"
    )
    assert invariant.proof is not None
    metric_id = invariant.proof.continuous_metric_ids[0]
    artifact_ref = f"artifact://{component}"
    evidence = InvariantRunEvidence(
        invariant_id="INV-16",
        applicable_exposures=1,
        observed_trace_facts=frozenset(invariant.proof.mandatory_trace_facts),
        executed_scenario_ids=frozenset(invariant.proof.suite_and_scenario_ids),
        metric_observations=(
            MetricObservation(
                metric_id=metric_id,
                metric_version=f"{component}-v1",
                raw_numerator=1,
                raw_denominator=1,
                point_estimate=1,
                artifact_refs=(artifact_ref,),
            ),
        ),
        achieved_evidence_tier=EvidenceTier.E3,
        denominator=FateDenominatorRecord(
            denominator_id=f"report-run:INV-16:{component}",
            denominator_version=f"{component}-v1",
            population_definition_version=f"{component}-objects-v1",
            query_or_manifest_hash=f"query:{component}",
            source_or_oracle_population=1,
            production_accepted=1,
            eligible=1,
            attempted_or_committed=1,
            terminal_fates={"covered": 1},
            report_cutoff="2026-07-16T01:00:00Z",
            population_partition_dimension=(
                CANONICAL_COMPONENT_PARTITION_DIMENSION
            ),
            population_partition_value=component,
            population_partition_proof_ref=(
                CANONICAL_COMPONENT_PARTITION_PROOF_REF
            ),
        ),
        artifact_refs=(artifact_ref,),
    )
    return InvariantEvidenceManifest(
        manifest_version=f"{component}-evidence-v1",
        run_id="report-run",
        architecture_digest=REGISTRY.digest,
        system_version="system-v1",
        created_at="2026-07-16T01:01:00Z",
        experiment_manifest_ref="experiment://report-run/sealed-v1",
        evidence=(evidence,),
        artifact_refs=(artifact_ref,),
    )


def test_report_cli_aggregates_repeated_component_evidence(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    paths = []
    for component in ("intent", "agency"):
        path = tmp_path / f"{component}.json"
        path.write_text(_manifest(component).model_dump_json(indent=2))
        paths.append(path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "report_objective_evaluation_state.py",
            "--evidence",
            str(paths[0]),
            "--evidence",
            str(paths[1]),
            "--format",
            "json",
        ],
    )
    assert report_objective_evaluation_state.main() == 0
    payload = json.loads(capsys.readouterr().out)

    aggregation = payload["evidence_aggregation"]
    assert len(aggregation["source_manifest_digests"]) == 2
    assert aggregation["aggregation"][0]["mode"] == (
        "declared_disjoint_partition_union"
    )
    row = next(item for item in payload["records"] if item["invariant_id"] == "INV-16")
    assert row["applicable_exposures"] == 2
    assert payload["coverage"]["observed_exposures"] == 2
    assert payload["production_freeze_ready"] is False
