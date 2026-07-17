"""Schema and honest initial artifact for the P2 truth-kernel evaluation."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping

from lib.evaluation.epistemic_repair.p2_oracles import P2CaseObservation, P2_GATE_IDS, evaluate_gate, stable_digest
from lib.evaluation.epistemic_repair.p2_population import P2Population, build_p2_population


ARTIFACT_NAME = "epistemic-repair-p2-truth-kernel-v1.json"
ARTIFACT_SCHEMA_VERSION = "epistemic-repair-p2-truth-kernel-v1"


def build_p2_exit_artifact(
    *,
    population: P2Population | None = None,
    case_observations: Mapping[str, P2CaseObservation] | None = None,
    execution_status: str = "unrun",
) -> dict[str, Any]:
    """Build a report that never mistakes missing runtime evidence for success."""

    population = population or build_p2_population()
    observations = dict(case_observations or {})
    gates = {}
    for gate in P2_GATE_IDS:
        eligible = [case.case_id for case in population.cases if gate in case.expected_invariants]
        gates[gate] = json.loads(
            json.dumps(
                asdict(
                    evaluate_gate(
                        gate,
                        eligible_case_ids=eligible,
                        observations=observations,
                        expected_dispositions={
                            case.case_id: case.expected_disposition
                            for case in population.cases
                        },
                    )
                )
            )
        )
    family_counts = {
        family: len(population.family(family))
        for family in sorted({case.family for case in population.cases})
    }
    observed_count = sum(item.status == "observed" for item in observations.values())
    phase_exit_ready = (
        execution_status == "complete"
        and observed_count == len(population.cases)
        and all(result["status"] == "pass" for result in gates.values())
    )
    report = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "execution_status": execution_status,
        "population_version": population.version,
        "population_digest": population.digest,
        "population": {
            "case_count": len(population.cases),
            "race_scenario_count": len(population.races),
            "family_counts": family_counts,
        },
        "hard_gates": gates,
        "command_receipts": [],
        "truth_snapshots": [],
        "race_results": [
            {"scenario_id": race.scenario_id, "status": "unrun", "expected_outcome": race.expected_outcome}
            for race in population.races
        ],
        "continuous_metrics": {
            "evidence_lineage_coverage": None,
            "scope_precision": None,
            "relation_joint_accuracy": None,
            "semantic_duplicate_absorption": None,
            "active_wrapper_contamination": None,
            "active_unexplained_perfect_confidence_relation_rate": None,
            "lifecycle_transition_latency_ms": None,
            "background_repair_latency_ms": None,
        },
        "reader_cutover_coverage": None,
        "remaining_compatibility_debt": ["not measured: database-backed P2 run has not executed"],
        "missing_evidence": [
            "database-backed case observations",
            "transaction and race observations",
            "command receipts and before/after truth snapshots",
            "reader cutover inventory",
            "continuous metric measurements",
        ],
        "phase_exit_ready": phase_exit_ready,
    }
    report["artifact_content_digest"] = stable_digest({key: value for key, value in report.items() if key not in {"generated_at", "artifact_content_digest"}})
    return report


def write_p2_exit_artifact(report: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n")
    return path


__all__ = ["ARTIFACT_NAME", "ARTIFACT_SCHEMA_VERSION", "build_p2_exit_artifact", "write_p2_exit_artifact"]
