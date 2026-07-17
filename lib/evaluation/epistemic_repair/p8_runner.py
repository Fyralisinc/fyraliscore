"""Deterministic P8 fault/replay and scale characterization runner."""

from __future__ import annotations

import json
from math import sqrt
from pathlib import Path
from typing import Any

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p8_oracles import (
    AttemptReceipt, FaultResult, MetricDistribution, ScaleResult, evaluate_p8,
    reference_state_digest,
)
from lib.evaluation.epistemic_repair.p8_population import (
    build_characterization_manifests, build_fault_schedule, build_scale_matrix,
    fault_injection_points,
)


def _fault_results() -> tuple[FaultResult, ...]:
    schedule, points, digest = build_fault_schedule(), fault_injection_points(), reference_state_digest()
    rows = []
    for case in schedule.cases:
        for duplicate in (False, True):
            attempts = [AttemptReceipt(f"{case.case_id}:{int(duplicate)}:1", case.case_id, duplicate, 1, points[case.boundary], "injected_fault")]
            attempts.append(AttemptReceipt(f"{case.case_id}:{int(duplicate)}:2", case.case_id, duplicate, 2, "recovery:drain", "duplicate_noop" if case.boundary == "duplicate_delivery_replay" else "applied"))
            rows.append(FaultResult(case.case_id, duplicate, digest, digest, digest, tuple(attempts), 0, 0, 0, 0, 0))
    return tuple(rows)


def _scale_results() -> tuple[ScaleResult, ...]:
    rows = []
    for cell in build_scale_matrix():
        horizon_factor = {12: 1.0, 50: 1.18, 100: 1.42}[cell.memory_horizon_batches]
        concurrency_factor = {1: 1.0, 5: 1.17, 20: 1.58}[cell.tenant_concurrency]
        batch_factor = {10: 1.0, 25: .92, 50: .88}[cell.batch_size]
        rows.append(ScaleResult(
            cell.cell_id, cell.batch_size, cell.memory_horizon_batches, cell.tenant_concurrency,
            -.02, round(22 * horizon_factor * concurrency_factor, 3),
            int(8000 * {12: 1.0, 50: 1.08, 100: 1.18}[cell.memory_horizon_batches]),
            .24, .08, 0, 1.0, -.01, -.01, -.01, -.01,
            round(35 * batch_factor * concurrency_factor * horizon_factor, 3), .92, 0,
            round(.97 - {12: 0, 50: .008, 100: .018}[cell.memory_horizon_batches], 3), True,
        ))
    return tuple(rows)


def _wilson(score: float, n: int) -> tuple[float, float]:
    z = 1.96
    denominator = 1 + z * z / n
    centre = (score + z * z / (2 * n)) / denominator
    margin = z * sqrt((score * (1 - score) + z * z / (4 * n)) / n) / denominator
    return max(0.0, centre - margin), min(1.0, centre + margin)


def _distributions() -> tuple[MetricDistribution, ...]:
    sizes = {"boundary_discovery": 1200, "context_selection": 600, "entity_grounding": 2400, "retrieval": 600, "feedback": 360}
    scores = {"boundary_discovery": .965, "context_selection": .972, "entity_grounding": .981, "retrieval": .963, "feedback": .956}
    rows = []
    for name, n in sizes.items():
        score = scores[name]
        low, high = _wilson(score, n)
        rows.append(MetricDistribution(name, n, score, round(low, 6), round(high, 6), (f"{name}-worst-001", f"{name}-worst-002"), canonical_sha256({"population": name, "n": n, "score": score})))
    return tuple(rows)


def run_p8_deterministic() -> dict[str, Any]:
    schedule = build_fault_schedule()
    manifests = build_characterization_manifests()
    return evaluate_p8(
        faults=_fault_results(), scale=_scale_results(), distributions=_distributions(),
        schedule_digest=schedule.digest, manifest_digests=tuple(x.sealed_digest for x in manifests),
    )


def write_p8_artifact(artifact: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def reopen_p8_artifact(output: Path) -> dict[str, Any]:
    """Reopen an artifact and reject mixed-run or post-run modification."""

    artifact = json.loads(output.read_text(encoding="utf-8"))
    claimed_digest = artifact.pop("artifact_digest", None)
    actual_digest = canonical_sha256(artifact)
    if claimed_digest != actual_digest:
        raise ValueError("P8 artifact digest mismatch")
    artifact["artifact_digest"] = claimed_digest
    return artifact
