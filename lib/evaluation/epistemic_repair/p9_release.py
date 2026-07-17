"""Fail-closed P9 release-candidate evidence composition.

P9 does not judge company semantics itself.  It verifies that every phase's
sealed evidence belongs to one release candidate and that the phase verdicts
jointly authorize exactly one bounded release verdict.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "epistemic-repair-p9-release-candidate-v1"
REQUIRED_PHASES = tuple(f"p{index}" for index in range(9))
READY_VERDICT = "ready_for_bounded_internal_company_learning"
ALLOWED_VERDICTS = {
    READY_VERDICT,
    "mechanically_ready_semantically_insufficient",
    "memory_not_earned_simplification_required",
    "operationally_insufficient",
    "safety_or_truth_blocked",
    "insufficient_evidence",
}
CURRENT_EVIDENCE_CLASSES = {"integrated_current", "bounded_current"}


@dataclass(frozen=True)
class PhaseEvidence:
    phase: str
    path: Path
    expected_sha256: str
    evidence_class: str = "integrated_current"


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest_bytes(payload: bytes) -> str:
    return sha256(payload).hexdigest()


def _read_artifact(item: PhaseEvidence) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    record: dict[str, Any] = {
        "phase": item.phase,
        "path": str(item.path),
        "expected_sha256": item.expected_sha256,
        "evidence_class": item.evidence_class,
    }
    try:
        payload = item.path.read_bytes()
        actual = _digest_bytes(payload)
        record["actual_sha256"] = actual
        record["digest_verified"] = actual == item.expected_sha256
        parsed = json.loads(payload)
        if not isinstance(parsed, dict):
            raise ValueError("phase artifact root must be an object")
        record["reopened"] = True
        return parsed, record
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        record.update(
            reopened=False,
            digest_verified=False,
            error=f"{type(exc).__name__}: {exc}",
        )
        return None, record


def _hard_gate_values(artifact: Mapping[str, Any]) -> list[bool]:
    gates = artifact.get("hard_gates")
    if not isinstance(gates, Mapping):
        return []
    values: list[bool] = []
    for value in gates.values():
        if isinstance(value, bool):
            values.append(value)
        elif isinstance(value, Mapping) and isinstance(value.get("passed"), bool):
            values.append(bool(value["passed"]))
    return values


def _record_green(record: Mapping[str, Any]) -> bool:
    """Recompute phase health instead of trusting a summary-ready flag alone."""

    return bool(
        record.get("phase_ready")
        and record.get("hard_gate_count", 0) > 0
        and record.get("all_declared_hard_gates_green")
    )


def _phase_ready(phase: str, artifact: Mapping[str, Any]) -> bool:
    if phase == "p0":
        return bool(artifact.get("passed", artifact.get("baseline_complete", False)))
    return bool(artifact.get("phase_exit_ready", artifact.get("passed", False)))


def _metric_contract_complete(artifact: Mapping[str, Any]) -> bool:
    """Require provenance for declared P9 continuous metrics.

    Phases may omit a P9 metric block when they have no continuous metrics.
    Once declared, every metric must expose numerator, denominator, coverage,
    and uncertainty (which may explicitly be ``not_applicable``).
    """

    metrics = artifact.get("p9_continuous_metrics", [])
    if not isinstance(metrics, list):
        return False
    required = {"name", "value", "numerator", "denominator", "coverage", "uncertainty"}
    return all(isinstance(metric, Mapping) and required <= set(metric) for metric in metrics)


def _select_verdict(
    *,
    evidence_complete: bool,
    constitutional_green: bool,
    semantic_green: bool,
    memory_decision: str | None,
    operational_green: bool,
) -> str:
    if not evidence_complete:
        return "insufficient_evidence"
    if not constitutional_green:
        return "safety_or_truth_blocked"
    if memory_decision in {"not_earned", "limited_compression_value"}:
        return "memory_not_earned_simplification_required"
    if not operational_green:
        return "operationally_insufficient"
    if not semantic_green or memory_decision != "primary_memory_earned":
        return "mechanically_ready_semantically_insufficient"
    return READY_VERDICT


def build_release_report(
    *,
    release_commit: str,
    worktree_clean: bool,
    evidence: Iterable[PhaseEvidence],
) -> dict[str, Any]:
    items = list(evidence)
    by_phase = {item.phase: item for item in items}
    duplicate_phases = len(by_phase) != len(items)
    missing_phases = sorted(set(REQUIRED_PHASES) - set(by_phase))
    records: list[dict[str, Any]] = []
    artifacts: dict[str, dict[str, Any]] = {}

    for phase in REQUIRED_PHASES:
        item = by_phase.get(phase)
        if item is None:
            continue
        artifact, record = _read_artifact(item)
        records.append(record)
        if artifact is not None:
            artifacts[phase] = artifact
            record["artifact_commit"] = artifact.get("commit")
            record["commit_matches"] = artifact.get("commit") == release_commit
            gates = _hard_gate_values(artifact)
            record["hard_gate_count"] = len(gates)
            record["all_declared_hard_gates_green"] = bool(gates) and all(gates)
            record["phase_ready"] = _phase_ready(phase, artifact)
            record["continuous_metric_contract_complete"] = _metric_contract_complete(artifact)

    evidence_complete = (
        bool(release_commit)
        and worktree_clean
        and not duplicate_phases
        and not missing_phases
        and len(records) == len(REQUIRED_PHASES)
        and all(
            record.get("reopened")
            and record.get("digest_verified")
            and record.get("commit_matches")
            and record.get("continuous_metric_contract_complete")
            and record.get("evidence_class") in CURRENT_EVIDENCE_CLASSES
            for record in records
        )
    )
    constitutional_phases = {"p0", "p1", "p2", "p3", "p4"}
    constitutional_green = all(
        _record_green(record)
        for record in records
        if record["phase"] in constitutional_phases
    ) and constitutional_phases <= set(artifacts)
    semantic_green = all(
        next((_record_green(r) for r in records if r["phase"] == phase), False)
        for phase in ("p5", "p6")
    )
    operational_green = all(
        next((_record_green(r) for r in records if r["phase"] == phase), False)
        for phase in ("p7", "p8")
    )
    p7 = artifacts.get("p7", {})
    memory_decision = p7.get("strategic_decision")
    verdict = _select_verdict(
        evidence_complete=evidence_complete,
        constitutional_green=constitutional_green,
        semantic_green=semantic_green,
        memory_decision=memory_decision if isinstance(memory_decision, str) else None,
        operational_green=operational_green,
    )
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "commit": release_commit,
        "worktree_clean": worktree_clean,
        "required_phases": list(REQUIRED_PHASES),
        "missing_phases": missing_phases,
        "duplicate_phases": duplicate_phases,
        "phase_evidence": records,
        "evidence_classes": {
            category: [r["phase"] for r in records if r["evidence_class"] == category]
            for category in (
                "integrated_current",
                "bounded_current",
                "historical_falsifying",
                "unmeasured",
            )
        },
        "constitutional_green": constitutional_green,
        "semantic_green": semantic_green,
        "memory_decision": memory_decision,
        "operational_green": operational_green,
        "evidence_complete": evidence_complete,
        "verdict": verdict,
        "completion_authorized": verdict == READY_VERDICT,
        "scope_boundaries": {
            "task_autonomy": "excluded",
            "connector_transport": "excluded",
            "customer_value": "not_claimed",
            "future_large_real_provider_run": "separately_authorized",
        },
    }
    report["report_digest"] = _digest_bytes(_canonical_bytes(report))
    return report


def write_release_report(report: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


__all__ = [
    "ALLOWED_VERDICTS",
    "PhaseEvidence",
    "READY_VERDICT",
    "REQUIRED_PHASES",
    "SCHEMA_VERSION",
    "build_release_report",
    "write_release_report",
]
