"""P4 P9 sidecar binding raw decision, outcome, refresh, barrier, and queue rows."""

from __future__ import annotations

import re
from typing import Any

from lib.contracts.kernel import canonical_sha256

_HEX40 = re.compile(r"[0-9a-f]{40}")
_CONTRACTS = {
    "causal_barrier_p95_seconds": ("<=", 30.0), "delayed_attribution_coverage": (">=", .90),
    "duplicate_refresh_key_processing_ratio": ("<=", 1.10), "immediate_attribution_coverage": ("=", 1.0),
    "late_actual_model_use_share": (">=", .70), "late_historical_observation_selected_count": (">=", 0.0),
    "late_unnecessary_historical_observation_count": (">=", 0.0),
    "late_unnecessary_historical_observation_use": ("<=", .10),
    "optional_queue_growth_slope_after_drain": ("<=", 0.0), "selected_context_utilization": (">=", .80),
}


def build_p4_p9_sidecar(*, report: dict[str, Any], commit: str, worktree_clean: bool) -> dict[str, Any]:
    if not _HEX40.fullmatch(commit) or not worktree_clean:
        raise ValueError("P4 P9 evidence requires a clean full release commit")
    raw = report.get("raw_p9_evidence")
    if not isinstance(raw, dict) or any(not isinstance(raw.get(key), list) for key in (
        "context_decisions", "outcomes", "refresh_jobs", "barrier_latencies_seconds", "queue_counts"
    )):
        raise ValueError("P4 raw member evidence is incomplete")
    if set(report.get("hard_gates", {})) != {"HG-10", "HG-11", "HG-12", "HG-13"}:
        raise ValueError("P4 hard gate set is incomplete")
    if set(report.get("continuous_metrics", {})) != set(_CONTRACTS):
        raise ValueError("P4 metric set is incomplete")
    source_digest = canonical_sha256(raw)
    gates = {key: bool(value) for key, value in report["hard_gates"].items()}
    metrics = []
    for name, value in sorted(report["continuous_metrics"].items()):
        operator, threshold = _CONTRACTS[name]
        passed = value >= threshold if operator == ">=" else value <= threshold if operator == "<=" else value == threshold
        metrics.append({"name": name, "numerator": float(value), "denominator": 1,
                        "value": float(value), "coverage": 1.0, "uncertainty": "not_applicable",
                        "status": "pass" if passed else "fail", "operator": operator,
                        "threshold": threshold, "source_artifact_digest": source_digest, "worst_cases": []})
    contributions = {
        "preregistered_contract_digest": canonical_sha256(_CONTRACTS),
        "gate_members": {key: {"source_member_digest": source_digest} for key in gates},
        "metric_members": {row["name"]: {"source_member_digest": source_digest} for row in metrics},
    }
    body = {"schema_version": "epistemic-repair-p4-p9-normalized-v1", "commit": commit,
            "phase_exit_ready": all(gates.values()) and all(x["status"] == "pass" for x in metrics),
            "hard_gates": gates, "p9_continuous_metrics": metrics,
            "p9_member_contributions": contributions,
            "run_provenance": {"git_commit": commit, "worktree_clean": True},
            "source_phase_artifact_digest": report.get("artifact_content_digest")}
    return {**body, "content_digest": canonical_sha256(body)}


__all__ = ["build_p4_p9_sidecar"]
