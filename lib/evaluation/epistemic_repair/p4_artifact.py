"""P4 artifact schema and fail-closed scoring."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p4_population import build_p4_population


SCHEMA_VERSION = "epistemic-repair-p4-online-learning-v1"


def build_unrun_p4_artifact() -> dict[str, Any]:
    population = build_p4_population()
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "execution_status": "unrun",
        "population_version": population.version,
        "population_digest": population.digest,
        "population": {"batch_count": 6, "signals_per_batch": 20, "signal_count": 120},
        "batch_results": [],
        "hard_gates": {key: "unrun" for key in ("HG-10", "HG-11", "HG-12", "HG-13")},
        "component_checks": {},
        "continuous_metrics": {
            "selected_context_utilization": None,
            "late_actual_model_use_share": None,
            "late_unnecessary_historical_observation_use": None,
            "late_historical_observation_selected_count": None,
            "late_unnecessary_historical_observation_count": None,
            "immediate_attribution_coverage": None,
            "delayed_attribution_coverage": None,
            "causal_barrier_p95_seconds": None,
            "duplicate_refresh_key_processing_ratio": None,
            "optional_queue_growth_slope_after_drain": None,
        },
        "missing_evidence": ["PostgreSQL-backed P4 evaluation has not run"],
        "phase_exit_ready": False,
    }
    return _seal(report)


def _seal(report: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(report)
    result["artifact_content_digest"] = canonical_sha256(
        {k: v for k, v in result.items() if k not in {"generated_at", "artifact_content_digest"}}
    )
    return result


__all__ = ["SCHEMA_VERSION", "build_unrun_p4_artifact", "_seal"]
