from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from services.workers.sage_topology_optimizer.worker import (
    RunReport,
    SessionOptimizationReport,
    _report_metrics,
)


@dataclass
class _OptimizerReport:
    metrics: dict[str, float]
    affordance_reinforces: int
    affordance_decays: int
    shortcut_creates_or_bumps: int
    shortcut_decays: int
    negative_memory_inserts: int
    region_refreshes: int
    question_policy_updates: int
    canonical_merge_candidates: tuple[dict, ...]
    canonical_split_candidates: tuple[dict, ...]
    canonical_promote_candidates: tuple[dict, ...]
    canonical_demote_candidates: tuple[dict, ...]


def test_report_metrics_flattens_optimizer_report() -> None:
    metrics = _report_metrics(
        _OptimizerReport(
            metrics={"useful_node_count": 4.0},
            affordance_reinforces=3,
            affordance_decays=1,
            shortcut_creates_or_bumps=2,
            shortcut_decays=0,
            negative_memory_inserts=5,
            region_refreshes=6,
            question_policy_updates=7,
            canonical_merge_candidates=({"kind": "merge"},),
            canonical_split_candidates=(),
            canonical_promote_candidates=({"kind": "promote"}, {"kind": "promote"}),
            canonical_demote_candidates=(),
        )
    )

    assert metrics["useful_node_count"] == 4.0
    assert metrics["affordance_reinforces"] == 3
    assert metrics["negative_memory_inserts"] == 5
    assert metrics["canonical_merge_candidates"] == 1
    assert metrics["canonical_promote_candidates"] == 2


def test_run_report_counts_completed_and_failed_sessions() -> None:
    tenant = uuid4()
    report = RunReport(
        sessions=[
            SessionOptimizationReport(
                tenant_id=tenant,
                inquiry_session_id=uuid4(),
                status="completed",
            ),
            SessionOptimizationReport(
                tenant_id=tenant,
                inquiry_session_id=uuid4(),
                status="failed",
                error="nope",
            ),
        ]
    )

    assert report.processed == 2
    assert report.completed == 1
    assert report.failed == 1

