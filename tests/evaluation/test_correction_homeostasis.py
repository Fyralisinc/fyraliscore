from uuid import uuid4

import pytest

from lib.evaluation.correction_homeostasis import evaluate_correction_homeostasis
from services.domain.correction_propagation.service import CorrectionPropagationService


class _DependencyConnection:
    def __init__(self, adjacency):
        self.adjacency = adjacency

    async def fetch(self, _query, _tenant_id, causes, *_args):
        return [
            {"dependent_model_id": dependent, "cause_model_id": cause}
            for cause in causes
            for dependent in self.adjacency.get(cause, ())
        ]


@pytest.mark.asyncio
async def test_repeated_correction_restart_and_cycle_safe_homeostasis():
    tenant = uuid4()
    root, a, b, c, d = [uuid4() for _ in range(5)]
    connection = _DependencyConnection({
        root: (a,), a: (b,), b: (c,), c: (d,), d: (b,),
    })
    first_runtime = CorrectionPropagationService()
    first = await first_runtime._load_recursive_dependency_pairs(
        connection, tenant_id=tenant, root_model_ids=(root,), max_depth=16
    )
    # A new service instance models process restart; replay must discover the
    # same bounded work set without multiplying cycle work.
    restarted_runtime = CorrectionPropagationService()
    replay = await restarted_runtime._load_recursive_dependency_pairs(
        connection, tenant_id=tenant, root_model_ids=(root,), max_depth=16
    )
    fingerprint = "root-a-b-c-d-fenced-v1"
    report = evaluate_correction_homeostasis(
        [
            {"sequence": 1, "repair_required": 5, "fenced": 4, "repaired": 1,
             "unsafe_readable": 0, "replay_new_work": 0, "batch_signal_count": 4,
             "residual_debt_by_fate": {"queued": 2, "deferred": 1},
             "durable_state_fingerprint": fingerprint},
            {"sequence": 2, "repair_required": 5, "fenced": 4, "repaired": 1,
             "unsafe_readable": 0, "replay_new_work": 0, "batch_signal_count": 4,
             "residual_debt_by_fate": {"queued": 1, "deferred": 1},
             "durable_state_fingerprint": fingerprint},
            {"sequence": 3, "repair_required": 5, "fenced": 4, "repaired": 1,
             "unsafe_readable": 0, "replay_new_work": 0, "batch_signal_count": 4,
             "residual_debt_by_fate": {"queued": 0, "deferred": 0},
             "durable_state_fingerprint": fingerprint},
        ],
        cascade={
            "reachable_unique_nodes": 4, "visited_unique_nodes": len(first),
            "max_depth": 4, "cycle_encounters": 1, "duplicate_work_items": 0,
            "terminated": True, "restart_replay_equal": replay == first,
            "pre_restart_fingerprint": fingerprint,
            "post_restart_fingerprint": fingerprint,
        },
    )

    assert len(first) == 4
    assert replay == first
    assert report["verdict"] == "meets_policy"
    assert report["continuous_score"] == 1.0
    assert report["residual_repair_debt"]["by_fate"] == {"deferred": 2, "queued": 3}
    assert report["residual_repair_debt"]["distribution"] == {
        "deferred": 0.4, "queued": 0.6,
    }


def test_homeostasis_report_exposes_replay_and_growing_debt_failure():
    report = evaluate_correction_homeostasis(
        [
            {"repair_required": 3, "fenced": 2, "unsafe_readable": 1,
             "replay_new_work": 0, "batch_signal_count": 2,
             "residual_debt_by_fate": {"queued": 1}},
            {"repair_required": 3, "fenced": 2, "unsafe_readable": 1,
             "replay_new_work": 2, "batch_signal_count": 1,
             "residual_debt_by_fate": {"queued": 3}},
            {"repair_required": 3, "fenced": 2, "unsafe_readable": 1,
             "replay_new_work": 1, "batch_signal_count": 1,
             "residual_debt_by_fate": {"queued": 5}},
        ],
        cascade={"reachable_unique_nodes": 5, "visited_unique_nodes": 3,
                 "max_depth": 2, "duplicate_work_items": 2, "terminated": True,
                 "restart_replay_equal": False, "pre_restart_fingerprint": "a",
                 "post_restart_fingerprint": "b"},
    )

    assert report["verdict"] == "below_policy"
    assert report["checks"]["replay_is_idempotent"] is False
    assert report["checks"]["repair_debt_does_not_grow"] is False
    assert report["measurements"]["cascade_reachability_ratio"] == 0.6
    assert 0.0 < report["continuous_score"] < 1.0
