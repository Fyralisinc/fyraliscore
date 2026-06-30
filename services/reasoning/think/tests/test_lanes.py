from __future__ import annotations

import pytest

from services.reasoning.think.lanes import (
    ThinkLane,
    classify_trigger_lane,
    lane_names,
    parse_lane_filter,
)
from services.reasoning.think.worker import WorkerConfig


def test_parse_lane_filter_defaults_to_all() -> None:
    assert parse_lane_filter(None) is None
    assert parse_lane_filter("") is None
    assert parse_lane_filter("all") is None
    assert lane_names(None) == "all"


def test_parse_lane_filter_accepts_aliases() -> None:
    assert parse_lane_filter("batch,relationships,deep") == frozenset(
        {
            ThinkLane.BATCH_MEMORY,
            ThinkLane.RELATIONSHIP,
            ThinkLane.DEEP_SYNTHESIS,
        }
    )


def test_parse_lane_filter_rejects_unknown_lane() -> None:
    with pytest.raises(ValueError, match="unknown Think lane"):
        parse_lane_filter("unknown")


def test_worker_config_reads_lane_filter(monkeypatch) -> None:
    monkeypatch.setenv("THINK_WORKER_LANES", "batch_memory,relationship")

    cfg = WorkerConfig.from_env()

    assert cfg.allowed_lanes == frozenset(
        {ThinkLane.BATCH_MEMORY, ThinkLane.RELATIONSHIP}
    )


@pytest.mark.parametrize(
    ("kind", "subkind", "payload", "expected"),
    [
        ("T1", "state_change", {}, ThinkLane.REFLEX),
        ("T1", "event_arrival", {}, ThinkLane.BATCH_MEMORY),
        ("T1", "event_batch", {"batch": True}, ThinkLane.BATCH_MEMORY),
        ("T4", "latent_relationship_candidate", {}, ThinkLane.RELATIONSHIP),
        ("T3", "customer_risk", {}, ThinkLane.DEEP_SYNTHESIS),
        ("T4", "representation_repair", {}, ThinkLane.REPAIR),
        (
            "T1",
            "event_arrival",
            {"validation_feedback": "missing evidence"},
            ThinkLane.REPAIR,
        ),
        ("T4", "model_reeval", {}, ThinkLane.REFLEX),
    ],
)
def test_classify_trigger_lane(kind, subkind, payload, expected) -> None:
    assert classify_trigger_lane(kind, subkind, payload).lane is expected
