from __future__ import annotations

from services.reasoning.think.execution_policy import (
    NORMAL_EXECUTION_POLICY,
    STAGE1_COMPANY_MEMORY_POLICY,
)
from services.reasoning.think.worker import ThinkWorker, WorkerConfig


def test_default_worker_selects_stage1_only_for_source_t1() -> None:
    worker = ThinkWorker(
        None,  # type: ignore[arg-type]
        config=WorkerConfig(stage1_company_memory_for_t1=True),
        embedder=object(),
    )

    assert worker._execution_policy_for_trigger(
        "T1", "event_arrival"
    ).is_stage1_company_memory
    assert worker._execution_policy_for_trigger(
        "T1", "event_batch"
    ).is_stage1_company_memory
    assert not worker._execution_policy_for_trigger(
        "T1", "state_change"
    ).is_stage1_company_memory
    assert not worker._execution_policy_for_trigger(
        "T2", "belief_updated"
    ).is_stage1_company_memory
    assert not worker._execution_policy_for_trigger(
        "T4", "open_question_search"
    ).is_stage1_company_memory


def test_stage1_t1_profile_has_explicit_environment_rollback(monkeypatch) -> None:
    monkeypatch.setenv("THINK_STAGE1_COMPANY_MEMORY_FOR_T1", "0")
    worker = ThinkWorker(
        None,  # type: ignore[arg-type]
        config=WorkerConfig.from_env(),
        embedder=object(),
    )

    assert not worker._execution_policy_for_trigger(
        "T1", "event_batch"
    ).is_stage1_company_memory


def test_explicit_worker_policy_overrides_trigger_selection() -> None:
    worker = ThinkWorker(
        None,  # type: ignore[arg-type]
        config=WorkerConfig(stage1_company_memory_for_t1=True),
        embedder=object(),
        execution_policy=NORMAL_EXECUTION_POLICY,
    )

    assert (
        worker._execution_policy_for_trigger("T1", "event_batch")
        is NORMAL_EXECUTION_POLICY
    )

    worker.execution_policy = STAGE1_COMPANY_MEMORY_POLICY
    assert (
        worker._execution_policy_for_trigger("T4", "open_question_search")
        is STAGE1_COMPANY_MEMORY_POLICY
    )
