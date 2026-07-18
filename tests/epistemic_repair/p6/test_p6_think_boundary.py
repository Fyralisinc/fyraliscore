import ast
import os
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from lib.shared.errors import InvariantViolation
from services.evaluation.epistemic_repair.p6_think_runner import (
    _complete_and_reopen_barrier, _snapshot,
)
from services.reasoning.think.lanes import ThinkLane
from services.reasoning.think.worker import ThinkWorker, WorkerConfig


def test_production_think_runner_cannot_import_or_read_sealed_gold() -> None:
    source = Path(
        "services/evaluation/epistemic_repair/p6_think_runner.py"
    ).read_text()
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    attributes = {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    assert "P6Gold" not in imported
    assert not {"gold", "synthesis_signal_by_storyline", "thesis_by_storyline"} & attributes


def test_production_think_runner_requires_real_batch_worker() -> None:
    source = Path(
        "services/evaluation/epistemic_repair/p6_think_runner.py"
    ).read_text()
    assert "_process_one_t1_batch" in source
    assert "retry_attempts=1" in source
    assert "run_timeout_s=attempt_timeout_s" in source
    assert "ThinkWorker" in source
    assert "t1_batch_max_size=25" in source
    assert "DeepSeek" not in source
    assert "P6 production proof requires a clean pinned worktree" in source
    assert "_drain_truth_critical_work" in source
    assert "process_background_triggers=True" in source
    assert 'expected_transport != "cli"' in source


@pytest.mark.asyncio
async def test_p6_barrier_separates_eventual_work_and_rejects_truth_pending() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the P6 barrier DB proof")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    tenant_id, completed_trigger, pending_trigger = uuid4(), uuid4(), uuid4()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO tenants(id,name,is_demo) VALUES($1,$2,FALSE)",
                tenant_id, "p6-barrier-proof",
            )
            await conn.execute("""
                INSERT INTO think_trigger_queue
                  (id,tenant_id,trigger_kind,payload,completed_at)
                VALUES ($1,$2,'observation','{}'::jsonb,now())
            """, completed_trigger, tenant_id)
            await conn.execute("""
                INSERT INTO pending_post_commit_actions
                  (tenant_id,trigger_id,action_kind,action_payload)
                VALUES ($1,$2,'materialize_projections','{}'::jsonb)
            """, tenant_id, completed_trigger)
        snapshot = await _snapshot(pool, tenant_id)
        assert snapshot["pending_work"] == {
            "truth_critical": {
                "total": 0, "by_queue": {"think_trigger_queue": 0},
            },
            "eventual_derived": {
                "total": 1,
                "by_action_kind": {"materialize_projections": 1},
            },
        }
        async with pool.acquire() as conn, conn.transaction():
            receipt, current = await _complete_and_reopen_barrier(
                conn, tenant_id=tenant_id, batch_number=1,
                previous_model_versions=set(),
            )
        assert current == set()
        assert receipt["barrier_version"] == 1
        assert receipt["truth_critical_pending_count"] == 0
        assert receipt["reopened_exactly"] is True
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO think_trigger_queue(id,tenant_id,trigger_kind,payload)
                VALUES ($1,$2,'observation','{}'::jsonb)
            """, pending_trigger, tenant_id)
            async with conn.transaction():
                with pytest.raises(InvariantViolation, match="truth-critical"):
                    await _complete_and_reopen_barrier(
                        conn, tenant_id=tenant_id, batch_number=2,
                        previous_model_versions=set(),
                    )
    finally:
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM pending_post_commit_actions WHERE tenant_id=$1",
                tenant_id,
            )
            await conn.execute(
                "DELETE FROM think_trigger_queue WHERE tenant_id=$1", tenant_id,
            )
            await conn.execute("DELETE FROM tenants WHERE id=$1", tenant_id)
        await pool.close()


@pytest.mark.asyncio
async def test_downstream_pruner_reads_legacy_fields_through_accepted_adapter() -> None:
    """Regression: truth-only view has no proposition_kind/falsifier sidecars."""

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the accepted-reader DB proof")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
    try:
        worker = ThinkWorker(
            pool,
            config=WorkerConfig(
                allowed_lanes=frozenset({ThinkLane.REFLEX}),
                tenant_filter=uuid4(),
            ),
            embedder=object(),
        )
        async with pool.acquire() as conn, conn.transaction():
            # Preparing/executing the exact production pruning SQL used to
            # raise UndefinedColumnError even when no queue row matched.
            await worker._prune_low_value_downstream_rows(conn)
            model_id = uuid4()
            await worker._t2_belief_batch_lanes(conn, [{
                "id": uuid4(), "trigger_kind": "T2",
                "trigger_subkind": "belief_updated", "model_id": model_id,
            }])

            candidate_id, trigger_id = uuid4(), uuid4()

            class CandidateThenDatabase:
                async def fetch(self, query, *args):
                    if "FROM relationship_candidates" in query:
                        return [{
                            "id": candidate_id, "candidate_kind": "edge",
                            "member_model_ids": [model_id],
                            "source_model_id": None, "target_model_id": None,
                        }]
                    return await conn.fetch(query, *args)

            await worker._t4_candidate_batch_lanes(CandidateThenDatabase(), [{
                "id": trigger_id, "trigger_kind": "T4",
                "trigger_subkind": "latent_relationship_candidate",
                "model_id": None,
                "payload": {
                    "relationship_candidate_id": str(candidate_id),
                    "member_model_ids": [str(model_id)],
                },
            }])
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_batch_retry_reuses_membership_after_timeout(monkeypatch) -> None:
    """One provider timeout retries the same queue batch exactly once."""

    import scripts.run_storyline_batch_benchmark as benchmark

    trigger_id = uuid4()
    row = {"id": trigger_id, "payload": {"batch_member_trigger_ids": [
        str(uuid4()), str(uuid4()),
    ]}}
    dispatched = []

    class Worker:
        async def _dispatch_trigger(self, item):
            dispatched.append(item)

    runs = iter((
        {"status": "failed", "error": "think_run_timeout after 300s"},
        {"status": "success", "error": None, "ops_applied": 1},
    ))

    async def run_for_trigger(_pool, _trigger_id):
        assert _trigger_id == trigger_id
        return next(runs)

    async def queue_state(_pool, *, trigger_id):
        return {"attempts": 1, "completed": False}

    async def relock(_pool, *, worker, trigger_id):
        return row

    monkeypatch.setattr(benchmark, "_run_for_trigger", run_for_trigger)
    monkeypatch.setattr(benchmark, "_t1_batch_queue_state", queue_state)
    monkeypatch.setattr(benchmark, "_lock_t1_batch_for_retry", relock)
    result, history = await benchmark._dispatch_t1_batch_with_retries(
        object(), Worker(), row, retry_attempts=1,
    )
    assert result["status"] == "success"
    assert result["recovered_after_retry"] is True
    assert len(history) == 2
    assert dispatched == [row, row]
    assert dispatched[0]["payload"]["batch_member_trigger_ids"] == (
        dispatched[1]["payload"]["batch_member_trigger_ids"]
    )
