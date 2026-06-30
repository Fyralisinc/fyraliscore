from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

import services.reasoning.retrieval.pathways as pathways
from services.reasoning.retrieval.read_fanout import ReadFanoutBudget


class _NamedConn:
    def __init__(self, name: str) -> None:
        self.name = name


class _TrackingAcquire:
    def __init__(self, pool: _TrackingPool) -> None:
        self.pool = pool

    async def __aenter__(self) -> _NamedConn:
        self.pool.current += 1
        self.pool.peak = max(self.pool.peak, self.pool.current)
        self.pool.counter += 1
        return _NamedConn(f"pool-{self.pool.counter}")

    async def __aexit__(self, *_args: object) -> bool:
        self.pool.current -= 1
        return False


class _TrackingPool:
    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self.current = 0
        self.peak = 0
        self.counter = 0

    def get_max_size(self) -> int:
        return self.max_size

    def acquire(self) -> _TrackingAcquire:
        return _TrackingAcquire(self)


class _FetchRecordingConn:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object) -> list[dict]:
        self.fetch_calls.append((query, args))
        return list(self.rows)


@pytest.mark.asyncio
async def test_read_fanout_budget_declines_nested_slot_when_full() -> None:
    pool = _TrackingPool(max_size=1)
    budget = ReadFanoutBudget.from_pool(pool)

    async with budget.connection() as outer_conn:
        assert outer_conn is not None
        async with budget.connection_if_available() as nested_conn:
            assert nested_conn is None

    snapshot = budget.snapshot()
    assert snapshot.max_concurrency == 1
    assert snapshot.peak_in_use == 1
    assert snapshot.acquired == 1
    assert snapshot.denied == 1
    assert pool.peak == 1


@pytest.mark.asyncio
async def test_pathway_a_sidecar_fanout_defers_without_losing_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_fetch(conn: _NamedConn, **kwargs: object):
        calls.append(conn.name)
        if conn.name.startswith("pool"):
            await asyncio.sleep(0.02)
        return [
            {
                "id": uuid4(),
                "_seed_priority": kwargs["entity_priorities"][0],
                "_local_rank": 0,
                "_seed_order": kwargs["entity_orders"][0],
                "activation": 1.0,
            }
        ]

    monkeypatch.setattr(pathways, "_fetch_pathway_a_entity_sidecar_rows", fake_fetch)
    pool = _TrackingPool(max_size=1)
    budget = ReadFanoutBudget.from_pool(pool)

    result = await pathways._fetch_pathway_a_entity_sidecar_rows_fanout(
        pool,  # type: ignore[arg-type]
        fallback_conn=_NamedConn("fallback"),  # type: ignore[arg-type]
        read_fanout_budget=budget,
        tenant_id=uuid4(),
        entity_types=["commitment", "commitment"],
        entity_ids=[uuid4(), uuid4()],
        entity_orders=[0, 1],
        entity_priorities=[0, 0],
        per_seed_limit=2,
        global_limit=10,
        chunk_size=1,
    )

    assert len(result.rows) == 2
    assert result.fanout_chunks == 1
    assert result.deferred_chunks == 1
    assert pool.peak == 1
    assert any(call.startswith("pool") for call in calls)
    assert "fallback" in calls


@pytest.mark.asyncio
async def test_pathway_a_entity_sidecar_uses_single_ranked_bulk_query() -> None:
    tenant_id = uuid4()
    first_entity_id = uuid4()
    second_entity_id = uuid4()
    conn = _FetchRecordingConn(
        [
            {
                "id": uuid4(),
                "_seed_priority": 0,
                "_seed_order": 0,
                "_local_rank": 0,
                "activation": 0.9,
            }
        ]
    )

    rows = await pathways._fetch_pathway_a_entity_sidecar_rows(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        entity_types=["commitment", "goal"],
        entity_ids=[first_entity_id, second_entity_id],
        entity_orders=[0, 1],
        entity_priorities=[0, 1],
        per_seed_limit=3,
        global_limit=10,
    )

    assert rows == conn.rows
    assert len(conn.fetch_calls) == 1
    query, args = conn.fetch_calls[0]
    assert "FROM unnest(" in query
    assert "JOIN model_scope_entities mse" in query
    assert "row_number() OVER" in query
    assert "PARTITION BY seeds.seed_order" in query
    assert "WHERE _local_rank < $6" in query
    assert args[1:5] == (
        ["commitment", "goal"],
        [first_entity_id, second_entity_id],
        [0, 1],
        [0, 1],
    )
    assert args[5:] == (3, 10)


@pytest.mark.asyncio
async def test_pathway_a_actor_sidecar_uses_single_ranked_bulk_query() -> None:
    tenant_id = uuid4()
    actor_id = uuid4()
    conn = _FetchRecordingConn(
        [
            {
                "id": uuid4(),
                "_seed_priority": 1,
                "_seed_order": 0,
                "_local_rank": 0,
                "activation": 0.8,
            }
        ]
    )

    rows = await pathways._fetch_pathway_a_actor_sidecar_rows(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        actor_ids=[actor_id],
        actor_orders=[0],
        per_seed_limit=4,
        global_limit=9,
    )

    assert rows == conn.rows
    assert len(conn.fetch_calls) == 1
    query, args = conn.fetch_calls[0]
    assert "FROM unnest($2::uuid[], $3::int[])" in query
    assert "JOIN model_scope_actors msa" in query
    assert "row_number() OVER" in query
    assert "PARTITION BY seeds.seed_order" in query
    assert "WHERE _local_rank < $4" in query
    assert args[1:] == ([actor_id], [0], 4, 9)
