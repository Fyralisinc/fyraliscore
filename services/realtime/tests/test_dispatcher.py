"""Wave 4-D dispatcher tests — real Postgres, real NOTIFY.

Coverage targets (from BUILD-PLAN §5 Prompt 4.D):
1. Dispatcher routes an emitted state_change to subscribed client.
2. Access control — client without a scope sees nothing private.
3. Backpressure drop-oldest + ``stream_lagged`` control frame.
4. Replay with ``since_sequence_num`` replays in sequence order.
5. 50 concurrent clients all receive an event.
6. Tenant isolation.
"""
from __future__ import annotations

import asyncio
import json
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.realtime.dispatcher import Dispatcher, EventFrame


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


async def _insert_observation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    kind: str = "state_change",
    source_channel: str = "internal:state_change",
    entity_kind: str | None = None,
    entity_id: UUID | None = None,
    actor_id: UUID | None = None,
    occurred_at_sql: str = "now()",
) -> UUID:
    """Direct insert + NOTIFY for dispatcher tests. We bypass the
    `observations.events` scope helper because we want tests to control
    timing explicitly.
    """
    obs_id = uuid7()
    content: dict = {"state_change_kind": "test"}
    if entity_kind:
        content["entity_kind"] = entity_kind
    if entity_id:
        content["entity_id"] = str(entity_id)
    await conn.execute(
        f"""
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            content, content_text, trust_tier
        )
        VALUES ($1, $2, {occurred_at_sql}, $3, $4,
                $5::jsonb, 'x', 'authoritative')
        """,
        obs_id,
        tenant_id,
        kind,
        source_channel,
        json.dumps(content),
    )
    # Emit NOTIFY on the same connection for deterministic dispatch.
    await conn.execute(
        "SELECT pg_notify('observations_new', $1)",
        json.dumps(
            {
                "id": str(obs_id),
                "kind": kind,
                "tenant_id": str(tenant_id),
                "source_channel": source_channel,
            },
            sort_keys=True,
        ),
    )
    return obs_id


async def _drain_one(state, timeout: float = 2.0):
    return await asyncio.wait_for(state.queue.get(), timeout=timeout)


async def _wait_for(cond, timeout: float = 2.0, poll: float = 0.02):
    from time import monotonic

    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if await cond() if asyncio.iscoroutinefunction(cond) else cond():
            return True
        await asyncio.sleep(poll)
    return False


# ---------------------------------------------------------------------
# 1. Goal delta is dispatched to a subscribed client in < 1s
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_goal_state_change_dispatched_to_subscribed_client(
    realtime_pool: asyncpg.Pool, tenant_id, seeded_actor
) -> None:
    disp = Dispatcher(realtime_pool)
    await disp.start()
    try:
        goal_id = uuid7()
        state = disp.register_client(
            tenant_id=tenant_id,
            actor_id=seeded_actor,
            initial_topics={f"goal:{goal_id}"},
        )
        async with realtime_pool.acquire() as c:
            await _insert_observation(
                c,
                tenant_id=tenant_id,
                entity_kind="goal",
                entity_id=goal_id,
            )
        frame = await _drain_one(state, timeout=5.0)
        assert isinstance(frame, EventFrame)
        assert frame.topic == f"goal:{goal_id}"
        assert frame.tenant_id == tenant_id
        assert frame.kind == "act_change"
    finally:
        await disp.stop()


# ---------------------------------------------------------------------
# 2. Access control stub — client without matching scope gets nothing
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_without_matching_scope_gets_nothing(
    realtime_pool: asyncpg.Pool, tenant_id, seeded_actor
) -> None:
    disp = Dispatcher(realtime_pool)
    await disp.start()
    try:
        goal_a = uuid7()
        goal_b = uuid7()
        state = disp.register_client(
            tenant_id=tenant_id,
            actor_id=seeded_actor,
            initial_topics={f"goal:{goal_a}"},
        )
        async with realtime_pool.acquire() as c:
            await _insert_observation(
                c,
                tenant_id=tenant_id,
                entity_kind="goal",
                entity_id=goal_b,
            )
        # Wait a moment — nothing should arrive for goal_b.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(state.queue.get(), timeout=0.6)
    finally:
        await disp.stop()


# ---------------------------------------------------------------------
# 3. Backpressure drops oldest and raises the dropped counter
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backpressure_drops_oldest_and_bumps_counter(
    realtime_pool: asyncpg.Pool, tenant_id, seeded_actor
) -> None:
    disp = Dispatcher(realtime_pool)
    await disp.start()
    try:
        # Register with a tiny queue so we can force a drop quickly.
        state = disp.register_client(
            tenant_id=tenant_id,
            actor_id=seeded_actor,
            initial_topics={f"tenant:{tenant_id}"},
            queue_maxsize=3,
        )
        # Exercise _enqueue directly (unit scope) with 10 synthetic frames.
        for i in range(10):
            disp._enqueue(
                state,
                EventFrame(
                    kind="observation",
                    id=uuid7(),
                    tenant_id=tenant_id,
                    topic=f"tenant:{tenant_id}",
                    sequence_num=i,
                    payload={"i": i},
                ),
            )
        assert state.queue.qsize() == 3
        assert state.dropped >= 7
        assert disp.stats["drops"] >= 7
        # Consume + verify a synthetic control frame is emitted next.
        emitted: list[dict] = []

        async def _sink(obj):
            emitted.append(obj)

        async def _drain_a_few():
            task = asyncio.create_task(
                state.drain_to(
                    _sink,
                    control_frame_factory=lambda n: {
                        "kind": "stream_lagged",
                        "dropped": n,
                    },
                )
            )
            # Give the drain task a chance to emit at least 1 lag frame
            # + 1 payload. The lag frame fires before the NEXT payload.
            await asyncio.sleep(0.2)
            state.closed = True
            state.queue.put_nowait(object())  # sentinel-like to exit
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        await _drain_a_few()
        # At least one stream_lagged frame should have been emitted.
        assert any(
            isinstance(x, dict) and x.get("kind") == "stream_lagged"
            for x in emitted
        )
    finally:
        await disp.stop()


# ---------------------------------------------------------------------
# 4. Replay since X replays in sequence order
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_since_sequence_num_ordered(
    realtime_pool: asyncpg.Pool, tenant_id, seeded_actor
) -> None:
    disp = Dispatcher(realtime_pool)
    await disp.start()
    try:
        goal_id = uuid7()
        # Insert 5 observations BEFORE the client connects.
        ids = []
        async with realtime_pool.acquire() as c:
            for _ in range(5):
                ids.append(
                    await _insert_observation(
                        c,
                        tenant_id=tenant_id,
                        entity_kind="goal",
                        entity_id=goal_id,
                    )
                )
        # Client subscribes.
        state = disp.register_client(
            tenant_id=tenant_id,
            actor_id=seeded_actor,
            initial_topics={f"goal:{goal_id}"},
        )
        pushed = await disp.replay_since(state, since_sequence_num=0)
        assert pushed == 5
        # Drain and check sequence order.
        seqs = []
        for _ in range(5):
            f = await _drain_one(state, timeout=2.0)
            assert isinstance(f, EventFrame)
            seqs.append(f.sequence_num)
        assert seqs == sorted(seqs)
    finally:
        await disp.stop()


# ---------------------------------------------------------------------
# 5. 50 concurrent clients on the same topic all see the event
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_many_concurrent_clients_all_receive_event(
    realtime_pool: asyncpg.Pool, tenant_id, seeded_actor
) -> None:
    disp = Dispatcher(realtime_pool)
    await disp.start()
    try:
        topic = f"tenant:{tenant_id}"
        states = [
            disp.register_client(
                tenant_id=tenant_id,
                actor_id=seeded_actor,
                initial_topics={topic},
            )
            for _ in range(50)
        ]
        async with realtime_pool.acquire() as c:
            await _insert_observation(c, tenant_id=tenant_id)
        # All 50 clients should receive one frame.
        received = 0
        for s in states:
            try:
                await _drain_one(s, timeout=5.0)
                received += 1
            except asyncio.TimeoutError:
                pass
        assert received == 50
    finally:
        await disp.stop()


# ---------------------------------------------------------------------
# 6. Tenant isolation
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tenant_isolation(
    realtime_pool: asyncpg.Pool,
    tenant_id,
    tenant_id_b,
    seeded_actor,
    seeded_actor_b,
) -> None:
    disp = Dispatcher(realtime_pool)
    await disp.start()
    try:
        sa = disp.register_client(
            tenant_id=tenant_id,
            actor_id=seeded_actor,
            initial_topics={f"tenant:{tenant_id}"},
        )
        sb = disp.register_client(
            tenant_id=tenant_id_b,
            actor_id=seeded_actor_b,
            initial_topics={f"tenant:{tenant_id_b}"},
        )
        # Event in tenant A only.
        async with realtime_pool.acquire() as c:
            await _insert_observation(c, tenant_id=tenant_id)
        await _drain_one(sa, timeout=5.0)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(sb.queue.get(), timeout=0.6)
    finally:
        await disp.stop()


# ---------------------------------------------------------------------
# 7. Malformed NOTIFY payload is dropped without crashing the dispatcher
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_malformed_notify_payload_is_safe(
    realtime_pool: asyncpg.Pool, tenant_id, seeded_actor
) -> None:
    disp = Dispatcher(realtime_pool)
    await disp.start()
    try:
        state = disp.register_client(
            tenant_id=tenant_id,
            actor_id=seeded_actor,
            initial_topics={f"tenant:{tenant_id}"},
        )
        # Fire a non-JSON NOTIFY; then a valid one.
        async with realtime_pool.acquire() as c:
            await c.execute("SELECT pg_notify('observations_new', 'not-json')")
            # Valid event after — confirm dispatcher still live.
            await _insert_observation(c, tenant_id=tenant_id)
        await _drain_one(state, timeout=5.0)
    finally:
        await disp.stop()
