from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from services.domain.triggers import ensure_event_arrival_trigger


class _FakeConn:
    def __init__(self, *, existing_trigger_id: UUID | None = None) -> None:
        self.existing_trigger_id = existing_trigger_id
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> None:
        self.execute_calls.append((sql, args))

    async def fetchval(self, sql: str, *args: object) -> UUID | None:
        self.fetchval_calls.append((sql, args))
        return self.existing_trigger_id


@pytest.mark.asyncio
async def test_ensure_event_arrival_trigger_reuses_existing_trigger() -> None:
    tenant_id = uuid4()
    observation_id = uuid4()
    existing_trigger_id = uuid4()
    conn = _FakeConn(existing_trigger_id=existing_trigger_id)

    trigger_id = await ensure_event_arrival_trigger(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        observation_id=observation_id,
        payload={"source_channel": "slack"},
    )

    assert trigger_id == existing_trigger_id
    assert len(conn.fetchval_calls) == 1
    assert (
        sum("INSERT INTO think_trigger_queue" in call[0] for call in conn.execute_calls)
        == 0
    )
    assert "pg_advisory_lock" in conn.execute_calls[0][0]
    assert "pg_advisory_unlock" in conn.execute_calls[-1][0]


@pytest.mark.asyncio
async def test_ensure_event_arrival_trigger_inserts_when_missing() -> None:
    tenant_id = uuid4()
    observation_id = uuid4()
    trigger_id = uuid4()
    conn = _FakeConn()

    returned = await ensure_event_arrival_trigger(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        observation_id=observation_id,
        payload={"source_channel": "slack"},
        trigger_id=trigger_id,
    )

    assert returned == trigger_id
    insert_calls = [
        call
        for call in conn.execute_calls
        if "INSERT INTO think_trigger_queue" in call[0]
    ]
    assert len(insert_calls) == 1
    assert "pg_advisory_unlock" in conn.execute_calls[-1][0]
    assert UUID(str(insert_calls[0][1][0])) == trigger_id
    assert UUID(str(insert_calls[0][1][1])) == tenant_id
    assert UUID(str(insert_calls[0][1][4])) == observation_id
