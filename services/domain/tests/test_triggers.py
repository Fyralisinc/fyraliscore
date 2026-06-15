from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from services.domain.obligations import open_obligation
from services.domain.triggers import enqueue_model_reeval, enqueue_trigger


class FakeConn:
    def __init__(self, *, fetchvals=None):
        self.executed = []
        self.fetches = []
        self._fetchvals = list(fetchvals or [])

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "INSERT 0 1"

    async def fetchval(self, sql, *args):
        self.fetches.append((sql, args))
        if self._fetchvals:
            return self._fetchvals.pop(0)
        return None


@pytest.mark.asyncio
async def test_enqueue_trigger_serializes_payload_and_returns_id():
    conn = FakeConn()
    tenant_id = uuid7()
    observation_id = uuid7()

    trigger_id = await enqueue_trigger(
        conn,
        tenant_id=tenant_id,
        trigger_kind="T1",
        trigger_subkind="event_arrival",
        observation_id=observation_id,
        payload={"source": "test", "observation_id": observation_id},
    )

    assert conn.executed
    args = conn.executed[0][1]
    assert args[0] == trigger_id
    assert args[1] == tenant_id
    assert args[2] == "T1"
    assert args[3] == "event_arrival"
    assert args[4] == observation_id
    assert json.loads(args[6]) == {
        "source": "test",
        "observation_id": str(observation_id),
    }


@pytest.mark.asyncio
async def test_enqueue_trigger_can_schedule_future_work():
    conn = FakeConn()
    scheduled_for = datetime(2026, 6, 20, tzinfo=timezone.utc)

    await enqueue_trigger(
        conn,
        tenant_id=uuid7(),
        trigger_kind="T4",
        payload={},
        scheduled_for=scheduled_for,
    )

    assert conn.executed[0][1][-1] == scheduled_for


@pytest.mark.asyncio
async def test_enqueue_trigger_can_prelock_batch_rows():
    conn = FakeConn()

    await enqueue_trigger(
        conn,
        tenant_id=uuid7(),
        trigger_kind="T1",
        trigger_subkind="event_batch",
        payload={"batch": True},
        locked_by="worker-1",
    )

    assert conn.executed[0][1][-1] == "worker-1"


@pytest.mark.asyncio
async def test_enqueue_model_reeval_returns_existing_pending_row_on_dedup():
    existing_id = uuid7()
    conn = FakeConn(fetchvals=[None, existing_id])

    result = await enqueue_model_reeval(
        conn,
        tenant_id=uuid7(),
        model_id=uuid7(),
        cause_model_id=uuid7(),
        cause_kind="supporting_archived",
    )

    assert result == existing_id
    assert len(conn.fetches) == 4


@pytest.mark.asyncio
async def test_open_obligation_dedups_existing_open_object_obligation():
    existing_id = uuid7()
    conn = FakeConn(fetchvals=[None, existing_id])
    tenant_id = uuid7()
    object_id = uuid7()

    result = await open_obligation(
        conn,
        tenant_id=tenant_id,
        kind="model_reeval",
        object_kind="model",
        object_id=object_id,
        trigger_kind="T4",
        trigger_subkind="model_reeval",
        payload={"cause_kind": "supporting_archived"},
    )

    assert result == existing_id
    assert "think_obligations" in conn.fetches[0][0]
    assert conn.fetches[0][1][1:5] == (
        tenant_id,
        "model_reeval",
        "model",
        object_id,
    )
