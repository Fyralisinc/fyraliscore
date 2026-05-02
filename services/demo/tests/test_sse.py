"""services/demo/tests/test_sse.py — pub/sub bus + SSE stream."""
from __future__ import annotations

import asyncio
import json

import pytest

from lib.shared.ids import uuid7
from services.demo.sse import publish_recommendation_event, stream_for_actor


@pytest.mark.asyncio
async def test_subscriber_receives_published_event():
    tenant_id = uuid7()
    actor_id = uuid7()
    rec_id = uuid7()

    stream = stream_for_actor(tenant_id=tenant_id, actor_id=actor_id)

    # First frame is always the `ready` event.
    ready = await stream.__anext__()
    assert b"event: ready" in ready

    # Publish on a separate task, then read.
    async def _producer():
        await asyncio.sleep(0.01)
        await publish_recommendation_event(
            tenant_id=tenant_id,
            actor_id=actor_id,
            event="created",
            recommendation_id=rec_id,
            summary={"natural": "test rec"},
        )

    asyncio.create_task(_producer())
    frame = await stream.__anext__()
    assert frame.startswith(b"data: ")
    body = json.loads(frame[len(b"data: "):].strip())
    assert body["event"] == "created"
    assert body["recommendation_id"] == str(rec_id)
    assert body["summary"]["natural"] == "test rec"

    await stream.aclose()


@pytest.mark.asyncio
async def test_publish_to_unrelated_actor_is_ignored():
    tenant_id = uuid7()
    actor_id = uuid7()
    other_actor = uuid7()
    rec_id = uuid7()

    stream = stream_for_actor(tenant_id=tenant_id, actor_id=actor_id)
    await stream.__anext__()  # ready frame

    await publish_recommendation_event(
        tenant_id=tenant_id,
        actor_id=other_actor,
        event="created",
        recommendation_id=rec_id,
    )
    # No event should be delivered to actor_id; the next read will time
    # out into a heartbeat. Use a short wait_for to confirm.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(stream.__anext__(), timeout=0.5)

    await stream.aclose()
