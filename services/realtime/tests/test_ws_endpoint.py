"""WS endpoint tests — Wave 4-D.

These tests are **synchronous** on purpose. ``fastapi.testclient.TestClient``
manages its own event loop via ``anyio.from_thread.start_blocking_portal``
and drives the ASGI app in that portal. Mixing its synchronous API with
``pytest-asyncio``'s active event loop creates dueling loops and the
asyncpg pool's connection-checkout races the portal, producing
``InterfaceError: cannot perform operation: another operation is in
progress``. Documented in BUILD-LOG Wave 4-D entry (realtime testing
strategy).

Pattern: each test creates its own asyncpg pool + FastAPI app inside a
sync helper and drives the WS through TestClient directly. Migrations
are applied by the ``realtime_pool`` fixture once per test session.
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import threading
from contextlib import contextmanager
from uuid import UUID, uuid4

import asyncpg
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from lib.shared.ids import uuid7
from services.gateway.auth import create_session
from services.realtime.dispatcher import Dispatcher
from services.realtime.main import configure_realtime


pytestmark = pytest.mark.integration


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


# ---------------------------------------------------------------------
# Sync helper: app + pool owned by TestClient's portal loop.
# ---------------------------------------------------------------------


@contextmanager
def _ws_harness():
    """Yield (TestClient, tenant_id, actor_id, bearer_token, pool_for_pg_notify).

    TestClient manages a background thread that runs an asyncio loop;
    the app + Dispatcher use THAT loop. We also return a separately-
    created pool (running on a throwaway thread's loop) so the test
    can INSERT + NOTIFY observations without contending with the WS
    loop's pool.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")

    # Seed actor + session via a dedicated thread so the TestClient
    # portal gets an independent pool later.
    tenant_id = uuid7()
    actor_id = uuid7()
    token_box: dict = {}

    def _seed_actor_and_session() -> None:
        async def _run() -> None:
            pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
            try:
                async with pool.acquire() as c:
                    await c.execute(
                        """
                        INSERT INTO actors (id, tenant_id, type, display_name, status)
                        VALUES ($1, $2, 'human_internal', 'Alice', 'active')
                        """,
                        actor_id,
                        tenant_id,
                    )
                token, _ctx = await create_session(
                    pool, actor_id=actor_id, tenant_id=tenant_id
                )
                token_box["token"] = token
            finally:
                await pool.close()

        asyncio.run(_run())

    t = threading.Thread(target=_seed_actor_and_session)
    t.start()
    t.join()
    if "token" not in token_box:
        raise RuntimeError("seed thread failed")

    # Build a FastAPI app with a lifespan that owns the pool + dispatcher.
    app = FastAPI()

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _lifespan(app_):
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        disp = Dispatcher(pool)
        configure_realtime(app_, pool=pool, dispatcher=disp, start=False)
        await disp.start()
        try:
            yield
        finally:
            await disp.stop()
            await pool.close()

    # Attach the lifespan.
    app.router.lifespan_context = _lifespan

    with TestClient(app) as client:
        yield client, tenant_id, actor_id, token_box["token"], dsn


def _insert_obs_sync(dsn: str, *, tenant_id: UUID, entity_kind: str, entity_id: UUID) -> None:
    """Open a throwaway pool in a new thread/loop and push an event."""
    obs_id = uuid7()
    content = {
        "state_change_kind": "test",
        "entity_kind": entity_kind,
        "entity_id": str(entity_id),
    }

    async def _run() -> None:
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        try:
            async with pool.acquire() as c:
                await c.execute(
                    """
                    INSERT INTO observations (
                        id, tenant_id, occurred_at, kind, source_channel,
                        content, content_text, trust_tier
                    ) VALUES (
                        $1, $2, now(), 'state_change',
                        'internal:state_change', $3::jsonb, 'x', 'authoritative'
                    )
                    """,
                    obs_id,
                    tenant_id,
                    json.dumps(content),
                )
                await c.execute(
                    "SELECT pg_notify('observations_new', $1)",
                    json.dumps(
                        {
                            "id": str(obs_id),
                            "kind": "state_change",
                            "tenant_id": str(tenant_id),
                            "source_channel": "internal:state_change",
                        },
                        sort_keys=True,
                    ),
                )
        finally:
            await pool.close()

    t = threading.Thread(target=lambda: asyncio.run(_run()))
    t.start()
    t.join()


# ---------------------------------------------------------------------
# 1. Unauthenticated WS closes with 1008
# ---------------------------------------------------------------------


def test_ws_rejects_missing_token(realtime_pool) -> None:
    with _ws_harness() as (client, *_):
        with client.websocket_connect("/stream") as ws:
            msg = ws.receive_text()
            data = json.loads(msg)
            assert data.get("kind") == "error"
            assert "token" in data.get("message", "")
            with pytest.raises(Exception):
                ws.receive_text()


# ---------------------------------------------------------------------
# 2. Authenticated subscribe + receive round-trip
# ---------------------------------------------------------------------


def test_ws_authenticated_subscribe_and_receive(realtime_pool) -> None:
    with _ws_harness() as (client, tenant_id, _actor, token, dsn):
        goal_id = uuid7()
        with client.websocket_connect(f"/stream?token={token}") as ws:
            ready = json.loads(ws.receive_text())
            assert ready["kind"] == "ready"
            ws.send_text(
                json.dumps(
                    {"action": "subscribe", "topics": [f"goal:{goal_id}"]}
                )
            )
            ack = json.loads(ws.receive_text())
            assert ack["kind"] == "subscribed"
            _insert_obs_sync(
                dsn,
                tenant_id=tenant_id,
                entity_kind="goal",
                entity_id=goal_id,
            )
            raw = ws.receive_text()
            frame = json.loads(raw)
            assert frame["kind"] == "act_change"
            assert frame["topic"] == f"goal:{goal_id}"


# ---------------------------------------------------------------------
# 3. Replay returns replay_complete with count
# ---------------------------------------------------------------------


def test_ws_replay_returns_replay_complete(realtime_pool) -> None:
    with _ws_harness() as (client, tenant_id, _actor, token, dsn):
        goal_id = uuid7()
        _insert_obs_sync(
            dsn, tenant_id=tenant_id, entity_kind="goal", entity_id=goal_id
        )
        _insert_obs_sync(
            dsn, tenant_id=tenant_id, entity_kind="goal", entity_id=goal_id
        )
        with client.websocket_connect(f"/stream?token={token}") as ws:
            json.loads(ws.receive_text())  # ready
            ws.send_text(
                json.dumps(
                    {"action": "subscribe", "topics": [f"goal:{goal_id}"]}
                )
            )
            json.loads(ws.receive_text())  # subscribed
            ws.send_text(
                json.dumps({"action": "replay", "since_sequence_num": 0})
            )
            for _ in range(6):
                msg = json.loads(ws.receive_text())
                if msg.get("kind") == "replay_complete":
                    assert msg.get("pushed") >= 2
                    return
            pytest.fail("no replay_complete frame observed")


# ---------------------------------------------------------------------
# 4. Unknown action returns error
# ---------------------------------------------------------------------


def test_ws_unknown_action_returns_error_frame(realtime_pool) -> None:
    with _ws_harness() as (client, _t, _a, token, _dsn):
        with client.websocket_connect(f"/stream?token={token}") as ws:
            json.loads(ws.receive_text())  # ready
            ws.send_text(json.dumps({"action": "nonsense"}))
            frame = json.loads(ws.receive_text())
            assert frame.get("kind") == "error"


# ---------------------------------------------------------------------
# 5. Ping/pong keepalive works
# ---------------------------------------------------------------------


def test_ws_ping_pong(realtime_pool) -> None:
    with _ws_harness() as (client, _t, _a, token, _dsn):
        with client.websocket_connect(f"/stream?token={token}") as ws:
            json.loads(ws.receive_text())  # ready
            ws.send_text(json.dumps({"action": "ping"}))
            frame = json.loads(ws.receive_text())
            assert frame.get("kind") == "pong"
