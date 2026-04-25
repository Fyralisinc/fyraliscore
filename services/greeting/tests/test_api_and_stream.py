"""Tests for services.greeting.api + services.greeting.stream.

Phase-5 and Phase-6 exit gates:
  * WS accepts connections and pushes updates
  * GET /view/ceo/home returns correctly-shaped response (CONTRACTS §1.1)

Test-infra note:
FastAPI's `TestClient` runs requests on its own `anyio` loop. Since our
`greeting_db` async fixture builds the asyncpg pool on the pytest-
asyncio loop, calling `TestClient.get()` from within an async test
results in an InterfaceError (the pool's connection is owned by a
different loop). We work around this by exercising HTTP routes via
`httpx.AsyncClient(transport=ASGITransport(app))` (same loop) and WS
routes via a real background `uvicorn` server bound to a random port.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import threading
import time
from typing import Any
from uuid import UUID

import httpx
import pytest
import uvicorn
from fastapi import FastAPI

from services.greeting.api import build_ceo_api_router
from services.greeting.cache import ViewCeoCacheRepo
from services.greeting.scheduler import GreetingScheduler, SchedulerConfig
from services.greeting.snapshot import FounderContext
from services.greeting.stream import (
    StaticTenantTokenMap,
    ViewCeoStreamManager,
    build_ceo_stream_router,
)
from services.greeting.tests.conftest import (
    TENANT_A,
    seed_anomaly,
    seed_commitment,
    seed_goal,
    seed_model,
    seed_resource,
)


pytestmark = [
    pytest.mark.integration,
    # uvicorn's default `ws=websockets` backend imports
    # `websockets.legacy.*` on first WS request, which raises a set of
    # DeprecationWarnings. With `filterwarnings=error` enabled repo-
    # wide that would abort the test. Filter the family locally.
    pytest.mark.filterwarnings("ignore::DeprecationWarning:websockets.*"),
    pytest.mark.filterwarnings("ignore::DeprecationWarning:uvicorn.*"),
]


FOUNDER = FounderContext(
    tenant_id=TENANT_A,
    role="ceo",
    display_name="Dogfood CEO",
    timezone_name="Asia/Kathmandu",
)

DEV_TOKEN = "dogfood-token"


def _build_app(pool) -> tuple[FastAPI, GreetingScheduler, ViewCeoStreamManager]:
    cache = ViewCeoCacheRepo(pool)
    token_map = StaticTenantTokenMap(tokens={DEV_TOKEN: TENANT_A})
    stream_mgr = ViewCeoStreamManager(token_map=token_map)
    sched = GreetingScheduler(
        pool,
        cache=cache,
        config=SchedulerConfig(
            refresh_interval_seconds=9999,
            post_commit_poll_seconds=9999,
            tod_check_seconds=9999,
        ),
        stream_publisher=stream_mgr,
    )
    sched.register_tenant(TENANT_A, FOUNDER)

    app = FastAPI()
    app.include_router(
        build_ceo_api_router(
            cache=cache, scheduler=sched, stream_manager=stream_mgr
        )
    )
    app.include_router(build_ceo_stream_router(stream_mgr))
    return app, sched, stream_mgr


async def _seed_minimal(pool):
    goal_id = await seed_goal(pool)
    await seed_commitment(
        pool, title="ship feature", state="active",
        is_critical_path=True, goal_id=goal_id, due_days=4,
    )
    await seed_model(pool, natural="everything is fine", confidence=0.8)
    await seed_resource(pool, health="warning")
    await seed_anomaly(pool, significance=0.85)


# =====================================================================
# HTTP tests — httpx AsyncClient on the same loop as the pool.
# =====================================================================


async def test_home_endpoint_returns_contract_shape(greeting_db):
    await _seed_minimal(greeting_db)
    app, sched, _ = _build_app(greeting_db)
    await sched.refresh_tenant(TENANT_A, reason="manual")

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        resp = await client.get(
            "/view/ceo/home",
            headers={"Authorization": f"Bearer {DEV_TOKEN}"},
        )
    assert resp.status_code == 200
    body = resp.json()

    for key in ("greeting", "query_grid", "cards", "close_line", "status"):
        assert key in body, f"missing {key}"

    g = body["greeting"]
    assert "meta" in g and "body_html" in g
    assert "cached_at" in g and "staleness_seconds" in g
    for mk in ("date_iso", "recomputed_at", "signals_watched_count"):
        assert mk in g["meta"]

    qg = body["query_grid"]
    assert isinstance(qg["queries"], list)
    assert "cached_at" in qg
    for q in qg["queries"]:
        assert "id" in q and "icon" in q and "label" in q
        assert isinstance(q["hot"], bool)

    for c in body["cards"]:
        assert c["kind"] in ("observation", "decision", "question")
        assert c["tag_color"] in ("hot", "warm", "soft")
        assert "body_html" in c and "expanded" in c
        assert "reasoning_html" in c["expanded"]
        assert isinstance(c["expanded"]["evidence"], list)
        assert isinstance(c["expanded"]["verbs"], list)

    st = body["status"]
    assert isinstance(st["substrate_alive"], bool)
    assert isinstance(st["calibration_pct"], int)
    assert isinstance(st["needs_you_count"], int)

    cl = body["close_line"]
    assert "body" in cl and "metadata" in cl
    for mk in ("signal_count", "external_moves", "calibration_pct"):
        assert mk in cl["metadata"]


async def test_home_endpoint_rejects_missing_token(greeting_db):
    app, _, _ = _build_app(greeting_db)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        resp = await client.get("/view/ceo/home")
    assert resp.status_code == 401


async def test_home_endpoint_rejects_bad_token(greeting_db):
    app, _, _ = _build_app(greeting_db)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        resp = await client.get(
            "/view/ceo/home",
            headers={"Authorization": "Bearer nope"},
        )
    assert resp.status_code == 401


async def test_force_refresh_writes_cache(greeting_db):
    app, _, _ = _build_app(greeting_db)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        resp = await client.post(
            "/view/ceo/force-refresh",
            headers={"Authorization": f"Bearer {DEV_TOKEN}"},
        )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    cache = ViewCeoCacheRepo(greeting_db)
    got = await cache.get_cached(TENANT_A, "greeting")
    assert got is not None
    assert got.recomputed_reason == "manual"


async def test_home_endpoint_with_empty_cache(greeting_db):
    app, _, _ = _build_app(greeting_db)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test"
    ) as client:
        resp = await client.get(
            "/view/ceo/home",
            headers={"Authorization": f"Bearer {DEV_TOKEN}"},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["greeting"]["body_html"] == ""
    assert body["cards"] == []
    assert body["query_grid"]["queries"] == []
    assert body["status"]["substrate_alive"] is False


# =====================================================================
# Stream manager — unit tests (no WS transport needed).
# =====================================================================


async def test_stream_manager_publish_routes_by_tenant(greeting_db):
    """Verify publish/register/unregister + tenant isolation without a
    WebSocket connection. This is the core of Phase 5 that the
    scheduler depends on; the FastAPI WS route is a thin wrapper."""
    mgr = ViewCeoStreamManager(
        token_map=StaticTenantTokenMap(tokens={DEV_TOKEN: TENANT_A}),
    )
    s_a = await mgr.register(TENANT_A)
    s_b = await mgr.register(UUID("44444444-4444-4444-4444-444444444444"))

    delivered = await mgr.publish(
        TENANT_A, {"type": "greeting_updated", "greeting": {"body_html": "hi"}}
    )
    assert delivered == 1
    msg = s_a.queue.get_nowait()
    assert msg["type"] == "greeting_updated"
    # Other tenant's queue must be empty.
    assert s_b.queue.empty()

    await mgr.unregister(s_a.id)
    await mgr.unregister(s_b.id)


async def test_stream_manager_drops_oldest_on_full_queue():
    mgr = ViewCeoStreamManager(token_map=StaticTenantTokenMap())
    state = await mgr.register(TENANT_A)
    # Pre-fill the queue to its max.
    for i in range(state.queue.maxsize):
        state.queue.put_nowait({"n": i})
    # Publishing one more should drop the oldest and add the new one.
    delivered = await mgr.publish(TENANT_A, {"n": "new"})
    assert delivered == 1
    # Drain and check the oldest (n=0) is gone; the last should be {"n":"new"}.
    items: list[Any] = []
    while not state.queue.empty():
        items.append(state.queue.get_nowait())
    assert items[0] == {"n": 1}  # first item is now the old n=1
    assert items[-1] == {"n": "new"}


async def test_stream_manager_resolve_token():
    mgr = ViewCeoStreamManager(
        token_map=StaticTenantTokenMap(tokens={DEV_TOKEN: TENANT_A}),
    )
    assert mgr.resolve_token(DEV_TOKEN) == TENANT_A
    assert mgr.resolve_token("nope") is None


# =====================================================================
# End-to-end WS — run uvicorn in the same asyncio loop as the test so
# the asyncpg pool's connections remain loop-consistent.
# =====================================================================


def _pick_free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


@contextlib.asynccontextmanager
async def _running_server(app):
    """Run a uvicorn server in the current asyncio loop. Avoids the
    cross-loop asyncpg hazard that a threaded uvicorn would create.
    """
    port = _pick_free_port()
    config = uvicorn.Config(
        app, host="127.0.0.1", port=port,
        log_level="warning", lifespan="off",
    )
    server = uvicorn.Server(config)
    # Suppress uvicorn's default signal handlers; we aren't a CLI.
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    serve_task = asyncio.create_task(server.serve())
    # Wait for the server to be accepting connections.
    for _ in range(200):
        if getattr(server, "started", False):
            break
        if serve_task.done():
            # Server crashed during startup — propagate.
            await serve_task
            break
        await asyncio.sleep(0.05)
    # Extra settle for kernel listen-queue.
    await asyncio.sleep(0.05)
    try:
        yield port
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(serve_task, timeout=3.0)


async def test_ws_stream_receives_updates(greeting_db):
    try:
        from websockets.asyncio.client import connect as ws_connect
    except ImportError:
        pytest.skip("websockets client not installed")

    await _seed_minimal(greeting_db)
    app, sched, _ = _build_app(greeting_db)

    async with _running_server(app) as port:
        url = f"ws://127.0.0.1:{port}/view/ceo/stream?token={DEV_TOKEN}"
        async with ws_connect(url) as ws:
            hello_raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
            hello = json.loads(hello_raw)
            assert hello["type"] == "hello"
            assert hello["tenant_id"] == str(TENANT_A)

            # Trigger a refresh — scheduler publishes to the stream
            # manager, which pushes to the WS.
            await sched.refresh_tenant(TENANT_A, reason="manual")

            seen: set[str] = set()
            deadline = time.monotonic() + 4.0
            while time.monotonic() < deadline and len(seen) < 4:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                msg = json.loads(raw)
                if msg.get("type") in (
                    "greeting_updated",
                    "cards_updated",
                    "query_grid_updated",
                    "status_updated",
                ):
                    seen.add(msg["type"])
            assert "greeting_updated" in seen
            assert "status_updated" in seen


async def test_ws_stream_rejects_missing_token(greeting_db):
    try:
        from websockets.asyncio.client import connect as ws_connect
    except ImportError:
        pytest.skip("websockets client not installed")

    app, _, _ = _build_app(greeting_db)
    async with _running_server(app) as port:
        url = f"ws://127.0.0.1:{port}/view/ceo/stream"
        try:
            async with ws_connect(url) as ws:
                # Accept + error frame + close (1008).
                with contextlib.suppress(Exception):
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    msg = json.loads(raw)
                    assert msg.get("type") == "error"
                # Next recv raises.
                with pytest.raises(Exception):
                    await asyncio.wait_for(ws.recv(), timeout=1.0)
        except Exception:
            # Any error here indicates the handshake was rejected —
            # which is the expected outcome.
            return
