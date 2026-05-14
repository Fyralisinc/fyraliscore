"""Test fixtures for IN-12 Gateway worker tests.

Two layers:

  `dispatch_deps` — assembled DispatchDeps fixture (real pool, real
    tenant resolver, no embedder) for tests that exercise the
    dispatch → ingest → observation path. The WSS layer is not
    involved.

  `fake_gateway` — in-process WSS server that speaks the documented
    Discord opcode protocol. For tests that exercise the connection
    lifecycle (HELLO/IDENTIFY/READY/heartbeat/RESUME). Used by
    test_client_lifecycle.py and test_client_reconnect.py.

Per Constitution §IV the DB is real (`fresh_db` fixture). The WSS
boundary is external network — explicitly permitted to mock under §X
(see research.md R12).
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, AsyncIterator
from uuid import UUID

import asyncpg
import pytest
import websockets

from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.integrations.discord.gateway import metrics as gateway_metrics
from services.integrations.discord.gateway.dispatch import DispatchDeps
from services.webhooks.tenant_resolver import (
    InstallationCache,
    TenantResolverDeps,
    build_tenant_resolver,
    noop_metrics,
)


_APPLICATION_ID = "1504474857914499194"
_TEST_GUILD_ID = "1504477009927999569"


@pytest.fixture(autouse=True)
def _reset_gateway_metrics() -> None:
    gateway_metrics.reset()


@pytest.fixture
async def dispatch_deps(fresh_db: asyncpg.Pool) -> DispatchDeps:
    """A fully-assembled DispatchDeps backed by `fresh_db`."""
    tenant_resolver = build_tenant_resolver(
        TenantResolverDeps(
            pool=fresh_db,
            cache=InstallationCache(),
            clock=time.monotonic,
            metrics=noop_metrics(),
        )
    )
    return DispatchDeps(
        pool=fresh_db,
        tenant_resolver=tenant_resolver,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=None,
        application_id=_APPLICATION_ID,
    )


async def _seed_install(
    pool: asyncpg.Pool, tenant_id: UUID, guild_id: str = _TEST_GUILD_ID,
) -> None:
    """Insert a minimal `provider_installations` row so tenant
    resolution succeeds for the test guild."""
    from lib.shared.ids import uuid7
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, 'discord', $3, TRUE)
        """,
        uuid7(), tenant_id, guild_id,
    )


@pytest.fixture
async def seeded_tenant(fresh_db: asyncpg.Pool) -> UUID:
    """Create a tenant and a provider_installations row for
    _TEST_GUILD_ID so dispatch tests can resolve it."""
    from uuid import uuid4
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"discord-gateway-{tid.hex[:8]}",
    )
    await _seed_install(fresh_db, tid)
    return tid


def make_message_create(
    *,
    message_id: str,
    content: str = "hello",
    guild_id: str = _TEST_GUILD_ID,
    channel_id: str = "channel_test_001",
    author_id: str = "user_test_001",
    author_bot: bool = False,
    webhook_id: str | None = None,
    attachments: list[dict[str, Any]] | None = None,
    mentions: list[dict[str, Any]] | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Build a synthetic MESSAGE_CREATE payload with sensible defaults.

    Mirrors the relevant fields of Discord's documented MESSAGE_CREATE
    event. All optional kwargs default to a benign value; tests override
    just what they need to exercise."""
    payload: dict[str, Any] = {
        "id": message_id,
        "channel_id": channel_id,
        "guild_id": guild_id,
        "content": content,
        "timestamp": timestamp or "2026-05-14T15:00:00.000+00:00",
        "author": {"id": author_id, "username": "tester", "bot": author_bot},
        "attachments": attachments or [],
        "mentions": mentions or [],
    }
    if webhook_id is not None:
        payload["webhook_id"] = webhook_id
    return payload


# ---------------------------------------------------------------------
# Fake Discord Gateway (WSS) — used by test_client_*.
# ---------------------------------------------------------------------

class FakeGateway:
    """In-process WSS server that speaks the documented Discord opcode
    protocol. Tests script the message stream and inject failures via
    helpers on this class.

    Lifecycle (typical):
        async with FakeGateway() as fg:
            # configure scripted events
            fg.script.append(dispatch_msg)
            fg.script.append(close(4000))
            # client connects to fg.url and consumes
    """

    def __init__(self) -> None:
        self.heartbeat_interval_ms = 1000
        self.script: list[dict[str, Any]] = []
        self.received: list[dict[str, Any]] = []
        self._server: Any = None
        self._port: int = 0
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self._port}"

    async def __aenter__(self) -> "FakeGateway":
        self._server = await websockets.serve(self._handler, "127.0.0.1", 0)
        for sock in self._server.sockets or []:
            self._port = sock.getsockname()[1]
            break
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handler(self, ws: Any) -> None:
        # Send HELLO immediately.
        await ws.send(json.dumps({
            "op": 10,
            "d": {"heartbeat_interval": self.heartbeat_interval_ms},
        }))
        # Receive IDENTIFY (or RESUME).
        ident = json.loads(await ws.recv())
        self.received.append(ident)
        if ident.get("op") == 2:
            await ws.send(json.dumps({
                "op": 0,
                "s": 1,
                "t": "READY",
                "d": {
                    "session_id": "session_test_001",
                    "resume_gateway_url": self.url,
                    "application": {"id": _APPLICATION_ID},
                },
            }))
        elif ident.get("op") == 6:
            await ws.send(json.dumps({
                "op": 0,
                "s": (ident.get("d") or {}).get("seq", 0) + 1,
                "t": "RESUMED",
                "d": {},
            }))

        # Run the scripted events; concurrently absorb heartbeats and
        # auto-ACK them.
        recv_task = asyncio.create_task(self._absorb(ws))
        try:
            for action in self.script:
                op = action.get("op")
                if op == "send":
                    await ws.send(json.dumps(action["frame"]))
                elif op == "close":
                    await ws.close(
                        code=action["code"],
                        reason=action.get("reason", ""),
                    )
                    return
                elif op == "sleep":
                    await asyncio.sleep(action["seconds"])
            # Park forever until the test closes the server.
            await asyncio.Event().wait()
        finally:
            recv_task.cancel()

    async def _absorb(self, ws: Any) -> None:
        try:
            async for raw in ws:
                frame = json.loads(raw)
                self.received.append(frame)
                if frame.get("op") == 1:
                    await ws.send(json.dumps({"op": 11, "d": None}))
        except Exception:
            return


@pytest.fixture
async def fake_gateway() -> AsyncIterator[FakeGateway]:
    """Async context-managed fake Discord gateway server."""
    async with FakeGateway() as fg:
        yield fg
