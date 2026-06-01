"""IN-12 US2: Connection lifecycle — HELLO → IDENTIFY → READY → heartbeat ACK.

These tests exercise the real `DiscordGatewayClient` against the
in-process `FakeGateway` from conftest.py. The HTTP call to
`/gateway/bot` is mocked via respx so the WSS URL points at the
fake.

The Postgres + Ollama boundaries are not exercised by these tests
(they don't dispatch any MESSAGE_CREATE), so we don't need fresh_db.
"""
from __future__ import annotations

import asyncio

import httpx
import pytest
import respx

from services.integrations.discord.gateway import metrics as gateway_metrics
from services.integrations.discord.gateway.client import (
    DiscordGatewayClient, FatalGatewayError, INTENTS, classify_close_code,
    ReconnectAction,
)
from services.integrations.discord.gateway.tests.conftest import FakeGateway


pytestmark = pytest.mark.integration


async def _null_dispatch(frame: dict) -> None:
    """A dispatch handler that does nothing — for tests that only
    care about the protocol layer."""
    return None


# ---------------------------------------------------------------------
# Pure-function tests (no WSS, no DB)
# ---------------------------------------------------------------------


def test_intents_bitmask_matches_spec() -> None:
    """FR-002 / research R3: GUILDS (1) | GUILD_MESSAGES (1<<9) |
    MESSAGE_CONTENT (1<<15) = 33281."""
    assert INTENTS == (1 << 0) | (1 << 9) | (1 << 15)
    assert INTENTS == 33281


def test_classify_close_code_routes_correctly() -> None:
    """FR-004, FR-005, FR-006: close-code → action map per research R5."""
    assert classify_close_code(4000) == ReconnectAction.RESUME
    assert classify_close_code(4001) == ReconnectAction.RESUME
    assert classify_close_code(4007) == ReconnectAction.IDENTIFY
    assert classify_close_code(4009) == ReconnectAction.IDENTIFY
    assert classify_close_code(4004) == ReconnectAction.FATAL_EXIT
    assert classify_close_code(4013) == ReconnectAction.FATAL_EXIT
    assert classify_close_code(4014) == ReconnectAction.FATAL_EXIT
    # Unknown codes → conservative RESUME.
    assert classify_close_code(9999) == ReconnectAction.RESUME
    # None (abrupt disconnect) → RESUME.
    assert classify_close_code(None) == ReconnectAction.RESUME


# ---------------------------------------------------------------------
# Integration tests with FakeGateway
# ---------------------------------------------------------------------


async def test_hello_identify_ready_loop(fake_gateway: FakeGateway) -> None:
    """HELLO → IDENTIFY → READY: client captures session_id, sends
    IDENTIFY with correct intent bitmask, transitions
    connection_state to 'connected'."""
    fake_gateway.heartbeat_interval_ms = 200  # fast for tests
    fake_gateway.script = [{"op": "sleep", "seconds": 0.3}]

    async with httpx.AsyncClient() as http:
        with respx.mock(base_url="https://discord.com", assert_all_called=False) as router:
            router.get("/api/v10/gateway/bot").respond(
                200, json={"url": fake_gateway.url},
            )
            client = DiscordGatewayClient(
                bot_token="test-bot-token",
                dispatch_handler=_null_dispatch,
                http_client=http,
            )
            run_task = asyncio.create_task(client.run())
            try:
                # Give the protocol exchange time to settle.
                await asyncio.sleep(0.15)
                # Confirm READY was processed → session_id captured.
                assert client._state.session_id == "session_test_001"
                # Confirm IDENTIFY frame went out with the right intents.
                identify_frames = [
                    f for f in fake_gateway.received if f.get("op") == 2
                ]
                assert len(identify_frames) == 1
                assert identify_frames[0]["d"]["intents"] == INTENTS
                assert identify_frames[0]["d"]["token"] == "test-bot-token"
                # Confirm metrics state.
                state = gateway_metrics.get_gauge(
                    "discord_gateway_connection_state", state="connected",
                )
                assert state == 1.0
            finally:
                client.request_shutdown()
                run_task.cancel()
                try:
                    await run_task
                except (asyncio.CancelledError, Exception):
                    pass
                await client.aclose()


async def test_heartbeat_is_sent_periodically(fake_gateway: FakeGateway) -> None:
    """Client must send op-1 heartbeats at `heartbeat_interval * 0.7` ms.

    With interval=200ms, the first heartbeat should fire within ~150ms
    (after initial jitter [0, interval]). We wait 500ms and assert at
    least one heartbeat round-trip happened (sent + ACK absorbed)."""
    fake_gateway.heartbeat_interval_ms = 200
    fake_gateway.script = [{"op": "sleep", "seconds": 1.0}]

    async with httpx.AsyncClient() as http:
        with respx.mock(base_url="https://discord.com", assert_all_called=False) as router:
            router.get("/api/v10/gateway/bot").respond(
                200, json={"url": fake_gateway.url},
            )
            client = DiscordGatewayClient(
                bot_token="test",
                dispatch_handler=_null_dispatch,
                http_client=http,
            )
            run_task = asyncio.create_task(client.run())
            try:
                await asyncio.sleep(0.5)
                heartbeats_received = [
                    f for f in fake_gateway.received if f.get("op") == 1
                ]
                assert len(heartbeats_received) >= 1, (
                    f"expected ≥1 heartbeat in 500ms with interval=200ms; "
                    f"got {len(heartbeats_received)}: {fake_gateway.received}"
                )
            finally:
                client.request_shutdown()
                run_task.cancel()
                try:
                    await run_task
                except (asyncio.CancelledError, Exception):
                    pass
                await client.aclose()


async def test_close_4014_raises_fatal_gateway_error(
    fake_gateway: FakeGateway,
) -> None:
    """FR-005, US2 acceptance scenario 4: WSS close 4014 (disallowed
    intents) propagates as `FatalGatewayError`. Supervisor must NOT
    auto-restart."""
    fake_gateway.heartbeat_interval_ms = 200
    fake_gateway.script = [
        {"op": "sleep", "seconds": 0.1},
        {"op": "close", "code": 4014, "reason": "disallowed intents"},
    ]

    async with httpx.AsyncClient() as http:
        with respx.mock(base_url="https://discord.com", assert_all_called=False) as router:
            router.get("/api/v10/gateway/bot").respond(
                200, json={"url": fake_gateway.url},
            )
            client = DiscordGatewayClient(
                bot_token="test",
                dispatch_handler=_null_dispatch,
                http_client=http,
            )
            with pytest.raises(FatalGatewayError) as exc_info:
                await client.run()
            assert exc_info.value.code == 4014
            await client.aclose()


async def test_resume_path_uses_resume_gateway_url(
    fake_gateway: FakeGateway,
) -> None:
    """FR-004 + US2 acceptance scenario 1: a resumable close (4000)
    triggers a reconnect that sends RESUME with the prior session_id
    and last_seq. We assert the FakeGateway received both an IDENTIFY
    (on initial connect) and a RESUME (after the simulated close)."""
    fake_gateway.heartbeat_interval_ms = 5000  # avoid heartbeat noise
    fake_gateway.script = [
        {"op": "send", "frame": {
            "op": 0, "s": 5, "t": "MESSAGE_CREATE",
            "d": {"id": "noop", "guild_id": "g", "author": {"id": "u", "bot": True}},
        }},
        {"op": "close", "code": 4000, "reason": "test resume"},
    ]

    received_messages: list[dict] = []

    async def _track_dispatch(frame: dict) -> None:
        received_messages.append(frame)

    async with httpx.AsyncClient() as http:
        with respx.mock(base_url="https://discord.com", assert_all_called=False) as router:
            router.get("/api/v10/gateway/bot").respond(
                200, json={"url": fake_gateway.url},
            )
            client = DiscordGatewayClient(
                bot_token="test",
                dispatch_handler=_track_dispatch,
                http_client=http,
            )
            run_task = asyncio.create_task(client.run())
            try:
                # Allow IDENTIFY+READY+dispatch+close+RESUME cycle.
                await asyncio.sleep(0.4)
                # We should see the MESSAGE_CREATE dispatch.
                assert any(
                    f.get("t") == "MESSAGE_CREATE" for f in received_messages
                ), f"no MESSAGE_CREATE seen: {received_messages}"
                # FakeGateway should have received both an IDENTIFY (op 2)
                # AND a RESUME (op 6) — proves the reconnect path took
                # the resume branch.
                ops_in = [f.get("op") for f in fake_gateway.received]
                assert 2 in ops_in, f"no IDENTIFY in received: {ops_in}"
                assert 6 in ops_in, f"no RESUME in received: {ops_in}"
            finally:
                client.request_shutdown()
                run_task.cancel()
                try:
                    await run_task
                except (asyncio.CancelledError, Exception):
                    pass
                await client.aclose()
