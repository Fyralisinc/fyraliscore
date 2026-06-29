"""IN-12 US5: SIGTERM → graceful drain → exit 0.

FR-013: SIGTERM stops accepting new dispatches, drains in-flight ones,
sends WSS close 1000, returns exit code 0 within 5 s.

These tests exercise `GatewayWorker.run_forever()` against the
FakeGateway and assert the graceful-shutdown semantics. Real signals
are tricky to deliver inside a pytest event loop; we exercise the
public hook `request_shutdown()` which is what the SIGTERM handler
ultimately calls.
"""
from __future__ import annotations

import asyncio

import httpx
import pytest
import respx

from services.ingest.integrations.discord.gateway.dispatch import DispatchDeps
from services.ingest.integrations.discord.gateway.tests.conftest import FakeGateway
from services.ingest.integrations.discord.gateway import worker as worker_mod
from services.ingest.integrations.discord.gateway.worker import GatewayWorker


pytestmark = pytest.mark.integration


async def _null_dispatch(frame: dict) -> None:
    return None


async def test_request_shutdown_returns_exit_zero(
    fake_gateway: FakeGateway, dispatch_deps: DispatchDeps,
) -> None:
    """SIGTERM equivalent: `request_shutdown()` causes `run_forever()`
    to return exit code 0 within the documented 5 s grace."""
    fake_gateway.heartbeat_interval_ms = 200
    fake_gateway.script = [{"op": "sleep", "seconds": 10.0}]

    async with httpx.AsyncClient():
        with respx.mock(base_url="https://discord.com", assert_all_called=False) as router:
            router.get("/api/v10/gateway/bot").respond(
                200, json={"url": fake_gateway.url},
            )
            worker = GatewayWorker(bot_token="test", deps=dispatch_deps)
            run_task = asyncio.create_task(worker.run_forever())
            # Let connect + IDENTIFY + READY complete.
            await asyncio.sleep(0.2)
            # Trigger shutdown.
            worker.request_shutdown()
            # Wait for graceful exit, with a hard cap above the 5 s
            # documented grace.
            try:
                exit_code = await asyncio.wait_for(run_task, timeout=6.0)
            except asyncio.TimeoutError:
                run_task.cancel()
                pytest.fail("worker did not exit within 6 s of request_shutdown()")
            assert exit_code == 0


async def test_request_shutdown_during_connect_failure_loop_exits_clean(
    dispatch_deps: DispatchDeps,
) -> None:
    """If shutdown is requested while the worker is in its
    connect-failure backoff loop (no FakeGateway → connect fails),
    the worker should observe the shutdown event and exit cleanly
    without waiting out the full backoff."""
    async with httpx.AsyncClient():
        with respx.mock(base_url="https://discord.com", assert_all_called=False) as router:
            # Point at an unreachable URL — connect attempts will fail.
            router.get("/api/v10/gateway/bot").respond(
                200, json={"url": "ws://127.0.0.1:1"},  # port 1: reserved
            )
            worker = GatewayWorker(bot_token="test", deps=dispatch_deps)
            run_task = asyncio.create_task(worker.run_forever())
            # Let one connect-fail cycle complete.
            await asyncio.sleep(0.3)
            worker.request_shutdown()
            try:
                exit_code = await asyncio.wait_for(run_task, timeout=3.0)
            except asyncio.TimeoutError:
                run_task.cancel()
                pytest.fail("worker stuck in backoff loop, did not honor shutdown")
            assert exit_code == 0


async def test_worker_rereads_bot_token_provider_between_attempts(
    monkeypatch: pytest.MonkeyPatch,
    dispatch_deps: DispatchDeps,
) -> None:
    created_tokens: list[str] = []
    tokens = iter(["first-token", "second-token"])

    class _FakeClient:
        def __init__(self, *, bot_token: str, **_kwargs) -> None:
            created_tokens.append(bot_token)

        async def run(self) -> None:
            if len(created_tokens) == 1:
                raise RuntimeError("force reconnect")
            return None

        def request_shutdown(self) -> None:
            return None

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(worker_mod, "DiscordGatewayClient", _FakeClient)
    monkeypatch.setattr(worker_mod, "_next_backoff", lambda _attempt: 0.0)

    worker = GatewayWorker(
        bot_token_provider=lambda: next(tokens),
        deps=dispatch_deps,
    )

    assert await worker.run_forever() == 0
    assert created_tokens == ["first-token", "second-token"]
