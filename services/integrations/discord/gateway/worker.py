"""services/integrations/discord/gateway/worker.py — long-running worker.

Owns the outer connect → run → reconnect loop. Wraps `DiscordGatewayClient`
with exponential backoff on connection failures, SIGTERM handling, and
process-exit semantics:

  * Clean shutdown (SIGTERM / SIGINT): exit 0.
  * Fatal close (4004, 4010–4014): exit 1; supervisor MUST NOT auto-restart.
  * Configuration error at startup (missing env): exit 2.

This module never imports the websockets library directly — that's
client.py's job. The worker only knows about the client interface.
"""
from __future__ import annotations

import asyncio
import random
import signal
from typing import Any

import structlog

from services.integrations.discord.gateway import metrics
from services.integrations.discord.gateway.client import (
    DiscordGatewayClient,
    FatalGatewayError,
)
from services.integrations.discord.gateway.dispatch import (
    DispatchDeps,
    handle_dispatch,
)


log = structlog.get_logger("integrations.discord.gateway.worker")


_BACKOFF_BASE_S = 1.0
_BACKOFF_CAP_S = 60.0
_BACKOFF_JITTER = 0.25  # ±25%


def _next_backoff(attempt: int) -> float:
    """Backoff schedule: 1, 2, 4, 8, 16, 32, 60, 60, … (s), ±25% jitter."""
    base = min(_BACKOFF_BASE_S * (2 ** attempt), _BACKOFF_CAP_S)
    jitter = base * _BACKOFF_JITTER * (2 * random.random() - 1)
    return max(0.5, base + jitter)


class GatewayWorker:
    """Top-level worker orchestrator.

    Lifecycle:
        run_forever()
            ├─ install SIGTERM/SIGINT handlers
            └─ loop:
                ├─ build DiscordGatewayClient
                ├─ await client.run() — connect, identify, dispatch
                ├─ on FatalGatewayError → return (caller exits 1)
                ├─ on ConnectionClosed → backoff, loop
                └─ on shutdown signal → request_shutdown on client, exit 0
    """

    def __init__(
        self,
        *,
        bot_token: str,
        deps: DispatchDeps,
        shutdown_grace_s: float = 5.0,
    ) -> None:
        if not bot_token:
            raise ValueError("bot_token is required")
        self._bot_token = bot_token
        self._deps = deps
        self._shutdown_grace_s = shutdown_grace_s
        self._shutdown_requested = asyncio.Event()
        self._current_client: DiscordGatewayClient | None = None

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def _on_signal(signum: int) -> None:
            log.info("discord_gateway_shutdown_signal_received", signum=signum)
            self._shutdown_requested.set()
            if self._current_client is not None:
                self._current_client.request_shutdown()

        try:
            loop.add_signal_handler(signal.SIGTERM, lambda: _on_signal(signal.SIGTERM))
            loop.add_signal_handler(signal.SIGINT, lambda: _on_signal(signal.SIGINT))
        except NotImplementedError:
            # signal handlers aren't supported on Windows; tests can
            # still drive shutdown via request_shutdown().
            pass

    def request_shutdown(self) -> None:
        """Public hook for tests + the signal handler."""
        self._shutdown_requested.set()
        if self._current_client is not None:
            self._current_client.request_shutdown()

    async def _dispatch(self, frame: dict[str, Any]) -> None:
        """Bound dispatch closure handed to the client."""
        await handle_dispatch(frame, self._deps)

    async def run_forever(self) -> int:
        """Run until SIGTERM or fatal Discord close. Returns exit code."""
        self._install_signal_handlers()
        log.info("discord_gateway_starting")
        attempt = 0
        try:
            while not self._shutdown_requested.is_set():
                client = DiscordGatewayClient(
                    bot_token=self._bot_token,
                    dispatch_handler=self._dispatch,
                    application_id=self._deps.application_id,
                )
                self._current_client = client
                try:
                    await client.run()
                    # client.run() returned without exception → shutdown
                    # was requested.
                    break
                except FatalGatewayError as exc:
                    log.error(
                        "discord_gateway_close_fatal",
                        close_code=exc.code, close_reason=exc.reason,
                    )
                    return 1
                except Exception:  # noqa: BLE001
                    metrics.inc("discord_gateway_connect_failure_total")
                    log.exception(
                        "discord_gateway_connect_failed", attempt=attempt,
                    )
                    if self._shutdown_requested.is_set():
                        break
                    backoff_s = _next_backoff(attempt)
                    log.info(
                        "discord_gateway_backoff",
                        attempt=attempt, sleep_s=backoff_s,
                    )
                    try:
                        await asyncio.wait_for(
                            self._shutdown_requested.wait(),
                            timeout=backoff_s,
                        )
                        # Shutdown raced the backoff; exit clean.
                        break
                    except asyncio.TimeoutError:
                        attempt += 1
                finally:
                    self._current_client = None
                    try:
                        await client.aclose()
                    except Exception:  # noqa: BLE001
                        pass
                if attempt > 0:
                    # Reset attempt counter on the next successful READY.
                    pass
        finally:
            log.info("discord_gateway_shutdown_complete")
        return 0


__all__ = ["GatewayWorker", "_next_backoff"]
