"""services/integrations/discord/gateway/lifecycle.py — M4.3 orchestrator.

Wraps the existing `GatewayWorker` with the M4.1 Redis lease and M4.2
session-state load/save lifecycle. Per ingestion LLD §1.5 + the M4
master prompt.

Production startup sequence:

    state-load  →  lease-acquire  →  RESUME-or-IDENTIFY  →  WS loop

  1. Connect to Redis + Postgres.
  2. Load `gateway_session_state` (returns None if absent or stale per
     STALENESS_THRESHOLD=4min).
  3. Acquire the Redis lease (backoff-bounded; exit non-zero on
     timeout — orchestrator restarts the pod, which retries).
  4. Construct DiscordGatewayClient with the loaded state injected.
     The client will RESUME if state is present + session_id non-NULL;
     IDENTIFY fresh otherwise. The choice is logged at INFO so
     operators can see which path each restart took.
  5. Run the existing `GatewayWorker.run_forever()` (which owns the
     connect → dispatch → reconnect loop).
  6. In parallel: refresh tick every `refresh_interval_seconds`. On
     refresh failure (lease lost — usually because we paused past the
     30s TTL and another pod took over), gracefully request worker
     shutdown. **Do not fight for the lease.**
  7. On shutdown (SIGTERM or lease loss): release the lease cleanly.

This module is a thin orchestration layer. It does NOT replace the
existing client / worker — it composes them with the M4.1/M4.2
primitives.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
from dataclasses import dataclass
from typing import Awaitable, Callable

import asyncpg
from redis.asyncio import Redis as AsyncRedis

from services.integrations.discord.gateway.client import (
    DiscordGatewayClient,
    GatewaySessionState,
)
from services.integrations.discord.gateway.leader_lock import (
    DEFAULT_REFRESH_INTERVAL_SECONDS,
    DEFAULT_TTL_SECONDS,
    LeaderLock,
)
from services.integrations.discord.gateway.session_state import (
    PersistedGatewaySession,
    load_session_state,
    save_session_state,
)


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Configuration.
# ---------------------------------------------------------------------
@dataclass
class LifecycleConfig:
    """Configuration knobs for the M4.3 orchestrator.

    Defaults match the M4 work order:
      - lease TTL 30s, refresh every 10s (M4.1)
      - lease-acquire backoff: 1s → 2 → 4 … capped at 30s, total cap 5min
      - staleness threshold for session state: 4 min (M4.2)
    """

    application_id: str
    shard_id: int = 0
    lease_ttl_seconds: int = DEFAULT_TTL_SECONDS
    lease_refresh_interval_seconds: int = DEFAULT_REFRESH_INTERVAL_SECONDS
    # Lease acquire-on-startup backoff schedule.
    lease_acquire_initial_backoff_s: float = 1.0
    lease_acquire_max_backoff_s: float = 30.0
    # Total time we'll spend trying to acquire the lease before giving
    # up and exiting non-zero. The orchestrator (k8s, supervisor)
    # restarts the pod which retries.
    lease_acquire_total_timeout_s: float = 300.0  # 5 minutes


# ---------------------------------------------------------------------
# Lease acquire with backoff.
# ---------------------------------------------------------------------
async def acquire_lease_with_backoff(
    lock: LeaderLock,
    *,
    config: LifecycleConfig,
    stop_event: asyncio.Event,
) -> bool:
    """Block until the lease is acquired OR the total timeout elapses
    OR `stop_event` is set. Returns True on acquired, False on
    timeout/shutdown.

    Backoff: exponential with ±25% jitter to avoid two pods retrying
    in lockstep. Capped at `lease_acquire_max_backoff_s`.
    """
    deadline = (
        asyncio.get_event_loop().time()
        + config.lease_acquire_total_timeout_s
    )
    backoff_s = config.lease_acquire_initial_backoff_s
    attempt = 0

    while not stop_event.is_set():
        if await lock.acquire():
            log.info(
                "gateway_lifecycle.lease_acquired",
                extra={
                    "attempt": attempt,
                    "lease_value": lock.lease_value,
                },
            )
            return True

        if asyncio.get_event_loop().time() >= deadline:
            log.error(
                "gateway_lifecycle.lease_acquire_timeout",
                extra={
                    "attempt": attempt,
                    "total_timeout_s": config.lease_acquire_total_timeout_s,
                },
            )
            return False

        # ±25% jitter on the backoff.
        jitter = backoff_s * 0.25 * (2 * random.random() - 1)
        sleep_s = max(0.1, backoff_s + jitter)
        log.info(
            "gateway_lifecycle.lease_busy",
            extra={"attempt": attempt, "sleep_s": sleep_s},
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=sleep_s)
            # Stop event fired during backoff — abandon.
            return False
        except asyncio.TimeoutError:
            pass

        attempt += 1
        backoff_s = min(
            config.lease_acquire_max_backoff_s,
            backoff_s * 2.0,
        )

    return False


# ---------------------------------------------------------------------
# Refresh tick.
# ---------------------------------------------------------------------
async def lease_refresh_loop(
    lock: LeaderLock,
    *,
    interval_s: float,
    on_lost: Callable[[], Awaitable[None]] | None = None,
    stop_event: asyncio.Event,
) -> None:
    """Periodically refresh the lease. On refresh failure, fire
    `on_lost` (usually shuts down the WS client) and exit the loop.

    PRIME DIRECTIVE: do NOT attempt to re-acquire the lease after a
    refresh failure. Another pod took over; let it own the surface.
    Fighting the new holder (by re-acquiring while the WS is still
    open) would produce duplicate frame delivery — exactly the
    M4 work order's "two pods with the same bot token" failure mode.
    """
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_s)
            return  # stop_event fired — clean exit
        except asyncio.TimeoutError:
            pass  # tick

        try:
            still_held = await lock.refresh()
        except Exception:  # noqa: BLE001
            log.exception("gateway_lifecycle.lease_refresh_error")
            still_held = False

        if not still_held:
            log.warning(
                "gateway_lifecycle.lease_lost",
                extra={"lease_value": lock.lease_value},
            )
            if on_lost is not None:
                try:
                    await on_lost()
                except Exception:  # noqa: BLE001
                    log.exception("gateway_lifecycle.on_lost_error")
            return


# ---------------------------------------------------------------------
# State translation: persisted → in-memory.
# ---------------------------------------------------------------------
def persisted_to_in_memory(
    persisted: PersistedGatewaySession | None,
) -> GatewaySessionState | None:
    """Adapt the M4.2 Pydantic row into the M2/M4 in-memory dataclass
    that DiscordGatewayClient operates on. Returns None when the input
    is None — the client will then construct a fresh state and
    IDENTIFY rather than RESUME.

    Note: only the fields needed for RESUME-or-IDENTIFY decision are
    transferred. `last_heartbeat_ack` is reset to a fresh monotonic
    clock value on connect (the persisted timestamp is from a previous
    process and meaningless in the new monotonic frame).
    """
    if persisted is None:
        return None
    if persisted.session_id is None or persisted.last_seq is None:
        # Row present but session_id never set (worker died before
        # first READY). Treat as no state — IDENTIFY fresh.
        return None
    return GatewaySessionState(
        session_id=persisted.session_id,
        resume_gateway_url=persisted.resume_gateway_url,
        last_seq=persisted.last_seq,
        heartbeat_interval_ms=persisted.heartbeat_interval_ms or 0,
        application_id=persisted.application_id,
    )


# ---------------------------------------------------------------------
# Save hook factory.
# ---------------------------------------------------------------------
def make_save_hook(
    pool: asyncpg.Pool,
    *,
    application_id: str,
    shard_id: int = 0,
    lease_holder: str | None = None,
) -> Callable[[GatewaySessionState], Awaitable[None]]:
    """Build the per-frame save hook handed to DiscordGatewayClient.

    Returned callable is invoked by the client's `_dispatch_loop` AFTER
    every successful (or non-fatal-failed) `dispatch_handler` return on
    an op-0 DISPATCH frame with `seq`. The call is wrapped in
    `asyncio.create_task` by the client — see client.py.
    """
    async def _save(state: GatewaySessionState) -> None:
        await save_session_state(
            pool,
            application_id=application_id,
            shard_id=shard_id,
            session_id=state.session_id,
            resume_gateway_url=state.resume_gateway_url,
            last_seq=state.last_seq,
            heartbeat_interval_ms=state.heartbeat_interval_ms or None,
            leader_lease_holder=lease_holder,
        )
    return _save


__all__ = [
    "LifecycleConfig",
    "acquire_lease_with_backoff",
    "lease_refresh_loop",
    "make_save_hook",
    "persisted_to_in_memory",
]
