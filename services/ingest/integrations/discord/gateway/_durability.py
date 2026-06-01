"""A6 — Discord Gateway broker-ack durability barrier.

A small dedicated module so the load-bearing test simulation in
[tests/_subprocess_entrypoint.py](tests/_subprocess_entrypoint.py)
can call the production flush logic without pulling in httpx +
websockets (the subprocess does not speak HTTP or WSS — it
hand-rolls a dispatch-loop simulation around `shadow_write_raw` and
`save_session_state` to exercise the SIGKILL/recovery property).

Originally extracted from `DiscordGatewayClient._pre_save_flush`
during A6 Phase 3 (Option A from the Phase 3 gate review). The
client.py method is now a thin wrapper bound to its instance's
producer.

See:
  - docs/ingestion/05-lld-amendments.md §A6 (the finding)
  - docs/decisions/a6-resolution.md (Phase 1 decision + Phase 3
    refactor + import-graph trade-off note)
"""
from __future__ import annotations

from typing import Any


async def pre_save_flush(
    kafka_producer: Any,
    *,
    timeout_seconds: float,
) -> None:
    """Flush the Kafka producer so the broker has acked all in-flight
    shadow-write messages before the caller advances the persisted
    `last_seq`. Raises `TimeoutError` when the flush returns with
    messages still in queue (timeout exhausted); raises whatever the
    producer raises on broker errors. The caller decides what to do
    on failure (production client: skip save + increment failure
    metric; test simulation: let exception propagate to fail the test
    loudly).

    No-op when `kafka_producer` is None — used by tests that don't
    wire the shadow producer.

    Call sites (the only two should ever be):

      - `DiscordGatewayClient._dispatch_loop` (production) via the
        thin `_pre_save_flush` method wrapper.
      - `tests/_subprocess_entrypoint.py` (test simulation) — calls
        directly. The subprocess uses this function instead of the
        method wrapper because it doesn't instantiate a client.

    A6 Phase 3 split: this function used to live on
    `DiscordGatewayClient`. Was extracted to a free function so the
    cross-process load-bearing test exercises the production code
    path. Moved into a dedicated module (Phase 3 follow-up) so the
    subprocess simulation doesn't pull httpx + websockets into its
    import graph just to access this one function.
    """
    if kafka_producer is None:
        return
    remaining = await kafka_producer.flush(timeout_seconds)
    if remaining > 0:
        raise TimeoutError(
            f"Kafka flush returned with {remaining} message(s) still "
            f"in queue after {timeout_seconds}s timeout"
        )


__all__ = ["pre_save_flush"]
