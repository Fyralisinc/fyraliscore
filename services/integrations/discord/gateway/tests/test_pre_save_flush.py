"""A6 — `_pre_save_flush` per-frame flush barrier tests.

Three tests, each pairing an internal-call assertion with an
observable-state assertion (per the Phase 1 review on this work-unit):

  1. test_flush_called_between_shadow_write_and_save_state
     — observable ordering: Postgres `last_seq` snapshot taken DURING
       flush shows the prior value; AFTER dispatch shows the new
       value. Proves flush ran before save.

  2. test_flush_timeout_does_not_block_indefinitely
     — unit test of the flush wrapper: when `kafka_producer.flush()`
       returns `remaining > 0` within `timeout_seconds`, our wrapper
       raises `TimeoutError` and the wall-time is bounded.

  3. test_flush_failure_does_not_save_state
     — observable post-failure state: pre-seed Postgres with
       `last_seq=N_old`, drive a frame with `s=N_new`, force flush to
       raise; assert Postgres STILL has `last_seq=N_old` (NOT N_new).
       The internal-call assertion (save hook not invoked) is the
       secondary check.

These tests drive a single op-0 DISPATCH frame through
`DiscordGatewayClient._dispatch_loop` using a minimal in-process fake
WebSocket. They do NOT use FakeGateway — the WSS protocol layer is
not under test here; the flush+save call site is.

See:
  - docs/decisions/a6-resolution.md (Phase 1 decision doc)
  - docs/ingestion/05-lld-amendments.md §A6 (amendments tracker)
  - services/integrations/discord/gateway/client.py::_pre_save_flush
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import time
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from services.integrations.discord.gateway import metrics as gateway_metrics
from services.integrations.discord.gateway.client import (
    DiscordGatewayClient,
    GatewaySessionState,
)
from services.integrations.discord.gateway.session_state import (
    load_session_state,
    save_session_state,
)


pytestmark = [pytest.mark.timeout(60)]


# ---------------------------------------------------------------------
# Test harness.
# ---------------------------------------------------------------------

class _FakeKafkaProducer:
    """Records flush calls, can be configured to fail/delay.

    Surface mimics services/ingestion/kafka/producer.py::IdempotentProducer.flush
    — returns an int count of messages still in queue (0 = all
    delivered). >0 → our `_pre_save_flush` raises TimeoutError.
    """

    def __init__(
        self,
        *,
        return_remaining: int = 0,
        raise_on_flush: BaseException | None = None,
        flush_delay_s: float = 0.0,
        during_flush_hook: Any = None,
    ) -> None:
        self.flush_calls: list[float] = []  # captured timeout args
        self._return_remaining = return_remaining
        self._raise = raise_on_flush
        self._delay_s = flush_delay_s
        self._during_flush_hook = during_flush_hook

    async def flush(self, timeout_seconds: float) -> int:
        if self._during_flush_hook is not None:
            await self._during_flush_hook()
        if self._delay_s > 0:
            await asyncio.sleep(self._delay_s)
        self.flush_calls.append(timeout_seconds)
        if self._raise is not None:
            raise self._raise
        return self._return_remaining


class _SingleFrameWS:
    """Minimal WebSocket stand-in. `recv()` returns the pre-loaded
    frame once, then awaits a shutdown event and raises to unwind the
    dispatch loop.
    """

    def __init__(self, frame: dict[str, Any], shutdown: asyncio.Event) -> None:
        self._payload = json.dumps(frame)
        self._sent = False
        self._shutdown = shutdown

    async def recv(self) -> str:
        if not self._sent:
            self._sent = True
            return self._payload
        # After the single frame, block until shutdown then raise so
        # the dispatch loop's `while not self._shutdown.is_set()` exits
        # on the next iteration check.
        await self._shutdown.wait()
        raise asyncio.CancelledError()

    async def close(self, code: int = 1000) -> None:  # noqa: ARG002
        return None


def _make_dispatch_frame(seq: int, message_id: str = "msg-x") -> dict[str, Any]:
    """Build a minimal op-0 DISPATCH frame with the required shape:
    op=0, t='MESSAGE_CREATE' (so the dispatch loop records the seq),
    s=seq. The dispatch_handler is mocked separately."""
    return {
        "op": 0,
        "t": "MESSAGE_CREATE",
        "s": seq,
        "d": {"id": message_id},
    }


async def _drive_one_frame(
    *,
    client: DiscordGatewayClient,
    frame: dict[str, Any],
    initial_session_id: str,
    initial_last_seq: int,
) -> None:
    """Set up the client's in-memory state as-if it had completed
    IDENTIFY (so the dispatch loop sees `session_id` populated), then
    drive the single frame through `_dispatch_loop`.

    Cancels the dispatch task after the frame is processed; uses the
    client's `_shutdown` event to signal the WS to release.
    """
    client._state.session_id = initial_session_id
    client._state.last_seq = initial_last_seq
    client._state.application_id = "test-app"
    client._state.resume_gateway_url = "wss://test/"
    client._ws = _SingleFrameWS(frame, client._shutdown)
    dispatch_task = asyncio.create_task(client._dispatch_loop())
    try:
        # Give the loop time to process the single frame: read frame,
        # call dispatch_handler, call flush, schedule save task. We
        # wait until either the save observable lands in Postgres OR
        # we give up; the caller polls Postgres after this returns.
        # 200ms is enough headroom for a local Postgres write + the
        # fake flush (which is essentially instant).
        await asyncio.sleep(0.2)
    finally:
        client.request_shutdown()
        # Wait for the dispatch task to actually exit so any save
        # tasks have completed (or at least had their chance).
        try:
            await asyncio.wait_for(dispatch_task, timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
            pass
        if not dispatch_task.done():
            dispatch_task.cancel()
            try:
                await dispatch_task
            except (asyncio.CancelledError, Exception):
                pass


async def _poll_for_last_seq(
    pool: asyncpg.Pool,
    application_id: str,
    *,
    expected: int,
    timeout_s: float = 2.0,
) -> int | None:
    """Poll Postgres for `last_seq == expected`. Returns the observed
    last_seq when the condition is met, or the last-observed value at
    timeout (so the test can assert on "still unchanged" too).
    """
    deadline = time.monotonic() + timeout_s
    last_observed: int | None = None
    while time.monotonic() < deadline:
        row = await load_session_state(
            pool, application_id=application_id, shard_id=0,
        )
        last_observed = row.last_seq if row is not None else None
        if last_observed == expected:
            return last_observed
        await asyncio.sleep(0.02)
    return last_observed


async def _read_last_seq(
    pool: asyncpg.Pool, application_id: str,
) -> int | None:
    row = await load_session_state(
        pool, application_id=application_id, shard_id=0,
    )
    return row.last_seq if row is not None else None


async def _seed_initial_state(
    pool: asyncpg.Pool, application_id: str, *, last_seq: int,
) -> None:
    """Pre-populate gateway_session_state with a known `last_seq` so
    the failure-path tests can assert 'unchanged from before.'"""
    await save_session_state(
        pool,
        application_id=application_id,
        shard_id=0,
        session_id="seed-session",
        resume_gateway_url="wss://seed/",
        last_seq=last_seq,
        heartbeat_interval_ms=41250,
    )


def _make_save_hook(pool: asyncpg.Pool, application_id: str):
    """Build an on_dispatched hook that calls the real save_session_state
    against `pool`."""
    async def _save(state: GatewaySessionState) -> None:
        await save_session_state(
            pool,
            application_id=application_id,
            shard_id=0,
            session_id=state.session_id,
            resume_gateway_url=state.resume_gateway_url,
            last_seq=state.last_seq,
            heartbeat_interval_ms=state.heartbeat_interval_ms or None,
        )
    return _save


async def _noop_dispatch(_frame: dict[str, Any]) -> None:
    """Stand-in for `handle_dispatch`. The flush guards the
    shadow-write boundary; for this unit test the dispatch handler
    itself does nothing."""


# =====================================================================
# Test 1 — Observable ordering: flush runs BEFORE save.
# =====================================================================

async def test_flush_called_between_shadow_write_and_save_state(
    fresh_db: asyncpg.Pool,
) -> None:
    """Drives one frame with `s=42` through the dispatch loop with
    `last_seq=10` pre-seeded in Postgres. The fake producer's flush
    callback snapshots `last_seq` from Postgres AS the flush runs.

    Observable-state assertions (Postgres-driven):

      A. Snapshot taken DURING flush: `last_seq == 10` (the pre-seed),
         NOT 42. This proves the save has not advanced Postgres yet
         when flush is called.

      B. AFTER the dispatch settles: Postgres `last_seq == 42`. This
         proves the save did fire after the flush returned.

    Together (A) + (B) prove the ordering `flush → save`. A mock
    call-order assertion alone would catch code regression but miss
    behavioral regression (e.g., save bypassing flush). The Postgres
    snapshot inside the flush is what catches both.
    """
    app_id = f"a6-order-{uuid4().hex[:8]}"

    # Pre-seed last_seq=10.
    await _seed_initial_state(fresh_db, app_id, last_seq=10)
    assert await _read_last_seq(fresh_db, app_id) == 10

    snapshot_during_flush: list[int | None] = []

    async def _snapshot_pg_during_flush() -> None:
        snapshot_during_flush.append(
            await _read_last_seq(fresh_db, app_id)
        )

    fake_producer = _FakeKafkaProducer(
        return_remaining=0,  # successful flush
        during_flush_hook=_snapshot_pg_during_flush,
    )

    client = DiscordGatewayClient(
        bot_token="test",
        dispatch_handler=_noop_dispatch,
        on_dispatched=_make_save_hook(fresh_db, app_id),
        kafka_producer=fake_producer,
    )

    await _drive_one_frame(
        client=client,
        frame=_make_dispatch_frame(seq=42, message_id="msg-42"),
        initial_session_id="seed-session",
        initial_last_seq=41,
    )

    # ===== Assertion A — observable state during flush =====
    assert fake_producer.flush_calls, (
        "flush was never invoked — _pre_save_flush did not gate the save"
    )
    assert len(snapshot_during_flush) == 1, (
        f"Expected 1 flush call, observed {len(snapshot_during_flush)}"
    )
    assert snapshot_during_flush[0] == 10, (
        f"During flush, Postgres last_seq was {snapshot_during_flush[0]}; "
        f"expected 10 (the pre-seed). If this is 42, the save fired "
        f"BEFORE the flush — flush is not gating the save."
    )

    # ===== Assertion B — observable state after save =====
    final = await _poll_for_last_seq(
        fresh_db, app_id, expected=42, timeout_s=2.0,
    )
    assert final == 42, (
        f"After dispatch, Postgres last_seq is {final}; expected 42. "
        f"The save did not fire after the (successful) flush."
    )


# =====================================================================
# Test 2 — Wrapper bounds the flush wait + raises on remaining > 0.
# =====================================================================

async def test_flush_timeout_does_not_block_indefinitely(
    fresh_db: asyncpg.Pool,
) -> None:
    """Direct unit test of `_pre_save_flush`. When the producer's
    flush returns with `remaining > 0`, our wrapper raises
    `TimeoutError`. The wall-time is bounded by the fake's configured
    delay (well under any pytest timeout).

    NOTE: this is a Python-internal unit test. Observable state is
    not the focus — the wrapper's contract is the focus. The
    behavioral consequence (save skipped, Postgres unchanged) is
    covered separately by test_flush_failure_does_not_save_state.
    """
    fake_producer = _FakeKafkaProducer(
        return_remaining=5,
        flush_delay_s=0.1,  # simulate a slow-but-not-hung broker
    )
    client = DiscordGatewayClient(
        bot_token="test",
        dispatch_handler=_noop_dispatch,
        kafka_producer=fake_producer,
    )

    t0 = time.monotonic()
    with pytest.raises(TimeoutError) as exc_info:
        await client._pre_save_flush(timeout_seconds=2.0)
    elapsed = time.monotonic() - t0

    # Wall-time bound: the fake delay is 0.1s; the wrapper should
    # return as soon as flush returns. Leave a generous ceiling for
    # CI jitter.
    assert elapsed < 1.0, (
        f"_pre_save_flush wall-time was {elapsed:.3f}s; expected < 1.0s. "
        f"The wrapper may be blocking past the underlying flush return."
    )
    assert "5" in str(exc_info.value), (
        f"TimeoutError message should mention the remaining count; got "
        f"{exc_info.value!r}"
    )
    # The wrapper passed our timeout_seconds=2.0 through to flush.
    assert fake_producer.flush_calls == [2.0], (
        f"Expected flush to be called once with timeout_seconds=2.0; "
        f"got {fake_producer.flush_calls}"
    )


# =====================================================================
# Test 3 — On flush failure, Postgres last_seq stays UNCHANGED.
# =====================================================================

async def test_flush_failure_does_not_save_state(
    fresh_db: asyncpg.Pool,
) -> None:
    """LOAD-BEARING for A6: when the per-frame flush fails, the save
    MUST NOT fire. Otherwise we persist `last_seq=N` while the Kafka
    side hasn't acked frame N — exactly the silent-N1-breach A6 was
    opened to close.

    Observable-state assertion (per Phase 1 reinforcement):

      Pre-seed Postgres with `last_seq=10`. Drive a frame with `s=42`.
      Force flush to raise. AFTER the dispatch settles, Postgres
      `last_seq` MUST still be 10 — not 42, not None. This catches
      both "code regressed" (save called when it shouldn't be) AND
      "behavior regressed" (a future change might call save with a
      partial state — still observable as wrong Postgres state).

    Secondary internal-call assertion: the `on_dispatched` save hook
    was never invoked.
    """
    app_id = f"a6-fail-{uuid4().hex[:8]}"

    # Pre-seed last_seq=10.
    await _seed_initial_state(fresh_db, app_id, last_seq=10)
    assert await _read_last_seq(fresh_db, app_id) == 10

    save_hook_call_count = 0

    async def _counting_save_hook(state: GatewaySessionState) -> None:
        nonlocal save_hook_call_count
        save_hook_call_count += 1
        # Defensively delegate to the real save so a regression that
        # calls the hook would update Postgres — that's exactly the
        # observable-state regression we want to catch.
        await save_session_state(
            fresh_db,
            application_id=app_id,
            shard_id=0,
            session_id=state.session_id,
            resume_gateway_url=state.resume_gateway_url,
            last_seq=state.last_seq,
            heartbeat_interval_ms=state.heartbeat_interval_ms or None,
        )

    fake_producer = _FakeKafkaProducer(
        raise_on_flush=ConnectionError("broker unreachable for A6 test"),
    )

    client = DiscordGatewayClient(
        bot_token="test",
        dispatch_handler=_noop_dispatch,
        on_dispatched=_counting_save_hook,
        kafka_producer=fake_producer,
    )

    await _drive_one_frame(
        client=client,
        frame=_make_dispatch_frame(seq=42, message_id="msg-42"),
        initial_session_id="seed-session",
        initial_last_seq=41,
    )

    # ===== PRIMARY assertion: observable Postgres state unchanged ====
    last_seq_after = await _read_last_seq(fresh_db, app_id)
    assert last_seq_after == 10, (
        f"After a failed flush, Postgres last_seq is {last_seq_after}; "
        f"expected 10 (the pre-seed, unchanged). A6 contract violated: "
        f"the save advanced state even though the broker-ack barrier "
        f"failed. The next worker will RESUME past a frame that may "
        f"not be durable on Kafka — silent N1 breach."
    )

    # ===== SECONDARY assertion: save hook not called ====
    assert save_hook_call_count == 0, (
        f"on_dispatched hook was called {save_hook_call_count} times "
        f"despite flush failure. The hook MUST be gated on flush success."
    )

    # The flush was attempted.
    assert len(fake_producer.flush_calls) == 1, (
        f"Expected flush to be attempted exactly once; got "
        f"{len(fake_producer.flush_calls)} attempts"
    )


# =====================================================================
# Test 4 — Broad-scope failure metric: any flush exception, not just
#           TimeoutError, increments the failure metric AND skips save.
# =====================================================================

async def test_flush_broker_disconnect_increments_failure_metric_and_skips_save(
    fresh_db: asyncpg.Pool,
) -> None:
    """The metric `discord_gateway_pre_save_flush_failures_total` MUST
    increment for any flush failure — TimeoutError, ConnectionError,
    or any other Exception subclass — because operators care about
    "frame durability uncertain" as a class, not just "flush timed out
    specifically." Narrow-scoping the metric to TimeoutError would
    mask broker disconnects, serialization errors, and other recovery
    signals.

    This test injects a broker-side disconnect (modeled as a
    `BrokerNotAvailableError`-style exception, NOT a TimeoutError)
    and asserts:

      A. Counter `discord_gateway_pre_save_flush_failures_total` == 1.
      B. Postgres `last_seq` UNCHANGED from pre-seed (observable
         behavior: save was skipped).

    The call-site catch in client.py is `except Exception` (broad).
    If a future refactor narrows it to `except TimeoutError`, this
    test fires.

    Companion to `test_flush_failure_does_not_save_state` — that
    test verifies the save-skip behavior with the SAME injected
    exception type (ConnectionError); this test specifically
    verifies the failure metric for the broad-scope class.
    """
    app_id = f"a6-broad-metric-{uuid4().hex[:8]}"
    await _seed_initial_state(fresh_db, app_id, last_seq=7)
    assert await _read_last_seq(fresh_db, app_id) == 7

    # The autouse `_reset_gateway_metrics` fixture in conftest.py
    # already clears the counter to 0; defensive re-read:
    assert gateway_metrics.get(
        "discord_gateway_pre_save_flush_failures_total"
    ) == 0

    # A non-TimeoutError exception modeling a broker disconnect. The
    # exact class doesn't matter — the broad-scope catch should fire
    # for any Exception subclass.
    class BrokerDisconnectError(ConnectionError):
        """Stand-in for confluent_kafka.KafkaException with
        BROKER_NOT_AVAILABLE state."""

    save_calls = 0

    async def _save_counter(state: GatewaySessionState) -> None:
        nonlocal save_calls
        save_calls += 1
        # If the save IS called by a regression, also write Postgres
        # so the observable-state assertion catches it too.
        await save_session_state(
            fresh_db,
            application_id=app_id,
            shard_id=0,
            session_id=state.session_id,
            resume_gateway_url=state.resume_gateway_url,
            last_seq=state.last_seq,
            heartbeat_interval_ms=state.heartbeat_interval_ms or None,
        )

    fake_producer = _FakeKafkaProducer(
        raise_on_flush=BrokerDisconnectError(
            "broker disconnected mid-flush (A6 broad-scope test)"
        ),
    )
    client = DiscordGatewayClient(
        bot_token="test",
        dispatch_handler=_noop_dispatch,
        on_dispatched=_save_counter,
        kafka_producer=fake_producer,
    )

    await _drive_one_frame(
        client=client,
        frame=_make_dispatch_frame(seq=99, message_id="msg-99"),
        initial_session_id="seed-session",
        initial_last_seq=98,
    )

    # ===== Assertion A — metric incremented for non-TimeoutError ====
    failure_count = gateway_metrics.get(
        "discord_gateway_pre_save_flush_failures_total"
    )
    assert failure_count == 1, (
        f"Failure metric was {failure_count}; expected 1. The broad-"
        f"scope exception handler at client.py's _pre_save_flush call "
        f"site is not firing for non-TimeoutError exceptions. "
        f"Operators will miss broker-disconnect signals because the "
        f"metric narrowed silently."
    )

    # ===== Assertion B — observable state unchanged ====
    last_seq_after = await _read_last_seq(fresh_db, app_id)
    assert last_seq_after == 7, (
        f"After a broker-disconnect flush failure, Postgres last_seq "
        f"is {last_seq_after}; expected 7 (pre-seed, unchanged). The "
        f"save fired despite a non-TimeoutError flush failure — "
        f"broad-scope save-skip behavior is broken."
    )
    assert save_calls == 0, (
        f"on_dispatched was called {save_calls} time(s) after a "
        f"non-TimeoutError flush failure; expected 0."
    )
    assert len(fake_producer.flush_calls) == 1
