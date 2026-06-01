"""M6.0 Phase 2 — real-subprocess SIGTERM test.

LOAD-BEARING test for the CLI entrypoint's SIGTERM handler. Same
shape as `tests/test_embedding_backlog.py::test_backlog_service_resumes_from_cursor`
(M3.3) and `services/integrations/discord/gateway/tests/test_gateway_lifecycle.py`
(M4.3 / A6 precedent).

============================================================
WHY A SEPARATE FILE
============================================================
This test forks a real Python subprocess running `python -m
services.ingestion.workflows`. It takes ~5-15 seconds — too slow for
the fast-iteration test suite. Splitting it out lets contributors
run `pytest test_feels_onboarded_monitor.py` for the inner-loop
validation and reserve this file for the gate test.

============================================================
WHY DB MARKERS, NOT TIMING-DELAYS
============================================================
Per the [M3.3 + A6 marker pattern]: observability of progress is via
deterministic checkpoints (DB row mutations), NOT `asyncio.sleep(2)`-
and-hope-the-tick-fired. `FeelsOnboardedMonitor` writes a
`workflow_states` row on every tick; the test polls that row's
`last_advanced_at` as the "at least one tick happened" signal.
SIGTERM is then guaranteed to land mid-loop with the state row in
the post-first-tick shape, regardless of how slow CI is.
"""
from __future__ import annotations

import datetime as dt
import os
import signal
import subprocess
import sys
import time
from uuid import UUID, uuid4

import asyncpg
import pytest


pytestmark = [pytest.mark.timeout(120)]


async def _seed_tenant_and_run(pool: asyncpg.Pool) -> tuple[UUID, UUID]:
    tid = uuid4()
    rid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"sigterm-test-{tid.hex[:8]}",
    )
    await pool.execute(
        """
        INSERT INTO onboarding_runs
            (id, tenant_id, trigger_kind, workflow_id, status,
             sources_enabled, started_at)
        VALUES ($1, $2, 'install', $3, 'running',
                ARRAY['slack']::text[], now())
        """,
        rid, tid, f"wf-{rid.hex[:8]}",
    )
    return tid, rid


async def _read_state_row(pool: asyncpg.Pool) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT last_advanced_at, state_data "
        "FROM workflow_states "
        "WHERE workflow_kind = $1 AND workflow_id = $2",
        "feels_onboarded_monitor", "default",
    )


async def test_feels_monitor_sigterm_subprocess(
    fresh_db: asyncpg.Pool,
) -> None:
    """Spawn the monitor as a real subprocess, wait for the first
    `workflow_states` row write (proof of one completed tick),
    SIGTERM, and require a clean exit (rc=0) within 15 seconds.

    What this proves beyond the in-process Phase 1 stop_event test:
      - `__main__.py` actually installs a SIGTERM handler that ties
        to the asyncio Event the loop awaits.
      - The state-persistence path survives across process exit
        (the row is durable in Postgres, not lost in process memory).
      - `LongRunningService.run` returns cleanly when stop_event is
        set during the inter-tick sleep AS WELL AS during a tick.

    If the SIGTERM plumbing is broken (e.g. handler not wired, loop
    awaiting on something that doesn't see the event), proc.wait()
    times out at 15s and we fall through to `proc.kill()` + raise.
    """
    tid, rid = await _seed_tenant_and_run(fresh_db)

    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    # No Kafka needed — there are no observations seeded, so the
    # tick will scan-empty + persist-state + sleep without ever
    # publishing. librdkafka tolerates an unreachable broker at
    # init time; no connection is attempted until produce() runs.
    env["KAFKA_BOOTSTRAP_SERVERS"] = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
    )
    env["WORKFLOW_SERVICE"] = "feels_onboarded_monitor"
    # Fast tick so the subprocess persists state within the
    # polling window.
    env["FEELS_MONITOR_TICK_SEC"] = "0.1"
    env["FEELS_MONITOR_MIN_OBS"] = "1"
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"

    proc = subprocess.Popen(
        [sys.executable, "-m", "services.ingestion.workflows"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # ---- Poll for the state row to appear (proof of one tick) ---
        deadline = time.monotonic() + 30.0
        first_tick_state = None
        while time.monotonic() < deadline:
            first_tick_state = await _read_state_row(fresh_db)
            if first_tick_state is not None:
                break
            import asyncio
            await asyncio.sleep(0.2)

        if first_tick_state is None:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"FeelsOnboardedMonitor subprocess did not write a "
                f"workflow_states row within 30s. The tick loop is "
                f"broken or never reached persist_state. "
                f"stderr: {stderr[:1000]}"
            )

        first_tick_at = first_tick_state["last_advanced_at"]
        assert first_tick_at is not None

        # ---- SIGTERM and require clean exit within 15s ----
        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"FeelsOnboardedMonitor subprocess did NOT exit "
                f"within 15s of SIGTERM. The CLI entry's SIGTERM "
                f"handler is broken or the loop is stuck. "
                f"stderr: {stderr[:1000]}"
            )

        stderr = proc.stderr.read().decode() if proc.stderr else ""
        assert rc == 0, (
            f"FeelsOnboardedMonitor subprocess exited with rc={rc} "
            f"after SIGTERM (expected 0). "
            f"stderr: {stderr[:1000]}"
        )

        # ---- State row is still there and was written by the
        #      subprocess (durable across exit, not held in
        #      in-process state). ----
        final_state = await _read_state_row(fresh_db)
        assert final_state is not None, (
            "workflow_states row vanished after subprocess exit — "
            "the persist_state path is not actually durable."
        )
        assert final_state["last_advanced_at"] is not None
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
