"""M6.1 Phase 1 — real-subprocess SIGTERM + idempotent-restart test.

Spawns `python -m services.ingestion.workflows.oauth_poller` as a
real subprocess. Same shape as M6.0's
`test_feels_monitor_sigterm_subprocess`: DB markers (the
`workflow_states` row written by `_persist_scan_state`) as the
deterministic synchronization checkpoint; NO timing-based delays.

LOAD-BEARING property (M6.1):
  - The CLI entry's SIGTERM handler is wired to stop_event.
  - Cross-restart idempotency: a SIGTERM mid-tick leaves the trigger
    unclaimed (the transaction was active at SIGTERM time, so the
    transaction rolled back); the restarted process re-claims and
    processes it cleanly.

Split from `test_oauth_poller.py` because this test forks a real
process and takes ~5-15 seconds — too slow for fast inner-loop
iteration.
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from lib.shared.ids import uuid7


pytestmark = [pytest.mark.timeout(120)]


async def _seed_tenant_and_trigger(
    pool: asyncpg.Pool, *, source: str = "slack",
) -> tuple[UUID, UUID]:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"poller-subproc-{tid.hex[:8]}",
    )
    trigger_id = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_triggers
            (id, tenant_id, source, trigger_kind, payload)
        VALUES ($1, $2, $3, 'install', '{}'::jsonb)
        """,
        trigger_id, tid, source,
    )
    return tid, trigger_id


async def _read_poller_state(
    pool: asyncpg.Pool, instance: str,
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT last_advanced_at, state_data FROM workflow_states "
        "WHERE workflow_kind = 'oauth_poller' AND workflow_id = $1",
        instance,
    )


# =====================================================================
# 1. SIGTERM clean-exit + state durability.
# =====================================================================

async def test_oauth_poller_sigterm_subprocess(
    fresh_db: asyncpg.Pool,
) -> None:
    """Spawn the poller as a real subprocess. Seed one trigger.
    Wait for the workflow_states row to appear (proof of at least one
    completed tick + state persist). SIGTERM. Assert clean exit
    within 15 seconds.

    What this proves beyond the in-process tests:
      - `__main__` block's SIGTERM/SIGINT handlers wire to the
        stop_event the run-loop awaits.
      - The pool is closed on exit (no leaked connections).
      - State persistence survives across process exit.
    """
    tid, trigger_id = await _seed_tenant_and_trigger(fresh_db)
    instance = f"subproc-{tid.hex[:6]}"

    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["OAUTH_POLLER_TICK_SEC"] = "0.1"  # fast tick for the test
    env["OAUTH_POLLER_BATCH"] = "1"
    env["OAUTH_POLLER_INSTANCE"] = instance
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"

    proc = subprocess.Popen(
        [sys.executable, "-m", "services.ingestion.workflows.oauth_poller"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Poll for the state row to appear OR the trigger to be consumed.
        deadline = time.monotonic() + 30.0
        observed_state = None
        while time.monotonic() < deadline:
            observed_state = await _read_poller_state(fresh_db, instance)
            if observed_state is not None:
                break
            await asyncio.sleep(0.2)

        if observed_state is None:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"Poller subprocess did not write a workflow_states "
                f"row within 30s. The tick loop is broken or never "
                f"reached persist_state. stderr: {stderr[:1000]}"
            )

        # Trigger should also be consumed by now.
        trigger_row = await fresh_db.fetchrow(
            "SELECT consumed_at FROM onboarding_triggers WHERE id = $1",
            trigger_id,
        )
        assert trigger_row["consumed_at"] is not None, (
            "Poller subprocess wrote state row but did NOT consume "
            "the trigger — the atomic transaction is broken."
        )

        # SIGTERM and require clean exit within 15s.
        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"Poller subprocess did NOT exit within 15s of "
                f"SIGTERM. stderr: {stderr[:1000]}"
            )

        stderr = proc.stderr.read().decode() if proc.stderr else ""
        assert rc == 0, (
            f"Poller subprocess exited with rc={rc} (expected 0). "
            f"stderr: {stderr[:1000]}"
        )

        # State row is durable across exit.
        final_state = await _read_poller_state(fresh_db, instance)
        assert final_state is not None
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


# =====================================================================
# 2. LOAD-BEARING — idempotent across restart (kill mid-flight).
# =====================================================================

async def test_oauth_poller_idempotent_across_restart(
    fresh_db: asyncpg.Pool,
) -> None:
    """Seed 5 triggers. Start a poller subprocess. Wait until some
    triggers are consumed AND some remain. Hard-kill the process
    (SIGKILL — simulates a crash mid-transaction; SIGTERM lets the
    current tick finish). Restart. Assert:
      (a) The trigger(s) in-flight at SIGKILL roll back to NULL
          consumed_at (the transaction was active; rollback erases
          consumed_at + onboarding_runs + signal).
      (b) The restart picks up the remaining triggers and finishes
          all 5 cleanly.
      (c) Each trigger produced exactly one onboarding_runs row
          (no duplicates from the killed attempt + the restart).
    """
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"poller-idem-{tid.hex[:8]}",
    )
    n = 5
    trigger_ids: list[UUID] = []
    for _ in range(n):
        tid_id = uuid7()
        await fresh_db.execute(
            """
            INSERT INTO onboarding_triggers
                (id, tenant_id, source, trigger_kind, payload)
            VALUES ($1, $2, 'slack', 'install', '{}'::jsonb)
            """,
            tid_id, tid,
        )
        trigger_ids.append(tid_id)

    instance = f"idem-{tid.hex[:6]}"
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["OAUTH_POLLER_TICK_SEC"] = "0.05"
    env["OAUTH_POLLER_BATCH"] = "1"
    env["OAUTH_POLLER_INSTANCE"] = instance
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"

    # ---- Run 1: poller starts, processes some, gets SIGKILLed. ----
    proc1 = subprocess.Popen(
        [sys.executable, "-m", "services.ingestion.workflows.oauth_poller"],
        env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        # Wait until at least 2 triggers are consumed (some but not all).
        deadline = time.monotonic() + 30.0
        consumed_before_kill = 0
        while time.monotonic() < deadline:
            consumed_before_kill = int(await fresh_db.fetchval(
                "SELECT count(*) FROM onboarding_triggers "
                "WHERE tenant_id = $1 AND consumed_at IS NOT NULL",
                tid,
            ))
            if 2 <= consumed_before_kill < n:
                break
            await asyncio.sleep(0.1)

        if not (2 <= consumed_before_kill < n):
            proc1.kill()
            proc1.wait(timeout=5)
            stderr = proc1.stderr.read().decode() if proc1.stderr else ""
            raise AssertionError(
                f"Could not catch poller mid-flight: consumed "
                f"{consumed_before_kill} of {n} before timeout. "
                f"stderr: {stderr[:500]}"
            )

        # Hard kill — no clean shutdown; transaction in flight rolls back.
        proc1.kill()
        proc1.wait(timeout=5)
    finally:
        if proc1.poll() is None:
            proc1.kill()
            proc1.wait(timeout=5)

    # Some triggers should still be unconsumed (those not yet
    # processed when SIGKILL fired).
    unconsumed_mid = int(await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND consumed_at IS NULL",
        tid,
    ))
    assert unconsumed_mid > 0, (
        "All triggers consumed before SIGKILL — the test timing "
        "missed the in-flight window. Retry; this is a test-fixture "
        "timing concern, not a service correctness concern."
    )

    # ---- Run 2: restart, drain to zero. ----
    proc2 = subprocess.Popen(
        [sys.executable, "-m", "services.ingestion.workflows.oauth_poller"],
        env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        # Wait until all 5 triggers consumed.
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            unconsumed = int(await fresh_db.fetchval(
                "SELECT count(*) FROM onboarding_triggers "
                "WHERE tenant_id = $1 AND consumed_at IS NULL",
                tid,
            ))
            if unconsumed == 0:
                break
            await asyncio.sleep(0.1)

        final_unconsumed = int(await fresh_db.fetchval(
            "SELECT count(*) FROM onboarding_triggers "
            "WHERE tenant_id = $1 AND consumed_at IS NULL",
            tid,
        ))
        proc2.send_signal(signal.SIGTERM)
        try:
            proc2.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc2.kill()
    finally:
        if proc2.poll() is None:
            proc2.kill()
            proc2.wait(timeout=5)

    # ----- LOAD-BEARING (a): all 5 triggers consumed post-restart. -----
    assert final_unconsumed == 0, (
        f"After SIGKILL + restart, {final_unconsumed} triggers "
        f"remain unconsumed. The poller's resume-from-postgres-state "
        f"is broken."
    )

    # ----- LOAD-BEARING (b): exactly 5 onboarding_runs rows. -----
    n_runs = int(await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_runs WHERE tenant_id = $1",
        tid,
    ))
    assert n_runs == n, (
        f"Expected {n} onboarding_runs (one per trigger); got "
        f"{n_runs}. SIGKILL mid-transaction left dangling state — "
        f"either the in-flight trigger committed partially (no "
        f"rollback) OR the restart double-processed a previously "
        f"consumed trigger."
    )

    # ----- LOAD-BEARING (c): exactly 5 onboarding_run_created signals. -----
    n_signals = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = 'tenant_onboarding' "
        "AND signal_kind = 'onboarding_run_created'",
    ))
    assert n_signals == n
