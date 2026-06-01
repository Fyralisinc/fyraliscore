"""M6.2a Phase 1 — real-subprocess SIGTERM test for SourceOnboarding.

Spawns `python -m services.ingestion.workflows.source_onboarding` as
a real subprocess. Same shape as M6.1's
`test_orchestrator_sigterm_subprocess` + M6.0's
`test_feels_monitor_sigterm_subprocess`: DB markers (workflow_states
row) as the deterministic synchronization checkpoint; no
timing-based delays.

What this proves beyond the in-process tests:
  - __main__'s SIGTERM/SIGINT handlers wire to the stop_event.
  - The pool closes cleanly on exit.
  - State persistence survives the process boundary.

The signal we inject for the heartbeat trigger is
`source_onboarding_requested` for source='slack', which the
NotImplementedError stub will mark as failed cleanly without
needing a real planner.
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.ingestion.workflows.signals import emit_signal
from services.ingestion.workflows.source_onboarding import (
    SIGNAL_KIND_REQUESTED,
    WORKFLOW_ID_INBOX,
    WORKFLOW_KIND,
)


pytestmark = [pytest.mark.timeout(120)]


async def _read_state(
    pool: asyncpg.Pool, instance: str,
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT last_advanced_at, state_data FROM workflow_states "
        "WHERE workflow_kind = $1 AND workflow_id = $2",
        WORKFLOW_KIND, instance,
    )


async def test_source_onboarding_sigterm_subprocess(
    fresh_db: asyncpg.Pool,
) -> None:
    """Spawn the service as a real subprocess. Inject one
    source_onboarding_requested signal. Wait for the workflow_states
    row to appear (proof of at least one completed tick + persist).
    SIGTERM. Assert clean exit within 15s.
    """
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"src-subproc-{tid.hex[:8]}",
    )
    await fresh_db.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, 'slack', $3, TRUE)
        """,
        uuid7(), tid, f"inst-{tid.hex[:8]}",
    )
    run_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO onboarding_runs
            (id, tenant_id, trigger_kind, workflow_id, status,
             sources_enabled, started_at)
        VALUES ($1, $2, 'install', $3, 'running',
                ARRAY['slack']::text[], now())
        """,
        run_id, tid, f"wf-{run_id.hex[:8]}",
    )
    await fresh_db.execute(
        """
        INSERT INTO source_onboarding_runs
            (onboarding_run_id, source, tenant_id, status)
        VALUES ($1, $2, $3, 'pending')
        """,
        run_id, "slack", tid,
    )
    # NotImplementedError stub will fail this cleanly — that's fine for
    # the heartbeat-and-clean-exit test.
    await emit_signal(
        fresh_db,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_REQUESTED,
        idempotency_key=f"{run_id}:slack",
        signal_data={
            "onboarding_run_id": str(run_id),
            "tenant_id": str(tid),
            "source": "slack",
        },
    )

    instance = f"src-sub-{tid.hex[:6]}"
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["SOURCE_ONBOARDING_TICK_SEC"] = "0.1"
    env["SOURCE_ONBOARDING_BATCH"] = "5"
    env["SOURCE_ONBOARDING_INSTANCE"] = instance
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"

    proc = subprocess.Popen(
        [sys.executable, "-m",
         "services.ingestion.workflows.source_onboarding"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        deadline = time.monotonic() + 30.0
        observed_state = None
        while time.monotonic() < deadline:
            observed_state = await _read_state(fresh_db, instance)
            if observed_state is not None:
                break
            await asyncio.sleep(0.2)

        if observed_state is None:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"SourceOnboarding subprocess did not write a "
                f"workflow_states row within 30s. stderr: "
                f"{stderr[:1000]}"
            )

        # Signal consumed (stub path: mark failed + emit completed +
        # consume the request signal — all in one transaction).
        signal_row = await fresh_db.fetchrow(
            "SELECT consumed_at FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            WORKFLOW_KIND, WORKFLOW_ID_INBOX,
            SIGNAL_KIND_REQUESTED, f"{run_id}:slack",
        )
        assert signal_row["consumed_at"] is not None, (
            "Service wrote state row but did not consume the signal "
            "— atomic transaction broken."
        )

        # SIGTERM and require clean exit.
        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"SourceOnboarding did NOT exit within 15s of SIGTERM. "
                f"stderr: {stderr[:1000]}"
            )

        stderr = proc.stderr.read().decode() if proc.stderr else ""
        assert rc == 0, (
            f"SourceOnboarding subprocess exited with rc={rc} "
            f"(expected 0). stderr: {stderr[:1000]}"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
