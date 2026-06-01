"""M6.2b Phase 1 — real-subprocess SIGTERM test for Reconciler.

Spawns `python -m services.ingestion.workflows.reconciler` as a real
subprocess. Same shape as M6.2a's
`test_source_onboarding_sigterm_subprocess`: DB markers
(workflow_states row) as deterministic synchronization checkpoint;
no timing-based delays.

What this proves beyond the in-process tests:
  - __main__'s SIGTERM/SIGINT handlers wire to the stop_event.
  - The pool closes cleanly on exit.
  - State persistence survives the process boundary.

Uses the default-clean stub (no monkeypatching needed in subprocess):
the Reconciler stub for slack returns clean, so injected
source_shards_completed gets a clean reconciliation pass.
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
from services.ingestion.workflows.reconciler import (
    SIGNAL_KIND_SHARDS_COMPLETED,
    WORKFLOW_ID_INBOX,
    WORKFLOW_KIND,
)
from services.ingestion.workflows.signals import emit_signal


pytestmark = [pytest.mark.timeout(120)]


async def _read_state(
    pool: asyncpg.Pool, instance: str,
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        "SELECT last_advanced_at, state_data FROM workflow_states "
        "WHERE workflow_kind = $1 AND workflow_id = $2",
        WORKFLOW_KIND, instance,
    )


async def test_reconciler_sigterm_subprocess(
    fresh_db: asyncpg.Pool,
) -> None:
    """Spawn the service as a real subprocess. Inject one
    source_shards_completed signal (which the default-clean stub will
    reconcile cleanly). Wait for the workflow_states diagnostic row
    to appear. SIGTERM. Assert rc=0 within 15s.
    """
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"rec-subproc-{tid.hex[:8]}",
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
            (onboarding_run_id, source, tenant_id, status,
             started_at, completed_at, reconciliation_pass_count)
        VALUES ($1, 'slack', $2, 'completed', now(), now(), 0)
        """,
        run_id, tid,
    )
    await fresh_db.execute(
        """
        INSERT INTO onboarding_shards
            (id, onboarding_run_id, tenant_id, source, shard_kind,
             shard_identifier, recency_score, state, created_at,
             completed_at)
        VALUES ($1, $2, $3, 'slack', 'slack_channel_window',
                '{}'::jsonb, 1.0, 'done', now(), now())
        """,
        uuid7(), run_id, tid,
    )
    await emit_signal(
        fresh_db,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_SHARDS_COMPLETED,
        idempotency_key=f"{run_id}:slack:pass_0",
        signal_data={
            "onboarding_run_id": str(run_id),
            "tenant_id": str(tid),
            "source": "slack",
            "reconciliation_pass_count": 0,
        },
    )

    instance = f"rec-sub-{tid.hex[:6]}"
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["RECONCILER_TICK_SEC"] = "0.1"
    env["RECONCILER_BATCH"] = "5"
    env["RECONCILER_INSTANCE"] = instance
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"

    proc = subprocess.Popen(
        [sys.executable, "-m",
         "services.ingestion.workflows.reconciler"],
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
                f"Reconciler subprocess did not write a "
                f"workflow_states row within 30s. stderr: "
                f"{stderr[:1000]}"
            )

        # Signal consumed; default-clean stub reconciled the run.
        signal_row = await fresh_db.fetchrow(
            "SELECT consumed_at FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            WORKFLOW_KIND, WORKFLOW_ID_INBOX,
            SIGNAL_KIND_SHARDS_COMPLETED, f"{run_id}:slack:pass_0",
        )
        assert signal_row["consumed_at"] is not None, (
            "Reconciler wrote state row but did not consume the "
            "signal — atomic transaction broken."
        )
        reconciled = await fresh_db.fetchval(
            "SELECT reconciled_at FROM source_onboarding_runs "
            "WHERE onboarding_run_id = $1 AND source = $2",
            run_id, "slack",
        )
        assert reconciled is not None

        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            raise AssertionError(
                f"Reconciler did NOT exit within 15s of SIGTERM. "
                f"stderr: {stderr[:1000]}"
            )

        stderr = proc.stderr.read().decode() if proc.stderr else ""
        assert rc == 0, (
            f"Reconciler subprocess exited with rc={rc} "
            f"(expected 0). stderr: {stderr[:1000]}"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
