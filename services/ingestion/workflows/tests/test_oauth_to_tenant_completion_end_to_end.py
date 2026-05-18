"""M6.1 Phase 3 — end-to-end integration test (poller → orchestrator).

The MILESTONE-SHAPING artifact for M6.1. If this test fails, M6.1
does NOT ship.

What it proves end-to-end:
  1. The OAuth poller, run as a REAL subprocess
     (`python -m services.ingestion.workflows.oauth_poller`), claims
     a freshly-seeded `onboarding_triggers` row, creates an
     `onboarding_runs` row, and emits an `onboarding_run_created`
     signal — all in one atomic transaction.
  2. The TenantOnboarding orchestrator, run as a REAL subprocess
     (`python -m services.ingestion.workflows.tenant_onboarding`),
     claims the `onboarding_run_created` signal from its inbox,
     determines the applicable sources from `provider_installations`
     + `gmail_installations` at tick-time (per A13), inserts per-
     source `source_onboarding_runs` rows, and emits
     `source_onboarding_requested` signals to M6.2's deferred
     inbox — all in one atomic transaction per signal.
  3. With the M6.2 SourceOnboarding service intentionally absent,
     we inject fake `source_onboarding_completed` signals into the
     orchestrator's inbox to simulate what M6.2 will eventually
     emit. The orchestrator drains them, marks each source
     completed, and on the final source emits
     `tenant_onboarding_completed` to Bridge's inbox.

Synchronization strategy: Postgres-state-as-checkpoint (per M6.0
Phase 2's precedent + the M3.3 `test_embedding_backlog_sigterm_resume`
shape). Each phase polls a specific row/column rather than relying
on timing. The test is deterministic; no `asyncio.sleep(N)` and
hope.

What this test does NOT cover (out of M6.1 scope):
  - M6.2's SourceOnboarding service consuming the
    `source_onboarding_requested` signals it sees. The injection of
    `source_onboarding_completed` is the stand-in until M6.2 ships.
  - Bridge consuming `tenant_onboarding_completed`. We assert the
    signal exists in workflow_signals; Bridge wiring is out of M6.1
    scope.
  - The OAuth-callback HTTP shape that originally writes the
    trigger row. The test seeds `onboarding_triggers` directly.

Same shape as the M3.3 end-to-end embedding test, the M5.1 cutover
end-to-end test, and the M6.0 feels-monitor subprocess test.
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
from services.ingestion.workflows.tenant_onboarding import (
    BRIDGE_INBOX_ID,
    BRIDGE_INBOX_KIND,
    SIGNAL_KIND_RUN_CREATED,
    SIGNAL_KIND_SOURCE_COMPLETED,
    SIGNAL_KIND_SOURCE_REQUESTED,
    SIGNAL_KIND_TENANT_COMPLETED,
    SOURCE_ONBOARDING_INBOX_ID,
    SOURCE_ONBOARDING_INBOX_KIND,
    WORKFLOW_ID_INBOX,
    WORKFLOW_KIND,
)


pytestmark = [pytest.mark.timeout(180)]


# ---------------------------------------------------------------------
# Seed helpers — mirror the Phase 1 + Phase 2 test fixtures.
# ---------------------------------------------------------------------
async def _seed_tenant_with_two_installs(
    pool: asyncpg.Pool,
) -> tuple[UUID, list[str]]:
    """Seed a tenant with slack + gmail active installs.

    Returns (tenant_id, [active_source_names]). Two sources is the
    minimum interesting case — enough to verify the
    "complete-on-final-source" branch fires only after BOTH sources
    report completion.
    """
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"e2e-{tid.hex[:8]}",
    )
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, 'slack', $3, TRUE)
        """,
        uuid7(), tid, f"inst-slack-{tid.hex[:8]}",
    )
    await pool.execute(
        """
        INSERT INTO gmail_installations
            (id, tenant_id, workspace_domain, service_account_email,
             scope, disabled_at)
        VALUES ($1, $2, $3, $4, 'gmail.readonly', NULL)
        """,
        uuid7(), tid,
        f"workspace-{tid.hex[:8]}.example.com",
        f"svc-{tid.hex[:8]}@example.iam.gserviceaccount.com",
    )
    return tid, ["slack", "gmail"]


async def _seed_trigger(
    pool: asyncpg.Pool, *, tenant_id: UUID, source: str = "slack",
) -> UUID:
    trigger_id = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_triggers
            (id, tenant_id, source, trigger_kind, payload)
        VALUES ($1, $2, $3, 'install', '{}'::jsonb)
        """,
        trigger_id, tenant_id, source,
    )
    return trigger_id


def _poller_env(*, instance: str) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["OAUTH_POLLER_TICK_SEC"] = "0.1"
    env["OAUTH_POLLER_BATCH"] = "5"
    env["OAUTH_POLLER_INSTANCE"] = instance
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"
    return env


def _orchestrator_env(*, instance: str) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["ORCHESTRATOR_TICK_SEC"] = "0.1"
    env["ORCHESTRATOR_BATCH"] = "20"
    env["ORCHESTRATOR_INSTANCE"] = instance
    env["WORKFLOWS_LOG_LEVEL"] = "WARNING"
    return env


async def _poll_until(
    pool: asyncpg.Pool, *, sql: str, args: tuple, predicate,
    timeout: float = 30.0, interval: float = 0.2,
) -> int | None:
    """Poll Postgres until `predicate(value)` is True; return the
    final observed value. Returns None on timeout."""
    deadline = time.monotonic() + timeout
    last: int | None = None
    while time.monotonic() < deadline:
        last = int(await pool.fetchval(sql, *args))
        if predicate(last):
            return last
        await asyncio.sleep(interval)
    return last


# =====================================================================
# THE LOAD-BEARING TEST.
# =====================================================================

async def test_oauth_trigger_to_tenant_completion_end_to_end(
    fresh_db: asyncpg.Pool,
) -> None:
    """End-to-end M6.1 chain — the milestone-shaping artifact.

    Pre-condition: tenant has slack + gmail active installs;
    `onboarding_triggers` has one fresh row.

    Steps (each gated by Postgres-state-as-checkpoint, no timing
    delays):

      [1] Start poller subprocess. Poll `onboarding_triggers` until
          `consumed_at IS NOT NULL` (proof of poller-side
          consumption).

      [2] Assert exactly one `onboarding_runs` row exists for the
          tenant with status `pending` AND exactly one
          `onboarding_run_created` signal exists in the orchestrator's
          inbox. This is the poller's atomic-write invariant; the
          subprocess Phase 1 test already verifies it but we re-
          assert here because it's the chain's first hand-off.

      [3] Start orchestrator subprocess. Poll
          `source_onboarding_runs` until count == 2 (proof of
          orchestrator-side fan-out for slack + gmail).

      [4] Assert the parent `onboarding_runs.status == 'running'`
          AND the original `onboarding_run_created` signal is
          consumed AND 2 `source_onboarding_requested` signals
          have been emitted to the M6.2 inbox.

      [5] Inject fake `source_onboarding_completed` signals — one
          per source — into the orchestrator's inbox. This is the
          M6.2 stand-in (real M6.2 hasn't shipped).

      [6] Poll `onboarding_runs.status` until == 'complete' (proof
          of orchestrator-side completion-roll-up).

      [7] Assert exactly one `tenant_onboarding_completed` signal
          exists in Bridge's inbox with the right
          `idempotency_key`. This is the chain's final output.

      [8] SIGTERM both subprocesses; require rc==0 within 15s
          each.
    """
    tid, sources = await _seed_tenant_with_two_installs(fresh_db)
    trigger_id = await _seed_trigger(fresh_db, tenant_id=tid, source="slack")

    poller_instance = f"e2e-poll-{tid.hex[:6]}"
    orch_instance = f"e2e-orch-{tid.hex[:6]}"

    poller_proc: subprocess.Popen | None = None
    orch_proc: subprocess.Popen | None = None
    try:
        # ----- [1] Start poller, wait for trigger consumption. -----
        poller_proc = subprocess.Popen(
            [sys.executable, "-m",
             "services.ingestion.workflows.oauth_poller"],
            env=_poller_env(instance=poller_instance),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        consumed = await _poll_until(
            fresh_db,
            sql=(
                "SELECT count(*) FROM onboarding_triggers "
                "WHERE id = $1 AND consumed_at IS NOT NULL"
            ),
            args=(trigger_id,),
            predicate=lambda n: n == 1,
            timeout=30.0,
        )
        if consumed != 1:
            stderr = poller_proc.stderr.read().decode() if poller_proc.stderr else ""
            raise AssertionError(
                f"Poller did not consume trigger within 30s. "
                f"stderr: {stderr[:1000]}"
            )

        # ----- [2] Atomic write invariant at the poller boundary. -----
        run_row = await fresh_db.fetchrow(
            "SELECT id, status FROM onboarding_runs "
            "WHERE tenant_id = $1",
            tid,
        )
        assert run_row is not None, (
            "Trigger consumed but no onboarding_runs row exists — "
            "poller atomic transaction is broken."
        )
        run_id = run_row["id"]
        assert run_row["status"] == "pending", (
            f"Newly-created run should be 'pending'; got "
            f"{run_row['status']!r}."
        )

        n_run_created_signals = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            WORKFLOW_KIND, WORKFLOW_ID_INBOX,
            SIGNAL_KIND_RUN_CREATED, str(run_id),
        ))
        assert n_run_created_signals == 1, (
            f"Expected exactly one onboarding_run_created signal "
            f"in ({WORKFLOW_KIND}, {WORKFLOW_ID_INBOX}) for run "
            f"{run_id}; got {n_run_created_signals}."
        )

        # ----- [3] Start orchestrator, wait for fan-out. -----
        orch_proc = subprocess.Popen(
            [sys.executable, "-m",
             "services.ingestion.workflows.tenant_onboarding"],
            env=_orchestrator_env(instance=orch_instance),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        n_sources = await _poll_until(
            fresh_db,
            sql=(
                "SELECT count(*) FROM source_onboarding_runs "
                "WHERE onboarding_run_id = $1"
            ),
            args=(run_id,),
            predicate=lambda n: n == len(sources),
            timeout=30.0,
        )
        if n_sources != len(sources):
            stderr = orch_proc.stderr.read().decode() if orch_proc.stderr else ""
            raise AssertionError(
                f"Orchestrator did not fan out to {len(sources)} "
                f"source_onboarding_runs rows within 30s; observed "
                f"{n_sources}. stderr: {stderr[:1000]}"
            )

        # ----- [4] Orchestrator atomic-write invariant. -----
        # Parent run advanced to 'running'.
        parent_status = await fresh_db.fetchval(
            "SELECT status FROM onboarding_runs WHERE id = $1", run_id,
        )
        assert parent_status == "running", (
            f"Parent run should be 'running' after fan-out; got "
            f"{parent_status!r}. Orchestrator missed the "
            f"_MARK_RUN_RUNNING_SQL step."
        )

        # Original onboarding_run_created signal consumed.
        consumed_at_run_created = await fresh_db.fetchval(
            "SELECT consumed_at FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            WORKFLOW_KIND, WORKFLOW_ID_INBOX,
            SIGNAL_KIND_RUN_CREATED, str(run_id),
        )
        assert consumed_at_run_created is not None, (
            "Orchestrator inserted source rows but did NOT consume "
            "the onboarding_run_created signal — atomic transaction "
            "broken (the A12 + A13 contract)."
        )

        # source_onboarding_requested signal emitted per source.
        n_requested = int(await fresh_db.fetchval(
            "SELECT count(*) FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3",
            SOURCE_ONBOARDING_INBOX_KIND, SOURCE_ONBOARDING_INBOX_ID,
            SIGNAL_KIND_SOURCE_REQUESTED,
        ))
        assert n_requested == len(sources), (
            f"Expected {len(sources)} source_onboarding_requested "
            f"signals in ({SOURCE_ONBOARDING_INBOX_KIND}, "
            f"{SOURCE_ONBOARDING_INBOX_ID}); got {n_requested}."
        )

        # ----- [5] Inject fake source_onboarding_completed. -----
        # This is the M6.2 stand-in. Real M6.2 SourceOnboarding
        # would consume the source_onboarding_requested signals,
        # do per-source backfill, and emit these completion signals
        # back to the orchestrator's inbox. Until M6.2 ships, the
        # test simulates that step directly.
        for source in sources:
            await emit_signal(
                fresh_db,
                workflow_kind=WORKFLOW_KIND,
                workflow_id=WORKFLOW_ID_INBOX,
                signal_kind=SIGNAL_KIND_SOURCE_COMPLETED,
                idempotency_key=f"{run_id}:{source}",
                signal_data={
                    "onboarding_run_id": str(run_id),
                    "source": source,
                },
            )

        # ----- [6] Wait for parent run to complete. -----
        is_complete = await _poll_until(
            fresh_db,
            sql=(
                "SELECT count(*) FROM onboarding_runs "
                "WHERE id = $1 AND status = 'complete'"
            ),
            args=(run_id,),
            predicate=lambda n: n == 1,
            timeout=30.0,
        )
        if is_complete != 1:
            stderr = orch_proc.stderr.read().decode() if orch_proc.stderr else ""
            final_status = await fresh_db.fetchval(
                "SELECT status FROM onboarding_runs WHERE id = $1",
                run_id,
            )
            raise AssertionError(
                f"Parent onboarding_run did not reach 'complete' "
                f"within 30s; final status={final_status!r}. "
                f"orchestrator stderr: {stderr[:1000]}"
            )

        # All source_onboarding_runs rows in terminal-completed state.
        n_completed_sources = int(await fresh_db.fetchval(
            "SELECT count(*) FROM source_onboarding_runs "
            "WHERE onboarding_run_id = $1 AND status = 'completed'",
            run_id,
        ))
        assert n_completed_sources == len(sources), (
            f"Expected {len(sources)} source rows in 'completed' "
            f"state; got {n_completed_sources}."
        )

        # ----- [7] tenant_onboarding_completed emitted to Bridge. -----
        bridge_signal = await fresh_db.fetchrow(
            "SELECT id, signal_data, idempotency_key "
            "FROM workflow_signals "
            "WHERE workflow_kind = $1 AND workflow_id = $2 "
            "AND signal_kind = $3 AND idempotency_key = $4",
            BRIDGE_INBOX_KIND, BRIDGE_INBOX_ID,
            SIGNAL_KIND_TENANT_COMPLETED, str(run_id),
        )
        assert bridge_signal is not None, (
            f"Expected a tenant_onboarding_completed signal in the "
            f"Bridge inbox ({BRIDGE_INBOX_KIND}, {BRIDGE_INBOX_ID}) "
            f"with idempotency_key={run_id!s}; none found. "
            f"The orchestrator's completion-phase emit is broken."
        )

        # ----- [8] Clean SIGTERM both subprocesses. -----
        for proc, name in (
            (poller_proc, "poller"), (orch_proc, "orchestrator"),
        ):
            proc.send_signal(signal.SIGTERM)
            try:
                rc = proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
                stderr = proc.stderr.read().decode() if proc.stderr else ""
                raise AssertionError(
                    f"{name} subprocess did NOT exit within 15s of "
                    f"SIGTERM. stderr: {stderr[:1000]}"
                )
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            assert rc == 0, (
                f"{name} subprocess exited with rc={rc} "
                f"(expected 0). stderr: {stderr[:1000]}"
            )
    finally:
        for proc in (poller_proc, orch_proc):
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
