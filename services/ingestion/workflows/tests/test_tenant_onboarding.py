"""M6.1 Phase 2 — TenantOnboarding orchestrator tests.

Covers the two-phase orchestrator (new-runs + completions):
  - LOAD-BEARING: handles onboarding_run_created signals atomically
    (claim + insert source rows + emit per-source signals + mark
    run running, all in one transaction).
  - LOAD-BEARING: rollback on failure preserves the signal as
    claimable on next tick (the A12 + A13 property at the
    orchestrator level).
  - Source applicability per A13: provider_installations +
    gmail_installations at tick-time is the source of truth.
  - Completion semantics: all sources done → parent run complete +
    tenant_onboarding_completed emitted. Any source failed →
    parent run failed (M6.1 default).
  - Concurrent poller + orchestrator: no deadlock; signals flow.

The subprocess SIGTERM test lives in
`test_tenant_onboarding_subprocess.py`.
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from lib.shared.ids import uuid7
from services.ingestion.workflows.oauth_poller import (
    OAuthPoller,
    OAuthPollerConfig,
)
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
    TenantOnboardingConfig,
    TenantOnboardingOrchestrator,
    WORKFLOW_ID_INBOX,
    WORKFLOW_KIND,
)


pytestmark = [pytest.mark.timeout(60)]


# =====================================================================
# Helpers.
# =====================================================================

async def _seed_tenant(pool: asyncpg.Pool, label: str = "orch") -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"{label}-{tid.hex[:8]}",
    )
    return tid


async def _seed_provider_install(
    pool: asyncpg.Pool, *, tenant_id: UUID, provider: str,
    enabled: bool = True,
) -> None:
    """Seed a provider_installations row (slack/github/discord)."""
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, $3, $4, $5)
        """,
        uuid7(), tenant_id, provider,
        f"inst-{tenant_id.hex[:8]}-{provider}", enabled,
    )


async def _seed_gmail_install(
    pool: asyncpg.Pool, *, tenant_id: UUID, disabled: bool = False,
) -> None:
    """Seed a gmail_installations row."""
    await pool.execute(
        """
        INSERT INTO gmail_installations
            (id, tenant_id, workspace_domain, service_account_email,
             scope, disabled_at)
        VALUES ($1, $2, $3, $4, 'gmail.readonly', $5)
        """,
        uuid7(), tenant_id,
        f"workspace-{tenant_id.hex[:8]}.example.com",
        f"svc-{tenant_id.hex[:8]}@example.iam.gserviceaccount.com",
        ("2026-01-01T00:00:00+00:00" if disabled else None),
    )


async def _seed_onboarding_run(
    pool: asyncpg.Pool, *, tenant_id: UUID, source: str = "slack",
    status: str = "pending",
) -> UUID:
    """Seed an onboarding_runs row directly (bypassing the poller)."""
    run_id = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_runs
            (id, tenant_id, trigger_kind, workflow_id, status,
             sources_enabled, started_at)
        VALUES ($1, $2, 'install', $3, $4, $5::text[], now())
        """,
        run_id, tenant_id, f"wf-{run_id.hex[:8]}", status, [source],
    )
    return run_id


async def _emit_run_created_signal(
    pool: asyncpg.Pool, *, run_id: UUID, tenant_id: UUID,
) -> None:
    """Inject an onboarding_run_created signal directly (simulates
    what the poller would emit)."""
    await emit_signal(
        pool,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_RUN_CREATED,
        idempotency_key=str(run_id),
        signal_data={
            "onboarding_run_id": str(run_id),
            "tenant_id": str(tenant_id),
            "trigger_id": str(uuid7()),
            "source": "slack",
            "trigger_kind": "install",
        },
    )


async def _emit_source_completed_signal(
    pool: asyncpg.Pool, *, run_id: UUID, source: str,
    failure_reason: str | None = None,
) -> None:
    """Inject a source_onboarding_completed signal (simulates what
    M6.2's SourceOnboarding would emit)."""
    data: dict[str, Any] = {
        "onboarding_run_id": str(run_id),
        "source": source,
    }
    if failure_reason is not None:
        data["failure_reason"] = failure_reason
    await emit_signal(
        pool,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_SOURCE_COMPLETED,
        idempotency_key=f"{run_id}:{source}",
        signal_data=data,
    )


def _orch(pool: asyncpg.Pool) -> TenantOnboardingOrchestrator:
    return TenantOnboardingOrchestrator(
        pool,
        config=TenantOnboardingConfig(
            tick_interval_seconds=0.01,
            max_signals_per_tick=20,
        ),
    )


# =====================================================================
# 1. LOAD-BEARING — atomic new-run handling.
# =====================================================================

async def test_orchestrator_handles_run_created_signal_atomically(
    fresh_db: asyncpg.Pool,
) -> None:
    """Emit onboarding_run_created; tick orchestrator; assert in
    ONE Postgres-observable read that:
      (a) source_onboarding_runs row(s) created.
      (b) source_onboarding_requested signals emitted per source.
      (c) original onboarding_run_created signal marked consumed.
      (d) parent onboarding_runs.status == 'running'.

    All four changes are part of the same transaction (the
    orchestrator's per-signal claim_signals + writes block)."""
    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="github")
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    await _emit_run_created_signal(fresh_db, run_id=run_id, tenant_id=tid)

    await _orch(fresh_db).run(max_ticks=1)

    # ----- (a) source_onboarding_runs rows -----
    source_rows = await fresh_db.fetch(
        "SELECT source, status FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 ORDER BY source",
        run_id,
    )
    assert len(source_rows) == 2
    assert {r["source"] for r in source_rows} == {"slack", "github"}
    assert all(r["status"] == "pending" for r in source_rows)

    # ----- (b) source_onboarding_requested signals -----
    requested = await fresh_db.fetch(
        "SELECT idempotency_key FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 ORDER BY idempotency_key",
        SOURCE_ONBOARDING_INBOX_KIND, SOURCE_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_SOURCE_REQUESTED,
    )
    assert len(requested) == 2
    expected_keys = {f"{run_id}:slack", f"{run_id}:github"}
    assert {r["idempotency_key"] for r in requested} == expected_keys

    # ----- (c) original run_created signal consumed -----
    original_signal = await fresh_db.fetchrow(
        "SELECT consumed_at FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        WORKFLOW_KIND, WORKFLOW_ID_INBOX,
        SIGNAL_KIND_RUN_CREATED, str(run_id),
    )
    assert original_signal["consumed_at"] is not None

    # ----- (d) parent run status='running' -----
    run_row = await fresh_db.fetchrow(
        "SELECT status FROM onboarding_runs WHERE id = $1", run_id,
    )
    assert run_row["status"] == "running"


# =====================================================================
# 2. LOAD-BEARING — atomic rollback on failure mid-transaction.
# =====================================================================

async def test_orchestrator_claim_signals_rolls_back_on_failure(
    fresh_db: asyncpg.Pool,
) -> None:
    """LOAD-BEARING (A12 + A13): inject a failure between claim and
    source_row insert. Assert:
      (a) signal NOT marked consumed (still claimable on next tick).
      (b) no source_onboarding_runs rows created.
      (c) parent run status still 'pending' (no 'running' transition).

    Same shape as Phase 1's
    `test_oauth_poller_atomic_rollback_on_signal_failure`.
    """
    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    await _emit_run_created_signal(fresh_db, run_id=run_id, tenant_id=tid)

    # Inject failure inside the orchestrator's atomic block by
    # monkey-patching _insert_source_row (which runs AFTER the
    # claim, inside the same txn).
    class _SyntheticFailure(RuntimeError):
        pass

    import services.ingestion.workflows.tenant_onboarding as orch_module

    original_insert = orch_module._insert_source_row

    async def _raising_insert(*args: Any, **kwargs: Any) -> Any:
        raise _SyntheticFailure("simulated source-row insert failure")

    orch_module._insert_source_row = _raising_insert  # type: ignore[assignment]
    try:
        with pytest.raises(_SyntheticFailure):
            await _orch(fresh_db).run(max_ticks=1)
    finally:
        orch_module._insert_source_row = original_insert  # type: ignore[assignment]

    # ----- (a) signal NOT consumed (still claimable) -----
    signal_row = await fresh_db.fetchrow(
        "SELECT consumed_at FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        WORKFLOW_KIND, WORKFLOW_ID_INBOX,
        SIGNAL_KIND_RUN_CREATED, str(run_id),
    )
    assert signal_row["consumed_at"] is None, (
        f"A12 + A13 INVARIANT VIOLATED: signal.consumed_at = "
        f"{signal_row['consumed_at']!r} after orchestrator failure; "
        f"expected NULL. The claim_signals call did NOT participate "
        f"in the orchestrator's transaction — investigate the use "
        f"of claim_signals(conn) vs poll_signals(pool)."
    )

    # ----- (b) no source rows -----
    source_count = await fresh_db.fetchval(
        "SELECT count(*) FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1", run_id,
    )
    assert source_count == 0

    # ----- (c) parent run status still pending -----
    run_row = await fresh_db.fetchrow(
        "SELECT status FROM onboarding_runs WHERE id = $1", run_id,
    )
    assert run_row["status"] == "pending"


# =====================================================================
# 3. Source applicability — provider_installations + gmail at tick-time.
# =====================================================================

async def test_orchestrator_determines_applicable_sources_from_installs(
    fresh_db: asyncpg.Pool,
) -> None:
    """Tenant has slack + gmail active installs; tick orchestrator
    on a run created from a discord trigger (not installed). Assert
    EXACTLY 2 source_onboarding_runs rows — slack + gmail — because
    provider_installations + gmail_installations are the source of
    truth, NOT the trigger's source. Per A13 / Phase 2 design
    decision."""
    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    await _seed_gmail_install(fresh_db, tenant_id=tid)
    # NOT installed: github, discord.
    # Note the run's sources_enabled snapshot is ["slack"] (from a
    # hypothetical slack trigger), but applicability is decided
    # from installs at tick-time.
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, source="slack",
    )
    await _emit_run_created_signal(fresh_db, run_id=run_id, tenant_id=tid)

    await _orch(fresh_db).run(max_ticks=1)

    rows = await fresh_db.fetch(
        "SELECT source FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 ORDER BY source",
        run_id,
    )
    actual = {r["source"] for r in rows}
    assert actual == {"slack", "gmail"}, (
        f"Source applicability rule broken: expected {{slack, gmail}} "
        f"(both installed); got {actual}. provider_installations + "
        f"gmail_installations is the source of truth per A13."
    )


async def test_orchestrator_fails_run_when_no_installs_active(
    fresh_db: asyncpg.Pool,
) -> None:
    """No active installs at tick-time → run marked 'failed' rather
    than zero-source onboarding that never completes. Documented
    edge case in tenant_onboarding.py's _handle_run_created."""
    tid = await _seed_tenant(fresh_db)
    # No installs seeded — the slack trigger's run has zero
    # applicable sources at tick-time.
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    await _emit_run_created_signal(fresh_db, run_id=run_id, tenant_id=tid)

    await _orch(fresh_db).run(max_ticks=1)

    run_row = await fresh_db.fetchrow(
        "SELECT status, error_summary FROM onboarding_runs WHERE id = $1",
        run_id,
    )
    assert run_row["status"] == "failed"
    assert "No active installs" in run_row["error_summary"]


# =====================================================================
# 4. Completion: all sources done → run complete + tenant signal emitted.
# =====================================================================

async def test_orchestrator_completes_run_when_all_sources_done(
    fresh_db: asyncpg.Pool,
) -> None:
    """Seed a run with 4 source rows (all sources). Emit
    source_onboarding_completed signals for all 4. Tick orchestrator
    enough times to drain.

    Assert (all observable in one Postgres state read):
      (a) all 4 source rows status='completed'.
      (b) parent run status='complete'.
      (c) tenant_onboarding_completed signal emitted with
          idempotency_key=str(run_id) to Bridge's inbox.
    """
    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, status="running",
    )
    # Seed 4 source rows directly (no need to go through new-run
    # phase — testing the completion phase in isolation).
    for source in ("slack", "github", "discord", "gmail"):
        await fresh_db.execute(
            """
            INSERT INTO source_onboarding_runs
                (onboarding_run_id, source, tenant_id, status, started_at)
            VALUES ($1, $2, $3, 'in_progress', now())
            """,
            run_id, source, tid,
        )

    # Inject 4 completion signals (M6.2 simulator).
    for source in ("slack", "github", "discord", "gmail"):
        await _emit_source_completed_signal(
            fresh_db, run_id=run_id, source=source,
        )

    # Drain.
    await _orch(fresh_db).run(max_ticks=1)

    # ----- (a) all source rows completed -----
    sources = await fresh_db.fetch(
        "SELECT source, status, completed_at FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 ORDER BY source",
        run_id,
    )
    assert len(sources) == 4
    assert all(s["status"] == "completed" for s in sources)
    assert all(s["completed_at"] is not None for s in sources)

    # ----- (b) parent run complete -----
    run_row = await fresh_db.fetchrow(
        "SELECT status, completed_at FROM onboarding_runs WHERE id = $1",
        run_id,
    )
    assert run_row["status"] == "complete", (
        f"Parent run status is {run_row['status']!r} after all "
        f"4 sources completed; expected 'complete'."
    )
    assert run_row["completed_at"] is not None

    # ----- (c) tenant_onboarding_completed signal to Bridge -----
    bridge_signal = await fresh_db.fetchrow(
        "SELECT idempotency_key, signal_data FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3",
        BRIDGE_INBOX_KIND, BRIDGE_INBOX_ID, SIGNAL_KIND_TENANT_COMPLETED,
    )
    assert bridge_signal is not None
    assert bridge_signal["idempotency_key"] == str(run_id)
    data = (
        orjson.loads(bridge_signal["signal_data"])
        if isinstance(bridge_signal["signal_data"], (str, bytes, bytearray))
        else dict(bridge_signal["signal_data"])
    )
    assert data["onboarding_run_id"] == str(run_id)
    assert data["tenant_id"] == str(tid)


# =====================================================================
# 5. Partial-failure: one source failed → parent run failed.
# =====================================================================

async def test_orchestrator_fails_run_on_source_failure(
    fresh_db: asyncpg.Pool,
) -> None:
    """Emit source_onboarding_completed with failure_reason for ONE
    source out of 2. Assert parent run status='failed' (M6.1
    default: any source failure fails the run; no 'partial'
    status)."""
    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, status="running",
    )
    for source in ("slack", "github"):
        await fresh_db.execute(
            """
            INSERT INTO source_onboarding_runs
                (onboarding_run_id, source, tenant_id, status, started_at)
            VALUES ($1, $2, $3, 'in_progress', now())
            """,
            run_id, source, tid,
        )

    await _emit_source_completed_signal(
        fresh_db, run_id=run_id, source="slack",
        failure_reason="OAuth token revoked by user",
    )

    await _orch(fresh_db).run(max_ticks=1)

    # Source row marked failed with reason.
    slack_row = await fresh_db.fetchrow(
        "SELECT status, failure_reason FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = 'slack'", run_id,
    )
    assert slack_row["status"] == "failed"
    assert "OAuth token revoked" in slack_row["failure_reason"]

    # Parent run marked failed even though github is still in_progress.
    run_row = await fresh_db.fetchrow(
        "SELECT status, error_summary FROM onboarding_runs WHERE id = $1",
        run_id,
    )
    assert run_row["status"] == "failed"
    assert "slack" in run_row["error_summary"]


# =====================================================================
# 6. Pattern-alignment analyzer accepts tenant_onboarding.py.
# =====================================================================

def test_orchestrator_passes_pattern_alignment_analyzer() -> None:
    """The M6.0 static analyzer must accept tenant_onboarding.py."""
    from services.ingestion.workflows.tests.test_pattern_alignment import (
        WORKFLOWS_DIR,
        _all_rules,
    )

    path = WORKFLOWS_DIR / "tenant_onboarding.py"
    assert path.exists()
    violations = _all_rules(path)
    if violations:
        formatted = "\n".join(str(v) for v in violations)
        raise AssertionError(
            f"tenant_onboarding.py violates M6 pattern-alignment "
            f"rules:\n{formatted}\n\n"
            f"See docs/ingestion/pattern-alignment-rules.md."
        )


# =====================================================================
# 7. Concurrent poller + orchestrator: stress test.
# =====================================================================

async def test_poller_and_orchestrator_run_concurrently_without_deadlock(
    fresh_db: asyncpg.Pool,
) -> None:
    """Seed 20 onboarding_triggers + provider_installations. Run
    poller + orchestrator concurrently. Assert all 20 triggers
    consumed AND all 20 onboarding_run_created signals consumed by
    the orchestrator AND source rows materialized — within a
    bounded number of ticks.

    This is the M6.1 design assertion: two processes, two inboxes,
    no row-level lock contention via SKIP LOCKED. If either service
    deadlocked the other, the bounded-tick budget runs out without
    all 20 runs draining."""
    tid = await _seed_tenant(fresh_db, "stress")
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="github")

    n = 20
    for _ in range(n):
        await fresh_db.execute(
            """
            INSERT INTO onboarding_triggers
                (id, tenant_id, source, trigger_kind, payload)
            VALUES ($1, $2, 'slack', 'install', '{}'::jsonb)
            """,
            uuid7(), tid,
        )

    poller = OAuthPoller(
        fresh_db,
        config=OAuthPollerConfig(
            tick_interval_seconds=0.005,
            max_triggers_per_tick=5,
        ),
    )
    orch = TenantOnboardingOrchestrator(
        fresh_db,
        config=TenantOnboardingConfig(
            tick_interval_seconds=0.005,
            max_signals_per_tick=5,
        ),
    )

    # Run both concurrently for a bounded number of ticks each.
    # 10 ticks × max-per-tick=5 = 50-capacity, enough for n=20 with
    # interleaving.
    await asyncio.gather(
        poller.run(max_ticks=10),
        orch.run(max_ticks=10),
    )

    # All triggers consumed.
    unconsumed_triggers = await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND consumed_at IS NULL", tid,
    )
    assert unconsumed_triggers == 0, (
        f"{unconsumed_triggers} of {n} triggers remain unconsumed. "
        f"Poller and orchestrator did not converge within 10 ticks "
        f"each — investigate row-lock contention or signal-flow "
        f"breakage."
    )

    # All onboarding_run_created signals consumed.
    unconsumed_signals = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND consumed_at IS NULL",
        WORKFLOW_KIND, WORKFLOW_ID_INBOX, SIGNAL_KIND_RUN_CREATED,
    )
    assert unconsumed_signals == 0, (
        f"{unconsumed_signals} run_created signals unconsumed. "
        f"Orchestrator did not drain its inbox."
    )

    # Source rows materialized (2 per run × n runs).
    source_count = await fresh_db.fetchval(
        "SELECT count(*) FROM source_onboarding_runs "
        "WHERE tenant_id = $1", tid,
    )
    assert source_count == 2 * n


# =====================================================================
# 8. Idempotency: re-tick after partial completion → resume cleanly.
# =====================================================================

async def test_orchestrator_idempotent_across_partial_completion(
    fresh_db: asyncpg.Pool,
) -> None:
    """Process some sources, then a fresh orchestrator instance
    ticks again with a remaining signal. Assert it resumes
    correctly: the partially-completed run finishes and the
    tenant_onboarding_completed signal fires.

    This is the in-process equivalent of the subprocess
    SIGTERM/restart test — same property, no subprocess overhead.
    """
    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, status="running",
    )
    for source in ("slack", "github"):
        await fresh_db.execute(
            """
            INSERT INTO source_onboarding_runs
                (onboarding_run_id, source, tenant_id, status, started_at)
            VALUES ($1, $2, $3, 'in_progress', now())
            """,
            run_id, source, tid,
        )
    # Slack completes first.
    await _emit_source_completed_signal(
        fresh_db, run_id=run_id, source="slack",
    )

    orch_a = _orch(fresh_db)
    await orch_a.run(max_ticks=1)

    # After one signal: slack completed, github still in_progress,
    # parent run still 'running'.
    slack = await fresh_db.fetchval(
        "SELECT status FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = 'slack'", run_id,
    )
    assert slack == "completed"
    run_status = await fresh_db.fetchval(
        "SELECT status FROM onboarding_runs WHERE id = $1", run_id,
    )
    assert run_status == "running"

    # Github completes (simulates SIGTERM/restart — different
    # orchestrator instance picks up).
    await _emit_source_completed_signal(
        fresh_db, run_id=run_id, source="github",
    )

    orch_b = _orch(fresh_db)  # fresh instance, like a restart
    await orch_b.run(max_ticks=1)

    # Now everything terminal.
    run_status_final = await fresh_db.fetchval(
        "SELECT status FROM onboarding_runs WHERE id = $1", run_id,
    )
    assert run_status_final == "complete"

    tenant_completed = await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND signal_kind = $2 "
        "AND idempotency_key = $3",
        BRIDGE_INBOX_KIND, SIGNAL_KIND_TENANT_COMPLETED, str(run_id),
    )
    assert tenant_completed == 1
