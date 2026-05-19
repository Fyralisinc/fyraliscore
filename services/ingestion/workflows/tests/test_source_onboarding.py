"""M6.2a SourceOnboarding service tests (M6.2b chain-change updated).

Covers the two-phase service (new-request + shard-completion):
  - LOAD-BEARING: source_onboarding_requested handler creates shard
    rows + emits shard_fetch_requested + marks parent run
    'in_progress', all atomic.
  - LOAD-BEARING: rollback on shard-insert failure preserves the
    signal as claimable on next tick.
  - NotImplementedError from a stubbed planner: parent run marked
    'failed' + **source_onboarding_completed** emitted with failure
    (failure path; unchanged by M6.2b).
  - Empty planner result: parent run marked 'completed' + **emit
    source_shards_completed to Reconciler** (success path; M6.2b
    chain change — even the zero-shard case goes through Reconciler
    for consistency).
  - Completion roll-up: all shards done → parent run 'completed' +
    **source_shards_completed to Reconciler inbox** (M6.2b chain
    change). Idempotency key = f"{run_id}:{source}:pass_{N}" to
    survive re-share cycles.
  - Failure roll-up: any shard failed → parent run 'failed' with
    rolled-up failure_reason + source_onboarding_completed direct
    to TenantOnboarding (failure path bypasses Reconciler).
  - Concurrent shard completions: exactly one source_shards_completed
    emit (idempotency via emit_signal's UNIQUE constraint).

Subprocess SIGTERM test in test_source_onboarding_subprocess.py.

A15 column-naming map applied throughout: tests write/read `id`,
`shard_kind`, `shard_identifier`, `state`, `last_error` per the
M1-shipped 0045 schema, not the M6.2a-prompt-words.
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from lib.shared.ids import uuid7
from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.workflows.signals import emit_signal
from services.ingestion.workflows.source_onboarding import (
    RECONCILER_INBOX_ID,
    RECONCILER_INBOX_KIND,
    SHARD_FETCH_INBOX_ID,
    SHARD_FETCH_INBOX_KIND,
    SIGNAL_KIND_COMPLETED,
    SIGNAL_KIND_REQUESTED,
    SIGNAL_KIND_SHARD_COMPLETED,
    SIGNAL_KIND_SHARD_REQUESTED,
    SIGNAL_KIND_SHARDS_COMPLETED,
    SourceOnboarding,
    SourceOnboardingConfig,
    TENANT_ONBOARDING_INBOX_ID,
    TENANT_ONBOARDING_INBOX_KIND,
    WORKFLOW_ID_INBOX,
    WORKFLOW_KIND,
)


pytestmark = [pytest.mark.timeout(60)]


# =====================================================================
# Helpers.
# =====================================================================
async def _seed_tenant(pool: asyncpg.Pool, label: str = "src") -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"{label}-{tid.hex[:8]}",
    )
    return tid


async def _seed_provider_install(
    pool: asyncpg.Pool, *, tenant_id: UUID, provider: str,
) -> None:
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, $3, $4, TRUE)
        """,
        uuid7(), tenant_id, provider,
        f"inst-{tenant_id.hex[:8]}-{provider}",
    )


async def _seed_gmail_install(pool: asyncpg.Pool, *, tenant_id: UUID) -> None:
    await pool.execute(
        """
        INSERT INTO gmail_installations
            (id, tenant_id, workspace_domain, service_account_email,
             scope, disabled_at)
        VALUES ($1, $2, $3, $4, 'gmail.readonly', NULL)
        """,
        uuid7(), tenant_id,
        f"workspace-{tenant_id.hex[:8]}.example.com",
        f"svc-{tenant_id.hex[:8]}@example.iam.gserviceaccount.com",
    )


async def _seed_onboarding_run(
    pool: asyncpg.Pool, *, tenant_id: UUID, source: str = "slack",
) -> UUID:
    run_id = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_runs
            (id, tenant_id, trigger_kind, workflow_id, status,
             sources_enabled, started_at)
        VALUES ($1, $2, 'install', $3, 'running', $4::text[], now())
        """,
        run_id, tenant_id, f"wf-{run_id.hex[:8]}", [source],
    )
    return run_id


async def _seed_source_run(
    pool: asyncpg.Pool, *, run_id: UUID, source: str, tenant_id: UUID,
    status: str = "pending",
) -> None:
    await pool.execute(
        """
        INSERT INTO source_onboarding_runs
            (onboarding_run_id, source, tenant_id, status)
        VALUES ($1, $2, $3, $4)
        """,
        run_id, source, tenant_id, status,
    )


async def _emit_source_requested(
    pool: asyncpg.Pool, *, run_id: UUID, tenant_id: UUID, source: str,
) -> None:
    """Inject a source_onboarding_requested signal (simulates M6.1)."""
    await emit_signal(
        pool,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_REQUESTED,
        idempotency_key=f"{run_id}:{source}",
        signal_data={
            "onboarding_run_id": str(run_id),
            "tenant_id": str(tenant_id),
            "source": source,
        },
    )


async def _emit_shard_completed(
    pool: asyncpg.Pool, *, shard_id: UUID, status: str = "done",
    failure_reason: str | None = None,
) -> None:
    """Inject a shard_fetch_completed signal (simulates Phase 2's
    ShardFetch)."""
    data: dict[str, Any] = {
        "shard_id": str(shard_id),
        "status": status,
    }
    if failure_reason:
        data["failure_reason"] = failure_reason
    await emit_signal(
        pool,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_SHARD_COMPLETED,
        idempotency_key=str(shard_id),
        signal_data=data,
    )


async def _seed_shard(
    pool: asyncpg.Pool, *, run_id: UUID, tenant_id: UUID, source: str,
    state: str = "pending", shard_kind: str = "slack_channel_window",
    identifier: dict | None = None, last_error: str | None = None,
) -> UUID:
    """Seed an onboarding_shards row directly using the existing 0045
    schema columns (A15)."""
    shard_id = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_shards
            (id, onboarding_run_id, tenant_id, source, shard_kind,
             shard_identifier, recency_score, state, last_error,
             created_at, completed_at)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, now(),
                CASE WHEN $8 IN ('done','failed') THEN now() ELSE NULL END)
        """,
        shard_id, run_id, tenant_id, source, shard_kind,
        orjson.dumps(identifier or {"k": "v"}).decode("utf-8"),
        1.0, state, last_error,
    )
    return shard_id


def _service(pool: asyncpg.Pool) -> SourceOnboarding:
    """Construct a SourceOnboarding with a tight tick interval for
    tests."""
    return SourceOnboarding(
        pool,
        config=SourceOnboardingConfig(
            tick_interval_seconds=0.01,
            max_signals_per_tick=20,
        ),
    )


# Test planners — updated for M6.4 / A18.6 PlannerContext signature.
from services.ingestion.planners.context import PlannerContext  # noqa: E402


async def _test_planner_three_shards(ctx: PlannerContext) -> list[Shard]:
    return [
        Shard(
            shard_kind="slack_channel_window",
            shard_identifier={"channel_id": f"C{i:03d}"},
            recency_score=1.0 - i * 0.1,
        )
        for i in range(3)
    ]


async def _test_planner_empty(ctx: PlannerContext) -> list[Shard]:
    return []


# =====================================================================
# 1. LOAD-BEARING — atomic new-request handling with test planner.
# =====================================================================

async def test_source_onboarding_handles_request_with_test_planner(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Emit source_onboarding_requested; tick service; assert in ONE
    Postgres-observable read that:
      (a) 3 onboarding_shards rows created (state='pending').
      (b) 3 shard_fetch_requested signals emitted (to shard_fetch inbox).
      (c) source_onboarding_runs.status == 'in_progress'.
      (d) original source_onboarding_requested signal consumed.

    All four changes are part of the same transaction (the service's
    per-signal claim_signals + writes block)."""
    monkeypatch.setitem(
        PLANNER_DISPATCH, "slack", _test_planner_three_shards,
    )

    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    await _seed_source_run(
        fresh_db, run_id=run_id, source="slack", tenant_id=tid,
    )
    await _emit_source_requested(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )

    await _service(fresh_db).run(max_ticks=1)

    # (a) 3 shards created.
    shard_rows = await fresh_db.fetch(
        "SELECT id, state, shard_kind, shard_identifier, source "
        "FROM onboarding_shards WHERE onboarding_run_id = $1 "
        "ORDER BY created_at, id",
        run_id,
    )
    assert len(shard_rows) == 3, (
        f"Expected 3 shards; got {len(shard_rows)}."
    )
    for row in shard_rows:
        assert row["state"] == "pending"
        assert row["shard_kind"] == "slack_channel_window"
        assert row["source"] == "slack"
        # shard_identifier is JSONB; asyncpg returns it as a string.
        ident_raw = row["shard_identifier"]
        ident = (
            orjson.loads(ident_raw) if isinstance(ident_raw, (str, bytes))
            else dict(ident_raw)
        )
        assert "channel_id" in ident

    # (b) 3 shard_fetch_requested signals to ShardFetch inbox.
    sig_count = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3",
        SHARD_FETCH_INBOX_KIND, SHARD_FETCH_INBOX_ID,
        SIGNAL_KIND_SHARD_REQUESTED,
    ))
    assert sig_count == 3, (
        f"Expected 3 shard_fetch_requested signals; got {sig_count}."
    )

    # (c) source_onboarding_runs marked in_progress.
    status = await fresh_db.fetchval(
        "SELECT status FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert status == "in_progress"

    # (d) original signal consumed.
    consumed_at = await fresh_db.fetchval(
        "SELECT consumed_at FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        WORKFLOW_KIND, WORKFLOW_ID_INBOX,
        SIGNAL_KIND_REQUESTED, f"{run_id}:slack",
    )
    assert consumed_at is not None


# =====================================================================
# 2. LOAD-BEARING — rollback on shard-insert failure (A12 contract).
# =====================================================================

async def test_source_onboarding_atomic_rollback_on_shard_insert_failure(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Monkeypatch _insert_shard to raise on the SECOND insert.
    Assert ALL four observable changes roll back:
      (a) NO onboarding_shards rows for this run.
      (b) NO shard_fetch_requested signals.
      (c) source_onboarding_runs.status still 'pending'
          (not advanced to in_progress).
      (d) source_onboarding_requested signal NOT consumed
          (still claimable).

    This is the A12 + A13 transactional contract at the service-
    integration level — same shape as M6.1's
    test_oauth_poller_atomic_rollback_on_signal_failure."""
    monkeypatch.setitem(
        PLANNER_DISPATCH, "slack", _test_planner_three_shards,
    )

    # Patch _insert_shard to raise on the second call.
    from services.ingestion.workflows import source_onboarding as so_module
    real = so_module._insert_shard
    call_count = {"n": 0}

    async def _failing_insert(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise RuntimeError(
                "synthetic failure on 2nd insert — rollback test"
            )
        await real(*args, **kwargs)

    monkeypatch.setattr(so_module, "_insert_shard", _failing_insert)

    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    await _seed_source_run(
        fresh_db, run_id=run_id, source="slack", tenant_id=tid,
    )
    await _emit_source_requested(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )

    # The service surfaces the exception on tick.
    with pytest.raises(RuntimeError, match="synthetic failure"):
        await _service(fresh_db).run(max_ticks=1)

    # (a) NO shards survived rollback.
    n_shards = int(await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_shards WHERE onboarding_run_id = $1",
        run_id,
    ))
    assert n_shards == 0, (
        f"Atomic rollback broken: {n_shards} shard rows survived a "
        f"raised RuntimeError mid-transaction."
    )

    # (b) NO shard_fetch_requested signals survived.
    n_sigs = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE signal_kind = $1",
        SIGNAL_KIND_SHARD_REQUESTED,
    ))
    assert n_sigs == 0

    # (c) source_onboarding_runs status still 'pending'.
    status = await fresh_db.fetchval(
        "SELECT status FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert status == "pending", (
        f"Parent run status leaked through rollback as {status!r}."
    )

    # (d) Original signal NOT consumed — still claimable next tick.
    consumed_at = await fresh_db.fetchval(
        "SELECT consumed_at FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        WORKFLOW_KIND, WORKFLOW_ID_INBOX,
        SIGNAL_KIND_REQUESTED, f"{run_id}:slack",
    )
    assert consumed_at is None, (
        "Signal consumed_at was set despite transaction rollback — "
        "the A12 + A13 caller-managed atomicity contract is broken."
    )


# =====================================================================
# 3. NotImplementedError stub planner → run failed + completed-signal emitted.
# =====================================================================

async def test_source_onboarding_handles_not_implemented_planner(
    fresh_db: asyncpg.Pool,
) -> None:
    """source='slack' uses the dispatch table's NotImplementedError
    stub (since M6.5 hasn't shipped). Assert:
      (a) source_onboarding_runs marked 'failed' with informative
          failure_reason that names M6.5.
      (b) source_onboarding_completed emitted to TenantOnboarding
          inbox with failure_reason in signal_data.
      (c) No shard rows created.
    """
    # No monkeypatch: use the real stub from PLANNER_DISPATCH.
    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    await _seed_source_run(
        fresh_db, run_id=run_id, source="slack", tenant_id=tid,
    )
    await _emit_source_requested(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )

    await _service(fresh_db).run(max_ticks=1)

    # (a) source_onboarding_runs failed with informative reason.
    row = await fresh_db.fetchrow(
        "SELECT status, failure_reason FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert row["status"] == "failed"
    assert "M6.5" in (row["failure_reason"] or ""), (
        f"failure_reason should name M6.5 (the responsible sub-block "
        f"for slack's planner); got: {row['failure_reason']!r}"
    )

    # (b) source_onboarding_completed emitted to TenantOnboarding inbox.
    completion = await fresh_db.fetchrow(
        "SELECT signal_data FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_COMPLETED, f"{run_id}:slack",
    )
    assert completion is not None
    data_raw = completion["signal_data"]
    data = (
        orjson.loads(data_raw) if isinstance(data_raw, (str, bytes))
        else dict(data_raw)
    )
    assert "M6.5" in data.get("failure_reason", "")

    # (c) NO shards created — the stub raised before any insert.
    n_shards = int(await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_shards WHERE onboarding_run_id = $1",
        run_id,
    ))
    assert n_shards == 0


# =====================================================================
# 4. Empty planner result → immediate success.
# =====================================================================

async def test_source_onboarding_handles_empty_planner_result(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test planner returns []; assert run immediately completes
    (source has nothing to fetch — edge case)."""
    monkeypatch.setitem(PLANNER_DISPATCH, "gmail", _test_planner_empty)

    tid = await _seed_tenant(fresh_db)
    await _seed_gmail_install(fresh_db, tenant_id=tid)
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, source="gmail",
    )
    await _seed_source_run(
        fresh_db, run_id=run_id, source="gmail", tenant_id=tid,
    )
    await _emit_source_requested(
        fresh_db, run_id=run_id, tenant_id=tid, source="gmail",
    )

    await _service(fresh_db).run(max_ticks=1)

    # Parent run completed (not failed).
    status = await fresh_db.fetchval(
        "SELECT status FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "gmail",
    )
    assert status == "completed"

    # M6.2b chain change: empty-planner success path now emits
    # source_shards_completed to Reconciler inbox (not source_onboarding_completed
    # to TenantOnboarding directly). pass_count is 0 (no re-shares yet).
    completion = await fresh_db.fetchrow(
        "SELECT signal_data FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        RECONCILER_INBOX_KIND, RECONCILER_INBOX_ID,
        SIGNAL_KIND_SHARDS_COMPLETED, f"{run_id}:gmail:pass_0",
    )
    assert completion is not None
    data_raw = completion["signal_data"]
    data = (
        orjson.loads(data_raw) if isinstance(data_raw, (str, bytes))
        else dict(data_raw)
    )
    # No failure_reason on the success path.
    assert "failure_reason" not in data

    # No shards.
    n_shards = int(await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_shards WHERE onboarding_run_id = $1",
        run_id,
    ))
    assert n_shards == 0


# =====================================================================
# 5. Completion roll-up — all shards 'done' → run 'completed'.
# =====================================================================

async def test_source_onboarding_completes_when_all_shards_done(
    fresh_db: asyncpg.Pool,
) -> None:
    """Pre-seed 3 'done' shards + 1 'in_progress' shard; emit
    shard_fetch_completed for the last one; assert:
      (a) Last shard marked 'done'.
      (b) Parent source_onboarding_runs marked 'completed'.
      (c) source_onboarding_completed emitted to TenantOnboarding inbox.
    """
    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="github")
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, source="github",
    )
    await _seed_source_run(
        fresh_db, run_id=run_id, source="github", tenant_id=tid,
        status="in_progress",
    )
    # 3 'done' shards.
    for _ in range(3):
        await _seed_shard(
            fresh_db, run_id=run_id, tenant_id=tid, source="github",
            state="done", shard_kind="github_repo_events",
        )
    # 1 'in_progress' shard — the one whose completion we'll emit.
    last_shard = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="github",
        state="in_progress", shard_kind="github_repo_events",
    )

    await _emit_shard_completed(fresh_db, shard_id=last_shard, status="done")

    await _service(fresh_db).run(max_ticks=1)

    # (a) Last shard now 'done'.
    last_state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", last_shard,
    )
    assert last_state == "done"

    # (b) Parent run completed.
    status = await fresh_db.fetchval(
        "SELECT status FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "github",
    )
    assert status == "completed"

    # (c) M6.2b chain change: success path emits source_shards_completed
    # to Reconciler inbox (not source_onboarding_completed). pass_count
    # is 0 (no re-shares yet for this run).
    n_emits = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        RECONCILER_INBOX_KIND, RECONCILER_INBOX_ID,
        SIGNAL_KIND_SHARDS_COMPLETED, f"{run_id}:github:pass_0",
    ))
    assert n_emits == 1


# =====================================================================
# 6. Failure roll-up — any shard 'failed' → run 'failed'.
# =====================================================================

async def test_source_onboarding_marks_run_failed_if_any_shard_failed(
    fresh_db: asyncpg.Pool,
) -> None:
    """Pre-seed 2 'done' + 1 'failed' shard + 1 'in_progress'; emit
    shard_fetch_completed (done) for the last in-progress. Assert:
      (a) Parent run marked 'failed' (not 'completed', because one
          sibling failed).
      (b) failure_reason rolls up the failed shard's last_error.
    """
    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="github")
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, source="github",
    )
    await _seed_source_run(
        fresh_db, run_id=run_id, source="github", tenant_id=tid,
        status="in_progress",
    )
    for _ in range(2):
        await _seed_shard(
            fresh_db, run_id=run_id, tenant_id=tid, source="github",
            state="done", shard_kind="github_repo_events",
        )
    await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="github",
        state="failed", shard_kind="github_repo_events",
        last_error="repo permission denied",
    )
    last_shard = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="github",
        state="in_progress", shard_kind="github_repo_events",
    )
    await _emit_shard_completed(fresh_db, shard_id=last_shard, status="done")

    await _service(fresh_db).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT status, failure_reason FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "github",
    )
    assert row["status"] == "failed", (
        f"Run status should be 'failed' when any sibling failed; "
        f"got {row['status']!r}."
    )
    assert "repo permission denied" in (row["failure_reason"] or "")


# =====================================================================
# 7. Concurrent shard-completion signals → exactly one parent emit.
# =====================================================================

async def test_source_onboarding_concurrent_completion_signals(
    fresh_db: asyncpg.Pool,
) -> None:
    """Pre-seed 3 'in_progress' shards. Emit 3 shard_fetch_completed
    signals (all 'done'). Run two service replicas concurrently
    draining the inbox. Assert: exactly one
    source_onboarding_completed emit landed in the TenantOnboarding
    inbox (the emit_signal UNIQUE constraint on idempotency_key
    deduplicates concurrent completion attempts).
    """
    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="discord")
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, source="discord",
    )
    await _seed_source_run(
        fresh_db, run_id=run_id, source="discord", tenant_id=tid,
        status="in_progress",
    )
    shard_ids = [
        await _seed_shard(
            fresh_db, run_id=run_id, tenant_id=tid, source="discord",
            state="in_progress", shard_kind="discord_channel_window",
        )
        for _ in range(3)
    ]
    for sid in shard_ids:
        await _emit_shard_completed(fresh_db, shard_id=sid, status="done")

    # Two replicas drain concurrently. SKIP LOCKED gives disjoint
    # signal subsets; emit_signal's ON CONFLICT DO NOTHING dedups
    # the final completion emit.
    replica_a = _service(fresh_db)
    replica_b = _service(fresh_db)
    await asyncio.gather(
        replica_a.run(max_ticks=3),
        replica_b.run(max_ticks=3),
    )

    # M6.2b chain change: success path emits source_shards_completed
    # to Reconciler inbox. The idempotency-key dedup test still holds
    # — only one rollup emit per run+source+pass_count across
    # concurrent SourceOnboarding replicas.
    n_emits = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        RECONCILER_INBOX_KIND, RECONCILER_INBOX_ID,
        SIGNAL_KIND_SHARDS_COMPLETED, f"{run_id}:discord:pass_0",
    ))
    assert n_emits == 1, (
        f"Expected exactly one source_shards_completed emit "
        f"under concurrent completion-signal drains; got {n_emits}. "
        f"The emit_signal idempotency-key UNIQUE constraint did not "
        f"dedupe."
    )

    # All shards 'done'.
    n_done = int(await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_shards "
        "WHERE onboarding_run_id = $1 AND state = 'done'",
        run_id,
    ))
    assert n_done == 3

    # Parent run 'completed'.
    status = await fresh_db.fetchval(
        "SELECT status FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "discord",
    )
    assert status == "completed"


# =====================================================================
# 8. Edge case — install was disabled between trigger and pickup.
# =====================================================================

async def test_source_onboarding_handles_missing_install(
    fresh_db: asyncpg.Pool,
) -> None:
    """No provider_installations row for the tenant + source (a
    disabled-between-trigger-and-pickup A14 race). Assert the run is
    marked 'failed' with an informative reason BEFORE any planner
    call is attempted."""
    tid = await _seed_tenant(fresh_db)
    # No provider_install seeded.
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    await _seed_source_run(
        fresh_db, run_id=run_id, source="slack", tenant_id=tid,
    )
    await _emit_source_requested(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )

    await _service(fresh_db).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT status, failure_reason FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert row["status"] == "failed"
    assert "No active install" in (row["failure_reason"] or "")

    # source_onboarding_completed emitted with failure_reason.
    completion = await fresh_db.fetchrow(
        "SELECT signal_data FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_COMPLETED, f"{run_id}:slack",
    )
    assert completion is not None


# =====================================================================
# 9. Pattern-alignment analyzer accepts source_onboarding.py.
# =====================================================================

def test_source_onboarding_passes_pattern_alignment_analyzer() -> None:
    """The M6.0 static analyzer must accept source_onboarding.py."""
    from services.ingestion.workflows.tests.test_pattern_alignment import (
        WORKFLOWS_DIR,
        _all_rules,
    )

    path = WORKFLOWS_DIR / "source_onboarding.py"
    assert path.exists()
    violations = _all_rules(path)
    if violations:
        formatted = "\n".join(str(v) for v in violations)
        raise AssertionError(
            f"source_onboarding.py violates M6 pattern-alignment "
            f"rules:\n{formatted}\n\n"
            f"See docs/ingestion/pattern-alignment-rules.md."
        )
