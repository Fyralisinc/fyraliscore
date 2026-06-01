"""PeriodicReconciler service tests.

Covers the steady-state re-reconciliation safety net:
  - Eligibility: only settled runs (status='completed' AND
    reconciled_at IS NOT NULL) past their min-age are claimed; runs
    checked too recently, or not yet reconciled, are skipped.
  - Clean decision → watermark advances, run untouched.
  - Gap decision → re-share via the SHARED reconciler.apply_reshare
    (status→in_progress, pass_count++, original resharded, new shards
    + shard_fetch_requested emitted).
  - Transient dispatch error is swallowed (self-healing): watermark
    still advances, run untouched, service does not crash.
  - Pattern-alignment analyzer accepts periodic_reconciler.py.
"""
from __future__ import annotations

import datetime as dt
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from lib.shared.ids import uuid7
from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingest.ingestion.workflows.periodic_reconciler import (
    PeriodicReconciler,
    PeriodicReconcilerConfig,
)
from services.ingest.ingestion.workflows.reconciler import (
    SHARD_FETCH_INBOX_ID,
    SHARD_FETCH_INBOX_KIND,
    SIGNAL_KIND_SHARD_REQUESTED,
)


pytestmark = [pytest.mark.timeout(60)]


# =====================================================================
# Helpers.
# =====================================================================
async def _seed_tenant(pool: asyncpg.Pool, label: str = "pr") -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"{label}-{tid.hex[:8]}",
    )
    return tid


async def _seed_reconciled_run(
    pool: asyncpg.Pool, *, tenant_id: UUID, source: str = "slack",
    last_check: dt.datetime | None = None,
) -> UUID:
    """Seed a settled (completed + reconciled) source run, optionally
    with a `last_reconcile_check_at` watermark already set."""
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
    await pool.execute(
        """
        INSERT INTO source_onboarding_runs
            (onboarding_run_id, source, tenant_id, status,
             started_at, completed_at, reconciled_at,
             reconciliation_pass_count, last_reconcile_check_at)
        VALUES ($1, $2, $3, 'completed', now(), now(), now(), 0, $4)
        """,
        run_id, source, tenant_id, last_check,
    )
    return run_id


async def _seed_shard(
    pool: asyncpg.Pool, *, run_id: UUID, tenant_id: UUID, source: str,
    state: str = "done", shard_kind: str = "slack_channel_window",
    identifier: dict | None = None,
) -> UUID:
    shard_id = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_shards
            (id, onboarding_run_id, tenant_id, source, shard_kind,
             shard_identifier, recency_score, state, created_at,
             completed_at)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, now(),
                CASE WHEN $8 IN ('done','failed') THEN now() ELSE NULL END)
        """,
        shard_id, run_id, tenant_id, source, shard_kind,
        orjson.dumps(identifier or {"channel_id": "C001"}).decode("utf-8"),
        1.0, state,
    )
    return shard_id


def _service(
    pool: asyncpg.Pool, *, min_age: float = 0.0, batch: int = 20,
) -> PeriodicReconciler:
    return PeriodicReconciler(
        pool,
        config=PeriodicReconcilerConfig(
            tick_interval_seconds=0.01,
            min_age_seconds=min_age,
            batch_size=batch,
        ),
    )


async def _clean(shards, run) -> ReconciliationDecision:
    return ReconciliationDecision(has_gaps=False)


def _reshare_factory(parent_shard_id: UUID, num_new: int = 2) -> Any:
    async def _reshare(shards, run) -> ReconciliationDecision:
        return ReconciliationDecision(
            has_gaps=True,
            new_shards=[
                ResharedShard(
                    shard=Shard(
                        shard_kind="slack_channel_window",
                        shard_identifier={"channel_id": f"C10{i}",
                                          "gap": f"w{i}"},
                        recency_score=1.5,
                    ),
                    parent_shard_id=parent_shard_id,
                )
                for i in range(num_new)
            ],
        )
    return _reshare


# =====================================================================
# 1. Clean decision → watermark advances, run untouched.
# =====================================================================
async def test_periodic_clean_advances_watermark_without_reshare(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(RECONCILER_DISPATCH, "slack", _clean)

    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_reconciled_run(fresh_db, tenant_id=tid, source="slack")
    await _seed_shard(fresh_db, run_id=run_id, tenant_id=tid, source="slack")

    await _service(fresh_db).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT status, reconciliation_pass_count, last_reconcile_check_at "
        "FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert row["status"] == "completed"          # untouched
    assert row["reconciliation_pass_count"] == 0  # no reshare
    assert row["last_reconcile_check_at"] is not None  # watermark stamped

    # No reshare side effects.
    n_new = int(await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_shards "
        "WHERE onboarding_run_id = $1 AND parent_shard_id IS NOT NULL",
        run_id,
    ))
    assert n_new == 0
    n_req = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals WHERE signal_kind = $1",
        SIGNAL_KIND_SHARD_REQUESTED,
    ))
    assert n_req == 0


# =====================================================================
# 2. Gap decision → re-share via the shared apply_reshare path.
# =====================================================================
async def test_periodic_gap_reshares_via_shared_path(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_reconciled_run(fresh_db, tenant_id=tid, source="slack")
    orig = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack", state="done",
    )
    monkeypatch.setitem(
        RECONCILER_DISPATCH, "slack",
        _reshare_factory(parent_shard_id=orig, num_new=2),
    )

    await _service(fresh_db).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT status, reconciliation_pass_count, reconciled_at, "
        "       last_reconcile_check_at "
        "FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    # Re-share re-opens the run for another fetch cycle.
    assert row["status"] == "in_progress"
    assert row["reconciliation_pass_count"] == 1
    # reconciled_at is NOT cleared (the one-shot clean stamp persists);
    # the run re-enters eligibility once its reshare cycle completes.
    assert row["reconciled_at"] is not None
    assert row["last_reconcile_check_at"] is not None

    # Original shard resharded, 2 new shards parented to it.
    assert (await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", orig,
    )) == "reconciliation_resharded"
    new_shards = await fresh_db.fetch(
        "SELECT parent_shard_id, state, recency_score FROM onboarding_shards "
        "WHERE onboarding_run_id = $1 AND parent_shard_id IS NOT NULL",
        run_id,
    )
    assert len(new_shards) == 2
    for sh in new_shards:
        assert sh["parent_shard_id"] == orig
        assert sh["state"] == "pending"
        assert float(sh["recency_score"]) == 1.5

    # One shard_fetch_requested per new shard.
    assert int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 AND signal_kind = $3",
        SHARD_FETCH_INBOX_KIND, SHARD_FETCH_INBOX_ID,
        SIGNAL_KIND_SHARD_REQUESTED,
    )) == 2


# =====================================================================
# 3. Not-yet-due run is skipped (min_age throttle).
# =====================================================================
async def test_periodic_skips_recently_checked_run(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Reshare reconciler would fire IF the run were eligible — proving
    # the skip is due to the watermark, not a clean decision.
    tid = await _seed_tenant(fresh_db)
    just_now = dt.datetime.now(tz=dt.timezone.utc)
    run_id = await _seed_reconciled_run(
        fresh_db, tenant_id=tid, source="slack", last_check=just_now,
    )
    orig = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )
    monkeypatch.setitem(
        RECONCILER_DISPATCH, "slack", _reshare_factory(parent_shard_id=orig),
    )

    # min_age 1h: a run checked "just now" is not yet due.
    await _service(fresh_db, min_age=3600.0).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT status, reconciliation_pass_count FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert row["status"] == "completed"           # not re-opened
    assert row["reconciliation_pass_count"] == 0   # dispatch never ran


# =====================================================================
# 4. Unreconciled (mid-handoff) run is not eligible.
# =====================================================================
async def test_periodic_ignores_unreconciled_run(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_reconciled_run(fresh_db, tenant_id=tid, source="slack")
    # Clear reconciled_at → simulate the transient completed-but-not-yet-
    # reconciled hand-off window; the periodic loop must not touch it.
    await fresh_db.execute(
        "UPDATE source_onboarding_runs SET reconciled_at = NULL "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    orig = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )
    monkeypatch.setitem(
        RECONCILER_DISPATCH, "slack", _reshare_factory(parent_shard_id=orig),
    )

    await _service(fresh_db).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT reconciliation_pass_count, last_reconcile_check_at "
        "FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert row["reconciliation_pass_count"] == 0     # never dispatched
    assert row["last_reconcile_check_at"] is None     # never claimed


# =====================================================================
# 5. Transient dispatch error → self-healing (watermark advances,
#    run untouched, service survives).
# =====================================================================
async def test_periodic_swallows_dispatch_error_and_advances(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _boom(shards, run) -> ReconciliationDecision:
        raise RuntimeError("simulated transient gap-check failure")

    monkeypatch.setitem(RECONCILER_DISPATCH, "slack", _boom)

    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_reconciled_run(fresh_db, tenant_id=tid, source="slack")
    await _seed_shard(fresh_db, run_id=run_id, tenant_id=tid, source="slack")

    # Does not raise — the loop completes normally.
    await _service(fresh_db).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT status, reconciliation_pass_count, last_reconcile_check_at "
        "FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert row["status"] == "completed"               # untouched
    assert row["reconciliation_pass_count"] == 0       # no reshare
    # Watermark advanced so the failing run is retried only after
    # min_age — the error is retried next cycle, not lost.
    assert row["last_reconcile_check_at"] is not None


# =====================================================================
# 6. Pattern-alignment analyzer accepts periodic_reconciler.py.
# =====================================================================
def test_periodic_reconciler_passes_pattern_alignment_analyzer() -> None:
    from services.ingest.ingestion.workflows.tests.test_pattern_alignment import (
        WORKFLOWS_DIR,
        _all_rules,
    )

    path = WORKFLOWS_DIR / "periodic_reconciler.py"
    assert path.exists()
    violations = _all_rules(path)
    if violations:
        formatted = "\n".join(str(v) for v in violations)
        raise AssertionError(
            f"periodic_reconciler.py violates pattern-alignment rules:\n"
            f"{formatted}"
        )
