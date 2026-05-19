"""M6.2b Phase 1 — Reconciler service tests.

Covers the M6.2b chain-interception service:
  - LOAD-BEARING: clean path stamps reconciled_at + emits
    source_onboarding_completed to TenantOnboarding (preserving M6.1
    consumer contract).
  - LOAD-BEARING: re-share path increments pass_count, transitions
    status back to 'in_progress', marks originals 'reconciliation_resharded',
    INSERTs new shards with parent_shard_id linkage, emits
    shard_fetch_requested per new shard. ALL atomic.
  - LOAD-BEARING: atomic rollback on emit failure (A12 contract).
  - Default-clean stub path: pre-M6.3-M6.6 expected behaviour;
    every (run, source) defaults to clean.
  - Signal-replay idempotent (emit_signal UNIQUE constraint).
  - Replay-after-reconciled: second signal with same key returns
    fast (idempotent re-emit of completion).
  - Pattern-alignment analyzer accepts reconciler.py.

The subprocess SIGTERM test lives in
test_reconciler_subprocess.py.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from lib.shared.ids import uuid7
from services.ingestion.planners import Shard
from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingestion.workflows.reconciler import (
    Reconciler,
    ReconcilerConfig,
    SHARD_FETCH_INBOX_ID,
    SHARD_FETCH_INBOX_KIND,
    SIGNAL_KIND_SHARDS_COMPLETED,
    SIGNAL_KIND_SHARD_REQUESTED,
    SIGNAL_KIND_SOURCE_COMPLETED,
    TENANT_ONBOARDING_INBOX_ID,
    TENANT_ONBOARDING_INBOX_KIND,
    WORKFLOW_ID_INBOX,
    WORKFLOW_KIND,
)
from services.ingestion.workflows.signals import emit_signal


pytestmark = [pytest.mark.timeout(60)]


# =====================================================================
# Helpers.
# =====================================================================
async def _seed_tenant(pool: asyncpg.Pool, label: str = "rec") -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"{label}-{tid.hex[:8]}",
    )
    return tid


async def _seed_run(
    pool: asyncpg.Pool, *, tenant_id: UUID, source: str = "slack",
    status: str = "completed", pass_count: int = 0,
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
    await pool.execute(
        """
        INSERT INTO source_onboarding_runs
            (onboarding_run_id, source, tenant_id, status,
             started_at, completed_at, reconciliation_pass_count)
        VALUES ($1, $2, $3, $4, now(),
                CASE WHEN $4 = 'completed' THEN now() ELSE NULL END,
                $5)
        """,
        run_id, source, tenant_id, status, pass_count,
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


async def _emit_shards_completed(
    pool: asyncpg.Pool, *, run_id: UUID, tenant_id: UUID, source: str,
    pass_count: int = 0,
) -> None:
    """Inject a source_shards_completed signal (simulates SourceOnboarding
    rollup post-M6.2b chain change)."""
    await emit_signal(
        pool,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_SHARDS_COMPLETED,
        idempotency_key=f"{run_id}:{source}:pass_{pass_count}",
        signal_data={
            "onboarding_run_id": str(run_id),
            "tenant_id": str(tenant_id),
            "source": source,
            "reconciliation_pass_count": pass_count,
        },
    )


def _service(pool: asyncpg.Pool) -> Reconciler:
    return Reconciler(
        pool,
        config=ReconcilerConfig(
            tick_interval_seconds=0.01,
            max_signals_per_tick=20,
        ),
    )


async def _clean_reconciler(
    shards: list[asyncpg.Record], run: asyncpg.Record,
) -> ReconciliationDecision:
    return ReconciliationDecision(has_gaps=False, message="test: clean")


def _reshare_reconciler_factory(
    parent_shard_id: UUID, num_new: int = 2,
) -> Any:
    """Test reconciler returning has_gaps=True with `num_new` new shards
    all parented to `parent_shard_id`."""
    async def _reshare(
        shards: list[asyncpg.Record], run: asyncpg.Record,
    ) -> ReconciliationDecision:
        new_shards = [
            ResharedShard(
                shard=Shard(
                    shard_kind="slack_channel_window",
                    shard_identifier={"channel_id": f"C00{i + 2}",
                                      "gap": f"window_{i}"},
                    recency_score=1.5,  # boosted per LLD §3
                ),
                parent_shard_id=parent_shard_id,
            )
            for i in range(num_new)
        ]
        return ReconciliationDecision(
            has_gaps=True, message="test: gap detected",
            new_shards=new_shards,
        )
    return _reshare


# =====================================================================
# 1. LOAD-BEARING — clean-path atomic handling.
# =====================================================================

async def test_reconciler_handles_source_shards_completed_clean_path(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Emit source_shards_completed; mock dispatch to clean. Assert:
      (a) reconciled_at stamped on source_onboarding_runs.
      (b) source_onboarding_completed emitted to TenantOnboarding.
      (c) original signal consumed.
    All three observable changes commit atomically.
    """
    monkeypatch.setitem(RECONCILER_DISPATCH, "slack", _clean_reconciler)

    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_run(fresh_db, tenant_id=tid, source="slack")
    await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack", state="done",
    )
    await _emit_shards_completed(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )

    await _service(fresh_db).run(max_ticks=1)

    # (a) reconciled_at stamped.
    row = await fresh_db.fetchrow(
        "SELECT reconciled_at, status, reconciliation_pass_count "
        "FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert row["reconciled_at"] is not None
    assert row["status"] == "completed"
    assert row["reconciliation_pass_count"] == 0  # no reshares

    # (b) source_onboarding_completed emitted to TenantOnboarding.
    completed = await fresh_db.fetchrow(
        "SELECT signal_data FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_SOURCE_COMPLETED, f"{run_id}:slack",
    )
    assert completed is not None

    # (c) Original signal consumed.
    consumed_at = await fresh_db.fetchval(
        "SELECT consumed_at FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        WORKFLOW_KIND, WORKFLOW_ID_INBOX,
        SIGNAL_KIND_SHARDS_COMPLETED, f"{run_id}:slack:pass_0",
    )
    assert consumed_at is not None


# =====================================================================
# 2. LOAD-BEARING — re-share path atomic handling.
# =====================================================================

async def test_reconciler_handles_source_shards_completed_reshare_path(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Emit source_shards_completed; mock dispatch to gappy with 2
    new shards. Assert ALL FIVE observable conditions for re-share:
      (a) source_onboarding_runs.status back to 'in_progress'.
      (b) source_onboarding_runs.reconciliation_pass_count == 1.
      (c) Original shard marked 'reconciliation_resharded'.
      (d) 2 new shards created with parent_shard_id linkage.
      (e) 2 shard_fetch_requested signals emitted.
    """
    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_run(fresh_db, tenant_id=tid, source="slack")
    orig_shard_id = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
        state="done",
    )

    monkeypatch.setitem(
        RECONCILER_DISPATCH, "slack",
        _reshare_reconciler_factory(parent_shard_id=orig_shard_id, num_new=2),
    )
    await _emit_shards_completed(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )

    await _service(fresh_db).run(max_ticks=1)

    # (a) status back to in_progress.
    row = await fresh_db.fetchrow(
        "SELECT status, reconciliation_pass_count, reconciled_at "
        "FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert row["status"] == "in_progress", (
        f"Run status should be 'in_progress' during re-share; "
        f"got {row['status']!r}."
    )

    # (b) pass_count incremented.
    assert row["reconciliation_pass_count"] == 1
    assert row["reconciled_at"] is None  # not reconciled clean yet

    # (c) Original shard marked 'reconciliation_resharded'.
    orig_state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", orig_shard_id,
    )
    assert orig_state == "reconciliation_resharded", (
        f"Original shard should be marked 'reconciliation_resharded' "
        f"when re-shared; got state={orig_state!r}."
    )

    # (d) 2 new shards with parent_shard_id linkage.
    new_shards = await fresh_db.fetch(
        "SELECT id, state, parent_shard_id, recency_score "
        "FROM onboarding_shards "
        "WHERE onboarding_run_id = $1 AND parent_shard_id IS NOT NULL "
        "ORDER BY created_at, id",
        run_id,
    )
    assert len(new_shards) == 2, (
        f"Expected 2 reshared shards; got {len(new_shards)}."
    )
    for sh in new_shards:
        assert sh["parent_shard_id"] == orig_shard_id
        assert sh["state"] == "pending"
        assert float(sh["recency_score"]) == 1.5

    # (e) 2 shard_fetch_requested signals emitted to ShardFetch inbox.
    n_shard_req = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3",
        SHARD_FETCH_INBOX_KIND, SHARD_FETCH_INBOX_ID,
        SIGNAL_KIND_SHARD_REQUESTED,
    ))
    assert n_shard_req == 2


# =====================================================================
# 3. LOAD-BEARING — atomic rollback on emit failure (A12).
# =====================================================================

async def test_reconciler_atomic_rollback_on_emit_failure(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Monkeypatch the downstream emit to raise. Assert ALL observable
    state changes roll back:
      (a) reconciled_at NOT stamped.
      (b) NO source_onboarding_completed signal exists.
      (c) Original signal still claimable (consumed_at IS NULL).
    Verifies the A12 + A13 caller-managed transactional contract at
    the Reconciler-integration level.
    """
    monkeypatch.setitem(RECONCILER_DISPATCH, "slack", _clean_reconciler)

    # Patch the emit_signal function the Reconciler imports to raise.
    from services.ingestion.workflows import reconciler as rec_module

    async def _failing_emit(*args, **kwargs):
        raise RuntimeError("synthetic emit failure — rollback test")

    monkeypatch.setattr(rec_module, "emit_signal", _failing_emit)

    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_run(fresh_db, tenant_id=tid, source="slack")
    await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack", state="done",
    )
    await _emit_shards_completed(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )

    with pytest.raises(RuntimeError, match="synthetic emit failure"):
        await _service(fresh_db).run(max_ticks=1)

    # (a) reconciled_at NOT stamped.
    reconciled_at = await fresh_db.fetchval(
        "SELECT reconciled_at FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert reconciled_at is None, (
        f"Atomic rollback broken: reconciled_at={reconciled_at!r} "
        f"survived a raised RuntimeError mid-transaction."
    )

    # (b) NO source_onboarding_completed signal.
    n_completed = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE signal_kind = $1",
        SIGNAL_KIND_SOURCE_COMPLETED,
    ))
    assert n_completed == 0

    # (c) Original signal NOT consumed.
    consumed_at = await fresh_db.fetchval(
        "SELECT consumed_at FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        WORKFLOW_KIND, WORKFLOW_ID_INBOX,
        SIGNAL_KIND_SHARDS_COMPLETED, f"{run_id}:slack:pass_0",
    )
    assert consumed_at is None, (
        "Signal consumed_at was set despite txn rollback — "
        "A12 caller-managed atomicity contract broken at the "
        "Reconciler level."
    )


# =====================================================================
# 4. Clean-decision path stamps reconciled_at and emits completion.
# =====================================================================

async def test_reconciler_clean_decision_path(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Monkeypatch github dispatch to a clean reconciler; assert run
    reconciles cleanly (status='completed', reconciled_at stamped,
    source_onboarding_completed emitted once).

    Pre-M6.3 this verified the default-clean stub. Post-M6.3-M6.6 all
    sources have real reconcilers, so this verifies the clean-decision
    branch of the Reconciler service via an explicit monkeypatch.
    """
    monkeypatch.setitem(RECONCILER_DISPATCH, "github", _clean_reconciler)

    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_run(fresh_db, tenant_id=tid, source="github")
    await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="github",
        state="done", shard_kind="github_repo_events",
    )
    await _emit_shards_completed(
        fresh_db, run_id=run_id, tenant_id=tid, source="github",
    )

    await _service(fresh_db).run(max_ticks=1)

    # Run reconciled cleanly.
    row = await fresh_db.fetchrow(
        "SELECT status, reconciled_at FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "github",
    )
    assert row["status"] == "completed"
    assert row["reconciled_at"] is not None

    # source_onboarding_completed emitted.
    n_emits = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND signal_kind = $2 "
        "AND idempotency_key = $3",
        TENANT_ONBOARDING_INBOX_KIND, SIGNAL_KIND_SOURCE_COMPLETED,
        f"{run_id}:github",
    ))
    assert n_emits == 1


# =====================================================================
# 5. Signal-replay idempotency (emit_signal UNIQUE).
# =====================================================================

async def test_reconciler_idempotent_on_signal_replay(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Emit source_shards_completed twice with the same idempotency_key.
    The second emit returns was_new=False (deduped by UNIQUE
    constraint); the Reconciler only sees one signal.
    """
    monkeypatch.setitem(RECONCILER_DISPATCH, "slack", _clean_reconciler)

    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_run(fresh_db, tenant_id=tid, source="slack")
    await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack", state="done",
    )

    await _emit_shards_completed(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )
    await _emit_shards_completed(  # duplicate
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )

    n_signals = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND idempotency_key = $2",
        WORKFLOW_KIND, f"{run_id}:slack:pass_0",
    ))
    assert n_signals == 1, (
        f"emit_signal idempotency-key UNIQUE failed: {n_signals} rows."
    )


# =====================================================================
# 6. LOAD-BEARING — cross-service idempotency on replay-after-reconciled.
# =====================================================================

async def test_reconciler_replays_completion_for_already_reconciled_run(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cross-service idempotency property (per M6.2b Phase 2
    acceptance verification): a replayed `source_shards_completed`
    AFTER a clean reconciliation MUST NOT produce a duplicate
    `source_onboarding_completed` in TenantOnboarding's inbox.

    The mechanism: the Reconciler emits `source_onboarding_completed`
    with idempotency_key `f"{run_id}:{source}"` — NO pass_count
    suffix — so the second emit collides on
    workflow_signals' UNIQUE constraint and `emit_signal` returns
    `was_new=False` silently.

    Without this property, TenantOnboarding would see multiple
    `source_onboarding_completed` signals across re-share cycles +
    stale replays and the M6.1 roll-up logic would over-count.

    Sequence verified here:
      1. Pre-seed reconciled_at + a prior `source_onboarding_completed`
         signal in TenantOnboarding's inbox (simulating "Reconciler
         already finished and the consumer already saw it").
      2. Emit a stale `source_shards_completed` with a fresh
         idempotency_key (pass_count=1 — a different cycle).
      3. Run Reconciler.

    Assertions:
      (a) Reconciler does NOT re-stamp reconciled_at (the
          _STAMP_RECONCILED_SQL WHERE-guard makes re-stamping a
          no-op).
      (b) **Exactly ONE `source_onboarding_completed` exists in
          TenantOnboarding's inbox** — the Reconciler's re-emit
          deduped on the UNIQUE constraint.
    """
    monkeypatch.setitem(RECONCILER_DISPATCH, "slack", _clean_reconciler)

    tid = await _seed_tenant(fresh_db)
    run_id = await _seed_run(
        fresh_db, tenant_id=tid, source="slack", pass_count=1,
    )
    # Pre-stamp reconciled_at.
    await fresh_db.execute(
        "UPDATE source_onboarding_runs SET reconciled_at = now() "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    original_reconciled = await fresh_db.fetchval(
        "SELECT reconciled_at FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    # Pre-emit `source_onboarding_completed` to TenantOnboarding's
    # inbox (simulating the prior Reconciler clean pass).
    await emit_signal(
        fresh_db,
        workflow_kind=TENANT_ONBOARDING_INBOX_KIND,
        workflow_id=TENANT_ONBOARDING_INBOX_ID,
        signal_kind=SIGNAL_KIND_SOURCE_COMPLETED,
        idempotency_key=f"{run_id}:slack",
        signal_data={
            "onboarding_run_id": str(run_id), "source": "slack",
        },
    )

    # Emit a stale source_shards_completed with pass_count=1 (a
    # different idempotency key from any prior shards_completed,
    # so it lands fresh in Reconciler's inbox and gets processed).
    await _emit_shards_completed(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack", pass_count=1,
    )

    await _service(fresh_db).run(max_ticks=1)

    # (a) reconciled_at NOT updated.
    new_reconciled = await fresh_db.fetchval(
        "SELECT reconciled_at FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert new_reconciled == original_reconciled

    # (b) LOAD-BEARING: exactly ONE source_onboarding_completed in
    # TenantOnboarding's inbox. The Reconciler's idempotent re-emit
    # deduped on the UNIQUE constraint (key `{run_id}:slack`).
    n_completed = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        TENANT_ONBOARDING_INBOX_KIND, TENANT_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_SOURCE_COMPLETED, f"{run_id}:slack",
    ))
    assert n_completed == 1, (
        f"Cross-service idempotency broken: TenantOnboarding's inbox "
        f"has {n_completed} source_onboarding_completed signals for "
        f"this run after a replay-after-reconciled cycle. The "
        f"Reconciler's emit key should be `{{run_id}}:{{source}}` "
        f"(no pass_count) so replays dedup at the UNIQUE constraint."
    )


# =====================================================================
# 7. Pattern-alignment analyzer accepts reconciler.py.
# =====================================================================

def test_reconciler_passes_pattern_alignment_analyzer() -> None:
    """The M6.0 static analyzer must accept reconciler.py."""
    from services.ingestion.workflows.tests.test_pattern_alignment import (
        WORKFLOWS_DIR,
        _all_rules,
    )

    path = WORKFLOWS_DIR / "reconciler.py"
    assert path.exists()
    violations = _all_rules(path)
    if violations:
        formatted = "\n".join(str(v) for v in violations)
        raise AssertionError(
            f"reconciler.py violates M6 pattern-alignment rules:\n"
            f"{formatted}\n\n"
            f"See docs/ingestion/pattern-alignment-rules.md."
        )
