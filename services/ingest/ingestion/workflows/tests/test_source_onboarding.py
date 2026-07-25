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

from lib.observability import counter, reset_default_for_tests
from lib.shared.ids import uuid7
from lib.shared.product_workflow_metrics import (
    PRODUCT_WORKFLOW_EVENT_OUTCOMES,
    PRODUCT_WORKFLOW_EVENTS,
    PRODUCT_WORKFLOWS,
)
from services.ingest.ingestion.planners import Shard
from services.ingest.ingestion.workflows.signals import emit_signal
import services.ingest.ingestion.workflows.source_onboarding as source_onboarding_module
from services.ingest.ingestion.workflows.source_onboarding import (
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


def _product_workflow_events():
    return counter(
        "product_workflow_events_total",
        "lookup",
        ("workflow", "event", "outcome"),
        allowed_label_values={
            "workflow": PRODUCT_WORKFLOWS,
            "event": PRODUCT_WORKFLOW_EVENTS,
            "outcome": PRODUCT_WORKFLOW_EVENT_OUTCOMES,
        },
    )


@pytest.fixture(autouse=True)
def _clean_product_metrics():
    reset_default_for_tests()
    yield
    reset_default_for_tests()


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
) -> UUID:
    install_id = uuid7()
    await pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, $3, $4, TRUE)
        """,
        install_id, tenant_id, provider,
        f"inst-{tenant_id.hex[:8]}-{provider}",
    )
    return install_id


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


async def _seed_figma_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    connection_state: str = "pending",
    last_error: str | None = None,
) -> UUID:
    """Seed one active Figma installation for connection-state tests."""
    install_id = uuid7()
    await pool.execute(
        """
        INSERT INTO figma_installations
            (id, tenant_id, base_url, team_id, connection_state, last_error)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        install_id,
        tenant_id,
        f"https://api.figma.test/{install_id}",
        f"team-{install_id.hex[:8]}",
        connection_state,
        last_error,
    )
    return install_id


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
    status: str = "pending", installation_row_id: UUID | None = None,
) -> UUID:
    if installation_row_id is None:
        table = (
            "gmail_installations"
            if source == "gmail"
            else "figma_installations"
            if source == "figma"
            else "provider_installations"
        )
        provider_filter = " AND provider = $2" if table == "provider_installations" else ""
        args: tuple[object, ...] = (
            (tenant_id, source) if provider_filter else (tenant_id,)
        )
        installation_row_id = await pool.fetchval(
            f"SELECT id FROM {table} WHERE tenant_id = $1"
            f"{provider_filter} ORDER BY id LIMIT 1",
            *args,
        )
    # Missing-install tests still carry an exact durable identity; the runtime
    # must fail because that row does not exist, not because identity was lost.
    installation_row_id = installation_row_id or uuid7()
    await pool.execute(
        """
        INSERT INTO source_onboarding_runs
            (onboarding_run_id, source, tenant_id, status, installation_row_id)
        VALUES ($1, $2, $3, $4, $5)
        """,
        run_id, source, tenant_id, status, installation_row_id,
    )
    return installation_row_id


async def _emit_source_requested(
    pool: asyncpg.Pool, *, run_id: UUID, tenant_id: UUID, source: str,
    installation_row_id: UUID | None = None,
) -> None:
    """Inject a source_onboarding_requested signal (simulates M6.1)."""
    if installation_row_id is None:
        installation_row_id = await pool.fetchval(
            """
            SELECT installation_row_id
              FROM source_onboarding_runs
             WHERE onboarding_run_id = $1 AND source = $2
            """,
            run_id,
            source,
        )
    signal_data = {
        "onboarding_run_id": str(run_id),
        "tenant_id": str(tenant_id),
        "source": source,
        "installation_row_id": str(installation_row_id),
    }

    await emit_signal(
        pool,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_REQUESTED,
        idempotency_key=f"{run_id}:{source}",
        signal_data=signal_data,
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
    installation_row_id = await pool.fetchval(
        """
        SELECT installation_row_id
          FROM source_onboarding_runs
         WHERE onboarding_run_id = $1 AND source = $2
        """,
        run_id,
        source,
    )
    await pool.execute(
        """
        INSERT INTO onboarding_shards
            (id, onboarding_run_id, tenant_id, source, shard_kind,
             shard_identifier, recency_score, state, last_error,
             installation_row_id, created_at, completed_at)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, now(),
                CASE WHEN $8 IN ('done','failed') THEN now() ELSE NULL END)
        """,
        shard_id, run_id, tenant_id, source, shard_kind,
        orjson.dumps(identifier or {"k": "v"}).decode("utf-8"),
        1.0, state, last_error, installation_row_id,
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


class _CapturingProducer:
    """Records produce() calls; flush() is a no-op."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []

    async def produce(
        self, topic: str, value: bytes, *,
        key: bytes | None = None, **_kw: Any,
    ) -> None:
        self.published.append((topic, value, key))

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        return 0


def _service_p(
    pool: asyncpg.Pool, producer: _CapturingProducer,
) -> SourceOnboarding:
    return SourceOnboarding(
        pool, kafka_producer=producer,
        config=SourceOnboardingConfig(
            tick_interval_seconds=0.01, max_signals_per_tick=20,
        ),
    )


# Test planners — updated for M6.4 / A18.6 PlannerContext signature.
from services.ingest.ingestion.planners.context import PlannerContext  # noqa: E402


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
    monkeypatch.setattr(
        source_onboarding_module,
        "resolve_planner",
        lambda _source: _test_planner_three_shards,
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


async def test_source_onboarding_uses_requested_provider_installation(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multiple Discord guild installs must plan the exact OAuth install."""
    seen_installation_ids: list[str] = []

    async def _capturing_planner(ctx: PlannerContext) -> list[Shard]:
        seen_installation_ids.append(ctx.install["installation_id"])
        return []

    async def _no_source_client(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    monkeypatch.setattr(
        source_onboarding_module,
        "resolve_planner",
        lambda _source: _capturing_planner,
    )
    monkeypatch.setattr(
        source_onboarding_module, "_build_source_client", _no_source_client,
    )

    tid = await _seed_tenant(fresh_db)
    first_install = uuid7()
    second_install = uuid7()
    await fresh_db.executemany(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, enabled)
        VALUES ($1, $2, 'discord', $3, TRUE)
        """,
        [
            (first_install, tid, "guild-first"),
            (second_install, tid, "guild-second"),
        ],
    )
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, source="discord",
    )
    await _seed_source_run(
        fresh_db,
        run_id=run_id,
        source="discord",
        tenant_id=tid,
        installation_row_id=second_install,
    )
    await _emit_source_requested(
        fresh_db, run_id=run_id, tenant_id=tid, source="discord",
        installation_row_id=second_install,
    )

    await _service(fresh_db).run(max_ticks=1)

    assert seen_installation_ids == ["guild-second"]


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
    monkeypatch.setattr(
        source_onboarding_module,
        "resolve_planner",
        lambda _source: _test_planner_three_shards,
    )

    # Patch _insert_shard to raise on the second call.
    from services.ingest.ingestion.workflows import source_onboarding as so_module
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
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inject a not-implemented planner stub for slack and verify:
      (a) source_onboarding_runs marked 'failed' with informative
          failure_reason that names M6.5.
      (b) source_onboarding_completed emitted to TenantOnboarding
          inbox with failure_reason in signal_data.
      (c) No shard rows created.

    Keep verifying the planner's raise-NotImplementedError handling.
    """
    async def _not_implemented(_ctx: PlannerContext) -> list[Shard]:
        raise NotImplementedError(
            "historical backfill planner for source 'slack' is not "
            "implemented (owned by M6.5)"
        )

    monkeypatch.setattr(
        source_onboarding_module,
        "resolve_planner",
        lambda _source: _not_implemented,
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
    assert (
        _product_workflow_events().get(
            workflow="source_onboarding",
            event="source_onboarding_failed",
            outcome="error",
        )
        == 1
    )

    # (c) NO shards created — the stub raised before any insert.
    n_shards = int(await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_shards WHERE onboarding_run_id = $1",
        run_id,
    ))
    assert n_shards == 0


# =====================================================================
# 3b. Unexpected planner exception → run failed + service keeps serving.
# =====================================================================

async def test_source_onboarding_handles_unexpected_planner_exception(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per A19: framework dispatch call sites catch Exception, not
    narrow subclasses. Verify that a planner raising RuntimeError
    (e.g., the M6.5 slack-planner-missing-source-client case that
    motivated A19) still marks the run failed and keeps the service
    serving.

    Load-bearing test for the 29b797c fix that broadened
    SourceOnboarding's planner exception handler.
    """
    async def _exploding_planner(ctx):
        raise RuntimeError("simulated planner failure")

    monkeypatch.setattr(
        source_onboarding_module,
        "resolve_planner",
        lambda _source: _exploding_planner,
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

    # Service does NOT crash — tick completes normally.
    await _service(fresh_db).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT status, failure_reason FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = $2",
        run_id, "slack",
    )
    assert row["status"] == "failed"
    failure_reason = row["failure_reason"] or ""
    assert "RuntimeError" in failure_reason, (
        f"failure_reason should contain exception type name; "
        f"got {failure_reason!r}"
    )
    assert "simulated planner failure" in failure_reason

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
    assert "RuntimeError" in data.get("failure_reason", "")


# =====================================================================
# 4. Empty planner result → immediate success.
# =====================================================================

async def test_source_onboarding_handles_empty_planner_result(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test planner returns []; assert run immediately completes
    (source has nothing to fetch — edge case)."""
    monkeypatch.setattr(
        source_onboarding_module,
        "resolve_planner",
        lambda _source: _test_planner_empty,
    )

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
# 6b. Figma failure handoff — run + connection card stay in sync.
# =====================================================================

async def test_figma_failed_shard_degrades_matching_install_with_safe_error(
    fresh_db: asyncpg.Pool,
) -> None:
    """A terminal Figma shard failure atomically degrades only its install.

    The onboarding card's error is UI-visible, so it must not retain a bearer
    token, token key/value, or email from an untrusted fetcher exception.
    """
    tenant_id = await _seed_tenant(fresh_db, "figma-failure")
    install_id = await _seed_figma_install(
        fresh_db, tenant_id=tenant_id, connection_state="pending",
    )
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tenant_id, source="figma",
    )
    await _seed_source_run(
        fresh_db, run_id=run_id, source="figma", tenant_id=tenant_id,
        status="in_progress",
    )
    shard_id = await _seed_shard(
        fresh_db,
        run_id=run_id,
        tenant_id=tenant_id,
        source="figma",
        state="in_progress",
        shard_kind="figma_file_snapshot",
        identifier={
            "installation_id": str(install_id),
            "file_key": "figma-file-1",
        },
    )
    await _emit_shard_completed(
        fresh_db,
        shard_id=shard_id,
        status="failed",
        failure_reason=(
            "Figma GET failed: Bearer secret-access-token "
            "access_token=another-secret owner=designer@example.com\nretry later"
        ),
    )

    await _service(fresh_db).run(max_ticks=1)

    run = await fresh_db.fetchrow(
        "SELECT status FROM source_onboarding_runs "
        "WHERE onboarding_run_id = $1 AND source = 'figma'",
        run_id,
    )
    install = await fresh_db.fetchrow(
        "SELECT connection_state, last_error FROM figma_installations WHERE id = $1",
        install_id,
    )
    assert run["status"] == "failed"
    assert install["connection_state"] == "degraded"
    error = install["last_error"] or ""
    assert "secret-access-token" not in error
    assert "another-secret" not in error
    assert "designer@example.com" not in error
    assert "Bearer [redacted]" in error
    assert "access_token=[redacted]" in error
    assert "[redacted-email]" in error
    assert "\n" not in error


async def test_figma_planner_failure_degrades_triggered_install(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run-level (pre-shard) Figma failure uses the trigger's exact install."""
    async def _exploding_figma_planner(ctx: PlannerContext) -> list[Shard]:
        raise RuntimeError("Figma provider unavailable")

    monkeypatch.setattr(
        source_onboarding_module,
        "resolve_planner",
        lambda _source: _exploding_figma_planner,
    )
    tenant_id = await _seed_tenant(fresh_db, "figma-planner")
    install_id = await _seed_figma_install(
        fresh_db, tenant_id=tenant_id, connection_state="pending",
    )
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tenant_id, source="figma",
    )
    await _seed_source_run(
        fresh_db, run_id=run_id, source="figma", tenant_id=tenant_id,
    )
    await _emit_source_requested(
        fresh_db,
        run_id=run_id,
        tenant_id=tenant_id,
        source="figma",
        installation_row_id=install_id,
    )

    await _service(fresh_db).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        """
        SELECT sr.status, fi.connection_state, fi.last_error
          FROM source_onboarding_runs sr
          JOIN figma_installations fi ON fi.id = $2
         WHERE sr.onboarding_run_id = $1 AND sr.source = 'figma'
        """,
        run_id,
        install_id,
    )
    assert row["status"] == "failed"
    assert row["connection_state"] == "degraded"
    assert "Figma provider unavailable" in (row["last_error"] or "")


@pytest.mark.parametrize("connection_state", ["reauthorization_required", "disconnected"])
async def test_figma_failure_preserves_stronger_connection_state(
    fresh_db: asyncpg.Pool,
    connection_state: str,
) -> None:
    """A generic shard failure never obscures reconnect/disconnect intent."""
    tenant_id = await _seed_tenant(fresh_db, f"figma-{connection_state}")
    original_error = "Keep this operator state"
    install_id = await _seed_figma_install(
        fresh_db,
        tenant_id=tenant_id,
        connection_state=connection_state,
        last_error=original_error,
    )
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tenant_id, source="figma",
    )
    await _seed_source_run(
        fresh_db, run_id=run_id, source="figma", tenant_id=tenant_id,
        status="in_progress",
    )
    shard_id = await _seed_shard(
        fresh_db,
        run_id=run_id,
        tenant_id=tenant_id,
        source="figma",
        state="in_progress",
        shard_kind="figma_file_events",
        identifier={"installation_id": str(install_id), "file_key": "figma-file-2"},
    )
    await _emit_shard_completed(
        fresh_db, shard_id=shard_id, status="failed", failure_reason="transient failure",
    )

    await _service(fresh_db).run(max_ticks=1)

    install = await fresh_db.fetchrow(
        "SELECT connection_state, last_error FROM figma_installations WHERE id = $1",
        install_id,
    )
    assert install["connection_state"] == connection_state
    assert install["last_error"] == original_error


async def test_figma_failure_cannot_update_another_tenants_install(
    fresh_db: asyncpg.Pool,
) -> None:
    """The shard's installation id is additionally constrained by run tenant."""
    tenant_id = await _seed_tenant(fresh_db, "figma-owner")
    other_tenant_id = await _seed_tenant(fresh_db, "figma-other")
    other_install_id = await _seed_figma_install(
        fresh_db,
        tenant_id=other_tenant_id,
        connection_state="connected",
    )
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tenant_id, source="figma",
    )
    await _seed_source_run(
        fresh_db, run_id=run_id, source="figma", tenant_id=tenant_id,
        status="in_progress",
    )
    shard_id = await _seed_shard(
        fresh_db,
        run_id=run_id,
        tenant_id=tenant_id,
        source="figma",
        state="in_progress",
        shard_kind="figma_file_snapshot",
        identifier={"installation_id": str(other_install_id), "file_key": "figma-file-3"},
    )
    await _emit_shard_completed(
        fresh_db, shard_id=shard_id, status="failed", failure_reason="bad shard id",
    )

    await _service(fresh_db).run(max_ticks=1)

    other_install = await fresh_db.fetchrow(
        "SELECT connection_state, last_error FROM figma_installations WHERE id = $1",
        other_install_id,
    )
    assert other_install["connection_state"] == "connected"
    assert other_install["last_error"] is None


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
    from services.ingest.ingestion.workflows.tests.test_pattern_alignment import (
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


# =====================================================================
# 10. Progress event — `source.onboarding.started`.
# =====================================================================

async def test_source_onboarding_emits_source_started_progress_event(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful plan publishes exactly one `source.onboarding.started`
    on onboarding.progress, carrying the planned shard count."""
    from services.ingest.ingestion.progress.events import (
        SourceOnboardingStarted,
    )
    from services.ingest.ingestion.progress.publisher import (
        TOPIC_ONBOARDING_PROGRESS,
    )

    monkeypatch.setattr(
        source_onboarding_module,
        "resolve_planner",
        lambda _source: _test_planner_three_shards,
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

    producer = _CapturingProducer()
    await _service_p(fresh_db, producer).run(max_ticks=1)

    started = [
        SourceOnboardingStarted.model_validate_json(val)
        for topic, val, _ in producer.published
        if topic == TOPIC_ONBOARDING_PROGRESS
        and b"source.onboarding.started" in val
    ]
    assert len(started) == 1, (
        f"Expected one source.onboarding.started; got {len(started)}. "
        f"Publishes: {producer.published}"
    )
    ev = started[0]
    assert ev.tenant_id == tid
    assert ev.source == "slack"
    assert ev.planned_shard_count == 3
    assert (
        _product_workflow_events().get(
            workflow="source_onboarding",
            event="source_onboarding_started",
            outcome="success",
        )
        == 1
    )
