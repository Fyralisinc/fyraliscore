"""M6.2a Phase 2 — ShardFetch service tests.

ShardFetch is the N1 primitive's first real production consumer. The
load-bearing test is `test_shard_fetch_N1_invariant_holds` — same
shape as M6.0 Phase 1's `test_advance_cursor_atomic_publishes_before_persists`
but at the service-integration level.

Covers:
  - Happy path: fetch loop runs through pages, shard 'done',
    shard_fetch_completed emitted.
  - **LOAD-BEARING: N1 invariant holds.** Flush failure on page 2 →
    cursor reflects post-page-1, NOT post-page-2. Shard stays
    in_progress; next tick re-attempts.
  - NotImplementedError stub fetcher → shard 'failed' with the
    stub message naming the responsible M6.x sub-block.
  - Signal replay idempotency — duplicate emit doesn't double-fetch.
  - Already-completed shard → idempotent re-emit of completion
    (the M6.2a SourceOnboarding consumer relies on this for cross-
    replica recovery).
  - Pattern-alignment analyzer.

The subprocess SIGTERM + resume-from-persisted-cursor tests live in
test_shard_fetch_subprocess.py.

A15 column-naming applies throughout: tests write/read
`id`/`state`/`shard_identifier`/`shard_kind`/`last_error` on
`onboarding_shards`. Cursor lives in `workflow_states.state_data`,
keyed by `(workflow_kind="shard_fetch", workflow_id=str(shard_id))`.
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from lib.shared.ids import uuid7
from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.workflows.shard_fetch import (
    RAW_TOPIC,
    SIGNAL_KIND_COMPLETED,
    SIGNAL_KIND_REQUESTED,
    SOURCE_ONBOARDING_INBOX_ID,
    SOURCE_ONBOARDING_INBOX_KIND,
    ShardFetch,
    ShardFetchConfig,
    WORKFLOW_ID_INBOX,
    WORKFLOW_KIND,
)
from services.ingestion.workflows.signals import emit_signal
from services.ingestion.workflows.state import load_state


pytestmark = [pytest.mark.timeout(60)]


# =====================================================================
# Fake producer with per-call flush failure injection.
# =====================================================================
class _CapturingProducer:
    """IdempotentProducer stand-in.

    `produce` captures call. `flush` consumes from `flush_returns`
    list — each element is the queue-remaining count to return on
    that flush call. Default empty list → return 0 (happy path).
    """

    def __init__(self, flush_returns: list[int] | None = None) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []
        self.flush_calls = 0
        self._flush_returns = list(flush_returns or [])

    async def produce(
        self, topic: str, value: bytes, *,
        key: bytes | None = None, **_kw: Any,
    ) -> None:
        self.published.append((topic, value, key))

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        self.flush_calls += 1
        if self._flush_returns:
            return self._flush_returns.pop(0)
        return 0


# =====================================================================
# Helpers — same shape as M6.1 + Phase 1.
# =====================================================================
async def _seed_tenant(pool: asyncpg.Pool, label: str = "shf") -> UUID:
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


async def _seed_shard(
    pool: asyncpg.Pool, *, run_id: UUID, tenant_id: UUID, source: str,
    state: str = "pending", shard_kind: str = "slack_channel_window",
    identifier: dict | None = None,
) -> UUID:
    shard_id = uuid7()
    await pool.execute(
        """
        INSERT INTO onboarding_shards
            (id, onboarding_run_id, tenant_id, source, shard_kind,
             shard_identifier, recency_score, state, created_at)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, now())
        """,
        shard_id, run_id, tenant_id, source, shard_kind,
        orjson.dumps(identifier or {"channel_id": "C001"}).decode("utf-8"),
        1.0, state,
    )
    return shard_id


async def _emit_shard_requested(
    pool: asyncpg.Pool, *, shard_id: UUID, run_id: UUID,
    tenant_id: UUID, source: str,
) -> None:
    await emit_signal(
        pool,
        workflow_kind=WORKFLOW_KIND,
        workflow_id=WORKFLOW_ID_INBOX,
        signal_kind=SIGNAL_KIND_REQUESTED,
        idempotency_key=str(shard_id),
        signal_data={
            "shard_id": str(shard_id),
            "onboarding_run_id": str(run_id),
            "tenant_id": str(tenant_id),
            "source": source,
        },
    )


def _service(
    pool: asyncpg.Pool, producer: _CapturingProducer,
    *, lease_timeout: float = 30.0,
) -> ShardFetch:
    return ShardFetch(
        pool, producer,
        config=ShardFetchConfig(
            tick_interval_seconds=0.01,
            max_signals_per_tick=20,
            lease_timeout_seconds=lease_timeout,
            flush_timeout_seconds=1.0,
        ),
    )


def _make_three_page_fetcher(
    pages: list[list[dict]],
) -> Any:
    """Build a test fetcher that returns one page per call until
    pages exhausted, then end_of_data=True with empty records.

    `pages` is a list-of-lists of record dicts. Each call returns
    one outer-list element.

    The next_cursor advances `{"page": N}` where N is the page index
    just emitted (so after page 0 returns, cursor is `{"page": 0}`;
    next call sees that cursor and returns page 1; etc.).
    """
    state = {"call": 0}

    async def _fetcher(
        install: asyncpg.Record,
        shard_identifier: dict[str, Any],
        cursor: dict[str, Any] | None,
    ) -> FetchResult:
        i = state["call"]
        state["call"] = i + 1
        if i >= len(pages):
            return FetchResult(records=[], next_cursor=None, end_of_data=True)
        records = pages[i]
        is_last = (i == len(pages) - 1)
        return FetchResult(
            records=records,
            next_cursor={"page": i},
            end_of_data=is_last,
        )

    _fetcher._call_state = state  # type: ignore[attr-defined]
    return _fetcher


# =====================================================================
# 1. Happy path — fetcher returns pages, loop runs to end, shard done.
# =====================================================================

async def test_shard_fetch_picks_up_request_and_calls_fetcher(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Emit shard_fetch_requested. Tick service with test fetcher
    returning 2 pages then end_of_data. Assert:
      (a) shard marked 'done'.
      (b) Kafka records published (one per record across pages).
      (c) workflow_states cursor advanced.
      (d) shard_fetch_completed signal emitted to SourceOnboarding.
    """
    pages = [
        [{"id": 1}, {"id": 2}],
        [{"id": 3}, {"id": 4}, {"id": 5}],
    ]
    fetcher = _make_three_page_fetcher(pages)
    monkeypatch.setitem(FETCHER_DISPATCH, "slack", fetcher)

    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    shard_id = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )
    await _emit_shard_requested(
        fresh_db, shard_id=shard_id, run_id=run_id,
        tenant_id=tid, source="slack",
    )

    producer = _CapturingProducer()
    await _service(fresh_db, producer).run(max_ticks=1)

    # (a) Shard done.
    state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
    )
    assert state == "done"

    # (b) Kafka records: 5 records across 2 pages.
    assert len(producer.published) == 5
    for topic, _val, key in producer.published:
        assert topic == RAW_TOPIC
        assert key == str(tid).encode("utf-8")

    # (c) workflow_states cursor reflects last advance.
    ws = await load_state(fresh_db, WORKFLOW_KIND, str(shard_id))
    assert ws is not None
    # After the final page (page 1), next_cursor was {"page": 1} and
    # end_of_data was True; the N1 advance persists that state.
    assert ws.state_data.get("cursor") == {"page": 1}
    assert ws.state_data.get("end_of_data") is True
    assert ws.state_data.get("pages_fetched") == 2

    # (d) shard_fetch_completed emitted to SourceOnboarding inbox.
    completion = await fresh_db.fetchrow(
        "SELECT signal_data FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        SOURCE_ONBOARDING_INBOX_KIND, SOURCE_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_COMPLETED, str(shard_id),
    )
    assert completion is not None
    data_raw = completion["signal_data"]
    data = (
        orjson.loads(data_raw) if isinstance(data_raw, (str, bytes))
        else dict(data_raw)
    )
    assert data["status"] == "done"
    assert data["shard_id"] == str(shard_id)


# =====================================================================
# 2. LOAD-BEARING — N1 invariant under flush failure mid-loop.
# =====================================================================

async def test_shard_fetch_N1_invariant_holds(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The architecturally most important assertion of M6.2a.

    Inject a Kafka flush failure on page 2 (the 2nd advance call).
    Assert:
      (a) Page-1 records WERE published (produce calls observed).
      (b) workflow_states.state_data reflects the POST-PAGE-1
          cursor — `{"page": 0}` — NOT post-page-2.
      (c) Page-2 records WERE enqueued via produce() (the N1 primitive
          publishes before flushing), but the cursor advance for
          page 2 did NOT happen.
      (d) shard.state remains 'in_progress' (loop exited; shard
          claimable for resume).
      (e) shard_fetch_completed NOT emitted — the parent
          SourceOnboarding hasn't seen completion yet.

    The N1 invariant — "publish-then-advance, never advance-then-
    publish" — holds: a successful flush is the precondition for a
    cursor advance. Page-2's flush failed → page-2's cursor advance
    did NOT happen → shard stays claimable. The next attempt sees
    cursor=`{"page": 0}` and the fetcher is called again with that
    cursor — re-fetching page 2 from the source. The Kafka idempotent
    producer dedups the broker side; the downstream UNIQUE
    constraint dedups the writer side.

    Same shape as M6.0 Phase 1's
    `test_advance_cursor_atomic_publishes_before_persists`, lifted
    to the service-integration level.
    """
    pages = [
        [{"id": 1}, {"id": 2}],          # page 0 — flushes cleanly
        [{"id": 3}, {"id": 4}, {"id": 5}],  # page 1 — flush fails
        [{"id": 6}],                      # page 2 — never reached
    ]
    fetcher = _make_three_page_fetcher(pages)
    monkeypatch.setitem(FETCHER_DISPATCH, "github", fetcher)

    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="github")
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, source="github",
    )
    shard_id = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="github",
        shard_kind="github_repo_events",
        identifier={"repo": "owner/name"},
    )
    await _emit_shard_requested(
        fresh_db, shard_id=shard_id, run_id=run_id,
        tenant_id=tid, source="github",
    )

    # Flush returns: 0 (success) then 3 (3 messages still queued —
    # failure) for the second flush.
    producer = _CapturingProducer(flush_returns=[0, 3])
    await _service(fresh_db, producer).run(max_ticks=1)

    # (a) Page-1 records (2) + page-2 records (3) all published via
    # produce(). The N1 primitive enqueues BEFORE flushing.
    assert len(producer.published) == 5

    # (b) Cursor reflects post-page-1 (page index 0), NOT post-page-2
    # (page index 1). This is THE load-bearing assertion.
    ws = await load_state(fresh_db, WORKFLOW_KIND, str(shard_id))
    assert ws is not None, (
        "workflow_states row missing — bootstrap didn't happen."
    )
    assert ws.state_data.get("cursor") == {"page": 0}, (
        f"N1 INVARIANT BROKEN: cursor advanced past page 1 despite "
        f"page-2 flush failure. Got cursor={ws.state_data.get('cursor')!r}. "
        f"Expected {{'page': 0}} — the post-page-1 cursor. The "
        f"advance_cursor_atomic_with_kafka_publish primitive is "
        f"updating state BEFORE the flush succeeds; surface as a "
        f"substrate finding (same shape as A12/A13/A15)."
    )

    # The 'pages_fetched' counter should also reflect only the
    # successful advance (1, not 2).
    assert ws.state_data.get("pages_fetched") == 1, (
        f"pages_fetched={ws.state_data.get('pages_fetched')!r} but "
        f"only 1 page was successfully advanced; counter is "
        f"out of sync with cursor truth."
    )

    # (c) Both flush calls observed.
    assert producer.flush_calls == 2

    # (d) Shard stays 'in_progress' — loop exited without marking done.
    state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
    )
    assert state == "in_progress", (
        f"Shard state={state!r}; expected 'in_progress' after flush "
        f"failure mid-loop. The N1 primitive correctly raised "
        f"CursorAdvanceFlushFailure; the service must not transition "
        f"the shard to done/failed when the loop exits this way."
    )

    # (e) shard_fetch_completed NOT emitted.
    n_completions = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND workflow_id = $2 "
        "AND signal_kind = $3 AND idempotency_key = $4",
        SOURCE_ONBOARDING_INBOX_KIND, SOURCE_ONBOARDING_INBOX_ID,
        SIGNAL_KIND_COMPLETED, str(shard_id),
    ))
    assert n_completions == 0, (
        f"Shard completion signal emitted ({n_completions}) despite "
        f"mid-loop flush failure. SourceOnboarding would receive a "
        f"premature 'done' and roll up the parent run incorrectly."
    )


# =====================================================================
# 3. NotImplementedError stub fetcher → shard 'failed' with M6.x ref.
# =====================================================================

async def test_shard_fetch_handles_not_implemented_fetcher(
    fresh_db: asyncpg.Pool,
) -> None:
    """Default dispatch table: source='slack' raises NotImplementedError.
    Assert:
      (a) shard.state == 'failed'.
      (b) last_error names M6.5 (slack's responsible sub-block).
      (c) shard_fetch_completed emitted with status='failed' and
          failure_reason set.
    """
    # No monkeypatch: use the real stub.
    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    shard_id = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )
    await _emit_shard_requested(
        fresh_db, shard_id=shard_id, run_id=run_id,
        tenant_id=tid, source="slack",
    )

    producer = _CapturingProducer()
    await _service(fresh_db, producer).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT state, last_error FROM onboarding_shards WHERE id = $1",
        shard_id,
    )
    assert row["state"] == "failed"
    assert "M6.5" in (row["last_error"] or ""), (
        f"last_error should name M6.5 (slack's fetcher sub-block); "
        f"got {row['last_error']!r}"
    )

    completion = await fresh_db.fetchrow(
        "SELECT signal_data FROM workflow_signals "
        "WHERE signal_kind = $1 AND idempotency_key = $2",
        SIGNAL_KIND_COMPLETED, str(shard_id),
    )
    assert completion is not None
    data_raw = completion["signal_data"]
    data = (
        orjson.loads(data_raw) if isinstance(data_raw, (str, bytes))
        else dict(data_raw)
    )
    assert data["status"] == "failed"
    assert "M6.5" in data.get("failure_reason", "")


# =====================================================================
# 4. Signal-replay idempotency (emit_signal UNIQUE constraint).
# =====================================================================

async def test_shard_fetch_idempotent_on_signal_replay(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Emit the same shard_fetch_requested twice (same
    idempotency_key=shard_id). The second emit returns
    was_new=False; only one signal row exists. The service consumes
    it once."""
    pages = [[{"id": 1}]]
    fetcher = _make_three_page_fetcher(pages)
    monkeypatch.setitem(FETCHER_DISPATCH, "discord", fetcher)

    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="discord")
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, source="discord",
    )
    shard_id = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="discord",
        shard_kind="discord_channel_window",
    )

    await _emit_shard_requested(
        fresh_db, shard_id=shard_id, run_id=run_id,
        tenant_id=tid, source="discord",
    )
    # Second emit — same idempotency_key.
    await _emit_shard_requested(
        fresh_db, shard_id=shard_id, run_id=run_id,
        tenant_id=tid, source="discord",
    )

    n_signals = int(await fresh_db.fetchval(
        "SELECT count(*) FROM workflow_signals "
        "WHERE workflow_kind = $1 AND idempotency_key = $2 "
        "AND signal_kind = $3",
        WORKFLOW_KIND, str(shard_id), SIGNAL_KIND_REQUESTED,
    ))
    assert n_signals == 1, (
        f"emit_signal idempotency UNIQUE constraint failed: "
        f"{n_signals} rows for the same idempotency_key."
    )

    producer = _CapturingProducer()
    await _service(fresh_db, producer).run(max_ticks=1)

    # Fetcher called once.
    assert fetcher._call_state["call"] == 1


# =====================================================================
# 5. Already-completed shard → idempotent re-emit of completion.
# =====================================================================

async def test_shard_fetch_skips_already_completed_shard(
    fresh_db: asyncpg.Pool,
) -> None:
    """Pre-seed shard with state='done'. Emit shard_fetch_requested.
    Assert:
      (a) No fetcher calls made.
      (b) shard stays 'done'.
      (c) shard_fetch_completed is re-emitted (idempotent at the
          emit_signal layer; the SourceOnboarding consumer sees one
          signal regardless of replay count).
    """
    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="slack")
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    shard_id = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
        state="done",
    )
    await _emit_shard_requested(
        fresh_db, shard_id=shard_id, run_id=run_id,
        tenant_id=tid, source="slack",
    )

    producer = _CapturingProducer()
    await _service(fresh_db, producer).run(max_ticks=1)

    # No produce/flush calls — fetcher not called.
    assert len(producer.published) == 0
    assert producer.flush_calls == 0

    # Shard stays done.
    state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
    )
    assert state == "done"

    # Completion emitted (idempotent re-emit for the consumer).
    completion = await fresh_db.fetchrow(
        "SELECT signal_data FROM workflow_signals "
        "WHERE signal_kind = $1 AND idempotency_key = $2",
        SIGNAL_KIND_COMPLETED, str(shard_id),
    )
    assert completion is not None


# =====================================================================
# 6. Missing install at fetch time → shard 'failed' before fetcher.
# =====================================================================

async def test_shard_fetch_handles_missing_install(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Install row was deleted between SourceOnboarding's shard
    creation and ShardFetch's pickup (A14 race). Assert shard marked
    'failed' before any fetcher dispatch attempt — the per-install
    fetcher call would otherwise NPE on `None`.

    Test fetcher is monkeypatched to raise if called — proof we
    bailed out BEFORE the dispatch."""

    async def _should_not_be_called(*args, **kwargs):
        raise AssertionError("Fetcher should not have been called.")
    monkeypatch.setitem(FETCHER_DISPATCH, "slack", _should_not_be_called)

    tid = await _seed_tenant(fresh_db)
    # No provider_install seeded.
    run_id = await _seed_onboarding_run(fresh_db, tenant_id=tid)
    shard_id = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="slack",
    )
    await _emit_shard_requested(
        fresh_db, shard_id=shard_id, run_id=run_id,
        tenant_id=tid, source="slack",
    )

    producer = _CapturingProducer()
    await _service(fresh_db, producer).run(max_ticks=1)

    row = await fresh_db.fetchrow(
        "SELECT state, last_error FROM onboarding_shards WHERE id = $1",
        shard_id,
    )
    assert row["state"] == "failed"
    assert "No active install" in (row["last_error"] or "")


# =====================================================================
# 7. Orphan resume — in-progress shard with stale lease.
# =====================================================================

async def test_shard_fetch_resumes_orphan_with_stale_lease(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-seed a shard already in state='in_progress' with no
    workflow_states row (lease never bootstrapped — equivalent to
    "claim crashed before first advance"). The orphan scan should
    pick it up and run the fetch loop.

    This is the in-process precursor to the subprocess
    resume-from-cursor test."""
    pages = [[{"id": 1}], [{"id": 2}]]
    fetcher = _make_three_page_fetcher(pages)
    monkeypatch.setitem(FETCHER_DISPATCH, "github", fetcher)

    tid = await _seed_tenant(fresh_db)
    await _seed_provider_install(fresh_db, tenant_id=tid, provider="github")
    run_id = await _seed_onboarding_run(
        fresh_db, tenant_id=tid, source="github",
    )
    shard_id = await _seed_shard(
        fresh_db, run_id=run_id, tenant_id=tid, source="github",
        state="in_progress", shard_kind="github_repo_events",
    )
    # No workflow_states row. The orphan scan finds this shard
    # because LEFT JOIN shows ws.last_advanced_at IS NULL.

    # No signal emitted — orphan scan path drives this.
    producer = _CapturingProducer()
    # lease_timeout=0.01 so the scan considers the shard stale
    # immediately.
    await _service(
        fresh_db, producer, lease_timeout=0.01,
    ).run(max_ticks=1)

    # Shard should be 'done' (fetched 2 pages).
    state = await fresh_db.fetchval(
        "SELECT state FROM onboarding_shards WHERE id = $1", shard_id,
    )
    assert state == "done", (
        f"Orphan shard not resumed: state={state!r}. The orphan-scan "
        f"path is broken."
    )


# =====================================================================
# 8. Pattern-alignment analyzer accepts shard_fetch.py.
# =====================================================================

def test_shard_fetch_passes_pattern_alignment_analyzer() -> None:
    """The M6.0 static analyzer must accept shard_fetch.py."""
    from services.ingestion.workflows.tests.test_pattern_alignment import (
        WORKFLOWS_DIR,
        _all_rules,
    )

    path = WORKFLOWS_DIR / "shard_fetch.py"
    assert path.exists()
    violations = _all_rules(path)
    if violations:
        formatted = "\n".join(str(v) for v in violations)
        raise AssertionError(
            f"shard_fetch.py violates M6 pattern-alignment rules:\n"
            f"{formatted}\n\n"
            f"See docs/ingestion/pattern-alignment-rules.md."
        )
