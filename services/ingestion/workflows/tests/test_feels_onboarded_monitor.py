"""M6.0 Phase 2 — FeelsOnboardedMonitor tests.

The monitor is the substrate's first real consumer. These tests
validate:
  - No event when the recency threshold is not met.
  - Event published when the threshold IS met; claim-via-UPDATE
    stamps `onboarding_runs.feels_onboarded_at`.
  - No duplicate event when feels_onboarded_at is already stamped
    (the CLAIM_VIA_UPDATE invariant).
  - Per-tick state persistence in `workflow_states`.

A separate subprocess SIGTERM test lives in
test_feels_monitor_subprocess.py (split so the heavy test isn't
mandatory for fast iteration).
"""
from __future__ import annotations

import datetime as dt
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from services.ingestion.progress.events import (
    SourceOnboardingFeelsOnboarded,
)
from services.ingestion.progress.publisher import (
    TOPIC_ONBOARDING_PROGRESS,
)
from services.ingestion.workflows.feels_onboarded_monitor import (
    WORKFLOW_ID_GLOBAL,
    WORKFLOW_KIND,
    FeelsMonitorConfig,
    FeelsOnboardedMonitor,
)
from services.ingestion.workflows.state import load_state


pytestmark = [pytest.mark.timeout(60)]


_NOW = dt.datetime(2026, 5, 18, 12, 0, 0, tzinfo=dt.timezone.utc)


# =====================================================================
# Fakes.
# =====================================================================

class _CapturingProducer:
    """Records every produce() call. flush() is a no-op (the monitor
    uses claim-via-UPDATE, not the N1 cursor-advance primitive, so
    flush is not on the critical path here)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []

    async def produce(
        self, topic: str, value: bytes, *,
        key: bytes | None = None, **_kw: Any,
    ) -> None:
        self.published.append((topic, value, key))

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        return 0


# =====================================================================
# Helpers.
# =====================================================================

async def _ensure_partition(pool: asyncpg.Pool) -> None:
    from services.observations import partitions
    await partitions.ensure_partitions(
        pool, as_of=dt.date.today(), months_ahead=1,
    )


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"feels-test-{tid.hex[:8]}",
    )
    return tid


async def _seed_run(
    pool: asyncpg.Pool, *, tenant_id: UUID,
    sources: list[str], feels_onboarded_at: dt.datetime | None = None,
) -> UUID:
    rid = uuid4()
    await pool.execute(
        """
        INSERT INTO onboarding_runs
            (id, tenant_id, trigger_kind, workflow_id, status,
             sources_enabled, started_at, feels_onboarded_at)
        VALUES ($1, $2, 'install', $3, 'running', $4::text[],
                now(), $5)
        """,
        rid, tenant_id, f"wf-{rid.hex[:8]}", sources, feels_onboarded_at,
    )
    return rid


async def _seed_observation(
    pool: asyncpg.Pool,
    *, tenant_id: UUID, source_channel: str,
    occurred_at: dt.datetime | None = None,
) -> None:
    occurred_at = occurred_at or dt.datetime.now(tz=dt.timezone.utc)
    await pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, ingested_at, kind, source_channel,
            source_actor_ref, actor_id, content, content_text,
            embedding_pending, embedding, trust_tier, external_id
        ) VALUES (
            $1, $2, $3, $3, 'signal', $4,
            NULL, NULL, '{}'::jsonb, $5,
            FALSE, NULL::vector, 'T2', $6
        )
        """,
        uuid4(), tenant_id, occurred_at, source_channel,
        "feels-test", f"ext-{uuid4().hex[:12]}",
    )


# =====================================================================
# 1. Threshold not met — no event.
# =====================================================================

async def test_feels_monitor_no_event_when_threshold_not_met(
    fresh_db: asyncpg.Pool,
) -> None:
    """An active run with ZERO recent observations does NOT produce
    a feels_onboarded event. The run's feels_onboarded_at remains
    NULL."""
    await _ensure_partition(fresh_db)
    tid = await _seed_tenant(fresh_db)
    rid = await _seed_run(fresh_db, tenant_id=tid, sources=["slack"])

    producer = _CapturingProducer()
    monitor = FeelsOnboardedMonitor(
        fresh_db, producer,
        config=FeelsMonitorConfig(
            tick_interval_seconds=0.01,
            min_observations_for_feels_onboarded=5,
        ),
    )
    await monitor.run(max_ticks=1)

    assert producer.published == [], (
        f"Expected zero publishes when no observations exist; got "
        f"{len(producer.published)}."
    )
    stamped = await fresh_db.fetchval(
        "SELECT feels_onboarded_at FROM onboarding_runs WHERE id = $1",
        rid,
    )
    assert stamped is None, (
        "feels_onboarded_at was stamped despite the threshold not "
        "being met. The recency-gap check is broken."
    )


# =====================================================================
# 2. LOAD-BEARING — threshold met → event + UPDATE.
# =====================================================================

async def test_feels_monitor_emits_when_threshold_met(
    fresh_db: asyncpg.Pool,
) -> None:
    """Active run + recent observations → exactly one
    SourceOnboardingFeelsOnboarded event published on
    `onboarding.progress`; run.feels_onboarded_at stamped.
    """
    await _ensure_partition(fresh_db)
    tid = await _seed_tenant(fresh_db)
    rid = await _seed_run(fresh_db, tenant_id=tid, sources=["slack"])
    for _ in range(3):
        await _seed_observation(
            fresh_db, tenant_id=tid, source_channel="slack:channel-1",
        )

    producer = _CapturingProducer()
    monitor = FeelsOnboardedMonitor(
        fresh_db, producer,
        config=FeelsMonitorConfig(
            tick_interval_seconds=0.01,
            min_observations_for_feels_onboarded=1,
        ),
    )
    await monitor.run(max_ticks=1)

    # ----- Exactly one publish on the progress topic -----
    assert len(producer.published) == 1, (
        f"Expected exactly 1 publish; got {len(producer.published)}. "
        f"Published topics: {[t for t, _, _ in producer.published]}"
    )
    topic, value, key = producer.published[0]
    assert topic == TOPIC_ONBOARDING_PROGRESS
    assert key == tid.bytes, (
        "Partition key MUST be tenant_id.bytes for per-tenant "
        "ordering (LLD §6)."
    )

    # ----- Event payload validates against the Pydantic model -----
    event = SourceOnboardingFeelsOnboarded.model_validate_json(value)
    assert event.tenant_id == tid
    assert event.source == "slack"
    assert event.observations_count == 3
    assert event.recency_window_days == 7
    assert event.event_kind == "source.onboarding.feels_onboarded"

    # ----- Run is now stamped -----
    stamped = await fresh_db.fetchval(
        "SELECT feels_onboarded_at FROM onboarding_runs WHERE id = $1",
        rid,
    )
    assert stamped is not None, (
        "feels_onboarded_at was NOT stamped after the publish. "
        "The claim-via-UPDATE is broken."
    )


# =====================================================================
# 3. Already stamped → no duplicate event.
# =====================================================================

async def test_feels_monitor_skips_already_stamped_runs(
    fresh_db: asyncpg.Pool,
) -> None:
    """A run with feels_onboarded_at already set is NOT in the active
    scan; the monitor doesn't re-publish. This is the
    'fire-exactly-once' contract LLD §2.6 demands."""
    await _ensure_partition(fresh_db)
    tid = await _seed_tenant(fresh_db)
    rid = await _seed_run(
        fresh_db, tenant_id=tid, sources=["slack"],
        feels_onboarded_at=_NOW,
    )
    for _ in range(3):
        await _seed_observation(
            fresh_db, tenant_id=tid, source_channel="slack:channel-1",
        )

    producer = _CapturingProducer()
    monitor = FeelsOnboardedMonitor(
        fresh_db, producer,
        config=FeelsMonitorConfig(
            tick_interval_seconds=0.01,
            min_observations_for_feels_onboarded=1,
        ),
    )
    await monitor.run(max_ticks=1)

    assert producer.published == [], (
        f"Expected zero publishes for an already-stamped run; got "
        f"{len(producer.published)}. The active-runs filter "
        f"(feels_onboarded_at IS NULL) is not being honoured."
    )
    # Stamp UNCHANGED (the load-bearing claim invariant).
    stamped = await fresh_db.fetchval(
        "SELECT feels_onboarded_at FROM onboarding_runs WHERE id = $1",
        rid,
    )
    assert stamped == _NOW, (
        f"feels_onboarded_at was overwritten from {_NOW!r} to "
        f"{stamped!r}. The claim-via-UPDATE guard "
        f"`WHERE feels_onboarded_at IS NULL` is missing or broken."
    )


# =====================================================================
# 4. Concurrent monitors race-safe (claim-via-UPDATE).
# =====================================================================

async def test_feels_monitor_concurrent_ticks_publish_once(
    fresh_db: asyncpg.Pool,
) -> None:
    """Two monitor instances running tick() concurrently on the same
    run MUST produce exactly ONE publish. The claim-via-UPDATE's
    `WHERE feels_onboarded_at IS NULL` is what enforces this."""
    import asyncio

    await _ensure_partition(fresh_db)
    tid = await _seed_tenant(fresh_db)
    await _seed_run(fresh_db, tenant_id=tid, sources=["slack"])
    for _ in range(3):
        await _seed_observation(
            fresh_db, tenant_id=tid, source_channel="slack:c1",
        )

    producer_a = _CapturingProducer()
    producer_b = _CapturingProducer()
    cfg = FeelsMonitorConfig(
        tick_interval_seconds=0.01,
        min_observations_for_feels_onboarded=1,
    )
    monitor_a = FeelsOnboardedMonitor(fresh_db, producer_a, config=cfg)
    monitor_b = FeelsOnboardedMonitor(fresh_db, producer_b, config=cfg)

    await asyncio.gather(
        monitor_a.run(max_ticks=1),
        monitor_b.run(max_ticks=1),
    )

    total_published = len(producer_a.published) + len(producer_b.published)
    assert total_published == 1, (
        f"Two concurrent monitors emitted {total_published} events for "
        f"the same run; expected exactly 1. The claim-via-UPDATE "
        f"invariant is broken — both ticks won the UPDATE, which "
        f"means the WHERE feels_onboarded_at IS NULL guard isn't "
        f"actually preventing duplicates."
    )


# =====================================================================
# 5. Per-tick state persistence.
# =====================================================================

async def test_feels_monitor_persists_scan_diagnostics(
    fresh_db: asyncpg.Pool,
) -> None:
    """Each tick writes scan diagnostics to `workflow_states`. The
    last_advanced_at timestamp + state_data keys are what operators
    grep for when asking 'is the monitor making progress?'"""
    await _ensure_partition(fresh_db)
    tid = await _seed_tenant(fresh_db)
    await _seed_run(fresh_db, tenant_id=tid, sources=["slack"])

    producer = _CapturingProducer()
    monitor = FeelsOnboardedMonitor(
        fresh_db, producer,
        config=FeelsMonitorConfig(tick_interval_seconds=0.01),
    )
    await monitor.run(max_ticks=1)

    state = await load_state(
        fresh_db, WORKFLOW_KIND, WORKFLOW_ID_GLOBAL,
    )
    assert state is not None, (
        "FeelsOnboardedMonitor did NOT persist a workflow_states row "
        "after its first tick. Operators have no signal that the "
        "monitor is alive."
    )
    assert state.state_data["last_runs_scanned"] == 1
    assert state.state_data["last_events_emitted"] == 0
    assert "last_scan_at" in state.state_data
    assert state.tenant_id is None, (
        "FeelsOnboardedMonitor is global (scans all tenants); "
        "tenant_id MUST be NULL on its state row."
    )

    # Run another tick → lifetime_events_emitted accumulates.
    for _ in range(3):
        await _seed_observation(
            fresh_db, tenant_id=tid, source_channel="slack:c1",
        )
    await monitor.run(max_ticks=1)
    state2 = await load_state(
        fresh_db, WORKFLOW_KIND, WORKFLOW_ID_GLOBAL,
    )
    assert state2 is not None
    assert state2.state_data["lifetime_events_emitted"] == 1, (
        f"lifetime_events_emitted should be 1 after one event was "
        f"emitted across two ticks; got "
        f"{state2.state_data['lifetime_events_emitted']}."
    )
