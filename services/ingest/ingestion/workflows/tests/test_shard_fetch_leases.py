"""Database proofs for ShardFetch owner/version leases and durable retry."""
from __future__ import annotations

import asyncio
import datetime as dt
from unittest.mock import AsyncMock

import asyncpg
import pytest

import services.ingest.ingestion.workflows.shard_fetch as sf
from services.ingest.ingestion.fetchers import FetchResult
from services.ingest.ingestion.workflows.state import load_state
from services.ingest.ingestion.workflows.tests._fake_s3 import FakeS3Client
from services.ingest.ingestion.workflows.tests.test_shard_fetch import (
    _CapturingProducer,
    _emit_shard_requested,
    _seed_onboarding_run,
    _seed_shard,
    _seed_tenant,
)


pytestmark = [pytest.mark.timeout(60)]


def test_effective_lease_timeout_has_safe_database_round_trip_floor() -> None:
    assert sf._effective_lease_timeout_seconds(0.01) == 1.0
    assert sf._effective_lease_timeout_seconds(30.0) == 30.0
    with pytest.raises(ValueError, match="must be > 0"):
        sf._effective_lease_timeout_seconds(0)


def _service(
    pool: asyncpg.Pool,
    *,
    instance_name: str,
    lease_timeout_seconds: float,
) -> sf.ShardFetch:
    return sf.ShardFetch(
        pool,
        _CapturingProducer(),
        config=sf.ShardFetchConfig(
            tick_interval_seconds=0.01,
            max_signals_per_tick=1,
            lease_timeout_seconds=lease_timeout_seconds,
            flush_timeout_seconds=1.0,
            instance_name=instance_name,
            max_concurrent_shards=1,
            zero_progress_retry_seconds=1.0,
        ),
        s3_client=FakeS3Client(),
    )


async def test_slow_provider_call_is_heartbeated_and_cannot_be_reclaimed(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker B cannot reclaim while worker A is blocked in provider I/O."""
    tenant_id = await _seed_tenant(fresh_db, label="lease-heartbeat")
    run_id = await _seed_onboarding_run(
        fresh_db,
        tenant_id=tenant_id,
        source="slack",
    )
    shard_id = await _seed_shard(
        fresh_db,
        run_id=run_id,
        tenant_id=tenant_id,
        source="slack",
    )
    await _emit_shard_requested(
        fresh_db,
        shard_id=shard_id,
        run_id=run_id,
        tenant_id=tenant_id,
        source="slack",
    )

    # Installation selection is orthogonal to this proof. Keep the provider
    # call itself real and deliberately slower than the original lease TTL.
    monkeypatch.setattr(
        sf,
        "_load_install",
        AsyncMock(return_value={"id": "test-install"}),
    )
    provider_started = asyncio.Event()
    release_provider = asyncio.Event()

    async def _slow_fetcher(*_args: object) -> FetchResult:
        provider_started.set()
        await release_provider.wait()
        return FetchResult(records=[], next_cursor=None, end_of_data=True)

    monkeypatch.setattr(sf, "resolve_fetcher", lambda _source: _slow_fetcher)

    lease_timeout = 0.3
    worker_a = _service(
        fresh_db,
        instance_name="lease-worker-a",
        lease_timeout_seconds=lease_timeout,
    )
    worker_b = _service(
        fresh_db,
        instance_name="lease-worker-b",
        lease_timeout_seconds=lease_timeout,
    )

    task_a = asyncio.create_task(worker_a._process_one_signal())
    await asyncio.wait_for(provider_started.wait(), timeout=5.0)
    await asyncio.sleep(lease_timeout * 2.0)

    row = await fresh_db.fetchrow(
        """
        SELECT state, lease_owner, lease_version,
               lease_expires_at > now() AS lease_is_live
          FROM onboarding_shards
         WHERE id = $1
        """,
        shard_id,
    )
    assert row["state"] == "in_progress"
    assert row["lease_owner"] == "lease-worker-a"
    assert row["lease_version"] == 1
    assert row["lease_is_live"] is True
    assert await worker_b._scan_and_resume_orphans() == 0

    release_provider.set()
    assert await asyncio.wait_for(task_a, timeout=5.0) is True
    terminal = await fresh_db.fetchrow(
        """
        SELECT state, lease_owner, lease_expires_at
          FROM onboarding_shards
         WHERE id = $1
        """,
        shard_id,
    )
    assert terminal["state"] == "done"
    assert terminal["lease_owner"] is None
    assert terminal["lease_expires_at"] is None
    assert not [
        task
        for task in asyncio.all_tasks()
        if task.get_name() == f"shard-fetch-lease-{shard_id}"
        and not task.done()
    ]


async def test_old_generation_cannot_commit_after_worker_handoff(
    fresh_db: asyncpg.Pool,
) -> None:
    """After B increments lease_version, every A mutation is rejected."""
    tenant_id = await _seed_tenant(fresh_db, label="lease-fence")
    run_id = await _seed_onboarding_run(
        fresh_db,
        tenant_id=tenant_id,
        source="slack",
    )
    shard_id = await _seed_shard(
        fresh_db,
        run_id=run_id,
        tenant_id=tenant_id,
        source="slack",
    )

    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            lease_a = await sf._claim_shard_for_fetch(
                conn,
                shard_id,
                lease_owner="lease-worker-a",
                lease_timeout_seconds=30.0,
            )
            assert lease_a is not None
            await sf._persist_initial_workflow_state(conn, lease_a)
    assert await fresh_db.fetchval(
        "SELECT attempt_count FROM onboarding_shards WHERE id = $1",
        shard_id,
    ) == 1

    # Simulate A becoming partitioned long enough for its lease to expire.
    await fresh_db.execute(
        """
        UPDATE onboarding_shards
           SET lease_expires_at = now() - interval '1 second'
         WHERE id = $1
        """,
        shard_id,
    )
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            lease_b = await sf._refresh_shard_lease(
                conn,
                shard_id,
                lease_owner="lease-worker-b",
                lease_timeout_seconds=30.0,
            )
    assert lease_b is not None
    assert lease_b.version == lease_a.version + 1
    assert await fresh_db.fetchval(
        "SELECT attempt_count FROM onboarding_shards WHERE id = $1",
        shard_id,
    ) == 2

    shard = await sf._load_shard(fresh_db, shard_id)
    assert shard is not None
    ctx_a = sf._FetchLoopContext.from_shard(shard, lease=lease_a)
    producer = _CapturingProducer()
    current_state = await load_state(
        fresh_db,
        sf.WORKFLOW_KIND,
        str(shard_id),
    )
    assert current_state is not None

    # Publishing may have happened before lease validation, but the cursor
    # commit itself must be rejected. Downstream publication is idempotent.
    with pytest.raises(sf.ShardLeaseLost):
        await sf._advance_fetch_cursor(
            fresh_db,
            producer,
            sf.ShardFetchConfig(instance_name="lease-worker-a"),
            ctx_a,
            current_state=current_state,
            result=FetchResult(
                records=[],
                next_cursor={"page": 1},
                end_of_data=False,
            ),
            messages=[],
        )
    after_stale_advance = await load_state(
        fresh_db,
        sf.WORKFLOW_KIND,
        str(shard_id),
    )
    assert after_stale_advance is not None
    assert after_stale_advance.state_data["cursor"] is None

    retry_at = dt.datetime.now(tz=dt.timezone.utc) + dt.timedelta(minutes=5)
    attempts_before = await fresh_db.fetchval(
        "SELECT attempt_count FROM onboarding_shards WHERE id = $1",
        shard_id,
    )
    assert not await sf._schedule_shard_retry(
        fresh_db,
        shard_id,
        not_before=retry_at,
        reason="transient",
        detail="stale worker must not schedule",
        lease=lease_a,
    )
    assert not await sf._mark_shard_done(
        fresh_db,
        shard_id,
        lease=lease_a,
    )
    assert not await sf._mark_shard_failed(
        fresh_db,
        shard_id,
        "stale worker must not fail",
        lease=lease_a,
    )

    # The current generation can schedule exactly once. It records a
    # timezone-aware deadline/reason/error and releases ownership so no
    # process sleeps while holding the shard.
    assert await sf._schedule_shard_retry(
        fresh_db,
        shard_id,
        not_before=retry_at,
        reason="transient",
        detail="provider unavailable",
        lease=lease_b,
    )
    parked = await fresh_db.fetchrow(
        """
        SELECT state, next_attempt_at, attempt_count, retry_reason, last_error,
               lease_owner, lease_expires_at, lease_version
          FROM onboarding_shards
         WHERE id = $1
        """,
        shard_id,
    )
    assert parked["state"] == "in_progress"
    assert parked["next_attempt_at"].tzinfo is not None
    assert parked["next_attempt_at"] == retry_at
    # attempt_count advances exactly once when a worker claims a generation;
    # scheduling that generation must not double-count the same execution.
    assert parked["attempt_count"] == attempts_before
    assert parked["retry_reason"] == "transient"
    assert parked["last_error"] == "provider unavailable"
    assert parked["lease_owner"] is None
    assert parked["lease_expires_at"] is None
    assert parked["lease_version"] == lease_b.version


async def test_expired_generation_renews_only_until_it_is_superseded(
    fresh_db: asyncpg.Pool,
) -> None:
    """A delayed heartbeat may win the takeover race, never bypass fencing."""
    tenant_id = await _seed_tenant(fresh_db, label="lease-renewal-race")
    run_id = await _seed_onboarding_run(
        fresh_db,
        tenant_id=tenant_id,
        source="slack",
    )
    shard_id = await _seed_shard(
        fresh_db,
        run_id=run_id,
        tenant_id=tenant_id,
        source="slack",
    )

    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            lease_a = await sf._claim_shard_for_fetch(
                conn,
                shard_id,
                lease_owner="lease-worker-a",
                lease_timeout_seconds=30.0,
            )
    assert lease_a is not None

    # Expiry alone does not invalidate the fencing token. If no replacement
    # owner has claimed the row, the current generation can safely renew it.
    await fresh_db.execute(
        """
        UPDATE onboarding_shards
           SET lease_expires_at = now() - interval '1 second'
         WHERE id = $1
        """,
        shard_id,
    )
    assert await sf._heartbeat_shard_lease(
        fresh_db,
        shard_id,
        lease_owner=lease_a.owner,
        lease_version=lease_a.version,
        lease_timeout_seconds=30.0,
    )

    # Once another worker wins takeover, the incremented version permanently
    # fences the old generation even if it later resumes.
    await fresh_db.execute(
        """
        UPDATE onboarding_shards
           SET lease_expires_at = now() - interval '1 second'
         WHERE id = $1
        """,
        shard_id,
    )
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            lease_b = await sf._refresh_shard_lease(
                conn,
                shard_id,
                lease_owner="lease-worker-b",
                lease_timeout_seconds=30.0,
            )
    assert lease_b is not None
    assert lease_b.version == lease_a.version + 1
    assert not await sf._heartbeat_shard_lease(
        fresh_db,
        shard_id,
        lease_owner=lease_a.owner,
        lease_version=lease_a.version,
        lease_timeout_seconds=30.0,
    )


async def test_retry_deadline_must_be_timezone_aware(
    fresh_db: asyncpg.Pool,
) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        await sf._schedule_shard_retry(
            fresh_db,
            sf.UUID(int=1),
            not_before=dt.datetime(2026, 1, 1),
            reason="transient",
            detail="invalid deadline",
            lease=sf._ShardLease(
                shard_id=sf.UUID(int=1),
                owner="test",
                version=1,
            ),
        )
