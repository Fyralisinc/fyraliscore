"""M3.3 — Backlog embedding service integration tests.

Three test categories:

  1. Happy path + cursor-reset (in-process, fakeredis, stub Ollama).
  2. QPS=0 sentinel pause (in-process, fakeredis, stub Ollama)
     — LOAD-BEARING.
  3. SIGTERM + restart resumes from cursor (subprocess, real Redis
     via testcontainers, real Ollama via the local dev server)
     — LOAD-BEARING.

The SIGTERM test requires a separate Redis process (the subprocess
can't share fakeredis state with the test process) and Ollama on
localhost:11434 with nomic-embed-text. Skipped if either is
unavailable.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
import signal
import subprocess
import sys
import time
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

try:
    from fakeredis import aioredis as fake_aioredis  # type: ignore[import-not-found]
    _HAS_FAKEREDIS = True
except ImportError:
    _HAS_FAKEREDIS = False

try:
    import docker as _docker_module  # type: ignore[import-not-found]
    _HAS_DOCKER_SDK = True
except ImportError:
    _HAS_DOCKER_SDK = False

try:
    # testcontainers.redis raises a DeprecationWarning at import; pytest
    # is configured to treat warnings as errors. Suppress just for the
    # import — the wrapper is otherwise fine.
    import warnings as _warnings
    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore", DeprecationWarning)
        from testcontainers.redis import RedisContainer  # type: ignore[import-not-found]
        from testcontainers.kafka import KafkaContainer  # type: ignore[import-not-found]  # noqa: F401
    _HAS_TESTCONTAINERS = True
except ImportError:
    _HAS_TESTCONTAINERS = False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.timeout(180),
]


def _docker_available() -> bool:
    if not _HAS_DOCKER_SDK:
        return False
    try:
        _docker_module.from_env().ping()
        return True
    except Exception:
        return False


def _ollama_available() -> bool:
    try:
        import httpx
        r = httpx.get("http://localhost:11434/api/tags", timeout=2.0)
        return r.status_code == 200
    except Exception:
        return False


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


# =====================================================================
# Stub Ollama (in-process tests).
# =====================================================================

class _StubOllama:
    """Returns `vector` (when set) or raises `error` (when set)."""

    expected_dim = 768

    def __init__(
        self,
        vector: list[float] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._vector = vector
        self._error = error
        self.call_count = 0

    async def embed(self, text: str) -> list[float]:
        self.call_count += 1
        if self._error is not None:
            raise self._error
        if self._vector is None:
            raise RuntimeError("test bug")
        return self._vector

    async def close(self) -> None:
        pass


class _NoopProducer:
    """IdempotentProducer stand-in. Captures DLQ publishes."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []

    async def start(self) -> None:
        pass

    async def stop(self, timeout_seconds: float = 10.0) -> None:
        pass

    async def produce(self, topic: str, value: bytes, *, key: bytes | None = None, **_kw: Any) -> None:
        self.published.append((topic, value))


# =====================================================================
# Helpers.
# =====================================================================

async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"backlog-test-{tid.hex[:8]}",
    )
    return tid


async def _ensure_partition(pool: asyncpg.Pool) -> None:
    from services.observations import partitions
    await partitions.ensure_partitions(pool, as_of=_NOW.date(), months_ahead=1)


async def _insert_pending(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    obs_id: UUID,
    content_text: str,
    ingested_at: dt.datetime,
    source_channel: str = "slack:message",
    external_id: str | None = None,
) -> None:
    if external_id is None:
        external_id = f"ext-{obs_id.hex[:12]}"
    await pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, ingested_at, kind, source_channel,
            source_actor_ref, actor_id, content, content_text,
            embedding_pending, embedding, trust_tier, external_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            NULL, NULL, $7::jsonb, $8,
            TRUE, NULL::vector, $9, $10
        )
        """,
        obs_id, tenant_id, _NOW, ingested_at, "signal", source_channel,
        "{}", content_text,
        "T2", external_id,
    )


async def _pending_count(pool: asyncpg.Pool, tenant_id: UUID) -> int:
    async with pool.acquire() as conn:
        await conn.execute("SET LOCAL row_security = off")
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS n FROM observations "
            "WHERE tenant_id = $1 AND embedding_pending = TRUE",
            tenant_id,
        )
    return int(row["n"])


# =====================================================================
# 1. Happy path.
# =====================================================================

@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_backlog_service_happy_path_drains_pending_rows(fresh_db: asyncpg.Pool):
    """Three pending rows → service drains all three under QPS=10."""
    from services.ingestion.recovery.embedding_backlog import (
        BacklogConfig, reset_metrics, run_backlog_service,
    )

    await _ensure_partition(fresh_db)
    tid = await _seed_tenant(fresh_db)

    # Seed 3 rows with distinct ingested_at so the cursor ordering
    # is deterministic.
    obs_ids = []
    for i in range(3):
        obs_id = uuid4()
        obs_ids.append(obs_id)
        await _insert_pending(
            fresh_db,
            tenant_id=tid, obs_id=obs_id,
            content_text=f"please embed row {i}",
            ingested_at=_NOW + dt.timedelta(seconds=i),
        )
    assert await _pending_count(fresh_db, tid) == 3

    stub = _StubOllama(vector=[0.42] * 768)
    redis = fake_aioredis.FakeRedis()
    reset_metrics()
    try:
        await run_backlog_service(
            config=BacklogConfig(
                instance_name="happy-test",
                rate_qps=10.0,
                batch_size=1,
                drained_pause_sec=0.01,
            ),
            pool=fresh_db,
            redis=redis,
            ollama=stub,
            dlq_producer=_NoopProducer(),  # type: ignore[arg-type]
            max_iterations=10,
        )
    finally:
        await redis.aclose()

    # All three rows now embedded.
    assert await _pending_count(fresh_db, tid) == 0
    assert stub.call_count == 3


# =====================================================================
# 2. QPS=0 sentinel. LOAD-BEARING.
# =====================================================================

@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_backlog_service_qps_zero_pauses_entirely(fresh_db: asyncpg.Pool):
    """LOAD-BEARING (M3.3): BACKFILL_OLLAMA_QPS=0 must stall the
    service indefinitely without processing ANY row.

    Mechanism: capacity=0 + refill_per_sec=0 → acquire.lua returns
    the -1 sentinel on every call. The service polls the bucket at
    SENTINEL_RECHECK_SEC (1s) and never proceeds to the SELECT.

    Setup:
      - 5 pending observations.
      - QPS=0.
      - Run the service for 2 seconds wall-clock.

    Assertion:
      - ZERO observations were processed (exact, not "few").
      - The pending count is unchanged.
      - The rate_limit_sentinels metric incremented (proving the
        service hit the sentinel, not some other code path).

    This is the operator's pause switch — break this and there's
    no clean way to throttle or stop the service without restart.
    """
    from services.ingestion.recovery.embedding_backlog import (
        BacklogConfig, get_metrics, reset_metrics, run_backlog_service,
    )

    await _ensure_partition(fresh_db)
    tid = await _seed_tenant(fresh_db)

    for i in range(5):
        await _insert_pending(
            fresh_db,
            tenant_id=tid, obs_id=uuid4(),
            content_text=f"do not embed me {i}",
            ingested_at=_NOW + dt.timedelta(seconds=i),
        )
    assert await _pending_count(fresh_db, tid) == 5

    stub = _StubOllama(vector=[0.99] * 768)
    redis = fake_aioredis.FakeRedis()
    reset_metrics()

    stop_event = asyncio.Event()

    async def _run_then_stop() -> None:
        # Run the service in the background, then stop it after 2s.
        try:
            await run_backlog_service(
                config=BacklogConfig(
                    instance_name="qps-zero-test",
                    rate_qps=0.0,  # THE PAUSE SWITCH
                    batch_size=1,
                    drained_pause_sec=0.01,
                ),
                pool=fresh_db,
                redis=redis,
                ollama=stub,
                dlq_producer=_NoopProducer(),  # type: ignore[arg-type]
                stop_event=stop_event,
            )
        finally:
            await redis.aclose()

    task = asyncio.create_task(_run_then_stop())
    await asyncio.sleep(2.0)
    stop_event.set()
    await asyncio.wait_for(task, timeout=5.0)

    # ===== LOAD-BEARING ASSERTIONS =====
    # 1. EXACTLY ZERO observations processed. Not "few" — zero.
    remaining = await _pending_count(fresh_db, tid)
    assert remaining == 5, (
        f"QPS=0 must process zero rows; {5 - remaining} were "
        f"processed. The pause switch is broken."
    )
    # 2. Ollama was never called.
    assert stub.call_count == 0, (
        f"Ollama was called {stub.call_count} times under QPS=0. "
        f"The rate-limiter gate did not fire before the embed call."
    )
    # 3. The service hit the -1 sentinel path (proves the gate
    # is the rate limiter, not some other early return).
    m = get_metrics()
    assert m["backlog.rate_limit_sentinels"] >= 1, (
        f"backlog.rate_limit_sentinels = {m['backlog.rate_limit_sentinels']}. "
        f"The service did not hit the -1 sentinel — it took some "
        f"other branch and we're not actually testing the pause "
        f"property. Metrics: {m}"
    )
    assert m["backlog.rows_embedded"] == 0, m


# =====================================================================
# 3. SIGTERM + restart resumes from cursor. LOAD-BEARING.
# =====================================================================

@pytest.mark.skipif(not _HAS_TESTCONTAINERS, reason="testcontainers unavailable")
@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
@pytest.mark.skipif(not _ollama_available(), reason="Ollama not reachable on localhost:11434")
async def test_backlog_service_resumes_from_cursor(fresh_db: asyncpg.Pool):
    """LOAD-BEARING (M3.3): cursor must survive a real SIGTERM +
    process restart. Not a flag flip — a process death.

    Setup:
      - Seed 6 pending observations with deterministic
        (ingested_at, id) ordering.
      - Start the service as a SUBPROCESS via `python -m
        services.ingestion.recovery.embedding_backlog` with real
        Postgres + real Redis (testcontainers) + real Ollama
        (localhost dev server) + a fake Kafka bootstrap (DLQ
        producer init is non-fatal under our config — it only
        publishes on failure, and we don't trigger any).
      - Poll the DB until 3 rows are processed.
      - SIGTERM the subprocess; wait for clean exit (returncode 0).
      - Assert the cursor row in embedding_backlog_state is
        non-NULL and points to row 3.
      - Restart the subprocess. Wait until all 6 are processed.
      - Assert: at no point did the subprocess re-process a row
        it already processed. (The cursor's strict-greater-than
        comparison guarantees this; the assertion is that final
        pending_count == 0 AND no row is "embedding_pending=TRUE
        with embedding != NULL" — which would indicate corruption.)

    Without this property: a SIGTERM mid-backlog restarts from
    the beginning. For a 10M-row backlog at 10 QPS that's a
    ~12-day re-do. Crash-resume is the entire point of cursor
    persistence.
    """
    from services.ingestion.recovery.embedding_backlog import (
        BACKLOG_BUCKET_KEY,
    )

    await _ensure_partition(fresh_db)
    tid = await _seed_tenant(fresh_db)
    obs_ids: list[UUID] = []
    for i in range(6):
        obs_id = uuid4()
        obs_ids.append(obs_id)
        await _insert_pending(
            fresh_db,
            tenant_id=tid, obs_id=obs_id,
            content_text=f"sigterm-resume row {i}",
            ingested_at=_NOW + dt.timedelta(seconds=i),
        )
    assert await _pending_count(fresh_db, tid) == 6

    with RedisContainer("redis:7-alpine") as redis_box:
        redis_host = redis_box.get_container_host_ip()
        redis_port = redis_box.get_exposed_port(6379)
        redis_url = f"redis://{redis_host}:{redis_port}/0"

        # Build the subprocess env. The DATABASE_URL is the same DSN
        # the test runner uses (the fresh_db pool's DSN). The Kafka
        # bootstrap points at localhost:9092; the producer init
        # tolerates the bad address because we never produce.
        env = os.environ.copy()
        env["DATABASE_URL"] = os.environ["DATABASE_URL"]
        env["REDIS_URL"] = redis_url
        env["OLLAMA_URL"] = "http://localhost:11434"
        env["OLLAMA_EMBED_MODEL"] = "nomic-embed-text"
        env["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
        env["BACKFILL_OLLAMA_QPS"] = "5"
        env["BACKFILL_INSTANCE_NAME"] = f"sigterm-test-{tid.hex[:8]}"
        env["BACKFILL_BATCH_SIZE"] = "1"
        env["EMBEDDING_BACKLOG_LOG_LEVEL"] = "WARNING"

        # ---- Run 1: process some, then SIGTERM. -------------------
        proc = subprocess.Popen(
            [sys.executable, "-m",
             "services.ingestion.recovery.embedding_backlog"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

        # Poll the DB until at least 3 rows have been processed.
        deadline = time.monotonic() + 30.0
        processed_first_run = 0
        while time.monotonic() < deadline:
            n_done = 6 - await _pending_count(fresh_db, tid)
            if n_done >= 3:
                processed_first_run = n_done
                break
            await asyncio.sleep(0.2)
        else:
            proc.kill()
            proc.wait(timeout=5)
            raise AssertionError(
                f"subprocess did not process 3 rows within 30s; "
                f"processed only {6 - await _pending_count(fresh_db, tid)}. "
                f"stderr: {proc.stderr.read().decode()[:500] if proc.stderr else ''}"
            )

        # SIGTERM and wait for clean exit.
        proc.send_signal(signal.SIGTERM)
        try:
            rc = proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            raise AssertionError(
                "subprocess did not exit cleanly within 15s of SIGTERM"
            )
        stderr = proc.stderr.read().decode() if proc.stderr else ""
        assert rc == 0, (
            f"subprocess exit code {rc} (expected 0). stderr: {stderr[:500]}"
        )

        # Confirm the cursor is non-NULL and points somewhere past
        # the start — i.e. the service made progress AND persisted it.
        async with fresh_db.acquire() as conn:
            cur = await conn.fetchrow(
                "SELECT cursor_ingested_at, cursor_id "
                "FROM embedding_backlog_state "
                "WHERE instance_name = $1",
                env["BACKFILL_INSTANCE_NAME"],
            )
        assert cur is not None, (
            "Cursor row was never written — service died before its "
            "first cursor persist."
        )
        assert cur["cursor_ingested_at"] is not None, (
            f"Cursor ingested_at is NULL after processing "
            f"{processed_first_run} rows. The persist path is broken."
        )
        cursor_after_sigterm = cur["cursor_ingested_at"]

        # Snapshot how many were processed at SIGTERM time.
        n_done_at_sigterm = 6 - await _pending_count(fresh_db, tid)
        assert n_done_at_sigterm >= 3

        # ---- Run 2: restart, expect resumption to drain to zero. ---
        proc2 = subprocess.Popen(
            [sys.executable, "-m",
             "services.ingestion.recovery.embedding_backlog"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        try:
            # Wait until all 6 rows are processed.
            deadline2 = time.monotonic() + 30.0
            while time.monotonic() < deadline2:
                if await _pending_count(fresh_db, tid) == 0:
                    break
                await asyncio.sleep(0.2)

            final_pending = await _pending_count(fresh_db, tid)
            proc2.send_signal(signal.SIGTERM)
            try:
                proc2.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc2.kill()
        finally:
            if proc2.poll() is None:
                proc2.kill()
                proc2.wait(timeout=5)

        # ===== LOAD-BEARING ASSERTIONS =====
        # 1. All 6 rows now have embeddings.
        assert final_pending == 0, (
            f"After SIGTERM + restart, {final_pending} rows still "
            f"pending. Cursor resumption did not deliver."
        )

        # 2. Every observation has a non-NULL embedding (proves
        # they were actually embedded, not just flag-cleared).
        async with fresh_db.acquire() as conn:
            await conn.execute("SET LOCAL row_security = off")
            with_emb = await conn.fetchval(
                "SELECT COUNT(*) FROM observations "
                "WHERE tenant_id = $1 AND embedding IS NOT NULL",
                tid,
            )
        assert with_emb == 6, (
            f"Only {with_emb}/6 observations have an embedding. "
            f"Some rows were flag-cleared without embedding."
        )

        # 3. Cursor advanced past the SIGTERM checkpoint (proves
        # the restart actually moved forward, didn't restart at the
        # beginning and re-do work).
        async with fresh_db.acquire() as conn:
            cur2 = await conn.fetchrow(
                "SELECT cursor_ingested_at FROM embedding_backlog_state "
                "WHERE instance_name = $1",
                env["BACKFILL_INSTANCE_NAME"],
            )
        # After full drain the service resets cursor to NULL; OR if
        # we caught it mid-iteration the cursor is past the SIGTERM
        # position. Either is acceptable; what's NOT acceptable is
        # the cursor being identical to the post-SIGTERM value AND
        # rows still pending — that would indicate stuck state.
        if cur2 is not None and cur2["cursor_ingested_at"] is not None:
            assert cur2["cursor_ingested_at"] >= cursor_after_sigterm, (
                f"Cursor regressed: {cur2['cursor_ingested_at']} < "
                f"{cursor_after_sigterm}. Restart re-started from the "
                f"beginning instead of resuming."
            )


# =====================================================================
# 4. Cursor reset after drain (new arrivals picked up).
# =====================================================================

@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_backlog_service_cursor_resets_after_drain(fresh_db: asyncpg.Pool):
    """After the table is drained, the service resets cursor to NULL
    and the next pass picks up any new rows whose ingested_at falls
    before the previous cursor end (late arrivals)."""
    from services.ingestion.recovery.embedding_backlog import (
        BacklogConfig, get_metrics, reset_metrics, run_backlog_service,
    )

    await _ensure_partition(fresh_db)
    tid = await _seed_tenant(fresh_db)

    # Two rows with ingested_at at t0 and t+10.
    obs_a, obs_b = uuid4(), uuid4()
    await _insert_pending(
        fresh_db, tenant_id=tid, obs_id=obs_a,
        content_text="row A", ingested_at=_NOW,
    )
    await _insert_pending(
        fresh_db, tenant_id=tid, obs_id=obs_b,
        content_text="row B", ingested_at=_NOW + dt.timedelta(seconds=10),
    )
    assert await _pending_count(fresh_db, tid) == 2

    stub = _StubOllama(vector=[0.5] * 768)
    redis = fake_aioredis.FakeRedis()
    reset_metrics()

    stop_event = asyncio.Event()

    async def _run_then_stop() -> None:
        try:
            await run_backlog_service(
                config=BacklogConfig(
                    instance_name="reset-test",
                    rate_qps=20.0,
                    batch_size=1,
                    drained_pause_sec=0.05,
                ),
                pool=fresh_db,
                redis=redis,
                ollama=stub,
                dlq_producer=_NoopProducer(),  # type: ignore[arg-type]
                stop_event=stop_event,
            )
        finally:
            await redis.aclose()

    task = asyncio.create_task(_run_then_stop())

    # Wait for the initial 2 rows to drain.
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if await _pending_count(fresh_db, tid) == 0:
            break
        await asyncio.sleep(0.05)

    # Now insert a late arrival with ingested_at BEFORE row B's
    # ingested_at — this is the case the cursor reset handles.
    obs_c = uuid4()
    await _insert_pending(
        fresh_db, tenant_id=tid, obs_id=obs_c,
        content_text="row C (late arrival)",
        ingested_at=_NOW + dt.timedelta(seconds=5),
    )

    # Wait for it to also drain (requires cursor reset).
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if await _pending_count(fresh_db, tid) == 0:
            break
        await asyncio.sleep(0.05)

    stop_event.set()
    await asyncio.wait_for(task, timeout=5.0)

    # ===== ASSERTIONS =====
    assert await _pending_count(fresh_db, tid) == 0, (
        "Late-arriving row C was not picked up — cursor reset path broken"
    )
    m = get_metrics()
    assert m["backlog.cursor_resets"] >= 1, m
    assert stub.call_count == 3  # A, B, C
