"""services/ingestion/recovery/embedding_backlog/embedding_backlog.py
   — backlog embedding service.

Per ingestion LLD §12.1 (Q3 — embedding backlog backfill) reshaped
per LLD amendment A4: a long-running rate-limited SERVICE, not a
one-shot script. M3.3.

=== Design summary ===

  • Reads `observations WHERE embedding_pending = TRUE` in
    ingested_at + id order, cursored by `embedding_backlog_state`.
  • Calls Ollama for the row's content_text; updates the
    observation under the LLD §5.4 guard `WHERE embedding_pending
    = TRUE` (the same guard M3.2 uses — race-safe coexistence with
    inline + worker paths).
  • Rate-limits via the M1.3 Lua bucket `rate:*system:ollama:embed`.
    BACKFILL_OLLAMA_QPS=0 produces the -1 sentinel from acquire.lua
    — the service stalls indefinitely without thrashing (operator
    pause switch).
  • On terminal Ollama failure: publishes ingestion.dlq with
    failure_kind="embedding.ollama_failure" (same envelope shape
    as M3.2) AND advances the cursor past the row so the service
    doesn't loop on a permanently-broken record. Recovery is via
    the replay tool, not retry-in-place.
  • SIGTERM handling: completes the current iteration (at most one
    row) and exits with code 0. Cursor was persisted before SIGTERM
    arrived, so a restart resumes from the same position.

=== PATH A — pgbouncer-compatible ===
Same pattern as the M3.1 DLQ writer + M3.2 embedding worker:
`asyncpg.create_pool(..., statement_cache_size=0)`. Runs behind a
pgbouncer sidecar in transaction mode.

=== Why no Kafka publish at the front ===
Inline `ingest()` publishes to `ingestion.embedding` for new rows;
the M3.2 worker consumes that topic. The backlog drainer exists for
rows that NEVER hit the topic — pre-M3.2 historical rows, plus
rows where the inline publish itself failed (Kafka outage). It
reads from Postgres directly so a Kafka outage cannot block the
recovery path.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import signal
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
from redis.asyncio import Redis as AsyncRedis

from lib.embeddings.ollama import (
    OllamaClient,
    OllamaConfig,
    OllamaDimensionMismatch,
    OllamaError,
)
from services.ingestion.dlq.publish import publish_dlq
from services.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingestion.rate_limit import RateLimiter


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Rate limiter integration — reuse the M1.3 bucket.
# ---------------------------------------------------------------------
# Per the M3 work order: `(tenant_id="*system", source="ollama",
# method="embed")`. The "*system" tenant marker is the operator-
# global bucket for cross-tenant work (vs. per-tenant API budgets).
BACKLOG_BUCKET_KEY = "rate:*system:ollama:embed"

# When the rate limiter returns the -1 sentinel (zero refill +
# empty bucket), poll this often before re-checking. Don't make
# this <1s — that just hammers Redis with deny-loops. Don't make
# it >5s — operators expect a QPS-raise to take effect within
# seconds, not minutes.
SENTINEL_RECHECK_SEC = 1.0


# In-process metrics. M5+ swaps to OTel.
_metrics: dict[str, float] = {
    "backlog.iterations":             0.0,
    "backlog.rows_selected":          0.0,
    "backlog.rows_embedded":          0.0,
    "backlog.rows_skipped_no_text":   0.0,
    "backlog.rows_failed":            0.0,
    "backlog.rate_limit_denials":     0.0,
    "backlog.rate_limit_sentinels":   0.0,
    "backlog.cursor_resets":          0.0,
    "backlog.dlq_publish.success":    0.0,
    "backlog.dlq_publish.failure":    0.0,
    "backlog.dlq_publish.skipped":    0.0,
}


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for k in _metrics:
        _metrics[k] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass
class BacklogConfig:
    """Configuration for one backlog service instance.

    Env-var-driven for production (see __main__.py); fields are
    public for test injection.
    """

    instance_name: str = "default"
    rate_qps: float = 10.0
    batch_size: int = 50
    # When the table is drained (empty SELECT), pause this long
    # before resetting the cursor and re-scanning.
    drained_pause_sec: float = 5.0
    # Bucket capacity derived from rate_qps: ~1 second of burst.
    # QPS=0 → capacity=0, which combined with refill=0 produces the
    # -1 sentinel on EVERY acquire (operator pause).
    @property
    def bucket_capacity(self) -> int:
        if self.rate_qps <= 0.0:
            return 0
        return max(1, int(self.rate_qps))

    @property
    def bucket_refill_per_sec(self) -> float:
        return max(0.0, float(self.rate_qps))


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
# Cursor read.
_LOAD_CURSOR_SQL = """
SELECT cursor_ingested_at, cursor_id
  FROM embedding_backlog_state
 WHERE instance_name = $1
"""

# Cursor write (UPSERT).
_PERSIST_CURSOR_SQL = """
INSERT INTO embedding_backlog_state
    (instance_name, cursor_ingested_at, cursor_id, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (instance_name) DO UPDATE
   SET cursor_ingested_at = EXCLUDED.cursor_ingested_at,
       cursor_id          = EXCLUDED.cursor_id,
       updated_at         = now()
"""

# Cursor-paged scan. The compound (ingested_at, id) tuple ordering
# is strictly ascending; the WHERE clause is a row-comparison so the
# resume is byte-exact (no skipped or repeated rows at the boundary).
_SELECT_BATCH_SQL = """
SELECT id, tenant_id, source_channel, content_text, occurred_at, ingested_at
  FROM observations
 WHERE embedding_pending = TRUE
   AND (
        $1::timestamptz IS NULL
     OR (ingested_at, id) > ($1::timestamptz, $2::uuid)
   )
 ORDER BY ingested_at ASC, id ASC
 LIMIT $3
"""

# LLD §5.4 guard. Identical to the M3.2 worker's UPDATE (race-safe
# coexistence: whoever clears the flag first wins).
_UPDATE_SQL = """
UPDATE observations
   SET embedding = $1::vector,
       embedding_pending = FALSE
 WHERE id = $2
   AND embedding_pending = TRUE
"""

# Skip-rows-with-empty-content fallback.
_CLEAR_FLAG_SQL = """
UPDATE observations
   SET embedding_pending = FALSE
 WHERE id = $1
   AND embedding_pending = TRUE
"""


# ---------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------
@dataclass
class _Cursor:
    ingested_at: dt.datetime | None
    id: UUID | None

    @property
    def is_null(self) -> bool:
        return self.ingested_at is None and self.id is None


async def _load_cursor(pool: asyncpg.Pool, instance: str) -> _Cursor:
    row = await pool.fetchrow(_LOAD_CURSOR_SQL, instance)
    if row is None:
        return _Cursor(ingested_at=None, id=None)
    return _Cursor(
        ingested_at=row["cursor_ingested_at"],
        id=row["cursor_id"],
    )


async def _persist_cursor(
    pool: asyncpg.Pool,
    instance: str,
    cursor: _Cursor,
) -> None:
    await pool.execute(
        _PERSIST_CURSOR_SQL,
        instance, cursor.ingested_at, cursor.id,
    )


async def _select_batch(
    pool: asyncpg.Pool, cursor: _Cursor, batch_size: int,
) -> list[asyncpg.Record]:
    return await pool.fetch(
        _SELECT_BATCH_SQL,
        cursor.ingested_at, cursor.id, batch_size,
    )


def _source_family(source_channel: str) -> str | None:
    """Extract source family ('slack'|'github'|'discord'|'gmail')
    from source_channel. Returns None for non-source-family channels
    (internal:*, synthetic:*) — those skip DLQ publish."""
    family = source_channel.split(":", 1)[0]
    if family in ("slack", "github", "discord", "gmail"):
        return family
    return None


async def _process_row(
    *,
    row: asyncpg.Record,
    pool: asyncpg.Pool,
    ollama: OllamaClient,
    dlq_producer: IdempotentProducer,
) -> None:
    """Embed + update ONE row. Failures publish DLQ but do not raise.

    Returns nothing — caller is responsible for cursor advance, which
    happens regardless of outcome to prevent loop-on-bad-row."""
    obs_id: UUID = row["id"]
    tenant_id: UUID = row["tenant_id"]
    content_text: str | None = row["content_text"]

    if not content_text:
        _bump("backlog.rows_skipped_no_text")
        async with pool.acquire() as conn:
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1, true)",
                str(tenant_id),
            )
            await conn.execute(_CLEAR_FLAG_SQL, obs_id)
        return

    try:
        vec = await ollama.embed(content_text)
    except (OllamaError, OllamaDimensionMismatch) as exc:
        _bump("backlog.rows_failed")
        log.warning(
            "backlog.ollama_failed",
            extra={
                "observation_id": str(obs_id),
                "tenant_id": str(tenant_id),
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )
        family = _source_family(row["source_channel"])
        if family is not None:
            await publish_dlq(
                producer=dlq_producer,
                failure_kind="embedding.ollama_failure",
                error_summary=f"{type(exc).__name__}: {str(exc)[:200]}",
                tenant_id=tenant_id,
                source=family,
                error_context={
                    "observation_id": str(obs_id),
                    "via": "backlog",
                },
                on_success=lambda: _bump("backlog.dlq_publish.success"),
                on_failure=lambda: _bump("backlog.dlq_publish.failure"),
                on_skipped=lambda: _bump("backlog.dlq_publish.skipped"),
            )
        else:
            _bump("backlog.dlq_publish.skipped")
        return

    vec_str = "[" + ",".join(repr(float(x)) for x in vec) + "]"
    async with pool.acquire() as conn:
        await conn.execute(
            "SELECT set_config('app.current_tenant', $1, true)",
            str(tenant_id),
        )
        result = await conn.execute(_UPDATE_SQL, vec_str, obs_id)
    rows_updated = 0
    if result.startswith("UPDATE "):
        try:
            rows_updated = int(result[len("UPDATE "):])
        except ValueError:
            rows_updated = 0
    if rows_updated == 1:
        _bump("backlog.rows_embedded")
    # rows_updated == 0 means another writer (M3.2 worker or inline)
    # claimed the row between SELECT and UPDATE — that's a no-op
    # success from this service's POV, NOT a failure.


# ---------------------------------------------------------------------
# Public entry.
# ---------------------------------------------------------------------
async def run_backlog_service(
    config: BacklogConfig,
    pool: asyncpg.Pool,
    redis: AsyncRedis,
    *,
    ollama: OllamaClient | None = None,
    dlq_producer: IdempotentProducer | None = None,
    stop_event: asyncio.Event | None = None,
    max_iterations: int | None = None,
) -> dict[str, int]:
    """Main loop. Returns when:
      - `stop_event` is set (SIGTERM, manual stop)
      - `max_iterations` iterations completed (test mode)

    The loop performs ONE iteration = (acquire token, fetch one row,
    embed, update, advance cursor). One row per iteration so the
    rate limiter governs per-Ollama-call as designed.
    """
    rl = RateLimiter(redis)
    stop_event = stop_event or asyncio.Event()
    own_ollama = ollama is None
    ollama_client = ollama or OllamaClient(OllamaConfig.from_env())
    own_dlq = dlq_producer is None
    if dlq_producer is None:
        dlq_producer = IdempotentProducer(ProducerConfig(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
            ),
            client_id=f"backlog-{config.instance_name}",
        ))
        await dlq_producer.start()

    cursor = await _load_cursor(pool, config.instance_name)

    iterations = 0
    try:
        while not stop_event.is_set():
            if max_iterations is not None and iterations >= max_iterations:
                break

            _bump("backlog.iterations")
            iterations += 1

            # ---- Rate limit BEFORE fetching anything. -----------------
            # Acquiring before the SELECT means a paused service
            # doesn't hold a row in memory; it just stalls on the
            # bucket. The bucket key is module-level — every
            # instance shares the same `*system` budget by design.
            acq = await rl.acquire(
                BACKLOG_BUCKET_KEY,
                capacity=config.bucket_capacity,
                refill_per_sec=config.bucket_refill_per_sec,
            )
            if not acq.granted:
                if acq.retry_after_ms == -1:
                    _bump("backlog.rate_limit_sentinels")
                    # The -1 sentinel says "this bucket will never
                    # recover on its own." For the backlog service,
                    # that's the operator's pause switch. We poll
                    # the bucket at SENTINEL_RECHECK_SEC so a
                    # config bump (refill_per_sec > 0) takes effect
                    # promptly without thrashing in between.
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=SENTINEL_RECHECK_SEC,
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue
                _bump("backlog.rate_limit_denials")
                # Finite retry_after_ms — wait the bucket out, then
                # loop. asyncio.wait_for lets SIGTERM interrupt it.
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=max(0.001, acq.retry_after_ms / 1000.0),
                    )
                except asyncio.TimeoutError:
                    pass
                continue

            # ---- Fetch ONE row past the cursor. -----------------------
            rows = await _select_batch(pool, cursor, batch_size=1)
            if not rows:
                # End of scan. Reset cursor for the next pass (to
                # catch new arrivals with ingested_at < previous
                # cursor) and pause.
                if not cursor.is_null:
                    cursor = _Cursor(ingested_at=None, id=None)
                    await _persist_cursor(pool, config.instance_name, cursor)
                    _bump("backlog.cursor_resets")
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=config.drained_pause_sec,
                    )
                except asyncio.TimeoutError:
                    pass
                continue

            _bump("backlog.rows_selected")
            row = rows[0]

            # ---- Embed + UPDATE. --------------------------------------
            await _process_row(
                row=row, pool=pool,
                ollama=ollama_client, dlq_producer=dlq_producer,
            )

            # ---- Advance + persist cursor BEFORE the next iteration. --
            # Persisting on every advance means a SIGTERM mid-loop
            # never re-processes a row. The cost is one UPSERT per
            # iteration; the cursor table has a single primary-key
            # row so the cost is trivial.
            cursor = _Cursor(ingested_at=row["ingested_at"], id=row["id"])
            await _persist_cursor(pool, config.instance_name, cursor)
    finally:
        if own_ollama:
            await ollama_client.close()
        if own_dlq:
            await dlq_producer.stop()

    return {
        "iterations": iterations,
        "rows_embedded": int(_metrics["backlog.rows_embedded"]),
    }


# ---------------------------------------------------------------------
# CLI entry — signal handling + asyncpg/redis bootstrap.
# ---------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(
        level=os.environ.get("EMBEDDING_BACKLOG_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = BacklogConfig(
        instance_name=os.environ.get("BACKFILL_INSTANCE_NAME", "default"),
        rate_qps=float(os.environ.get("BACKFILL_OLLAMA_QPS", "10")),
        batch_size=int(os.environ.get("BACKFILL_BATCH_SIZE", "50")),
    )

    async def _run() -> None:
        pool = await asyncpg.create_pool(
            os.environ["DATABASE_URL"],
            min_size=1,
            max_size=5,
            command_timeout=30.0,
            statement_cache_size=0,  # pgbouncer transaction mode
        )
        redis = AsyncRedis.from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=False,
        )

        stop_event = asyncio.Event()

        # SIGTERM handler. The current iteration completes (at most
        # one row) and the loop exits. Cursor was persisted on the
        # PREVIOUS iteration, so a SIGTERM mid-iteration doesn't
        # re-process the row that was about to be advanced — at
        # WORST the current row is in-flight at Ollama, and on
        # restart we re-process it (idempotent under the LLD §5.4
        # guard, so no data harm).
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)

        try:
            await run_backlog_service(
                config=config, pool=pool, redis=redis,
                stop_event=stop_event,
            )
        finally:
            await redis.aclose()
            await pool.close()

    asyncio.run(_run())


__all__ = [
    "BACKLOG_BUCKET_KEY",
    "BacklogConfig",
    "get_metrics",
    "main",
    "reset_metrics",
    "run_backlog_service",
]
