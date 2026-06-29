"""services/ingest/ingestion/writers/dlq_writer/dlq_writer.py — DLQ → Postgres.

Per ingestion LLD §5.5: a separate consumer reads `ingestion.dlq` and
UPSERTs each envelope into `ingestion_failures` (LLD §1.3 schema,
migration 0046). This is the queryable ops surface — every failure
the new pipeline can't process becomes a queryable row.

=== PATH A — first DB-write surface in the new pipeline ===
M2's normalizer + writer are Path B (no DB). M3.1's DLQ writer is the
first place the new pipeline opens an asyncpg pool. The pool is
configured with `pgbouncer_compatible=True` (per Q1 ADR + LLD §5.2)
so it survives behind a pgbouncer sidecar in transaction mode.

=== Wire failure_kind vs DB failure_kind ===
The Kafka envelope's failure_kind is producer-namespaced (e.g.
"normalizer.parse_failure"); the DB CHECK constraint on
`ingestion_failures.failure_kind` enumerates a coarser bucket (e.g.
"normalizer_parse_error"). The map below is the bridge; see
`services/ingest/ingestion/dlq/models.py` header for the rationale.

=== UPSERT key ===
Per LLD §5.5: `(tenant_id, source, raw_s3_key, failure_kind)`.
Re-published failures bump `attempt_count` and update `last_seen_at`
rather than creating duplicate rows. Migration 0051 added the
DB-enforced UNIQUE index on this 4-tuple, so the writer uses
`INSERT ... ON CONFLICT DO UPDATE` and the DB serialises concurrent
publishes of the same logical failure. raw_s3_key may be NULL for
failures that have no upstream S3 body (e.g. byte garbage on Kafka);
Postgres treats NULLs as DISTINCT in unique indexes, so those rows
genuinely-distinct-occurrence per LLD §1.3 and the UPSERT does not
collapse them (this is intentional — separate rate-limit episodes
ARE separate failures).

=== Failure handling ===
A transient Postgres error on one batch must NOT crash the consumer
— the next message is processed normally. The DLQ writer is the
last-resort sink; if IT can't make progress, the whole new pipeline
has nowhere to put failure records. Defensive: catch broad Exception
around the batch insert, log, bump metric, do NOT commit the Kafka
offset (so the batch is retried on next poll).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import asyncpg
import orjson
from aiokafka import AIOKafkaConsumer

from lib.shared.db import configure_connection_timeouts
from lib.shared.ids import uuid7
from services.ingest.ingestion.alerts import send_ops_alert
from services.ingest.ingestion.dlq.models import DLQEnvelope
from services.ingest.ingestion.kafka.shutdown import install_shutdown_event
from services.ingest.ingestion.kafka.topics import consumer_group, subscribe_topics
from services.ingest.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)


log = logging.getLogger(__name__)


_DLQ_TOPIC = "ingestion.dlq"
_CONSUMER_GROUP = "ingestion.dlq.writer"


# Wire failure_kind → DB failure_kind (per LLD §1.3 CHECK constraint).
# Wire form is dot-separated producer-namespaced; DB form is the
# underscore-separated bucket from migration 0046 + 0051. The bridge
# is intentional: producers get fine-grained kinds for alerting; ops
# queries get stable buckets that don't churn each release.
_WIRE_TO_DB_FAILURE_KIND: dict[str, str] = {
    "normalizer.parse_failure":     "normalizer_parse_error",
    # invariant failures are pre-validation rejections — same bucket
    # as parse errors for ops triage purposes.
    "normalizer.invariant_failure": "normalizer_parse_error",
    # writer invariant failures happen at the observation-insert
    # stage (LLD §5.2 writer pool); the bucket name reflects that.
    "writer.invariant_failure":     "observation_insert_error",
    # Summarization is a post-observation write surface. Reuse the existing
    # observation_insert_error DB bucket to avoid a CHECK-migration for this
    # first phase while preserving the fine-grained wire kind for alerts.
    "summarization.llm_failure":     "observation_insert_error",
    # M3.2: OllamaError after the OllamaClient's internal retry loop
    # (default 3 attempts with exponential backoff). The bucket
    # `embedding_ollama_failure` was added to the CHECK enum by
    # migration 0051.
    "embedding.ollama_failure":     "embedding_ollama_failure",
}


# In-process metrics. M5+ swap to OTel.
_metrics: dict[str, float] = {
    "dlq_writer.messages_consumed":     0.0,
    "dlq_writer.upserts":               0.0,
    "dlq_writer.parse_failure":         0.0,
    "dlq_writer.db_error":              0.0,
    "dlq_writer.consumer_lag_seconds":  0.0,
    # Gauge: unresolved ingestion_failures rows at the last depth poll.
    # Scrapeable; the writer also fires an ops alert when it crosses
    # the configured threshold (see DLQWriterConfig.depth_alert_*).
    "dlq_writer.unresolved_depth":      0.0,
    "dlq_writer.depth_alerts_sent":     0.0,
}


# Cheap with the migration-0046 partial index
# `ingestion_failures_failure_kind_idx WHERE resolved_at IS NULL`. No
# tenant context is set, so the table's NULL-current_tenant RLS policy
# returns the global unresolved total (the ops-wide backlog depth).
_COUNT_UNRESOLVED_SQL = """
SELECT count(*) FROM ingestion_failures WHERE resolved_at IS NULL
"""


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for k in _metrics:
        _metrics[k] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


# Single-statement UPSERT per LLD §5.5 / migration 0051.
#
# Conflict target = the 4-tuple UNIQUE index from migration 0051.
# Postgres treats two NULL raw_s3_keys as DISTINCT, so a row with
# raw_s3_key IS NULL will never match the conflict target and will
# always INSERT a fresh row — that is the intended behaviour for the
# genuinely-distinct-occurrence cases (rate_limit_exhausted,
# reconciliation_gap_unresolved) the LLD §1.3 carve-out names.
#
# On conflict (the common case — same envelope re-published or two
# concurrent producers racing on the same logical failure) we bump
# attempt_count, push last_seen_at forward, and refresh error_summary
# and error_context with the latest values. The DB takes the row
# lock, so concurrent writers serialise without app-level
# coordination — this is what closes the race the app-dedup pattern
# from M3.1's initial implementation had under READ COMMITTED.
_UPSERT_SQL = """
INSERT INTO ingestion_failures
    (id, tenant_id, source, failure_kind, raw_s3_key,
     error_summary, error_context,
     attempt_count, first_seen_at, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $8)
ON CONFLICT (tenant_id, source, raw_s3_key, failure_kind)
DO UPDATE SET
    attempt_count = ingestion_failures.attempt_count + 1,
    last_seen_at  = EXCLUDED.last_seen_at,
    error_summary = EXCLUDED.error_summary,
    error_context = EXCLUDED.error_context
"""


async def upsert_failure(
    conn: asyncpg.Connection,
    env: DLQEnvelope,
) -> None:
    """Tenant-bound UPSERT of one envelope. Caller holds the conn
    (so RLS context can be set per-tenant). Per LLD §5.5.

    Single statement: `INSERT ... ON CONFLICT DO UPDATE` against the
    UNIQUE index from migration 0051. DB serialises concurrent
    publishes of the same logical failure (same 4-tuple); each one
    increments attempt_count by exactly 1.
    """
    db_failure_kind = _WIRE_TO_DB_FAILURE_KIND.get(env.failure_kind)
    if db_failure_kind is None:
        # Wire kind missing from the map — programmer error. Raise
        # to fail loudly; do NOT silently swallow.
        raise ValueError(
            f"DLQ writer has no DB mapping for wire failure_kind "
            f"{env.failure_kind!r}. Update _WIRE_TO_DB_FAILURE_KIND."
        )

    # RLS context — per the project's RLS pattern. SET LOCAL scopes
    # to the current transaction; auto-resets at COMMIT/ROLLBACK.
    await conn.execute(
        "SELECT set_config('app.current_tenant', $1, true)",
        str(env.tenant_id),
    )

    await conn.execute(
        _UPSERT_SQL,
        uuid7(),
        env.tenant_id,
        env.source,
        db_failure_kind,
        env.raw_s3_key,
        env.error_summary,
        json.dumps(env.error_context),
        env.failed_at,
    )
    _bump("dlq_writer.upserts")


@dataclass
class DLQWriterConfig:
    """Configuration for the DLQ writer process."""

    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = _CONSUMER_GROUP
    # Source isolation: when set, consume ONLY ingestion.dlq.<source>
    # under group "<consumer_group>.<source>"; None → all per-source DLQ
    # topics under the bare group. DLQ is low-volume so the default
    # single all-sources writer is usually sufficient.
    source: str | None = None
    # Small pool — DLQ is low-volume by design (LLD §5.5).
    postgres_pool_size: int = 5
    # Stop after N messages (test mode); production = None.
    stop_after: int | None = None
    # Batch size — max envelopes per transaction. LLD §5.5 default 50.
    batch_max_size: int = 50
    # Idle timeout — flush partial batch when no new messages arrive.
    batch_idle_ms: int = 500
    # --- DLQ-depth monitoring (gap #5: alert on backlog growth) ---
    # Alert when unresolved ingestion_failures rows reach this count.
    # 0 disables the alert (the gauge is still exported). Default off
    # so it must be opted into per deployment.
    depth_alert_threshold: int = 0
    # How often to poll the unresolved count (seconds). 0 disables the
    # poll entirely.
    depth_check_interval_sec: float = 60.0
    # Minimum seconds between repeat alerts while over threshold, so a
    # standing backlog doesn't spam the on-call channel every poll.
    depth_alert_cooldown_sec: float = 3600.0


async def count_unresolved_failures(pool: asyncpg.Pool) -> int:
    """Global count of unresolved ingestion_failures rows (the ops-wide
    DLQ backlog depth). No tenant context → RLS returns all tenants."""
    async with pool.acquire() as conn:
        return int(await conn.fetchval(_COUNT_UNRESOLVED_SQL))


async def poll_dlq_depth(
    config: DLQWriterConfig,
    pool: asyncpg.Pool,
    *,
    last_alert_monotonic: float,
    now_monotonic: float,
) -> float:
    """Poll the unresolved-failure depth, update the gauge, and fire a
    threshold alert (cooldown-debounced). Best-effort: any error is
    logged and swallowed so the DLQ writer's main loop never stalls on
    a depth poll.

    Returns the (possibly updated) `last_alert_monotonic` watermark —
    advanced only when an alert fired. Pass `float("-inf")` to mean
    "never alerted" so the first breach fires immediately instead of
    waiting out the cooldown against an epoch-zero watermark.
    """
    try:
        depth = await count_unresolved_failures(pool)
    except Exception as exc:  # noqa: BLE001 — monitoring must not crash the sink
        log.warning(
            "dlq_writer.depth_poll_failed",
            extra={"error_type": type(exc).__name__, "error": str(exc)[:200]},
        )
        return last_alert_monotonic

    _metrics["dlq_writer.unresolved_depth"] = float(depth)

    if config.depth_alert_threshold <= 0 or depth < config.depth_alert_threshold:
        return last_alert_monotonic
    if (now_monotonic - last_alert_monotonic) < config.depth_alert_cooldown_sec:
        return last_alert_monotonic

    log.warning(
        "dlq_writer.depth_threshold_exceeded",
        extra={"depth": depth, "threshold": config.depth_alert_threshold},
    )
    sent = await send_ops_alert(
        "dlq.depth_threshold_exceeded",
        {
            "unresolved_depth": depth,
            "threshold": config.depth_alert_threshold,
        },
    )
    if sent:
        _bump("dlq_writer.depth_alerts_sent")
    # Advance the cooldown watermark whether or not the webhook was
    # configured/succeeded — the log line above is itself the alert of
    # record, and we don't want a missing webhook to turn every poll
    # into a repeated warning storm.
    return now_monotonic


async def run_dlq_writer(
    config: DLQWriterConfig,
    pool: asyncpg.Pool,
) -> dict[str, int]:
    """Main loop. Caller owns the pool (so tests can inject their own
    fixture-managed pool; production uses init_pool).

    Returns a stats dict for tests.
    """
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=consumer_group(config.consumer_group, config.source),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    consumer.subscribe(subscribe_topics("dlq", config.source))

    consumed = 0
    upserted = 0

    # Ticket #45: getmany returns every batch_idle_ms, so checking the
    # stop event each iteration gives a clean rc=0 exit on SIGTERM.
    stop_event = install_shutdown_event()
    # Liveness + metrics surface (opt-in via INGESTION_HEALTH_PORT).
    heartbeat = Heartbeat()
    health = start_health_server(get_metrics=get_metrics, heartbeat=heartbeat)
    ticker = asyncio.ensure_future(run_heartbeat_ticker(heartbeat, stop_event))
    # DLQ-depth monitor state (wall-clock, monotonic): poll on an
    # interval independent of message arrival so a standing backlog is
    # detected even when the DLQ topic is quiet. `-inf` means "never
    # alerted" so the FIRST threshold breach fires immediately rather
    # than waiting out a cooldown against an epoch-zero watermark.
    last_depth_check = float("-inf")
    last_depth_alert = float("-inf")
    try:
        while not stop_event.is_set():
            if config.depth_check_interval_sec > 0:
                mono = time.monotonic()
                if (mono - last_depth_check) >= config.depth_check_interval_sec:
                    last_depth_check = mono
                    last_depth_alert = await poll_dlq_depth(
                        config, pool,
                        last_alert_monotonic=last_depth_alert,
                        now_monotonic=mono,
                    )
            # getmany returns a dict[TopicPartition, list[record]].
            # max_records caps the batch size; timeout_ms is the
            # idle flush deadline.
            batches = await consumer.getmany(
                timeout_ms=config.batch_idle_ms,
                max_records=config.batch_max_size,
            )
            messages: list[Any] = []
            for partition_msgs in batches.values():
                messages.extend(partition_msgs)
            if not messages:
                if config.stop_after is not None and consumed >= config.stop_after:
                    break
                continue

            for msg in messages:
                consumed += 1
                _bump("dlq_writer.messages_consumed")
                if msg.timestamp:
                    lag_s = max(
                        0.0,
                        (time.time() * 1000 - msg.timestamp) / 1000.0,
                    )
                    _metrics["dlq_writer.consumer_lag_seconds"] = lag_s

                try:
                    env = DLQEnvelope.model_validate(
                        orjson.loads(msg.value)
                    )
                except Exception as exc:  # noqa: BLE001
                    _bump("dlq_writer.parse_failure")
                    log.warning(
                        "dlq_writer.envelope_parse_failed",
                        extra={
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:200],
                        },
                    )
                    # Garbage envelope on the DLQ topic itself — log
                    # + skip + commit. We cannot recursively DLQ a
                    # bad DLQ envelope (would loop). Same prime
                    # directive as M2.4's "don't get stuck on garbage."
                    continue

                # Per-message transaction. Small txns keep the pool
                # connection time low and let one bad envelope not
                # block N-1 good ones from making progress.
                try:
                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            await upsert_failure(conn, env)
                    upserted += 1
                except Exception as exc:  # noqa: BLE001
                    _bump("dlq_writer.db_error")
                    log.warning(
                        "dlq_writer.upsert_failed",
                        extra={
                            "tenant_id": str(env.tenant_id),
                            "failure_kind": env.failure_kind,
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:200],
                        },
                    )
                    # Do NOT re-raise — next message must proceed.
                    # The current message's offset will still be
                    # committed below, so it will NOT be re-tried.
                    # This is intentional: the DLQ writer is a
                    # best-effort sink; if a row genuinely can't be
                    # inserted, surfacing in metrics + logs is enough.

            # Commit ONCE per batch — at-least-once delivery
            # semantics. Per-message commits would 5x the broker
            # round-trips on hot batches.
            await consumer.commit()

            if config.stop_after is not None and consumed >= config.stop_after:
                break
    finally:
        ticker.cancel()
        if health is not None:
            health.shutdown()
        await consumer.stop()

    return {"consumed": consumed, "upserted": upserted}


def main() -> None:
    """Synchronous CLI entry. Wraps run_dlq_writer in asyncio.run."""
    logging.basicConfig(
        level=os.environ.get("DLQ_WRITER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = DLQWriterConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
        ),
        source=os.environ.get("INGESTION_SOURCE") or None,
        postgres_pool_size=int(
            os.environ.get("POSTGRES_POOL_SIZE", "5")
        ),
        depth_alert_threshold=int(
            os.environ.get("DLQ_DEPTH_ALERT_THRESHOLD", "0")
        ),
        depth_check_interval_sec=float(
            os.environ.get("DLQ_DEPTH_CHECK_INTERVAL_SEC", "60")
        ),
        depth_alert_cooldown_sec=float(
            os.environ.get("DLQ_DEPTH_ALERT_COOLDOWN_SEC", "3600")
        ),
    )

    async def _run() -> None:
        # Production-side pool: pgbouncer-compatible per Q1 ADR.
        # The DLQ writer is the FIRST activation of this flag in the
        # new pipeline; see LLD §5.2.
        pool = await asyncpg.create_pool(
            os.environ["DATABASE_URL"],
            min_size=1,
            max_size=config.postgres_pool_size,
            command_timeout=30.0,
            init=configure_connection_timeouts,
            statement_cache_size=0,  # pgbouncer transaction mode
        )
        try:
            await run_dlq_writer(config, pool)
        finally:
            await pool.close()

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover - process entrypoint
    main()


__all__ = [
    "DLQWriterConfig",
    "count_unresolved_failures",
    "get_metrics",
    "main",
    "poll_dlq_depth",
    "reset_metrics",
    "run_dlq_writer",
    "upsert_failure",
]
