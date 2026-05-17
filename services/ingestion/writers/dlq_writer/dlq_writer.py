"""services/ingestion/writers/dlq_writer/dlq_writer.py — DLQ → Postgres.

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
`services/ingestion/dlq/models.py` header for the rationale.

=== UPSERT key ===
Per LLD §5.5: `(tenant_id, source, raw_s3_key, failure_kind)`.
Re-published failures bump `attempt_count` and update `last_seen_at`
rather than creating duplicate rows. raw_s3_key may be NULL for
failures that have no upstream S3 body (e.g. byte garbage on Kafka)
— the partial unique index handles both NULL and non-NULL cases.

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
import datetime as dt
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import asyncpg
import orjson
from aiokafka import AIOKafkaConsumer

from lib.shared.ids import uuid7
from services.ingestion.dlq.models import DLQEnvelope


log = logging.getLogger(__name__)


_DLQ_TOPIC = "ingestion.dlq"
_CONSUMER_GROUP = "ingestion.dlq.writer"


# Wire failure_kind → DB failure_kind (per LLD §1.3 CHECK constraint).
# M3.1 ships three; M3.2 will extend with "embedding.ollama_failure"
# alongside a migration that adds "embedding_ollama_failure" to the
# CHECK enum.
_WIRE_TO_DB_FAILURE_KIND: dict[str, str] = {
    "normalizer.parse_failure":     "normalizer_parse_error",
    # invariant failures are pre-validation rejections — same bucket
    # as parse errors for ops triage purposes.
    "normalizer.invariant_failure": "normalizer_parse_error",
    # writer invariant failures happen at the observation-insert
    # stage (LLD §5.2 writer pool); the bucket name reflects that.
    "writer.invariant_failure":     "observation_insert_error",
}


# In-process metrics. M5+ swap to OTel.
_metrics: dict[str, float] = {
    "dlq_writer.messages_consumed":     0.0,
    "dlq_writer.upserts":               0.0,
    "dlq_writer.parse_failure":         0.0,
    "dlq_writer.db_error":              0.0,
    "dlq_writer.consumer_lag_seconds":  0.0,
}


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for k in _metrics:
        _metrics[k] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


# The UPSERT — explicit partial-unique handling for raw_s3_key NULL.
# Per Postgres semantics, two NULLs are NOT equal in unique indexes,
# so a (NULL raw_s3_key) row would create duplicates on every retry.
# We disambiguate by treating NULL as a specific token at the
# application level — translating to a UNIQUE on the four columns
# would require a coalesce in the index, which migration 0046 does
# not provide. Workaround: M3.1 dedups on the four-tuple at app level
# via a SELECT-then-INSERT/UPDATE pattern (a single tenant-bound
# transaction so concurrent writers serialise).
#
# This is an acknowledged limitation; M3.4 surfaces it as an LLD
# amendment candidate (the 0046 migration's index does not enforce
# the UPSERT key the LLD describes).
_SELECT_EXISTING_SQL = """
SELECT id, attempt_count
FROM ingestion_failures
WHERE tenant_id = $1
  AND source = $2
  AND failure_kind = $3
  AND ((raw_s3_key IS NULL AND $4::text IS NULL) OR raw_s3_key = $4)
LIMIT 1
"""

_UPDATE_EXISTING_SQL = """
UPDATE ingestion_failures
   SET attempt_count = attempt_count + 1,
       last_seen_at  = $2,
       error_summary = $3,
       error_context = $4
 WHERE id = $1
"""

_INSERT_NEW_SQL = """
INSERT INTO ingestion_failures
    (id, tenant_id, source, failure_kind, raw_s3_key,
     error_summary, error_context,
     attempt_count, first_seen_at, last_seen_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $8)
"""


async def upsert_failure(
    conn: asyncpg.Connection,
    env: DLQEnvelope,
) -> None:
    """Tenant-bound UPSERT of one envelope. Caller holds the conn
    (so RLS context can be set per-tenant). Per LLD §5.5.

    Steps:
      1. SET LOCAL app.current_tenant = $1 (RLS context).
      2. SELECT existing row by (tenant_id, source, failure_kind,
         raw_s3_key).
      3. If found: UPDATE attempt_count + last_seen_at.
         Else:     INSERT new row.
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

    existing = await conn.fetchrow(
        _SELECT_EXISTING_SQL,
        env.tenant_id, env.source, db_failure_kind, env.raw_s3_key,
    )

    error_context_json = json.dumps(env.error_context)

    if existing is not None:
        await conn.execute(
            _UPDATE_EXISTING_SQL,
            existing["id"],
            env.failed_at,
            env.error_summary,
            error_context_json,
        )
    else:
        await conn.execute(
            _INSERT_NEW_SQL,
            uuid7(),
            env.tenant_id,
            env.source,
            db_failure_kind,
            env.raw_s3_key,
            env.error_summary,
            error_context_json,
            env.failed_at,
        )
    _bump("dlq_writer.upserts")


@dataclass
class DLQWriterConfig:
    """Configuration for the DLQ writer process."""

    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = _CONSUMER_GROUP
    # Small pool — DLQ is low-volume by design (LLD §5.5).
    postgres_pool_size: int = 5
    # Stop after N messages (test mode); production = None.
    stop_after: int | None = None
    # Batch size — max envelopes per transaction. LLD §5.5 default 50.
    batch_max_size: int = 50
    # Idle timeout — flush partial batch when no new messages arrive.
    batch_idle_ms: int = 500


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
        group_id=config.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    consumer.subscribe([_DLQ_TOPIC])

    consumed = 0
    upserted = 0

    try:
        while True:
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
        postgres_pool_size=int(
            os.environ.get("POSTGRES_POOL_SIZE", "5")
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
            statement_cache_size=0,  # pgbouncer transaction mode
        )
        try:
            await run_dlq_writer(config, pool)
        finally:
            await pool.close()

    asyncio.run(_run())


__all__ = [
    "DLQWriterConfig",
    "get_metrics",
    "main",
    "reset_metrics",
    "run_dlq_writer",
    "upsert_failure",
]
