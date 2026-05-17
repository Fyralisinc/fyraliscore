"""services/ingestion/writers/embedding_worker/embedding_worker.py
   — live Ollama embedding worker.

Per ingestion LLD §5.4. M3.2.

This worker is the second Path A surface in the new pipeline (after
M3.1's DLQ writer). It:

  1. Consumes `ingestion.embedding` (group: `ingestion-embedder`).
  2. For each envelope: SELECTs the observation, calls Ollama embed,
     UPDATEs the row under the load-bearing guard
     `WHERE id = $1 AND embedding_pending = TRUE` (LLD §5.4).
  3. On terminal OllamaError (after the OllamaClient's internal retry
     loop — default 3 attempts with exponential backoff), publishes
     a DLQ envelope with failure_kind="embedding.ollama_failure" and
     commits the offset (the message MUST NOT be redelivered — the
     client already burned its retries; a Kafka redelivery would
     pile on more retries against an Ollama instance that is already
     refusing).

=== LLD §5.4 guard rationale ===
The guard `WHERE embedding_pending = TRUE` is load-bearing for TWO
properties (see [docs/ingestion/05-lld-amendments.md] A3):

  (a) **Coexistence with inline path.** During the M3 → M5 cutover
      window, both inline `services.ingestion.core.ingest` and this
      worker can write the same observation's embedding column.
      The guard makes the second writer's UPDATE a no-op — exactly
      one writer wins. The alternative guard `WHERE embedding IS
      NULL` is WRONG because the inline path sets `embedding`
      atomically with clearing `embedding_pending`, so a worker
      reading the row between commit and replication could see both
      `embedding IS NOT NULL` and `embedding_pending = TRUE` (in
      different snapshots) — a race the flag-only guard closes.

  (b) **Re-embed support.** Operators force a re-compute by setting
      `embedding_pending = TRUE` on a row that already has an
      embedding. The flag-only guard succeeds; the `embedding IS
      NULL` guard would silently fail.

=== PATH A — pgbouncer-compatible pool ===
Per LLD §5.2 + M1.3 ADR Q1: `asyncpg.create_pool(...,
statement_cache_size=0)`. This worker can run behind a pgbouncer
sidecar in transaction mode. Same pattern as M3.1's dlq_writer.

=== Retry semantics ===
OllamaClient internally retries on transient errors (5xx, connection,
timeout) with exponential backoff — default 3 attempts. The worker
treats `OllamaError` raised after that loop as terminal:

  - DLQ publish (so ops surfaces the failure)
  - Commit the Kafka offset (do NOT loop)

This matches LLD §5.4: "the message lands in DLQ after N retries."
The N is the client's max_retries, not Kafka redelivery count.

=== Best-effort observation lookup ===
If the worker pulls a message and the observation doesn't exist
(e.g. tenant data deletion between publish and consume), the worker
logs + skips + commits. No DLQ — this isn't an ingestion failure
mode, just stale work. Metric: `embedding_worker.observation_missing`.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any

import asyncpg
import orjson
from aiokafka import AIOKafkaConsumer

from lib.embeddings.ollama import (
    OllamaClient,
    OllamaConfig,
    OllamaDimensionMismatch,
    OllamaError,
)
from services.ingestion.dlq.publish import publish_dlq
from services.ingestion.embedding.models import EmbeddingEnvelope
from services.ingestion.kafka.producer import IdempotentProducer, ProducerConfig


log = logging.getLogger(__name__)


_EMBEDDING_TOPIC = "ingestion.embedding"
_CONSUMER_GROUP = "ingestion-embedder"


# In-process metrics. M5+ swap to OTel Prometheus.
_metrics: dict[str, float] = {
    "embedding_worker.messages_consumed":   0.0,
    "embedding_worker.envelope_parse_failure": 0.0,
    "embedding_worker.observation_missing": 0.0,
    "embedding_worker.guard_no_op":         0.0,
    "embedding_worker.embeds_succeeded":    0.0,
    "embedding_worker.embeds_failed":       0.0,
    "embedding_worker.dlq_publish.success": 0.0,
    "embedding_worker.dlq_publish.failure": 0.0,
    "embedding_worker.dlq_publish.skipped": 0.0,
}


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for k in _metrics:
        _metrics[k] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


# ---------------------------------------------------------------------
# The load-bearing UPDATE (LLD §5.4).
# ---------------------------------------------------------------------
# `embedding_pending = TRUE` is the ONLY guard. Do NOT add
# `embedding IS NULL` — see module docstring + A3 in
# docs/ingestion/05-lld-amendments.md. The cast to ::vector is
# explicit so a row with a different vector dim would surface a
# clear DB error instead of silent truncation.
_UPDATE_SQL = """
UPDATE observations
   SET embedding = $1::vector,
       embedding_pending = FALSE
 WHERE id = $2
   AND embedding_pending = TRUE
"""

_SELECT_OBSERVATION_SQL = """
SELECT content_text, embedding_pending
  FROM observations
 WHERE id = $1
 LIMIT 1
"""


async def embed_and_update(
    *,
    env: EmbeddingEnvelope,
    pool: asyncpg.Pool,
    ollama: OllamaClient,
    dlq_producer: IdempotentProducer,
) -> str:
    """Process one envelope. Returns a short status code for tests:
      - "embedded"            — wrote embedding, cleared pending
      - "guard_no_op"         — row no longer pending (inline won, or
                                worker ran twice; UPDATE matched 0)
      - "observation_missing" — observation row not found
      - "ollama_failed"       — terminal OllamaError; DLQ published

    Caller is responsible for offset commits.
    """
    # RLS context (per the project's tenant_isolation policy). The
    # observations table has FORCE ROW LEVEL SECURITY, so without
    # `app.current_tenant` the SELECT/UPDATE see no rows.
    async with pool.acquire() as conn:
        await conn.execute(
            "SELECT set_config('app.current_tenant', $1, true)",
            str(env.tenant_id),
        )

        row = await conn.fetchrow(
            _SELECT_OBSERVATION_SQL, env.observation_id,
        )
        if row is None:
            _bump("embedding_worker.observation_missing")
            log.info(
                "embedding_worker.observation_missing",
                extra={
                    "tenant_id": str(env.tenant_id),
                    "observation_id": str(env.observation_id),
                },
            )
            return "observation_missing"

        if not row["embedding_pending"]:
            # Inline already filled in the embedding (the publish
            # happens unconditionally; the worker filters here so
            # the inline path retains source-of-truth semantics).
            _bump("embedding_worker.guard_no_op")
            return "guard_no_op"

        content_text = row["content_text"]
        if not content_text:
            # Empty content_text — nothing to embed. Clear the
            # pending flag so we don't keep re-processing.
            await conn.execute(
                """
                UPDATE observations
                   SET embedding_pending = FALSE
                 WHERE id = $1 AND embedding_pending = TRUE
                """,
                env.observation_id,
            )
            _bump("embedding_worker.guard_no_op")
            return "guard_no_op"

    # Ollama call OUTSIDE the conn-acquired block so we don't hold
    # the pool slot during a remote network call.
    try:
        vec = await ollama.embed(content_text)
    except (OllamaError, OllamaDimensionMismatch) as exc:
        _bump("embedding_worker.embeds_failed")
        log.warning(
            "embedding_worker.ollama_failed",
            extra={
                "tenant_id": str(env.tenant_id),
                "observation_id": str(env.observation_id),
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )
        await publish_dlq(
            producer=dlq_producer,
            failure_kind="embedding.ollama_failure",
            error_summary=f"{type(exc).__name__}: {str(exc)[:200]}",
            tenant_id=env.tenant_id,
            source=env.source,
            error_context={"observation_id": str(env.observation_id)},
            on_success=lambda: _bump("embedding_worker.dlq_publish.success"),
            on_failure=lambda: _bump("embedding_worker.dlq_publish.failure"),
            on_skipped=lambda: _bump("embedding_worker.dlq_publish.skipped"),
        )
        return "ollama_failed"

    # Write the embedding under the LLD §5.4 guard.
    async with pool.acquire() as conn:
        await conn.execute(
            "SELECT set_config('app.current_tenant', $1, true)",
            str(env.tenant_id),
        )
        # pgvector accepts the string form "[f1,f2,...]" for vector
        # literals; encoding here keeps the call site agnostic of
        # whether pgvector's Python adapter is registered.
        vec_str = "[" + ",".join(repr(float(x)) for x in vec) + "]"
        result = await conn.execute(_UPDATE_SQL, vec_str, env.observation_id)

    # asyncpg returns "UPDATE <rowcount>" as the status string.
    rows_updated = 0
    if result.startswith("UPDATE "):
        try:
            rows_updated = int(result[len("UPDATE "):])
        except ValueError:
            rows_updated = 0

    if rows_updated == 0:
        # Race: the row's embedding_pending flipped to FALSE between
        # our SELECT and our UPDATE. Treat as a no-op (the other
        # writer won) — not a failure.
        _bump("embedding_worker.guard_no_op")
        return "guard_no_op"

    _bump("embedding_worker.embeds_succeeded")
    return "embedded"


@dataclass
class EmbeddingWorkerConfig:
    """Configuration for one worker process."""

    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = _CONSUMER_GROUP
    # Small pool — embedding is bottlenecked by Ollama, not the DB.
    postgres_pool_size: int = 5
    # Stop after N messages (test mode). Production = None.
    stop_after: int | None = None
    # Idle poll timeout for getmany — keeps the worker responsive to
    # SIGTERM without spinning.
    poll_timeout_ms: int = 500
    # DLQ producer config. Defaults are the idempotent/zstd/acks=all
    # set from LLD §5.2.
    dlq_producer_config: ProducerConfig | None = None
    # Ollama client config. Defaults to env-driven (OLLAMA_URL etc.).
    ollama_config: OllamaConfig | None = None


async def run_embedding_worker(
    config: EmbeddingWorkerConfig,
    pool: asyncpg.Pool,
    *,
    ollama: OllamaClient | None = None,
) -> dict[str, int]:
    """Main loop. Caller owns the pool (tests inject a fixture pool;
    production uses init_pool).

    Returns a stats dict for tests.
    """
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    dlq_producer = IdempotentProducer(
        config.dlq_producer_config or ProducerConfig(
            bootstrap_servers=config.bootstrap_servers,
            client_id=f"embedding-worker-dlq-{id(config)}",
        )
    )

    own_ollama = ollama is None
    ollama_client = ollama or OllamaClient(config.ollama_config)

    await consumer.start()
    await dlq_producer.start()
    consumer.subscribe([_EMBEDDING_TOPIC])

    consumed = 0
    embedded = 0
    try:
        while True:
            batches = await consumer.getmany(
                timeout_ms=config.poll_timeout_ms,
            )
            messages: list[Any] = []
            for partition_msgs in batches.values():
                messages.extend(partition_msgs)
            if not messages:
                if (
                    config.stop_after is not None
                    and consumed >= config.stop_after
                ):
                    break
                continue

            for msg in messages:
                consumed += 1
                _bump("embedding_worker.messages_consumed")

                try:
                    env = EmbeddingEnvelope.model_validate(
                        orjson.loads(msg.value)
                    )
                except Exception as exc:  # noqa: BLE001
                    _bump("embedding_worker.envelope_parse_failure")
                    log.warning(
                        "embedding_worker.envelope_parse_failed",
                        extra={
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:200],
                        },
                    )
                    # Parse failures on this topic are programmer
                    # errors (producer schema drift); skip + commit.
                    # No DLQ — best-effort extraction has nothing
                    # to extract from a garbage embedding envelope.
                    continue

                try:
                    status = await embed_and_update(
                        env=env,
                        pool=pool,
                        ollama=ollama_client,
                        dlq_producer=dlq_producer,
                    )
                    if status == "embedded":
                        embedded += 1
                except Exception as exc:  # noqa: BLE001
                    # Catch-all for unexpected errors (DB connection
                    # loss, etc.). Log + bump + continue — same
                    # prime directive as the DLQ writer.
                    _bump("embedding_worker.embeds_failed")
                    log.warning(
                        "embedding_worker.unexpected_error",
                        extra={
                            "tenant_id": str(env.tenant_id),
                            "observation_id": str(env.observation_id),
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:200],
                        },
                    )

            await consumer.commit()

            if (
                config.stop_after is not None
                and consumed >= config.stop_after
            ):
                break
    finally:
        await consumer.stop()
        await dlq_producer.stop()
        if own_ollama:
            await ollama_client.close()

    return {"consumed": consumed, "embedded": embedded}


def main() -> None:
    """Synchronous CLI entry."""
    logging.basicConfig(
        level=os.environ.get("EMBEDDING_WORKER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = EmbeddingWorkerConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
        ),
        postgres_pool_size=int(
            os.environ.get("POSTGRES_POOL_SIZE", "5")
        ),
    )

    async def _run() -> None:
        # Path A — pgbouncer-compatible. Same pattern as M3.1.
        pool = await asyncpg.create_pool(
            os.environ["DATABASE_URL"],
            min_size=1,
            max_size=config.postgres_pool_size,
            command_timeout=30.0,
            statement_cache_size=0,  # pgbouncer transaction mode
        )
        try:
            await run_embedding_worker(config, pool)
        finally:
            await pool.close()

    asyncio.run(_run())


__all__ = [
    "EmbeddingWorkerConfig",
    "embed_and_update",
    "get_metrics",
    "main",
    "reset_metrics",
    "run_embedding_worker",
]
