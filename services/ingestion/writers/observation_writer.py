"""services/ingestion/writers/observation_writer.py
   — Observation writer with flag-branched full-mode.

History:
  - M2.4: Path B no-op. Consumed `ingestion.normalized`, logged each
    NormalizedEnvelope, appended a `ShadowWriteEvent` to an in-process
    list. NO Postgres write. The inline `ingest()` was the source of
    truth during the 48h zero-divergence soak.
  - M5.2: full-mode transition. Per-envelope the writer reads
    `ingestion.kafka_path_enabled` from `tenant_flags`. When TRUE,
    the writer calls `services.ingestion.core.ingest_from_draft(...)`
    to write the observation (the normalizer already ran the handler,
    so the draft fields embedded in the envelope are used directly).
    When FALSE (default; pre-cutover tenants), the writer preserves
    M2's shadow-log no-op behavior.

============================================================
PATH A — the writer is now Path A for full-mode tenants
============================================================
The M2.4 import-graph contract ("writer MUST NOT import asyncpg") is
INTENTIONALLY LIFTED in M5.2. The writer now:
  - Holds an asyncpg.Pool (pgbouncer-compatible — fifth activation
    of `statement_cache_size=0` after M3.1, M3.3, M4.2, M5.1).
  - Wires ActorRepo + EntityAliasRepo for actor/entity resolution
    inside `ingest_from_draft`.
  - Reads `tenant_flags` per envelope.

The M2 e2e shadow test (`test_e2e_shadow.py`) continues to pass
because its tenants have no row in `tenant_flags` for
`ingestion.kafka_path_enabled` → reader returns the default
`False` → shadow log path runs unchanged.

============================================================
PER-ENVELOPE TRANSACTION CONTRACT (M5 Finding 4)
============================================================
Each envelope gets ONE call to `ingest_from_draft`, which opens its
own transaction. There is NO batched-transaction wrapper. The
performance floor is ~50 obs/sec/process — acceptable for M5/M6
load profiles; a future M-Throughput work-unit may refactor
`ingest_from_draft` to share a transaction across envelopes if
M-Load binds.

============================================================
ERROR HANDLING
============================================================
  - Parse failure (NormalizedEnvelope.model_validate raises):
    bump parse_failure, DLQ-publish, COMMIT offset. Same as M2.4.
  - Full-mode permanent error (ValidationError, HandlerNotFound):
    bump full_mode_failure, DLQ-publish, COMMIT offset.
  - Full-mode transient error (any other Exception): re-raise.
    The consumer loop exits; the supervisor restarts the writer;
    Kafka redelivers from the last committed offset.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer

from lib.shared.errors import ValidationError
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.ingestion.core import (
    IngestResult,
    PayloadTooLarge,
    ingest_from_draft,
)
from services.ingestion.dlq.publish import publish_dlq
from services.ingestion.feature_flags.client import (
    KAFKA_PATH_ENABLED,
    TenantFlags,
)
from services.ingestion.handlers import (
    HandlerNotFound,
    ObservationDraft,
)
from services.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingestion.normalizer.models import NormalizedEnvelope


log = logging.getLogger(__name__)


_NORMALIZED_TOPIC = "ingestion.normalized"
_WRITER_GROUP = "observation-writer"


# In-process metrics. M3 swaps to OTel Prometheus.
_metrics: dict[str, float] = {
    "writer.messages_consumed": 0.0,
    "writer.shadow_write_events": 0.0,
    "writer.full_mode_writes": 0.0,
    "writer.full_mode_dedup_hits": 0.0,
    "writer.full_mode_failures": 0.0,
    # A28 — observation routed to DLQ because no partition covers its
    # occurred_at (permanent, not transient; see ticket #44).
    "writer.partition_missing": 0.0,
    "writer.parse_failure": 0.0,
    # M3.1 — DLQ publish metrics.
    "writer.dlq_publish.success": 0.0,
    "writer.dlq_publish.failure": 0.0,
    "writer.dlq_publish.skipped": 0.0,
}


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for k in _metrics:
        _metrics[k] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


# ---------------------------------------------------------------------
# M2 shadow log (preserved for flag=FALSE tenants).
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class ShadowWriteEvent:
    """One record the writer would have INSERTed in M2. Preserved
    in M5.2 for tenants whose `ingestion.kafka_path_enabled` is
    FALSE — those tenants are still on the inline path, so the
    writer remains a no-op shadow observer for them.
    """

    tenant_id: str
    source: str
    ingress_kind: str
    source_channel: str
    external_id: str | None
    content_hash: str
    raw_s3_key: str
    occurred_at: dt.datetime
    normalized_at: dt.datetime


_shadow_log: list[ShadowWriteEvent] = []
_shadow_log_lock: asyncio.Lock | None = None


def _get_lock() -> asyncio.Lock:
    global _shadow_log_lock
    if _shadow_log_lock is None:
        _shadow_log_lock = asyncio.Lock()
    return _shadow_log_lock


def get_shadow_log() -> list[ShadowWriteEvent]:
    return list(_shadow_log)


def reset_shadow_log() -> None:
    _shadow_log.clear()


async def _record_shadow_event(env: NormalizedEnvelope) -> None:
    event = ShadowWriteEvent(
        tenant_id=str(env.tenant_id),
        source=env.source,
        ingress_kind=env.ingress_kind,
        source_channel=env.source_channel,
        external_id=env.external_id,
        content_hash=env.content_hash,
        raw_s3_key=env.raw_s3_key,
        occurred_at=env.occurred_at,
        normalized_at=env.normalized_at,
    )
    async with _get_lock():
        _shadow_log.append(event)
    _bump("writer.shadow_write_events")
    log.info(
        "writer.shadow_write_event",
        extra={
            "tenant_id": event.tenant_id,
            "source": event.source,
            "source_channel": event.source_channel,
            "external_id": event.external_id,
            "content_hash_prefix": event.content_hash[:16],
        },
    )


# ---------------------------------------------------------------------
# M5.2 — full-mode draft reconstruction + Postgres write.
# ---------------------------------------------------------------------
def _draft_from_envelope(env: NormalizedEnvelope) -> ObservationDraft:
    """Rebuild the `ObservationDraft` the normalizer (M2.3) emitted.

    `NormalizedEnvelope` carries the draft fields 1:1 (see
    `services/ingestion/normalizer/models.py`), so we reconstruct
    without re-running the handler. `unresolved_phrases` is left
    empty — the normalizer doesn't surface it on the wire, and
    `ingest_from_draft` re-derives candidate phrases from
    `content_text` in step 4.
    """
    return ObservationDraft(
        source_channel=env.source_channel,
        content_text=env.content_text,
        content=dict(env.content),
        occurred_at=env.occurred_at,
        trust_tier=env.trust_tier,  # type: ignore[arg-type]
        kind=env.kind,  # type: ignore[arg-type]
        source_actor_ref=env.source_actor_ref,
        external_id=env.external_id,
        entities_hint=list(env.entities_hint),
        unresolved_phrases=[],
        raw_payload=None,
    )


async def _full_mode_write(
    env: NormalizedEnvelope,
    *,
    pool: asyncpg.Pool,
    actor_repo: ActorRepo | None,
    alias_repo: EntityAliasRepo | None,
    embedder: Any,
    embedding_producer: Any,
) -> IngestResult:
    """Call `ingest_from_draft` per envelope. One transaction per
    envelope per Finding 4. Caller is responsible for catching
    permanent vs transient errors and committing the offset only
    after a definitive outcome.
    """
    draft = _draft_from_envelope(env)
    result = await ingest_from_draft(
        channel=env.source_channel,
        draft=draft,
        pool=pool,
        tenant_id=env.tenant_id,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        enqueue_trigger=True,
        embedding_producer=embedding_producer,
    )
    if result.deduped:
        _bump("writer.full_mode_dedup_hits")
    else:
        _bump("writer.full_mode_writes")
    return result


# ---------------------------------------------------------------------
# Pool helper — pgbouncer-compatible. Fifth activation of
# `statement_cache_size=0` after M3.1, M3.3, M4.2, M5.1.
# ---------------------------------------------------------------------
async def make_writer_pool(
    dsn: str,
    *,
    max_size: int = 10,
    command_timeout: float = 30.0,
) -> asyncpg.Pool:
    """Construct an asyncpg pool for the observation writer's
    full-mode Postgres writes. `statement_cache_size=0` per the
    M1.3 ADR Q1 pgbouncer-transaction-mode contract.

    Mirrors the M5.1 circuit-breaker pool init at
    `services/ingestion/feature_flags/circuit_breaker.py::make_breaker_pool`
    and the M4.2 session-state pool at
    `services/integrations/discord/gateway/session_state.py::make_session_state_pool`.
    """
    return await asyncpg.create_pool(
        dsn,
        min_size=2,
        max_size=max_size,
        command_timeout=command_timeout,
        statement_cache_size=0,  # pgbouncer transaction mode (M1.3 ADR Q1)
    )


@dataclass
class WriterConfig:
    """Configuration for one writer process.

    Production startup wires deps from env vars. Tests inject
    pre-built deps via the fields below; the writer then skips its
    own startup wiring.
    """

    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = _WRITER_GROUP
    # Stop after N events (test mode). Production = None.
    stop_after: int | None = None
    # M3.1 — producer config for DLQ publishes + embedding-pending
    # publishes (same producer instance, different topics).
    dlq_producer_config: ProducerConfig | None = None
    # M5.2 — Path A deps for full-mode envelopes. When `pool` is
    # None, the writer stays in shadow-only mode for every envelope
    # (matches M2.4 behaviour; useful for tests that don't want a
    # DB).
    pool: asyncpg.Pool | None = None
    tenant_flags: TenantFlags | None = None
    actor_repo: ActorRepo | None = None
    alias_repo: EntityAliasRepo | None = None
    embedder: Any = None
    # M5.2 — Kafka producer used by `ingest_from_draft` to emit
    # ingestion.embedding requests. Defaults to the same producer
    # used for DLQ publishes (one IdempotentProducer can publish to
    # multiple topics).
    embedding_producer: Any = None


async def _handle_message(
    msg_value: bytes,
    *,
    config: WriterConfig,
    dlq_producer: IdempotentProducer,
    embedding_producer: Any,
    msg_topic: str = _NORMALIZED_TOPIC,
    msg_partition: int = 0,
    msg_offset: int = 0,
) -> None:
    """Per-message logic, factored out of `run_writer` so M5.2 unit
    tests can drive it without spinning up Kafka.

    Outcome contract — callers (run_writer) should `commit()` the
    offset after this returns; we either succeeded or DLQ'd. The
    transient-error path raises so the consumer loop exits and the
    supervisor restarts (the message is reprocessed from the last
    committed offset).
    """
    _bump("writer.messages_consumed")
    try:
        env = NormalizedEnvelope.model_validate(json.loads(msg_value))
    except Exception as exc:  # noqa: BLE001
        _bump("writer.parse_failure")
        log.warning(
            "writer.parse_failed",
            extra={
                "topic": msg_topic,
                "partition": msg_partition,
                "offset": msg_offset,
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )
        await publish_dlq(
            producer=dlq_producer,
            failure_kind="writer.invariant_failure",
            error_summary=f"{type(exc).__name__}: {str(exc)[:200]}",
            msg_bytes=msg_value,
            on_success=lambda: _bump("writer.dlq_publish.success"),
            on_failure=lambda: _bump("writer.dlq_publish.failure"),
            on_skipped=lambda: _bump("writer.dlq_publish.skipped"),
        )
        return

    # ---- Flag-branched write ----
    should_full_mode = False
    if config.tenant_flags is not None and config.pool is not None:
        # LLD §11: default missing → False (pre-cutover tenants stay
        # on the inline path; writer remains shadow-only for them).
        should_full_mode = await config.tenant_flags.get_bool(
            env.tenant_id, KAFKA_PATH_ENABLED, default=False,
        )

    if not should_full_mode:
        await _record_shadow_event(env)
        return

    try:
        await _full_mode_write(
            env,
            pool=config.pool,
            actor_repo=config.actor_repo,
            alias_repo=config.alias_repo,
            embedder=config.embedder,
            embedding_producer=embedding_producer,
        )
    except (ValidationError, HandlerNotFound, PayloadTooLarge) as exc:
        # Permanent error — DLQ + commit. Same shape as the
        # parse-failure branch.
        _bump("writer.full_mode_failures")
        log.warning(
            "writer.full_mode_permanent_failure",
            extra={
                "topic": msg_topic,
                "partition": msg_partition,
                "offset": msg_offset,
                "tenant_id": str(env.tenant_id),
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )
        await publish_dlq(
            producer=dlq_producer,
            failure_kind="writer.full_mode_permanent_failure",
            error_summary=f"{type(exc).__name__}: {str(exc)[:200]}",
            msg_bytes=msg_value,
            on_success=lambda: _bump("writer.dlq_publish.success"),
            on_failure=lambda: _bump("writer.dlq_publish.failure"),
            on_skipped=lambda: _bump("writer.dlq_publish.skipped"),
        )
    except asyncpg.exceptions.CheckViolationError as exc:
        # A28: a missing-partition routing failure on the range-
        # partitioned `observations` table (no partition covers this
        # row's occurred_at) raises CheckViolationError with NO
        # constraint name — structurally distinct from a *named* CHECK
        # constraint violation, which carries `constraint_name`. The
        # missing-partition case is PERMANENT: retrying never creates
        # the partition, so the prior transient-classification crash-
        # loops the consumer on the first out-of-range message (e.g. a
        # backfill of historical data older than partition coverage).
        # Route it to the DLQ with an operational diagnostic instead.
        # A *named* CHECK violation keeps the prior transient behavior.
        # See A28 + ticket #44 (partition-coverage extension).
        if exc.constraint_name is not None:
            raise
        _bump("writer.partition_missing")
        occurred = (
            env.occurred_at.isoformat()
            if env.occurred_at is not None else "<none>"
        )
        summary = (
            f"partition_missing: occurred_at={occurred} outside partition "
            f"range; observations partitioning may need extension"
        )
        log.warning(
            "writer.partition_missing",
            extra={
                "topic": msg_topic,
                "partition": msg_partition,
                "offset": msg_offset,
                "tenant_id": str(env.tenant_id),
                "occurred_at": occurred,
            },
        )
        await publish_dlq(
            producer=dlq_producer,
            failure_kind="writer.invariant_failure",
            error_summary=summary,
            tenant_id=env.tenant_id,
            source=env.source,
            raw_s3_key=env.raw_s3_key,
            msg_bytes=msg_value,
            error_context={
                "reason": "partition_missing",
                "occurred_at": occurred,
                "table": "observations",
            },
            on_success=lambda: _bump("writer.dlq_publish.success"),
            on_failure=lambda: _bump("writer.dlq_publish.failure"),
            on_skipped=lambda: _bump("writer.dlq_publish.skipped"),
        )
    # Transient errors propagate — consumer loop exits, supervisor
    # restarts, Kafka redelivers from last committed offset.


async def run_writer(config: WriterConfig) -> dict[str, int]:
    """Writer's main loop. Returns a stats dict for tests."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    dlq_producer_cfg = config.dlq_producer_config or ProducerConfig(
        bootstrap_servers=config.bootstrap_servers,
        client_id=f"observation-writer-dlq-{id(config)}",
    )
    dlq_producer = IdempotentProducer(dlq_producer_cfg)
    # By default, reuse the dlq_producer for embedding publishes —
    # one IdempotentProducer can publish to any topic, and we don't
    # want to start two Kafka clients per writer process for one
    # extra topic.
    embedding_producer = config.embedding_producer or dlq_producer

    await dlq_producer.start()
    await consumer.start()
    consumer.subscribe([_NORMALIZED_TOPIC])

    consumed = 0
    try:
        async for msg in consumer:
            consumed += 1
            await _handle_message(
                msg.value,
                config=config,
                dlq_producer=dlq_producer,
                embedding_producer=embedding_producer,
                msg_topic=msg.topic,
                msg_partition=msg.partition,
                msg_offset=msg.offset,
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

    return {"consumed": consumed}


def main() -> None:
    """Synchronous CLI entry. Wires Path A deps from env vars."""
    logging.basicConfig(
        level=os.environ.get("WRITER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    async def _run() -> None:
        dsn = os.environ.get("DATABASE_URL")
        config = WriterConfig(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
            ),
        )
        if dsn is not None:
            pool = await make_writer_pool(dsn)
            config = WriterConfig(
                bootstrap_servers=config.bootstrap_servers,
                consumer_group=config.consumer_group,
                pool=pool,
                tenant_flags=TenantFlags(pool),
                actor_repo=ActorRepo(pool),
                alias_repo=EntityAliasRepo(pool),
                # `embedder` defaults to None — observations land at
                # embedding_pending=TRUE and the M3.2 embedding
                # worker (or M3.3 backlog drainer) picks them up.
                embedder=None,
            )
            try:
                await run_writer(config)
            finally:
                await pool.close()
        else:
            # No DSN — run in pure shadow mode (matches M2.4).
            await run_writer(config)

    asyncio.run(_run())


__all__ = [
    "ShadowWriteEvent",
    "WriterConfig",
    "get_metrics",
    "get_shadow_log",
    "main",
    "make_writer_pool",
    "reset_metrics",
    "reset_shadow_log",
    "run_writer",
]
