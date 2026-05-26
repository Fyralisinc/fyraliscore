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
from services.ingestion.kafka.shutdown import install_shutdown_event, next_or_stop
from services.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from services.ingestion.normalizer.models import NormalizedEnvelope
from services.observations.partitions import ensure_partitions


# Transient-error retry (avoids a tight crash-loop on a brief DB blip).
# Permanent errors are DLQ'd inside `_handle_message`; only transient/
# unknown errors escape to the loop, so retrying them in place is safe
# (the offset is not committed until the message succeeds).
_TRANSIENT_MAX_ATTEMPTS = int(
    os.environ.get("WRITER_TRANSIENT_MAX_ATTEMPTS", "5")
)
_TRANSIENT_BACKOFF_BASE_S = float(
    os.environ.get("WRITER_TRANSIENT_BACKOFF_BASE_SEC", "0.5")
)
_TRANSIENT_BACKOFF_MAX_S = float(
    os.environ.get("WRITER_TRANSIENT_BACKOFF_MAX_SEC", "30")
)


# Ticket #44 — partition self-heal guardrail. `observations` is range-
# partitioned by `occurred_at` and the partition manager is forward-only
# (`services/observations/partitions.py`), so a historical-backfill row
# whose `occurred_at` predates partition coverage routes to no partition
# and asyncpg raises an *unnamed* CheckViolationError. Rather than DLQ
# such rows (silent, success-shaped data loss), the writer creates the
# covering month and retries the insert once — but ONLY when occurred_at
# falls within [now - MAX_BACKFILL_LOOKBACK, now + FUTURE_SKEW]. Outside
# that window the timestamp is treated as corrupt source data and DLQ'd
# deliberately (reason="out_of_bounds_occurred_at") with NO partition
# created, so a bad far-future / pre-historic value can't spawn a
# pathological partition.
_PARTITION_MAX_BACKFILL_LOOKBACK_DAYS = int(
    os.environ.get("WRITER_PARTITION_MAX_BACKFILL_LOOKBACK_DAYS", "3660")  # ~10y
)
_PARTITION_FUTURE_SKEW_DAYS = int(
    os.environ.get("WRITER_PARTITION_FUTURE_SKEW_DAYS", "7")
)


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
    # occurred_at AND the row could not be self-healed (residual fallback;
    # see ticket #44).
    "writer.partition_missing": 0.0,
    # Ticket #44 — missing partition was auto-created and the row inserted
    # on retry (the self-heal path; replaces the old silent DLQ drop).
    "writer.partition_autocreated": 0.0,
    # Ticket #44 — occurred_at outside the auto-create guardrail window;
    # DLQ'd deliberately as corrupt source data (no partition created).
    "writer.partition_out_of_bounds": 0.0,
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


# Self-heal outcomes (ticket #44). Returned by
# `_attempt_partition_self_heal` so the caller can pick the right DLQ
# reason / metric without re-deriving the guardrail decision.
_HEAL_INSERTED = "inserted"
_HEAL_OUT_OF_BOUNDS = "out_of_bounds"
_HEAL_STILL_MISSING = "still_missing"


async def _attempt_partition_self_heal(
    env: NormalizedEnvelope,
    *,
    config: WriterConfig,
    embedding_producer: Any,
) -> str:
    """Ticket #44: heal a missing-partition write by creating the
    covering month and retrying the insert once.

    Returns one of:
      `_HEAL_INSERTED`      — partition ensured and the row was written.
      `_HEAL_OUT_OF_BOUNDS` — `occurred_at` is missing or outside the
                              guardrail window; NO partition was created
                              and the caller should DLQ it deliberately.
      `_HEAL_STILL_MISSING` — the retry still hit an unnamed
                              CheckViolation after creating the partition
                              (should not happen in practice); the caller
                              DLQs as `partition_missing`.

    Permanent (ValidationError / HandlerNotFound / PayloadTooLarge) and
    transient errors raised by the retried write propagate unchanged —
    transient ones reach `_handle_message_with_retry` and are retried.
    """
    occurred = env.occurred_at
    if occurred is None:
        return _HEAL_OUT_OF_BOUNDS
    if occurred.tzinfo is None:
        occurred = occurred.replace(tzinfo=dt.timezone.utc)
    now = dt.datetime.now(tz=dt.timezone.utc)
    too_old = now - dt.timedelta(days=_PARTITION_MAX_BACKFILL_LOOKBACK_DAYS)
    too_new = now + dt.timedelta(days=_PARTITION_FUTURE_SKEW_DAYS)
    if occurred < too_old or occurred > too_new:
        return _HEAL_OUT_OF_BOUNDS

    # Create exactly the covering month (months_ahead=0). Idempotent via
    # CREATE TABLE IF NOT EXISTS, and runs in its own connection/
    # transaction — the failed first INSERT already rolled back and
    # released its connection. Tolerate a concurrent writer winning the
    # race and surfacing DuplicateTableError despite IF NOT EXISTS.
    try:
        created = await ensure_partitions(
            config.pool, as_of=occurred.date(), months_ahead=0,
        )
    except asyncpg.exceptions.DuplicateTableError:
        created = []  # another writer created it first — treat as success
    if created:
        log.info(
            "writer.partition_autocreate",
            extra={"created": created, "occurred_at": occurred.isoformat()},
        )

    # Retry the write once in a fresh transaction.
    try:
        await _full_mode_write(
            env,
            pool=config.pool,
            actor_repo=config.actor_repo,
            alias_repo=config.alias_repo,
            embedder=config.embedder,
            embedding_producer=embedding_producer,
        )
    except asyncpg.exceptions.CheckViolationError as exc:
        if exc.constraint_name is not None:
            raise
        return _HEAL_STILL_MISSING
    return _HEAL_INSERTED


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
        # A28 / ticket #44: an *unnamed* CheckViolationError on the range-
        # partitioned `observations` table means no partition covers this
        # row's occurred_at (the implicit partition-routing constraint
        # carries no name; a *named* CHECK violation does carry one and
        # stays on the transient re-raise path). Historical-backfill rows
        # carry their original event time, which is older than the
        # forward-only partition window — so instead of DLQ-ing them
        # (silent, success-shaped data loss), self-heal: create the
        # covering month and retry the insert once, bounded by a guardrail
        # so corrupt timestamps don't spawn pathological partitions.
        if exc.constraint_name is not None:
            raise
        occurred = (
            env.occurred_at.isoformat()
            if env.occurred_at is not None else "<none>"
        )
        try:
            status = await _attempt_partition_self_heal(
                env, config=config, embedding_producer=embedding_producer,
            )
        except (ValidationError, HandlerNotFound, PayloadTooLarge) as exc2:
            # A permanent error surfaced only on the retry (not expected,
            # since validation precedes the INSERT in ingest_from_draft).
            # DLQ it like any other full-mode permanent failure.
            _bump("writer.full_mode_failures")
            log.warning(
                "writer.full_mode_permanent_failure",
                extra={
                    "topic": msg_topic,
                    "partition": msg_partition,
                    "offset": msg_offset,
                    "tenant_id": str(env.tenant_id),
                    "error_type": type(exc2).__name__,
                    "error": str(exc2)[:200],
                },
            )
            await publish_dlq(
                producer=dlq_producer,
                failure_kind="writer.full_mode_permanent_failure",
                error_summary=f"{type(exc2).__name__}: {str(exc2)[:200]}",
                tenant_id=env.tenant_id,
                source=env.source,
                raw_s3_key=env.raw_s3_key,
                msg_bytes=msg_value,
                on_success=lambda: _bump("writer.dlq_publish.success"),
                on_failure=lambda: _bump("writer.dlq_publish.failure"),
                on_skipped=lambda: _bump("writer.dlq_publish.skipped"),
            )
            return

        if status == _HEAL_INSERTED:
            _bump("writer.partition_autocreated")
            log.info(
                "writer.partition_autocreated",
                extra={
                    "topic": msg_topic,
                    "partition": msg_partition,
                    "offset": msg_offset,
                    "tenant_id": str(env.tenant_id),
                    "occurred_at": occurred,
                },
            )
            return

        # status is out_of_bounds (deliberate) or still_missing (residual
        # fallback) — DLQ with the matching reason/metric. No partition
        # was created for the out_of_bounds case.
        if status == _HEAL_OUT_OF_BOUNDS:
            _bump("writer.partition_out_of_bounds")
            reason = "out_of_bounds_occurred_at"
            summary = (
                f"out_of_bounds_occurred_at: occurred_at={occurred} outside "
                f"the partition auto-create window "
                f"[-{_PARTITION_MAX_BACKFILL_LOOKBACK_DAYS}d, "
                f"+{_PARTITION_FUTURE_SKEW_DAYS}d]; treated as corrupt "
                f"source data (no partition created)"
            )
        else:
            _bump("writer.partition_missing")
            reason = "partition_missing"
            summary = (
                f"partition_missing: occurred_at={occurred} still outside "
                f"partition range after auto-create; observations "
                f"partitioning may need extension"
            )
        log.warning(
            "writer.partition_dlq",
            extra={
                "topic": msg_topic,
                "partition": msg_partition,
                "offset": msg_offset,
                "tenant_id": str(env.tenant_id),
                "occurred_at": occurred,
                "reason": reason,
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
                "reason": reason,
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
    # Ticket #45: SIGTERM/SIGINT sets this; next_or_stop returns None and
    # the loop exits into normal teardown (rc=0) instead of dying mid-poll.
    stop_event = install_shutdown_event()
    # Liveness + metrics surface (opt-in via INGESTION_HEALTH_PORT).
    heartbeat = Heartbeat()
    health = start_health_server(get_metrics=get_metrics, heartbeat=heartbeat)
    ticker = asyncio.ensure_future(run_heartbeat_ticker(heartbeat, stop_event))
    try:
        while True:
            msg = await next_or_stop(consumer, stop_event)
            if msg is None:
                break
            consumed += 1
            await _handle_message_with_retry(
                msg, config=config, dlq_producer=dlq_producer,
                embedding_producer=embedding_producer, stop_event=stop_event,
            )
            await consumer.commit()
            if (
                config.stop_after is not None
                and consumed >= config.stop_after
            ):
                break
    finally:
        ticker.cancel()
        if health is not None:
            health.shutdown()
        await consumer.stop()
        await dlq_producer.stop()

    return {"consumed": consumed}


async def _handle_message_with_retry(
    msg: Any,
    *,
    config: WriterConfig,
    dlq_producer: IdempotentProducer,
    embedding_producer: IdempotentProducer,
    stop_event: asyncio.Event,
) -> None:
    """Run `_handle_message`, retrying transient errors in place with
    bounded exponential backoff.

    Permanent errors (ValidationError / HandlerNotFound / PayloadTooLarge
    / partition-missing) are already DLQ'd and swallowed inside
    `_handle_message`, so anything that escapes here is transient (e.g. a
    brief DB outage). Retrying in place — without committing the offset —
    rides out the blip without a process restart and its reprocessing
    churn. After `_TRANSIENT_MAX_ATTEMPTS` it re-raises so a sustained
    outage still surfaces (the supervisor/orchestrator restarts the
    process, and Kafka redelivers from the last committed offset).
    """
    attempt = 0
    while True:
        try:
            await _handle_message(
                msg.value,
                config=config,
                dlq_producer=dlq_producer,
                embedding_producer=embedding_producer,
                msg_topic=msg.topic,
                msg_partition=msg.partition,
                msg_offset=msg.offset,
            )
            return
        except Exception as exc:  # noqa: BLE001 — transient; bounded retry
            attempt += 1
            _bump("writer.transient_retry")
            if attempt >= _TRANSIENT_MAX_ATTEMPTS or stop_event.is_set():
                _bump("writer.transient_giveup")
                raise
            backoff = min(
                _TRANSIENT_BACKOFF_MAX_S,
                _TRANSIENT_BACKOFF_BASE_S * (2 ** (attempt - 1)),
            )
            log.warning(
                "writer.transient_error_retry",
                extra={
                    "attempt": attempt,
                    "max_attempts": _TRANSIENT_MAX_ATTEMPTS,
                    "backoff_s": backoff,
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:200],
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                },
            )
            # Sleep, but wake immediately on shutdown.
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass


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


if __name__ == "__main__":  # pragma: no cover - process entrypoint
    main()


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
