"""services/ingest/ingestion/writers/observation_writer.py
   — Observation writer with flag-branched full-mode.

History:
  - M2.4: Path B no-op. Consumed `ingestion.normalized`, logged each
    NormalizedEnvelope, appended a `ShadowWriteEvent` to an in-process
    list. NO Postgres write. The inline `ingest()` was the source of
    truth during the 48h zero-divergence soak.
  - M5.2: full-mode transition. Per-envelope the writer reads
    `ingestion.kafka_path_enabled` from `tenant_flags`. When TRUE,
    the writer calls `services.ingest.ingestion.core.ingest_from_draft(...)`
    to write the observation (the normalizer already ran the handler,
    so the draft fields embedded in the envelope are used directly).
    When FALSE, the writer preserves M2's shadow-log no-op behavior.
  - Gate inversion: the default is now kafka-first. The writer reads
    `tenant_flags.kafka_path_enabled()` (shared single-source default,
    `KAFKA_PATH_ENABLED_DEFAULT=True`), so a tenant with NO flag row is
    full-mode. Shadow-only now requires an EXPLICIT FALSE (operator /
    circuit-breaker kill-switch). The ingress readers use the same helper,
    so a publishing ingress can never pair with a shadow-logging writer
    (that split would silently drop observations).

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

The M2 e2e shadow test (`test_e2e_shadow.py`) exercises the shadow-log
path by seeding `ingestion.kafka_path_enabled=FALSE` for its tenants
(under the inverted default a missing row would instead take the
full-mode write path).

============================================================
PER-ENVELOPE TRANSACTION + BATCHED CONSUME CONTRACT
============================================================
Each envelope still gets ONE call to `ingest_from_draft`, which opens
its own transaction and preserves the existing DLQ / partition self-heal
semantics. The writer can now batch-consume Kafka records and process
independent tenant groups concurrently; offsets commit only after the
whole batch reaches a definitive outcome. A crash before commit replays
the batch, and the observation unique key dedups already-written rows.

============================================================
ERROR HANDLING
============================================================
  - Parse failure (NormalizedEnvelope.model_validate raises):
    bump parse_failure, DLQ-publish, COMMIT offset. Same as M2.4.
  - Full-mode permanent error (ValidationError, HandlerNotFound):
    bump full_mode_failure, DLQ-publish, COMMIT offset.
  - Full-mode transient error (any other Exception): retry in place,
    then re-raise so the consumer loop exits, the supervisor restarts
    the writer, and Kafka redelivers from the last committed offset.
    F3: a DETERMINISTIC poison message would otherwise redeliver forever
    (head-of-line-blocking the partition — and any backfill sharing the
    key). A durable, restart-surviving give-up counter
    (`writer_poison_attempts`, keyed by topic/partition/offset) routes the
    message to the DLQ and COMMITs after `_POISON_MAX_DURABLE_ATTEMPTS`
    cross-restart give-ups, so the partition advances.

KILL-SWITCH / BACKFILL EXEMPTION (F1)
============================================================
The `kafka_path_enabled` flag is a LIVE cutover switch. When FALSE, live
ingress falls back to inline `ingest()` and the writer shadow-logs to avoid
double-writing. Backfill has NO inline fallback (`shard_fetch` always
publishes `ingress_kind="backfill"` and advances its cursor on the
broker-ack), so the writer ALWAYS persists backfill envelopes regardless of
the flag — otherwise a flag flip mid-backfill would silently drop rows the
shard cursor has already moved past. Backfill is single-path, so writing it
unconditionally cannot double-write.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer

from lib.shared.backoff import exponential_backoff_seconds
from lib.shared.db import configure_connection_timeouts
from lib.shared.errors import ValidationError
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.ingest.ingestion.core import (
    IngestResult,
    PayloadTooLarge,
    ingest_from_draft,
)
from services.ingest.ingestion.dlq.publish import publish_dlq
from services.ingest.ingestion.feature_flags.client import (
    TenantFlags,
)
from services.ingest.ingestion.handlers import (
    HandlerNotFound,
    ObservationDraft,
)
from services.ingest.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingest.ingestion.kafka.shutdown import install_shutdown_event, next_or_stop
from services.ingest.ingestion.kafka.topics import consumer_group, subscribe_topics
from services.ingest.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from services.ingest.ingestion.normalizer.models import NormalizedEnvelope
from services.ingest.ingestion.payload_validation import validate_ingest_json_payload
from services.domain.observations.partitions import ensure_partitions


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

# F3 — durable, restart-surviving poison cap. The in-process retry above
# resets to 0 on every process restart, so a DETERMINISTIC poison message (a
# code bug — TypeError/KeyError/etc. — that fails identically every time) is
# redelivered forever: retry 5x -> re-raise -> supervisor restarts -> Kafka
# redelivers the SAME uncommitted offset -> repeat. That head-of-line-blocks
# the partition indefinitely, stalling every record behind it on the same key
# — INCLUDING concurrent backfill (live + backfill share the per-tenant key).
# We persist a give-up counter keyed by (topic, partition, offset) that
# survives restarts; after this many cross-restart give-ups the message is
# routed to the DLQ and committed so the partition advances. The cap is set
# HIGH so a genuine sustained infra outage (DB failover) rides out on the
# in-process retry + restart loop and recovers BEFORE the cap, whereas a code
# bug burns through give-ups fast. 0 disables the cap (pure legacy re-raise).
# NOTE: messages DLQ'd during a long (> cap) outage need replay — that is the
# companion drainer (F4), tracked separately.
_POISON_MAX_DURABLE_ATTEMPTS = int(
    os.environ.get("WRITER_POISON_MAX_DURABLE_ATTEMPTS", "50")
)


# Ticket #44 — partition self-heal guardrail. `observations` is range-
# partitioned by `occurred_at` and the partition manager is forward-only
# (`services/domain/observations/partitions.py`), so a historical-backfill row
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
    "writer.batches_consumed": 0.0,
    "writer.batch_messages_consumed": 0.0,
    # F2 — an envelope reached the shadow path and was DROPPED (no row, no
    # DLQ, offset committed). Distinct from the benign `shadow_write_events`
    # (which counts the legacy M2 soak no-op): a SUSTAINED nonzero here means
    # ingress is publishing while the writer shadow-logs — silent data loss /
    # ingress↔writer drift. Alert-worthy.
    "writer.shadow_drop": 0.0,
    # F3 — a transient/unknown-error message exceeded the durable poison cap
    # and was routed to the DLQ so the partition could advance. Any nonzero
    # value warrants a look (a real poison message, or a > cap outage).
    "writer.poison_dlq": 0.0,
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
    # F2 — make the drop LOUD. Reaching the shadow path means this envelope
    # was published to Kafka but the writer will NOT persist it (the flag is
    # FALSE). For LIVE ingress that is by design (the inline path writes it);
    # for BACKFILL it must be IMPOSSIBLE after F1 (backfill is exempt from the
    # kill-switch and always persisted), so a backfill envelope here is a BUG,
    # logged at ERROR as defense-in-depth for F1's invariant.
    is_backfill = event.ingress_kind == "backfill"
    _bump("writer.shadow_drop")
    (log.error if is_backfill else log.warning)(
        "writer.shadow_drop",
        extra={
            "tenant_id": event.tenant_id,
            "source": event.source,
            "ingress_kind": event.ingress_kind,
            "source_channel": event.source_channel,
            "external_id": event.external_id,
            "content_hash_prefix": event.content_hash[:16],
            "reason": (
                "backfill_envelope_in_shadow_path_BUG"
                if is_backfill
                else "live_envelope_dropped_kafka_path_disabled"
            ),
        },
    )


# ---------------------------------------------------------------------
# M5.2 — full-mode draft reconstruction + Postgres write.
# ---------------------------------------------------------------------
def _draft_from_envelope(env: NormalizedEnvelope) -> ObservationDraft:
    """Rebuild the `ObservationDraft` the normalizer (M2.3) emitted.

    `NormalizedEnvelope` carries the draft fields 1:1 (see
    `services/ingest/ingestion/normalizer/models.py`), so we reconstruct
    without re-running the handler. `unresolved_phrases` is left
    empty — the normalizer doesn't surface it on the wire, and
    `ingest_from_draft` re-derives candidate phrases from
    `content_text` in step 4.
    """
    validate_ingest_json_payload(dict(env.content), channel=env.source_channel)
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
    summarization_producer: Any,
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
        summarization_producer=summarization_producer,
        raw_s3_key=env.raw_s3_key,
        ingress_kind=env.ingress_kind,
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
    summarization_producer: Any,
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
        # `created` is a reserved LogRecord attribute (record creation time);
        # putting it in `extra` raises "Attempt to overwrite 'created' in
        # LogRecord" once INFO logging is active and a partition is actually
        # autocreated (the backfill self-heal path). Use a prefixed key — same
        # fix as lib/shared/migrations.py for `filename`/`module`.
        log.info(
            "writer.partition_autocreate",
            extra={"created_partitions": created, "occurred_at": occurred.isoformat()},
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
            summarization_producer=summarization_producer,
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
    `services/ingest/ingestion/feature_flags/circuit_breaker.py::make_breaker_pool`
    and the M4.2 session-state pool at
    `services/ingest/integrations/discord/gateway/session_state.py::make_session_state_pool`.
    """
    return await asyncpg.create_pool(
        dsn,
        min_size=2,
        max_size=max_size,
        command_timeout=command_timeout,
        init=configure_connection_timeouts,
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
    # Source isolation: when set, subscribe ONLY to
    # ingestion.normalized.<source> under group "<consumer_group>.<source>";
    # when None, subscribe to all per-source normalized topics under the
    # bare group (dev fallback). See docs/ingestion/source-isolation.md.
    source: str | None = None
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
    summarization_producer: Any = None
    # Batch-consume normalized messages and process tenant groups
    # concurrently. Defaults preserve the historical serial loop; prod
    # enables this via WRITER_BATCH_SIZE / WRITER_MAX_CONCURRENCY.
    batch_size: int = 1
    batch_timeout_ms: int = 500
    max_batch_concurrency: int = 1


async def _publish_writer_dlq(
    *,
    producer: IdempotentProducer,
    failure_kind: str,
    error_summary: str,
    msg_bytes: bytes,
    tenant_id: Any = None,
    source: str | None = None,
    raw_s3_key: str | None = None,
    error_context: dict[str, Any] | None = None,
) -> None:
    await publish_dlq(
        producer=producer,
        failure_kind=failure_kind,
        error_summary=error_summary,
        tenant_id=tenant_id,
        source=source,
        raw_s3_key=raw_s3_key,
        msg_bytes=msg_bytes,
        error_context=error_context,
        on_success=lambda: _bump("writer.dlq_publish.success"),
        on_failure=lambda: _bump("writer.dlq_publish.failure"),
        on_skipped=lambda: _bump("writer.dlq_publish.skipped"),
    )


async def _handle_parse_failure(
    exc: Exception,
    *,
    dlq_producer: IdempotentProducer,
    msg_value: bytes,
    msg_topic: str,
    msg_partition: int,
    msg_offset: int,
) -> None:
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
    await _publish_writer_dlq(
        producer=dlq_producer,
        failure_kind="writer.invariant_failure",
        error_summary=f"{type(exc).__name__}: {str(exc)[:200]}",
        msg_bytes=msg_value,
    )


async def _handle_full_mode_permanent_failure(
    exc: Exception,
    *,
    env: NormalizedEnvelope,
    dlq_producer: IdempotentProducer,
    msg_value: bytes,
    msg_topic: str,
    msg_partition: int,
    msg_offset: int,
    include_env_fields: bool = False,
) -> None:
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
    # #46: this path historically published the wire kind
    # "writer.full_mode_permanent_failure", which is NOT a member of
    # WireFailureKind. DLQEnvelope construction raised a Pydantic
    # ValidationError that publish_dlq swallowed via on_failure, so
    # full-mode permanent failures were SILENTLY dropped (never reached
    # the DLQ). Use the valid "writer.invariant_failure" kind — the same
    # kind the parse-failure and partition-DLQ paths already use — so the
    # envelope validates and the failure is durably recorded.
    await _publish_writer_dlq(
        producer=dlq_producer,
        failure_kind="writer.invariant_failure",
        error_summary=f"{type(exc).__name__}: {str(exc)[:200]}",
        tenant_id=env.tenant_id if include_env_fields else None,
        source=env.source if include_env_fields else None,
        raw_s3_key=env.raw_s3_key if include_env_fields else None,
        msg_bytes=msg_value,
    )


async def _handle_partition_dlq(
    *,
    status: str,
    occurred: str,
    env: NormalizedEnvelope,
    dlq_producer: IdempotentProducer,
    msg_value: bytes,
    msg_topic: str,
    msg_partition: int,
    msg_offset: int,
) -> None:
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
    await _publish_writer_dlq(
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
    )


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
        await _handle_parse_failure(
            exc,
            dlq_producer=dlq_producer,
            msg_value=msg_value,
            msg_topic=msg_topic,
            msg_partition=msg_partition,
            msg_offset=msg_offset,
        )
        return

    # ---- Flag-branched write ----
    should_full_mode = False
    if config.pool is not None:
        if env.ingress_kind == "backfill":
            # F1 — backfill is ALWAYS persisted, regardless of the kill-switch.
            # The `kafka_path_enabled` flag is a LIVE cutover switch: when an
            # operator / the circuit-breaker flips it FALSE, live ingress falls
            # back to the inline `ingest()` path (so live data is still
            # written) and the writer shadow-logs to avoid double-writing it.
            # Backfill has NO inline fallback — `shard_fetch` publishes
            # `ingress_kind="backfill"` straight to Kafka with no flag check and
            # advances its shard cursor on the broker-ack. So a FALSE flag would
            # make the writer shadow-DROP backfill (no row, no DLQ, offset
            # committed, cursor already past it) — silent, success-shaped,
            # unrecoverable data loss. Backfill is strictly single-path
            # (shard_fetch → Kafka → normalizer → writer; the inline
            # ingest()/ingest_from_draft() paths are live-only), so always
            # writing it here can never double-write.
            should_full_mode = True
        elif config.tenant_flags is not None:
            # Live ingress. Inverted default (kafka-first): a tenant with no
            # flag row is full-mode. Only an explicit FALSE (operator /
            # circuit-breaker kill-switch) keeps the writer in shadow-only mode.
            # MUST read through `kafka_path_enabled()` — the same single-source
            # default the ingress readers use — so the two ends never drift
            # (ingress publishing while the writer shadow-logs would silently
            # drop observations).
            should_full_mode = await config.tenant_flags.kafka_path_enabled(
                env.tenant_id,
            )

    if not should_full_mode:
        await _record_shadow_event(env)
        return

    try:
        summarization_producer = config.summarization_producer or embedding_producer
        await _full_mode_write(
            env,
            pool=config.pool,
            actor_repo=config.actor_repo,
            alias_repo=config.alias_repo,
            embedder=config.embedder,
            embedding_producer=embedding_producer,
            summarization_producer=summarization_producer,
        )
    except (ValidationError, HandlerNotFound, PayloadTooLarge) as exc:
        # Permanent error — DLQ + commit. Same shape as the
        # parse-failure branch.
        await _handle_full_mode_permanent_failure(
            exc,
            env=env,
            dlq_producer=dlq_producer,
            msg_value=msg_value,
            msg_topic=msg_topic,
            msg_partition=msg_partition,
            msg_offset=msg_offset,
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
                env,
                config=config,
                embedding_producer=embedding_producer,
                summarization_producer=summarization_producer,
            )
        except (ValidationError, HandlerNotFound, PayloadTooLarge) as exc2:
            # A permanent error surfaced only on the retry (not expected,
            # since validation precedes the INSERT in ingest_from_draft).
            # DLQ it like any other full-mode permanent failure.
            await _handle_full_mode_permanent_failure(
                exc2,
                env=env,
                dlq_producer=dlq_producer,
                msg_value=msg_value,
                msg_topic=msg_topic,
                msg_partition=msg_partition,
                msg_offset=msg_offset,
                include_env_fields=True,
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
        await _handle_partition_dlq(
            status=status,
            occurred=occurred,
            env=env,
            dlq_producer=dlq_producer,
            msg_value=msg_value,
            msg_topic=msg_topic,
            msg_partition=msg_partition,
            msg_offset=msg_offset,
        )
    # Transient errors propagate — consumer loop exits, supervisor
    # restarts, Kafka redelivers from last committed offset.


async def run_writer(config: WriterConfig) -> dict[str, int]:
    """Writer's main loop. Returns a stats dict for tests."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=consumer_group(config.consumer_group, config.source),
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
    consumer.subscribe(subscribe_topics("normalized", config.source))

    consumed = 0
    # Ticket #45: SIGTERM/SIGINT sets this; next_or_stop returns None and
    # the loop exits into normal teardown (rc=0) instead of dying mid-poll.
    stop_event = install_shutdown_event()
    # Liveness + metrics surface (opt-in via INGESTION_HEALTH_PORT).
    heartbeat = Heartbeat()
    health = start_health_server(get_metrics=get_metrics, heartbeat=heartbeat)
    ticker = asyncio.ensure_future(run_heartbeat_ticker(heartbeat, stop_event))
    try:
        if config.batch_size <= 1 and config.max_batch_concurrency <= 1:
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
        else:
            sem = asyncio.Semaphore(max(1, config.max_batch_concurrency))

            async def _process_group(group: list[Any]) -> None:
                # Concurrency is between tenant groups. Messages within one
                # Kafka key stay serial, preserving the ordering contract for
                # a tenant while independent tenants overlap DB/S3/Kafka I/O.
                async with sem:
                    for m in group:
                        await _handle_message_with_retry(
                            m,
                            config=config,
                            dlq_producer=dlq_producer,
                            embedding_producer=embedding_producer,
                            stop_event=stop_event,
                        )

            while not stop_event.is_set():
                try:
                    batches = await consumer.getmany(
                        timeout_ms=config.batch_timeout_ms,
                        max_records=config.batch_size,
                    )
                except TypeError:
                    # Test fakes in this repo predate aiokafka's
                    # max_records parameter.
                    batches = await consumer.getmany(
                        timeout_ms=config.batch_timeout_ms,
                    )
                messages: list[Any] = []
                for partition_messages in batches.values():
                    messages.extend(partition_messages)
                if not messages:
                    if (
                        config.stop_after is not None
                        and consumed >= config.stop_after
                    ):
                        break
                    continue

                consumed += len(messages)
                _bump("writer.batches_consumed")
                _bump("writer.batch_messages_consumed", float(len(messages)))

                groups: dict[bytes, list[Any]] = defaultdict(list)
                for msg in messages:
                    groups[msg.key or b""].append(msg)

                results = await asyncio.gather(
                    *(_process_group(group) for group in groups.values()),
                    return_exceptions=True,
                )
                first_error = next(
                    (r for r in results if isinstance(r, Exception)),
                    None,
                )
                if first_error is not None:
                    raise first_error

                # Commit only after the entire batch has reached a definitive
                # outcome. A crash before this point replays the batch, and
                # observation-level dedup handles already-inserted rows.
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


# ---------------------------------------------------------------------
# F3 — durable poison-attempt counter (survives process restarts).
# Keyed by Kafka coordinates, NOT tenant — it is infra bookkeeping about a
# stuck log position, so the table carries no tenant_id / RLS (same pattern
# as workflow_states/workflow_signals).
# ---------------------------------------------------------------------
_POISON_BUMP_SQL = """
INSERT INTO writer_poison_attempts (topic, partition, "offset", attempts, last_error)
VALUES ($1, $2, $3, 1, $4)
ON CONFLICT (topic, partition, "offset") DO UPDATE
   SET attempts = writer_poison_attempts.attempts + 1,
       last_seen_at = now(),
       last_error = EXCLUDED.last_error
RETURNING attempts
"""

_POISON_CLEAR_SQL = """
DELETE FROM writer_poison_attempts
 WHERE topic = $1 AND partition = $2 AND "offset" = $3
"""


async def _bump_durable_poison_attempts(
    config: WriterConfig,
    msg: Any,
    *,
    last_error: str,
) -> int:
    """Increment and return the restart-surviving give-up count for this
    message's (topic, partition, offset). Best-effort: a counter-store outage
    returns 0 so the caller falls through to the legacy re-raise — we NEVER
    DLQ a message because the bookkeeping itself failed.
    """
    if config.pool is None or _POISON_MAX_DURABLE_ATTEMPTS <= 0:
        return 0
    try:
        async with config.pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    _POISON_BUMP_SQL,
                    msg.topic, int(msg.partition), int(msg.offset), last_error,
                )
            )
    except Exception as exc:  # noqa: BLE001 — bookkeeping must never crash the writer
        log.warning(
            "writer.poison_counter_bump_failed",
            extra={
                "topic": getattr(msg, "topic", None),
                "partition": getattr(msg, "partition", None),
                "offset": getattr(msg, "offset", None),
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )
        return 0


async def _clear_durable_poison_attempts(config: WriterConfig, msg: Any) -> None:
    """Drop the poison counter row once a message finally succeeds or is DLQ'd,
    so a transient blip that later recovers doesn't accrue toward the cap. Best-
    effort; a stale row is harmless (its offset is committed and never recurs,
    and the last_seen_at janitor index backs up cleanup).
    """
    if config.pool is None or _POISON_MAX_DURABLE_ATTEMPTS <= 0:
        return
    try:
        async with config.pool.acquire() as conn:
            await conn.execute(
                _POISON_CLEAR_SQL,
                msg.topic, int(msg.partition), int(msg.offset),
            )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "writer.poison_counter_clear_failed",
            extra={
                "topic": getattr(msg, "topic", None),
                "error_type": type(exc).__name__,
            },
        )


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
            if attempt > 0:
                # Recovered after in-process retries — clear any durable poison
                # counter so a transient blip doesn't carry toward the cap.
                await _clear_durable_poison_attempts(config, msg)
            return
        except Exception as exc:  # noqa: BLE001 — transient; bounded retry
            attempt += 1
            _bump("writer.transient_retry")
            if attempt >= _TRANSIENT_MAX_ATTEMPTS or stop_event.is_set():
                _bump("writer.transient_giveup")
                # F3 — a shutdown-driven give-up is NOT poison: re-raise so the
                # loop exits cleanly and the offset replays on restart. Only a
                # non-shutdown give-up counts toward the durable poison cap.
                if not stop_event.is_set():
                    durable = await _bump_durable_poison_attempts(
                        config, msg,
                        last_error=f"{type(exc).__name__}: {str(exc)[:200]}",
                    )
                    if 0 < _POISON_MAX_DURABLE_ATTEMPTS <= durable:
                        # Deterministic poison: redelivered past the cap across
                        # restarts. Park to DLQ and return so the caller commits
                        # and the partition (incl. any backfill behind it on the
                        # same key) advances instead of jamming forever.
                        _bump("writer.poison_dlq")
                        log.error(
                            "writer.poison_dlq",
                            extra={
                                "topic": msg.topic,
                                "partition": msg.partition,
                                "offset": msg.offset,
                                "durable_attempts": durable,
                                "error_type": type(exc).__name__,
                                "error": str(exc)[:200],
                            },
                        )
                        await _publish_writer_dlq(
                            producer=dlq_producer,
                            failure_kind="writer.invariant_failure",
                            error_summary=(
                                f"transient_poison: {type(exc).__name__}: "
                                f"{str(exc)[:200]} (durable_attempts={durable})"
                            ),
                            msg_bytes=msg.value,
                            error_context={
                                "reason": "transient_poison",
                                "durable_attempts": durable,
                                "topic": msg.topic,
                                "partition": msg.partition,
                                "offset": msg.offset,
                            },
                        )
                        await _clear_durable_poison_attempts(config, msg)
                        return  # caller commits → partition advances
                raise
            backoff = exponential_backoff_seconds(
                attempt,
                base_seconds=_TRANSIENT_BACKOFF_BASE_S,
                cap_seconds=_TRANSIENT_BACKOFF_MAX_S,
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
        source = os.environ.get("INGESTION_SOURCE") or None
        # WRITER_CONSUMER_GROUP overrides the shared group id — see the
        # normalizer's NORMALIZER_CONSUMER_GROUP note: a concurrent stack on
        # the same broker would otherwise split the normalized partitions and
        # steal a tenant's observations into the wrong DB.
        group = os.environ.get("WRITER_CONSUMER_GROUP", _WRITER_GROUP)
        config = WriterConfig(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
            ),
            consumer_group=group,
            source=source,
        )
        if dsn is not None:
            # Source isolation: per-source writers each own their pool,
            # so the size is a per-source DB budget. Tunable so a noisy
            # source can be given headroom without touching the others.
            pool = await make_writer_pool(
                dsn,
                max_size=int(os.environ.get("WRITER_POSTGRES_POOL_SIZE", "10")),
            )
            config = WriterConfig(
                bootstrap_servers=config.bootstrap_servers,
                consumer_group=config.consumer_group,
                source=source,
                pool=pool,
                tenant_flags=TenantFlags(pool),
                actor_repo=ActorRepo(pool),
                alias_repo=EntityAliasRepo(pool),
                # `embedder` defaults to None — observations land at
                # embedding_pending=TRUE and the M3.2 embedding
                # worker (or M3.3 backlog drainer) picks them up.
                embedder=None,
                batch_size=int(os.environ.get("WRITER_BATCH_SIZE", "1")),
                batch_timeout_ms=int(
                    os.environ.get("WRITER_BATCH_TIMEOUT_MS", "500")
                ),
                max_batch_concurrency=int(
                    os.environ.get("WRITER_MAX_CONCURRENCY", "1")
                ),
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
