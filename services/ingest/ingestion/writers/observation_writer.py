"""Contract-only normalized-envelope observation writer.

Every envelope is persisted through ``ingest_from_draft``. Offsets advance
only after a durable write, a dedup hit, or a deliberate DLQ decision. There
is no alternate inline owner or runtime routing flag.
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

from lib.observability.metrics import WRITER_SHADOW_DROP
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
from services.ingest.ingestion.handlers import (
    HandlerNotFound,
    ObservationDraft,
)
from services.ingest.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingest.ingestion.kafka.shutdown import (
    install_shutdown_event,
    next_or_stop,
)
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
_TRANSIENT_MAX_ATTEMPTS = int(os.environ.get("WRITER_TRANSIENT_MAX_ATTEMPTS", "5"))
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
    # BYOC §12 G6 — promote this silent-data-loss log line to a fleet-scraped
    # counter (the in-process `_metrics` dict above resets on restart and is
    # not a real Prometheus family). ingress_kind label lets the control plane
    # alert hard on backfill (invariant violation) vs. tolerate live (the
    # inline path persists those when kafka_path_enabled is FALSE).
    WRITER_SHADOW_DROP.inc(ingress_kind=event.ingress_kind)
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
        artifact_descriptors=list(env.artifact_descriptors),
        source_object=env.source_object,
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
        evidence_context={
            "source": env.source,
            "connector_installation_id": env.connector_installation_id,
            "content_hash": env.content_hash,
            "raw_ingested_at": env.raw_ingested_at,
            "normalized_at": env.normalized_at,
            "ingress_metadata": dict(env.ingress_metadata),
            "idem_hints": dict(env.idem_hints),
            "contract_version": env.envelope_version,
            "connector_version": env.connector_version,
            "parser_version": env.parser_version,
            "normalizer_version": env.normalizer_version,
        },
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
            config.pool,
            as_of=occurred.date(),
            months_ahead=0,
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

    Uses the same transaction-pool settings as the connector workflow pools.
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
    # Durable persistence dependencies. A missing pool is a startup/runtime
    # error; there is no shadow or alternate writer mode.
    pool: asyncpg.Pool | None = None
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

    if config.pool is None:
        raise RuntimeError("contract observation writer requires DATABASE_URL")

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
            env.occurred_at.isoformat() if env.occurred_at is not None else "<none>"
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
                    msg,
                    config=config,
                    dlq_producer=dlq_producer,
                    embedding_producer=embedding_producer,
                    stop_event=stop_event,
                )
                await consumer.commit()
                if config.stop_after is not None and consumed >= config.stop_after:
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
                    if config.stop_after is not None and consumed >= config.stop_after:
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

                if config.stop_after is not None and consumed >= config.stop_after:
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
                    msg.topic,
                    int(msg.partition),
                    int(msg.offset),
                    last_error,
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
                msg.topic,
                int(msg.partition),
                int(msg.offset),
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
                        config,
                        msg,
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
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092",
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
                actor_repo=ActorRepo(pool),
                alias_repo=EntityAliasRepo(pool),
                # `embedder` defaults to None — observations land at
                # embedding_pending=TRUE and the M3.2 embedding
                # worker (or M3.3 backlog drainer) picks them up.
                embedder=None,
                batch_size=int(os.environ.get("WRITER_BATCH_SIZE", "1")),
                batch_timeout_ms=int(os.environ.get("WRITER_BATCH_TIMEOUT_MS", "500")),
                max_batch_concurrency=int(
                    os.environ.get("WRITER_MAX_CONCURRENCY", "1")
                ),
            )
            try:
                await run_writer(config)
            finally:
                await pool.close()
        else:
            raise RuntimeError("contract observation writer requires DATABASE_URL")

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover - process entrypoint
    main()


__all__ = [
    "WriterConfig",
    "get_metrics",
    "main",
    "make_writer_pool",
    "reset_metrics",
    "run_writer",
]
