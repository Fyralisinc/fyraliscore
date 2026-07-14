"""Live worker for summarize-on-ingest large document observations."""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

import asyncpg
import orjson
from aiokafka import AIOKafkaConsumer

from lib.embeddings.mode import write_obs_embeddings
from lib.shared.db import configure_connection_timeouts
from services.domain.triggers import enqueue_trigger
from services.ingest.ingestion.dlq.publish import publish_dlq
from services.ingest.ingestion.embedding.publish import publish_embedding_request
from services.ingest.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingest.ingestion.kafka.shutdown import install_shutdown_event
from services.ingest.ingestion.kafka.topics import consumer_group, subscribe_topics
from services.ingest.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from services.ingest.ingestion.raw_tier.s3 import S3Client
from services.ingest.ingestion.summarization.batch_store import enqueue_batch_item
from services.ingest.ingestion.summarization.llm import (
    LLMSummarizer,
    SummaryResult,
    Summarizer,
    build_default_summarizer,
)
from services.ingest.ingestion.summarization.models import SummarizationEnvelope
from services.ingest.ingestion.summarization.source_text import (
    decode_json_content,
    metadata_from_content,
    source_text_from_content,
    source_text_from_raw_s3,
)
from services.ingest.ingestion.writers.summarization_worker.doc_memory import (
    DocMemoryScope,
    doc_memory_enabled,
    resolve_document_scope,
)
from lib.observability.metrics import (
    DOC_MEMORY_ENRICHED_T1,
    DOC_MEMORY_MINT_FAILURE,
    DOC_MEMORY_SCOPE_UNRESOLVED,
    doc_memory_source_label,
)


log = logging.getLogger(__name__)

_CONSUMER_GROUP = "ingestion-summarizer"


_metrics: dict[str, float] = {
    "summarization_worker.messages_consumed": 0.0,
    "summarization_worker.envelope_parse_failure": 0.0,
    "summarization_worker.observation_missing": 0.0,
    "summarization_worker.guard_no_op": 0.0,
    "summarization_worker.summaries_succeeded": 0.0,
    "summarization_worker.summaries_failed": 0.0,
    "summarization_worker.dlq_publish.success": 0.0,
    "summarization_worker.dlq_publish.failure": 0.0,
    "summarization_worker.dlq_publish.skipped": 0.0,
    "summarization_worker.embedding_publish.success": 0.0,
    "summarization_worker.embedding_publish.failure": 0.0,
    # Document-memory Layer 2 (Phase 1, INGEST_DOC_MEMORY_ENABLED).
    "summarization_worker.doc_memory.scope_resolved": 0.0,
    "summarization_worker.doc_memory.scope_failed": 0.0,
}


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for key in _metrics:
        _metrics[key] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


_SELECT_SQL = """
SELECT source_channel, content, content_text, occurred_at, kind, trust_tier,
       actor_id, embedding_pending, entities_mentioned
  FROM observations
 WHERE id = $1
 LIMIT 1
"""


_UPDATE_SQL = """
UPDATE observations
   SET content_text = $1,
       content = $2::jsonb,
       embedding = NULL,
       embedding_pending = TRUE
 WHERE id = $3
   AND COALESCE(content->'summarization'->>'status', '') <> 'complete'
 RETURNING source_channel, content_text, occurred_at, kind, trust_tier, actor_id
"""


# Variant of `_UPDATE_SQL` that also rewrites `entities_mentioned` with the
# Layer-2 re-resolution over the structured summary (document-memory substrate).
# Used only when `INGEST_DOC_MEMORY_ENABLED` is on and re-resolution succeeded.
_UPDATE_WITH_ENTITIES_SQL = """
UPDATE observations
   SET content_text = $1,
       content = $2::jsonb,
       entities_mentioned = $4::jsonb,
       embedding = NULL,
       embedding_pending = TRUE
 WHERE id = $3
   AND COALESCE(content->'summarization'->>'status', '') <> 'complete'
 RETURNING source_channel, content_text, occurred_at, kind, trust_tier, actor_id
"""


# OBS_EMBEDDING_MODE=cutover: embeddings are decommissioned, so leave the row
# embedding-less and unflagged (the T1 trigger re-embeds the summary on demand).
_UPDATE_SQL_NO_EMBED = """
UPDATE observations
   SET content_text = $1,
       content = $2::jsonb,
       embedding = NULL,
       embedding_pending = FALSE
 WHERE id = $3
   AND COALESCE(content->'summarization'->>'status', '') <> 'complete'
 RETURNING source_channel, content_text, occurred_at, kind, trust_tier, actor_id
"""


_UPDATE_WITH_ENTITIES_SQL_NO_EMBED = """
UPDATE observations
   SET content_text = $1,
       content = $2::jsonb,
       entities_mentioned = $4::jsonb,
       embedding = NULL,
       embedding_pending = FALSE
 WHERE id = $3
   AND COALESCE(content->'summarization'->>'status', '') <> 'complete'
 RETURNING source_channel, content_text, occurred_at, kind, trust_tier, actor_id
"""


_FAIL_UPDATE_SQL = """
UPDATE observations
   SET content_text = $1,
       content = $2::jsonb
 WHERE id = $3
   AND COALESCE(content->'summarization'->>'status', '') <> 'complete'
 RETURNING 1
"""


async def _load_observation(
    *,
    pool: asyncpg.Pool,
    env: SummarizationEnvelope,
) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(env.tenant_id),
            )
            row = await conn.fetchrow(_SELECT_SQL, env.observation_id)
    if row is None:
        return None
    data = dict(row)
    data["content"] = decode_json_content(data.get("content"))
    data["entities_mentioned"] = _decode_json_list(data.get("entities_mentioned"))
    return data


def _decode_json_list(value: Any) -> list[dict[str, Any]]:
    """Coerce a jsonb column (str | list | None) to a list of dict refs."""
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return []
        if isinstance(decoded, list):
            return [item for item in decoded if isinstance(item, dict)]
    return []


async def _write_summary_and_enqueue(
    *,
    pool: asyncpg.Pool,
    env: SummarizationEnvelope,
    existing: dict[str, Any],
    result: SummaryResult,
    source_chars: int,
) -> str:
    content = dict(existing["content"])
    summary = dict(content.get("summarization") or {})
    summary.update(
        {
            "status": "complete",
            "completed_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "model": result.model,
            "summary_chars": len(result.summary_text),
            "source_chars": source_chars,
        }
    )
    summary.pop("source_text", None)
    # Retain the structured extraction (decisions/commitments/risks) instead of
    # discarding it after rendering content_text. Feeds the document-memory
    # substrate (docs/plans/document-memory-substrate.md). Live + batch lanes
    # both reach here via apply_summary_to_observation.
    if result.structured is not None:
        summary["structured"] = result.structured
    content["summarization"] = summary
    content["summary_provenance"] = "llm_summarizer"

    # ---- Document-memory Layer 2 (Phase 1, INGEST_DOC_MEMORY_ENABLED) -------
    # Re-resolve entity/actor scope over the RICH structured summary (not the
    # placeholder content_text) and build the enriched-T1 payload Think needs to
    # mint document Models. Strictly failure-isolated: any error here is logged
    # to a metric and the plain summarize+T1 path is taken (§8 failure
    # isolation). Runs BEFORE the write tx so re-resolution reads never share
    # the observation write lock.
    scope = await _maybe_resolve_doc_memory_scope(
        pool=pool,
        env=env,
        existing=existing,
        structured=result.structured,
    )

    update_sql = _UPDATE_SQL if write_obs_embeddings() else _UPDATE_SQL_NO_EMBED
    update_args: tuple[Any, ...] = (
        result.summary_text,
        json.dumps(content),
        env.observation_id,
    )
    if scope is not None:
        update_sql = (
            _UPDATE_WITH_ENTITIES_SQL
            if write_obs_embeddings()
            else _UPDATE_WITH_ENTITIES_SQL_NO_EMBED
        )
        update_args = (
            result.summary_text,
            json.dumps(content),
            env.observation_id,
            json.dumps(scope.entities_mentioned),
        )

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(env.tenant_id),
            )
            updated = await conn.fetchrow(update_sql, *update_args)
            if updated is None:
                _bump("summarization_worker.guard_no_op")
                return "guard_no_op"
            payload: dict[str, Any] = {
                "source_channel": updated["source_channel"],
                "kind": updated["kind"],
                "trust_tier": updated["trust_tier"],
                "seed_occurred_at": updated["occurred_at"].isoformat(),
                "seed_natural_text": (updated["content_text"] or "")[:2000],
                "scope_actors": (
                    [str(updated["actor_id"])] if updated["actor_id"] else []
                ),
                "summarized": True,
            }
            if scope is not None:
                _enrich_t1_payload(
                    payload,
                    scope,
                    result.structured,
                    source_channel=updated["source_channel"],
                )
            await enqueue_trigger(
                conn,
                tenant_id=env.tenant_id,
                trigger_kind="T1",
                trigger_subkind="event_arrival",
                observation_id=env.observation_id,
                payload=payload,
            )
    _bump("summarization_worker.summaries_succeeded")
    return "summarized"


async def _maybe_resolve_doc_memory_scope(
    *,
    pool: asyncpg.Pool,
    env: SummarizationEnvelope,
    existing: dict[str, Any],
    structured: dict[str, Any] | None,
) -> DocMemoryScope | None:
    """Re-resolve document scope when Layer 2 is enabled and structured exists.

    Returns ``None`` (so the plain path is taken) when the flag is off, there is
    no structured payload, or re-resolution raised — re-resolution failure must
    NEVER fail the summary (§8).
    """
    if not doc_memory_enabled() or not isinstance(structured, dict) or not structured:
        return None
    try:
        return await resolve_document_scope(
            pool=pool,
            tenant_id=env.tenant_id,
            source_channel=existing.get("source_channel") or env.source,
            structured=structured,
            existing_entities=existing.get("entities_mentioned") or [],
            actor_id=existing.get("actor_id"),
        )
    except Exception as exc:  # noqa: BLE001 — failure isolation (§8)
        _bump("summarization_worker.doc_memory.scope_failed")
        DOC_MEMORY_MINT_FAILURE.inc(
            source=doc_memory_source_label(
                existing.get("source_channel") or env.source
            )
        )
        log.warning(
            "summarization_worker.doc_memory.scope_failed",
            extra={
                "tenant_id": str(env.tenant_id),
                "observation_id": str(env.observation_id),
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )
        return None


def _enrich_t1_payload(
    payload: dict[str, Any],
    scope: DocMemoryScope,
    structured: dict[str, Any] | None,
    *,
    source_channel: str | None = None,
) -> None:
    """Carry the structured extraction + re-resolved scope onto the T1 payload.

    Think's context builder reads ``doc_structured_summary`` to recognize a
    document evidence block, and merges ``doc_scope_entities`` /
    ``doc_scope_actors`` into the Models it mints (§4.2–§4.4). ``scope_actors``
    is widened with the resolved doc actors; unresolved owners ride as text.

    Observability (Phase 2, §7 step 12): this is the worker-side mint DISPATCH —
    a structured document is handed to Think to mint document Models from — so it
    bumps ``doc_memory_enriched_t1_total``. This is NOT the mint count: under the
    ratified Option A, Think (not this worker) mints the Models later, counted by
    ``doc_memory_models_minted_total`` at Think's apply site. When re-resolution
    produced no scoped recall surface (no resolved entities AND no resolved
    actors), the document Models will fall back to semantic-only recall, counted
    by ``doc_memory_scope_unresolved_total`` (§10 scope-unresolved rate).
    """
    _bump("summarization_worker.doc_memory.scope_resolved")
    source = doc_memory_source_label(source_channel)
    DOC_MEMORY_ENRICHED_T1.inc(source=source)
    if not scope.scope_entities and not scope.scope_actors:
        DOC_MEMORY_SCOPE_UNRESOLVED.inc(source=source)
    if structured:
        payload["doc_structured_summary"] = structured
    if scope.scope_entities:
        payload["doc_scope_entities"] = scope.scope_entities
        # Surface resolved entity refs as seed entities so Pathway A can scope
        # the retrieval that mints the document Models.
        payload["seed_entity_ids"] = scope.scope_entities
    if scope.scope_actors:
        payload["doc_scope_actors"] = scope.scope_actors
        merged = list(dict.fromkeys([*payload.get("scope_actors", []), *scope.scope_actors]))
        payload["scope_actors"] = merged
    if scope.unresolved_actor_refs:
        payload["doc_unresolved_actor_refs"] = scope.unresolved_actor_refs


def _failure_content_text(content: dict[str, Any], error_type: str, error: str) -> str:
    title = None
    for key in ("name", "title", "file_name"):
        value = content.get(key)
        if isinstance(value, str) and value.strip():
            title = value.strip()
            break
    prefix = f"Document '{title}'" if title else "Document"
    return f"{prefix} summarization failed: {error_type}: {error[:200]}"


async def _mark_summary_failed(
    *,
    pool: asyncpg.Pool,
    env: SummarizationEnvelope,
    existing: dict[str, Any],
    error_type: str,
    error: str,
    raw_s3_key: str | None,
) -> None:
    content = dict(existing["content"])
    summary = dict(content.get("summarization") or {})
    summary.update(
        {
            "status": "failed",
            "failed_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "error_type": error_type,
            "error": error[:500],
        }
    )
    if raw_s3_key:
        summary["raw_s3_key"] = raw_s3_key
    content["summarization"] = summary
    content_text = _failure_content_text(content, error_type, error)

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(env.tenant_id),
            )
            updated = await conn.fetchrow(
                _FAIL_UPDATE_SQL,
                content_text,
                json.dumps(content),
                env.observation_id,
            )
    if updated is None:
        _bump("summarization_worker.guard_no_op")


async def apply_summary_to_observation(
    *,
    env: SummarizationEnvelope,
    pool: asyncpg.Pool,
    result: SummaryResult,
    source_chars: int,
) -> str:
    existing = await _load_observation(pool=pool, env=env)
    if existing is None:
        _bump("summarization_worker.observation_missing")
        return "observation_missing"
    return await _write_summary_and_enqueue(
        pool=pool,
        env=env,
        existing=existing,
        result=result,
        source_chars=source_chars,
    )


async def summarize_and_update(
    *,
    env: SummarizationEnvelope,
    pool: asyncpg.Pool,
    summarizer: Summarizer,
    dlq_producer: IdempotentProducer,
    embedding_producer: IdempotentProducer | None = None,
    s3: S3Client | None = None,
) -> str:
    existing = await _load_observation(pool=pool, env=env)
    if existing is None:
        _bump("summarization_worker.observation_missing")
        return "observation_missing"

    content: dict[str, Any] = existing["content"]
    summary_meta = content.get("summarization")
    if not isinstance(summary_meta, dict) or summary_meta.get("status") == "complete":
        _bump("summarization_worker.guard_no_op")
        return "guard_no_op"

    raw_key = env.raw_s3_key or summary_meta.get("raw_s3_key")
    source_text = await source_text_from_raw_s3(
        s3, raw_key if isinstance(raw_key, str) else None,
    )
    if not source_text:
        source_text = source_text_from_content(content, existing["content_text"])
    if not source_text:
        await _mark_summary_failed(
            pool=pool,
            env=env,
            existing=existing,
            error_type="SourceTextMissing",
            error="No source text available for document summarization",
            raw_s3_key=raw_key if isinstance(raw_key, str) else None,
        )
        await publish_dlq(
            producer=dlq_producer,
            failure_kind="summarization.llm_failure",
            error_summary="No source text available for document summarization",
            tenant_id=env.tenant_id,
            source=env.source,
            raw_s3_key=raw_key if isinstance(raw_key, str) else None,
            error_context={"observation_id": str(env.observation_id)},
            on_success=lambda: _bump("summarization_worker.dlq_publish.success"),
            on_failure=lambda: _bump("summarization_worker.dlq_publish.failure"),
            on_skipped=lambda: _bump("summarization_worker.dlq_publish.skipped"),
        )
        _bump("summarization_worker.summaries_failed")
        return "summarize_failed"

    try:
        result = await summarizer.summarize(
            source_text,
            metadata=metadata_from_content(
                content=content,
                source_channel=existing["source_channel"],
            ),
        )
    except Exception as exc:  # noqa: BLE001
        _bump("summarization_worker.summaries_failed")
        log.warning(
            "summarization_worker.summarize_failed",
            extra={
                "tenant_id": str(env.tenant_id),
                "observation_id": str(env.observation_id),
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )
        await _mark_summary_failed(
            pool=pool,
            env=env,
            existing=existing,
            error_type=type(exc).__name__,
            error=str(exc),
            raw_s3_key=raw_key if isinstance(raw_key, str) else None,
        )
        await publish_dlq(
            producer=dlq_producer,
            failure_kind="summarization.llm_failure",
            error_summary=f"{type(exc).__name__}: {str(exc)[:200]}",
            tenant_id=env.tenant_id,
            source=env.source,
            raw_s3_key=raw_key if isinstance(raw_key, str) else None,
            error_context={"observation_id": str(env.observation_id)},
            on_success=lambda: _bump("summarization_worker.dlq_publish.success"),
            on_failure=lambda: _bump("summarization_worker.dlq_publish.failure"),
            on_skipped=lambda: _bump("summarization_worker.dlq_publish.skipped"),
        )
        return "summarize_failed"

    status = await _write_summary_and_enqueue(
        pool=pool,
        env=env,
        existing=existing,
        result=result,
        source_chars=len(source_text),
    )
    if (
        status == "summarized"
        and embedding_producer is not None
        and write_obs_embeddings()
    ):
        await publish_embedding_request(
            producer=embedding_producer,
            tenant_id=env.tenant_id,
            source=env.source,
            observation_id=env.observation_id,
            on_success=lambda: _bump("summarization_worker.embedding_publish.success"),
            on_failure=lambda: _bump("summarization_worker.embedding_publish.failure"),
        )
    return status


@dataclass
class SummarizationWorkerConfig:
    bootstrap_servers: str = "localhost:9092"
    consumer_group: str = _CONSUMER_GROUP
    source: str | None = None
    postgres_pool_size: int = 5
    max_concurrency: int = 1
    stop_after: int | None = None
    poll_timeout_ms: int = 500
    dlq_producer_config: ProducerConfig | None = None
    s3_endpoint_url: str | None = None
    s3_bucket: str = "fyralis-raw"
    s3_region_name: str = "auto"
    batch_backfill_enabled: bool = True


async def queue_batch_summarization(
    *,
    env: SummarizationEnvelope,
    pool: asyncpg.Pool,
) -> str:
    queued = await enqueue_batch_item(pool=pool, env=env)
    return "batch_queued" if queued else "batch_duplicate"


async def run_summarization_worker(
    config: SummarizationWorkerConfig,
    pool: asyncpg.Pool,
    *,
    summarizer: Summarizer | None = None,
    s3: S3Client | None = None,
) -> dict[str, int]:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=consumer_group(config.consumer_group, config.source),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = IdempotentProducer(
        config.dlq_producer_config or ProducerConfig(
            bootstrap_servers=config.bootstrap_servers,
            client_id=f"summarization-worker-{id(config)}",
        )
    )
    summarizer_obj: Summarizer | None = summarizer
    own_s3 = s3 is None
    s3_obj = s3 or S3Client(
        config.s3_bucket,
        endpoint_url=config.s3_endpoint_url,
        region_name=config.s3_region_name,
    )

    await consumer.start()
    await producer.start()
    await s3_obj.connect()
    consumer.subscribe(subscribe_topics("summarization", config.source))

    consumed = 0
    summarized = 0
    stop_event = install_shutdown_event()
    heartbeat = Heartbeat()
    health = start_health_server(get_metrics=get_metrics, heartbeat=heartbeat)
    ticker = asyncio.ensure_future(run_heartbeat_ticker(heartbeat, stop_event))
    try:
        while not stop_event.is_set():
            batches = await consumer.getmany(timeout_ms=config.poll_timeout_ms)
            messages: list[Any] = []
            for partition_msgs in batches.values():
                messages.extend(partition_msgs)
            if not messages:
                if config.stop_after is not None and consumed >= config.stop_after:
                    break
                continue

            consumed += len(messages)
            _bump("summarization_worker.messages_consumed", float(len(messages)))
            sem = asyncio.Semaphore(max(1, config.max_concurrency))

            async def _process_one(msg: Any) -> str | None:
                nonlocal summarizer_obj
                try:
                    env = SummarizationEnvelope.model_validate(
                        orjson.loads(msg.value)
                    )
                except Exception as exc:  # noqa: BLE001
                    _bump("summarization_worker.envelope_parse_failure")
                    log.warning(
                        "summarization_worker.envelope_parse_failed",
                        extra={
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:200],
                        },
                    )
                    return None
                async with sem:
                    try:
                        if (
                            config.batch_backfill_enabled
                            and env.ingress_kind == "backfill"
                        ):
                            return await queue_batch_summarization(
                                env=env,
                                pool=pool,
                            )
                        else:
                            if summarizer_obj is None:
                                summarizer_obj = build_default_summarizer()
                            return await summarize_and_update(
                                env=env,
                                pool=pool,
                                summarizer=summarizer_obj,
                                dlq_producer=producer,
                                embedding_producer=producer,
                                s3=s3_obj,
                            )
                    except Exception as exc:  # noqa: BLE001
                        _bump("summarization_worker.summaries_failed")
                        log.warning(
                            "summarization_worker.unexpected_error",
                            extra={
                                "tenant_id": str(env.tenant_id),
                                "observation_id": str(env.observation_id),
                                "error_type": type(exc).__name__,
                                "error": str(exc)[:200],
                            },
                        )
                        return None

            statuses = await asyncio.gather(*(_process_one(m) for m in messages))
            summarized += sum(1 for s in statuses if s == "summarized")
            await consumer.commit()
            if config.stop_after is not None and consumed >= config.stop_after:
                break
    finally:
        ticker.cancel()
        if health is not None:
            health.shutdown()
        await consumer.stop()
        await producer.stop()
        if own_s3:
            await s3_obj.close()

    return {"consumed": consumed, "summarized": summarized}


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("SUMMARIZATION_WORKER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = SummarizationWorkerConfig(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        source=os.environ.get("INGESTION_SOURCE") or None,
        postgres_pool_size=int(os.environ.get("POSTGRES_POOL_SIZE", "5")),
        max_concurrency=int(os.environ.get("SUMMARIZATION_MAX_CONCURRENCY", "1")),
        s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        s3_bucket=os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
        s3_region_name=os.environ.get("S3_REGION_NAME", "auto"),
        batch_backfill_enabled=(
            os.environ.get("INGEST_SUMMARIZATION_BATCH_BACKFILL", "1")
            .strip()
            .lower()
            not in {"0", "false", "off", "no"}
        ),
    )

    async def _run() -> None:
        pool = await asyncpg.create_pool(
            os.environ["DATABASE_URL"],
            min_size=1,
            max_size=config.postgres_pool_size,
            command_timeout=30.0,
            init=configure_connection_timeouts,
            statement_cache_size=0,
        )
        try:
            await run_summarization_worker(config, pool)
        finally:
            await pool.close()

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover
    main()


__all__ = [
    "LLMSummarizer",
    "SummarizationWorkerConfig",
    "SummaryResult",
    "apply_summary_to_observation",
    "get_metrics",
    "main",
    "queue_batch_summarization",
    "reset_metrics",
    "run_summarization_worker",
    "summarize_and_update",
]
