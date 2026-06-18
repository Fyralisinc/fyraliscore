"""Batch API worker for summarize-on-ingest backfill documents."""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from services.ingest.ingestion.dlq.publish import publish_dlq
from services.ingest.ingestion.embedding.publish import publish_embedding_request
from services.ingest.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingest.ingestion.kafka.shutdown import install_shutdown_event
from services.ingest.ingestion.observability import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from services.ingest.ingestion.raw_tier.s3 import S3Client
from services.ingest.ingestion.summarization.batch_api import (
    BatchClient,
    OpenAIBatchClient,
    build_batch_request_line,
    parse_batch_output_line,
)
from services.ingest.ingestion.summarization.batch_store import mark_batch_item_failed
from services.ingest.ingestion.summarization.models import SummarizationEnvelope
from services.ingest.ingestion.summarization.source_text import (
    decode_json_content,
    metadata_from_content,
    source_text_from_content,
    source_text_from_raw_s3,
)
from services.ingest.ingestion.writers.summarization_worker.summarization_worker import (
    apply_summary_to_observation,
)


log = logging.getLogger(__name__)


_metrics: dict[str, float] = {
    "summarization_batch.iterations": 0.0,
    "summarization_batch.items_claimed": 0.0,
    "summarization_batch.items_submitted": 0.0,
    "summarization_batch.items_completed": 0.0,
    "summarization_batch.items_failed": 0.0,
    "summarization_batch.jobs_submitted": 0.0,
    "summarization_batch.jobs_polled": 0.0,
    "summarization_batch.dlq_publish.success": 0.0,
    "summarization_batch.dlq_publish.failure": 0.0,
    "summarization_batch.dlq_publish.skipped": 0.0,
    "summarization_batch.embedding_publish.success": 0.0,
    "summarization_batch.embedding_publish.failure": 0.0,
}


def get_metrics() -> dict[str, float]:
    return dict(_metrics)


def reset_metrics() -> None:
    for key in _metrics:
        _metrics[key] = 0.0


def _bump(key: str, by: float = 1.0) -> None:
    _metrics[key] = _metrics.get(key, 0.0) + by


@dataclass
class SummarizationBatchWorkerConfig:
    postgres_pool_size: int = 5
    batch_size: int = 50
    poll_limit: int = 10
    idle_sleep_seconds: float = 5.0
    submit_max_attempts: int = 3
    stop_after_iterations: int | None = None
    dlq_producer_config: ProducerConfig | None = None
    bootstrap_servers: str = "localhost:9092"
    s3_endpoint_url: str | None = None
    s3_bucket: str = "fyralis-raw"
    s3_region_name: str = "auto"


_CLAIM_SQL = """
WITH picked AS (
    SELECT id
      FROM summarization_batch_items
     WHERE status = 'queued'
     ORDER BY queued_at ASC
     LIMIT $1
     FOR UPDATE SKIP LOCKED
)
UPDATE summarization_batch_items i
   SET status = 'submitting',
       attempts = attempts + 1,
       updated_at = now()
  FROM picked
 WHERE i.id = picked.id
RETURNING i.id, i.tenant_id, i.source, i.observation_id, i.raw_s3_key,
          i.ingress_kind, i.custom_id, i.attempts
"""

_LOAD_OBSERVATION_SQL = """
SELECT source_channel, content, content_text
  FROM observations
 WHERE id = $1
 LIMIT 1
"""

_INSERT_JOB_SQL = """
INSERT INTO summarization_batch_jobs (
    provider_batch_id, input_file_id, status, item_count, metadata
) VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT (provider_batch_id) DO UPDATE
   SET input_file_id = EXCLUDED.input_file_id,
       status = EXCLUDED.status,
       item_count = EXCLUDED.item_count,
       metadata = EXCLUDED.metadata,
       updated_at = now()
RETURNING id
"""

_MARK_SUBMITTED_SQL = """
UPDATE summarization_batch_items
   SET status = 'submitted',
       job_id = $2,
       source_chars = $3,
       submitted_at = now(),
       updated_at = now()
 WHERE id = $1
"""

_RESET_CLAIM_SQL = """
UPDATE summarization_batch_items
   SET status = CASE WHEN attempts >= $3 THEN 'failed' ELSE 'queued' END,
       last_error = $2,
       completed_at = CASE WHEN attempts >= $3 THEN now() ELSE completed_at END,
       updated_at = now()
 WHERE id = ANY($1::uuid[])
"""

_SELECT_JOBS_SQL = """
SELECT id, provider_batch_id, status
  FROM summarization_batch_jobs
 WHERE status IN ('submitted', 'validating', 'in_progress', 'finalizing', 'cancelling')
 ORDER BY submitted_at ASC
 LIMIT $1
"""

_UPDATE_JOB_SQL = """
UPDATE summarization_batch_jobs
   SET status = $2,
       input_file_id = COALESCE($3, input_file_id),
       output_file_id = $4,
       error_file_id = $5,
       error_context = COALESCE($6::jsonb, error_context),
       last_polled_at = now(),
       completed_at = CASE
           WHEN $2 IN ('completed', 'failed', 'expired', 'cancelled') THEN now()
           ELSE completed_at
       END,
       updated_at = now()
 WHERE id = $1
"""

_ITEM_BY_CUSTOM_ID_SQL = """
SELECT id, tenant_id, source, observation_id, raw_s3_key, ingress_kind,
       source_chars, status
  FROM summarization_batch_items
 WHERE custom_id = $1
 LIMIT 1
"""

_ITEMS_FOR_JOB_SQL = """
SELECT id, tenant_id, source, observation_id, raw_s3_key, ingress_kind
  FROM summarization_batch_items
 WHERE job_id = $1
   AND status = 'submitted'
"""

_MARK_COMPLETED_SQL = """
UPDATE summarization_batch_items
   SET status = 'completed',
       completed_at = now(),
       updated_at = now()
 WHERE id = $1
"""


def _env_from_item(item: asyncpg.Record | dict[str, Any]) -> SummarizationEnvelope:
    return SummarizationEnvelope(
        tenant_id=item["tenant_id"],
        source=item["source"],
        observation_id=item["observation_id"],
        raw_s3_key=item["raw_s3_key"],
        ingress_kind=item["ingress_kind"],
        enqueued_at=dt.datetime.now(tz=dt.timezone.utc),
    )


async def _load_observation_for_item(
    pool: asyncpg.Pool,
    item: asyncpg.Record,
) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(item["tenant_id"]),
            )
            row = await conn.fetchrow(_LOAD_OBSERVATION_SQL, item["observation_id"])
    if row is None:
        return None
    data = dict(row)
    data["content"] = decode_json_content(data.get("content"))
    return data


async def _source_text_for_item(
    *,
    pool: asyncpg.Pool,
    s3: S3Client,
    item: asyncpg.Record,
) -> tuple[str | None, dict[str, Any] | None]:
    existing = await _load_observation_for_item(pool, item)
    if existing is None:
        return None, None
    content = existing["content"]
    summary_meta = content.get("summarization")
    raw_key = item["raw_s3_key"]
    if isinstance(summary_meta, dict):
        raw_key = raw_key or summary_meta.get("raw_s3_key")
    source_text = await source_text_from_raw_s3(
        s3,
        raw_key if isinstance(raw_key, str) else None,
    )
    if not source_text:
        source_text = source_text_from_content(content, existing["content_text"])
    metadata = metadata_from_content(
        content=content,
        source_channel=existing["source_channel"],
    )
    return source_text, metadata


async def _publish_failure_dlq(
    *,
    producer: IdempotentProducer,
    item: asyncpg.Record | dict[str, Any],
    error_summary: str,
) -> None:
    await publish_dlq(
        producer=producer,
        failure_kind="summarization.llm_failure",
        error_summary=error_summary,
        tenant_id=item["tenant_id"],
        source=item["source"],
        raw_s3_key=item["raw_s3_key"],
        error_context={"observation_id": str(item["observation_id"]), "via": "batch"},
        on_success=lambda: _bump("summarization_batch.dlq_publish.success"),
        on_failure=lambda: _bump("summarization_batch.dlq_publish.failure"),
        on_skipped=lambda: _bump("summarization_batch.dlq_publish.skipped"),
    )


async def submit_queued_batch(
    *,
    pool: asyncpg.Pool,
    client: BatchClient,
    producer: IdempotentProducer,
    s3: S3Client,
    config: SummarizationBatchWorkerConfig,
) -> int:
    async with pool.acquire() as conn:
        async with conn.transaction():
            claimed = await conn.fetch(_CLAIM_SQL, max(1, config.batch_size))
    if not claimed:
        return 0

    _bump("summarization_batch.items_claimed", float(len(claimed)))
    lines: list[str] = []
    line_items: list[tuple[asyncpg.Record, int]] = []
    for item in claimed:
        source_text, metadata = await _source_text_for_item(pool=pool, s3=s3, item=item)
        if not source_text or metadata is None:
            await mark_batch_item_failed(
                pool=pool,
                item_id=item["id"],
                error="No source text available for batch summarization",
            )
            await _publish_failure_dlq(
                producer=producer,
                item=item,
                error_summary="No source text available for batch summarization",
            )
            _bump("summarization_batch.items_failed")
            continue
        lines.append(
            build_batch_request_line(
                custom_id=item["custom_id"],
                source_text=source_text,
                metadata=metadata,
            )
        )
        line_items.append((item, len(source_text)))

    if not lines:
        return 0

    try:
        submitted = await client.submit_jsonl(
            "\n".join(lines) + "\n",
            metadata={"purpose": "fyralis_summarization"},
        )
    except Exception as exc:  # noqa: BLE001
        ids = [item["id"] for item, _source_chars in line_items]
        await pool.execute(
            _RESET_CLAIM_SQL,
            ids,
            f"{type(exc).__name__}: {str(exc)[:200]}",
            config.submit_max_attempts,
        )
        log.warning(
            "summarization_batch.submit_failed",
            extra={"error_type": type(exc).__name__, "error": str(exc)[:200]},
        )
        return 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            job_id = await conn.fetchval(
                _INSERT_JOB_SQL,
                submitted.provider_batch_id,
                submitted.input_file_id,
                submitted.status,
                len(line_items),
                json.dumps({"purpose": "fyralis_summarization"}),
            )
            for item, source_chars in line_items:
                await conn.execute(_MARK_SUBMITTED_SQL, item["id"], job_id, source_chars)
    _bump("summarization_batch.jobs_submitted")
    _bump("summarization_batch.items_submitted", float(len(line_items)))
    return len(line_items)


async def _apply_success_line(
    *,
    pool: asyncpg.Pool,
    producer: IdempotentProducer,
    custom_id: str,
    result: Any,
) -> None:
    item = await pool.fetchrow(_ITEM_BY_CUSTOM_ID_SQL, custom_id)
    if item is None:
        return
    env = _env_from_item(item)
    status = await apply_summary_to_observation(
        env=env,
        pool=pool,
        result=result,
        source_chars=item["source_chars"] or 0,
    )
    if status == "summarized":
        await publish_embedding_request(
            producer=producer,
            tenant_id=env.tenant_id,
            source=env.source,
            observation_id=env.observation_id,
            on_success=lambda: _bump("summarization_batch.embedding_publish.success"),
            on_failure=lambda: _bump("summarization_batch.embedding_publish.failure"),
        )
    if status in {"summarized", "guard_no_op"}:
        await pool.execute(_MARK_COMPLETED_SQL, item["id"])
        _bump("summarization_batch.items_completed")
    else:
        await mark_batch_item_failed(pool=pool, item_id=item["id"], error=status)
        _bump("summarization_batch.items_failed")


async def _apply_error_line(
    *,
    pool: asyncpg.Pool,
    producer: IdempotentProducer,
    custom_id: str,
    error: str,
) -> None:
    item = await pool.fetchrow(_ITEM_BY_CUSTOM_ID_SQL, custom_id)
    if item is None:
        return
    await mark_batch_item_failed(pool=pool, item_id=item["id"], error=error)
    await _publish_failure_dlq(
        producer=producer,
        item=item,
        error_summary=error,
    )
    _bump("summarization_batch.items_failed")


async def _process_output_file(
    *,
    pool: asyncpg.Pool,
    producer: IdempotentProducer,
    client: BatchClient,
    output_file_id: str,
) -> None:
    text = await client.file_text(output_file_id)
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        custom_id, result, error = parse_batch_output_line(line)
        if result is not None:
            await _apply_success_line(
                pool=pool,
                producer=producer,
                custom_id=custom_id,
                result=result,
            )
        else:
            await _apply_error_line(
                pool=pool,
                producer=producer,
                custom_id=custom_id,
                error=error or "batch output line failed",
            )


async def _fail_open_items_for_job(
    *,
    pool: asyncpg.Pool,
    producer: IdempotentProducer,
    job_id: UUID,
    error_summary: str,
) -> None:
    rows = await pool.fetch(_ITEMS_FOR_JOB_SQL, job_id)
    for item in rows:
        await mark_batch_item_failed(pool=pool, item_id=item["id"], error=error_summary)
        await _publish_failure_dlq(
            producer=producer,
            item=item,
            error_summary=error_summary,
        )
        _bump("summarization_batch.items_failed")


async def poll_submitted_batches(
    *,
    pool: asyncpg.Pool,
    client: BatchClient,
    producer: IdempotentProducer,
    config: SummarizationBatchWorkerConfig,
) -> int:
    jobs = await pool.fetch(_SELECT_JOBS_SQL, max(1, config.poll_limit))
    if not jobs:
        return 0
    processed = 0
    for job in jobs:
        status = await client.retrieve(job["provider_batch_id"])
        await pool.execute(
            _UPDATE_JOB_SQL,
            job["id"],
            status.status,
            status.input_file_id,
            status.output_file_id,
            status.error_file_id,
            json.dumps(status.error_context) if status.error_context else None,
        )
        _bump("summarization_batch.jobs_polled")
        processed += 1
        if status.status == "completed" and status.output_file_id:
            await _process_output_file(
                pool=pool,
                producer=producer,
                client=client,
                output_file_id=status.output_file_id,
            )
        elif status.status in {"failed", "expired", "cancelled"}:
            await _fail_open_items_for_job(
                pool=pool,
                producer=producer,
                job_id=job["id"],
                error_summary=f"Batch {status.status}",
            )
    return processed


async def run_batch_worker(
    config: SummarizationBatchWorkerConfig,
    pool: asyncpg.Pool,
    *,
    client: BatchClient | None = None,
    producer: IdempotentProducer | None = None,
    s3: S3Client | None = None,
) -> dict[str, int]:
    batch_client = client or OpenAIBatchClient()
    own_producer = producer is None
    producer_obj = producer or IdempotentProducer(
        config.dlq_producer_config or ProducerConfig(
            bootstrap_servers=config.bootstrap_servers,
            client_id=f"summarization-batch-worker-{id(config)}",
        )
    )
    own_s3 = s3 is None
    s3_obj = s3 or S3Client(
        config.s3_bucket,
        endpoint_url=config.s3_endpoint_url,
        region_name=config.s3_region_name,
    )

    if own_producer:
        await producer_obj.start()
    await s3_obj.connect()

    stop_event = install_shutdown_event()
    heartbeat = Heartbeat()
    health = start_health_server(get_metrics=get_metrics, heartbeat=heartbeat)
    ticker = asyncio.ensure_future(run_heartbeat_ticker(heartbeat, stop_event))
    iterations = 0
    submitted = 0
    polled = 0
    try:
        while not stop_event.is_set():
            iterations += 1
            _bump("summarization_batch.iterations")
            submitted += await submit_queued_batch(
                pool=pool,
                client=batch_client,
                producer=producer_obj,
                s3=s3_obj,
                config=config,
            )
            polled += await poll_submitted_batches(
                pool=pool,
                client=batch_client,
                producer=producer_obj,
                config=config,
            )
            if (
                config.stop_after_iterations is not None
                and iterations >= config.stop_after_iterations
            ):
                break
            await asyncio.sleep(max(0.0, config.idle_sleep_seconds))
    finally:
        ticker.cancel()
        if health is not None:
            health.shutdown()
        if own_producer:
            await producer_obj.stop()
        if own_s3:
            await s3_obj.close()
    return {"iterations": iterations, "submitted": submitted, "polled": polled}


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("SUMMARIZATION_BATCH_WORKER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = SummarizationBatchWorkerConfig(
        postgres_pool_size=int(os.environ.get("POSTGRES_POOL_SIZE", "5")),
        batch_size=int(os.environ.get("SUMMARIZATION_BATCH_SIZE", "50")),
        poll_limit=int(os.environ.get("SUMMARIZATION_BATCH_POLL_LIMIT", "10")),
        idle_sleep_seconds=float(
            os.environ.get("SUMMARIZATION_BATCH_IDLE_SECONDS", "5")
        ),
        submit_max_attempts=int(
            os.environ.get("SUMMARIZATION_BATCH_SUBMIT_MAX_ATTEMPTS", "3")
        ),
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        s3_bucket=os.environ.get("S3_RAW_BUCKET", "fyralis-raw"),
        s3_region_name=os.environ.get("S3_REGION_NAME", "auto"),
    )

    async def _run() -> None:
        pool = await asyncpg.create_pool(
            os.environ["DATABASE_URL"],
            min_size=1,
            max_size=config.postgres_pool_size,
            command_timeout=30.0,
            statement_cache_size=0,
        )
        try:
            await run_batch_worker(config, pool)
        finally:
            await pool.close()

    asyncio.run(_run())


if __name__ == "__main__":  # pragma: no cover
    main()


__all__ = [
    "SummarizationBatchWorkerConfig",
    "get_metrics",
    "main",
    "poll_submitted_batches",
    "reset_metrics",
    "run_batch_worker",
    "submit_queued_batch",
]
