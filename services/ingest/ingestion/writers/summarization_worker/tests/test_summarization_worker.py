from __future__ import annotations

import datetime as dt
import json
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

from services.domain.observations import partitions
from services.ingest.ingestion.summarization.batch_api import (
    BatchStatus,
    BatchSubmitResult,
)
from services.ingest.ingestion.summarization.models import SummarizationEnvelope
from services.ingest.ingestion.writers.summarization_batch_worker import (
    summarization_batch_worker as bw,
)
from services.ingest.ingestion.writers.summarization_worker import (
    summarization_worker as sw,
)
from services.ingest.ingestion.writers.summarization_worker.summarization_worker import (
    SummaryResult,
)
from services.ingest.ingestion.workflows.tests._fake_s3 import FakeS3Client


pytestmark = [
    pytest.mark.integration,
    pytest.mark.timeout(180),
]


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


class _FakeProducer:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []

    async def produce(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        **_kwargs: Any,
    ) -> None:
        self.published.append((topic, value, key))


class _StubSummarizer:
    def __init__(self, summary: str) -> None:
        self.summary = summary
        self.calls: list[dict[str, Any]] = []

    async def summarize(
        self,
        text: str,
        *,
        metadata: dict[str, Any],
    ) -> SummaryResult:
        self.calls.append({"text": text, "metadata": metadata})
        return SummaryResult(summary_text=self.summary, model="test-summarizer")


class _FailingSummarizer:
    def __init__(self, message: str = "codex app-server unavailable") -> None:
        self.message = message
        self.calls: list[dict[str, Any]] = []

    async def summarize(
        self,
        text: str,
        *,
        metadata: dict[str, Any],
    ) -> SummaryResult:
        self.calls.append({"text": text, "metadata": metadata})
        raise RuntimeError(self.message)


class _FakeBatchClient:
    def __init__(self) -> None:
        self.submissions: list[tuple[str, dict[str, str]]] = []
        self.output_text = ""

    async def submit_jsonl(
        self,
        jsonl: str,
        *,
        metadata: dict[str, str],
    ) -> BatchSubmitResult:
        self.submissions.append((jsonl, metadata))
        return BatchSubmitResult(
            provider_batch_id="batch_test_1",
            input_file_id="file_input_1",
            status="submitted",
        )

    async def retrieve(self, provider_batch_id: str) -> BatchStatus:
        assert provider_batch_id == "batch_test_1"
        return BatchStatus(
            provider_batch_id=provider_batch_id,
            status="completed",
            input_file_id="file_input_1",
            output_file_id="file_output_1",
        )

    async def file_text(self, file_id: str) -> str:
        assert file_id == "file_output_1"
        return self.output_text


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid,
        f"summarization-test-{tid.hex[:8]}",
    )
    return tid


async def _ensure_partition(pool: asyncpg.Pool) -> None:
    await partitions.ensure_partitions(pool, as_of=_NOW.date(), months_ahead=1)


async def _insert_pending_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    obs_id: UUID,
    raw_s3_key: str,
    source_text: str | None = None,
) -> None:
    summary_meta = {
        "status": "pending",
        "reason": "large_document",
        "raw_s3_key": raw_s3_key,
        "original_chars": 25000,
    }
    if source_text is not None:
        summary_meta["source_text"] = source_text
    content = {
        "object_type": "file",
        "file_id": "drive-file-1",
        "name": "Operating Plan.pdf",
        "is_document": True,
        "summarization": summary_meta,
    }
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(tenant_id),
            )
            await conn.execute(
                """
                INSERT INTO observations (
                    id, tenant_id, occurred_at, kind, source_channel,
                    source_actor_ref, actor_id, content, content_text,
                    embedding_pending, embedding, trust_tier, external_id
                ) VALUES (
                    $1, $2, $3, 'signal', 'google_drive:file',
                    NULL, NULL, $4::jsonb, $5,
                    TRUE, NULL, 'authoritative', $6
                )
                """,
                obs_id,
                tenant_id,
                _NOW,
                json.dumps(content),
                "Document 'Operating Plan.pdf' is queued for summarization.",
                f"gdrive:{obs_id}:1",
            )


async def _read_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    obs_id: UUID,
) -> dict[str, Any]:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(tenant_id),
            )
            row = await conn.fetchrow(
                "SELECT content, content_text, embedding_pending, embedding "
                "FROM observations WHERE id = $1",
                obs_id,
            )
    assert row is not None
    data = dict(row)
    if isinstance(data["content"], str):
        data["content"] = json.loads(data["content"])
    return data


async def test_summarizer_updates_observation_and_enqueues_t1(
    fresh_db: asyncpg.Pool,
) -> None:
    await _ensure_partition(fresh_db)
    tenant_id = await _seed_tenant(fresh_db)
    obs_id = uuid4()
    raw_s3_key = f"dev/google_drive/{tenant_id}/2026-05/aa/{'a' * 40}.json.zst"
    await _insert_pending_observation(
        fresh_db,
        tenant_id=tenant_id,
        obs_id=obs_id,
        raw_s3_key=raw_s3_key,
    )

    s3 = FakeS3Client()
    s3.store[raw_s3_key] = orjson.dumps(
        {
            "record": {
                "_fyralis_extracted_text": (
                    "Alice owns the Q3 operating plan. "
                    "The main risk is enterprise onboarding capacity."
                )
            }
        }
    )
    producer = _FakeProducer()
    summarizer = _StubSummarizer(
        "Alice owns the Q3 operating plan; enterprise onboarding capacity is the main risk."
    )
    env = SummarizationEnvelope(
        tenant_id=tenant_id,
        source="google_drive",
        observation_id=obs_id,
        raw_s3_key=raw_s3_key,
        ingress_kind="backfill",
        enqueued_at=_NOW,
    )

    sw.reset_metrics()
    status = await sw.summarize_and_update(
        env=env,
        pool=fresh_db,
        summarizer=summarizer,
        dlq_producer=producer,  # no DLQ expected
        embedding_producer=producer,
        s3=s3,
    )

    assert status == "summarized"
    assert summarizer.calls
    assert "Q3 operating plan" in summarizer.calls[0]["text"]
    assert summarizer.calls[0]["metadata"]["name"] == "Operating Plan.pdf"

    row = await _read_observation(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    assert row["content_text"] == summarizer.summary
    assert row["embedding"] is None
    assert row["embedding_pending"] is True
    summary = row["content"]["summarization"]
    assert summary["status"] == "complete"
    assert summary["model"] == "test-summarizer"
    assert "source_text" not in summary

    trigger = await fresh_db.fetchrow(
        """
        SELECT trigger_kind, trigger_subkind, observation_id, payload
        FROM think_trigger_queue
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    assert trigger is not None
    assert trigger["trigger_kind"] == "T1"
    assert trigger["trigger_subkind"] == "event_arrival"
    assert trigger["observation_id"] == obs_id
    payload = trigger["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["summarized"] is True
    assert "enterprise onboarding capacity" in payload["seed_natural_text"]

    embedding_publishes = [
        (topic, value) for (topic, value, _key) in producer.published
        if topic.startswith("ingestion.embedding")
    ]
    assert len(embedding_publishes) == 1
    assert embedding_publishes[0][0] == "ingestion.embedding.google_drive"
    embedding_env = orjson.loads(embedding_publishes[0][1])
    assert embedding_env["observation_id"] == str(obs_id)

    metrics = sw.get_metrics()
    assert metrics["summarization_worker.summaries_succeeded"] == 1
    assert metrics["summarization_worker.summaries_failed"] == 0

    replay_status = await sw.summarize_and_update(
        env=env,
        pool=fresh_db,
        summarizer=summarizer,
        dlq_producer=producer,
        embedding_producer=producer,
        s3=s3,
    )
    assert replay_status == "guard_no_op"
    trigger_count = await fresh_db.fetchval(
        """
        SELECT count(*)
        FROM think_trigger_queue
        WHERE tenant_id = $1 AND observation_id = $2
        """,
        tenant_id,
        obs_id,
    )
    assert trigger_count == 1


async def test_summarizer_falls_back_to_inline_source_text_when_raw_s3_missing(
    fresh_db: asyncpg.Pool,
) -> None:
    await _ensure_partition(fresh_db)
    tenant_id = await _seed_tenant(fresh_db)
    obs_id = uuid4()
    raw_s3_key = f"dev/google_drive/{tenant_id}/2026-05/bb/{'b' * 40}.json.zst"
    await _insert_pending_observation(
        fresh_db,
        tenant_id=tenant_id,
        obs_id=obs_id,
        raw_s3_key=raw_s3_key,
        source_text="Inline retained source text about renewal risk.",
    )

    producer = _FakeProducer()
    summarizer = _StubSummarizer("Renewal risk is called out in the document.")
    env = SummarizationEnvelope(
        tenant_id=tenant_id,
        source="google_drive",
        observation_id=obs_id,
        raw_s3_key=raw_s3_key,
        ingress_kind="backfill",
        enqueued_at=_NOW,
    )

    sw.reset_metrics()
    status = await sw.summarize_and_update(
        env=env,
        pool=fresh_db,
        summarizer=summarizer,
        dlq_producer=producer,
        embedding_producer=producer,
        s3=FakeS3Client(),
    )

    assert status == "summarized"
    assert summarizer.calls[0]["text"] == "Inline retained source text about renewal risk."
    row = await _read_observation(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    assert row["content_text"] == "Renewal risk is called out in the document."
    assert "source_text" not in row["content"]["summarization"]
    assert not [
        topic for (topic, _value, _key) in producer.published
        if topic.startswith("ingestion.dlq")
    ]


async def test_summarizer_failure_marks_observation_failed_not_pending(
    fresh_db: asyncpg.Pool,
) -> None:
    await _ensure_partition(fresh_db)
    tenant_id = await _seed_tenant(fresh_db)
    obs_id = uuid4()
    raw_s3_key = f"dev/google_drive/{tenant_id}/2026-05/dd/{'d' * 40}.json.zst"
    await _insert_pending_observation(
        fresh_db,
        tenant_id=tenant_id,
        obs_id=obs_id,
        raw_s3_key=raw_s3_key,
    )

    s3 = FakeS3Client()
    s3.store[raw_s3_key] = orjson.dumps(
        {
            "record": {
                "_fyralis_extracted_text": (
                    "Alice owns the renewal escalation plan. "
                    "Support capacity is the key risk."
                )
            }
        }
    )
    producer = _FakeProducer()
    summarizer = _FailingSummarizer("codex app-server unavailable")
    env = SummarizationEnvelope(
        tenant_id=tenant_id,
        source="google_drive",
        observation_id=obs_id,
        raw_s3_key=raw_s3_key,
        ingress_kind="backfill",
        enqueued_at=_NOW,
    )

    sw.reset_metrics()
    status = await sw.summarize_and_update(
        env=env,
        pool=fresh_db,
        summarizer=summarizer,
        dlq_producer=producer,
        embedding_producer=producer,
        s3=s3,
    )

    assert status == "summarize_failed"
    assert summarizer.calls
    row = await _read_observation(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    summary = row["content"]["summarization"]
    assert summary["status"] == "failed"
    assert summary["error_type"] == "RuntimeError"
    assert "codex app-server unavailable" in summary["error"]
    assert summary["failed_at"]
    assert row["content_text"].startswith(
        "Document 'Operating Plan.pdf' summarization failed:"
    )
    assert "codex app-server unavailable" in row["content_text"]
    assert row["embedding_pending"] is True

    dlq_publishes = [
        topic for (topic, _value, _key) in producer.published
        if topic.startswith("ingestion.dlq")
    ]
    assert dlq_publishes == ["ingestion.dlq.google_drive"]
    metrics = sw.get_metrics()
    assert metrics["summarization_worker.summaries_failed"] == 1


async def test_backfill_batch_lane_submits_polls_and_applies_summary(
    fresh_db: asyncpg.Pool,
) -> None:
    await _ensure_partition(fresh_db)
    tenant_id = await _seed_tenant(fresh_db)
    obs_id = uuid4()
    raw_s3_key = f"dev/google_drive/{tenant_id}/2026-05/cc/{'c' * 40}.json.zst"
    await _insert_pending_observation(
        fresh_db,
        tenant_id=tenant_id,
        obs_id=obs_id,
        raw_s3_key=raw_s3_key,
    )
    env = SummarizationEnvelope(
        tenant_id=tenant_id,
        source="google_drive",
        observation_id=obs_id,
        raw_s3_key=raw_s3_key,
        ingress_kind="backfill",
        enqueued_at=_NOW,
    )

    status = await sw.queue_batch_summarization(env=env, pool=fresh_db)
    assert status == "batch_queued"
    assert await sw.queue_batch_summarization(env=env, pool=fresh_db) == "batch_duplicate"

    s3 = FakeS3Client()
    s3.store[raw_s3_key] = orjson.dumps(
        {
            "record": {
                "_fyralis_extracted_text": (
                    "The Q4 renewal plan depends on support capacity. "
                    "Alice owns the escalation plan."
                )
            }
        }
    )
    producer = _FakeProducer()
    client = _FakeBatchClient()
    bw.reset_metrics()

    submitted = await bw.submit_queued_batch(
        pool=fresh_db,
        client=client,
        producer=producer,
        s3=s3,
        config=bw.SummarizationBatchWorkerConfig(batch_size=10),
    )

    assert submitted == 1
    assert client.submissions
    submitted_jsonl = client.submissions[0][0]
    assert "Q4 renewal plan" in submitted_jsonl
    assert '"/v1/responses"' in submitted_jsonl
    assert '"reasoning":{"effort":"low"}' in submitted_jsonl

    item = await fresh_db.fetchrow(
        """
        SELECT status, custom_id, job_id
        FROM summarization_batch_items
        WHERE tenant_id = $1 AND observation_id = $2
        """,
        tenant_id,
        obs_id,
    )
    assert item is not None
    assert item["status"] == "submitted"
    assert item["job_id"] is not None

    response_body = {
        "model": "test-batch-model",
        "output_text": json.dumps({
            "summary": "Alice owns Q4 renewal escalations; support capacity is the dependency.",
            "key_points": ["Support capacity gates renewal execution."],
            "decisions": [],
            "action_items": ["Alice owns the escalation plan."],
            "risks": ["Renewal execution depends on support capacity."],
        }),
    }
    client.output_text = (
        orjson.dumps({
            "id": "batch_req_test",
            "custom_id": item["custom_id"],
            "response": {"status_code": 200, "body": response_body},
            "error": None,
        }).decode("utf-8")
        + "\n"
    )

    polled = await bw.poll_submitted_batches(
        pool=fresh_db,
        client=client,
        producer=producer,
        config=bw.SummarizationBatchWorkerConfig(),
    )

    assert polled == 1
    row = await _read_observation(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    assert "Alice owns Q4 renewal escalations" in row["content_text"]
    assert row["content"]["summarization"]["status"] == "complete"
    assert row["content"]["summarization"]["model"] == "test-batch-model"

    item_after = await fresh_db.fetchrow(
        """
        SELECT status
        FROM summarization_batch_items
        WHERE tenant_id = $1 AND observation_id = $2
        """,
        tenant_id,
        obs_id,
    )
    assert item_after is not None
    assert item_after["status"] == "completed"

    trigger = await fresh_db.fetchrow(
        """
        SELECT payload
        FROM think_trigger_queue
        WHERE tenant_id = $1 AND observation_id = $2
        """,
        tenant_id,
        obs_id,
    )
    assert trigger is not None
    payload = trigger["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["summarized"] is True

    embedding_publishes = [
        topic for (topic, _value, _key) in producer.published
        if topic.startswith("ingestion.embedding")
    ]
    assert embedding_publishes == ["ingestion.embedding.google_drive"]
    metrics = bw.get_metrics()
    assert metrics["summarization_batch.jobs_submitted"] == 1
    assert metrics["summarization_batch.jobs_polled"] == 1
    assert metrics["summarization_batch.items_completed"] == 1
