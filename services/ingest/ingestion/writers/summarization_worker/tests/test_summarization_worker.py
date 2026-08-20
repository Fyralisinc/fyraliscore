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


class _StructuredSummarizer:
    """Returns a SummaryResult carrying the structured extraction (Layer 0).

    Mirrors the live/batch lanes after the document-memory change: the parsed
    DocumentSummarySchema (incl. structured {who?, what, due?} action_items) is
    retained on the result so the writer can persist
    content.summarization.structured.
    """

    def __init__(self, summary: str, structured: dict[str, Any]) -> None:
        self.summary = summary
        self.structured = structured
        self.calls: list[dict[str, Any]] = []

    async def summarize(
        self,
        text: str,
        *,
        metadata: dict[str, Any],
    ) -> SummaryResult:
        self.calls.append({"text": text, "metadata": metadata})
        return SummaryResult(
            summary_text=self.summary,
            model="test-structured",
            structured=self.structured,
        )


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
            evidence_id = uuid4()
            await conn.execute(
                """
                INSERT INTO source_evidence (
                  id, tenant_id, source, installation_scope, source_channel,
                  source_object_type, source_object_id, source_revision_id,
                  operation, source_recorded_at, raw_object_key, content_hash,
                  raw_ingested_at, normalized_at, ingress_kind,
                  contract_version, connector_version, parser_version,
                  normalizer_version, raw_retention_state,
                  access_policy, access_captured_at
                ) VALUES (
                  $1, $2, 'google_drive', 'stateless:google_drive',
                  'google_drive:file', 'file', $3, $4, 'snapshot', $5,
                  $6, repeat('a', 40), $5, $5, 'backfill',
                  1, 'test', 'test', 'test', 'available',
                  '{"visibility":"tenant","audience":[],"source_acl_version":"test-v1"}'::jsonb,
                  $5
                )
                """,
                evidence_id,
                tenant_id,
                str(obs_id),
                f"summary:{obs_id}",
                _NOW,
                raw_s3_key,
            )
            await conn.execute(
                """
                INSERT INTO observations (
                    id, tenant_id, occurred_at, kind, source_channel,
                    source_actor_ref, actor_id, content, content_text,
                    embedding_pending, embedding, trust_tier, external_id,
                    evidence_id
                ) VALUES (
                    $1, $2, $3, 'signal', 'google_drive:file',
                    NULL, NULL, $4::jsonb, $5,
                    TRUE, NULL, 'authoritative', $6, $7
                )
                """,
                obs_id,
                tenant_id,
                _NOW,
                json.dumps(content),
                "Document 'Operating Plan.pdf' is queued for summarization.",
                f"gdrive:{obs_id}:1",
                evidence_id,
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
                "SELECT content, content_text, embedding_pending, embedding, "
                "entities_mentioned "
                "FROM observations WHERE id = $1",
                obs_id,
            )
    assert row is not None
    data = dict(row)
    if isinstance(data["content"], str):
        data["content"] = json.loads(data["content"])
    if isinstance(data.get("entities_mentioned"), str):
        data["entities_mentioned"] = json.loads(data["entities_mentioned"])
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
    outbox = await fresh_db.fetchrow(
        """
        SELECT status, observation_id, evidence_id
        FROM identity_resolution_outbox
        WHERE tenant_id = $1 AND observation_id = $2
        """,
        tenant_id,
        obs_id,
    )
    assert outbox is not None
    assert outbox["status"] == "pending"

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
    assert await fresh_db.fetchval(
        """
        SELECT count(*) FROM identity_resolution_outbox
        WHERE tenant_id = $1 AND observation_id = $2
        """,
        tenant_id,
        obs_id,
    ) == 1


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
    assert await fresh_db.fetchval(
        """
        SELECT count(*) FROM identity_resolution_outbox
        WHERE tenant_id = $1 AND observation_id = $2
        """,
        tenant_id,
        obs_id,
    ) == 0


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


async def test_structured_summary_is_persisted_to_content(
    fresh_db: asyncpg.Pool,
) -> None:
    """Layer-0 DB persistence: content.summarization.structured lands verbatim.

    The shared apply path (used by both live + batch lanes) must write the
    retained structured extraction, including the {who?, what, due?} action-item
    shape, while leaving content_text = the rendered brief unchanged.
    See docs/plans/document-memory-substrate.md §3.1.
    """
    await _ensure_partition(fresh_db)
    tenant_id = await _seed_tenant(fresh_db)
    obs_id = uuid4()
    raw_s3_key = f"dev/google_drive/{tenant_id}/2026-05/ee/{'e' * 40}.json.zst"
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
                    "Priya owns the Acme revised SOW. "
                    "A SOC2 audit slip endangers the renewal."
                )
            }
        }
    )
    structured = {
        "summary": "Acme renewal planning brief.",
        "key_points": ["Billing revamp discussed"],
        "decisions": ["Ship billing revamp before the Sept 30 Acme renewal"],
        "action_items": [
            {"who": "Priya", "what": "send Acme revised SOW", "due": "2026-06-17"}
        ],
        "risks": ["SOC2 audit slip endangers the Acme renewal"],
    }
    producer = _FakeProducer()
    summarizer = _StructuredSummarizer(
        "Acme renewal planning brief.",
        structured,
    )
    env = SummarizationEnvelope(
        tenant_id=tenant_id,
        source="google_drive",
        observation_id=obs_id,
        raw_s3_key=raw_s3_key,
        ingress_kind="gateway",
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

    assert status == "summarized"
    row = await _read_observation(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    summary = row["content"]["summarization"]
    assert summary["status"] == "complete"
    # content_text is still the rendered brief, not the structured payload.
    assert row["content_text"] == "Acme renewal planning brief."
    # The structured extraction is persisted verbatim, including the structured
    # {who?, what, due?} action-item shape.
    assert summary["structured"] == structured
    assert summary["structured"]["action_items"][0]["due"] == "2026-06-17"
    # sibling provenance marker is set.
    assert row["content"]["summary_provenance"] == "llm_summarizer"


async def test_summary_without_structured_omits_structured_key(
    fresh_db: asyncpg.Pool,
) -> None:
    """Back-compat: a SummaryResult without structured data writes no
    content.summarization.structured key (writer skips it)."""
    await _ensure_partition(fresh_db)
    tenant_id = await _seed_tenant(fresh_db)
    obs_id = uuid4()
    raw_s3_key = f"dev/google_drive/{tenant_id}/2026-05/ff/{'f' * 40}.json.zst"
    await _insert_pending_observation(
        fresh_db,
        tenant_id=tenant_id,
        obs_id=obs_id,
        raw_s3_key=raw_s3_key,
        source_text="Some source text about a renewal.",
    )

    producer = _FakeProducer()
    summarizer = _StubSummarizer("A plain brief with no structured payload.")
    env = SummarizationEnvelope(
        tenant_id=tenant_id,
        source="google_drive",
        observation_id=obs_id,
        raw_s3_key=raw_s3_key,
        ingress_kind="gateway",
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
    row = await _read_observation(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    assert "structured" not in row["content"]["summarization"]


# ---------------------------------------------------------------------------
# Layer 2 (Phase 1) — INGEST_DOC_MEMORY_ENABLED scope re-resolution + enriched
# T1. These exercise the worker's document-memory path end-to-end against a real
# DB: entities_mentioned is rewritten from the structured summary, and the T1
# payload carries the structured extraction + resolved scope.
# ---------------------------------------------------------------------------


_ACME_CUSTOMER_ID = "11111111-1111-1111-1111-111111111111"


async def _seed_acme_alias(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    from services.domain.entity_aliases.repo import EntityAliasRepo

    repo = EntityAliasRepo(pool)
    await repo.insert_alias(
        phrase="Acme",
        resolved_entity_ref={"type": "customer", "id": _ACME_CUSTOMER_ID},
        source="manual",
        confidence=1.0,
        tenant_id=tenant_id,
    )


def _acme_structured() -> dict[str, Any]:
    return {
        "summary": "Acme renewal planning brief.",
        "key_points": ["Globex onboarding mentioned in passing"],
        "decisions": ["Ship the billing revamp before the Sept 30 Acme renewal"],
        "action_items": [
            {"who": "Priya", "what": "send Acme the revised SOW", "due": "2026-06-17"}
        ],
        "risks": ["SOC2 audit slip endangers the Acme renewal"],
    }


async def _run_doc_memory_summary(
    pool: asyncpg.Pool, *, tenant_id: UUID, obs_id: UUID, raw_s3_key: str
):
    s3 = FakeS3Client()
    s3.store[raw_s3_key] = orjson.dumps(
        {"record": {"_fyralis_extracted_text": "Acme renewal; Priya owns the SOW."}}
    )
    producer = _FakeProducer()
    summarizer = _StructuredSummarizer("Acme renewal planning brief.", _acme_structured())
    env = SummarizationEnvelope(
        tenant_id=tenant_id,
        source="google_drive",
        observation_id=obs_id,
        raw_s3_key=raw_s3_key,
        ingress_kind="gateway",
        enqueued_at=_NOW,
    )
    sw.reset_metrics()
    status = await sw.summarize_and_update(
        env=env,
        pool=pool,
        summarizer=summarizer,
        dlq_producer=producer,
        embedding_producer=producer,
        s3=s3,
    )
    return status


async def _read_t1_payload(
    pool: asyncpg.Pool, *, tenant_id: UUID, obs_id: UUID
) -> dict[str, Any]:
    trigger = await pool.fetchrow(
        "SELECT payload FROM think_trigger_queue "
        "WHERE tenant_id = $1 AND observation_id = $2",
        tenant_id,
        obs_id,
    )
    assert trigger is not None
    payload = trigger["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return payload


async def test_doc_memory_enriches_entities_and_t1_when_enabled(
    fresh_db: asyncpg.Pool, monkeypatch
) -> None:
    monkeypatch.setenv("INGEST_DOC_MEMORY_ENABLED", "1")
    await _ensure_partition(fresh_db)
    tenant_id = await _seed_tenant(fresh_db)
    await _seed_acme_alias(fresh_db, tenant_id)
    obs_id = uuid4()
    raw_s3_key = f"dev/google_drive/{tenant_id}/2026-05/g1/{'1' * 40}.json.zst"
    await _insert_pending_observation(
        fresh_db, tenant_id=tenant_id, obs_id=obs_id, raw_s3_key=raw_s3_key
    )

    status = await _run_doc_memory_summary(
        fresh_db, tenant_id=tenant_id, obs_id=obs_id, raw_s3_key=raw_s3_key
    )
    assert status == "summarized"

    # entities_mentioned now carries the Acme ref resolved from the structured
    # summary (the placeholder content_text never named Acme).
    row = await _read_observation(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    ids = {(e.get("type"), e.get("id")) for e in row["entities_mentioned"]}
    assert ("customer", _ACME_CUSTOMER_ID) in ids

    # The T1 payload carries the structured extraction + resolved scope so Think
    # can mint the document Models.
    payload = await _read_t1_payload(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    assert payload["summarized"] is True
    assert "doc_structured_summary" in payload
    assert payload["doc_structured_summary"]["action_items"][0]["due"] == "2026-06-17"
    assert {"type": "customer", "id": _ACME_CUSTOMER_ID} in payload["doc_scope_entities"]
    # seed_entity_ids carries the resolved refs for Pathway A scoping.
    assert {"type": "customer", "id": _ACME_CUSTOMER_ID} in payload["seed_entity_ids"]
    # "Priya" did not resolve to an actor UUID -> stays as text, NOT in scope.
    assert "doc_scope_actors" not in payload or all(
        a != "Priya" for a in payload.get("doc_scope_actors", [])
    )
    assert "Priya" in payload.get("doc_unresolved_actor_refs", [])

    metrics = sw.get_metrics()
    assert metrics["summarization_worker.doc_memory.scope_resolved"] == 1
    assert metrics["summarization_worker.doc_memory.scope_failed"] == 0


async def test_doc_memory_disabled_leaves_t1_and_entities_untouched(
    fresh_db: asyncpg.Pool, monkeypatch
) -> None:
    monkeypatch.delenv("INGEST_DOC_MEMORY_ENABLED", raising=False)
    await _ensure_partition(fresh_db)
    tenant_id = await _seed_tenant(fresh_db)
    await _seed_acme_alias(fresh_db, tenant_id)
    obs_id = uuid4()
    raw_s3_key = f"dev/google_drive/{tenant_id}/2026-05/g2/{'2' * 40}.json.zst"
    await _insert_pending_observation(
        fresh_db, tenant_id=tenant_id, obs_id=obs_id, raw_s3_key=raw_s3_key
    )

    status = await _run_doc_memory_summary(
        fresh_db, tenant_id=tenant_id, obs_id=obs_id, raw_s3_key=raw_s3_key
    )
    assert status == "summarized"

    # Flag off: no re-resolution, no enrichment. entities_mentioned untouched and
    # the T1 payload carries no doc_* keys (but the structured summary is still
    # persisted to content by Layer 0).
    row = await _read_observation(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    assert row["entities_mentioned"] == []
    assert row["content"]["summarization"]["structured"]["risks"]  # Layer 0 intact

    payload = await _read_t1_payload(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    assert "doc_structured_summary" not in payload
    assert "doc_scope_entities" not in payload
    metrics = sw.get_metrics()
    assert metrics["summarization_worker.doc_memory.scope_resolved"] == 0


async def test_doc_memory_resolves_action_item_owner_to_scope_actor(
    fresh_db: asyncpg.Pool, monkeypatch
) -> None:
    # When the action-item owner resolves to a real actor, it lands in
    # scope_actors (resolved UUIDs only — §8 scope-actor existence).
    monkeypatch.setenv("INGEST_DOC_MEMORY_ENABLED", "1")
    await _ensure_partition(fresh_db)
    tenant_id = await _seed_tenant(fresh_db)
    await _seed_acme_alias(fresh_db, tenant_id)

    # Seed an actor + identity mapping so "Priya" (channel google_drive)
    # resolves through ActorRepo.resolve_by_source_actor_ref. The owner is
    # written as a channel-qualified ref ("google_drive:Priya"), which is what a
    # resolvable owner looks like; resolve_actor_ref then partitions it to
    # (google_drive, Priya).
    from services.domain.actors.repo import ActorRepo

    actor_repo = ActorRepo(fresh_db)
    actor = await actor_repo.create_actor(
        email=None,
        display_name="Priya",
        type="human_internal",
        tenant_id=tenant_id,
    )
    actor_id = actor.id
    await actor_repo.add_identity_mapping(
        actor_id=actor_id,
        source_channel="google_drive",
        source_actor_ref="Priya",
        confidence=1.0,
    )

    obs_id = uuid4()
    raw_s3_key = f"dev/google_drive/{tenant_id}/2026-05/g3/{'3' * 40}.json.zst"
    await _insert_pending_observation(
        fresh_db, tenant_id=tenant_id, obs_id=obs_id, raw_s3_key=raw_s3_key
    )

    structured = _acme_structured()
    structured["action_items"] = [
        {"who": "google_drive:Priya", "what": "send Acme the revised SOW", "due": "2026-06-17"}
    ]
    s3 = FakeS3Client()
    s3.store[raw_s3_key] = orjson.dumps(
        {"record": {"_fyralis_extracted_text": "Acme renewal; Priya owns the SOW."}}
    )
    producer = _FakeProducer()
    summarizer = _StructuredSummarizer("Acme renewal planning brief.", structured)
    env = SummarizationEnvelope(
        tenant_id=tenant_id,
        source="google_drive",
        observation_id=obs_id,
        raw_s3_key=raw_s3_key,
        ingress_kind="gateway",
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
    assert status == "summarized"

    payload = await _read_t1_payload(fresh_db, tenant_id=tenant_id, obs_id=obs_id)
    assert str(actor_id) in payload.get("doc_scope_actors", [])
    assert str(actor_id) in payload.get("scope_actors", [])
    # Resolved -> NOT in the unresolved-text bucket.
    assert "google_drive:Priya" not in payload.get("doc_unresolved_actor_refs", [])
