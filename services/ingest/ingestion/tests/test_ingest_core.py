"""Contract-neutral tests for the direct ingestion core.

External sources no longer enter through this handler registry. Their payloads are
authenticated, normalized, and emitted by Source Connector capabilities. The
direct core remains available for Fyralis-owned and non-source product channels.
"""

from __future__ import annotations

from lib.shared.ids import uuid7
import pytest

from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.ingest.ingestion.core import (
    MAX_PAYLOAD_BYTES,
    PayloadTooLarge,
    _dedup_lock_key,
    _build_source_evidence_create,
    candidate_phrases,
    ingest,
    ingest_from_draft,
)
from services.ingest.ingestion.handlers import ObservationDraft
from services.ingest.source_contract.models import SourceObjectRef
from datetime import datetime, timezone
from services.ingest.ingestion.handlers import (
    HandlerNotFound,
    get_handler,
    handler_channels,
)


def test_candidate_phrases_are_bounded_and_deduplicated() -> None:
    text = "foo bar FOO BAR " + " ".join(f"Alpha{i}" for i in range(200))
    phrases = candidate_phrases(text, max_phrases=50)

    assert len(phrases) == 50
    assert "foo" in {phrase.lower() for phrase in phrases}


def test_dedup_lock_key_is_stable_and_tenant_scoped() -> None:
    tenant_id = uuid7()
    other_tenant_id = uuid7()

    key = _dedup_lock_key(tenant_id, "internal:state_change", "event-1")

    assert key == _dedup_lock_key(
        tenant_id, "internal:state_change", "event-1"
    )
    assert key != _dedup_lock_key(
        other_tenant_id, "internal:state_change", "event-1"
    )
    assert 0 <= key <= 0x7FFFFFFFFFFFFFFF


def test_direct_registry_excludes_external_source_channels() -> None:
    channels = set(handler_channels())

    assert {
        "internal:state_change",
        "internal:anomaly",
        "internal:prediction_resolution",
    } <= channels
    assert "slack:message" not in channels
    assert "github:webhook" not in channels
    with pytest.raises(HandlerNotFound):
        get_handler("slack:message")


def test_evidence_contract_separates_object_and_revision_identity() -> None:
    now = datetime(2026, 8, 4, tzinfo=timezone.utc)
    draft = ObservationDraft(
        source_channel="notion:object",
        content_text="Audit status is in progress",
        content={"status": "in_progress"},
        occurred_at=now,
        trust_tier="attested_agent",
        external_id="notion:page:audit",
        source_object=SourceObjectRef(
            object_type="page",
            object_id="audit",
            revision_id="2026-08-04T00:00:00Z",
            operation="update",
            source_recorded_at=now,
            supersedes_revision_id="2026-08-03T00:00:00Z",
        ),
    )

    evidence = _build_source_evidence_create(
        tenant_id=uuid7(),
        draft=draft,
        raw_s3_key="prod/notion/raw.json",
        ingress_kind="poll",
        context={
            "source": "notion",
            "content_hash": "a" * 40,
            "raw_ingested_at": now,
            "normalized_at": now,
        },
    )

    assert evidence.source_object_id == "audit"
    assert evidence.source_revision_id == "2026-08-04T00:00:00Z"
    assert evidence.supersedes_revision_id == "2026-08-03T00:00:00Z"
    assert evidence.raw_retention_state == "available"


@pytest.mark.asyncio
async def test_object_updates_and_deletion_persist_as_distinct_evidence_revisions(
    gateway_pool,
    tenant_id,
) -> None:
    now = datetime(2026, 8, 4, tzinfo=timezone.utc)

    def revision(revision_id: str, operation: str, text: str) -> ObservationDraft:
        return ObservationDraft(
            source_channel="notion:object",
            content_text=text,
            content={"text": text},
            occurred_at=now,
            trust_tier="attested_agent",
            external_id="notion:page:audit",
            source_object=SourceObjectRef(
                object_type="page",
                object_id="audit",
                revision_id=revision_id,
                operation=operation,
                source_recorded_at=now,
            ),
        )

    first = await ingest_from_draft(
        channel="notion:object",
        draft=revision("r1", "create", "audit opened"),
        pool=gateway_pool,
        tenant_id=tenant_id,
        enqueue_trigger=False,
    )
    second = await ingest_from_draft(
        channel="notion:object",
        draft=revision("r2", "update", "audit complete"),
        pool=gateway_pool,
        tenant_id=tenant_id,
        enqueue_trigger=False,
    )
    replay = await ingest_from_draft(
        channel="notion:object",
        draft=revision("r2", "update", "audit complete"),
        pool=gateway_pool,
        tenant_id=tenant_id,
        enqueue_trigger=False,
    )
    deleted = await ingest_from_draft(
        channel="notion:object",
        draft=revision("r3", "delete", "audit page deleted"),
        pool=gateway_pool,
        tenant_id=tenant_id,
        enqueue_trigger=False,
    )

    assert len({first.observation.id, second.observation.id, deleted.observation.id}) == 3
    assert replay.deduped
    assert replay.observation.id == second.observation.id
    operations = await gateway_pool.fetch(
        """
        SELECT operation FROM source_evidence
         WHERE tenant_id = $1 AND source_object_id = 'audit'
         ORDER BY source_revision_id
        """,
        tenant_id,
    )
    assert [row["operation"] for row in operations] == ["create", "update", "delete"]


@pytest.mark.asyncio
async def test_internal_state_change_ingest(
    gateway_pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    cause_id = uuid7()
    await gateway_pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            content, content_text, trust_tier
        ) VALUES ($1, $2, now(), 'signal', 'test:harness',
                  '{}'::jsonb, 'origin', 'authoritative')
        """,
        cause_id,
        tenant_id,
    )
    result = await ingest(
        "internal:state_change",
        {
            "content_text": "commitment c-1 transitioned doneverified",
            "content": {"entity_id": "c-1"},
            "cause_event_id": str(cause_id),
        },
        pool=gateway_pool,
        tenant_id=tenant_id,
        actor_repo=ActorRepo(gateway_pool),
        alias_repo=EntityAliasRepo(gateway_pool),
        embedder=_DeterministicEmbedder(),
    )

    assert result.observation.source_channel == "internal:state_change"
    assert result.observation.trust_tier == "authoritative"
    assert result.observation.cause_id == cause_id


@pytest.mark.asyncio
async def test_source_channel_cannot_bypass_connector_runtime(
    gateway_pool,
    tenant_id,
) -> None:
    with pytest.raises(HandlerNotFound):
        await ingest(
            "slack:message",
            {},
            pool=gateway_pool,
            tenant_id=tenant_id,
            actor_repo=ActorRepo(gateway_pool),
            alias_repo=EntityAliasRepo(gateway_pool),
        )


@pytest.mark.asyncio
async def test_oversized_direct_payload_is_rejected(
    gateway_pool,
    tenant_id,
) -> None:
    with pytest.raises(PayloadTooLarge):
        await ingest(
            "internal:anomaly",
            {
                "content_text": "x" * (MAX_PAYLOAD_BYTES + 1),
                "content": {},
            },
            pool=gateway_pool,
            tenant_id=tenant_id,
            actor_repo=ActorRepo(gateway_pool),
            alias_repo=EntityAliasRepo(gateway_pool),
        )
