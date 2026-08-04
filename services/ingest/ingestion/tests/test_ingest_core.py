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
    candidate_phrases,
    ingest,
)
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
