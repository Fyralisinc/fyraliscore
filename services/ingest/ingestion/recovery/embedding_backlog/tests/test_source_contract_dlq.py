"""Contract coverage for terminal embedding-backlog failures."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import orjson
import pytest

from lib.embeddings.base import EmbedderError
from services.ingest.ingestion.dlq.models import DLQEnvelope
from services.ingest.ingestion.recovery.embedding_backlog.embedding_backlog import (
    _DLQ_SOURCE_CONTRACT_BY_CHANNEL,
    _process_row,
    _source_contract_for_channel,
    get_metrics,
    reset_metrics,
)
from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    NON_SOURCE_CHANNEL_DEFINITIONS,
    SOURCE_DEFINITIONS,
)


class _TerminalEmbedder:
    async def embed(self, _text: str) -> list[float]:
        raise EmbedderError("terminal provider failure")


class _PoolMustNotBeUsed:
    def acquire(self) -> None:
        raise AssertionError("terminal embedding failure must not use the pool")


class _CapturingProducer:
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


_CANONICAL_CHANNEL_CASES = tuple(
    (source.source_id, source.provider_id, channel)
    for source in SOURCE_DEFINITIONS
    for channel in source.normalization_inputs
)


def test_embedding_dlq_channel_index_matches_entire_source_catalog() -> None:
    """A catalog change cannot silently leave embedding failures uncovered."""

    assert len(CANONICAL_SOURCE_IDS) == 27
    assert set(_DLQ_SOURCE_CONTRACT_BY_CHANNEL) == {
        channel
        for source in SOURCE_DEFINITIONS
        for channel in source.normalization_inputs
    }
    assert {
        source.source_id for source in _DLQ_SOURCE_CONTRACT_BY_CHANNEL.values()
    } == set(CANONICAL_SOURCE_IDS)
    for source in SOURCE_DEFINITIONS:
        assert source.normalization_inputs
        for channel in source.normalization_inputs:
            assert _source_contract_for_channel(channel) is source


@pytest.mark.parametrize(
    ("source_id", "provider_id", "source_channel"),
    _CANONICAL_CHANNEL_CASES,
)
async def test_terminal_failure_publishes_dlq_for_every_canonical_channel(
    source_id: str,
    provider_id: str,
    source_channel: str,
) -> None:
    """Every canonical channel emits correctly attributed DLQ evidence."""

    observation_id = uuid4()
    tenant_id = uuid4()
    producer = _CapturingProducer()
    reset_metrics()

    await _process_row(
        row={
            "id": observation_id,
            "tenant_id": tenant_id,
            "content_text": "needs an embedding",
            "source_channel": source_channel,
        },
        pool=_PoolMustNotBeUsed(),  # type: ignore[arg-type]
        embedder=_TerminalEmbedder(),  # type: ignore[arg-type]
        dlq_producer=producer,  # type: ignore[arg-type]
    )

    assert len(producer.published) == 1
    topic, payload, key = producer.published[0]
    envelope = DLQEnvelope.model_validate(orjson.loads(payload))
    assert topic == f"ingestion.dlq.{source_id}"
    assert key == str(tenant_id).encode()
    assert envelope.tenant_id == tenant_id
    assert envelope.source == source_id
    assert envelope.failure_kind == "embedding.ollama_failure"
    assert envelope.error_context == {
        "observation_id": str(observation_id),
        "via": "backlog",
        "source_channel": source_channel,
        "provider_id": provider_id,
    }
    metrics = get_metrics()
    assert metrics["backlog.rows_failed"] == 1
    assert metrics["backlog.dlq_publish.success"] == 1
    assert metrics["backlog.dlq_publish.skipped"] == 0


@pytest.mark.parametrize(
    "source_channel",
    (
        *(definition.channel for definition in NON_SOURCE_CHANNEL_DEFINITIONS),
        "synthetic:test",
        "slack:undeclared",
        "",
        None,
        123,
    ),
)
async def test_terminal_failure_does_not_misattribute_non_source_channel(
    source_channel: object,
) -> None:
    """Unknown/non-source channels remain fail-closed, not prefix-guessed."""

    producer = _CapturingProducer()
    reset_metrics()

    await _process_row(
        row={
            "id": uuid4(),
            "tenant_id": uuid4(),
            "content_text": "needs an embedding",
            "source_channel": source_channel,
        },
        pool=_PoolMustNotBeUsed(),  # type: ignore[arg-type]
        embedder=_TerminalEmbedder(),  # type: ignore[arg-type]
        dlq_producer=producer,  # type: ignore[arg-type]
    )

    assert producer.published == []
    metrics = get_metrics()
    assert metrics["backlog.rows_failed"] == 1
    assert metrics["backlog.dlq_publish.success"] == 0
    assert metrics["backlog.dlq_publish.skipped"] == 1
