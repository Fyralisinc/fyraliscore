"""Unit tests for the per-source topic registry (source-isolation Phase 1).

Pure functions, no infra — these run in the default unit lane.
"""
from __future__ import annotations

from typing import get_args

import pytest

from services.ingest.ingestion.kafka import topics
from services.ingest.ingestion.raw_tier.envelope import SourceLiteral


def test_sources_match_envelope_literal() -> None:
    # The registry's source list MUST be exactly the envelope's literal so the
    # two can never drift (add a source -> its topics appear automatically).
    assert topics.INGESTION_SOURCES == tuple(get_args(SourceLiteral))


def test_topic_for_shape() -> None:
    assert topics.topic_for("raw", "slack") == "ingestion.raw.slack"
    assert topics.topic_for("normalized", "github") == "ingestion.normalized.github"
    assert topics.topic_for("embedding", "gmail") == "ingestion.embedding.gmail"
    assert topics.topic_for("dlq", "jira") == "ingestion.dlq.jira"


def test_topic_for_rejects_unknown_stage() -> None:
    with pytest.raises(ValueError, match="unknown ingestion stage"):
        topics.topic_for("bogus", "slack")


def test_topic_for_rejects_unknown_source() -> None:
    with pytest.raises(ValueError, match="unknown ingestion source"):
        topics.topic_for("raw", "myspace")


def test_topics_for_stage_covers_every_source() -> None:
    raw = topics.topics_for_stage("raw")
    assert len(raw) == len(topics.INGESTION_SOURCES)
    assert raw[0] == "ingestion.raw." + topics.INGESTION_SOURCES[0]
    assert all(t.startswith("ingestion.raw.") for t in raw)


def test_all_data_plane_topics_count_and_uniqueness() -> None:
    all_topics = topics.all_data_plane_topics()
    expected = len(topics.DATA_PLANE_STAGES) * len(topics.INGESTION_SOURCES)
    assert len(all_topics) == expected
    assert len(set(all_topics)) == expected  # no dupes


def test_consumer_group_isolated_vs_all_sources() -> None:
    assert topics.consumer_group("normalizer", "slack") == "normalizer.slack"
    # all-sources worker keeps the historical bare group id
    assert topics.consumer_group("normalizer", None) == "normalizer"


def test_consumer_group_rejects_unknown_source() -> None:
    with pytest.raises(ValueError, match="unknown ingestion source"):
        topics.consumer_group("normalizer", "myspace")


def test_subscribe_topics_isolated() -> None:
    assert topics.subscribe_topics("raw", "discord") == ["ingestion.raw.discord"]


def test_subscribe_topics_all_sources_fallback() -> None:
    subs = topics.subscribe_topics("raw", None)
    assert subs == topics.topics_for_stage("raw")
    assert len(subs) == len(topics.INGESTION_SOURCES)
