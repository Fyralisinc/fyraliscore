"""Guard: the prod topic provisioner stays in sync with the topic
names the producers/consumers actually use.

If a new ingestion topic is introduced in code but not added to
`scripts/provision_kafka_topics.INGESTION_TOPICS`, prod (which runs with
auto-create disabled) would silently never create it. This test pins
the known set so that drift is caught.

Source-isolation: the data-plane topics are now one per (stage, source)
lane, derived from the source registry. The provisioner must cover every
registry lane plus the control-plane topics.
"""
from __future__ import annotations

from scripts.provision_kafka_topics import (
    CONTROL_PLANE_TOPICS,
    INGESTION_TOPICS,
)
from services.ingest.ingestion.kafka import topics


def test_provisioner_covers_every_per_source_lane() -> None:
    # Every (stage, source) data-plane lane is provisioned.
    for topic in topics.all_data_plane_topics():
        assert topic in INGESTION_TOPICS, f"missing data-plane topic {topic}"


def test_provisioner_covers_control_plane() -> None:
    for topic in CONTROL_PLANE_TOPICS:
        assert topic in INGESTION_TOPICS


def test_provisioner_set_equals_registry_plus_control_plane() -> None:
    expected = set(topics.all_data_plane_topics()) | set(CONTROL_PLANE_TOPICS)
    assert set(INGESTION_TOPICS) == expected


def test_no_duplicate_topics() -> None:
    assert len(INGESTION_TOPICS) == len(set(INGESTION_TOPICS))


def test_count_is_stages_times_sources_plus_control() -> None:
    expected = (
        len(topics.DATA_PLANE_STAGES) * len(topics.INGESTION_SOURCES)
        + len(CONTROL_PLANE_TOPICS)
    )
    assert len(INGESTION_TOPICS) == expected
