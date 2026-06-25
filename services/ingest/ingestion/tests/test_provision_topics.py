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
    validate_topic_descriptions,
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


def _description(name: str, partitions: int) -> dict:
    return {
        "topic": name,
        "error_code": 0,
        "partitions": [{"partition": idx} for idx in range(partitions)],
    }


def test_topic_verification_accepts_expected_partition_count() -> None:
    expected = ("ingestion.raw.slack", "ingestion.tenant_traffic_signal")
    descriptions = [_description(topic, 12) for topic in expected]

    assert validate_topic_descriptions(
        descriptions,
        expected_topics=expected,
        expected_partitions=12,
    ) == []


def test_topic_verification_rejects_missing_topic() -> None:
    issues = validate_topic_descriptions(
        [_description("ingestion.raw.slack", 12)],
        expected_topics=("ingestion.raw.slack", "ingestion.tenant_traffic_signal"),
        expected_partitions=12,
    )

    assert issues == ["ingestion.tenant_traffic_signal: missing after provisioning"]


def test_topic_verification_rejects_partition_drift() -> None:
    issues = validate_topic_descriptions(
        [
            _description("ingestion.raw.slack", 12),
            _description("ingestion.tenant_traffic_signal", 8),
        ],
        expected_topics=("ingestion.raw.slack", "ingestion.tenant_traffic_signal"),
        expected_partitions=12,
    )

    assert issues == [
        "ingestion.tenant_traffic_signal: has 8 partitions; expected 12"
    ]


def test_topic_verification_surfaces_broker_topic_errors() -> None:
    issues = validate_topic_descriptions(
        [
            {
                "topic": "ingestion.raw.slack",
                "error_code": 3,
                "partitions": [{"partition": 0}],
            }
        ],
        expected_topics=("ingestion.raw.slack",),
        expected_partitions=1,
    )

    assert issues == ["ingestion.raw.slack: broker returned topic error 3"]
