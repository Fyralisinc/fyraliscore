"""Guard: the prod topic provisioner stays in sync with the topic
names the producers/consumers actually use.

If a new ingestion topic is introduced in code but not added to
`scripts/provision_kafka_topics.INGESTION_TOPICS`, prod (which runs with
auto-create disabled) would silently never create it. This test pins
the known set so that drift is caught.
"""
from __future__ import annotations

from scripts.provision_kafka_topics import INGESTION_TOPICS


def test_provisioner_covers_known_ingestion_topics() -> None:
    # The topics referenced as module-level constants across the
    # ingestion data plane (producers + consumers).
    expected = {
        "ingestion.raw",            # shard_fetch / shadow_write / webhook router
        "ingestion.normalized",     # normalizer -> observation_writer
        "ingestion.embedding",      # observation_writer -> embedding_worker
        "ingestion.dlq",            # normalizer/writer -> dlq_writer
        "ingestion.tenant_traffic_signal",  # circuit-breaker traffic signal
    }
    assert set(INGESTION_TOPICS) == expected
    # No accidental duplicates.
    assert len(INGESTION_TOPICS) == len(set(INGESTION_TOPICS))
