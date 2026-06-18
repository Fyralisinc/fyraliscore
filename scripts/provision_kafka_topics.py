#!/usr/bin/env python3
"""Idempotently provision the ingestion Kafka topics for production.

The deployed broker runs with `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`,
so the topics must be created explicitly before any producer/consumer
starts. This is the production counterpart of the validation harness's
delete+recreate (`services/ingest/synthetic/validation_runs/cleanup.py`) — it
only *creates* missing topics and never deletes data.

Run once on deploy (the compose `kafka-init` one-shot calls it):

    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 python scripts/provision_kafka_topics.py

Env:
    KAFKA_BOOTSTRAP_SERVERS   broker (default localhost:9092)
    KAFKA_TOPIC_PARTITIONS    partitions per topic (default 12)
    KAFKA_TOPIC_REPLICATION   replication factor (default 1; single broker)
    KAFKA_TOPIC_RETENTION_MS  retention (default 604800000 = 7 days)

Exit 0 if every topic exists (created or already present), 1 on error.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from services.ingest.ingestion.kafka.topics import all_data_plane_topics

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("provision_kafka_topics")

# Control-plane topics — NOT per-source (they carry per-tenant signals,
# not per-source data). See docs/ingestion/source-isolation.md.
CONTROL_PLANE_TOPICS = (
    "ingestion.tenant_traffic_signal",
)

# Every topic the ingestion data plane uses: one per (stage, source)
# lane derived from the source registry (source-isolation), plus the
# control-plane topics. The registry is the single source of truth —
# add a source to RawEnvelope.SourceLiteral and its four topics appear
# here automatically. A test asserts this set matches the registry.
INGESTION_TOPICS = tuple(all_data_plane_topics()) + CONTROL_PLANE_TOPICS

# Platform egress topics (ADR-0004 E3.1) — provisioned alongside ingestion, kept
# out of INGESTION_TOPICS so the source-registry parity test stays ingestion-scoped.
from services.platform.extensions.egress.kafka import egress_topics  # noqa: E402

ALL_TOPICS = INGESTION_TOPICS + tuple(egress_topics())


async def _provision() -> int:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    partitions = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "12"))
    replication = int(os.environ.get("KAFKA_TOPIC_REPLICATION", "1"))
    retention_ms = os.environ.get("KAFKA_TOPIC_RETENTION_MS", "604800000")
    topic_configs = {
        "compression.type": "zstd",
        "retention.ms": retention_ms,
    }

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        missing = [t for t in ALL_TOPICS if t not in existing]
        if not missing:
            log.info("all %d ingestion topics already present", len(INGESTION_TOPICS))
            return 0

        new_topics = [
            NewTopic(
                name=name,
                num_partitions=partitions,
                replication_factor=replication,
                topic_configs=dict(topic_configs),
            )
            for name in missing
        ]
        try:
            await admin.create_topics(new_topics)
        except TopicAlreadyExistsError:
            # Concurrent provisioner won the race — fine.
            pass
        log.info(
            "created topics %s (partitions=%d replication=%d)",
            missing, partitions, replication,
        )
        # Verify.
        existing = set(await admin.list_topics())
        still_missing = [t for t in ALL_TOPICS if t not in existing]
        if still_missing:
            log.error("topics still missing after create: %s", still_missing)
            return 1
        return 0
    finally:
        await admin.close()


def main() -> int:
    try:
        return asyncio.run(_provision())
    except Exception as exc:  # noqa: BLE001
        log.error("provisioning failed: %r", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
