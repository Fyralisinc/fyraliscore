"""Between-run state reset (A29 / Decision 10).

M6.7 verification cost hours to stale Kafka state: envelopes from prior
runs (pointing at deleted S3 objects, or out-of-range tenants) sat in
`ingestion.raw` and the consumer groups' committed offsets carried
across runs, so a fresh run's consumers chewed backlog or shadow-moded
re-read messages. The fix pattern that finally gave a clean signal was
delete + recreate the topics.

This module makes that the runner's explicit discipline: before each
validation run, delete + recreate the four ingestion topics (which also
drops the consumer groups' committed offsets for them — no separate
offset-reset needed) and clear the moto raw bucket. Pollution becomes
structurally impossible rather than a thing to remember.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import aioboto3
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import UnknownTopicOrPartitionError


log = logging.getLogger(__name__)

# The four ingestion topics the chain uses. Recreated with the same
# shape the dev broker provisions (4 partitions, zstd, 7-day retention).
INGESTION_TOPICS = (
    "ingestion.raw",
    "ingestion.normalized",
    "ingestion.embedding",
    "ingestion.dlq",
)
_PARTITIONS = 4
_REPLICATION = 1
_TOPIC_CONFIGS = {
    "compression.type": "zstd",
    "retention.ms": "604800000",
}


@dataclass
class CleanupResult:
    topics_recreated: list[str]
    s3_objects_deleted: int


async def _delete_and_recreate_topics(bootstrap_servers: str) -> list[str]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    recreated: list[str] = []
    try:
        # Delete (ignore not-found), wait for the broker to drop them,
        # then recreate. Topic deletion also clears consumer-group
        # committed offsets for those topics.
        try:
            await admin.delete_topics(list(INGESTION_TOPICS))
        except UnknownTopicOrPartitionError:
            pass
        except Exception as exc:  # noqa: BLE001 — partial pre-existing set
            log.warning("validation.cleanup.delete_topics: %r", exc)

        # Poll until all are gone (deletion is async on the broker).
        deadline = asyncio.get_event_loop().time() + 30.0
        while asyncio.get_event_loop().time() < deadline:
            existing = set(await admin.list_topics())
            if not (set(INGESTION_TOPICS) & existing):
                break
            await asyncio.sleep(0.5)

        new_topics = [
            NewTopic(
                name=name,
                num_partitions=_PARTITIONS,
                replication_factor=_REPLICATION,
                topic_configs=dict(_TOPIC_CONFIGS),
            )
            for name in INGESTION_TOPICS
        ]
        # Recreate; tolerate races where auto-create beat us to it.
        deadline = asyncio.get_event_loop().time() + 30.0
        while True:
            try:
                await admin.create_topics(new_topics)
                recreated = list(INGESTION_TOPICS)
                break
            except Exception as exc:  # noqa: BLE001
                if asyncio.get_event_loop().time() >= deadline:
                    raise
                log.debug("validation.cleanup.create retry: %r", exc)
                await asyncio.sleep(0.5)
    finally:
        await admin.close()
    return recreated


async def _clear_s3_bucket(
    *, endpoint_url: str | None, bucket: str, region: str = "us-east-1",
) -> int:
    if not endpoint_url:
        return 0
    session = aioboto3.Session()
    deleted = 0
    async with session.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
    ) as s3:
        try:
            resp = await s3.list_objects_v2(Bucket=bucket)
        except Exception:  # noqa: BLE001 — bucket may not exist yet
            return 0
        for obj in resp.get("Contents", []):
            await s3.delete_object(Bucket=bucket, Key=obj["Key"])
            deleted += 1
    return deleted


async def reset_state(
    *,
    bootstrap_servers: str,
    s3_endpoint_url: str | None,
    s3_bucket: str,
) -> CleanupResult:
    """Delete+recreate the ingestion topics and empty the raw bucket.

    Idempotent: safe to call from a clean state (nothing to delete) and
    safe to call repeatedly (each call leaves genuinely-empty topics +
    bucket). This is what makes back-to-back runs independent.
    """
    recreated = await _delete_and_recreate_topics(bootstrap_servers)
    deleted = await _clear_s3_bucket(
        endpoint_url=s3_endpoint_url, bucket=s3_bucket,
    )
    log.info(
        "validation.cleanup: recreated=%s s3_deleted=%d",
        recreated, deleted,
    )
    return CleanupResult(topics_recreated=recreated, s3_objects_deleted=deleted)
