"""Single source of truth for ingestion data-plane topic names.

Per `docs/ingestion/source-isolation.md`. Every source ingests on its own
physical lane so lag/failures/backpressure in one source cannot head-of-line
block another. The data-plane stages are split per source:

    ingestion.raw.{source}
    ingestion.normalized.{source}
    ingestion.embedding.{source}
    ingestion.dlq.{source}

Producers, consumers, the provisioner (`scripts/provision_kafka_topics.py`),
and the circuit-breaker lag probe all derive names from THIS module. Nothing
else may hardcode a per-source topic string — a test asserts the provisioner's
topic set equals `all_data_plane_topics()`.

The canonical source list is `RawEnvelope.SourceLiteral`, so the topic
registry and the envelope schema can never drift: add a source to the literal
and its four topics appear here automatically.

Control-plane topics (`ingestion.tenant_traffic_signal`, progress) are NOT
per-source — they carry per-tenant signals, not per-source data — and live as
plain constants at their existing call sites.
"""
from __future__ import annotations

from typing import get_args

from services.ingestion.raw_tier.envelope import SourceLiteral

# The canonical, ordered tuple of ingestion sources, derived from the
# envelope's Literal so the two can never diverge.
INGESTION_SOURCES: tuple[str, ...] = tuple(get_args(SourceLiteral))

# The per-source data-plane stages. The string is both the topic infix and
# the logical stage name used in consumer-group construction.
DATA_PLANE_STAGES: tuple[str, ...] = (
    "raw",
    "normalized",
    "embedding",
    "dlq",
)

_TOPIC_PREFIX = "ingestion"


def topic_for(stage: str, source: str) -> str:
    """The per-source topic name for a stage, e.g. ``ingestion.raw.slack``.

    Raises ValueError on an unknown stage or source so a typo fails loudly at
    the producer/consumer boundary rather than silently creating a stray topic
    (the broker runs with auto-create disabled, so a stray name would just
    drop messages).
    """
    if stage not in DATA_PLANE_STAGES:
        raise ValueError(
            f"unknown ingestion stage {stage!r}; "
            f"expected one of {DATA_PLANE_STAGES}"
        )
    if source not in INGESTION_SOURCES:
        raise ValueError(
            f"unknown ingestion source {source!r}; "
            f"expected one of {INGESTION_SOURCES}"
        )
    return f"{_TOPIC_PREFIX}.{stage}.{source}"


def topics_for_stage(stage: str) -> list[str]:
    """All per-source topics for a stage — used by an all-sources worker
    (no ``INGESTION_SOURCE`` set) to subscribe to every source's lane, and by
    the provisioner.
    """
    if stage not in DATA_PLANE_STAGES:
        raise ValueError(
            f"unknown ingestion stage {stage!r}; "
            f"expected one of {DATA_PLANE_STAGES}"
        )
    return [f"{_TOPIC_PREFIX}.{stage}.{source}" for source in INGESTION_SOURCES]


def all_data_plane_topics() -> list[str]:
    """Every per-source data-plane topic across every stage. The provisioner
    creates exactly this set (plus the control-plane topics).
    """
    return [
        topic
        for stage in DATA_PLANE_STAGES
        for topic in topics_for_stage(stage)
    ]


def consumer_group(stage_group: str, source: str | None) -> str:
    """Consumer-group id for a worker.

    A per-source worker (``source`` set) joins ``{stage_group}.{source}`` so
    its lag/offsets are independent of every other source. An all-sources
    worker (``source`` is None) uses the bare ``stage_group`` — the historical
    group id, preserving dev/sandbox behaviour.
    """
    if source is None:
        return stage_group
    if source not in INGESTION_SOURCES:
        raise ValueError(
            f"unknown ingestion source {source!r}; "
            f"expected one of {INGESTION_SOURCES}"
        )
    return f"{stage_group}.{source}"


def subscribe_topics(stage: str, source: str | None) -> list[str]:
    """Topics a stage's worker should subscribe to.

    ``source`` set  -> just that source's topic (isolated lane).
    ``source`` None -> every per-source topic for the stage (all-sources dev
                       fallback).
    """
    if source is None:
        return topics_for_stage(stage)
    return [topic_for(stage, source)]


__all__ = [
    "DATA_PLANE_STAGES",
    "INGESTION_SOURCES",
    "all_data_plane_topics",
    "consumer_group",
    "subscribe_topics",
    "topic_for",
    "topics_for_stage",
]
