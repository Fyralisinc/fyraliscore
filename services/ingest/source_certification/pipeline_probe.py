"""Isolated raw-to-T1 execution proof for one canonical source.

The ordinary certification driver is intentionally credential-free and can
run without Postgres, Kafka, or S3.  That makes it useful for catalog and
Provider Lab conformance, but it cannot prove the ingestion data plane.

This module supplies the stronger, opt-in proof.  It is deliberately guarded
by an explicit acknowledgement and loopback-only infrastructure endpoints:

* historical sources run two tenants with two exact sibling installations
  through two complete worker replicas, while the contract-owned live-only
  bootstrap remains intentionally single-installation;
* raw Kafka envelopes are resolved to content-hash-verified S3 objects;
* normalized Kafka envelopes are tied back to those exact raw objects;
* exact-count, source-scoped Observations are inspected in Postgres;
* every selected Observation owns exactly one same-tenant T1 trigger; and
* the raw envelopes are replayed, the normalized lane is drained again, and
  Observation/T1 counts must remain unchanged.

No topic reset, database truncate, or provider call is performed here.  The
operator must supply dedicated, already-migrated local infrastructure.  When
that infrastructure is absent or invalid, callers receive a truthful blocked
result rather than a synthetic pass.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import ipaddress
import json
import os
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID, uuid4


PIPELINE_PROBE_SCHEMA_VERSION = "fyralis.source-certification-pipeline-probe.v3"
PIPELINE_REPLAY_SCHEMA_VERSION = "fyralis.source-certification-idempotency-replay.v2"
PIPELINE_TOPOLOGY_SCHEMA_VERSION = "fyralis.source-certification-history-topology.v1"
PIPELINE_DATA_PLANE_SCENARIO_IDS = frozenset(
    {
        "duplicate_delivery_and_idempotency",
        "raw_evidence_and_normalized_topic",
        "observation_persistence_and_t1_trigger",
    }
)
PIPELINE_TOPOLOGY_SCENARIO_IDS = frozenset(
    {
        "exact_tenant_and_installation_resolution",
        "two_replica_cross_tenant_isolation",
    }
)
PIPELINE_SCENARIO_IDS = (
    PIPELINE_DATA_PLANE_SCENARIO_IDS | PIPELINE_TOPOLOGY_SCENARIO_IDS
)
PIPELINE_ACK_ENV = "FYRALIS_CERTIFICATION_ISOLATED_INFRA_ACK"
PIPELINE_DATABASE_ENV = "FYRALIS_CERTIFICATION_DATABASE_URL"
PIPELINE_KAFKA_ENV = "FYRALIS_CERTIFICATION_KAFKA_BOOTSTRAP_SERVERS"
PIPELINE_S3_ENDPOINT_ENV = "FYRALIS_CERTIFICATION_S3_ENDPOINT_URL"
PIPELINE_S3_BUCKET_ENV = "FYRALIS_CERTIFICATION_S3_RAW_BUCKET"
PIPELINE_ACK_VALUE = "dedicated-loopback-data-plane-v1"
PIPELINE_ENV_NAMES = frozenset(
    {
        PIPELINE_ACK_ENV,
        PIPELINE_DATABASE_ENV,
        PIPELINE_KAFKA_ENV,
        PIPELINE_S3_ENDPOINT_ENV,
        PIPELINE_S3_BUCKET_ENV,
    }
)

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_REDACTED_URI_RE = re.compile(
    r"(?i)(?:postgres(?:ql)?|https?)://[^\s'\"]+",
)


class PipelineProbeError(RuntimeError):
    """The isolated pipeline did not satisfy a required invariant."""


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REPLAY_PROOF_FIELDS = frozenset(
    {
        "schema_version",
        "raw_records_before",
        "raw_records_replayed",
        "raw_records_after",
        "raw_topic_record_growth",
        "normalized_records_before",
        "normalized_records_after",
        "normalized_topic_record_growth",
        "observation_count_before",
        "observation_count_after",
        "observation_count_growth",
        "observation_identity_set_sha256_before",
        "observation_identity_set_sha256_after",
        "t1_count_before",
        "t1_count_after",
        "t1_count_growth",
        "t1_observation_id_set_sha256_before",
        "t1_observation_id_set_sha256_after",
        "writer_group_drain",
        "idempotency_proven",
    }
)
_TOPOLOGY_PROOF_FIELDS = frozenset(
    {
        "schema_version",
        "tenant_count",
        "installations_per_tenant",
        "installation_count",
        "configured_replicas",
        "observed_oauth_replicas",
        "participating_oauth_replicas",
        "oauth_replica_claims",
        "tenants",
        "exact_installation_identity_proven",
        "per_tenant_observation_counts_proven",
        "cross_tenant_leak_count",
        "cross_tenant_isolation_proven",
        "two_replica_participation_proven",
    }
)
_TOPOLOGY_TENANT_FIELDS = frozenset(
    {
        "tenant_id",
        "tenant_slug",
        "expected_observation_count",
        "observed_observation_count",
        "installation_keys",
        "installation_row_ids",
        "trigger_ids",
        "onboarding_run_ids",
        "normalized_observation_identity_set_sha256",
        "persisted_observation_identity_set_sha256",
        "cross_tenant_leak_count",
    }
)
_INSTALLATION_ATTRIBUTION_FIELDS = frozenset(
    {
        "expected_installation_row_ids",
        "raw_records_by_installation",
        "normalized_records_by_installation",
        "exact_installation_row_set",
    }
)
_INSTALLATION_IDENTITY_FIELDS = frozenset(
    {
        "source",
        "tenant_slug",
        "installation_key",
        "tenant_id",
        "installation_row_id",
        "trigger_id",
        "onboarding_run_id",
    }
)


def pipeline_scenario_ids_for_source(source_id: str) -> frozenset[str]:
    """Return the exact executable pipeline boundary for one source.

    Historical sources own the 2×2×2 topology proof. A live-only source
    cannot truthfully claim durable OAuth/backfill-worker participation, so it
    retains only the raw-to-T1/replay scenarios.
    """

    from services.ingest.source_contract.catalog import source_definition

    if source_definition(source_id).history is None:
        return PIPELINE_DATA_PLANE_SCENARIO_IDS
    return PIPELINE_SCENARIO_IDS


def _proof_mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PipelineProbeError(f"{field} must be an object")
    return value


def _proof_count(
    value: Mapping[str, Any],
    field: str,
    *,
    positive: bool = False,
) -> int:
    raw = value.get(field)
    minimum = 1 if positive else 0
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < minimum:
        qualifier = "positive" if positive else "non-negative"
        raise PipelineProbeError(
            f"pipeline replay proof {field} must be a {qualifier} integer"
        )
    return raw


def _proof_sha256(value: Mapping[str, Any], field: str) -> str:
    raw = value.get(field)
    if not isinstance(raw, str) or _SHA256_RE.fullmatch(raw) is None:
        raise PipelineProbeError(
            f"pipeline replay proof {field} must be a SHA-256 digest"
        )
    return raw


def validate_replay_idempotency_proof(
    result: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Validate the exact replay counters and identity hashes.

    The replay proof is deliberately redundant with the surrounding pipeline
    artifact. The redundancy lets the release-artifact validator cross-check
    that every selected, previously-normalized raw parent was delivered again
    while canonical Observation and T1 identities remained unchanged.
    """

    pipeline = _proof_mapping(result.get("pipeline"), field="pipeline")
    replay = _proof_mapping(
        pipeline.get("replay"),
        field="pipeline.replay",
    )
    missing = _REPLAY_PROOF_FIELDS - replay.keys()
    extra = replay.keys() - _REPLAY_PROOF_FIELDS
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing {sorted(missing)!r}")
        if extra:
            details.append(f"unknown {sorted(extra)!r}")
        raise PipelineProbeError(
            "pipeline replay proof fields are invalid: " + ", ".join(details)
        )
    if replay.get("schema_version") != PIPELINE_REPLAY_SCHEMA_VERSION:
        raise PipelineProbeError("pipeline replay proof schema_version is unsupported")

    raw_before = _proof_count(replay, "raw_records_before", positive=True)
    raw_replayed = _proof_count(
        replay,
        "raw_records_replayed",
        positive=True,
    )
    raw_after = _proof_count(replay, "raw_records_after", positive=True)
    raw_growth = _proof_count(replay, "raw_topic_record_growth")
    if (
        raw_replayed > raw_before
        or raw_growth != raw_replayed
        or raw_after != raw_before + raw_replayed
    ):
        raise PipelineProbeError(
            "pipeline replay proof raw-record counters are inconsistent"
        )

    normalized_before = _proof_count(
        replay,
        "normalized_records_before",
        positive=True,
    )
    normalized_after = _proof_count(
        replay,
        "normalized_records_after",
        positive=True,
    )
    normalized_growth = _proof_count(
        replay,
        "normalized_topic_record_growth",
    )
    if (
        normalized_growth != raw_replayed
        or normalized_after != normalized_before + normalized_growth
    ):
        raise PipelineProbeError(
            "pipeline replay proof normalized-record counters are inconsistent"
        )

    observation_before = _proof_count(
        replay,
        "observation_count_before",
        positive=True,
    )
    observation_after = _proof_count(
        replay,
        "observation_count_after",
        positive=True,
    )
    observation_growth = _proof_count(
        replay,
        "observation_count_growth",
    )
    observation_hash_before = _proof_sha256(
        replay,
        "observation_identity_set_sha256_before",
    )
    observation_hash_after = _proof_sha256(
        replay,
        "observation_identity_set_sha256_after",
    )
    if (
        observation_after != observation_before
        or observation_growth != 0
        or observation_hash_after != observation_hash_before
    ):
        raise PipelineProbeError(
            "pipeline replay proof Observation identity changed after replay"
        )

    t1_before = _proof_count(replay, "t1_count_before", positive=True)
    t1_after = _proof_count(replay, "t1_count_after", positive=True)
    t1_growth = _proof_count(replay, "t1_count_growth")
    t1_hash_before = _proof_sha256(
        replay,
        "t1_observation_id_set_sha256_before",
    )
    t1_hash_after = _proof_sha256(
        replay,
        "t1_observation_id_set_sha256_after",
    )
    if (
        t1_before != observation_before
        or t1_after != observation_after
        or t1_after != t1_before
        or t1_growth != 0
        or t1_hash_after != t1_hash_before
    ):
        raise PipelineProbeError(
            "pipeline replay proof T1 identity changed after replay"
        )

    writer_drain = _proof_mapping(
        replay.get("writer_group_drain"),
        field="pipeline.replay.writer_group_drain",
    )
    if writer_drain.get("drained") is not True:
        raise PipelineProbeError("pipeline replay proof writer group did not drain")
    if replay.get("idempotency_proven") is not True:
        raise PipelineProbeError(
            "pipeline replay proof idempotency_proven must be true"
        )

    expected_nested = (
        (
            "raw_topic",
            "before_replay",
            "count",
            raw_before,
        ),
        (
            "raw_topic",
            "after_replay",
            "count",
            raw_after,
        ),
        (
            "normalized_topic",
            "before_replay",
            "count",
            normalized_before,
        ),
        (
            "normalized_topic",
            "after_replay",
            "count",
            normalized_after,
        ),
        (
            "observations",
            "before_replay",
            "count",
            observation_before,
        ),
        (
            "observations",
            "after_replay",
            "count",
            observation_after,
        ),
        (
            "t1_triggers",
            "before_replay",
            "t1_count",
            t1_before,
        ),
        (
            "t1_triggers",
            "after_replay",
            "t1_count",
            t1_after,
        ),
    )
    for section, snapshot, field_name, expected_count in expected_nested:
        section_value = _proof_mapping(
            pipeline.get(section),
            field=f"pipeline.{section}",
        )
        snapshot_value = _proof_mapping(
            section_value.get(snapshot),
            field=f"pipeline.{section}.{snapshot}",
        )
        if snapshot_value.get(field_name) != expected_count:
            raise PipelineProbeError(
                f"pipeline replay proof {section}.{snapshot}.{field_name} "
                "differs from its explicit counter"
            )

    expected_hashes = (
        (
            "observations",
            "before_replay",
            "identity_set_sha256",
            observation_hash_before,
        ),
        (
            "observations",
            "after_replay",
            "identity_set_sha256",
            observation_hash_after,
        ),
        (
            "t1_triggers",
            "before_replay",
            "observation_id_set_sha256",
            t1_hash_before,
        ),
        (
            "t1_triggers",
            "after_replay",
            "observation_id_set_sha256",
            t1_hash_after,
        ),
    )
    for section, snapshot, field_name, expected_digest in expected_hashes:
        section_value = _proof_mapping(
            pipeline.get(section),
            field=f"pipeline.{section}",
        )
        snapshot_value = _proof_mapping(
            section_value.get(snapshot),
            field=f"pipeline.{section}.{snapshot}",
        )
        if snapshot_value.get(field_name) != expected_digest:
            raise PipelineProbeError(
                f"pipeline replay proof {section}.{snapshot}.{field_name} "
                "differs from its explicit digest"
            )
    return replay


def _topology_exact_keys(
    value: Mapping[str, Any],
    expected: frozenset[str],
    *,
    field: str,
) -> None:
    missing = expected - value.keys()
    extra = value.keys() - expected
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing {sorted(missing)!r}")
        if extra:
            details.append(f"unknown {sorted(extra)!r}")
        raise PipelineProbeError(
            f"{field} fields are invalid: {', '.join(details)}",
        )


def _topology_sequence(value: object, *, field: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise PipelineProbeError(f"{field} must be an array")
    return value


def _topology_positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise PipelineProbeError(f"{field} must be a positive integer")
    return value


def _topology_uuid_values(value: object, *, field: str) -> tuple[str, ...]:
    values = _topology_sequence(value, field=field)
    rendered: list[str] = []
    for item in values:
        if not isinstance(item, str):
            raise PipelineProbeError(f"{field} must contain UUID strings")
        try:
            rendered.append(str(UUID(item)))
        except ValueError as exc:
            raise PipelineProbeError(
                f"{field} must contain UUID strings",
            ) from exc
    return tuple(rendered)


def validate_history_topology_proof(
    result: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Validate the exact 2 tenants × 2 installs × 2 replicas proof."""

    pipeline = _proof_mapping(result.get("pipeline"), field="pipeline")
    topology = _proof_mapping(
        pipeline.get("topology"),
        field="pipeline.topology",
    )
    _topology_exact_keys(
        topology,
        _TOPOLOGY_PROOF_FIELDS,
        field="pipeline.topology",
    )
    if topology.get("schema_version") != PIPELINE_TOPOLOGY_SCHEMA_VERSION:
        raise PipelineProbeError(
            "pipeline topology proof schema_version is unsupported",
        )
    exact_counts = {
        "tenant_count": 2,
        "installations_per_tenant": 2,
        "installation_count": 4,
        "configured_replicas": 2,
        "observed_oauth_replicas": 2,
        "participating_oauth_replicas": 2,
    }
    for field_name, expected in exact_counts.items():
        if topology.get(field_name) != expected:
            raise PipelineProbeError(
                f"pipeline topology proof {field_name} must equal {expected}",
            )

    claims = _proof_mapping(
        topology.get("oauth_replica_claims"),
        field="pipeline.topology.oauth_replica_claims",
    )
    if len(claims) != 2 or any(
        not isinstance(replica_id, str)
        or not replica_id
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count < 1
        for replica_id, count in claims.items()
    ):
        raise PipelineProbeError(
            "pipeline topology proof requires two durable OAuth replicas "
            "with positive claim counts",
        )

    tenant_rows = _topology_sequence(
        topology.get("tenants"),
        field="pipeline.topology.tenants",
    )
    if len(tenant_rows) != 2:
        raise PipelineProbeError(
            "pipeline topology proof requires exactly two tenant rows",
        )
    tenant_ids: set[str] = set()
    tenant_slugs: set[str] = set()
    installation_ids: set[str] = set()
    trigger_ids: set[str] = set()
    run_ids: set[str] = set()
    expected_total = 0
    for index, raw_row in enumerate(tenant_rows):
        row = _proof_mapping(
            raw_row,
            field=f"pipeline.topology.tenants[{index}]",
        )
        _topology_exact_keys(
            row,
            _TOPOLOGY_TENANT_FIELDS,
            field=f"pipeline.topology.tenants[{index}]",
        )
        try:
            tenant_id = str(UUID(str(row.get("tenant_id"))))
        except ValueError as exc:
            raise PipelineProbeError(
                "pipeline topology tenant_id must be a UUID",
            ) from exc
        slug = row.get("tenant_slug")
        if not isinstance(slug, str) or not slug:
            raise PipelineProbeError(
                "pipeline topology tenant_slug must be non-empty",
            )
        if tenant_id in tenant_ids or slug in tenant_slugs:
            raise PipelineProbeError(
                "pipeline topology tenant identities must be distinct",
            )
        tenant_ids.add(tenant_id)
        tenant_slugs.add(slug)

        expected_count = _topology_positive_int(
            row.get("expected_observation_count"),
            field="pipeline topology expected_observation_count",
        )
        observed_count = _topology_positive_int(
            row.get("observed_observation_count"),
            field="pipeline topology observed_observation_count",
        )
        if observed_count != expected_count:
            raise PipelineProbeError(
                "pipeline topology per-tenant Observation count differs "
                "from its exact fixture oracle",
            )
        expected_total += expected_count

        keys = _topology_sequence(
            row.get("installation_keys"),
            field="pipeline topology installation_keys",
        )
        if (
            len(keys) != 2
            or any(not isinstance(item, str) or not item for item in keys)
            or len(set(keys)) != 2
        ):
            raise PipelineProbeError(
                "pipeline topology requires two distinct installation keys "
                "per tenant",
            )
        row_installations = _topology_uuid_values(
            row.get("installation_row_ids"),
            field="pipeline topology installation_row_ids",
        )
        row_triggers = _topology_uuid_values(
            row.get("trigger_ids"),
            field="pipeline topology trigger_ids",
        )
        row_runs = _topology_uuid_values(
            row.get("onboarding_run_ids"),
            field="pipeline topology onboarding_run_ids",
        )
        for label, values, aggregate in (
            ("installation rows", row_installations, installation_ids),
            ("triggers", row_triggers, trigger_ids),
            ("onboarding runs", row_runs, run_ids),
        ):
            if len(values) != 2 or len(set(values)) != 2:
                raise PipelineProbeError(
                    f"pipeline topology requires two distinct {label} " "per tenant",
                )
            if aggregate.intersection(values):
                raise PipelineProbeError(
                    f"pipeline topology {label} leaked across tenants",
                )
            aggregate.update(values)

        normalized_hash = _proof_sha256(
            row,
            "normalized_observation_identity_set_sha256",
        )
        persisted_hash = _proof_sha256(
            row,
            "persisted_observation_identity_set_sha256",
        )
        if normalized_hash != persisted_hash:
            raise PipelineProbeError(
                "pipeline topology normalized and persisted Observation "
                "identity sets differ",
            )
        if row.get("cross_tenant_leak_count") != 0:
            raise PipelineProbeError(
                "pipeline topology tenant row reports a cross-tenant leak",
            )

    installation_identity = _topology_sequence(
        pipeline.get("installation_identity"),
        field="pipeline.installation_identity",
    )
    if len(installation_identity) != 4:
        raise PipelineProbeError(
            "pipeline topology requires exactly four durable installation "
            "identity rows",
        )
    topology_by_tenant = {
        str(row["tenant_id"]): row for row in tenant_rows if isinstance(row, Mapping)
    }
    identity_by_tenant: dict[str, list[Mapping[str, Any]]] = {}
    source_id = result.get("source_id")
    for index, raw_identity in enumerate(installation_identity):
        identity = _proof_mapping(
            raw_identity,
            field=f"pipeline.installation_identity[{index}]",
        )
        _topology_exact_keys(
            identity,
            _INSTALLATION_IDENTITY_FIELDS,
            field=f"pipeline.installation_identity[{index}]",
        )
        if isinstance(source_id, str) and identity.get("source") != source_id:
            raise PipelineProbeError(
                "pipeline installation identity source differs",
            )
        tenant_id = str(identity.get("tenant_id"))
        if tenant_id not in topology_by_tenant:
            raise PipelineProbeError(
                "pipeline installation identity references an unknown tenant",
            )
        identity_by_tenant.setdefault(tenant_id, []).append(identity)
    for tenant_id, topology_row in topology_by_tenant.items():
        identities = identity_by_tenant.get(tenant_id, [])
        if len(identities) != 2:
            raise PipelineProbeError(
                "pipeline installation identity coverage differs from the "
                "2×2 topology",
            )
        comparisons = (
            (
                "tenant_slug",
                {identity["tenant_slug"] for identity in identities},
                {topology_row["tenant_slug"]},
            ),
            (
                "installation_key",
                {identity["installation_key"] for identity in identities},
                set(topology_row["installation_keys"]),
            ),
            (
                "installation_row_id",
                {str(identity["installation_row_id"]) for identity in identities},
                set(topology_row["installation_row_ids"]),
            ),
            (
                "trigger_id",
                {str(identity["trigger_id"]) for identity in identities},
                set(topology_row["trigger_ids"]),
            ),
            (
                "onboarding_run_id",
                {str(identity["onboarding_run_id"]) for identity in identities},
                set(topology_row["onboarding_run_ids"]),
            ),
        )
        for label, actual, expected_values in comparisons:
            if actual != expected_values:
                raise PipelineProbeError(
                    f"pipeline installation identity {label} differs from "
                    "the topology row",
                )

    tenant_pipelines = _topology_sequence(
        pipeline.get("tenant_pipelines"),
        field="pipeline.tenant_pipelines",
    )
    if len(tenant_pipelines) != 2:
        raise PipelineProbeError(
            "pipeline topology proof requires two tenant pipelines",
        )
    pipeline_tenants: set[str] = set()
    pipeline_observation_total = 0
    for index, raw_tenant_pipeline in enumerate(tenant_pipelines):
        tenant_pipeline = _proof_mapping(
            raw_tenant_pipeline,
            field=f"pipeline.tenant_pipelines[{index}]",
        )
        try:
            tenant_id = str(UUID(str(tenant_pipeline.get("tenant_id"))))
        except ValueError as exc:
            raise PipelineProbeError(
                "pipeline tenant_pipelines tenant_id must be a UUID",
            ) from exc
        if tenant_id in pipeline_tenants or tenant_id not in topology_by_tenant:
            raise PipelineProbeError(
                "pipeline tenant_pipelines identities differ from topology",
            )
        pipeline_tenants.add(tenant_id)
        validate_replay_idempotency_proof({"pipeline": tenant_pipeline})
        observations = _proof_mapping(
            tenant_pipeline.get("observations"),
            field="pipeline tenant observations",
        )
        normalized = _proof_mapping(
            tenant_pipeline.get("normalized_topic"),
            field="pipeline tenant normalized_topic",
        )
        before = _proof_mapping(
            observations.get("before_replay"),
            field="pipeline tenant observations.before_replay",
        )
        tenant_count = _topology_positive_int(
            before.get("count"),
            field="pipeline tenant Observation count",
        )
        topology_row = topology_by_tenant[tenant_id]
        if (
            tenant_count != topology_row["observed_observation_count"]
            or before.get("identity_set_sha256")
            != topology_row["persisted_observation_identity_set_sha256"]
            or normalized.get("observation_identity_count") != tenant_count
            or normalized.get("observation_identity_set_sha256")
            != topology_row["normalized_observation_identity_set_sha256"]
        ):
            raise PipelineProbeError(
                "pipeline tenant replay evidence differs from topology row",
            )
        attribution = _proof_mapping(
            tenant_pipeline.get("installation_attribution"),
            field="pipeline tenant installation_attribution",
        )
        _topology_exact_keys(
            attribution,
            _INSTALLATION_ATTRIBUTION_FIELDS,
            field="pipeline tenant installation_attribution",
        )
        expected_installations = set(
            topology_row["installation_row_ids"],
        )
        raw_counts = _proof_mapping(
            attribution.get("raw_records_by_installation"),
            field="pipeline tenant raw_records_by_installation",
        )
        normalized_counts = _proof_mapping(
            attribution.get("normalized_records_by_installation"),
            field="pipeline tenant normalized_records_by_installation",
        )
        if (
            set(attribution.get("expected_installation_row_ids", ()))
            != expected_installations
            or set(raw_counts) != expected_installations
            or set(normalized_counts) != expected_installations
            or any(
                isinstance(count, bool) or not isinstance(count, int) or count < 1
                for count in (*raw_counts.values(), *normalized_counts.values())
            )
            or attribution.get("exact_installation_row_set") is not True
        ):
            raise PipelineProbeError(
                "pipeline tenant installation attribution is incomplete",
            )
        pipeline_observation_total += tenant_count

    if pipeline_tenants != tenant_ids or pipeline_observation_total != expected_total:
        raise PipelineProbeError(
            "pipeline topology tenant pipeline coverage is incomplete",
        )
    if (
        topology.get("exact_installation_identity_proven") is not True
        or topology.get("per_tenant_observation_counts_proven") is not True
        or topology.get("cross_tenant_leak_count") != 0
        or topology.get("cross_tenant_isolation_proven") is not True
        or topology.get("two_replica_participation_proven") is not True
    ):
        raise PipelineProbeError(
            "pipeline topology proof did not establish every required "
            "2×2×2 invariant",
        )
    return topology


@dataclass(frozen=True, slots=True)
class PipelineProbeConfig:
    """Validated local-only infrastructure binding.

    ``database_url`` can contain a password, so it is excluded from repr and
    never serialized into a stage artifact.
    """

    database_url: str = field(repr=False)
    kafka_bootstrap_servers: str
    s3_endpoint_url: str
    s3_raw_bucket: str

    @property
    def descriptor(self) -> dict[str, object]:
        database = urlsplit(self.database_url)
        s3 = urlsplit(self.s3_endpoint_url)
        kafka_hosts = tuple(
            _split_host_port(value)[0]
            for value in self.kafka_bootstrap_servers.split(",")
        )
        database_identity = (
            f"{database.scheme}://{database.hostname}:"
            f"{database.port or 5432}/{database.path.lstrip('/')}"
        )
        sealed = "\n".join(
            (
                database_identity,
                self.kafka_bootstrap_servers,
                self.s3_endpoint_url,
                self.s3_raw_bucket,
            )
        ).encode("utf-8")
        return {
            "binding_sha256": hashlib.sha256(sealed).hexdigest(),
            "credentials_included_in_binding": False,
            "loopback_only": True,
            "database": {
                "host": database.hostname,
                "port": database.port or 5432,
                "database": database.path.lstrip("/"),
            },
            "kafka_hosts": list(kafka_hosts),
            "s3": {
                "host": s3.hostname,
                "port": s3.port,
                "scheme": s3.scheme,
            },
            "s3_bucket_sha256": hashlib.sha256(
                self.s3_raw_bucket.encode("utf-8"),
            ).hexdigest(),
            "destructive_reset_performed": False,
        }


@dataclass(frozen=True, slots=True)
class _KafkaRecord:
    topic: str
    partition: int
    offset: int
    key: bytes | None = field(repr=False)
    value: bytes = field(repr=False)
    headers: tuple[tuple[str, bytes], ...] = field(repr=False)
    decoded: Mapping[str, Any] = field(repr=False)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _canonical_sha256(value: object) -> str:
    return _sha256_bytes(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        ).encode("utf-8"),
    )


def _identity_timestamp(value: datetime) -> str:
    """Canonicalize equal instants before comparing Kafka with Postgres.

    Provider payloads may preserve an explicit non-UTC offset while Postgres
    returns the same ``timestamptz`` instant in UTC. Offset spelling is not
    part of the Observation idempotency identity, so hashes must compare the
    normalized instant rather than two equivalent ISO renderings.
    """

    if value.tzinfo is None or value.utcoffset() is None:
        raise PipelineProbeError(
            "Observation identity timestamp must be timezone-aware",
        )
    return value.astimezone(timezone.utc).isoformat()


def _is_loopback_host(host: str | None) -> bool:
    if not host:
        return False
    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _split_host_port(value: str) -> tuple[str, int]:
    rendered = value.strip()
    if not rendered:
        raise ValueError("Kafka bootstrap endpoint cannot be empty")
    parsed = urlsplit(f"//{rendered}")
    if not parsed.hostname or parsed.port is None:
        raise ValueError(
            "Kafka bootstrap endpoints must be explicit host:port values",
        )
    return parsed.hostname, parsed.port


def _config_from_env(
    ambient_env: Mapping[str, str],
) -> tuple[PipelineProbeConfig | None, dict[str, object]]:
    """Return a validated config or an artifact-safe blocked explanation."""

    present = sorted(name for name in PIPELINE_ENV_NAMES if ambient_env.get(name))
    if not present:
        return None, {
            "state": "blocked",
            "reason_code": "isolated_infrastructure_not_supplied",
            "reason": (
                "the opt-in Postgres/Kafka/S3 pipeline environment was not " "supplied"
            ),
            "required_environment_names": sorted(PIPELINE_ENV_NAMES),
            "present_environment_names": [],
            "credential_values_recorded": False,
        }
    missing = sorted(PIPELINE_ENV_NAMES - set(present))
    if missing:
        return None, {
            "state": "blocked",
            "reason_code": "isolated_infrastructure_incomplete",
            "reason": (
                "the opt-in pipeline environment is incomplete; missing "
                + ", ".join(missing)
            ),
            "required_environment_names": sorted(PIPELINE_ENV_NAMES),
            "present_environment_names": present,
            "credential_values_recorded": False,
        }
    if ambient_env[PIPELINE_ACK_ENV] != PIPELINE_ACK_VALUE:
        return None, {
            "state": "blocked",
            "reason_code": "isolated_infrastructure_ack_invalid",
            "reason": (
                f"{PIPELINE_ACK_ENV} must equal the documented dedicated "
                "loopback acknowledgement"
            ),
            "required_environment_names": sorted(PIPELINE_ENV_NAMES),
            "present_environment_names": present,
            "credential_values_recorded": False,
        }

    database_url = ambient_env[PIPELINE_DATABASE_ENV]
    kafka = ambient_env[PIPELINE_KAFKA_ENV]
    s3_endpoint = ambient_env[PIPELINE_S3_ENDPOINT_ENV]
    bucket = ambient_env[PIPELINE_S3_BUCKET_ENV]
    errors: list[str] = []

    database = urlsplit(database_url)
    if database.scheme not in {"postgres", "postgresql"}:
        errors.append("database URL scheme must be postgres or postgresql")
    if not _is_loopback_host(database.hostname):
        errors.append("database host must be loopback")
    if not database.path.lstrip("/"):
        errors.append("database URL must name a database")
    try:
        _ = database.port
    except ValueError:
        errors.append("database URL port is invalid")

    try:
        kafka_endpoints = tuple(_split_host_port(value) for value in kafka.split(","))
    except ValueError as exc:
        errors.append(str(exc))
        kafka_endpoints = ()
    if kafka_endpoints and not all(
        _is_loopback_host(host) for host, _port in kafka_endpoints
    ):
        errors.append("every Kafka bootstrap host must be loopback")

    s3 = urlsplit(s3_endpoint)
    if s3.scheme not in {"http", "https"}:
        errors.append("S3 endpoint scheme must be http or https")
    if not _is_loopback_host(s3.hostname):
        errors.append("S3 endpoint host must be loopback")
    try:
        _ = s3.port
    except ValueError:
        errors.append("S3 endpoint port is invalid")
    if _BUCKET_RE.fullmatch(bucket) is None:
        errors.append("S3 raw bucket is not a valid local bucket name")

    if errors:
        return None, {
            "state": "blocked",
            "reason_code": "isolated_infrastructure_rejected",
            "reason": "; ".join(errors),
            "required_environment_names": sorted(PIPELINE_ENV_NAMES),
            "present_environment_names": present,
            "credential_values_recorded": False,
        }
    return (
        PipelineProbeConfig(
            database_url=database_url,
            kafka_bootstrap_servers=kafka,
            s3_endpoint_url=s3_endpoint,
            s3_raw_bucket=bucket,
        ),
        {
            "state": "accepted",
            "reason_code": "isolated_infrastructure_accepted",
            "required_environment_names": sorted(PIPELINE_ENV_NAMES),
            "present_environment_names": present,
            "credential_values_recorded": False,
        },
    )


@contextlib.contextmanager
def _runtime_environment(config: PipelineProbeConfig) -> Any:
    """Temporarily expose standard runtime names to real subprocesses."""

    previous = dict(os.environ)
    os.environ.update(
        {
            "AWS_ACCESS_KEY_ID": "certification-local",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_SECRET_ACCESS_KEY": "certification-local",
            "COMPANY_OS_ENV": "test",
            "DATABASE_URL": config.database_url,
            "FYRALIS_ENV": "test",
            "INGESTION_ENV": "certification",
            "KAFKA_BOOTSTRAP_SERVERS": config.kafka_bootstrap_servers,
            "S3_ENDPOINT_URL": config.s3_endpoint_url,
            "S3_RAW_BUCKET": config.s3_raw_bucket,
        },
    )
    try:
        yield
    finally:
        # SigningSecrets and provider helpers add environment variables too.
        # Restore the entire mapping so a direct in-process test cannot leak
        # synthetic secrets into a later stage.
        os.environ.clear()
        os.environ.update(previous)


async def _database_readiness(pool: Any, source_id: str) -> dict[str, object]:
    required_tables = (
        "ingestion_source_catalog",
        "observations",
        "think_trigger_queue",
        "tenants",
        "tenant_flags",
    )
    missing = [
        table
        for table in required_tables
        if await pool.fetchval("SELECT to_regclass($1)", f"public.{table}") is None
    ]
    if missing:
        raise PipelineProbeError(
            "the dedicated certification database is not migrated; missing "
            + ", ".join(missing),
        )
    membership = await pool.fetchrow(
        """
        SELECT id, historical_supported, data_plane
          FROM ingestion_source_catalog
         WHERE id = $1
        """,
        source_id,
    )
    if membership is None or membership["data_plane"] is not True:
        raise PipelineProbeError(
            f"{source_id} is absent from the database source catalog",
        )
    return {
        "required_tables_present": list(required_tables),
        "source_catalog_id": membership["id"],
        "historical_supported": bool(
            membership["historical_supported"],
        ),
        "data_plane": bool(membership["data_plane"]),
    }


async def _read_topic(
    *,
    bootstrap_servers: str,
    topic: str,
    source_id: str,
    tenant_id: UUID,
    timeout_s: float = 15.0,
) -> list[_KafkaRecord]:
    """Read a bounded topic snapshot and retain only the exact tenant."""

    from aiokafka import (  # type: ignore[import-untyped]
        AIOKafkaConsumer,
        TopicPartition,
    )
    import orjson

    partition_ids = await _topic_partition_ids(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        request_timeout_ms=max(1_000, int(timeout_s * 1_000)),
    )
    await consumer.start()
    try:
        partitions = [
            TopicPartition(topic, partition) for partition in sorted(partition_ids)
        ]
        consumer.assign(partitions)
        await consumer.seek_to_beginning(*partitions)
        ends = await consumer.end_offsets(partitions)
        records: list[_KafkaRecord] = []
        deadline = time.monotonic() + timeout_s
        while True:
            positions = {
                partition: await consumer.position(partition)
                for partition in partitions
            }
            if all(positions[partition] >= ends[partition] for partition in partitions):
                break
            if time.monotonic() >= deadline:
                raise PipelineProbeError(
                    f"timed out reading Kafka snapshot for {topic}",
                )
            batches = await consumer.getmany(
                *partitions,
                timeout_ms=500,
                max_records=10_000,
            )
            for batch in batches.values():
                for record in batch:
                    try:
                        decoded = orjson.loads(record.value)
                    except Exception as exc:  # noqa: BLE001
                        raise PipelineProbeError(
                            f"{topic} contains a non-JSON record at "
                            f"{record.partition}:{record.offset}",
                        ) from exc
                    if not isinstance(decoded, Mapping):
                        raise PipelineProbeError(
                            f"{topic} record is not a JSON object",
                        )
                    if str(decoded.get("tenant_id")) != str(tenant_id):
                        continue
                    if decoded.get("source") != source_id:
                        raise PipelineProbeError(
                            f"{topic} contains selected tenant {tenant_id} "
                            f"with foreign source {decoded.get('source')!r}",
                        )
                    records.append(
                        _KafkaRecord(
                            topic=topic,
                            partition=record.partition,
                            offset=record.offset,
                            key=record.key,
                            value=bytes(record.value),
                            headers=tuple(record.headers or ()),
                            decoded=decoded,
                        )
                    )
        return records
    finally:
        with contextlib.suppress(asyncio.CancelledError):
            await consumer.stop()


async def _topic_partition_ids(
    *,
    bootstrap_servers: str,
    topic: str,
) -> tuple[int, ...]:
    from aiokafka.admin import (  # type: ignore[import-untyped]
        AIOKafkaAdminClient,
    )

    admin = AIOKafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
    )
    await admin.start()
    try:
        descriptions = await admin.describe_topics([topic])
    finally:
        await admin.close()
    if len(descriptions) != 1 or descriptions[0].get("error_code") != 0:
        raise PipelineProbeError(
            f"required Kafka topic {topic!r} does not exist",
        )
    partition_ids = tuple(
        sorted(
            int(partition["partition"])
            for partition in descriptions[0].get("partitions", ())
            if partition.get("error_code") == 0
        )
    )
    if not partition_ids:
        raise PipelineProbeError(
            f"required Kafka topic {topic!r} has no readable partitions",
        )
    return partition_ids


def _records_artifact(
    records: Sequence[_KafkaRecord],
) -> dict[str, object]:
    offsets: dict[int, list[int]] = {}
    for record in records:
        offsets.setdefault(record.partition, []).append(record.offset)
    return {
        "count": len(records),
        "value_sha256": sorted(_sha256_bytes(record.value) for record in records),
        "partitions": [
            {
                "partition": partition,
                "first_offset": min(values),
                "last_offset": max(values),
                "record_count": len(values),
            }
            for partition, values in sorted(offsets.items())
        ],
    }


async def _verify_s3_objects(
    config: PipelineProbeConfig,
    *,
    source_id: str,
    tenant_id: UUID,
    raw_records: Sequence[_KafkaRecord],
) -> dict[str, object]:
    from services.ingest.ingestion.raw_tier.envelope import RawEnvelope
    from services.ingest.ingestion.raw_tier.s3 import S3Client

    key_hashes: list[str] = []
    body_hashes: list[str] = []
    total_bytes = 0
    seen: set[tuple[str, str]] = set()
    async with S3Client(
        config.s3_raw_bucket,
        endpoint_url=config.s3_endpoint_url,
        region_name="us-east-1",
    ) as s3:
        for record in raw_records:
            envelope = RawEnvelope.model_validate(record.decoded)
            if envelope.source != source_id or envelope.tenant_id != tenant_id:
                raise PipelineProbeError(
                    "raw envelope source/tenant escaped selected scope",
                )
            expected_fragment = f"/{source_id}/{tenant_id}/"
            if expected_fragment not in f"/{envelope.raw_s3_key}":
                raise PipelineProbeError(
                    "raw S3 key is not scoped to the selected source/tenant",
                )
            identity = (envelope.raw_s3_key, envelope.content_hash)
            if identity in seen:
                continue
            body = await s3.get_verified(
                envelope.raw_s3_key,
                envelope.content_hash,
            )
            seen.add(identity)
            total_bytes += len(body)
            key_hashes.append(
                _sha256_bytes(envelope.raw_s3_key.encode("utf-8")),
            )
            body_hashes.append(_sha256_bytes(body))
    if not seen:
        raise PipelineProbeError("no raw S3 object was verified")
    return {
        "verified_object_count": len(seen),
        "verified_total_bytes": total_bytes,
        "key_sha256": sorted(key_hashes),
        "body_sha256": sorted(body_hashes),
        "content_hash_verified": True,
        "source_tenant_key_scope_verified": True,
    }


def _verify_normalized_records(
    *,
    source_id: str,
    tenant_id: UUID,
    raw_records: Sequence[_KafkaRecord],
    normalized_records: Sequence[_KafkaRecord],
) -> dict[str, object]:
    from services.ingest.ingestion.normalizer.models import (
        NormalizedEnvelope,
    )
    from services.ingest.ingestion.raw_tier.envelope import RawEnvelope

    raw_identities = {
        (
            RawEnvelope.model_validate(record.decoded).raw_s3_key,
            RawEnvelope.model_validate(record.decoded).content_hash,
        )
        for record in raw_records
    }
    channels: set[str] = set()
    identities: set[tuple[str, str, str]] = set()
    for record in normalized_records:
        envelope = NormalizedEnvelope.model_validate(record.decoded)
        if envelope.source != source_id or envelope.tenant_id != tenant_id:
            raise PipelineProbeError(
                "normalized envelope source/tenant escaped selected scope",
            )
        if (envelope.raw_s3_key, envelope.content_hash) not in raw_identities:
            raise PipelineProbeError(
                "normalized envelope has no selected raw/S3 parent",
            )
        channels.add(envelope.source_channel)
        if envelope.external_id is None:
            raise PipelineProbeError(
                "normalized certification envelope has no external_id",
            )
        identities.add(
            (
                envelope.source_channel,
                envelope.external_id,
                _identity_timestamp(envelope.occurred_at),
            ),
        )
    if not normalized_records:
        raise PipelineProbeError("no normalized Kafka envelope was observed")
    return {
        "source_channels": sorted(channels),
        "all_records_have_selected_raw_parent": True,
        "observation_identity_count": len(identities),
        "observation_identity_set_sha256": _canonical_sha256(
            sorted(identities),
        ),
        "source_channel_set_sha256": _canonical_sha256(
            sorted(identity[0] for identity in identities),
        ),
        "external_id_set_sha256": _canonical_sha256(
            sorted(identity[1] for identity in identities),
        ),
        "occurred_at_set_sha256": _canonical_sha256(
            sorted(identity[2] for identity in identities),
        ),
    }


def _replayable_raw_records(
    *,
    raw_records: Sequence[_KafkaRecord],
    normalized_records: Sequence[_KafkaRecord],
) -> list[_KafkaRecord]:
    """Select one raw delivery for each successfully normalized parent.

    A replicated at-least-once run may already contain duplicate raw or
    normalized deliveries before certification takes its baseline snapshot.
    It may also contain a raw record that was deliberately routed to a DLQ.
    Treating every baseline delivery as a distinct replay candidate makes the
    expected growth depend on those transport accidents. Instead, replay one
    deterministic representative for every unique raw parent that is proven
    to have produced normalized output.
    """

    from services.ingest.ingestion.normalizer.models import NormalizedEnvelope
    from services.ingest.ingestion.raw_tier.envelope import RawEnvelope

    normalized_parents = {
        (
            envelope.raw_s3_key,
            envelope.content_hash,
        )
        for record in normalized_records
        for envelope in (NormalizedEnvelope.model_validate(record.decoded),)
    }
    representatives: dict[tuple[str, str], _KafkaRecord] = {}
    for record in raw_records:
        envelope = RawEnvelope.model_validate(record.decoded)
        identity = (envelope.raw_s3_key, envelope.content_hash)
        if identity in normalized_parents:
            representatives.setdefault(identity, record)
    if set(representatives) != normalized_parents:
        raise PipelineProbeError(
            "normalized replay candidates have no exact selected raw parent",
        )
    if not representatives:
        raise PipelineProbeError("no successfully normalized raw parent is replayable")
    return list(representatives.values())


def _installation_attribution(
    *,
    raw_records: Sequence[_KafkaRecord],
    normalized_records: Sequence[_KafkaRecord],
    expected_installation_row_ids: Sequence[UUID],
) -> dict[str, object]:
    """Prove both sibling installations contributed to both Kafka lanes."""

    from services.ingest.ingestion.normalizer.models import NormalizedEnvelope
    from services.ingest.ingestion.raw_tier.envelope import RawEnvelope

    expected = {str(value) for value in expected_installation_row_ids}
    if len(expected) != 2:
        raise PipelineProbeError(
            "history topology requires exactly two installation row IDs " "per tenant",
        )

    def _counts(
        records: Sequence[_KafkaRecord],
        *,
        normalized: bool,
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in records:
            envelope = (
                NormalizedEnvelope.model_validate(record.decoded)
                if normalized
                else RawEnvelope.model_validate(record.decoded)
            )
            raw_installation = envelope.ingress_metadata.get(
                "installation_row_id",
            )
            try:
                installation_id = str(UUID(str(raw_installation)))
            except (TypeError, ValueError) as exc:
                raise PipelineProbeError(
                    "historical envelope lacks an exact installation row ID",
                ) from exc
            counts[installation_id] = counts.get(installation_id, 0) + 1
        return counts

    raw_counts = _counts(raw_records, normalized=False)
    normalized_counts = _counts(normalized_records, normalized=True)
    if (
        set(raw_counts) != expected
        or set(normalized_counts) != expected
        or any(
            count < 1 for count in (*raw_counts.values(), *normalized_counts.values())
        )
    ):
        raise PipelineProbeError(
            "historical Kafka lanes did not retain both exact sibling "
            "installation identities",
        )
    return {
        "expected_installation_row_ids": sorted(expected),
        "raw_records_by_installation": dict(sorted(raw_counts.items())),
        "normalized_records_by_installation": dict(
            sorted(normalized_counts.items()),
        ),
        "exact_installation_row_set": True,
    }


async def _observation_snapshot(
    pool: Any,
    *,
    source_id: str,
    tenant_id: UUID,
    expected_count: int,
) -> dict[str, object]:
    from services.ingest.source_contract.catalog import source_definition

    rows = await pool.fetch(
        """
        SELECT id, tenant_id, source_channel, external_id, occurred_at
          FROM observations
         WHERE tenant_id = $1
         ORDER BY source_channel, external_id, occurred_at, id
        """,
        tenant_id,
    )
    if len(rows) != expected_count:
        raise PipelineProbeError(
            f"{source_id} expected {expected_count} selected-tenant "
            f"Observations, found {len(rows)}",
        )
    if not rows:
        raise PipelineProbeError("Observation assertion would be vacuous")
    allowed_channels = set(
        source_definition(source_id).normalization_inputs,
    )
    observed_channels = {str(row["source_channel"]) for row in rows}
    if not observed_channels.issubset(allowed_channels):
        raise PipelineProbeError(
            f"{source_id} persisted foreign source channels "
            f"{sorted(observed_channels - allowed_channels)}",
        )
    if any(row["tenant_id"] != tenant_id for row in rows):
        raise PipelineProbeError("Observation tenant attribution escaped")
    if any(row["external_id"] is None for row in rows):
        raise PipelineProbeError(
            f"{source_id} certification Observation has no external_id",
        )
    identities = [
        (
            str(row["source_channel"]),
            str(row["external_id"]),
            _identity_timestamp(row["occurred_at"]),
        )
        for row in rows
    ]
    if len(identities) != len(set(identities)):
        raise PipelineProbeError(
            "duplicate Observation idempotency identities were persisted",
        )
    return {
        "count": len(rows),
        "expected_count": expected_count,
        "source_channels": sorted(observed_channels),
        "allowed_source_channels": sorted(allowed_channels),
        "all_external_ids_present": True,
        "unique_idempotency_identities": True,
        "identity_set_sha256": _canonical_sha256(sorted(identities)),
        "source_channel_set_sha256": _canonical_sha256(
            sorted(identity[0] for identity in identities),
        ),
        "external_id_set_sha256": _canonical_sha256(
            sorted(identity[1] for identity in identities),
        ),
        "occurred_at_set_sha256": _canonical_sha256(
            sorted(identity[2] for identity in identities),
        ),
    }


async def _t1_snapshot(
    pool: Any,
    *,
    tenant_id: UUID,
) -> dict[str, object]:
    from services.ingest.synthetic.validation_runs.assertions import (
        assert_observations_have_exactly_one_t1_trigger,
    )

    count = await assert_observations_have_exactly_one_t1_trigger(
        pool,
        {tenant_id},
    )
    identities = await pool.fetch(
        """
        SELECT q.observation_id, q.tenant_id
          FROM think_trigger_queue q
          JOIN observations o ON o.id = q.observation_id
         WHERE o.tenant_id = $1
           AND q.trigger_kind = 'T1'
           AND q.trigger_subkind = 'event_arrival'
         ORDER BY q.observation_id
        """,
        tenant_id,
    )
    return {
        "observation_count": count,
        "t1_count": len(identities),
        "exactly_one_per_observation": len(identities) == count,
        "same_tenant": all(row["tenant_id"] == tenant_id for row in identities),
        "observation_id_set_sha256": _canonical_sha256(
            [str(row["observation_id"]) for row in identities],
        ),
    }


async def _replay_raw_records(
    *,
    bootstrap_servers: str,
    records: Sequence[_KafkaRecord],
) -> None:
    from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()
    try:
        for record in records:
            await producer.send_and_wait(
                record.topic,
                value=record.value,
                key=record.key,
                headers=list(record.headers),
            )
    finally:
        await producer.stop()


async def _wait_for_record_count(
    *,
    config: PipelineProbeConfig,
    topic: str,
    source_id: str,
    tenant_id: UUID,
    minimum_count: int,
    timeout_s: float = 30.0,
) -> list[_KafkaRecord]:
    deadline = time.monotonic() + timeout_s
    latest: list[_KafkaRecord] = []
    while time.monotonic() < deadline:
        latest = await _read_topic(
            bootstrap_servers=config.kafka_bootstrap_servers,
            topic=topic,
            source_id=source_id,
            tenant_id=tenant_id,
            timeout_s=min(10.0, max(2.0, deadline - time.monotonic())),
        )
        if len(latest) >= minimum_count:
            return latest
        await asyncio.sleep(0.25)
    raise PipelineProbeError(
        f"{topic} did not reach {minimum_count} selected records after replay; "
        f"found {len(latest)}",
    )


async def _wait_for_group_topic_drain(
    *,
    bootstrap_servers: str,
    group_id: str,
    topic: str,
    timeout_s: float = 30.0,
) -> dict[str, object]:
    from aiokafka import (  # type: ignore[import-untyped]
        AIOKafkaConsumer,
        TopicPartition,
    )
    from aiokafka.admin import (  # type: ignore[import-untyped]
        AIOKafkaAdminClient,
    )

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
    )
    await admin.start()
    await consumer.start()
    try:
        partition_ids = await _topic_partition_ids(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )
        partitions = [
            TopicPartition(topic, partition) for partition in sorted(partition_ids)
        ]
        deadline = time.monotonic() + timeout_s
        last_lag: dict[int, int] = {}
        while time.monotonic() < deadline:
            ends = await consumer.end_offsets(partitions)
            committed = await admin.list_consumer_group_offsets(
                group_id,
                partitions=partitions,
            )
            last_lag = {
                partition.partition: max(
                    0,
                    ends[partition] - max(0, committed.get(partition, (-1, ""))[0]),
                )
                for partition in partitions
            }
            if all(lag == 0 for lag in last_lag.values()):
                return {
                    "consumer_group_sha256": _sha256_bytes(
                        group_id.encode("utf-8"),
                    ),
                    "topic": topic,
                    "partition_lag": {
                        str(partition): lag
                        for partition, lag in sorted(last_lag.items())
                    },
                    "drained": True,
                }
            await asyncio.sleep(0.25)
        raise PipelineProbeError(
            f"consumer group did not drain {topic}; lag={last_lag}",
        )
    finally:
        with contextlib.suppress(asyncio.CancelledError):
            await consumer.stop()
        await admin.close()


async def _pipeline_evidence(
    *,
    pool: Any,
    config: PipelineProbeConfig,
    source_id: str,
    tenant_id: UUID,
    expected_count: int,
    consumer_groups: Mapping[str, str],
    expected_installation_row_ids: Sequence[UUID] = (),
) -> dict[str, object]:
    from services.ingest.ingestion.kafka.topics import topic_for

    raw_topic = topic_for("raw", source_id)
    normalized_topic = topic_for("normalized", source_id)
    raw_initial = await _read_topic(
        bootstrap_servers=config.kafka_bootstrap_servers,
        topic=raw_topic,
        source_id=source_id,
        tenant_id=tenant_id,
    )
    normalized_initial = await _read_topic(
        bootstrap_servers=config.kafka_bootstrap_servers,
        topic=normalized_topic,
        source_id=source_id,
        tenant_id=tenant_id,
    )
    if not raw_initial:
        raise PipelineProbeError("no raw Kafka envelope was observed")
    s3_evidence = await _verify_s3_objects(
        config,
        source_id=source_id,
        tenant_id=tenant_id,
        raw_records=raw_initial,
    )
    normalized_parent_evidence = _verify_normalized_records(
        source_id=source_id,
        tenant_id=tenant_id,
        raw_records=raw_initial,
        normalized_records=normalized_initial,
    )
    observation_before = await _observation_snapshot(
        pool,
        source_id=source_id,
        tenant_id=tenant_id,
        expected_count=expected_count,
    )
    t1_before = await _t1_snapshot(pool, tenant_id=tenant_id)
    if (
        normalized_parent_evidence["observation_identity_count"]
        != observation_before["count"]
        or normalized_parent_evidence["observation_identity_set_sha256"]
        != observation_before["identity_set_sha256"]
    ):
        raise PipelineProbeError(
            "normalized and persisted Observation identity sets differ: "
            f"normalized_count="
            f"{normalized_parent_evidence['observation_identity_count']}, "
            f"observation_count={observation_before['count']}, "
            f"normalized_sha256="
            f"{normalized_parent_evidence['observation_identity_set_sha256']}, "
            f"observation_sha256={observation_before['identity_set_sha256']}, "
            f"channel_sha256="
            f"{normalized_parent_evidence['source_channel_set_sha256']}/"
            f"{observation_before['source_channel_set_sha256']}, "
            f"external_id_sha256="
            f"{normalized_parent_evidence['external_id_set_sha256']}/"
            f"{observation_before['external_id_set_sha256']}, "
            f"occurred_at_sha256="
            f"{normalized_parent_evidence['occurred_at_set_sha256']}/"
            f"{observation_before['occurred_at_set_sha256']}",
        )
    installation_evidence = (
        _installation_attribution(
            raw_records=raw_initial,
            normalized_records=normalized_initial,
            expected_installation_row_ids=expected_installation_row_ids,
        )
        if expected_installation_row_ids
        else None
    )

    replay_records = _replayable_raw_records(
        raw_records=raw_initial,
        normalized_records=normalized_initial,
    )
    await _replay_raw_records(
        bootstrap_servers=config.kafka_bootstrap_servers,
        records=replay_records,
    )
    # A producer ACK proves the replay reached Kafka, not that the replicated
    # normalizer group has finished every record. Wait on its committed raw
    # offsets before asserting normalized growth; reading the normalized topic
    # first created an off-by-one race under cooperative rebalances.
    await _wait_for_group_topic_drain(
        bootstrap_servers=config.kafka_bootstrap_servers,
        group_id=consumer_groups["raw"],
        topic=raw_topic,
    )
    raw_after = await _wait_for_record_count(
        config=config,
        topic=raw_topic,
        source_id=source_id,
        tenant_id=tenant_id,
        minimum_count=len(raw_initial) + len(replay_records),
    )
    normalized_after = await _wait_for_record_count(
        config=config,
        topic=normalized_topic,
        source_id=source_id,
        tenant_id=tenant_id,
        minimum_count=len(normalized_initial) + len(replay_records),
    )
    writer_drain = await _wait_for_group_topic_drain(
        bootstrap_servers=config.kafka_bootstrap_servers,
        group_id=consumer_groups["normalized"],
        topic=normalized_topic,
    )
    observation_after = await _observation_snapshot(
        pool,
        source_id=source_id,
        tenant_id=tenant_id,
        expected_count=expected_count,
    )
    t1_after = await _t1_snapshot(pool, tenant_id=tenant_id)
    if observation_after != observation_before:
        raise PipelineProbeError(
            "Observation identity changed after raw-envelope replay",
        )
    if t1_after != t1_before:
        raise PipelineProbeError(
            "T1 identity/count changed after raw-envelope replay",
        )

    evidence: dict[str, object] = {
        "tenant_id": str(tenant_id),
        "raw_topic": {
            "topic": raw_topic,
            "before_replay": _records_artifact(raw_initial),
            "after_replay": _records_artifact(raw_after),
            "selected_source_tenant_only": True,
        },
        "s3_raw_evidence": s3_evidence,
        "normalized_topic": {
            "topic": normalized_topic,
            "before_replay": _records_artifact(normalized_initial),
            "after_replay": _records_artifact(normalized_after),
            **normalized_parent_evidence,
            "selected_source_tenant_only": True,
        },
        "observations": {
            "before_replay": observation_before,
            "after_replay": observation_after,
            "unchanged_after_replay": True,
        },
        "t1_triggers": {
            "before_replay": t1_before,
            "after_replay": t1_after,
            "unchanged_after_replay": True,
        },
        "replay": {
            "schema_version": PIPELINE_REPLAY_SCHEMA_VERSION,
            "raw_records_before": len(raw_initial),
            "raw_records_replayed": len(replay_records),
            "raw_records_after": len(raw_after),
            "raw_topic_record_growth": len(raw_after) - len(raw_initial),
            "normalized_records_before": len(normalized_initial),
            "normalized_records_after": len(normalized_after),
            "normalized_topic_record_growth": (
                len(normalized_after) - len(normalized_initial)
            ),
            "observation_count_before": observation_before["count"],
            "observation_count_after": observation_after["count"],
            "observation_identity_set_sha256_before": (
                observation_before["identity_set_sha256"]
            ),
            "observation_identity_set_sha256_after": (
                observation_after["identity_set_sha256"]
            ),
            "t1_count_before": t1_before["t1_count"],
            "t1_count_after": t1_after["t1_count"],
            "t1_observation_id_set_sha256_before": (
                t1_before["observation_id_set_sha256"]
            ),
            "t1_observation_id_set_sha256_after": (
                t1_after["observation_id_set_sha256"]
            ),
            "writer_group_drain": writer_drain,
            "observation_count_growth": 0,
            "t1_count_growth": 0,
            "idempotency_proven": True,
        },
    }
    if installation_evidence is not None:
        evidence["installation_attribution"] = installation_evidence
    return evidence


def _unique_history_scenarios(source_id: str) -> list[Any]:
    from services.ingest.synthetic.backfill_harness.scenarios import (
        BackfillScenario,
    )
    from services.ingest.synthetic.validation_runs.runs import (
        certification_history_scenarios,
    )

    templates = [
        scenario
        for scenario in certification_history_scenarios(
            tenants_per_source=2,
            installations_per_tenant=2,
        )
        if scenario.source == source_id
    ]
    if len(templates) != 4:
        raise PipelineProbeError(
            f"{source_id} did not produce the exact four-scenario " "history topology",
        )
    token = uuid4().hex[:12]
    tenant_slugs: dict[str, str] = {}
    installation_indexes: dict[str, int] = {}
    scenarios: list[Any] = []
    for template in templates:
        tenant_slug = tenant_slugs.setdefault(
            template.tenant_slug,
            f"cert-{source_id}-{token}-tenant-{len(tenant_slugs)}",
        )
        installation_index = installation_indexes.get(tenant_slug, 0)
        installation_indexes[tenant_slug] = installation_index + 1
        scenarios.append(
            BackfillScenario(
                tenant_slug=tenant_slug,
                source=source_id,
                installation_key=(f"{tenant_slug}-installation-{installation_index}"),
                fixture_params=dict(template.fixture_params),
                fault_profile=template.fault_profile,
                expected_observation_count=(template.expected_observation_count),
            ),
        )
    return scenarios


def _harness_consumer_groups(harness: Any) -> dict[str, str]:
    """Read the public group map, rejecting an older harness fail closed."""

    value = getattr(harness, "consumer_group_ids", None)
    if not isinstance(value, Mapping) or set(value) != {
        "raw",
        "normalized",
    }:
        raise PipelineProbeError(
            "BackfillHarness does not expose exact consumer group identities",
        )
    return {str(key): str(item) for key, item in value.items()}


def _assert_harness_processes(result: Any) -> dict[str, object]:
    failures = {
        name: returncode
        for name, returncode in result.subprocess_returncodes.items()
        if returncode != 0
    }
    if failures:
        stderr_tails = {
            name: str(result.subprocess_stderr_tails.get(name, ""))[-800:]
            for name in failures
            if result.subprocess_stderr_tails.get(name)
        }
        raise PipelineProbeError(
            "pipeline subprocesses did not shut down cleanly: "
            f"returncodes={failures!r}, stderr_tails={stderr_tails!r}",
        )
    return {
        "configured_replicas": result.configured_replicas,
        "subprocess_count": len(result.subprocess_returncodes),
        "all_subprocess_returncodes_zero": True,
        "wall_time_seconds": result.wall_time_seconds,
    }


def _history_topology_evidence(
    *,
    result: Any,
    harness: Any,
    tenant_pipelines: Sequence[Mapping[str, Any]],
) -> dict[str, object]:
    """Build exact, validator-replayable 2×2×2 history evidence."""

    outcomes_by_tenant: dict[UUID, list[Any]] = {}
    for outcome in result.outcomes:
        outcomes_by_tenant.setdefault(outcome.tenant_id, []).append(outcome)
    if len(outcomes_by_tenant) != 2 or any(
        len(outcomes) != 2 for outcomes in outcomes_by_tenant.values()
    ):
        raise PipelineProbeError(
            "history topology did not yield two tenants with two exact "
            "installations each",
        )

    pipelines_by_tenant = {
        UUID(str(pipeline.get("tenant_id"))): pipeline for pipeline in tenant_pipelines
    }
    if set(pipelines_by_tenant) != set(outcomes_by_tenant):
        raise PipelineProbeError(
            "history topology pipeline coverage differs from tenant outcomes",
        )

    normalized_identities: dict[UUID, str] = {}
    tenant_rows: list[dict[str, object]] = []
    for tenant_id, outcomes in sorted(
        outcomes_by_tenant.items(),
        key=lambda item: item[1][0].scenario.tenant_slug,
    ):
        expected = sum(
            outcome.scenario.expected_observation_count for outcome in outcomes
        )
        pipeline = pipelines_by_tenant[tenant_id]
        normalized = _proof_mapping(
            pipeline.get("normalized_topic"),
            field="history tenant normalized_topic",
        )
        observations = _proof_mapping(
            pipeline.get("observations"),
            field="history tenant observations",
        )
        before = _proof_mapping(
            observations.get("before_replay"),
            field="history tenant observations.before_replay",
        )
        normalized_hash = _proof_sha256(
            normalized,
            "observation_identity_set_sha256",
        )
        persisted_hash = _proof_sha256(
            before,
            "identity_set_sha256",
        )
        if (
            before.get("count") != expected
            or normalized.get("observation_identity_count") != expected
            or normalized_hash != persisted_hash
        ):
            raise PipelineProbeError(
                "history topology per-tenant Observation identity/count "
                "evidence differs from the exact fixture oracle",
            )
        normalized_identities[tenant_id] = normalized_hash
        tenant_rows.append(
            {
                "tenant_id": str(tenant_id),
                "tenant_slug": outcomes[0].scenario.tenant_slug,
                "expected_observation_count": expected,
                "observed_observation_count": before["count"],
                "installation_keys": sorted(
                    outcome.scenario.resolved_installation_key for outcome in outcomes
                ),
                "installation_row_ids": sorted(
                    str(outcome.installation_row_id) for outcome in outcomes
                ),
                "trigger_ids": sorted(str(outcome.trigger_id) for outcome in outcomes),
                "onboarding_run_ids": sorted(
                    str(outcome.onboarding_run_id) for outcome in outcomes
                ),
                "normalized_observation_identity_set_sha256": (normalized_hash),
                "persisted_observation_identity_set_sha256": (persisted_hash),
                "cross_tenant_leak_count": 0,
            },
        )
    if len(set(normalized_identities.values())) != 2:
        raise PipelineProbeError(
            "history topology selected tenants have an ambiguous Observation "
            "identity set",
        )

    replica_activity = result.replica_workflow_activity.get(
        "oauth_poller",
        {},
    )
    expected_replica_ids = set(
        harness.replica_workflow_ids("oauth_poller"),
    )
    if (
        result.configured_replicas != 2
        or result.observed_replica_count != 2
        or result.participating_replica_count != 2
        or set(replica_activity) != expected_replica_ids
        or any(int(claims) < 1 for claims in replica_activity.values())
    ):
        raise PipelineProbeError(
            "history topology did not record durable work claims from both "
            "OAuth replicas",
        )

    return {
        "schema_version": PIPELINE_TOPOLOGY_SCHEMA_VERSION,
        "tenant_count": 2,
        "installations_per_tenant": 2,
        "installation_count": 4,
        "configured_replicas": result.configured_replicas,
        "observed_oauth_replicas": result.observed_replica_count,
        "participating_oauth_replicas": (result.participating_replica_count),
        "oauth_replica_claims": dict(sorted(replica_activity.items())),
        "tenants": tenant_rows,
        "exact_installation_identity_proven": True,
        "per_tenant_observation_counts_proven": True,
        "cross_tenant_leak_count": 0,
        "cross_tenant_isolation_proven": True,
        "two_replica_participation_proven": True,
    }


async def _run_history_pipeline(
    *,
    pool: Any,
    config: PipelineProbeConfig,
    source_id: str,
) -> tuple[dict[str, object], set[UUID]]:
    from services.ingest.synthetic.backfill_harness.assertions import (
        assert_all_complete,
        assert_completion_emitted_per_tenant,
        assert_cursor_monotonic_per_shard,
        assert_no_duplicate_observations,
        assert_observation_count_matches_fixture,
        assert_sibling_installation_identity,
    )
    from services.ingest.synthetic.backfill_harness.harness import (
        BackfillHarness,
    )

    scenarios = _unique_history_scenarios(source_id)
    harness = BackfillHarness(
        pool=pool,
        scenarios=scenarios,
        concurrency=4,
        completion_deadline_s=120.0,
        kafka_bootstrap_servers=config.kafka_bootstrap_servers,
        drain_timeout_s=120.0,
        replicas=2,
    )
    stderrs: dict[str, str] = {}
    evidence: dict[str, object] | None = None
    outcomes = await harness.setup()
    tenant_ids = {outcome.tenant_id for outcome in outcomes}
    cleanup: dict[str, object] = {
        "deleted_tenants": 0,
        "completed": False,
    }
    try:
        await harness.start_services_with_replica_barrier()
        await harness.wait_for_backfill()
        await harness.drain()
        await harness.collect()
        provisional = harness.build_result({})
        assert_all_complete(provisional)
        assert_completion_emitted_per_tenant(provisional)
        assert_cursor_monotonic_per_shard(provisional)
        assert_no_duplicate_observations(provisional)
        assert_observation_count_matches_fixture(provisional)
        assert_sibling_installation_identity(
            provisional,
            installations_per_tenant=2,
        )
        outcomes_by_tenant: dict[UUID, list[Any]] = {}
        for outcome in outcomes:
            outcomes_by_tenant.setdefault(
                outcome.tenant_id,
                [],
            ).append(outcome)
        tenant_pipelines: list[dict[str, object]] = []
        for tenant_id, tenant_outcomes in sorted(
            outcomes_by_tenant.items(),
            key=lambda item: item[1][0].scenario.tenant_slug,
        ):
            tenant_pipelines.append(
                await _pipeline_evidence(
                    pool=pool,
                    config=config,
                    source_id=source_id,
                    tenant_id=tenant_id,
                    expected_count=sum(
                        outcome.scenario.expected_observation_count
                        for outcome in tenant_outcomes
                    ),
                    consumer_groups=_harness_consumer_groups(harness),
                    expected_installation_row_ids=[
                        outcome.installation_row_id
                        for outcome in tenant_outcomes
                        if outcome.installation_row_id is not None
                    ],
                ),
            )
        topology = _history_topology_evidence(
            result=provisional,
            harness=harness,
            tenant_pipelines=tenant_pipelines,
        )
        evidence = {
            "ingress_mode": "historical_backfill",
            "expected_observation_count": sum(
                scenario.expected_observation_count for scenario in scenarios
            ),
            "installation_identity": list(
                provisional.installation_identity_evidence,
            ),
            "tenant_pipelines": tenant_pipelines,
            "topology": topology,
            "completion_signals_exactly_once": True,
            "cursor_monotonic": True,
        }
        validate_history_topology_proof(
            {"source_id": source_id, "pipeline": evidence},
        )
    finally:
        stderrs = harness.teardown()
        cleanup = await _cleanup_tenants(pool, tenant_ids)
    result = harness.build_result(stderrs)
    process_evidence = _assert_harness_processes(result)
    if evidence is None:
        raise PipelineProbeError("history pipeline emitted no evidence")
    evidence.update(
        {
            "processes": process_evidence,
            "cleanup": cleanup,
        },
    )
    return evidence, tenant_ids


async def _wait_for_observations(
    pool: Any,
    *,
    tenant_id: UUID,
    expected_count: int,
    timeout_s: float = 60.0,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        count = int(
            await pool.fetchval(
                "SELECT count(*) FROM observations WHERE tenant_id = $1",
                tenant_id,
            ),
        )
        if count >= expected_count:
            return
        await asyncio.sleep(0.25)
    raise PipelineProbeError(
        f"live-only pipeline did not persist {expected_count} Observations",
    )


async def _run_live_only_pipeline(
    *,
    pool: Any,
    config: PipelineProbeConfig,
    source_id: str,
) -> tuple[dict[str, object], set[UUID]]:
    from services.ingest.ingestion.feature_flags.client import TenantFlags
    from services.ingest.ingestion.kafka.producer import (
        IdempotentProducer,
        ProducerConfig,
    )
    from services.ingest.ingestion.raw_tier.s3 import S3Client
    from services.ingest.synthetic.backfill_harness.harness import (
        BackfillHarness,
    )
    from services.ingest.synthetic.validation_runs.composition import (
        SigningSecrets,
        dispatch_live_concurrent,
        prepare_live_drivers,
        seed_contract_live_only_targets,
        teardown_live_drivers,
    )

    harness = BackfillHarness(
        pool=pool,
        scenarios=[],
        concurrency=1,
        completion_deadline_s=60.0,
        kafka_bootstrap_servers=config.kafka_bootstrap_servers,
        drain_timeout_s=60.0,
    )
    producer: IdempotentProducer | None = None
    s3: S3Client | None = None
    drivers: Any = None
    stderrs: dict[str, str] = {}
    tenant_ids: set[UUID] = set()
    evidence: dict[str, object] | None = None
    cleanup: dict[str, object] = {
        "deleted_tenants": 0,
        "completed": False,
    }
    await harness.setup()
    try:
        targets = await seed_contract_live_only_targets(
            pool,
            tenants_per_source=1,
        )
        selected = [target for target in targets if target.source == source_id]
        if len(selected) != 1 or len(targets) != 1:
            raise PipelineProbeError(
                "live-only bootstrap did not resolve exactly one selected "
                f"{source_id} target",
            )
        target = selected[0]
        tenant_ids = {target.tenant_id}
        producer = IdempotentProducer(
            ProducerConfig(
                bootstrap_servers=config.kafka_bootstrap_servers,
                client_id=f"certification-{source_id}-{uuid4().hex[:8]}",
            ),
        )
        await producer.start()
        s3 = S3Client(
            config.s3_raw_bucket,
            endpoint_url=config.s3_endpoint_url,
            region_name="us-east-1",
        )
        await s3.connect()
        drivers = await prepare_live_drivers(
            pool,
            selected,
            SigningSecrets(),
            kafka_producer=producer,
            s3_raw_client=s3,
            tenant_flags=TenantFlags(pool),
        )
        harness.start_services()
        dispatch = await dispatch_live_concurrent(
            drivers,
            selected,
            events_per_tenant=2,
        )
        if dispatch.dispatched_by_tenant != {target.tenant_id: 2}:
            raise PipelineProbeError(
                "live-only dispatch accounting did not match two events",
            )
        statuses = dispatch.http_status_by_source.get(source_id, set())
        if statuses != {202}:
            raise PipelineProbeError(
                f"{source_id} did not acknowledge Kafka-first ingress with "
                f"HTTP 202; got {sorted(statuses)}",
            )
        await _wait_for_observations(
            pool,
            tenant_id=target.tenant_id,
            expected_count=2,
        )
        evidence = await _pipeline_evidence(
            pool=pool,
            config=config,
            source_id=source_id,
            tenant_id=target.tenant_id,
            expected_count=2,
            consumer_groups=_harness_consumer_groups(harness),
        )
        await harness.collect()
        evidence.update(
            {
                "ingress_mode": "contract_live_only",
                "expected_observation_count": 2,
                "installation_identity": [
                    {
                        "source": source_id,
                        "tenant_id": str(target.tenant_id),
                        "provider_identity_sha256": _sha256_bytes(
                            str(
                                target.whatsapp_phone_number_id,
                            ).encode("utf-8"),
                        ),
                    },
                ],
                "dispatch": {
                    "events": 2,
                    "http_statuses": sorted(statuses),
                    "kafka_first_acknowledged": True,
                },
            },
        )
    finally:
        if drivers is not None:
            await teardown_live_drivers(drivers)
        if producer is not None:
            await producer.stop()
        if s3 is not None:
            await s3.close()
        stderrs = harness.teardown()
        cleanup = await _cleanup_tenants(pool, tenant_ids)
    result = harness.build_result(stderrs)
    process_evidence = _assert_harness_processes(result)
    if evidence is None:
        raise PipelineProbeError("live-only pipeline emitted no evidence")
    evidence["processes"] = process_evidence
    evidence["cleanup"] = cleanup
    return evidence, tenant_ids


async def _cleanup_tenants(pool: Any, tenant_ids: set[UUID]) -> dict[str, object]:
    if not tenant_ids:
        return {"deleted_tenants": 0, "completed": True}

    def _quote_identifier(value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    deleted_rows = 0
    deleted_tables: set[str] = set()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT c.relname AS table_name
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_attribute a ON a.attrelid = c.oid
             WHERE n.nspname = 'public'
               AND c.relkind IN ('r', 'p')
               AND a.attname = 'tenant_id'
               AND NOT a.attisdropped
               AND c.relname <> 'tenants'
             ORDER BY c.relkind = 'p', c.relname
            """,
        )
        pending = [str(row["table_name"]) for row in rows]
        blocked: dict[str, str] = {}
        for _pass in range(8):
            if not pending:
                break
            next_pending: list[str] = []
            progressed = False
            blocked = {}
            for table in pending:
                try:
                    status = await conn.execute(
                        f"DELETE FROM {_quote_identifier(table)} "
                        "WHERE tenant_id = ANY($1::uuid[])",
                        list(tenant_ids),
                    )
                except Exception as exc:  # noqa: BLE001
                    # Only FK ordering is expected. Preserve the exact error
                    # for a fail-closed terminal result after bounded passes.
                    if type(exc).__name__ != "ForeignKeyViolationError":
                        raise
                    next_pending.append(table)
                    blocked[table] = str(exc).splitlines()[0]
                    continue
                count = int(status.rsplit(" ", 1)[-1])
                deleted_rows += count
                if count:
                    deleted_tables.add(table)
                progressed = True
            pending = next_pending
            if not progressed:
                break
        if pending:
            summary = ", ".join(
                f"{table}: {blocked.get(table, 'blocked')}" for table in pending[:10]
            )
            raise PipelineProbeError(
                "tenant-scoped cleanup remained blocked after eight passes: " + summary,
            )
        result = await conn.execute(
            "DELETE FROM tenants WHERE id = ANY($1::uuid[])",
            list(tenant_ids),
        )
        deleted = int(result.rsplit(" ", 1)[-1])
    if deleted != len(tenant_ids):
        raise PipelineProbeError(
            f"pipeline cleanup deleted {deleted}/{len(tenant_ids)} tenants",
        )
    return {
        "deleted_tenants": deleted,
        "deleted_tenant_scoped_rows": deleted_rows,
        "deleted_table_count": len(deleted_tables),
        "completed": True,
    }


async def _execute_pipeline_probe(
    *,
    source_id: str,
    config: PipelineProbeConfig,
) -> dict[str, object]:
    import asyncpg  # type: ignore[import-untyped]
    from services.ingest.source_contract.catalog import source_definition

    started = time.monotonic()
    pool = await asyncpg.create_pool(
        config.database_url,
        min_size=2,
        max_size=10,
    )
    try:
        readiness = await _database_readiness(pool, source_id)
        source = source_definition(source_id)
        if source.history is None:
            pipeline, _tenant_ids = await _run_live_only_pipeline(
                pool=pool,
                config=config,
                source_id=source_id,
            )
        else:
            pipeline, _tenant_ids = await _run_history_pipeline(
                pool=pool,
                config=config,
                source_id=source_id,
            )
    finally:
        await pool.close()
    cleanup = pipeline.pop("cleanup")
    certified_scenarios = pipeline_scenario_ids_for_source(source_id)
    return {
        "schema_version": PIPELINE_PROBE_SCHEMA_VERSION,
        "state": "passed",
        "source_id": source_id,
        "certified_scenarios": sorted(certified_scenarios),
        "infrastructure": config.descriptor,
        "database_readiness": readiness,
        "pipeline": pipeline,
        "cleanup": cleanup,
        "elapsed_seconds": time.monotonic() - started,
        "claim_boundary": (
            (
                "This proves selected-source raw S3/Kafka, normalization, "
                "Observation idempotency, exactly-one same-tenant T1, two "
                "tenants with two exact installations, cross-tenant "
                "isolation, and durable work by two OAuth replicas on "
                "dedicated local infrastructure. It does not prove "
                "unrelated lifecycle, cursor-failure, distributed "
                "rate-limit, throughput, or real-provider scenarios."
            )
            if source.history is not None
            else (
                "This proves selected-source raw S3/Kafka, normalization, "
                "Observation idempotency, and exactly-one same-tenant T1 on "
                "dedicated local infrastructure. The source is live-only, "
                "so exact sibling-installation and durable two-replica "
                "topology scenarios remain explicitly blocked."
            )
        ),
    }


def _safe_error(exc: BaseException, config: PipelineProbeConfig) -> str:
    rendered = str(exc)
    for secret in (
        config.database_url,
        config.kafka_bootstrap_servers,
        config.s3_endpoint_url,
    ):
        rendered = rendered.replace(secret, "[redacted-endpoint]")
    return _REDACTED_URI_RE.sub("[redacted-uri]", rendered)[:2_000]


async def run_pipeline_probe(
    *,
    source_id: str,
    ambient_env: Mapping[str, str],
    executor: Callable[..., Any] | None = None,
) -> dict[str, object]:
    """Run the opt-in proof, returning blocked/failed/passed evidence.

    ``executor`` is an internal unit-test seam.  Production callers always use
    the real pipeline implementation.
    """

    config, resolution = _config_from_env(ambient_env)
    if config is None:
        return {
            "schema_version": PIPELINE_PROBE_SCHEMA_VERSION,
            "source_id": source_id,
            "certified_scenarios": [],
            "configuration": resolution,
            "state": "blocked",
            "claim_boundary": (
                "No data-plane scenario is promoted without explicit, "
                "dedicated loopback Postgres/Kafka/S3 infrastructure."
            ),
        }
    execute = executor or _execute_pipeline_probe
    try:
        with _runtime_environment(config):
            result = execute(source_id=source_id, config=config)
            if hasattr(result, "__await__"):
                result = await result
    except Exception as exc:  # noqa: BLE001 - stage artifact must survive
        return {
            "schema_version": PIPELINE_PROBE_SCHEMA_VERSION,
            "source_id": source_id,
            "certified_scenarios": [],
            "configuration": resolution,
            "infrastructure": config.descriptor,
            "state": "failed",
            "error_type": type(exc).__name__,
            "error": _safe_error(exc, config),
            "claim_boundary": (
                "The isolated pipeline was attempted and failed; no "
                "data-plane scenario is promoted."
            ),
        }
    if not isinstance(result, Mapping) or result.get("state") != "passed":
        raise PipelineProbeError(
            "pipeline executor returned an invalid passing artifact",
        )
    certified = result.get("certified_scenarios")
    expected_scenarios = pipeline_scenario_ids_for_source(source_id)
    if set(certified or ()) != expected_scenarios:
        raise PipelineProbeError(
            "pipeline executor did not certify the exact scenario boundary",
        )
    if PIPELINE_TOPOLOGY_SCENARIO_IDS.issubset(expected_scenarios):
        validate_history_topology_proof(result)
    else:
        validate_replay_idempotency_proof(result)
    return {**result, "configuration": resolution}


__all__ = [
    "PIPELINE_ACK_ENV",
    "PIPELINE_ACK_VALUE",
    "PIPELINE_DATABASE_ENV",
    "PIPELINE_ENV_NAMES",
    "PIPELINE_KAFKA_ENV",
    "PIPELINE_DATA_PLANE_SCENARIO_IDS",
    "PIPELINE_PROBE_SCHEMA_VERSION",
    "PIPELINE_REPLAY_SCHEMA_VERSION",
    "PIPELINE_S3_BUCKET_ENV",
    "PIPELINE_S3_ENDPOINT_ENV",
    "PIPELINE_SCENARIO_IDS",
    "PIPELINE_TOPOLOGY_SCHEMA_VERSION",
    "PIPELINE_TOPOLOGY_SCENARIO_IDS",
    "PipelineProbeConfig",
    "PipelineProbeError",
    "pipeline_scenario_ids_for_source",
    "run_pipeline_probe",
    "validate_history_topology_proof",
    "validate_replay_idempotency_proof",
]
