"""Promotion-grade offered-load orchestration for the isolated ingest pipeline.

The runner owns rate scheduling, topology, stability search, evidence
cross-checking, and a strict self-hashed artifact.  Provider/source-specific
setup remains behind :class:`PipelineBoundaryAdapter`: an adapter must drive
the real S3 raw evidence -> raw Kafka -> normalized Kafka -> Observation ->
same-tenant T1 boundary and report cumulative measurements from that boundary.

No in-memory or Provider-Lab-only adapter can produce release evidence.  A
release run requires:

* explicitly acknowledged loopback Postgres, Kafka, and S3 endpoints;
* exactly two tenants, two installations per tenant, and two Fyralis replicas;
* a real wall clock and the declared 2m/step/15m/60m durations;
* evidence-backed quota constraints in provider-safe mode; and
* exact, drained, duplicate-free measurements at every pipeline layer.

The injected clock/adapter seams exist for deterministic unit tests.  Any run
using them is marked ``diagnostic`` and is structurally ineligible for release.
This module deliberately does not substitute the batch-oriented
``BackfillHarness`` for an offered-load adapter: callers without a concrete
long-lived exact-pipeline adapter receive a fail-closed ``blocked`` artifact.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import math
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable
from urllib.parse import urlsplit

from services.ingest.source_certification.models import (
    CertificationInvariantError,
    ExecutableLoadOperation,
    LoadOperationContractAbsence,
    LoadQuotaMapping,
    LoadSuiteNonApplicability,
)
from services.ingest.source_certification.pipeline_probe import (
    PIPELINE_ACK_ENV,
    PIPELINE_ACK_VALUE,
    PIPELINE_DATABASE_ENV,
    PIPELINE_ENV_NAMES,
    PIPELINE_KAFKA_ENV,
    PIPELINE_S3_BUCKET_ENV,
    PIPELINE_S3_ENDPOINT_ENV,
)
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


PIPELINE_LOAD_ARTIFACT_SCHEMA_VERSION = (
    "fyralis.source-certification-pipeline-load.v2"
)
PipelineLoadMode = Literal["provider_safe", "fyralis_ceiling"]
PipelineLoadState = Literal[
    "passed",
    "diagnostic",
    "failed",
    "blocked",
    "not_applicable",
]
PipelineWorkloadKind = Literal["historical", "live", "combined"]
TrialPhase = Literal["warmup", "step", "binary_search", "validation", "soak"]
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_PIPELINE_LAYERS = (
    "s3_raw",
    "raw_kafka",
    "normalized_kafka",
    "observation",
    "t1",
)
_TRIAL_METRIC_FIELDS = frozenset(
    {
        "offered_items",
        "scheduled_data_operations",
        "scheduled_control_operations",
        "accepted_items",
        "expected_observations",
        "expected_raw_s3_objects",
        "expected_raw_kafka_records",
        "expected_normalized_records",
        "raw_s3_objects",
        "raw_kafka_records",
        "normalized_records",
        "observations",
        "t1_triggers",
        "unique_observation_identities",
        "unique_t1_observation_ids",
        "raw_bytes",
        "normalized_bytes",
        "provider_requests",
        "quota_units",
        "missing_records",
        "unexpected_duplicates",
        "cross_tenant_leaks",
        "cursor_checks",
        "cursor_required_operations",
        "cursor_proven_operations",
        "cursor_consistency_errors",
        "cooldown_violations",
        "failed_requests",
        "dlq_entries",
        "raw_kafka_lag",
        "normalized_kafka_lag",
        "observation_to_t1_lag",
        "peak_backlog",
        "backlog_growth_per_second",
        "offered_items_per_second",
        "data_operations_per_second",
        "raw_records_per_second",
        "normalized_records_per_second",
        "observations_per_second",
        "t1_triggers_per_second",
        "bytes_per_second",
        "quota_units_per_second",
        "scheduled_elapsed_seconds",
        "wall_elapsed_seconds",
        "scheduled_duration_ratio",
        "end_to_end_duration_ratio",
        "offered_rate_achievement_ratio",
        "p50_raw_latency_ms",
        "p95_raw_latency_ms",
        "p99_raw_latency_ms",
        "p50_normalized_latency_ms",
        "p95_normalized_latency_ms",
        "p99_normalized_latency_ms",
        "p50_observation_latency_ms",
        "p95_observation_latency_ms",
        "p99_observation_latency_ms",
        "p50_t1_latency_ms",
        "p95_t1_latency_ms",
        "p99_t1_latency_ms",
        "cpu_percent",
        "memory_bytes",
        "tenant_count",
        "installation_count",
        "replica_count",
        "participating_replica_count",
    }
)


class PipelineLoadError(RuntimeError):
    """The runner cannot produce trustworthy pipeline-load evidence."""


class PipelineLoadArtifactError(PipelineLoadError):
    """A serialized artifact is malformed, inconsistent, or changed."""


class _PipelineLoadSearchError(PipelineLoadError):
    def __init__(
        self,
        message: str,
        trials: Sequence[Mapping[str, object]],
    ) -> None:
        super().__init__(message)
        self.trials = [dict(trial) for trial in trials]


def _aware(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _finite_positive(value: float, field_name: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) <= 0
    ):
        raise ValueError(f"{field_name} must be a finite positive number")
    return float(value)


def _nonnegative_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_loopback(host: str | None) -> bool:
    if not host:
        return False
    if host.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _split_host_port(value: str) -> tuple[str, int]:
    parsed = urlsplit(f"//{value.strip()}")
    if not parsed.hostname or parsed.port is None:
        raise ValueError(
            "Kafka endpoints must be explicit loopback host:port values"
        )
    return parsed.hostname, parsed.port


@dataclass(frozen=True, slots=True)
class IsolatedPipelineInfrastructure:
    """Credential-sealed binding to dedicated local data-plane services."""

    database_url: str = field(repr=False)
    kafka_bootstrap_servers: str
    s3_endpoint_url: str
    s3_raw_bucket: str

    def __post_init__(self) -> None:
        database = urlsplit(self.database_url)
        if (
            database.scheme not in {"postgres", "postgresql"}
            or not _is_loopback(database.hostname)
            or not database.path.lstrip("/")
        ):
            raise ValueError(
                "database URL must name a database on a loopback Postgres host"
            )
        try:
            _ = database.port
        except ValueError as exc:
            raise ValueError("database URL port is invalid") from exc

        endpoints = tuple(
            _split_host_port(value)
            for value in self.kafka_bootstrap_servers.split(",")
        )
        if not endpoints or not all(_is_loopback(host) for host, _ in endpoints):
            raise ValueError("every Kafka bootstrap host must be loopback")

        s3 = urlsplit(self.s3_endpoint_url)
        if (
            s3.scheme not in {"http", "https"}
            or not _is_loopback(s3.hostname)
        ):
            raise ValueError("S3 endpoint must be HTTP(S) on a loopback host")
        try:
            _ = s3.port
        except ValueError as exc:
            raise ValueError("S3 endpoint port is invalid") from exc
        if _BUCKET_RE.fullmatch(self.s3_raw_bucket) is None:
            raise ValueError("S3 raw bucket name is invalid")

    @property
    def binding_sha256(self) -> str:
        database = urlsplit(self.database_url)
        identity = {
            "database": {
                "scheme": database.scheme,
                "host": database.hostname,
                "port": database.port or 5432,
                "name": database.path.lstrip("/"),
            },
            "kafka": self.kafka_bootstrap_servers,
            "s3_endpoint": self.s3_endpoint_url,
            "s3_bucket": self.s3_raw_bucket,
        }
        return _sha256(_canonical_bytes(identity))

    @property
    def descriptor(self) -> dict[str, object]:
        database = urlsplit(self.database_url)
        s3 = urlsplit(self.s3_endpoint_url)
        return {
            "binding_sha256": self.binding_sha256,
            "loopback_only": True,
            "credentials_recorded": False,
            "database": {
                "host": database.hostname,
                "port": database.port or 5432,
                "database": database.path.lstrip("/"),
            },
            "kafka_hosts": [
                host
                for host, _port in (
                    _split_host_port(item)
                    for item in self.kafka_bootstrap_servers.split(",")
                )
            ],
            "s3": {
                "scheme": s3.scheme,
                "host": s3.hostname,
                "port": s3.port,
                "bucket_sha256": _sha256(
                    self.s3_raw_bucket.encode("utf-8")
                ),
            },
        }


def resolve_isolated_pipeline_infrastructure(
    ambient_env: Mapping[str, str],
) -> tuple[IsolatedPipelineInfrastructure | None, str | None]:
    """Resolve the exact pipeline environment without serializing its values."""

    present = {name for name in PIPELINE_ENV_NAMES if ambient_env.get(name)}
    if not present:
        return None, "isolated_infrastructure_not_supplied"
    if present != PIPELINE_ENV_NAMES:
        return None, "isolated_infrastructure_incomplete"
    if ambient_env.get(PIPELINE_ACK_ENV) != PIPELINE_ACK_VALUE:
        return None, "isolated_infrastructure_ack_invalid"
    try:
        return (
            IsolatedPipelineInfrastructure(
                database_url=ambient_env[PIPELINE_DATABASE_ENV],
                kafka_bootstrap_servers=ambient_env[PIPELINE_KAFKA_ENV],
                s3_endpoint_url=ambient_env[PIPELINE_S3_ENDPOINT_ENV],
                s3_raw_bucket=ambient_env[PIPELINE_S3_BUCKET_ENV],
            ),
            None,
        )
    except (KeyError, ValueError):
        return None, "isolated_infrastructure_rejected"


@dataclass(frozen=True, slots=True)
class PipelineLoadTopology:
    tenants: int = 2
    installations_per_tenant: int = 2
    replicas: int = 2

    def __post_init__(self) -> None:
        for name in ("tenants", "installations_per_tenant", "replicas"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")

    @property
    def lane_count(self) -> int:
        return self.tenants * self.installations_per_tenant * self.replicas

    @property
    def installation_count(self) -> int:
        return self.tenants * self.installations_per_tenant

    @property
    def release_exact(self) -> bool:
        return (
            self.tenants,
            self.installations_per_tenant,
            self.replicas,
        ) == (2, 2, 2)

    def to_dict(self) -> dict[str, int]:
        return {
            "tenants": self.tenants,
            "installations_per_tenant": self.installations_per_tenant,
            "replicas": self.replicas,
        }


@dataclass(frozen=True, slots=True)
class PipelineLoadTiming:
    warmup_seconds: float = 120.0
    step_seconds: float = 120.0
    validation_seconds: float = 900.0
    soak_seconds: float = 3_600.0

    def __post_init__(self) -> None:
        for name in (
            "warmup_seconds",
            "step_seconds",
            "validation_seconds",
            "soak_seconds",
        ):
            _finite_positive(getattr(self, name), name)

    @property
    def release_minimums_met(self) -> bool:
        return (
            self.warmup_seconds >= 120
            and self.step_seconds >= 120
            and self.validation_seconds >= 900
            and self.soak_seconds >= 3_600
        )

    def to_dict(self) -> dict[str, float]:
        return {
            "warmup_seconds": self.warmup_seconds,
            "step_seconds": self.step_seconds,
            "validation_seconds": self.validation_seconds,
            "soak_seconds": self.soak_seconds,
        }


@dataclass(frozen=True, slots=True)
class DeclaredPipelineWorkload:
    """One catalog-declared executable operation contract."""

    kind: PipelineWorkloadKind
    executable_operations: tuple[ExecutableLoadOperation, ...]
    contract_absence_assertions: tuple[LoadOperationContractAbsence, ...] = ()
    non_applicability: LoadSuiteNonApplicability | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"historical", "live", "combined"}:
            raise ValueError("pipeline workload kind is invalid")
        if (
            not isinstance(self.executable_operations, tuple)
            or not all(
                isinstance(operation, ExecutableLoadOperation)
                for operation in self.executable_operations
            )
        ):
            raise ValueError(
                "pipeline workload executable_operations must be a tuple"
            )
        if self.non_applicability is not None and not isinstance(
            self.non_applicability,
            LoadSuiteNonApplicability,
        ):
            raise ValueError("pipeline workload non_applicability is invalid")
        if bool(self.executable_operations) == (
            self.non_applicability is not None
        ):
            raise ValueError(
                "pipeline workload must be executable or not applicable"
            )
        operation_ids = self.operation_ids
        if len(operation_ids) != len(set(operation_ids)):
            raise ValueError(
                "pipeline workload executable operation IDs must be unique"
            )
        if not isinstance(self.contract_absence_assertions, tuple) or not all(
            isinstance(assertion, LoadOperationContractAbsence)
            for assertion in self.contract_absence_assertions
        ):
            raise ValueError(
                "pipeline workload contract absences are invalid"
            )
        absence_ids = tuple(
            assertion.operation_id
            for assertion in self.contract_absence_assertions
        )
        if (
            len(absence_ids) != len(set(absence_ids))
            or set(absence_ids).intersection(operation_ids)
        ):
            raise ValueError(
                "pipeline workload contract absence IDs are inconsistent"
            )

    @property
    def operation_ids(self) -> tuple[str, ...]:
        return tuple(
            operation.operation_id
            for operation in self.executable_operations
        )

    def operation(self, operation_id: str) -> ExecutableLoadOperation:
        for operation in self.executable_operations:
            if operation.operation_id == operation_id:
                return operation
        raise KeyError(operation_id)

    @property
    def declaration_sha256(self) -> str:
        return _sha256(
            _canonical_bytes(
                {
                    "kind": self.kind,
                    "executable_operations": [
                        operation.to_dict()
                        for operation in self.executable_operations
                    ],
                    "contract_absence_assertions": [
                        assertion.to_dict()
                        for assertion in self.contract_absence_assertions
                    ],
                    "non_applicability": (
                        self.non_applicability.to_dict()
                        if self.non_applicability is not None
                        else None
                    ),
                }
            )
        )

    def validate_source(self, source_id: str) -> None:
        if any(
            not operation_id.startswith(f"{source_id}.")
            for operation_id in (
                *self.operation_ids,
                *(
                    assertion.operation_id
                    for assertion in self.contract_absence_assertions
                ),
            )
        ):
            raise PipelineLoadError(
                "workload contract contains a foreign source operation"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "executable_operations": [
                operation.to_dict()
                for operation in self.executable_operations
            ],
            "contract_absence_assertions": [
                assertion.to_dict()
                for assertion in self.contract_absence_assertions
            ],
            "non_applicability": (
                self.non_applicability.to_dict()
                if self.non_applicability is not None
                else None
            ),
            "declaration_sha256": self.declaration_sha256,
        }


@dataclass(frozen=True, slots=True)
class QuotaConstraint:
    """One exact evidence-backed limiting quota for a workload item."""

    limit_id: str
    scope: str
    units_per_item: float
    steady_units: float
    steady_window_seconds: float
    burst_units: float
    burst_window_seconds: float
    evidence_uri: str
    verified_at: datetime

    def __post_init__(self) -> None:
        for name in ("limit_id", "scope"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be non-empty")
        for name in (
            "units_per_item",
            "steady_units",
            "steady_window_seconds",
            "burst_units",
            "burst_window_seconds",
        ):
            _finite_positive(getattr(self, name), name)
        parsed = urlsplit(self.evidence_uri)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("quota evidence_uri must be an absolute HTTPS URL")
        _aware(self.verified_at, "verified_at")

    @property
    def modeled_rate(self) -> float:
        return min(
            self.steady_units
            / self.steady_window_seconds
            / self.units_per_item,
            self.burst_units
            / self.burst_window_seconds
            / self.units_per_item,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "limit_id": self.limit_id,
            "scope": self.scope,
            "units_per_item": self.units_per_item,
            "steady_units": self.steady_units,
            "steady_window_seconds": self.steady_window_seconds,
            "burst_units": self.burst_units,
            "burst_window_seconds": self.burst_window_seconds,
            "evidence_uri": self.evidence_uri,
            "verified_at": self.verified_at.astimezone(timezone.utc).isoformat(),
            "modeled_rate": self.modeled_rate,
        }


@dataclass(frozen=True, slots=True)
class VerifiedQuotaConfiguration:
    source_id: str
    constraints: tuple[QuotaConstraint, ...]

    def __post_init__(self) -> None:
        if self.source_id not in CANONICAL_SOURCE_IDS:
            raise ValueError("quota source_id must be canonical")
        if not self.constraints:
            raise ValueError("provider-safe quota constraints must not be empty")
        identities = tuple(
            (item.limit_id, item.scope) for item in self.constraints
        )
        if len(identities) != len(set(identities)):
            raise ValueError("quota constraints must have unique limit/scope")

    @property
    def modeled_maximum_rate(self) -> float:
        return min(item.modeled_rate for item in self.constraints)

    @property
    def declaration_sha256(self) -> str:
        return _sha256(
            _canonical_bytes(
                {
                    "source_id": self.source_id,
                    "constraints": [
                        item.to_dict() for item in self.constraints
                    ],
                }
            )
        )

    def validate_freshness(
        self,
        *,
        source_id: str,
        now: datetime,
        maximum_age: timedelta,
    ) -> None:
        current = _aware(now, "now")
        if self.source_id != source_id:
            raise PipelineLoadError("quota configuration source differs")
        for item in self.constraints:
            verified = item.verified_at.astimezone(timezone.utc)
            if verified > current + timedelta(minutes=5):
                raise PipelineLoadError("quota verification timestamp is future")
            if current - verified > maximum_age:
                raise PipelineLoadError("quota verification is stale")

    def to_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "declaration_sha256": self.declaration_sha256,
            "modeled_maximum_rate": self.modeled_maximum_rate,
            "constraints": [item.to_dict() for item in self.constraints],
        }


@dataclass(frozen=True, slots=True)
class PipelineLoadRunConfig:
    topology: PipelineLoadTopology = field(default_factory=PipelineLoadTopology)
    timing: PipelineLoadTiming = field(default_factory=PipelineLoadTiming)
    initial_rate: float = 1.0
    maximum_offered_rate: float = 10_000.0
    step_fraction: float = 0.25
    search_tolerance_fraction: float = 0.05
    maximum_step_trials: int = 40
    maximum_binary_trials: int = 20
    maximum_in_flight: int = 256
    maximum_p99_observation_latency_ms: float = 30_000.0
    include_soak: bool = True
    release: bool = True
    quota_maximum_age: timedelta = timedelta(days=30)

    def __post_init__(self) -> None:
        _finite_positive(self.initial_rate, "initial_rate")
        _finite_positive(self.maximum_offered_rate, "maximum_offered_rate")
        if self.maximum_offered_rate < self.initial_rate:
            raise ValueError("maximum_offered_rate cannot be below initial_rate")
        if not 0 < self.step_fraction <= 1:
            raise ValueError("step_fraction must be in (0, 1]")
        if not 0 < self.search_tolerance_fraction <= 0.05:
            raise ValueError(
                "search_tolerance_fraction must be in (0, 0.05]"
            )
        for name in (
            "maximum_step_trials",
            "maximum_binary_trials",
            "maximum_in_flight",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        _finite_positive(
            self.maximum_p99_observation_latency_ms,
            "maximum_p99_observation_latency_ms",
        )
        if self.quota_maximum_age <= timedelta(0):
            raise ValueError("quota_maximum_age must be positive")
        if self.release and (
            not self.topology.release_exact
            or not self.timing.release_minimums_met
            or not self.include_soak
        ):
            raise ValueError(
                "release runs require 2x2x2 topology, 2m/2m/15m/60m "
                "timings, and the soak"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "topology": self.topology.to_dict(),
            "timing": self.timing.to_dict(),
            "initial_rate": self.initial_rate,
            "maximum_offered_rate": self.maximum_offered_rate,
            "step_fraction": self.step_fraction,
            "search_tolerance_fraction": self.search_tolerance_fraction,
            "maximum_step_trials": self.maximum_step_trials,
            "maximum_binary_trials": self.maximum_binary_trials,
            "maximum_in_flight": self.maximum_in_flight,
            "maximum_p99_observation_latency_ms": (
                self.maximum_p99_observation_latency_ms
            ),
            "include_soak": self.include_soak,
            "release": self.release,
            "quota_maximum_age_seconds": (
                self.quota_maximum_age.total_seconds()
            ),
        }


@dataclass(frozen=True, slots=True)
class PipelineBoundaryProof:
    """Exact relations and deployment topology exercised by an adapter."""

    evidence_class: Literal["exact_pipeline", "test_double"]
    source_id: str
    binding_sha256: str
    dedicated_namespace: str
    workload_kind: PipelineWorkloadKind
    operation_contract_sha256: str
    raw_topic: str
    normalized_topic: str
    observation_relation: str
    t1_relation: str
    quota_mode: Literal["strict", "disabled"]
    topology: PipelineLoadTopology
    loopback_only: bool = True
    s3_raw_evidence_verified: bool = True

    def __post_init__(self) -> None:
        if self.evidence_class not in {"exact_pipeline", "test_double"}:
            raise ValueError("boundary evidence_class is invalid")
        if self.source_id not in CANONICAL_SOURCE_IDS:
            raise ValueError("boundary source_id must be canonical")
        if _SHA256_RE.fullmatch(self.binding_sha256) is None:
            raise ValueError("boundary binding_sha256 is invalid")
        if (
            self.workload_kind not in {"historical", "live", "combined"}
            or _SHA256_RE.fullmatch(self.operation_contract_sha256) is None
        ):
            raise ValueError("boundary workload identity is invalid")
        if (
            not self.dedicated_namespace.strip()
            or self.observation_relation != "observations"
            or self.t1_relation != "think_trigger_queue"
        ):
            raise ValueError("boundary relations/namespace are invalid")
        if not self.loopback_only or not self.s3_raw_evidence_verified:
            raise ValueError("boundary must prove loopback S3 raw evidence")

    def to_dict(self) -> dict[str, object]:
        return {
            "evidence_class": self.evidence_class,
            "source_id": self.source_id,
            "binding_sha256": self.binding_sha256,
            "dedicated_namespace": self.dedicated_namespace,
            "workload_kind": self.workload_kind,
            "operation_contract_sha256": self.operation_contract_sha256,
            "raw_topic": self.raw_topic,
            "normalized_topic": self.normalized_topic,
            "observation_relation": self.observation_relation,
            "t1_relation": self.t1_relation,
            "quota_mode": self.quota_mode,
            "topology": self.topology.to_dict(),
            "loopback_only": self.loopback_only,
            "s3_raw_evidence_verified": self.s3_raw_evidence_verified,
        }


@dataclass(frozen=True, slots=True)
class LatencySummary:
    count: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    maximum_ms: float

    def __post_init__(self) -> None:
        _nonnegative_int(self.count, "latency count")
        values = (
            self.p50_ms,
            self.p95_ms,
            self.p99_ms,
            self.maximum_ms,
        )
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or float(value) < 0
            for value in values
        ):
            raise ValueError("latency values must be finite and non-negative")
        if not (
            self.p50_ms
            <= self.p95_ms
            <= self.p99_ms
            <= self.maximum_ms
        ):
            raise ValueError("latency percentiles must be monotonic")


@dataclass(frozen=True, slots=True)
class PipelineSnapshot:
    """Cumulative measurements read from the exact isolated boundary."""

    offered_items: int
    accepted_items: int
    expected_observations: int
    raw_s3_objects: int
    raw_kafka_records: int
    normalized_records: int
    observations: int
    t1_triggers: int
    unique_observation_identities: int
    unique_t1_observation_ids: int
    raw_bytes: int
    normalized_bytes: int
    provider_requests: int
    quota_units: float
    unexpected_duplicates: int
    cross_tenant_leaks: int
    cursor_checks: int
    cursor_consistency_errors: int
    cooldown_violations: int
    failed_requests: int
    dlq_entries: int
    raw_kafka_lag: int
    normalized_kafka_lag: int
    observation_to_t1_lag: int
    peak_backlog: int
    raw_latency: LatencySummary
    normalized_latency: LatencySummary
    observation_latency: LatencySummary
    t1_latency: LatencySummary
    tenant_ids: tuple[str, ...]
    installation_ids: tuple[str, ...]
    replica_ids: tuple[str, ...]
    replica_processed_items: tuple[tuple[str, int], ...]
    event_ledger_sha256: str
    receipt_ledger_sha256: str
    cursor_ledger_sha256: str
    cpu_percent: float = 0.0
    memory_bytes: int = 0

    def __post_init__(self) -> None:
        integer_fields = (
            "offered_items",
            "accepted_items",
            "expected_observations",
            "raw_s3_objects",
            "raw_kafka_records",
            "normalized_records",
            "observations",
            "t1_triggers",
            "unique_observation_identities",
            "unique_t1_observation_ids",
            "raw_bytes",
            "normalized_bytes",
            "provider_requests",
            "unexpected_duplicates",
            "cross_tenant_leaks",
            "cursor_checks",
            "cursor_consistency_errors",
            "cooldown_violations",
            "failed_requests",
            "dlq_entries",
            "raw_kafka_lag",
            "normalized_kafka_lag",
            "observation_to_t1_lag",
            "peak_backlog",
            "memory_bytes",
        )
        for name in integer_fields:
            _nonnegative_int(getattr(self, name), name)
        if (
            isinstance(self.quota_units, bool)
            or not isinstance(self.quota_units, (int, float))
            or not math.isfinite(float(self.quota_units))
            or self.quota_units < 0
        ):
            raise ValueError("quota_units must be finite and non-negative")
        if (
            isinstance(self.cpu_percent, bool)
            or not isinstance(self.cpu_percent, (int, float))
            or not math.isfinite(float(self.cpu_percent))
            or self.cpu_percent < 0
        ):
            raise ValueError("cpu_percent must be finite and non-negative")
        for name in ("tenant_ids", "installation_ids", "replica_ids"):
            values = getattr(self, name)
            if len(values) != len(set(values)) or any(
                not isinstance(value, str) or not value for value in values
            ):
                raise ValueError(f"{name} must contain unique non-empty IDs")
        processed_ids = tuple(
            replica_id for replica_id, _count in self.replica_processed_items
        )
        if (
            processed_ids != self.replica_ids
            or any(
                not isinstance(count, int)
                or isinstance(count, bool)
                or count < 0
                for _replica_id, count in self.replica_processed_items
            )
        ):
            raise ValueError(
                "replica_processed_items must cover replica_ids exactly"
            )
        for name in (
            "event_ledger_sha256",
            "receipt_ledger_sha256",
            "cursor_ledger_sha256",
        ):
            if _SHA256_RE.fullmatch(getattr(self, name)) is None:
                raise ValueError(f"{name} must be a lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class TrialContext:
    trial_index: int
    phase: TrialPhase
    target_rate: float
    duration_seconds: float
    mode: PipelineLoadMode
    workload_kind: PipelineWorkloadKind


@dataclass(frozen=True, slots=True)
class WorkItem:
    sequence: int
    event_id: str
    operation_id: str
    operation_contract_sha256: str
    evidence_id: str
    tenant_slot: int
    installation_slot: int
    replica_slot: int
    scheduled_monotonic: float


@dataclass(frozen=True, slots=True)
class ReceiptQuotaCharge:
    operation_id: str
    quota_bucket: str | None
    request_count: int
    units: float

    def __post_init__(self) -> None:
        if not self.operation_id:
            raise ValueError("receipt quota operation_id must be non-empty")
        if self.quota_bucket is not None and not self.quota_bucket:
            raise ValueError("receipt quota_bucket must be non-empty or null")
        _nonnegative_int(self.request_count, "receipt quota request_count")
        if self.request_count < 1:
            raise ValueError("receipt quota request_count must be positive")
        if (
            isinstance(self.units, bool)
            or not isinstance(self.units, (int, float))
            or not math.isfinite(float(self.units))
            or self.units <= 0
        ):
            raise ValueError(
                "receipt quota units must be finite and positive"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "quota_bucket": self.quota_bucket,
            "request_count": self.request_count,
            "units": float(self.units),
        }


@dataclass(frozen=True, slots=True)
class OfferReceipt:
    sequence: int
    event_id: str
    operation_id: str
    operation_contract_sha256: str
    evidence_id: str
    execution_evidence_sha256: str
    accepted: bool
    expected_observations: int
    raw_s3_objects: int
    raw_kafka_records: int
    normalized_records: int
    raw_bytes: int
    provider_requests: int
    quota_units: float
    cursor_checks: int
    cursor_consistency_errors: int
    proofs: tuple[str, ...]
    quota_charges: tuple[ReceiptQuotaCharge, ...] = ()

    def __post_init__(self) -> None:
        if self.sequence < 1 or not self.event_id or not self.operation_id:
            raise ValueError("offer receipt identity is invalid")
        if (
            _SHA256_RE.fullmatch(self.operation_contract_sha256) is None
            or _SHA256_RE.fullmatch(self.execution_evidence_sha256) is None
            or not self.evidence_id
        ):
            raise ValueError("offer receipt contract/evidence identity is invalid")
        if not isinstance(self.accepted, bool):
            raise ValueError("offer receipt accepted must be a boolean")
        if (
            isinstance(self.expected_observations, bool)
            or not isinstance(self.expected_observations, int)
            or self.expected_observations < 0
        ):
            raise ValueError("expected_observations must be non-negative")
        for name in (
            "raw_s3_objects",
            "raw_kafka_records",
            "normalized_records",
            "raw_bytes",
            "provider_requests",
            "cursor_checks",
            "cursor_consistency_errors",
        ):
            _nonnegative_int(getattr(self, name), name)
        if (
            isinstance(self.quota_units, bool)
            or not isinstance(self.quota_units, (int, float))
            or self.quota_units < 0
            or not math.isfinite(self.quota_units)
        ):
            raise ValueError("receipt quota_units must be finite/non-negative")
        if (
            not isinstance(self.proofs, tuple)
            or len(self.proofs) != len(set(self.proofs))
            or any(not isinstance(proof, str) or not proof for proof in self.proofs)
        ):
            raise ValueError("offer receipt proofs must be unique non-empty IDs")
        if not isinstance(self.quota_charges, tuple) or not all(
            isinstance(charge, ReceiptQuotaCharge)
            for charge in self.quota_charges
        ):
            raise ValueError(
                "offer receipt quota_charges must be ReceiptQuotaCharge values"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "event_id": self.event_id,
            "operation_id": self.operation_id,
            "operation_contract_sha256": self.operation_contract_sha256,
            "evidence_id": self.evidence_id,
            "execution_evidence_sha256": self.execution_evidence_sha256,
            "accepted": self.accepted,
            "expected_observations": self.expected_observations,
            "raw_s3_objects": self.raw_s3_objects,
            "raw_kafka_records": self.raw_kafka_records,
            "normalized_records": self.normalized_records,
            "raw_bytes": self.raw_bytes,
            "provider_requests": self.provider_requests,
            "quota_units": float(self.quota_units),
            "cursor_checks": self.cursor_checks,
            "cursor_consistency_errors": self.cursor_consistency_errors,
            "proofs": list(self.proofs),
            "quota_charges": [
                charge.to_dict() for charge in self.quota_charges
            ],
        }


@runtime_checkable
class PipelineBoundaryAdapter(Protocol):
    """Source-specific bridge to the real isolated pipeline."""

    @property
    def boundary(self) -> PipelineBoundaryProof:
        ...

    async def begin_trial(self, context: TrialContext) -> PipelineSnapshot:
        """Reset only the dedicated namespace and return its zero baseline."""

    async def offer(self, item: WorkItem) -> OfferReceipt:
        """Drive one unique provider-shaped item through the real ingress."""

    async def finish_trial(self) -> PipelineSnapshot:
        """Drain all three backlogs and return a terminal cumulative snapshot."""

    async def close(self) -> None:
        """Stop workers and release source-scoped resources."""


PipelineAdapterFactory = Callable[
    [
        IsolatedPipelineInfrastructure,
        str,
        PipelineLoadMode,
        DeclaredPipelineWorkload,
        PipelineLoadTopology,
        VerifiedQuotaConfiguration | None,
    ],
    Awaitable[PipelineBoundaryAdapter] | PipelineBoundaryAdapter,
]


@runtime_checkable
class PipelineLoadClock(Protocol):
    @property
    def release_wall_clock(self) -> bool:
        ...

    def monotonic(self) -> float:
        ...

    def now(self) -> datetime:
        ...

    async def sleep(self, seconds: float) -> None:
        ...


class SystemPipelineLoadClock:
    """The only clock eligible to create release evidence."""

    @property
    def release_wall_clock(self) -> bool:
        return True

    def monotonic(self) -> float:
        return time.monotonic()

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))


def _zero_snapshot(snapshot: PipelineSnapshot) -> bool:
    counters = (
        snapshot.offered_items,
        snapshot.accepted_items,
        snapshot.expected_observations,
        snapshot.raw_s3_objects,
        snapshot.raw_kafka_records,
        snapshot.normalized_records,
        snapshot.observations,
        snapshot.t1_triggers,
        snapshot.unique_observation_identities,
        snapshot.unique_t1_observation_ids,
        snapshot.raw_bytes,
        snapshot.normalized_bytes,
        snapshot.provider_requests,
        snapshot.quota_units,
        snapshot.unexpected_duplicates,
        snapshot.cross_tenant_leaks,
        snapshot.cursor_checks,
        snapshot.cursor_consistency_errors,
        snapshot.cooldown_violations,
        snapshot.failed_requests,
        snapshot.dlq_entries,
        snapshot.raw_kafka_lag,
        snapshot.normalized_kafka_lag,
        snapshot.observation_to_t1_lag,
        snapshot.peak_backlog,
        snapshot.raw_latency.count,
        snapshot.normalized_latency.count,
        snapshot.observation_latency.count,
        snapshot.t1_latency.count,
    )
    return (
        all(value == 0 for value in counters)
        and not snapshot.tenant_ids
        and not snapshot.installation_ids
        and not snapshot.replica_ids
        and not snapshot.replica_processed_items
    )


def _validate_boundary(
    boundary: PipelineBoundaryProof,
    *,
    source_id: str,
    mode: PipelineLoadMode,
    workload: DeclaredPipelineWorkload,
    topology: PipelineLoadTopology,
    infrastructure: IsolatedPipelineInfrastructure,
) -> None:
    if (
        boundary.source_id != source_id
        or boundary.binding_sha256 != infrastructure.binding_sha256
        or boundary.workload_kind != workload.kind
        or boundary.operation_contract_sha256 != workload.declaration_sha256
        or boundary.topology != topology
        or boundary.raw_topic != f"ingestion.raw.{source_id}"
        or boundary.normalized_topic != f"ingestion.normalized.{source_id}"
    ):
        raise PipelineLoadError(
            "adapter boundary differs from source/infrastructure/topology"
        )
    expected_quota_mode = "strict" if mode == "provider_safe" else "disabled"
    if boundary.quota_mode != expected_quota_mode:
        raise PipelineLoadError("adapter quota mode differs from run mode")


def _lane(
    sequence: int,
    topology: PipelineLoadTopology,
) -> tuple[int, int, int]:
    index = (sequence - 1) % topology.lane_count
    replica = index % topology.replicas
    installation_index = index // topology.replicas
    installation = installation_index % topology.installations_per_tenant
    tenant = installation_index // topology.installations_per_tenant
    return tenant, installation, replica


async def _collect_pending(
    pending: set[asyncio.Task[OfferReceipt]],
    receipts: list[OfferReceipt],
    *,
    all_tasks: bool,
) -> None:
    if not pending:
        return
    done, remaining = await asyncio.wait(
        pending,
        return_when=(
            asyncio.ALL_COMPLETED if all_tasks else asyncio.FIRST_COMPLETED
        ),
    )
    pending.clear()
    pending.update(remaining)
    receipts.extend(task.result() for task in done)


def _cardinality_allows(cardinality: str, count: int) -> bool:
    if cardinality == "none":
        return count == 0
    if cardinality == "exactly_one":
        return count == 1
    if cardinality == "zero_or_more":
        return count >= 0
    return count >= 1


def _validate_offer_receipt(
    receipt: OfferReceipt,
    *,
    item: WorkItem,
    operation: ExecutableLoadOperation,
) -> None:
    if (
        receipt.sequence != item.sequence
        or receipt.event_id != item.event_id
        or receipt.operation_id != operation.operation_id
        or receipt.operation_contract_sha256
        != operation.declaration_sha256
        or receipt.operation_contract_sha256
        != item.operation_contract_sha256
        or receipt.evidence_id != operation.evidence_id
        or receipt.evidence_id != item.evidence_id
    ):
        raise PipelineLoadError(
            "offer receipt differs from its scheduled operation contract"
        )
    if "binding_invocation" not in receipt.proofs:
        raise PipelineLoadError(
            "offer receipt lacks binding invocation proof"
        )
    if receipt.raw_s3_objects != receipt.raw_kafka_records:
        raise PipelineLoadError(
            "offer receipt raw S3/Kafka cardinalities differ"
        )
    if receipt.accepted:
        if not set(operation.receipt_proof_requirements).issubset(
            receipt.proofs
        ):
            raise PipelineLoadError(
                "accepted offer receipt lacks required operation proofs"
            )
        if not _cardinality_allows(
            operation.raw_cardinality,
            receipt.raw_s3_objects,
        ):
            raise PipelineLoadError(
                "offer receipt violates raw cardinality contract"
            )
        if not _cardinality_allows(
            operation.observation_cardinality,
            receipt.expected_observations,
        ):
            raise PipelineLoadError(
                "offer receipt violates observation cardinality contract"
            )
        if not _cardinality_allows(
            operation.normalized_cardinality,
            receipt.normalized_records,
        ):
            raise PipelineLoadError(
                "offer receipt violates normalized cardinality contract"
            )
    elif any(
        (
            receipt.expected_observations,
            receipt.raw_s3_objects,
            receipt.raw_kafka_records,
            receipt.normalized_records,
            receipt.raw_bytes,
            receipt.cursor_checks,
        )
    ):
        raise PipelineLoadError(
            "rejected offer receipt claims accepted pipeline output"
        )
    if operation.kind == "control" and any(
        (
            receipt.expected_observations,
            receipt.raw_s3_objects,
            receipt.raw_kafka_records,
            receipt.normalized_records,
            receipt.raw_bytes,
        )
    ):
        raise PipelineLoadError(
            "control operation receipt claims data-plane output"
        )
    if (
        operation.cursor_applicability == "required"
        and receipt.accepted
        and receipt.cursor_checks < 1
    ):
        raise PipelineLoadError(
            "cursor-required operation receipt lacks a cursor check"
        )
    if (
        operation.cursor_applicability == "not_applicable"
        and (
            receipt.cursor_checks != 0
            or "cursor_consistency" in receipt.proofs
        )
    ):
        raise PipelineLoadError(
            "cursor-inapplicable operation receipt claims cursor proof"
        )
    if (
        operation.cursor_applicability == "optional"
        and receipt.cursor_checks > 0
        and "cursor_consistency" not in receipt.proofs
    ):
        raise PipelineLoadError(
            "optional cursor checks require cursor consistency proof"
        )

    allowed_charges = {
        (mapping.operation_id, mapping.quota_bucket): mapping
        for mapping in operation.quota_mappings
    }
    charge_ids = tuple(
        (charge.operation_id, charge.quota_bucket)
        for charge in receipt.quota_charges
    )
    if len(charge_ids) != len(set(charge_ids)):
        raise PipelineLoadError(
            "offer receipt contains duplicate quota charge identities"
        )
    if any(identity not in allowed_charges for identity in charge_ids):
        raise PipelineLoadError(
            "offer receipt quota charge is outside the operation allowlist"
        )
    if sum(
        charge.request_count for charge in receipt.quota_charges
    ) != receipt.provider_requests:
        raise PipelineLoadError(
            "offer receipt provider request count differs from quota charges"
        )
    for charge in receipt.quota_charges:
        mapping = allowed_charges[
            (charge.operation_id, charge.quota_bucket)
        ]
        if not math.isclose(
            charge.units,
            charge.request_count * mapping.units_per_request,
            rel_tol=1e-9,
            abs_tol=1e-9,
        ):
            raise PipelineLoadError(
                "offer receipt quota units differ from the declared mapping"
            )
    if not math.isclose(
        receipt.quota_units,
        sum(charge.units for charge in receipt.quota_charges),
        rel_tol=1e-9,
        abs_tol=1e-9,
    ):
        raise PipelineLoadError(
            "offer receipt quota units differ from quota charges"
        )


def _target_data_operations(
    workload: DeclaredPipelineWorkload,
    *,
    target_rate: float,
    duration_seconds: float,
) -> int:
    if not any(
        operation.execution_frequency == "per_item"
        for operation in workload.executable_operations
    ):
        return 0
    return max(1, math.floor(target_rate * duration_seconds))


def _scheduled_operation_counts(
    workload: DeclaredPipelineWorkload,
    *,
    target_rate: float,
    duration_seconds: float,
) -> tuple[dict[str, int], int, int]:
    counts = {operation_id: 0 for operation_id in workload.operation_ids}
    per_item = tuple(
        operation
        for operation in workload.executable_operations
        if operation.execution_frequency == "per_item"
    )
    target_data = _target_data_operations(
        workload,
        target_rate=target_rate,
        duration_seconds=duration_seconds,
    )
    weighted = tuple(
        operation
        for operation in per_item
        for _ in range(operation.selection_weight or 0)
    )
    if target_data and not weighted:
        raise PipelineLoadError(
            "workload has no weighted per-item operation selection"
        )
    full_cycles, remainder = (
        divmod(target_data, len(weighted)) if weighted else (0, 0)
    )
    for operation in weighted:
        counts[operation.operation_id] += full_cycles
    for operation in weighted[:remainder]:
        counts[operation.operation_id] += 1

    control_count = 0
    for operation in workload.executable_operations:
        if operation.execution_frequency == "once_per_trial":
            counts[operation.operation_id] = 1
            control_count += 1
        elif operation.execution_frequency == "periodic":
            assert operation.cadence_seconds is not None
            executions = math.ceil(
                duration_seconds / operation.cadence_seconds
            )
            counts[operation.operation_id] = executions
            control_count += executions
    return counts, target_data, control_count


def _scheduled_operations(
    workload: DeclaredPipelineWorkload,
    *,
    target_rate: float,
    duration_seconds: float,
):  # noqa: ANN202
    """Yield (offset, operation, data ordinal) without materializing the load."""

    per_item = tuple(
        operation
        for operation in workload.executable_operations
        if operation.execution_frequency == "per_item"
    )
    weighted = tuple(
        operation
        for operation in per_item
        for _ in range(operation.selection_weight or 0)
    )
    target_data = _target_data_operations(
        workload,
        target_rate=target_rate,
        duration_seconds=duration_seconds,
    )
    controls: list[tuple[float, int, ExecutableLoadOperation]] = []
    for declaration_index, operation in enumerate(
        workload.executable_operations
    ):
        if operation.execution_frequency == "once_per_trial":
            controls.append(
                (
                    (
                        0.0
                        if operation.trial_position == "before_load"
                        else duration_seconds
                    ),
                    declaration_index,
                    operation,
                )
            )
        elif operation.execution_frequency == "periodic":
            assert operation.cadence_seconds is not None
            for execution_index in range(
                math.ceil(duration_seconds / operation.cadence_seconds)
            ):
                controls.append(
                    (
                        execution_index * operation.cadence_seconds,
                        declaration_index,
                        operation,
                    )
                )
    controls.sort(key=lambda item: (item[0], item[1]))
    control_index = 0
    for data_index in range(target_data):
        offset = data_index / target_rate
        while (
            control_index < len(controls)
            and controls[control_index][0] <= offset
        ):
            control_offset, _index, operation = controls[control_index]
            yield control_offset, operation, None
            control_index += 1
        yield offset, weighted[data_index % len(weighted)], data_index + 1
    while control_index < len(controls):
        control_offset, _index, operation = controls[control_index]
        yield control_offset, operation, None
        control_index += 1


def _receipt_ledger_sha256(receipts: Sequence[OfferReceipt]) -> str:
    return _sha256(
        _canonical_bytes([receipt.to_dict() for receipt in receipts])
    )


def _metrics(
    *,
    snapshot: PipelineSnapshot,
    receipts: Sequence[OfferReceipt],
    workload: DeclaredPipelineWorkload,
    duration_seconds: float,
    target_rate: float,
    scheduled_elapsed_seconds: float,
    wall_elapsed_seconds: float,
    topology: PipelineLoadTopology,
) -> tuple[dict[str, int | float], tuple[str, ...]]:
    offered = len(receipts)
    operations = {
        operation.operation_id: operation
        for operation in workload.executable_operations
    }
    scheduled_data = sum(
        operations[receipt.operation_id].kind == "data"
        for receipt in receipts
    )
    scheduled_control = offered - scheduled_data
    accepted_receipts = [receipt for receipt in receipts if receipt.accepted]
    accepted = len(accepted_receipts)
    expected = sum(
        receipt.expected_observations for receipt in accepted_receipts
    )
    receipt_raw_bytes = sum(receipt.raw_bytes for receipt in accepted_receipts)
    expected_raw_s3 = sum(
        receipt.raw_s3_objects for receipt in accepted_receipts
    )
    expected_raw_kafka = sum(
        receipt.raw_kafka_records for receipt in accepted_receipts
    )
    expected_normalized = sum(
        receipt.normalized_records for receipt in accepted_receipts
    )
    receipt_cursor_checks = sum(
        receipt.cursor_checks for receipt in receipts
    )
    receipt_cursor_errors = sum(
        receipt.cursor_consistency_errors for receipt in receipts
    )
    cursor_required = sum(
        receipt.accepted
        and operations[receipt.operation_id].cursor_applicability
        == "required"
        for receipt in receipts
    )
    cursor_proven = sum(
        receipt.accepted
        and operations[receipt.operation_id].cursor_applicability
        == "required"
        and receipt.cursor_checks > 0
        for receipt in receipts
    )
    receipt_provider_requests = sum(
        receipt.provider_requests for receipt in receipts
    )
    receipt_quota_units = sum(receipt.quota_units for receipt in receipts)
    missing = sum(
        (
            max(0, expected_raw_s3 - snapshot.raw_s3_objects),
            max(0, expected_raw_kafka - snapshot.raw_kafka_records),
            max(0, expected_normalized - snapshot.normalized_records),
            max(0, expected - snapshot.observations),
            max(0, expected - snapshot.t1_triggers),
        )
    )
    duplicates = snapshot.unexpected_duplicates + max(
        0,
        snapshot.observations - snapshot.unique_observation_identities,
    )
    failures: list[str] = []
    exact_pairs = (
        ("offered_items", snapshot.offered_items, offered),
        ("accepted_items", snapshot.accepted_items, accepted),
        ("expected_observations", snapshot.expected_observations, expected),
        ("raw_s3_objects", snapshot.raw_s3_objects, expected_raw_s3),
        (
            "raw_kafka_records",
            snapshot.raw_kafka_records,
            expected_raw_kafka,
        ),
        (
            "normalized_records",
            snapshot.normalized_records,
            expected_normalized,
        ),
        ("raw_bytes", snapshot.raw_bytes, receipt_raw_bytes),
        (
            "provider_requests",
            snapshot.provider_requests,
            receipt_provider_requests,
        ),
        ("cursor_checks", snapshot.cursor_checks, receipt_cursor_checks),
        (
            "cursor_consistency_errors",
            snapshot.cursor_consistency_errors,
            receipt_cursor_errors,
        ),
    )
    failures.extend(
        f"{name} differs from offer receipts"
        for name, actual, wanted in exact_pairs
        if actual != wanted
    )
    if not math.isclose(
        snapshot.quota_units,
        receipt_quota_units,
        rel_tol=1e-9,
        abs_tol=1e-9,
    ):
        failures.append("quota_units differ from offer receipts")
    if snapshot.observation_latency.count != snapshot.observations:
        failures.append("observation latency count differs")
    if snapshot.t1_latency.count != snapshot.t1_triggers:
        failures.append("T1 latency count differs")
    if snapshot.raw_latency.count != snapshot.raw_kafka_records:
        failures.append("raw latency count differs")
    if snapshot.normalized_latency.count != snapshot.normalized_records:
        failures.append("normalized latency count differs")
    if scheduled_data and len(snapshot.tenant_ids) != topology.tenants:
        failures.append("tenant topology coverage differs")
    if (
        scheduled_data
        and len(snapshot.installation_ids) != topology.installation_count
    ):
        failures.append("installation topology coverage differs")
    if len(snapshot.replica_ids) != topology.replicas:
        failures.append("replica topology coverage differs")
    participating_replicas = sum(
        count > 0 for _replica_id, count in snapshot.replica_processed_items
    )
    if scheduled_data and participating_replicas != topology.replicas:
        failures.append("actual replica participation differs")
    if sum(
        count for _replica_id, count in snapshot.replica_processed_items
    ) != expected_raw_kafka:
        failures.append(
            "replica processed count differs from raw Kafka records"
        )

    metrics: dict[str, int | float] = {
        "offered_items": offered,
        "scheduled_data_operations": scheduled_data,
        "scheduled_control_operations": scheduled_control,
        "accepted_items": accepted,
        "expected_observations": expected,
        "expected_raw_s3_objects": expected_raw_s3,
        "expected_raw_kafka_records": expected_raw_kafka,
        "expected_normalized_records": expected_normalized,
        "raw_s3_objects": snapshot.raw_s3_objects,
        "raw_kafka_records": snapshot.raw_kafka_records,
        "normalized_records": snapshot.normalized_records,
        "observations": snapshot.observations,
        "t1_triggers": snapshot.t1_triggers,
        "unique_observation_identities": (
            snapshot.unique_observation_identities
        ),
        "unique_t1_observation_ids": snapshot.unique_t1_observation_ids,
        "raw_bytes": snapshot.raw_bytes,
        "normalized_bytes": snapshot.normalized_bytes,
        "provider_requests": snapshot.provider_requests,
        "quota_units": snapshot.quota_units,
        "missing_records": missing,
        "unexpected_duplicates": duplicates,
        "cross_tenant_leaks": snapshot.cross_tenant_leaks,
        "cursor_checks": snapshot.cursor_checks,
        "cursor_required_operations": cursor_required,
        "cursor_proven_operations": cursor_proven,
        "cursor_consistency_errors": snapshot.cursor_consistency_errors,
        "cooldown_violations": snapshot.cooldown_violations,
        "failed_requests": snapshot.failed_requests,
        "dlq_entries": snapshot.dlq_entries,
        "raw_kafka_lag": snapshot.raw_kafka_lag,
        "normalized_kafka_lag": snapshot.normalized_kafka_lag,
        "observation_to_t1_lag": snapshot.observation_to_t1_lag,
        "peak_backlog": snapshot.peak_backlog,
        "backlog_growth_per_second": (
            (
                snapshot.raw_kafka_lag
                + snapshot.normalized_kafka_lag
                + snapshot.observation_to_t1_lag
            )
            / wall_elapsed_seconds
        ),
        "offered_items_per_second": offered / wall_elapsed_seconds,
        "data_operations_per_second": (
            scheduled_data / wall_elapsed_seconds
        ),
        "raw_records_per_second": (
            snapshot.raw_kafka_records / wall_elapsed_seconds
        ),
        "normalized_records_per_second": (
            snapshot.normalized_records / wall_elapsed_seconds
        ),
        "observations_per_second": (
            snapshot.observations / wall_elapsed_seconds
        ),
        "t1_triggers_per_second": (
            snapshot.t1_triggers / wall_elapsed_seconds
        ),
        "bytes_per_second": snapshot.raw_bytes / wall_elapsed_seconds,
        "quota_units_per_second": (
            snapshot.quota_units / wall_elapsed_seconds
        ),
        "scheduled_elapsed_seconds": scheduled_elapsed_seconds,
        "wall_elapsed_seconds": wall_elapsed_seconds,
        "scheduled_duration_ratio": (
            scheduled_elapsed_seconds / duration_seconds
        ),
        "end_to_end_duration_ratio": (
            wall_elapsed_seconds / duration_seconds
        ),
        "offered_rate_achievement_ratio": (
            (
                (scheduled_data / wall_elapsed_seconds) / target_rate
                if scheduled_data
                else 1.0
            )
        ),
        "p50_raw_latency_ms": snapshot.raw_latency.p50_ms,
        "p95_raw_latency_ms": snapshot.raw_latency.p95_ms,
        "p99_raw_latency_ms": snapshot.raw_latency.p99_ms,
        "p50_normalized_latency_ms": snapshot.normalized_latency.p50_ms,
        "p95_normalized_latency_ms": snapshot.normalized_latency.p95_ms,
        "p99_normalized_latency_ms": snapshot.normalized_latency.p99_ms,
        "p50_observation_latency_ms": snapshot.observation_latency.p50_ms,
        "p95_observation_latency_ms": snapshot.observation_latency.p95_ms,
        "p99_observation_latency_ms": snapshot.observation_latency.p99_ms,
        "p50_t1_latency_ms": snapshot.t1_latency.p50_ms,
        "p95_t1_latency_ms": snapshot.t1_latency.p95_ms,
        "p99_t1_latency_ms": snapshot.t1_latency.p99_ms,
        "cpu_percent": snapshot.cpu_percent,
        "memory_bytes": snapshot.memory_bytes,
        "tenant_count": len(snapshot.tenant_ids),
        "installation_count": len(snapshot.installation_ids),
        "replica_count": len(snapshot.replica_ids),
        "participating_replica_count": participating_replicas,
    }
    return metrics, tuple(failures)


def _stable(
    metrics: Mapping[str, int | float],
    *,
    maximum_p99_latency_ms: float,
    crosscheck_failures: Sequence[str],
) -> tuple[bool, tuple[str, ...]]:
    failures = list(crosscheck_failures)
    equality_checks = (
        ("accepted_items", "offered_items"),
        ("raw_s3_objects", "expected_raw_s3_objects"),
        ("raw_kafka_records", "expected_raw_kafka_records"),
        ("normalized_records", "expected_normalized_records"),
        ("observations", "expected_observations"),
        ("t1_triggers", "expected_observations"),
        ("unique_observation_identities", "observations"),
        ("unique_t1_observation_ids", "observations"),
        ("cursor_proven_operations", "cursor_required_operations"),
    )
    failures.extend(
        f"{left} differs from {right}"
        for left, right in equality_checks
        if metrics[left] != metrics[right]
    )
    for name in (
        "missing_records",
        "unexpected_duplicates",
        "cross_tenant_leaks",
        "cursor_consistency_errors",
        "cooldown_violations",
        "failed_requests",
        "dlq_entries",
        "raw_kafka_lag",
        "normalized_kafka_lag",
        "observation_to_t1_lag",
    ):
        if metrics[name] != 0:
            failures.append(f"{name} must equal zero")
    if metrics["scheduled_duration_ratio"] < 0.99:
        failures.append("scheduled duration ratio is below 0.99")
    if (
        metrics["end_to_end_duration_ratio"]
        < metrics["scheduled_duration_ratio"]
    ):
        failures.append("end-to-end duration precedes scheduled duration")
    if metrics["offered_rate_achievement_ratio"] < 0.9:
        failures.append("end-to-end offered rate is below 90% of target")
    if (
        metrics["p99_observation_latency_ms"]
        > maximum_p99_latency_ms
    ):
        failures.append("observation p99 exceeds configured maximum")
    return not failures, tuple(failures)


async def _run_trial(
    *,
    adapter: PipelineBoundaryAdapter,
    clock: PipelineLoadClock,
    source_id: str,
    mode: PipelineLoadMode,
    workload: DeclaredPipelineWorkload,
    config: PipelineLoadRunConfig,
    trial_index: int,
    phase: TrialPhase,
    target_rate: float,
    duration_seconds: float,
) -> dict[str, object]:
    context = TrialContext(
        trial_index=trial_index,
        phase=phase,
        target_rate=target_rate,
        duration_seconds=duration_seconds,
        mode=mode,
        workload_kind=workload.kind,
    )
    baseline = await adapter.begin_trial(context)
    if not _zero_snapshot(baseline):
        raise PipelineLoadError(
            "trial baseline is not zero in the dedicated namespace"
        )
    scheduled_counts, target_data_operations, control_operations = (
        _scheduled_operation_counts(
            workload,
            target_rate=target_rate,
            duration_seconds=duration_seconds,
        )
    )
    target_items = target_data_operations + control_operations
    per_item_weight = sum(
        operation.selection_weight or 0
        for operation in workload.executable_operations
        if operation.execution_frequency == "per_item"
    )
    if (
        config.release
        and target_data_operations
        and target_data_operations
        < max(config.topology.lane_count, per_item_weight)
    ):
        raise PipelineLoadError(
            "release trial does not offer enough data operations to cover "
            "every lane and weighted operation"
        )

    started_at = clock.now()
    started_monotonic = clock.monotonic()
    receipts: list[OfferReceipt] = []
    pending: set[asyncio.Task[OfferReceipt]] = set()
    scheduled_items: dict[
        int,
        tuple[WorkItem, ExecutableLoadOperation],
    ] = {}
    for sequence, (
        scheduled_offset,
        operation,
        data_ordinal,
    ) in enumerate(
        _scheduled_operations(
            workload,
            target_rate=target_rate,
            duration_seconds=duration_seconds,
        ),
        start=1,
    ):
        scheduled = started_monotonic + scheduled_offset
        await clock.sleep(max(0.0, scheduled - clock.monotonic()))
        tenant, installation, replica = _lane(
            data_ordinal or 1,
            config.topology,
        )
        item = WorkItem(
            sequence=sequence,
            event_id=(
                f"{source_id}:{trial_index}:{sequence}:"
                f"{tenant}:{installation}:{replica}"
            ),
            operation_id=operation.operation_id,
            operation_contract_sha256=operation.declaration_sha256,
            evidence_id=operation.evidence_id,
            tenant_slot=tenant,
            installation_slot=installation,
            replica_slot=replica,
            scheduled_monotonic=scheduled,
        )
        scheduled_items[sequence] = (item, operation)
        pending.add(asyncio.create_task(adapter.offer(item)))
        if len(pending) >= config.maximum_in_flight:
            await _collect_pending(pending, receipts, all_tasks=False)
    await clock.sleep(
        max(
            0.0,
            started_monotonic + duration_seconds - clock.monotonic(),
        )
    )
    await _collect_pending(pending, receipts, all_tasks=True)
    scheduled_elapsed_seconds = clock.monotonic() - started_monotonic
    receipts.sort(key=lambda item: item.sequence)
    if [item.sequence for item in receipts] != list(
        range(1, target_items + 1)
    ):
        raise PipelineLoadError("offer receipts are missing or duplicated")
    if len({item.event_id for item in receipts}) != len(receipts):
        raise PipelineLoadError("offer receipts contain duplicate event IDs")
    if len(
        {item.execution_evidence_sha256 for item in receipts}
    ) != len(receipts):
        raise PipelineLoadError(
            "offer receipts contain duplicate execution evidence"
        )
    actual_operation_counts = {
        operation_id: sum(
            receipt.operation_id == operation_id
            for receipt in receipts
        )
        for operation_id in workload.operation_ids
    }
    if actual_operation_counts != scheduled_counts:
        raise PipelineLoadError(
            "offer receipts differ from the deterministic operation schedule"
        )
    for receipt in receipts:
        try:
            scheduled_item, operation = scheduled_items[receipt.sequence]
        except KeyError as exc:
            raise PipelineLoadError(
                "offer receipt has an unscheduled sequence"
            ) from exc
        _validate_offer_receipt(
            receipt,
            item=scheduled_item,
            operation=operation,
        )

    terminal = await adapter.finish_trial()
    completed_at = clock.now()
    wall_elapsed_seconds = clock.monotonic() - started_monotonic
    if scheduled_elapsed_seconds <= 0 or wall_elapsed_seconds <= 0:
        raise PipelineLoadError("trial elapsed time is not positive")
    metrics, crosscheck_failures = _metrics(
        snapshot=terminal,
        receipts=receipts,
        workload=workload,
        duration_seconds=duration_seconds,
        target_rate=target_rate,
        scheduled_elapsed_seconds=scheduled_elapsed_seconds,
        wall_elapsed_seconds=wall_elapsed_seconds,
        topology=config.topology,
    )
    stable, failures = _stable(
        metrics,
        maximum_p99_latency_ms=(
            config.maximum_p99_observation_latency_ms
        ),
        crosscheck_failures=crosscheck_failures,
    )
    receipt_ledger_sha256 = _receipt_ledger_sha256(receipts)
    if terminal.receipt_ledger_sha256 != receipt_ledger_sha256:
        stable = False
        failures = (
            *failures,
            "receipt ledger differs from offer receipts",
        )
    return {
        "trial_index": trial_index,
        "phase": phase,
        "target_rate": target_rate,
        "duration_seconds": duration_seconds,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "stable": stable,
        "failures": list(failures),
        "event_ledger_sha256": terminal.event_ledger_sha256,
        "receipt_ledger_sha256": terminal.receipt_ledger_sha256,
        "cursor_ledger_sha256": terminal.cursor_ledger_sha256,
        "operation_counts": actual_operation_counts,
        "replica_processed_items": dict(
            terminal.replica_processed_items
        ),
        "metrics": metrics,
    }


def _trial_rate(trial: Mapping[str, object]) -> float:
    value = trial.get("target_rate")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise PipelineLoadError("trial target rate is invalid")
    return float(value)


async def _search(
    *,
    adapter: PipelineBoundaryAdapter,
    clock: PipelineLoadClock,
    source_id: str,
    mode: PipelineLoadMode,
    workload: DeclaredPipelineWorkload,
    config: PipelineLoadRunConfig,
    maximum_rate: float,
) -> tuple[list[dict[str, object]], float, bool]:
    trials: list[dict[str, object]] = []

    async def run(
        phase: TrialPhase,
        rate: float,
        duration: float,
    ) -> dict[str, object]:
        try:
            trial = await _run_trial(
                adapter=adapter,
                clock=clock,
                source_id=source_id,
                mode=mode,
                workload=workload,
                config=config,
                trial_index=len(trials) + 1,
                phase=phase,
                target_rate=rate,
                duration_seconds=duration,
            )
        except PipelineLoadError as exc:
            raise _PipelineLoadSearchError(str(exc), trials) from exc
        trials.append(trial)
        return trial

    initial = min(config.initial_rate, maximum_rate)
    warmup = await run("warmup", initial, config.timing.warmup_seconds)
    if warmup["stable"] is not True:
        raise _PipelineLoadSearchError("warmup was not stable", trials)

    low = initial
    high: float | None = None
    for _ in range(config.maximum_step_trials):
        if math.isclose(low, maximum_rate, rel_tol=1e-12):
            break
        candidate = min(
            maximum_rate,
            low * (1.0 + config.step_fraction),
        )
        trial = await run("step", candidate, config.timing.step_seconds)
        if trial["stable"] is True:
            low = candidate
            continue
        high = candidate
        break

    if high is not None:
        for _ in range(config.maximum_binary_trials):
            if (high - low) / max(low, 1e-12) <= (
                config.search_tolerance_fraction
            ):
                break
            candidate = (low + high) / 2
            trial = await run(
                "binary_search",
                candidate,
                config.timing.step_seconds,
            )
            if trial["stable"] is True:
                low = candidate
            else:
                high = candidate

    if mode == "fyralis_ceiling" and high is None:
        raise _PipelineLoadSearchError(
            "Fyralis ceiling was not found before the safety cap",
            trials,
        )

    validation = await run(
        "validation",
        low,
        config.timing.validation_seconds,
    )
    if validation["stable"] is not True:
        raise _PipelineLoadSearchError(
            "15-minute validation was not stable",
            trials,
        )
    if config.include_soak:
        soak = await run("soak", low, config.timing.soak_seconds)
        if soak["stable"] is not True:
            raise _PipelineLoadSearchError(
                "60-minute soak was not stable",
                trials,
            )
    return trials, low, high is not None


def _artifact_hash(payload: Mapping[str, object]) -> str:
    return _sha256(_canonical_bytes(payload))


def _finish_artifact(payload: dict[str, object]) -> dict[str, object]:
    payload["artifact_sha256"] = _artifact_hash(payload)
    validate_pipeline_load_artifact(payload)
    return payload


def _blocked_artifact(
    *,
    source_id: str,
    mode: PipelineLoadMode,
    workload: DeclaredPipelineWorkload,
    config: PipelineLoadRunConfig,
    now: datetime,
    reason_code: str,
    quota: VerifiedQuotaConfiguration | None,
    state: Literal["blocked", "not_applicable"] = "blocked",
) -> dict[str, object]:
    current = _aware(now, "now").isoformat()
    return _finish_artifact(
        {
            "schema_version": PIPELINE_LOAD_ARTIFACT_SCHEMA_VERSION,
            "source_id": source_id,
            "mode": mode,
            "workload": workload.to_dict(),
            "state": state,
            "promotion_eligible": False,
            "clock": "not_started",
            "started_at": current,
            "completed_at": current,
            "configuration": config.to_dict(),
            "infrastructure": None,
            "boundary": None,
            "quota": quota.to_dict() if quota is not None else None,
            "trials": [],
            "maximum_stable_rate": None,
            "reason_code": reason_code,
            "claim_boundary": (
                (
                    "No load claim is applicable to this explicitly "
                    "unsupported workload shape."
                    if state == "not_applicable"
                    else (
                        "No load claim is made because release prerequisites "
                        "were not accepted."
                    )
                )
            ),
        }
    )


async def run_pipeline_load(
    *,
    source_id: str,
    mode: PipelineLoadMode,
    workload: DeclaredPipelineWorkload,
    ambient_env: Mapping[str, str],
    adapter_factory: PipelineAdapterFactory | None,
    quota: VerifiedQuotaConfiguration | None = None,
    config: PipelineLoadRunConfig | None = None,
    clock: PipelineLoadClock | None = None,
) -> dict[str, object]:
    """Search and validate one exact end-to-end pipeline load envelope."""

    if source_id not in CANONICAL_SOURCE_IDS:
        raise PipelineLoadError(f"unknown canonical source {source_id!r}")
    if mode not in {"provider_safe", "fyralis_ceiling"}:
        raise PipelineLoadError(f"unknown pipeline load mode {mode!r}")
    workload.validate_source(source_id)
    effective_config = config or PipelineLoadRunConfig()
    effective_clock = clock or SystemPipelineLoadClock()
    started_at = effective_clock.now()
    if workload.non_applicability is not None:
        return _blocked_artifact(
            source_id=source_id,
            mode=mode,
            workload=workload,
            config=effective_config,
            now=started_at,
            reason_code="workload_declared_not_applicable",
            quota=quota,
            state="not_applicable",
        )
    if any(
        assertion.blocks_execution
        for assertion in workload.contract_absence_assertions
    ):
        return _blocked_artifact(
            source_id=source_id,
            mode=mode,
            workload=workload,
            config=effective_config,
            now=started_at,
            reason_code="required_executable_contract_absent",
            quota=quota,
        )

    infrastructure, infrastructure_error = (
        resolve_isolated_pipeline_infrastructure(ambient_env)
    )
    if infrastructure is None:
        return _blocked_artifact(
            source_id=source_id,
            mode=mode,
            workload=workload,
            config=effective_config,
            now=started_at,
            reason_code=infrastructure_error
            or "isolated_infrastructure_rejected",
            quota=quota,
        )
    if adapter_factory is None:
        return _blocked_artifact(
            source_id=source_id,
            mode=mode,
            workload=workload,
            config=effective_config,
            now=started_at,
            reason_code="exact_pipeline_adapter_absent",
            quota=quota,
        )
    if mode == "provider_safe":
        if quota is None:
            return _blocked_artifact(
                source_id=source_id,
                mode=mode,
                workload=workload,
                config=effective_config,
                now=started_at,
                reason_code="verified_quota_configuration_absent",
                quota=None,
            )
        try:
            quota.validate_freshness(
                source_id=source_id,
                now=started_at,
                maximum_age=effective_config.quota_maximum_age,
            )
        except PipelineLoadError:
            return _blocked_artifact(
                source_id=source_id,
                mode=mode,
                workload=workload,
                config=effective_config,
                now=started_at,
                reason_code="verified_quota_configuration_rejected",
                quota=quota,
            )
    elif quota is not None:
        return _blocked_artifact(
            source_id=source_id,
            mode=mode,
            workload=workload,
            config=effective_config,
            now=started_at,
            reason_code="ceiling_mode_must_disable_provider_quotas",
            quota=quota,
        )

    created = adapter_factory(
        infrastructure,
        source_id,
        mode,
        workload,
        effective_config.topology,
        quota,
    )
    adapter = await created if isinstance(created, Awaitable) else created
    if not isinstance(adapter, PipelineBoundaryAdapter):
        raise PipelineLoadError(
            "adapter_factory did not return a PipelineBoundaryAdapter"
        )
    _validate_boundary(
        adapter.boundary,
        source_id=source_id,
        mode=mode,
        workload=workload,
        topology=effective_config.topology,
        infrastructure=infrastructure,
    )
    maximum_rate = effective_config.maximum_offered_rate
    if quota is not None:
        maximum_rate = min(maximum_rate, quota.modeled_maximum_rate)

    trials: list[dict[str, object]] = []
    stable_rate: float | None = None
    failure: str | None = None
    try:
        trials, stable_rate, _ceiling_observed = await _search(
            adapter=adapter,
            clock=effective_clock,
            source_id=source_id,
            mode=mode,
            workload=workload,
            config=effective_config,
            maximum_rate=maximum_rate,
        )
        if (
            mode == "provider_safe"
            and quota is not None
            and stable_rate < quota.modeled_maximum_rate * 0.9
        ):
            raise PipelineLoadError(
                "provider-safe stable rate is below 90% of modeled quota"
            )
    except _PipelineLoadSearchError as exc:
        trials = exc.trials
        failure = str(exc)
    except PipelineLoadError as exc:
        failure = str(exc)
    finally:
        await adapter.close()

    release_clock = (
        isinstance(effective_clock, SystemPipelineLoadClock)
        and effective_clock.release_wall_clock
    )
    promotion_eligible = (
        failure is None
        and effective_config.release
        and release_clock
        and adapter.boundary.evidence_class == "exact_pipeline"
        and stable_rate is not None
    )
    state: PipelineLoadState
    if failure is not None:
        state = "failed"
    elif promotion_eligible:
        state = "passed"
    else:
        state = "diagnostic"
    completed_at = effective_clock.now()
    payload: dict[str, object] = {
        "schema_version": PIPELINE_LOAD_ARTIFACT_SCHEMA_VERSION,
        "source_id": source_id,
        "mode": mode,
        "workload": workload.to_dict(),
        "state": state,
        "promotion_eligible": promotion_eligible,
        "clock": (
            "system_wall_clock" if release_clock else "injected_test_clock"
        ),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "configuration": effective_config.to_dict(),
        "infrastructure": infrastructure.descriptor,
        "boundary": adapter.boundary.to_dict(),
        "quota": quota.to_dict() if quota is not None else None,
        "trials": trials,
        "maximum_stable_rate": stable_rate,
        "reason_code": (
            None
            if failure is None
            else "pipeline_load_execution_failed"
        ),
        "claim_boundary": (
            (
                "The artifact measures the exact isolated S3 raw evidence, "
                "raw and normalized Kafka lanes, Observation persistence, "
                "and same-tenant T1 trigger boundary under scheduled "
                "offered load."
                if adapter.boundary.evidence_class == "exact_pipeline"
                else (
                    "Diagnostic test-double evidence only; no exact-pipeline "
                    "or release claim is made."
                )
            )
            if failure is None
            else failure
        ),
    }
    return _finish_artifact(payload)


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PipelineLoadArtifactError(f"{field_name} must be an object")
    return value


def _exact_fields(
    value: Mapping[str, Any],
    fields: frozenset[str],
    field_name: str,
) -> None:
    if set(value) != fields:
        raise PipelineLoadArtifactError(f"{field_name} fields differ")


def _parse_time(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise PipelineLoadArtifactError(f"{field_name} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise PipelineLoadArtifactError(
            f"{field_name} is not ISO-8601"
        ) from exc
    try:
        return _aware(parsed, field_name)
    except ValueError as exc:
        raise PipelineLoadArtifactError(str(exc)) from exc


def _artifact_number(
    value: object,
    field_name: str,
    *,
    positive: bool = True,
) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or (positive and float(value) <= 0)
        or (not positive and float(value) < 0)
    ):
        qualifier = "positive" if positive else "non-negative"
        raise PipelineLoadArtifactError(
            f"{field_name} must be a finite {qualifier} number"
        )
    return float(value)


def _artifact_integer(
    value: object,
    field_name: str,
    *,
    positive: bool = True,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or (positive and value < 1)
        or (not positive and value < 0)
    ):
        qualifier = "positive" if positive else "non-negative"
        raise PipelineLoadArtifactError(
            f"{field_name} must be a {qualifier} integer"
        )
    return value


def _declared_workload_from_artifact(
    value: Mapping[str, Any],
    *,
    source_id: str,
) -> DeclaredPipelineWorkload:
    _exact_fields(
        value,
        frozenset(
            {
                "kind",
                "executable_operations",
                "contract_absence_assertions",
                "non_applicability",
                "declaration_sha256",
            }
        ),
        "workload",
    )
    raw_operations = value.get("executable_operations")
    raw_absences = value.get("contract_absence_assertions")
    if (
        not isinstance(raw_operations, list)
        or not isinstance(raw_absences, list)
    ):
        raise PipelineLoadArtifactError(
            "workload executable contract arrays are invalid"
        )
    operations: list[ExecutableLoadOperation] = []
    for index, raw_operation in enumerate(raw_operations):
        operation_map = _mapping(
            raw_operation,
            f"workload.executable_operations[{index}]",
        )
        _exact_fields(
            operation_map,
            frozenset(
                {
                    "operation_id",
                    "executable_binding",
                    "evidence_id",
                    "kind",
                    "execution_frequency",
                    "selection_weight",
                    "cadence_seconds",
                    "trial_position",
                    "raw_cardinality",
                    "normalized_cardinality",
                    "observation_cardinality",
                    "cursor_applicability",
                    "quota_mappings",
                    "receipt_proof_requirements",
                }
            ),
            f"workload.executable_operations[{index}]",
        )
        raw_mappings = operation_map.get("quota_mappings")
        raw_proofs = operation_map.get("receipt_proof_requirements")
        if not isinstance(raw_mappings, list) or not isinstance(
            raw_proofs,
            list,
        ):
            raise PipelineLoadArtifactError(
                "workload operation quota/proof arrays are invalid"
            )
        mappings: list[LoadQuotaMapping] = []
        for mapping_index, raw_mapping in enumerate(raw_mappings):
            mapping = _mapping(
                raw_mapping,
                (
                    "workload.executable_operations"
                    f"[{index}].quota_mappings[{mapping_index}]"
                ),
            )
            _exact_fields(
                mapping,
                frozenset(
                    {
                        "operation_id",
                        "quota_bucket",
                        "units_per_request",
                    }
                ),
                "workload operation quota mapping",
            )
            try:
                mappings.append(
                    LoadQuotaMapping(
                        operation_id=mapping.get("operation_id"),  # type: ignore[arg-type]
                        quota_bucket=mapping.get("quota_bucket"),  # type: ignore[arg-type]
                        units_per_request=mapping.get("units_per_request"),  # type: ignore[arg-type]
                    )
                )
            except (CertificationInvariantError, TypeError) as exc:
                raise PipelineLoadArtifactError(
                    "workload quota mapping is invalid"
                ) from exc
        try:
            operations.append(
                ExecutableLoadOperation(
                    operation_id=operation_map.get("operation_id"),  # type: ignore[arg-type]
                    executable_binding=operation_map.get("executable_binding"),  # type: ignore[arg-type]
                    evidence_id=operation_map.get("evidence_id"),  # type: ignore[arg-type]
                    kind=operation_map.get("kind"),  # type: ignore[arg-type]
                    execution_frequency=operation_map.get("execution_frequency"),  # type: ignore[arg-type]
                    selection_weight=operation_map.get("selection_weight"),  # type: ignore[arg-type]
                    cadence_seconds=operation_map.get("cadence_seconds"),  # type: ignore[arg-type]
                    trial_position=operation_map.get("trial_position"),  # type: ignore[arg-type]
                    raw_cardinality=operation_map.get("raw_cardinality"),  # type: ignore[arg-type]
                    normalized_cardinality=operation_map.get("normalized_cardinality"),  # type: ignore[arg-type]
                    observation_cardinality=operation_map.get("observation_cardinality"),  # type: ignore[arg-type]
                    cursor_applicability=operation_map.get("cursor_applicability"),  # type: ignore[arg-type]
                    quota_mappings=tuple(mappings),
                    receipt_proof_requirements=tuple(raw_proofs),  # type: ignore[arg-type]
                )
            )
        except (CertificationInvariantError, TypeError) as exc:
            raise PipelineLoadArtifactError(
                "workload executable operation is invalid"
            ) from exc

    absences: list[LoadOperationContractAbsence] = []
    for index, raw_absence in enumerate(raw_absences):
        absence_map = _mapping(
            raw_absence,
            f"workload.contract_absence_assertions[{index}]",
        )
        _exact_fields(
            absence_map,
            frozenset(
                {
                    "operation_id",
                    "evidence_id",
                    "missing_contract",
                    "reason",
                    "blocks_execution",
                }
            ),
            "workload contract absence",
        )
        try:
            absences.append(
                LoadOperationContractAbsence(
                    operation_id=absence_map.get("operation_id"),  # type: ignore[arg-type]
                    evidence_id=absence_map.get("evidence_id"),  # type: ignore[arg-type]
                    missing_contract=absence_map.get("missing_contract"),  # type: ignore[arg-type]
                    reason=absence_map.get("reason"),  # type: ignore[arg-type]
                    blocks_execution=absence_map.get("blocks_execution"),  # type: ignore[arg-type]
                )
            )
        except (CertificationInvariantError, TypeError) as exc:
            raise PipelineLoadArtifactError(
                "workload contract absence is invalid"
            ) from exc
    raw_non_applicability = value.get("non_applicability")
    non_applicability = None
    if raw_non_applicability is not None:
        non_applicability_map = _mapping(
            raw_non_applicability,
            "workload.non_applicability",
        )
        _exact_fields(
            non_applicability_map,
            frozenset({"evidence_id", "reason"}),
            "workload.non_applicability",
        )
        try:
            non_applicability = LoadSuiteNonApplicability(
                evidence_id=non_applicability_map.get("evidence_id"),  # type: ignore[arg-type]
                reason=non_applicability_map.get("reason"),  # type: ignore[arg-type]
            )
        except (CertificationInvariantError, TypeError) as exc:
            raise PipelineLoadArtifactError(
                "workload non-applicability declaration is invalid"
            ) from exc
    try:
        declared = DeclaredPipelineWorkload(
            kind=value.get("kind"),  # type: ignore[arg-type]
            executable_operations=tuple(operations),
            contract_absence_assertions=tuple(absences),
            non_applicability=non_applicability,
        )
        declared.validate_source(source_id)
    except (ValueError, PipelineLoadError) as exc:
        raise PipelineLoadArtifactError(
            "workload declaration is invalid"
        ) from exc
    if declared.to_dict() != dict(value):
        raise PipelineLoadArtifactError(
            "workload declaration is non-canonical or its hash differs"
        )
    return declared


def validate_pipeline_load_artifact(value: object) -> None:
    """Strictly validate semantic cross-checks and the self hash."""

    artifact = _mapping(value, "pipeline load artifact")
    top_fields = frozenset(
        {
            "schema_version",
            "source_id",
            "mode",
            "workload",
            "state",
            "promotion_eligible",
            "clock",
            "started_at",
            "completed_at",
            "configuration",
            "infrastructure",
            "boundary",
            "quota",
            "trials",
            "maximum_stable_rate",
            "reason_code",
            "claim_boundary",
            "artifact_sha256",
        }
    )
    _exact_fields(artifact, top_fields, "pipeline load artifact")
    if artifact.get("schema_version") != PIPELINE_LOAD_ARTIFACT_SCHEMA_VERSION:
        raise PipelineLoadArtifactError("unsupported artifact schema")
    source_id = artifact.get("source_id")
    if source_id not in CANONICAL_SOURCE_IDS:
        raise PipelineLoadArtifactError("artifact source is not canonical")
    mode = artifact.get("mode")
    state = artifact.get("state")
    if mode not in {"provider_safe", "fyralis_ceiling"}:
        raise PipelineLoadArtifactError("artifact mode is invalid")
    workload = _mapping(artifact.get("workload"), "workload")
    declared_workload = _declared_workload_from_artifact(
        workload,
        source_id=source_id,
    )
    workload_kind = declared_workload.kind
    operation_ids = declared_workload.operation_ids
    expected_workload_sha = declared_workload.declaration_sha256
    if state not in {
        "passed",
        "diagnostic",
        "failed",
        "blocked",
        "not_applicable",
    }:
        raise PipelineLoadArtifactError("artifact state is invalid")
    digest = artifact.get("artifact_sha256")
    if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
        raise PipelineLoadArtifactError("artifact_sha256 is invalid")
    unhashed = dict(artifact)
    del unhashed["artifact_sha256"]
    if _artifact_hash(unhashed) != digest:
        raise PipelineLoadArtifactError("artifact_sha256 differs")
    started = _parse_time(artifact.get("started_at"), "started_at")
    completed = _parse_time(artifact.get("completed_at"), "completed_at")
    if completed < started:
        raise PipelineLoadArtifactError("artifact completed before it started")
    configuration = _mapping(
        artifact.get("configuration"),
        "configuration",
    )
    _exact_fields(
        configuration,
        frozenset(
            {
                "topology",
                "timing",
                "initial_rate",
                "maximum_offered_rate",
                "step_fraction",
                "search_tolerance_fraction",
                "maximum_step_trials",
                "maximum_binary_trials",
                "maximum_in_flight",
                "maximum_p99_observation_latency_ms",
                "include_soak",
                "release",
                "quota_maximum_age_seconds",
            }
        ),
        "configuration",
    )
    release = configuration.get("release")
    topology = _mapping(configuration.get("topology"), "topology")
    timing = _mapping(configuration.get("timing"), "timing")
    _exact_fields(
        topology,
        frozenset(
            {"tenants", "installations_per_tenant", "replicas"}
        ),
        "topology",
    )
    _exact_fields(
        timing,
        frozenset(
            {
                "warmup_seconds",
                "step_seconds",
                "validation_seconds",
                "soak_seconds",
            }
        ),
        "timing",
    )
    if (
        not isinstance(configuration.get("release"), bool)
        or not isinstance(configuration.get("include_soak"), bool)
    ):
        raise PipelineLoadArtifactError(
            "configuration release/include_soak must be booleans"
        )
    try:
        validated_config = PipelineLoadRunConfig(
            topology=PipelineLoadTopology(
                tenants=_artifact_integer(
                    topology.get("tenants"),
                    "topology.tenants",
                ),
                installations_per_tenant=_artifact_integer(
                    topology.get("installations_per_tenant"),
                    "topology.installations_per_tenant",
                ),
                replicas=_artifact_integer(
                    topology.get("replicas"),
                    "topology.replicas",
                ),
            ),
            timing=PipelineLoadTiming(
                warmup_seconds=_artifact_number(
                    timing.get("warmup_seconds"),
                    "timing.warmup_seconds",
                ),
                step_seconds=_artifact_number(
                    timing.get("step_seconds"),
                    "timing.step_seconds",
                ),
                validation_seconds=_artifact_number(
                    timing.get("validation_seconds"),
                    "timing.validation_seconds",
                ),
                soak_seconds=_artifact_number(
                    timing.get("soak_seconds"),
                    "timing.soak_seconds",
                ),
            ),
            initial_rate=_artifact_number(
                configuration.get("initial_rate"),
                "configuration.initial_rate",
            ),
            maximum_offered_rate=_artifact_number(
                configuration.get("maximum_offered_rate"),
                "configuration.maximum_offered_rate",
            ),
            step_fraction=_artifact_number(
                configuration.get("step_fraction"),
                "configuration.step_fraction",
            ),
            search_tolerance_fraction=_artifact_number(
                configuration.get("search_tolerance_fraction"),
                "configuration.search_tolerance_fraction",
            ),
            maximum_step_trials=_artifact_integer(
                configuration.get("maximum_step_trials"),
                "configuration.maximum_step_trials",
            ),
            maximum_binary_trials=_artifact_integer(
                configuration.get("maximum_binary_trials"),
                "configuration.maximum_binary_trials",
            ),
            maximum_in_flight=_artifact_integer(
                configuration.get("maximum_in_flight"),
                "configuration.maximum_in_flight",
            ),
            maximum_p99_observation_latency_ms=_artifact_number(
                configuration.get("maximum_p99_observation_latency_ms"),
                "configuration.maximum_p99_observation_latency_ms",
            ),
            include_soak=configuration["include_soak"],
            release=configuration["release"],
            quota_maximum_age=timedelta(
                seconds=_artifact_number(
                    configuration.get("quota_maximum_age_seconds"),
                    "configuration.quota_maximum_age_seconds",
                )
            ),
        )
    except ValueError as exc:
        raise PipelineLoadArtifactError(
            "artifact configuration is internally invalid"
        ) from exc
    if validated_config.to_dict() != dict(configuration):
        raise PipelineLoadArtifactError(
            "artifact configuration is not canonical"
        )
    if state == "passed" and (
        artifact.get("promotion_eligible") is not True
        or release is not True
        or artifact.get("clock") != "system_wall_clock"
        or topology
        != {
            "tenants": 2,
            "installations_per_tenant": 2,
            "replicas": 2,
        }
        or float(timing.get("warmup_seconds", 0)) < 120
        or float(timing.get("step_seconds", 0)) < 120
        or float(timing.get("validation_seconds", 0)) < 900
        or float(timing.get("soak_seconds", 0)) < 3_600
        or configuration.get("include_soak") is not True
    ):
        raise PipelineLoadArtifactError(
            "passing artifact lacks release clock/topology/durations"
        )
    if state != "passed" and artifact.get("promotion_eligible") is not False:
        raise PipelineLoadArtifactError(
            "non-passing artifact cannot be promotion eligible"
        )
    trials = artifact.get("trials")
    if not isinstance(trials, list):
        raise PipelineLoadArtifactError("trials must be an array")
    if state in {"blocked", "not_applicable"}:
        if trials or artifact.get("maximum_stable_rate") is not None:
            raise PipelineLoadArtifactError(
                "non-executed artifact cannot contain load results"
            )
        if state == "not_applicable" and (
            declared_workload.non_applicability is None
            or artifact.get("reason_code")
            != "workload_declared_not_applicable"
        ):
            raise PipelineLoadArtifactError(
                "not-applicable artifact lacks its workload declaration"
            )
        return
    if not trials and state != "failed":
        raise PipelineLoadArtifactError(
            "executed artifact must contain trials"
        )
    trial_fields = frozenset(
        {
            "trial_index",
            "phase",
            "target_rate",
            "duration_seconds",
            "started_at",
            "completed_at",
            "stable",
            "failures",
            "event_ledger_sha256",
            "receipt_ledger_sha256",
            "cursor_ledger_sha256",
            "operation_counts",
            "replica_processed_items",
            "metrics",
        }
    )
    for index, raw_trial in enumerate(trials, start=1):
        trial = _mapping(raw_trial, f"trials[{index - 1}]")
        _exact_fields(trial, trial_fields, f"trials[{index - 1}]")
        if trial.get("trial_index") != index:
            raise PipelineLoadArtifactError("trial indexes are not contiguous")
        if trial.get("phase") not in {
            "warmup",
            "step",
            "binary_search",
            "validation",
            "soak",
        }:
            raise PipelineLoadArtifactError("trial phase is invalid")
        trial_started = _parse_time(
            trial.get("started_at"),
            f"trials[{index - 1}].started_at",
        )
        trial_completed = _parse_time(
            trial.get("completed_at"),
            f"trials[{index - 1}].completed_at",
        )
        if (
            trial_completed < trial_started
            or trial_started < started
            or trial_completed > completed
        ):
            raise PipelineLoadArtifactError(
                "trial timestamp falls outside artifact window"
            )
        target_rate = trial.get("target_rate")
        duration = trial.get("duration_seconds")
        if (
            isinstance(target_rate, bool)
            or not isinstance(target_rate, (int, float))
            or not math.isfinite(float(target_rate))
            or target_rate <= 0
            or isinstance(duration, bool)
            or not isinstance(duration, (int, float))
            or not math.isfinite(float(duration))
            or duration <= 0
        ):
            raise PipelineLoadArtifactError(
                "trial rate/duration must be finite and positive"
            )
        expected_duration_key = {
            "warmup": "warmup_seconds",
            "step": "step_seconds",
            "binary_search": "step_seconds",
            "validation": "validation_seconds",
            "soak": "soak_seconds",
        }[trial["phase"]]
        if not math.isclose(
            float(duration),
            float(timing[expected_duration_key]),
            rel_tol=1e-9,
        ):
            raise PipelineLoadArtifactError(
                "trial duration differs from declared phase timing"
            )
        if any(
            not isinstance(trial.get(name), str)
            or _SHA256_RE.fullmatch(trial[name]) is None
            for name in (
                "event_ledger_sha256",
                "receipt_ledger_sha256",
                "cursor_ledger_sha256",
            )
        ):
            raise PipelineLoadArtifactError("trial ledger digest is invalid")
        replica_processed_items = _mapping(
            trial.get("replica_processed_items"),
            "trial replica_processed_items",
        )
        if (
            len(replica_processed_items) != topology["replicas"]
            or any(
                not isinstance(replica_id, str)
                or not replica_id
                or isinstance(count, bool)
                or not isinstance(count, int)
                or count < 0
                for replica_id, count in replica_processed_items.items()
            )
        ):
            raise PipelineLoadArtifactError(
                "trial replica participation evidence is invalid"
            )
        stable = trial.get("stable")
        failures = trial.get("failures")
        if (
            not isinstance(stable, bool)
            or not isinstance(failures, list)
            or not all(isinstance(item, str) and item for item in failures)
            or (stable and failures)
            or (not stable and not failures)
        ):
            raise PipelineLoadArtifactError(
                "trial stability/failure summary is invalid"
            )
        metrics = _mapping(trial.get("metrics"), "trial metrics")
        _exact_fields(metrics, _TRIAL_METRIC_FIELDS, "trial metrics")
        if any(
            isinstance(metric, bool)
            or not isinstance(metric, (int, float))
            or not math.isfinite(float(metric))
            or metric < 0
            for metric in metrics.values()
        ):
            raise PipelineLoadArtifactError(
                "trial metrics must be finite and non-negative"
            )
        operation_counts = _mapping(
            trial.get("operation_counts"),
            "trial operation_counts",
        )
        (
            expected_operation_counts,
            expected_data_operations,
            expected_control_operations,
        ) = _scheduled_operation_counts(
            declared_workload,
            target_rate=float(target_rate),
            duration_seconds=float(duration),
        )
        if (
            set(operation_counts) != set(operation_ids)
            or any(
                isinstance(count, bool)
                or not isinstance(count, int)
                or count < 0
                for count in operation_counts.values()
            )
            or sum(operation_counts.values()) != metrics["offered_items"]
            or dict(operation_counts) != expected_operation_counts
            or metrics["scheduled_data_operations"]
            != expected_data_operations
            or metrics["scheduled_control_operations"]
            != expected_control_operations
        ):
            raise PipelineLoadArtifactError(
                "trial operation counts differ from the executable schedule"
            )
        wall_elapsed = float(metrics["wall_elapsed_seconds"])
        scheduled_elapsed = float(metrics["scheduled_elapsed_seconds"])
        if (
            wall_elapsed <= 0
            or scheduled_elapsed <= 0
            or not math.isclose(
                float(metrics["scheduled_duration_ratio"]),
                scheduled_elapsed / float(duration),
                rel_tol=1e-9,
            )
            or not math.isclose(
                float(metrics["end_to_end_duration_ratio"]),
                wall_elapsed / float(duration),
                rel_tol=1e-9,
            )
        ):
            raise PipelineLoadArtifactError(
                "trial duration ratios differ from measured elapsed time"
            )
        if not math.isclose(
            float(metrics["offered_rate_achievement_ratio"]),
            (
                float(metrics["data_operations_per_second"])
                if expected_data_operations
                else float(target_rate)
            )
            / float(target_rate),
            rel_tol=1e-9,
        ):
            raise PipelineLoadArtifactError(
                "trial offered-rate achievement ratio differs"
            )
        rate_checks = (
            ("offered_items_per_second", "offered_items"),
            (
                "data_operations_per_second",
                "scheduled_data_operations",
            ),
            ("raw_records_per_second", "raw_kafka_records"),
            ("normalized_records_per_second", "normalized_records"),
            ("observations_per_second", "observations"),
            ("t1_triggers_per_second", "t1_triggers"),
            ("bytes_per_second", "raw_bytes"),
            ("quota_units_per_second", "quota_units"),
        )
        if any(
            not math.isclose(
                float(metrics[rate_name]),
                float(metrics[count_name]) / wall_elapsed,
                rel_tol=1e-9,
                abs_tol=1e-12,
            )
            for rate_name, count_name in rate_checks
        ):
            raise PipelineLoadArtifactError(
                "trial throughput differs from measured wall elapsed time"
            )
        if stable:
            equality_checks = (
                ("accepted_items", "offered_items"),
                ("raw_s3_objects", "expected_raw_s3_objects"),
                ("raw_kafka_records", "expected_raw_kafka_records"),
                ("normalized_records", "expected_normalized_records"),
                ("observations", "expected_observations"),
                ("t1_triggers", "expected_observations"),
                ("unique_observation_identities", "observations"),
                ("unique_t1_observation_ids", "observations"),
                (
                    "cursor_proven_operations",
                    "cursor_required_operations",
                ),
            )
            if any(metrics[left] != metrics[right] for left, right in equality_checks):
                raise PipelineLoadArtifactError(
                    "stable trial has inconsistent layer counts"
                )
            if any(
                metrics[name] != 0
                for name in (
                    "missing_records",
                    "unexpected_duplicates",
                    "cross_tenant_leaks",
                    "cursor_consistency_errors",
                    "cooldown_violations",
                    "failed_requests",
                    "dlq_entries",
                    "raw_kafka_lag",
                    "normalized_kafka_lag",
                    "observation_to_t1_lag",
                )
            ):
                raise PipelineLoadArtifactError(
                    "stable trial retains an invariant failure"
                )
            if (
                (
                    expected_data_operations > 0
                    and (
                        metrics["participating_replica_count"]
                        != topology["replicas"]
                        or any(
                            count <= 0
                            for count in replica_processed_items.values()
                        )
                    )
                )
                or sum(replica_processed_items.values())
                != metrics["raw_kafka_records"]
                or metrics["replica_count"] != topology["replicas"]
                or (
                    expected_data_operations > 0
                    and (
                        metrics["tenant_count"] != topology["tenants"]
                        or metrics["installation_count"]
                        != (
                            topology["tenants"]
                            * topology["installations_per_tenant"]
                        )
                    )
                )
                or metrics["scheduled_duration_ratio"] < 0.99
                or metrics["end_to_end_duration_ratio"]
                < metrics["scheduled_duration_ratio"]
                or metrics["offered_rate_achievement_ratio"] < 0.9
            ):
                raise PipelineLoadArtifactError(
                    "stable trial lacks duration/topology participation proof"
                )
    maximum = artifact.get("maximum_stable_rate")
    if state in {"passed", "diagnostic"}:
        if not isinstance(maximum, (int, float)) or maximum <= 0:
            raise PipelineLoadArtifactError(
                "successful artifact has no maximum stable rate"
            )
        validations = [
            trial
            for trial in trials
            if isinstance(trial, Mapping)
            and trial.get("phase") == "validation"
        ]
        if (
            len(validations) != 1
            or validations[0].get("stable") is not True
            or not math.isclose(
                float(validations[0]["target_rate"]),
                float(maximum),
                rel_tol=1e-9,
            )
        ):
            raise PipelineLoadArtifactError(
                "maximum stable rate differs from validation"
            )
        if configuration.get("include_soak") is True:
            soaks = [
                trial
                for trial in trials
                if isinstance(trial, Mapping)
                and trial.get("phase") == "soak"
            ]
            if len(soaks) != 1 or soaks[0].get("stable") is not True:
                raise PipelineLoadArtifactError(
                    "successful artifact lacks a stable soak"
                )
    quota = artifact.get("quota")
    boundary = artifact.get("boundary")
    if boundary is None or artifact.get("infrastructure") is None:
        raise PipelineLoadArtifactError(
            "executed artifact lacks boundary/infrastructure"
        )
    boundary_map = _mapping(boundary, "boundary")
    _exact_fields(
        boundary_map,
        frozenset(
            {
                "source_id",
                "evidence_class",
                "binding_sha256",
                "dedicated_namespace",
                "workload_kind",
                "operation_contract_sha256",
                "raw_topic",
                "normalized_topic",
                "observation_relation",
                "t1_relation",
                "quota_mode",
                "topology",
                "loopback_only",
                "s3_raw_evidence_verified",
            }
        ),
        "boundary",
    )
    infrastructure = _mapping(
        artifact.get("infrastructure"),
        "infrastructure",
    )
    if (
        boundary_map.get("evidence_class")
        not in {"exact_pipeline", "test_double"}
        or boundary_map.get("source_id") != source_id
        or boundary_map.get("binding_sha256")
        != infrastructure.get("binding_sha256")
        or boundary_map.get("workload_kind") != workload_kind
        or boundary_map.get("operation_contract_sha256")
        != expected_workload_sha
        or boundary_map.get("raw_topic") != f"ingestion.raw.{source_id}"
        or boundary_map.get("normalized_topic")
        != f"ingestion.normalized.{source_id}"
        or boundary_map.get("observation_relation") != "observations"
        or boundary_map.get("t1_relation") != "think_trigger_queue"
        or boundary_map.get("topology") != topology
        or boundary_map.get("loopback_only") is not True
        or boundary_map.get("s3_raw_evidence_verified") is not True
    ):
        raise PipelineLoadArtifactError(
            "boundary identity differs from workload/infrastructure"
        )
    if state == "passed" and boundary_map.get(
        "evidence_class"
    ) != "exact_pipeline":
        raise PipelineLoadArtifactError(
            "passing artifact requires an exact-pipeline adapter"
        )
    if mode == "provider_safe":
        if quota is None or boundary_map.get("quota_mode") != "strict":
            raise PipelineLoadArtifactError(
                "provider-safe artifact lacks strict verified quota"
            )
        if state in {"passed", "diagnostic"}:
            quota_map = _mapping(quota, "quota")
            _exact_fields(
                quota_map,
                frozenset(
                    {
                        "source_id",
                        "declaration_sha256",
                        "modeled_maximum_rate",
                        "constraints",
                    }
                ),
                "quota",
            )
            constraints = quota_map.get("constraints")
            if (
                quota_map.get("source_id") != source_id
                or not isinstance(constraints, list)
                or not constraints
                or not all(isinstance(item, Mapping) for item in constraints)
            ):
                raise PipelineLoadArtifactError(
                    "provider-safe quota declaration is invalid"
                )
            expected_quota_sha = _sha256(
                _canonical_bytes(
                    {
                        "source_id": source_id,
                        "constraints": constraints,
                    }
                )
            )
            if quota_map.get("declaration_sha256") != expected_quota_sha:
                raise PipelineLoadArtifactError(
                    "provider-safe quota declaration hash differs"
                )
            modeled = quota_map.get("modeled_maximum_rate")
            if (
                not isinstance(modeled, (int, float))
                or not isinstance(maximum, (int, float))
                or maximum < modeled * 0.9
            ):
                raise PipelineLoadArtifactError(
                    "provider-safe rate is below 90% of modeled quota"
                )
    elif quota is not None or boundary_map.get("quota_mode") != "disabled":
        raise PipelineLoadArtifactError(
            "ceiling artifact must disable provider quota"
        )
    elif state in {"passed", "diagnostic"} and not any(
        isinstance(trial, Mapping)
        and trial.get("phase") in {"step", "binary_search"}
        and trial.get("stable") is False
        for trial in trials
    ):
        raise PipelineLoadArtifactError(
            "ceiling artifact did not observe an unstable offered rate"
        )


def write_pipeline_load_artifact(path: Path, artifact: Mapping[str, object]) -> None:
    validate_pipeline_load_artifact(artifact)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(artifact))


__all__ = [
    "PIPELINE_LOAD_ARTIFACT_SCHEMA_VERSION",
    "DeclaredPipelineWorkload",
    "IsolatedPipelineInfrastructure",
    "LatencySummary",
    "OfferReceipt",
    "PipelineAdapterFactory",
    "PipelineBoundaryAdapter",
    "PipelineBoundaryProof",
    "PipelineLoadArtifactError",
    "PipelineLoadClock",
    "PipelineLoadError",
    "PipelineLoadMode",
    "PipelineLoadRunConfig",
    "PipelineLoadTiming",
    "PipelineLoadTopology",
    "PipelineSnapshot",
    "PipelineWorkloadKind",
    "QuotaConstraint",
    "ReceiptQuotaCharge",
    "SystemPipelineLoadClock",
    "TrialContext",
    "VerifiedQuotaConfiguration",
    "WorkItem",
    "resolve_isolated_pipeline_infrastructure",
    "run_pipeline_load",
    "validate_pipeline_load_artifact",
    "write_pipeline_load_artifact",
]
