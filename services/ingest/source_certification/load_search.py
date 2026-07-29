"""Artifact-producing maximum-stable-throughput search.

The controller owns the prescribed typed workload while an injected driver
owns traffic generation and measurement. Tests may use a virtual clock and
short durations, but the resulting artifact records that provenance and is not
promotion eligible. Release promotion requires the exact declared topology,
executable operation receipts, wall-clock durations, verified quota evidence,
Provider Lab calibration, and an end-to-end pipeline proof.
"""
from __future__ import annotations

import dataclasses
import json
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from services.ingest.source_certification.models import LoadSuite, SuiteKind
from services.ingest.synthetic.provider_lab.calibration import (
    LabCalibration,
    require_lab_calibration,
)


LOAD_ARTIFACT_SCHEMA_VERSION = "fyralis.source-load-envelope.v3"
LoadMode = Literal["provider_safe", "fyralis_ceiling"]
Phase = Literal["warmup", "step", "binary_search", "validation", "soak"]
ClockMode = Literal["wall", "virtual"]
_QUOTA_SCOPE_COMPONENTS = frozenset(
    {
        "app",
        "application",
        "global",
        "installation",
        "method",
        "realm",
        "region",
        "route",
        "tenant",
        "user",
        "workspace",
    }
)


class LoadPromotionError(RuntimeError):
    """A measured load artifact does not satisfy release-promotion rules."""


@dataclass(frozen=True, slots=True)
class LoadTopology:
    tenants: int
    installations_per_tenant: int
    replicas: int

    def __post_init__(self) -> None:
        for name in ("tenants", "installations_per_tenant", "replicas"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

    @classmethod
    def from_suite(cls, suite: LoadSuite) -> "LoadTopology":
        return cls(
            tenants=suite.tenants,
            installations_per_tenant=suite.installations_per_tenant,
            replicas=suite.replicas,
        )


@dataclass(frozen=True, slots=True)
class VerifiedQuotaEvidence:
    """Evidence label attached to an exact Provider Lab quota budget."""

    bucket: str
    scope: str
    capacity: float
    refill_per_second: float
    evidence_uri: str
    verified_at: datetime
    limit_id: str = "default"
    cost: float = 1.0

    def __post_init__(self) -> None:
        for name in ("bucket", "scope", "evidence_uri", "limit_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not self.evidence_uri.startswith("https://"):
            raise ValueError("evidence_uri must be an HTTPS URI")
        scope_components = self.scope.casefold().split("/")
        if (
            any(not component for component in scope_components)
            or len(scope_components) != len(set(scope_components))
            or not set(scope_components).issubset(_QUOTA_SCOPE_COMPONENTS)
        ):
            raise ValueError(
                "scope must be a slash-separated combination of supported "
                "quota dimensions: "
                + ", ".join(sorted(_QUOTA_SCOPE_COMPONENTS)),
            )
        if self.verified_at.tzinfo is None or self.verified_at.utcoffset() is None:
            raise ValueError("verified_at must be timezone-aware")
        for name in ("capacity", "refill_per_second", "cost"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) < 0
            ):
                raise ValueError(f"{name} must be a finite non-negative number")
        if self.capacity <= 0:
            raise ValueError("capacity must be greater than zero")
        if self.cost <= 0:
            raise ValueError("cost must be greater than zero")

    def as_dict(self) -> dict[str, object]:
        return {
            "bucket": self.bucket,
            "scope": self.scope,
            "limit_id": self.limit_id,
            "cost": float(self.cost),
            "capacity": float(self.capacity),
            "refill_per_second": float(self.refill_per_second),
            "evidence_uri": self.evidence_uri,
            "verified_at": self.verified_at.astimezone(timezone.utc).isoformat(),
        }


@dataclass(frozen=True, slots=True)
class LoadSearchConfig:
    initial_rate: float
    step_fraction: float = 0.25
    tolerance_fraction: float = 0.05
    warmup_seconds: int = 120
    step_seconds: int = 120
    validation_seconds: int = 900
    soak_seconds: int = 3600
    maximum_steps: int = 40

    def __post_init__(self) -> None:
        if self.initial_rate <= 0:
            raise ValueError("initial_rate must be > 0")
        if not 0 < self.step_fraction < 1:
            raise ValueError("step_fraction must be in (0, 1)")
        if not 0 < self.tolerance_fraction < 1:
            raise ValueError("tolerance_fraction must be in (0, 1)")
        for name in (
            "warmup_seconds",
            "step_seconds",
            "validation_seconds",
            "soak_seconds",
            "maximum_steps",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")

    @classmethod
    def from_suite(
        cls,
        suite: LoadSuite,
        *,
        initial_rate: float,
    ) -> "LoadSearchConfig":
        return cls(
            initial_rate=initial_rate,
            step_fraction=suite.step_percent / 100,
            tolerance_fraction=suite.search_tolerance_percent / 100,
            warmup_seconds=suite.warmup_seconds,
            step_seconds=suite.warmup_seconds,
            validation_seconds=suite.stable_seconds,
            soak_seconds=suite.weekly_soak_seconds,
        )


@dataclass(frozen=True, slots=True)
class LoadMeasurement:
    offered_rate: float
    duration_seconds: int
    stable: bool
    requests_per_second: float
    quota_units_per_second: float
    records_per_second: float
    bytes_per_second: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    kafka_lag: float
    observation_p99_latency_ms: float
    backlog_growth_per_second: float
    missing_records: int
    unexpected_duplicates: int
    cross_tenant_leaks: int
    cooldown_violations: int
    cursor_consistency_errors: int
    dlq_entries: int
    cpu_percent: float
    memory_bytes: int
    limiting_component: str
    retries: int = 0
    rate_limited_responses: int = 0
    hot_loops: int = 0
    wall_elapsed_seconds: float = 0.0
    request_count: int = 0
    response_bytes: int = 0
    operation_counts: tuple[tuple[str, int], ...] = ()
    status_counts: tuple[tuple[str, int], ...] = ()

    def __post_init__(self) -> None:
        if self.offered_rate <= 0:
            raise ValueError("offered_rate must be > 0")
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be > 0")
        if not self.limiting_component:
            raise ValueError("limiting_component must be non-empty")
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if field.name in {
                "stable",
                "limiting_component",
                "operation_counts",
                "status_counts",
            }:
                continue
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise ValueError(f"{field.name} must be a finite number")
        for name, values in (
            ("operation_counts", self.operation_counts),
            ("status_counts", self.status_counts),
        ):
            labels = [label for label, _count in values]
            if len(labels) != len(set(labels)):
                raise ValueError(f"{name} labels must be unique")
            if any(not label or count < 0 for label, count in values):
                raise ValueError(f"{name} entries must be non-empty/non-negative")


@dataclass(frozen=True, slots=True)
class LoadTrial:
    phase: Phase
    measurement: LoadMeasurement


@dataclass(frozen=True, slots=True)
class LoadEnvelope:
    mode: LoadMode
    maximum_stable_rate: float
    tolerance_fraction: float
    validation: LoadMeasurement
    soak: LoadMeasurement | None
    trials: tuple[LoadTrial, ...]


@dataclass(frozen=True, slots=True)
class LoadArtifact:
    source_id: str
    suite_kind: SuiteKind
    mode: LoadMode
    compatibility_operation_mix: tuple[str, ...]
    workload_declaration_sha256: str
    executable_operation_ids: tuple[str, ...]
    control_operation_ids: tuple[str, ...]
    contract_absence_operation_ids: tuple[str, ...]
    non_applicability_evidence_id: str | None
    required_provider_operation_labels: tuple[str, ...]
    topology: LoadTopology
    config: LoadSearchConfig
    clock_mode: ClockMode
    calibration: LabCalibration
    envelope: LoadEnvelope
    quota_evidence: tuple[VerifiedQuotaEvidence, ...]
    executable_operation_coverage_ratio: float
    pipeline_e2e_proven: bool
    started_at: datetime
    completed_at: datetime
    promotion_failures: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.source_id:
            raise ValueError("source_id must be non-empty")
        if self.suite_kind not in {"historical", "live", "combined"}:
            raise ValueError(f"invalid suite_kind {self.suite_kind!r}")
        if self.clock_mode not in {"wall", "virtual"}:
            raise ValueError(f"invalid clock_mode {self.clock_mode!r}")
        if len(self.workload_declaration_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.workload_declaration_sha256
        ):
            raise ValueError("workload_declaration_sha256 must be a SHA-256")
        for name in (
            "executable_operation_ids",
            "control_operation_ids",
            "contract_absence_operation_ids",
            "required_provider_operation_labels",
        ):
            values = getattr(self, name)
            if len(values) != len(set(values)) or any(
                not isinstance(value, str) or not value
                for value in values
            ):
                raise ValueError(f"{name} must contain unique non-empty IDs")
        if not self.executable_operation_ids:
            raise ValueError("an executable load artifact requires operation IDs")
        if self.non_applicability_evidence_id is not None:
            raise ValueError(
                "load-search artifacts cannot represent non-applicable suites"
            )
        if len(self.required_provider_operation_labels) != len(
            set(self.required_provider_operation_labels),
        ):
            raise ValueError("required_provider_operation_labels must be unique")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at cannot precede started_at")
        if not 0 <= self.executable_operation_coverage_ratio <= 1:
            raise ValueError(
                "executable_operation_coverage_ratio must be between 0 and 1",
            )

    @property
    def promotion_eligible(self) -> bool:
        return not self.promotion_failures

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": LOAD_ARTIFACT_SCHEMA_VERSION,
            "source_id": self.source_id,
            "suite_kind": self.suite_kind,
            "mode": self.mode,
            "workload": {
                "declaration_sha256": self.workload_declaration_sha256,
                "executable_operation_ids": list(self.executable_operation_ids),
                "control_operation_ids": list(self.control_operation_ids),
                "contract_absence_operation_ids": list(
                    self.contract_absence_operation_ids,
                ),
                "non_applicability_evidence_id": (
                    self.non_applicability_evidence_id
                ),
            },
            # Retained only for readers of v2 diagnostic files.  Active
            # scheduling, coverage, comparison, and promotion use the typed
            # workload fields above.
            "compatibility_operation_mix": list(
                self.compatibility_operation_mix,
            ),
            "operation_mix": list(self.compatibility_operation_mix),
            "required_provider_operation_labels": list(
                self.required_provider_operation_labels,
            ),
            "topology": dataclasses.asdict(self.topology),
            "config": dataclasses.asdict(self.config),
            "clock_mode": self.clock_mode,
            "calibration": dataclasses.asdict(self.calibration),
            "envelope": _envelope_dict(self.envelope),
            "quota_evidence": [
                evidence.as_dict() for evidence in self.quota_evidence
            ],
            "executable_operation_coverage_ratio": (
                self.executable_operation_coverage_ratio
            ),
            "pipeline_e2e_proven": self.pipeline_e2e_proven,
            "started_at": self.started_at.astimezone(timezone.utc).isoformat(),
            "completed_at": self.completed_at.astimezone(timezone.utc).isoformat(),
            "promotion_eligible": self.promotion_eligible,
            "promotion_failures": list(self.promotion_failures),
        }


@dataclass(frozen=True, slots=True)
class LoadEnvelopeComparison:
    suite_kind: SuiteKind
    provider_safe_rate: float
    fyralis_ceiling_rate: float
    headroom_ratio: float


LoadRunner = Callable[
    [float, int, Phase, LoadMode],
    Awaitable[LoadMeasurement],
]


def certification_stable(measurement: LoadMeasurement) -> bool:
    """Apply non-negotiable correctness and stability conditions."""

    return (
        measurement.stable
        and measurement.backlog_growth_per_second <= 0
        and measurement.missing_records == 0
        and measurement.unexpected_duplicates == 0
        and measurement.cross_tenant_leaks == 0
        and measurement.cooldown_violations == 0
        and measurement.cursor_consistency_errors == 0
        and measurement.dlq_entries == 0
        and measurement.hot_loops == 0
    )


async def find_maximum_stable_rate(
    run: LoadRunner,
    *,
    mode: LoadMode,
    config: LoadSearchConfig,
    lab_calibration: LabCalibration,
    include_soak: bool,
) -> LoadEnvelope:
    """Warm up, step by 25%, bracket, binary search, and validate."""

    require_lab_calibration(lab_calibration)
    trials: list[LoadTrial] = []

    async def measure(rate: float, seconds: int, phase: Phase) -> LoadMeasurement:
        result = await run(rate, seconds, phase, mode)
        if not math.isclose(
            result.offered_rate,
            rate,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ) or result.duration_seconds != seconds:
            raise ValueError(
                "load runner returned measurement for a different rate/duration"
            )
        trials.append(LoadTrial(phase=phase, measurement=result))
        return result

    warmup = await measure(
        config.initial_rate,
        config.warmup_seconds,
        "warmup",
    )
    if not certification_stable(warmup):
        raise RuntimeError("initial warmup rate is not stable")

    low = config.initial_rate
    high: float | None = None
    for _ in range(config.maximum_steps):
        candidate = low * (1 + config.step_fraction)
        stepped = await measure(candidate, config.step_seconds, "step")
        if certification_stable(stepped):
            low = candidate
            continue
        high = candidate
        break
    if high is None:
        raise RuntimeError(
            "load search never found an unstable upper bound; "
            "increase the offered-load limit"
        )

    for _ in range(config.maximum_steps):
        if (high - low) / low <= config.tolerance_fraction:
            break
        candidate = (low + high) / 2
        searched = await measure(candidate, config.step_seconds, "binary_search")
        if certification_stable(searched):
            low = candidate
        else:
            high = candidate
    else:
        raise RuntimeError("binary search exceeded maximum_steps")

    validation = await measure(low, config.validation_seconds, "validation")
    if not certification_stable(validation):
        raise RuntimeError("maximum candidate failed stable validation")

    soak: LoadMeasurement | None = None
    if include_soak:
        soak = await measure(low, config.soak_seconds, "soak")
        if not certification_stable(soak):
            raise RuntimeError("maximum candidate failed weekly soak")

    return LoadEnvelope(
        mode=mode,
        maximum_stable_rate=low,
        tolerance_fraction=(high - low) / low,
        validation=validation,
        soak=soak,
        trials=tuple(trials),
    )


def compare_load_envelopes(
    provider_safe: LoadArtifact,
    fyralis_ceiling: LoadArtifact,
) -> LoadEnvelopeComparison:
    """Require a ceiling at or above the provider-safe stable envelope."""

    if provider_safe.mode != "provider_safe":
        raise ValueError("provider_safe artifact has the wrong mode")
    if fyralis_ceiling.mode != "fyralis_ceiling":
        raise ValueError("fyralis_ceiling artifact has the wrong mode")
    if (
        provider_safe.source_id != fyralis_ceiling.source_id
        or provider_safe.suite_kind != fyralis_ceiling.suite_kind
        or (
            provider_safe.workload_declaration_sha256
            != fyralis_ceiling.workload_declaration_sha256
        )
        or (
            provider_safe.executable_operation_ids
            != fyralis_ceiling.executable_operation_ids
        )
        or (
            provider_safe.control_operation_ids
            != fyralis_ceiling.control_operation_ids
        )
        or (
            provider_safe.contract_absence_operation_ids
            != fyralis_ceiling.contract_absence_operation_ids
        )
        or (
            provider_safe.required_provider_operation_labels
            != fyralis_ceiling.required_provider_operation_labels
        )
        or provider_safe.topology != fyralis_ceiling.topology
    ):
        raise ValueError("load artifacts do not describe the same workload")
    provider_rate = provider_safe.envelope.maximum_stable_rate
    ceiling_rate = fyralis_ceiling.envelope.maximum_stable_rate
    if ceiling_rate < provider_rate:
        raise LoadPromotionError(
            "Fyralis ceiling is below the provider-safe stable rate"
        )
    return LoadEnvelopeComparison(
        suite_kind=provider_safe.suite_kind,
        provider_safe_rate=provider_rate,
        fyralis_ceiling_rate=ceiling_rate,
        headroom_ratio=ceiling_rate / provider_rate,
    )


def promotion_failures(
    *,
    suite: LoadSuite,
    mode: LoadMode,
    workload_declaration_sha256: str,
    topology: LoadTopology,
    config: LoadSearchConfig,
    clock_mode: ClockMode,
    calibration: LabCalibration,
    envelope: LoadEnvelope,
    quota_evidence: tuple[VerifiedQuotaEvidence, ...],
    executable_operation_coverage_ratio: float,
    pipeline_e2e_proven: bool,
) -> tuple[str, ...]:
    """Return every reason this measurement cannot be release evidence."""

    failures: list[str] = []
    declared_topology = LoadTopology.from_suite(suite)
    if workload_declaration_sha256 != suite.execution_workload_sha256:
        failures.append("typed workload declaration differs from the source contract")
    if topology != declared_topology:
        failures.append("topology differs from the source declaration")
    if not math.isclose(
        config.step_fraction,
        suite.step_percent / 100,
        rel_tol=1e-12,
        abs_tol=1e-12,
    ):
        failures.append("step fraction differs from the declared 25% search")
    if config.tolerance_fraction > suite.search_tolerance_percent / 100:
        failures.append("configured search tolerance exceeds the declaration")
    if envelope.tolerance_fraction > suite.search_tolerance_percent / 100:
        failures.append("measured search tolerance exceeds the declaration")
    if config.warmup_seconds < suite.warmup_seconds:
        failures.append("warmup duration is below the declaration")
    if config.step_seconds < suite.warmup_seconds:
        failures.append("offered-load step duration is below the declaration")
    if config.validation_seconds < suite.stable_seconds:
        failures.append("stable validation duration is below the declaration")
    if envelope.soak is None:
        failures.append("weekly soak was not executed")
    elif config.soak_seconds < suite.weekly_soak_seconds:
        failures.append("weekly soak duration is below the declaration")
    if clock_mode != "wall":
        failures.append("virtual-clock load cannot be used for promotion")
    if not calibration.passed:
        failures.append("Provider Lab calibration did not pass")
    if calibration.elapsed_seconds < 29.7:
        failures.append(
            "Provider Lab calibration did not run for the required wall-clock "
            "duration",
        )
    if executable_operation_coverage_ratio < 1:
        failures.append("declared executable operations were not fully exercised")
    if not pipeline_e2e_proven:
        failures.append("raw-to-Observation-to-T1 pipeline proof is missing")
    if mode == "provider_safe" and not quota_evidence:
        failures.append("verified provider quota evidence is missing")

    validation_ratio = (
        envelope.validation.wall_elapsed_seconds
        / envelope.validation.duration_seconds
    )
    if validation_ratio < 0.99:
        failures.append("stable validation did not run for wall-clock duration")
    if envelope.soak is not None:
        soak_ratio = (
            envelope.soak.wall_elapsed_seconds
            / envelope.soak.duration_seconds
        )
        if soak_ratio < 0.99:
            failures.append("weekly soak did not run for wall-clock duration")
    warmup = next(
        (
            trial.measurement
            for trial in envelope.trials
            if trial.phase == "warmup"
        ),
        None,
    )
    if (
        warmup is None
        or warmup.wall_elapsed_seconds / warmup.duration_seconds < 0.99
    ):
        failures.append("warmup did not run for wall-clock duration")
    if any(
        trial.measurement.wall_elapsed_seconds
        / trial.measurement.duration_seconds
        < 0.99
        for trial in envelope.trials
    ):
        failures.append(
            "one or more offered-load search trials did not run for wall-clock "
            "duration",
        )
    return tuple(failures)


async def run_artifact_load_search(
    run: LoadRunner,
    *,
    source_id: str,
    suite: LoadSuite,
    mode: LoadMode,
    topology: LoadTopology,
    config: LoadSearchConfig,
    clock_mode: ClockMode,
    lab_calibration: LabCalibration,
    include_soak: bool,
    quota_evidence: tuple[VerifiedQuotaEvidence, ...] = (),
    verified_executable_operation_coverage_ratio: float = 0.0,
    required_provider_operation_labels: tuple[str, ...] = (),
    pipeline_e2e_proven: bool = False,
    artifact_path: Path | None = None,
    require_promotion: bool = False,
    now: Callable[[], datetime] | None = None,
) -> LoadArtifact:
    """Run one envelope, assess promotion, and optionally write canonical JSON."""

    if suite.non_applicability is not None:
        raise ValueError(
            "not-applicable suites must be handled by the typed pipeline "
            "load runner",
        )
    clock = now or (lambda: datetime.now(timezone.utc))
    started_at = clock().astimezone(timezone.utc)
    envelope = await find_maximum_stable_rate(
        run,
        mode=mode,
        config=config,
        lab_calibration=lab_calibration,
        include_soak=include_soak,
    )
    completed_at = clock().astimezone(timezone.utc)
    declared_data_operation_ids = {
        operation.operation_id for operation in suite.data_operations
    }
    declared_control_operation_ids = {
        operation.operation_id for operation in suite.control_operations
    }
    observed_data_operation_ids = {
        label.removeprefix("executed_data_operation:")
        for trial in envelope.trials
        for label, count in trial.measurement.operation_counts
        if label.startswith("executed_data_operation:") and count > 0
    }
    observed_control_operation_ids = {
        label.removeprefix("executed_control_operation:")
        for trial in envelope.trials
        for label, count in trial.measurement.operation_counts
        if label.startswith("executed_control_operation:") and count > 0
    }
    observed_data_coverage = (
        len(observed_data_operation_ids & declared_data_operation_ids)
        / len(declared_data_operation_ids)
    )
    observed_control_coverage = (
        len(observed_control_operation_ids & declared_control_operation_ids)
        / len(declared_control_operation_ids)
        if declared_control_operation_ids
        else 1.0
    )
    observed_operation_labels = {
        label
        for trial in envelope.trials
        for label, count in trial.measurement.operation_counts
        if count > 0
    }
    observed_required_coverage = (
        len(observed_operation_labels & set(required_provider_operation_labels))
        / len(required_provider_operation_labels)
        if required_provider_operation_labels
        else 1.0
    )
    effective_operation_coverage = min(
        verified_executable_operation_coverage_ratio,
        observed_data_coverage,
        observed_control_coverage,
        observed_required_coverage,
    )
    failures = promotion_failures(
        suite=suite,
        mode=mode,
        workload_declaration_sha256=suite.execution_workload_sha256,
        topology=topology,
        config=config,
        clock_mode=clock_mode,
        calibration=lab_calibration,
        envelope=envelope,
        quota_evidence=quota_evidence,
        executable_operation_coverage_ratio=effective_operation_coverage,
        pipeline_e2e_proven=pipeline_e2e_proven,
    )
    artifact = LoadArtifact(
        source_id=source_id,
        suite_kind=suite.kind,
        mode=mode,
        compatibility_operation_mix=suite.operation_mix,
        workload_declaration_sha256=suite.execution_workload_sha256,
        executable_operation_ids=tuple(
            operation.operation_id
            for operation in suite.executable_operations
        ),
        control_operation_ids=tuple(
            operation.operation_id for operation in suite.control_operations
        ),
        contract_absence_operation_ids=tuple(
            assertion.operation_id
            for assertion in suite.contract_absence_assertions
        ),
        non_applicability_evidence_id=None,
        required_provider_operation_labels=required_provider_operation_labels,
        topology=topology,
        config=config,
        clock_mode=clock_mode,
        calibration=lab_calibration,
        envelope=envelope,
        quota_evidence=quota_evidence,
        executable_operation_coverage_ratio=effective_operation_coverage,
        pipeline_e2e_proven=pipeline_e2e_proven,
        started_at=started_at,
        completed_at=completed_at,
        promotion_failures=failures,
    )
    if artifact_path is not None:
        write_load_artifact(artifact_path, artifact)
    if require_promotion and failures:
        raise LoadPromotionError("; ".join(failures))
    return artifact


def write_load_artifact(path: Path, artifact: LoadArtifact) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            artifact.as_dict(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _measurement_dict(measurement: LoadMeasurement) -> dict[str, object]:
    payload = dataclasses.asdict(measurement)
    payload["operation_counts"] = dict(measurement.operation_counts)
    payload["status_counts"] = dict(measurement.status_counts)
    return payload


def _envelope_dict(envelope: LoadEnvelope) -> dict[str, object]:
    return {
        "mode": envelope.mode,
        "maximum_stable_rate": envelope.maximum_stable_rate,
        "tolerance_fraction": envelope.tolerance_fraction,
        "validation": _measurement_dict(envelope.validation),
        "soak": (
            _measurement_dict(envelope.soak)
            if envelope.soak is not None
            else None
        ),
        "trials": [
            {
                "phase": trial.phase,
                "measurement": _measurement_dict(trial.measurement),
            }
            for trial in envelope.trials
        ],
    }


__all__ = [
    "LOAD_ARTIFACT_SCHEMA_VERSION",
    "ClockMode",
    "LoadArtifact",
    "LoadEnvelope",
    "LoadEnvelopeComparison",
    "LoadMeasurement",
    "LoadMode",
    "LoadPromotionError",
    "LoadRunner",
    "LoadSearchConfig",
    "LoadTopology",
    "LoadTrial",
    "Phase",
    "VerifiedQuotaEvidence",
    "certification_stable",
    "compare_load_envelopes",
    "find_maximum_stable_rate",
    "promotion_failures",
    "run_artifact_load_search",
    "write_load_artifact",
]
