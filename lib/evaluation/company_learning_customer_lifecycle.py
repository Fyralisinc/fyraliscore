"""Typed continuous evaluation for resource-backed customer identity lifecycle."""

from __future__ import annotations

import json
import unicodedata
from collections import Counter
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_population import (
    IntervalEstimate,
    _wilson_estimate,
)


CUSTOMER_LIFECYCLE_SCENARIO_ID = "CUSTOMER-IDENTITY-LIFECYCLE-POPULATION"


class _LifecycleModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class CustomerRef(_LifecycleModel):
    type: Literal["customer"] = "customer"
    id: str = Field(min_length=1)


class CustomerLifecycleCase(_LifecycleModel):
    case_id: str = Field(min_length=1)
    case_version: Literal["v1"] = "v1"
    initial_identity: str = Field(min_length=1)
    renamed_identity: str = Field(min_length=1)
    reuse_initial_identity: bool

    @model_validator(mode="after")
    def distinct_names(self) -> Self:
        if _normalized_name(self.initial_identity) == _normalized_name(
            self.renamed_identity
        ):
            raise ValueError("customer lifecycle names must be distinct")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class CustomerLifecyclePopulation(_LifecycleModel):
    schema_version: Literal["company-learning-customer-lifecycle-population-v1"] = (
        "company-learning-customer-lifecycle-population-v1"
    )
    scenario_id: Literal["CUSTOMER-IDENTITY-LIFECYCLE-POPULATION"] = (
        CUSTOMER_LIFECYCLE_SCENARIO_ID
    )
    population_definition_version: Literal["customer-lifecycle-population-v1"] = (
        "customer-lifecycle-population-v1"
    )
    cases: tuple[CustomerLifecycleCase, ...]

    @model_validator(mode="after")
    def sealed_registry(self) -> Self:
        if len(self.cases) != 8:
            raise ValueError("customer lifecycle registry must contain 8 cases")
        if len({case.case_id for case in self.cases}) != len(self.cases):
            raise ValueError("customer lifecycle case ids must be unique")
        if len({case.digest for case in self.cases}) != len(self.cases):
            raise ValueError("customer lifecycle cases must be distinct")
        if sum(case.reuse_initial_identity for case in self.cases) != 4:
            raise ValueError(
                "customer lifecycle registry must contain 4 name-reuse cases"
            )
        names = [
            _normalized_name(name)
            for case in self.cases
            for name in (case.initial_identity, case.renamed_identity)
        ]
        if len(names) != len(set(names)):
            raise ValueError("sealed customer lifecycle names must be unique")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ResolutionProbeCategory(StrEnum):
    VALID_TIME = "valid_time"
    STALE_ALIAS_REJECTION = "stale_alias_rejection"
    CURRENT_ALIAS_SAFETY = "current_alias_safety"
    HISTORICAL_NAME_REUSE = "historical_name_reuse"
    ARCHIVE_REJECTION = "archive_rejection"
    TENANT_ISOLATION = "tenant_isolation"


class ResolutionProbeRole(StrEnum):
    PRE_RENAME_OLD_NAME = "pre_rename_old_name"
    POST_RENAME_STALE_OLD_NAME = "post_rename_stale_old_name"
    CURRENT_RENAMED_NAME = "current_renamed_name"
    PRE_ARCHIVE_DELAYED_RENAMED_NAME = "pre_archive_delayed_renamed_name"
    POST_ARCHIVE_REJECTION = "post_archive_rejection"
    TENANT_ISOLATION = "tenant_isolation"
    HISTORICAL_REUSED_OLD_NAME = "historical_reused_old_name"
    CURRENT_REUSED_OLD_NAME = "current_reused_old_name"


_REQUIRED_PROBE_CATEGORIES = {
    ResolutionProbeRole.PRE_RENAME_OLD_NAME: (ResolutionProbeCategory.VALID_TIME,),
    ResolutionProbeRole.POST_RENAME_STALE_OLD_NAME: (
        ResolutionProbeCategory.VALID_TIME,
        ResolutionProbeCategory.STALE_ALIAS_REJECTION,
    ),
    ResolutionProbeRole.CURRENT_RENAMED_NAME: (
        ResolutionProbeCategory.CURRENT_ALIAS_SAFETY,
    ),
    ResolutionProbeRole.PRE_ARCHIVE_DELAYED_RENAMED_NAME: (
        ResolutionProbeCategory.VALID_TIME,
        ResolutionProbeCategory.CURRENT_ALIAS_SAFETY,
    ),
    ResolutionProbeRole.POST_ARCHIVE_REJECTION: (
        ResolutionProbeCategory.VALID_TIME,
        ResolutionProbeCategory.ARCHIVE_REJECTION,
    ),
    ResolutionProbeRole.TENANT_ISOLATION: (ResolutionProbeCategory.TENANT_ISOLATION,),
    ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME: (
        ResolutionProbeCategory.VALID_TIME,
        ResolutionProbeCategory.HISTORICAL_NAME_REUSE,
    ),
    ResolutionProbeRole.CURRENT_REUSED_OLD_NAME: (
        ResolutionProbeCategory.CURRENT_ALIAS_SAFETY,
        ResolutionProbeCategory.HISTORICAL_NAME_REUSE,
    ),
}


class CustomerResolutionProbe(_LifecycleModel):
    probe_id: str = Field(min_length=1)
    role: ResolutionProbeRole
    phrase: str = Field(min_length=1)
    as_of: datetime | None = None
    categories: tuple[ResolutionProbeCategory, ...] = Field(min_length=1)
    expected_ref: CustomerRef | None
    observed_ref: CustomerRef | None

    @model_validator(mode="after")
    def coherent_probe(self) -> Self:
        if len(self.categories) != len(set(self.categories)):
            raise ValueError("resolution probe categories must be unique")
        if self.categories != _REQUIRED_PROBE_CATEGORIES[self.role]:
            raise ValueError(
                "resolution probe categories must match the sealed probe role"
            )
        if self.as_of is not None and self.as_of.utcoffset() is None:
            raise ValueError("resolution probe as_of must be timezone-aware")
        return self

    @property
    def correct(self) -> bool:
        return self.expected_ref == self.observed_ref


class CustomerAliasIntervalEvidence(_LifecycleModel):
    phrase: str = Field(min_length=1)
    resolved_ref: CustomerRef
    valid_from: datetime
    valid_until: datetime | None = None
    validity_reason: str | None = None

    @model_validator(mode="after")
    def valid_interval(self) -> Self:
        if self.valid_from.utcoffset() is None:
            raise ValueError("alias valid_from must be timezone-aware")
        if self.valid_until is not None:
            if self.valid_until.utcoffset() is None:
                raise ValueError("alias valid_until must be timezone-aware")
            if self.valid_until <= self.valid_from:
                raise ValueError("alias interval must have positive duration")
        return self


class CustomerLifecycleObservation(_LifecycleModel):
    case_id: str = Field(min_length=1)
    execution_status: Literal["observed", "unsupported"] = "observed"
    unsupported_reason: str | None = None
    canonical_ref_before: CustomerRef | None = None
    canonical_ref_after_rename: CustomerRef | None = None
    canonical_ref_after_archive: CustomerRef | None = None
    resolution_probes: tuple[CustomerResolutionProbe, ...] = ()
    alias_intervals: tuple[CustomerAliasIntervalEvidence, ...] = ()
    old_observation_before: tuple[CustomerRef, ...] | None = None
    old_observation_after: tuple[CustomerRef, ...] | None = None
    old_model_before: tuple[CustomerRef, ...] | None = None
    old_model_after: tuple[CustomerRef, ...] | None = None
    rename_replay_alias_count_before: int | None = Field(default=None, ge=0)
    rename_replay_alias_count_after: int | None = Field(default=None, ge=0)
    rename_replay_event_count_before: int | None = Field(default=None, ge=0)
    rename_replay_event_count_after: int | None = Field(default=None, ge=0)
    archive_replay_alias_count_before: int | None = Field(default=None, ge=0)
    archive_replay_alias_count_after: int | None = Field(default=None, ge=0)
    archive_replay_event_count_before: int | None = Field(default=None, ge=0)
    archive_replay_event_count_after: int | None = Field(default=None, ge=0)
    post_archive_rename_rejected: bool | None = None
    artifact_refs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def execution_is_complete(self) -> Self:
        measurements = (
            self.canonical_ref_before,
            self.canonical_ref_after_rename,
            self.canonical_ref_after_archive,
            self.old_observation_before,
            self.old_observation_after,
            self.old_model_before,
            self.old_model_after,
            self.rename_replay_alias_count_before,
            self.rename_replay_alias_count_after,
            self.rename_replay_event_count_before,
            self.rename_replay_event_count_after,
            self.archive_replay_alias_count_before,
            self.archive_replay_alias_count_after,
            self.archive_replay_event_count_before,
            self.archive_replay_event_count_after,
            self.post_archive_rename_rejected,
        )
        if self.execution_status == "unsupported":
            if (
                not self.unsupported_reason
                or any(value is not None for value in measurements)
                or self.resolution_probes
                or self.alias_intervals
                or self.artifact_refs
            ):
                raise ValueError(
                    "unsupported lifecycle cases require one reason and no evidence"
                )
            return self
        if self.unsupported_reason or any(value is None for value in measurements):
            raise ValueError("observed lifecycle cases require complete measurements")
        if not self.resolution_probes or not self.alias_intervals:
            raise ValueError(
                "observed lifecycle cases require probes and alias intervals"
            )
        roles = [probe.role for probe in self.resolution_probes]
        if len(roles) != len(set(roles)):
            raise ValueError("observed lifecycle probe roles must be unique")
        base_roles = {
            ResolutionProbeRole.PRE_RENAME_OLD_NAME,
            ResolutionProbeRole.POST_RENAME_STALE_OLD_NAME,
            ResolutionProbeRole.CURRENT_RENAMED_NAME,
            ResolutionProbeRole.PRE_ARCHIVE_DELAYED_RENAMED_NAME,
            ResolutionProbeRole.POST_ARCHIVE_REJECTION,
            ResolutionProbeRole.TENANT_ISOLATION,
        }
        reuse_roles = {
            ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME,
            ResolutionProbeRole.CURRENT_REUSED_OLD_NAME,
        }
        if set(roles) not in (base_roles, base_roles | reuse_roles):
            raise ValueError(
                "observed lifecycle probes must exactly match a sealed role set"
            )
        if not self.artifact_refs:
            raise ValueError("observed lifecycle cases require artifact refs")
        return self

    @property
    def rename_continuity(self) -> bool:
        return (
            self.canonical_ref_before is not None
            and self.canonical_ref_before == self.canonical_ref_after_rename
            and self.canonical_ref_before == self.canonical_ref_after_archive
        )

    @property
    def observation_immutable(self) -> bool:
        return self.old_observation_before == self.old_observation_after

    @property
    def model_immutable(self) -> bool:
        return self.old_model_before == self.old_model_after

    @property
    def replay_idempotent(self) -> bool:
        return (
            self.rename_replay_alias_count_before
            == self.rename_replay_alias_count_after
            and self.rename_replay_event_count_before
            == self.rename_replay_event_count_after
            and self.archive_replay_alias_count_before
            == self.archive_replay_alias_count_after
            and self.archive_replay_event_count_before
            == self.archive_replay_event_count_after
        )

    @property
    def intervals_non_overlapping(self) -> bool:
        by_name: dict[str, list[CustomerAliasIntervalEvidence]] = {}
        for interval in self.alias_intervals:
            by_name.setdefault(_normalized_name(interval.phrase), []).append(interval)
        for intervals in by_name.values():
            ordered = sorted(intervals, key=lambda interval: interval.valid_from)
            for left, right in zip(ordered, ordered[1:], strict=False):
                if left.valid_until is None or left.valid_until > right.valid_from:
                    return False
        return True


class CustomerLifecycleReport(_LifecycleModel):
    schema_version: Literal["company-learning-customer-lifecycle-report-v1"] = (
        "company-learning-customer-lifecycle-report-v1"
    )
    population_definition_version: str = Field(min_length=1)
    population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal["observed", "observed_with_gaps", "contradicted"]
    case_count: int = Field(ge=0)
    observed_case_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    unsupported_reason_counts: dict[str, int]
    violating_case_count: int = Field(ge=0)
    runtime_support_rate: IntervalEstimate
    rename_continuity_rate: IntervalEstimate
    valid_time_resolution_accuracy: IntervalEstimate
    stale_alias_rejection_rate: IntervalEstimate
    current_alias_safety_rate: IntervalEstimate
    historical_name_reuse_accuracy: IntervalEstimate
    observation_immutability_rate: IntervalEstimate
    model_immutability_rate: IntervalEstimate
    archive_alias_rejection_rate: IntervalEstimate
    archived_mutation_rejection_rate: IntervalEstimate
    alias_interval_non_overlap_rate: IntervalEstimate
    tenant_isolation_rate: IntervalEstimate
    replay_idempotency_rate: IntervalEstimate

    @model_validator(mode="after")
    def exact_accounting(self) -> Self:
        if self.case_count != 8:
            raise ValueError("lifecycle report must retain all 8 sealed cases")
        if self.observed_case_count + self.unsupported_case_count != self.case_count:
            raise ValueError("lifecycle observations must partition the registry")
        if self.runtime_support_rate.sample_size != self.case_count:
            raise ValueError("runtime support must include every sealed case")
        case_metrics = (
            self.rename_continuity_rate,
            self.observation_immutability_rate,
            self.model_immutability_rate,
            self.archived_mutation_rejection_rate,
            self.alias_interval_non_overlap_rate,
            self.replay_idempotency_rate,
        )
        if any(
            metric.sample_size != self.observed_case_count for metric in case_metrics
        ):
            raise ValueError("case-level lifecycle metrics changed denominator")
        expected_status = (
            "contradicted"
            if self.violating_case_count
            else "observed_with_gaps"
            if self.unsupported_case_count
            else "observed"
        )
        if self.status != expected_status:
            raise ValueError("lifecycle status does not match evidence")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def build_customer_lifecycle_population() -> CustomerLifecyclePopulation:
    specs = (
        ("CLI-001", "Acme", "Acme Holdings", True),
        ("CLI-002", "Nimbus", "Nimbus Labs", False),
        ("CLI-003", "Northstar", "Northstar Group", True),
        ("CLI-004", "Blue Harbor", "Blue Harbor Systems", False),
        ("CLI-005", "Cedar", "Cedar Analytics", True),
        ("CLI-006", "Orbit", "Orbit Cloud", False),
        ("CLI-007", "Pioneer", "Pioneer Works", True),
        ("CLI-008", "Vertex", "Vertex AI", False),
    )
    return CustomerLifecyclePopulation(
        cases=tuple(
            CustomerLifecycleCase(
                case_id=case_id,
                initial_identity=initial,
                renamed_identity=renamed,
                reuse_initial_identity=reuse,
            )
            for case_id, initial, renamed, reuse in specs
        )
    )


def load_customer_lifecycle_population(
    path: str | Path,
) -> CustomerLifecyclePopulation:
    rows = [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return CustomerLifecyclePopulation(
        cases=tuple(CustomerLifecycleCase.model_validate(row) for row in rows)
    )


def evaluate_customer_lifecycle_population(
    *,
    population: CustomerLifecyclePopulation,
    observations: tuple[CustomerLifecycleObservation, ...],
) -> CustomerLifecycleReport:
    observation_ids = [observation.case_id for observation in observations]
    if len(observation_ids) != len(set(observation_ids)):
        raise ValueError("lifecycle observations must be unique by case")
    expected_ids = {case.case_id for case in population.cases}
    if set(observation_ids) != expected_ids:
        missing = sorted(expected_ids - set(observation_ids))
        extra = sorted(set(observation_ids) - expected_ids)
        raise ValueError(
            "lifecycle observations must exactly cover the sealed population; "
            f"missing={missing}, extra={extra}"
        )
    by_case = {observation.case_id: observation for observation in observations}
    ordered = tuple(by_case[case.case_id] for case in population.cases)
    measured = tuple(
        observation
        for observation in ordered
        if observation.execution_status == "observed"
    )
    if not measured:
        raise ValueError("customer lifecycle population has no observed cases")
    unsupported = tuple(
        observation
        for observation in ordered
        if observation.execution_status == "unsupported"
    )
    cases = {case.case_id: case for case in population.cases}
    for observation in measured:
        case = cases[observation.case_id]
        has_reuse = any(
            probe.role
            in {
                ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME,
                ResolutionProbeRole.CURRENT_REUSED_OLD_NAME,
            }
            for probe in observation.resolution_probes
        )
        if has_reuse != case.reuse_initial_identity:
            raise ValueError(
                "historical-name-reuse evidence does not match sealed case"
            )
        _validate_case_probe_contract(case=case, observation=observation)

    probe_values = {
        category: [
            float(probe.correct)
            for observation in measured
            for probe in observation.resolution_probes
            if category in probe.categories
        ]
        for category in ResolutionProbeCategory
    }
    if any(not values for values in probe_values.values()):
        raise ValueError("lifecycle report requires every probe category")

    case_failures = {
        observation.case_id
        for observation in measured
        if not (
            observation.rename_continuity
            and observation.observation_immutable
            and observation.model_immutable
            and bool(observation.post_archive_rename_rejected)
            and observation.intervals_non_overlapping
            and observation.replay_idempotent
            and all(probe.correct for probe in observation.resolution_probes)
        )
    }
    status = (
        "contradicted"
        if case_failures
        else "observed_with_gaps"
        if unsupported
        else "observed"
    )
    return CustomerLifecycleReport(
        population_definition_version=population.population_definition_version,
        population_digest=population.digest,
        observation_digest=canonical_sha256(
            [observation.model_dump(mode="json") for observation in ordered]
        ),
        status=status,
        case_count=len(population.cases),
        observed_case_count=len(measured),
        unsupported_case_count=len(unsupported),
        unsupported_reason_counts=dict(
            sorted(
                Counter(
                    observation.unsupported_reason
                    for observation in unsupported
                    if observation.unsupported_reason is not None
                ).items()
            )
        ),
        violating_case_count=len(case_failures),
        runtime_support_rate=_wilson_estimate(
            [
                float(observation.execution_status == "observed")
                for observation in ordered
            ]
        ),
        rename_continuity_rate=_wilson_estimate(
            [float(observation.rename_continuity) for observation in measured]
        ),
        valid_time_resolution_accuracy=_wilson_estimate(
            probe_values[ResolutionProbeCategory.VALID_TIME]
        ),
        stale_alias_rejection_rate=_wilson_estimate(
            probe_values[ResolutionProbeCategory.STALE_ALIAS_REJECTION]
        ),
        current_alias_safety_rate=_wilson_estimate(
            probe_values[ResolutionProbeCategory.CURRENT_ALIAS_SAFETY]
        ),
        historical_name_reuse_accuracy=_wilson_estimate(
            probe_values[ResolutionProbeCategory.HISTORICAL_NAME_REUSE]
        ),
        observation_immutability_rate=_wilson_estimate(
            [float(observation.observation_immutable) for observation in measured]
        ),
        model_immutability_rate=_wilson_estimate(
            [float(observation.model_immutable) for observation in measured]
        ),
        archive_alias_rejection_rate=_wilson_estimate(
            probe_values[ResolutionProbeCategory.ARCHIVE_REJECTION]
        ),
        archived_mutation_rejection_rate=_wilson_estimate(
            [
                float(bool(observation.post_archive_rename_rejected))
                for observation in measured
            ]
        ),
        alias_interval_non_overlap_rate=_wilson_estimate(
            [float(observation.intervals_non_overlapping) for observation in measured]
        ),
        tenant_isolation_rate=_wilson_estimate(
            probe_values[ResolutionProbeCategory.TENANT_ISOLATION]
        ),
        replay_idempotency_rate=_wilson_estimate(
            [float(observation.replay_idempotent) for observation in measured]
        ),
    )


def _normalized_name(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _validate_case_probe_contract(
    *,
    case: CustomerLifecycleCase,
    observation: CustomerLifecycleObservation,
) -> None:
    probes = {probe.role: probe for probe in observation.resolution_probes}
    initial_roles = {
        ResolutionProbeRole.PRE_RENAME_OLD_NAME,
        ResolutionProbeRole.POST_RENAME_STALE_OLD_NAME,
        ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME,
        ResolutionProbeRole.CURRENT_REUSED_OLD_NAME,
        ResolutionProbeRole.TENANT_ISOLATION,
    }
    renamed_roles = {
        ResolutionProbeRole.CURRENT_RENAMED_NAME,
        ResolutionProbeRole.PRE_ARCHIVE_DELAYED_RENAMED_NAME,
        ResolutionProbeRole.POST_ARCHIVE_REJECTION,
    }
    if any(
        _normalized_name(probes[role].phrase) != _normalized_name(case.initial_identity)
        for role in initial_roles.intersection(probes)
    ) or any(
        _normalized_name(probes[role].phrase) != _normalized_name(case.renamed_identity)
        for role in renamed_roles.intersection(probes)
    ):
        raise ValueError("lifecycle probe phrase does not match sealed role")

    historical_roles = {
        ResolutionProbeRole.PRE_RENAME_OLD_NAME,
        ResolutionProbeRole.POST_RENAME_STALE_OLD_NAME,
        ResolutionProbeRole.PRE_ARCHIVE_DELAYED_RENAMED_NAME,
        ResolutionProbeRole.POST_ARCHIVE_REJECTION,
        ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME,
    }
    current_roles = set(probes) - historical_roles
    if any(probes[role].as_of is None for role in historical_roles & set(probes)):
        raise ValueError("historical lifecycle probes require as_of")
    if any(probes[role].as_of is not None for role in current_roles):
        raise ValueError("current lifecycle probes must not set as_of")

    canonical = observation.canonical_ref_before
    canonical_expected_roles = {
        ResolutionProbeRole.PRE_RENAME_OLD_NAME,
        ResolutionProbeRole.CURRENT_RENAMED_NAME,
        ResolutionProbeRole.PRE_ARCHIVE_DELAYED_RENAMED_NAME,
        ResolutionProbeRole.HISTORICAL_REUSED_OLD_NAME,
    }
    rejected_roles = {
        ResolutionProbeRole.POST_RENAME_STALE_OLD_NAME,
        ResolutionProbeRole.POST_ARCHIVE_REJECTION,
    }
    if any(
        probes[role].expected_ref != canonical
        for role in canonical_expected_roles & set(probes)
    ):
        raise ValueError("lifecycle probe expected ref changed canonical identity")
    if any(
        probes[role].expected_ref is not None for role in rejected_roles & set(probes)
    ):
        raise ValueError("stale or archived lifecycle probes must expect rejection")
    isolation = probes[ResolutionProbeRole.TENANT_ISOLATION].expected_ref
    if isolation is None or isolation == canonical:
        raise ValueError("tenant isolation probe must expect another tenant ref")
    reused = probes.get(ResolutionProbeRole.CURRENT_REUSED_OLD_NAME)
    if reused is not None and (
        reused.expected_ref is None or reused.expected_ref == canonical
    ):
        raise ValueError("current reused name must expect a new canonical ref")


__all__ = [
    "CUSTOMER_LIFECYCLE_SCENARIO_ID",
    "CustomerAliasIntervalEvidence",
    "CustomerLifecycleCase",
    "CustomerLifecycleObservation",
    "CustomerLifecyclePopulation",
    "CustomerLifecycleReport",
    "CustomerRef",
    "CustomerResolutionProbe",
    "ResolutionProbeCategory",
    "ResolutionProbeRole",
    "build_customer_lifecycle_population",
    "evaluate_customer_lifecycle_population",
    "load_customer_lifecycle_population",
]
