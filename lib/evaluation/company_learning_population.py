"""Deterministic held-out populations and interval estimates for company learning."""

from __future__ import annotations

import math
import random
from statistics import fmean
from typing import Callable, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256


class _PopulationModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class HeldOutExactAliasCase(_PopulationModel):
    case_id: str = Field(min_length=1)
    case_version: str = "v1"
    entity_type: str = Field(min_length=1)
    source_channel: str = "slack:message"
    slack_context: str = Field(min_length=1)
    wording_variant: str = Field(min_length=1)
    consequence: str = Field(min_length=1)
    recurrence_distance: str = Field(min_length=1)
    alias_surface: str = Field(min_length=1)
    training_text: str = Field(min_length=1)
    recurrence_text: str = Field(min_length=1)

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class HeldOutExactAliasPopulation(_PopulationModel):
    schema_version: str = "company-learning-heldout-population-v1"
    population_definition_version: str = "exact-alias-slack-v1"
    cases: tuple[HeldOutExactAliasCase, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def complete_unique_population(self) -> Self:
        case_ids = [case.case_id for case in self.cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("held-out case IDs must be unique")
        case_digests = [case.digest for case in self.cases]
        if len(case_digests) != len(set(case_digests)):
            raise ValueError("held-out cases must be independently defined")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class HeldOutPairObservation(_PopulationModel):
    case_id: str = Field(min_length=1)
    execution_status: Literal["observed", "unsupported"] = "observed"
    unsupported_reason: str | None = None
    adaptive_correct: bool | None = None
    frozen_correct: bool | None = None
    adaptive_unsafe: bool | None = None
    frozen_unsafe: bool | None = None
    adaptive_llm_calls: int | None = Field(default=None, ge=0)
    frozen_llm_calls: int | None = Field(default=None, ge=0)
    adaptive_latency_ms: float | None = Field(default=None, ge=0.0)
    frozen_latency_ms: float | None = Field(default=None, ge=0.0)

    @model_validator(mode="after")
    def status_matches_measurement(self) -> Self:
        measurements = (
            self.adaptive_correct,
            self.frozen_correct,
            self.adaptive_unsafe,
            self.frozen_unsafe,
            self.adaptive_llm_calls,
            self.frozen_llm_calls,
            self.adaptive_latency_ms,
            self.frozen_latency_ms,
        )
        if self.execution_status == "observed":
            if self.unsupported_reason is not None or any(
                value is None for value in measurements
            ):
                raise ValueError(
                    "observed held-out cases require complete measurements"
                )
        elif (
            not self.unsupported_reason
            or any(value is not None for value in measurements)
        ):
            raise ValueError(
                "unsupported held-out cases require one reason and no metrics"
            )
        return self


class IntervalEstimate(_PopulationModel):
    point_estimate: float
    lower_95: float
    upper_95: float
    method: str = Field(min_length=1)
    sample_size: int = Field(ge=0)


class HeldOutPopulationReport(_PopulationModel):
    schema_version: str = "company-learning-population-report-v1"
    population_definition_version: str
    population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    pair_count: int = Field(ge=0)
    observed_pair_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    complete_population: bool
    strata_counts: dict[str, dict[str, int]]
    observed_strata_counts: dict[str, dict[str, int]]
    unsupported_strata_counts: dict[str, dict[str, int]]
    unsupported_reason_counts: dict[str, int]
    adaptive_correctness: IntervalEstimate
    frozen_correctness: IntervalEstimate
    adaptive_minus_frozen_correctness: IntervalEstimate
    adaptive_unsafe_rate: IntervalEstimate
    frozen_unsafe_rate: IntervalEstimate
    mean_llm_calls_avoided: IntervalEstimate
    adaptive_minus_frozen_latency_ms: IntervalEstimate


def build_exact_alias_heldout_population(
    *,
    size: int = 60,
) -> HeldOutExactAliasPopulation:
    """Build a deterministic, sealed exact-alias Slack population."""

    if size < 1:
        raise ValueError("held-out population size must be positive")
    entity_types = ("customer", "project", "team", "system")
    contexts = ("public_channel", "private_channel", "cross_thread_recurrence")
    wordings = (
        "status_update",
        "risk_report",
        "commitment",
        "decision",
        "support_escalation",
    )
    consequences = ("low", "medium", "high")
    distances = ("same_day", "one_week", "one_month", "one_quarter")
    cases = []
    for index in range(size):
        entity_type = entity_types[index % len(entity_types)]
        context = contexts[(index // len(entity_types)) % len(contexts)]
        wording = wordings[
            (index // (len(entity_types) * len(contexts))) % len(wordings)
        ]
        consequence = consequences[index % len(consequences)]
        distance = distances[(index // len(consequences)) % len(distances)]
        alias = f"H{index:03d}"
        cases.append(
            HeldOutExactAliasCase(
                case_id=f"heldout-exact-{index:03d}",
                entity_type=entity_type,
                slack_context=context,
                wording_variant=wording,
                consequence=consequence,
                recurrence_distance=distance,
                alias_surface=alias,
                training_text=(
                    f"{alias} is the sealed {entity_type} for case {index:03d}."
                ),
                recurrence_text=_recurrence_text(
                    alias=alias,
                    wording=wording,
                    consequence=consequence,
                ),
            )
        )
    return HeldOutExactAliasPopulation(cases=tuple(cases))


def evaluate_heldout_population(
    *,
    population: HeldOutExactAliasPopulation,
    observations: tuple[HeldOutPairObservation, ...],
    bootstrap_samples: int = 2000,
) -> HeldOutPopulationReport:
    """Evaluate exactly one complete population and reject survivor-only reruns."""

    if bootstrap_samples < 200:
        raise ValueError("bootstrap_samples must be at least 200")
    observed_ids = [observation.case_id for observation in observations]
    if len(observed_ids) != len(set(observed_ids)):
        raise ValueError("held-out observations must be unique by case")
    expected_ids = {case.case_id for case in population.cases}
    observed_set = set(observed_ids)
    if observed_set != expected_ids:
        missing = sorted(expected_ids - observed_set)
        extra = sorted(observed_set - expected_ids)
        raise ValueError(
            "held-out observations must exactly cover the sealed population; "
            f"missing={missing}, extra={extra}"
        )
    by_case = {observation.case_id: observation for observation in observations}
    ordered = tuple(by_case[case.case_id] for case in population.cases)
    measured = tuple(
        observation
        for observation in ordered
        if observation.execution_status == "observed"
    )
    unsupported = tuple(
        observation
        for observation in ordered
        if observation.execution_status == "unsupported"
    )
    if not measured:
        raise ValueError(
            "held-out population has no runtime-supported measured cases"
        )
    seed = int(
        canonical_sha256(
            {
                "population_digest": population.digest,
                "observations": [
                    observation.model_dump(mode="json")
                    for observation in ordered
                ],
            }
        )[:16],
        16,
    )
    adaptive_correct = [
        float(_required(item.adaptive_correct)) for item in measured
    ]
    frozen_correct = [
        float(_required(item.frozen_correct)) for item in measured
    ]
    adaptive_unsafe = [
        float(_required(item.adaptive_unsafe)) for item in measured
    ]
    frozen_unsafe = [
        float(_required(item.frozen_unsafe)) for item in measured
    ]
    lift = [
        float(_required(item.adaptive_correct))
        - float(_required(item.frozen_correct))
        for item in measured
    ]
    llm_avoided = [
        float(
            _required(item.frozen_llm_calls)
            - _required(item.adaptive_llm_calls)
        )
        for item in measured
    ]
    latency_delta = [
        _required(item.adaptive_latency_ms)
        - _required(item.frozen_latency_ms)
        for item in measured
    ]
    observed_ids = {item.case_id for item in measured}
    unsupported_ids = {item.case_id for item in unsupported}
    unsupported_reasons: dict[str, int] = {}
    for item in unsupported:
        reason = str(item.unsupported_reason)
        unsupported_reasons[reason] = unsupported_reasons.get(reason, 0) + 1
    return HeldOutPopulationReport(
        population_definition_version=population.population_definition_version,
        population_digest=population.digest,
        observation_digest=canonical_sha256(
            [item.model_dump(mode="json") for item in ordered]
        ),
        pair_count=len(ordered),
        observed_pair_count=len(measured),
        unsupported_case_count=len(unsupported),
        complete_population=True,
        strata_counts=_strata_counts(population),
        observed_strata_counts=_strata_counts(
            population,
            case_ids=observed_ids,
        ),
        unsupported_strata_counts=_strata_counts(
            population,
            case_ids=unsupported_ids,
        ),
        unsupported_reason_counts=dict(sorted(unsupported_reasons.items())),
        adaptive_correctness=_wilson_estimate(adaptive_correct),
        frozen_correctness=_wilson_estimate(frozen_correct),
        adaptive_minus_frozen_correctness=_bootstrap_mean_estimate(
            lift,
            seed=seed,
            samples=bootstrap_samples,
        ),
        adaptive_unsafe_rate=_wilson_estimate(adaptive_unsafe),
        frozen_unsafe_rate=_wilson_estimate(frozen_unsafe),
        mean_llm_calls_avoided=_bootstrap_mean_estimate(
            llm_avoided,
            seed=seed + 1,
            samples=bootstrap_samples,
        ),
        adaptive_minus_frozen_latency_ms=_bootstrap_mean_estimate(
            latency_delta,
            seed=seed + 2,
            samples=bootstrap_samples,
        ),
    )


def _recurrence_text(*, alias: str, wording: str, consequence: str) -> str:
    templates = {
        "status_update": f"{alias} is still on track.",
        "risk_report": f"{alias} now has a {consequence} delivery risk.",
        "commitment": f"We committed the next milestone for {alias}.",
        "decision": f"The decision for {alias} is ready for review.",
        "support_escalation": f"{alias} has a {consequence} support escalation.",
    }
    return templates[wording]


def _strata_counts(
    population: HeldOutExactAliasPopulation,
    *,
    case_ids: set[str] | None = None,
) -> dict[str, dict[str, int]]:
    fields: tuple[tuple[str, Callable[[HeldOutExactAliasCase], str]], ...] = (
        ("entity_type", lambda case: case.entity_type),
        ("source_channel", lambda case: case.source_channel),
        ("slack_context", lambda case: case.slack_context),
        ("wording_variant", lambda case: case.wording_variant),
        ("consequence", lambda case: case.consequence),
        ("recurrence_distance", lambda case: case.recurrence_distance),
    )
    result: dict[str, dict[str, int]] = {}
    for field_name, getter in fields:
        counts: dict[str, int] = {}
        for case in population.cases:
            if case_ids is not None and case.case_id not in case_ids:
                continue
            value = getter(case)
            counts[value] = counts.get(value, 0) + 1
        result[field_name] = dict(sorted(counts.items()))
    return result


def _required(value):
    if value is None:
        raise ValueError("observed held-out metric is unexpectedly missing")
    return value


def _wilson_estimate(values: list[float], z: float = 1.96) -> IntervalEstimate:
    sample_size = len(values)
    successes = int(sum(values))
    point = successes / sample_size
    denominator = 1.0 + z**2 / sample_size
    center = (point + z**2 / (2 * sample_size)) / denominator
    margin = (
        z
        * math.sqrt(
            (
                point * (1.0 - point)
                + z**2 / (4 * sample_size)
            )
            / sample_size
        )
        / denominator
    )
    return IntervalEstimate(
        point_estimate=point,
        lower_95=max(0.0, center - margin),
        upper_95=min(1.0, center + margin),
        method="wilson_95",
        sample_size=sample_size,
    )


def _bootstrap_mean_estimate(
    values: list[float],
    *,
    seed: int,
    samples: int,
) -> IntervalEstimate:
    rng = random.Random(seed)
    sample_size = len(values)
    estimates = sorted(
        fmean(rng.choice(values) for _ in range(sample_size))
        for _ in range(samples)
    )
    return IntervalEstimate(
        point_estimate=fmean(values),
        lower_95=_percentile(estimates, 0.025),
        upper_95=_percentile(estimates, 0.975),
        method=f"paired_bootstrap_{samples}",
        sample_size=sample_size,
    )


def _percentile(values: list[float], quantile: float) -> float:
    position = (len(values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    weight = position - lower
    return values[lower] * (1.0 - weight) + values[upper] * weight
