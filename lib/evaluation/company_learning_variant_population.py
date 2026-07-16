"""Sealed continuous evaluation for governed variant-alias learning."""

from __future__ import annotations

import json
from collections import Counter
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    CorrectiveMemoryArm,
    CorrectiveMemoryArmAssessment,
    CorrectiveMemoryExperimentReport,
    RecurrenceCaseKind,
)
from lib.evaluation.company_learning_population import (
    IntervalEstimate,
    _bootstrap_mean_estimate,
    _wilson_estimate,
)


VARIANT_ALIAS_SCENARIO_ID = (
    "ENTITY-CORRECTIVE-MEMORY-VARIANT-ALIAS-POPULATION"
)


class _VariantPopulationModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class VariantAliasFamily(StrEnum):
    ACRONYM_FROM_LONG_FORM = "acronym_from_long_form"
    PUNCTUATION_COMPACT_FORM = "punctuation_compact_form"
    HYPHEN_SPACING = "hyphen_spacing"
    ANCHORED_SHORT_FORM = "anchored_short_form"
    ORTHOGRAPHIC_OMISSION_SUBSEQUENCE = (
        "orthographic_omission_subsequence"
    )
    POSSESSIVE_OR_PLURAL = "possessive_or_plural"


class HeldOutVariantAliasCase(_VariantPopulationModel):
    case_id: str = Field(min_length=1)
    case_version: str = "v1"
    entity_type: Literal["customer", "project", "team", "system"]
    variant_family: VariantAliasFamily
    candidate_label: str = Field(min_length=1)
    source_channel: Literal["slack:message"] = "slack:message"
    channel: str = Field(min_length=1)
    slack_context: Literal[
        "public_channel",
        "private_channel",
        "cross_thread_recurrence",
    ]
    wording_variant: Literal[
        "status_update",
        "risk_report",
        "commitment",
        "decision",
        "support_escalation",
    ]
    consequence: Literal["low", "medium", "high"]
    recurrence_distance: Literal[
        "same_day",
        "one_week",
        "one_month",
        "one_quarter",
    ]
    training_alias_surface: str = Field(min_length=1)
    recurrence_alias_surface: str = Field(min_length=1)
    training_text: str = Field(min_length=1)
    recurrence_text: str = Field(min_length=1)
    ranking_basis: str = Field(min_length=1)

    @model_validator(mode="after")
    def variant_is_source_anchored_and_mechanically_rankable(self) -> Self:
        if _norm(self.training_alias_surface) == _norm(
            self.recurrence_alias_surface
        ):
            raise ValueError("variant cases cannot repeat the exact alias")
        if self.training_alias_surface not in self.training_text:
            raise ValueError("training text must contain the training alias")
        if self.recurrence_alias_surface not in self.recurrence_text:
            raise ValueError(
                "recurrence text must contain the recurrence alias"
            )
        if self.ranking_basis != _ranking_basis(self.variant_family):
            raise ValueError(
                "variant ranking basis must match the sealed family rule"
            )
        if not _ranking_rule_matches(self):
            raise ValueError(
                "variant is not justified by the current candidate ranker"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class HeldOutVariantAliasPopulation(_VariantPopulationModel):
    schema_version: Literal["company-learning-variant-population-v1"] = (
        "company-learning-variant-population-v1"
    )
    population_definition_version: Literal["variant-alias-slack-v1"] = (
        "variant-alias-slack-v1"
    )
    cases: tuple[HeldOutVariantAliasCase, ...] = Field(
        min_length=24,
        max_length=24,
    )

    @model_validator(mode="after")
    def exact_balanced_registry(self) -> Self:
        case_ids = [case.case_id for case in self.cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("variant-alias case IDs must be unique")
        case_digests = [case.digest for case in self.cases]
        if len(case_digests) != len(set(case_digests)):
            raise ValueError("variant-alias cases must be independently defined")
        entity_counts = Counter(case.entity_type for case in self.cases)
        if entity_counts != {
            "customer": 6,
            "project": 6,
            "team": 6,
            "system": 6,
        }:
            raise ValueError(
                "variant registry requires six cases per entity type"
            )
        family_counts = Counter(case.variant_family for case in self.cases)
        if family_counts != {
            family: 4 for family in VariantAliasFamily
        }:
            raise ValueError(
                "variant registry requires four cases per variant family"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class VariantAliasExecutionObservation(_VariantPopulationModel):
    case_id: str = Field(min_length=1)
    execution_status: Literal["observed", "unsupported"] = "observed"
    unsupported_reason: str | None = None

    @model_validator(mode="after")
    def unsupported_reason_matches_status(self) -> Self:
        if self.execution_status == "observed" and self.unsupported_reason:
            raise ValueError("observed variant cases cannot be unsupported")
        if self.execution_status == "unsupported" and not self.unsupported_reason:
            raise ValueError(
                "unsupported variant cases require an explicit reason"
            )
        return self


class VariantAliasStratumReport(_VariantPopulationModel):
    sealed_case_count: int = Field(ge=0)
    observed_case_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    adaptive_correctness: IntervalEstimate | None
    frozen_correctness: IntervalEstimate | None
    adaptive_minus_frozen_correctness: IntervalEstimate | None
    adaptive_unsafe_rate: IntervalEstimate | None
    frozen_unsafe_rate: IntervalEstimate | None

    @model_validator(mode="after")
    def exact_stratum_accounting(self) -> Self:
        if (
            self.observed_case_count + self.unsupported_case_count
            != self.sealed_case_count
        ):
            raise ValueError(
                "variant stratum observations must partition sealed cases"
            )
        estimates = (
            self.adaptive_correctness,
            self.frozen_correctness,
            self.adaptive_minus_frozen_correctness,
            self.adaptive_unsafe_rate,
            self.frozen_unsafe_rate,
        )
        if self.observed_case_count == 0 and any(
            estimate is not None for estimate in estimates
        ):
            raise ValueError(
                "unobserved variant strata cannot contain estimates"
            )
        if self.observed_case_count > 0 and any(
            estimate is None for estimate in estimates
        ):
            raise ValueError(
                "observed variant strata require every estimate"
            )
        return self


class VariantAliasPopulationReport(_VariantPopulationModel):
    schema_version: Literal["company-learning-variant-report-v1"] = (
        "company-learning-variant-report-v1"
    )
    population_definition_version: str = Field(min_length=1)
    population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    experiment_report_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
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
    family_reports: dict[str, VariantAliasStratumReport]
    entity_type_reports: dict[str, VariantAliasStratumReport]

    @model_validator(mode="after")
    def exact_population_accounting(self) -> Self:
        if self.pair_count != 24 or not self.complete_population:
            raise ValueError(
                "variant report must retain the complete 24-case registry"
            )
        if (
            self.observed_pair_count + self.unsupported_case_count
            != self.pair_count
        ):
            raise ValueError(
                "observed and unsupported variant cases must partition registry"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def build_variant_alias_population() -> HeldOutVariantAliasPopulation:
    """Build the deterministic 24-case variant-alias registry."""

    cases: list[HeldOutVariantAliasCase] = []
    entities = ("customer", "project", "team", "system")
    contexts = (
        "public_channel",
        "private_channel",
        "cross_thread_recurrence",
    )
    distances = ("same_day", "one_week", "one_month", "one_quarter")
    wordings = (
        "status_update",
        "risk_report",
        "commitment",
        "decision",
        "support_escalation",
        "status_update",
    )
    consequences = ("low", "medium", "high")
    surfaces = _variant_surfaces()
    for family_index, family in enumerate(VariantAliasFamily):
        for entity_index, entity_type in enumerate(entities):
            training_alias, recurrence_alias = surfaces[family][entity_type]
            wording = wordings[family_index]
            consequence = consequences[
                (family_index + entity_index) % len(consequences)
            ]
            cases.append(
                HeldOutVariantAliasCase(
                    case_id=(
                        f"heldout-variant-{family_index:02d}-"
                        f"{entity_type}"
                    ),
                    entity_type=entity_type,
                    variant_family=family,
                    candidate_label=(
                        f"Canonical {training_alias} {entity_type}"
                    ),
                    channel=(
                        f"C-VARIANT-{family_index:02d}-"
                        f"{entity_type.upper()}"
                    ),
                    slack_context=contexts[
                        (family_index + entity_index) % len(contexts)
                    ],
                    wording_variant=wording,
                    consequence=consequence,
                    recurrence_distance=distances[
                        (family_index + entity_index) % len(distances)
                    ],
                    training_alias_surface=training_alias,
                    recurrence_alias_surface=recurrence_alias,
                    training_text=(
                        f"{training_alias} is the sealed {entity_type} "
                        f"for {family.value}."
                    ),
                    recurrence_text=_recurrence_text(
                        alias=recurrence_alias,
                        wording=wording,
                        consequence=consequence,
                    ),
                    ranking_basis=_ranking_basis(family),
                )
            )
    return HeldOutVariantAliasPopulation(cases=tuple(cases))


def load_variant_alias_population(
    path: Path | str,
) -> HeldOutVariantAliasPopulation:
    cases = tuple(
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    )
    return HeldOutVariantAliasPopulation(cases=cases)


def evaluate_variant_alias_population(
    *,
    population: HeldOutVariantAliasPopulation,
    experiment_report: CorrectiveMemoryExperimentReport,
    observations: tuple[VariantAliasExecutionObservation, ...],
    bootstrap_samples: int = 2000,
) -> VariantAliasPopulationReport:
    """Evaluate one exact registry without survivor-only reruns."""

    if bootstrap_samples < 200:
        raise ValueError("bootstrap_samples must be at least 200")
    ordered_observations = _ordered_observations(
        population=population,
        observations=observations,
    )
    observed_ids = {
        observation.case_id
        for observation in ordered_observations
        if observation.execution_status == "observed"
    }
    unsupported_ids = {
        observation.case_id
        for observation in ordered_observations
        if observation.execution_status == "unsupported"
    }
    _validate_experiment_report(
        experiment_report,
        observed_ids=observed_ids,
    )
    if not observed_ids:
        raise ValueError(
            "variant-alias population has no runtime-supported cases"
        )
    assessments = {
        (assessment.case_id, assessment.arm): assessment
        for assessment in experiment_report.assessments
    }
    seed = int(
        canonical_sha256(
            {
                "population_digest": population.digest,
                "report_digest": experiment_report.digest,
                "observations": [
                    observation.model_dump(mode="json")
                    for observation in ordered_observations
                ],
            }
        )[:16],
        16,
    )
    overall = _estimate_assessments(
        case_ids=observed_ids,
        assessments=assessments,
        seed=seed,
        bootstrap_samples=bootstrap_samples,
    )
    unsupported_reasons = Counter(
        str(observation.unsupported_reason)
        for observation in ordered_observations
        if observation.execution_status == "unsupported"
    )
    return VariantAliasPopulationReport(
        population_definition_version=population.population_definition_version,
        population_digest=population.digest,
        experiment_report_digest=experiment_report.digest,
        observation_digest=canonical_sha256(
            [
                observation.model_dump(mode="json")
                for observation in ordered_observations
            ]
        ),
        pair_count=len(population.cases),
        observed_pair_count=len(observed_ids),
        unsupported_case_count=len(unsupported_ids),
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
        adaptive_correctness=overall["adaptive_correctness"],
        frozen_correctness=overall["frozen_correctness"],
        adaptive_minus_frozen_correctness=overall[
            "adaptive_minus_frozen_correctness"
        ],
        adaptive_unsafe_rate=overall["adaptive_unsafe_rate"],
        frozen_unsafe_rate=overall["frozen_unsafe_rate"],
        family_reports=_stratum_reports(
            population=population,
            observations=ordered_observations,
            assessments=assessments,
            dimension="variant_family",
            bootstrap_samples=bootstrap_samples,
            seed=seed + 100,
        ),
        entity_type_reports=_stratum_reports(
            population=population,
            observations=ordered_observations,
            assessments=assessments,
            dimension="entity_type",
            bootstrap_samples=bootstrap_samples,
            seed=seed + 200,
        ),
    )


def _ordered_observations(
    *,
    population: HeldOutVariantAliasPopulation,
    observations: tuple[VariantAliasExecutionObservation, ...],
) -> tuple[VariantAliasExecutionObservation, ...]:
    observation_ids = [observation.case_id for observation in observations]
    if len(observation_ids) != len(set(observation_ids)):
        raise ValueError(
            "variant-alias observations must be unique by case"
        )
    expected_ids = {case.case_id for case in population.cases}
    observed_ids = set(observation_ids)
    if observed_ids != expected_ids:
        raise ValueError(
            "variant-alias observations must exactly cover the sealed "
            f"population; missing={sorted(expected_ids - observed_ids)}, "
            f"extra={sorted(observed_ids - expected_ids)}"
        )
    by_case = {
        observation.case_id: observation for observation in observations
    }
    return tuple(by_case[case.case_id] for case in population.cases)


def _validate_experiment_report(
    report: CorrectiveMemoryExperimentReport,
    *,
    observed_ids: set[str],
) -> None:
    if VARIANT_ALIAS_SCENARIO_ID not in report.scenario_ids:
        raise ValueError(
            "variant experiment report is missing the sealed scenario"
        )
    pair_ids = [pair.case_id for pair in report.pairs]
    if len(pair_ids) != len(set(pair_ids)) or set(pair_ids) != observed_ids:
        raise ValueError(
            "variant experiment pairs must exactly cover observed cases"
        )
    if report.pair_results_digest != canonical_sha256(
        [pair.model_dump(mode="json") for pair in report.pairs]
    ):
        raise ValueError("variant experiment pair digest mismatch")
    assessment_keys = [
        (assessment.case_id, assessment.arm)
        for assessment in report.assessments
    ]
    expected_keys = {
        (case_id, arm)
        for case_id in observed_ids
        for arm in CorrectiveMemoryArm
    }
    if (
        len(assessment_keys) != len(set(assessment_keys))
        or set(assessment_keys) != expected_keys
    ):
        raise ValueError(
            "variant experiment assessments must cover both arms exactly"
        )
    pair_by_case = {pair.case_id: pair for pair in report.pairs}
    for assessment in report.assessments:
        pair = pair_by_case[assessment.case_id]
        result = (
            pair.adaptive
            if assessment.arm is CorrectiveMemoryArm.ADAPTIVE
            else pair.frozen
        )
        if (
            assessment.consumer_fate is not result.consumer_fate
            or assessment.resolved_entity_ref != result.resolved_entity_ref
            or assessment.observed_model_count != len(result.lineage.model_ids)
            or not result.observed_safety_incidents.issubset(
                assessment.incident_classes
            )
        ):
            raise ValueError(
                "variant experiment assessments do not match pair results"
            )
    incident_keys = {
        (
            incident.case_id,
            incident.arm,
            incident.incident_class,
        )
        for incident in report.incidents
    }
    assessed_incident_keys = {
        (
            assessment.case_id,
            assessment.arm,
            incident_class,
        )
        for assessment in report.assessments
        for incident_class in assessment.incident_classes
    }
    if incident_keys != assessed_incident_keys:
        raise ValueError(
            "variant experiment incidents do not match assessments"
        )
    if report.metrics.pair_count != len(observed_ids):
        raise ValueError("variant experiment pair count mismatch")
    variant_metrics = report.metrics.case_kind_metrics.get(
        RecurrenceCaseKind.VARIANT_ALIAS_POSITIVE.value
    )
    if (
        variant_metrics is None
        or variant_metrics.get("pair_count") != len(observed_ids)
    ):
        raise ValueError(
            "variant experiment report is not sealed to variant-alias cases"
        )
    adaptive = tuple(
        assessment
        for assessment in report.assessments
        if assessment.arm is CorrectiveMemoryArm.ADAPTIVE
    )
    frozen = tuple(
        assessment
        for assessment in report.assessments
        if assessment.arm is CorrectiveMemoryArm.FROZEN
    )
    expected_metrics = {
        "adaptive_correct_count": sum(item.correct for item in adaptive),
        "frozen_correct_count": sum(item.correct for item in frozen),
        "adaptive_unsafe_count": sum(
            bool(item.incident_classes) for item in adaptive
        ),
        "frozen_unsafe_count": sum(
            bool(item.incident_classes) for item in frozen
        ),
    }
    if any(
        getattr(report.metrics, key) != value
        for key, value in expected_metrics.items()
    ):
        raise ValueError(
            "variant experiment metrics do not match assessments"
        )


def _estimate_assessments(
    *,
    case_ids: set[str],
    assessments: dict[
        tuple[str, CorrectiveMemoryArm],
        CorrectiveMemoryArmAssessment,
    ],
    seed: int,
    bootstrap_samples: int,
) -> dict[str, IntervalEstimate]:
    adaptive = [
        assessments[(case_id, CorrectiveMemoryArm.ADAPTIVE)]
        for case_id in sorted(case_ids)
    ]
    frozen = [
        assessments[(case_id, CorrectiveMemoryArm.FROZEN)]
        for case_id in sorted(case_ids)
    ]
    adaptive_correct = [float(item.correct) for item in adaptive]
    frozen_correct = [float(item.correct) for item in frozen]
    adaptive_unsafe = [
        float(bool(item.incident_classes)) for item in adaptive
    ]
    frozen_unsafe = [
        float(bool(item.incident_classes)) for item in frozen
    ]
    lift = [
        adaptive_value - frozen_value
        for adaptive_value, frozen_value in zip(
            adaptive_correct,
            frozen_correct,
            strict=True,
        )
    ]
    return {
        "adaptive_correctness": _wilson_estimate(adaptive_correct),
        "frozen_correctness": _wilson_estimate(frozen_correct),
        "adaptive_minus_frozen_correctness": _bootstrap_mean_estimate(
            lift,
            seed=seed,
            samples=bootstrap_samples,
        ),
        "adaptive_unsafe_rate": _wilson_estimate(adaptive_unsafe),
        "frozen_unsafe_rate": _wilson_estimate(frozen_unsafe),
    }


def _strata_counts(
    population: HeldOutVariantAliasPopulation,
    *,
    case_ids: set[str] | None = None,
) -> dict[str, dict[str, int]]:
    dimensions = {
        "entity_type": lambda case: case.entity_type,
        "variant_family": lambda case: case.variant_family.value,
        "slack_context": lambda case: case.slack_context,
        "wording_variant": lambda case: case.wording_variant,
        "consequence": lambda case: case.consequence,
        "recurrence_distance": lambda case: case.recurrence_distance,
    }
    result: dict[str, dict[str, int]] = {}
    for dimension, getter in dimensions.items():
        counts = Counter(
            getter(case)
            for case in population.cases
            if case_ids is None or case.case_id in case_ids
        )
        result[dimension] = dict(sorted(counts.items()))
    return result


def _stratum_reports(
    *,
    population: HeldOutVariantAliasPopulation,
    observations: tuple[VariantAliasExecutionObservation, ...],
    assessments: dict[
        tuple[str, CorrectiveMemoryArm],
        CorrectiveMemoryArmAssessment,
    ],
    dimension: Literal["entity_type", "variant_family"],
    bootstrap_samples: int,
    seed: int,
) -> dict[str, VariantAliasStratumReport]:
    observation_by_case = {
        observation.case_id: observation for observation in observations
    }
    values = sorted(
        {
            (
                case.entity_type
                if dimension == "entity_type"
                else case.variant_family.value
            )
            for case in population.cases
        }
    )
    result: dict[str, VariantAliasStratumReport] = {}
    for index, value in enumerate(values):
        sealed_ids = {
            case.case_id
            for case in population.cases
            if (
                case.entity_type
                if dimension == "entity_type"
                else case.variant_family.value
            )
            == value
        }
        observed_ids = {
            case_id
            for case_id in sealed_ids
            if observation_by_case[case_id].execution_status == "observed"
        }
        estimates = (
            _estimate_assessments(
                case_ids=observed_ids,
                assessments=assessments,
                seed=seed + index,
                bootstrap_samples=bootstrap_samples,
            )
            if observed_ids
            else {}
        )
        result[value] = VariantAliasStratumReport(
            sealed_case_count=len(sealed_ids),
            observed_case_count=len(observed_ids),
            unsupported_case_count=len(sealed_ids - observed_ids),
            adaptive_correctness=estimates.get("adaptive_correctness"),
            frozen_correctness=estimates.get("frozen_correctness"),
            adaptive_minus_frozen_correctness=estimates.get(
                "adaptive_minus_frozen_correctness"
            ),
            adaptive_unsafe_rate=estimates.get("adaptive_unsafe_rate"),
            frozen_unsafe_rate=estimates.get("frozen_unsafe_rate"),
        )
    return result


def _ranking_rule_matches(case: HeldOutVariantAliasCase) -> bool:
    training = case.training_alias_surface
    recurrence = case.recurrence_alias_surface
    training_norm = _norm(training)
    recurrence_norm = _norm(recurrence)
    training_compact = _compact(training)
    recurrence_compact = _compact(recurrence)
    family = case.variant_family
    if family is VariantAliasFamily.ACRONYM_FROM_LONG_FORM:
        return recurrence_compact == "".join(
            token[0] for token in _tokens(training)
        )
    if family is VariantAliasFamily.PUNCTUATION_COMPACT_FORM:
        return "." in training and recurrence_compact == training_compact
    if family is VariantAliasFamily.HYPHEN_SPACING:
        return "-" in training and recurrence_compact == training_compact
    if family is VariantAliasFamily.ANCHORED_SHORT_FORM:
        return recurrence_norm in training_norm
    if family is VariantAliasFamily.ORTHOGRAPHIC_OMISSION_SUBSEQUENCE:
        return _is_subsequence(recurrence_compact, training_compact)
    if family is VariantAliasFamily.POSSESSIVE_OR_PLURAL:
        return training_norm in recurrence_norm
    return False


def _variant_surfaces() -> dict[
    VariantAliasFamily,
    dict[str, tuple[str, str]],
]:
    return {
        VariantAliasFamily.ACRONYM_FROM_LONG_FORM: {
            "customer": ("Nimbus Banking Initiative", "NBI"),
            "project": ("Phoenix Launch Program", "PLP"),
            "team": ("Revenue Operations Group", "ROG"),
            "system": ("Order Management Service", "OMS"),
        },
        VariantAliasFamily.PUNCTUATION_COMPACT_FORM: {
            "customer": ("Atlas.Pay", "AtlasPay"),
            "project": ("Project.Nova", "ProjectNova"),
            "team": ("Growth.Ops", "GrowthOps"),
            "system": ("Data.Hub", "DataHub"),
        },
        VariantAliasFamily.HYPHEN_SPACING: {
            "customer": ("North-Star", "North Star"),
            "project": ("Launch-Bridge", "Launch Bridge"),
            "team": ("Core-Platform", "Core Platform"),
            "system": ("Risk-Engine", "Risk Engine"),
        },
        VariantAliasFamily.ANCHORED_SHORT_FORM: {
            "customer": ("Orion Holdings", "Orion"),
            "project": ("Mercury Migration", "Mercury"),
            "team": ("Falcon Support", "Falcon"),
            "system": ("Beacon Gateway", "Beacon"),
        },
        VariantAliasFamily.ORTHOGRAPHIC_OMISSION_SUBSEQUENCE: {
            "customer": ("Nimbus", "Nmbus"),
            "project": ("Phoenix", "Phenix"),
            "team": ("Revenue", "Revnue"),
            "system": ("Telemetry", "Telemtry"),
        },
        VariantAliasFamily.POSSESSIVE_OR_PLURAL: {
            "customer": ("Acme", "Acme's"),
            "project": ("Horizon", "Horizons"),
            "team": ("Platform", "Platform's"),
            "system": ("Sentinel", "Sentinels"),
        },
    }


def _ranking_basis(family: VariantAliasFamily) -> str:
    return {
        VariantAliasFamily.ACRONYM_FROM_LONG_FORM: (
            "recurrence compact form equals the training-token acronym"
        ),
        VariantAliasFamily.PUNCTUATION_COMPACT_FORM: (
            "training and recurrence surfaces have equal alphanumeric compact forms"
        ),
        VariantAliasFamily.HYPHEN_SPACING: (
            "hyphen and whitespace variants have equal compact forms"
        ),
        VariantAliasFamily.ANCHORED_SHORT_FORM: (
            "the source-anchored short form is a normalized substring of the alias"
        ),
        VariantAliasFamily.ORTHOGRAPHIC_OMISSION_SUBSEQUENCE: (
            "the omitted-letter compact form is a subsequence of the alias"
        ),
        VariantAliasFamily.POSSESSIVE_OR_PLURAL: (
            "the normalized training alias is contained in the inflected form"
        ),
    }[family]


def _recurrence_text(
    *,
    alias: str,
    wording: str,
    consequence: str,
) -> str:
    return {
        "status_update": f"{alias} is ready for the next review.",
        "risk_report": f"{alias} now has a {consequence} delivery risk.",
        "commitment": f"We committed the next milestone for {alias}.",
        "decision": f"The decision for {alias} is ready for review.",
        "support_escalation": (
            f"{alias} has a {consequence} support escalation."
        ),
    }[wording]


def _norm(value: str) -> str:
    return " ".join(value.casefold().split())


def _compact(value: str) -> str:
    return "".join(character for character in _norm(value) if character.isalnum())


def _tokens(value: str) -> list[str]:
    return [
        token
        for token in _norm(value).replace("-", " ").split()
        if token and any(character.isalpha() for character in token)
    ]


def _is_subsequence(needle: str, haystack: str) -> bool:
    if not needle:
        return False
    position = 0
    for character in haystack:
        if position < len(needle) and needle[position] == character:
            position += 1
    return position == len(needle)


__all__ = [
    "HeldOutVariantAliasCase",
    "HeldOutVariantAliasPopulation",
    "VARIANT_ALIAS_SCENARIO_ID",
    "VariantAliasExecutionObservation",
    "VariantAliasFamily",
    "VariantAliasPopulationReport",
    "VariantAliasStratumReport",
    "build_variant_alias_population",
    "evaluate_variant_alias_population",
    "load_variant_alias_population",
]
