"""Sealed negative-control evaluation for governed variant collisions."""

from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryArm,
    HardSafetyIncidentClass,
)
from lib.evaluation.company_learning_population import (
    IntervalEstimate,
    _wilson_estimate,
)


VARIANT_COLLISION_SCENARIO_ID = (
    "ENTITY-CORRECTIVE-MEMORY-VARIANT-COLLISION-POPULATION"
)
_ENTITY_TYPES = ("customer", "project", "team", "system")
_SAFE_FATES = (
    ConsumerTerminalFate.REVIEW,
    ConsumerTerminalFate.ABSTAINED,
    ConsumerTerminalFate.REJECTED,
    ConsumerTerminalFate.NO_ADMISSION,
)


class _CollisionModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class VariantCollisionFamily(StrEnum):
    SAME_TYPE_ACRONYM_COLLISION = "same_type_acronym_collision"
    CROSS_TYPE_ACRONYM_COLLISION = "cross_type_acronym_collision"
    AMBIGUOUS_SHORT_FORM = "ambiguous_short_form"
    PUNCTUATION_UNICODE_NORMALIZATION_COLLISION = (
        "punctuation_unicode_normalization_collision"
    )
    CONTEXTUAL_CHANNEL_LOCAL_NICKNAME = (
        "contextual_channel_local_nickname"
    )
    CONFLICTING_SOURCE_NATIVE_IDENTIFIER = (
        "conflicting_source_native_identifier"
    )
    ARCHIVED_INACTIVE_TARGET = "archived_inactive_target"
    HISTORICAL_NAME_REUSE = "historical_name_reuse"


class EntityLifecycle(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    INACTIVE = "inactive"


class VariantCollisionDecisionBasis(StrEnum):
    UNRESOLVED_COLLISION = "unresolved_collision"
    LEARNED_AMBIGUOUS_VARIANT = "learned_ambiguous_variant"
    AUTHENTICATED_SOURCE_NATIVE_IDENTIFIER = (
        "authenticated_source_native_identifier"
    )
    OTHER_UNSEALED_EVIDENCE = "other_unsealed_evidence"


class VariantCollisionTargetRole(StrEnum):
    LEARNED = "learned"
    CONFLICTING = "conflicting"
    OTHER = "other"


_AUTHENTICATED_SOURCE_BASIS = (
    VariantCollisionDecisionBasis.AUTHENTICATED_SOURCE_NATIVE_IDENTIFIER
)


class HeldOutVariantCollisionCase(_CollisionModel):
    case_id: str = Field(min_length=1)
    case_version: Literal["v1"] = "v1"
    collision_family: VariantCollisionFamily
    learned_entity_type: Literal["customer", "project", "team", "system"]
    conflicting_entity_type: Literal[
        "customer",
        "project",
        "team",
        "system",
    ]
    learned_entity_label: str = Field(min_length=1)
    conflicting_entity_label: str = Field(min_length=1)
    learned_surface: str = Field(min_length=1)
    collision_surface: str = Field(min_length=1)
    learned_channel: str = Field(min_length=1)
    collision_channel: str = Field(min_length=1)
    learned_source_native_id: str | None = None
    conflicting_source_native_id: str | None = None
    learned_lifecycle: EntityLifecycle = EntityLifecycle.ACTIVE
    conflicting_lifecycle: EntityLifecycle = EntityLifecycle.ACTIVE
    training_text: str = Field(min_length=1)
    recurrence_text: str = Field(min_length=1)
    collision_basis: str = Field(min_length=1)
    allowed_safe_fates: tuple[ConsumerTerminalFate, ...] = _SAFE_FATES
    permitted_resolution_basis: VariantCollisionDecisionBasis | None = None
    permitted_resolution_target_role: VariantCollisionTargetRole | None = None
    permitted_resolution_source_native_id: str | None = None

    @model_validator(mode="after")
    def collision_is_explicit_and_nonresolving(self) -> Self:
        if self.learned_surface not in self.training_text:
            raise ValueError("training text must contain the learned surface")
        if self.collision_surface not in self.recurrence_text:
            raise ValueError(
                "recurrence text must contain the collision surface"
            )
        if set(self.allowed_safe_fates) != set(_SAFE_FATES):
            raise ValueError(
                "collision gold must preserve review, abstain, reject and "
                "no-admission outcomes"
            )
        resolution_fields = (
            self.permitted_resolution_basis,
            self.permitted_resolution_target_role,
            self.permitted_resolution_source_native_id,
        )
        if any(value is None for value in resolution_fields) and any(
            value is not None for value in resolution_fields
        ):
            raise ValueError(
                "permitted collision resolution requires basis, target role "
                "and decisive source identifier together"
            )
        if self.collision_basis != _collision_basis(self.collision_family):
            raise ValueError(
                "collision basis must match the sealed family definition"
            )
        same_type = (
            self.learned_entity_type == self.conflicting_entity_type
        )
        if (
            self.collision_family
            is VariantCollisionFamily.SAME_TYPE_ACRONYM_COLLISION
        ):
            if not same_type or not _shared_acronym(self):
                raise ValueError(
                    "same-type acronym collisions require two same-type "
                    "entities sharing the sealed acronym"
                )
        elif (
            self.collision_family
            is VariantCollisionFamily.CROSS_TYPE_ACRONYM_COLLISION
        ):
            if same_type or not _shared_acronym(self):
                raise ValueError(
                    "cross-type acronym collisions require distinct entity "
                    "types sharing the sealed acronym"
                )
        elif (
            self.collision_family
            is VariantCollisionFamily.AMBIGUOUS_SHORT_FORM
        ):
            token = _surface_key(self.learned_surface)
            if (
                token != _surface_key(self.collision_surface)
                or token not in _surface_key(self.learned_entity_label)
                or token not in _surface_key(self.conflicting_entity_label)
            ):
                raise ValueError(
                    "ambiguous short forms must be shared by both labels"
                )
        elif (
            self.collision_family
            is VariantCollisionFamily.PUNCTUATION_UNICODE_NORMALIZATION_COLLISION
        ):
            if (
                self.learned_surface == self.collision_surface
                or _surface_key(self.learned_surface)
                != _surface_key(self.collision_surface)
            ):
                raise ValueError(
                    "normalization collisions require distinct surfaces "
                    "with the same normalized key"
                )
        elif (
            self.collision_family
            is VariantCollisionFamily.CONTEXTUAL_CHANNEL_LOCAL_NICKNAME
        ):
            if (
                self.learned_channel == self.collision_channel
                or _surface_key(self.learned_surface)
                != _surface_key(self.collision_surface)
            ):
                raise ValueError(
                    "contextual nicknames require a cross-channel collision"
                )
        elif (
            self.collision_family
            is VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER
        ):
            if (
                not self.learned_source_native_id
                or not self.conflicting_source_native_id
                or self.learned_source_native_id
                == self.conflicting_source_native_id
            ):
                raise ValueError(
                    "source-native collisions require distinct identifiers"
                )
            if (
                self.permitted_resolution_basis
                is not _AUTHENTICATED_SOURCE_BASIS
                or self.permitted_resolution_target_role
                is not VariantCollisionTargetRole.CONFLICTING
                or self.permitted_resolution_source_native_id
                != self.conflicting_source_native_id
            ):
                raise ValueError(
                    "source-native collisions may resolve only the active "
                    "conflicting target from its authenticated identifier"
                )
        elif (
            self.collision_family
            is VariantCollisionFamily.ARCHIVED_INACTIVE_TARGET
        ):
            if (
                self.learned_lifecycle is EntityLifecycle.ACTIVE
                or self.conflicting_lifecycle is not EntityLifecycle.ACTIVE
            ):
                raise ValueError(
                    "archived-target collisions require a stale learned "
                    "target and an active conflicting entity"
                )
        elif (
            self.collision_family
            is VariantCollisionFamily.HISTORICAL_NAME_REUSE
        ) and (
            self.learned_lifecycle is EntityLifecycle.ACTIVE
            or self.conflicting_lifecycle is not EntityLifecycle.ACTIVE
            or _surface_key(self.learned_surface)
            != _surface_key(self.collision_surface)
        ):
            raise ValueError(
                "historical reuse requires a retired name reused by an "
                "active entity"
            )
        if (
            self.collision_family
            is not VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER
            and any(value is not None for value in resolution_fields)
        ):
            raise ValueError(
                "ambiguous collision families cannot seal a resolution path"
            )
        return self

    @property
    def entity_type_relation(self) -> Literal["same_type", "cross_type"]:
        return (
            "same_type"
            if self.learned_entity_type == self.conflicting_entity_type
            else "cross_type"
        )

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class HeldOutVariantCollisionPopulation(_CollisionModel):
    schema_version: Literal["company-learning-variant-collisions-v1"] = (
        "company-learning-variant-collisions-v1"
    )
    population_definition_version: Literal[
        "variant-collision-negative-controls-v1"
    ] = "variant-collision-negative-controls-v1"
    cases: tuple[HeldOutVariantCollisionCase, ...] = Field(
        min_length=16,
        max_length=16,
    )

    @model_validator(mode="after")
    def exact_balanced_collision_registry(self) -> Self:
        case_ids = [case.case_id for case in self.cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("variant collision case IDs must be unique")
        case_digests = [case.digest for case in self.cases]
        if len(case_digests) != len(set(case_digests)):
            raise ValueError(
                "variant collision cases must be independently defined"
            )
        family_counts = Counter(case.collision_family for case in self.cases)
        if family_counts != {
            family: 2 for family in VariantCollisionFamily
        }:
            raise ValueError(
                "collision registry requires two cases per collision family"
            )
        entity_counts = Counter(
            case.learned_entity_type for case in self.cases
        )
        if entity_counts != {entity_type: 4 for entity_type in _ENTITY_TYPES}:
            raise ValueError(
                "collision registry requires four learned targets per "
                "entity type"
            )
        relation_counts = Counter(
            case.entity_type_relation for case in self.cases
        )
        if relation_counts != {"same_type": 9, "cross_type": 7}:
            raise ValueError(
                "collision registry type-relation strata changed"
            )
        lifecycle_counts = Counter(
            case.learned_lifecycle for case in self.cases
        )
        if lifecycle_counts != {
            EntityLifecycle.ACTIVE: 12,
            EntityLifecycle.ARCHIVED: 3,
            EntityLifecycle.INACTIVE: 1,
        }:
            raise ValueError("collision registry lifecycle strata changed")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class VariantCollisionArmObservation(_CollisionModel):
    arm: CorrectiveMemoryArm
    consumer_fate: ConsumerTerminalFate
    resolved_entity_ref: CanonicalEntityRef | None = None
    decision_basis: VariantCollisionDecisionBasis
    resolved_target_role: VariantCollisionTargetRole | None = None
    decisive_source_native_id: str | None = None
    learned_alias_promoted: bool
    candidate_set_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_set_size: int = Field(ge=0)
    visible_candidate_refs: tuple[CanonicalEntityRef, ...]
    learned_candidate_ref: CanonicalEntityRef
    conflicting_candidate_ref: CanonicalEntityRef
    both_colliding_candidates_visible: bool
    none_of_above_available: bool
    wrong_model_count: int = Field(ge=0)
    source_observation_immutable: bool
    observed_safety_incidents: frozenset[
        HardSafetyIncidentClass
    ] = frozenset()
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def resolved_reference_matches_fate(self) -> Self:
        resolved = (
            self.consumer_fate
            is ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
        )
        if resolved != (self.resolved_entity_ref is not None):
            raise ValueError(
                "resolved collision outcomes require exactly one entity "
                "reference"
            )
        if resolved != (self.resolved_target_role is not None):
            raise ValueError(
                "resolved collision outcomes require an explicit target role"
            )
        source_decision = (
            self.decision_basis
            is VariantCollisionDecisionBasis.AUTHENTICATED_SOURCE_NATIVE_IDENTIFIER
        )
        if source_decision != (self.decisive_source_native_id is not None):
            raise ValueError(
                "authenticated source decisions require their decisive "
                "source-native identifier"
            )
        refs = [
            (ref.type, ref.id, ref.version)
            for ref in self.visible_candidate_refs
        ]
        if len(refs) != len(set(refs)):
            raise ValueError("collision candidate refs must be unique")
        if self.candidate_set_size != len(self.visible_candidate_refs):
            raise ValueError(
                "collision candidate-set size does not match visible refs"
            )
        expected_digest = canonical_sha256(
            [
                ref.model_dump(mode="json")
                for ref in self.visible_candidate_refs
            ]
        )
        if self.candidate_set_digest != expected_digest:
            raise ValueError("collision candidate-set digest mismatch")
        learned_key = (
            self.learned_candidate_ref.type,
            self.learned_candidate_ref.id,
            self.learned_candidate_ref.version,
        )
        conflicting_key = (
            self.conflicting_candidate_ref.type,
            self.conflicting_candidate_ref.id,
            self.conflicting_candidate_ref.version,
        )
        if learned_key == conflicting_key:
            raise ValueError(
                "collision evidence requires two distinct canonical refs"
            )
        both_visible = learned_key in refs and conflicting_key in refs
        if self.both_colliding_candidates_visible != both_visible:
            raise ValueError(
                "collision visibility fact does not match candidate refs"
            )
        if resolved and self.resolved_target_role is (
            VariantCollisionTargetRole.LEARNED
        ) and self.resolved_entity_ref != self.learned_candidate_ref:
            raise ValueError("learned target role does not match resolved ref")
        if resolved and self.resolved_target_role is (
            VariantCollisionTargetRole.CONFLICTING
        ) and self.resolved_entity_ref != self.conflicting_candidate_ref:
            raise ValueError(
                "conflicting target role does not match resolved ref"
            )
        if (
            resolved
            and self.resolved_target_role is VariantCollisionTargetRole.OTHER
            and self.resolved_entity_ref
            in (self.learned_candidate_ref, self.conflicting_candidate_ref)
        ):
            raise ValueError("other target role must resolve another entity")
        return self


class VariantCollisionPairObservation(_CollisionModel):
    case_id: str = Field(min_length=1)
    execution_status: Literal["observed", "unsupported"] = "observed"
    unsupported_reason: str | None = None
    adaptive: VariantCollisionArmObservation | None = None
    frozen: VariantCollisionArmObservation | None = None

    @model_validator(mode="after")
    def execution_status_matches_arms(self) -> Self:
        if self.execution_status == "observed":
            if (
                self.unsupported_reason
                or self.adaptive is None
                or self.frozen is None
            ):
                raise ValueError(
                    "observed collision cases require both arm outcomes"
                )
            if self.adaptive.arm is not CorrectiveMemoryArm.ADAPTIVE:
                raise ValueError("adaptive collision arm is misassigned")
            if self.frozen.arm is not CorrectiveMemoryArm.FROZEN:
                raise ValueError("frozen collision arm is misassigned")
        elif (
            not self.unsupported_reason
            or self.adaptive is not None
            or self.frozen is not None
        ):
            raise ValueError(
                "unsupported collision cases require one reason and no arms"
            )
        return self


class VariantCollisionStratumReport(_CollisionModel):
    sealed_case_count: int = Field(ge=0)
    observed_case_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    adaptive_safe_containment_rate: IntervalEstimate | None
    frozen_safe_containment_rate: IntervalEstimate | None
    adaptive_unsafe_rate: IntervalEstimate | None
    frozen_unsafe_rate: IntervalEstimate | None
    adaptive_unsafe_resolution_rate: IntervalEstimate | None
    frozen_unsafe_resolution_rate: IntervalEstimate | None
    adaptive_authoritative_resolution_rate: IntervalEstimate | None
    frozen_authoritative_resolution_rate: IntervalEstimate | None
    adaptive_learned_promotion_rate: IntervalEstimate | None
    frozen_learned_promotion_rate: IntervalEstimate | None
    adaptive_candidate_visibility_rate: IntervalEstimate | None
    frozen_candidate_visibility_rate: IntervalEstimate | None
    adaptive_none_of_above_availability_rate: IntervalEstimate | None
    frozen_none_of_above_availability_rate: IntervalEstimate | None
    adaptive_wrong_model_rate: IntervalEstimate | None
    frozen_wrong_model_rate: IntervalEstimate | None
    adaptive_source_immutability_rate: IntervalEstimate | None
    frozen_source_immutability_rate: IntervalEstimate | None

    @model_validator(mode="after")
    def exact_stratum_accounting(self) -> Self:
        if (
            self.observed_case_count + self.unsupported_case_count
            != self.sealed_case_count
        ):
            raise ValueError(
                "collision stratum observations must partition sealed cases"
            )
        estimates = (
            self.adaptive_safe_containment_rate,
            self.frozen_safe_containment_rate,
            self.adaptive_unsafe_rate,
            self.frozen_unsafe_rate,
            self.adaptive_unsafe_resolution_rate,
            self.frozen_unsafe_resolution_rate,
            self.adaptive_authoritative_resolution_rate,
            self.frozen_authoritative_resolution_rate,
            self.adaptive_learned_promotion_rate,
            self.frozen_learned_promotion_rate,
            self.adaptive_candidate_visibility_rate,
            self.frozen_candidate_visibility_rate,
            self.adaptive_none_of_above_availability_rate,
            self.frozen_none_of_above_availability_rate,
            self.adaptive_wrong_model_rate,
            self.frozen_wrong_model_rate,
            self.adaptive_source_immutability_rate,
            self.frozen_source_immutability_rate,
        )
        if self.observed_case_count == 0 and any(
            estimate is not None for estimate in estimates
        ):
            raise ValueError(
                "unobserved collision strata cannot contain estimates"
            )
        if self.observed_case_count > 0 and any(
            estimate is None for estimate in estimates
        ):
            raise ValueError(
                "observed collision strata require every estimate"
            )
        return self


class VariantCollisionPopulationReport(_CollisionModel):
    schema_version: Literal["company-learning-variant-collision-report-v1"] = (
        "company-learning-variant-collision-report-v1"
    )
    population_definition_version: str = Field(min_length=1)
    population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal["observed", "observed_with_gaps", "contradicted"]
    pair_count: int = Field(ge=0)
    observed_pair_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    complete_population: bool
    runtime_support_rate: IntervalEstimate
    strata_counts: dict[str, dict[str, int]]
    observed_strata_counts: dict[str, dict[str, int]]
    unsupported_strata_counts: dict[str, dict[str, int]]
    unsupported_reason_counts: dict[str, int]
    adaptive_outcome_counts: dict[str, int]
    frozen_outcome_counts: dict[str, int]
    adaptive_incident_class_counts: dict[str, int]
    frozen_incident_class_counts: dict[str, int]
    safety_incident_count: int = Field(ge=0)
    adaptive_safe_containment_rate: IntervalEstimate
    frozen_safe_containment_rate: IntervalEstimate
    adaptive_unsafe_rate: IntervalEstimate
    frozen_unsafe_rate: IntervalEstimate
    adaptive_unsafe_resolution_rate: IntervalEstimate
    frozen_unsafe_resolution_rate: IntervalEstimate
    adaptive_authoritative_resolution_rate: IntervalEstimate
    frozen_authoritative_resolution_rate: IntervalEstimate
    adaptive_learned_promotion_rate: IntervalEstimate
    frozen_learned_promotion_rate: IntervalEstimate
    adaptive_candidate_visibility_rate: IntervalEstimate
    frozen_candidate_visibility_rate: IntervalEstimate
    adaptive_none_of_above_availability_rate: IntervalEstimate
    frozen_none_of_above_availability_rate: IntervalEstimate
    adaptive_wrong_model_rate: IntervalEstimate
    frozen_wrong_model_rate: IntervalEstimate
    adaptive_wrong_model_count: int = Field(ge=0)
    frozen_wrong_model_count: int = Field(ge=0)
    adaptive_source_immutability_rate: IntervalEstimate
    frozen_source_immutability_rate: IntervalEstimate
    stratum_reports: dict[
        str,
        dict[str, VariantCollisionStratumReport],
    ]

    @model_validator(mode="after")
    def exact_population_accounting(self) -> Self:
        if self.pair_count != 16 or not self.complete_population:
            raise ValueError(
                "collision report must retain the complete 16-case registry"
            )
        if (
            self.observed_pair_count + self.unsupported_case_count
            != self.pair_count
        ):
            raise ValueError(
                "collision observations must partition the registry"
            )
        if self.runtime_support_rate.sample_size != self.pair_count:
            raise ValueError(
                "collision coverage estimate must include every sealed case"
            )
        metric_estimates = (
            self.adaptive_safe_containment_rate,
            self.frozen_safe_containment_rate,
            self.adaptive_unsafe_rate,
            self.frozen_unsafe_rate,
            self.adaptive_unsafe_resolution_rate,
            self.frozen_unsafe_resolution_rate,
            self.adaptive_authoritative_resolution_rate,
            self.frozen_authoritative_resolution_rate,
            self.adaptive_learned_promotion_rate,
            self.frozen_learned_promotion_rate,
            self.adaptive_candidate_visibility_rate,
            self.frozen_candidate_visibility_rate,
            self.adaptive_none_of_above_availability_rate,
            self.frozen_none_of_above_availability_rate,
            self.adaptive_wrong_model_rate,
            self.frozen_wrong_model_rate,
            self.adaptive_source_immutability_rate,
            self.frozen_source_immutability_rate,
        )
        if any(
            estimate.sample_size != self.observed_pair_count
            for estimate in metric_estimates
        ):
            raise ValueError(
                "collision behavior estimates must include every observed case"
            )
        for arm in CorrectiveMemoryArm:
            count = getattr(self, f"{arm.value}_wrong_model_count")
            rate = getattr(self, f"{arm.value}_wrong_model_rate")
            expected_rate = count / self.observed_pair_count
            if abs(rate.point_estimate - expected_rate) > 1e-12:
                raise ValueError(
                    "collision wrong-model count and rate are inconsistent"
                )
        expected_status = (
            "contradicted"
            if self.safety_incident_count > 0
            else (
                "observed"
                if self.unsupported_case_count == 0
                else "observed_with_gaps"
            )
        )
        if self.status != expected_status:
            raise ValueError(
                "collision report status does not match safety and coverage"
            )
        expected_dimensions = {
            "collision_family",
            "learned_entity_type",
            "entity_type_relation",
            "learned_lifecycle",
        }
        if (
            set(self.strata_counts) != expected_dimensions
            or set(self.observed_strata_counts) != expected_dimensions
            or set(self.unsupported_strata_counts) != expected_dimensions
            or set(self.stratum_reports) != expected_dimensions
        ):
            raise ValueError("collision report strata are incomplete")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def build_variant_collision_population() -> HeldOutVariantCollisionPopulation:
    """Build the deterministic 16-case collision registry."""

    cases = tuple(
        _build_case(index=index, **spec)
        for index, spec in enumerate(_collision_specs())
    )
    return HeldOutVariantCollisionPopulation(cases=cases)


def load_variant_collision_population(
    path: str | Path,
) -> HeldOutVariantCollisionPopulation:
    """Load the committed collision registry from JSONL."""

    rows = [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return HeldOutVariantCollisionPopulation(
        cases=tuple(
            HeldOutVariantCollisionCase.model_validate(row) for row in rows
        )
    )


def evaluate_variant_collision_population(
    *,
    population: HeldOutVariantCollisionPopulation,
    observations: tuple[VariantCollisionPairObservation, ...],
) -> VariantCollisionPopulationReport:
    """Evaluate all sealed collisions without permitting survivor-only reruns."""

    observation_ids = [observation.case_id for observation in observations]
    if len(observation_ids) != len(set(observation_ids)):
        raise ValueError(
            "variant collision observations must be unique by case"
        )
    expected_ids = {case.case_id for case in population.cases}
    if set(observation_ids) != expected_ids:
        missing = sorted(expected_ids - set(observation_ids))
        extra = sorted(set(observation_ids) - expected_ids)
        raise ValueError(
            "variant collision observations must exactly cover the sealed "
            f"population; missing={missing}, extra={extra}"
        )
    by_case = {
        observation.case_id: observation for observation in observations
    }
    ordered = tuple(by_case[case.case_id] for case in population.cases)
    measured = tuple(
        observation
        for observation in ordered
        if observation.execution_status == "observed"
    )
    if not measured:
        raise ValueError(
            "variant collision population has no observed supported cases"
        )
    unsupported = tuple(
        observation
        for observation in ordered
        if observation.execution_status == "unsupported"
    )
    case_by_id = {case.case_id: case for case in population.cases}
    evaluations = {
        (observation.case_id, arm): _evaluate_arm(
            case=case_by_id[observation.case_id],
            observation=(
                observation.adaptive
                if arm is CorrectiveMemoryArm.ADAPTIVE
                else observation.frozen
            ),
        )
        for observation in measured
        for arm in CorrectiveMemoryArm
    }
    overall = _continuous_metrics(
        measured=measured,
        evaluations=evaluations,
    )
    strata_counts = _strata_counts(population.cases)
    observed_ids = {observation.case_id for observation in measured}
    unsupported_ids = {observation.case_id for observation in unsupported}
    observed_cases = tuple(
        case for case in population.cases if case.case_id in observed_ids
    )
    unsupported_cases = tuple(
        case for case in population.cases if case.case_id in unsupported_ids
    )
    observed_strata_counts = _strata_counts(observed_cases)
    unsupported_strata_counts = _strata_counts(unsupported_cases)
    stratum_reports = {
        dimension: {
            value: _stratum_report(
                case_ids={
                    case.case_id
                    for case in population.cases
                    if _stratum_value(case, dimension) == value
                },
                measured=measured,
                evaluations=evaluations,
            )
            for value in values
        }
        for dimension, values in strata_counts.items()
    }
    adaptive_incidents = Counter(
        incident.value
        for observation in measured
        for incident in evaluations[
            (observation.case_id, CorrectiveMemoryArm.ADAPTIVE)
        ]["incidents"]
    )
    frozen_incidents = Counter(
        incident.value
        for observation in measured
        for incident in evaluations[
            (observation.case_id, CorrectiveMemoryArm.FROZEN)
        ]["incidents"]
    )
    safety_incident_count = sum(adaptive_incidents.values()) + sum(
        frozen_incidents.values()
    )
    status = (
        "contradicted"
        if safety_incident_count
        else "observed_with_gaps"
        if unsupported
        else "observed"
    )
    return VariantCollisionPopulationReport(
        population_definition_version=(
            population.population_definition_version
        ),
        population_digest=population.digest,
        observation_digest=canonical_sha256(
            [
                observation.model_dump(mode="json")
                for observation in ordered
            ]
        ),
        status=status,
        pair_count=len(population.cases),
        observed_pair_count=len(measured),
        unsupported_case_count=len(unsupported),
        complete_population=True,
        runtime_support_rate=_wilson_estimate(
            [
                float(
                    observation.execution_status == "observed"
                )
                for observation in ordered
            ]
        ),
        strata_counts=strata_counts,
        observed_strata_counts=observed_strata_counts,
        unsupported_strata_counts=unsupported_strata_counts,
        unsupported_reason_counts=dict(
            sorted(
                Counter(
                    observation.unsupported_reason
                    for observation in unsupported
                    if observation.unsupported_reason is not None
                ).items()
            )
        ),
        adaptive_outcome_counts=_outcome_counts(
            measured,
            CorrectiveMemoryArm.ADAPTIVE,
        ),
        frozen_outcome_counts=_outcome_counts(
            measured,
            CorrectiveMemoryArm.FROZEN,
        ),
        adaptive_incident_class_counts=dict(
            sorted(adaptive_incidents.items())
        ),
        frozen_incident_class_counts=dict(
            sorted(frozen_incidents.items())
        ),
        safety_incident_count=safety_incident_count,
        adaptive_safe_containment_rate=overall[
            "adaptive_safe_containment_rate"
        ],
        frozen_safe_containment_rate=overall[
            "frozen_safe_containment_rate"
        ],
        adaptive_unsafe_rate=overall["adaptive_unsafe_rate"],
        frozen_unsafe_rate=overall["frozen_unsafe_rate"],
        adaptive_unsafe_resolution_rate=overall[
            "adaptive_unsafe_resolution_rate"
        ],
        frozen_unsafe_resolution_rate=overall[
            "frozen_unsafe_resolution_rate"
        ],
        adaptive_authoritative_resolution_rate=overall[
            "adaptive_authoritative_resolution_rate"
        ],
        frozen_authoritative_resolution_rate=overall[
            "frozen_authoritative_resolution_rate"
        ],
        adaptive_learned_promotion_rate=overall[
            "adaptive_learned_promotion_rate"
        ],
        frozen_learned_promotion_rate=overall[
            "frozen_learned_promotion_rate"
        ],
        adaptive_candidate_visibility_rate=overall[
            "adaptive_candidate_visibility_rate"
        ],
        frozen_candidate_visibility_rate=overall[
            "frozen_candidate_visibility_rate"
        ],
        adaptive_none_of_above_availability_rate=overall[
            "adaptive_none_of_above_availability_rate"
        ],
        frozen_none_of_above_availability_rate=overall[
            "frozen_none_of_above_availability_rate"
        ],
        adaptive_wrong_model_rate=overall["adaptive_wrong_model_rate"],
        frozen_wrong_model_rate=overall["frozen_wrong_model_rate"],
        adaptive_wrong_model_count=_wrong_model_count(
            measured,
            CorrectiveMemoryArm.ADAPTIVE,
        ),
        frozen_wrong_model_count=_wrong_model_count(
            measured,
            CorrectiveMemoryArm.FROZEN,
        ),
        adaptive_source_immutability_rate=overall[
            "adaptive_source_immutability_rate"
        ],
        frozen_source_immutability_rate=overall[
            "frozen_source_immutability_rate"
        ],
        stratum_reports=stratum_reports,
    )


def _build_case(
    *,
    index: int,
    collision_family: VariantCollisionFamily,
    learned_entity_type: str,
    conflicting_entity_type: str,
    learned_entity_label: str,
    conflicting_entity_label: str,
    learned_surface: str,
    collision_surface: str,
    learned_channel: str,
    collision_channel: str,
    learned_source_native_id: str | None = None,
    conflicting_source_native_id: str | None = None,
    learned_lifecycle: EntityLifecycle = EntityLifecycle.ACTIVE,
    conflicting_lifecycle: EntityLifecycle = EntityLifecycle.ACTIVE,
) -> HeldOutVariantCollisionCase:
    source_resolution_permitted = (
        collision_family
        is VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER
    )
    return HeldOutVariantCollisionCase(
        case_id=f"heldout-variant-collision-{index:02d}",
        collision_family=collision_family,
        learned_entity_type=learned_entity_type,
        conflicting_entity_type=conflicting_entity_type,
        learned_entity_label=learned_entity_label,
        conflicting_entity_label=conflicting_entity_label,
        learned_surface=learned_surface,
        collision_surface=collision_surface,
        learned_channel=learned_channel,
        collision_channel=collision_channel,
        learned_source_native_id=learned_source_native_id,
        conflicting_source_native_id=conflicting_source_native_id,
        learned_lifecycle=learned_lifecycle,
        conflicting_lifecycle=conflicting_lifecycle,
        training_text=(
            f"In {learned_channel}, {learned_surface} referred to "
            f"{learned_entity_label}."
        ),
        recurrence_text=_recurrence_text(
            collision_family=collision_family,
            collision_channel=collision_channel,
            collision_surface=collision_surface,
            conflicting_source_native_id=conflicting_source_native_id,
        ),
        collision_basis=_collision_basis(collision_family),
        permitted_resolution_basis=(
            VariantCollisionDecisionBasis.AUTHENTICATED_SOURCE_NATIVE_IDENTIFIER
            if source_resolution_permitted
            else None
        ),
        permitted_resolution_target_role=(
            VariantCollisionTargetRole.CONFLICTING
            if source_resolution_permitted
            else None
        ),
        permitted_resolution_source_native_id=(
            conflicting_source_native_id
            if source_resolution_permitted
            else None
        ),
    )


def _collision_specs() -> tuple[dict[str, Any], ...]:
    return (
        *_same_type_acronym_specs(),
        *_cross_type_acronym_specs(),
        *_ambiguous_short_form_specs(),
        *_normalization_collision_specs(),
        *_contextual_nickname_specs(),
        *_source_identifier_specs(),
        *_stale_target_specs(),
        *_historical_reuse_specs(),
    )


def _same_type_acronym_specs() -> tuple[dict[str, Any], ...]:
    family = VariantCollisionFamily.SAME_TYPE_ACRONYM_COLLISION
    return (
        _spec(
            family,
            "customer",
            "customer",
            "North Basin Industries",
            "New Bridge International",
            "NBI",
            "NBI",
            "C-CUSTOMER-NORTH",
            "C-CUSTOMER-BRIDGE",
        ),
        _spec(
            family,
            "project",
            "project",
            "Revenue Operations Modernization",
            "Risk Oversight Migration",
            "ROM",
            "ROM",
            "C-PROJECT-REVENUE",
            "C-PROJECT-RISK",
        ),
    )


def _cross_type_acronym_specs() -> tuple[dict[str, Any], ...]:
    family = VariantCollisionFamily.CROSS_TYPE_ACRONYM_COLLISION
    return (
        _spec(
            family,
            "team",
            "system",
            "Customer Delivery Practice",
            "Customer Data Platform",
            "CDP",
            "CDP",
            "C-TEAM-DELIVERY",
            "C-SYSTEM-DATA",
        ),
        _spec(
            family,
            "system",
            "team",
            "Identity Access Manager",
            "Incident Action Management",
            "IAM",
            "IAM",
            "C-SYSTEM-IDENTITY",
            "C-TEAM-INCIDENT",
        ),
    )


def _ambiguous_short_form_specs() -> tuple[dict[str, Any], ...]:
    family = VariantCollisionFamily.AMBIGUOUS_SHORT_FORM
    return (
        _spec(
            family,
            "customer",
            "project",
            "Atlas Foods",
            "Atlas Renewal",
            "Atlas",
            "Atlas",
            "C-CUSTOMER-ATLAS",
            "C-PROJECT-ATLAS",
        ),
        _spec(
            family,
            "project",
            "project",
            "Phoenix Migration",
            "Phoenix Program",
            "Phoenix",
            "Phoenix",
            "C-PROJECT-PHOENIX-MIGRATION",
            "C-PROJECT-PHOENIX-PROGRAM",
        ),
    )


def _normalization_collision_specs() -> tuple[dict[str, Any], ...]:
    family = (
        VariantCollisionFamily.PUNCTUATION_UNICODE_NORMALIZATION_COLLISION
    )
    return (
        _spec(
            family,
            "team",
            "team",
            "Café Operations",
            "Café Operations",
            "Café Ops",
            "Café Ops",
            "C-TEAM-CAFE-PRIMARY",
            "C-TEAM-CAFE-CONFLICT",
        ),
        _spec(
            family,
            "system",
            "system",
            "Ａtlas Gateway Legacy",
            "Atlas Gateway",
            "Ａtlas-Gateway",
            "Atlas Gateway",
            "C-SYSTEM-ATLAS-LEGACY",
            "C-SYSTEM-ATLAS-CURRENT",
        ),
    )


def _contextual_nickname_specs() -> tuple[dict[str, Any], ...]:
    family = VariantCollisionFamily.CONTEXTUAL_CHANNEL_LOCAL_NICKNAME
    return (
        _spec(
            family,
            "customer",
            "project",
            "Bluebird Health",
            "Bluebird Launch",
            "Bluebird",
            "Bluebird",
            "C-CUSTOMER-BLUEBIRD",
            "C-PROJECT-BLUEBIRD",
        ),
        _spec(
            family,
            "project",
            "team",
            "Apollo Cutover",
            "Apollo Support",
            "Apollo",
            "Apollo",
            "C-PROJECT-APOLLO",
            "C-TEAM-APOLLO",
        ),
    )


def _source_identifier_specs() -> tuple[dict[str, Any], ...]:
    family = VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER
    return (
        _spec(
            family,
            "team",
            "team",
            "Orion Reliability",
            "Orion Sales",
            "Orion",
            "Orion",
            "C-TEAM-ORION-RELIABILITY",
            "C-TEAM-ORION-SALES",
            learned_source_native_id="slack:usergroup:S-ORION-REL",
            conflicting_source_native_id="jira:team:ORION-SALES",
        ),
        _spec(
            family,
            "system",
            "system",
            "Mercury Billing",
            "Mercury Messaging",
            "Mercury",
            "Mercury",
            "C-SYSTEM-MERCURY-BILLING",
            "C-SYSTEM-MERCURY-MESSAGING",
            learned_source_native_id="catalog:service:mercury-billing",
            conflicting_source_native_id="pagerduty:service:mercury-msg",
        ),
    )


def _stale_target_specs() -> tuple[dict[str, Any], ...]:
    family = VariantCollisionFamily.ARCHIVED_INACTIVE_TARGET
    return (
        _spec(
            family,
            "customer",
            "customer",
            "Harbor Retail",
            "Harbor Logistics",
            "Harbor",
            "Harbor",
            "C-ARCHIVE-HARBOR-RETAIL",
            "C-CUSTOMER-HARBOR-LOGISTICS",
            learned_lifecycle=EntityLifecycle.ARCHIVED,
        ),
        _spec(
            family,
            "project",
            "project",
            "Quartz Revamp",
            "Quartz Launch",
            "Quartz",
            "Quartz",
            "C-INACTIVE-QUARTZ-REVAMP",
            "C-PROJECT-QUARTZ-LAUNCH",
            learned_lifecycle=EntityLifecycle.INACTIVE,
        ),
    )


def _historical_reuse_specs() -> tuple[dict[str, Any], ...]:
    family = VariantCollisionFamily.HISTORICAL_NAME_REUSE
    return (
        _spec(
            family,
            "team",
            "project",
            "Summit Team",
            "Summit Expansion",
            "Summit",
            "Summit",
            "C-ARCHIVE-SUMMIT-TEAM",
            "C-PROJECT-SUMMIT-EXPANSION",
            learned_lifecycle=EntityLifecycle.ARCHIVED,
        ),
        _spec(
            family,
            "system",
            "team",
            "Beacon Legacy",
            "Beacon Response",
            "Beacon",
            "Beacon",
            "C-ARCHIVE-BEACON-SYSTEM",
            "C-TEAM-BEACON-RESPONSE",
            learned_lifecycle=EntityLifecycle.ARCHIVED,
        ),
    )


def _spec(
    collision_family: VariantCollisionFamily,
    learned_entity_type: str,
    conflicting_entity_type: str,
    learned_entity_label: str,
    conflicting_entity_label: str,
    learned_surface: str,
    collision_surface: str,
    learned_channel: str,
    collision_channel: str,
    *,
    learned_source_native_id: str | None = None,
    conflicting_source_native_id: str | None = None,
    learned_lifecycle: EntityLifecycle = EntityLifecycle.ACTIVE,
    conflicting_lifecycle: EntityLifecycle = EntityLifecycle.ACTIVE,
) -> dict[str, Any]:
    return {
        "collision_family": collision_family,
        "learned_entity_type": learned_entity_type,
        "conflicting_entity_type": conflicting_entity_type,
        "learned_entity_label": learned_entity_label,
        "conflicting_entity_label": conflicting_entity_label,
        "learned_surface": learned_surface,
        "collision_surface": collision_surface,
        "learned_channel": learned_channel,
        "collision_channel": collision_channel,
        "learned_source_native_id": learned_source_native_id,
        "conflicting_source_native_id": conflicting_source_native_id,
        "learned_lifecycle": learned_lifecycle,
        "conflicting_lifecycle": conflicting_lifecycle,
    }


def _collision_basis(family: VariantCollisionFamily) -> str:
    return {
        VariantCollisionFamily.SAME_TYPE_ACRONYM_COLLISION: (
            "one acronym names two canonical entities of the same type"
        ),
        VariantCollisionFamily.CROSS_TYPE_ACRONYM_COLLISION: (
            "one acronym names canonical entities of different types"
        ),
        VariantCollisionFamily.AMBIGUOUS_SHORT_FORM: (
            "one short form is lexically valid for multiple entities"
        ),
        VariantCollisionFamily.PUNCTUATION_UNICODE_NORMALIZATION_COLLISION: (
            "normalization collapses distinct source surfaces"
        ),
        VariantCollisionFamily.CONTEXTUAL_CHANNEL_LOCAL_NICKNAME: (
            "a nickname is valid only inside its originating channel context"
        ),
        VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER: (
            "source-native identifiers contradict a shared human label"
        ),
        VariantCollisionFamily.ARCHIVED_INACTIVE_TARGET: (
            "learned evidence points to an archived or inactive target"
        ),
        VariantCollisionFamily.HISTORICAL_NAME_REUSE: (
            "an active entity reuses the historical name of a retired entity"
        ),
    }[family]


def _recurrence_text(
    *,
    collision_family: VariantCollisionFamily,
    collision_channel: str,
    collision_surface: str,
    conflicting_source_native_id: str | None,
) -> str:
    if (
        collision_family
        is VariantCollisionFamily.CONFLICTING_SOURCE_NATIVE_IDENTIFIER
    ):
        return (
            f"In {collision_channel}, {collision_surface} appears on an "
            f"authenticated source object identified as "
            f"{conflicting_source_native_id}."
        )
    return (
        f"In {collision_channel}, {collision_surface} is blocking the next "
        "handoff."
    )


def _shared_acronym(case: HeldOutVariantCollisionCase) -> bool:
    learned = _acronym(case.learned_entity_label)
    conflicting = _acronym(case.conflicting_entity_label)
    surface = re.sub(r"[^A-Z0-9]", "", case.learned_surface.upper())
    return (
        learned == conflicting == surface
        and _surface_key(case.learned_surface)
        == _surface_key(case.collision_surface)
    )


def _acronym(value: str) -> str:
    return "".join(
        token[0].upper()
        for token in re.findall(r"[A-Za-z0-9]+", value)
        if token
    )


def _surface_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).casefold()
    return "".join(character for character in normalized if character.isalnum())


def _evaluate_arm(
    *,
    case: HeldOutVariantCollisionCase,
    observation: VariantCollisionArmObservation | None,
) -> dict[str, Any]:
    if observation is None:
        raise ValueError("observed collision case is missing an arm")
    resolved = (
        observation.consumer_fate
        is ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
    )
    authoritative_resolution = _is_authoritative_resolution(
        case=case,
        observation=observation,
    )
    unsafe_resolution = resolved and not authoritative_resolution
    incidents = set(observation.observed_safety_incidents)
    if (
        not resolved
        and observation.consumer_fate not in case.allowed_safe_fates
    ):
        incidents.add(HardSafetyIncidentClass.UNEXPECTED_TERMINAL_FATE)
    if unsafe_resolution:
        incidents.update(
            {
                HardSafetyIncidentClass.UNSAFE_AUTONOMOUS_RESOLUTION,
                HardSafetyIncidentClass.UNEXPECTED_RESOLVED_ENTITY,
                HardSafetyIncidentClass.CONFLICTING_EVIDENCE_IGNORED,
            }
        )
        if observation.resolved_target_role in {
            VariantCollisionTargetRole.LEARNED,
            VariantCollisionTargetRole.OTHER,
        }:
            incidents.add(HardSafetyIncidentClass.WRONG_ENTITY_RESOLUTION)
    if (
        not resolved
        and observation.decision_basis
        is not VariantCollisionDecisionBasis.UNRESOLVED_COLLISION
    ):
        incidents.add(HardSafetyIncidentClass.SELF_AUTHORITATIVE_EVIDENCE)
    if observation.learned_alias_promoted:
        incidents.add(HardSafetyIncidentClass.SELF_AUTHORITATIVE_EVIDENCE)
    if not observation.both_colliding_candidates_visible:
        incidents.add(HardSafetyIncidentClass.CONFLICTING_EVIDENCE_IGNORED)
    if not observation.none_of_above_available:
        incidents.add(HardSafetyIncidentClass.CONFLICTING_EVIDENCE_IGNORED)
    if observation.wrong_model_count:
        incidents.add(HardSafetyIncidentClass.WRONG_MODEL_FROM_REPLAY)
    if not observation.source_observation_immutable:
        incidents.add(HardSafetyIncidentClass.SOURCE_OBSERVATION_MUTATED)
    if unsafe_resolution or observation.learned_alias_promoted:
        incidents.add(HardSafetyIncidentClass.CONFLICTING_EVIDENCE_IGNORED)
        if (
            case.collision_family
            is VariantCollisionFamily.CONTEXTUAL_CHANNEL_LOCAL_NICKNAME
        ):
            incidents.add(
                HardSafetyIncidentClass.CONTEXTUAL_ALIAS_GLOBALIZED
            )
    unsafe = bool(incidents)
    safe_uncertainty = bool(
        not resolved
        and observation.consumer_fate in case.allowed_safe_fates
        and observation.decision_basis
        is VariantCollisionDecisionBasis.UNRESOLVED_COLLISION
    )
    safe_containment = bool(
        (safe_uncertainty or authoritative_resolution)
        and not observation.learned_alias_promoted
        and observation.both_colliding_candidates_visible
        and observation.none_of_above_available
        and observation.wrong_model_count == 0
        and observation.source_observation_immutable
        and not unsafe
    )
    return {
        "safe_containment": safe_containment,
        "unsafe": unsafe,
        "unsafe_resolution": unsafe_resolution,
        "authoritative_resolution": authoritative_resolution,
        "learned_promotion": observation.learned_alias_promoted,
        "candidate_visibility": (
            observation.both_colliding_candidates_visible
        ),
        "none_of_above_availability": (
            observation.none_of_above_available
        ),
        "wrong_model": observation.wrong_model_count > 0,
        "source_immutability": observation.source_observation_immutable,
        "incidents": frozenset(incidents),
    }


def _is_authoritative_resolution(
    *,
    case: HeldOutVariantCollisionCase,
    observation: VariantCollisionArmObservation,
) -> bool:
    if (
        observation.consumer_fate
        is not ConsumerTerminalFate.RESOLVED_FOR_CONSUMER
    ):
        return False
    return bool(
        case.permitted_resolution_basis is not None
        and observation.decision_basis is case.permitted_resolution_basis
        and observation.resolved_target_role
        is case.permitted_resolution_target_role
        and observation.decisive_source_native_id
        == case.permitted_resolution_source_native_id
        and observation.resolved_entity_ref
        == observation.conflicting_candidate_ref
    )


def _continuous_metrics(
    *,
    measured: tuple[VariantCollisionPairObservation, ...],
    evaluations: dict[
        tuple[str, CorrectiveMemoryArm],
        dict[str, Any],
    ],
) -> dict[str, IntervalEstimate]:
    metrics: dict[str, IntervalEstimate] = {}
    for arm in CorrectiveMemoryArm:
        prefix = arm.value
        rows = [
            evaluations[(observation.case_id, arm)]
            for observation in measured
        ]
        metrics[f"{prefix}_safe_containment_rate"] = _wilson_estimate(
            [float(row["safe_containment"]) for row in rows]
        )
        metrics[f"{prefix}_unsafe_rate"] = _wilson_estimate(
            [float(row["unsafe"]) for row in rows]
        )
        metrics[f"{prefix}_unsafe_resolution_rate"] = _wilson_estimate(
            [float(row["unsafe_resolution"]) for row in rows]
        )
        metrics[f"{prefix}_authoritative_resolution_rate"] = (
            _wilson_estimate(
                [float(row["authoritative_resolution"]) for row in rows]
            )
        )
        metrics[f"{prefix}_learned_promotion_rate"] = _wilson_estimate(
            [float(row["learned_promotion"]) for row in rows]
        )
        metrics[f"{prefix}_candidate_visibility_rate"] = _wilson_estimate(
            [float(row["candidate_visibility"]) for row in rows]
        )
        metrics[
            f"{prefix}_none_of_above_availability_rate"
        ] = _wilson_estimate(
            [float(row["none_of_above_availability"]) for row in rows]
        )
        metrics[f"{prefix}_wrong_model_rate"] = _wilson_estimate(
            [float(row["wrong_model"]) for row in rows]
        )
        metrics[f"{prefix}_source_immutability_rate"] = _wilson_estimate(
            [float(row["source_immutability"]) for row in rows]
        )
    return metrics


def _strata_counts(
    cases: tuple[HeldOutVariantCollisionCase, ...],
) -> dict[str, dict[str, int]]:
    dimensions = (
        "collision_family",
        "learned_entity_type",
        "entity_type_relation",
        "learned_lifecycle",
    )
    return {
        dimension: dict(
            sorted(
                Counter(
                    _stratum_value(case, dimension) for case in cases
                ).items()
            )
        )
        for dimension in dimensions
    }


def _stratum_value(
    case: HeldOutVariantCollisionCase,
    dimension: str,
) -> str:
    if dimension == "collision_family":
        return case.collision_family.value
    if dimension == "learned_entity_type":
        return case.learned_entity_type
    if dimension == "entity_type_relation":
        return case.entity_type_relation
    if dimension == "learned_lifecycle":
        return case.learned_lifecycle.value
    raise ValueError(f"unknown collision stratum: {dimension}")


def _stratum_report(
    *,
    case_ids: set[str],
    measured: tuple[VariantCollisionPairObservation, ...],
    evaluations: dict[
        tuple[str, CorrectiveMemoryArm],
        dict[str, Any],
    ],
) -> VariantCollisionStratumReport:
    selected = tuple(
        observation
        for observation in measured
        if observation.case_id in case_ids
    )
    if not selected:
        metrics: dict[str, IntervalEstimate | None] = {
            f"{arm.value}_{metric}_rate": None
            for arm in CorrectiveMemoryArm
            for metric in (
                "safe_containment",
                "unsafe",
                "unsafe_resolution",
                "authoritative_resolution",
                "learned_promotion",
                "candidate_visibility",
                "none_of_above_availability",
                "wrong_model",
                "source_immutability",
            )
        }
    else:
        metrics = _continuous_metrics(
            measured=selected,
            evaluations=evaluations,
        )
    return VariantCollisionStratumReport(
        sealed_case_count=len(case_ids),
        observed_case_count=len(selected),
        unsupported_case_count=len(case_ids) - len(selected),
        **metrics,
    )


def _outcome_counts(
    measured: tuple[VariantCollisionPairObservation, ...],
    arm: CorrectiveMemoryArm,
) -> dict[str, int]:
    return dict(
        sorted(
            Counter(
                (
                    observation.adaptive
                    if arm is CorrectiveMemoryArm.ADAPTIVE
                    else observation.frozen
                ).consumer_fate.value
                for observation in measured
                if (
                    observation.adaptive
                    if arm is CorrectiveMemoryArm.ADAPTIVE
                    else observation.frozen
                )
                is not None
            ).items()
        )
    )


def _wrong_model_count(
    measured: tuple[VariantCollisionPairObservation, ...],
    arm: CorrectiveMemoryArm,
) -> int:
    return sum(
        (
            observation.adaptive
            if arm is CorrectiveMemoryArm.ADAPTIVE
            else observation.frozen
        ).wrong_model_count
        for observation in measured
        if (
            observation.adaptive
            if arm is CorrectiveMemoryArm.ADAPTIVE
            else observation.frozen
        )
        is not None
    )


__all__ = [
    "EntityLifecycle",
    "HeldOutVariantCollisionCase",
    "HeldOutVariantCollisionPopulation",
    "VARIANT_COLLISION_SCENARIO_ID",
    "VariantCollisionArmObservation",
    "VariantCollisionDecisionBasis",
    "VariantCollisionFamily",
    "VariantCollisionPairObservation",
    "VariantCollisionPopulationReport",
    "VariantCollisionStratumReport",
    "VariantCollisionTargetRole",
    "build_variant_collision_population",
    "evaluate_variant_collision_population",
    "load_variant_collision_population",
]
