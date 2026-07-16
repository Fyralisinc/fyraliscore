"""Gold-first evaluation for boundaryless Slack context reconstruction."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path
from statistics import fmean
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import SufficiencyDisposition


class _GoldModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class SlackGoldFamily(StrEnum):
    THREAD_ROOT_REPLIES = "thread_root_replies"
    EDIT_REVISION = "edit_revision"
    DELETION_TOMBSTONE = "deletion_tombstone"
    REACTION_EVIDENCE = "reaction_evidence"
    CROSS_THREAD_DEPENDENCY = "cross_thread_dependency"
    CROSS_CHANNEL_DEPENDENCY = "cross_channel_dependency"
    LONG_RANGE_RECURRENCE = "long_range_recurrence"
    PRONOUN_COREFERENCE = "pronoun_coreference"
    HIGH_SIMILARITY_CONTAMINATION = "high_similarity_contamination"


class SlackRevisionFate(StrEnum):
    CURRENT = "current"
    SUPERSEDED = "superseded"
    TOMBSTONE = "tombstone"
    REACTION_EVIDENCE = "reaction_evidence"
    UNSUPPORTED = "unsupported"


_SAFE_INSUFFICIENT_DISPOSITIONS = {
    SufficiencyDisposition.NEEDS_EXPANSION,
    SufficiencyDisposition.NEEDS_CLARIFICATION,
    SufficiencyDisposition.BUDGET_EXHAUSTED,
    SufficiencyDisposition.NON_IDENTIFIABLE,
}


class SlackGoldEvent(_GoldModel):
    event_revision_id: str = Field(min_length=1)
    payload: dict[str, Any]
    token_count: int = Field(ge=0)


class SlackReconstructionGoldCase(_GoldModel):
    case_id: str = Field(min_length=1)
    case_version: str = Field(min_length=1)
    family: SlackGoldFamily
    phrase: str = Field(min_length=1)
    focal_event_revision_id: str = Field(min_length=1)
    events: tuple[SlackGoldEvent, ...] = Field(min_length=1)
    candidate_event_revision_ids: tuple[str, ...] = Field(min_length=1)
    acceptable_sufficient_sets: tuple[tuple[str, ...], ...] = ()
    forbidden_event_revision_ids: tuple[str, ...] = ()
    required_topology_edge_ids: tuple[str, ...] = ()
    expected_revision_fates: dict[str, SlackRevisionFate]
    allowed_dispositions: tuple[SufficiencyDisposition, ...] = Field(
        min_length=1
    )
    insufficient_evidence: bool
    long_range: bool = False
    cross_channel: bool = False
    max_selected_events: int = Field(ge=1)
    max_selected_tokens: int = Field(ge=1)
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def sealed_population_is_coherent(self) -> Self:
        event_ids = [event.event_revision_id for event in self.events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("gold Slack event revisions must be unique")
        candidates = set(self.candidate_event_revision_ids)
        if len(candidates) != len(self.candidate_event_revision_ids):
            raise ValueError("gold candidate event revisions must be unique")
        if set(event_ids) != candidates:
            raise ValueError(
                "gold events must exactly equal the sealed candidate population"
            )
        if self.focal_event_revision_id not in candidates:
            raise ValueError("focal event must belong to the candidate population")
        forbidden = set(self.forbidden_event_revision_ids)
        if not forbidden <= candidates:
            raise ValueError("forbidden context must belong to the candidate population")
        if self.insufficient_evidence and self.acceptable_sufficient_sets:
            raise ValueError(
                "insufficient-evidence gold cannot seal a sufficient context set"
            )
        if not self.insufficient_evidence and not self.acceptable_sufficient_sets:
            raise ValueError(
                "sufficient gold requires at least one acceptable context set"
            )
        for sufficient_set in self.acceptable_sufficient_sets:
            if not sufficient_set:
                raise ValueError("acceptable sufficient context sets cannot be empty")
            if not set(sufficient_set) <= candidates:
                raise ValueError(
                    "acceptable sufficient context must belong to candidates"
                )
            if forbidden.intersection(sufficient_set):
                raise ValueError(
                    "one event cannot be both sufficient gold and contamination"
                )
        if not set(self.expected_revision_fates) <= candidates:
            raise ValueError("revision fate gold must name candidate revisions")
        if self.insufficient_evidence and not set(
            self.allowed_dispositions
        ) <= _SAFE_INSUFFICIENT_DISPOSITIONS:
            raise ValueError(
                "insufficient evidence may only allow safe non-resolution dispositions"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def token_counts(self) -> dict[str, int]:
        return {
            event.event_revision_id: event.token_count
            for event in self.events
        }


class SlackReconstructionObservation(_GoldModel):
    case_id: str = Field(min_length=1)
    candidate_event_revision_ids: tuple[str, ...] = ()
    selected_event_revision_ids: tuple[str, ...] = ()
    selected_topology_edge_ids: tuple[str, ...] = ()
    revision_fates: dict[str, SlackRevisionFate] = Field(default_factory=dict)
    disposition: SufficiencyDisposition
    selected_token_count: int = Field(ge=0)
    unsupported_reasons: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def observed_collections_are_unique(self) -> Self:
        for name, values in (
            ("candidate event revisions", self.candidate_event_revision_ids),
            ("selected event revisions", self.selected_event_revision_ids),
            ("selected topology edges", self.selected_topology_edge_ids),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"observed {name} must be unique")
        return self


class SlackReconstructionCaseAssessment(_GoldModel):
    case_id: str
    family: SlackGoldFamily
    selected_event_count: int = Field(ge=0)
    relevant_selected_event_count: int = Field(ge=0)
    sufficient_set_recall: float | None
    sufficient_set_complete: bool | None
    selected_context_precision: float | None
    contamination_count: int = Field(ge=0)
    contamination_rate: float | None
    candidate_population_match: bool
    selected_within_candidates: bool
    candidate_reconstructable: bool
    topology_recall: float | None
    revision_fate_correctness: float | None
    disposition_allowed: bool
    budget_adherent: bool
    safe_abstention_under_insufficiency: bool | None
    supported: bool
    correct: bool
    unsupported_reasons: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)


class SlackReconstructionMetrics(_GoldModel):
    case_count: int = Field(ge=0)
    supported_case_count: int = Field(ge=0)
    supported_case_rate: float | None
    correct_case_count: int = Field(ge=0)
    correct_case_rate: float | None
    mean_sufficient_set_recall: float | None
    complete_sufficient_set_rate: float | None
    selected_context_precision: float | None
    contamination_rate: float | None
    reconstructability_rate: float | None
    mean_topology_recall: float | None
    edit_delete_correctness_rate: float | None
    long_range_recall: float | None
    cross_channel_recall: float | None
    budget_adherence_rate: float | None
    abstention_under_insufficiency_rate: float | None
    family_metrics: dict[str, dict[str, int | float | None]]


class SlackReconstructionReport(_GoldModel):
    schema_version: str = "slack-reconstruction-gold-report-v1"
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    gold_manifest_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    observation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: str
    metrics: SlackReconstructionMetrics
    assessments: tuple[SlackReconstructionCaseAssessment, ...]
    proof_gaps: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def load_slack_reconstruction_gold(
    path: Path | str,
) -> tuple[SlackReconstructionGoldCase, ...]:
    cases: list[SlackReconstructionGoldCase] = []
    for line_number, line in enumerate(
        Path(path).read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"invalid Slack gold JSONL at line {line_number}"
            ) from exc
        cases.append(SlackReconstructionGoldCase.model_validate(payload))
    case_ids = [case.case_id for case in cases]
    if len(case_ids) != len(set(case_ids)):
        raise ValueError("Slack gold case IDs must be unique")
    if not cases:
        raise ValueError("Slack reconstruction gold population is empty")
    return tuple(cases)


def load_slack_reconstruction_observations(
    path: Path | str,
) -> tuple[SlackReconstructionObservation, ...]:
    observations: list[SlackReconstructionObservation] = []
    for line_number, line in enumerate(
        Path(path).read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"invalid Slack observation JSONL at line {line_number}"
            ) from exc
        observations.append(SlackReconstructionObservation.model_validate(payload))
    return tuple(observations)


def evaluate_slack_reconstruction(
    *,
    cases: tuple[SlackReconstructionGoldCase, ...],
    observations: tuple[SlackReconstructionObservation, ...],
    run_id: str,
    system_version: str,
    artifact_refs: tuple[str, ...],
) -> SlackReconstructionReport:
    """Compare one complete observed population with sealed Slack context gold."""

    cases_by_id = {case.case_id: case for case in cases}
    observations_by_id = {
        observation.case_id: observation for observation in observations
    }
    if len(cases_by_id) != len(cases):
        raise ValueError("Slack gold cases must be unique by case_id")
    if len(observations_by_id) != len(observations):
        raise ValueError("Slack observations must be unique by case_id")
    if set(cases_by_id) != set(observations_by_id):
        raise ValueError(
            "observed Slack results must exactly cover the sealed gold population"
        )

    assessments = tuple(
        _assess_case(
            case=case,
            observation=observations_by_id[case.case_id],
        )
        for case in cases
    )
    metrics = _metrics(assessments)
    unsupported = tuple(
        sorted(
            {
                reason
                for assessment in assessments
                for reason in assessment.unsupported_reasons
            }
        )
    )
    covered_families = {case.family for case in cases}
    unsealed_families = tuple(
        family
        for family in SlackGoldFamily
        if family not in covered_families
    )
    proof_gaps = (
        "Synthetic Slack gold is E4 reconstruction evidence, not open-world E5 proof.",
        "Gold covers selected Slack boundary families but not every workspace policy or connector history shape.",
        *(
            f"Gold family not yet sealed: {family.value}"
            for family in unsealed_families
        ),
        *(
            f"Current surface unsupported: {reason}"
            for reason in unsupported
        ),
    )
    return SlackReconstructionReport(
        run_id=run_id,
        system_version=system_version,
        gold_manifest_digest=canonical_sha256(
            [case.model_dump(mode="json") for case in cases]
        ),
        observation_digest=canonical_sha256(
            [observation.model_dump(mode="json") for observation in observations]
        ),
        status=(
            "not_observed"
            if not assessments
            else "observed_with_gaps"
            if unsupported
            else "observed"
        ),
        metrics=metrics,
        assessments=assessments,
        proof_gaps=proof_gaps,
        artifact_refs=artifact_refs,
    )


def _assess_case(
    *,
    case: SlackReconstructionGoldCase,
    observation: SlackReconstructionObservation,
) -> SlackReconstructionCaseAssessment:
    selected = set(observation.selected_event_revision_ids)
    candidates = set(observation.candidate_event_revision_ids)
    sealed_candidates = set(case.candidate_event_revision_ids)
    candidate_population_match = candidates == sealed_candidates
    selected_within_candidates = selected <= candidates
    forbidden = set(case.forbidden_event_revision_ids)
    sufficient_sets = tuple(
        set(sufficient_set)
        for sufficient_set in case.acceptable_sufficient_sets
    )
    sufficient_recalls = tuple(
        _rate(len(selected & sufficient_set), len(sufficient_set))
        for sufficient_set in sufficient_sets
    )
    sufficient_recall = max(
        (value for value in sufficient_recalls if value is not None),
        default=None,
    )
    sufficient_complete = (
        any(sufficient_set <= selected for sufficient_set in sufficient_sets)
        if sufficient_sets
        else None
    )
    relevant = {
        case.focal_event_revision_id,
        *(item for sufficient_set in sufficient_sets for item in sufficient_set),
    }
    contamination_count = len(selected & forbidden)
    precision = _rate(len(selected & relevant), len(selected))
    contamination_rate = _rate(contamination_count, len(selected))
    candidate_reconstructable = (
        candidate_population_match
        and case.focal_event_revision_id in candidates
        and (
            not sufficient_sets
            or any(sufficient_set <= candidates for sufficient_set in sufficient_sets)
        )
    )
    required_topology = set(case.required_topology_edge_ids)
    topology_recall = (
        _rate(
            len(required_topology & set(observation.selected_topology_edge_ids)),
            len(required_topology),
        )
        if required_topology
        else None
    )
    revision_expected = case.expected_revision_fates
    revision_correct = sum(
        observation.revision_fates.get(event_id) == expected
        for event_id, expected in revision_expected.items()
    )
    revision_correctness = _rate(revision_correct, len(revision_expected))
    disposition_allowed = observation.disposition in case.allowed_dispositions
    budget_adherent = (
        len(observation.selected_event_revision_ids)
        <= case.max_selected_events
        and observation.selected_token_count <= case.max_selected_tokens
    )
    safe_abstention = (
        observation.disposition in _SAFE_INSUFFICIENT_DISPOSITIONS
        if case.insufficient_evidence
        else None
    )
    supported = not observation.unsupported_reasons
    if case.insufficient_evidence:
        correct = bool(
            supported
            and safe_abstention
            and candidate_population_match
            and selected_within_candidates
            and disposition_allowed
            and contamination_count == 0
            and budget_adherent
        )
    else:
        correct = bool(
            supported
            and candidate_population_match
            and selected_within_candidates
            and sufficient_complete
            and contamination_count == 0
            and (topology_recall in {None, 1.0})
            and revision_correctness == 1.0
            and disposition_allowed
            and budget_adherent
        )
    return SlackReconstructionCaseAssessment(
        case_id=case.case_id,
        family=case.family,
        selected_event_count=len(selected),
        relevant_selected_event_count=len(selected & relevant),
        sufficient_set_recall=sufficient_recall,
        sufficient_set_complete=sufficient_complete,
        selected_context_precision=precision,
        contamination_count=contamination_count,
        contamination_rate=contamination_rate,
        candidate_population_match=candidate_population_match,
        selected_within_candidates=selected_within_candidates,
        candidate_reconstructable=candidate_reconstructable,
        topology_recall=topology_recall,
        revision_fate_correctness=revision_correctness,
        disposition_allowed=disposition_allowed,
        budget_adherent=budget_adherent,
        safe_abstention_under_insufficiency=safe_abstention,
        supported=supported,
        correct=correct,
        unsupported_reasons=observation.unsupported_reasons,
        artifact_refs=tuple(
            dict.fromkeys(
                (*case.artifact_refs, *observation.artifact_refs)
            )
        ),
    )


def _metrics(
    assessments: tuple[SlackReconstructionCaseAssessment, ...],
) -> SlackReconstructionMetrics:
    family_metrics: dict[str, dict[str, int | float | None]] = {}
    for family in SlackGoldFamily:
        selected = tuple(
            assessment
            for assessment in assessments
            if assessment.family is family
        )
        if not selected:
            continue
        family_metrics[family.value] = {
            "case_count": len(selected),
            "correct_case_rate": _rate(
                sum(assessment.correct for assessment in selected),
                len(selected),
            ),
            "mean_sufficient_set_recall": _mean(
                assessment.sufficient_set_recall
                for assessment in selected
            ),
            "contamination_rate": _aggregate_ratio(
                (
                    assessment.contamination_count,
                    assessment.selected_event_count,
                )
                for assessment in selected
            ),
        }
    sufficient = tuple(
        assessment
        for assessment in assessments
        if assessment.sufficient_set_recall is not None
    )
    topology = tuple(
        assessment
        for assessment in assessments
        if assessment.topology_recall is not None
    )
    edit_delete = tuple(
        assessment
        for assessment in assessments
        if assessment.family
        in {
            SlackGoldFamily.EDIT_REVISION,
            SlackGoldFamily.DELETION_TOMBSTONE,
        }
    )
    long_range = tuple(
        assessment
        for assessment in assessments
        if assessment.family is SlackGoldFamily.LONG_RANGE_RECURRENCE
    )
    cross_channel = tuple(
        assessment
        for assessment in assessments
        if assessment.family is SlackGoldFamily.CROSS_CHANNEL_DEPENDENCY
    )
    insufficient = tuple(
        assessment
        for assessment in assessments
        if assessment.safe_abstention_under_insufficiency is not None
    )
    contamination_total = sum(
        assessment.contamination_count for assessment in assessments
    )
    selected_total = sum(
        assessment.selected_event_count for assessment in assessments
    )
    relevant_selected_total = sum(
        assessment.relevant_selected_event_count
        for assessment in assessments
    )
    return SlackReconstructionMetrics(
        case_count=len(assessments),
        supported_case_count=sum(
            assessment.supported for assessment in assessments
        ),
        supported_case_rate=_rate(
            sum(assessment.supported for assessment in assessments),
            len(assessments),
        ),
        correct_case_count=sum(
            assessment.correct for assessment in assessments
        ),
        correct_case_rate=_rate(
            sum(assessment.correct for assessment in assessments),
            len(assessments),
        ),
        mean_sufficient_set_recall=_mean(
            assessment.sufficient_set_recall for assessment in sufficient
        ),
        complete_sufficient_set_rate=_rate(
            sum(bool(assessment.sufficient_set_complete) for assessment in sufficient),
            len(sufficient),
        ),
        selected_context_precision=(
            _rate(relevant_selected_total, selected_total)
            if selected_total
            else None
        ),
        contamination_rate=_rate(contamination_total, selected_total),
        reconstructability_rate=_rate(
            sum(assessment.candidate_reconstructable for assessment in assessments),
            len(assessments),
        ),
        mean_topology_recall=_mean(
            assessment.topology_recall for assessment in topology
        ),
        edit_delete_correctness_rate=_mean(
            assessment.revision_fate_correctness
            for assessment in edit_delete
        ),
        long_range_recall=_mean(
            assessment.sufficient_set_recall for assessment in long_range
        ),
        cross_channel_recall=_mean(
            assessment.sufficient_set_recall for assessment in cross_channel
        ),
        budget_adherence_rate=_rate(
            sum(assessment.budget_adherent for assessment in assessments),
            len(assessments),
        ),
        abstention_under_insufficiency_rate=_rate(
            sum(
                bool(assessment.safe_abstention_under_insufficiency)
                for assessment in insufficient
            ),
            len(insufficient),
        ),
        family_metrics=family_metrics,
    )


def _aggregate_ratio(values) -> float | None:
    materialized = tuple(values)
    numerator = sum(value[0] for value in materialized)
    denominator = sum(value[1] for value in materialized)
    return _rate(numerator, denominator)


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _mean(values) -> float | None:
    materialized = tuple(
        float(value) for value in values if value is not None
    )
    return fmean(materialized) if materialized else None


__all__ = [
    "SlackGoldEvent",
    "SlackGoldFamily",
    "SlackReconstructionCaseAssessment",
    "SlackReconstructionGoldCase",
    "SlackReconstructionMetrics",
    "SlackReconstructionObservation",
    "SlackReconstructionReport",
    "SlackRevisionFate",
    "evaluate_slack_reconstruction",
    "load_slack_reconstruction_gold",
    "load_slack_reconstruction_observations",
]
