"""Typed recurrence populations shared by company-learning runners."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_experiment import (
    CanonicalEntityRef,
    ConsumerTerminalFate,
    CorrectiveMemoryExperimentReport,
    CorrectiveMemoryExperimentSpec,
    PairedRecurrenceResult,
    RecurrenceCaseKind,
    SealedArmExpectation,
    SealedRecurrenceCase,
)
from lib.shared.ids import uuid7


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NEGATIVE_CONTROL_FIXTURE = (
    ROOT
    / "tests"
    / "fixtures"
    / "company_learning"
    / "negative_controls_v1.json"
)


class _RuntimeModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class NegativeControlCaseDefinition(_RuntimeModel):
    case_id: str = Field(min_length=1)
    kind: RecurrenceCaseKind
    entity_type: str = Field(min_length=1)
    slack_context: str = Field(min_length=1)
    wording_variant: str = Field(min_length=1)
    consequence: str = Field(min_length=1)
    recurrence_distance: int = Field(ge=1)
    alias_surface: str = Field(min_length=1)
    training_text: str = Field(min_length=1)
    training_phrase: str = Field(min_length=1)
    candidate_alias: str = Field(min_length=1)
    recurrence_text: str = Field(min_length=1)
    recurrence_phrase: str = Field(min_length=1)
    channel: str = Field(min_length=1)
    resolution_scope: Literal["source_context_only", "tenant_global_exact"]
    inject_conflicting_source_hint: bool
    recurrence_response: Literal["target_low", "conflicting_high"]
    expected_model_count: int = Field(ge=0)


class NegativeControlFixture(_RuntimeModel):
    fixture_version: str = Field(min_length=1)
    scenario_ids: tuple[str, ...] = Field(min_length=1)
    cases: tuple[NegativeControlCaseDefinition, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def complete_unique_population(self) -> Self:
        case_ids = [case.case_id for case in self.cases]
        if len(case_ids) != len(set(case_ids)):
            raise ValueError("negative-control case IDs must be unique")
        required = {
            RecurrenceCaseKind.CONTEXTUAL_PHRASE_NEGATIVE,
            RecurrenceCaseKind.UNRELATED_NEGATIVE_CONTROL,
            RecurrenceCaseKind.HOMONYM_LOCAL_ASSOCIATION,
            RecurrenceCaseKind.CONFLICTING_SOURCE_HINT,
        }
        if {case.kind for case in self.cases} != required:
            raise ValueError(
                "negative-control fixture must contain the four sealed case kinds"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class NegativeControlAssignment(_RuntimeModel):
    case_id: str = Field(min_length=1)
    adaptive_tenant_id: UUID
    frozen_tenant_id: UUID
    adaptive_target_id: UUID
    frozen_target_id: UUID
    adaptive_conflicting_id: UUID
    frozen_conflicting_id: UUID


class NegativeControlExecutionPlan(_RuntimeModel):
    schema_version: str = "company-learning-negative-control-plan-v1"
    status: Literal["not_executed"] = "not_executed"
    created_at: str = Field(min_length=1)
    fixture_version: str = Field(min_length=1)
    fixture_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    spec: CorrectiveMemoryExperimentSpec
    assignments: tuple[NegativeControlAssignment, ...] = Field(min_length=1)
    execution_dependencies: tuple[str, ...] = Field(min_length=1)
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def assignments_match_sealed_spec(self) -> Self:
        assignments = {row.case_id: row for row in self.assignments}
        cases = {case.case_id: case for case in self.spec.cases}
        if len(assignments) != len(self.assignments):
            raise ValueError("negative-control assignments must be unique")
        if set(assignments) != set(cases):
            raise ValueError(
                "negative-control assignments must exactly cover sealed cases"
            )
        tenant_ids = [
            tenant_id
            for assignment in self.assignments
            for tenant_id in (
                assignment.adaptive_tenant_id,
                assignment.frozen_tenant_id,
            )
        ]
        if len(tenant_ids) != len(set(tenant_ids)):
            raise ValueError(
                "negative-control arms require globally distinct tenants"
            )
        for case_id, case in cases.items():
            assignment = assignments[case_id]
            if (
                case.adaptive_expectation.tenant_id
                != assignment.adaptive_tenant_id
                or case.frozen_expectation.tenant_id
                != assignment.frozen_tenant_id
            ):
                raise ValueError(
                    "negative-control assignment tenants do not match sealed gold"
                )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class NegativeControlExperimentEvidence(_RuntimeModel):
    schema_version: str = "company-learning-negative-control-evidence-v1"
    executed_at: str = Field(min_length=1)
    fixture_version: str = Field(min_length=1)
    fixture_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    spec: CorrectiveMemoryExperimentSpec
    pairs: tuple[PairedRecurrenceResult, ...] = Field(min_length=1)
    report: CorrectiveMemoryExperimentReport
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def execution_exactly_matches_sealed_plan(self) -> Self:
        sealed_case_ids = {case.case_id for case in self.spec.cases}
        pair_case_ids = [pair.case_id for pair in self.pairs]
        if len(pair_case_ids) != len(set(pair_case_ids)):
            raise ValueError("negative-control result case IDs must be unique")
        if set(pair_case_ids) != sealed_case_ids:
            raise ValueError(
                "negative-control results must exactly cover sealed cases"
            )
        if self.report.spec_digest != self.spec.digest:
            raise ValueError(
                "negative-control report must compile the sealed spec"
            )
        if self.report.pairs != self.pairs:
            raise ValueError(
                "negative-control evidence pairs must match compiled report"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


def load_negative_control_fixture(
    path: Path = DEFAULT_NEGATIVE_CONTROL_FIXTURE,
) -> NegativeControlFixture:
    return NegativeControlFixture.model_validate_json(
        path.read_text(encoding="utf-8")
    )


def build_negative_control_plan(
    fixture: NegativeControlFixture,
    *,
    run_id: str,
    system_version: str,
    created_at: datetime | None = None,
    fixture_path: Path = DEFAULT_NEGATIVE_CONTROL_FIXTURE,
) -> NegativeControlExecutionPlan:
    created_at = created_at or datetime.now(timezone.utc)
    assignments = tuple(
        NegativeControlAssignment(
            case_id=case.case_id,
            adaptive_tenant_id=uuid7(),
            frozen_tenant_id=uuid7(),
            adaptive_target_id=uuid7(),
            frozen_target_id=uuid7(),
            adaptive_conflicting_id=uuid7(),
            frozen_conflicting_id=uuid7(),
        )
        for case in fixture.cases
    )
    by_case = {assignment.case_id: assignment for assignment in assignments}
    cases = tuple(
        _sealed_case(case, assignment=by_case[case.case_id])
        for case in fixture.cases
    )
    spec = CorrectiveMemoryExperimentSpec(
        experiment_id=f"corrective-memory-negative-controls:{run_id}",
        run_id=run_id,
        system_version=system_version,
        created_at=created_at.isoformat(),
        scenario_ids=fixture.scenario_ids,
        company_foundation_digest=canonical_sha256(
            {
                "fixture_version": fixture.fixture_version,
                "cases": [
                    {
                        "case_id": case.case_id,
                        "entity_type": case.entity_type,
                        "training_text": case.training_text,
                        "recurrence_text": case.recurrence_text,
                        "resolution_scope": case.resolution_scope,
                    }
                    for case in fixture.cases
                ],
            }
        ),
        provider_behavior_digest=canonical_sha256(
            {
                case.case_id: case.recurrence_response
                for case in fixture.cases
            }
        ),
        cases=cases,
        artifact_refs=(
            f"fixture:{fixture_path}",
            f"fixture-digest:sha256:{fixture.digest}",
        ),
    )
    return NegativeControlExecutionPlan(
        created_at=created_at.isoformat(),
        fixture_version=fixture.fixture_version,
        fixture_digest=fixture.digest,
        spec=spec,
        assignments=assignments,
        execution_dependencies=(
            "real Postgres with current migrations",
            "entity resolver and source-semantic workers",
            "persisted clarification adjudication and alias lineage",
            "typed adaptive/frozen recurrence results for all sealed cases",
        ),
        artifact_refs=(
            f"fixture:{fixture_path}",
            "runner:scripts/run_company_learning_negative_controls.py",
        ),
    )


def write_negative_control_plan(
    plan: NegativeControlExecutionPlan,
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        **plan.model_dump(mode="json"),
        "plan_digest": plan.digest,
    }
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _sealed_case(
    definition: NegativeControlCaseDefinition,
    *,
    assignment: NegativeControlAssignment,
) -> SealedRecurrenceCase:
    is_conflict = (
        definition.kind is RecurrenceCaseKind.CONFLICTING_SOURCE_HINT
    )
    adaptive_ref = (
        CanonicalEntityRef(
            type=definition.entity_type,
            id=str(assignment.adaptive_conflicting_id),
        )
        if is_conflict
        else None
    )
    frozen_ref = (
        CanonicalEntityRef(
            type=definition.entity_type,
            id=str(assignment.frozen_conflicting_id),
        )
        if is_conflict
        else None
    )
    safe_fates = (
        ConsumerTerminalFate.REVIEW,
        ConsumerTerminalFate.ABSTAINED,
        ConsumerTerminalFate.REJECTED,
        ConsumerTerminalFate.NO_ADMISSION,
    )
    return SealedRecurrenceCase(
        case_id=definition.case_id,
        case_version="negative-controls-v1",
        kind=definition.kind,
        alias_surface=definition.alias_surface,
        source_text_digest=canonical_sha256(definition.recurrence_text),
        context_digest=canonical_sha256(
            {
                "slack_context": definition.slack_context,
                "wording_variant": definition.wording_variant,
                "consequence": definition.consequence,
                "recurrence_distance": definition.recurrence_distance,
                "channel": definition.channel,
            }
        ),
        adaptive_expectation=SealedArmExpectation(
            tenant_id=assignment.adaptive_tenant_id,
            allowed_consumer_fates=(
                (ConsumerTerminalFate.RESOLVED_FOR_CONSUMER,)
                if is_conflict
                else safe_fates
            ),
            expected_entity_ref=adaptive_ref,
            expected_model_count=definition.expected_model_count,
            autonomous_resolution_permitted=is_conflict,
        ),
        frozen_expectation=SealedArmExpectation(
            tenant_id=assignment.frozen_tenant_id,
            allowed_consumer_fates=safe_fates,
            expected_entity_ref=frozen_ref,
            expected_model_count=0 if is_conflict else definition.expected_model_count,
            autonomous_resolution_permitted=False,
        ),
        artifact_refs=(f"fixture-case:{definition.case_id}",),
    )


__all__ = [
    "DEFAULT_NEGATIVE_CONTROL_FIXTURE",
    "NegativeControlAssignment",
    "NegativeControlCaseDefinition",
    "NegativeControlExecutionPlan",
    "NegativeControlFixture",
    "build_negative_control_plan",
    "load_negative_control_fixture",
    "write_negative_control_plan",
]
