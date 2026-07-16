"""Continuous, incident-preserving compilation of invariant proof state.

This module consumes the canonical ArchitectureContractRegistry plus explicit
run evidence.  It does not infer success from test names, neighboring rows, an
empty queue, or the absence of reported failures.  Missing definitions,
denominators, traces, scenarios, metrics, or evidence tier remain visible as
insufficient proof.
"""

from __future__ import annotations

from collections import Counter
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.kernel import canonical_sha256


CANONICAL_COMPONENT_PARTITION_DIMENSION = "canonical_evaluation_component"
CANONICAL_COMPONENT_PARTITION_PROOF_REF = (
    "contract://objective-evaluation/disjoint-canonical-component-populations-v1"
)


class _ProofModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class EvidenceTier(StrEnum):
    E0 = "E0"
    E1 = "E1"
    E2 = "E2"
    E3 = "E3"
    E4 = "E4"
    E5 = "E5"
    E6 = "E6"

    @property
    def rank(self) -> int:
        return int(self.value[1:])


class SubstantiationState(StrEnum):
    SUBSTANTIATED = "substantiated"
    CONTRADICTED = "contradicted"
    INSUFFICIENT = "insufficient"
    NOT_APPLICABLE = "not_applicable"


class IncidentStatus(StrEnum):
    CANDIDATE = "candidate"
    CONFIRMED = "confirmed"
    DISMISSED = "dismissed"


class FateDenominatorRecord(_ProofModel):
    """A complete-population funnel rather than a survivor count."""

    denominator_id: str = Field(min_length=1)
    denominator_version: str = Field(min_length=1)
    population_definition_version: str = Field(min_length=1)
    query_or_manifest_hash: str = Field(min_length=1)
    source_or_oracle_population: int = Field(ge=0)
    production_accepted: int = Field(ge=0)
    eligible: int = Field(ge=0)
    attempted_or_committed: int = Field(ge=0)
    terminal_fates: dict[str, int] = Field(default_factory=dict)
    nonterminal_fates: dict[str, int] = Field(default_factory=dict)
    excluded_by_preregistered_reason: int = Field(default=0, ge=0)
    unknown_or_untraced: int = Field(default=0, ge=0)
    successor_lineages: int = Field(default=0, ge=0)
    effective_heads: int = Field(default=0, ge=0)
    report_cutoff: str = Field(min_length=1)
    population_partition_dimension: str | None = None
    population_partition_value: str | None = None
    population_partition_proof_ref: str | None = None

    @model_validator(mode="after")
    def counts_form_a_possible_funnel(self) -> Self:
        partition_fields = (
            self.population_partition_dimension,
            self.population_partition_value,
            self.population_partition_proof_ref,
        )
        if any(partition_fields) and not all(partition_fields):
            raise ValueError(
                "population partition dimension, value and proof ref must be supplied together"
            )
        if not (
            self.source_or_oracle_population
            >= self.production_accepted
            >= self.eligible
            >= self.attempted_or_committed
        ):
            raise ValueError(
                "denominator populations must be monotonically non-increasing"
            )
        fate_counts = (*self.terminal_fates.values(), *self.nonterminal_fates.values())
        if any(count < 0 for count in fate_counts):
            raise ValueError("fate counts cannot be negative")
        if self.known_fate_count + self.unknown_or_untraced > self.attempted_or_committed:
            raise ValueError("current fates cannot exceed attempted population")
        if (
            self.attempted_or_committed
            + self.excluded_by_preregistered_reason
            + self.unknown_or_untraced
            > self.eligible
        ):
            raise ValueError("attempted, excluded and unknown units cannot exceed eligible")
        if self.effective_heads > self.successor_lineages:
            raise ValueError("effective heads cannot exceed successor lineages")
        return self

    @property
    def known_fate_count(self) -> int:
        return sum(self.terminal_fates.values()) + sum(self.nonterminal_fates.values())

    @property
    def population_coverage(self) -> float:
        if self.eligible == 0:
            return 1.0
        known = self.attempted_or_committed + self.excluded_by_preregistered_reason
        return min(1.0, known / self.eligible)

    @property
    def fate_coverage(self) -> float:
        if self.attempted_or_committed == 0:
            return 1.0
        return min(1.0, self.known_fate_count / self.attempted_or_committed)

    @property
    def denominator_coverage(self) -> float:
        return min(self.population_coverage, self.fate_coverage)

    @property
    def complete(self) -> bool:
        return self.denominator_coverage == 1.0 and self.unknown_or_untraced == 0


class MetricObservation(_ProofModel):
    metric_id: str = Field(min_length=1)
    metric_version: str = Field(min_length=1)
    raw_numerator: float
    raw_denominator: float = Field(ge=0)
    point_estimate: float | None = None
    violation_count: int = Field(default=0, ge=0)
    severity_mass: float = Field(default=0.0, ge=0.0)
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def point_estimate_is_not_invented(self) -> Self:
        if self.raw_denominator == 0:
            if self.point_estimate is not None:
                raise ValueError("zero denominator requires an unknown point estimate")
            return self
        expected = self.raw_numerator / self.raw_denominator
        if self.point_estimate is not None and abs(self.point_estimate - expected) > 1e-9:
            raise ValueError("point estimate must equal raw numerator / raw denominator")
        return self


class IncidentObservation(_ProofModel):
    incident_id: str = Field(min_length=1)
    incident_class: str = Field(min_length=1)
    status: IncidentStatus
    severity: int = Field(ge=1, le=5)
    summary: str = Field(min_length=1)
    artifact_refs: tuple[str, ...] = Field(min_length=1)


class InvariantRunEvidence(_ProofModel):
    invariant_id: str = Field(pattern=r"^INV-[0-9]{2}$")
    applicable_exposures: int = Field(ge=0)
    observed_trace_facts: frozenset[str] = frozenset()
    executed_scenario_ids: frozenset[str] = frozenset()
    metric_observations: tuple[MetricObservation, ...] = ()
    incidents: tuple[IncidentObservation, ...] = ()
    achieved_evidence_tier: EvidenceTier
    denominator: FateDenominatorRecord
    not_applicable_reason: str | None = None
    uncertainty: tuple[str, ...] = ()
    blind_spots: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def applicability_is_explicit(self) -> Self:
        if self.not_applicable_reason and self.applicable_exposures != 0:
            raise ValueError("not-applicable evidence cannot contain exposures")
        if self.applicable_exposures != self.denominator.eligible:
            raise ValueError("applicable exposures must equal denominator eligible population")
        metric_ids = [item.metric_id for item in self.metric_observations]
        if len(metric_ids) != len(set(metric_ids)):
            raise ValueError("metric observations must be unique by metric_id")
        incident_ids = [item.incident_id for item in self.incidents]
        if len(incident_ids) != len(set(incident_ids)):
            raise ValueError("incidents must be unique by incident_id")
        return self


class InvariantEvidenceManifest(_ProofModel):
    manifest_version: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    architecture_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    system_version: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    experiment_manifest_ref: str = Field(min_length=1)
    evidence: tuple[InvariantRunEvidence, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def evidence_rows_are_unique(self) -> Self:
        invariant_ids = [item.invariant_id for item in self.evidence]
        if len(invariant_ids) != len(set(invariant_ids)):
            raise ValueError("manifest evidence must be unique by invariant_id")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class EvidenceAggregationMode(StrEnum):
    SINGLE_SOURCE = "single_source"
    DECLARED_DISJOINT_PARTITION_UNION = "declared_disjoint_partition_union"


class InvariantEvidenceAggregationRecord(_ProofModel):
    invariant_id: str = Field(pattern=r"^INV-[0-9]{2}$")
    mode: EvidenceAggregationMode
    source_manifest_digests: tuple[str, ...] = Field(min_length=1)
    source_denominator_ids: tuple[str, ...] = Field(min_length=1)
    population_partition_dimension: str | None = None
    population_partition_values: tuple[str, ...] = ()
    population_partition_proof_ref: str | None = None


class InvariantEvidenceBundle(_ProofModel):
    """Compatible component manifests plus their auditable aggregation result."""

    bundle_version: str = "invariant-evidence-bundle-v1"
    run_id: str = Field(min_length=1)
    architecture_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    system_version: str = Field(min_length=1)
    experiment_manifest_ref: str = Field(min_length=1)
    source_manifest_digests: tuple[str, ...] = Field(min_length=1)
    source_manifest_versions: tuple[str, ...] = Field(min_length=1)
    source_created_at: tuple[str, ...] = Field(min_length=1)
    aggregation: tuple[InvariantEvidenceAggregationRecord, ...]
    evidence: tuple[InvariantRunEvidence, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class InvariantSubstantiationRecord(_ProofModel):
    invariant_id: str
    run_id: str
    definition_present: bool
    applicable_exposures: int
    required_trace_fact_count: int
    observed_required_trace_fact_count: int
    required_trace_fact_coverage: float = Field(ge=0.0, le=1.0)
    denominator_coverage: float = Field(ge=0.0, le=1.0)
    denominator_complete: bool
    required_scenario_count: int
    executed_required_scenario_count: int
    scenario_execution_coverage: float = Field(ge=0.0, le=1.0)
    required_metric_count: int
    observed_required_metric_count: int
    metric_observation_coverage: float = Field(ge=0.0, le=1.0)
    metric_observation_refs: tuple[str, ...]
    incident_refs: tuple[str, ...]
    confirmed_incident_count: int
    candidate_incident_count: int
    violation_count: int
    achieved_evidence_tier: EvidenceTier
    minimum_evidence_tier: EvidenceTier
    substantiation_state: SubstantiationState
    proof_gaps: tuple[str, ...]
    uncertainty: tuple[str, ...]
    blind_spots: tuple[str, ...]
    artifact_refs: tuple[str, ...]


class MatrixCoverage(_ProofModel):
    required_architecture_rows: int
    registered_rows: int
    registered_row_coverage: float
    executable_definition_rows: int
    executable_definition_coverage: float
    observed_rows: int
    observed_row_coverage: float
    observed_trace_slots: int
    required_trace_slots: int
    trace_slot_coverage: float
    executed_scenarios: int
    required_scenarios: int
    scenario_execution_coverage: float
    denominator_record_coverage_mean: float
    observed_exposures: int
    denominator_equivalent_covered_exposures: float
    exposure_coverage: float
    state_counts: dict[str, int]
    evidence_tier_counts: dict[str, int]
    confirmed_incident_count: int
    candidate_incident_count: int
    violation_count: int
    stale_proof_share: float | None = None


class InvariantProofMatrixReport(_ProofModel):
    run_id: str
    architecture_registry_id: str
    architecture_registry_version: str
    architecture_digest: str
    records: tuple[InvariantSubstantiationRecord, ...]
    coverage: MatrixCoverage

    @property
    def production_freeze_ready(self) -> bool:
        return (
            len(self.records) == 42
            and all(
                row.substantiation_state is SubstantiationState.SUBSTANTIATED
                for row in self.records
            )
            and self.coverage.confirmed_incident_count == 0
            and self.coverage.candidate_incident_count == 0
            and self.coverage.violation_count == 0
        )


def render_invariant_proof_markdown(report: InvariantProofMatrixReport) -> str:
    """Render a compact system-state report without inventing one overall score."""

    coverage = report.coverage
    lines = [
        f"# Invariant proof state: {report.run_id}",
        "",
        f"- Architecture registry: `{report.architecture_registry_id}` "
        f"`{report.architecture_registry_version}`",
        f"- Architecture digest: `{report.architecture_digest}`",
        f"- Production-freeze ready: **{'yes' if report.production_freeze_ready else 'no'}**",
        "- This report has no compensatory overall score. Contradictions and proof gaps remain row-local and visible.",
        "",
        "## Coverage",
        "",
        "| Dimension | Observed | Required | Coverage |",
        "| --- | ---: | ---: | ---: |",
        f"| Registered invariant rows | {coverage.registered_rows} | {coverage.required_architecture_rows} | {coverage.registered_row_coverage:.1%} |",
        f"| Executable proof definitions | {coverage.executable_definition_rows} | {coverage.required_architecture_rows} | {coverage.executable_definition_coverage:.1%} |",
        f"| Rows with run evidence | {coverage.observed_rows} | {coverage.required_architecture_rows} | {coverage.observed_row_coverage:.1%} |",
        f"| Mandatory trace slots | {coverage.observed_trace_slots} | {coverage.required_trace_slots} | {coverage.trace_slot_coverage:.1%} |",
        f"| Registered scenarios | {coverage.executed_scenarios} | {coverage.required_scenarios} | {coverage.scenario_execution_coverage:.1%} |",
        f"| Exposure denominator | {coverage.denominator_equivalent_covered_exposures:.2f} | {coverage.observed_exposures} | {coverage.exposure_coverage:.1%} |",
        "",
        f"Confirmed incidents: **{coverage.confirmed_incident_count}**; "
        f"candidate incidents: **{coverage.candidate_incident_count}**; "
        f"metric violations: **{coverage.violation_count}**.",
        "",
        "## Per-invariant state",
        "",
        "| Invariant | State | Tier | Traces | Denominator | Scenarios | Metrics | Gaps |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in report.records:
        gaps = "; ".join(row.proof_gaps) or "none"
        gaps = gaps.replace("|", "\\|")
        lines.append(
            f"| {row.invariant_id} | {row.substantiation_state.value} | "
            f"{row.achieved_evidence_tier.value}/{row.minimum_evidence_tier.value} | "
            f"{row.required_trace_fact_coverage:.1%} | "
            f"{row.denominator_coverage:.1%} | "
            f"{row.scenario_execution_coverage:.1%} | "
            f"{row.metric_observation_coverage:.1%} | {gaps} |"
        )
    lines.extend(
        [
            "",
            "## State distribution",
            "",
            *(f"- {state}: {count}" for state, count in sorted(coverage.state_counts.items())),
            "",
        ]
    )
    return "\n".join(lines)


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator / denominator) if denominator else 0.0


def _compile_row(
    *,
    run_id: str,
    invariant,
    evidence: InvariantRunEvidence | None,
) -> InvariantSubstantiationRecord:
    proof = invariant.proof
    definition_present = proof is not None
    required_traces = set(proof.mandatory_trace_facts) if proof else set()
    required_scenarios = set(proof.suite_and_scenario_ids) if proof else set()
    required_metrics = set(proof.continuous_metric_ids) if proof else set()
    observed_traces = set(evidence.observed_trace_facts) if evidence else set()
    executed_scenarios = set(evidence.executed_scenario_ids) if evidence else set()
    metric_observations = evidence.metric_observations if evidence else ()
    observed_metrics = {item.metric_id for item in metric_observations}
    incidents = evidence.incidents if evidence else ()

    trace_hits = len(required_traces & observed_traces)
    scenario_hits = len(required_scenarios & executed_scenarios)
    metric_hits = len(required_metrics & observed_metrics)
    trace_coverage = _ratio(trace_hits, len(required_traces)) if proof else 0.0
    scenario_coverage = _ratio(scenario_hits, len(required_scenarios)) if proof else 0.0
    metric_coverage = _ratio(metric_hits, len(required_metrics)) if proof else 0.0
    denominator_coverage = evidence.denominator.denominator_coverage if evidence else 0.0
    denominator_complete = evidence.denominator.complete if evidence else False
    achieved_tier = evidence.achieved_evidence_tier if evidence else EvidenceTier.E0
    minimum_tier = EvidenceTier(invariant.evidence_floor)
    confirmed_incidents = tuple(
        item for item in incidents if item.status is IncidentStatus.CONFIRMED
    )
    candidate_incidents = tuple(
        item for item in incidents if item.status is IncidentStatus.CANDIDATE
    )
    violation_count = sum(item.violation_count for item in metric_observations)

    gaps: list[str] = []
    if not definition_present:
        gaps.append("missing executable proof definition")
    if evidence is None:
        gaps.append("missing run evidence")
    else:
        if trace_coverage < 1.0:
            gaps.append("mandatory trace facts incomplete")
        if not denominator_complete:
            gaps.append("population denominator incomplete or unknown")
        if scenario_coverage < 1.0 and not evidence.not_applicable_reason:
            gaps.append("registered scenarios incomplete")
        if metric_coverage < 1.0 and not evidence.not_applicable_reason:
            gaps.append("required metric observations incomplete")
        if achieved_tier.rank < minimum_tier.rank:
            gaps.append("minimum evidence tier not achieved")
        if candidate_incidents:
            gaps.append("candidate incidents remain unresolved")

    if confirmed_incidents or violation_count:
        state = SubstantiationState.CONTRADICTED
    elif (
        evidence is not None
        and evidence.not_applicable_reason
        and definition_present
        and denominator_complete
        and achieved_tier.rank >= minimum_tier.rank
        and not candidate_incidents
    ):
        state = SubstantiationState.NOT_APPLICABLE
    elif not gaps:
        state = SubstantiationState.SUBSTANTIATED
    else:
        state = SubstantiationState.INSUFFICIENT

    registered_blind_spots = proof.known_blind_spots if proof else ()
    observed_blind_spots = evidence.blind_spots if evidence else ()

    return InvariantSubstantiationRecord(
        invariant_id=invariant.invariant_id,
        run_id=run_id,
        definition_present=definition_present,
        applicable_exposures=evidence.applicable_exposures if evidence else 0,
        required_trace_fact_count=len(required_traces),
        observed_required_trace_fact_count=trace_hits,
        required_trace_fact_coverage=trace_coverage,
        denominator_coverage=denominator_coverage,
        denominator_complete=denominator_complete,
        required_scenario_count=len(required_scenarios),
        executed_required_scenario_count=scenario_hits,
        scenario_execution_coverage=scenario_coverage,
        required_metric_count=len(required_metrics),
        observed_required_metric_count=metric_hits,
        metric_observation_coverage=metric_coverage,
        metric_observation_refs=tuple(
            sorted(item.metric_id for item in metric_observations)
        ),
        incident_refs=tuple(sorted(item.incident_id for item in incidents)),
        confirmed_incident_count=len(confirmed_incidents),
        candidate_incident_count=len(candidate_incidents),
        violation_count=violation_count,
        achieved_evidence_tier=achieved_tier,
        minimum_evidence_tier=minimum_tier,
        substantiation_state=state,
        proof_gaps=tuple(gaps),
        uncertainty=evidence.uncertainty if evidence else (),
        blind_spots=tuple(
            dict.fromkeys((*registered_blind_spots, *observed_blind_spots))
        ),
        artifact_refs=evidence.artifact_refs if evidence else (),
    )


def _unique_strings(values) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(value) for value in values))


def _sum_fates(
    rows: tuple[InvariantRunEvidence, ...],
    field: str,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts.update(getattr(row.denominator, field))
    return dict(sorted(counts.items()))


def _aggregate_metric_observations(
    rows: tuple[InvariantRunEvidence, ...],
) -> tuple[MetricObservation, ...]:
    by_metric: dict[str, list[MetricObservation]] = {}
    for row in rows:
        for observation in row.metric_observations:
            by_metric.setdefault(observation.metric_id, []).append(observation)

    aggregated = []
    for metric_id, observations in sorted(by_metric.items()):
        numerator = sum(item.raw_numerator for item in observations)
        denominator = sum(item.raw_denominator for item in observations)
        inputs_hash = canonical_sha256(
            [item.model_dump(mode="json") for item in observations]
        )
        aggregated.append(
            MetricObservation(
                metric_id=metric_id,
                metric_version=f"declared-disjoint-union-v1:{inputs_hash[:16]}",
                raw_numerator=numerator,
                raw_denominator=denominator,
                point_estimate=(numerator / denominator if denominator else None),
                violation_count=sum(item.violation_count for item in observations),
                severity_mass=sum(item.severity_mass for item in observations),
                artifact_refs=_unique_strings(
                    ref for item in observations for ref in item.artifact_refs
                ),
            )
        )
    return tuple(aggregated)


def _aggregate_invariant_rows(
    *,
    run_id: str,
    invariant_id: str,
    sourced_rows: tuple[tuple[str, InvariantRunEvidence], ...],
) -> tuple[InvariantRunEvidence, InvariantEvidenceAggregationRecord]:
    rows = tuple(row for _, row in sourced_rows)
    denominator_ids = tuple(row.denominator.denominator_id for row in rows)
    if len(set(denominator_ids)) != len(denominator_ids):
        raise ValueError(
            f"{invariant_id} repeats a denominator_id across manifests; "
            "the same population may not be counted twice"
        )

    dimensions = {row.denominator.population_partition_dimension for row in rows}
    values = tuple(row.denominator.population_partition_value for row in rows)
    proof_refs = {row.denominator.population_partition_proof_ref for row in rows}
    if None in dimensions or None in values or None in proof_refs:
        raise ValueError(
            f"{invariant_id} has overlapping evidence rows without a declared "
            "mutually exclusive population partition"
        )
    if len(dimensions) != 1 or len(proof_refs) != 1:
        raise ValueError(
            f"{invariant_id} population partition contracts are incompatible"
        )
    if len(set(values)) != len(values):
        raise ValueError(
            f"{invariant_id} repeats population partition values; disjointness is not established"
        )
    cutoffs = {row.denominator.report_cutoff for row in rows}
    if len(cutoffs) != 1:
        raise ValueError(
            f"{invariant_id} evidence uses different report cutoffs and cannot be summed"
        )

    incident_ids = [item.incident_id for row in rows for item in row.incidents]
    if len(incident_ids) != len(set(incident_ids)):
        raise ValueError(
            f"{invariant_id} repeats incident ids across manifests; deduplicate at the source"
        )

    denominators = [row.denominator.model_dump(mode="json") for row in rows]
    denominator_inputs_hash = canonical_sha256(denominators)
    partition_dimension = next(iter(dimensions))
    partition_proof_ref = next(iter(proof_refs))
    partition_values = tuple(str(value) for value in values)
    denominator = FateDenominatorRecord(
        denominator_id=(
            f"{run_id}:{invariant_id}:declared-disjoint-union:"
            f"{denominator_inputs_hash[:16]}"
        ),
        denominator_version="declared-disjoint-union-v1",
        population_definition_version=(
            f"declared-disjoint-union-v1:{denominator_inputs_hash[:16]}"
        ),
        query_or_manifest_hash=denominator_inputs_hash,
        source_or_oracle_population=sum(
            row.denominator.source_or_oracle_population for row in rows
        ),
        production_accepted=sum(row.denominator.production_accepted for row in rows),
        eligible=sum(row.denominator.eligible for row in rows),
        attempted_or_committed=sum(
            row.denominator.attempted_or_committed for row in rows
        ),
        terminal_fates=_sum_fates(rows, "terminal_fates"),
        nonterminal_fates=_sum_fates(rows, "nonterminal_fates"),
        excluded_by_preregistered_reason=sum(
            row.denominator.excluded_by_preregistered_reason for row in rows
        ),
        unknown_or_untraced=sum(
            row.denominator.unknown_or_untraced for row in rows
        ),
        successor_lineages=sum(row.denominator.successor_lineages for row in rows),
        effective_heads=sum(row.denominator.effective_heads for row in rows),
        report_cutoff=next(iter(cutoffs)),
    )
    not_applicable_reason = None
    if denominator.eligible == 0 and all(row.not_applicable_reason for row in rows):
        not_applicable_reason = "; ".join(
            _unique_strings(row.not_applicable_reason for row in rows)
        )
    partition_uncertainty = (
        "Population counts were summed under declared mutually exclusive partition "
        f"{partition_dimension!r} ({', '.join(partition_values)}) using "
        f"{partition_proof_ref}; this compiler validates the declaration but does not "
        "inspect raw population member identities."
    )
    aggregate = InvariantRunEvidence(
        invariant_id=invariant_id,
        applicable_exposures=denominator.eligible,
        observed_trace_facts=frozenset(
            fact for row in rows for fact in row.observed_trace_facts
        ),
        executed_scenario_ids=frozenset(
            scenario for row in rows for scenario in row.executed_scenario_ids
        ),
        metric_observations=_aggregate_metric_observations(rows),
        incidents=tuple(item for row in rows for item in row.incidents),
        achieved_evidence_tier=min(
            (row.achieved_evidence_tier for row in rows),
            key=lambda tier: tier.rank,
        ),
        denominator=denominator,
        not_applicable_reason=not_applicable_reason,
        uncertainty=_unique_strings(
            (
                *(item for row in rows for item in row.uncertainty),
                partition_uncertainty,
            )
        ),
        blind_spots=_unique_strings(item for row in rows for item in row.blind_spots),
        artifact_refs=_unique_strings(
            ref for row in rows for ref in row.artifact_refs
        ),
    )
    return aggregate, InvariantEvidenceAggregationRecord(
        invariant_id=invariant_id,
        mode=EvidenceAggregationMode.DECLARED_DISJOINT_PARTITION_UNION,
        source_manifest_digests=tuple(digest for digest, _ in sourced_rows),
        source_denominator_ids=denominator_ids,
        population_partition_dimension=partition_dimension,
        population_partition_values=partition_values,
        population_partition_proof_ref=partition_proof_ref,
    )


def aggregate_invariant_evidence_manifests(
    manifests: tuple[InvariantEvidenceManifest, ...],
) -> InvariantEvidenceBundle:
    """Combine compatible component evidence without overwrite or blind summation.

    Multiple rows for one invariant are only summed when their denominators cite
    one partition dimension and proof contract with unique partition values.
    """

    if not manifests:
        raise ValueError("at least one evidence manifest is required")
    ordered = tuple(sorted(manifests, key=lambda manifest: manifest.digest))
    digests = tuple(manifest.digest for manifest in ordered)
    if len(set(digests)) != len(digests):
        raise ValueError("duplicate evidence manifests may not be aggregated")

    compatibility_fields = {
        "run_id": {manifest.run_id for manifest in ordered},
        "architecture_digest": {manifest.architecture_digest for manifest in ordered},
        "system_version": {manifest.system_version for manifest in ordered},
        "experiment_manifest_ref": {
            manifest.experiment_manifest_ref for manifest in ordered
        },
    }
    mismatches = [name for name, values in compatibility_fields.items() if len(values) != 1]
    if mismatches:
        raise ValueError(
            "evidence manifests are not from one compatible evaluation run; mismatched "
            + ", ".join(mismatches)
        )

    run_id = ordered[0].run_id
    grouped: dict[str, list[tuple[str, InvariantRunEvidence]]] = {}
    for manifest, manifest_digest in zip(ordered, digests, strict=True):
        for row in manifest.evidence:
            grouped.setdefault(row.invariant_id, []).append((manifest_digest, row))

    evidence: list[InvariantRunEvidence] = []
    aggregation: list[InvariantEvidenceAggregationRecord] = []
    for invariant_id, sourced in sorted(grouped.items()):
        sourced_rows = tuple(sourced)
        if len(sourced_rows) == 1:
            manifest_digest, row = sourced_rows[0]
            evidence.append(row)
            denominator = row.denominator
            aggregation.append(
                InvariantEvidenceAggregationRecord(
                    invariant_id=invariant_id,
                    mode=EvidenceAggregationMode.SINGLE_SOURCE,
                    source_manifest_digests=(manifest_digest,),
                    source_denominator_ids=(denominator.denominator_id,),
                    population_partition_dimension=(
                        denominator.population_partition_dimension
                    ),
                    population_partition_values=(
                        (denominator.population_partition_value,)
                        if denominator.population_partition_value
                        else ()
                    ),
                    population_partition_proof_ref=(
                        denominator.population_partition_proof_ref
                    ),
                )
            )
            continue
        aggregate, record = _aggregate_invariant_rows(
            run_id=run_id,
            invariant_id=invariant_id,
            sourced_rows=sourced_rows,
        )
        evidence.append(aggregate)
        aggregation.append(record)

    return InvariantEvidenceBundle(
        run_id=run_id,
        architecture_digest=ordered[0].architecture_digest,
        system_version=ordered[0].system_version,
        experiment_manifest_ref=ordered[0].experiment_manifest_ref,
        source_manifest_digests=digests,
        source_manifest_versions=tuple(
            manifest.manifest_version for manifest in ordered
        ),
        source_created_at=tuple(manifest.created_at for manifest in ordered),
        aggregation=tuple(aggregation),
        evidence=tuple(evidence),
        artifact_refs=_unique_strings(
            ref for manifest in ordered for ref in manifest.artifact_refs
        ),
    )


def render_evidence_aggregation_markdown(bundle: InvariantEvidenceBundle) -> str:
    union_count = sum(
        record.mode is EvidenceAggregationMode.DECLARED_DISJOINT_PARTITION_UNION
        for record in bundle.aggregation
    )
    lines = [
        f"# Evidence aggregation: {bundle.run_id}",
        "",
        f"- Bundle digest: `{bundle.digest}`",
        f"- System version: `{bundle.system_version}`",
        f"- Experiment manifest: `{bundle.experiment_manifest_ref}`",
        f"- Source manifests: **{len(bundle.source_manifest_digests)}**",
        f"- Invariant rows after aggregation: **{len(bundle.evidence)}**",
        f"- Declared disjoint-partition unions: **{union_count}**",
        "- Counts are never merged merely because invariant IDs match; every union retains source denominators and its partition declaration.",
        "",
        "| Invariant | Mode | Sources | Denominators | Partition |",
        "| --- | --- | ---: | ---: | --- |",
    ]
    for record in bundle.aggregation:
        partition = "n/a"
        if record.population_partition_dimension:
            values = ", ".join(record.population_partition_values)
            partition = f"{record.population_partition_dimension}: {values}"
        lines.append(
            f"| {record.invariant_id} | {record.mode.value} | "
            f"{len(record.source_manifest_digests)} | "
            f"{len(record.source_denominator_ids)} | {partition} |"
        )
    return "\n".join(lines)


def compile_invariant_proof_matrix(
    registry: ArchitectureContractRegistry,
    *,
    run_id: str,
    evidence: tuple[InvariantRunEvidence, ...] = (),
) -> InvariantProofMatrixReport:
    """Compile one non-compensatory record for every registered invariant."""

    evidence_by_invariant = {item.invariant_id: item for item in evidence}
    if len(evidence_by_invariant) != len(evidence):
        raise ValueError("run evidence must be unique by invariant_id")
    registered_ids = {item.invariant_id for item in registry.invariants}
    unknown = set(evidence_by_invariant) - registered_ids
    if unknown:
        raise ValueError(f"evidence references unregistered invariants: {sorted(unknown)}")

    records = tuple(
        _compile_row(
            run_id=run_id,
            invariant=invariant,
            evidence=evidence_by_invariant.get(invariant.invariant_id),
        )
        for invariant in sorted(registry.invariants, key=lambda item: item.invariant_id)
    )
    required_rows = 42
    registered_rows = len(records)
    executable_rows = sum(row.definition_present for row in records)
    observed_rows = len(evidence)
    trace_slots = sum(row.required_trace_fact_count for row in records)
    observed_trace_slots = sum(row.observed_required_trace_fact_count for row in records)
    required_scenarios = sum(row.required_scenario_count for row in records)
    executed_scenarios = sum(row.executed_required_scenario_count for row in records)
    denominator_coverages = [
        evidence_by_invariant[row.invariant_id].denominator.denominator_coverage
        for row in records
        if row.invariant_id in evidence_by_invariant
    ]
    total_exposures = sum(row.applicable_exposures for row in records)
    covered_exposures = sum(
        row.applicable_exposures * row.denominator_coverage for row in records
    )

    return InvariantProofMatrixReport(
        run_id=run_id,
        architecture_registry_id=registry.meta.registry_id,
        architecture_registry_version=registry.meta.registry_version,
        architecture_digest=registry.digest,
        records=records,
        coverage=MatrixCoverage(
            required_architecture_rows=required_rows,
            registered_rows=registered_rows,
            registered_row_coverage=_ratio(registered_rows, required_rows),
            executable_definition_rows=executable_rows,
            executable_definition_coverage=_ratio(executable_rows, required_rows),
            observed_rows=observed_rows,
            observed_row_coverage=_ratio(observed_rows, required_rows),
            observed_trace_slots=observed_trace_slots,
            required_trace_slots=trace_slots,
            trace_slot_coverage=_ratio(observed_trace_slots, trace_slots),
            executed_scenarios=executed_scenarios,
            required_scenarios=required_scenarios,
            scenario_execution_coverage=_ratio(executed_scenarios, required_scenarios),
            denominator_record_coverage_mean=(
                sum(denominator_coverages) / len(denominator_coverages)
                if denominator_coverages
                else 0.0
            ),
            observed_exposures=total_exposures,
            denominator_equivalent_covered_exposures=covered_exposures,
            exposure_coverage=_ratio(covered_exposures, total_exposures),
            state_counts=dict(Counter(row.substantiation_state.value for row in records)),
            evidence_tier_counts=dict(
                Counter(row.achieved_evidence_tier.value for row in records)
            ),
            confirmed_incident_count=sum(row.confirmed_incident_count for row in records),
            candidate_incident_count=sum(row.candidate_incident_count for row in records),
            violation_count=sum(row.violation_count for row in records),
        ),
    )


__all__ = [
    "CANONICAL_COMPONENT_PARTITION_DIMENSION",
    "CANONICAL_COMPONENT_PARTITION_PROOF_REF",
    "EvidenceAggregationMode",
    "EvidenceTier",
    "FateDenominatorRecord",
    "IncidentObservation",
    "IncidentStatus",
    "InvariantEvidenceAggregationRecord",
    "InvariantEvidenceBundle",
    "InvariantEvidenceManifest",
    "InvariantProofMatrixReport",
    "InvariantRunEvidence",
    "MetricObservation",
    "SubstantiationState",
    "aggregate_invariant_evidence_manifests",
    "compile_invariant_proof_matrix",
    "render_evidence_aggregation_markdown",
    "render_invariant_proof_markdown",
]
