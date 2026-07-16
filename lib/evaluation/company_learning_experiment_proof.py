"""Canonical invariant evidence for corrective-memory pair experiments."""

from __future__ import annotations

from collections import Counter

from lib.evaluation.company_learning_experiment import (
    ConsumerTerminalFate,
    CorrectiveMemoryExperimentReport,
)
from lib.evaluation.proof import (
    CANONICAL_COMPONENT_PARTITION_DIMENSION,
    CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    EvidenceTier,
    FateDenominatorRecord,
    IncidentObservation,
    IncidentStatus,
    InvariantEvidenceManifest,
    InvariantRunEvidence,
    MetricObservation,
)


CORRECTIVE_MEMORY_INVARIANT_ID = "INV-05"
CORRECTIVE_MEMORY_SCENARIO_ID = "ENTITY-CORRECTIVE-MEMORY-PAIR"
CORRECTIVE_MEMORY_LIFT_METRIC_ID = "inv.entity_corrective_memory_lift"


def build_corrective_memory_experiment_evidence_manifest(
    report: CorrectiveMemoryExperimentReport,
    *,
    architecture_digest: str,
    experiment_manifest_ref: str,
    report_cutoff: str,
) -> InvariantEvidenceManifest:
    """Translate a validated paired report into canonical proof evidence."""

    results = tuple(
        result
        for pair in report.pairs
        for result in (pair.adaptive, pair.frozen)
    )
    terminal_fates = Counter(
        result.consumer_fate.value
        for result in results
        if result.consumer_fate is not ConsumerTerminalFate.INCOMPLETE
    )
    nonterminal_fates = Counter(
        result.consumer_fate.value
        for result in results
        if result.consumer_fate is ConsumerTerminalFate.INCOMPLETE
    )
    pair_count = report.metrics.pair_count
    adaptive_lift_numerator = (
        report.metrics.adaptive_correct_count
        - report.metrics.frozen_correct_count
    )
    artifact_refs = tuple(
        dict.fromkeys(
            (
                *report.artifact_refs,
                f"corrective-memory-report:sha256:{report.digest}",
            )
        )
    )
    evidence = InvariantRunEvidence(
        invariant_id=CORRECTIVE_MEMORY_INVARIANT_ID,
        applicable_exposures=len(results),
        observed_trace_facts=frozenset(),
        executed_scenario_ids=frozenset(
            scenario_id
            for scenario_id in report.scenario_ids
            if scenario_id == CORRECTIVE_MEMORY_SCENARIO_ID
        ),
        metric_observations=(
            MetricObservation(
                metric_id=CORRECTIVE_MEMORY_LIFT_METRIC_ID,
                metric_version="paired-adaptive-frozen-v1",
                raw_numerator=float(adaptive_lift_numerator),
                raw_denominator=float(pair_count),
                point_estimate=(
                    adaptive_lift_numerator / pair_count
                    if pair_count
                    else None
                ),
                violation_count=0,
                severity_mass=0.0,
                artifact_refs=artifact_refs,
            ),
        ),
        incidents=tuple(
            IncidentObservation(
                incident_id=incident.incident_id,
                incident_class=incident.incident_class.value,
                status=IncidentStatus.CONFIRMED,
                severity=5,
                summary=incident.summary,
                artifact_refs=incident.artifact_refs,
            )
            for incident in report.incidents
        ),
        achieved_evidence_tier=EvidenceTier.E4,
        denominator=FateDenominatorRecord(
            denominator_id=(
                f"{report.run_id}:{CORRECTIVE_MEMORY_INVARIANT_ID}:"
                f"corrective-memory-pairs:{report.pair_results_digest[:16]}"
            ),
            denominator_version="corrective-memory-pair-denominator-v1",
            population_definition_version="sealed-paired-recurrence-arms-v1",
            query_or_manifest_hash=report.pair_results_digest,
            source_or_oracle_population=len(results),
            production_accepted=len(results),
            eligible=len(results),
            attempted_or_committed=len(results),
            terminal_fates=dict(sorted(terminal_fates.items())),
            nonterminal_fates=dict(sorted(nonterminal_fates.items())),
            unknown_or_untraced=0,
            successor_lineages=sum(
                result.lineage.grounding_trace_id is not None
                for result in results
            ),
            effective_heads=sum(
                result.lineage.grounding_trace_id is not None
                for result in results
            ),
            report_cutoff=report_cutoff,
            population_partition_dimension=(
                CANONICAL_COMPONENT_PARTITION_DIMENSION
            ),
            population_partition_value="corrective_memory_pair_experiment",
            population_partition_proof_ref=(
                CANONICAL_COMPONENT_PARTITION_PROOF_REF
            ),
        ),
        uncertainty=report.proof_gaps,
        blind_spots=report.proof_gaps,
        artifact_refs=artifact_refs,
    )
    return InvariantEvidenceManifest(
        manifest_version="corrective-memory-experiment-evidence-v1",
        run_id=report.run_id,
        architecture_digest=architecture_digest,
        system_version=report.system_version,
        created_at=report.created_at,
        experiment_manifest_ref=experiment_manifest_ref,
        evidence=(evidence,),
        artifact_refs=artifact_refs,
    )


__all__ = [
    "CORRECTIVE_MEMORY_INVARIANT_ID",
    "CORRECTIVE_MEMORY_LIFT_METRIC_ID",
    "CORRECTIVE_MEMORY_SCENARIO_ID",
    "build_corrective_memory_experiment_evidence_manifest",
]
