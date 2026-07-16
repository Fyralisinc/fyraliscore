"""Shared pure candidate -> probe -> context-snapshot selection.

This function never reads canonical entity decisions.  It selects only from
pre-authorized evidence candidates and context-light probe results, so a focal
entity resolution cannot choose the evidence that then confirms itself.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from lib.contracts.conversation_context import (
    CommitInterpretationContextCommand,
    ContextProbeEnvelope,
    ContextSelectionOutcome,
    ConversationContextCandidate,
)
from lib.contracts.perception import (
    InterpretationContextSnapshot,
    OperationalSufficiencyVerdict,
    SelectionDependency,
    SufficiencyDisposition,
)


def _probe_delta(probe: ContextProbeEnvelope) -> float:
    return max(
        (abs(value) for value in probe.probe.perturbation_results.values()),
        default=0.0,
    )


def _safe(probe: ContextProbeEnvelope) -> bool:
    return not probe.probe.future_or_authority_incident_refs


def _eligible(
    *,
    command: CommitInterpretationContextCommand,
    probe: ContextProbeEnvelope,
) -> bool:
    required = set(command.request.required_probe_surfaces)
    return (
        _safe(probe)
        and required <= set(probe.completed_probe_surfaces)
        and not probe.failed_probe_surfaces
        and not probe.probe.unresolved_dependency_refs
        and _probe_delta(probe) <= command.policy.max_semantic_perturbation
        and probe.contamination_score <= command.policy.max_contamination_score
    )


def _candidate_rank(
    *,
    command: CommitInterpretationContextCommand,
    candidate: ConversationContextCandidate,
    probe: ContextProbeEnvelope,
) -> tuple[float, str]:
    return (
        command.policy.cost_score(candidate.cost),
        str(candidate.candidate_id),
    )


def _partial_rank(
    *,
    command: CommitInterpretationContextCommand,
    candidate: ConversationContextCandidate,
    probe: ContextProbeEnvelope,
) -> tuple[int, int, int, int, float, float, str]:
    required = set(command.request.required_probe_surfaces)
    completed = len(required & set(probe.completed_probe_surfaces))
    missing = len(required - set(probe.completed_probe_surfaces))
    return (
        -completed,
        len(probe.failed_probe_surfaces),
        missing + len(probe.probe.unresolved_dependency_refs),
        -len(candidate.layer_coverage),
        probe.contamination_score,
        command.policy.cost_score(candidate.cost),
        str(candidate.candidate_id),
    )


def select_context(
    command: CommitInterpretationContextCommand,
    *,
    aggregate_version: int,
    snapshot_id: UUID,
    dependency_id: UUID,
    frozen_at: datetime,
) -> ContextSelectionOutcome:
    """Select the cheapest sufficient context or an honest bounded partial one."""

    command = CommitInterpretationContextCommand.model_validate(
        command.model_dump(mode="json")
    )
    probes = {probe.candidate_id: probe for probe in command.probes}
    eligible = [
        candidate
        for candidate in command.candidates
        if _eligible(command=command, probe=probes[candidate.candidate_id])
    ]
    eligible.sort(
        key=lambda candidate: _candidate_rank(
            command=command,
            candidate=candidate,
            probe=probes[candidate.candidate_id],
        )
    )

    if eligible:
        best = eligible[0]
        best_cost = command.policy.cost_score(best.cost)
        close = [
            candidate
            for candidate in eligible
            if command.policy.cost_score(candidate.cost)
            <= best_cost * (1.0 + command.policy.multi_context_cost_tolerance)
        ]
        semantic_digests = {
            probes[candidate.candidate_id].semantic_output_digest
            for candidate in close
        }
        if len(semantic_digests) > 1:
            selected = close[: command.policy.max_multi_context_alternatives]
            disposition = SufficiencyDisposition.MULTI_CONTEXT
            rationale = ("equally_sufficient_semantic_alternatives",)
        else:
            selected = [best]
            disposition = SufficiencyDisposition.OPERATIONALLY_SUFFICIENT
            rationale = ("cheapest_probe_supported_context",)
    else:
        safe = [
            candidate
            for candidate in command.candidates
            if _safe(probes[candidate.candidate_id])
        ]
        if not safe:
            raise ValueError(
                "no context candidate is safe from future or authority incidents"
            )
        safe.sort(
            key=lambda candidate: _partial_rank(
                command=command,
                candidate=candidate,
                probe=probes[candidate.candidate_id],
            )
        )
        selected = [safe[0]]
        probe = probes[selected[0].candidate_id]
        if command.search_exhausted:
            disposition = SufficiencyDisposition.BUDGET_EXHAUSTED
            rationale = ("search_exhausted_without_sufficient_probe",)
        elif (
            probe.probe.unresolved_dependency_refs
            and probe.probe.expected_value_of_expansion <= probe.probe.cost_of_expansion
        ):
            disposition = SufficiencyDisposition.NEEDS_CLARIFICATION
            rationale = ("clarification_value_exceeds_context_expansion",)
        else:
            disposition = SufficiencyDisposition.NEEDS_EXPANSION
            rationale = ("required_probe_surface_or_stability_missing",)

    selected_probes = [probes[candidate.candidate_id] for candidate in selected]
    item_by_id = {
        item.event_revision_id: item
        for candidate in selected
        for item in candidate.selected_items
    }
    focal_order = {
        event_id: index
        for index, event_id in enumerate(command.request.focal_event_revision_ids)
    }
    selected_items = tuple(
        sorted(
            item_by_id.values(),
            key=lambda item: (
                0 if item.event_revision_id in focal_order else 1,
                focal_order.get(item.event_revision_id, 0),
                item.emitted_at,
                item.event_revision_id,
            ),
        )
    )
    topology_edge_ids = tuple(
        sorted(
            {
                edge_id
                for candidate in selected
                for edge_id in candidate.topology_edge_ids
            }
        )
    )
    hypothesis_by_hash = {
        hypothesis.content_hash: hypothesis
        for candidate in selected
        for hypothesis in candidate.embedded_episode_hypotheses
    }
    hypotheses = tuple(
        hypothesis_by_hash[key] for key in sorted(hypothesis_by_hash)
    )
    referent_by_id = {
        referent.referent_id: referent
        for candidate in selected
        for referent in candidate.discourse_referents
    }
    referents = tuple(referent_by_id[key] for key in sorted(referent_by_id))
    omissions = tuple(
        sorted(
            {
                f"{lane}: {reason}"
                for candidate in selected
                for lane, reason in candidate.omitted_lane_reasons.items()
            }
            | {
                f"{surface}: {reason}"
                for probe in selected_probes
                for surface, reason in probe.failed_probe_surfaces.items()
            }
        )
    )
    unresolved = tuple(
        sorted(
            {
                ref
                for probe in selected_probes
                for ref in probe.probe.unresolved_dependency_refs
            }
        )
    )
    if disposition is SufficiencyDisposition.BUDGET_EXHAUSTED and not (
        omissions or unresolved
    ):
        omissions = ("required context probe coverage was not completed",)

    verdict = OperationalSufficiencyVerdict(
        verdict_id=str(snapshot_id),
        probe_refs=tuple(probe.probe.probe_id for probe in selected_probes),
        risk_tier=command.request.risk_tier,
        perturbation_policy_version=command.policy.policy_version,
        budget=command.request.budget,
        disposition=disposition,
        omissions=omissions,
        unresolved_references=unresolved,
        stop_reason="; ".join(rationale),
    )
    versions = tuple(
        dict.fromkeys(
            [
                command.policy.policy_version,
                *(
                    candidate.generator_version
                    for candidate in selected
                ),
                *(
                    candidate.configuration_version
                    for candidate in selected
                ),
                *(probe.probe.probe_version for probe in selected_probes),
            ]
        )
    )
    snapshot = InterpretationContextSnapshot.build(
        snapshot_id=str(snapshot_id),
        snapshot_version=aggregate_version,
        request=command.request,
        focal_event_revision_ids=command.request.focal_event_revision_ids,
        selected_items=selected_items,
        topology_edge_ids=topology_edge_ids,
        embedded_episode_hypotheses=hypotheses,
        discourse_referents=referents,
        sufficiency_verdict=verdict,
        inherited_processing_authority=command.request.processing_authority,
        frozen_at=frozen_at,
        model_and_policy_versions=versions,
    )
    invalidation_keys = tuple(
        dict.fromkeys(
            [
                *command.invalidation_keys,
                *(f"event-revision:{item.event_revision_id}" for item in selected_items),
                f"topology-version:{command.request.source_topology_version}",
                *(f"topology-edge:{edge_id}" for edge_id in topology_edge_ids),
            ]
        )
    )
    dependency = SelectionDependency(
        dependency_id=str(dependency_id),
        snapshot_id=snapshot.snapshot_id,
        snapshot_version=snapshot.snapshot_version,
        embedded_hypothesis_hashes=tuple(
            hypothesis.content_hash for hypothesis in hypotheses
        ),
        selected_event_revision_ids=tuple(
            item.event_revision_id for item in selected_items
        ),
        topology_versions=(command.request.source_topology_version,),
        participant_and_role_versions=command.participant_and_role_versions,
        discourse_referent_versions=tuple(
            referent.referent_id for referent in referents
        ),
        linked_object_versions=command.linked_object_versions,
        invalidation_keys=invalidation_keys,
    )
    selected_cost = sum(
        command.policy.cost_score(candidate.cost) for candidate in selected
    )
    return ContextSelectionOutcome.build(
        selection_key=command.selection_key,
        aggregate_version=aggregate_version,
        snapshot=snapshot,
        dependency=dependency,
        selected_candidate_ids=tuple(candidate.candidate_id for candidate in selected),
        eligible_candidate_ids=tuple(candidate.candidate_id for candidate in eligible),
        disposition=disposition,
        rationale_codes=rationale,
        selected_cost_score=selected_cost,
    )


__all__ = ["select_context"]
