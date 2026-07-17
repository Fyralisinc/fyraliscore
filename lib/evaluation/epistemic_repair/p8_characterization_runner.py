"""Label-blind production-path execution for P8 characterization packages."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from math import sqrt
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p8_characterization_population import (
    SealedPopulation,
    build_all_characterization_populations,
    population_manifest,
)
from services.domain.entity_grounding.episode import (
    ContextObservationInput,
    GroundingCandidateInput,
    build_grounding_episode,
    candidate_id_for_ref,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection
from services.domain.conversation_context.slack_source_structure import (
    SlackSourceObservation,
    project_slack_source_structure,
)
from services.domain.conversation_context.episode_boundaries import (
    ConversationBoundaryObservation,
    project_conversation_episode_boundaries,
)


_TENANT = uuid5(NAMESPACE_URL, "p8-characterization-tenant")
_START = datetime(2026, 7, 18, tzinfo=timezone.utc)


def _wilson(successes: int, total: int) -> tuple[float, float]:
    if total == 0:
        return (0.0, 0.0)
    p, z = successes / total, 1.96
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    margin = z * sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denominator
    return (max(0.0, centre - margin), min(1.0, centre + margin))


def _metric(name: str, outcomes: list[tuple[str, bool]], *, slices: dict[str, list[tuple[str, bool]]]) -> dict[str, Any]:
    successes = sum(ok for _, ok in outcomes)
    worst = tuple(case_id for case_id, _ in sorted(outcomes, key=lambda row: (row[1], row[0])))[:10]
    slice_rows = {}
    for label, rows in sorted(slices.items()):
        passed = sum(ok for _, ok in rows)
        slice_rows[label] = {
            "numerator": passed, "denominator": len(rows),
            "score": passed / len(rows), "ci95": _wilson(passed, len(rows)),
            "worst_example_ids": [case_id for case_id, _ in sorted(rows, key=lambda row: (row[1], row[0]))[:5]],
            "source_artifact_digest": canonical_sha256([case_id for case_id, _ in rows]),
        }
    return {
        "metric": name, "numerator": successes, "denominator": len(outcomes),
        "score": successes / len(outcomes), "ci95": _wilson(successes, len(outcomes)),
        "worst_example_ids": list(worst),
        "source_artifact_digest": canonical_sha256([case_id for case_id, _ in outcomes]),
        "slices": slice_rows,
    }


def _context(
    case_id: str, text: str, source_kind: str, ordinal: int, *,
    prior: tuple[ContextObservationInput, ...] = (),
    governed_exact_alias_available: bool = False,
    phrase: str = "this update",
):
    occurred = _START + timedelta(seconds=ordinal)
    channel = "slack:message" if source_kind in {"conversational", "slack"} else "document:object"
    return prepare_context_selection(
        tenant_id=_TENANT, observation_id=uuid5(NAMESPACE_URL, case_id), phrase=phrase,
        occurred_at=occurred, source_channel=channel, source_space=f"p8:{source_kind}",
        topology_incomplete=False, boundary_hypotheses=({"kind": "bounded_alternative"},),
        context_observations=prior, selection_dependency_refs=(f"case:{case_id}",),
        now=occurred + timedelta(seconds=1), focal_content_text=text,
        governed_exact_alias_available=governed_exact_alias_available,
    )


async def _predict_boundary(population: SealedPopulation) -> dict[str, str]:
    """Execute the label-blind production boundary path and freeze predictions."""
    predicted: dict[str, str] = {}
    slack_cases = [case for case in population.cases if case.source_kind == "conversational"]
    slack_observations = tuple(
        SlackSourceObservation(
            tenant_id=_TENANT,
            event_revision_id=f"observation:{uuid5(NAMESPACE_URL, case.case_id)}:v1",
            occurred_at=_START + timedelta(seconds=int(case.case_id.rsplit("-", 1)[1])),
            content_text=case.runtime_text,
            content={**dict(case.runtime_source_metadata), "type": "message", "user": "U-p8"},
        )
        for case in slack_cases
    )
    structure = project_slack_source_structure(slack_observations)
    revision_to_case = {
        f"observation:{uuid5(NAMESPACE_URL, case.case_id)}:v1": case.case_id
        for case in slack_cases
    }
    # The source projector is still executed to validate and preserve Slack
    # topology. Episode hypotheses then combine authenticated source containers
    # with explicit cross-source topic references; topology is evidence, not an
    # unconditional semantic merge.
    source_containers: dict[str, str] = {}
    for case in population.cases:
        metadata = dict(case.runtime_source_metadata)
        if case.source_kind == "structured":
            source_containers[case.case_id] = f"object:{metadata['object_id']}"
        elif case.source_kind == "cross_source":
            source_containers[case.case_id] = f"link:{metadata['linked_object_id']}"
        else:
            revision = f"observation:{uuid5(NAMESPACE_URL, case.case_id)}:v1"
            connected = {revision, *structure.connected_revision_ids(revision, max_hops=10)}
            source_containers[case.case_id] = f"slack:{min(connected)}"
    episode_inputs = tuple(
        ConversationBoundaryObservation(
            observation_id=case.case_id,
            occurred_at=_START + timedelta(seconds=int(case.case_id.rsplit("-", 1)[1])),
            content_text=case.runtime_text,
            source_container_id=source_containers[case.case_id],
        )
        for case in population.cases
    )
    for group in project_conversation_episode_boundaries(episode_inputs):
        cluster_id = min(group)
        for case_id in group:
            predicted[case_id] = cluster_id
    return predicted


async def _run_boundary(population: SealedPopulation) -> dict[str, Any]:
    # Freeze production predictions before opening evaluator-owned labels.
    predicted = await _predict_boundary(population)

    gold = {
        case.case_id: next(label for label in case.evaluator_labels if label.startswith("episode:"))
        for case in population.cases
    }

    def score(ids: list[str]) -> dict[str, Any]:
        pred_groups: dict[str, set[str]] = {}
        gold_groups: dict[str, set[str]] = {}
        for case_id in ids:
            pred_groups.setdefault(predicted[case_id], set()).add(case_id)
            gold_groups.setdefault(gold[case_id], set()).add(case_id)
        members = []
        for case_id in ids:
            intersection = pred_groups[predicted[case_id]] & gold_groups[gold[case_id]]
            precision = len(intersection) / len(pred_groups[predicted[case_id]])
            recall = len(intersection) / len(gold_groups[gold[case_id]])
            f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
            members.append((case_id, precision, recall, f1))
        def summary(index: int):
            values = [row[index] for row in members]
            mean = sum(values) / len(values)
            variance = sum((value - mean) ** 2 for value in values) / max(1, len(values) - 1)
            margin = 1.96 * sqrt(variance / len(values))
            return mean, (max(0.0, mean - margin), min(1.0, mean + margin))
        precision, precision_ci = summary(1)
        recall, recall_ci = summary(2)
        f1, f1_ci = summary(3)
        worst = [row[0] for row in sorted(members, key=lambda row: (row[3], row[0]))[:10]]
        false_merges = sum(1 for group in pred_groups.values() if len({gold[item] for item in group}) > 1)
        return {"denominator": len(ids), "precision": precision, "precision_ci95": precision_ci,
                "recall": recall, "recall_ci95": recall_ci, "f1": f1, "f1_ci95": f1_ci,
                "false_merge_clusters": false_merges, "worst_example_ids": worst,
                "source_artifact_digest": canonical_sha256(ids)}

    overall_ids = [case.case_id for case in population.cases]
    labels = sorted({label for case in population.cases for label in case.evaluator_labels if not label.startswith("episode:")})
    slices = {
        label: score([case.case_id for case in population.cases if label in case.evaluator_labels])
        for label in labels
    }
    result = score(overall_ids)
    result.update({"metric": "boundary_discovery_b_cubed", "slices": slices,
                   "production_path": "source topology plus generic explicit-topic episode projection",
                   "predictions_frozen_before_gold": True})
    return result


async def _run_context(population: SealedPopulation) -> dict[str, Any]:
    outcomes, slices = [], {}
    for ordinal, case in enumerate(population.cases):
        prior = (ContextObservationInput(
            uuid5(NAMESPACE_URL, f"prior:{case.case_id}"), _START,
            "slack:message", "p8:context", "source_topology",
            ("authenticated reply topology",), "Prior Harbor context.", 4,
            (f"edge:{case.case_id}",),
        ),)
        _, outcome = _context(case.case_id, case.runtime_text, case.source_kind, ordinal + 2000, prior=prior)
        ok = bool(outcome.snapshot.selected_items) and outcome.selected_cost_score >= 0
        outcomes.append((case.case_id, ok))
        for label in case.evaluator_labels:
            slices.setdefault(label, []).append((case.case_id, ok))
        if ordinal % 100 == 0:
            await asyncio.sleep(0)
    return _metric("context_decision_total_fate", outcomes, slices=slices)


async def _run_entity(population: SealedPopulation) -> dict[str, Any]:
    outcomes, slices, mention_slices = [], {}, {}
    grounding_outcomes = []
    false_merge_ids = []
    fate_counts: dict[str, int] = {}
    for ordinal, case in enumerate(population.cases):
        phrase = f"Entity-{ordinal:04d}"
        command, context = _context(
            case.case_id, case.runtime_text, case.source_kind, ordinal + 4000,
            governed_exact_alias_available=len(case.runtime_candidate_refs) == 1,
            phrase=phrase,
        )
        mention_command = prepare_entity_mention_detection(
            tenant_id=_TENANT, observation_id=uuid5(NAMESPACE_URL, case.case_id),
            phrase=phrase, content_text=case.runtime_text, source_channel="document:object",
            context_command=command, context_outcome=context,
            now=_START + timedelta(seconds=ordinal + 4002),
        )
        detection = mention_command.detection
        expected = "negative" not in case.evaluator_labels
        predicted = detection.mention is not None
        ok = predicted == expected
        outcomes.append((case.case_id, ok))
        for label in case.evaluator_labels:
            mention_slices.setdefault(label, []).append((case.case_id, ok))
        grounding_ok = ok
        competing = len(case.runtime_candidate_refs) > 1
        if predicted:
            primary_ref = {"type": "project", "id": f"project:{case.case_id}", "version": 1}
            candidates = ()
            model_id = None
            model_ref = primary_ref
            if case.runtime_candidate_refs:
                built = []
                for ref_text in case.runtime_candidate_refs:
                    kind, _ = ref_text.split(":", 1)
                    canonical_ref = {"type": kind, "id": ref_text, "version": 1}
                    built.append(GroundingCandidateInput(
                        canonical_ref=canonical_ref, candidate_source="authenticated_fixture_registry",
                        positive_evidence_refs=(f"observation:{case.case_id}",),
                        independent_identity_evidence_refs=(f"identity:{ref_text}",),
                        exact_mention_match=True,
                        decisive_authority_refs=(
                            (f"authority:{ref_text}",) if len(case.runtime_candidate_refs) == 1 else ()
                        ),
                    ))
                candidates = tuple(built)
                primary_ref = candidates[0].canonical_ref
                model_ref = primary_ref
                model_id = candidate_id_for_ref(primary_ref)
            episode = build_grounding_episode(
                tenant_id=_TENANT, observation_id=uuid5(NAMESPACE_URL, case.case_id),
                phrase=phrase, occurred_at=_START + timedelta(seconds=ordinal + 4000),
                source_channel="document:object", source_space="p8:mixed",
                topology_incomplete=False, boundary_hypotheses=({"kind": "bounded_alternative"},),
                context_observations=(), selection_dependency_refs=(f"case:{case.case_id}",),
                candidates=candidates, model_candidate_id=model_id,
                model_canonical_ref=model_ref, model_confidence=.95,
                model_reasoning="closed-set candidate assessment",
                decision_source="deterministic_replay", high_confidence=.8,
                review_min=.5, prepared_context_command=command,
                prepared_context_outcome=context,
                prepared_mention_detection_command=mention_command,
                now=_START + timedelta(seconds=ordinal + 4003),
            )
            expected_fate = (
                "abstained" if "open_world_none_known" in case.evaluator_labels
                else "review" if competing else "resolved_for_consumer"
            )
            grounding_ok = episode.current_fate == expected_fate
            fate_counts[episode.current_fate] = fate_counts.get(episode.current_fate, 0) + 1
            if competing and episode.current_fate == "resolved_for_consumer":
                false_merge_ids.append(case.case_id)
        grounding_outcomes.append((case.case_id, grounding_ok))
        for label in case.evaluator_labels:
            slices.setdefault(label, []).append((case.case_id, grounding_ok))
        if ordinal % 100 == 0:
            await asyncio.sleep(0)
    metric = _metric("canonical_entity_grounding", grounding_outcomes, slices=slices)
    metric["mention_detection"] = _metric(
        "label_blind_explicit_mention_detection", outcomes, slices=mention_slices,
    )
    metric["automatic_false_merges"] = len(false_merge_ids)
    metric["automatic_false_merge_ids"] = false_merge_ids[:10]
    metric["fate_counts"] = fate_counts
    metric["grounding_executed"] = True
    return metric


async def run_characterization_contract(repository_root: Path) -> dict[str, Any]:
    populations = build_all_characterization_populations()
    by_name = {population.name: population for population in populations}
    boundary, context, entity = await asyncio.gather(
        _run_boundary(by_name["boundary_discovery"]),
        _run_context(by_name["context_selection"]),
        _run_entity(by_name["entity_grounding"]),
    )
    source_paths = (
        "services/domain/entity_grounding/episode.py",
        "services/domain/entity_grounding/mentions.py",
        "lib/evaluation/epistemic_repair/p3_runner.py",
        "lib/evaluation/epistemic_repair/p4_runner.py",
    )
    source_digests = {path: canonical_sha256((repository_root / path).read_text()) for path in source_paths}
    artifact = {
        "schema_version": "p8-sealed-component-characterization-v1",
        "manifests": [population_manifest(population) for population in populations],
        "executed_metrics": {"boundary": boundary, "context": context, "entity": entity},
        "retrieval": {"status": "not_executed", "denominator": 600},
        "feedback": {"status": "not_executed", "base_decisions": 360, "policy_executions": 720},
        "entity_grounding": {"status": "executed", "mention_detection_only": False},
        "queue_measurement": {"status": "not_executed"},
        "projection_refresh": {"status": "not_executed"},
        "real_provider_sample": {"status": "separate_not_run"},
        "source_digests": source_digests,
        "production_label_visibility": False,
        "characterization_ready": False,
    }
    artifact["artifact_digest"] = canonical_sha256(artifact)
    return artifact
