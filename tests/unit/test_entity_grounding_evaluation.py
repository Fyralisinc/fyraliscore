from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from lib.architecture_registry import load_architecture_registry
from lib.contracts.kernel import canonical_sha256
from lib.evaluation.entity_grounding import (
    GroundingEvaluationScope,
    analyze_entity_grounding_rows,
    build_entity_grounding_invariant_evidence,
    render_entity_grounding_markdown,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection
from lib.evaluation.proof import (
    EvidenceTier,
    SubstantiationState,
    compile_invariant_proof_matrix,
)
from services.domain.entity_grounding.episode import (
    GroundingCandidateInput,
    build_grounding_episode,
    prepare_context_selection,
)


ROOT = Path(__file__).resolve().parents[2]
REGISTRY = load_architecture_registry(ROOT / "architecture/registry.yaml")
NOW = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)


def _fixture_rows(
    *,
    review: bool = False,
    content_text: str = "NBI renewal is blocked",
    phrase: str = "NBI",
):
    tenant_id = uuid4()
    observation_id = uuid4()
    ref = {"type": "customer", "id": "customer:nimbus"}
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=phrase,
        occurred_at=NOW,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        now=NOW + timedelta(seconds=10),
    )
    mention_command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=phrase,
        content_text=content_text,
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=NOW + timedelta(seconds=30),
    )
    detection = mention_command.detection
    episode = None
    if detection.mention is not None:
        episode = build_grounding_episode(
            tenant_id=tenant_id,
            observation_id=observation_id,
            phrase=phrase,
            occurred_at=NOW,
            source_channel="slack:message",
            source_space="C-finance",
            topology_incomplete=False,
            boundary_hypotheses=({"kind": "source_topology"},),
            context_observations=(),
            selection_dependency_refs=(),
            prepared_context_command=context_command,
            prepared_context_outcome=context_outcome,
            prepared_mention_detection_command=mention_command,
            candidates=(
                GroundingCandidateInput(
                    canonical_ref=ref,
                    candidate_source="tenant_aliases",
                    positive_evidence_refs=("entity-alias:Nimbus Bank",),
                    independent_identity_evidence_refs=("manual-alias-adjudication:1",),
                ),
            ),
            model_candidate_id=None,
            model_canonical_ref=ref,
            model_confidence=0.65 if review else 0.91,
            model_reasoning="bounded candidate",
            high_confidence=0.8,
            review_min=0.5,
            now=NOW + timedelta(minutes=1),
        )
    scope = GroundingEvaluationScope(
        tenant_id=tenant_id,
        observation_start=NOW - timedelta(minutes=1),
        observation_end=NOW + timedelta(hours=1),
        run_id="grounding-fixture",
    )
    observation = {
        "id": observation_id,
        "occurred_at": NOW,
        "content": {"_unresolved_phrases": [phrase]},
        "content_text": content_text,
    }
    command_result_id = uuid4()
    event_id = uuid4()
    result = {
        "detection_key": mention_command.detection_key,
        "detection_id": str(detection.detection_id),
        "detection_digest": detection.detection_digest,
        "detection_version": detection.detection_version,
        "fate": detection.fate.value,
        "mention_id": detection.mention.mention_id if detection.mention else None,
        "context_snapshot_id": str(detection.context_snapshot_id),
        "context_snapshot_digest": detection.context_snapshot_digest,
        "source_content_hash": detection.source_content_hash,
    }
    event_payload = {
        "command_result_id": str(command_result_id),
        "writer_id": "GroundingAnnotationAppender",
        "object_type": "entity_mention_detection",
        "object_id": str(detection.detection_id),
        "object_version": detection.detection_version,
        "semantic_transition": "entity_mention_detection_recorded",
        **result,
    }
    mention_row = {
        "id": detection.detection_id,
        "detection_version": detection.detection_version,
        "source_observation_id": observation_id,
        "source_revision_id": detection.source_revision_id,
        "candidate_surface": detection.candidate_surface,
        "context_snapshot_id": detection.context_snapshot_id,
        "context_snapshot_digest": detection.context_snapshot_digest,
        "committed_context_snapshot_digest": detection.context_snapshot_digest,
        "source_content_hash": detection.source_content_hash,
        "fate": detection.fate.value,
        "mention_id": detection.mention.mention_id if detection.mention else None,
        "mention": detection.mention.model_dump(mode="json") if detection.mention else None,
        "reason_codes": list(detection.reason_codes),
        "extractor_version": detection.extractor_version,
        "detection_digest": detection.detection_digest,
        "command_result_id": command_result_id,
        "detected_at": detection.detected_at,
    }
    mention_command_row = {
        "id": command_result_id,
        "writer_id": "GroundingAnnotationAppender",
        "command_kind": "commit_entity_mention_detection",
        "request_digest": mention_command.request_digest,
        "command": mention_command.model_dump(mode="json"),
        "object_id": detection.detection_id,
        "object_version": detection.detection_version,
        "result": result,
    }
    mention_event = {
        "id": event_id,
        "command_result_id": command_result_id,
        "writer_id": "GroundingAnnotationAppender",
        "object_type": "entity_mention_detection",
        "object_id": detection.detection_id,
        "object_version": detection.detection_version,
        "event_payload": event_payload,
    }
    mention_outbox = {
        "event_id": event_id,
        "destination_operation": "grounding.entity_mention.detected",
        "payload_hash": canonical_sha256(event_payload),
        "payload": event_payload,
    }
    trace_id = uuid4()
    work = {
        "id": uuid4(),
        "source_observation_id": observation_id,
        "phrase": phrase,
        "processing_generation": 1,
        "status": episode.current_fate if episode is not None else "unresolved",
        "processing_class": "R2",
        "next_attempt_at": None,
        "current_trace_id": trace_id if episode is not None else None,
        "updated_at": NOW + timedelta(minutes=1),
    }
    episode_rows = []
    candidate_rows = []
    if episode is not None and detection.mention is not None:
        row = {
            "id": trace_id,
            "source_observation_id": observation_id,
            "phrase": phrase,
            "created_at": NOW + timedelta(minutes=1),
            "snapshot": episode.context_snapshot.model_dump(mode="json"),
            "request": episode.candidate_set.request.model_dump(mode="json"),
            "candidate_set": episode.candidate_set.model_dump(mode="json"),
            "candidate_set_hash": "0" * 64,
            "assessment": episode.assessment.model_dump(mode="json"),
            "selected_candidate_id": episode.selected_candidate_id,
            "model_output": episode.model_output,
            "decision": episode.admission.model_dump(mode="json"),
            "selected_referent": episode.admitted_canonical_ref,
            "identity_registry_mutated": False,
            "source_observation_mutated": False,
            "current_fate": episode.current_fate,
            "has_review_obligation": review,
            "context_snapshot_id": detection.context_snapshot_id,
            "trace_mention_detection_id": detection.detection_id,
            "trace_mention_id": detection.mention.mention_id,
            "candidate_mention_detection_id": detection.detection_id,
            "candidate_mention_id": detection.mention.mention_id,
            "trace": {},
        }
        episode_rows.append(row)
        candidate_payload = episode.candidate_set.request.model_dump(mode="json")
        candidate_payload["mention_ref"] = (
            f"mention:{detection.mention.mention_id}:v{detection.detection_version}"
        )
        candidate_rows.append(
            {
                "id": episode.candidate_set.request.request_id,
                "source_observation_id": observation_id,
                "phrase": phrase,
                "context_snapshot_id": detection.context_snapshot_id,
                "entity_mention_detection_id": detection.detection_id,
                "entity_mention_id": detection.mention.mention_id,
                "request": candidate_payload,
            }
        )
    return {
        "scope": scope,
        "observations": [observation],
        "work_items": [work],
        "episode_rows": episode_rows,
        "resolver_created_alias_count": 0,
        "self_authoritative_observation_count": 0,
        "artifact_refs": ("pytest://entity-grounding-fixture",),
        "mention_detection_rows": [mention_row],
        "candidate_request_rows": candidate_rows,
        "mention_commands": [mention_command_row],
        "mention_events": [mention_event],
        "mention_outboxes": [mention_outbox],
    }


def test_clean_grounding_scope_reports_complete_continuity_without_incidents() -> None:
    inputs = _fixture_rows()
    state = analyze_entity_grounding_rows(**inputs)

    assert state.work_population_coverage == 1.0
    assert state.mention_detection_population_coverage == 1.0
    assert state.explicit_anchor_reconstructability_rate == 1.0
    assert state.mention_source_hash_match_rate == 1.0
    assert state.mention_context_continuity_rate == 1.0
    assert state.mention_protocol_closure_rate == 1.0
    assert state.detected_mention_to_candidate_continuity_rate == 1.0
    assert state.terminal_trace_coverage == 1.0
    assert state.stage_continuity_rate == 1.0
    assert state.candidate_request_fate_coverage == 1.0
    assert state.incident_counts == {}
    assert state.processing_class_counts == {"R2": 1}


def test_governed_exact_alias_replay_reports_resolution_and_llm_avoidance() -> None:
    inputs = _fixture_rows()
    inputs["episode_rows"][0]["model_output"].update(
        {
            "decision_source": "governed_exact_alias_replay",
            "identity_basis_ref": "clarification-request:replay-1",
            "resolution_scope": "tenant_global_exact",
            "llm_invoked": False,
        }
    )

    state = analyze_entity_grounding_rows(**inputs)

    assert state.alias_replay_exposure_count == 1
    assert state.alias_replay_resolved_count == 1
    assert state.alias_replay_resolution_rate == 1.0
    assert state.alias_replay_llm_avoided_count == 1
    assert state.unsafe_alias_replay_count == 0
    assert state.contextual_alias_replay_count == 0
    evidence = build_entity_grounding_invariant_evidence(
        state,
        registry=REGISTRY,
        executed_scenario_ids=frozenset(),
    )
    inv05 = next(item for item in evidence if item.invariant_id == "INV-05")
    replay_metric = next(
        item
        for item in inv05.metric_observations
        if item.metric_id == "entity.governed_alias_replay_resolution"
    )
    assert replay_metric.raw_numerator == 1
    assert replay_metric.raw_denominator == 1
    assert replay_metric.point_estimate == 1.0


def test_governed_alias_replay_without_identity_basis_is_unsafe() -> None:
    inputs = _fixture_rows()
    row = inputs["episode_rows"][0]
    row["model_output"].update(
        {
            "decision_source": "governed_exact_alias_replay",
            "resolution_scope": "tenant_global_exact",
            "llm_invoked": False,
        }
    )
    row["assessment"]["identity_evidence_refs"] = []

    state = analyze_entity_grounding_rows(**inputs)

    assert state.alias_replay_exposure_count == 1
    assert state.alias_replay_resolved_count == 1
    assert state.unsafe_alias_replay_count == 1
    assert state.incident_counts["unsafe_governed_alias_replay"] == 1


def test_context_dependent_phrase_cannot_be_governed_alias_replay() -> None:
    inputs = _fixture_rows(
        phrase="the project",
        content_text="the project is blocked",
    )
    inputs["episode_rows"][0]["model_output"].update(
        {
            "decision_source": "governed_exact_alias_replay",
            "identity_basis_ref": "clarification-request:replay-contextual",
            "resolution_scope": "source_context_only",
            "llm_invoked": False,
        }
    )

    state = analyze_entity_grounding_rows(**inputs)

    assert state.alias_replay_exposure_count == 1
    assert state.contextual_alias_replay_count == 1
    assert state.incident_counts["contextual_governed_alias_replay"] == 1


def test_adjudicated_successor_generation_is_history_not_duplicate_trace() -> None:
    inputs = _fixture_rows(review=True)
    original = inputs["episode_rows"][0]
    successor = deepcopy(original)
    successor_trace_id = uuid4()
    successor_request_id = str(uuid4())
    successor.update(
        {
            "id": successor_trace_id,
            "created_at": NOW + timedelta(minutes=2),
            "current_fate": "resolved_for_consumer",
            "selected_referent": {
                "type": "customer",
                "id": "customer:nimbus",
            },
            "has_review_obligation": False,
            "trace": {
                "supersedes_grounding_trace_id": str(original["id"]),
                "adjudication_ref": f"clarification-request:{uuid4()}",
                "correction_kind": "entity_clarification_adjudication",
            },
        }
    )
    successor["request"]["request_id"] = successor_request_id
    successor["candidate_set"]["request"]["request_id"] = successor_request_id
    successor["decision"]["disposition"] = "single_referent"
    successor["decision"]["selected_referent"] = successor["selected_referent"]

    successor_candidate_request = deepcopy(inputs["candidate_request_rows"][0])
    successor_candidate_request["id"] = successor_request_id
    successor_candidate_request["request"]["request_id"] = successor_request_id
    inputs["candidate_request_rows"].append(successor_candidate_request)
    inputs["episode_rows"].append(successor)
    inputs["work_items"].insert(
        0,
        {
            "id": uuid4(),
            "source_observation_id": original["source_observation_id"],
            "phrase": original["phrase"],
            "processing_generation": 2,
            "status": "resolved_for_consumer",
            "processing_class": "R2",
            "next_attempt_at": None,
            "current_trace_id": successor_trace_id,
            "updated_at": NOW + timedelta(minutes=2),
        },
    )

    state = analyze_entity_grounding_rows(**inputs)

    assert state.work_fate_counts == {"resolved_for_consumer": 1}
    assert state.trace_count == 2
    assert state.duplicate_trace_count == 0
    assert state.stage_complete_trace_count == 2
    assert state.stage_continuity_rate == 1.0
    assert state.candidate_request_count == 2
    assert state.immutable_candidate_set_count == 2
    assert state.candidate_request_fate_coverage == 1.0
    assert state.review_fate_count == 0
    assert "duplicate_terminal_trace" not in state.incident_counts
    assert "grounding_trace_head_not_current_generation" not in state.incident_counts


def test_unrelated_trace_rows_remain_duplicate_current_heads() -> None:
    inputs = _fixture_rows()
    unrelated = deepcopy(inputs["episode_rows"][0])
    unrelated["id"] = uuid4()
    unrelated["created_at"] = NOW + timedelta(minutes=2)
    unrelated["trace"] = {}
    inputs["episode_rows"].append(unrelated)

    state = analyze_entity_grounding_rows(**inputs)

    assert state.trace_count == 2
    assert state.duplicate_trace_count == 1
    assert state.incident_counts["duplicate_terminal_trace"] == 1


def test_supersession_cycle_is_an_invalid_headless_lineage() -> None:
    inputs = _fixture_rows()
    first = inputs["episode_rows"][0]
    second = deepcopy(first)
    second["id"] = uuid4()
    second["created_at"] = NOW + timedelta(minutes=2)
    first["trace"] = {"supersedes_grounding_trace_id": str(second["id"])}
    second["trace"] = {"supersedes_grounding_trace_id": str(first["id"])}
    inputs["episode_rows"].append(second)
    inputs["work_items"][0]["processing_generation"] = 2
    inputs["work_items"][0]["current_trace_id"] = second["id"]

    state = analyze_entity_grounding_rows(**inputs)

    assert state.incident_counts["invalid_grounding_trace_supersession"] == 2
    assert state.incident_counts["grounding_trace_head_not_current_generation"] == 1


def test_zero_grounding_exposure_is_unknown_not_perfect() -> None:
    scope = GroundingEvaluationScope(
        tenant_id=uuid4(),
        observation_start=NOW,
        observation_end=NOW + timedelta(hours=1),
        run_id="empty-grounding-scope",
    )
    state = analyze_entity_grounding_rows(
        scope=scope,
        observations=(),
        work_items=(),
        episode_rows=(),
        resolver_created_alias_count=0,
        self_authoritative_observation_count=0,
        artifact_refs=("pytest://empty-grounding-scope",),
    )

    assert state.work_population_coverage is None
    assert state.mention_detection_population_coverage is None
    assert state.explicit_anchor_reconstructability_rate is None
    assert state.mention_source_hash_match_rate is None
    assert state.mention_context_continuity_rate is None
    assert state.mention_protocol_closure_rate is None
    assert state.detected_mention_to_candidate_continuity_rate is None
    assert state.rejected_not_anchored_correctness_rate is None
    assert state.terminal_trace_coverage is None
    assert state.stage_continuity_rate is None
    assert state.candidate_request_fate_coverage is None
    assert state.answered_entity_clarification_lineage_coverage is None
    assert state.adjudicated_alias_lineage_coverage is None
    assert state.alias_replay_exposure_count == 0
    assert state.alias_replay_resolved_count == 0
    assert state.alias_replay_resolution_rate is None
    assert state.alias_replay_llm_avoided_count == 0
    assert state.unsafe_alias_replay_count == 0
    assert state.contextual_alias_replay_count == 0
    assert "unknown/not exposed" in render_entity_grounding_markdown(state)


def test_corrective_memory_lineage_and_reuse_are_reported_continuously() -> None:
    inputs = _fixture_rows(review=True)
    inputs.update(
        {
            "answered_entity_clarification_count": 2,
            "answered_entity_clarification_lineage_count": 1,
            "adjudicated_alias_count": 1,
            "adjudicated_alias_lineage_count": 1,
            "corrective_memory_observed_reuse_count": 1,
        }
    )
    state = analyze_entity_grounding_rows(**inputs)

    assert state.answered_entity_clarification_lineage_coverage == 0.5
    assert state.adjudicated_alias_lineage_coverage == 1.0
    assert state.corrective_memory_observed_reuse_count == 1
    assert (
        state.incident_counts[
            "answered_clarification_without_grounding_lineage"
        ]
        == 1
    )
    assert "adjudicated_alias_without_grounding_lineage" not in state.incident_counts
    rendered = render_entity_grounding_markdown(state)
    assert "Answered entity-clarification lineage: **1/2 (50.0%)**" in rendered
    assert "Corrective-memory future reuse observed: **1**" in rendered


def test_evaluator_preserves_each_structural_failure_as_an_incident() -> None:
    inputs = _fixture_rows(review=True)
    row = inputs["episode_rows"][0]
    snapshot = row["snapshot"]
    snapshot["selected_items"][0]["emitted_at"] = (
        NOW + timedelta(minutes=2)
    ).isoformat()
    candidate_set = row["candidate_set"]
    candidate_set["lane_fates"] = []
    candidate_set["candidates"] = candidate_set["candidates"][:1]
    row["assessment"]["identity_evidence_refs"] = []
    row["decision"]["disposition"] = "single_referent"
    row.update(
        {
            "model_output": {
                "closed_set_match": False,
                "canonical_ref": {"type": "customer", "id": "invented"},
            },
            "selected_candidate_id": "candidate:invented",
            "selected_referent": {"type": "customer", "id": "invented"},
            "identity_registry_mutated": True,
            "source_observation_mutated": True,
            "has_review_obligation": False,
        }
    )
    mention_row = inputs["mention_detection_rows"][0]
    mention_row["mention"]["primary_anchor"]["surface_form"] = "BROKEN"
    mention_row["committed_context_snapshot_digest"] = "0" * 64
    inputs["candidate_request_rows"][0]["entity_mention_id"] = uuid4()
    inputs["mention_events"] = []
    inputs["mention_outboxes"] = []
    inputs["episode_rows"] = [row, row]
    inputs["resolver_created_alias_count"] = 1
    inputs["self_authoritative_observation_count"] = 1
    inputs["artifact_refs"] = ("pytest://tampered-grounding-fixture",)
    state = analyze_entity_grounding_rows(**inputs)

    assert state.duplicate_trace_count == 1
    assert state.future_context_leak_count == 1
    assert state.incomplete_lane_fate_count == 1
    assert state.missing_open_world_option_count == 1
    assert state.invented_candidate_admission_count == 1
    assert state.single_referent_without_identity_basis_count == 1
    assert state.incident_counts["resolver_mutated_identity_registry"] == 2
    assert state.incident_counts["resolver_mutated_source_observation"] == 1
    assert state.incident_counts["review_without_obligation"] == 1
    assert state.explicit_anchor_reconstructability_rate == 0.0
    assert state.mention_context_continuity_rate == 0.0
    assert state.mention_event_coverage == 0.0
    assert state.mention_outbox_coverage == 0.0
    assert state.detected_mention_to_candidate_continuity_rate == 0.0
    assert "mention_explicit_anchor_not_reconstructable" in state.incident_counts
    assert "mention_context_snapshot_discontinuity" in state.incident_counts
    assert "mention_command_event_closure" in state.incident_counts
    assert "detected_mention_without_exact_candidate_request" in state.incident_counts


def test_correct_rejected_not_anchored_is_terminal_without_candidate_or_trace() -> None:
    inputs = _fixture_rows(content_text="Nothing in this signal names that account")
    state = analyze_entity_grounding_rows(**inputs)

    assert state.mention_detection_fate_counts == {"rejected_not_anchored": 1}
    assert state.rejected_not_anchored_correctness_rate == 1.0
    assert state.rejected_candidate_request_count == 0
    assert state.terminal_work_count == 1
    assert state.terminal_trace_required_count == 0
    assert state.terminal_trace_coverage is None
    assert state.stage_continuity_rate is None
    assert state.incident_counts == {}


def test_false_not_anchored_rejection_and_candidate_leak_are_localized() -> None:
    inputs = _fixture_rows()
    mention_row = inputs["mention_detection_rows"][0]
    mention_row["fate"] = "rejected_not_anchored"
    mention_row["mention_id"] = None
    mention_row["mention"] = None
    inputs["episode_rows"] = []
    state = analyze_entity_grounding_rows(**inputs)

    assert state.rejected_not_anchored_correctness_rate == 0.0
    assert state.rejected_candidate_request_count == 1
    assert state.incident_counts["mention_false_rejected_not_anchored"] == 1
    assert state.incident_counts["rejected_mention_has_candidate_request"] == 1
    assert state.incident_refs["mention_false_rejected_not_anchored"] == (
        f"mention-detection:{mention_row['id']}",
    )


def test_proof_projection_is_continuous_and_honest_about_evidence_floor() -> None:
    inputs = _fixture_rows()
    scope = inputs["scope"]
    state = analyze_entity_grounding_rows(**inputs)
    evidence = build_entity_grounding_invariant_evidence(
        state,
        registry=REGISTRY,
        executed_scenario_ids=frozenset(
            {
                "EVIDENCE-REENTRY",
                "ENTITY-STAGE-FATE",
                "CANDIDATE-FATE",
                "ENTITY-LOCAL-NOT-GLOBAL",
            }
        ),
    )
    report = compile_invariant_proof_matrix(
        REGISTRY,
        run_id=scope.run_id,
        evidence=evidence,
    )

    assert len(evidence) == 4
    assert all(item.achieved_evidence_tier is EvidenceTier.E3 for item in evidence)
    inv05_evidence = next(
        item for item in evidence if item.invariant_id == "INV-05"
    )
    replay_metric = next(
        item
        for item in inv05_evidence.metric_observations
        if item.metric_id == "entity.governed_alias_replay_resolution"
    )
    assert replay_metric.raw_denominator == 0
    assert replay_metric.point_estimate is None
    inv06 = next(item for item in report.records if item.invariant_id == "INV-06")
    assert inv06.denominator_coverage == 1.0
    assert inv06.metric_observation_coverage == 1.0
    assert inv06.substantiation_state is SubstantiationState.INSUFFICIENT
    assert "minimum evidence tier not achieved" in inv06.proof_gaps


def test_markdown_exposes_fates_incidents_and_proof_limits() -> None:
    state = analyze_entity_grounding_rows(**_fixture_rows())
    rendered = render_entity_grounding_markdown(state)

    assert "Durable work coverage: **1/1 (100.0%)**" in rendered
    assert "Mention-candidate terminal-fate coverage: **1/1 (100.0%)**" in rendered
    assert "Gold entity-extraction quality: **not measured**" in rendered
    assert "Exact explicit-anchor reconstructability: **1/1 (100.0%)**" in rendered
    assert "resolved_for_consumer: 1" in rendered
    assert "none observed in this scope" in rendered
    assert "below the E4 full-system simulation floor" in rendered
