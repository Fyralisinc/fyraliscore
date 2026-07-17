from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.contracts.source_semantics import (
    SourceSemanticAdmissionDisposition,
)
from lib.embeddings.ollama import EMBEDDING_DIM
from lib.shared.ids import uuid7
from services.domain.entity_grounding.episode import (
    GroundingCandidateInput,
    GroundingEpisode,
    build_grounding_episode,
    candidate_id_for_ref,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection
from services.domain.entity_grounding.repo import EntityGroundingRepo
from services.domain.source_semantics.processor import GroundedBeliefProcessor


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


CUSTOMER_REF = {"type": "customer", "id": "customer:nimbus"}


async def _commit_grounding(
    conn: asyncpg.Connection,
    *,
    tenant_id,
    text: str,
    confidence: float,
) -> tuple[GroundingEpisode, object]:
    observation_id = uuid7()
    occurred_at = datetime.now(timezone.utc)
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            content, content_text, embedding_pending, trust_tier,
            entities_mentioned
        ) VALUES (
            $1, $2, $3, 'signal', 'slack:message', $4::jsonb, $5,
            TRUE, 'ordinary', '[]'::jsonb
        )
        """,
        observation_id,
        tenant_id,
        occurred_at,
        json.dumps({"text": text, "_unresolved_phrases": ["NBI"]}),
        text,
    )
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        occurred_at=occurred_at,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        now=occurred_at + timedelta(minutes=1),
    )
    mention_command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        content_text=text,
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=occurred_at + timedelta(minutes=1),
    )
    episode = build_grounding_episode(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        occurred_at=occurred_at,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        candidates=(
            GroundingCandidateInput(
                canonical_ref=CUSTOMER_REF,
                candidate_source="tenant_aliases",
                positive_evidence_refs=("entity-alias:NBI",),
                independent_identity_evidence_refs=("manual-alias:NBI",),
            ),
        ),
        model_candidate_id=candidate_id_for_ref(CUSTOMER_REF),
        model_canonical_ref=CUSTOMER_REF,
        model_confidence=confidence,
        model_reasoning="closed tenant candidate",
        high_confidence=0.8,
        review_min=0.5,
        prepared_context_command=context_command,
        prepared_context_outcome=context_outcome,
        prepared_mention_detection_command=mention_command,
        now=occurred_at + timedelta(minutes=1),
    )
    await EntityGroundingRepo(pool=object()).append_episode(  # type: ignore[arg-type]
        episode=episode,
        tenant_id=tenant_id,
        source_observation_id=observation_id,
        phrase="NBI",
        conn=conn,
    )
    trace_id = await conn.fetchval(
        """
        SELECT id FROM grounding_traces
        WHERE tenant_id=$1 AND source_observation_id=$2
        """,
        tenant_id,
        observation_id,
    )
    return episode, trace_id


def _payload(value):
    return json.loads(value) if isinstance(value, str) else value


async def test_asserted_report_is_the_only_grounded_semantic_path_to_one_model(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, 'grounded belief vertical', FALSE)
        """,
        tenant_id,
    )
    processor = GroundedBeliefProcessor()

    async with fresh_db.acquire() as conn:
        admitted_text = "NBI is blocked"
        admitted_episode, admitted_trace_id = await _commit_grounding(
            conn,
            tenant_id=tenant_id,
            text=admitted_text,
            confidence=0.91,
        )
        admitted = await processor.process_trace(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=admitted_trace_id,
            embedding=[0.01] * EMBEDDING_DIM,
        )
        duplicate = await processor.process_trace(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=admitted_trace_id,
            embedding=[0.01] * EMBEDDING_DIM,
        )

        question_text = "Is NBI blocked?"
        question_episode, question_trace_id = await _commit_grounding(
            conn,
            tenant_id=tenant_id,
            text=question_text,
            confidence=0.91,
        )
        question = await processor.process_trace(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=question_trace_id,
            embedding=[0.02] * EMBEDDING_DIM,
        )

        review_text = "NBI may be blocked"
        review_episode, review_trace_id = await _commit_grounding(
            conn,
            tenant_id=tenant_id,
            text=review_text,
            confidence=0.7,
        )
        review = await processor.process_trace(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=review_trace_id,
            embedding=[0.03] * EMBEDDING_DIM,
        )

        incidental_text = "NBI says Atlas is blocked"
        _incidental_episode, incidental_trace_id = await _commit_grounding(
            conn,
            tenant_id=tenant_id,
            text=incidental_text,
            confidence=0.91,
        )
        incidental = await processor.process_trace(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=incidental_trace_id,
            embedding=[0.04] * EMBEDDING_DIM,
        )

        models = await conn.fetch(
            """
            SELECT id, born_from_event_id, proposition, scope_entities
            FROM models WHERE tenant_id=$1 ORDER BY created_at
            """,
            tenant_id,
        )
        truth_rows = await conn.fetch(
            """
            SELECT h.model_id, h.version, h.lifecycle, v.proposition,
                   e.evidence_id, e.source_object_id, e.span_start, e.span_end,
                   d.decided_by, d.disposition
            FROM model_truth_heads h
            JOIN model_truth_versions v
              ON v.tenant_id=h.tenant_id AND v.version_id=h.version_id
            JOIN model_truth_evidence_references e
              ON e.tenant_id=v.tenant_id AND e.model_version_id=v.version_id
            JOIN truth_admission_decisions d
              ON d.tenant_id=v.tenant_id AND d.decision_id=v.admission_decision_id
            WHERE h.tenant_id=$1
            """,
            tenant_id,
        )
        interpretations = await conn.fetch(
            """
            SELECT id, source_assertion, semantic_frame, speech_act,
                   grounding_continuity
            FROM source_semantic_interpretations
            WHERE tenant_id=$1 ORDER BY recorded_at
            """,
            tenant_id,
        )
        decisions = await conn.fetch(
            """
            SELECT disposition, reason_codes, proposed_belief_assertion,
                   admitted_model_id
            FROM source_semantic_admission_decisions
            WHERE tenant_id=$1 ORDER BY decided_at
            """,
            tenant_id,
        )
        epistemic_admissions = await conn.fetch(
            """
            SELECT id, assessment_id, consumer, purpose, operation,
                   disposition, decision
            FROM grounding_admission_decisions
            WHERE tenant_id=$1 AND consumer='epistemic-applier'
            """,
            tenant_id,
        )
        forbidden_counts = {
            table: await conn.fetchval(
                f"SELECT count(*) FROM {table} WHERE tenant_id=$1",
                tenant_id,
            )
            for table in ("goals", "commitments", "decisions", "model_edges")
        }

    assert admitted.disposition is SourceSemanticAdmissionDisposition.BELIEF_APPLIED
    assert admitted.model_id is not None
    assert duplicate.duplicate is True
    assert duplicate.model_id == admitted.model_id
    assert question.disposition is SourceSemanticAdmissionDisposition.NO_ADMISSION
    assert question.model_id is None
    assert question.reason_codes == ("source_assertion_not_asserted",)
    assert review.disposition is SourceSemanticAdmissionDisposition.NO_ADMISSION
    assert review.model_id is None
    assert review.reason_codes == ("grounding_not_admitted_for_single_referent_use",)
    assert incidental.disposition is SourceSemanticAdmissionDisposition.NO_ADMISSION
    assert incidental.model_id is None
    assert incidental.reason_codes == ("source_assertion_not_asserted",)

    assert len(models) == 1
    model = models[0]
    proposition = _payload(model["proposition"])
    assert model["id"] == admitted.model_id
    assert model["born_from_event_id"] == (
        admitted_episode.mention_detection_command.detection.source_observation_id
    )
    assert proposition["kind"] == "belief"
    assert proposition["source_semantic_interpretation_id"] == str(
        admitted.interpretation_id
    )
    assert proposition["grounding_continuity"]["mention_ref"].startswith("mention:")
    assert _payload(model["scope_entities"]) == [
        {**CUSTOMER_REF, "version": 1}
    ]
    assert len(truth_rows) == 1
    truth = truth_rows[0]
    assert truth["model_id"] == admitted.model_id
    assert truth["version"] == 1
    assert truth["lifecycle"] == "active"
    assert truth["disposition"] == "accepted"
    assert truth["decided_by"] == "EpistemicApplier"
    assert truth["evidence_id"] == str(model["born_from_event_id"])
    assert truth["source_object_id"] == str(model["born_from_event_id"])
    assert truth["span_start"] == 0
    assert truth["span_end"] == len(admitted_text)

    assert len(interpretations) == 4
    admitted_interpretation = next(
        row for row in interpretations if row["id"] == admitted.interpretation_id
    )
    assertion = _payload(admitted_interpretation["source_assertion"])
    coordinate = assertion["coordinates"][0]
    assert admitted_text[coordinate["span_start"] : coordinate["span_end"]] == (
        assertion["expressed_content"]
    )
    continuity = _payload(admitted_interpretation["grounding_continuity"])
    assert continuity["downstream_object_ref"] == f"model:{admitted.model_id}"
    assert continuity["resolution_assessment_ref"].endswith(
        str(admitted_episode.assessment.assessment_id)
    )
    assert len(epistemic_admissions) == 1
    epistemic_admission = epistemic_admissions[0]
    assert epistemic_admission["assessment_id"] == UUID(
        admitted_episode.assessment.assessment_id
    )
    assert epistemic_admission["purpose"] == "belief-admission"
    assert epistemic_admission["operation"] == "create-grounded-belief"
    assert epistemic_admission["disposition"] == "single_referent"
    assert continuity["grounding_admission_ref"] == (
        f"grounding-admission:{epistemic_admission['id']}"
    )
    assert not continuity["grounding_admission_ref"].endswith(
        str(admitted_episode.admission.decision_id)
    )

    assert [row["disposition"] for row in decisions].count("belief_applied") == 1
    assert [row["disposition"] for row in decisions].count("no_admission") == 3
    assert all(count == 0 for count in forbidden_counts.values())
