"""Production-shaped Slack ingest -> grounding handoff integration proof."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import pytest

from lib.llm.provider import LLMConfig, LLMProvider
from lib.evaluation.source_semantics import (
    SourceSemanticEvaluationScope,
    evaluate_source_semantic_state,
)
from services.app.gateway.clarifications_router import (
    _apply_entity_resolution_answer,
)
from services.domain.clarifications import answer_clarification_request
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.ingest.ingestion.embedding.models import EmbeddingEnvelope
from services.ingest.ingestion.core import ingest_from_draft
from services.ingest.ingestion.handlers.slack import handle_slack_message
from services.ingest.ingestion.writers.embedding_worker.embedding_worker import (
    embed_and_update,
)
from services.workers.entity_resolver.worker import EntityResolverWorker
from services.workers.source_semantic_worker import SourceSemanticWorker


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _DeterministicEmbedder:
    class _Config:
        expected_dim = 768

    config = _Config()

    async def embed(self, _text: str) -> list[float]:
        return [0.01] * self.config.expected_dim


class _UnusedDlqProducer:
    async def produce(self, **_kwargs: Any) -> None:
        raise AssertionError("the successful embedding path must not publish a DLQ item")


class _ScriptedResolver(LLMProvider):
    def __init__(self, response: dict[str, Any]) -> None:
        super().__init__(
            LLMConfig(provider="anthropic", api_key="test", model="test")
        )
        self._response = json.dumps(response)
        self.calls: list[dict[str, Any]] = []

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: dict[str, Any] | None,
    ) -> str:
        self.calls.append(
            {
                "system": system,
                "user": user,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "schema_hint": schema_hint,
            }
        )
        return self._response


def _slack_message(
    *,
    text: str,
    ts: str,
    thread_ts: str | None = None,
) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "message",
        "user": "U-NORTHSTAR",
        "text": text,
        "ts": ts,
        "channel": "C-MIGRATIONS",
        "channel_type": "channel",
    }
    if thread_ts is not None:
        event["thread_ts"] = thread_ts
        event["parent_user_id"] = "U-NORTHSTAR"
    return {"team_id": "T-FYRALIS", "event": event}


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


async def test_slack_thread_review_adjudication_reaches_original_grounded_belief(
    resolver_db: asyncpg.Pool,
    tenant_id,
) -> None:
    """Prove review correction closes into one Model from the original Slack row."""

    alias_repo = EntityAliasRepo(resolver_db)
    await alias_repo.insert_alias(
        phrase="Project Northstar",
        resolved_entity_ref={"type": "goal", "id": "project-northstar"},
        source="manual",
        confidence=0.99,
        tenant_id=tenant_id,
        extra_metadata={
            "identity_basis_class": "independently_adjudicated",
            "identity_basis_ref": "test-adjudication:goal:project-northstar",
        },
    )

    base = datetime.now(timezone.utc) - timedelta(seconds=10)
    root_ts = f"{base.timestamp():.6f}"
    reply_ts = f"{(base + timedelta(seconds=1)).timestamp():.6f}"
    root_payload = _slack_message(
        text="Project Northstar is the migration program",
        ts=root_ts,
    )
    reply_payload = _slack_message(
        text="the project is blocked",
        ts=reply_ts,
        thread_ts=root_ts,
    )

    # The source handler preserves Slack topology but does not manufacture a
    # resolver queue. Opportunity discovery belongs to the shared ingest path.
    root_draft = await handle_slack_message(root_payload, {})
    reply_draft = await handle_slack_message(reply_payload, {})
    assert root_draft.unresolved_phrases == []
    assert reply_draft.unresolved_phrases == []
    assert reply_draft.content["thread_ts"] == root_ts
    assert "_unresolved_phrases" not in reply_draft.content

    root_result = await ingest_from_draft(
        channel="slack:message",
        draft=root_draft,
        pool=resolver_db,
        tenant_id=tenant_id,
        actor_repo=None,
        alias_repo=alias_repo,
        embedder=_DeterministicEmbedder(),
    )
    reply_result = await ingest_from_draft(
        channel="slack:message",
        draft=reply_draft,
        pool=resolver_db,
        tenant_id=tenant_id,
        actor_repo=None,
        alias_repo=alias_repo,
        embedder=_DeterministicEmbedder(),
    )
    root_id = root_result.observation.id
    reply_id = reply_result.observation.id

    assert reply_result.observation.content["_unresolved_phrases"] == [
        "the project"
    ]

    provider = _ScriptedResolver(
        {
            "canonical_ref": {"type": "goal", "id": "project-northstar"},
            "confidence": 0.97,
            "reasoning": "the threaded root names Project Northstar",
        }
    )
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=alias_repo,
    )

    decisions = await worker.process_observation(reply_id, tenant_id)
    assert len(provider.calls) == 1
    assert "Project Northstar is the migration program" in provider.calls[0]["user"]

    async with resolver_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              gt.id AS grounding_trace_id,
              gt.current_fate,
              gt.context_snapshot_id,
              gt.entity_mention_detection_id,
              gt.entity_mention_id,
              gt.candidate_request_id,
              gt.candidate_set_id,
              gt.resolution_assessment_id,
              gt.grounding_admission_id,
              ics.snapshot_version,
              ics.snapshot,
              emd.candidate_surface,
              emd.detection_version AS mention_detection_version,
              emd.fate AS mention_fate,
              emd.mention,
              ecgr.context_snapshot_id AS request_context_snapshot_id,
              ecgr.entity_mention_detection_id AS request_detection_id,
              ecgr.entity_mention_id AS request_mention_id,
              ecs.request_id AS set_request_id,
              ecs.candidate_set_version,
              ecs.candidates,
              ra.candidate_set_id AS assessment_candidate_set_id,
              ra.selected_candidate_id,
              ra.suggested_canonical_ref,
              ra.assessment_version,
              gad.assessment_id AS admission_assessment_id,
              gad.decision_version AS grounding_admission_version,
              gad.disposition AS admission_disposition,
              gad.reason_codes AS admission_reason_codes
            FROM grounding_traces gt
            JOIN interpretation_context_snapshots ics
              ON ics.id = gt.context_snapshot_id
            JOIN entity_mention_detections emd
              ON emd.id = gt.entity_mention_detection_id
            JOIN entity_candidate_generation_requests ecgr
              ON ecgr.id = gt.candidate_request_id
            JOIN entity_candidate_sets ecs
              ON ecs.id = gt.candidate_set_id
            JOIN resolution_assessments ra
              ON ra.id = gt.resolution_assessment_id
            JOIN grounding_admission_decisions gad
              ON gad.id = gt.grounding_admission_id
            WHERE gt.tenant_id = $1
              AND gt.source_observation_id = $2
              AND gt.phrase = 'the project'
            """,
            tenant_id,
            reply_id,
        )
        clarification = await conn.fetchrow(
            """
            SELECT id, object_kind, object_id, source_observation_id, payload
            FROM clarification_requests
            WHERE tenant_id = $1
              AND source_observation_id = $2
              AND kind = 'entity_resolution'
            """,
            tenant_id,
            reply_id,
        )
        downstream = await conn.fetch(
            """
            SELECT id, payload
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND observation_id = $2
              AND trigger_kind = 'T1'
              AND trigger_subkind = 'entity_resolved_late'
            ORDER BY enqueued_at, id
            """,
            tenant_id,
            reply_id,
        )

    assert row is not None
    snapshot = _json(row["snapshot"])
    selected_by_id = {
        item["event_revision_id"]: item for item in snapshot["selected_items"]
    }
    reply_revision = f"observation:{reply_id}:v1"
    root_revision = f"observation:{root_id}:v1"
    assert reply_revision in selected_by_id
    assert root_revision in selected_by_id
    assert selected_by_id[root_revision]["layer"] == "source_topology"
    assert "thread/reply/edit lineage" in selected_by_id[root_revision][
        "inclusion_reasons"
    ]
    assert snapshot["sufficiency_verdict"]["disposition"] == "needs_expansion"

    mention = _json(row["mention"])
    anchor = mention["primary_anchor"]
    assert row["candidate_surface"] == "the project"
    assert row["mention_fate"] == "detected"
    assert anchor["kind"] == "explicit"
    assert anchor["surface_form"] == "the project"
    assert anchor["coordinate"]["source_revision"] == (
        f"observation:{reply_id}:v1"
    )
    assert anchor["coordinate"]["span_start"] == 0
    assert anchor["coordinate"]["span_end"] == len("the project")

    assert row["request_context_snapshot_id"] == row["context_snapshot_id"]
    assert row["request_detection_id"] == row["entity_mention_detection_id"]
    assert row["request_mention_id"] == row["entity_mention_id"]
    assert row["set_request_id"] == row["candidate_request_id"]
    assert row["assessment_candidate_set_id"] == row["candidate_set_id"]
    assert row["admission_assessment_id"] == row["resolution_assessment_id"]
    candidates = _json(row["candidates"])
    northstar = next(
        candidate
        for candidate in candidates
        if candidate.get("canonical_referent_id") == "project-northstar"
    )
    assert northstar["candidate_type"] == "goal"
    assert northstar["canonical_referent_version"] == 1
    assert row["selected_candidate_id"] == northstar["candidate_id"]
    assert _json(row["suggested_canonical_ref"]) == {
        "type": "goal",
        "id": "project-northstar",
        "version": 1,
    }

    # A context-dependent Slack phrase cannot be auto-admitted merely because
    # its threaded root is useful. Lock the current conservative behavior: the
    # exact assessment reaches a live review obligation, while no downstream
    # Think trigger receives an unapproved seed_entity_ids value.
    assert decisions == [("the project", "review")]
    assert row["current_fate"] == "review"
    assert row["admission_disposition"] == "review"
    assert row["admission_reason_codes"] == [
        "context_not_operationally_sufficient:needs_expansion"
    ]
    assert downstream == []
    assert clarification is not None
    clarification_payload = _json(clarification["payload"])
    review_candidates = clarification_payload["candidates"]
    assert review_candidates == [
        {
            "candidate_id": northstar["candidate_id"],
            "canonical_ref": {
                "type": "goal",
                "id": "project-northstar",
                "version": 1,
            },
            "confidence": 0.97,
            "reasoning": "the threaded root names Project Northstar",
            "assessment_id": str(row["resolution_assessment_id"]),
            "assessment_version": row["assessment_version"],
            "grounding_trace_id": str(row["grounding_trace_id"]),
            "grounding_admission_id": str(row["grounding_admission_id"]),
            "grounding_admission_version": row["grounding_admission_version"],
        }
    ]
    assert clarification["object_kind"] == "grounding_trace"
    assert clarification["object_id"] == row["grounding_trace_id"]
    assert clarification["source_observation_id"] == reply_id
    feedback_lineage = clarification_payload["feedback_lineage"]
    assert feedback_lineage == {
        "grounding_trace_id": str(row["grounding_trace_id"]),
        "context_snapshot_id": str(row["context_snapshot_id"]),
        "context_snapshot_version": row["snapshot_version"],
        "entity_mention_detection_id": str(row["entity_mention_detection_id"]),
        "entity_mention_detection_version": row["mention_detection_version"],
        "entity_mention_id": str(row["entity_mention_id"]),
        "entity_mention_version": mention["mention_version"],
        "candidate_set_id": str(row["candidate_set_id"]),
        "candidate_set_version": row["candidate_set_version"],
        "resolution_assessment_id": str(row["resolution_assessment_id"]),
        "resolution_assessment_version": row["assessment_version"],
        "grounding_admission_id": str(row["grounding_admission_id"]),
        "grounding_admission_version": row["grounding_admission_version"],
        "grounding_disposition": row["admission_disposition"],
    }

    answer = {
        "action": "accept_candidate",
        "canonical_ref": {
            "type": "goal",
            "id": "project-northstar",
            "version": 1,
        },
        "confidence": 0.99,
    }
    async with resolver_db.acquire() as conn, conn.transaction():
        answered = await answer_clarification_request(
            conn,
            tenant_id=tenant_id,
            request_id=clarification["id"],
            answer=answer,
            answered_by=None,
        )
        assert answered is not None
        await _apply_entity_resolution_answer(
            conn,
            row=answered,
            answer=answer,
            tenant_id=tenant_id,
            answered_by=None,
        )

    async with resolver_db.acquire() as conn:
        trace_rows = await conn.fetch(
            """
            SELECT
              gt.id,
              gt.current_fate,
              gt.context_snapshot_id,
              gt.entity_mention_detection_id,
              gt.entity_mention_id,
              gt.candidate_request_id,
              gt.candidate_set_id,
              gt.resolution_assessment_id,
              gt.grounding_admission_id,
              gt.selected_referent,
              gt.trace,
              gad.disposition AS admission_disposition,
              gad.reason_codes AS admission_reason_codes
            FROM grounding_traces gt
            JOIN grounding_admission_decisions gad
              ON gad.tenant_id=gt.tenant_id
             AND gad.id=gt.grounding_admission_id
            WHERE gt.tenant_id=$1
              AND gt.source_observation_id=$2
              AND gt.phrase='the project'
            ORDER BY gt.created_at, gt.id
            """,
            tenant_id,
            reply_id,
        )
        grounding_work = await conn.fetch(
            """
            SELECT processing_generation, status, current_trace_id,
                   useful_safe_fate
            FROM entity_grounding_work_items
            WHERE tenant_id=$1
              AND source_observation_id=$2
              AND phrase='the project'
            ORDER BY processing_generation
            """,
            tenant_id,
            reply_id,
        )
        semantic_work = await conn.fetch(
            """
            SELECT work.grounding_trace_id, work.status, work.attempt_count
            FROM source_semantic_work_items work
            JOIN grounding_traces trace
              ON trace.tenant_id=work.tenant_id
             AND trace.id=work.grounding_trace_id
            WHERE work.tenant_id=$1
              AND trace.source_observation_id=$2
              AND trace.phrase='the project'
            """,
            tenant_id,
            reply_id,
        )
        model_count_before = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id=$1",
            tenant_id,
        )

    assert len(trace_rows) == 2
    original_trace = next(
        trace for trace in trace_rows if trace["id"] == row["grounding_trace_id"]
    )
    successor_trace = next(
        trace for trace in trace_rows if trace["id"] != row["grounding_trace_id"]
    )
    assert original_trace["current_fate"] == "review"
    assert successor_trace["current_fate"] == "resolved_for_consumer"
    assert successor_trace["admission_disposition"] == "single_referent"
    assert successor_trace["admission_reason_codes"] == [
        "independently_adjudicated_single_referent"
    ]
    assert _json(successor_trace["selected_referent"]) == {
        "type": "goal",
        "id": "project-northstar",
        "version": 1,
    }
    assert successor_trace["context_snapshot_id"] == original_trace[
        "context_snapshot_id"
    ]
    assert successor_trace["entity_mention_detection_id"] == original_trace[
        "entity_mention_detection_id"
    ]
    assert successor_trace["entity_mention_id"] == original_trace[
        "entity_mention_id"
    ]
    assert successor_trace["candidate_request_id"] != original_trace[
        "candidate_request_id"
    ]
    assert successor_trace["candidate_set_id"] != original_trace["candidate_set_id"]
    assert successor_trace["resolution_assessment_id"] != original_trace[
        "resolution_assessment_id"
    ]
    assert successor_trace["grounding_admission_id"] != original_trace[
        "grounding_admission_id"
    ]
    successor_lineage = _json(successor_trace["trace"])
    assert successor_lineage["supersedes_grounding_trace_id"] == str(
        original_trace["id"]
    )
    assert successor_lineage["adjudication_ref"] == (
        f"clarification-request:{clarification['id']}"
    )
    assert successor_lineage["correction_kind"] == (
        "entity_clarification_adjudication"
    )
    assert [
        (item["processing_generation"], item["status"], item["current_trace_id"])
        for item in grounding_work
    ] == [
        (1, "review", original_trace["id"]),
        (2, "resolved_for_consumer", successor_trace["id"]),
    ]
    assert _json(grounding_work[1]["useful_safe_fate"])[
        "supersedes_grounding_trace_id"
    ] == str(original_trace["id"])
    assert {
        (
            item["grounding_trace_id"],
            item["status"],
            item["attempt_count"],
        )
        for item in semantic_work
    } == {
        (original_trace["id"], "pending", 0),
        (successor_trace["id"], "pending", 0),
    }
    assert model_count_before == 0

    semantic_worker = SourceSemanticWorker(
        pool=resolver_db,
        worker_id=f"pytest:clarification-successor:{tenant_id}",
    )
    await semantic_worker.process_batch(limit=1000)

    async with resolver_db.acquire() as conn:
        applied = await conn.fetchrow(
            """
            SELECT
              work.status AS work_status,
              work.attempt_count,
              work.grounding_trace_id,
              interpretation.id AS interpretation_id,
              interpretation.source_observation_id,
              interpretation.resolution_assessment_id,
              interpretation.grounding_admission_id,
              interpretation.source_assertion,
              interpretation.grounding_continuity,
              admission.disposition,
              admission.admitted_model_id,
              model.born_from_event_id,
              model.proposition,
              model.scope_entities
            FROM source_semantic_work_items work
            JOIN source_semantic_interpretations interpretation
              ON interpretation.tenant_id=work.tenant_id
             AND interpretation.id=work.interpretation_id
            JOIN source_semantic_admission_decisions admission
              ON admission.tenant_id=work.tenant_id
             AND admission.id=work.admission_decision_id
            JOIN models model
              ON model.tenant_id=work.tenant_id
             AND model.id=work.admitted_model_id
            WHERE work.tenant_id=$1
              AND work.grounding_trace_id=$2
            """,
            tenant_id,
            successor_trace["id"],
        )
        review_terminal = await conn.fetchrow(
            """
            SELECT work.status, work.attempt_count,
                   admission.disposition, admission.admitted_model_id
            FROM source_semantic_work_items work
            JOIN source_semantic_admission_decisions admission
              ON admission.tenant_id=work.tenant_id
             AND admission.id=work.admission_decision_id
            WHERE work.tenant_id=$1
              AND work.grounding_trace_id=$2
            """,
            tenant_id,
            original_trace["id"],
        )
        counts = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM source_semantic_interpretations
               WHERE tenant_id=$1) AS interpretation_count,
              (SELECT count(*) FROM source_semantic_admission_decisions
               WHERE tenant_id=$1) AS admission_count,
              (SELECT count(*) FROM models
               WHERE tenant_id=$1 AND born_from_event_id=$2) AS model_count
            """,
            tenant_id,
            reply_id,
        )

    assert applied is not None
    assert review_terminal is not None
    assert review_terminal["status"] == "no_admission"
    assert review_terminal["attempt_count"] == 1
    assert review_terminal["disposition"] == "no_admission"
    assert review_terminal["admitted_model_id"] is None
    assert applied["work_status"] == "belief_applied"
    assert applied["attempt_count"] == 1
    assert applied["grounding_trace_id"] == successor_trace["id"]
    assert applied["source_observation_id"] == reply_id
    assert applied["disposition"] == "belief_applied"
    assert applied["admitted_model_id"] is not None
    assert applied["born_from_event_id"] == reply_id
    assertion = _json(applied["source_assertion"])
    assert assertion["expressed_content"] == "the project is blocked"
    assert len(assertion["coordinates"]) == 1
    coordinate = assertion["coordinates"][0]
    assert coordinate["evidence_record_id"] == f"observation:{reply_id}"
    assert coordinate["source_system"] == "slack"
    assert coordinate["source_object_id"] == f"observation:{reply_id}"
    assert coordinate["source_revision"] == f"observation:{reply_id}:v1"
    assert coordinate["field_path"] == "content_text"
    assert coordinate["span_start"] == 0
    assert coordinate["span_end"] == len("the project is blocked")
    continuity = _json(applied["grounding_continuity"])
    assert applied["resolution_assessment_id"] == successor_trace[
        "resolution_assessment_id"
    ]
    assert continuity["resolution_assessment_ref"] == (
        f"resolution-assessment:{successor_trace['resolution_assessment_id']}"
    )
    assert continuity["grounding_admission_ref"] == (
        f"grounding-admission:{applied['grounding_admission_id']}"
    )
    proposition = _json(applied["proposition"])
    assert proposition["source_semantic_interpretation_id"] == str(
        applied["interpretation_id"]
    )
    assert proposition["grounding_continuity"] == continuity
    assert _json(applied["scope_entities"]) == [
        {"type": "goal", "id": "project-northstar", "version": 1}
    ]
    assert dict(counts) == {
        "interpretation_count": 2,
        "admission_count": 2,
        "model_count": 1,
    }

    await semantic_worker.process_batch(limit=1000)
    async with resolver_db.acquire() as conn:
        assert await conn.fetchval(
            """
            SELECT count(*)
            FROM models
            WHERE tenant_id=$1 AND born_from_event_id=$2
            """,
            tenant_id,
            reply_id,
        ) == 1


async def test_slack_signal_reaches_one_grounded_belief_without_manual_handoff(
    resolver_db: asyncpg.Pool,
    tenant_id,
) -> None:
    """Prove the thin live path from a Slack payload to one canonical Model."""

    alias_repo = EntityAliasRepo(resolver_db)
    await alias_repo.insert_alias(
        phrase="Nimbus Bank",
        resolved_entity_ref={"type": "customer", "id": "customer-nimbus"},
        source="manual",
        confidence=0.99,
        tenant_id=tenant_id,
        extra_metadata={
            "identity_basis_class": "independently_adjudicated",
            "identity_basis_ref": "test-adjudication:customer:nimbus",
        },
    )
    payload = _slack_message(
        text="NBI is blocked",
        ts=f"{(datetime.now(timezone.utc) - timedelta(seconds=5)).timestamp():.6f}",
    )
    draft = await handle_slack_message(payload, {})
    result = await ingest_from_draft(
        channel="slack:message",
        draft=draft,
        pool=resolver_db,
        tenant_id=tenant_id,
        actor_repo=None,
        alias_repo=alias_repo,
        embedder=_DeterministicEmbedder(),
    )
    assert result.observation.content["_unresolved_phrases"] == ["NBI"]

    provider = _ScriptedResolver(
        {
            "canonical_ref": {
                "type": "customer",
                "id": "customer-nimbus",
            },
            "confidence": 0.97,
            "reasoning": "NBI selects the independently adjudicated tenant entity",
        }
    )
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=alias_repo,
    )
    assert await worker.process_observation(
        result.observation.id,
        tenant_id,
    ) == [("NBI", "resolved")]

    async with resolver_db.acquire() as conn:
        before_semantics = await conn.fetchrow(
            """
            SELECT work.status,
                   (SELECT count(*) FROM models WHERE tenant_id=$1) AS model_count
            FROM source_semantic_work_items work
            JOIN grounding_traces trace
              ON trace.tenant_id=work.tenant_id
             AND trace.id=work.grounding_trace_id
            WHERE work.tenant_id=$1 AND trace.source_observation_id=$2
            """,
            tenant_id,
            result.observation.id,
        )
    assert before_semantics is not None
    assert before_semantics["status"] == "pending"
    assert before_semantics["model_count"] == 0

    semantic_worker = SourceSemanticWorker(
        pool=resolver_db,
        worker_id=f"pytest:ready:{tenant_id}",
    )
    await semantic_worker.process_batch(limit=1000)

    async with resolver_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT gt.current_fate,
                   ssi.id AS interpretation_id,
                   ssi.source_assertion,
                   ssi.grounding_continuity,
                   ssad.disposition,
                   ssad.admitted_model_id,
                   work.status AS work_status,
                   m.born_from_event_id,
                   m.proposition,
                   m.scope_entities
            FROM grounding_traces gt
            JOIN source_semantic_work_items work
              ON work.tenant_id=gt.tenant_id
             AND work.grounding_trace_id=gt.id
            JOIN source_semantic_interpretations ssi
              ON ssi.tenant_id=gt.tenant_id
             AND ssi.grounding_trace_id=gt.id
            JOIN source_semantic_admission_decisions ssad
              ON ssad.tenant_id=ssi.tenant_id
             AND ssad.interpretation_id=ssi.id
            JOIN models m
              ON m.tenant_id=ssad.tenant_id
             AND m.id=ssad.admitted_model_id
            WHERE gt.tenant_id=$1 AND gt.source_observation_id=$2
              AND gt.phrase='NBI'
            """,
            tenant_id,
            result.observation.id,
        )
        model_count = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id=$1",
            tenant_id,
        )
        legacy_think_trigger_count = await conn.fetchval(
            """
            SELECT count(*) FROM think_trigger_queue
            WHERE tenant_id=$1 AND observation_id=$2
              AND trigger_subkind='entity_resolved_late'
            """,
            tenant_id,
            result.observation.id,
        )

    assert row is not None
    assert row["current_fate"] == "resolved_for_consumer"
    assert row["work_status"] == "belief_applied"
    assert row["disposition"] == "belief_applied"
    assert row["admitted_model_id"] is not None
    assert row["born_from_event_id"] == result.observation.id
    assert model_count == 1
    assert legacy_think_trigger_count == 0
    assertion = _json(row["source_assertion"])
    assert assertion["current_speaker_or_author"] == "slack:U-NORTHSTAR"
    coordinate = assertion["coordinates"][0]
    assert draft.content_text[
        coordinate["span_start"] : coordinate["span_end"]
    ] == assertion["expressed_content"]
    proposition = _json(row["proposition"])
    continuity = _json(row["grounding_continuity"])
    assert proposition["kind"] == "belief"
    assert proposition["source_author_ref"] == "slack:U-NORTHSTAR"
    assert proposition["source_semantic_interpretation_id"] == str(
        row["interpretation_id"]
    )
    assert proposition["grounding_continuity"] == continuity
    assert _json(row["scope_entities"]) == [
        {"type": "customer", "id": "customer-nimbus", "version": 1}
    ]

    async with resolver_db.acquire() as conn:
        evaluation = await evaluate_source_semantic_state(
            conn,
            scope=SourceSemanticEvaluationScope(
                tenant_id=tenant_id,
                start=result.observation.occurred_at - timedelta(seconds=1),
                end=result.observation.occurred_at + timedelta(seconds=1),
                run_id="pytest-live-slack-grounded-belief",
            ),
            artifact_refs=("pytest://live-slack-grounded-belief",),
        )
    assert evaluation.eligible_grounding_interpretation_coverage == 1.0
    assert evaluation.source_coordinate_reconstructability_rate == 1.0
    assert evaluation.interpretation_structural_closure_rate == 1.0
    assert evaluation.grounding_continuity_exactness_rate == 1.0
    assert evaluation.explicit_admission_fate_coverage == 1.0
    assert evaluation.supported_report_admission_precision == 1.0
    assert evaluation.supported_report_admission_recall == 1.0
    assert evaluation.epistemic_consumer_admission_continuity_rate == 1.0
    assert evaluation.model_dependency_closure_rate == 1.0
    assert evaluation.incident_counts == {}

    await semantic_worker.process_batch(limit=1000)
    async with resolver_db.acquire() as conn:
        assert await conn.fetchval(
            """
            SELECT count(*)
            FROM models
            WHERE tenant_id=$1 AND born_from_event_id=$2
            """,
            tenant_id,
            result.observation.id,
        ) == 1


async def test_pending_slack_embedding_recovers_to_one_grounded_belief(
    resolver_db: asyncpg.Pool,
    tenant_id,
) -> None:
    """Grounding-first and embedding-later still reaches one terminal belief."""

    alias_repo = EntityAliasRepo(resolver_db)
    await alias_repo.insert_alias(
        phrase="Nimbus Bank",
        resolved_entity_ref={"type": "customer", "id": "customer-nimbus"},
        source="manual",
        confidence=0.99,
        tenant_id=tenant_id,
        extra_metadata={
            "identity_basis_class": "independently_adjudicated",
            "identity_basis_ref": "test-adjudication:customer:nimbus",
        },
    )
    payload = _slack_message(
        text="NBI is blocked",
        ts=f"{(datetime.now(timezone.utc) - timedelta(seconds=5)).timestamp():.6f}",
    )
    draft = await handle_slack_message(payload, {})
    result = await ingest_from_draft(
        channel="slack:message",
        draft=draft,
        pool=resolver_db,
        tenant_id=tenant_id,
        actor_repo=None,
        alias_repo=alias_repo,
        embedder=None,
    )
    assert result.observation.embedding_pending is True

    resolver = EntityResolverWorker(
        pool=resolver_db,
        llm=_ScriptedResolver(
            {
                "canonical_ref": {
                    "type": "customer",
                    "id": "customer-nimbus",
                },
                "confidence": 0.97,
                "reasoning": "NBI selects the independently adjudicated tenant entity",
            }
        ),
        alias_repo=alias_repo,
    )
    assert await resolver.process_observation(
        result.observation.id,
        tenant_id,
    ) == [("NBI", "resolved")]

    async with resolver_db.acquire() as conn:
        awaiting = await conn.fetchrow(
            """
            SELECT work.status, work.attempt_count,
                   (SELECT count(*) FROM models WHERE tenant_id=$1) AS model_count
            FROM source_semantic_work_items work
            JOIN grounding_traces trace
              ON trace.tenant_id=work.tenant_id
             AND trace.id=work.grounding_trace_id
            WHERE work.tenant_id=$1 AND trace.source_observation_id=$2
            """,
            tenant_id,
            result.observation.id,
        )
    assert awaiting is not None
    assert awaiting["status"] == "awaiting_embedding"
    assert awaiting["attempt_count"] == 0
    assert awaiting["model_count"] == 0

    embedding_status = await embed_and_update(
        env=EmbeddingEnvelope(
            tenant_id=tenant_id,
            source="slack",
            observation_id=result.observation.id,
            enqueued_at=datetime.now(timezone.utc),
        ),
        pool=resolver_db,
        embedder=_DeterministicEmbedder(),
        dlq_producer=_UnusedDlqProducer(),
    )
    assert embedding_status == "embedded"

    semantic_worker = SourceSemanticWorker(
        pool=resolver_db,
        worker_id=f"pytest:embedding-recovery:{tenant_id}",
    )
    await semantic_worker.process_batch(limit=1000)

    async with resolver_db.acquire() as conn:
        recovered = await conn.fetchrow(
            """
            SELECT work.status, work.attempt_count,
                   work.interpretation_id, work.admission_decision_id,
                   work.admitted_model_id, model.proposition,
                   model.scope_entities,
                   (SELECT count(*)
                    FROM source_semantic_interpretations
                    WHERE tenant_id=$1) AS interpretation_count,
                   (SELECT count(*)
                    FROM source_semantic_admission_decisions
                    WHERE tenant_id=$1) AS admission_count,
                   (SELECT count(*) FROM models WHERE tenant_id=$1) AS model_count
            FROM source_semantic_work_items work
            JOIN grounding_traces trace
              ON trace.tenant_id=work.tenant_id
             AND trace.id=work.grounding_trace_id
            JOIN models model
              ON model.tenant_id=work.tenant_id
             AND model.id=work.admitted_model_id
            WHERE work.tenant_id=$1 AND trace.source_observation_id=$2
            """,
            tenant_id,
            result.observation.id,
        )
    assert recovered is not None
    assert recovered["status"] == "belief_applied"
    assert recovered["attempt_count"] == 1
    assert recovered["interpretation_id"] is not None
    assert recovered["admission_decision_id"] is not None
    assert recovered["admitted_model_id"] is not None
    assert recovered["interpretation_count"] == 1
    assert recovered["admission_count"] == 1
    assert recovered["model_count"] == 1
    proposition = _json(recovered["proposition"])
    assert proposition["source_author_ref"] == "slack:U-NORTHSTAR"
    assert _json(recovered["scope_entities"]) == [
        {"type": "customer", "id": "customer-nimbus", "version": 1}
    ]

    await semantic_worker.process_batch(limit=1000)
    async with resolver_db.acquire() as conn:
        counts = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM source_semantic_interpretations
               WHERE tenant_id=$1) AS interpretation_count,
              (SELECT count(*) FROM source_semantic_admission_decisions
               WHERE tenant_id=$1) AS admission_count,
              (SELECT count(*) FROM models WHERE tenant_id=$1) AS model_count
            """,
            tenant_id,
        )
    assert counts is not None
    assert dict(counts) == {
        "interpretation_count": 1,
        "admission_count": 1,
        "model_count": 1,
    }
