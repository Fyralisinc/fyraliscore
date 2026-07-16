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
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.ingest.ingestion.core import ingest_from_draft
from services.ingest.ingestion.handlers.slack import handle_slack_message
from services.workers.entity_resolver.worker import EntityResolverWorker


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _DeterministicEmbedder:
    class _Config:
        expected_dim = 768

    config = _Config()

    async def embed(self, _text: str) -> list[float]:
        return [0.01] * self.config.expected_dim


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


async def test_slack_thread_ingest_discovers_and_grounds_live_opportunity(
    resolver_db: asyncpg.Pool,
    tenant_id,
) -> None:
    """Exercise the real Slack draft and shared ingest path before grounding."""

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
              gt.current_fate,
              gt.context_snapshot_id,
              gt.entity_mention_detection_id,
              gt.entity_mention_id,
              gt.candidate_request_id,
              gt.candidate_set_id,
              gt.resolution_assessment_id,
              gt.grounding_admission_id,
              ics.snapshot,
              emd.candidate_surface,
              emd.fate AS mention_fate,
              emd.mention,
              ecgr.context_snapshot_id AS request_context_snapshot_id,
              ecgr.entity_mention_detection_id AS request_detection_id,
              ecgr.entity_mention_id AS request_mention_id,
              ecs.request_id AS set_request_id,
              ecs.candidates,
              ra.candidate_set_id AS assessment_candidate_set_id,
              ra.selected_candidate_id,
              ra.suggested_canonical_ref,
              ra.assessment_version,
              gad.assessment_id AS admission_assessment_id,
              gad.decision_version AS admission_version,
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
        review = await conn.fetchrow(
            """
            SELECT id, candidates
            FROM entity_review_queue
            WHERE tenant_id = $1 AND source_observation_id = $2
            """,
            tenant_id,
            reply_id,
        )
        clarification = await conn.fetchrow(
            """
            SELECT object_id, source_observation_id
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
    assert review is not None
    review_candidates = _json(review["candidates"])
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
        }
    ]
    assert clarification is not None
    assert clarification["object_id"] == review["id"]
    assert clarification["source_observation_id"] == reply_id


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
        row = await conn.fetchrow(
            """
            SELECT gt.current_fate,
                   ssi.id AS interpretation_id,
                   ssi.source_assertion,
                   ssi.grounding_continuity,
                   ssad.disposition,
                   ssad.admitted_model_id,
                   m.born_from_event_id,
                   m.proposition,
                   m.scope_entities
            FROM grounding_traces gt
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
