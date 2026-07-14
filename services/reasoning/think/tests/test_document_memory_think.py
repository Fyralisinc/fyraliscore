"""Phase 1 (Layer 2) — document memory minted via Think.

These DB-gated tests prove the end of the document-memory path: a Think run over
a document-summary observation distills the structured extraction into durable
Models. Test 1 drives `think()` with a ScriptedProvider (proving the enriched
T1 flows through reasoning and the doc evidence block reaches the prompt, and a
commitment lands as a prediction with evaluate_at + falsifier). Test 2 applies a
richer document-derived diff (anchor + concern + recommendation + prediction +
edges) and proves provenance (born_from_event_id=observation_id), the deadline
contract, and Pathway-A retrievability by the document's scoped entity.

See docs/plans/document-memory-substrate.md §4.2–§4.6, §8 and §9.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext, primary_retrieve
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.diff_schema import ClaimOp, EdgeOp, ValidatedDiff
from services.reasoning.think.reason import think
from services.reasoning.think.tests.conftest import (
    ScriptedProvider,
    _insert_observation,
    make_embedding,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


_DUE = "2026-06-17T00:00:00+00:00"
_CUSTOMER_ID = "11111111-1111-1111-1111-111111111111"

_DOC_STRUCTURED = {
    "summary": "Acme weekly sync: SOW pending, SOC2 risk to renewal.",
    "key_points": ["call ran 45 minutes"],
    "decisions": ["Ship the billing revamp before the Sept 30 Acme renewal"],
    "action_items": [
        {"who": "Priya", "what": "send Acme the revised SOW", "due": "2026-06-17"}
    ],
    "risks": ["SOC2 audit slip endangers the Acme renewal"],
}


@pytest.fixture(autouse=True)
def _deterministic_question_planning(monkeypatch):
    monkeypatch.setenv("INQUIRY_LLM_QUESTION_PLANNING_ENABLED", "0")


async def _seed_doc_observation(pool, tenant: UUID) -> UUID:
    async with pool.acquire() as conn:
        return await _insert_observation(
            conn,
            tenant,
            content_text="Document 'Acme sync' summary brief.",
            source_channel="fireflies:transcript",
            external_id="ff-1",
            entities_mentioned=[{"type": "customer", "id": _CUSTOMER_ID}],
        )


def _doc_trigger(tenant: UUID, obs_id: UUID, trigger_id: UUID) -> TriggerContext:
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs_id,
        seed_natural_text="Acme sync summary brief.",
        seed_entity_ids=[{"type": "customer", "id": _CUSTOMER_ID}],
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=[],
    )
    # The enriched-T1 payload the summarization worker emits, carried on
    # seed_signature exactly as the worker does.
    trigger.seed_signature = {
        "trigger_id": str(trigger_id),
        "source_channel": "fireflies:transcript",
        "trust_tier": "authoritative",
        "summarized": True,
        "doc_structured_summary": _DOC_STRUCTURED,
        "doc_scope_entities": [{"type": "customer", "id": _CUSTOMER_ID}],
    }
    return trigger


def _prediction_diff(trigger_id: UUID, tenant: UUID, obs_id: UUID) -> str:
    """A minimal valid diff: one commitment minted as a prediction.

    Carries evaluate_at = the action-item due date, a prediction_deadline
    falsifier, and a resolution criterion (§4.2 commitment mapping).
    """
    return json.dumps({
        "trigger_ref": str(trigger_id),
        "tenant_id": str(tenant),
        "claim_ops": [
            {
                "op": "insert",
                "entry": {
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(obs_id),
                    "proposition": {
                        "kind": "prediction",
                        "expected": "Priya sends Acme the revised SOW by 2026-06-17",
                        "resolution": "SOW is delivered to Acme on or before the due date",
                        "claim_role": "prediction",
                        "time_mode": "future",
                        "modality": "expected",
                    },
                    "natural": "Priya to send Acme the revised SOW by 2026-06-17.",
                    "scope_actors": [],
                    "scope_entities": [{"type": "customer", "id": _CUSTOMER_ID}],
                    "scope_temporal": {
                        "valid_from": "2026-06-03T00:00:00+00:00",
                        "valid_until": _DUE,
                    },
                    "evaluate_at": _DUE,
                    "resolution_criteria": "Acme has received the revised SOW.",
                    "confidence": 0.6,
                    "confidence_at_assertion": 0.6,
                    "falsifier": {
                        "kind": "prediction_deadline",
                        "evaluate_at": _DUE,
                        "check": "no SOW delivered to Acme by the due date",
                    },
                },
            }
        ],
        "act_ops": [],
        "resource_ops": [],
        "new_predictions": [],
        "reasoning_trace": "doc-memory: minted the SOW commitment as a prediction.",
    })


async def test_think_over_doc_observation_mints_commitment_prediction(
    fresh_db, tenant, tenant_cleanup,
):
    """A Think run over a document observation distills the commitment into a
    prediction Model with evaluate_at + a deadline falsifier, provenance =
    observation_id, and the doc evidence block reaches the prompt."""
    trigger_id = uuid7()
    obs_id = await _seed_doc_observation(fresh_db, tenant)
    trigger = _doc_trigger(tenant, obs_id, trigger_id)
    provider = ScriptedProvider(
        responses=[_prediction_diff(trigger_id, tenant, obs_id)],
    )

    outcome = await think(
        trigger,
        fresh_db,
        llm_provider=provider,
        triggering_content="Acme sync summary brief.",
        reason_for_trigger="post-summary T1",
    )
    assert outcome.status == "success", outcome.error

    # The doc evidence block + contract reached the LLM.
    assert len(provider.calls) == 1
    call = provider.calls[0]
    assert "<document_structured_summary>" in call["user"]
    assert "send Acme the revised SOW" in call["user"]
    assert "Document structured summaries:" in call["system"]

    # The prediction Model landed with provenance + the deadline contract.
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT born_from_event_id, supporting_event_ids, evaluate_at,
                   claim_role, scope_entities
            FROM models
            WHERE tenant_id = $1 AND claim_role = 'prediction'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tenant,
        )
    assert row is not None, "expected a prediction Model to be minted"
    assert row["born_from_event_id"] == obs_id
    # Provenance: supporting_event_ids derives [observation_id] from born_from.
    assert obs_id in list(row["supporting_event_ids"])
    assert row["evaluate_at"] is not None
    assert row["evaluate_at"].isoformat() == _DUE
    scope_entities = (
        json.loads(row["scope_entities"])
        if isinstance(row["scope_entities"], str)
        else row["scope_entities"]
    )
    assert {"type": "customer", "id": _CUSTOMER_ID} in scope_entities


def _document_derived_diff(
    trigger_id: UUID, tenant: UUID, obs_id: UUID, *, member_a: UUID, member_b: UUID
) -> ValidatedDiff:
    """Anchor situation + concern + recommendation, all doc-scoped, with an
    instance_of edge anchor->member and an explains edge concern->decision."""
    return ValidatedDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant,
        claim_ops=[
            ClaimOp(op="insert", entry={
                "tenant_id": str(tenant),
                "born_from_event_id": str(obs_id),
                "proposition": {
                    "kind": "belief",
                    "claim_role": "situation",
                    "abstraction_level": "composite",
                    "situation": "Acme renewal at risk",
                    "summary": "Acme renewal depends on the SOW and SOC2.",
                    "member_model_ids": [str(member_a), str(member_b)],
                    "relationship_summary": "SOW delivery and SOC2 both gate renewal.",
                    "status": "forming",
                },
                "natural": "Acme renewal is a composite risk.",
                "scope_actors": [],
                "scope_entities": [{"type": "customer", "id": _CUSTOMER_ID}],
                "scope_temporal": {},
                "confidence": 0.6,
                "confidence_at_assertion": 0.6,
            }),
            ClaimOp(op="insert", entry={
                "tenant_id": str(tenant),
                "born_from_event_id": str(obs_id),
                "proposition": {
                    "kind": "belief",
                    "about": "Acme renewal",
                    "nature": "SOC2 audit slip endangers the renewal",
                    "raised_by": "meeting",
                    "claim_role": "concern",
                    "polarity": "negative",
                },
                "natural": "SOC2 slip endangers the Acme renewal.",
                "scope_actors": [],
                "scope_entities": [{"type": "customer", "id": _CUSTOMER_ID}],
                "scope_temporal": {},
                "confidence": 0.6,
                "confidence_at_assertion": 0.6,
            }),
        ],
        edge_ops=[
            # Cross-claim link using an EXISTING edge kind; provenance rides on
            # born_from_event_id, not a bespoke edge kind (§4.4). detected_by is
            # left to default to the Think edge-op producer (the document Models
            # are minted by Think under ratified Option A).
            EdgeOp(
                op="add",
                source_model_id=member_a,
                target_model_id=member_b,
                edge_kind="co_occurs_with",
            ),
        ],
    )


async def test_apply_document_diff_mints_anchor_concern_and_is_retrievable(
    fresh_db, tenant, tenant_cleanup,
):
    """Anchor + concern Models land scoped to the document's entity with doc
    provenance and an edge, and Pathway A retrieval by that entity surfaces
    them (the document is *remembered*)."""
    trigger_id = uuid7()
    async with fresh_db.acquire() as conn:
        obs_id = await _insert_observation(
            conn,
            tenant,
            content_text="Acme renewal sync brief.",
            source_channel="fireflies:transcript",
            entities_mentioned=[{"type": "customer", "id": _CUSTOMER_ID}],
        )
        from services.reasoning.think.tests.test_applier import _insert_applier_model

        member_a = await _insert_applier_model(
            conn, tenant, obs_id, "SOW delivery gates Acme renewal."
        )
        member_b = await _insert_applier_model(
            conn, tenant, obs_id, "SOC2 audit gates Acme renewal."
        )
        diff = _document_derived_diff(
            trigger_id, tenant, obs_id, member_a=member_a, member_b=member_b
        )
        async with conn.transaction():
            result = await apply_diff(
                diff, conn, trigger_kind="T1", trigger_cause_event_id=obs_id
            )

    assert len(result["claim_ops"]) == 2
    async with fresh_db.acquire() as conn:
        situation = await conn.fetchrow(
            "SELECT born_from_event_id, scope_entities FROM models "
            "WHERE tenant_id = $1 AND claim_role = 'situation' LIMIT 1",
            tenant,
        )
        concern = await conn.fetchrow(
            "SELECT born_from_event_id FROM models "
            "WHERE tenant_id = $1 AND claim_role = 'concern' LIMIT 1",
            tenant,
        )
        edge = await conn.fetchrow(
            "SELECT detected_by FROM model_edges "
            "WHERE tenant_id = $1 AND edge_kind = 'co_occurs_with' LIMIT 1",
            tenant,
        )
    assert situation is not None and situation["born_from_event_id"] == obs_id
    assert concern is not None and concern["born_from_event_id"] == obs_id
    assert edge is not None
    assert edge["detected_by"] == "think_edge_op"

    # Pathway A: a trigger scoped to the Acme customer must surface the doc
    # Models (structural scope recall).
    retrieve_trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs_id,
        seed_entity_ids=[{"type": "customer", "id": _CUSTOMER_ID}],
        seed_occurred_at=datetime.now(timezone.utc),
        precomputed_seed_vector=make_embedding("Acme renewal SOW SOC2"),
    )
    async with fresh_db.acquire() as conn:
        retrieval = await primary_retrieve(retrieve_trigger, conn)
    retrieved_roles = {
        (m.proposition or {}).get("claim_role") for m in retrieval.models
    }
    assert "situation" in retrieved_roles
    assert "concern" in retrieved_roles
