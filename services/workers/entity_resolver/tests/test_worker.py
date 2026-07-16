"""Tests for services/workers/entity_resolver/worker.py.

Conventions:
- Uses a `ScriptedProvider` (same pattern as lib/llm/tests/test_provider.py)
  to feed canned LLM outputs. Never calls a live LLM.
- Real Postgres via `resolver_db` fixture.
- Each test uses a fresh tenant_id for hermetic isolation.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import asyncpg
import pytest

from lib.architecture_registry import load_architecture_registry
from lib.llm.provider import LLMConfig, LLMProvider
from lib.evaluation.entity_grounding import (
    GroundingEvaluationScope,
    evaluate_entity_grounding_state,
)
from lib.evaluation.company_learning import (
    CompanyLearningEvaluationScope,
    build_company_learning_evidence_manifest,
    company_learning_assurance_status,
    evaluate_company_learning_state,
)
from lib.evaluation.proof import compile_invariant_proof_matrix
from lib.embeddings.ollama import EMBEDDING_DIM
from lib.shared.ids import uuid7
from services.domain.clarifications import answer_clarification_request
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.domain.entity_resolution_adjudication import (
    adjudicate_entity_resolution_clarification,
)
from services.workers.entity_resolver.context import build_context
from services.workers.entity_resolver.worker import (
    EntityResolverWorker,
    ResolverLLMBudget,
)
from services.workers.source_semantic_worker import SourceSemanticWorker


pytestmark = pytest.mark.integration


# =====================================================================
# Test doubles
# =====================================================================

class ScriptedProvider(LLMProvider):
    """Pops scripted responses (strings or exceptions) in FIFO order."""

    def __init__(self, responses: list):
        super().__init__(
            LLMConfig(provider="anthropic", api_key="k", model="m")
        )
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        self.calls.append(
            {"system": system, "user": user, "schema_hint": schema_hint}
        )
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


def _resolution_json(
    *,
    type: str | None = "commitment",
    id: str = "commitment-uuid",
    confidence: float = 0.9,
    reasoning: str = "matches payments context",
) -> str:
    ref = None if type is None else {"type": type, "id": id}
    return json.dumps({
        "canonical_ref": ref,
        "confidence": confidence,
        "reasoning": reasoning,
    })


# =====================================================================
# Fixtures
# =====================================================================

async def _seed_observation(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    content_text: str,
    unresolved_phrases: list[str],
    source_channel: str = "slack:message",
    occurred_at: datetime | None = None,
    unresolved_location: str = "metadata",
    entities_mentioned: list[dict] | None = None,
    content_extra: dict[str, object] | None = None,
) -> UUID:
    obs_id = uuid7()
    occurred_at = occurred_at or datetime.now(timezone.utc)
    content: dict[str, object] = {"text": content_text}
    content.update(content_extra or {})
    if unresolved_location == "metadata":
        content["metadata"] = {"_unresolved_phrases": unresolved_phrases}
    elif unresolved_location == "top_level":
        content["_unresolved_phrases"] = unresolved_phrases
    elif unresolved_location == "both":
        content["_unresolved_phrases"] = unresolved_phrases
        content["metadata"] = {"_unresolved_phrases": unresolved_phrases}
    else:
        raise AssertionError(f"unknown unresolved_location={unresolved_location!r}")
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                content, content_text, trust_tier, entities_mentioned
            ) VALUES (
                $1, $2, $3, 'signal', $4, $5::jsonb, $6,
                'attested_agent', $7::jsonb
            )
            """,
            obs_id,
            tenant_id,
            occurred_at,
            source_channel,
            json.dumps(content),
            content_text,
            json.dumps(entities_mentioned or []),
        )
    return obs_id


async def _fetch_obs_entities(
    pool: asyncpg.Pool, obs_id: UUID
) -> list[dict]:
    async with pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT entities_mentioned FROM observations WHERE id = $1",
            obs_id,
        )
    if val is None:
        return []
    if isinstance(val, str):
        return json.loads(val)
    return list(val)


async def _count_review_rows(
    pool: asyncpg.Pool, tenant_id: UUID
) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM clarification_requests
            WHERE tenant_id = $1 AND kind = 'entity_resolution'
            """,
            tenant_id,
        ) or 0


async def _fetch_clarification_rows(
    pool: asyncpg.Pool, tenant_id: UUID
) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT *
            FROM clarification_requests
            WHERE tenant_id = $1
            ORDER BY created_at
            """,
            tenant_id,
        )


async def _count_trigger_rows(
    pool: asyncpg.Pool, tenant_id: UUID
) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM think_trigger_queue WHERE tenant_id = $1",
            tenant_id,
        ) or 0


async def _fetch_entity_resolved_triggers(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    observation_id: UUID,
) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT id, tenant_id, trigger_kind, trigger_subkind,
                   observation_id, payload
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND observation_id = $2
              AND trigger_kind = 'T1'
              AND trigger_subkind = 'entity_resolved_late'
            ORDER BY enqueued_at, id
            """,
            tenant_id,
            observation_id,
        )


async def _count_state_change_obs(
    pool: asyncpg.Pool, tenant_id: UUID
) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            """
            SELECT COUNT(*) FROM observations
            WHERE tenant_id = $1 AND kind = 'state_change'
            """,
            tenant_id,
        ) or 0


async def _seed_candidate_alias(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    alias: str,
    entity_type: str,
    entity_id: str,
    source: str = "manual",
    independently_governed: bool = True,
) -> None:
    await EntityAliasRepo(pool).insert_alias(
        phrase=alias,
        resolved_entity_ref={"type": entity_type, "id": entity_id},
        source=source,
        confidence=0.99,
        tenant_id=tenant_id,
        extra_metadata=(
            {
                "identity_basis_class": "independently_adjudicated",
                "identity_basis_ref": f"test-adjudication:{entity_type}:{entity_id}",
            }
            if independently_governed
            else None
        ),
    )


async def _fetch_grounding_traces(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT * FROM grounding_traces
            WHERE tenant_id = $1
            ORDER BY created_at, id
            """,
            tenant_id,
        )


async def _fetch_grounding_work_items(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT * FROM entity_grounding_work_items
            WHERE tenant_id = $1
            ORDER BY created_at, id
            """,
            tenant_id,
        )


async def _fetch_mention_detections(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> list[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT * FROM entity_mention_detections
            WHERE tenant_id = $1
            ORDER BY recorded_at, id
            """,
            tenant_id,
        )


# =====================================================================
# Resolved-path tests
# =====================================================================

async def test_context_dependent_phrase_routes_to_review_without_stable_boundary(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Payments Service V2",
        entity_type="commitment",
        entity_id="payments_service_v2",
    )
    obs_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="the billing thing is down",
        unresolved_phrases=["the billing thing"],
    )
    provider = ScriptedProvider([
        _resolution_json(
            type="commitment",
            id="payments_service_v2",
            confidence=0.91,
        )
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("the billing thing", "review")]

    # Resolver choice is a sidecar admission, not identity-registry evidence.
    ref = await EntityAliasRepo(resolver_db).fast_path_resolve(
        "the billing thing", tenant_id
    )
    assert ref is None

    # The source observation is immutable.
    ents = await _fetch_obs_entities(resolver_db, obs_id)
    assert ents == []

    assert await _count_state_change_obs(resolver_db, tenant_id) == 0
    traces = await _fetch_grounding_traces(resolver_db, tenant_id)
    assert len(traces) == 1
    assert traces[0]["current_fate"] == "review"
    assert traces[0]["identity_registry_mutated"] is False
    assert traces[0]["source_observation_mutated"] is False


async def test_top_level_unresolved_phrases_from_ingestion_are_processed(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Nimbus Bank",
        entity_type="customer",
        entity_id="customer-nimbus",
    )
    obs_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI renewal is blocked on audit export proof",
        unresolved_phrases=["NBI"],
        unresolved_location="top_level",
    )
    provider = ScriptedProvider([
        _resolution_json(type="customer", id="customer-nimbus", confidence=0.94)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    decisions = await worker.process_observation(obs_id, tenant_id)

    assert decisions == [("NBI", "resolved")]
    ref = await EntityAliasRepo(resolver_db).fast_path_resolve("NBI", tenant_id)
    assert ref is None
    traces = await _fetch_grounding_traces(resolver_db, tenant_id)
    detections = await _fetch_mention_detections(resolver_db, tenant_id)
    assert len(traces) == 1
    assert len(detections) == 1
    detection = detections[0]
    assert detection["fate"] == "detected"
    assert detection["mention_id"] == detection["id"]
    assert traces[0]["entity_mention_detection_id"] == detection["id"]
    assert traces[0]["entity_mention_id"] == detection["mention_id"]
    async with resolver_db.acquire() as conn:
        request = await conn.fetchrow(
            """
            SELECT mention_ref, entity_mention_detection_id, entity_mention_id
            FROM entity_candidate_generation_requests
            WHERE tenant_id=$1 AND source_observation_id=$2
            """,
            tenant_id,
            obs_id,
        )
    assert request["mention_ref"] == f"mention:{detection['mention_id']}:v1"
    assert request["entity_mention_detection_id"] == detection["id"]
    assert request["entity_mention_id"] == detection["mention_id"]
    assert '"mention_detection"' in provider.calls[0]["user"]
    assert str(detection["mention_id"]) in provider.calls[0]["user"]

    # A grounding admission issued to the sidecar consumer must not authorize
    # a second legacy Think consumer. The source-semantic lane owns the exact
    # downstream belief/no-admission fate once an embedding is available.
    assert await _fetch_entity_resolved_triggers(
        resolver_db,
        tenant_id,
        obs_id,
    ) == []
    async with resolver_db.acquire() as conn:
        admission = await conn.fetchrow(
            """
            SELECT assessment_id, decision_version, expires_at
            FROM grounding_admission_decisions
            WHERE tenant_id = $1 AND id = $2
            """,
            tenant_id,
            traces[0]["grounding_admission_id"],
        )
        assessment_version = await conn.fetchval(
            """
            SELECT assessment_version
            FROM resolution_assessments
            WHERE tenant_id = $1 AND id = $2
            """,
            tenant_id,
            traces[0]["resolution_assessment_id"],
        )
    assert admission is not None
    assert admission["assessment_id"] == traces[0]["resolution_assessment_id"]
    assert assessment_version == 1
    assert admission["decision_version"] == 1
    assert admission["expires_at"] is not None

    # A terminal grounding trace closes this processing generation, so replay
    # must neither call the model nor enqueue a legacy downstream trigger.
    assert await worker.process_observation(obs_id, tenant_id) == []
    assert len(provider.calls) == 1
    assert len(
        await _fetch_entity_resolved_triggers(resolver_db, tenant_id, obs_id)
    ) == 0


async def test_duplicate_top_level_and_metadata_phrases_are_deduped(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Nimbus Bank",
        entity_type="customer",
        entity_id="customer-nimbus",
    )
    obs_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI renewal is blocked on audit export proof",
        unresolved_phrases=["NBI"],
        unresolved_location="both",
    )
    provider = ScriptedProvider([
        _resolution_json(type="customer", id="customer-nimbus", confidence=0.94)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    decisions = await worker.process_observation(obs_id, tenant_id)

    assert decisions == [("NBI", "resolved")]
    assert len(provider.calls) == 1


async def test_resolver_prompt_includes_source_entities_and_known_candidates(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await EntityAliasRepo(resolver_db).insert_alias(
        phrase="Nimbus Bank",
        resolved_entity_ref={"type": "customer", "id": "customer-nimbus"},
        source="manual",
        confidence=0.98,
        tenant_id=tenant_id,
    )
    obs_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="Nimbus Bank should also be known as NBI",
        unresolved_phrases=["NBI"],
        unresolved_location="top_level",
        entities_mentioned=[{"type": "customer", "id": "customer-nimbus"}],
    )
    provider = ScriptedProvider([
        _resolution_json(type="customer", id="customer-nimbus", confidence=0.94)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    await worker.process_observation(obs_id, tenant_id)

    prompt = provider.calls[0]["user"]
    assert "source_entities_mentioned" in prompt
    assert "known_entity_candidates" in prompt
    assert "customer-nimbus" in prompt


async def test_resolver_candidate_set_excludes_inactive_and_archived_targets(
    resolver_db: asyncpg.Pool,
    tenant_id: UUID,
) -> None:
    inactive_actor_id = uuid7()
    archived_resource_id = uuid7()
    active_resource_id = uuid7()
    async with resolver_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT DO NOTHING",
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO actors (
                id, tenant_id, type, display_name, status
            ) VALUES ($1, $2, 'human_internal', 'Former Gateway Owner', 'inactive')
            """,
            inactive_actor_id,
            tenant_id,
        )
        await conn.executemany(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, current_value,
                metadata, archived_at
            ) VALUES (
                $1, $2, 'infrastructure', $3,
                '{}'::jsonb, '{"semantic_kind":"system"}'::jsonb, $4
            )
            """,
            (
                (
                    archived_resource_id,
                    tenant_id,
                    "Archived Gateway",
                    datetime.now(timezone.utc),
                ),
                (active_resource_id, tenant_id, "Active Gateway", None),
            ),
        )
    aliases = (
        (
            "Former Gateway Owner",
            {"type": "actor", "id": str(inactive_actor_id)},
        ),
        (
            "Archived Gateway",
            {"type": "resource", "id": str(archived_resource_id)},
        ),
        (
            "Active Gateway",
            {"type": "resource", "id": str(active_resource_id)},
        ),
    )
    for phrase, ref in aliases:
        await EntityAliasRepo(resolver_db).insert_alias(
            phrase=phrase,
            resolved_entity_ref=ref,
            source="manual",
            confidence=0.99,
            tenant_id=tenant_id,
            extra_metadata={
                "identity_basis_class": "source_authoritative",
                "identity_basis_ref": f"test-source:{ref['id']}",
            },
        )
    observation_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="Gateway ownership is changing",
        unresolved_phrases=["Gateway"],
    )

    ctx = await build_context(
        pool=resolver_db,
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="Gateway",
    )
    refs = {
        (item.canonical_ref["type"], item.canonical_ref["id"])
        for item in EntityResolverWorker._candidate_inputs(ctx)
    }

    assert ("resource", str(active_resource_id)) in refs
    assert ("resource", str(archived_resource_id)) not in refs
    assert ("actor", str(inactive_actor_id)) not in refs


async def test_slack_prompt_never_mixes_channels_or_future_messages(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    cutoff = datetime.now(timezone.utc)
    await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="SAFE_FINANCE_CONTEXT",
        unresolved_phrases=[],
        occurred_at=cutoff - timedelta(minutes=2),
        content_extra={"channel": "C-finance", "ts": "100.1"},
    )
    await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="SECRET_OTHER_CHANNEL",
        unresolved_phrases=[],
        occurred_at=cutoff - timedelta(minutes=1),
        content_extra={"channel": "C-private", "ts": "101.1"},
    )
    await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="FUTURE_FINANCE_MESSAGE",
        unresolved_phrases=[],
        occurred_at=cutoff + timedelta(minutes=1),
        content_extra={"channel": "C-finance", "ts": "103.1"},
    )
    focal = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="what about it?",
        unresolved_phrases=["it"],
        occurred_at=cutoff,
        content_extra={"channel": "C-finance", "ts": "102.1"},
    )
    provider = ScriptedProvider([_resolution_json(type=None, confidence=0.7)])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    await worker.process_observation(focal, tenant_id)

    prompt = provider.calls[0]["user"]
    assert "SAFE_FINANCE_CONTEXT" in prompt
    assert "SECRET_OTHER_CHANNEL" not in prompt
    assert "FUTURE_FINANCE_MESSAGE" not in prompt
    assert '"source_space":"C-finance"' in prompt
    assert "scoped_models" not in prompt


async def test_explicit_phrase_prompt_excludes_unselected_same_channel_distractor(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Nimbus Bank",
        entity_type="customer",
        entity_id="customer-nimbus",
    )
    cutoff = datetime.now(timezone.utc)
    await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="DISTRACTOR_FROM_SAME_CHANNEL",
        unresolved_phrases=[],
        occurred_at=cutoff - timedelta(minutes=1),
        content_extra={"channel": "C-finance", "ts": "100.1"},
    )
    focal = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI renewal is blocked",
        unresolved_phrases=["NBI"],
        occurred_at=cutoff,
        content_extra={"channel": "C-finance", "ts": "101.1"},
    )
    provider = ScriptedProvider([
        _resolution_json(type="customer", id="customer-nimbus", confidence=0.95)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    assert await worker.process_observation(focal, tenant_id) == [
        ("NBI", "resolved")
    ]

    prompt = provider.calls[0]["user"]
    assert "DISTRACTOR_FROM_SAME_CHANNEL" not in prompt
    assert '"disposition":"operationally_sufficient"' in prompt


async def test_context_dependent_customer_does_not_enqueue_think_trigger(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Acme Corporation",
        entity_type="customer",
        entity_id="customer-acme",
    )
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="we just lost the big one",
        unresolved_phrases=["the big one"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="customer", id="customer-acme", confidence=0.95)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    await worker.process_observation(obs_id, tenant_id)
    assert await _count_trigger_rows(resolver_db, tenant_id) == 0
    assert await _count_review_rows(resolver_db, tenant_id) == 1


async def test_resolved_non_material_type_does_not_enqueue_trigger(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Company Wiki",
        entity_type="url",
        entity_id="https://wiki",
    )
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="a link to the wiki",
        unresolved_phrases=["the wiki"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="url", id="https://wiki", confidence=0.9)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    await worker.process_observation(obs_id, tenant_id)
    assert await _count_trigger_rows(resolver_db, tenant_id) == 0


# =====================================================================
# Review-queue path
# =====================================================================

async def test_ambiguous_confidence_goes_to_review_queue(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Project One",
        entity_type="goal",
        entity_id="g-1",
    )
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="the project", unresolved_phrases=["the project"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="goal", id="g-1", confidence=0.6)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("the project", "review")]

    # No alias written; one canonical clarification exists.
    assert await _count_review_rows(resolver_db, tenant_id) == 1
    clarifications = await _fetch_clarification_rows(resolver_db, tenant_id)
    assert len(clarifications) == 1
    clarification = clarifications[0]
    assert clarification["kind"] == "entity_resolution"
    assert clarification["object_kind"] == "grounding_trace"
    assert clarification["object_id"] is not None
    assert clarification["source_observation_id"] == obs_id
    assert "the project" in clarification["question"]
    options = clarification["options"]
    if isinstance(options, str):
        options = json.loads(options)
    assert {option["id"] for option in options} >= {
        "accept_candidate",
        "not_same_entity",
        "needs_new_entity",
    }


@pytest.mark.parametrize("alias_source", ["resolver_worker", "manual"])
async def test_alias_without_governed_identity_basis_cannot_auto_admit(
    resolver_db: asyncpg.Pool,
    tenant_id: UUID,
    alias_source: str,
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Legacy NBI guess",
        entity_type="customer",
        entity_id="customer-nimbus",
        source=alias_source,
        independently_governed=False,
    )
    obs_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI renewal is blocked",
        unresolved_phrases=["NBI"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="customer", id="customer-nimbus", confidence=0.99)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    decisions = await worker.process_observation(obs_id, tenant_id)

    assert decisions == [("NBI", "review")]
    assert await _count_review_rows(resolver_db, tenant_id) == 1
    traces = await _fetch_grounding_traces(resolver_db, tenant_id)
    assert traces[0]["current_fate"] == "review"
    assert traces[0]["selected_referent"] is None
    assert await _count_trigger_rows(resolver_db, tenant_id) == 0


async def test_clarification_adjudication_changes_future_grounding_fate(
    resolver_db: asyncpg.Pool,
    tenant_id: UUID,
) -> None:
    scope_start = datetime.now(timezone.utc) - timedelta(seconds=1)
    adjudicator_id = uuid7()
    customer_id = uuid7()
    async with resolver_db.acquire() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT DO NOTHING",
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, 'human_internal', 'Identity Admin', 'active')
            """,
            adjudicator_id,
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO actor_roles (
                tenant_id, actor_id, entity_type, entity_id, role
            ) VALUES ($1, $2, 'tenant', NULL, 'admin')
            """,
            tenant_id,
            adjudicator_id,
        )
        await conn.execute(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, current_value,
                metadata, archived_at
            ) VALUES (
                $1, $2, 'relational', 'Nimbus Bank',
                '{"semantic_kind":"customer"}'::jsonb,
                '{"semantic_kind":"customer"}'::jsonb, NULL
            )
            """,
            customer_id,
            tenant_id,
        )
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="NBI",
        entity_type="customer",
        entity_id=str(customer_id),
        source="manual",
        independently_governed=False,
    )
    first_observation_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI renewal is blocked",
        unresolved_phrases=["NBI"],
    )
    provider = ScriptedProvider(
        [
            _resolution_json(
                type="customer",
                id=str(customer_id),
                confidence=0.99,
            ),
        ]
    )
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    assert await worker.process_observation(first_observation_id, tenant_id) == [
        ("NBI", "review")
    ]
    assert len(provider.calls) == 1
    clarification = (await _fetch_clarification_rows(resolver_db, tenant_id))[0]
    payload = clarification["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    candidate = payload["candidates"][0]["canonical_ref"]
    answer = {
        "action": "accept_candidate",
        "canonical_ref": candidate,
        "confidence": 0.99,
        "resolution_scope": "tenant_global_exact",
        "confirm_tenant_global_reuse": True,
    }

    async with resolver_db.acquire() as conn, conn.transaction():
        answered = await answer_clarification_request(
            conn,
            tenant_id=tenant_id,
            request_id=clarification["id"],
            answer=answer,
            answered_by=adjudicator_id,
        )
        assert answered is not None
        await adjudicate_entity_resolution_clarification(
            conn,
            clarification=answered,
            answer=answer,
            tenant_id=tenant_id,
            answered_by=adjudicator_id,
        )

    async with resolver_db.acquire() as conn:
        before_future_exposure = await evaluate_entity_grounding_state(
            conn,
            scope=GroundingEvaluationScope(
                tenant_id=tenant_id,
                observation_start=scope_start,
                observation_end=datetime.now(timezone.utc) + timedelta(seconds=1),
                run_id="clarification-before-future-exposure",
            ),
            artifact_refs=("pytest://clarification-before-future-exposure",),
        )
    assert before_future_exposure.corrective_memory_observed_reuse_count == 0
    assert before_future_exposure.duplicate_trace_count == 0

    second_observation_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI is delayed",
        unresolved_phrases=["NBI"],
    )
    async with resolver_db.acquire() as conn:
        await conn.execute(
            """
            UPDATE observations
            SET embedding=$1::vector, embedding_pending=FALSE
            WHERE tenant_id=$2 AND id=ANY($3::uuid[])
            """,
            json.dumps([0.01] * EMBEDDING_DIM),
            tenant_id,
            [first_observation_id, second_observation_id],
        )
    replay_context = await build_context(
        pool=resolver_db,
        tenant_id=tenant_id,
        observation_id=second_observation_id,
        phrase="NBI",
    )
    assert len(replay_context.recent_aliases) == 1
    learned_alias = replay_context.recent_aliases[0]
    assert learned_alias.adjudication_state == "active"
    assert learned_alias.resolution_scope == "tenant_global_exact"
    assert learned_alias.autonomous_replay_eligible is True
    assert learned_alias.replay_lineage_valid is True
    assert await worker.process_observation(second_observation_id, tenant_id) == [
        ("NBI", "resolved")
    ]
    assert len(provider.calls) == 1
    semantic_worker = SourceSemanticWorker(
        pool=resolver_db,
        worker_id=f"pytest:governed-alias-replay:{tenant_id}",
    )
    assert await semantic_worker.process_batch(limit=100) == 3
    unrelated_observation_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="UNRELATED is noisy",
        unresolved_phrases=["UNRELATED"],
    )
    await EntityAliasRepo(resolver_db).insert_alias(
        phrase="UNRELATED",
        resolved_entity_ref={"type": "customer", "id": str(customer_id)},
        source="resolver_worker",
        confidence=0.51,
        tenant_id=tenant_id,
        source_event_id=unrelated_observation_id,
    )
    async with resolver_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                content, content_text, trust_tier, cause_id
            ) VALUES (
                $1, $2, now(), 'state_change', 'internal:state_change',
                $3::jsonb, 'unrelated resolver state', 'authoritative', $4
            )
            """,
            uuid7(),
            tenant_id,
            json.dumps(
                {
                    "_state_change_kind": "entity_late_resolution",
                    "source_observation_id": str(unrelated_observation_id),
                }
            ),
            unrelated_observation_id,
        )

    async with resolver_db.acquire() as conn:
        alias = await conn.fetchrow(
            """
            SELECT resolved_entity_ref, entity_metadata, confirmed_count,
                   contested_count
            FROM entity_aliases
            WHERE tenant_id = $1 AND alias_text = 'NBI'
            """,
            tenant_id,
        )
        trace = await conn.fetchrow(
            """
            SELECT trace.current_fate, trace.selected_referent, trace.trace,
                   assessment.model_output,
                   assessment.scorer_and_calibration_version
            FROM grounding_traces trace
            JOIN resolution_assessments assessment
              ON assessment.tenant_id=trace.tenant_id
             AND assessment.id=trace.resolution_assessment_id
            WHERE trace.tenant_id = $1 AND trace.source_observation_id = $2
            """,
            tenant_id,
            second_observation_id,
        )
        replay_model = await conn.fetchrow(
            """
            SELECT model.id, model.born_from_event_id,
                   work.status AS semantic_work_status
            FROM models model
            JOIN source_semantic_work_items work
              ON work.tenant_id=model.tenant_id
             AND work.admitted_model_id=model.id
            WHERE model.tenant_id=$1
              AND model.born_from_event_id=$2
            """,
            tenant_id,
            second_observation_id,
        )
        replay_feedback = await conn.fetchrow(
            """
            SELECT attempt_count, success_count, dropped_count, failure_count
            FROM think_feedback_stats
            WHERE tenant_id=$1
              AND surface='entity_grounding'
              AND op_type='autonomous_learning'
              AND op_kind='governed_exact_alias_replay'
            """,
            tenant_id,
        )
        evaluation = await evaluate_entity_grounding_state(
            conn,
            scope=GroundingEvaluationScope(
                tenant_id=tenant_id,
                observation_start=scope_start,
                observation_end=datetime.now(timezone.utc) + timedelta(seconds=1),
                run_id="clarification-corrective-memory",
            ),
            artifact_refs=("pytest://clarification-corrective-memory",),
        )
        company_learning = await evaluate_company_learning_state(
            conn,
            scope=CompanyLearningEvaluationScope(
                tenant_id=tenant_id,
                observation_ids=(
                    first_observation_id,
                    second_observation_id,
                ),
                start=scope_start,
                end=datetime.now(timezone.utc) + timedelta(seconds=1),
                run_id="governed-alias-replay-company-learning",
            ),
            artifact_refs=(
                "pytest://governed-alias-replay-company-learning",
            ),
        )
        self_authoritative_observation_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM observations
            WHERE tenant_id=$1
              AND source_channel='internal:state_change'
              AND content ->> '_state_change_kind'='entity_late_resolution'
              AND content ->> 'source_observation_id'=ANY($2::text[])
            """,
            tenant_id,
            [str(first_observation_id), str(second_observation_id)],
        )

    registry = load_architecture_registry(
        Path(__file__).resolve().parents[4] / "architecture" / "registry.yaml"
    )
    evidence_manifest = build_company_learning_evidence_manifest(
        company_learning,
        registry=registry,
        system_version="pytest-governed-alias-replay",
        experiment_manifest_ref="pytest://governed-alias-replay",
    )
    invariant_proof = compile_invariant_proof_matrix(
        registry,
        run_id=company_learning.scope.run_id,
        evidence=evidence_manifest.evidence,
    )

    assert alias is not None
    metadata = alias["entity_metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    assert metadata["identity_basis_class"] == "independently_adjudicated"
    assert metadata["identity_basis_ref"] == (
        f"clarification-request:{clarification['id']}"
    )
    assert metadata["grounding_feedback_lineage"]["grounding_trace_id"]
    assert alias["confirmed_count"] == 1
    assert alias["contested_count"] == 1
    assert trace is not None
    assert trace["current_fate"] == "resolved_for_consumer"
    selected = trace["selected_referent"]
    if isinstance(selected, str):
        selected = json.loads(selected)
    assert selected["id"] == str(customer_id)
    model_output = trace["model_output"]
    if isinstance(model_output, str):
        model_output = json.loads(model_output)
    assert model_output["decision_source"] == "governed_exact_alias_replay"
    assert model_output["llm_invoked"] is False
    assert model_output["identity_basis_ref"] == (
        f"clarification-request:{clarification['id']}"
    )
    assert trace["scorer_and_calibration_version"] == (
        "governed-exact-alias-replay-v1"
    )
    trace_payload = trace["trace"]
    if isinstance(trace_payload, str):
        trace_payload = json.loads(trace_payload)
    assert trace_payload["model_output_is_evidence"] is False
    assert await _fetch_obs_entities(resolver_db, first_observation_id) == []
    assert await _fetch_obs_entities(resolver_db, second_observation_id) == []
    assert self_authoritative_observation_count == 0
    assert replay_model is not None
    assert replay_model["born_from_event_id"] == second_observation_id
    assert replay_model["semantic_work_status"] == "belief_applied"
    assert dict(replay_feedback) == {
        "attempt_count": 1,
        "success_count": 1,
        "dropped_count": 0,
        "failure_count": 0,
    }
    assert evaluation.answered_entity_clarification_count == 1
    assert evaluation.answered_entity_clarification_lineage_coverage == 1.0
    assert evaluation.adjudicated_alias_count == 1
    assert evaluation.adjudicated_alias_lineage_coverage == 1.0
    assert evaluation.corrective_memory_observed_reuse_count == 1
    assert evaluation.duplicate_trace_count == 0
    assert evaluation.alias_replay_exposure_count == 1
    assert evaluation.alias_replay_resolution_rate == 1.0
    assert evaluation.alias_replay_llm_avoided_count == 1
    assert evaluation.unsafe_alias_replay_count == 0
    assert evaluation.contextual_alias_replay_count == 0
    assert company_learning.status == "insufficient"
    assert company_learning.observed_slice_health == "healthy"
    assert company_learning.violation_count == 0
    assert company_learning.entity_grounding.resolver_created_alias_count == 0
    assert (
        company_learning.entity_grounding.self_authoritative_observation_count
        == 0
    )
    assert evidence_manifest.run_id == company_learning.scope.run_id
    assert len(evidence_manifest.evidence) == 9
    assert (
        company_learning_assurance_status(company_learning, invariant_proof)
        == "insufficient"
    )
    assert company_learning.learning_loop[
        "governed_alias_replay_resolution_rate"
    ] == 1.0
    assert company_learning.learning_loop[
        "grounding_to_interpretation_coverage"
    ] == 1.0
    assert company_learning.learning_loop["one_model_cardinality_rate"] == 1.0

    # A contradictory source-native hint defeats deterministic alias replay.
    # The adjudicated alias remains an authorized candidate, but the resolver
    # must ask the model to assess the conflicting closed set.
    conflicting_observation_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI is delayed",
        unresolved_phrases=["NBI"],
        entities_mentioned=[{"type": "customer", "id": "customer-other"}],
    )
    conflict_provider = ScriptedProvider(
        [
            _resolution_json(
                type="customer",
                id=str(customer_id),
                confidence=0.99,
            )
        ]
    )
    conflict_worker = EntityResolverWorker(
        pool=resolver_db,
        llm=conflict_provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    assert await conflict_worker.process_observation(
        conflicting_observation_id,
        tenant_id,
    ) == [("NBI", "resolved")]
    assert len(conflict_provider.calls) == 1

    # The frozen experiment arm preserves the adjudicated alias in storage but
    # cannot see or replay it. An unrelated manual candidate remains available
    # to the model, proving that the control disables learned corrective memory
    # rather than all alias-backed candidate generation.
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Nimbus Bank",
        entity_type="customer",
        entity_id=str(customer_id),
        source="manual",
    )
    frozen_observation_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI renewal is delayed again",
        unresolved_phrases=["NBI"],
    )
    frozen_provider = ScriptedProvider(
        [
            _resolution_json(
                type="customer",
                id=str(customer_id),
                confidence=0.99,
            )
        ]
    )
    frozen_worker = EntityResolverWorker(
        pool=resolver_db,
        llm=frozen_provider,
        alias_repo=EntityAliasRepo(resolver_db),
        corrective_memory_reuse_enabled=False,
    )

    assert await frozen_worker.process_observation(
        frozen_observation_id,
        tenant_id,
    ) == [("NBI", "resolved")]
    assert len(frozen_provider.calls) == 1
    frozen_prompt = frozen_provider.calls[0]["user"]
    assert '"alias":"NBI"' not in frozen_prompt
    assert f"clarification-request:{clarification['id']}" not in frozen_prompt
    assert '"alias":"Nimbus Bank"' in frozen_prompt
    assert f"test-adjudication:customer:{customer_id}" in frozen_prompt

    async with resolver_db.acquire() as conn:
        persisted_alias = await conn.fetchrow(
            """
            SELECT resolved_entity_ref, entity_metadata
            FROM entity_aliases
            WHERE tenant_id=$1 AND alias_text='NBI'
            """,
            tenant_id,
        )
        frozen_trace = await conn.fetchrow(
            """
            SELECT assessment.model_output
            FROM grounding_traces trace
            JOIN resolution_assessments assessment
              ON assessment.tenant_id=trace.tenant_id
             AND assessment.id=trace.resolution_assessment_id
            WHERE trace.tenant_id=$1
              AND trace.source_observation_id=$2
            """,
            tenant_id,
            frozen_observation_id,
        )
        replay_attempt_count = await conn.fetchval(
            """
            SELECT attempt_count
            FROM think_feedback_stats
            WHERE tenant_id=$1
              AND surface='entity_grounding'
              AND op_type='autonomous_learning'
              AND op_kind='governed_exact_alias_replay'
            """,
            tenant_id,
        )

    assert persisted_alias is not None
    persisted_metadata = persisted_alias["entity_metadata"]
    if isinstance(persisted_metadata, str):
        persisted_metadata = json.loads(persisted_metadata)
    assert persisted_metadata["identity_basis_ref"] == (
        f"clarification-request:{clarification['id']}"
    )
    assert persisted_metadata["autonomous_replay_eligible"] is True
    assert frozen_trace is not None
    frozen_model_output = frozen_trace["model_output"]
    if isinstance(frozen_model_output, str):
        frozen_model_output = json.loads(frozen_model_output)
    assert frozen_model_output["decision_source"] == "llm"
    assert frozen_model_output["llm_invoked"] is True
    assert replay_attempt_count == 1


async def test_contextual_adjudication_does_not_enable_global_deterministic_replay(
    resolver_db: asyncpg.Pool,
    tenant_id: UUID,
) -> None:
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Project Northstar",
        entity_type="goal",
        entity_id="project-northstar",
    )
    first_observation_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="the project is blocked",
        unresolved_phrases=["the project"],
    )
    provider = ScriptedProvider(
        [
            _resolution_json(
                type="goal",
                id="project-northstar",
                confidence=0.97,
            ),
            _resolution_json(
                type="goal",
                id="project-northstar",
                confidence=0.97,
            ),
        ]
    )
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    assert await worker.process_observation(first_observation_id, tenant_id) == [
        ("the project", "review")
    ]
    assert len(provider.calls) == 1
    clarification = (await _fetch_clarification_rows(resolver_db, tenant_id))[0]
    payload = clarification["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    candidate = payload["candidates"][0]["canonical_ref"]
    answer = {
        "action": "accept_candidate",
        "canonical_ref": candidate,
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
        await adjudicate_entity_resolution_clarification(
            conn,
            clarification=answered,
            answer=answer,
            tenant_id=tenant_id,
            answered_by=None,
        )

    second_observation_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="the project is delayed",
        unresolved_phrases=["the project"],
    )
    assert await worker.process_observation(second_observation_id, tenant_id) == [
        ("the project", "review")
    ]

    # Human adjudication corrects the original occurrence. It must not turn a
    # context-dependent description into a tenant-global exact-name shortcut.
    assert len(provider.calls) == 2
    assert "the project is delayed" in provider.calls[1]["user"]
    async with resolver_db.acquire() as conn:
        later_trace = await conn.fetchrow(
            """
            SELECT current_fate, selected_referent
            FROM grounding_traces
            WHERE tenant_id=$1 AND source_observation_id=$2
              AND phrase='the project'
            """,
            tenant_id,
            second_observation_id,
        )
    assert later_trace is not None
    assert later_trace["current_fate"] == "review"


# =====================================================================
# Explicit unresolved/abstention path
# =====================================================================

async def test_low_confidence_is_preserved_with_an_explicit_fate(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="we just said hi",
        unresolved_phrases=["just said hi"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="signal", id="x", confidence=0.1)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("just said hi", "unresolved")]

    # No alias, no review, no trigger.
    ref = await EntityAliasRepo(resolver_db).fast_path_resolve(
        "just said hi", tenant_id
    )
    assert ref is None
    assert await _count_review_rows(resolver_db, tenant_id) == 0
    traces = await _fetch_grounding_traces(resolver_db, tenant_id)
    assert len(traces) == 1
    assert traces[0]["current_fate"] == "abstained"


async def test_null_canonical_ref_remains_mention_local(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="not an entity",
        unresolved_phrases=["not an entity"],
    )
    provider = ScriptedProvider([
        _resolution_json(type=None, id="", confidence=0.9)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("not an entity", "unresolved")]
    traces = await _fetch_grounding_traces(resolver_db, tenant_id)
    assert len(traces) == 1
    assert traces[0]["current_fate"] == "unresolved"


async def test_unanchored_phrase_is_rejected_without_an_llm_call(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    occurred_at = datetime.now(timezone.utc)
    obs_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="???",
        unresolved_phrases=["metadata-only phantom"],
        occurred_at=occurred_at,
    )
    provider = ScriptedProvider([])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    assert await worker.process_observation(obs_id, tenant_id) == [
        ("metadata-only phantom", "unresolved")
    ]
    assert provider.calls == []
    assert await _fetch_grounding_traces(resolver_db, tenant_id) == []
    detections = await _fetch_mention_detections(resolver_db, tenant_id)
    assert len(detections) == 1
    assert detections[0]["fate"] == "rejected_not_anchored"
    assert detections[0]["mention_id"] is None
    work = await _fetch_grounding_work_items(resolver_db, tenant_id)
    assert len(work) == 1
    assert work[0]["status"] == "unresolved"
    async with resolver_db.acquire() as conn:
        state = await evaluate_entity_grounding_state(
            conn,
            scope=GroundingEvaluationScope(
                tenant_id=tenant_id,
                observation_start=occurred_at - timedelta(seconds=1),
                observation_end=occurred_at + timedelta(seconds=1),
                run_id="rejected-unanchored-mention-component",
            ),
            artifact_refs=(
                "pytest://entity-resolver/rejected-unanchored-mention",
            ),
        )
    assert state.mention_detection_population_coverage == 1.0
    assert state.rejected_not_anchored_correctness_rate == 1.0
    assert state.rejected_candidate_request_count == 0
    assert state.incident_counts == {}


# =====================================================================
# Failure-mode paths: LLM timeout, rate-limit, malformed response.
# =====================================================================

async def test_llm_timeout_is_requeued(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="a", unresolved_phrases=["a"],
    )
    provider = ScriptedProvider([asyncio.TimeoutError()])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("a", "rate_limited")]  # requeue semantics
    assert worker.requeue_delay_s(obs_id) > 0
    work = await _fetch_grounding_work_items(resolver_db, tenant_id)
    assert len(work) == 1
    assert work[0]["status"] == "retry_scheduled"
    assert work[0]["last_failure_class"] == "provider_timeout"
    assert work[0]["next_attempt_at"] is not None


async def test_llm_rate_limit_is_requeued(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    class RateLimit(Exception):
        """Provider raises something class-named 'RateLimit...'."""

    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="a", unresolved_phrases=["a"],
    )
    provider = ScriptedProvider([
        type("RateLimitError", (Exception,), {})("slow down")
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("a", "rate_limited")]
    work = await _fetch_grounding_work_items(resolver_db, tenant_id)
    assert work[0]["last_failure_class"] == "provider_rate_limited"


async def test_llm_malformed_response_exhausts_local_retries_but_keeps_work_open(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="a", unresolved_phrases=["a"],
    )
    # 3 consecutive unparseable responses (default max_retries=2 →
    # 3 total attempts).
    provider = ScriptedProvider(["not json", "still junk", "nope"])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == [("a", "retryable")]
    assert await _fetch_grounding_traces(resolver_db, tenant_id) == []
    work = await _fetch_grounding_work_items(resolver_db, tenant_id)
    assert work[0]["status"] == "retry_scheduled"
    assert work[0]["last_failure_class"] == "provider_parse_exhausted"
    async with resolver_db.acquire() as conn:
        content = await conn.fetchval(
            "SELECT content FROM observations WHERE id = $1",
            obs_id,
        )
    assert content["metadata"]["_unresolved_phrases"] == ["a"]


# =====================================================================
# Budget / rate limiter
# =====================================================================

async def test_per_tenant_budget_skips_call_when_exhausted(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Default Commitment",
        entity_type="commitment",
        entity_id="commitment-uuid",
    )
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="a and b", unresolved_phrases=["a", "b"],
    )
    # Budget = 1 per minute — first phrase consumes, second is rate-limited.
    provider = ScriptedProvider([_resolution_json(confidence=0.95)])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
        budget=ResolverLLMBudget(per_minute=1),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    # First phrase resolved, second skipped without an LLM call.
    assert decisions[0][1] == "resolved"
    assert decisions[1][1] == "rate_limited"
    assert len(provider.calls) == 1
    work = await _fetch_grounding_work_items(resolver_db, tenant_id)
    by_phrase = {row["phrase"]: row for row in work}
    assert by_phrase["a"]["status"] == "resolved_for_consumer"
    assert by_phrase["b"]["status"] == "retry_scheduled"
    assert by_phrase["b"]["last_failure_class"] == "local_budget_exhausted"


# =====================================================================
# No unresolved phrases → no LLM calls
# =====================================================================

async def test_no_unresolved_phrases_is_noop(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="nothing", unresolved_phrases=[],
    )
    provider = ScriptedProvider([])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    decisions = await worker.process_observation(obs_id, tenant_id)
    assert decisions == []
    assert len(provider.calls) == 0


async def test_process_pending_finds_top_level_unresolved_phrases(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI renewal is blocked on audit export proof",
        unresolved_phrases=["NBI"],
        unresolved_location="top_level",
    )
    provider = ScriptedProvider([
        _resolution_json(type="customer", id="customer-nimbus", confidence=0.94)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    processed = await worker.process_pending(limit=1)

    assert processed == 1
    assert len(provider.calls) == 1


async def test_process_pending_preserves_source_and_uses_trace_as_terminal_fate(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Nimbus Bank",
        entity_type="customer",
        entity_id="customer-nimbus",
    )
    obs_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI renewal is blocked on audit export proof",
        unresolved_phrases=["NBI"],
        unresolved_location="top_level",
    )
    provider = ScriptedProvider([
        _resolution_json(type="customer", id="customer-nimbus", confidence=0.94)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )

    assert await worker.process_pending(limit=1) == 1
    assert await worker.process_observation(obs_id, tenant_id) == []
    assert len(provider.calls) == 1

    async with resolver_db.acquire() as conn:
        content = await conn.fetchval(
            "SELECT content FROM observations WHERE id = $1",
            obs_id,
        )
    assert content["_unresolved_phrases"] == ["NBI"]
    work = await _fetch_grounding_work_items(resolver_db, tenant_id)
    assert work[0]["status"] == "resolved_for_consumer"
    traces = await _fetch_grounding_traces(resolver_db, tenant_id)
    assert len(traces) == 1
    assert traces[0]["source_observation_mutated"] is False


async def test_process_pending_keeps_rate_limited_unresolved_phrases(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    obs_id = await _seed_observation(
        resolver_db,
        tenant_id,
        content_text="NBI renewal is blocked on audit export proof",
        unresolved_phrases=["NBI"],
        unresolved_location="top_level",
    )
    provider = ScriptedProvider([])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
        budget=ResolverLLMBudget(per_minute=0),
    )

    assert await worker.process_pending(limit=1) == 1
    assert len(provider.calls) == 0

    async with resolver_db.acquire() as conn:
        content = await conn.fetchval(
            "SELECT content FROM observations WHERE id = $1",
            obs_id,
        )
    assert content["_unresolved_phrases"] == ["NBI"]
    work = await _fetch_grounding_work_items(resolver_db, tenant_id)
    assert work[0]["status"] == "retry_scheduled"


# =====================================================================
# Idempotency: one terminal trace; no identity or source mutation
# =====================================================================

async def test_terminal_grounding_trace_is_idempotent_on_rerun(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Commitment One",
        entity_type="commitment",
        entity_id="c1",
    )
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="ship it now",
        unresolved_phrases=["ship it"],
    )
    responses = [_resolution_json(type="commitment", id="c1", confidence=0.9)]
    provider = ScriptedProvider(responses)
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    assert await worker.process_observation(obs_id, tenant_id) == [
        ("ship it", "review")
    ]
    assert await worker.process_observation(obs_id, tenant_id) == []

    # Resolver confidence never creates a new alias.
    async with resolver_db.acquire() as conn:
        n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM entity_aliases
            WHERE tenant_id = $1 AND alias_text = 'ship it'
            """,
            tenant_id,
        )
    assert n == 0

    assert await _fetch_obs_entities(resolver_db, obs_id) == []
    assert len(await _fetch_grounding_traces(resolver_db, tenant_id)) == 1
    assert len(provider.calls) == 1


# =====================================================================
# End-to-end fixture: 50 mixed events → correct count & no dupes
# =====================================================================

async def test_end_to_end_50_mixed_events(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    """Replay a 50-observation fixture spanning Slack/GitHub/Linear.

    10 of those have an unresolved phrase. The resolver should produce ten
    complete grounding traces without fabricating aliases, rewriting source
    observations, or creating self-authoritative state-change evidence.
    """
    base = datetime.now(timezone.utc)
    channels = ["slack:message", "github:webhook", "linear:webhook"]
    obs_ids: list[UUID] = []
    for i in range(50):
        channel = channels[i % 3]
        phrases = [f"phrase_{i}"] if i % 5 == 0 else []
        obs = await _seed_observation(
            resolver_db, tenant_id,
            content_text=(
                f"event {i} {phrases[0]}" if phrases else f"event {i}"
            ),
            unresolved_phrases=phrases,
            source_channel=channel,
            occurred_at=base.replace(microsecond=i * 1000),
        )
        obs_ids.append(obs)

    # Script a resolve for every unresolved phrase.
    n_phrases = sum(1 for i in range(50) if i % 5 == 0)
    for i in range(n_phrases):
        await _seed_candidate_alias(
            resolver_db,
            tenant_id,
            alias=f"Known commitment {i}",
            entity_type="commitment",
            entity_id=f"c{i}",
        )
    provider = ScriptedProvider([
        _resolution_json(
            type="commitment", id=f"c{i}", confidence=0.9
        )
        for i in range(n_phrases)
    ])
    worker = EntityResolverWorker(
        pool=resolver_db,
        llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
        # Relax budget for the e2e test.
        budget=ResolverLLMBudget(per_minute=1000),
    )

    decisions: list[tuple[str, str]] = []
    for obs_id in obs_ids:
        decisions.extend(await worker.process_observation(obs_id, tenant_id))

    resolved = [d for d in decisions if d[1] == "resolved"]
    assert len(resolved) == n_phrases
    # Only the independently seeded candidates exist; phrases were not added.
    async with resolver_db.acquire() as conn:
        alias_rows = await conn.fetch(
            """
            SELECT alias_text, COUNT(*) c
            FROM entity_aliases
            WHERE tenant_id = $1
            GROUP BY alias_text
            """,
            tenant_id,
        )
    assert all(r["c"] == 1 for r in alias_rows)
    assert len(alias_rows) == n_phrases

    assert await _count_state_change_obs(resolver_db, tenant_id) == 0
    traces = await _fetch_grounding_traces(resolver_db, tenant_id)
    assert len(traces) == n_phrases
    assert all(row["identity_registry_mutated"] is False for row in traces)
    assert all(row["source_observation_mutated"] is False for row in traces)

    # The objective evaluator reads the complete source opportunity population,
    # not only successful traces, and reports this scoped component replay.
    async with resolver_db.acquire() as conn:
        state = await evaluate_entity_grounding_state(
            conn,
            scope=GroundingEvaluationScope(
                tenant_id=tenant_id,
                observation_start=base - timedelta(seconds=1),
                observation_end=base + timedelta(seconds=1),
                run_id="entity-grounding-50-event-component-replay",
            ),
            artifact_refs=(
                "pytest://services/workers/entity_resolver/tests/test_worker.py::test_end_to_end_50_mixed_events",
            ),
        )
    assert state.eligible_opportunities == n_phrases
    assert state.work_population_coverage == 1.0
    assert state.terminal_trace_coverage == 1.0
    assert state.stage_continuity_rate == 1.0
    assert state.candidate_request_fate_coverage == 1.0
    assert state.mention_detection_population_coverage == 1.0
    assert state.detected_mention_count == n_phrases
    assert state.explicit_anchor_reconstructability_rate == 1.0
    assert state.mention_context_continuity_rate == 1.0
    assert state.mention_command_result_coverage == 1.0
    assert state.mention_event_coverage == 1.0
    assert state.mention_outbox_coverage == 1.0
    assert state.detected_mention_to_candidate_continuity_rate == 1.0
    assert state.incident_counts == {}


# =====================================================================
# Review-queue visibility (tenant isolation)
# =====================================================================

async def test_review_queue_tenant_isolated(
    resolver_db: asyncpg.Pool, tenant_id: UUID
):
    other_tenant = uuid7()
    await _seed_candidate_alias(
        resolver_db,
        tenant_id,
        alias="Commitment One",
        entity_type="commitment",
        entity_id="c1",
    )
    await _seed_candidate_alias(
        resolver_db,
        other_tenant,
        alias="Commitment Two",
        entity_type="commitment",
        entity_id="c2",
    )
    obs_id = await _seed_observation(
        resolver_db, tenant_id,
        content_text="ambi", unresolved_phrases=["ambi"],
    )
    other_obs = await _seed_observation(
        resolver_db, other_tenant,
        content_text="ambi2", unresolved_phrases=["ambi2"],
    )
    provider = ScriptedProvider([
        _resolution_json(type="commitment", id="c1", confidence=0.6),
        _resolution_json(type="commitment", id="c2", confidence=0.6),
    ])
    worker = EntityResolverWorker(
        pool=resolver_db, llm=provider,
        alias_repo=EntityAliasRepo(resolver_db),
    )
    await worker.process_observation(obs_id, tenant_id)
    await worker.process_observation(other_obs, other_tenant)
    # Each tenant has its own row; neither sees the other.
    assert await _count_review_rows(resolver_db, tenant_id) == 1
    assert await _count_review_rows(resolver_db, other_tenant) == 1


# =====================================================================
# Budget instance method — direct unit test (no DB)
# =====================================================================

def test_budget_refills_tokens_over_time(monkeypatch):
    budget = ResolverLLMBudget(per_minute=60)  # 1 per sec
    t = UUID("11111111-1111-1111-1111-111111111111")
    # Monkey-patch monotonic clock.
    fake_now = [0.0]

    def _mono():
        return fake_now[0]

    import time

    monkeypatch.setattr(time, "monotonic", _mono)

    # Consume 60 tokens immediately.
    for _ in range(60):
        assert budget.check_and_consume(t)
    assert not budget.check_and_consume(t)
    # Advance 1 second → 1 token refilled.
    fake_now[0] = 1.0
    assert budget.check_and_consume(t)
