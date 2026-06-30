from __future__ import annotations

import importlib.util
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.platform.execution.inquiry import (
    InquiryConfig,
    InquiryResult,
    run_inquiry_retrieval,
)
from services.domain.models.repo import pgvector_pool_init
from services.reasoning.retrieval.assembler import AccessContext, assemble_context
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.prompt import build_prompt


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_REPORT: list[dict[str, Any]] = []
_REPORT_PATH = Path(__file__).with_name("_last_run.json")
_FIXTURES_PATH = Path(__file__).resolve().parents[1] / "synthesis_harness" / "_fixtures.py"
_MAX_DEEP_RETRIEVAL_ACTIONS = 9
_FIXTURES_SPEC = importlib.util.spec_from_file_location(
    "fyralis_synthesis_harness_fixtures",
    _FIXTURES_PATH,
)
if _FIXTURES_SPEC is None or _FIXTURES_SPEC.loader is None:
    raise ImportError(f"cannot load synthesis harness fixtures from {_FIXTURES_PATH}")
F = importlib.util.module_from_spec(_FIXTURES_SPEC)
_FIXTURES_SPEC.loader.exec_module(F)


@dataclass(frozen=True)
class _RouteDecision:
    route: str
    score: float
    reason: str


class _QuestionAwareEmbedder:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def embed(self, text: str) -> list[float]:
        self.queries.append(text)
        lower = text.casefold()
        if "owner responsible" in lower:
            return F.deterministic_vector("owner-question")
        if "counterevidence" in lower:
            return F.deterministic_vector("counter-question")
        if "recurring pattern" in lower:
            return F.deterministic_vector("recurrence-question")
        return F.deterministic_vector("generic-question")


def teardown_module(_module: object) -> None:
    if os.environ.get("RETRIEVAL_E2E_REPORT", "1").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return
    passed = sum(1 for case in _REPORT if case.get("passed"))
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "passed": passed,
            "total": len(_REPORT),
        },
        "cases": _REPORT,
    }
    _REPORT_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


async def _make_resource(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    identity: str,
) -> UUID:
    resource_id = uuid7()
    await conn.execute(
        """
        INSERT INTO resources (
          id, tenant_id, kind, identity, description, current_value,
          utilization_state, controllability, temporal_character
        ) VALUES (
          $1, $2, 'relational', $3, $4, $5::jsonb,
          'available', 'joint', 'renewable'
        )
        """,
        resource_id,
        tenant_id,
        identity,
        f"Customer {identity}",
        json.dumps({"arr_usd": 850000}),
    )
    return resource_id


async def _link_customer_commitment(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    customer_resource_id: UUID,
    commitment_id: UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO customer_commitments (
          tenant_id, customer_resource_id, commitment_id, served_description
        ) VALUES ($1, $2, $3, 'retrieval-e2e')
        ON CONFLICT DO NOTHING
        """,
        tenant_id,
        customer_resource_id,
        commitment_id,
    )


async def _make_scoped_model(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    natural: str,
    proposition: dict[str, Any] | None = None,
    scope_actors: list[UUID] | None = None,
    scope_entities: list[dict[str, Any]] | None = None,
    activation: float = 0.8,
    embed_seed: str | None = None,
    supporting_event_ids: list[UUID] | None = None,
) -> UUID:
    scope_actors = scope_actors or []
    scope_entities = scope_entities or []
    model_id = await F.make_model(
        conn,
        tenant_id,
        natural=natural,
        proposition=proposition,
        scope_actors=scope_actors,
        scope_entities=scope_entities,
        activation=activation,
        embed_seed=embed_seed or natural,
        supporting_event_ids=supporting_event_ids,
    )
    if scope_actors:
        await conn.executemany(
            """
            INSERT INTO model_scope_actors (model_id, tenant_id, actor_id)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
            """,
            [(model_id, tenant_id, actor_id) for actor_id in scope_actors],
        )
    entity_rows: list[tuple[UUID, UUID, str, UUID]] = []
    for entity in scope_entities:
        try:
            entity_rows.append((
                model_id,
                tenant_id,
                str(entity["type"]),
                UUID(str(entity["id"])),
            ))
        except (KeyError, TypeError, ValueError):
            continue
    if entity_rows:
        await conn.executemany(
            """
            INSERT INTO model_scope_entities (
              model_id, tenant_id, entity_type, entity_id
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
            """,
            entity_rows,
        )
    return model_id


async def _make_model_edge(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    source_model_id: UUID,
    target_model_id: UUID,
    edge_kind: str,
    confidence: float = 0.86,
) -> None:
    await conn.execute(
        """
        INSERT INTO model_edges (
          id, tenant_id, source_model_id, target_model_id, edge_kind,
          weight, metadata, status, detected_by, confidence,
          review_status, explanation, last_confirmed_at, confirmed_count
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, '{}'::jsonb, 'active', 'system', $7,
          'accepted', 'retrieval e2e fixture', now(), 1
        )
        ON CONFLICT ON CONSTRAINT model_edges_unique DO UPDATE
          SET confidence = GREATEST(model_edges.confidence, EXCLUDED.confidence),
              review_status = 'accepted',
              status = 'active'
        """,
        uuid7(),
        tenant_id,
        source_model_id,
        target_model_id,
        edge_kind,
        confidence,
        confidence,
    )


def _trigger(
    *,
    tenant_id: UUID,
    observation_id: UUID,
    text: str,
    occurred_at: datetime,
    actor_id: UUID | None = None,
    entities: list[dict[str, Any]] | None = None,
    seed_signature: dict[str, Any] | None = None,
    seed_vector: str | None = None,
) -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=observation_id,
        seed_natural_text=text,
        seed_occurred_at=occurred_at,
        seed_entity_ids=entities or [],
        scope_actors=[actor_id] if actor_id else [],
        seed_signature=seed_signature,
        precomputed_seed_vector=F.deterministic_vector(seed_vector or text),
    )


def _model_section(prompt_user: str) -> str:
    start = prompt_user.index("  <models>")
    end = prompt_user.index("  </models>") + len("  </models>")
    return prompt_user[start:end]


def _metrics(
    *,
    case: str,
    result: InquiryResult,
    prompt_user: str,
    elapsed_ms: int,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    packet = result.context_packet
    model_section = _model_section(prompt_user)
    pathway_runs = list(result.retrieval_result.notes.get("pathways_run", []))
    metrics = {
        "case": case,
        "elapsed_ms": elapsed_ms,
        "route": result.route,
        "sufficiency": result.sufficiency.status,
        "question_ids": [q.question_id for q in result.questions],
        "question_rounds": {q.question_id: q.round_index for q in result.questions},
        "question_primitives": [q.primitive for q in result.questions],
        "retrieval_action_count": len(result.retrieval_actions),
        "semantic_action_count": sum(1 for a in result.retrieval_actions if a.path == "semantic"),
        "pathways_run": pathway_runs,
        "evidence_count": len(result.evidence_cards),
        "reservoir_evidence_count": packet.get("budget", {}).get("reservoir_evidence_count"),
        "packet_estimated_tokens": packet.get("budget", {}).get("estimated_tokens_used"),
        "prompt_chars": len(prompt_user),
        "model_section_chars": len(model_section),
        "model_detail_full_rows": model_section.count("detail=full"),
        "model_detail_manifest_rows": model_section.count("detail=manifest"),
    }
    if extra:
        metrics.update(extra)
    return metrics


def _record(case: str, passed: bool, metrics: dict[str, Any], issues: list[str] | None = None) -> None:
    _REPORT.append(
        {
            "case": case,
            "passed": passed,
            "metrics": metrics,
            "issues": issues or [],
        }
    )


async def _seed_blocker_case(conn: asyncpg.Connection) -> dict[str, Any]:
    tenant = await F.make_tenant(conn)
    owner = await F.make_actor(conn, tenant, display_name="Alice Owner")
    other_tenant = await F.make_tenant(conn)
    other_actor = await F.make_actor(conn, other_tenant, display_name="Mallory Other")
    observed_at = F.isoplus(0)
    customer = await _make_resource(conn, tenant, identity="Acme Corp")
    signal_text = "Acme launch is blocked by missing SSO; Sales promised go-live this month."
    signal_obs = await F.make_observation(
        conn,
        tenant,
        content_text=signal_text,
        actor_id=owner,
        occurred_at=observed_at,
        trust_tier="authoritative",
        embed_seed="acme-sso-blocker",
    )
    goal = await F.make_goal(
        conn,
        tenant,
        title="Launch Acme Enterprise",
        cached_health="at_risk",
        created_by_event_id=signal_obs,
    )
    commitment = await F.make_commitment(
        conn,
        tenant,
        title="Ship Acme SSO launch",
        state="active",
        owner_id=owner,
        due_date=F.isoplus(7 * 86400),
        external_counterparty_ref={"type": "customer_resource", "id": str(customer)},
        created_by_event_id=signal_obs,
    )
    await F.add_contributes_to(
        conn,
        commitment_id=commitment,
        goal_id=goal,
        is_critical_path=True,
    )
    await _link_customer_commitment(
        conn,
        tenant_id=tenant,
        customer_resource_id=customer,
        commitment_id=commitment,
    )
    risk_model = await _make_scoped_model(
        conn,
        tenant,
        natural="Acme enterprise launch is blocked by missing SSO readiness.",
        proposition={"kind": "concern", "about": "Acme launch", "nature": "SSO blocker", "raised_by": str(owner)},
        scope_actors=[owner],
        scope_entities=[
            {"type": "commitment", "id": str(commitment)},
            {"type": "goal", "id": str(goal)},
            {"type": "customer_resource", "id": str(customer)},
        ],
        activation=0.95,
        embed_seed="acme-sso-blocker",
        supporting_event_ids=[signal_obs],
    )
    commitment_model = await _make_scoped_model(
        conn,
        tenant,
        natural="Sales promised Acme go-live this month through the SSO launch commitment.",
        proposition={"kind": "state", "subject": "Acme SSO launch", "assertion": "is committed this month"},
        scope_actors=[owner],
        scope_entities=[
            {"type": "commitment", "id": str(commitment)},
            {"type": "customer_resource", "id": str(customer)},
        ],
        activation=0.84,
        embed_seed="acme-go-live-promise",
    )
    unrelated_model = await _make_scoped_model(
        conn,
        tenant,
        natural="Marketing budget approval is unrelated to customer support staffing.",
        proposition={"kind": "state", "subject": "marketing budget", "assertion": "is approved"},
        scope_actors=[owner],
        scope_entities=[],
        activation=0.73,
        embed_seed="marketing-budget-unrelated",
    )
    distractor = await _make_scoped_model(
        conn,
        other_tenant,
        natural="Acme launch is blocked by missing SSO in the wrong tenant.",
        proposition={"kind": "concern", "about": "wrong tenant", "nature": "distractor", "raised_by": str(other_actor)},
        scope_actors=[other_actor],
        activation=0.99,
        embed_seed="acme-sso-blocker",
    )
    return {
        "tenant": tenant,
        "owner": owner,
        "observed_at": observed_at,
        "signal_obs": signal_obs,
        "signal_text": signal_text,
        "customer": customer,
        "goal": goal,
        "commitment": commitment,
        "risk_model": risk_model,
        "commitment_model": commitment_model,
        "unrelated_model": unrelated_model,
        "other_tenant_distractor": distractor,
    }


async def _seed_recurrence_case(conn: asyncpg.Connection) -> dict[str, Any]:
    tenant = await F.make_tenant(conn)
    owner = await F.make_actor(conn, tenant, display_name="Rina Reliability")
    observed_at = F.isoplus(0)
    signal_text = "Again the export job has the same issue: Thursday latency spike."
    signal_obs = await F.make_observation(
        conn,
        tenant,
        content_text=signal_text,
        actor_id=owner,
        occurred_at=observed_at,
        trust_tier="authoritative",
        embed_seed="export-recurring-latency",
    )
    commitment = await F.make_commitment(
        conn,
        tenant,
        title="Stabilize weekly export job",
        state="active",
        owner_id=owner,
        created_by_event_id=signal_obs,
    )
    pattern = await _make_scoped_model(
        conn,
        tenant,
        natural="Export latency spikes recur on Thursday batch windows.",
        proposition={
            "kind": "pattern",
            "signature": {"kind": "export_latency", "window": "thursday_batch"},
            "observed_tendency": "latency spikes repeat during Thursday export batches",
            "trigger_conditions": ["export job", "Thursday"],
        },
        scope_actors=[owner],
        scope_entities=[{"type": "commitment", "id": str(commitment)}],
        activation=0.93,
        embed_seed="export-pattern",
        supporting_event_ids=[signal_obs],
    )
    instance = await _make_scoped_model(
        conn,
        tenant,
        natural="This week's export failure is another instance of the Thursday latency pattern.",
        proposition={
            "kind": "pattern_instance",
            "pattern_id": str(pattern),
            "matched_context": {"job": "export", "window": "thursday_batch"},
        },
        scope_actors=[owner],
        scope_entities=[{"type": "commitment", "id": str(commitment)}],
        activation=0.89,
        embed_seed="export-recurring-latency",
        supporting_event_ids=[signal_obs],
    )
    await _make_model_edge(
        conn,
        tenant,
        source_model_id=instance,
        target_model_id=pattern,
        edge_kind="instance_of",
    )
    return {
        "tenant": tenant,
        "owner": owner,
        "observed_at": observed_at,
        "signal_obs": signal_obs,
        "signal_text": signal_text,
        "commitment": commitment,
        "pattern": pattern,
        "instance": instance,
    }


async def _seed_ambiguous_owner_case(conn: asyncpg.Connection) -> dict[str, Any]:
    tenant = await F.make_tenant(conn)
    actor = await F.make_actor(conn, tenant, display_name="Morgan PM")
    observed_at = F.isoplus(0)
    customer = await _make_resource(conn, tenant, identity="Acme Corp")
    signal_text = "Acme renewal is blocked by the API migration; ownership is missing from the plan."
    signal_obs = await F.make_observation(
        conn,
        tenant,
        content_text=signal_text,
        actor_id=actor,
        occurred_at=observed_at,
        trust_tier="authoritative",
        embed_seed="acme-owner-ambiguous",
    )
    goal = await F.make_goal(
        conn,
        tenant,
        title="Renew Acme Enterprise",
        cached_health="at_risk",
        created_by_event_id=signal_obs,
    )
    commitment = await F.make_commitment(
        conn,
        tenant,
        title="Complete Acme API migration",
        state="active",
        owner_id=None,
        external_counterparty_ref={"type": "customer_resource", "id": str(customer)},
        created_by_event_id=signal_obs,
    )
    await F.add_contributes_to(
        conn,
        commitment_id=commitment,
        goal_id=goal,
        is_critical_path=True,
    )
    await _link_customer_commitment(
        conn,
        tenant_id=tenant,
        customer_resource_id=customer,
        commitment_id=commitment,
    )
    risk_model = await _make_scoped_model(
        conn,
        tenant,
        natural="Acme API migration is blocked and has no recorded accountable owner.",
        proposition={"kind": "concern", "about": "Acme API migration", "nature": "owner missing"},
        scope_actors=[actor],
        scope_entities=[
            {"type": "commitment", "id": str(commitment)},
            {"type": "goal", "id": str(goal)},
            {"type": "customer_resource", "id": str(customer)},
        ],
        activation=0.91,
        embed_seed="acme-owner-ambiguous",
        supporting_event_ids=[signal_obs],
    )
    return {
        "tenant": tenant,
        "actor": actor,
        "observed_at": observed_at,
        "signal_obs": signal_obs,
        "signal_text": signal_text,
        "customer": customer,
        "goal": goal,
        "commitment": commitment,
        "risk_model": risk_model,
    }


async def _seed_stale_counterevidence_case(conn: asyncpg.Connection) -> dict[str, Any]:
    tenant = await F.make_tenant(conn)
    owner = await F.make_actor(conn, tenant, display_name="Casey Owner")
    observed_at = F.isoplus(0)
    old_at = observed_at - timedelta(days=120)
    customer = await _make_resource(conn, tenant, identity="Acme Corp")
    signal_text = "Acme SSO launch is blocked today by the missing IdP certificate."
    signal_obs = await F.make_observation(
        conn,
        tenant,
        content_text=signal_text,
        actor_id=owner,
        occurred_at=observed_at,
        trust_tier="authoritative",
        embed_seed="acme-stale-counter",
    )
    goal = await F.make_goal(
        conn,
        tenant,
        title="Launch Acme SSO",
        cached_health="at_risk",
        created_by_event_id=signal_obs,
    )
    commitment = await F.make_commitment(
        conn,
        tenant,
        title="Ship Acme SSO",
        state="active",
        owner_id=owner,
        external_counterparty_ref={"type": "customer_resource", "id": str(customer)},
        created_by_event_id=signal_obs,
    )
    await F.add_contributes_to(
        conn,
        commitment_id=commitment,
        goal_id=goal,
        is_critical_path=True,
    )
    risk_model = await _make_scoped_model(
        conn,
        tenant,
        natural="Acme SSO launch is blocked today by the missing IdP certificate.",
        proposition={"kind": "concern", "about": "Acme SSO", "nature": "current certificate blocker"},
        scope_actors=[owner],
        scope_entities=[
            {"type": "commitment", "id": str(commitment)},
            {"type": "goal", "id": str(goal)},
            {"type": "customer_resource", "id": str(customer)},
        ],
        activation=0.94,
        embed_seed="acme-stale-counter",
        supporting_event_ids=[signal_obs],
    )
    stale_counter = await _make_scoped_model(
        conn,
        tenant,
        natural="Acme SSO launch was unblocked and launched after a certificate fix last quarter.",
        proposition={"kind": "state", "subject": "Acme SSO", "assertion": "was unblocked last quarter"},
        scope_actors=[owner],
        scope_entities=[
            {"type": "commitment", "id": str(commitment)},
            {"type": "customer_resource", "id": str(customer)},
        ],
        activation=0.99,
        embed_seed="acme-stale-counter",
    )
    await conn.execute(
        "UPDATE models SET created_at = $1, last_retrieved_at = NULL WHERE id = $2",
        old_at,
        stale_counter,
    )
    return {
        "tenant": tenant,
        "owner": owner,
        "observed_at": observed_at,
        "signal_obs": signal_obs,
        "signal_text": signal_text,
        "customer": customer,
        "goal": goal,
        "commitment": commitment,
        "risk_model": risk_model,
        "stale_counter": stale_counter,
    }


async def test_deep_blocker_retrieval_finds_decisive_context_efficiently(fresh_db):
    case = "deep_blocker_context"
    async with fresh_db.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            ctx = await _seed_blocker_case(conn)
            route = _RouteDecision(
                route="DEEP_INQUIRY_PATH",
                score=1.0,
                reason="routing gate retired; e2e fixture exercises deep inquiry",
            )
            trigger = _trigger(
                tenant_id=ctx["tenant"],
                observation_id=ctx["signal_obs"],
                text=ctx["signal_text"],
                occurred_at=ctx["observed_at"],
                actor_id=ctx["owner"],
                entities=[
                    {"type": "commitment", "id": str(ctx["commitment"])},
                    {"type": "goal", "id": str(ctx["goal"])},
                    {"type": "customer_resource", "id": str(ctx["customer"])},
                ],
                seed_vector="acme-sso-blocker",
            )
            t0 = time.monotonic()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                route=route.route,
                config=InquiryConfig(
                    max_rounds=1,
                    questions_per_round=3,
                    evidence_reservoir_limit=120,
                    reasoning_packet_token_budget=6000,
                    persist=True,
                ),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            bundle = await assemble_context(
                result.retrieval_result,
                AccessContext(tenant_id=ctx["tenant"], requestor_actor_id=ctx["owner"]),
                conn,
            )
            prompt = build_prompt(trigger, bundle, triggering_content=ctx["signal_text"])
            persisted = await conn.fetchrow(
                """
                SELECT question_count, evidence_count, stop_status
                FROM inquiry_sessions
                WHERE id = $1
                """,
                result.session_id,
            )

    model_section = _model_section(prompt.user)
    evidence_refs = {card.source_ref for card in result.evidence_cards}
    metrics = _metrics(
        case=case,
        result=result,
        prompt_user=prompt.user,
        elapsed_ms=elapsed_ms,
        extra={
            "routing_score": route.score,
            "routing_reason": route.reason,
            "persisted_question_count": persisted["question_count"] if persisted else None,
            "persisted_evidence_count": persisted["evidence_count"] if persisted else None,
        },
    )
    issues = []
    if "Q_COUNTEREVIDENCE" not in metrics["question_ids"]:
        issues.append("counterevidence question was not asked in the first bounded deep round")
    if result.sufficiency.status != "sufficient_for_reasoning":
        issues.append(f"expected sufficient_for_reasoning, got {result.sufficiency.status}")
    if f"model:{ctx['risk_model']}" not in evidence_refs:
        issues.append("risk model did not survive into evidence reservoir")
    if f"commitment:{ctx['commitment']}" not in evidence_refs:
        issues.append("active commitment did not survive into evidence reservoir")
    if str(ctx["other_tenant_distractor"]) in prompt.user:
        issues.append("cross-tenant distractor leaked into prompt")
    if metrics["retrieval_action_count"] > _MAX_DEEP_RETRIEVAL_ACTIONS:
        issues.append(f"too many retrieval actions: {metrics['retrieval_action_count']}")
    if metrics["model_detail_full_rows"] > 8:
        issues.append(f"too many full model rows: {metrics['model_detail_full_rows']}")
    if "manifest_mode: compact" not in model_section:
        issues.append("prompt did not use compact model manifest")
    passed = not issues
    _record(case, passed, metrics, issues)

    assert route.route == "DEEP_INQUIRY_PATH"
    assert persisted is not None
    assert passed, issues


async def test_fast_query_path_stays_bounded_and_lean(fresh_db):
    case = "fast_query_bounded"
    async with fresh_db.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            ctx = await _seed_blocker_case(conn)
            trigger = _trigger(
                tenant_id=ctx["tenant"],
                observation_id=ctx["signal_obs"],
                text="What is happening with the Acme launch?",
                occurred_at=ctx["observed_at"],
                actor_id=ctx["owner"],
                entities=[{"type": "commitment", "id": str(ctx["commitment"])}],
                seed_vector="acme-sso-blocker",
            )
            t0 = time.monotonic()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                route="FAST_PATH",
                mode="fast",
                config=InquiryConfig(
                    fast_path_evidence_limit=12,
                    reasoning_packet_token_budget=3000,
                    persist=False,
                ),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            bundle = await assemble_context(
                result.retrieval_result,
                AccessContext(tenant_id=ctx["tenant"], requestor_actor_id=ctx["owner"]),
                conn,
            )
            prompt = build_prompt(trigger, bundle)

    metrics = _metrics(case=case, result=result, prompt_user=prompt.user, elapsed_ms=elapsed_ms)
    issues = []
    if result.questions:
        issues.append("fast path asked deep inquiry questions")
    if result.retrieval_actions:
        issues.append("fast path compiled retrieval actions")
    if len(result.evidence_cards) > 12:
        issues.append(f"fast path exceeded evidence cap: {len(result.evidence_cards)}")
    if metrics["model_section_chars"] > 2400:
        issues.append(f"model section too large: {metrics['model_section_chars']}")
    if "manifest_mode: compact" not in _model_section(prompt.user):
        issues.append("fast prompt did not use compact manifest")
    passed = not issues
    _record(case, passed, metrics, issues)

    assert result.route == "FAST_PATH"
    assert passed, issues


async def test_recurrence_signal_asks_pattern_question_and_uses_graph_paths(fresh_db):
    case = "recurrence_question_quality"
    async with fresh_db.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            ctx = await _seed_recurrence_case(conn)
            trigger = _trigger(
                tenant_id=ctx["tenant"],
                observation_id=ctx["signal_obs"],
                text=ctx["signal_text"],
                occurred_at=ctx["observed_at"],
                actor_id=ctx["owner"],
                entities=[{"type": "commitment", "id": str(ctx["commitment"])}],
                seed_signature={"kind": "export_latency"},
                seed_vector="export-recurring-latency",
            )
            t0 = time.monotonic()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                route="DEEP_INQUIRY_PATH",
                config=InquiryConfig(
                    max_rounds=1,
                    questions_per_round=3,
                    evidence_reservoir_limit=120,
                    reasoning_packet_token_budget=6000,
                    persist=False,
                ),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            bundle = await assemble_context(
                result.retrieval_result,
                AccessContext(tenant_id=ctx["tenant"], requestor_actor_id=ctx["owner"]),
                conn,
            )
            prompt = build_prompt(trigger, bundle, triggering_content=ctx["signal_text"])

    retrieval_paths = {
        path
        for card in result.evidence_cards
        for path in card.retrieval_paths
    }
    evidence_refs = {card.source_ref for card in result.evidence_cards}
    metrics = _metrics(case=case, result=result, prompt_user=prompt.user, elapsed_ms=elapsed_ms)
    issues = []
    if "Q_RECURRENCE" not in metrics["question_ids"]:
        issues.append("recurring signal did not ask Q_RECURRENCE in the first round")
    if not {"pattern", "model_edge"} & retrieval_paths:
        issues.append(f"recurrence question did not use pattern/model-edge paths: {retrieval_paths}")
    if f"model:{ctx['pattern']}" not in evidence_refs:
        issues.append("pattern model did not survive into evidence reservoir")
    if f"model:{ctx['instance']}" not in evidence_refs:
        issues.append("pattern instance did not survive into evidence reservoir")
    if metrics["semantic_action_count"] > 3:
        issues.append(f"semantic action count too high for recurrence case: {metrics['semantic_action_count']}")
    passed = not issues
    _record(case, passed, metrics, issues)

    assert passed, issues


async def test_ambiguous_owner_escalates_instead_of_inventing_ownership(fresh_db):
    case = "ambiguous_owner_human_validation"
    async with fresh_db.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            ctx = await _seed_ambiguous_owner_case(conn)
            trigger = _trigger(
                tenant_id=ctx["tenant"],
                observation_id=ctx["signal_obs"],
                text=ctx["signal_text"],
                occurred_at=ctx["observed_at"],
                actor_id=ctx["actor"],
                entities=[
                    {"type": "commitment", "id": str(ctx["commitment"])},
                    {"type": "goal", "id": str(ctx["goal"])},
                    {"type": "customer_resource", "id": str(ctx["customer"])},
                ],
                seed_vector="acme-owner-ambiguous",
            )
            t0 = time.monotonic()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                route="DEEP_INQUIRY_PATH",
                config=InquiryConfig(
                    max_rounds=1,
                    questions_per_round=3,
                    evidence_reservoir_limit=120,
                    reasoning_packet_token_budget=6000,
                    persist=False,
                ),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            bundle = await assemble_context(
                result.retrieval_result,
                AccessContext(tenant_id=ctx["tenant"], requestor_actor_id=ctx["actor"]),
                conn,
            )
            prompt = build_prompt(trigger, bundle, triggering_content=ctx["signal_text"])

    owner_answer = next(
        (answer for answer in result.question_answers if answer.question_id == "Q_OWNER"),
        None,
    )
    metrics = _metrics(case=case, result=result, prompt_user=prompt.user, elapsed_ms=elapsed_ms)
    issues = []
    if "Q_OWNER" not in metrics["question_ids"]:
        issues.append("ownership question was not asked")
    if owner_answer is None:
        issues.append("ownership question did not produce an answer record")
    elif owner_answer.answer_status not in {"inconclusive", "unanswered"}:
        issues.append(f"unassigned commitment resolved ownership as {owner_answer.answer_status}")
    if result.sufficiency.status != "human_validation_required":
        issues.append(f"expected human_validation_required, got {result.sufficiency.status}")
    if "responsible owner" not in result.sufficiency.remaining_unknowns:
        issues.append("responsible owner was not preserved as an important unknown")
    passed = not issues
    _record(case, passed, metrics, issues)

    assert passed, issues


async def test_stale_counterevidence_does_not_close_current_blocker_question(fresh_db):
    case = "stale_counterevidence_guard"
    async with fresh_db.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            ctx = await _seed_stale_counterevidence_case(conn)
            trigger = _trigger(
                tenant_id=ctx["tenant"],
                observation_id=ctx["signal_obs"],
                text=ctx["signal_text"],
                occurred_at=ctx["observed_at"],
                actor_id=ctx["owner"],
                entities=[
                    {"type": "commitment", "id": str(ctx["commitment"])},
                    {"type": "goal", "id": str(ctx["goal"])},
                    {"type": "customer_resource", "id": str(ctx["customer"])},
                ],
                seed_vector="acme-stale-counter",
            )
            t0 = time.monotonic()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                route="DEEP_INQUIRY_PATH",
                config=InquiryConfig(
                    max_rounds=1,
                    questions_per_round=3,
                    evidence_reservoir_limit=120,
                    reasoning_packet_token_budget=6000,
                    temporal_window_days=30,
                    persist=False,
                ),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            bundle = await assemble_context(
                result.retrieval_result,
                AccessContext(tenant_id=ctx["tenant"], requestor_actor_id=ctx["owner"]),
                conn,
            )
            prompt = build_prompt(trigger, bundle, triggering_content=ctx["signal_text"])

    counter_answer = next(
        (answer for answer in result.question_answers if answer.question_id == "Q_COUNTEREVIDENCE"),
        None,
    )
    stale_card = next(
        (card for card in result.evidence_cards if card.source_ref == f"model:{ctx['stale_counter']}"),
        None,
    )
    metrics = _metrics(case=case, result=result, prompt_user=prompt.user, elapsed_ms=elapsed_ms)
    issues = []
    if counter_answer is None:
        issues.append("counterevidence question did not produce an answer")
    elif stale_card and str(stale_card.evidence_id) in counter_answer.counterevidence:
        issues.append("stale counterevidence was treated as fresh counterevidence")
    if counter_answer and "fresh counterevidence" not in counter_answer.new_uncertainties:
        issues.append("fresh counterevidence did not remain an uncertainty")
    if result.sufficiency.status == "sufficient_for_reasoning":
        issues.append("stale counterevidence closed sufficiency for a current blocker")
    passed = not issues
    _record(case, passed, metrics, issues)

    assert passed, issues


async def test_human_validation_route_stays_bounded_and_refuses_deep_inquiry(fresh_db):
    case = "human_validation_bounded"
    async with fresh_db.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            ctx = await _seed_blocker_case(conn)
            text = "No recorded decision exists for Acme launch, but offline alignment says the scope changed."
            route = _RouteDecision(
                route="HUMAN_VALIDATION_PATH",
                score=1.0,
                reason="routing gate retired; e2e fixture exercises human validation",
            )
            trigger = _trigger(
                tenant_id=ctx["tenant"],
                observation_id=ctx["signal_obs"],
                text=text,
                occurred_at=ctx["observed_at"],
                actor_id=ctx["owner"],
                entities=[{"type": "commitment", "id": str(ctx["commitment"])}],
                seed_vector="acme-sso-blocker",
            )
            t0 = time.monotonic()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                route=route.route,
                config=InquiryConfig(
                    max_rounds=2,
                    questions_per_round=3,
                    evidence_reservoir_limit=120,
                    reasoning_packet_token_budget=6000,
                    persist=False,
                ),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            bundle = await assemble_context(
                result.retrieval_result,
                AccessContext(tenant_id=ctx["tenant"], requestor_actor_id=ctx["owner"]),
                conn,
            )
            prompt = build_prompt(trigger, bundle, triggering_content=text)

    metrics = _metrics(case=case, result=result, prompt_user=prompt.user, elapsed_ms=elapsed_ms)
    issues = []
    if route.route != "HUMAN_VALIDATION_PATH":
        issues.append(f"route did not select human validation: {route.route}")
    if result.questions:
        issues.append("human validation route asked deep inquiry questions")
    if result.retrieval_actions:
        issues.append("human validation route compiled retrieval actions")
    if result.sufficiency.status != "human_validation_required":
        issues.append(f"expected human_validation_required, got {result.sufficiency.status}")
    passed = not issues
    _record(case, passed, metrics, issues)

    assert passed, issues


async def test_semantic_actions_embed_the_question_not_only_the_trigger(fresh_db):
    case = "question_conditioned_semantic_embedding"
    embedder = _QuestionAwareEmbedder()
    async with fresh_db.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            ctx = await _seed_blocker_case(conn)
            await _make_scoped_model(
                conn,
                ctx["tenant"],
                natural="Alice Owner is responsible for the Acme SSO dependency.",
                proposition={"kind": "state", "subject": "Acme SSO", "assertion": "Alice Owner is responsible"},
                scope_actors=[ctx["owner"]],
                scope_entities=[{"type": "commitment", "id": str(ctx["commitment"])}],
                activation=0.97,
                embed_seed="owner-question",
            )
            trigger = _trigger(
                tenant_id=ctx["tenant"],
                observation_id=ctx["signal_obs"],
                text=ctx["signal_text"],
                occurred_at=ctx["observed_at"],
                actor_id=ctx["owner"],
                entities=[{"type": "commitment", "id": str(ctx["commitment"])}],
                seed_vector="trigger-only",
            )
            t0 = time.monotonic()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                embedder=embedder,
                route="DEEP_INQUIRY_PATH",
                config=InquiryConfig(
                    max_rounds=1,
                    questions_per_round=3,
                    evidence_reservoir_limit=120,
                    reasoning_packet_token_budget=6000,
                    persist=False,
                ),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            bundle = await assemble_context(
                result.retrieval_result,
                AccessContext(tenant_id=ctx["tenant"], requestor_actor_id=ctx["owner"]),
                conn,
            )
            prompt = build_prompt(trigger, bundle, triggering_content=ctx["signal_text"])

    metrics = _metrics(
        case=case,
        result=result,
        prompt_user=prompt.user,
        elapsed_ms=elapsed_ms,
        extra={"embedder_query_count": len(embedder.queries)},
    )
    issues = []
    if not embedder.queries:
        issues.append("semantic actions reused the trigger vector and never called the embedder")
    if not any("owner responsible assigned owns dependency" in q for q in embedder.queries):
        issues.append("owner semantic action did not embed the owner-specific question")
    if not any("counterevidence" in q for q in embedder.queries):
        issues.append("counterevidence semantic action did not embed the counterevidence question")
    passed = not issues
    _record(case, passed, metrics, issues)

    assert passed, issues


async def test_noisy_same_actor_semantic_distractors_do_not_become_decisive(fresh_db):
    case = "semantic_noise_distractor_guard"
    async with fresh_db.acquire() as conn:
        await pgvector_pool_init(conn)
        async with conn.transaction():
            ctx = await _seed_blocker_case(conn)
            for index in range(10):
                await _make_scoped_model(
                    conn,
                    ctx["tenant"],
                    natural=(
                        "Zenith blocker blocker launch launch delay delay "
                        f"noise item {index} unrelated to Acme SSO."
                    ),
                    proposition={
                        "kind": "concern",
                        "about": "Zenith launch",
                        "nature": "semantic keyword distractor",
                    },
                    scope_actors=[ctx["owner"]],
                    scope_entities=[],
                    activation=0.98,
                    embed_seed="acme-sso-blocker",
                )
            trigger = _trigger(
                tenant_id=ctx["tenant"],
                observation_id=ctx["signal_obs"],
                text=ctx["signal_text"],
                occurred_at=ctx["observed_at"],
                actor_id=ctx["owner"],
                entities=[
                    {"type": "commitment", "id": str(ctx["commitment"])},
                    {"type": "goal", "id": str(ctx["goal"])},
                    {"type": "customer_resource", "id": str(ctx["customer"])},
                ],
                seed_vector="acme-sso-blocker",
            )
            t0 = time.monotonic()
            result = await run_inquiry_retrieval(
                trigger,
                conn,
                route="DEEP_INQUIRY_PATH",
                config=InquiryConfig(
                    max_rounds=1,
                    questions_per_round=3,
                    evidence_reservoir_limit=120,
                    reasoning_packet_token_budget=6000,
                    persist=False,
                ),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            bundle = await assemble_context(
                result.retrieval_result,
                AccessContext(tenant_id=ctx["tenant"], requestor_actor_id=ctx["owner"]),
                conn,
            )
            prompt = build_prompt(trigger, bundle, triggering_content=ctx["signal_text"])

    packet_text = json.dumps(result.context_packet.get("tiers", {}), default=str)
    h1_groups = [
        group
        for group in result.context_packet.get("tiers", {}).get("supporting_evidence_groups", [])
        if group.get("claim_supported") == "H1"
    ]
    metrics = _metrics(case=case, result=result, prompt_user=prompt.user, elapsed_ms=elapsed_ms)
    issues = []
    if "Zenith" in json.dumps(result.context_packet["tiers"]["decisive_evidence"], default=str):
        issues.append("same-actor semantic distractor became decisive evidence")
    if any("Zenith" in json.dumps(group, default=str) for group in h1_groups):
        issues.append("same-actor semantic distractor was grouped as H1 support")
    if metrics["retrieval_action_count"] > _MAX_DEEP_RETRIEVAL_ACTIONS:
        issues.append(f"noisy case exceeded retrieval action budget: {metrics['retrieval_action_count']}")
    if packet_text.count("Zenith") > 10:
        issues.append("semantic noise dominated the context packet")
    passed = not issues
    _record(case, passed, metrics, issues)

    assert passed, issues
