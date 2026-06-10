#!/usr/bin/env python3
"""Run a large-universe real-LLM retrieval-engine stress campaign.

The older retrieval scale curve proves a few fixed shapes across model counts.
This runner is intentionally wider: it builds one high-density tenant with a
large noisy Model universe, plants known themed "needle" memories, then runs
many diverse T1 inquiry retrievals through the live LLM question-planning path.

Reports land under tests/real_llm/reports/runs/retrieval-engine-stress-*.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("LLM_CACHE_BYPASS", "1")

import asyncpg
from dotenv import load_dotenv

from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from lib.shared.types import ModelCreate
from services.platform.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.domain.models.repo import ModelsRepo, pgvector_pool_init
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.tests._fixtures import build_fixture, make_embedding
from scripts.run_1000_signal_model_layer_probe import _build_cached_provider


load_dotenv(REPO_ROOT / ".env", override=False)


@dataclass(frozen=True)
class Archetype:
    key: str
    signal_class: str
    text_template: str
    domain_tags: tuple[str, ...]
    required_primitives: tuple[str, ...] = ()
    required_paths: tuple[str, ...] = ()
    selected_min: int = 1
    selected_max: int = 64
    evidence_hit_min: int = 2
    final_hit_min: int = 1
    use_seed_entities: bool = True
    include_decision: bool = False
    pattern_case: bool = False
    graph_case: bool = False
    broad_case: bool = False
    weak_case: bool = False


@dataclass
class StressCase:
    index: int
    name: str
    archetype: Archetype
    trigger: TriggerContext
    expected_model_ids: list[UUID] = field(default_factory=list)
    expected_observation_ids: list[UUID] = field(default_factory=list)
    theme_label: str = ""


ARCHETYPES: tuple[Archetype, ...] = (
    Archetype(
        key="compliance_dependency",
        signal_class="material",
        text_template=(
            "{customer} cannot pass the compliance gate because {commitment} "
            "is missing SOC2 evidence, the reviewer is overloaded, and launch "
            "depends on the audit packet this week. Marker {label}."
        ),
        domain_tags=("compliance", "customers", "execution"),
        required_primitives=("COUNTEREVIDENCE", "DEPENDENCY"),
        required_paths=("semantic", "temporal", "structural"),
    ),
    Archetype(
        key="owner_capacity",
        signal_class="material",
        text_template=(
            "{customer} has no clear owner for {commitment}; the named owner is "
            "split across escalations, the delivery date is slipping, and the "
            "team needs an escalation path. Marker {label}."
        ),
        domain_tags=("people", "execution", "customers"),
        required_primitives=("COUNTEREVIDENCE", "OWNERSHIP"),
        required_paths=("structural", "semantic"),
    ),
    Archetype(
        key="revenue_churn_conflict",
        signal_class="material",
        text_template=(
            "{customer} is marked healthy in the renewal deck, but usage fell, "
            "finance paused the invoice, and the sponsor says churn risk is "
            "still active. Marker {label}."
        ),
        domain_tags=("revenue", "customers", "risk"),
        required_primitives=("COUNTEREVIDENCE", "GOAL_IMPACT"),
        selected_min=2,
    ),
    Archetype(
        key="recurring_incident",
        signal_class="material",
        text_template=(
            "{customer} reported the same incident again; the export freshness "
            "failure repeated after the prior mitigation and may be a recurring "
            "pattern. Marker {label}."
        ),
        domain_tags=("reliability", "patterns", "customers"),
        required_primitives=("COUNTEREVIDENCE", "RECURRENCE"),
        required_paths=("pattern", "model_edge"),
        pattern_case=True,
        graph_case=True,
    ),
    Archetype(
        key="stale_replay_vs_fresh_risk",
        signal_class="material",
        text_template=(
            "{customer} received a stale replay of last month's incident, but "
            "today's data shows a fresh reconciliation delay and procurement "
            "needs the new risk separated from the old one. Marker {label}."
        ),
        domain_tags=("risk", "customers", "temporal"),
        required_primitives=("COUNTEREVIDENCE",),
        required_paths=("temporal", "semantic"),
    ),
    Archetype(
        key="decision_reversal",
        signal_class="material",
        text_template=(
            "The prior decision for {customer} said no custom exceptions, but "
            "the CRO now wants {commitment} reversed before pricing expires. "
            "Marker {label}."
        ),
        domain_tags=("decisions", "revenue", "execution"),
        required_primitives=("COUNTEREVIDENCE", "DEPENDENCY"),
        required_paths=("structural", "semantic"),
        include_decision=True,
    ),
    Archetype(
        key="pricing_policy_pressure",
        signal_class="material",
        text_template=(
            "{customer} needs a pricing exception; finance wants margin "
            "protection, sales says renewal risk is high, and product says "
            "{commitment} is still the binding constraint. Marker {label}."
        ),
        domain_tags=("pricing", "revenue", "customers"),
        required_primitives=("COUNTEREVIDENCE", "DEPENDENCY"),
    ),
    Archetype(
        key="broad_portfolio",
        signal_class="broad",
        text_template=(
            "Board update across all enterprise customers: renewal risk, legal "
            "review, security approvals, capacity pressure, and billing disputes "
            "may affect the portfolio renewal base. Marker {label}."
        ),
        domain_tags=("portfolio", "revenue", "risk"),
        required_primitives=("COUNTEREVIDENCE",),
        selected_min=18,
        selected_max=64,
        evidence_hit_min=8,
        final_hit_min=4,
        use_seed_entities=False,
        broad_case=True,
    ),
    Archetype(
        key="weak_workspace_noise",
        signal_class="weak",
        text_template=(
            "Workspace chatter for {customer}: lunch notes, travel plans, and "
            "general team coordination; no blocker, owner change, decision, "
            "customer risk, or commitment update. Marker {label}."
        ),
        domain_tags=("noise",),
        selected_min=0,
        selected_max=8,
        evidence_hit_min=0,
        final_hit_min=0,
        weak_case=True,
    ),
    Archetype(
        key="hidden_graph_dependency",
        signal_class="material",
        text_template=(
            "{customer} launch is blocked by an integration dependency that is "
            "not lexically obvious in the status note; model graph evidence "
            "links it to {commitment}. Marker {label}."
        ),
        domain_tags=("graph", "dependencies", "execution"),
        required_primitives=("COUNTEREVIDENCE", "DEPENDENCY"),
        required_paths=("model_edge",),
        graph_case=True,
    ),
)


async def _insert_observation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    text: str,
    occurred_at: datetime,
    entities: list[dict[str, str]],
) -> UUID:
    obs_id = uuid7()
    mentions = list(entities) + [{"type": "actor", "id": str(actor_id)}]
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            source_actor_ref, actor_id, content, content_text,
            embedding, embedding_pending, trust_tier, external_id,
            entities_mentioned
        ) VALUES (
            $1, $2, $3, 'signal', 'retrieval-stress',
            $4, $5, $6::jsonb, $7,
            $8, FALSE, 'authoritative', $9, $10::jsonb
        )
        """,
        obs_id,
        tenant_id,
        occurred_at,
        f"fixture:{actor_id}",
        actor_id,
        json.dumps({"text": text}),
        text,
        make_embedding(text),
        f"retrieval-stress-{tenant_id}-{obs_id}",
        json.dumps(mentions),
    )
    return obs_id


def _belief_prop(
    *,
    about: str,
    nature: str,
    claim_role: str,
    domain_tags: tuple[str, ...],
    abstraction_level: str = "atomic",
    time_mode: str = "current",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prop: dict[str, Any] = {
        "kind": "belief",
        "about": about,
        "nature": nature,
        "modality": "observed",
        "polarity": "negative",
        "time_mode": time_mode,
        "claim_role": claim_role,
        "domain_tags": list(domain_tags),
        "abstraction_level": abstraction_level,
    }
    if extra:
        prop.update(extra)
    return prop


async def _add_model(
    repo: ModelsRepo,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    born_from_event_id: UUID,
    natural: str,
    scope_entities: list[dict[str, str]],
    scope_actors: list[UUID],
    embedding_key: str,
    proposition: dict[str, Any],
    domain_tags: tuple[str, ...],
    supporting_model_ids: list[UUID] | None = None,
) -> UUID:
    row = await repo.insert(
        ModelCreate(
            tenant_id=tenant_id,
            born_from_event_id=born_from_event_id,
            proposition=proposition,
            natural=natural,
            embedding=make_embedding(embedding_key),
            scope_actors=scope_actors,
            scope_entities=scope_entities,
            scope_temporal={"type": "now"},
            confidence=0.62,
            confidence_at_assertion=0.62,
            supporting_event_ids=[born_from_event_id],
            supporting_model_ids=list(supporting_model_ids or []),
            domain_tags=list(domain_tags),
        ),
        conn=conn,
    )
    return row.id


async def _insert_edge(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    source_model_id: UUID,
    target_model_id: UUID,
    edge_kind: str,
    event_id: UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO model_edges (
            id, tenant_id, source_model_id, target_model_id, edge_kind,
            weight, metadata, status, detected_by, created_by_event_id,
            confidence, evidence_event_ids, evidence_model_ids,
            explanation, review_status, confirmed_count
        ) VALUES (
            $1, $2, $3, $4, $5,
            0.82, '{}'::jsonb, 'active', 'retrieval_stress', $6,
            0.82, ARRAY[$6]::uuid[], ARRAY[$3]::uuid[],
            'retrieval stress planted edge', 'accepted', 1
        )
        ON CONFLICT ON CONSTRAINT model_edges_unique DO NOTHING
        """,
        uuid7(),
        tenant_id,
        source_model_id,
        target_model_id,
        edge_kind,
        event_id,
    )


def _scope_for_case(fs: Any, case_index: int, archetype: Archetype) -> dict[str, Any]:
    actor = fs.actor_ids[(case_index * 3) % len(fs.actor_ids)]
    customer = fs.customer_resource_ids[(case_index * 5) % len(fs.customer_resource_ids)]
    commitment = fs.commitment_ids[(case_index * 7) % len(fs.commitment_ids)]
    goal = fs.goal_ids[(case_index * 11) % len(fs.goal_ids)]
    decision = fs.decision_ids[(case_index * 13) % len(fs.decision_ids)]
    entities = [
        {"type": "customer_resource", "id": str(customer)},
        {"type": "commitment", "id": str(commitment)},
        {"type": "goal", "id": str(goal)},
    ]
    if archetype.include_decision:
        entities.append({"type": "decision", "id": str(decision)})
    return {
        "actor": actor,
        "customer": customer,
        "commitment": commitment,
        "goal": goal,
        "decision": decision,
        "entities": entities,
        "customer_label": f"customer-{(case_index * 5) % len(fs.customer_resource_ids)}",
        "commitment_label": f"commitment {(case_index * 7) % len(fs.commitment_ids)}",
    }


async def _plant_case_memory(
    repo: ModelsRepo,
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    fs: Any,
    case_index: int,
    archetype: Archetype,
    now: datetime,
) -> StressCase:
    scope = _scope_for_case(fs, case_index, archetype)
    label = f"RTS-{case_index:03d}-{archetype.key}"
    signal_text = archetype.text_template.format(
        customer=scope["customer_label"],
        commitment=scope["commitment_label"],
        label=label,
    )
    seed_entities = list(scope["entities"]) if archetype.use_seed_entities else []
    if archetype.weak_case:
        seed_entities = []
    observation_ids: list[UUID] = []
    for offset in range(2):
        obs_text = (
            f"{label} evidence observation {offset}: {signal_text} "
            f"diagnostic-marker-{offset}"
        )
        observation_ids.append(
            await _insert_observation(
                conn,
                tenant_id=tenant_id,
                actor_id=scope["actor"],
                text=obs_text,
                occurred_at=now - timedelta(hours=offset + 1),
                entities=list(scope["entities"]),
            )
        )

    expected_ids: list[UUID] = []
    embedding_key = f"{label} {archetype.key} {signal_text}"
    if not archetype.weak_case:
        material_model_count = 16 if archetype.broad_case else 4
        for i in range(material_model_count):
            model_scope = scope
            if archetype.broad_case:
                model_scope = _scope_for_case(fs, case_index + i + 1, archetype)
            natural = (
                f"{label} material memory {i}: {model_scope['customer_label']} "
                f"{archetype.key} evidence ties {model_scope['commitment_label']} "
                "to current operational pressure, owner risk, and customer impact."
            )
            mid = await _add_model(
                repo,
                conn,
                tenant_id=tenant_id,
                born_from_event_id=observation_ids[i % len(observation_ids)],
                natural=natural,
                scope_entities=list(model_scope["entities"]),
                scope_actors=[model_scope["actor"]],
                embedding_key=embedding_key,
                proposition=_belief_prop(
                    about=f"{model_scope['customer_label']} {archetype.key}",
                    nature=natural,
                    claim_role="concern",
                    domain_tags=archetype.domain_tags,
                ),
                domain_tags=archetype.domain_tags,
            )
            expected_ids.append(mid)

        counter = await _add_model(
            repo,
            conn,
            tenant_id=tenant_id,
            born_from_event_id=observation_ids[0],
            natural=(
                f"{label} counterevidence memory: possible mitigation exists, "
                "but it does not yet remove the active customer risk."
            ),
            scope_entities=list(scope["entities"]),
            scope_actors=[scope["actor"]],
            embedding_key=embedding_key,
            proposition=_belief_prop(
                about=f"{scope['customer_label']} counterevidence",
                nature="possible mitigation exists but risk remains active",
                claim_role="concern",
                domain_tags=archetype.domain_tags,
            ),
            domain_tags=archetype.domain_tags,
        )
        expected_ids.append(counter)

    if archetype.pattern_case:
        signature = {"stress_pattern": archetype.key, "variant": case_index % 5}
        pattern = await _add_model(
            repo,
            conn,
            tenant_id=tenant_id,
            born_from_event_id=observation_ids[0],
            natural=(
                f"{label} pattern memory: repeated export freshness failures "
                "cluster after mitigation claims."
            ),
            scope_entities=list(scope["entities"]),
            scope_actors=[scope["actor"]],
            embedding_key=embedding_key,
            proposition=_belief_prop(
                about=f"{scope['customer_label']} recurring incident pattern",
                nature="repeated export freshness failures cluster after mitigation",
                claim_role="pattern",
                domain_tags=archetype.domain_tags,
                abstraction_level="pattern",
                extra={"signature": signature},
            ),
            domain_tags=archetype.domain_tags,
        )
        instance = await _add_model(
            repo,
            conn,
            tenant_id=tenant_id,
            born_from_event_id=observation_ids[1],
            natural=f"{label} pattern instance memory: latest repeated incident.",
            scope_entities=list(scope["entities"]),
            scope_actors=[scope["actor"]],
            embedding_key=embedding_key,
            proposition=_belief_prop(
                about=f"{scope['customer_label']} pattern instance",
                nature="latest repeated export freshness incident",
                claim_role="pattern",
                domain_tags=archetype.domain_tags,
                abstraction_level="pattern",
                time_mode="past",
                extra={
                    "pattern_id": str(pattern),
                    "matched_context": {
                        "label": label,
                        "incident": "latest repeated export freshness incident",
                    },
                },
            ),
            domain_tags=archetype.domain_tags,
        )
        await _insert_edge(
            conn,
            tenant_id=tenant_id,
            source_model_id=instance,
            target_model_id=pattern,
            edge_kind="instance_of",
            event_id=observation_ids[1],
        )
        expected_ids.extend([pattern, instance])
        seed_signature = signature
    else:
        seed_signature = None

    if archetype.graph_case and expected_ids:
        hidden = await _add_model(
            repo,
            conn,
            tenant_id=tenant_id,
            born_from_event_id=observation_ids[0],
            natural=(
                f"{label} graph-only memory: latent platform invariant B-17 "
                "explains the dependency without sharing the surface language."
            ),
            scope_entities=[],
            scope_actors=[],
            embedding_key=f"graph-hidden-{label}",
            proposition=_belief_prop(
                about="latent platform invariant B-17",
                nature="graph-only dependency explanation",
                claim_role="relation",
                domain_tags=archetype.domain_tags,
                abstraction_level="relationship",
                extra={
                    "subject": f"{label} scoped customer risk",
                    "relation": "same_issue_as",
                    "object": "latent platform invariant B-17",
                },
            ),
            domain_tags=archetype.domain_tags,
        )
        await _insert_edge(
            conn,
            tenant_id=tenant_id,
            source_model_id=expected_ids[0],
            target_model_id=hidden,
            edge_kind="same_issue_as",
            event_id=observation_ids[0],
        )
        expected_ids.append(hidden)

    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_entity_ids=seed_entities,
        seed_natural_text=signal_text,
        seed_occurred_at=now,
        scope_actors=[] if archetype.broad_case or archetype.weak_case else [scope["actor"]],
        precomputed_seed_vector=make_embedding(embedding_key),
        seed_signature=seed_signature,
    )
    return StressCase(
        index=case_index,
        name=f"{case_index:03d}_{archetype.key}",
        archetype=archetype,
        trigger=trigger,
        expected_model_ids=expected_ids,
        expected_observation_ids=observation_ids,
        theme_label=label,
    )


async def _build_universe(
    conn: asyncpg.Connection,
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    target_models: int,
    case_count: int,
) -> tuple[list[StressCase], int, dict[str, Any]]:
    now = datetime.now(timezone.utc)
    themed_model_budget = case_count * 12
    fixture_models = max(100, target_models - themed_model_budget)
    fixture_started = time.monotonic()
    fs = await build_fixture(
        conn,
        tenant_id,
        pool=pool,
        rng_seed=9000 + target_models + case_count,
        n_actors=max(20, min(80, target_models // 250)),
        n_goals=max(48, min(180, target_models // 90)),
        n_commitments=max(160, min(520, target_models // 35)),
        n_observations=max(520, min(1800, target_models // 12)),
        n_models=fixture_models,
        n_customers=max(32, min(160, target_models // 80)),
        n_decisions=max(24, min(96, target_models // 150)),
    )
    fixture_elapsed_ms = round((time.monotonic() - fixture_started) * 1000, 2)
    repo = ModelsRepo(pool, embedder=None, run_topology_on_insert=False)
    cases: list[StressCase] = []
    plant_started = time.monotonic()
    for index in range(case_count):
        archetype = ARCHETYPES[index % len(ARCHETYPES)]
        cases.append(
            await _plant_case_memory(
                repo,
                conn,
                tenant_id=tenant_id,
                fs=fs,
                case_index=index,
                archetype=archetype,
                now=now,
            )
        )
    plant_elapsed_ms = round((time.monotonic() - plant_started) * 1000, 2)
    total_models = await conn.fetchval(
        "SELECT COUNT(*)::int FROM models WHERE tenant_id = $1 AND status = 'active'",
        tenant_id,
    )
    diagnostics = {
        "fixture_elapsed_ms": fixture_elapsed_ms,
        "plant_elapsed_ms": plant_elapsed_ms,
        "fixture_models_requested": fixture_models,
        "themed_model_budget": themed_model_budget,
    }
    return cases, int(total_models), diagnostics


def _path_counts(result: Any) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for action in result.retrieval_actions:
        counts[action.path] += 1
    return dict(sorted(counts.items()))


def _evidence_path_set(result: Any) -> set[str]:
    paths: set[str] = set()
    for card in result.evidence_cards:
        paths.update(str(p) for p in card.retrieval_paths)
    return paths


def _evaluate_case(case: StressCase, result: Any, elapsed_ms: float) -> dict[str, Any]:
    relevance = result.retrieval_result.notes.get("relevance_gate") or {}
    planning = result.notes.get("question_planning") or []
    question_primitives = [q.primitive for q in result.questions]
    question_ids = [q.question_id for q in result.questions]
    evidence_refs = {card.source_ref for card in result.evidence_cards}
    selected_ids = {model.id for model in result.retrieval_result.models}
    expected_refs = {f"model:{mid}" for mid in case.expected_model_ids}
    evidence_hits = sorted(ref for ref in expected_refs if ref in evidence_refs)
    final_hits = sorted(str(mid) for mid in case.expected_model_ids if mid in selected_ids)
    paths = _evidence_path_set(result)
    issues: list[str] = []
    warnings: list[str] = []
    arch = case.archetype
    selected_count = len(result.retrieval_result.models)
    signal_class = str(
        relevance.get("signal_class")
        or (result.notes or {}).get("signal_class")
    )
    if signal_class != arch.signal_class:
        issues.append(f"signal_class {signal_class} != expected {arch.signal_class}")
    if selected_count < arch.selected_min or selected_count > arch.selected_max:
        issues.append(
            f"selected_count {selected_count} outside "
            f"{arch.selected_min}-{arch.selected_max}"
        )
    if len(evidence_hits) < arch.evidence_hit_min:
        issues.append(
            f"expected evidence hits {len(evidence_hits)} < {arch.evidence_hit_min}"
        )
    if len(final_hits) < arch.final_hit_min:
        issues.append(f"expected final hits {len(final_hits)} < {arch.final_hit_min}")
    for primitive in arch.required_primitives:
        if primitive not in question_primitives:
            issues.append(f"missing required question primitive {primitive}")
    for path in arch.required_paths:
        if path not in paths and path not in _path_counts(result):
            issues.append(f"missing required retrieval path {path}")
    non_llm_planning = [
        note for note in planning if note.get("mode") != "llm"
    ]
    if not planning and arch.weak and result.sufficiency.status == "no_update_needed":
        pass
    elif not planning:
        issues.append("question planning did not produce telemetry")
    elif non_llm_planning:
        timeout_only = all(
            note.get("mode") == "deterministic_fallback"
            and note.get("reason") == "TimeoutError"
            for note in non_llm_planning
        )
        if timeout_only:
            warnings.append(f"question planning timed out and fell back: {planning}")
        else:
            issues.append(f"question planning was not fully llm: {planning}")
    return {
        "case": case.name,
        "archetype": arch.key,
        "theme_label": case.theme_label,
        "passed": not issues,
        "issues": issues,
        "warnings": warnings,
        "elapsed_ms": round(elapsed_ms, 2),
        "route": result.route,
        "sufficiency": result.sufficiency.status,
        "questions": len(result.questions),
        "question_ids": question_ids,
        "question_primitives": question_primitives,
        "planning_modes": [note.get("mode") for note in planning],
        "planning_providers": [
            note.get("llm_provider")
            for note in planning
            if note.get("llm_provider")
        ],
        "planning_models": [
            note.get("llm_model")
            for note in planning
            if note.get("llm_model")
        ],
        "planning_efforts": [
            note.get("llm_reasoning_effort")
            for note in planning
            if note.get("llm_reasoning_effort")
        ],
        "uses_codex_low_effort": any(
            bool(note.get("uses_codex_low_effort")) for note in planning
        ),
        "planning_primitives": [
            primitive
            for note in planning
            for primitive in (note.get("llm_primitives") or [])
        ],
        "retrieval_actions": len(result.retrieval_actions),
        "action_path_counts": _path_counts(result),
        "evidence_count": len(result.evidence_cards),
        "evidence_paths": sorted(paths),
        "candidate_count": relevance.get("candidate_count"),
        "selected_count": selected_count,
        "signal_class": signal_class,
        "threshold": relevance.get("threshold"),
        "cutoff_reason": relevance.get("cutoff_reason"),
        "dropped_below_threshold": relevance.get("dropped_below_threshold"),
        "dropped_redundant": relevance.get("dropped_redundant"),
        "expected_model_count": len(case.expected_model_ids),
        "expected_evidence_hits": len(evidence_hits),
        "expected_final_hits": len(final_hits),
        "evidence_hit_count": len(evidence_hits),
        "final_hit_count": len(final_hits),
        "evidence_hit_min": arch.evidence_hit_min,
        "final_hit_min": arch.final_hit_min,
        "expected_hit_refs": evidence_hits,
        "evidence_hit_refs": evidence_hits,
        "final_hit_model_ids": final_hits,
    }


async def _run_campaign(
    pool: asyncpg.Pool,
    *,
    provider: Any,
    target_models: int,
    case_count: int,
    max_rounds: int,
    questions_per_round: int,
) -> dict[str, Any]:
    tenant_id = uuid7()
    conn = await pool.acquire()
    tx = conn.transaction()
    await tx.start()
    campaign_started = time.monotonic()
    try:
        await pgvector_pool_init(conn)
        await conn.execute("SET CONSTRAINTS ALL DEFERRED")
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, $2, true)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
            f"retrieval_engine_stress_{target_models}_{case_count}",
        )
        build_started = time.monotonic()
        cases, total_models, build_diagnostics = await _build_universe(
            conn,
            pool,
            tenant_id=tenant_id,
            target_models=target_models,
            case_count=case_count,
        )
        build_elapsed_ms = round((time.monotonic() - build_started) * 1000, 2)
        cfg = InquiryConfig(
            max_rounds=max_rounds,
            questions_per_round=questions_per_round,
            evidence_reservoir_limit=700,
            fast_path_evidence_limit=80,
            candidate_model_limit=220,
            result_model_limit=64,
            action_model_budget_limit=48,
            action_observation_budget_limit=32,
            relevance_min_material_models=4,
            temporal_window_days=14,
            semantic_budget=42,
            structural_max_hops=2,
            model_edge_max_hops=2,
            persist=False,
        )
        results: list[dict[str, Any]] = []
        for i, case in enumerate(cases, start=1):
            print(
                f"retrieval stress case {i}/{len(cases)} "
                f"{case.name} models={total_models}",
                flush=True,
            )
            case_started = time.monotonic()
            result = await run_inquiry_retrieval(
                case.trigger,
                conn,
                llm_provider=provider,
                mode="deep",
                top_n=220,
                config=cfg,
            )
            elapsed_ms = (time.monotonic() - case_started) * 1000
            evaluated = _evaluate_case(case, result, elapsed_ms)
            print(
                "  selected={selected} evidence={evidence} "
                "hits={evidence_hits}/{expected} final={final_hits}/{expected} "
                "class={klass} status={status}".format(
                    selected=evaluated["selected_count"],
                    evidence=evaluated["evidence_count"],
                    evidence_hits=evaluated["evidence_hit_count"],
                    final_hits=evaluated["final_hit_count"],
                    expected=evaluated["expected_model_count"],
                    klass=evaluated["signal_class"],
                    status="pass" if evaluated["passed"] else "issue",
                ),
                flush=True,
            )
            results.append(evaluated)
        return {
            "tenant_id": str(tenant_id),
            "target_models": target_models,
            "total_models": total_models,
            "case_count": len(results),
            "max_rounds": max_rounds,
            "questions_per_round": questions_per_round,
            "build_elapsed_ms": build_elapsed_ms,
            "campaign_elapsed_ms": round((time.monotonic() - campaign_started) * 1000, 2),
            "build_diagnostics": build_diagnostics,
            "results": results,
        }
    finally:
        await tx.rollback()
        await pool.release(conn)


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = (len(ordered) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


def _summarize(campaign: dict[str, Any]) -> dict[str, Any]:
    results = campaign["results"]
    latencies = [float(r["elapsed_ms"]) for r in results]
    evidence = [int(r["evidence_count"]) for r in results]
    selected = [int(r["selected_count"]) for r in results]
    candidate = [int(r["candidate_count"] or 0) for r in results]
    failures = [r for r in results if not r["passed"]]
    by_issue: Counter[str] = Counter()
    for result in failures:
        for issue in result["issues"]:
            by_issue[issue.split(":", 1)[0]] += 1
    return {
        "passes": not failures,
        "failure_count": len(failures),
        "failed_cases": [r["case"] for r in failures],
        "issue_patterns": dict(by_issue.most_common(20)),
        "latency_ms": {
            "min": min(latencies) if latencies else 0,
            "p50": _percentile(latencies, 0.50),
            "p90": _percentile(latencies, 0.90),
            "p95": _percentile(latencies, 0.95),
            "max": max(latencies) if latencies else 0,
            "mean": statistics.mean(latencies) if latencies else 0,
        },
        "evidence_count": {
            "min": min(evidence) if evidence else 0,
            "p50": _percentile([float(v) for v in evidence], 0.50),
            "max": max(evidence) if evidence else 0,
            "mean": statistics.mean(evidence) if evidence else 0,
        },
        "selected_count": {
            "min": min(selected) if selected else 0,
            "p50": _percentile([float(v) for v in selected], 0.50),
            "max": max(selected) if selected else 0,
            "mean": statistics.mean(selected) if selected else 0,
        },
        "candidate_count": {
            "min": min(candidate) if candidate else 0,
            "p50": _percentile([float(v) for v in candidate], 0.50),
            "max": max(candidate) if candidate else 0,
            "mean": statistics.mean(candidate) if candidate else 0,
        },
        "signal_classes": dict(Counter(r["signal_class"] for r in results)),
        "archetype_failures": dict(
            Counter(r["archetype"] for r in failures).most_common()
        ),
    }


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    campaign = report["campaign"]
    summary = report["summary"]
    lines = [
        "# Retrieval Engine Stress Report",
        "",
        f"- Run id: `{report['run_id']}`",
        f"- Total Models: {campaign['total_models']}",
        f"- Cases: {campaign['case_count']}",
        f"- Passes: {summary['passes']}",
        f"- Failures: {summary['failure_count']}",
        f"- Build elapsed ms: {campaign['build_elapsed_ms']}",
        f"- Campaign elapsed ms: {campaign['campaign_elapsed_ms']}",
        "",
        "## Latency",
        "",
        json.dumps(summary["latency_ms"], indent=2, sort_keys=True),
        "",
        "## Count Summaries",
        "",
        f"- Evidence: `{summary['evidence_count']}`",
        f"- Selected: `{summary['selected_count']}`",
        f"- Candidates: `{summary['candidate_count']}`",
        "",
        "## Failures",
        "",
    ]
    failures = [r for r in campaign["results"] if not r["passed"]]
    if not failures:
        lines.append("None.")
    else:
        for result in failures:
            lines.append(
                f"- `{result['case']}` ({result['archetype']}): "
                + "; ".join(result["issues"])
            )
    lines.extend([
        "",
        "## Cases",
        "",
        "| Case | Class | Evidence | Selected | Evidence Hits | Final Hits | Latency ms | Status |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ])
    for result in campaign["results"]:
        lines.append(
            "| {case} | {klass} | {evidence} | {selected} | "
            "{evidence_hits}/{expected} | {final_hits}/{expected} | "
            "{latency} | {status} |".format(
                case=result["case"],
                klass=result["signal_class"],
                evidence=result["evidence_count"],
                selected=result["selected_count"],
                evidence_hits=result.get("evidence_hit_count", result["expected_evidence_hits"]),
                final_hits=result.get("final_hit_count", result["expected_final_hits"]),
                expected=result["expected_model_count"],
                latency=result["elapsed_ms"],
                status="pass" if result["passed"] else "issue",
            )
        )
    path.write_text("\n".join(lines) + "\n")


async def run_probe(
    *,
    target_models: int,
    case_count: int,
    max_rounds: int,
    questions_per_round: int,
    pool_max_size: int = 3,
    run_id: str | None = None,
) -> dict[str, Any]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set")
    provider = _build_cached_provider()
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=pool_max_size,
        init=pgvector_pool_init,
    )
    try:
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
        campaign = await _run_campaign(
            pool,
            provider=provider,
            target_models=target_models,
            case_count=case_count,
            max_rounds=max_rounds,
            questions_per_round=questions_per_round,
        )
        report = {
            "run_id": run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            "campaign": campaign,
        }
        report["summary"] = _summarize(campaign)
        report_dir = REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"
        report_dir.mkdir(parents=True, exist_ok=True)
        stem = f"retrieval-engine-stress-{report['run_id']}"
        json_path = report_dir / f"{stem}.json"
        md_path = report_dir / f"{stem}.md"
        json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str))
        _write_markdown(report, md_path)
        report["report_path"] = str(json_path)
        report["markdown_path"] = str(md_path)
        return report
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-models", type=int, default=12_000)
    parser.add_argument("--cases", type=int, default=50)
    parser.add_argument("--max-rounds", type=int, default=2)
    parser.add_argument("--questions-per-round", type=int, default=3)
    parser.add_argument("--pool-max-size", type=int, default=3)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    if args.cases < 50:
        print("warning: fewer than 50 cases requested", file=sys.stderr)
    report = asyncio.run(
        run_probe(
            target_models=args.target_models,
            case_count=args.cases,
            max_rounds=args.max_rounds,
            questions_per_round=args.questions_per_round,
            pool_max_size=args.pool_max_size,
            run_id=args.run_id,
        )
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0 if report["summary"]["passes"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
