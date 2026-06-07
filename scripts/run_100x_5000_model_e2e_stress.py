#!/usr/bin/env python3
"""Run 100 large end-to-end Model/retrieval stress cases.

This harness is intentionally outside pytest. It exercises the real
Model write boundary (`ModelsRepo.insert_many`) and deterministic Ask
Fyralis inquiry retrieval against a fresh >5,000-Model tenant per case.

Each case runs in its own transaction and is rolled back after metrics
are captured. That keeps the run honest without leaving 500k synthetic
Models in the local database.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import random
import statistics
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import UUID

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")

import asyncpg
from dotenv import load_dotenv

from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migrations_dir
from lib.shared.types import ModelCreate
from services.execution.inquiry import InquiryConfig, run_inquiry_retrieval
from services.models.repo import ModelsRepo, pgvector_pool_init
from services.retrieval.primary import TriggerContext


load_dotenv(REPO_ROOT / ".env", override=False)

LOCAL_DATABASE_URL = "postgresql://company_os:company_os@localhost:5432/company_os"


@dataclass(frozen=True, slots=True)
class Archetype:
    key: str
    signal: str
    domain_tags: tuple[str, ...]
    expected_final_min: int = 1
    expected_evidence_min: int = 1
    expected_selected_max: int | None = None
    use_seed_entities: bool = True
    use_actor_scope: bool = True
    broad: bool = False
    weak: bool = False
    operational: bool = False
    pipeline: bool = False
    graph: bool = False
    situation: bool = False
    entity_type: str = "customer_resource"


@dataclass(frozen=True, slots=True)
class Scaffold:
    tenant_id: UUID
    actor_id: UUID
    customer_id: UUID
    goal_id: UUID
    commitment_id: UUID
    decision_id: UUID
    target_observation_id: UUID
    counter_observation_id: UUID
    stale_observation_id: UUID
    noise_observation_id: UUID
    customer_label: str
    commitment_label: str


@dataclass(slots=True)
class StressCase:
    index: int
    name: str
    archetype: Archetype
    trigger: TriggerContext
    expected_model_ids: list[UUID] = field(default_factory=list)
    expected_member_ids: list[UUID] = field(default_factory=list)
    marker: str = ""


ARCHETYPES: tuple[Archetype, ...] = (
    Archetype(
        key="compliance_dependency",
        signal=(
            "{customer} launch is blocked because {commitment} still lacks "
            "SOC2 evidence and the compliance reviewer is overloaded. "
            "Marker {marker}."
        ),
        domain_tags=("compliance", "customers", "execution", "risk"),
        expected_final_min=2,
        expected_evidence_min=2,
    ),
    Archetype(
        key="owner_capacity",
        signal=(
            "{customer} delivery risk is active because {commitment} has no "
            "clear owner, review capacity is split, and escalation is needed. "
            "Marker {marker}."
        ),
        domain_tags=("people", "execution", "customers", "risk"),
        expected_final_min=2,
        expected_evidence_min=2,
    ),
    Archetype(
        key="revenue_churn_conflict",
        signal=(
            "{customer} is marked healthy in the renewal deck, but usage fell, "
            "finance paused the invoice, and churn risk remains active. "
            "Marker {marker}."
        ),
        domain_tags=("revenue", "customers", "risk"),
        expected_final_min=2,
        expected_evidence_min=2,
    ),
    Archetype(
        key="recurring_incident",
        signal=(
            "{customer} reported the export freshness incident again after "
            "the prior mitigation, so this may be a recurring pattern. "
            "Marker {marker}."
        ),
        domain_tags=("reliability", "patterns", "customers", "risk"),
        expected_final_min=2,
        expected_evidence_min=2,
        graph=True,
    ),
    Archetype(
        key="operational_form_state",
        signal=(
            "{customer} cannot complete the admin portal workflow because the "
            "configuration form state and option price deltas changed. "
            "Marker {marker}."
        ),
        domain_tags=("operations", "systems", "customers"),
        expected_final_min=2,
        expected_evidence_min=2,
        operational=True,
    ),
    Archetype(
        key="pipeline_stage",
        signal=(
            "{customer} onboarding is blocked because the pipeline stage chain "
            "has an unresolved security review and remaining item count. "
            "Marker {marker}."
        ),
        domain_tags=("operations", "execution", "systems", "customers"),
        expected_final_min=2,
        expected_evidence_min=2,
        pipeline=True,
    ),
    Archetype(
        key="hidden_graph_dependency",
        signal=(
            "{customer} launch is blocked by an integration dependency that "
            "is only obvious through related Model links, not surface text. "
            "Marker {marker}."
        ),
        domain_tags=("graph", "dependencies", "execution", "customers"),
        expected_final_min=2,
        expected_evidence_min=2,
        graph=True,
    ),
    Archetype(
        key="situation_composite",
        signal=(
            "{customer} has a composite operational situation: owner gap, "
            "security gate, and renewal exposure are reinforcing each other. "
            "Marker {marker}."
        ),
        domain_tags=("situations", "execution", "customers", "risk"),
        expected_final_min=2,
        expected_evidence_min=2,
        situation=True,
    ),
    Archetype(
        key="broad_portfolio",
        signal=(
            "Board update across enterprise customers: renewal risk, legal "
            "review, security approvals, capacity pressure, and billing "
            "disputes may affect the portfolio renewal base. Marker {marker}."
        ),
        domain_tags=("portfolio", "revenue", "risk", "customers"),
        expected_final_min=4,
        expected_evidence_min=4,
        use_seed_entities=False,
        use_actor_scope=False,
        broad=True,
    ),
    Archetype(
        key="customer_entity_vocabulary",
        signal=(
            "{customer} support escalation is blocked by {commitment}; this "
            "case intentionally writes customer scope vocabulary to test "
            "customer/customer_resource addressability. Marker {marker}."
        ),
        domain_tags=("customers", "support", "execution", "risk"),
        expected_final_min=2,
        expected_evidence_min=2,
        entity_type="customer",
    ),
    Archetype(
        key="fresh_vs_stale",
        signal=(
            "{customer} received a stale replay of last month's incident, but "
            "today's data shows a fresh reconciliation delay and procurement "
            "needs the new risk separated from the old one. Marker {marker}."
        ),
        domain_tags=("temporal", "customers", "risk", "finance"),
        expected_final_min=2,
        expected_evidence_min=2,
    ),
    Archetype(
        key="weak_workspace_noise",
        signal=(
            "Workspace chatter for {customer}: lunch notes, travel plans, "
            "and general team coordination. Marker {marker}."
        ),
        domain_tags=("noise", "workspace"),
        expected_final_min=0,
        expected_evidence_min=0,
        expected_selected_max=8,
        use_seed_entities=False,
        use_actor_scope=False,
        weak=True,
    ),
)


def _embedding(text: str) -> list[float]:
    return list(_embedding_tuple(text))


@lru_cache(maxsize=4096)
def _embedding_tuple(text: str, dim: int = 768) -> tuple[float, ...]:
    seed = int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "big")
    rng = random.Random(seed)
    vec = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0.0:
        return tuple(vec)
    return tuple(x / norm for x in vec)


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = (len(ordered) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac


def _belief_prop(
    *,
    about: str,
    nature: str,
    claim_role: str,
    domain_tags: tuple[str, ...],
    abstraction_level: str = "atomic",
    time_mode: str = "current",
    polarity: str = "negative",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    prop: dict[str, Any] = {
        "kind": "belief",
        "about": about,
        "nature": nature,
        "claim": nature,
        "summary": nature,
        "assessment": nature,
        "modality": "observed",
        "polarity": polarity,
        "time_mode": time_mode,
        "claim_role": claim_role,
        "domain_tags": list(domain_tags),
        "abstraction_level": abstraction_level,
    }
    if extra:
        prop.update(extra)
    return prop


def _scope_entities(scaffold: Scaffold, archetype: Archetype) -> list[dict[str, str]]:
    customer_entry = {"type": archetype.entity_type, "id": str(scaffold.customer_id)}
    return [
        customer_entry,
        {"type": "commitment", "id": str(scaffold.commitment_id)},
        {"type": "goal", "id": str(scaffold.goal_id)},
        {"type": "decision", "id": str(scaffold.decision_id)},
    ]


async def _insert_scaffold(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    index: int,
    archetype: Archetype,
    now: datetime,
) -> Scaffold:
    await conn.execute(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, $2, true)
        ON CONFLICT (id) DO NOTHING
        """,
        tenant_id,
        f"model_e2e_stress_{index:03d}_{archetype.key}",
    )
    actor_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status, metadata
        ) VALUES (
            $1, $2, 'human_internal', $3, $4, 'active', '{}'::jsonb
        )
        """,
        actor_id,
        tenant_id,
        f"Stress Operator {index:03d}",
        f"stress-{index:03d}@fyralis.example",
    )

    customer_id = uuid7()
    customer_label = f"StressCustomer{index:03d}"
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
        customer_id,
        tenant_id,
        customer_label,
        f"Customer account {customer_label}",
        _jsonb({"arr_cents": 25000000 + index * 1000}),
    )

    target_observation_id = uuid7()
    counter_observation_id = uuid7()
    stale_observation_id = uuid7()
    noise_observation_id = uuid7()
    entities = [
        {"type": archetype.entity_type, "id": str(customer_id)},
        {"type": "actor", "id": str(actor_id)},
    ]
    await conn.executemany(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, ingested_at, kind,
            source_channel, source_actor_ref, actor_id,
            content, content_text, embedding, embedding_pending,
            trust_tier, external_id, cause_id, entities_mentioned
        ) VALUES (
            $1, $2, $3, $4, 'signal',
            'model-100x5000-stress', $5, $6,
            $7::jsonb, $8, $9, FALSE,
            'authoritative', $10, NULL, $11::jsonb
        )
        """,
        [
            (
                target_observation_id,
                tenant_id,
                now,
                now,
                f"stress:{actor_id}",
                actor_id,
                _jsonb({"text": f"target signal {index}"}),
                f"target signal {index}",
                _embedding(f"obs-target-{index}"),
                f"stress-target-{tenant_id}-{target_observation_id}",
                _jsonb(entities),
            ),
            (
                counter_observation_id,
                tenant_id,
                now - timedelta(minutes=5),
                now,
                f"stress:{actor_id}",
                actor_id,
                _jsonb({"text": f"counterevidence signal {index}"}),
                f"counterevidence signal {index}",
                _embedding(f"obs-counter-{index}"),
                f"stress-counter-{tenant_id}-{counter_observation_id}",
                _jsonb(entities),
            ),
            (
                stale_observation_id,
                tenant_id,
                now - timedelta(days=48),
                now,
                f"stress:{actor_id}",
                actor_id,
                _jsonb({"text": f"stale signal {index}"}),
                f"stale signal {index}",
                _embedding(f"obs-stale-{index}"),
                f"stress-stale-{tenant_id}-{stale_observation_id}",
                _jsonb(entities),
            ),
            (
                noise_observation_id,
                tenant_id,
                now - timedelta(hours=3),
                now,
                f"stress:{actor_id}",
                actor_id,
                _jsonb({"text": f"noise signal {index}"}),
                f"noise signal {index}",
                _embedding(f"obs-noise-{index}"),
                f"stress-noise-{tenant_id}-{noise_observation_id}",
                _jsonb([]),
            ),
        ],
    )

    goal_id = uuid7()
    commitment_id = uuid7()
    decision_id = uuid7()
    commitment_label = f"enterprise assurance gate {index:03d}"
    await conn.execute(
        """
        INSERT INTO goals (
          id, tenant_id, title, state, altitude, cached_health,
          cached_health_computed_at, created_by_event_id
        ) VALUES ($1, $2, $3, 'active', 'operational', 'warning', now(), $4)
        """,
        goal_id,
        tenant_id,
        f"Protect {customer_label} renewal",
        target_observation_id,
    )
    await conn.execute(
        """
        INSERT INTO commitments (
          id, tenant_id, title, state, owner_id, due_date,
          ambition_level, priority, external_counterparty_ref,
          created_by_event_id
        ) VALUES (
          $1, $2, $3, 'blocked', $4, $5,
          'base', 3, $6::jsonb, $7
        )
        """,
        commitment_id,
        tenant_id,
        commitment_label,
        actor_id,
        now + timedelta(days=14),
        _jsonb({"customer_resource_id": str(customer_id)}),
        target_observation_id,
    )
    await conn.execute(
        """
        INSERT INTO decisions (
          id, tenant_id, title, decision_text, state, created_by_event_id
        ) VALUES ($1, $2, $3, $4, 'active', $5)
        """,
        decision_id,
        tenant_id,
        f"{customer_label} escalation policy",
        "Route customer-risk escalations through the enterprise readiness lane.",
        target_observation_id,
    )
    await conn.execute(
        """
        INSERT INTO contributes_to (commitment_id, goal_id, is_critical_path)
        VALUES ($1, $2, true)
        ON CONFLICT (commitment_id, goal_id) DO NOTHING
        """,
        commitment_id,
        goal_id,
    )
    await conn.execute(
        """
        INSERT INTO constrained_by (commitment_id, decision_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        commitment_id,
        decision_id,
    )
    await conn.execute(
        """
        INSERT INTO customer_commitments (
          tenant_id, customer_resource_id, commitment_id, served_description
        ) VALUES ($1, $2, $3, $4)
        ON CONFLICT (customer_resource_id, commitment_id) DO NOTHING
        """,
        tenant_id,
        customer_id,
        commitment_id,
        "stress customer delivery promise",
    )
    return Scaffold(
        tenant_id=tenant_id,
        actor_id=actor_id,
        customer_id=customer_id,
        goal_id=goal_id,
        commitment_id=commitment_id,
        decision_id=decision_id,
        target_observation_id=target_observation_id,
        counter_observation_id=counter_observation_id,
        stale_observation_id=stale_observation_id,
        noise_observation_id=noise_observation_id,
        customer_label=customer_label,
        commitment_label=commitment_label,
    )


def _target_natural(
    *,
    archetype: Archetype,
    scaffold: Scaffold,
    marker: str,
    offset: int,
) -> str:
    base = archetype.signal.format(
        customer=scaffold.customer_label,
        commitment=scaffold.commitment_label,
        marker=marker,
    )
    if archetype.operational:
        return (
            f"{marker} operational memory {offset}: {base} "
            "radio Premium support [add $2400] selected=true; "
            "checkbox Expedited review checked=true; "
            "'Escalation owner' value='implementation lead'; "
            "related links visible Runbook, Billing exception, Security packet."
        )
    if archetype.pipeline:
        return (
            f"{marker} pipeline memory {offset}: {base} "
            "pipeline stage chain: Intake (complete) > Security review "
            "(blocked) > Legal packet (waiting) > Production launch "
            "(not started); remaining items count = 3."
        )
    if archetype.situation:
        return (
            f"{marker} situation member memory {offset}: {base} "
            "The pressure combines owner gap, security gate, renewal exposure, "
            "and customer confidence loss."
        )
    return (
        f"{marker} material memory {offset}: {base} "
        "This is current, consequence-bearing evidence for the account."
    )


def _make_model(
    *,
    model_id: UUID | None,
    tenant_id: UUID,
    born_from_event_id: UUID,
    natural: str,
    embedding_key: str,
    proposition: dict[str, Any],
    scope_actors: list[UUID],
    scope_entities: list[dict[str, Any]],
    supporting_event_ids: list[UUID],
    supporting_model_ids: list[UUID] | None = None,
    confidence: float = 0.62,
    domain_tags: tuple[str, ...] = (),
) -> ModelCreate:
    return ModelCreate(
        id=model_id,
        tenant_id=tenant_id,
        born_from_event_id=born_from_event_id,
        proposition=proposition,
        natural=natural,
        embedding=_embedding(embedding_key),
        scope_actors=scope_actors,
        scope_entities=scope_entities,
        scope_temporal={"valid_from": "now", "valid_until": None},
        confidence=confidence,
        confidence_at_assertion=confidence,
        supporting_event_ids=supporting_event_ids,
        supporting_model_ids=list(supporting_model_ids or []),
        domain_tags=list(domain_tags),
    )


def _build_case_models(
    *,
    index: int,
    archetype: Archetype,
    scaffold: Scaffold,
    models_per_case: int,
) -> tuple[list[ModelCreate], StressCase]:
    marker = f"MLSTRESS-{index:03d}-{archetype.key}"
    signal_text = archetype.signal.format(
        customer=scaffold.customer_label,
        commitment=scaffold.commitment_label,
        marker=marker,
    )
    target_embedding_key = f"{marker}:target"
    scope = _scope_entities(scaffold, archetype)
    actor_scope = [scaffold.actor_id] if archetype.use_actor_scope else []
    expected_ids: list[UUID] = []
    member_ids: list[UUID] = []
    drafts: list[ModelCreate] = []

    if not archetype.weak:
        target_count = 16 if archetype.broad else 7
        for offset in range(target_count):
            mid = uuid7()
            expected_ids.append(mid)
            member_ids.append(mid)
            event_id = (
                scaffold.stale_observation_id
                if archetype.key == "fresh_vs_stale" and offset == target_count - 1
                else scaffold.target_observation_id
            )
            time_mode = "past" if event_id == scaffold.stale_observation_id else "current"
            natural = _target_natural(
                archetype=archetype,
                scaffold=scaffold,
                marker=marker,
                offset=offset,
            )
            drafts.append(
                _make_model(
                    model_id=mid,
                    tenant_id=scaffold.tenant_id,
                    born_from_event_id=event_id,
                    natural=natural,
                    embedding_key=target_embedding_key,
                    proposition=_belief_prop(
                        about=f"{scaffold.customer_label} {archetype.key}",
                        nature=natural,
                        claim_role=(
                            "pattern"
                            if archetype.key == "recurring_incident"
                            else "concern"
                        ),
                        domain_tags=archetype.domain_tags,
                        abstraction_level=(
                            "pattern"
                            if archetype.key == "recurring_incident"
                            else "atomic"
                        ),
                        time_mode=time_mode,
                    ),
                    scope_actors=actor_scope,
                    scope_entities=scope if not archetype.broad else [scope[0]],
                    supporting_event_ids=[event_id],
                    supporting_model_ids=[expected_ids[0]] if offset > 0 else [],
                    confidence=0.66,
                    domain_tags=archetype.domain_tags,
                )
            )

        counter_id = uuid7()
        expected_ids.append(counter_id)
        counter_natural = (
            f"{marker} counterevidence memory: a mitigation exists, but it "
            "does not remove the active risk and should not erase the blocker."
        )
        drafts.append(
            _make_model(
                model_id=counter_id,
                tenant_id=scaffold.tenant_id,
                born_from_event_id=scaffold.counter_observation_id,
                natural=counter_natural,
                embedding_key=target_embedding_key,
                proposition=_belief_prop(
                    about=f"{scaffold.customer_label} counterevidence",
                    nature="mitigation exists but active risk remains",
                    claim_role="concern",
                    domain_tags=archetype.domain_tags,
                    polarity="mixed",
                ),
                scope_actors=actor_scope,
                scope_entities=scope,
                supporting_event_ids=[scaffold.counter_observation_id],
                supporting_model_ids=[expected_ids[0]],
                confidence=0.58,
                domain_tags=archetype.domain_tags,
            )
        )

        if archetype.graph:
            hidden_id = uuid7()
            expected_ids.append(hidden_id)
            hidden_natural = (
                f"{marker} graph-only memory: latent invariant B-17 explains "
                "the dependency without using customer surface language."
            )
            drafts.append(
                _make_model(
                    model_id=hidden_id,
                    tenant_id=scaffold.tenant_id,
                    born_from_event_id=scaffold.target_observation_id,
                    natural=hidden_natural,
                    embedding_key=f"{marker}:hidden-graph",
                    proposition=_belief_prop(
                        about="latent invariant B-17",
                        nature=hidden_natural,
                        claim_role="relation",
                        abstraction_level="relationship",
                        domain_tags=archetype.domain_tags,
                        extra={
                            "subject": str(expected_ids[0]),
                            "relation": "explains",
                            "object": "latent invariant B-17",
                        },
                    ),
                    scope_actors=[],
                    scope_entities=[],
                    supporting_event_ids=[scaffold.target_observation_id],
                    supporting_model_ids=[expected_ids[0]],
                    confidence=0.57,
                    domain_tags=archetype.domain_tags,
                )
            )

        if archetype.situation:
            situation_id = uuid7()
            expected_ids.append(situation_id)
            situation_natural = (
                f"{marker} composite situation: owner gap, security gate, "
                "renewal exposure, and confidence loss are mutually reinforcing."
            )
            drafts.append(
                _make_model(
                    model_id=situation_id,
                    tenant_id=scaffold.tenant_id,
                    born_from_event_id=scaffold.target_observation_id,
                    natural=situation_natural,
                    embedding_key=target_embedding_key,
                    proposition=_belief_prop(
                        about=f"{scaffold.customer_label} composite situation",
                        nature=situation_natural,
                        claim_role="situation",
                        abstraction_level="composite",
                        polarity="mixed",
                        domain_tags=archetype.domain_tags,
                        extra={
                            "situation": situation_natural,
                            "member_model_ids": [str(mid) for mid in member_ids[:5]],
                            "relationship_summary": (
                                "Owner, security, and renewal pressures reinforce "
                                "the same operational situation."
                            ),
                            "pressure_type": "execution",
                            "shared_mechanism": "customer delivery readiness",
                            "judgment_change": "treat as one escalated situation",
                        },
                    ),
                    scope_actors=actor_scope,
                    scope_entities=scope,
                    supporting_event_ids=[scaffold.target_observation_id],
                    supporting_model_ids=member_ids[:2],
                    confidence=0.61,
                    domain_tags=archetype.domain_tags,
                )
            )

    noise_customers = [uuid7() for _ in range(160)]
    previous_noise_id: UUID | None = None
    noise_needed = models_per_case - len(drafts)
    for noise_idx in range(noise_needed):
        mid = uuid7()
        same_scope = noise_idx % 37 == 0
        semantic_collision = noise_idx % 251 == 0
        operational_noise = noise_idx % 311 == 0
        relation_noise = noise_idx % 173 == 0
        if same_scope:
            noise_scope = scope
            noise_actors = actor_scope if noise_idx % 2 == 0 else []
            natural = (
                f"{marker} same-scope distractor {noise_idx}: "
                f"{scaffold.customer_label} status note mentions general "
                "queue hygiene but no decisive blocker."
            )
        else:
            noise_customer = noise_customers[noise_idx % len(noise_customers)]
            noise_scope = [
                {"type": "customer_resource", "id": str(noise_customer)},
                {"type": "commitment", "id": str(uuid7())},
            ]
            noise_actors = [scaffold.actor_id] if noise_idx % 19 == 0 else []
            natural = (
                f"Unrelated Model {noise_idx} for NoiseCustomer"
                f"{noise_idx % 160}: billing telemetry, support rotation, "
                "capacity planning, and normal operational chatter."
            )
        if operational_noise:
            natural += (
                " field list Admin portal option order: Basic, Premium, "
                "Enterprise; bottom_option = Enterprise; results count = 12."
            )
        if relation_noise:
            natural += " This relation explains a generic backend queue dependency."
        embedding_key = target_embedding_key if semantic_collision else f"noise-{noise_idx % 997}"
        support_ids: list[UUID] = []
        if previous_noise_id is not None and noise_idx % 503 == 0:
            support_ids.append(previous_noise_id)
        previous_noise_id = mid
        claim_role = "relation" if relation_noise else "fact"
        abstraction = "relationship" if relation_noise else "atomic"
        drafts.append(
            _make_model(
                model_id=mid,
                tenant_id=scaffold.tenant_id,
                born_from_event_id=scaffold.noise_observation_id,
                natural=natural,
                embedding_key=embedding_key,
                proposition=_belief_prop(
                    about=f"noise model {noise_idx}",
                    nature=natural,
                    claim_role=claim_role,
                    abstraction_level=abstraction,
                    domain_tags=("noise", "operations"),
                    polarity="neutral",
                    extra=(
                        {
                            "subject": f"NoiseCustomer{noise_idx % 160}",
                            "relation": "co_occurs_with",
                            "object": "generic queue",
                        }
                        if relation_noise
                        else None
                    ),
                ),
                scope_actors=noise_actors,
                scope_entities=noise_scope,
                supporting_event_ids=[scaffold.noise_observation_id],
                supporting_model_ids=support_ids,
                confidence=0.31 + (noise_idx % 31) / 100,
                domain_tags=("noise", "operations"),
            )
        )

    seed_entities = _scope_entities(scaffold, archetype) if archetype.use_seed_entities else []
    trigger = TriggerContext(
        kind="T1",
        tenant_id=scaffold.tenant_id,
        observation_id=scaffold.target_observation_id,
        seed_entity_ids=seed_entities,
        seed_natural_text=signal_text,
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=actor_scope,
        precomputed_seed_vector=_embedding(target_embedding_key),
        semantic_k=48,
        temporal_window=timedelta(days=30),
    )
    return drafts, StressCase(
        index=index,
        name=f"{index:03d}_{archetype.key}",
        archetype=archetype,
        trigger=trigger,
        expected_model_ids=expected_ids,
        expected_member_ids=member_ids,
        marker=marker,
    )


async def _sidecar_counts(conn: asyncpg.Connection, tenant_id: UUID) -> dict[str, int]:
    rows = await conn.fetch(
        """
        SELECT 'models' AS name, COUNT(*)::int AS count
          FROM models WHERE tenant_id = $1
        UNION ALL
        SELECT 'model_scope_entities', COUNT(*)::int
          FROM model_scope_entities WHERE tenant_id = $1
        UNION ALL
        SELECT 'model_scope_actors', COUNT(*)::int
          FROM model_scope_actors WHERE tenant_id = $1
        UNION ALL
        SELECT 'model_edges', COUNT(*)::int
          FROM model_edges WHERE tenant_id = $1
        UNION ALL
        SELECT 'model_composition_members', COUNT(*)::int
          FROM model_composition_members WHERE tenant_id = $1
        UNION ALL
        SELECT 'audit_events', COUNT(*)::int
          FROM audit_events WHERE tenant_id = $1
        UNION ALL
        SELECT 'state_change_observations', COUNT(*)::int
          FROM observations WHERE tenant_id = $1 AND kind = 'state_change'
        UNION ALL
        SELECT 'relationship_candidates', COUNT(*)::int
          FROM relationship_candidates WHERE tenant_id = $1
        """,
        tenant_id,
    )
    return {str(row["name"]): int(row["count"]) for row in rows}


async def _insert_graph_edges(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    case: StressCase,
) -> None:
    if not case.archetype.graph or len(case.expected_model_ids) < 2:
        return
    rows: list[tuple[Any, ...]] = []
    source = case.expected_model_ids[0]
    for target in case.expected_model_ids[1:4]:
        rows.append(
            (
                uuid7(),
                tenant_id,
                source,
                target,
                "same_issue_as",
                0.88,
                _jsonb({"source": "stress_harness", "case": case.name}),
                "active",
                "manual",
                case.trigger.observation_id,
                0.88,
                [case.trigger.observation_id],
                [source],
                "stress graph edge",
                "accepted",
                1,
            )
        )
    await conn.executemany(
        """
        INSERT INTO model_edges (
            id, tenant_id, source_model_id, target_model_id, edge_kind,
            weight, metadata, status, detected_by, created_by_event_id,
            confidence, evidence_event_ids, evidence_model_ids,
            explanation, review_status, confirmed_count
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7::jsonb, $8, $9, $10,
            $11, $12::uuid[], $13::uuid[],
            $14, $15, $16
        )
        ON CONFLICT ON CONSTRAINT model_edges_unique DO NOTHING
        """,
        rows,
    )


def _path_counts(result: Any) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for action in result.retrieval_actions:
        counts[str(action.path)] += 1
    for pathway_result in result.retrieval_result.pathway_results:
        counts[f"primary_{pathway_result.source_pathway}"] += len(pathway_result.models)
    return dict(sorted(counts.items()))


def _evidence_paths(result: Any) -> list[str]:
    paths: set[str] = set()
    for card in result.evidence_cards:
        paths.update(str(path) for path in card.retrieval_paths)
    return sorted(paths)


def _model_role_counts(models: list[Any]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for model in models:
        role = getattr(model, "claim_role", None)
        if not role:
            prop = getattr(model, "proposition", {}) or {}
            role = prop.get("claim_role") if isinstance(prop, dict) else "unknown"
        counts[str(role or "unknown")] += 1
    return dict(counts.most_common())


def _sage_stage_timing_summary(
    sage_notes: dict[str, Any],
) -> tuple[dict[str, dict[str, int]], dict[str, int], dict[str, int]]:
    by_question: dict[str, dict[str, int]] = {}
    max_by_stage: dict[str, int] = {}
    total_by_stage: Counter[str] = Counter()
    questions = sage_notes.get("questions") or {}
    if not isinstance(questions, dict):
        return by_question, max_by_stage, {}

    for question_id, note in questions.items():
        if not isinstance(note, dict):
            continue
        debug = note.get("debug") or {}
        if not isinstance(debug, dict):
            continue
        raw_timings = debug.get("stage_timings_ms") or {}
        if not isinstance(raw_timings, dict):
            continue
        timings: dict[str, int] = {}
        for stage, raw_value in raw_timings.items():
            try:
                value = int(raw_value)
            except (TypeError, ValueError):
                continue
            timings[str(stage)] = value
            max_by_stage[str(stage)] = max(value, max_by_stage.get(str(stage), 0))
            total_by_stage[str(stage)] += value
        if timings:
            by_question[str(question_id)] = dict(sorted(timings.items()))

    return (
        by_question,
        dict(sorted(max_by_stage.items())),
        dict(sorted(total_by_stage.items())),
    )


def _action_timing_summary(notes: dict[str, Any]) -> tuple[dict[str, int], dict[str, int]]:
    max_by_path: dict[str, int] = {}
    total_by_path: Counter[str] = Counter()
    for raw in notes.get("retrieval_action_timings") or ():
        if not isinstance(raw, dict):
            continue
        path = str(raw.get("path") or "")
        if not path:
            continue
        try:
            elapsed = int(raw.get("elapsed_ms") or 0)
        except (TypeError, ValueError):
            continue
        max_by_path[path] = max(elapsed, max_by_path.get(path, 0))
        total_by_path[path] += elapsed
    return dict(sorted(max_by_path.items())), dict(sorted(total_by_path.items()))


def _evaluate_case(
    *,
    case: StressCase,
    result: Any,
    insert_ms: float,
    retrieval_ms: float,
    sidecars: dict[str, int],
    models_per_case: int,
) -> dict[str, Any]:
    selected_ids = [model.id for model in result.retrieval_result.models]
    selected_set = set(selected_ids)
    selected_rank = {
        model_id: rank
        for rank, model_id in enumerate(selected_ids, start=1)
        if model_id in case.expected_model_ids
    }
    evidence_refs = {card.source_ref for card in result.evidence_cards}
    expected_refs = {f"model:{mid}" for mid in case.expected_model_ids}
    evidence_hits = sorted(expected_refs & evidence_refs)
    final_hits = sorted(str(mid) for mid in case.expected_model_ids if mid in selected_set)
    relevance = result.retrieval_result.notes.get("relevance_gate") or {}
    sage_notes = result.notes.get("sage_reader") or {}
    sage_question_timings, sage_stage_max, sage_stage_total = _sage_stage_timing_summary(
        sage_notes if isinstance(sage_notes, dict) else {}
    )
    action_timing_max, action_timing_total = _action_timing_summary(result.notes or {})
    planning_notes = result.notes.get("question_planning") or []
    issues: list[str] = []
    arch = case.archetype
    if len(final_hits) < arch.expected_final_min:
        issues.append(
            f"expected final hits {len(final_hits)} < {arch.expected_final_min}"
        )
    if len(evidence_hits) < arch.expected_evidence_min:
        issues.append(
            f"expected evidence hits {len(evidence_hits)} < {arch.expected_evidence_min}"
        )
    if arch.expected_selected_max is not None and len(selected_ids) > arch.expected_selected_max:
        issues.append(
            f"selected count {len(selected_ids)} > {arch.expected_selected_max}"
        )
    if sidecars.get("models") != models_per_case:
        issues.append(f"model count {sidecars.get('models')} != {models_per_case}")
    if not arch.weak and not result.questions:
        issues.append("no inquiry questions generated")
    if arch.situation and sidecars.get("model_composition_members", 0) < 2:
        issues.append("situation composition sidecars missing")
    if arch.graph and sidecars.get("model_edges", 0) < 1:
        issues.append("graph/model edge sidecars missing")

    return {
        "case": case.name,
        "index": case.index,
        "archetype": arch.key,
        "marker": case.marker,
        "passed": not issues,
        "issues": issues,
        "models": models_per_case,
        "insert_ms": round(insert_ms, 2),
        "insert_ms_per_model": round(insert_ms / max(1, models_per_case), 4),
        "retrieval_ms": round(retrieval_ms, 2),
        "selected_count": len(selected_ids),
        "evidence_count": len(result.evidence_cards),
        "questions": len(result.questions),
        "answers": len(result.question_answers),
        "retrieval_actions": len(result.retrieval_actions),
        "action_path_counts": _path_counts(result),
        "evidence_paths": _evidence_paths(result),
        "selected_role_counts": _model_role_counts(list(result.retrieval_result.models)),
        "expected_model_count": len(case.expected_model_ids),
        "expected_final_hits": len(final_hits),
        "expected_evidence_hits": len(evidence_hits),
        "best_expected_rank": min(selected_rank.values()) if selected_rank else None,
        "expected_selected_ranks": {
            str(model_id): rank for model_id, rank in selected_rank.items()
        },
        "signal_class": result.notes.get("signal_class"),
        "route": result.route,
        "sufficiency": result.sufficiency.status,
        "candidate_count": relevance.get("candidate_count"),
        "threshold": relevance.get("threshold"),
        "cutoff_reason": relevance.get("cutoff_reason"),
        "dropped_below_threshold": relevance.get("dropped_below_threshold"),
        "dropped_redundant": relevance.get("dropped_redundant"),
        "sage_selected_count": len(sage_notes.get("selected_model_ids") or []),
        "sage_activation_trace_count": sage_notes.get("activation_trace_count"),
        "sage_stage_timings_ms_by_question": sage_question_timings,
        "sage_stage_timings_ms_max": sage_stage_max,
        "sage_stage_timings_ms_total": sage_stage_total,
        "retrieval_action_cache": result.notes.get("retrieval_action_cache") or {},
        "retrieval_action_timings_ms_max": action_timing_max,
        "retrieval_action_timings_ms_total": action_timing_total,
        "planning_modes": [note.get("mode") for note in planning_notes],
        "question_primitives": [question.primitive for question in result.questions],
        "sidecars": sidecars,
    }


async def _run_one_case(
    pool: asyncpg.Pool,
    *,
    index: int,
    models_per_case: int,
    run_topology_on_insert: bool,
    persist_inquiry: bool,
) -> dict[str, Any]:
    archetype = ARCHETYPES[index % len(ARCHETYPES)]
    tenant_id = uuid7()
    now = datetime.now(timezone.utc)
    conn = await pool.acquire()
    tx = conn.transaction()
    await tx.start()
    try:
        await pgvector_pool_init(conn)
        await conn.execute("SET CONSTRAINTS ALL DEFERRED")
        scaffold = await _insert_scaffold(
            conn,
            tenant_id=tenant_id,
            index=index,
            archetype=archetype,
            now=now,
        )
        drafts, case = _build_case_models(
            index=index,
            archetype=archetype,
            scaffold=scaffold,
            models_per_case=models_per_case,
        )
        repo = ModelsRepo(
            pool,
            embedder=None,
            run_topology_on_insert=run_topology_on_insert,
        )
        insert_started = time.monotonic()
        await repo.insert_many(drafts, conn=conn)
        await _insert_graph_edges(conn, tenant_id=tenant_id, case=case)
        insert_ms = (time.monotonic() - insert_started) * 1000.0

        cfg = InquiryConfig(
            max_rounds=1,
            questions_per_round=3,
            evidence_reservoir_limit=300,
            fast_path_evidence_limit=48,
            candidate_model_limit=220,
            result_model_limit=64,
            action_model_budget_limit=48,
            action_observation_budget_limit=32,
            relevance_min_material_models=3,
            temporal_window_days=30,
            semantic_budget=48,
            structural_max_hops=2,
            model_edge_max_hops=2,
            llm_question_planning_enabled=False,
            sage_reader_enabled=True,
            persist=persist_inquiry,
        )
        retrieve_started = time.monotonic()
        result = await run_inquiry_retrieval(
            case.trigger,
            conn,
            embedder=None,
            llm_provider=None,
            mode="deep",
            top_n=220,
            config=cfg,
        )
        retrieval_ms = (time.monotonic() - retrieve_started) * 1000.0
        sidecars = await _sidecar_counts(conn, tenant_id)
        return _evaluate_case(
            case=case,
            result=result,
            insert_ms=insert_ms,
            retrieval_ms=retrieval_ms,
            sidecars=sidecars,
            models_per_case=models_per_case,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "case": f"{index:03d}_{archetype.key}",
            "index": index,
            "archetype": archetype.key,
            "passed": False,
            "issues": [f"{type(exc).__name__}: {exc}"],
            "models": models_per_case,
            "error_type": type(exc).__name__,
        }
    finally:
        await tx.rollback()
        await pool.release(conn)


def _summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    failures = [result for result in results if not result.get("passed")]
    insert_lat = [
        float(result["insert_ms_per_model"])
        for result in results
        if "insert_ms_per_model" in result
    ]
    retrieval_lat = [
        float(result["retrieval_ms"])
        for result in results
        if "retrieval_ms" in result
    ]
    selected = [
        int(result["selected_count"])
        for result in results
        if "selected_count" in result
    ]
    evidence = [
        int(result["evidence_count"])
        for result in results
        if "evidence_count" in result
    ]
    issue_patterns: Counter[str] = Counter()
    for failure in failures:
        for issue in failure.get("issues", []):
            issue_patterns[str(issue).split(":", 1)[0]] += 1
    sage_stage_max: dict[str, int] = {}
    sage_stage_total: Counter[str] = Counter()
    action_timing_max: dict[str, int] = {}
    action_timing_total: Counter[str] = Counter()
    action_cache_hits = 0
    action_cache_misses = 0
    for result in results:
        for stage, raw_value in (result.get("sage_stage_timings_ms_max") or {}).items():
            try:
                value = int(raw_value)
            except (TypeError, ValueError):
                continue
            sage_stage_max[str(stage)] = max(value, sage_stage_max.get(str(stage), 0))
        for stage, raw_value in (result.get("sage_stage_timings_ms_total") or {}).items():
            try:
                sage_stage_total[str(stage)] += int(raw_value)
            except (TypeError, ValueError):
                continue
        for path, raw_value in (result.get("retrieval_action_timings_ms_max") or {}).items():
            try:
                value = int(raw_value)
            except (TypeError, ValueError):
                continue
            action_timing_max[str(path)] = max(value, action_timing_max.get(str(path), 0))
        for path, raw_value in (result.get("retrieval_action_timings_ms_total") or {}).items():
            try:
                action_timing_total[str(path)] += int(raw_value)
            except (TypeError, ValueError):
                continue
        cache = result.get("retrieval_action_cache") or {}
        try:
            action_cache_hits += int(cache.get("hits") or 0)
            action_cache_misses += int(cache.get("misses") or 0)
        except (TypeError, ValueError):
            pass
    return {
        "cases": len(results),
        "passes": len(results) - len(failures),
        "failures": len(failures),
        "failed_cases": [result["case"] for result in failures],
        "archetype_failures": dict(Counter(r["archetype"] for r in failures)),
        "issue_patterns": dict(issue_patterns.most_common(30)),
        "insert_ms_per_model": {
            "min": min(insert_lat) if insert_lat else 0,
            "p50": _percentile(insert_lat, 0.50),
            "p90": _percentile(insert_lat, 0.90),
            "p95": _percentile(insert_lat, 0.95),
            "max": max(insert_lat) if insert_lat else 0,
            "mean": statistics.mean(insert_lat) if insert_lat else 0,
        },
        "retrieval_ms": {
            "min": min(retrieval_lat) if retrieval_lat else 0,
            "p50": _percentile(retrieval_lat, 0.50),
            "p90": _percentile(retrieval_lat, 0.90),
            "p95": _percentile(retrieval_lat, 0.95),
            "max": max(retrieval_lat) if retrieval_lat else 0,
            "mean": statistics.mean(retrieval_lat) if retrieval_lat else 0,
        },
        "selected_count": {
            "min": min(selected) if selected else 0,
            "p50": _percentile([float(x) for x in selected], 0.50),
            "max": max(selected) if selected else 0,
            "mean": statistics.mean(selected) if selected else 0,
        },
        "evidence_count": {
            "min": min(evidence) if evidence else 0,
            "p50": _percentile([float(x) for x in evidence], 0.50),
            "max": max(evidence) if evidence else 0,
            "mean": statistics.mean(evidence) if evidence else 0,
        },
        "sage_stage_timings_ms_max": dict(sorted(sage_stage_max.items())),
        "sage_stage_timings_ms_total": dict(sorted(sage_stage_total.items())),
        "retrieval_action_timings_ms_max": dict(sorted(action_timing_max.items())),
        "retrieval_action_timings_ms_total": dict(sorted(action_timing_total.items())),
        "retrieval_action_cache": {
            "hits": action_cache_hits,
            "misses": action_cache_misses,
        },
    }


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    summary = report["summary"]
    lines = [
        "# Model Layer 100x5000 E2E Stress Report",
        "",
        f"- Run id: `{report['run_id']}`",
        f"- Cases: {summary['cases']}",
        f"- Models per case: {report['models_per_case']}",
        f"- Run topology on insert: {report['run_topology_on_insert']}",
        f"- Persist inquiry: {report['persist_inquiry']}",
        f"- Passes: {summary['passes']}",
        f"- Failures: {summary['failures']}",
        "",
        "## Latency",
        "",
        f"- Insert ms/model: `{summary['insert_ms_per_model']}`",
        f"- Retrieval ms: `{summary['retrieval_ms']}`",
        f"- SAGE stage max ms: `{summary.get('sage_stage_timings_ms_max', {})}`",
        f"- Action path max ms: `{summary.get('retrieval_action_timings_ms_max', {})}`",
        f"- Action cache: `{summary.get('retrieval_action_cache', {})}`",
        "",
        "## Failures",
        "",
    ]
    failures = [result for result in report["results"] if not result.get("passed")]
    if not failures:
        lines.append("None.")
    else:
        for result in failures:
            lines.append(
                f"- `{result['case']}` ({result['archetype']}): "
                + "; ".join(result.get("issues", []))
            )
    lines.extend(
        [
            "",
            "## Cases",
            "",
            "| Case | Archetype | Insert ms/model | Retrieval ms | Selected | Evidence | Hits | Best Rank | Status |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for result in report["results"]:
        lines.append(
            "| {case} | {arch} | {insert} | {ret} | {selected} | {evidence} | "
            "{hits}/{expected} | {rank} | {status} |".format(
                case=result["case"],
                arch=result["archetype"],
                insert=result.get("insert_ms_per_model", ""),
                ret=result.get("retrieval_ms", ""),
                selected=result.get("selected_count", ""),
                evidence=result.get("evidence_count", ""),
                hits=result.get("expected_final_hits", ""),
                expected=result.get("expected_model_count", ""),
                rank=result.get("best_expected_rank", ""),
                status="pass" if result.get("passed") else "issue",
            )
        )
    path.write_text("\n".join(lines) + "\n")


async def run_probe(
    *,
    cases: int,
    start_index: int,
    models_per_case: int,
    database_url: str,
    run_topology_on_insert: bool,
    persist_inquiry: bool,
    pool_max_size: int,
    run_id: str | None,
) -> dict[str, Any]:
    if cases < 1:
        raise ValueError("cases must be >= 1")
    if models_per_case <= 5000:
        raise ValueError("models_per_case must be > 5000")
    pool = await asyncpg.create_pool(
        database_url,
        min_size=1,
        max_size=pool_max_size,
        init=pgvector_pool_init,
    )
    try:
        async with pool.acquire() as conn:
            await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")

        results: list[dict[str, Any]] = []
        started = time.monotonic()
        for index in range(start_index, start_index + cases):
            result = await _run_one_case(
                pool,
                index=index,
                models_per_case=models_per_case,
                run_topology_on_insert=run_topology_on_insert,
                persist_inquiry=persist_inquiry,
            )
            results.append(result)
            elapsed_s = round(time.monotonic() - started, 2)
            print(
                "MODEL_E2E_STRESS_CASE "
                + json.dumps(
                    {
                        "case_number": index - start_index + 1,
                        "case_count": cases,
                        "elapsed_s": elapsed_s,
                        **result,
                    },
                    sort_keys=True,
                    default=str,
                ),
                flush=True,
            )

        report = {
            "run_id": run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
            "cases": cases,
            "start_index": start_index,
            "models_per_case": models_per_case,
            "run_topology_on_insert": run_topology_on_insert,
            "persist_inquiry": persist_inquiry,
            "elapsed_ms": round((time.monotonic() - started) * 1000.0, 2),
            "results": results,
        }
        report["summary"] = _summarize(results)
        report_dir = REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"
        report_dir.mkdir(parents=True, exist_ok=True)
        stem = f"model-layer-100x5000-stress-{report['run_id']}"
        json_path = report_dir / f"{stem}.json"
        md_path = report_dir / f"{stem}.md"
        json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str))
        _write_markdown(report, md_path)
        report["report_path"] = str(json_path)
        report["markdown_path"] = str(md_path)
        print(
            "MODEL_E2E_STRESS_SUMMARY "
            + json.dumps(report["summary"], sort_keys=True, default=str),
            flush=True,
        )
        print(
            "MODEL_E2E_STRESS_REPORT "
            + json.dumps(
                {"json": str(json_path), "markdown": str(md_path)},
                sort_keys=True,
            ),
            flush=True,
        )
        return report
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", type=int, default=100)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--models-per-case", type=int, default=5200)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL") or LOCAL_DATABASE_URL,
    )
    parser.add_argument("--pool-max-size", type=int, default=2)
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--run-topology-on-insert",
        action="store_true",
        help="exercise synchronous latent topology generation on every Model insert",
    )
    parser.add_argument(
        "--persist-inquiry",
        action="store_true",
        help="persist inquiry telemetry inside the rolled-back case transaction",
    )
    args = parser.parse_args()
    report = asyncio.run(
        run_probe(
            cases=args.cases,
            start_index=args.start_index,
            models_per_case=args.models_per_case,
            database_url=args.database_url,
            run_topology_on_insert=args.run_topology_on_insert,
            persist_inquiry=args.persist_inquiry,
            pool_max_size=args.pool_max_size,
            run_id=args.run_id,
        )
    )
    return 0 if report["summary"]["failures"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
