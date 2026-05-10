"""Synthetic fixture builders.

Each helper directly inserts a row via raw SQL — bypasses repos so we
keep the harness simple and don't accidentally trigger production-side
emit_state_change side effects we don't want for retrieval tests.

Per-tenant isolation: every case uses a fresh tenant_id, so cases run
in parallel against the same DB without colliding (all production
queries filter by tenant_id).
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg
from pgvector.asyncpg import register_vector

from lib.shared.ids import uuid7


async def _ensure_vector(conn: asyncpg.Connection) -> None:
    """Idempotently register pgvector codec on this connection.

    Some retrieval pathways call register_vector on connections checked
    out of the pool, which mutates the connection's codec map. If a
    fixture then writes via `'[…]'::vector` SQL cast on the same pooled
    connection, asyncpg's codec interprets the string as float[] and
    fails. Registering up-front and writing as a Python list avoids
    that race entirely.
    """
    try:
        await register_vector(conn)
    except Exception:
        pass

EMBED_DIM = 768


def deterministic_vector(seed: str) -> list[float]:
    """Build a deterministic, L2-normalized 768-dim vector from a string seed.

    Cosine distance between two such vectors with the *same* seed is 0,
    so we use seed strings to control retrieval ranking in pathway B.
    """
    # FNV-ish hash → seed numpy/random LCG; output normalized.
    import random
    rng = random.Random(hash(seed) & 0xFFFFFFFF)
    raw = [rng.gauss(0.0, 1.0) for _ in range(EMBED_DIM)]
    norm = math.sqrt(sum(x * x for x in raw)) or 1.0
    return [x / norm for x in raw]


def isoplus(seconds: float = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


# =====================================================================
# Tenant + actors
# =====================================================================


async def make_tenant(conn: asyncpg.Connection) -> UUID:
    """Create a fresh tenant id (no row required — tenants are implicit)."""
    return uuid7()


async def make_actor(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    display_name: str = "Test Actor",
    actor_type: str = "human",
) -> UUID:
    aid = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, $3, $4, 'active')
        """,
        aid, tenant_id, actor_type, display_name,
    )
    return aid


# =====================================================================
# Observations
# =====================================================================


async def make_observation(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    occurred_at: datetime | None = None,
    kind: str = "signal",
    content_text: str = "synthetic observation",
    actor_id: UUID | None = None,
    entities_mentioned: list[dict] | None = None,
    trust_tier: str = "authoritative",
    embed_seed: str | None = None,
) -> UUID:
    await _ensure_vector(conn)
    obs_id = uuid7()
    occurred_at = occurred_at or isoplus(0)
    embed = deterministic_vector(embed_seed or content_text) if embed_seed or content_text else None
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, ingested_at, kind,
            source_channel, source_actor_ref, actor_id,
            content, content_text,
            embedding, embedding_pending,
            trust_tier, external_id, cause_id, entities_mentioned
        ) VALUES (
            $1, $2, $3, $3, $4,
            'harness:synthetic', NULL, $5,
            $6::jsonb, $7,
            $8, FALSE,
            $9, NULL, NULL, $10::jsonb
        )
        """,
        obs_id, tenant_id, occurred_at, kind,
        actor_id,
        json.dumps({"content_text": content_text}),
        content_text,
        embed,
        trust_tier,
        json.dumps(entities_mentioned or []),
    )
    return obs_id


# =====================================================================
# Models — direct raw insert, bypasses repo's nine-step pipeline so we
# can place rows in arbitrary states for retrieval tests.
# =====================================================================


async def make_model(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    born_from_event_id: UUID | None = None,
    proposition: dict | None = None,
    natural: str = "Synthetic model",
    confidence: float = 0.6,
    activation: float = 0.5,
    scope_actors: list[UUID] | None = None,
    scope_entities: list[dict] | None = None,
    scope_temporal: dict | None = None,
    falsifier: dict | None = None,
    status: str = "active",
    archive_reason: str | None = None,
    embed_seed: str | None = None,
    evaluate_at: datetime | None = None,
    last_retrieved_at: datetime | None = None,
    supporting_event_ids: list[UUID] | None = None,
    supporting_model_ids: list[UUID] | None = None,
) -> UUID:
    await _ensure_vector(conn)
    mid = uuid7()
    if born_from_event_id is None:
        # Need a valid observation as parent
        born_from_event_id = await make_observation(
            conn, tenant_id, content_text="seed for " + natural,
        )
    proposition = proposition or {"kind": "state", "subject": natural}
    scope_actors = scope_actors or []
    scope_entities = scope_entities or []
    scope_temporal = scope_temporal or {
        "valid_from": isoplus(0).isoformat(),
        "valid_until": None,
    }
    embed = deterministic_vector(embed_seed or natural)

    await conn.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, activation, falsifier,
            signal_readings, reading_contestable,
            supporting_event_ids, supporting_model_ids, evidential_weight,
            status, archived_at, archive_reason,
            evaluate_at, resolution_criteria, contributing_models,
            visible_to_subjects, confidence_at_assertion, last_retrieved_at
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            $7::uuid[], $8::jsonb, $9::jsonb,
            $10, $11, $12::jsonb,
            '[]'::jsonb, TRUE,
            $13::uuid[], $14::uuid[], 0.5,
            $15, $16, $17,
            $18, NULL, '{}'::uuid[],
            TRUE, $10, $19
        )
        """,
        mid, tenant_id, born_from_event_id,
        json.dumps(proposition), natural, embed,
        scope_actors, json.dumps(scope_entities), json.dumps(scope_temporal),
        confidence, activation,
        json.dumps(falsifier) if falsifier is not None else None,
        supporting_event_ids or [], supporting_model_ids or [],
        status,
        isoplus(0) if status != "active" else None,
        archive_reason,
        evaluate_at,
        last_retrieved_at,
    )
    return mid


# =====================================================================
# Acts
# =====================================================================


async def make_goal(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    title: str = "Synthetic Goal",
    state: str = "active",
    parent_goal_id: UUID | None = None,
    cached_health: str = "healthy",
    created_by_event_id: UUID | None = None,
) -> UUID:
    gid = uuid7()
    if created_by_event_id is None:
        created_by_event_id = await make_observation(conn, tenant_id, content_text=f"goal seed: {title}")
    await conn.execute(
        """
        INSERT INTO goals (id, tenant_id, title, state, parent_goal_id,
                           cached_health, created_by_event_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        gid, tenant_id, title, state, parent_goal_id, cached_health,
        created_by_event_id,
    )
    return gid


async def make_commitment(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    title: str = "Synthetic Commitment",
    state: str = "active",
    owner_id: UUID | None = None,
    due_date: datetime | None = None,
    external_counterparty_ref: dict | None = None,
    created_by_event_id: UUID | None = None,
) -> UUID:
    cid = uuid7()
    if created_by_event_id is None:
        created_by_event_id = await make_observation(conn, tenant_id, content_text=f"commit seed: {title}")
    await conn.execute(
        """
        INSERT INTO commitments (
            id, tenant_id, title, state, owner_id, due_date,
            external_counterparty_ref, created_by_event_id
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
        """,
        cid, tenant_id, title, state, owner_id, due_date,
        json.dumps(external_counterparty_ref) if external_counterparty_ref else None,
        created_by_event_id,
    )
    return cid


async def add_contributor(
    conn: asyncpg.Connection,
    *,
    commitment_id: UUID,
    actor_id: UUID,
    role: str = "contributor",
) -> None:
    await conn.execute(
        """
        INSERT INTO commitment_contributors (commitment_id, actor_id, role)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        """,
        commitment_id, actor_id, role,
    )


async def add_contributes_to(
    conn: asyncpg.Connection,
    *,
    commitment_id: UUID,
    goal_id: UUID,
    is_critical_path: bool = False,
) -> None:
    await conn.execute(
        """
        INSERT INTO contributes_to (commitment_id, goal_id, is_critical_path)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        """,
        commitment_id, goal_id, is_critical_path,
    )


async def add_depends_on(
    conn: asyncpg.Connection,
    *,
    dependent: UUID,
    dependency: UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO depends_on (dependent_commitment_id, dependency_commitment_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        dependent, dependency,
    )


async def make_decision(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    title: str = "Synthetic Decision",
    state: str = "active",
    decision_text: str = "synthetic decision text",
    created_by_event_id: UUID | None = None,
) -> UUID:
    did = uuid7()
    if created_by_event_id is None:
        created_by_event_id = await make_observation(conn, tenant_id, content_text=f"decision seed: {title}")
    await conn.execute(
        """
        INSERT INTO decisions (id, tenant_id, title, decision_text, state, created_by_event_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        did, tenant_id, title, decision_text, state, created_by_event_id,
    )
    return did


async def add_constrained_by(
    conn: asyncpg.Connection,
    *,
    commitment_id: UUID,
    decision_id: UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO constrained_by (commitment_id, decision_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        commitment_id, decision_id,
    )
