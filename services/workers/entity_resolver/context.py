"""services/workers/entity_resolver/context.py — context bundle builder.

For each unresolved phrase on an Observation, the resolver worker
needs "what did we know around the same time in the same channel,
and what canonical entities are already in scope." This module
assembles that bundle on demand.

Inputs:
    - observation_id (UUID)
    - phrase (str)
    - tenant_id (UUID)
    - asyncpg.Pool (or Connection)

Outputs: `ResolverContext` — a small dataclass with:
    - recent_observations: list of 20 most recent observations in
      the same source_channel (occurred before the current one)
    - scoped_models: up to 10 recent Models whose scope_entities
      overlaps any candidate entity shape inferred from the phrase
    - recent_aliases: list of aliases already seen for the phrase
      (useful to LLM as "we've seen this before")

The context is intentionally small — LLM budget is 2K tokens per
spec §15 "Context budget".
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg


_DEFAULT_RECENT_OBS = 20
_DEFAULT_SCOPED_MODELS = 10


@dataclass
class RecentObservation:
    id: UUID
    occurred_at: Any   # datetime
    source_channel: str
    content_text: str
    entities_mentioned: list[dict[str, Any]]


@dataclass
class ScopedModel:
    id: UUID
    natural: str
    confidence: float
    scope_entities: list[dict[str, Any]]


@dataclass
class RecentAlias:
    alias_text: str
    resolved_entity_ref: dict[str, Any]
    confidence: float


@dataclass
class ResolverContext:
    observation_id: UUID
    phrase: str
    tenant_id: UUID
    recent_observations: list[RecentObservation] = field(default_factory=list)
    scoped_models: list[ScopedModel] = field(default_factory=list)
    recent_aliases: list[RecentAlias] = field(default_factory=list)
    source_channel: str = ""
    content_text: str = ""

    def to_prompt_blob(self) -> str:
        """Compact JSON-ish rendering for the LLM user message."""
        out: dict[str, Any] = {
            "phrase": self.phrase,
            "source_channel": self.source_channel,
            "source_content_excerpt": self.content_text[:500],
            "recent_observations": [
                {
                    "channel": o.source_channel,
                    "text": o.content_text[:200],
                    "entities": o.entities_mentioned[:5],
                }
                for o in self.recent_observations[:10]
            ],
            "candidate_entities_in_context": [
                {
                    "natural": m.natural,
                    "confidence": m.confidence,
                    "scope": m.scope_entities[:3],
                }
                for m in self.scoped_models[:5]
            ],
            "prior_alias_matches": [
                {
                    "alias": a.alias_text,
                    "entity_ref": a.resolved_entity_ref,
                }
                for a in self.recent_aliases
            ],
        }
        return json.dumps(out, default=str, separators=(",", ":"))


async def build_context(
    *,
    pool: asyncpg.Pool | asyncpg.Connection,
    tenant_id: UUID,
    observation_id: UUID,
    phrase: str,
    recent_n: int = _DEFAULT_RECENT_OBS,
    scoped_models_n: int = _DEFAULT_SCOPED_MODELS,
) -> ResolverContext:
    """Assemble the context bundle used by the resolver LLM prompt.

    Accepts either a pool or a connection (tests pin one connection
    per test transaction — see the observations conftest pattern).
    """
    conn_owned: asyncpg.Connection | None = None
    if isinstance(pool, asyncpg.Connection):
        conn = pool
    else:
        conn_owned = await pool.acquire()
        conn = conn_owned

    try:
        # Source observation — need source_channel, content_text,
        # occurred_at for the context-window query.
        src = await conn.fetchrow(
            """
            SELECT id, source_channel, content_text, occurred_at
            FROM observations
            WHERE id = $1 AND tenant_id = $2
            """,
            observation_id,
            tenant_id,
        )
        if src is None:
            return ResolverContext(
                observation_id=observation_id,
                phrase=phrase,
                tenant_id=tenant_id,
            )

        source_channel = src["source_channel"]
        content_text = src["content_text"]
        occurred_at = src["occurred_at"]

        # 20 most recent observations in the same channel BEFORE this
        # one, same tenant. We exclude the current observation itself.
        recent_rows = await conn.fetch(
            """
            SELECT id, occurred_at, source_channel, content_text,
                   entities_mentioned
            FROM observations
            WHERE tenant_id = $1
              AND source_channel = $2
              AND occurred_at <= $3
              AND id <> $4
            ORDER BY occurred_at DESC
            LIMIT $5
            """,
            tenant_id,
            source_channel,
            occurred_at,
            observation_id,
            recent_n,
        )
        recent_observations = [
            RecentObservation(
                id=r["id"],
                occurred_at=r["occurred_at"],
                source_channel=r["source_channel"],
                content_text=r["content_text"],
                entities_mentioned=_parse_jsonb(r["entities_mentioned"]) or [],
            )
            for r in recent_rows
        ]

        # Scoped models — any active model whose scope_entities has
        # an element matching the "type" guessed from the phrase. We
        # skip the guess for now and return the most recently
        # retrieved active models in the tenant as a generic context
        # bundle. The resolver LLM can winnow from there.
        # `natural` is a reserved SQL keyword; quote it on read just
        # like the foundation migration (see BUILD-LOG Wave 0).
        model_rows = await conn.fetch(
            """
            SELECT id, "natural", confidence, scope_entities
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
            ORDER BY COALESCE(last_retrieved_at, created_at) DESC
            LIMIT $2
            """,
            tenant_id,
            scoped_models_n,
        )
        scoped_models = [
            ScopedModel(
                id=r["id"],
                natural=r["natural"],
                confidence=float(r["confidence"]),
                scope_entities=_parse_jsonb(r["scope_entities"]) or [],
            )
            for r in model_rows
        ]

        # Prior aliases for the exact phrase (if any).
        alias_rows = await conn.fetch(
            """
            SELECT alias_text, resolved_entity_ref, confidence
            FROM entity_aliases
            WHERE tenant_id = $1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g')
                  = regexp_replace(lower($2::text), '\\s+', ' ', 'g')
            ORDER BY confidence DESC
            LIMIT 5
            """,
            tenant_id,
            phrase,
        )
        recent_aliases = [
            RecentAlias(
                alias_text=r["alias_text"],
                resolved_entity_ref=_parse_jsonb(r["resolved_entity_ref"]) or {},
                confidence=float(r["confidence"]),
            )
            for r in alias_rows
        ]

        return ResolverContext(
            observation_id=observation_id,
            phrase=phrase,
            tenant_id=tenant_id,
            source_channel=source_channel,
            content_text=content_text,
            recent_observations=recent_observations,
            scoped_models=scoped_models,
            recent_aliases=recent_aliases,
        )
    finally:
        if conn_owned is not None:
            await pool.release(conn_owned)


def _parse_jsonb(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode()
    if isinstance(v, str):
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return None
    return v


__all__ = [
    "RecentObservation",
    "ScopedModel",
    "RecentAlias",
    "ResolverContext",
    "build_context",
]
