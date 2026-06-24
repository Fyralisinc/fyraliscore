"""Document-memory Layer-2 scope re-resolution (Phase 1, Think-mediated).

When a large document is summarized, the structured extraction
(decisions/commitments/risks) is far richer than the placeholder
``content_text`` the ingest path resolved entities over. This module re-resolves
entity/actor scope over that structured text — through the *same* blessed
resolvers ``core.py`` uses (``services.ingest.ingestion.scope_resolution``) — so
the document becomes a first-class, scoped memory object.

Per the ratified Option A (Think-mediated minting, see
``docs/plans/document-memory-substrate.md`` §4.1), this module does NOT create
Models. It (1) produces a richer ``entities_mentioned`` for the observation and
(2) builds ``scope_entities`` / ``scope_actors`` + the structured payload that
the worker carries on the *enriched T1 trigger*; Think then distills the
document into Models via its sanctioned path (§4.2–§4.6).

Everything here is gated by ``INGEST_DOC_MEMORY_ENABLED`` (default OFF) and is
strictly failure-isolated by the worker: a re-resolution error must never fail
summarization (§8 — failure isolation). Scope-actor existence (§8) is honored by
construction: only actor UUIDs that *resolved* through ``ActorRepo`` enter
``scope_actors``; unresolved owners stay as text in the structured payload.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.ingest.ingestion.scope_resolution import (
    resolve_actor_ref,
    resolve_entities_in_text,
)


log = logging.getLogger(__name__)


# Structured fields that name people / customers / systems worth resolving.
# `summary` and `key_points` are deliberately excluded from the resolution text:
# they are prose recap (noise-gating, §8) — decisions/commitments/risks are the
# sharp, scope-bearing items the document-memory Models are built from.
_SCOPE_FIELDS = ("decisions", "action_items", "risks")


def doc_memory_enabled() -> bool:
    """Feature flag gating ALL of Layer 2 (default OFF).

    Mirrors the repo's other ingestion boolean knobs: truthy values are
    1/true/yes/on (case-insensitive); anything else (including unset) is OFF.
    """
    raw = os.environ.get("INGEST_DOC_MEMORY_ENABLED", "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class DocMemoryScope:
    """Re-resolved scope + structured payload for the enriched T1 trigger."""

    entities_mentioned: list[dict[str, Any]] = field(default_factory=list)
    scope_entities: list[dict[str, Any]] = field(default_factory=list)
    scope_actors: list[str] = field(default_factory=list)
    unresolved_actor_refs: list[str] = field(default_factory=list)
    structured: dict[str, Any] = field(default_factory=dict)


def _as_text(item: Any) -> str:
    """Flatten a structured field item (str or {who?, what, due?}) to text."""
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        parts = [
            str(item[key])
            for key in ("who", "what")
            if isinstance(item.get(key), str) and item[key].strip()
        ]
        return " ".join(parts)
    return ""


def structured_scope_text(structured: dict[str, Any]) -> str:
    """Concatenate the scope-bearing structured fields into one resolution blob.

    Joins decisions/commitments/risks (NOT the prose summary/key_points) so the
    fast-path resolver sees the sharp entity/actor mentions, not recap noise.
    """
    pieces: list[str] = []
    for key in _SCOPE_FIELDS:
        values = structured.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            text = _as_text(item).strip()
            if text:
                pieces.append(text)
    return "\n".join(pieces)


def _action_item_owner_refs(structured: dict[str, Any]) -> list[str]:
    """Owner phrases from action_items, as `<channel>:<ref>` actor references.

    The summarizer's `who` is a display string (e.g. "Priya"), not a channel
    ref, so it rarely resolves to a UUID. We still pass it through the actor
    resolver (it may match an alias-backed mapping); whatever does not resolve
    stays as unresolved text — never invented into scope_actors (§8).
    """
    refs: list[str] = []
    items = structured.get("action_items")
    if not isinstance(items, list):
        return refs
    for item in items:
        if isinstance(item, dict):
            who = item.get("who")
            if isinstance(who, str) and who.strip() and who.strip() not in refs:
                refs.append(who.strip())
    return refs


async def resolve_document_scope(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    source_channel: str,
    structured: dict[str, Any],
    existing_entities: list[dict[str, Any]] | None = None,
    actor_id: UUID | None = None,
) -> DocMemoryScope:
    """Re-resolve entity/actor scope over the structured summary text.

    - ``entities_mentioned`` is the observation's existing refs PLUS any new
      refs resolved from the structured text (additive — never drops prior
      resolution).
    - ``scope_entities`` is the resolved refs in ``{"type","id"}`` shape that
      Think can put directly on a Model's ``scope_entities``.
    - ``scope_actors`` carries the triggering observation's ``actor_id`` (if
      any) plus any action-item owners that resolved to a real actor UUID.
      Unresolved owners go to ``unresolved_actor_refs`` as text.
    """
    text = structured_scope_text(structured)
    alias_repo = EntityAliasRepo(pool)
    entities_mentioned, _unresolved_phrases = await resolve_entities_in_text(
        text,
        alias_repo,
        tenant_id,
        seed_entities=list(existing_entities or []),
    )

    # scope_entities: resolved refs only, in {type,id} shape. The fast-path
    # resolver returns refs shaped like {"type": "...", "id": "..."}; keep only
    # those that carry both keys (never invent).
    scope_entities: list[dict[str, Any]] = []
    seen_scope: set[tuple[str, str]] = set()
    for ref in entities_mentioned:
        if not isinstance(ref, dict):
            continue
        etype = ref.get("type")
        eid = ref.get("id")
        if not isinstance(etype, str) or eid is None:
            continue
        key = (etype, str(eid))
        if key in seen_scope:
            continue
        seen_scope.add(key)
        scope_entities.append({"type": etype, "id": str(eid)})

    # scope_actors: resolved UUIDs ONLY (§8 scope-actor existence). Start with
    # the observation's own actor, then try to resolve action-item owners.
    actor_repo = ActorRepo(pool)
    scope_actors: list[str] = []
    if actor_id is not None:
        scope_actors.append(str(actor_id))
    unresolved_actor_refs: list[str] = []
    for owner_ref in _action_item_owner_refs(structured):
        resolved, unresolved = await resolve_actor_ref(
            owner_ref, source_channel, actor_repo
        )
        if resolved is not None and str(resolved) not in scope_actors:
            scope_actors.append(str(resolved))
        elif unresolved is not None and owner_ref not in unresolved_actor_refs:
            # Keep the bare owner display string (not the channel-prefixed ref)
            # so Think can still surface "Priya" in the natural text.
            unresolved_actor_refs.append(owner_ref)

    return DocMemoryScope(
        entities_mentioned=entities_mentioned,
        scope_entities=scope_entities,
        scope_actors=scope_actors,
        unresolved_actor_refs=unresolved_actor_refs,
        structured=structured,
    )


__all__ = [
    "DocMemoryScope",
    "doc_memory_enabled",
    "resolve_document_scope",
    "structured_scope_text",
]
