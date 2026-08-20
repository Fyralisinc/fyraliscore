"""services/ingest/ingestion/scope_resolution.py — shared scope resolvers.

Entity + actor resolution factored out of ``core.py`` so both the ingest hot
path and the document-memory summary worker (Layer 2 re-resolution, see
``docs/plans/document-memory-substrate.md`` §4.3) can resolve scope from the
same blessed helpers.

``core.py`` resolves over an ``ObservationDraft.content_text`` at ingest time;
for large documents that text is a placeholder, so the summary worker re-runs
the *same* resolution over the structured summary text (concatenated
decisions/commitments/risks) and produces a richer ``entities_mentioned`` plus
the ``scope_entities`` / ``scope_actors`` the Models substrate needs.

The functions here are deliberately driver-agnostic — they take raw text and
the repos, never an ``ObservationDraft`` — so the worker can reuse them without
importing the ingest draft machinery.
"""
from __future__ import annotations

import json
import re
from typing import Any
from uuid import UUID

from lib.shared.errors import ValidationError
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo, normalize_phrase


# Phrase extraction: a tiny tokenizer that yields 1- to 3-word runs of
# alphanumerics + hyphens. Not linguistic — the fast path does exact
# lookups against known aliases, so precision > recall here. The Wave 2-B
# entity resolver worker handles the long tail with LLM help.
_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9\-]{1,}")


def candidate_phrases(text: str, *, max_phrases: int = 50) -> list[str]:
    """Generate candidate phrases (1-, 2-, and 3-grams) for fast-path
    entity lookup.

    - Only alpha starters; skips tokens with no letters to drop stray
      numeric / timestamp-like chunks.
    - Deterministic, case-preserving order; normalization happens
      inside EntityAliasRepo.fast_path_resolve.
    - Capped at `max_phrases` so pathological long text doesn't
      explode the fan-out. 50 is generous for typical Slack chatter.
    """
    if not text:
        return []
    tokens = [m.group(0) for m in _TOKEN_RE.finditer(text)]
    phrases: list[str] = []
    seen: set[str] = set()
    for i, tok in enumerate(tokens):
        for n in (1, 2, 3):
            if i + n > len(tokens):
                break
            gram = " ".join(tokens[i : i + n])
            norm = normalize_phrase(gram)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            phrases.append(gram)
            if len(phrases) >= max_phrases:
                return phrases
    return phrases


def looks_like_entity(phrase: str) -> bool:
    """Heuristic: phrase has a capital letter or contains a hyphen.

    This intentionally errs on the side of enqueueing fewer common
    words for the resolver worker. Wave 2-B can refine the rule or
    move to a POS tagger — the queue key is stable either way.
    """
    if not phrase:
        return False
    if "-" in phrase:
        return True
    return any(c.isupper() for c in phrase)


async def resolve_actor_ref(
    source_actor_ref: str | None,
    source_channel: str,
    actor_repo: ActorRepo | None,
    tenant_id: UUID,
    connector_installation_id: UUID | None = None,
) -> tuple[UUID | None, str | None]:
    """Resolve a ``<channel>:<ref>`` actor reference to an actor UUID.

    Returns ``(actor_id, unresolved_actor_ref)``: at most one is non-None.
    A missing repo or ref yields ``(None, None)``. Mirrors ``core.py``'s
    ingest-time actor resolution so the document-memory worker resolves
    actors through the same blessed path (never inventing IDs).
    """
    if not source_actor_ref or actor_repo is None:
        return None, None
    ref = source_actor_ref
    if ":" not in ref:
        ref = f"{source_channel}:{ref}"
    try:
        resolved_actor_id = await actor_repo.resolve_by_source_actor_ref(
            ref,
            tenant_id,
            connector_installation_id,
        )
    except ValidationError:
        resolved_actor_id = None
    return (
        (resolved_actor_id, None)
        if resolved_actor_id is not None
        else (None, ref)
    )


def _display_name_key(name: str) -> str:
    """Casefold + collapse internal whitespace for display-name matching."""
    return " ".join(name.split()).casefold()


async def resolve_owner_actor(
    who: str | None,
    source_channel: str,
    tenant_id: UUID,
    actor_repo: ActorRepo | None,
) -> tuple[UUID | None, str | None]:
    """Resolve an action-item owner (e.g. ``"Priya"``) to an actor UUID.

    A summarizer ``who`` is a bare display string, not a ``<channel>:<ref>``
    identity, so the channel-prefixed source-ref path (``resolve_actor_ref``)
    almost never hits an ``actor_identity_mappings`` row. This widens the
    resolution: when the source-ref path misses, try matching ``who`` against an
    active actor's ``display_name`` (case-insensitive, whitespace-normalized) for
    this tenant — the only additional, **read-only**, never-inventing way a bare
    name can become a real actor UUID.

    Returns ``(actor_id, unresolved_owner)``: at most one is non-None. The
    display-name match is intentionally exact-after-normalization and bails on
    ambiguity (two active actors sharing the name) so we never guess which
    "Priya" was meant — an ambiguous or unmatched name stays text-only (§8
    scope-actor existence: only resolved UUIDs may enter ``scope_actors``).
    """
    if not who or not who.strip() or actor_repo is None:
        return None, (who.strip() if who and who.strip() else None)

    # 1) Existing path: treat the bare name as a source ref (works when the
    #    summarizer happens to emit a channel-qualified handle).
    resolved, unresolved = await resolve_actor_ref(
        who,
        source_channel,
        actor_repo,
        tenant_id,
    )
    if resolved is not None:
        return resolved, None

    # 2) Display-name fallback (the actual fix). Read-only scan of this tenant's
    #    active actors; match on normalized display_name. Never invents an ID.
    key = _display_name_key(who)
    try:
        actors = await actor_repo.list_active_actors(tenant_id)
    except Exception:  # noqa: BLE001 — resolution must never raise into the worker
        return None, who.strip()
    matches = [
        a for a in actors
        if getattr(a, "display_name", None)
        and _display_name_key(a.display_name) == key
    ]
    if len(matches) == 1:
        return matches[0].id, None
    # 0 matches → unresolved; >1 → ambiguous, refuse to guess (text-only).
    return None, who.strip()


async def resolve_entities_in_text(
    text: str,
    alias_repo: EntityAliasRepo | None,
    tenant_id: UUID,
    *,
    seed_entities: list[dict[str, Any]] | None = None,
    seed_unresolved: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Fast-path entity resolution over arbitrary ``text``.

    Returns ``(entities_mentioned, unresolved_phrases)``.

    ``seed_entities`` / ``seed_unresolved`` carry forward any refs already
    known (e.g. an observation's existing ``entities_mentioned``) so the
    document-memory re-resolution is *additive* — it never drops the refs the
    ingest path already established, it only enriches them from the richer
    structured summary text.
    """
    entities_mentioned: list[dict[str, Any]] = list(seed_entities or [])
    unresolved_phrases: list[str] = list(seed_unresolved or [])
    if alias_repo is None or not text:
        return entities_mentioned, unresolved_phrases

    seen_ref_keys = {json.dumps(e, sort_keys=True) for e in entities_mentioned}
    phrases = candidate_phrases(text)
    resolved_by_norm = await alias_repo.fast_path_resolve_many(phrases, tenant_id)
    for phrase in phrases:
        ref = resolved_by_norm.get(normalize_phrase(phrase))
        if ref is not None:
            key = json.dumps(ref, sort_keys=True)
            if key not in seen_ref_keys:
                seen_ref_keys.add(key)
                entities_mentioned.append(ref)
        elif looks_like_entity(phrase) and phrase not in unresolved_phrases:
            unresolved_phrases.append(phrase)
    return entities_mentioned, unresolved_phrases


__all__ = [
    "candidate_phrases",
    "looks_like_entity",
    "resolve_actor_ref",
    "resolve_owner_actor",
    "resolve_entities_in_text",
]
