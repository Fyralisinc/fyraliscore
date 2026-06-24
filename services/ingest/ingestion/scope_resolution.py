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
        resolved_actor_id = await actor_repo.resolve_by_source_actor_ref(ref)
    except ValidationError:
        resolved_actor_id = None
    return (
        (resolved_actor_id, None)
        if resolved_actor_id is not None
        else (None, ref)
    )


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
    "resolve_entities_in_text",
]
