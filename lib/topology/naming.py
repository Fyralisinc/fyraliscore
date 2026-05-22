"""
lib/topology/naming.py — heuristic neighborhood-naming.

Pure functions over in-memory Model summaries. Produces a short
human-readable signature for a neighborhood like:

  "state x recommendation @ commitment / Sarah"
  "prediction @ pricing-v2 / Carmen / Bob"
  "concern @ Globex"

The signature is deterministic for a given member set and is good
enough for the T6 prompt + the CEO view's neighborhood label. The LLM
is allowed to overwrite it via T6 reasoning; this naming is the
fallback so a freshly-emerged neighborhood is never anonymous.

Why not LLM-only naming
-----------------------

Letting the LLM name every neighborhood the first time it sees one
ties topology recompute latency to LLM availability and cost. The
heuristic guarantees a name within the same transaction the
neighborhood is materialized; T6 (LLM-driven) refines it.

Two surface-area decisions
--------------------------

1. The signature is a SHORT string (≤120 chars), not structured
   metadata. The naming SQL column is `named_signature TEXT` for
   exactly this reason — easy to read in admin tools, cheap to
   index/search, and obvious in logs.
2. We name from `proposition_kind` + most-frequent scope members
   (entity types and actor names, when available). We do NOT name
   from the natural-language `natural` field because that would
   re-encode noise: any single Model's phrasing dominates.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence
from uuid import UUID


_MAX_SIGNATURE_LEN = 120
_MAX_KIND_PARTS = 3
_MAX_SCOPE_PARTS = 3


@dataclass(frozen=True)
class MemberSummary:
    """Compact per-Model facts the namer reads. Callers pass a list
    of these — the namer never touches the DB. Keeping this
    dataclass small (proposition_kind + scope_actors + scope_entities)
    makes it cheap to build inside `recompute_for_tenant`."""
    model_id: UUID
    proposition_kind: str | None
    scope_actor_ids: tuple[UUID, ...]
    scope_entity_refs: tuple[tuple[str, str], ...]
    # Optional: actor display names + entity titles for richer naming.
    # When absent (the cheap path inside the recompute hot loop) the
    # namer falls back to UUIDs / counts.
    actor_labels: Mapping[UUID, str] | None = None
    entity_labels: Mapping[tuple[str, str], str] | None = None


def derive_signature(members: Sequence[MemberSummary]) -> str:
    """Compute a stable, human-readable name for a neighborhood.

    The signature has up to three parts joined with " @ ":

      <kinds>  @  <entities>  /  <actors>

    Each part is omitted when there's nothing to put in it. Empty
    inputs return "unnamed".

    Examples
    --------
      [state, state, recommendation], scope_entity={commitment:X},
      actors={Sarah}                  → "state+recommendation @ commitment / Sarah"

      [concern, prediction], scope_entity={customer:Globex}      → "concern+prediction @ customer:Globex"

      [state] no scope                          → "state"

    Determinism: same members → same signature. We sort frequency-tied
    parts alphabetically so two equally-popular kinds always render
    in the same order.
    """
    if not members:
        return "unnamed"

    kinds = _top_kinds(members)
    actors = _top_actors(members)
    entities = _top_entities(members)

    kind_part = "+".join(kinds) if kinds else ""
    entity_part = ", ".join(entities) if entities else ""
    actor_part = ", ".join(actors) if actors else ""

    scope_segments = []
    if entity_part:
        scope_segments.append(entity_part)
    if actor_part:
        scope_segments.append(actor_part)
    scope = " / ".join(scope_segments)

    if kind_part and scope:
        sig = f"{kind_part} @ {scope}"
    elif kind_part:
        sig = kind_part
    elif scope:
        sig = scope
    else:
        sig = "unnamed"

    if len(sig) > _MAX_SIGNATURE_LEN:
        sig = sig[: _MAX_SIGNATURE_LEN - 1] + "…"
    return sig


def _top_kinds(members: Sequence[MemberSummary]) -> list[str]:
    counter: Counter[str] = Counter()
    for m in members:
        if m.proposition_kind:
            counter[m.proposition_kind] += 1
    if not counter:
        return []
    items = sorted(
        counter.items(),
        key=lambda kv: (-kv[1], kv[0]),
    )[:_MAX_KIND_PARTS]
    return [k for k, _ in items]


def _top_actors(members: Sequence[MemberSummary]) -> list[str]:
    counter: Counter[UUID] = Counter()
    label_for: dict[UUID, str] = {}
    for m in members:
        for a in m.scope_actor_ids:
            counter[a] += 1
            if m.actor_labels and a in m.actor_labels and a not in label_for:
                label_for[a] = m.actor_labels[a]
    if not counter:
        return []
    items = sorted(
        counter.items(),
        key=lambda kv: (-kv[1], str(kv[0])),
    )[:_MAX_SCOPE_PARTS]
    out: list[str] = []
    for actor_id, _ in items:
        out.append(label_for.get(actor_id) or _abbr_uuid(actor_id))
    return out


def _top_entities(members: Sequence[MemberSummary]) -> list[str]:
    counter: Counter[tuple[str, str]] = Counter()
    label_for: dict[tuple[str, str], str] = {}
    for m in members:
        for ref in m.scope_entity_refs:
            counter[ref] += 1
            if m.entity_labels and ref in m.entity_labels and ref not in label_for:
                label_for[ref] = m.entity_labels[ref]
    if not counter:
        return []
    items = sorted(
        counter.items(),
        key=lambda kv: (-kv[1], kv[0][0], kv[0][1]),
    )[:_MAX_SCOPE_PARTS]
    out: list[str] = []
    for ref, _ in items:
        label = label_for.get(ref)
        if label:
            out.append(f"{ref[0]}:{label}")
        else:
            out.append(f"{ref[0]}:{_abbr_uuid_str(ref[1])}")
    return out


def _abbr_uuid(u: UUID) -> str:
    s = str(u)
    return s[:8] if len(s) >= 8 else s


def _abbr_uuid_str(s: str) -> str:
    return s[:8] if len(s) >= 8 else s


def member_summaries_from_rows(
    rows: Iterable[Mapping[str, object]],
) -> list[MemberSummary]:
    """Build MemberSummary list from asyncpg Records / dicts. Each row
    must expose: id, proposition_kind, scope_actors (UUID[] or list),
    scope_entities (list[dict[type,id]]). Optional: actor_labels (dict
    UUID→str), entity_labels (dict (type,id)→str)."""
    out: list[MemberSummary] = []
    for r in rows:
        actor_ids: list[UUID] = []
        for a in (r.get("scope_actors") or []):
            if isinstance(a, UUID):
                actor_ids.append(a)
            else:
                try:
                    actor_ids.append(UUID(str(a)))
                except (ValueError, TypeError):
                    continue
        ent_refs: list[tuple[str, str]] = []
        for e in (r.get("scope_entities") or []):
            if not isinstance(e, dict):
                continue
            t = e.get("type")
            i = e.get("id")
            if t is None or i is None:
                continue
            ent_refs.append((str(t), str(i)))
        out.append(
            MemberSummary(
                model_id=r["id"],  # type: ignore[arg-type]
                proposition_kind=(
                    str(r.get("proposition_kind"))
                    if r.get("proposition_kind") is not None
                    else None
                ),
                scope_actor_ids=tuple(actor_ids),
                scope_entity_refs=tuple(ent_refs),
                actor_labels=r.get("actor_labels"),  # type: ignore[arg-type]
                entity_labels=r.get("entity_labels"),  # type: ignore[arg-type]
            )
        )
    return out


__all__ = [
    "MemberSummary",
    "derive_signature",
    "member_summaries_from_rows",
]
