"""
services/topology/events_repo.py — phase-event log + detector.

S3 of the Self-Organizing Substrate plan. Sits on top of
NeighborhoodsRepo (S2): every neighborhood recompute calls into the
detector here, which diffs prev vs new community structure and emits
phase events into `topology_events` (migration 0033).

Public API
----------

  TopologyEventsRepo(pool=None)

    .record(conn, *, event)               — insert one PhaseEvent
    .list_recent(conn, *, tenant_id, limit) — read tail for UI / audit
    .pending(conn, *, tenant_id, limit)   — unprocessed events for the
                                            T6 dispatcher
    .mark_processed(conn, *, event_id)    — close out a dispatched
                                            event
    .for_neighborhood(conn, *, neighborhood_id) — history for one
                                                    neighborhood

  detect_phase_events(
      prev_neighborhoods,         # list[PrevSnapshot]
      new_communities,            # dict[label, set[UUID]]
      label_to_neighborhood_id,   # dict[label, UUID]  (post-upsert)
      matched_prev_ids_by_label,  # dict[label, UUID | None]
      member_summaries_by_id,     # dict[UUID, MemberSummary] (members)
  ) -> list[PhaseEvent]

      Pure function. Compares the snapshot from before recompute to
      the just-materialized result. Maps to the closed taxonomy in
      `topology_events.kind`:

        emergence    : new community whose members do not intersect
                       any prior active neighborhood at all.
        dissolution  : prior neighborhood whose members fragmented or
                       fell below MIN_COMMUNITY_SIZE in the new run.
        split        : single prior whose members now span ≥2 new
                       neighborhoods (≥1 new with shared members).
        merge        : ≥2 priors whose members coalesced into one new.
        drift        : same matched neighborhood survived but its
                       Jaccard distance from prior membership exceeds
                       DRIFT_JACCARD_THRESHOLD.

      Returned events carry magnitude + named_signature so the events
      table can be the primary surface for downstream consumers
      (T6 dispatcher, CEO view, audit log).

Events are written by `NeighborhoodsRepo.recompute_for_tenant` in the
same transaction as the neighborhood UPSERT — atomicity matters: if
the recompute rolls back, the events do too.

Why detection lives here, not on NeighborhoodsRepo
--------------------------------------------------

NeighborhoodsRepo orchestrates the materialization (load + cluster +
match + write). Phase-event detection is a separate concern: it reads
prev/new snapshots and emits events. Splitting them keeps each focused
and lets us swap detection algorithms (drift threshold, merge
heuristics) without touching the materialization path.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, Mapping
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError
from lib.shared.ids import uuid7
from lib.topology.naming import (
    MemberSummary,
    derive_signature,
)


PhaseEventKind = Literal[
    "emergence", "dissolution", "split", "merge", "drift"
]


# A drift Jaccard distance > this threshold (members changed enough)
# triggers a 'drift' event for an otherwise-matched neighborhood.
# 0.4 is permissive — a quarter to a third of the membership churning
# is enough to re-name. Operators can tune via env if drift events
# spam.
DRIFT_JACCARD_THRESHOLD = 0.4


class TopologyEventsRepoError(CompanyOSError):
    default_code = "topology_events_repo_error"


# ---------------------------------------------------------------------
# PhaseEvent dataclass
# ---------------------------------------------------------------------


@dataclass
class PhaseEvent:
    """In-memory phase event before insertion. Built by
    `detect_phase_events`, persisted by `TopologyEventsRepo.record`."""
    kind: PhaseEventKind
    tenant_id: UUID
    member_model_ids: list[UUID]
    neighborhood_id: UUID | None = None
    predecessor_neighborhood_ids: list[UUID] = field(default_factory=list)
    sibling_neighborhood_ids: list[UUID] = field(default_factory=list)
    magnitude: float | None = None
    named_signature: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------
# Snapshot helpers (used by detect_phase_events)
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class PrevSnapshot:
    """Compact pre-recompute view of an active neighborhood. The
    detector consumes lists of these — it never needs the full
    centroid vector."""
    id: UUID
    members: frozenset[UUID]


# ---------------------------------------------------------------------
# Repo
# ---------------------------------------------------------------------


class TopologyEventsRepo:
    def __init__(self, pool: asyncpg.Pool | None = None) -> None:
        self._pool = pool

    async def record(
        self,
        conn: asyncpg.Connection,
        *,
        event: PhaseEvent,
    ) -> UUID:
        """Insert one phase event. Returns the new event id."""
        event_id = uuid7()
        await conn.execute(
            """
            INSERT INTO topology_events (
              id, tenant_id, kind, neighborhood_id,
              predecessor_neighborhood_ids, sibling_neighborhood_ids,
              member_model_ids, magnitude, named_signature, payload
            )
            VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb
            )
            """,
            event_id,
            event.tenant_id,
            event.kind,
            event.neighborhood_id,
            event.predecessor_neighborhood_ids or None,
            event.sibling_neighborhood_ids or None,
            event.member_model_ids,
            event.magnitude,
            event.named_signature,
            _jsonb(event.payload),
        )
        return event_id

    async def list_recent(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, kind, neighborhood_id,
                   predecessor_neighborhood_ids,
                   sibling_neighborhood_ids,
                   member_model_ids, magnitude, named_signature,
                   payload, occurred_at, processed_at
            FROM topology_events
            WHERE tenant_id = $1
            ORDER BY occurred_at DESC
            LIMIT $2
            """,
            tenant_id, limit,
        )
        return [dict(r) for r in rows]

    async def pending(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        if tenant_id is not None:
            rows = await conn.fetch(
                """
                SELECT id, tenant_id, kind, neighborhood_id,
                       predecessor_neighborhood_ids,
                       sibling_neighborhood_ids,
                       member_model_ids, magnitude, named_signature,
                       payload, occurred_at
                FROM topology_events
                WHERE processed_at IS NULL AND tenant_id = $1
                ORDER BY occurred_at
                LIMIT $2
                """,
                tenant_id, limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT id, tenant_id, kind, neighborhood_id,
                       predecessor_neighborhood_ids,
                       sibling_neighborhood_ids,
                       member_model_ids, magnitude, named_signature,
                       payload, occurred_at
                FROM topology_events
                WHERE processed_at IS NULL
                ORDER BY occurred_at
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]

    async def mark_processed(
        self,
        conn: asyncpg.Connection,
        *,
        event_id: UUID,
    ) -> None:
        await conn.execute(
            "UPDATE topology_events SET processed_at = now() "
            "WHERE id = $1 AND processed_at IS NULL",
            event_id,
        )

    async def for_neighborhood(
        self,
        conn: asyncpg.Connection,
        *,
        neighborhood_id: UUID,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, kind, neighborhood_id,
                   predecessor_neighborhood_ids,
                   sibling_neighborhood_ids,
                   member_model_ids, magnitude, named_signature,
                   payload, occurred_at, processed_at
            FROM topology_events
            WHERE neighborhood_id = $1
            ORDER BY occurred_at DESC
            LIMIT $2
            """,
            neighborhood_id, limit,
        )
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------
# detect_phase_events — pure function
# ---------------------------------------------------------------------


def detect_phase_events(
    *,
    tenant_id: UUID,
    prev_neighborhoods: Iterable[PrevSnapshot],
    new_communities: Mapping[int, set[UUID]],
    label_to_neighborhood_id: Mapping[int, UUID],
    matched_prev_ids_by_label: Mapping[int, UUID | None],
    member_summaries_by_id: Mapping[UUID, MemberSummary] | None = None,
) -> list[PhaseEvent]:
    """Compare prev to new community structure; produce phase events.

    Inputs come straight from `NeighborhoodsRepo.recompute_for_tenant`
    just after the UPSERT phase, so:

      - `prev_neighborhoods` is the snapshot loaded BEFORE this
        recompute pass.
      - `new_communities` is the {label -> {model_ids}} map after
        prune_singletons.
      - `label_to_neighborhood_id` and `matched_prev_ids_by_label`
        are the IDs assigned by the upsert.

    The detector is intentionally conservative: only structural
    transitions emit events. A no-change recompute (matched 1:1, no
    drift) emits nothing.

    Memoization
    -----------
    To classify split vs drift correctly we need to know, for each
    new community, which prior neighborhoods share members with it.
    We pre-compute that overlap once (O(|prev| × |new| × avg-size)
    set intersection) — for tenant scales of hundreds of
    neighborhoods this is microseconds.

    Magnitude calculation
    ---------------------
      emergence   : len(new_members)
      dissolution : len(prev.members)
      split       : 1 - (largest_share / total_members)
                    (1.0 = perfectly even split; 0.0 = one tiny
                     fragment off a large parent)
      merge       : len(new_members)
      drift       : 1 - jaccard(prev_members, new_members)
                    (0 = identical, 1 = disjoint)
    """
    prev_list = list(prev_neighborhoods)
    prev_by_id = {p.id: p for p in prev_list}

    # Reverse map: which new communities does each prior touch?
    prior_touches: dict[UUID, list[int]] = {p.id: [] for p in prev_list}
    new_touches: dict[int, list[UUID]] = {label: [] for label in new_communities}
    for label, members in new_communities.items():
        for prev in prev_list:
            if prev.members & members:
                prior_touches[prev.id].append(label)
                new_touches[label].append(prev.id)

    events: list[PhaseEvent] = []
    summaries = member_summaries_by_id or {}

    def _name_for(member_ids: Iterable[UUID]) -> str | None:
        if not summaries:
            return None
        members = [summaries[mid] for mid in member_ids if mid in summaries]
        if not members:
            return None
        return derive_signature(members)

    # ---- emergence + drift detection (per new community) ------------
    for label, members in new_communities.items():
        nid = label_to_neighborhood_id.get(label)
        if nid is None:
            # Defensive: should always be set after upsert.
            continue
        matched_prev = matched_prev_ids_by_label.get(label)
        touched_priors = new_touches.get(label, [])

        if matched_prev is None and not touched_priors:
            # New community with no prior overlap → emergence.
            events.append(
                PhaseEvent(
                    kind="emergence",
                    tenant_id=tenant_id,
                    neighborhood_id=nid,
                    member_model_ids=sorted(members, key=str),
                    magnitude=float(len(members)),
                    named_signature=_name_for(members),
                )
            )
            continue

        if matched_prev is not None:
            prev = prev_by_id.get(matched_prev)
            if prev is None:
                continue
            inter = len(prev.members & members)
            union = len(prev.members | members)
            jaccard = (inter / union) if union else 1.0
            distance = 1.0 - jaccard
            if distance >= DRIFT_JACCARD_THRESHOLD:
                events.append(
                    PhaseEvent(
                        kind="drift",
                        tenant_id=tenant_id,
                        neighborhood_id=nid,
                        predecessor_neighborhood_ids=[matched_prev],
                        member_model_ids=sorted(members, key=str),
                        magnitude=distance,
                        named_signature=_name_for(members),
                        payload={
                            "jaccard_to_prev": jaccard,
                            "added_count": len(members - prev.members),
                            "removed_count": len(prev.members - members),
                        },
                    )
                )
            continue

        # matched_prev is None but priors overlap → either merge
        # (≥2 priors) or split (the new community is a fragment of
        # one prior). Detected separately below; nothing here.

    # ---- merge detection (per new community) ------------------------
    for label, members in new_communities.items():
        if matched_prev_ids_by_label.get(label) is not None:
            continue
        priors = new_touches.get(label, [])
        if len(priors) >= 2:
            nid = label_to_neighborhood_id.get(label)
            events.append(
                PhaseEvent(
                    kind="merge",
                    tenant_id=tenant_id,
                    neighborhood_id=nid,
                    predecessor_neighborhood_ids=sorted(priors, key=str),
                    member_model_ids=sorted(members, key=str),
                    magnitude=float(len(members)),
                    named_signature=_name_for(members),
                )
            )

    # ---- split detection (per prior) --------------------------------
    for prev in prev_list:
        # Which new communities does this prior touch?
        children = prior_touches.get(prev.id, [])
        if len(children) < 2:
            continue
        # Determine total members and the largest-shared-share. The
        # "largest" is the new community that shares the most members
        # with the prev. The neighborhood the event is "about" is the
        # largest-share child (so consumers see continuity); siblings
        # are the rest.
        share_sizes = {
            label: len(prev.members & new_communities[label])
            for label in children
        }
        ordered_children = sorted(
            children,
            key=lambda lab: (-share_sizes[lab], lab),
        )
        largest = ordered_children[0]
        siblings = ordered_children[1:]
        total = sum(share_sizes.values())
        split_balance = (
            1.0 - (share_sizes[largest] / total) if total > 0 else 0.0
        )
        # Combine all child members for naming purposes.
        all_child_members: set[UUID] = set()
        for lab in ordered_children:
            all_child_members.update(new_communities[lab])
        events.append(
            PhaseEvent(
                kind="split",
                tenant_id=tenant_id,
                neighborhood_id=label_to_neighborhood_id.get(largest),
                predecessor_neighborhood_ids=[prev.id],
                sibling_neighborhood_ids=[
                    label_to_neighborhood_id[lab]
                    for lab in siblings
                    if label_to_neighborhood_id.get(lab) is not None
                ],
                member_model_ids=sorted(
                    new_communities[largest], key=str
                ),
                magnitude=split_balance,
                named_signature=_name_for(new_communities[largest]),
                payload={
                    "child_share_sizes": {
                        str(label_to_neighborhood_id.get(lab) or lab): share_sizes[lab]
                        for lab in ordered_children
                    },
                },
            )
        )

    # ---- dissolution detection (per prior) --------------------------
    for prev in prev_list:
        if prior_touches.get(prev.id):
            continue
        # No new community shares any member → dissolved.
        events.append(
            PhaseEvent(
                kind="dissolution",
                tenant_id=tenant_id,
                neighborhood_id=prev.id,
                predecessor_neighborhood_ids=[prev.id],
                member_model_ids=sorted(prev.members, key=str),
                magnitude=float(len(prev.members)),
            )
        )

    return events


# ---------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------


def _jsonb(value: dict | None) -> str:
    import json
    return json.dumps(value or {}, default=str, sort_keys=True)


__all__ = [
    "TopologyEventsRepo",
    "TopologyEventsRepoError",
    "PhaseEvent",
    "PhaseEventKind",
    "PrevSnapshot",
    "DRIFT_JACCARD_THRESHOLD",
    "detect_phase_events",
]
