"""
services/topology/neighborhoods_repo.py — repository for materialized
neighborhoods (S2, migration 0032).

Owns:

  - `model_neighborhoods` reads/writes (centroid, members, density,
    lifecycle status)
  - `model_neighborhood_membership` reads/writes (per-Model
    membership + centrality)
  - the orchestration of one neighborhood-detection pass over a
    tenant's active edge graph

Public API
----------

  NeighborhoodsRepo(pool=None)

  .recompute_for_tenant(conn, *, tenant_id) -> RecomputeReport
      Single sweep:
        1. Load all active Models in tenant + their topo_embeddings.
        2. Load all active edges in tenant.
        3. detect_communities() → {model_id -> label}.
        4. prune_singletons() → drop communities below min size.
        5. Read existing active neighborhoods (PrevNeighborhood
           tuples) for matching.
        6. match_communities() → {new_label -> existing_id_or_None}.
        7. UPSERT neighborhoods:
             - matched: UPDATE existing row (members, centroid,
               density, last_recomputed_at).
             - unmatched: INSERT new row (predecessor_neighborhood_ids
               filled if the new community shares ≥1 prior member).
        8. Old neighborhoods that didn't match → status='dissolved'.
        9. Recompute membership table (per-Model centrality).
       10. Return RecomputeReport for observability.

  .list_active(conn, tenant_id) -> list[NeighborhoodRow]
      Read-only. Used by the debug UI in S3.

  .membership_for(conn, *, model_id) -> NeighborhoodRow | None
      Reverse lookup: which active neighborhood contains this
      Model? Returns None for isolated Models.

Pure separation
---------------
Detection / matching / density / centrality ALL live in
lib/topology/community.py as pure functions over in-memory data.
This repo is the I/O boundary: load from DB, hand to pure functions,
write back.

See:
  - lib/topology/community.py — algorithms
  - services/workers/neighborhood_detector/worker.py — scheduler
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError
from lib.shared.ids import uuid7
from lib.shared.types import TOPO_EMBEDDING_DIM
from lib.topology.community import (
    PrevNeighborhood,
    compute_centrality,
    compute_density,
    detect_communities,
    match_communities,
    prune_singletons,
)
from lib.topology.naming import MemberSummary, derive_signature

from .events_repo import (
    PhaseEvent,
    PrevSnapshot,
    TopologyEventsRepo,
    detect_phase_events,
)


class NeighborhoodsRepoError(CompanyOSError):
    default_code = "neighborhoods_repo_error"


@dataclass
class RecomputeReport:
    """Bookkeeping for one tenant-level recompute pass."""
    tenant_id: UUID
    models_seen: int = 0
    edges_seen: int = 0
    communities_detected: int = 0
    communities_after_prune: int = 0
    matched_to_existing: int = 0
    new_neighborhoods: int = 0
    dissolved_neighborhoods: int = 0
    membership_rows_written: int = 0
    # S3: phase events emitted by this recompute (emergence /
    # dissolution / split / merge / drift). Counted per kind so the
    # neighborhood_detector worker can decide how many T6 triggers to
    # enqueue downstream.
    phase_events_emitted: int = 0
    phase_event_ids: list[UUID] = field(default_factory=list)


class NeighborhoodsRepo:
    def __init__(self, pool: asyncpg.Pool | None = None) -> None:
        self._pool = pool

    # =================================================================
    # recompute_for_tenant — one full pass
    # =================================================================
    async def recompute_for_tenant(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
    ) -> RecomputeReport:
        report = RecomputeReport(tenant_id=tenant_id)

        # 1. Load active Models + topo_embeddings + the fields the
        #    namer needs (proposition_kind, scope_actors,
        #    scope_entities). Naming is deferred until we know which
        #    Models belong to each community, but we read them up
        #    front so a single fetch covers the recompute pass.
        model_rows = await conn.fetch(
            """
            SELECT id, topo_embedding, proposition_kind,
                   scope_actors, scope_entities
            FROM models
            WHERE tenant_id = $1 AND status = 'active'
            """,
            tenant_id,
        )
        if not model_rows:
            return report
        report.models_seen = len(model_rows)

        # Map id → topo (None if not yet computed). Models without
        # topo are still in scope for community detection (graph
        # structure, not vector geometry, drives v1 detection) but
        # they don't contribute to centroid math.
        model_topos: dict[UUID, list[float] | None] = {}
        member_summaries_by_id: dict[UUID, MemberSummary] = {}
        for r in model_rows:
            te = r["topo_embedding"]
            if te is not None:
                te = [float(x) for x in te]
            model_topos[r["id"]] = te
            # Build the namer-facing summary up front. We keep the
            # full map so split / merge / drift events can name from
            # any combination of members in O(1) lookup.
            scope_entities_raw = r["scope_entities"]
            if isinstance(scope_entities_raw, (bytes, bytearray)):
                scope_entities_raw = scope_entities_raw.decode()
            if isinstance(scope_entities_raw, str):
                import json as _json
                try:
                    scope_entities_raw = _json.loads(scope_entities_raw)
                except _json.JSONDecodeError:
                    scope_entities_raw = []
            ent_refs: list[tuple[str, str]] = []
            for e in (scope_entities_raw or []):
                if isinstance(e, dict):
                    t = e.get("type")
                    i = e.get("id")
                    if t is not None and i is not None:
                        ent_refs.append((str(t), str(i)))
            actor_tuple = tuple(r["scope_actors"] or [])
            member_summaries_by_id[r["id"]] = MemberSummary(
                model_id=r["id"],
                proposition_kind=r["proposition_kind"],
                scope_actor_ids=actor_tuple,
                scope_entity_refs=tuple(ent_refs),
            )
        all_node_ids = set(model_topos.keys())

        # 2. Load active edges. Treat as undirected for community
        #    detection (matches the topology graph semantics).
        edge_rows = await conn.fetch(
            """
            SELECT source_model_id, target_model_id
            FROM model_edges
            WHERE tenant_id = $1 AND status = 'active'
            """,
            tenant_id,
        )
        edges = [
            (r["source_model_id"], r["target_model_id"])
            for r in edge_rows
        ]
        report.edges_seen = len(edges)

        # 3 + 4. Detect communities + prune singletons.
        labels = detect_communities(edges, all_node_ids)
        report.communities_detected = len(set(labels.values()))
        labels = prune_singletons(labels)
        report.communities_after_prune = len(set(labels.values()))

        # Group by community label.
        new_communities: dict[int, set[UUID]] = {}
        for node, label in labels.items():
            new_communities.setdefault(label, set()).add(node)

        # 5. Load existing active neighborhoods for matching.
        prev_rows = await conn.fetch(
            """
            SELECT id, member_model_ids, centroid_topo_embedding,
                   emergence_at
            FROM model_neighborhoods
            WHERE tenant_id = $1 AND status = 'active'
            """,
            tenant_id,
        )
        prev_neighborhoods = [
            PrevNeighborhood(
                id=r["id"],
                members=set(r["member_model_ids"] or []),
                centroid=(
                    [float(x) for x in r["centroid_topo_embedding"]]
                    if r["centroid_topo_embedding"] is not None
                    else None
                ),
            )
            for r in prev_rows
        ]
        prev_by_id = {p.id: p for p in prev_neighborhoods}

        # 6. Match.
        matches = match_communities(prev_neighborhoods, new_communities)

        # 7 + 8. Upsert + dissolve.
        used_prev_ids: set[UUID] = set()
        new_membership_rows: list[tuple[UUID, UUID, UUID, float]] = []
        # S3: track label -> neighborhood_id and label -> matched_prev
        # so the phase-event detector can correlate new communities
        # to prior neighborhoods after the upsert.
        label_to_neighborhood_id: dict[int, UUID] = {}
        matched_prev_ids_by_label: dict[int, UUID | None] = {}
        for new_label, members in new_communities.items():
            matched_prev_id = matches.get(new_label)
            matched_prev_ids_by_label[new_label] = matched_prev_id
            centroid = _centroid([
                model_topos[m]
                for m in members
                if model_topos.get(m) is not None
            ])
            density = compute_density(members, edges)
            # Heuristic name for the neighborhood — written every
            # recompute pass. The LLM may overwrite via T6 reasoning,
            # but the heuristic is the always-on fallback.
            signature = derive_signature([
                member_summaries_by_id[m]
                for m in members
                if m in member_summaries_by_id
            ])
            if matched_prev_id is not None:
                used_prev_ids.add(matched_prev_id)
                # UPDATE existing row. Refresh named_signature only if
                # currently NULL — the LLM-assigned name should not
                # be silently clobbered. Operators wanting to refresh
                # can NULL the column manually.
                await conn.execute(
                    """
                    UPDATE model_neighborhoods
                    SET member_model_ids = $2,
                        centroid_topo_embedding = $3::vector,
                        density = $4,
                        named_signature = COALESCE(named_signature, $5),
                        named_at = COALESCE(named_at, now()),
                        last_recomputed_at = now()
                    WHERE id = $1
                    """,
                    matched_prev_id,
                    list(members),
                    centroid,
                    density,
                    signature,
                )
                neighborhood_id = matched_prev_id
                report.matched_to_existing += 1
            else:
                # INSERT new row. Predecessor inheritance: any prior
                # neighborhood whose membership intersects this new
                # community (even below the matching threshold)
                # counts as a predecessor for audit purposes.
                predecessors = [
                    p.id for p in prev_neighborhoods
                    if (p.members & members) and p.id not in used_prev_ids
                ]
                neighborhood_id = uuid7()
                await conn.execute(
                    """
                    INSERT INTO model_neighborhoods
                      (id, tenant_id, centroid_topo_embedding,
                       member_model_ids, predecessor_neighborhood_ids,
                       density, status, named_signature, named_at)
                    VALUES ($1, $2, $3::vector, $4, $5, $6, 'active',
                            $7, now())
                    """,
                    neighborhood_id,
                    tenant_id,
                    centroid,
                    list(members),
                    predecessors if predecessors else None,
                    density,
                    signature,
                )
                report.new_neighborhoods += 1
            label_to_neighborhood_id[new_label] = neighborhood_id

            # Build membership rows with per-Model centrality.
            for m in members:
                cent = compute_centrality(m, members, edges)
                new_membership_rows.append(
                    (tenant_id, m, neighborhood_id, cent)
                )

        # 8. Dissolve unmatched previous neighborhoods.
        for prev in prev_neighborhoods:
            if prev.id not in used_prev_ids:
                await conn.execute(
                    """
                    UPDATE model_neighborhoods
                    SET status = 'dissolved',
                        status_changed_at = now(),
                        status_reason = 'no_match_in_recompute'
                    WHERE id = $1 AND status = 'active'
                    """,
                    prev.id,
                )
                report.dissolved_neighborhoods += 1

        # 9. Refresh membership table. Drop all prior rows for this
        #    tenant; insert the new set. Per-Model membership
        #    typically changes minimally, but a full rewrite is
        #    O(n) and avoids tracking per-row diffs.
        await conn.execute(
            "DELETE FROM model_neighborhood_membership WHERE tenant_id = $1",
            tenant_id,
        )
        if new_membership_rows:
            await conn.executemany(
                """
                INSERT INTO model_neighborhood_membership
                  (tenant_id, model_id, neighborhood_id, centrality)
                VALUES ($1, $2, $3, $4)
                """,
                new_membership_rows,
            )
        report.membership_rows_written = len(new_membership_rows)

        # 10. S3 — phase event detection. Compare prev snapshot to
        #     newly-materialized communities and record any
        #     emergence / dissolution / split / merge / drift events.
        #     Lives in the same transaction as the upsert so a
        #     rollback rolls back the events too.
        prev_snapshots = [
            PrevSnapshot(id=p.id, members=frozenset(p.members))
            for p in prev_neighborhoods
        ]
        events = detect_phase_events(
            tenant_id=tenant_id,
            prev_neighborhoods=prev_snapshots,
            new_communities=new_communities,
            label_to_neighborhood_id=label_to_neighborhood_id,
            matched_prev_ids_by_label=matched_prev_ids_by_label,
            member_summaries_by_id=member_summaries_by_id,
        )
        if events:
            events_repo = TopologyEventsRepo()
            for ev in events:
                ev_id = await events_repo.record(conn, event=ev)
                report.phase_event_ids.append(ev_id)
            report.phase_events_emitted = len(events)

        return report

    # =================================================================
    # list_active / membership_for — read-only consumers
    # =================================================================
    async def list_active(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
    ) -> list[dict[str, Any]]:
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, centroid_topo_embedding,
                   member_model_ids, emergence_at,
                   predecessor_neighborhood_ids, named_signature,
                   named_at, density, status, last_recomputed_at
            FROM model_neighborhoods
            WHERE tenant_id = $1 AND status = 'active'
            ORDER BY last_recomputed_at DESC
            """,
            tenant_id,
        )
        return [dict(r) for r in rows]

    async def membership_for(
        self,
        conn: asyncpg.Connection,
        *,
        model_id: UUID,
    ) -> dict[str, Any] | None:
        row = await conn.fetchrow(
            """
            SELECT n.id, n.tenant_id, n.centroid_topo_embedding,
                   n.member_model_ids, n.emergence_at,
                   n.named_signature, n.density, n.status,
                   m.centrality
            FROM model_neighborhood_membership m
            JOIN model_neighborhoods n ON n.id = m.neighborhood_id
            WHERE m.model_id = $1 AND n.status = 'active'
            LIMIT 1
            """,
            model_id,
        )
        return dict(row) if row else None


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------


def _centroid(vectors: list[list[float] | None]) -> list[float]:
    """Mean vector of `vectors`, with None entries filtered out.
    Returns a zero vector if all entries are None or list is empty.
    L2-normalized."""
    valid = [v for v in vectors if v is not None]
    if not valid:
        return [0.0] * TOPO_EMBEDDING_DIM
    out = [0.0] * TOPO_EMBEDDING_DIM
    for v in valid:
        for j in range(TOPO_EMBEDDING_DIM):
            out[j] += v[j]
    n = len(valid)
    out = [x / n for x in out]
    # L2-normalize so distance comparisons stay stable.
    import math
    norm = math.sqrt(sum(x * x for x in out))
    if norm > 0:
        out = [x / norm for x in out]
    return out


__all__ = [
    "NeighborhoodsRepo",
    "NeighborhoodsRepoError",
    "RecomputeReport",
]
