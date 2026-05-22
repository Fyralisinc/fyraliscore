"""services/model_trace/repo.py — graph walks over model_edges.

Two BFS walks (back / forward) with bounded depth. We treat the active
edge graph in `model_edges` as the canonical evidence-chain substrate;
edge_kind decides which direction counts as "upstream" for each walk.

Direction conventions
---------------------

Trace back (what supports this node):
  - incoming `supports` edges          : source supports target (=node)
  - incoming `contributes_to_resolution`: source contributes to target
  - outgoing `instance_of` edges       : node is instance_of target → the
                                          pattern is upstream
  - outgoing `superseded_by` edges     : node was superseded; the new
                                          model is upstream

Trace forward (what this node enables):
  - outgoing `supports` edges          : node supports target
  - outgoing `contributes_to_resolution`: node contributes to target
  - incoming `instance_of` edges       : other models are instances of
                                          node (it's a pattern)
  - incoming `superseded_by` edges     : node supersedes other models

If the edge data is sparse, the walk simply terminates and the
returned chain is short or contains only the seed step. Callers (the
Model UI) handle empty states.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg


# Edge kinds and their semantics for the two walks. Each entry is
# (edge_kind, direction): direction="in" means "follow incoming edges"
# (source becomes the next node); direction="out" means "follow
# outgoing edges" (target becomes the next node).
_BACK_EDGE_DIRS: tuple[tuple[str, str], ...] = (
    ("supports", "in"),
    ("contributes_to_resolution", "in"),
    ("instance_of", "out"),
    ("superseded_by", "out"),
)

_FORWARD_EDGE_DIRS: tuple[tuple[str, str], ...] = (
    ("supports", "out"),
    ("contributes_to_resolution", "out"),
    ("instance_of", "in"),
    ("superseded_by", "in"),
)


# Map proposition_kind onto a high-level "kind" label the Model UI
# uses in trace bubbles (Observation / Claim / Pattern / Belief /
# Recommendation / Delta). Keep the mapping coarse — the UI only needs
# enough granularity to render the right icon.
_KIND_LABELS: dict[str, str] = {
    "recommendation": "recommendation",
    "pattern": "pattern",
    "pattern_instance": "pattern_instance",
    "state": "claim",
    "relation": "claim",
    "prediction": "belief",
    "hypothesis": "belief",
    "concern": "risk",
    "capability_assessment": "claim",
    "market_assessment": "claim",
    "environmental_trend": "pattern",
}


Direction = Literal["back", "forward"]


@dataclass
class TraceStep:
    """One node in a trace chain. JSON-shape: {id, kind, label,
    summary, ts}. `ts` may be None when the upstream/downstream node
    has no resolved/asserted timestamp."""
    id: UUID
    kind: str
    label: str
    summary: str
    ts: datetime | None = None
    # Edge kind that connected this step to the previous step. Null on
    # the seed step. Lets the renderer pick the right path color.
    via_edge_kind: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "kind": self.kind,
            "label": self.label,
            "summary": self.summary,
            "ts": self.ts.isoformat() if self.ts is not None else None,
            "via_edge_kind": self.via_edge_kind,
            **({"extra": self.extra} if self.extra else {}),
        }


# ---------------------------------------------------------------------
# Adjacency helpers
# ---------------------------------------------------------------------


_NODE_COLS_SQL = (
    'm.id, m."natural" AS natural, m.proposition_kind, '
    "m.confidence, m.created_at, m.last_confirmed_at, "
    "m.resolved_at, m.status"
)


async def _fetch_node(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    node_id: UUID,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        f"""
        SELECT {_NODE_COLS_SQL}
        FROM models m
        WHERE m.id = $1 AND m.tenant_id = $2
        """,
        node_id, tenant_id,
    )
    return dict(row) if row is not None else None


def _row_to_step(
    row: dict[str, Any],
    *,
    via_edge_kind: str | None,
) -> TraceStep:
    natural = (row.get("natural") or "").strip()
    pk = row.get("proposition_kind") or ""
    kind = _KIND_LABELS.get(pk, pk or "model")
    label = natural[:80] if natural else f"{kind or 'model'}:{str(row['id'])[:8]}"
    summary = natural[:240] if natural else ""
    # Pick the most useful timestamp: resolved_at > last_confirmed_at >
    # created_at. The UI shows this to anchor the step in time.
    ts = (
        row.get("resolved_at")
        or row.get("last_confirmed_at")
        or row.get("created_at")
    )
    return TraceStep(
        id=row["id"],
        kind=kind,
        label=label,
        summary=summary,
        ts=ts,
        via_edge_kind=via_edge_kind,
    )


async def _neighbors(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    node_id: UUID,
    edge_dirs: tuple[tuple[str, str], ...],
) -> list[tuple[UUID, str]]:
    """Return [(neighbor_id, via_edge_kind), ...] following each edge
    in `edge_dirs`. Active edges only; deduped on neighbor_id (keeping
    the first edge_kind that produced it)."""
    in_kinds = [k for (k, d) in edge_dirs if d == "in"]
    out_kinds = [k for (k, d) in edge_dirs if d == "out"]
    seen: dict[UUID, str] = {}
    if in_kinds:
        rows = await conn.fetch(
            """
            SELECT source_model_id AS neighbor_id, edge_kind
            FROM model_edges
            WHERE tenant_id = $1
              AND target_model_id = $2
              AND status = 'active'
              AND edge_kind = ANY($3::text[])
            """,
            tenant_id, node_id, in_kinds,
        )
        for r in rows:
            if r["neighbor_id"] not in seen:
                seen[r["neighbor_id"]] = r["edge_kind"]
    if out_kinds:
        rows = await conn.fetch(
            """
            SELECT target_model_id AS neighbor_id, edge_kind
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND status = 'active'
              AND edge_kind = ANY($3::text[])
            """,
            tenant_id, node_id, out_kinds,
        )
        for r in rows:
            if r["neighbor_id"] not in seen:
                seen[r["neighbor_id"]] = r["edge_kind"]
    return list(seen.items())


# ---------------------------------------------------------------------
# Public walks
# ---------------------------------------------------------------------


async def _walk(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    node_id: UUID,
    max_depth: int,
    edge_dirs: tuple[tuple[str, str], ...],
) -> list[TraceStep]:
    """BFS-with-priority walk. Picks one canonical chain by always
    expanding the lowest-id neighbor at each hop (deterministic and
    stable across reads). Returns the chain seed-first.

    If the seed node doesn't exist for this tenant, returns []. If the
    seed exists but has no inbound/outbound evidence, returns a single-
    step chain (just the seed).
    """
    if max_depth < 0:
        max_depth = 0
    seed_row = await _fetch_node(
        conn, tenant_id=tenant_id, node_id=node_id,
    )
    if seed_row is None:
        return []
    chain: list[TraceStep] = [_row_to_step(seed_row, via_edge_kind=None)]
    visited: set[UUID] = {node_id}
    current = node_id
    for _ in range(max_depth):
        neighbors = await _neighbors(
            conn,
            tenant_id=tenant_id,
            node_id=current,
            edge_dirs=edge_dirs,
        )
        # Drop visited so cycles don't loop forever (model_edges is
        # DAG-scoped for `supports` / `instance_of`, but other kinds
        # may still admit cycles).
        unvisited = [(nid, ek) for (nid, ek) in neighbors if nid not in visited]
        if not unvisited:
            break
        # Deterministic pick: smallest UUID. Trace chains are a thin
        # narrative spine — we don't try to surface every branch.
        unvisited.sort(key=lambda t: str(t[0]))
        next_id, via_kind = unvisited[0]
        next_row = await _fetch_node(
            conn, tenant_id=tenant_id, node_id=next_id,
        )
        if next_row is None:
            break
        chain.append(_row_to_step(next_row, via_edge_kind=via_kind))
        visited.add(next_id)
        current = next_id
    return chain


async def trace_back(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    node_id: UUID,
    max_depth: int = 4,
) -> list[TraceStep]:
    """Walk evidence/supports edges upstream from `node_id`.

    Returns the chain Observation → Claim → Pattern → Belief →
    Recommendation/Delta ending at this node. The seed (this node) is
    included as the FIRST element; upstream steps follow.
    """
    return await _walk(
        conn,
        tenant_id=tenant_id,
        node_id=node_id,
        max_depth=max_depth,
        edge_dirs=_BACK_EDGE_DIRS,
    )


async def trace_forward(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    node_id: UUID,
    max_depth: int = 4,
) -> list[TraceStep]:
    """Walk downstream from `node_id`.

    Returns the chain this node enables: Node → Recommendation →
    Commitment impact → Customer/revenue impact. The seed is FIRST;
    downstream steps follow.
    """
    return await _walk(
        conn,
        tenant_id=tenant_id,
        node_id=node_id,
        max_depth=max_depth,
        edge_dirs=_FORWARD_EDGE_DIRS,
    )


# ---------------------------------------------------------------------
# Small adjacency queries — supports / depends_on
# ---------------------------------------------------------------------


async def supports(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    node_id: UUID,
) -> list[TraceStep]:
    """Direct downstream adjacency: nodes that this node SUPPORTS or
    contributes_to_resolution for. One hop, no walk."""
    rows = await conn.fetch(
        f"""
        SELECT {_NODE_COLS_SQL}, e.edge_kind AS via_edge_kind
        FROM model_edges e
        JOIN models m ON m.id = e.target_model_id
        WHERE e.tenant_id = $1
          AND e.source_model_id = $2
          AND e.status = 'active'
          AND e.edge_kind = ANY($3::text[])
          AND m.tenant_id = $1
        ORDER BY m.created_at DESC
        """,
        tenant_id,
        node_id,
        ["supports", "contributes_to_resolution"],
    )
    return [
        _row_to_step(dict(r), via_edge_kind=r["via_edge_kind"])
        for r in rows
    ]


async def depends_on(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    node_id: UUID,
) -> list[TraceStep]:
    """Direct upstream adjacency: nodes this node depends on. Reverse
    of `supports`: the inbound side of supports / contributes_to_resolution,
    plus the outgoing instance_of (the pattern this is an instance of).
    One hop, no walk."""
    rows_in = await conn.fetch(
        f"""
        SELECT {_NODE_COLS_SQL}, e.edge_kind AS via_edge_kind
        FROM model_edges e
        JOIN models m ON m.id = e.source_model_id
        WHERE e.tenant_id = $1
          AND e.target_model_id = $2
          AND e.status = 'active'
          AND e.edge_kind = ANY($3::text[])
          AND m.tenant_id = $1
        ORDER BY m.created_at DESC
        """,
        tenant_id,
        node_id,
        ["supports", "contributes_to_resolution"],
    )
    rows_out = await conn.fetch(
        f"""
        SELECT {_NODE_COLS_SQL}, e.edge_kind AS via_edge_kind
        FROM model_edges e
        JOIN models m ON m.id = e.target_model_id
        WHERE e.tenant_id = $1
          AND e.source_model_id = $2
          AND e.status = 'active'
          AND e.edge_kind = 'instance_of'
          AND m.tenant_id = $1
        ORDER BY m.created_at DESC
        """,
        tenant_id,
        node_id,
    )
    out: list[TraceStep] = []
    seen: set[UUID] = set()
    for r in list(rows_in) + list(rows_out):
        rid = r["id"]
        if rid in seen:
            continue
        seen.add(rid)
        out.append(_row_to_step(dict(r), via_edge_kind=r["via_edge_kind"]))
    return out


__all__ = [
    "TraceStep",
    "trace_back",
    "trace_forward",
    "supports",
    "depends_on",
]
