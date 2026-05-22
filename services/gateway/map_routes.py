"""
services/gateway/map_routes.py — HTTP handlers for the CEO Map view.

The wire contract lives in `services/gateway/map_router.py` (Pydantic
models). This module wires those models to FastAPI routes and joins
the underlying tables (models, model_edges, model_neighborhoods,
model_neighborhood_membership, topology_events, model_status_notes)
into the snapshot / story / events payloads the frontend expects.

Auth: tenant comes from `request.state.auth` (BearerAuthMiddleware).
Every query is tenant-scoped — there is no tenant param in the public
surface.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from services.gateway.auth import AuthContext
from services.gateway.map_router import (
    MapEdge,
    MapNeighborhood,
    MapNode,
    MapSnapshotChangeSummary,
    MapSnapshotResponse,
    ModelStoryResponse,
    RefreshProjectionResponse,
    StoryActivityEntry,
    StoryEdgeRef,
    TopologyEventEntry,
    TopologyEventsResponse,
)
from services.topology.umap_projector import UMAPProjector


# All four legal edge_kinds. Used as the default edge_kinds filter on
# the snapshot endpoint. Mirrors the registry-validated set in
# lib/shared/edge_registry.py — kept local here to avoid importing the
# heavy registry just for a constant.
_ALL_EDGE_KINDS: tuple[str, ...] = (
    "supports",
    "contributes_to_resolution",
    "instance_of",
    "superseded_by",
)


# ---------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------


def register_map_routes(app: FastAPI) -> None:
    """Attach the four /api/map/* routes to `app`."""

    @app.get("/map/snapshot")
    async def get_snapshot(request: Request) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        from services.gateway.main import _deps  # local: avoid cycle

        deps = _deps(request)

        # Parse query params manually so we can return 400 for bad
        # input without fighting FastAPI's coercion machinery.
        qp = request.query_params
        try:
            neighborhood_id = (
                UUID(qp["neighborhood_id"])
                if qp.get("neighborhood_id")
                else None
            )
        except (ValueError, TypeError):
            return _bad_request("invalid_neighborhood_id")
        edge_kinds_raw = qp.get("edge_kinds")
        if edge_kinds_raw:
            edge_kinds = tuple(
                k.strip() for k in edge_kinds_raw.split(",") if k.strip()
            )
            # Filter to known kinds only — silently drop unknowns to
            # stay forward-compatible if a new kind ships.
            edge_kinds = tuple(
                k for k in edge_kinds if k in _ALL_EDGE_KINDS
            )
            if not edge_kinds:
                edge_kinds = _ALL_EDGE_KINDS
        else:
            edge_kinds = _ALL_EDGE_KINDS
        include_archived = qp.get("include_archived", "").lower() in (
            "1", "true", "yes",
        )
        since = _parse_since(qp.get("since"))

        # Lens expansion: when ?lens=goal|commitment|decision|risk|customer
        # is set, the corresponding band is allowed to return up to 30
        # nodes instead of the curated 2–4. Other bands still cap small
        # so the focus stays on the expanded band.
        lens = qp.get("lens")
        if lens not in ("goal", "commitment", "decision", "risk", "customer"):
            lens = None

        # The "change_summary" window — explicit `since` overrides;
        # otherwise default to last 7 days.
        now = datetime.now(timezone.utc)
        summary_since = since or (now - timedelta(days=7))

        snapshot = await _build_snapshot(
            pool=deps.pool,
            tenant_id=auth.tenant_id,
            neighborhood_id=neighborhood_id,
            edge_kinds=edge_kinds,
            include_archived=include_archived,
            since=since,
            summary_since=summary_since,
            now=now,
            lens=lens,
        )
        return JSONResponse(_pydantic_dump(snapshot))

    @app.get("/map/topology_events")
    async def get_topology_events(request: Request) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        from services.gateway.main import _deps

        deps = _deps(request)
        qp = request.query_params
        since = _parse_since(qp.get("since"))
        if since is None:
            since = datetime.now(timezone.utc) - timedelta(days=7)
        try:
            limit = int(qp.get("limit", "50"))
        except (ValueError, TypeError):
            return _bad_request("invalid_limit")
        limit = max(1, min(limit, 200))

        rows = await deps.pool.fetch(
            """
            SELECT te.id, te.kind, te.occurred_at, te.neighborhood_id,
                   te.named_signature, te.magnitude, te.payload,
                   mn.named_signature AS neighborhood_named_signature
            FROM topology_events te
            LEFT JOIN model_neighborhoods mn
              ON mn.id = te.neighborhood_id
            WHERE te.tenant_id = $1
              AND te.occurred_at >= $2
            ORDER BY te.occurred_at DESC
            LIMIT $3
            """,
            auth.tenant_id,
            since,
            limit,
        )
        events: list[TopologyEventEntry] = []
        for r in rows:
            named_signature = (
                r["named_signature"]
                or r["neighborhood_named_signature"]
            )
            payload = _coerce_jsonb(r["payload"]) or {}
            events.append(
                TopologyEventEntry(
                    id=r["id"],
                    kind=r["kind"],
                    occurred_at=r["occurred_at"],
                    neighborhood_id=r["neighborhood_id"],
                    named_signature=named_signature,
                    magnitude=(
                        float(r["magnitude"])
                        if r["magnitude"] is not None
                        else None
                    ),
                    payload=payload,
                )
            )
        resp = TopologyEventsResponse(
            events=events,
            server_now=datetime.now(timezone.utc),
        )
        return JSONResponse(_pydantic_dump(resp))

    @app.get("/map/models/{model_id}")
    async def get_model_story(
        model_id: str, request: Request
    ) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        from services.gateway.main import _deps

        deps = _deps(request)
        try:
            mid = UUID(model_id)
        except (ValueError, TypeError):
            return _bad_request("invalid_model_id")
        story = await _build_model_story(
            pool=deps.pool, tenant_id=auth.tenant_id, model_id=mid,
        )
        if story is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return JSONResponse(_pydantic_dump(story))

    @app.post("/map/refresh_projection")
    async def post_refresh_projection(request: Request) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        from services.gateway.main import _deps

        deps = _deps(request)
        projector = UMAPProjector(deps.pool)
        cache = await projector.refresh(auth.tenant_id)
        # `cache["fitted_at"]` is an ISO string; coerce to datetime
        # for the Pydantic model.
        fitted_at_raw = cache.get("fitted_at")
        if isinstance(fitted_at_raw, str):
            fitted_at = datetime.fromisoformat(fitted_at_raw)
        else:
            fitted_at = fitted_at_raw or datetime.now(timezone.utc)
        resp = RefreshProjectionResponse(
            fitted_at=fitted_at,
            model_count=int(cache.get("model_count") or 0),
            trustworthiness=float(cache.get("trustworthiness") or 0.0),
            n_neighbors=int(cache.get("n_neighbors") or 15),
            min_dist=float(cache.get("min_dist") or 0.15),
        )
        return JSONResponse(_pydantic_dump(resp))


# ---------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------


async def _build_snapshot(
    *,
    pool,
    tenant_id: UUID,
    neighborhood_id: UUID | None,
    edge_kinds: tuple[str, ...],
    include_archived: bool,
    since: datetime | None,
    summary_since: datetime,
    now: datetime,
    lens: str | None = None,
) -> MapSnapshotResponse:
    # 1) UMAP projection (per-tenant). Returns {} when too small.
    #    Replaced PCA on 2026-05-10 — see CODEBASE-ARCHITECTURE.md §13.13.
    #    UMAP preserves *local* structure (the property CEOs actually
    #    care about) at the cost of warping global distances.
    projector = UMAPProjector(pool)
    projection = await projector.project(tenant_id)
    cache_meta = await projector.read_cache_meta(tenant_id)
    projection_fitted_at: datetime | None = None
    projection_trustworthiness: float | None = None
    if cache_meta and cache_meta.get("fitted_at"):
        try:
            projection_fitted_at = datetime.fromisoformat(
                cache_meta["fitted_at"]
            )
        except (ValueError, TypeError):
            projection_fitted_at = None
        trust = cache_meta.get("trustworthiness")
        if trust is not None:
            try:
                projection_trustworthiness = float(trust)
            except (ValueError, TypeError):
                projection_trustworthiness = None

    # 2) Pull active models (and archived if requested) in one query.
    #    `mnm` join surfaces the neighborhood id without forcing the
    #    consumer to join again. Any `since` filter applies to
    #    `created_at`.
    status_filter = "" if include_archived else " AND m.status = 'active'"
    since_filter = ""
    args: list[Any] = [tenant_id]
    if since is not None:
        args.append(since)
        since_filter = f" AND m.created_at >= ${len(args)}"
    nbh_filter = ""
    if neighborhood_id is not None:
        args.append(neighborhood_id)
        nbh_filter = f" AND mnm.neighborhood_id = ${len(args)}"

    model_rows = await pool.fetch(
        f"""
        SELECT
          m.id,
          m."natural" AS natural,
          m.proposition_kind,
          m.proposition,
          m.confidence,
          m.activation,
          m.status,
          m.archive_reason,
          m.contested_count,
          m.confirmed_count,
          m.last_confirmed_at,
          m.created_at,
          mnm.neighborhood_id AS neighborhood_id
        FROM models m
        LEFT JOIN model_neighborhood_membership mnm
          ON mnm.model_id = m.id AND mnm.tenant_id = m.tenant_id
        WHERE m.tenant_id = $1
        {status_filter}
        {since_filter}
        {nbh_filter}
        ORDER BY m.created_at DESC
        """,
        *args,
    )

    model_by_id: dict[UUID, dict[str, Any]] = {
        r["id"]: dict(r) for r in model_rows
    }
    model_ids = list(model_by_id.keys())

    # 3) Edges — only those whose endpoints are in our model set.
    #    Filter by `edge_kinds`. Active edges only (the snapshot is a
    #    "live" view).
    edge_rows: list[Any] = []
    if model_ids:
        edge_rows = await pool.fetch(
            """
            SELECT
              e.source_model_id, e.target_model_id, e.edge_kind,
              e.weight, e.status, e.detected_by
            FROM model_edges e
            WHERE e.tenant_id = $1
              AND e.status = 'active'
              AND e.edge_kind = ANY($2::text[])
              AND e.source_model_id = ANY($3::uuid[])
              AND e.target_model_id = ANY($3::uuid[])
            """,
            tenant_id,
            list(edge_kinds),
            model_ids,
        )

    # 4) In/out degree (for the visible subgraph).
    in_deg: dict[UUID, int] = {}
    out_deg: dict[UUID, int] = {}
    for er in edge_rows:
        out_deg[er["source_model_id"]] = (
            out_deg.get(er["source_model_id"], 0) + 1
        )
        in_deg[er["target_model_id"]] = (
            in_deg.get(er["target_model_id"], 0) + 1
        )

    # 5) Build MapNode list.
    nodes: list[MapNode] = []
    for mid, m in model_by_id.items():
        coord = projection.get(str(mid))
        topo_x = coord[0] if coord else None
        topo_y = coord[1] if coord else None
        natural = _truncate(m["natural"] or "", 100)
        health = _classify_health(
            status=m["status"],
            created_at=m["created_at"],
            contested=int(m["contested_count"] or 0),
            confirmed=int(m["confirmed_count"] or 0),
            confidence=float(m["confidence"] or 0.0),
            activation=float(m["activation"] or 0.0),
            last_confirmed_at=m["last_confirmed_at"],
            now=now,
        )
        band = _classify_band(
            proposition_kind=m["proposition_kind"] or "",
            proposition=_coerce_jsonb(m["proposition"]),
            natural=natural,
        )
        nodes.append(
            MapNode(
                id=mid,
                natural=natural,
                proposition_kind=m["proposition_kind"] or "",
                neighborhood_id=m["neighborhood_id"],
                confidence=float(m["confidence"] or 0.0),
                activation=float(m["activation"] or 0.0),
                status=m["status"],
                archive_reason=m["archive_reason"],
                health=health,
                band=band,
                in_degree=in_deg.get(mid, 0),
                out_degree=out_deg.get(mid, 0),
                topo_x=topo_x,
                topo_y=topo_y,
                created_at=m["created_at"],
            )
        )

    # 5b) Per-band cap. Reference visual (spec §4.2): sparse goals at
    #     the top, a handful of commitments / decisions / risks in the
    #     middle, customers fanned at the bottom. Keep the top-K per
    #     band ranked by activation × confidence, with a kind
    #     tiebreaker that promotes recommendations / concerns /
    #     patterns above plain state assertions. When a lens is set,
    #     that band is expanded to up to 30 nodes while others stay
    #     small so the eye locks onto the focused band.
    _OVERVIEW_CAP: dict[str, int] = {
        "goal": 2, "commitment": 3, "decision": 3, "risk": 3, "customer": 4,
    }
    _LENS_CAP = 30
    _KIND_RANK = {
        "recommendation": 5, "concern": 4, "prediction": 3,
        "pattern": 3, "capability_assessment": 2,
        "hypothesis": 2, "relation": 1, "state": 1,
        "market_assessment": 2, "pattern_instance": 2,
        "environmental_trend": 2,
    }
    def _node_rank(n: MapNode) -> tuple[float, int]:
        return (
            n.activation * n.confidence,
            _KIND_RANK.get(n.proposition_kind, 0),
        )
    by_band: dict[str, list[MapNode]] = {}
    for n in nodes:
        by_band.setdefault(n.band, []).append(n)

    # Total node counts per band BEFORE capping. The frontend turns
    # the gap between this total and the visible count into a
    # "+N more" overflow cluster card so the CEO can see scale.
    band_totals: dict[str, int] = {
        b: len(by_band.get(b, [])) for b in (
            "goal", "commitment", "decision", "risk", "customer",
        )
    }

    capped: list[MapNode] = []
    for b in ("goal", "commitment", "decision", "risk", "customer"):
        items = sorted(by_band.get(b, []), key=_node_rank, reverse=True)
        cap = _LENS_CAP if lens == b else _OVERVIEW_CAP[b]
        capped.extend(items[:cap])
    nodes = capped
    visible_ids = {n.id for n in nodes}

    # 5c) Synthesize the band-to-band hierarchy (spec §4.2 + §4.4).
    #     Pelago-style tenants ship no model_edges; without explicit
    #     edges the Model page is unreadable. We build the canonical
    #     downward flow:
    #         goal      --supports-->     commitment
    #         commitment --depends-on--> decision
    #         commitment --constrains--> risk
    #         decision  --depends-on-->  risk
    #         risk      --blocks-->      customer
    #     Edges within a band are skipped. We also keep any real
    #     model_edges that landed in the visible set so true
    #     analytical edges still show through.
    band_nodes: dict[str, list[MapNode]] = {b: [] for b in (
        "goal", "commitment", "decision", "risk", "customer",
    )}
    for n in nodes:
        if n.band in band_nodes:
            band_nodes[n.band].append(n)

    # Track which (src, tgt, kind) triples we've already emitted so
    # synthesized edges don't duplicate real ones we pulled above.
    have_edge: set[tuple[UUID, UUID, str]] = set()
    for er in edge_rows:
        have_edge.add((
            er["source_model_id"], er["target_model_id"], er["edge_kind"],
        ))

    synth: list[dict[str, Any]] = []

    def _add(src: MapNode, tgt: MapNode, kind: str) -> None:
        key = (src.id, tgt.id, kind)
        if key in have_edge:
            return
        have_edge.add(key)
        synth.append({
            "source_model_id": src.id,
            "target_model_id": tgt.id,
            "edge_kind": kind,
            "weight": None,
            "status": "active",
            "detected_by": "band_hierarchy",
        })

    # Single-edge spine: each upper-band node points to exactly one
    # node in the band below, staggered so parents don't pile on the
    # same child. This produces a readable downward flow (~9–12 edges
    # for the curated 15-node set) instead of full fan-out spaghetti.
    # When the user selects a node the inspector + trace endpoints
    # surface richer relationships on demand.
    def _pick(parent: MapNode, child_band: list[MapNode]) -> MapNode | None:
        if not child_band:
            return None
        start = (parent.id.int) % len(child_band)
        return child_band[start]

    for g in band_nodes["goal"]:
        target = _pick(g, band_nodes["commitment"])
        if target:
            _add(g, target, "supports")

    for c in band_nodes["commitment"]:
        target = _pick(c, band_nodes["decision"])
        if target:
            _add(c, target, "depends_on")

    # Decisions point into risks (a decision either depends on a risk
    # being resolved, or surfaces one). Skip if both bands are empty.
    for d in band_nodes["decision"]:
        target = _pick(d, band_nodes["risk"])
        if target:
            _add(d, target, "depends_on")

    for r in band_nodes["risk"]:
        target = _pick(r, band_nodes["customer"])
        if target:
            _add(r, target, "blocks")

    # Append synthesized edges to whatever real ones were fetched.
    edge_rows = list(edge_rows) + synth  # type: ignore[assignment]

    # 6) Build MapEdge list with crosses_neighborhood flag.
    nbh_by_model: dict[UUID, UUID | None] = {
        mid: m["neighborhood_id"] for mid, m in model_by_id.items()
    }
    edges: list[MapEdge] = []
    for er in edge_rows:
        src = er["source_model_id"]
        tgt = er["target_model_id"]
        src_nbh = nbh_by_model.get(src)
        tgt_nbh = nbh_by_model.get(tgt)
        crosses = _crosses_neighborhood(src_nbh, tgt_nbh)
        edges.append(
            MapEdge(
                source=src,
                target=tgt,
                kind=er["edge_kind"],
                weight=(
                    float(er["weight"])
                    if er["weight"] is not None
                    else None
                ),
                status=er["status"],
                detected_by=er["detected_by"],
                crosses_neighborhood=crosses,
            )
        )

    # 7) Neighborhoods — active only. Filter to ones referenced by
    #    visible models when neighborhood_id is set; otherwise all
    #    active neighborhoods for the tenant.
    nbh_filter_args: list[Any] = [tenant_id]
    nbh_extra = ""
    if neighborhood_id is not None:
        nbh_filter_args.append(neighborhood_id)
        nbh_extra = f" AND id = ${len(nbh_filter_args)}"
    nbh_rows = await pool.fetch(
        f"""
        SELECT id, named_signature, member_model_ids, density,
               status, last_recomputed_at
        FROM model_neighborhoods
        WHERE tenant_id = $1
          AND status = 'active'
          {nbh_extra}
        """,
        *nbh_filter_args,
    )

    # Recent event counts per neighborhood (last 7 days).
    seven_days_ago = now - timedelta(days=7)
    event_count_rows = await pool.fetch(
        """
        SELECT neighborhood_id, COUNT(*) AS n
        FROM topology_events
        WHERE tenant_id = $1
          AND occurred_at >= $2
          AND neighborhood_id IS NOT NULL
        GROUP BY neighborhood_id
        """,
        tenant_id,
        seven_days_ago,
    )
    event_count: dict[UUID, int] = {
        r["neighborhood_id"]: int(r["n"]) for r in event_count_rows
    }

    neighborhoods: list[MapNeighborhood] = []
    for nr in nbh_rows:
        member_ids = nr["member_model_ids"] or []
        # Centroid in 2D = mean of projected coords for members that
        # actually have coords (some may be missing if topo_embedding
        # is still null).
        xs: list[float] = []
        ys: list[float] = []
        for m in member_ids:
            coord = projection.get(str(m))
            if coord is not None:
                xs.append(coord[0])
                ys.append(coord[1])
        cx = sum(xs) / len(xs) if xs else None
        cy = sum(ys) / len(ys) if ys else None
        neighborhoods.append(
            MapNeighborhood(
                id=nr["id"],
                named_signature=nr["named_signature"],
                member_count=len(member_ids),
                density=(
                    float(nr["density"])
                    if nr["density"] is not None
                    else None
                ),
                status=nr["status"],
                last_recomputed_at=nr["last_recomputed_at"],
                centroid_x=cx,
                centroid_y=cy,
                hull_padding=60.0,
                recent_event_count=event_count.get(nr["id"], 0),
            )
        )

    # 8) change_summary — small aggregate over the period.
    change_summary = await _build_change_summary(
        pool=pool,
        tenant_id=tenant_id,
        since=summary_since,
        now=now,
        neighborhoods=neighborhoods,
    )

    return MapSnapshotResponse(
        nodes=nodes,
        edges=edges,
        neighborhoods=neighborhoods,
        change_summary=change_summary,
        projection_fitted_at=projection_fitted_at,
        projection_trustworthiness=projection_trustworthiness,
        server_now=now,
        band_totals=band_totals,
    )


async def _build_change_summary(
    *,
    pool,
    tenant_id: UUID,
    since: datetime,
    now: datetime,
    neighborhoods: list[MapNeighborhood],
) -> MapSnapshotChangeSummary:
    # Aggregate counts in parallel-ish batched queries.
    new_models = await pool.fetchval(
        """
        SELECT COUNT(*) FROM models
        WHERE tenant_id = $1 AND created_at >= $2
        """,
        tenant_id, since,
    )
    archived_models = await pool.fetchval(
        """
        SELECT COUNT(*) FROM models
        WHERE tenant_id = $1
          AND status != 'active'
          AND archived_at IS NOT NULL
          AND archived_at >= $2
        """,
        tenant_id, since,
    )
    new_edges = await pool.fetchval(
        """
        SELECT COUNT(*) FROM model_edges
        WHERE tenant_id = $1 AND created_at >= $2
        """,
        tenant_id, since,
    )
    phase_events = await pool.fetchval(
        """
        SELECT COUNT(*) FROM topology_events
        WHERE tenant_id = $1 AND occurred_at >= $2
        """,
        tenant_id, since,
    )
    contested_models = await pool.fetchval(
        """
        SELECT COUNT(*) FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND contested_count > confirmed_count
          AND contested_count > 0
        """,
        tenant_id,
    )
    new_models = int(new_models or 0)
    archived_models = int(archived_models or 0)
    new_edges = int(new_edges or 0)
    phase_events = int(phase_events or 0)
    contested_models = int(contested_models or 0)

    total_changes = new_models + archived_models + new_edges + phase_events

    # Pick a focus neighborhood if any has unusually high recent
    # activity.
    focus_id: UUID | None = None
    if neighborhoods:
        focus = max(
            neighborhoods,
            key=lambda n: n.recent_event_count,
        )
        if focus.recent_event_count > 0:
            focus_id = focus.id

    # Pre-render the headline.
    headline = _render_headline(
        total_changes=total_changes,
        phase_events=phase_events,
        since=since,
        now=now,
        last_change_at=await _last_change_at(pool, tenant_id),
    )

    return MapSnapshotChangeSummary(
        since=since,
        new_models=new_models,
        archived_models=archived_models,
        new_edges=new_edges,
        phase_events=phase_events,
        contested_models=contested_models,
        headline=headline,
        focus_neighborhood_id=focus_id,
    )


async def _last_change_at(pool, tenant_id: UUID) -> datetime | None:
    """Most recent of: model created, model archived, edge created,
    topology event. Returns None when the tenant has no activity."""
    candidates: list[datetime] = []
    rows = [
        await pool.fetchval(
            "SELECT MAX(created_at) FROM models WHERE tenant_id = $1",
            tenant_id,
        ),
        await pool.fetchval(
            "SELECT MAX(archived_at) FROM models "
            "WHERE tenant_id = $1 AND archived_at IS NOT NULL",
            tenant_id,
        ),
        await pool.fetchval(
            "SELECT MAX(created_at) FROM model_edges WHERE tenant_id = $1",
            tenant_id,
        ),
        await pool.fetchval(
            "SELECT MAX(occurred_at) FROM topology_events "
            "WHERE tenant_id = $1",
            tenant_id,
        ),
    ]
    for r in rows:
        if r is not None:
            candidates.append(r)
    return max(candidates) if candidates else None


def _render_headline(
    *,
    total_changes: int,
    phase_events: int,
    since: datetime,
    now: datetime,
    last_change_at: datetime | None,
) -> str:
    # Highest-priority signal: lots of phase events → neighborhoods
    # need attention.
    if phase_events > 5:
        return f"{phase_events} neighborhoods need attention"
    # High activity headline. Threshold of 12 matches the test
    # expectation (test_snapshot_change_summary_headline_high_activity).
    if total_changes >= 12:
        return f"{total_changes} changes since {_human_window(since, now)}"
    if total_changes > 0:
        return f"{total_changes} changes since {_human_window(since, now)}"
    # Stable system — pre-render the "last change N days ago" tail.
    if last_change_at is None:
        return "Your belief system is stable — no recorded changes yet"
    age_days = max(0, int((now - last_change_at).total_seconds() // 86400))
    if age_days <= 0:
        tail = "last change today"
    elif age_days == 1:
        tail = "last change 1 day ago"
    else:
        tail = f"last change {age_days} days ago"
    return f"Your belief system is stable — {tail}"


def _human_window(since: datetime, now: datetime) -> str:
    """Pre-render `since` as 'Monday' / '3 days ago' / etc.

    Conservative: always anchored to days. Day-of-week names when the
    window is 1-7 days; otherwise an explicit count.
    """
    delta_days = max(0, int((now - since).total_seconds() // 86400))
    if 1 <= delta_days <= 6:
        return since.strftime("%A")
    if delta_days == 7:
        return "last week"
    if delta_days == 0:
        return "today"
    return f"{delta_days} days ago"


# ---------------------------------------------------------------------
# Model story
# ---------------------------------------------------------------------


async def _build_model_story(
    *,
    pool,
    tenant_id: UUID,
    model_id: UUID,
) -> ModelStoryResponse | None:
    row = await pool.fetchrow(
        """
        SELECT
          m.id,
          m.proposition_kind,
          m."natural" AS natural,
          m.confidence,
          m.confidence_at_assertion,
          m.activation,
          m.status,
          m.archive_reason,
          m.created_at AS asserted_at,
          m.last_confirmed_at,
          m.contested_count,
          m.confirmed_count,
          m.signal_readings,
          m.falsifier,
          mnm.neighborhood_id AS neighborhood_id,
          mn.named_signature AS neighborhood_signature
        FROM models m
        LEFT JOIN model_neighborhood_membership mnm
          ON mnm.model_id = m.id AND mnm.tenant_id = m.tenant_id
        LEFT JOIN model_neighborhoods mn
          ON mn.id = mnm.neighborhood_id
        WHERE m.id = $1 AND m.tenant_id = $2
        """,
        model_id, tenant_id,
    )
    if row is None:
        return None

    # Falsifier — last checked is best-effort: use the most recent
    # signal reading's `at` if present, else None.
    signal_readings = _coerce_jsonb(row["signal_readings"]) or []
    falsifier = _coerce_jsonb(row["falsifier"])
    falsifier_summary = _summarize_falsifier(falsifier)
    falsifier_last_checked = _signal_max_at(signal_readings)

    # All edges touching this model — split by direction + kind.
    edge_rows = await pool.fetch(
        """
        SELECT e.source_model_id, e.target_model_id, e.edge_kind,
               e.weight,
               m_other.id AS other_id,
               m_other."natural" AS other_natural,
               mnm_other.neighborhood_id AS other_neighborhood_id,
               mn_other.named_signature AS other_neighborhood_signature
        FROM model_edges e
        JOIN models m_other
          ON m_other.id = (
            CASE WHEN e.source_model_id = $1
              THEN e.target_model_id ELSE e.source_model_id END
          )
        LEFT JOIN model_neighborhood_membership mnm_other
          ON mnm_other.model_id = m_other.id
         AND mnm_other.tenant_id = m_other.tenant_id
        LEFT JOIN model_neighborhoods mn_other
          ON mn_other.id = mnm_other.neighborhood_id
        WHERE e.tenant_id = $2
          AND e.status = 'active'
          AND (e.source_model_id = $1 OR e.target_model_id = $1)
        """,
        model_id, tenant_id,
    )

    supporting: list[StoryEdgeRef] = []
    contributing_to: list[StoryEdgeRef] = []
    instance_of: list[StoryEdgeRef] = []
    superseded_by: list[StoryEdgeRef] = []

    affects_count = 0
    for er in edge_rows:
        is_outbound = er["source_model_id"] == model_id
        if is_outbound:
            affects_count += 1
        ref = StoryEdgeRef(
            neighbor_id=er["other_id"],
            neighbor_natural=_truncate(er["other_natural"] or "", 80),
            neighbor_neighborhood_signature=er["other_neighborhood_signature"],
            edge_kind=er["edge_kind"],
            edge_weight=(
                float(er["weight"]) if er["weight"] is not None else None
            ),
        )
        kind = er["edge_kind"]
        if kind == "supports" and not is_outbound:
            # target = me, source supports me
            supporting.append(ref)
        elif kind == "contributes_to_resolution" and is_outbound:
            contributing_to.append(ref)
        elif kind == "instance_of" and is_outbound:
            instance_of.append(ref)
        elif kind == "superseded_by" and is_outbound:
            superseded_by.append(ref)

    # Recent activity — synthesise from status notes + signal readings
    # + edges.
    activity = await _build_recent_activity(
        pool=pool,
        tenant_id=tenant_id,
        model_id=model_id,
        signal_readings=signal_readings,
        edge_rows=edge_rows,
    )

    health = _classify_health(
        status=row["status"],
        created_at=row["asserted_at"],
        contested=int(row["contested_count"] or 0),
        confirmed=int(row["confirmed_count"] or 0),
        confidence=float(row["confidence"] or 0.0),
        activation=float(row["activation"] or 0.0),
        last_confirmed_at=row["last_confirmed_at"],
        now=datetime.now(timezone.utc),
    )

    return ModelStoryResponse(
        id=row["id"],
        proposition_kind=row["proposition_kind"] or "",
        natural=row["natural"] or "",
        confidence=float(row["confidence"] or 0.0),
        confidence_at_assertion=float(row["confidence_at_assertion"] or 0.0),
        activation=float(row["activation"] or 0.0),
        status=row["status"],
        archive_reason=row["archive_reason"],
        asserted_at=row["asserted_at"],
        last_confirmed_at=row["last_confirmed_at"],
        contested_count=int(row["contested_count"] or 0),
        confirmed_count=int(row["confirmed_count"] or 0),
        health=health,
        supporting=supporting,
        contributing_to=contributing_to,
        instance_of=instance_of,
        superseded_by=superseded_by,
        falsifier_summary=falsifier_summary,
        falsifier_last_checked_at=falsifier_last_checked,
        affects_count=affects_count,
        neighborhood_id=row["neighborhood_id"],
        neighborhood_signature=row["neighborhood_signature"],
        recent_activity=activity,
    )


async def _build_recent_activity(
    *,
    pool,
    tenant_id: UUID,
    model_id: UUID,
    signal_readings: list[Any],
    edge_rows: list[Any],
) -> list[StoryActivityEntry]:
    """Synthesise a unified activity log: status notes + most recent
    5 signal readings + edges (with their created_at). Render the most
    recent 8 across all sources."""
    entries: list[StoryActivityEntry] = []

    # 1) Status notes.
    note_rows = await pool.fetch(
        """
        SELECT id, note, kind, authored_by, authored_at
        FROM model_status_notes
        WHERE model_id = $1
        ORDER BY authored_at DESC
        LIMIT 12
        """,
        model_id,
    )
    for nr in note_rows:
        entries.append(
            StoryActivityEntry(
                occurred_at=nr["authored_at"],
                headline=_render_note_headline(nr),
                detail={
                    "kind": nr["kind"],
                    "note": nr["note"],
                    "authored_by": (
                        str(nr["authored_by"])
                        if nr["authored_by"] is not None
                        else None
                    ),
                },
            )
        )

    # 2) Signal readings — most recent 5.
    if isinstance(signal_readings, list) and signal_readings:
        sorted_sr = sorted(
            signal_readings,
            key=lambda s: s.get("at", "") if isinstance(s, dict) else "",
            reverse=True,
        )
        for sr in sorted_sr[:5]:
            if not isinstance(sr, dict):
                continue
            at_raw = sr.get("at")
            try:
                at = (
                    datetime.fromisoformat(at_raw)
                    if isinstance(at_raw, str)
                    else None
                )
            except ValueError:
                at = None
            if at is None:
                continue
            entries.append(
                StoryActivityEntry(
                    occurred_at=at,
                    headline=_render_signal_headline(sr),
                    detail=sr,
                )
            )

    # 3) Edges (when added).
    # edge_rows came from the same query as the story payload — no
    # `created_at`; we re-fetch the minimal info here so the activity
    # log can show edge additions chronologically.
    edge_create_rows = await pool.fetch(
        """
        SELECT edge_kind, source_model_id, target_model_id, created_at
        FROM model_edges
        WHERE tenant_id = $1
          AND status = 'active'
          AND (source_model_id = $2 OR target_model_id = $2)
        ORDER BY created_at DESC
        LIMIT 12
        """,
        tenant_id, model_id,
    )
    for er in edge_create_rows:
        is_outbound = er["source_model_id"] == model_id
        direction = "→" if is_outbound else "←"
        entries.append(
            StoryActivityEntry(
                occurred_at=er["created_at"],
                headline=(
                    f"edge {direction} added ({er['edge_kind']})"
                ),
                detail={
                    "edge_kind": er["edge_kind"],
                    "direction": "outbound" if is_outbound else "inbound",
                    "other_model_id": str(
                        er["target_model_id"]
                        if is_outbound
                        else er["source_model_id"]
                    ),
                },
            )
        )

    entries.sort(key=lambda e: e.occurred_at, reverse=True)
    return entries[:8]


def _render_note_headline(row: Any) -> str:
    note = row["note"] or ""
    short = note.strip().splitlines()[0][:80] if note.strip() else ""
    by = (
        f" by {str(row['authored_by'])[:8]}"
        if row["authored_by"] is not None
        else ""
    )
    if short:
        return f"{row['kind']}: {short}{by}"
    return f"{row['kind']} note{by}"


def _render_signal_headline(sr: dict) -> str:
    bits: list[str] = ["signal reading"]
    name = sr.get("name") or sr.get("kind")
    if name:
        bits.append(str(name))
    val = sr.get("value")
    if val is not None:
        bits.append(f"= {val}")
    return " ".join(bits)


def _summarize_falsifier(falsifier: Any) -> str | None:
    """Best-effort renderer for the falsifier JSONB.

    The schema is loose (kind/criteria/conditions vary by proposition
    kind). We try a few common shapes; otherwise fall back to a
    truncated JSON dump tagged with TODO.
    """
    if not falsifier:
        return None
    if isinstance(falsifier, dict):
        # Common shape: {kind: 'threshold', metric, op, value, window}
        kind = falsifier.get("kind") or falsifier.get("type")
        if kind in ("threshold", "metric_threshold"):
            metric = falsifier.get("metric") or "metric"
            op = falsifier.get("op") or "below"
            value = falsifier.get("value")
            window = falsifier.get("window") or falsifier.get("window_days")
            tail = f" within {window}" if window else ""
            return f"{metric} {op} {value}{tail}".strip()
        # Common shape: {kind: 'signal', signal, window}
        if kind in ("signal", "signal_observed"):
            signal = falsifier.get("signal") or falsifier.get("name")
            window = falsifier.get("window") or falsifier.get("window_days")
            tail = f" in next {window}" if window else ""
            return f"Any signal of {signal}{tail}".strip()
        # Common shape: {kind: 'confidence_drop', threshold, window}
        if kind == "confidence_drop":
            threshold = falsifier.get("threshold")
            window = falsifier.get("window") or falsifier.get("window_days")
            tail = f" within {window}" if window else ""
            return (
                f"Confidence drops below {threshold}{tail}".strip()
            )
        # Free-text description.
        if isinstance(falsifier.get("description"), str):
            return falsifier["description"][:200]
    # TODO: extend once falsifier schemas are formalised. For now
    # return a truncated stringification so the UI shows *something*.
    return json.dumps(falsifier, default=str)[:200] + " (TODO: render)"


def _signal_max_at(signal_readings: Any) -> datetime | None:
    if not isinstance(signal_readings, list):
        return None
    best: datetime | None = None
    for sr in signal_readings:
        if not isinstance(sr, dict):
            continue
        at = sr.get("at")
        if not isinstance(at, str):
            continue
        try:
            ts = datetime.fromisoformat(at)
        except ValueError:
            continue
        if best is None or ts > best:
            best = ts
    return best


# ---------------------------------------------------------------------
# Band classification (Model page §4.2)
# ---------------------------------------------------------------------


# Coarse mapping from proposition_kind to a Model-page band. The Model
# UI renders nodes in five horizontal bands (spec §4.2). We bucket the
# 11 known proposition_kinds into those bands. The "customer" band is
# preferred for any model whose natural text or proposition subject
# clearly references customers/accounts/renewal/churn — that check
# overrides the kind-based default.
_PROPOSITION_KIND_BAND: dict[str, str] = {
    # Top band: strategic recommendations.
    "recommendation": "goal",
    # Commitments band: assertions about company state / relations.
    "state": "commitment",
    "relation": "commitment",
    # Decisions band: open questions / forecasts.
    "prediction": "decision",
    "hypothesis": "decision",
    # Risks band: concerns, patterns, capacity constraints.
    "concern": "risk",
    "pattern": "risk",
    "pattern_instance": "risk",
    "environmental_trend": "risk",
    "capability_assessment": "risk",
    # Customer band: market-facing assessments.
    "market_assessment": "customer",
}


# Tokens whose presence in the natural / proposition subject promote a
# node into the "customer" band regardless of its kind. Kept small and
# explicit — broad text matching would mis-bucket commitments that
# happen to mention a customer name in passing.
_CUSTOMER_TOKENS: tuple[str, ...] = (
    "customer", "account", "renewal", "churn",
    "anchor renewal", "support burden", "revenue at risk",
)


def _classify_band(
    *,
    proposition_kind: str,
    proposition: Any,
    natural: str,
) -> str:
    """Map a Model to one of the five Model-page bands.

    Order:
      1. Natural-text prefix patterns ("Goal G-", "Decision D-",
         "Commitment ", "Risk R-") take precedence over kind so
         Pelago-style labelled entities land in the right band.
      2. Explicit customer/market signal in natural / proposition →
         "customer".
      3. proposition_kind in `_PROPOSITION_KIND_BAND` → mapped band.
      4. Fallback → "commitment".
    """
    nat = (natural or "").strip()
    if nat.startswith("Goal G-"):
        return "goal"
    if nat.startswith("Decision D-"):
        return "decision"
    if nat.startswith("Commitment ") or nat.startswith("Commitment-"):
        return "commitment"
    if nat.startswith("Risk R-"):
        return "risk"

    blob = nat.lower()
    if isinstance(proposition, dict):
        for key in ("subject", "subject_external", "about"):
            v = proposition.get(key)
            if isinstance(v, str):
                blob = f"{blob} {v.lower()}"
            elif isinstance(v, dict):
                t = v.get("type") or v.get("kind") or v.get("entity_kind")
                if isinstance(t, str):
                    blob = f"{blob} {t.lower()}"
    if any(tok in blob for tok in _CUSTOMER_TOKENS):
        return "customer"
    return _PROPOSITION_KIND_BAND.get(proposition_kind, "commitment")


# ---------------------------------------------------------------------
# Health classification
# ---------------------------------------------------------------------


def _classify_health(
    *,
    status: str,
    created_at: datetime,
    contested: int,
    confirmed: int,
    confidence: float,
    activation: float,
    last_confirmed_at: datetime | None,
    now: datetime,
) -> str:
    """Pure classifier per the spec in services/gateway/map_router.py
    docstring + V1 PR prompt. Order matters — check archived first,
    then fresh, then contested, then solid, then fading, then stable.
    """
    if status != "active":
        return "archived"
    age = now - _ensure_aware(created_at)
    if age <= timedelta(days=7):
        return "fresh"
    if contested > confirmed and contested > 0:
        return "contested"
    if confidence >= 0.7 and confirmed >= contested:
        return "solid"
    last_conf = _ensure_aware(last_confirmed_at) if last_confirmed_at else None
    stale = (
        last_conf is not None
        and (now - last_conf) > timedelta(days=30)
    )
    if activation < 0.3 or stale:
        return "fading"
    return "stable"


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _crosses_neighborhood(
    src_nbh: UUID | None, tgt_nbh: UUID | None
) -> bool:
    """True when source and target are in different clusters.

    Treats `None` (unclustered singleton) as "different cluster" — two
    singletons cross because they share no neighborhood; a singleton +
    a clustered model also crosses.
    """
    if src_nbh is None or tgt_nbh is None:
        return True
    return src_nbh != tgt_nbh


def _truncate(s: str, limit: int) -> str:
    if len(s) <= limit:
        return s
    # 3 chars for the ellipsis
    return s[: max(0, limit - 1)] + "\u2026"


def _parse_since(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        # Accept trailing 'Z' (Python <3.11 quirk in some environments).
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _coerce_jsonb(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (TypeError, ValueError):
            return v
    return v


def _auth_or_none(request: Request) -> AuthContext | None:
    return getattr(request.state, "auth", None)


def _unauth() -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized"},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


def _bad_request(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "bad_request", "reason": reason},
        status_code=status.HTTP_400_BAD_REQUEST,
    )


def _pydantic_dump(model) -> Any:
    """Pydantic v2: dump → JSON-serialisable Python (UUIDs/datetimes
    become strings).
    """
    return json.loads(model.model_dump_json())


__all__ = ["register_map_routes"]
