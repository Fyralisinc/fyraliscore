"""
services/app/gateway/map_routes.py — HTTP handlers for the CEO Map view.

The wire contract lives in `services/app/gateway/map_router.py` (Pydantic
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

from services.app.gateway.auth import AuthContext
from services.app.gateway.map_router import (
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
from services.platform.access_control.audit import (
    record_override_if_needed as record_access_override_if_needed,
)
from services.platform.access_control.checks import (
    AccessDecision,
    can_read,
)
from services.platform.access_control.roles import has_role
from services.platform.operator_action_audit import record_operator_action
from services.reasoning.topology.umap_projector import UMAPProjector


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
        from services.app.gateway.deps import get_gateway_deps

        deps = get_gateway_deps(request)

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
            auth=auth,
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
        from services.app.gateway.deps import get_gateway_deps

        deps = get_gateway_deps(request)
        qp = request.query_params
        since = _parse_since(qp.get("since"))
        if since is None:
            since = datetime.now(timezone.utc) - timedelta(days=7)
        try:
            limit = int(qp.get("limit", "50"))
        except (ValueError, TypeError):
            return _bad_request("invalid_limit")
        limit = max(1, min(limit, 200))

        visible_model_ids = await _fetch_visible_model_ids(deps.pool, auth)
        if not visible_model_ids:
            resp = TopologyEventsResponse(
                events=[],
                server_now=datetime.now(timezone.utc),
            )
            return JSONResponse(_pydantic_dump(resp))

        rows = await deps.pool.fetch(
            """
            SELECT te.id, te.kind, te.occurred_at, te.neighborhood_id,
                   te.named_signature, te.magnitude, te.payload,
                   te.member_model_ids,
                   mn.named_signature AS neighborhood_named_signature,
                   mn.member_model_ids AS neighborhood_member_model_ids
            FROM topology_events te
            LEFT JOIN model_neighborhoods mn
              ON mn.id = te.neighborhood_id
            WHERE te.tenant_id = $1
              AND te.occurred_at >= $2
              AND (
                te.member_model_ids && $3::uuid[]
                OR mn.member_model_ids && $3::uuid[]
              )
            ORDER BY te.occurred_at DESC
            LIMIT $4
            """,
            auth.tenant_id,
            since,
            visible_model_ids,
            limit,
        )
        visible_id_set = set(visible_model_ids)
        events: list[TopologyEventEntry] = []
        for r in rows:
            event_member_ids = set(r["member_model_ids"] or [])
            nbh_member_ids = set(r["neighborhood_member_model_ids"] or [])
            payload_members = event_member_ids or nbh_member_ids
            named_signature = (
                r["named_signature"]
                if _all_visible(event_member_ids, visible_id_set)
                else None
            ) or (
                r["neighborhood_named_signature"]
                if _all_visible(nbh_member_ids, visible_id_set)
                else None
            )
            payload = _coerce_jsonb(r["payload"]) or {}
            if not _all_visible(payload_members, visible_id_set):
                payload = {}
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
        from services.app.gateway.deps import get_gateway_deps

        deps = get_gateway_deps(request)
        try:
            mid = UUID(model_id)
        except (ValueError, TypeError):
            return _bad_request("invalid_model_id")
        story = await _build_model_story(
            pool=deps.pool, auth=auth, model_id=mid,
        )
        if story is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return JSONResponse(_pydantic_dump(story))

    @app.post("/map/refresh_projection")
    async def post_refresh_projection(request: Request) -> JSONResponse:
        auth = _auth_or_none(request)
        if auth is None:
            return _unauth()
        from services.app.gateway.deps import get_gateway_deps

        deps = get_gateway_deps(request)
        async with deps.pool.acquire() as conn:
            if not await _can_refresh_projection(auth, conn=conn):
                return JSONResponse(
                    {
                        "error": "forbidden",
                        "reason": "projection_refresh_requires_admin_or_leadership",
                    },
                    status_code=status.HTTP_403_FORBIDDEN,
                )
        projector = UMAPProjector(deps.pool)
        cache = await projector.refresh(auth.tenant_id)
        visible_model_ids = await _fetch_visible_model_ids(deps.pool, auth)
        # `cache["fitted_at"]` is an ISO string; coerce to datetime
        # for the Pydantic model.
        fitted_at_raw = cache.get("fitted_at")
        if isinstance(fitted_at_raw, str):
            fitted_at = datetime.fromisoformat(fitted_at_raw)
        else:
            fitted_at = fitted_at_raw or datetime.now(timezone.utc)
        resp = RefreshProjectionResponse(
            fitted_at=fitted_at,
            model_count=len(visible_model_ids),
            trustworthiness=float(cache.get("trustworthiness") or 0.0),
            n_neighbors=int(cache.get("n_neighbors") or 15),
            min_dist=float(cache.get("min_dist") or 0.15),
        )
        async with deps.pool.acquire() as conn:
            await record_operator_action(
                conn,
                tenant_id=auth.tenant_id,
                actor_id=auth.actor_id,
                action="map.projection.refresh",
                resource_type="map_projection",
                metadata={
                    "model_count": resp.model_count,
                    "trustworthiness": resp.trustworthiness,
                    "n_neighbors": resp.n_neighbors,
                    "min_dist": resp.min_dist,
                    "fitted_at": resp.fitted_at.isoformat(),
                },
            )
        return JSONResponse(_pydantic_dump(resp))


# ---------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------


async def _read_projection_context(
    pool,
    tenant_id: UUID,
) -> tuple[dict[str, tuple[float, float]], datetime | None, float | None]:
    projector = UMAPProjector(pool)
    projection = await projector.project(tenant_id)
    cache_meta = await projector.read_cache_meta(tenant_id)
    projection_fitted_at: datetime | None = None
    projection_trustworthiness: float | None = None
    if cache_meta and cache_meta.get("fitted_at"):
        try:
            projection_fitted_at = datetime.fromisoformat(cache_meta["fitted_at"])
        except (ValueError, TypeError):
            projection_fitted_at = None
        trust = cache_meta.get("trustworthiness")
        if trust is not None:
            try:
                projection_trustworthiness = float(trust)
            except (ValueError, TypeError):
                projection_trustworthiness = None
    return projection, projection_fitted_at, projection_trustworthiness


async def _fetch_snapshot_models(
    *,
    pool,
    auth: AuthContext,
    neighborhood_id: UUID | None,
    include_archived: bool,
    since: datetime | None,
) -> dict[UUID, dict[str, Any]]:
    status_filter = "" if include_archived else " AND m.status = 'active'"
    since_filter = ""
    args: list[Any] = [auth.tenant_id]
    if since is not None:
        args.append(since)
        since_filter = f" AND m.created_at >= ${len(args)}"
    nbh_filter = ""
    if neighborhood_id is not None:
        args.append(neighborhood_id)
        nbh_filter = f" AND mnm.neighborhood_id = ${len(args)}"

    rows = await pool.fetch(
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
          m.visible_to_subjects,
          m.scope_actors,
          m.scope_entities,
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
    out: dict[UUID, dict[str, Any]] = {}
    async with pool.acquire() as conn:
        for row in rows:
            rec = dict(row)
            decision = await can_read(
                auth.actor_id,
                _model_entity(rec, auth.tenant_id),
                conn=conn,
                tenant_id=auth.tenant_id,
            )
            if not decision.allowed:
                continue
            await _record_model_override_if_needed(
                decision,
                conn=conn,
                auth=auth,
                model_id=rec["id"],
            )
            out[rec["id"]] = rec
    return out


async def _fetch_snapshot_edges(
    *,
    pool,
    tenant_id: UUID,
    edge_kinds: tuple[str, ...],
    model_ids: list[UUID],
) -> list[Any]:
    if not model_ids:
        return []
    return list(
        await pool.fetch(
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
    )


def _degree_maps(edge_rows: list[Any]) -> tuple[dict[UUID, int], dict[UUID, int]]:
    in_deg: dict[UUID, int] = {}
    out_deg: dict[UUID, int] = {}
    for row in edge_rows:
        out_deg[row["source_model_id"]] = out_deg.get(row["source_model_id"], 0) + 1
        in_deg[row["target_model_id"]] = in_deg.get(row["target_model_id"], 0) + 1
    return in_deg, out_deg


def _build_snapshot_nodes(
    *,
    model_by_id: dict[UUID, dict[str, Any]],
    projection: dict[str, tuple[float, float]],
    in_deg: dict[UUID, int],
    out_deg: dict[UUID, int],
    now: datetime,
) -> list[MapNode]:
    nodes: list[MapNode] = []
    for mid, model in model_by_id.items():
        coord = projection.get(str(mid))
        natural = _truncate(model["natural"] or "", 100)
        nodes.append(
            MapNode(
                id=mid,
                natural=natural,
                proposition_kind=model["proposition_kind"] or "",
                neighborhood_id=model["neighborhood_id"],
                confidence=float(model["confidence"] or 0.0),
                activation=float(model["activation"] or 0.0),
                status=model["status"],
                archive_reason=model["archive_reason"],
                health=_classify_health(
                    status=model["status"],
                    created_at=model["created_at"],
                    contested=int(model["contested_count"] or 0),
                    confirmed=int(model["confirmed_count"] or 0),
                    confidence=float(model["confidence"] or 0.0),
                    activation=float(model["activation"] or 0.0),
                    last_confirmed_at=model["last_confirmed_at"],
                    now=now,
                ),
                band=_classify_band(
                    proposition_kind=model["proposition_kind"] or "",
                    proposition=_coerce_jsonb(model["proposition"]),
                    natural=natural,
                ),
                in_degree=in_deg.get(mid, 0),
                out_degree=out_deg.get(mid, 0),
                topo_x=coord[0] if coord else None,
                topo_y=coord[1] if coord else None,
                created_at=model["created_at"],
            )
        )
    return nodes


def _cap_snapshot_nodes(
    nodes: list[MapNode],
    lens: str | None,
) -> tuple[list[MapNode], dict[str, int]]:
    overview_cap = {
        "goal": 2,
        "commitment": 3,
        "decision": 3,
        "risk": 3,
        "customer": 4,
    }
    kind_rank = {
        "recommendation": 5,
        "concern": 4,
        "prediction": 3,
        "situation": 4,
        "pattern": 3,
        "capability_assessment": 2,
        "hypothesis": 2,
        "relation": 1,
        "state": 1,
        "market_assessment": 2,
        "pattern_instance": 2,
        "environmental_trend": 2,
    }

    def node_rank(node: MapNode) -> tuple[float, int]:
        return (
            node.activation * node.confidence,
            kind_rank.get(node.proposition_kind, 0),
        )

    by_band: dict[str, list[MapNode]] = {}
    for node in nodes:
        by_band.setdefault(node.band, []).append(node)
    bands = ("goal", "commitment", "decision", "risk", "customer")
    band_totals = {band: len(by_band.get(band, [])) for band in bands}
    capped: list[MapNode] = []
    for band in bands:
        cap = 30 if lens == band else overview_cap[band]
        capped.extend(sorted(by_band.get(band, []), key=node_rank, reverse=True)[:cap])
    return capped, band_totals


def _with_band_hierarchy_edges(
    edge_rows: list[Any],
    nodes: list[MapNode],
) -> list[Any]:
    band_nodes: dict[str, list[MapNode]] = {
        band: [] for band in ("goal", "commitment", "decision", "risk", "customer")
    }
    for node in nodes:
        if node.band in band_nodes:
            band_nodes[node.band].append(node)

    have_edge = {
        (row["source_model_id"], row["target_model_id"], row["edge_kind"])
        for row in edge_rows
    }
    synth: list[dict[str, Any]] = []

    def add(src: MapNode, tgt: MapNode, kind: str) -> None:
        key = (src.id, tgt.id, kind)
        if key in have_edge:
            return
        have_edge.add(key)
        synth.append(
            {
                "source_model_id": src.id,
                "target_model_id": tgt.id,
                "edge_kind": kind,
                "weight": None,
                "status": "active",
                "detected_by": "band_hierarchy",
            }
        )

    def pick(parent: MapNode, child_band: list[MapNode]) -> MapNode | None:
        if not child_band:
            return None
        return child_band[parent.id.int % len(child_band)]

    for goal in band_nodes["goal"]:
        if target := pick(goal, band_nodes["commitment"]):
            add(goal, target, "supports")
    for commitment in band_nodes["commitment"]:
        if target := pick(commitment, band_nodes["decision"]):
            add(commitment, target, "depends_on")
    for decision in band_nodes["decision"]:
        if target := pick(decision, band_nodes["risk"]):
            add(decision, target, "depends_on")
    for risk in band_nodes["risk"]:
        if target := pick(risk, band_nodes["customer"]):
            add(risk, target, "blocks")

    return [*edge_rows, *synth]


def _build_snapshot_edges(
    *,
    edge_rows: list[Any],
    model_by_id: dict[UUID, dict[str, Any]],
) -> list[MapEdge]:
    nbh_by_model = {
        mid: model["neighborhood_id"] for mid, model in model_by_id.items()
    }
    edges: list[MapEdge] = []
    for row in edge_rows:
        src = row["source_model_id"]
        tgt = row["target_model_id"]
        edges.append(
            MapEdge(
                source=src,
                target=tgt,
                kind=row["edge_kind"],
                weight=float(row["weight"]) if row["weight"] is not None else None,
                status=row["status"],
                detected_by=row["detected_by"],
                crosses_neighborhood=_crosses_neighborhood(
                    nbh_by_model.get(src),
                    nbh_by_model.get(tgt),
                ),
            )
        )
    return edges


async def _build_snapshot_neighborhoods(
    *,
    pool,
    tenant_id: UUID,
    neighborhood_id: UUID | None,
    projection: dict[str, tuple[float, float]],
    now: datetime,
    visible_model_ids: set[UUID],
) -> list[MapNeighborhood]:
    if not visible_model_ids:
        return []
    args: list[Any] = [tenant_id]
    nbh_extra = ""
    if neighborhood_id is not None:
        args.append(neighborhood_id)
        nbh_extra = f" AND id = ${len(args)}"
    nbh_rows = await pool.fetch(
        f"""
        SELECT id, named_signature, member_model_ids, density,
               status, last_recomputed_at
        FROM model_neighborhoods
        WHERE tenant_id = $1
          AND status = 'active'
          {nbh_extra}
        """,
        *args,
    )
    visible_neighborhood_ids: set[UUID] = set()
    for row in nbh_rows:
        member_ids = set(row["member_model_ids"] or [])
        if member_ids & visible_model_ids:
            visible_neighborhood_ids.add(row["id"])

    event_count_rows = await pool.fetch(
        """
        SELECT neighborhood_id, COUNT(*) AS n
        FROM topology_events
        WHERE tenant_id = $1
          AND occurred_at >= $2
          AND neighborhood_id IS NOT NULL
          AND (
            member_model_ids && $3::uuid[]
            OR neighborhood_id = ANY($4::uuid[])
          )
        GROUP BY neighborhood_id
        """,
        tenant_id,
        now - timedelta(days=7),
        list(visible_model_ids),
        list(visible_neighborhood_ids),
    )
    event_count = {
        row["neighborhood_id"]: int(row["n"]) for row in event_count_rows
    }
    neighborhoods: list[MapNeighborhood] = []
    for row in nbh_rows:
        raw_member_ids = set(row["member_model_ids"] or [])
        member_ids = raw_member_ids & visible_model_ids
        if not member_ids:
            continue
        coords = [
            coord
            for member_id in member_ids
            if (coord := projection.get(str(member_id))) is not None
        ]
        neighborhoods.append(
            MapNeighborhood(
                id=row["id"],
                named_signature=(
                    row["named_signature"]
                    if _all_visible(raw_member_ids, visible_model_ids)
                    else None
                ),
                member_count=len(member_ids),
                density=float(row["density"]) if row["density"] is not None else None,
                status=row["status"],
                last_recomputed_at=row["last_recomputed_at"],
                centroid_x=sum(coord[0] for coord in coords) / len(coords)
                if coords
                else None,
                centroid_y=sum(coord[1] for coord in coords) / len(coords)
                if coords
                else None,
                hull_padding=60.0,
                recent_event_count=event_count.get(row["id"], 0),
            )
        )
    return neighborhoods


async def _build_snapshot(
    *,
    pool,
    auth: AuthContext,
    neighborhood_id: UUID | None,
    edge_kinds: tuple[str, ...],
    include_archived: bool,
    since: datetime | None,
    summary_since: datetime,
    now: datetime,
    lens: str | None = None,
) -> MapSnapshotResponse:
    tenant_id = auth.tenant_id
    projection, projection_fitted_at, projection_trustworthiness = (
        await _read_projection_context(pool, tenant_id)
    )
    model_by_id = await _fetch_snapshot_models(
        pool=pool,
        auth=auth,
        neighborhood_id=neighborhood_id,
        include_archived=include_archived,
        since=since,
    )
    visible_model_ids = set(model_by_id.keys())
    edge_rows = await _fetch_snapshot_edges(
        pool=pool,
        tenant_id=tenant_id,
        edge_kinds=edge_kinds,
        model_ids=list(model_by_id.keys()),
    )
    in_deg, out_deg = _degree_maps(edge_rows)
    all_nodes = _build_snapshot_nodes(
        model_by_id=model_by_id,
        projection=projection,
        in_deg=in_deg,
        out_deg=out_deg,
        now=now,
    )
    nodes, band_totals = _cap_snapshot_nodes(all_nodes, lens)
    edges = _build_snapshot_edges(
        edge_rows=_with_band_hierarchy_edges(edge_rows, nodes),
        model_by_id=model_by_id,
    )
    neighborhoods = await _build_snapshot_neighborhoods(
        pool=pool,
        tenant_id=tenant_id,
        neighborhood_id=neighborhood_id,
        projection=projection,
        now=now,
        visible_model_ids=visible_model_ids,
    )
    change_summary = await _build_change_summary(
        pool=pool,
        tenant_id=tenant_id,
        since=summary_since,
        now=now,
        neighborhoods=neighborhoods,
        visible_model_ids=visible_model_ids,
    )

    return MapSnapshotResponse(
        nodes=nodes,
        edges=edges,
        neighborhoods=neighborhoods,
        change_summary=change_summary,
        degraded_reasons=_snapshot_degraded_reasons(
            visible_model_count=len(visible_model_ids),
            projection_fitted_at=projection_fitted_at,
            neighborhoods=neighborhoods,
        ),
        projection_fitted_at=projection_fitted_at,
        projection_trustworthiness=projection_trustworthiness,
        server_now=now,
        band_totals=band_totals,
    )


def _snapshot_degraded_reasons(
    *,
    visible_model_count: int,
    projection_fitted_at: datetime | None,
    neighborhoods: list[MapNeighborhood],
) -> list[str]:
    reasons: list[str] = []
    if visible_model_count == 0:
        return ["no_visible_models"]
    if projection_fitted_at is None:
        reasons.append("projection_warming")
    if not neighborhoods:
        reasons.append("topology_warming")
    return reasons


async def _build_change_summary(
    *,
    pool,
    tenant_id: UUID,
    since: datetime,
    now: datetime,
    neighborhoods: list[MapNeighborhood],
    visible_model_ids: set[UUID],
) -> MapSnapshotChangeSummary:
    visible_ids = list(visible_model_ids)
    # Aggregate counts in parallel-ish batched queries.
    if visible_ids:
        new_models = await pool.fetchval(
            """
            SELECT COUNT(*) FROM models
            WHERE tenant_id = $1
              AND id = ANY($3::uuid[])
              AND created_at >= $2
            """,
            tenant_id, since, visible_ids,
        )
        archived_models = await pool.fetchval(
            """
            SELECT COUNT(*) FROM models
            WHERE tenant_id = $1
              AND id = ANY($3::uuid[])
              AND status != 'active'
              AND archived_at IS NOT NULL
              AND archived_at >= $2
            """,
            tenant_id, since, visible_ids,
        )
        new_edges = await pool.fetchval(
            """
            SELECT COUNT(*) FROM model_edges
            WHERE tenant_id = $1
              AND created_at >= $2
              AND source_model_id = ANY($3::uuid[])
              AND target_model_id = ANY($3::uuid[])
            """,
            tenant_id, since, visible_ids,
        )
        visible_neighborhood_ids = [n.id for n in neighborhoods]
        phase_events = await pool.fetchval(
            """
            SELECT COUNT(*) FROM topology_events
            WHERE tenant_id = $1
              AND occurred_at >= $2
              AND (
                member_model_ids && $3::uuid[]
                OR neighborhood_id = ANY($4::uuid[])
              )
            """,
            tenant_id, since, visible_ids, visible_neighborhood_ids,
        )
        contested_models = await pool.fetchval(
            """
            SELECT COUNT(*) FROM models
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
              AND status = 'active'
              AND contested_count > confirmed_count
              AND contested_count > 0
            """,
            tenant_id, visible_ids,
        )
    else:
        new_models = 0
        archived_models = 0
        new_edges = 0
        phase_events = 0
        contested_models = 0
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
        last_change_at=await _last_change_at(pool, tenant_id, visible_model_ids),
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


async def _last_change_at(
    pool,
    tenant_id: UUID,
    visible_model_ids: set[UUID],
) -> datetime | None:
    """Most recent of: model created, model archived, edge created,
    topology event. Returns None when the tenant has no activity."""
    if not visible_model_ids:
        return None
    visible_ids = list(visible_model_ids)
    candidates: list[datetime] = []
    rows = [
        await pool.fetchval(
            """
            SELECT MAX(created_at) FROM models
            WHERE tenant_id = $1 AND id = ANY($2::uuid[])
            """,
            tenant_id, visible_ids,
        ),
        await pool.fetchval(
            """
            SELECT MAX(archived_at) FROM models
            WHERE tenant_id = $1
              AND id = ANY($2::uuid[])
              AND archived_at IS NOT NULL
            """,
            tenant_id, visible_ids,
        ),
        await pool.fetchval(
            """
            SELECT MAX(created_at) FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = ANY($2::uuid[])
              AND target_model_id = ANY($2::uuid[])
            """,
            tenant_id, visible_ids,
        ),
        await pool.fetchval(
            """
            SELECT MAX(occurred_at) FROM topology_events
            WHERE tenant_id = $1
              AND member_model_ids && $2::uuid[]
            """,
            tenant_id, visible_ids,
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
    auth: AuthContext,
    model_id: UUID,
) -> ModelStoryResponse | None:
    tenant_id = auth.tenant_id
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
              m.id,
              m.tenant_id,
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
              m.visible_to_subjects,
              m.scope_actors,
              m.scope_entities,
              mnm.neighborhood_id AS neighborhood_id,
              mn.named_signature AS neighborhood_signature,
              mn.member_model_ids AS neighborhood_member_model_ids
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
        decision = await can_read(
            auth.actor_id,
            _model_entity(dict(row), tenant_id),
            conn=conn,
            tenant_id=tenant_id,
        )
        if not decision.allowed:
            return None
        await _record_model_override_if_needed(
            decision,
            conn=conn,
            auth=auth,
            model_id=model_id,
        )

    # Falsifier — last checked is best-effort: use the most recent
    # signal reading's `at` if present, else None.
    signal_readings = _coerce_jsonb(row["signal_readings"]) or []
    falsifier = _coerce_jsonb(row["falsifier"])
    falsifier_summary = _summarize_falsifier(falsifier)
    falsifier_last_checked = _signal_max_at(signal_readings)

    # All edges touching this model — split by direction + kind.
    edge_rows_raw = await pool.fetch(
        """
        SELECT e.source_model_id, e.target_model_id, e.edge_kind,
               e.weight,
               m_other.id AS other_id,
               m_other.tenant_id AS other_tenant_id,
               m_other."natural" AS other_natural,
               m_other.visible_to_subjects AS other_visible_to_subjects,
               m_other.scope_actors AS other_scope_actors,
               m_other.scope_entities AS other_scope_entities,
               mnm_other.neighborhood_id AS other_neighborhood_id,
               mn_other.named_signature AS other_neighborhood_signature,
               mn_other.member_model_ids AS other_neighborhood_member_model_ids
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
    edge_rows: list[Any] = []
    visible_neighbor_ids: set[UUID] = set()
    async with pool.acquire() as conn:
        for er in edge_rows_raw:
            other = {
                "id": er["other_id"],
                "tenant_id": er["other_tenant_id"],
                "visible_to_subjects": er["other_visible_to_subjects"],
                "scope_actors": er["other_scope_actors"],
                "scope_entities": er["other_scope_entities"],
            }
            decision = await can_read(
                auth.actor_id,
                _model_entity(other, tenant_id),
                conn=conn,
                tenant_id=tenant_id,
            )
            if not decision.allowed:
                continue
            await _record_model_override_if_needed(
                decision,
                conn=conn,
                auth=auth,
                model_id=er["other_id"],
            )
            edge_rows.append(er)
            visible_neighbor_ids.add(er["other_id"])

    supporting: list[StoryEdgeRef] = []
    contributing_to: list[StoryEdgeRef] = []
    instance_of: list[StoryEdgeRef] = []
    superseded_by: list[StoryEdgeRef] = []

    affects_count = 0
    for er in edge_rows:
        is_outbound = er["source_model_id"] == model_id
        if is_outbound:
            affects_count += 1
        other_members = set(er["other_neighborhood_member_model_ids"] or [])
        visible_context_ids = visible_neighbor_ids | {model_id}
        ref = StoryEdgeRef(
            neighbor_id=er["other_id"],
            neighbor_natural=_truncate(er["other_natural"] or "", 80),
            neighbor_neighborhood_signature=(
                er["other_neighborhood_signature"]
                if _all_visible(other_members, visible_context_ids)
                else None
            ),
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
        visible_neighbor_ids=visible_neighbor_ids,
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
        neighborhood_signature=(
            row["neighborhood_signature"]
            if _all_visible(
                set(row["neighborhood_member_model_ids"] or []),
                visible_neighbor_ids | {model_id},
            )
            else None
        ),
        recent_activity=activity,
    )


async def _build_recent_activity(
    *,
    pool,
    tenant_id: UUID,
    model_id: UUID,
    signal_readings: list[Any],
    visible_neighbor_ids: set[UUID],
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

    # 3) Edges (when added). Restrict to neighbors already proven
    # visible to avoid leaking hidden model ids through activity rows.
    edge_create_rows: list[Any] = []
    if visible_neighbor_ids:
        edge_create_rows = list(
            await pool.fetch(
                """
                SELECT edge_kind, source_model_id, target_model_id, created_at
                FROM model_edges
                WHERE tenant_id = $1
                  AND status = 'active'
                  AND (
                    (
                      source_model_id = $2
                      AND target_model_id = ANY($3::uuid[])
                    )
                    OR (
                      target_model_id = $2
                      AND source_model_id = ANY($3::uuid[])
                    )
                  )
                ORDER BY created_at DESC
                LIMIT 12
                """,
                tenant_id, model_id, list(visible_neighbor_ids),
            )
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
# known proposition_kinds into those bands. The "customer" band is
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
    "situation": "risk",
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
         prefix-labelled entities land in the right band.
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
    """Pure classifier per the spec in services/app/gateway/map_router.py
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


async def _fetch_visible_model_ids(pool, auth: AuthContext) -> list[UUID]:
    rows = await pool.fetch(
        """
        SELECT id, tenant_id, visible_to_subjects, scope_actors, scope_entities
        FROM models
        WHERE tenant_id = $1
        """,
        auth.tenant_id,
    )
    visible: list[UUID] = []
    async with pool.acquire() as conn:
        for row in rows:
            rec = dict(row)
            decision = await can_read(
                auth.actor_id,
                _model_entity(rec, auth.tenant_id),
                conn=conn,
                tenant_id=auth.tenant_id,
            )
            if not decision.allowed:
                continue
            await _record_model_override_if_needed(
                decision,
                conn=conn,
                auth=auth,
                model_id=rec["id"],
            )
            visible.append(rec["id"])
    return visible


def _model_entity(row: dict[str, Any], tenant_id: UUID) -> dict[str, Any]:
    return {
        "kind": "model",
        "id": row["id"],
        "tenant_id": row.get("tenant_id") or tenant_id,
        "visible_to_subjects": row.get("visible_to_subjects"),
        "scope_actors": row.get("scope_actors") or [],
        "scope_entities": row.get("scope_entities") or [],
    }


async def _record_model_override_if_needed(
    decision: AccessDecision,
    *,
    conn: Any,
    auth: AuthContext,
    model_id: UUID,
) -> None:
    await record_access_override_if_needed(
        decision,
        actor_id=auth.actor_id,
        entity_type="model",
        entity_id=model_id,
        conn=conn,
        tenant_id=auth.tenant_id,
    )


async def _can_refresh_projection(auth: AuthContext, *, conn: Any) -> bool:
    return bool(
        await has_role(
            auth.actor_id,
            "admin",
            conn=conn,
            tenant_id=auth.tenant_id,
        )
        or await has_role(
            auth.actor_id,
            "leadership",
            conn=conn,
            tenant_id=auth.tenant_id,
        )
    )


def _all_visible(member_ids: set[UUID], visible_model_ids: set[UUID]) -> bool:
    return not member_ids or member_ids <= visible_model_ids


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
