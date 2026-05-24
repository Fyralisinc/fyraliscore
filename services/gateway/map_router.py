"""
services/gateway/map_router.py — endpoints powering the CEO Map view.

The Map view answers a CEO's question: "what does my company believe,
and what's changing?" It reads from the substrate's existing
materializations (model_neighborhoods, topology_events, model_edges)
plus a per-tenant 2D PCA projection of the 128-d topo embeddings,
cached in view_ceo_cache and refreshed when a topology_event lands.

Endpoints
---------

  GET /api/map/snapshot
      Macro+meso payload — every active neighborhood (with named
      signature, member count, centroid in 2D) plus active models
      with their 2D position, neighborhood id, and health attributes.
      Edges are bucketed by edge_kind. Optional query params:
        ?neighborhood_id=<uuid> — restrict to one neighborhood
        ?edge_kinds=supports,instance_of — filter (default all 4)
        ?include_archived=false — default false
        ?since=<iso8601> — only models / edges newer than this

  GET /api/map/topology_events?since=<iso8601>&limit=50
      Recent phase events for the right-sidebar stream + bloom/contract
      animations. Default since = now() - 7 days, limit 50, ordered DESC.

  GET /api/map/models/{model_id}
      Belief story payload for the side panel. Includes proposition,
      confidence, falsifier, supporting/contributing edges with each
      neighbor's named_signature, recent activity log.

  POST /api/map/refresh_projection
      Manual cache bust for the PCA projection. Used by tests and
      ops; the worker normally rebuilds on its own when topology_events
      flow in.

Auth: Bearer token resolves to (actor_id, tenant_id) via the existing
gateway middleware. Every query filters by tenant_id from the auth
context — no tenant param in the public surface.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# =====================================================================
# Pydantic response types — the wire contract.
# =====================================================================


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


# Health encoding — pure function of (confidence, contested_count,
# confirmed_count, last_confirmed_at, status). Computed server-side so
# the client doesn't need to re-derive the same heuristic.
HealthClass = Literal[
    "solid",       # confidence ≥ 0.7, confirmed > contested, not stale
    "stable",      # confidence 0.4-0.7, no contestation churn
    "contested",   # contested_count > confirmed_count
    "fading",      # activation < 0.3 OR last_confirmed_at older than 30d
    "fresh",       # created_at within last 7 days
    "archived",    # status != 'active'
]


# Layered Model-page bands (spec §4.2). Every visible node is bucketed
# into one of these five horizontal bands so the renderer can place it
# without re-deriving the heuristic. Derived server-side from
# `proposition_kind` plus the proposition payload — see
# services/gateway/map_routes.py:_classify_band.
BandClass = Literal[
    "goal",        # strategic objectives, durable structural claims
    "commitment",  # promised work / assertions about company state
    "decision",    # open questions / forecasts / hypotheses
    "risk",        # concerns / blockers / contested claims
    "customer",    # customer / market impact
]


class MapNode(_Strict):
    id: UUID
    natural: str  # truncated to 100 chars server-side
    proposition_kind: str
    neighborhood_id: UUID | None  # None = unclustered (singletons)
    confidence: float
    activation: float
    status: str
    archive_reason: str | None
    health: HealthClass
    # Model-page band (spec §4.2). Derived from proposition_kind +
    # proposition payload. Frontend places the node in this band's row.
    band: BandClass
    in_degree: int  # how many edges point AT this model
    out_degree: int
    # 2D projection from the per-tenant PCA cache.
    # Coordinates are normalized to [-1, 1] so the renderer can
    # re-scale freely without re-fitting.
    topo_x: float | None  # None when no topo_embedding yet (newborns)
    topo_y: float | None
    created_at: datetime


class MapEdge(_Strict):
    source: UUID
    target: UUID
    kind: str  # 'supports' | 'contributes_to_resolution' | 'instance_of' | 'superseded_by'
    weight: float | None
    status: str
    detected_by: str
    crosses_neighborhood: bool  # True when source.neighborhood != target.neighborhood


class MapNeighborhood(_Strict):
    id: UUID
    named_signature: str | None  # null → renderer falls back to "[unnamed]"
    member_count: int
    density: float | None
    status: str  # 'active' | 'dissolved' | 'merged'
    last_recomputed_at: datetime | None
    # Centroid of member nodes in the 2D projection. Used to position
    # the neighborhood label at zoom-out level.
    centroid_x: float | None
    centroid_y: float | None
    # Convex-hull padding factor — renderer can use this directly to
    # compute the watercolor wash polygon.
    hull_padding: float = 60.0
    # Phase events in the last 7 days, for the badge.
    recent_event_count: int


class MapSnapshotChangeSummary(_Strict):
    """Drives the top banner: 'X changes since Monday' or
    'Your belief system is stable.'"""
    since: datetime
    new_models: int
    archived_models: int
    new_edges: int
    phase_events: int
    contested_models: int
    # Pre-rendered headline. Examples:
    #   "12 changes since Monday"
    #   "Your belief system is stable — last change 3 days ago"
    #   "3 neighborhoods need attention"
    headline: str
    # Optional pan target — neighborhood the user should look at first.
    focus_neighborhood_id: UUID | None


class MapSnapshotResponse(_Strict):
    nodes: list[MapNode]
    edges: list[MapEdge]
    neighborhoods: list[MapNeighborhood]
    change_summary: MapSnapshotChangeSummary
    # When the UMAP projection was last fitted. Null when the tenant
    # has too few models with topo_embedding (renderer falls back to
    # force-directed in that case).
    projection_fitted_at: datetime | None
    # Trustworthiness of the current 2D projection. Same semantics as
    # RefreshProjectionResponse.trustworthiness. Null when no
    # projection has been fitted (sub-threshold tenant).
    projection_trustworthiness: float | None
    server_now: datetime
    # Total node count per band BEFORE capping. The Model page renders
    # an overview of top-K-per-band; the frontend uses these totals to
    # draw "+N more" overflow cluster nodes so the CEO sees scale at a
    # glance and can drill into a band via the lens rail. Default
    # empty dict for backward compatibility with older clients.
    band_totals: dict[str, int] = Field(default_factory=dict)


class TopologyEventEntry(_Strict):
    id: UUID
    kind: str  # 'emergence' | 'dissolution' | 'split' | 'merge' | 'drift' | 'relocate'
    occurred_at: datetime
    neighborhood_id: UUID | None
    named_signature: str | None  # snapshotted into the event row
    magnitude: float | None
    payload: dict[str, Any] = Field(default_factory=dict)


class TopologyEventsResponse(_Strict):
    events: list[TopologyEventEntry]
    server_now: datetime


# Side-panel "story" payload. Notice: the field names speak
# CEO-language, not schema-language. The renderer is allowed to be dumb.


class StoryEdgeRef(_Strict):
    """A single edge surfaced in the story, with the neighbor's
    named context already resolved."""
    neighbor_id: UUID
    neighbor_natural: str  # truncated to 80
    neighbor_neighborhood_signature: str | None
    edge_kind: str
    edge_weight: float | None


class StoryActivityEntry(_Strict):
    occurred_at: datetime
    headline: str  # pre-rendered: "confidence raised 0.72 → 0.85 by Daniel's note"
    detail: dict[str, Any] = Field(default_factory=dict)


class ModelStoryResponse(_Strict):
    id: UUID
    proposition_kind: str
    natural: str  # full text, no truncation
    confidence: float
    confidence_at_assertion: float
    activation: float
    status: str
    archive_reason: str | None
    asserted_at: datetime
    last_confirmed_at: datetime | None
    contested_count: int
    confirmed_count: int
    health: HealthClass
    # The "why we believe this" section.
    supporting: list[StoryEdgeRef]
    contributing_to: list[StoryEdgeRef]
    instance_of: list[StoryEdgeRef]
    superseded_by: list[StoryEdgeRef]
    # The "what would change our mind" section.
    falsifier_summary: str | None  # pre-rendered human-readable summary
    falsifier_last_checked_at: datetime | None
    # Outbound edges count for "affects N downstream beliefs"
    affects_count: int
    # Belonging.
    neighborhood_id: UUID | None
    neighborhood_signature: str | None
    # Recent activity log.
    recent_activity: list[StoryActivityEntry]


class RefreshProjectionResponse(_Strict):
    fitted_at: datetime
    model_count: int
    # Trustworthiness score (sklearn.manifold.trustworthiness, k=7).
    # 0..1 — fraction of each point's k-nearest neighbors in 128-d
    # that remain k-nearest neighbors in the 2D projection. The
    # frontend surfaces this as a small "this view preserves N% of
    # nearest-neighbor relationships" badge so users see the lie size.
    trustworthiness: float
    n_neighbors: int  # UMAP n_neighbors used (capped to model_count-1)
    min_dist: float   # UMAP min_dist used


__all__ = [
    "HealthClass",
    "BandClass",
    "MapNode",
    "MapEdge",
    "MapNeighborhood",
    "MapSnapshotChangeSummary",
    "MapSnapshotResponse",
    "TopologyEventEntry",
    "TopologyEventsResponse",
    "StoryEdgeRef",
    "StoryActivityEntry",
    "ModelStoryResponse",
    "RefreshProjectionResponse",
]
