"""services.sage.structural_features.types — Pydantic row types.

Mirrors the columns of `model_structural_features` and
`model_edge_structural_features` (migration 0050). These types are the
exchange format between the pure-async compute layer
(`compute.py`), the repo (`repo.py`), and the recompute job
(`job.py`).
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class StructuralEdge(BaseModel):
    """Minimal edge projection consumed by the compute layer.

    Only the fields needed for structural feature computation are
    surfaced. `edge_kind` is kept so callers can later weight or
    filter by relationship semantics (e.g. drop `supports`-style
    self-loops in undirected statistics).
    """

    model_config = ConfigDict(frozen=True)

    edge_id: UUID
    source_model_id: UUID
    target_model_id: UUID
    edge_kind: str
    weight: Optional[float] = None


class ModelStructuralFeatures(BaseModel):
    """Per-Model structural feature row."""

    model_config = ConfigDict()

    model_id: UUID
    tenant_id: UUID
    degree_total: int = 0
    degree_in: int = 0
    degree_out: int = 0
    clustering_coefficient: Optional[float] = None
    core_number: Optional[int] = None
    avg_neighbor_degree: Optional[float] = None
    bridge_score: Optional[float] = None
    hub_score: Optional[float] = None
    community_id: Optional[UUID] = None
    region_ids: list[UUID] = Field(default_factory=list)
    updated_at: Optional[datetime] = None


class EdgeStructuralFeatures(BaseModel):
    """Per-edge structural feature row."""

    model_config = ConfigDict()

    edge_id: UUID
    tenant_id: UUID
    source_model_id: UUID
    target_model_id: UUID
    degree_difference: Optional[float] = None
    common_neighbors: Optional[int] = None
    jaccard_overlap: Optional[float] = None
    edge_betweenness_approx: Optional[float] = None
    bridge_likelihood: Optional[float] = None
    redundancy_score: Optional[float] = None
    updated_at: Optional[datetime] = None
