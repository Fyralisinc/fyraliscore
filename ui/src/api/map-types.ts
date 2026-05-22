// TypeScript counterparts to services/gateway/map_router.py response types.
// Keep field names + nullability identical.

export type HealthClass =
  | "solid"
  | "stable"
  | "contested"
  | "fading"
  | "fresh"
  | "archived";

export type MapBand =
  | "goal"
  | "commitment"
  | "decision"
  | "risk"
  | "customer";

export type MapNode = {
  id: string;
  natural: string;
  proposition_kind: string;
  neighborhood_id: string | null;
  confidence: number;
  activation: number;
  status: string;
  archive_reason: string | null;
  health: HealthClass;
  in_degree: number;
  out_degree: number;
  topo_x: number | null;
  topo_y: number | null;
  created_at: string; // ISO 8601
  // Layered Model page band assignment (spec §4.2). Backend tags this
  // on the snapshot response so the frontend doesn't have to derive
  // a band from proposition_kind.
  band?: MapBand;
};

export type MapEdge = {
  source: string;
  target: string;
  kind:
    | "supports"
    | "contributes_to_resolution"
    | "instance_of"
    | "superseded_by";
  weight: number | null;
  status: string;
  detected_by: string;
  crosses_neighborhood: boolean;
};

export type MapNeighborhood = {
  id: string;
  named_signature: string | null;
  member_count: number;
  density: number | null;
  status: "active" | "dissolved" | "merged";
  last_recomputed_at: string | null;
  centroid_x: number | null;
  centroid_y: number | null;
  hull_padding: number;
  recent_event_count: number;
};

export type MapSnapshotChangeSummary = {
  since: string;
  new_models: number;
  archived_models: number;
  new_edges: number;
  phase_events: number;
  contested_models: number;
  headline: string;
  focus_neighborhood_id: string | null;
};

export type MapSnapshotResponse = {
  nodes: MapNode[];
  edges: MapEdge[];
  neighborhoods: MapNeighborhood[];
  change_summary: MapSnapshotChangeSummary;
  // Null when the tenant has too few models with topo_embedding for
  // UMAP to fit (renderer falls back to force-directed in that case).
  projection_fitted_at: string | null;
  // Trustworthiness of the 2D projection, 0..1. Higher = better
  // local-structure preservation. Surfaced in the UI as a fidelity
  // badge so users see the lossiness honestly. Null when no
  // projection has been fitted (sub-threshold tenant).
  projection_trustworthiness: number | null;
  server_now: string;
  // Total node count per band BEFORE per-band capping. Frontend uses
  // this to render a "+N more" overflow cluster card per band so the
  // CEO sees the depth behind the curated view. Defaults to an empty
  // object on older servers that don't ship the field.
  band_totals?: Partial<Record<MapBand, number>>;
};

export type PhaseEventKind =
  | "emergence"
  | "dissolution"
  | "split"
  | "merge"
  | "drift"
  | "relocate";

export type TopologyEventEntry = {
  id: string;
  kind: PhaseEventKind;
  occurred_at: string;
  neighborhood_id: string | null;
  named_signature: string | null;
  magnitude: number | null;
  payload: Record<string, unknown>;
};

export type TopologyEventsResponse = {
  events: TopologyEventEntry[];
  server_now: string;
};

export type StoryEdgeRef = {
  neighbor_id: string;
  neighbor_natural: string;
  neighbor_neighborhood_signature: string | null;
  edge_kind: string;
  edge_weight: number | null;
};

export type StoryActivityEntry = {
  occurred_at: string;
  headline: string;
  detail: Record<string, unknown>;
};

export type ModelStoryResponse = {
  id: string;
  proposition_kind: string;
  natural: string;
  confidence: number;
  confidence_at_assertion: number;
  activation: number;
  status: string;
  archive_reason: string | null;
  asserted_at: string;
  last_confirmed_at: string | null;
  contested_count: number;
  confirmed_count: number;
  health: HealthClass;
  supporting: StoryEdgeRef[];
  contributing_to: StoryEdgeRef[];
  instance_of: StoryEdgeRef[];
  superseded_by: StoryEdgeRef[];
  falsifier_summary: string | null;
  falsifier_last_checked_at: string | null;
  affects_count: number;
  neighborhood_id: string | null;
  neighborhood_signature: string | null;
  recent_activity: StoryActivityEntry[];
};
