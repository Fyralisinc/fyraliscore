// Fixture for the Map view, served by mock-server.ts when USE_MOCK=1.
// Synthesizes a realistic substrate for a mid-size company so the
// visual design can be validated without a real backend connection.

import type {
  MapEdge,
  MapNeighborhood,
  MapNode,
  MapSnapshotResponse,
  ModelStoryResponse,
  TopologyEventEntry,
  TopologyEventsResponse,
} from "./map-types";

const NOW = new Date("2026-05-10T16:00:00Z").toISOString();

// Mini-tenant: 28 nodes across 6 neighborhoods + a few unclustered.
// Pre-arranged in topo space so the PCA-projected coords look meaningful
// even without an actual PCA cache.

const neighborhoodSeeds: Array<{
  id: string;
  name: string;
  cx: number;
  cy: number;
  recent_event_count: number;
}> = [
  { id: "nh-pelago",    name: "Pelago renewal",         cx: -0.55, cy:  0.50, recent_event_count: 3 },
  { id: "nh-engineering", name: "Engineering velocity",   cx:  0.55, cy:  0.55, recent_event_count: 0 },
  { id: "nh-pricing",   name: "Carmen / pricing pressure", cx: -0.75, cy: -0.20, recent_event_count: 2 },
  { id: "nh-q3roadmap", name: "Q3 roadmap commitments",  cx:  0.30, cy: -0.55, recent_event_count: 1 },
  { id: "nh-hiring",    name: "Hiring + capacity",       cx: -0.10, cy:  0.85, recent_event_count: 0 },
  { id: "nh-competitor", name: "Competitor signals",     cx:  0.80, cy: -0.05, recent_event_count: 0 },
];

type NodeSeed = {
  id: string;
  natural: string;
  kind: string;
  health: MapNode["health"];
  neighborhood: string | null;
  conf: number;
  inDeg: number;
  outDeg: number;
  dx: number;  // offset from neighborhood centroid
  dy: number;
};

const nodeSeeds: NodeSeed[] = [
  // Pelago renewal
  { id: "m-1", natural: "Carmen will renew before Q3", kind: "prediction",  health: "solid",     neighborhood: "nh-pelago", conf: 0.85, inDeg: 3, outDeg: 1, dx: -0.05, dy:  0.05 },
  { id: "m-2", natural: "Carmen signed 2 LOIs for Q3 features", kind: "state", health: "solid",     neighborhood: "nh-pelago", conf: 0.9,  inDeg: 0, outDeg: 1, dx:  0.10, dy:  0.10 },
  { id: "m-3", natural: "Last quarterly review: 95% NPS", kind: "state", health: "solid",     neighborhood: "nh-pelago", conf: 0.85, inDeg: 0, outDeg: 1, dx:  0.10, dy: -0.10 },
  { id: "m-4", natural: "CSM intent_to_renew: 9/10", kind: "state", health: "solid",     neighborhood: "nh-pelago", conf: 0.8,  inDeg: 0, outDeg: 1, dx: -0.10, dy: -0.05 },

  // Engineering velocity
  { id: "m-5",  natural: "Eng team ships 1.2 PRs/eng/day", kind: "state", health: "stable",   neighborhood: "nh-engineering", conf: 0.7, inDeg: 2, outDeg: 0, dx:  0.05, dy:  0.05 },
  { id: "m-6",  natural: "Hotfixes arrive on Fridays",     kind: "pattern", health: "stable", neighborhood: "nh-engineering", conf: 0.65, inDeg: 1, outDeg: 0, dx: -0.10, dy:  0.05 },
  { id: "m-7",  natural: "PR-1473 is a Friday hotfix",     kind: "pattern_instance", health: "fresh", neighborhood: "nh-engineering", conf: 0.75, inDeg: 0, outDeg: 1, dx:  0.10, dy: -0.05 },
  { id: "m-8",  natural: "Daniel is the Friday hotfix lead", kind: "state", health: "stable", neighborhood: "nh-engineering", conf: 0.7, inDeg: 0, outDeg: 1, dx: -0.05, dy: -0.10 },

  // Pricing pressure (contested)
  { id: "m-9",  natural: "Customers churning on price",     kind: "concern",  health: "contested", neighborhood: "nh-pricing", conf: 0.55, inDeg: 1, outDeg: 0, dx:  0.10, dy:  0.05 },
  { id: "m-10", natural: "Price elasticity is low for our ICP", kind: "hypothesis", health: "contested", neighborhood: "nh-pricing", conf: 0.4, inDeg: 0, outDeg: 1, dx: -0.05, dy:  0.05 },
  { id: "m-11", natural: "Survey: 12% would churn on +10% price", kind: "state", health: "fresh", neighborhood: "nh-pricing", conf: 0.65, inDeg: 0, outDeg: 1, dx:  0.05, dy: -0.10 },

  // Q3 roadmap
  { id: "m-12", natural: "Q3 roadmap will land on time",   kind: "prediction", health: "stable", neighborhood: "nh-q3roadmap", conf: 0.6, inDeg: 2, outDeg: 1, dx:  0.05, dy:  0.05 },
  { id: "m-13", natural: "Bulk import feature commitment", kind: "state", health: "solid", neighborhood: "nh-q3roadmap", conf: 0.8, inDeg: 0, outDeg: 1, dx: -0.10, dy:  0.10 },
  { id: "m-14", natural: "OAuth2 integration commitment",  kind: "state", health: "stable", neighborhood: "nh-q3roadmap", conf: 0.7, inDeg: 0, outDeg: 1, dx:  0.10, dy: -0.05 },

  // Hiring + capacity
  { id: "m-15", natural: "We can hire 4 engineers by Aug", kind: "prediction", health: "fading", neighborhood: "nh-hiring", conf: 0.5, inDeg: 1, outDeg: 0, dx:  0.05, dy: -0.05 },
  { id: "m-16", natural: "Engineering pipeline at 12 candidates", kind: "state", health: "stable", neighborhood: "nh-hiring", conf: 0.7, inDeg: 0, outDeg: 1, dx: -0.10, dy:  0.05 },

  // Competitor signals
  { id: "m-17", natural: "Globex launched competing feature in March", kind: "market_assessment", health: "stable", neighborhood: "nh-competitor", conf: 0.75, inDeg: 1, outDeg: 0, dx:  0.05, dy:  0.10 },
  { id: "m-18", natural: "Globex is sales-led, not product-led", kind: "market_assessment", health: "stable", neighborhood: "nh-competitor", conf: 0.65, inDeg: 0, outDeg: 1, dx: -0.05, dy: -0.05 },

  // Unclustered
  { id: "m-19", natural: "Series B fundraise targeting Q4", kind: "state", health: "fresh", neighborhood: null, conf: 0.55, inDeg: 0, outDeg: 0, dx: 0.0, dy: 0.0 },
  { id: "m-20", natural: "Hiring freeze considered if Pelago slips", kind: "hypothesis", health: "fading", neighborhood: null, conf: 0.45, inDeg: 0, outDeg: 0, dx: 0.0, dy: 0.0 },

  // A few archived models to ghost out
  { id: "m-21", natural: "Old pricing model — superseded by tiered",  kind: "state", health: "archived", neighborhood: "nh-pricing", conf: 0.3, inDeg: 0, outDeg: 1, dx: -0.15, dy: -0.05 },
  { id: "m-22", natural: "Original Q2 roadmap — completed",            kind: "prediction", health: "archived", neighborhood: "nh-q3roadmap", conf: 0.4, inDeg: 0, outDeg: 0, dx: 0.15, dy: 0.10 },
];

function buildNodes(): MapNode[] {
  return nodeSeeds.map((seed) => {
    const nh = seed.neighborhood
      ? neighborhoodSeeds.find((n) => n.id === seed.neighborhood)
      : null;
    return {
      id: seed.id,
      natural: seed.natural,
      proposition_kind: seed.kind,
      neighborhood_id: seed.neighborhood,
      confidence: seed.conf,
      activation: seed.health === "fading" ? 0.25 : seed.health === "archived" ? 0.1 : 1.0,
      status: seed.health === "archived" ? "archived" : "active",
      archive_reason: seed.health === "archived" ? "superseded" : null,
      health: seed.health,
      in_degree: seed.inDeg,
      out_degree: seed.outDeg,
      topo_x: nh ? nh.cx + seed.dx : seed.dx,
      topo_y: nh ? nh.cy + seed.dy : seed.dy,
      created_at: seed.health === "fresh"
        ? new Date(Date.now() - 2 * 24 * 3600 * 1000).toISOString()
        : new Date(Date.now() - 60 * 24 * 3600 * 1000).toISOString(),
    };
  });
}

const edgeSeeds: Array<[string, string, MapEdge["kind"]]> = [
  // Pelago renewal: m-1 supported by m-2, m-3, m-4
  ["m-2", "m-1", "supports"],
  ["m-3", "m-1", "supports"],
  ["m-4", "m-1", "supports"],
  // Engineering: m-7 instance_of m-6 (the pattern); m-6 supports m-5; m-8 supports m-5
  ["m-7", "m-6", "instance_of"],
  ["m-6", "m-5", "supports"],
  ["m-8", "m-5", "supports"],
  // Pricing: m-10 → m-9; m-11 supports m-9
  ["m-10", "m-9", "contributes_to_resolution"],
  ["m-11", "m-9", "supports"],
  // Pricing → Q3 roadmap (cross-neighborhood — interesting!)
  ["m-9",  "m-12", "contributes_to_resolution"],
  // Q3 roadmap: m-13 + m-14 support m-12
  ["m-13", "m-12", "supports"],
  ["m-14", "m-12", "supports"],
  // Hiring: m-16 supports m-15
  ["m-16", "m-15", "supports"],
  // Hiring → Engineering (cross-nh)
  ["m-16", "m-5",  "contributes_to_resolution"],
  // Competitor → Pricing (cross-nh — the tension point)
  ["m-17", "m-10", "supports"],
  ["m-18", "m-10", "contributes_to_resolution"],
  // Superseded
  ["m-21", "m-9",  "superseded_by"],
];

function buildEdges(): MapEdge[] {
  const nodeById = new Map(buildNodes().map((n) => [n.id, n]));
  return edgeSeeds.map(([source, target, kind]) => ({
    source,
    target,
    kind,
    weight: kind === "supports" ? 1.0 : null,
    status: "active",
    detected_by: "llm_explicit",
    crosses_neighborhood:
      (nodeById.get(source)?.neighborhood_id ?? null) !==
      (nodeById.get(target)?.neighborhood_id ?? null),
  }));
}

function buildNeighborhoods(): MapNeighborhood[] {
  return neighborhoodSeeds.map((seed) => {
    const members = nodeSeeds.filter((n) => n.neighborhood === seed.id);
    return {
      id: seed.id,
      named_signature: seed.name,
      member_count: members.length,
      density: 0.4 + Math.random() * 0.2,
      status: "active",
      last_recomputed_at: new Date(Date.now() - 1000 * 60 * 30).toISOString(),
      centroid_x: seed.cx,
      centroid_y: seed.cy,
      hull_padding: 60,
      recent_event_count: seed.recent_event_count,
    };
  });
}

export const MAP_SNAPSHOT_FIXTURE: MapSnapshotResponse = {
  nodes: buildNodes(),
  edges: buildEdges(),
  neighborhoods: buildNeighborhoods(),
  change_summary: {
    since: new Date(Date.now() - 7 * 24 * 3600 * 1000).toISOString(),
    new_models: 4,
    archived_models: 1,
    new_edges: 6,
    phase_events: 6,
    contested_models: 3,
    headline: "12 changes since Monday",
    focus_neighborhood_id: "nh-pricing",
  },
  projection_fitted_at: new Date(Date.now() - 1000 * 60 * 30).toISOString(),
  // Synthetic substrate is well-clustered; UMAP would score high here.
  projection_trustworthiness: 0.91,
  server_now: NOW,
};

export const TOPOLOGY_EVENTS_FIXTURE: TopologyEventsResponse = {
  events: [
    {
      id: "ev-1",
      kind: "drift",
      occurred_at: new Date(Date.now() - 2 * 60 * 1000).toISOString(),
      neighborhood_id: "nh-pricing",
      named_signature: "Carmen / pricing pressure",
      magnitude: 0.42,
      payload: {},
    },
    {
      id: "ev-2",
      kind: "emergence",
      occurred_at: new Date(Date.now() - 1000 * 60 * 60 * 6).toISOString(),
      neighborhood_id: "nh-competitor",
      named_signature: "Competitor signals",
      magnitude: null,
      payload: {},
    },
    {
      id: "ev-3",
      kind: "merge",
      occurred_at: new Date(Date.now() - 1000 * 60 * 60 * 24).toISOString(),
      neighborhood_id: "nh-q3roadmap",
      named_signature: "Q3 roadmap commitments",
      magnitude: 0.6,
      payload: {},
    },
    {
      id: "ev-4",
      kind: "split",
      occurred_at: new Date(Date.now() - 1000 * 60 * 60 * 36).toISOString(),
      neighborhood_id: "nh-pelago",
      named_signature: "Pelago renewal",
      magnitude: 0.3,
      payload: {},
    },
  ],
  server_now: NOW,
};

export const MODEL_STORY_FIXTURES: Record<string, ModelStoryResponse> = {
  "m-1": {
    id: "m-1",
    proposition_kind: "prediction",
    natural: "Carmen will renew before Q3",
    confidence: 0.85,
    confidence_at_assertion: 0.72,
    activation: 1.0,
    status: "active",
    archive_reason: null,
    asserted_at: new Date(Date.now() - 12 * 24 * 3600 * 1000).toISOString(),
    last_confirmed_at: new Date(Date.now() - 4 * 24 * 3600 * 1000).toISOString(),
    contested_count: 0,
    confirmed_count: 3,
    health: "solid",
    supporting: [
      {
        neighbor_id: "m-2",
        neighbor_natural: "Carmen signed 2 LOIs for Q3 features",
        neighbor_neighborhood_signature: "Pelago renewal",
        edge_kind: "supports",
        edge_weight: 1.0,
      },
      {
        neighbor_id: "m-3",
        neighbor_natural: "Last quarterly review: 95% NPS",
        neighbor_neighborhood_signature: "Pelago renewal",
        edge_kind: "supports",
        edge_weight: 1.0,
      },
      {
        neighbor_id: "m-4",
        neighbor_natural: "CSM intent_to_renew: 9/10",
        neighbor_neighborhood_signature: "Pelago renewal",
        edge_kind: "supports",
        edge_weight: 1.0,
      },
    ],
    contributing_to: [],
    instance_of: [],
    superseded_by: [],
    falsifier_summary: "Any signal of competitor evaluation in next 30d",
    falsifier_last_checked_at: new Date(Date.now() - 4 * 24 * 3600 * 1000).toISOString(),
    affects_count: 1,
    neighborhood_id: "nh-pelago",
    neighborhood_signature: "Pelago renewal",
    recent_activity: [
      {
        occurred_at: new Date(Date.now() - 3 * 24 * 3600 * 1000).toISOString(),
        headline: "confidence raised 0.72 → 0.85 by Daniel's note",
        detail: {},
      },
      {
        occurred_at: new Date(Date.now() - 7 * 24 * 3600 * 1000).toISOString(),
        headline: "connected to \"Q3 roadmap commitments\"",
        detail: {},
      },
    ],
  },
};

// Default story for any node not explicitly fixtured.
export function defaultStoryFor(modelId: string): ModelStoryResponse {
  const node = MAP_SNAPSHOT_FIXTURE.nodes.find((n) => n.id === modelId);
  return {
    id: modelId,
    proposition_kind: node?.proposition_kind ?? "state",
    natural: node?.natural ?? "(unknown belief)",
    confidence: node?.confidence ?? 0.5,
    confidence_at_assertion: node?.confidence ?? 0.5,
    activation: node?.activation ?? 1.0,
    status: node?.status ?? "active",
    archive_reason: node?.archive_reason ?? null,
    asserted_at: node?.created_at ?? NOW,
    last_confirmed_at: null,
    contested_count: node?.health === "contested" ? 2 : 0,
    confirmed_count: node?.health === "solid" ? 3 : node?.health === "stable" ? 1 : 0,
    health: node?.health ?? "stable",
    supporting: [],
    contributing_to: [],
    instance_of: [],
    superseded_by: [],
    falsifier_summary: null,
    falsifier_last_checked_at: null,
    affects_count: node?.out_degree ?? 0,
    neighborhood_id: node?.neighborhood_id ?? null,
    neighborhood_signature:
      MAP_SNAPSHOT_FIXTURE.neighborhoods.find((n) => n.id === node?.neighborhood_id)?.named_signature ?? null,
    recent_activity: [],
  };
}
