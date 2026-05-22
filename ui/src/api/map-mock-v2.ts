// Banded fixture for the Model page (v2 layered graph). The original
// map-mock.ts was built around a free-form topology projection; this
// fixture instead labels every node with a layer band so the Model
// page can lay out a stable 5-row company diagram.
//
// Storyline mirrors the spec screenshot: an enterprise motion goal at
// the top, four commitments, two open decisions including an
// unassigned pricing question, three risk/constraint nodes (one
// critical Deep Garnet), four customer-impact tiles.

import type {
  MapBand,
  MapEdge,
  MapNode,
  MapSnapshotResponse,
} from "./map-types";

const NOW = new Date("2026-05-16T14:32:00Z").toISOString();

type BandedSeed = {
  id: string;
  natural: string;
  band: MapBand;
  kind: string;
  health: MapNode["health"];
  conf: number;
  inDeg: number;
  outDeg: number;
  status?: string;
  awaitingConfirmation?: boolean;
  contested?: boolean;
  critical?: boolean;
  customerArr?: number;
  owner?: string | null;
  lastConfirmed?: string;
};

// Narrative-driven node order. Keep ~15-20 nodes per spec cap.
const seeds: BandedSeed[] = [
  // GOALS — sparse, stable.
  {
    id: "g-1",
    natural: "Expand enterprise motion",
    band: "goal",
    kind: "goal",
    health: "solid",
    conf: 0.92,
    inDeg: 4,
    outDeg: 0,
    owner: "Diana",
    lastConfirmed: "2026-05-12T18:04:00Z",
  },

  // COMMITMENTS — 4 boxes.
  {
    id: "c-1",
    natural: "Salesforce sync v2 GA by Aug 1",
    band: "commitment",
    kind: "commitment",
    health: "stable",
    conf: 0.74,
    inDeg: 1,
    outDeg: 1,
    owner: "Marisol",
    lastConfirmed: "2026-05-15T09:14:00Z",
  },
  {
    id: "c-2",
    natural: "SOC2 Type II by Q3",
    band: "commitment",
    kind: "commitment",
    health: "stable",
    conf: 0.81,
    inDeg: 0,
    outDeg: 1,
    owner: "Priya",
  },
  {
    id: "c-3",
    natural: "Enterprise pricing public by June",
    band: "commitment",
    kind: "commitment",
    health: "fading",
    conf: 0.58,
    inDeg: 2,
    outDeg: 1,
    owner: null,
    awaitingConfirmation: true,
  },
  {
    id: "c-4",
    natural: "Data warehouse export beta",
    band: "commitment",
    kind: "commitment",
    health: "fresh",
    conf: 0.7,
    inDeg: 1,
    outDeg: 1,
    owner: "Devon",
  },

  // DECISIONS — 2 boxes, one unassigned (gold), one awaiting (dashed).
  {
    id: "d-1",
    natural: "Pricing model has no owner",
    band: "decision",
    kind: "decision",
    health: "contested",
    conf: 0.6,
    inDeg: 0,
    outDeg: 1,
    status: "Unassigned",
    owner: null,
    awaitingConfirmation: false,
  },
  {
    id: "d-2",
    natural: "Who owns data warehouse pricing?",
    band: "decision",
    kind: "decision",
    health: "fresh",
    conf: 0.55,
    inDeg: 0,
    outDeg: 1,
    status: "Open question",
    awaitingConfirmation: true,
  },

  // CONSTRAINTS / RISKS — 3 boxes incl. critical garnet.
  {
    id: "r-1",
    natural: "Salesforce sync instability threatens anchor renewals",
    band: "risk",
    kind: "risk",
    health: "contested",
    conf: 0.78,
    inDeg: 2,
    outDeg: 4,
    critical: true,
    owner: "Fyralis inference",
    lastConfirmed: "2026-05-16T14:31:00Z",
  },
  {
    id: "r-2",
    natural: "Eng capacity at 92% sustained",
    band: "risk",
    kind: "risk",
    health: "stable",
    conf: 0.7,
    inDeg: 0,
    outDeg: 1,
    owner: "Devon",
  },
  {
    id: "r-3",
    natural: "Globex undercut on enterprise tier",
    band: "risk",
    kind: "risk",
    health: "fresh",
    conf: 0.6,
    inDeg: 0,
    outDeg: 1,
  },

  // CUSTOMER IMPACT — 4 tiles. ARR per customer in metadata.
  {
    id: "cust-beacon",
    natural: "Beacon",
    band: "customer",
    kind: "customer",
    health: "contested",
    conf: 0.8,
    inDeg: 1,
    outDeg: 0,
    customerArr: 820_000,
  },
  {
    id: "cust-northvale",
    natural: "Northvale",
    band: "customer",
    kind: "customer",
    health: "contested",
    conf: 0.74,
    inDeg: 1,
    outDeg: 0,
    customerArr: 680_000,
  },
  {
    id: "cust-conduit",
    natural: "Conduit",
    band: "customer",
    kind: "customer",
    health: "stable",
    conf: 0.66,
    inDeg: 1,
    outDeg: 0,
    customerArr: 410_000,
  },
  {
    id: "cust-arr",
    natural: "ARR at risk: $2.04M",
    band: "customer",
    kind: "customer",
    health: "fading",
    conf: 0.72,
    inDeg: 1,
    outDeg: 0,
    customerArr: 2_040_000,
  },
];

const NOW_MS = Date.parse(NOW);

function buildNodes(): MapNode[] {
  return seeds.map((seed) => {
    const node: MapNode = {
      id: seed.id,
      natural: seed.natural,
      proposition_kind: seed.kind,
      neighborhood_id: null,
      confidence: seed.conf,
      activation: seed.health === "fading" ? 0.35 : 1.0,
      status:
        seed.status ??
        (seed.awaitingConfirmation
          ? "awaiting_confirmation"
          : seed.contested
            ? "contested"
            : "active"),
      archive_reason: null,
      health: seed.health,
      in_degree: seed.inDeg,
      out_degree: seed.outDeg,
      topo_x: null,
      topo_y: null,
      created_at: new Date(NOW_MS - 30 * 24 * 3600 * 1000).toISOString(),
      band: seed.band,
    };
    return node;
  });
}

// Edge map encodes the storyline:
//   commitments support goal; r-1 blocks the renewal commitment;
//   d-1, d-2 contribute to commitments; risks point at customers;
//   r-1 → cust-beacon / northvale / conduit as the blocking edges.
const edgeSeeds: Array<{
  source: string;
  target: string;
  kind: MapEdge["kind"];
}> = [
  // Commitments → Goal (supports)
  { source: "c-1", target: "g-1", kind: "supports" },
  { source: "c-2", target: "g-1", kind: "supports" },
  { source: "c-3", target: "g-1", kind: "supports" },
  { source: "c-4", target: "g-1", kind: "supports" },

  // Decisions → Commitments (contributes_to_resolution = depends-on style)
  { source: "d-1", target: "c-3", kind: "contributes_to_resolution" },
  { source: "d-2", target: "c-4", kind: "contributes_to_resolution" },

  // Risks → Commitments (supports/blocks — we mark blocks via the
  // contributes_to_resolution kind, with the renderer using band+kind
  // to colour). r-2 constrains c-4.
  { source: "r-1", target: "c-1", kind: "contributes_to_resolution" },
  { source: "r-2", target: "c-4", kind: "contributes_to_resolution" },
  { source: "r-3", target: "c-3", kind: "contributes_to_resolution" },

  // Risks → Customers (the blocking edge — red in renderer)
  { source: "r-1", target: "cust-beacon", kind: "contributes_to_resolution" },
  { source: "r-1", target: "cust-northvale", kind: "contributes_to_resolution" },
  { source: "r-1", target: "cust-conduit", kind: "contributes_to_resolution" },
  { source: "r-1", target: "cust-arr", kind: "contributes_to_resolution" },
];

function buildEdges(): MapEdge[] {
  return edgeSeeds.map((e) => ({
    source: e.source,
    target: e.target,
    kind: e.kind,
    weight: e.kind === "supports" ? 1.0 : 0.7,
    status: "active",
    detected_by: "llm_explicit",
    crosses_neighborhood: false,
  }));
}

export const MAP_SNAPSHOT_V2_FIXTURE: MapSnapshotResponse = {
  nodes: buildNodes(),
  edges: buildEdges(),
  neighborhoods: [],
  change_summary: {
    since: new Date(NOW_MS - 24 * 3600 * 1000).toISOString(),
    new_models: 2,
    archived_models: 0,
    new_edges: 3,
    phase_events: 1,
    contested_models: 3,
    headline: "12 changes today",
    focus_neighborhood_id: null,
  },
  projection_fitted_at: null,
  projection_trustworthiness: null,
  server_now: NOW,
};

// Metadata sidecar — extra per-node values the Model page renders that
// aren't part of the MapNode wire shape. Keyed by node id.
export type NodeMetaV2 = {
  owner: string | null;
  arr: number | null;
  status_label: string | null;
  critical: boolean;
  awaiting_confirmation: boolean;
  last_confirmed_at: string | null;
};

export const NODE_META_V2: Record<string, NodeMetaV2> = Object.fromEntries(
  seeds.map((s) => [
    s.id,
    {
      owner: s.owner ?? null,
      arr: s.customerArr ?? null,
      status_label: s.status ?? null,
      critical: Boolean(s.critical),
      awaiting_confirmation: Boolean(s.awaitingConfirmation),
      last_confirmed_at: s.lastConfirmed ?? null,
    },
  ])
);

// Page-level chip counters (the 6-chip strip across the top). Derived
// from the seed counts so an edit here stays in sync.
export const MODEL_METRICS_V2 = {
  active_nodes: 148,
  changed_today: 12,
  contested: seeds.filter((s) => s.health === "contested" || s.contested).length,
  awaiting_confirmation: seeds.filter((s) => s.awaitingConfirmation).length + 5,
  blocked_commitments: 6,
  at_risk_arr_usd: 2_040_000,
};
