// Mock fixtures for the Model page trace controls. Two representative
// nodes get explicit chains; everything else falls back to a shallow
// 1-step chain derived from the map fixture so the UI never crashes
// on an unknown node id.

import type {
  AdjacencyResponse,
  TraceChain,
  TraceDirection,
  TraceStep,
} from "./model-trace-types";
import { MAP_SNAPSHOT_V2_FIXTURE } from "./map-mock-v2";

const NOW = new Date("2026-05-16T14:32:00Z");
const ISO = (offsetMin: number): string =>
  new Date(NOW.getTime() - offsetMin * 60_000).toISOString();

// Helper: build a synthetic step.
function step(
  id: string,
  kind: string,
  label: string,
  summary: string,
  via: string | null,
  ageMin: number
): TraceStep {
  return {
    id,
    kind,
    label,
    summary,
    ts: ISO(ageMin),
    via_edge_kind: via,
  };
}

// Trace chains keyed by `${nodeId}:${direction}`.
const CHAIN_FIXTURES: Record<string, TraceStep[]> = {
  "r-1:back": [
    step("r-1", "risk", "Critical risk", "Salesforce sync instability threatens anchor renewals", null, 1),
    step("obs-beacon-1", "observation", "Beacon ticket", "3 Salesforce sync failures last 72h", "supports", 90),
    step("obs-northvale-1", "observation", "Northvale ticket", "Sync drift > 4h reported", "supports", 240),
    step("obs-conduit-1", "observation", "Conduit email", "Renewal call: \"sync is unreliable\"", "supports", 480),
  ],
  "r-1:forward": [
    step("r-1", "risk", "Critical risk", "Salesforce sync instability threatens anchor renewals", null, 1),
    step("c-1", "commitment", "Commitment", "Salesforce sync v2 GA by Aug 1", "contributes_to_resolution", 60),
    step("cust-beacon", "customer", "Customer", "Beacon · $820K ARR exposure", "contributes_to_resolution", 60),
    step("cust-arr", "customer", "Customer", "ARR at risk: $2.04M aggregate", "contributes_to_resolution", 60),
  ],
  "d-1:back": [
    step("d-1", "decision", "Decision", "Pricing model has no owner", null, 1),
    step("obs-finance-1", "observation", "Finance memo", "Enterprise pricing draft has 3 conflicting versions", "supports", 360),
  ],
  "d-1:forward": [
    step("d-1", "decision", "Decision", "Pricing model has no owner", null, 1),
    step("c-3", "commitment", "Commitment", "Enterprise pricing public by June", "contributes_to_resolution", 30),
  ],
  "c-1:back": [
    step("c-1", "commitment", "Commitment", "Salesforce sync v2 GA by Aug 1", null, 1),
    step("r-1", "risk", "Risk", "Salesforce sync instability threatens anchor renewals", "contributes_to_resolution", 60),
    step("obs-beacon-1", "observation", "Beacon ticket", "3 Salesforce sync failures last 72h", "supports", 90),
  ],
  "c-1:forward": [
    step("c-1", "commitment", "Commitment", "Salesforce sync v2 GA by Aug 1", null, 1),
    step("g-1", "goal", "Goal", "Expand enterprise motion", "supports", 1),
  ],
};

const SUPPORTS_FIXTURES: Record<string, TraceStep[]> = {
  "r-1": [
    step("obs-beacon-1", "observation", "Beacon ticket", "3 Salesforce sync failures last 72h", "supports", 90),
    step("obs-northvale-1", "observation", "Northvale ticket", "Sync drift > 4h reported", "supports", 240),
    step("obs-conduit-1", "observation", "Conduit email", "Renewal call: \"sync is unreliable\"", "supports", 480),
  ],
  "c-1": [
    step("r-1", "risk", "Risk", "Salesforce sync instability threatens anchor renewals", "contributes_to_resolution", 60),
  ],
  "d-1": [
    step("obs-finance-1", "observation", "Finance memo", "Enterprise pricing draft has 3 conflicting versions", "supports", 360),
  ],
};

const DEPENDS_ON_FIXTURES: Record<string, TraceStep[]> = {
  "r-1": [
    step("dep-obs-12", "observation", "Observations", "12 observations across 3 customers", null, 60),
    step("dep-support-3", "support_thread", "Support", "3 support threads tagged sync_failure", null, 120),
    step("dep-crm-2", "crm_note", "CRM", "2 CRM notes from CSM check-ins", null, 240),
    step("dep-email-1", "email_thread", "Email", "1 email thread (Beacon renewal)", null, 480),
  ],
  "c-1": [
    step("dep-jira-7", "ticket", "Engineering", "7 Linear tickets in scope", null, 60),
  ],
  "d-1": [
    step("dep-memo-1", "memo", "Memo", "1 finance memo open", null, 60),
  ],
};

function fallbackChain(nodeId: string, direction: TraceDirection): TraceStep[] {
  const node = MAP_SNAPSHOT_V2_FIXTURE.nodes.find((n) => n.id === nodeId);
  if (!node) return [];
  return [
    step(nodeId, node.proposition_kind, node.proposition_kind, node.natural, null, 5),
    step(
      `${nodeId}-${direction}-1`,
      "context",
      direction === "back" ? "Upstream context" : "Downstream context",
      direction === "back" ? "Inferred ancestor" : "Inferred dependent",
      direction === "back" ? "supports" : "contributes_to_resolution",
      30
    ),
  ];
}

export function mockTrace(
  nodeId: string,
  direction: TraceDirection,
  maxDepth = 4
): TraceChain {
  const key = `${nodeId}:${direction}`;
  const chain = CHAIN_FIXTURES[key] ?? fallbackChain(nodeId, direction);
  return {
    node_id: nodeId,
    direction,
    max_depth: maxDepth,
    chain: chain.slice(0, maxDepth + 1),
  };
}

export function mockSupports(nodeId: string): AdjacencyResponse {
  return {
    node_id: nodeId,
    items: SUPPORTS_FIXTURES[nodeId] ?? [],
  };
}

export function mockDependsOn(nodeId: string): AdjacencyResponse {
  return {
    node_id: nodeId,
    items: DEPENDS_ON_FIXTURES[nodeId] ?? [],
  };
}
