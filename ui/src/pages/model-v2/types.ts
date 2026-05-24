// Wire and view types for the revamped Model page.
//
// Naming convention: this file's types are user-facing ("model item",
// "category", "relationship bundle"). Internally they map onto Nodes
// in the substrate, but we never call them Nodes in the customer UI.

export type CategoryId =
  | "goals"
  | "commitments"
  | "decisions"
  | "risks"
  | "customers"
  | "people"
  | "systems"
  | "finance";

export type RelationshipMode =
  | "impact"
  | "dependencies"
  | "ownership"
  | "evidence";

export type RelationshipVerb =
  | "serves"
  | "blocks"
  | "constrains"
  | "affects"
  | "exposes"
  | "owns"
  | "contributes to"
  | "funds"
  | "limits"
  | "supports"
  | "contradicts"
  | "evidences"
  | "falsifies"
  | "needs input from";

export type ModelItemStatus =
  | "healthy"
  | "watch"
  | "at_risk"
  | "blocked"
  | "critical"
  | "contested"
  | "stale";

export type SemanticColorToken =
  | "moss"
  | "lapis"
  | "iris"
  | "garnet"
  | "teal"
  | "ochre"
  | "blue"
  | "gold"
  | "coral"
  | "sage";

export type StatusBead = {
  status: ModelItemStatus;
  count: number;
};

export type ModelItemSummary = {
  id: string;
  categoryId: CategoryId;
  assertion: string;
  shortLabel: string;
  status: ModelItemStatus;
  // The strongest one-line relationship summary, e.g. "serves Beacon
  // renewal · $840K". Optional — when absent the card just shows the
  // assertion.
  relationshipHint?: string;
  // One headline metric: "$840K", "3 customers", "2 commitments". Used
  // as the right-anchored chip on the micro-card.
  impactMetric?: string;
  owner?: string;
  confidence?: number;
};

export type ModelItem = ModelItemSummary & {
  authority?: "system_inference" | "actor_declaration" | "external_source" | "mixed";
  evidenceSummary?: string;
  falsificationConditions?: string[];
  propositionKind?: string;
  lifecycle?: {
    createdAt: string;
    updatedAt?: string;
    lastConfirmedAt?: string;
  };
  metrics?: {
    arrExposure?: number;
    opportunityValue?: number;
    affectedCustomers?: number;
    blockedCommitments?: number;
    ownerGapCount?: number;
  };
  // Counts surfaced on the central NodeZoom card per design fix spec
  // §3 Problem 9. Keys are "{out|in}_{verb}" and "{verb}_{category}".
  // The renderer composes a humane summary line like:
  //   "Blocked by 2 · Serves 3 customers · Related decision 1"
  relationshipCounts?: Record<string, number>;
};

export type ModelCategory = {
  id: CategoryId;
  label: string;
  description: string;
  colorToken: SemanticColorToken;
  itemCount: number;
  changedTodayCount: number;
  contestedCount: number;
  blockedCount?: number;
  atRiskCount?: number;
  topItems: ModelItemSummary[];
  statusDistribution: StatusBead[];
  position: { x: number; y: number };
};

export type RelationshipBundle = {
  id: string;
  mode: RelationshipMode;
  sourceCategoryId: CategoryId;
  targetCategoryId: CategoryId;
  verb: RelationshipVerb;
  label: string;
  instanceCount: number;
  severity: "low" | "medium" | "high";
  impactLabel?: string;
  impactValue?: number;
  topExample?: {
    sourceShortLabel: string;
    targetShortLabel: string;
  };
  // True when the backend fabricated this bundle from category
  // populations (no real model_edges row), so the renderer can use
  // a softer / dashed line style.
  synthesized?: boolean;
  visual: {
    colorToken: SemanticColorToken;
    strength: "informational" | "low" | "medium" | "high";
    direction: "source_to_target" | "bidirectional";
    lineStyle: "solid" | "dashed" | "faint";
  };
};

export type RelationshipInstance = {
  id: string;
  sourceItem: ModelItemSummary;
  targetItem: ModelItemSummary;
  verb: RelationshipVerb;
  explanation: string;
  impactLabel?: string;
  confidence?: number;
  // True when this instance was synthesized by the backend (no real
  // model_edges row), so the renderer can draw it dashed / softer.
  synthesized?: boolean;
};

export type ModelOverview = {
  summary: {
    activeItemCount: number;
    changedTodayCount: number;
    blockedCount: number;
    contestedCount: number;
    exposureAtRisk?: number | null;
    lastUpdatedAt: string;
  };
  categories: ModelCategory[];
  relationshipBundles: RelationshipBundle[];
  mode: RelationshipMode;
};

export type CategoryFocus = {
  category: ModelCategory;
  relatedCategories: (ModelCategory & { isRelated: boolean })[];
  relationshipBundles: RelationshipBundle[];
  topItems: ModelItemSummary[];
  groups?: { id: string; label: string; itemIds: string[] }[];
};

export type RelationshipFocus = {
  bundle: RelationshipBundle;
  sourceCategory: { id: CategoryId; label: string; colorToken: SemanticColorToken };
  targetCategory: { id: CategoryId; label: string; colorToken: SemanticColorToken };
  instances: RelationshipInstance[];
  resolutionOpportunities: { id: string; label: string }[];
};

export type ItemDetail = {
  item: ModelItem;
  neighbors: {
    incoming: RelationshipInstance[];
    outgoing: RelationshipInstance[];
  };
  evidence: { id: string; source: string; summary: string }[];
  missingContext: { reason: string; impact: string }[];
};

export type TraceNode = {
  id: string;
  assertion: string;
  shortLabel: string;
  kind: string;
  step: number;
  status?: ModelItemStatus;
  source?: string;
};

export type TraceEdge = {
  source: string;
  target: string;
  verb: string;
};

export type Trace = {
  rootItemId: string;
  direction: "cause" | "consequence";
  nodes: TraceNode[];
  edges: TraceEdge[];
};

// View-side state machine. CategoryZoom is intentionally absent —
// category focus is rendered as a right-side drawer overlay
// (CategorySheet) rather than a canvas state, so the overview lattice
// stays visible behind a blurred backdrop.
export type ModelPageState =
  | { type: "overview" }
  | { type: "relationshipZoom"; bundleId: string }
  | { type: "nodeZoom"; itemId: string }
  | { type: "traceView"; itemId: string; direction: "cause" | "consequence"; depth: number }
  | { type: "searchFocus"; query?: string };
