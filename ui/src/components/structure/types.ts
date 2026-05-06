// Driftwood — Structure page types.
// See DRIFTWOOD_STRUCTURE_SPEC.md Part 16 for the canonical commitment shape.

export type CommitmentStatus = "on-track" | "slipping" | "at-risk" | "blocked";
export type CommitmentPriority = "low" | "standard" | "high";

export type TerritoryId =
  | "strategic"
  | "customer-facing"
  | "technical-infrastructure"
  | "internal-operations"
  | "personnel";

export type LayerId = "commits" | "decisions" | "people" | "customers" | "model";

export type LayoutMode = "relational" | "territory" | "two-axis";
export type EntityKind = "all" | "goals" | "commitments" | "people";

export type PatternEvidence = {
  // ISO YYYY-MM-DD or human-friendly window like "Q1 2026".
  when: string;
  // Short summary of the observation.
  text: string;
  // Optional pointers — clicking these in the UI focuses the artifact.
  ref?:
    | { kind: "commitment"; id: string }
    | { kind: "decision"; id: string }
    | { kind: "goal"; id: string };
  // Optional confidence in this single observation (0–1). Aggregated
  // confidence across evidences contributes to pattern strength.
  weight?: number;
};

export type LearnedPattern = {
  id: string;
  // The pattern statement (what the system has learned).
  statement: string;
  // 0–1 — how strongly the evidence supports this pattern.
  strength: number;
  // First → most relevant evidence supporting the pattern.
  evidence: PatternEvidence[];
};

export type PersonProfile = {
  id: string;
  label: string;
  role: string;
  // Curated, surfaceable patterns the system has inferred about this person.
  // Order: most load-bearing first.
  patterns: LearnedPattern[];
  // One-line "what's been observed lately" — drives the row's secondary line.
  recent_observation: string;
  // Calibration: 0–1, how confident the system is in the current model.
  calibration: number;
};

export type GoalRef = { id: string; label: string; altitude: "strategic" | "operational" };
export type DecisionRef = { id: string; label: string; state: "in-force" | "drifting" | "revisited" };
export type ResourceRef = { id: string; label: string; kind: "financial" | "human" | "technical" };

export type CommitmentEdges = {
  contributes_to: string[];   // goal ids
  constrained_by: string[];   // decision ids
  consumes: string[];         // resource ids
  contributors: string[];     // actor ids beyond owner
};
export type ColorMode = "status" | "owner" | "customer" | "decision";
export type TimeWindow = "next-7" | "quarter" | "all";

export type ActivityEntry = {
  date: string; // ISO YYYY-MM-DD
  desc: string;
};

export type Commitment = {
  id: string;
  label: string;
  territory: TerritoryId;
  owner: string;
  owner_display: string;
  due_date: string; // ISO YYYY-MM-DD
  created_date: string;
  status: CommitmentStatus;
  priority: CommitmentPriority;
  stakeholder: "internal" | "customer";
  stakeholder_label: string;
  customer?: string;
  traces_to: string[]; // decision ids (legacy alias for constrained_by)
  related: string[]; // commitment ids
  edges?: CommitmentEdges;
  progress?: string;
  substrate_insight?: string;
  activity: ActivityEntry[];
  // Curated, surfaceable patterns the system has noticed about this
  // commitment (slip history, drift cluster membership, etc.).
  learnings?: LearnedPattern[];
};

export type GoalLearnings = {
  // 0–1 — how confidently the system models this goal's progress.
  calibration: number;
  recent_observation: string;
  patterns: LearnedPattern[];
};

export type ShapeRef =
  | { type: "territory"; id: TerritoryId; text: string }
  | { type: "person"; id: string; text: string }
  | { type: "commitment"; id: string; text: string }
  | { type: "customer"; id: string; text: string }
  | { type: "decision"; id: string; text: string };

export type ShapeStatementToken =
  | { kind: "text"; text: string }
  | { kind: "ref"; ref: ShapeRef };

export type Filters = {
  entityKind: EntityKind;
  time: TimeWindow;
  statuses: Set<CommitmentStatus>;
  owner: string | null;
  customer: string | null;
};

// Focus target for the relational graph. Generalizes "selected
// commitment" so any node in the graph (goal, decision, resource,
// actor, related commitment) can become the center on click.
export type FocusKind =
  | "commitment" | "goal" | "decision" | "resource" | "actor";
export type FocusTarget = { kind: FocusKind; id: string };

export type ActiveRefFilter =
  | null
  | { kind: "territory"; id: TerritoryId }
  | { kind: "person"; id: string }
  | { kind: "commitment"; id: string }
  | { kind: "customer"; id: string };

export type DotPosition = {
  id: string;
  x: number;
  y: number;
  r: number;
};

export type Rect = { left: number; top: number; right: number; bottom: number };

export type LayerStripCounts = {
  commits: { active: number; at_risk: number };
  decisions: { in_force: number; in_drift: number };
  people: { count: number; teams: number };
  customers: { active: number; healthy_pct: number };
  model: { calibration: number; contested: number };
};
