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

export type LayoutMode = "territory" | "two-axis";
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
  traces_to: string[]; // decision ids
  related: string[]; // commitment ids
  progress?: string;
  substrate_insight?: string;
  activity: ActivityEntry[];
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
  time: TimeWindow;
  statuses: Set<CommitmentStatus>;
  owner: string | null;
  customer: string | null;
};

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
