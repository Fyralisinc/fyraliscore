// Decision Delta wire types — mirror services/decision_deltas/repo.py
// + router.py. The backend is the source of truth; UI types stay
// read-only with the exception of the request bodies below.

export type DeltaStatus =
  | "proposed"
  | "accepted"
  | "delegated"
  | "contested"
  | "superseded"
  | "dismissed";

export type DeltaLabel =
  | "proposed_change"
  | "needs_review"
  | "authority_required"
  | "recommended_update";

// Categories are open-ended on the backend but these are the common
// ones the Today page surfaces in chips.
export type DeltaCategory =
  | "customer_risk"
  | "capacity"
  | "delivery"
  | "strategy"
  | "decision"
  | "pricing"
  | "revenue"
  | (string & {});

export type DeltaSeverity = "critical" | "high" | "medium" | "low";

export type DeltaEvidenceSource =
  | "crm"
  | "support"
  | "email"
  | "slack"
  | "linear"
  | "github"
  | "calendar"
  | "finance"
  | "product"
  | "fyralis";

export interface DeltaEvidence {
  id: string;
  source: string;            // free-form on wire; UI maps to EvidenceSource
  title: string;
  ts: string;                // ISO timestamp
  trust_tier?: string | null;
  excerpt?: string | null;
  weight?: number | null;
  ordinal: number;
}

// Impact JSONB envelope used by the inspector. All fields optional —
// older deltas may not include any of them.
export interface DeltaImpact {
  arr_at_risk?: number;
  accounts_affected?: number;
  signals?: number;
  stale_days?: number;
  entity_refs?: string[];
  node_updates?: number;
  commitments_affected?: number;
  teams_notified?: number;
  why_this_matters?: string;
  // Routing metadata added by the backend on /delegate, /contest, /add_context.
  delegation?: {
    owner_id: string;
    note?: string | null;
    at: string;
  };
  contest?: {
    by: string;
    reason: string;
    at: string;
  };
  context_notes?: Array<{ by: string; note: string; at: string }>;
  [key: string]: unknown;
}

export interface DecisionDelta {
  id: string;
  tenant_id: string;
  status: DeltaStatus;
  label: DeltaLabel;
  main_assertion: string;
  current_state: Record<string, unknown> | null;
  suggested_update: Record<string, unknown> | null;
  target_node_kind: string | null;
  target_node_id: string | null;
  confidence: number | null;
  confidence_basis: string | null;
  falsification_condition: string | null;
  consequence_preview: Record<string, unknown> | null;
  impact: DeltaImpact | null;
  category: string | null;
  source_recommendation_id: string | null;
  created_at: string;
  updated_at: string;
  accepted_at: string | null;
  accepted_by: string | null;
  resolution_target_at: string | null;
  // Only present on /{id} detail + accept/delegate/contest responses
  // (with_evidence=True on the wire).
  evidence?: DeltaEvidence[];
  // UI-only metadata derived client-side (severity, owner display name,
  // etc) lives on `view` so the wire stays untouched.
  view?: DeltaView;
}

// View is a UI-only enrichment layer attached to the wire type so
// downstream components have one place to read derived fields.
export interface DeltaView {
  severity: DeltaSeverity;          // derived from label / impact
  title: string;                    // headline shown in the row
  body: string;                     // one-sentence summary under title
  chips: string[];                  // tag list (Customer Risk, Decision, …)
  entity_refs: string[];            // ["Beacon", "Northvale", "Conduit"]
  stale_days: number | null;        // days since update (or last_signal_at)
  stale_label?: string | null;      // override e.g. "Sustained 5 days"
  owner?: string | null;            // for delegatables ("Unassigned" / role)
  authority_required: boolean;      // surfaces the row in the top queue
}

// Request bodies.
export interface DelegateBody {
  owner_id: string;
  note?: string;
}

export interface ContestBody {
  reason: string;
}

export interface AddContextBody {
  note: string;
}

export interface ListDeltasParams {
  status?: DeltaStatus | DeltaStatus[];
  category?: string;
  target_kind?: string;
  target_id?: string;
  limit?: number;
}

export interface ListDeltasResponse {
  items: DecisionDelta[];
  count: number;
}

export interface MutationResponse {
  delta: DecisionDelta;
  triggered?: Record<string, unknown>;
}
