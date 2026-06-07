// Ask Fyralis stub client. UI scaffold only — no backend wired yet.
// Returns deterministic, typed responses keyed by the selected delta
// and prompt. The real implementation will POST /api/ask with an
// AskContext payload (spec §13.3); the shape returned here mirrors
// AskAnswer (§13.5) so the strip doesn't change when the wire lands.

import type { DecisionDelta } from "./today-page-types";
import { getAuthHeader, handleAuthFailure } from "./auth";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

export type AskScopeType =
  | "current_object"
  | "current_page"
  | "account"
  | "deal"
  | "goal"
  | "team"
  | "region"
  | "role_view"
  | "whole_company"
  | "custom";

export interface AskScope {
  type: AskScopeType;
  label: string;
  root_node_ids: string[];
  related_entity_ids: string[];
  filters: Record<string, unknown>;
  access_mode: "full" | "partial" | "restricted";
}

export interface AskSession {
  id: string;
  tenant_id: string;
  viewer_id: string;
  initial_scope: AskScope;
  current_scope: AskScope;
  source_route?: string | null;
  source_object_id?: string | null;
  source_object_type?: string | null;
  mode: AskMode;
  status: "open" | "closed" | "failed";
  created_at: string;
  updated_at: string;
}

export type AskMode =
  | "direct_synthesis_read"
  | "quick_inquiry"
  | "deep_inquiry"
  | "background_review";

export interface AskRelatedNode {
  id: string;
  label: string;
  confidence?: number | null;
  activation?: number | null;
  role: string;
}

export interface AskEvidenceLedgerItem {
  id: string;
  source_ref?: string | null;
  source_kind: string;
  summary: string;
  strength:
    | "decisive"
    | "supporting"
    | "contextual"
    | "weak"
    | "counterevidence"
    | "unknown";
  supports_answer: boolean;
  is_counterevidence: boolean;
  token_estimate?: number | null;
  omitted_reason?: string | null;
  raw_payload: Record<string, unknown>;
}

export interface AskProposedStateChange {
  id: string;
  answer_id: string;
  proposed_op: Record<string, unknown>;
  status:
    | "proposed"
    | "accepted"
    | "rejected"
    | "delegated"
    | "applied"
    | "failed_validation";
  linked_trigger_id?: string | null;
}

export interface AskStructuredPayload {
  answer: string;
  confidence: number;
  why: string[];
  counterevidence: string[];
  impact: string[];
  recommended_actions: string[];
  unknowns: string[];
  related_nodes: AskRelatedNode[];
  evidence: AskEvidenceLedgerItem[];
  omitted_evidence_count: number;
  possible_state_change?: AskProposedStateChange | null;
}

export interface AskTurnResponse {
  session: AskSession;
  message_id: string;
  answer_id: string;
  retrieval_run_id: string;
  mode: AskMode;
  intent: string;
  latency_ms: number;
  payload: AskStructuredPayload;
}

export interface EvidenceExpansionResponse {
  retrieval_run_id: string;
  evidence: AskEvidenceLedgerItem[];
  omitted: AskEvidenceLedgerItem[];
}

export interface CreateAskSessionBody {
  initial_scope: AskScope;
  source_route?: string;
  source_object_id?: string | null;
  source_object_type?: string | null;
}

async function askRequest<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal,
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...getAuthHeader(),
      ...((init?.headers as Record<string, string> | undefined) ?? {}),
    },
    signal,
  });
  if (!res.ok) {
    if (res.status === 401) handleAuthFailure();
    throw new Error(`${res.status} ${res.statusText}`);
  }
  return (await res.json()) as T;
}

export async function createAskSession(
  body: CreateAskSessionBody,
  signal?: AbortSignal,
): Promise<AskSession> {
  const response = await askRequest<{ session: AskSession }>(
    "/v1/ask/sessions",
    { method: "POST", body: JSON.stringify(body) },
    signal,
  );
  return response.session;
}

export function sendAskTurn(
  sessionId: string,
  body: { query: string; scope?: AskScope; requested_mode?: AskMode },
  signal?: AbortSignal,
): Promise<AskTurnResponse> {
  return askRequest<AskTurnResponse>(
    `/v1/ask/sessions/${encodeURIComponent(sessionId)}/messages`,
    { method: "POST", body: JSON.stringify(body) },
    signal,
  );
}

export function expandAskEvidence(
  retrievalRunId: string,
  signal?: AbortSignal,
): Promise<EvidenceExpansionResponse> {
  return askRequest<EvidenceExpansionResponse>(
    "/v1/ask/evidence/expand",
    { method: "POST", body: JSON.stringify({ retrieval_run_id: retrievalRunId }) },
    signal,
  );
}

export function actOnAskProposedChange(
  changeId: string,
  body: { action: "accept" | "reject" | "delegate" | "deep_review"; note?: string; delegate_to?: string },
  signal?: AbortSignal,
): Promise<{ change: AskProposedStateChange }> {
  return askRequest<{ change: AskProposedStateChange }>(
    `/v1/ask/proposed-state-changes/${encodeURIComponent(changeId)}/action`,
    { method: "POST", body: JSON.stringify(body) },
    signal,
  );
}

export type AskResponseType =
  | "explanation"
  | "evidence_summary"
  | "what_if_scenario"
  | "owner_recommendation"
  | "wait_analysis"
  | "model_context_link"
  | "action_preview"
  | "correction_prompt"
  | "unsupported_answer";

export interface AskAction {
  label: string;
  actionType:
    | "accept_delta"
    | "delegate"
    | "open_model"
    | "open_evidence"
    | "create_delta_preview"
    | "add_context"
    | "schedule_review";
}

export interface AskAnswer {
  type: AskResponseType;
  title: string;
  body: string;
  basedOn?: string[];
  mayBeMissing?: string[];
  actions?: AskAction[];
}

export interface AskSuggestion {
  key: string;
  label: string;
}

const DEFAULT_SUGGESTIONS: AskSuggestion[] = [
  { key: "why_now", label: "Why now?" },
  { key: "what_if_wait", label: "What if I wait?" },
  { key: "who_owns", label: "Who should own this?" },
  { key: "evidence_weakest", label: "What evidence is weakest?" },
  { key: "what_if_escalate", label: "What happens if we escalate?" },
];

export function getSuggestedPrompts(_delta: DecisionDelta): AskSuggestion[] {
  return DEFAULT_SUGGESTIONS;
}

export async function askFyralis(
  delta: DecisionDelta,
  prompt: string,
): Promise<AskAnswer> {
  const scope: AskScope = {
    type: "current_object",
    label: delta.title,
    root_node_ids: [],
    related_entity_ids: [],
    filters: {
      decision_delta_id: delta.id,
      source_category: delta.sourceCategory,
      status: delta.status,
      priority_rank: delta.priorityRank,
    },
    access_mode: "full",
  };
  const session = await createAskSession({
    initial_scope: scope,
    source_route: `/today?review=${encodeURIComponent(delta.id)}`,
    source_object_id: null,
    source_object_type: "decision_delta",
  });
  const response = await sendAskTurn(session.id, { query: prompt, scope });
  const payload = response.payload;
  const q = prompt.toLowerCase();

  return {
    type: responseTypeForPrompt(q, response.intent),
    title: compactTitleForPrompt(q, response.intent),
    body: payload.answer,
    basedOn: payload.evidence.map((e) => e.summary).slice(0, 4),
    mayBeMissing: payload.unknowns,
    actions: compactActions(payload),
  };
}

function responseTypeForPrompt(q: string, intent: string): AskResponseType {
  if (intent === "state_gap_inquiry") return "correction_prompt";
  if (q.includes("wait")) return "wait_analysis";
  if (q.includes("own") || q.includes("delegate")) return "owner_recommendation";
  if (q.includes("evidence") || q.includes("weak") || q.includes("missing")) {
    return "evidence_summary";
  }
  if (q.includes("what if")) return "what_if_scenario";
  return "explanation";
}

function compactTitleForPrompt(q: string, intent: string): string {
  if (intent === "state_gap_inquiry") return "Possible model gap";
  if (q.includes("why")) return "Why now";
  if (q.includes("wait")) return "If you wait";
  if (q.includes("own") || q.includes("delegate")) return "Recommended owner";
  if (q.includes("evidence") || q.includes("weak") || q.includes("missing")) {
    return "Evidence read";
  }
  return "Fyralis read";
}

function compactActions(payload: AskStructuredPayload): AskAction[] {
  const actions: AskAction[] = [
    { label: "Review evidence", actionType: "open_evidence" },
    { label: "Open in Model", actionType: "open_model" },
  ];
  if (payload.possible_state_change) {
    actions.unshift({
      label: "Queue validation",
      actionType: "create_delta_preview",
    });
  }
  return actions;
}
