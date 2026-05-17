// Ask Fyralis stub client. UI scaffold only — no backend wired yet.
// Returns deterministic, typed responses keyed by the selected delta
// and prompt. The real implementation will POST /api/ask with an
// AskContext payload (spec §13.3); the shape returned here mirrors
// AskAnswer (§13.5) so the strip doesn't change when the wire lands.

import type { DecisionDelta } from "./today-page-types";

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
];

export function getSuggestedPrompts(_delta: DecisionDelta): AskSuggestion[] {
  return DEFAULT_SUGGESTIONS;
}

export async function askFyralis(
  delta: DecisionDelta,
  prompt: string,
): Promise<AskAnswer> {
  // Visible latency so the loading state isn't imperceptible.
  await new Promise((r) => setTimeout(r, 220));

  const q = prompt.toLowerCase();
  const based = delta.evidenceSummary.groups.map((g) => g.label);
  const missing = delta.missingContext.map((m) => m.text);

  if (q.includes("why")) {
    return {
      type: "explanation",
      title: "Why now",
      body:
        delta.whyThisMatters ||
        "Fyralis surfaced this because related signals crossed a threshold tracked for this category.",
      basedOn: based,
      mayBeMissing: missing.length > 0 ? missing : undefined,
      actions: [
        { label: "Review evidence", actionType: "open_evidence" },
        { label: "Open in Model", actionType: "open_model" },
      ],
    };
  }
  if (q.includes("wait")) {
    return {
      type: "wait_analysis",
      title: "If you wait 7 days",
      body: "Risk stays elevated and the related commitment remains blocked unless ownership is assigned. Fyralis will re-evaluate if no new signals appear, but the current evidence still supports the proposed change.",
      basedOn: based,
      actions: [
        { label: "Delegate owner", actionType: "delegate" },
        { label: "Schedule reminder", actionType: "schedule_review" },
      ],
    };
  }
  if (q.includes("own") || q.includes("delegate")) {
    const suggestedOwner =
      delta.proposedState.find((f) => f.label.toLowerCase().includes("owner"))
        ?.value ?? "VP Engineering";
    return {
      type: "owner_recommendation",
      title: `Recommended owner: ${suggestedOwner}`,
      body: "Selected for direct accountability over the affected area and overlap with the related commitments. A secondary owner from the customer-communication side is also worth looping in.",
      basedOn: based,
      actions: [
        { label: `Delegate to ${suggestedOwner}`, actionType: "delegate" },
        { label: "Review ownership in Model", actionType: "open_model" },
      ],
    };
  }
  if (q.includes("evidence") || q.includes("weak") || q.includes("missing")) {
    const weakest = [...delta.evidenceSummary.groups].sort(
      (a, b) => qualityRank(a.quality) - qualityRank(b.quality),
    )[0];
    return {
      type: "evidence_summary",
      title: "Weakest evidence",
      body: weakest
        ? `${weakest.label} is the thinnest signal here (${weakest.quality}). The proposed change still has stronger support from other source types.`
        : "Evidence is uniformly strong across sources — no single source dominates.",
      basedOn: based,
      mayBeMissing: missing.length > 0 ? missing : undefined,
      actions: [
        { label: "Review all evidence", actionType: "open_evidence" },
        { label: "Report missing context", actionType: "add_context" },
      ],
    };
  }
  return {
    type: "unsupported_answer",
    title: "I can partially answer this",
    body: "I don't yet have a typed response template for this question. Ask is still in scaffolding — once the model is wired up, this will return grounded reasoning.",
    basedOn: based,
    mayBeMissing: missing.length > 0 ? missing : undefined,
    actions: [{ label: "Review evidence", actionType: "open_evidence" }],
  };
}

function qualityRank(q: string): number {
  if (q === "strong") return 3;
  if (q === "medium") return 2;
  if (q === "partial") return 1;
  return 0;
}
