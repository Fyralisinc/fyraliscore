// Single source of truth for Ledger event-type taxonomy: labels,
// brand-coloured accents, dot variants, and inspector classification.
// Per brand §6.4: color = event taxonomy, never decoration.

import type { LedgerEventType } from "@/api/history-types";

export type EventTypeMeta = {
  label: string;                 // uppercase taxonomy label
  shortLabel: string;            // for sentence-level use
  cssVar: string;                // brand CSS variable referenced by --ledger-accent
  className: string;             // BEM tail for type-specific styling
  semanticDescription: string;   // accessible description
};

export const EVENT_TYPE_META: Record<LedgerEventType, EventTypeMeta> = {
  action_taken: {
    label: "ACTION TAKEN",
    shortLabel: "Action taken",
    cssVar: "var(--color-moss-cipher)",
    className: "action-taken",
    semanticDescription:
      "Executed action — a decision was applied or escalated.",
  },
  model_update: {
    label: "MODEL UPDATE",
    shortLabel: "Model update",
    cssVar: "var(--color-weathered-sage)",
    className: "model-update",
    semanticDescription:
      "Substrate-level update to a belief, pattern, or recommendation.",
  },
  prediction_made: {
    label: "PREDICTION MADE",
    shortLabel: "Prediction made",
    cssVar: "var(--color-veiled-iris)",
    className: "prediction-made",
    semanticDescription:
      "A forecast was filed about a future outcome.",
  },
  prediction_resolved: {
    label: "PREDICTION RESOLVED",
    shortLabel: "Prediction resolved",
    cssVar: "var(--color-veiled-iris)",
    className: "prediction-resolved",
    semanticDescription:
      "A prior forecast resolved against observed outcome.",
  },
  observation_ingested: {
    label: "OBSERVATION INGESTED",
    shortLabel: "Observation ingested",
    cssVar: "var(--color-deep-lapis)",
    className: "observation-ingested",
    semanticDescription:
      "External evidence ingested from an integration source.",
  },
  contestation: {
    label: "CONTESTATION",
    shortLabel: "Contestation",
    cssVar: "var(--color-burnt-coral)",
    className: "contestation",
    semanticDescription:
      "A claim was contested and routed for review.",
  },
};

export function typeMeta(type: LedgerEventType): EventTypeMeta {
  return EVENT_TYPE_META[type];
}

export const TAB_ORDER: { id: "all" | LedgerEventType; label: string }[] = [
  { id: "all", label: "All activity" },
  { id: "model_update", label: "Model changes" },
  { id: "prediction_made", label: "Predictions" },
  { id: "action_taken", label: "Actions" },
  { id: "contestation", label: "Contestations" },
  { id: "observation_ingested", label: "Observations" },
];

// "Predictions" tab includes both made and resolved predictions per the
// spec — predictions is the user-facing umbrella concept, not the
// canonical type filter.
export function tabToTypes(
  tabId: "all" | LedgerEventType
): LedgerEventType[] | undefined {
  if (tabId === "all") return undefined;
  if (tabId === "prediction_made") {
    return ["prediction_made", "prediction_resolved"];
  }
  return [tabId];
}
