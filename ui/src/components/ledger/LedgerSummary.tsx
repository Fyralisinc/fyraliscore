import { SummaryStrip } from "@/components/primitives";
import type { SummaryStripCell } from "@/components/primitives";
import type { LedgerSummary } from "@/api/history-types";

export interface LedgerSummaryProps {
  summary: LedgerSummary | null;
  loading?: boolean;
}

function fmtNum(n: number): string {
  return n.toLocaleString("en-US");
}

function fmtPct(pct01: number): string {
  return `${Math.round(pct01 * 100)}%`;
}

export function LedgerSummary({ summary, loading }: LedgerSummaryProps) {
  if (loading || !summary) {
    const placeholders: SummaryStripCell[] = Array.from({ length: 6 }).map(
      (_, idx) => ({
        label: [
          "Events",
          "Model updates",
          "Predictions made",
          "Predictions accuracy",
          "Actions taken",
          "Contestations",
        ][idx],
        value: "—",
        sub: "Loading…",
      })
    );
    return (
      <SummaryStrip cells={placeholders} className="fy-ledger__summary" />
    );
  }

  const cells: SummaryStripCell[] = [
    {
      label: "Events",
      value: fmtNum(summary.events.value),
      sub: summary.events.delta_label,
    },
    {
      label: "Model updates",
      value: fmtNum(summary.model_updates.value),
      sub: summary.model_updates.delta_label,
    },
    {
      label: "Predictions made",
      value: fmtNum(summary.predictions_made.value),
      sub: summary.predictions_made.split,
    },
    {
      label: "Predictions accuracy",
      value: fmtPct(summary.predictions_accuracy.value),
      sub: summary.predictions_accuracy.delta_label,
    },
    {
      label: "Actions taken",
      value: fmtNum(summary.actions_taken.value),
      sub: summary.actions_taken.delta_label,
    },
    {
      label: "Contestations",
      value: fmtNum(summary.contestations.value),
      sub: summary.contestations.split,
    },
  ];

  return <SummaryStrip cells={cells} className="fy-ledger__summary" />;
}
