import type { ChainStatus } from "@/api/ledger-v2-types";

interface Props {
  status: ChainStatus;
}

// Status pill used on chain cards and in the inspector header. The
// label is the friendly variant ("Resolved", "Open", "Monitoring",
// "Corrected", "Contested"). Tone classes drive color.
export function StatusChip({ status }: Props) {
  const label = statusLabel(status);
  const tone = statusTone(status);
  return (
    <span className={`lg-status lg-status--${tone}`}>
      <span className="lg-status__dot" aria-hidden="true" />
      {label}
    </span>
  );
}

function statusLabel(s: ChainStatus): string {
  switch (s) {
    case "resolved":
    case "resolved_true":
      return "Resolved";
    case "resolved_false":
      return "Resolved false";
    case "monitoring":
      return "Monitoring";
    case "open":
      return "Open";
    case "contested":
      return "Contested";
    case "corrected":
      return "Accepted";
    case "archived":
      return "Archived";
  }
}

function statusTone(s: ChainStatus): "moss" | "gold" | "garnet" | "stone" {
  switch (s) {
    case "resolved":
    case "resolved_true":
      return "moss";
    case "open":
    case "monitoring":
      return "gold";
    case "resolved_false":
    case "contested":
      return "garnet";
    case "corrected":
      return "moss";
    case "archived":
      return "stone";
  }
}
