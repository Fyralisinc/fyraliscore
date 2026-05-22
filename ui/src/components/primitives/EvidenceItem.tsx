import { useState, type ReactNode } from "react";
import { StatusChip } from "./StatusChip";

export type EvidenceTrustTier =
  | "authoritative"
  | "attested"
  | "reputable"
  | "inferential"
  | "unvetted";

export type EvidenceSource =
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

export interface EvidenceItemProps {
  source: EvidenceSource;
  title: string;
  timestamp: string;
  trustTier?: EvidenceTrustTier;
  detail?: ReactNode;
  defaultExpanded?: boolean;
}

const sourceGlyph: Record<EvidenceSource, string> = {
  crm: "CR",
  support: "SP",
  email: "EM",
  slack: "SL",
  linear: "LN",
  github: "GH",
  calendar: "CA",
  finance: "FN",
  product: "PR",
  fyralis: "Fy",
};

export function EvidenceItem({
  source,
  title,
  timestamp,
  trustTier,
  detail,
  defaultExpanded = false,
}: EvidenceItemProps) {
  const [expanded, setExpanded] = useState(defaultExpanded);
  const expandable = Boolean(detail);

  return (
    <div className="fy-evidence">
      <div className="fy-evidence__row">
        <span className="fy-evidence__icon" aria-label={`Source: ${source}`}>
          {sourceGlyph[source]}
        </span>
        <span className="fy-evidence__title">{title}</span>
        {trustTier ? (
          <StatusChip variant="evidence">{trustTier}</StatusChip>
        ) : null}
        <span className="fy-evidence__time">{timestamp}</span>
      </div>
      {expandable ? (
        <button
          type="button"
          className="fy-evidence__expand-btn"
          aria-expanded={expanded}
          onClick={() => setExpanded((v) => !v)}
        >
          {expanded ? "Hide source excerpt" : "View source excerpt"}
        </button>
      ) : null}
      {expandable && expanded ? (
        <div className="fy-evidence__expanded">{detail}</div>
      ) : null}
    </div>
  );
}

export default EvidenceItem;
