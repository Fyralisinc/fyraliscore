import type { LedgerEvidenceItem, LedgerEvidenceSource } from "@/api/history-types";

export interface EvidenceMiniGridProps {
  items: LedgerEvidenceItem[];
}

const SOURCE_LABEL: Record<LedgerEvidenceSource, string> = {
  support: "Support",
  email: "Email",
  crm: "CRM",
  documents: "Docs",
  slack: "Slack",
  linear: "Linear",
  github: "GitHub",
  calendar: "Calendar",
  finance: "Finance",
  product: "Product",
};

function Icon({ source }: { source: LedgerEvidenceSource }) {
  switch (source) {
    case "support":
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <circle cx="7" cy="7" r="5" stroke="currentColor" strokeWidth="1.2" />
          <path
            d="M5 8c.5.6 1.2 1 2 1s1.5-.4 2-1"
            stroke="currentColor"
            strokeWidth="1.2"
            strokeLinecap="round"
          />
        </svg>
      );
    case "email":
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <rect x="2" y="4" width="10" height="6" rx="1" stroke="currentColor" strokeWidth="1.2" />
          <path d="M2.5 4.6 7 8l4.5-3.4" stroke="currentColor" strokeWidth="1.2" />
        </svg>
      );
    case "crm":
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <rect x="2.5" y="3" width="9" height="8" rx="1" stroke="currentColor" strokeWidth="1.2" />
          <path d="M4.5 6h5M4.5 8h3" stroke="currentColor" strokeWidth="1.2" />
        </svg>
      );
    case "documents":
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <path d="M3.5 2.5h5l2.5 2.5v6.5h-7.5z" stroke="currentColor" strokeWidth="1.2" />
          <path d="M8.5 2.5V5h2.5" stroke="currentColor" strokeWidth="1.2" />
        </svg>
      );
    default:
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <circle cx="7" cy="7" r="3.4" stroke="currentColor" strokeWidth="1.2" />
        </svg>
      );
  }
}

export function EvidenceMiniGrid({ items }: EvidenceMiniGridProps) {
  if (items.length === 0) return null;
  return (
    <ul className="fy-ledger__evgrid" role="list" data-testid="ledger-evidence-grid">
      {items.map((item, idx) => (
        <li className="fy-ledger__evgrid-item" key={`${item.source}-${idx}`}>
          <span className="fy-ledger__evgrid-icon" aria-hidden="true">
            <Icon source={item.source} />
          </span>
          <span className="fy-ledger__evgrid-count">{item.count}</span>
          <span className="fy-ledger__evgrid-label">
            {item.label || SOURCE_LABEL[item.source]}
          </span>
        </li>
      ))}
    </ul>
  );
}
