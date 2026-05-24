import type { LedgerChainCard } from "@/api/ledger-v2-types";
import { StageSequence } from "./StageSequence";
import { StatusChip } from "./StatusChip";

interface Props {
  chain: LedgerChainCard;
  selected: boolean;
  onSelect: (id: string) => void;
  now?: Date;
}

// One row in the Memory River. Compresses a chain into a story:
//   avatar / title + status chip
//   Detected → Forecasted → Proposed → ... sequence
//   N events · impact · resolved date
export function ChainCard({ chain, selected, onSelect }: Props) {
  return (
    <button
      type="button"
      className={`lg-chain-card${selected ? " lg-chain-card--selected" : ""}`}
      aria-current={selected ? "true" : undefined}
      onClick={() => onSelect(chain.id)}
    >
      <span
        className={`lg-chain-card__icon lg-chain-card__icon--${iconTone(chain)}`}
        aria-hidden="true"
      >
        <ChainIcon kind={iconKind(chain)} />
      </span>
      <div className="lg-chain-card__body">
        <header className="lg-chain-card__head">
          <h3 className="lg-chain-card__title">{chain.title}</h3>
          <StatusChip status={chain.status} />
        </header>
        <StageSequence stages={chain.stages} />
        <footer className="lg-chain-card__foot">
          <span>{chain.eventCount} events</span>
          {chain.impactLabel ? (
            <>
              <span className="lg-dot">·</span>
              <span>{chain.impactLabel}</span>
            </>
          ) : null}
          <span className="lg-chain-card__foot-spacer" />
          <span className="lg-chain-card__foot-date">
            {chain.resolvedAt
              ? `Resolved ${shortDate(chain.resolvedAt)}`
              : `Started ${shortDate(chain.startedAt)}`}
          </span>
        </footer>
      </div>
    </button>
  );
}

type IconKind = "customer" | "money" | "trend" | "spark" | "chat" | "alert";

function iconKind(c: LedgerChainCard): IconKind {
  const t = c.title.toLowerCase();
  if (t.includes("customer") || t.includes("partner")) return "customer";
  if (t.includes("pricing") || t.includes("revenue")) return "money";
  if (t.includes("capacity") || t.includes("forecast")) return "trend";
  if (t.includes("conversation") || t.includes("rescope") || t.includes("re-scope"))
    return "chat";
  if (t.includes("design")) return "spark";
  return "alert";
}

function iconTone(c: LedgerChainCard): "moss" | "gold" | "garnet" | "lapis" | "iris" | "stone" {
  switch (c.status) {
    case "resolved":
    case "resolved_true":
    case "corrected":
      return "moss";
    case "open":
      return "gold";
    case "monitoring":
      return "iris";
    case "resolved_false":
    case "contested":
      return "garnet";
    case "archived":
      return "stone";
  }
}

function ChainIcon({ kind }: { kind: IconKind }) {
  switch (kind) {
    case "customer":
      return (
        <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
          <circle cx="6.5" cy="7" r="2.6" fill="none" stroke="currentColor" strokeWidth="1.4" />
          <circle cx="12.5" cy="7" r="2.2" fill="none" stroke="currentColor" strokeWidth="1.4" opacity="0.7" />
          <path d="M2 14c.7-2 2.3-3 4.5-3s3.8 1 4.5 3" fill="none" stroke="currentColor" strokeWidth="1.4" />
          <path d="M11 14c.5-1.5 1.7-2.4 3-2.4 1 0 1.7.3 2.2.7" fill="none" stroke="currentColor" strokeWidth="1.3" opacity="0.7" />
        </svg>
      );
    case "money":
      return (
        <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
          <circle cx="9" cy="9" r="6.5" fill="none" stroke="currentColor" strokeWidth="1.4" />
          <path d="M11.5 6.5c-.6-.7-1.6-1-2.7-1-1.8 0-2.5.9-2.5 1.8 0 2.4 5.4 1.3 5.4 3.7 0 1-.9 2-2.7 2-1.3 0-2.5-.5-3.1-1.3M9 4.5v9" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
        </svg>
      );
    case "trend":
      return (
        <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
          <path d="M2 13 6 9l3 2 5-6" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          <path d="M10 5h4v4" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    case "spark":
      return (
        <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
          <path d="M9 1.5 10.6 6.7 16 8l-5.4 1.3L9 15l-1.6-5.7L2 8l5.4-1.3z" fill="currentColor" opacity="0.9" />
        </svg>
      );
    case "chat":
      return (
        <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
          <path d="M2.5 4.5h10a1.5 1.5 0 0 1 1.5 1.5v5a1.5 1.5 0 0 1-1.5 1.5H7L4 15v-2.5H2.5A1.5 1.5 0 0 1 1 11V6a1.5 1.5 0 0 1 1.5-1.5z" fill="none" stroke="currentColor" strokeWidth="1.4" />
          <path d="M4.5 8h6M4.5 10h4" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
        </svg>
      );
    case "alert":
    default:
      return (
        <svg width="18" height="18" viewBox="0 0 18 18" aria-hidden="true">
          <path d="M9 2 16 14H2z" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinejoin="round" />
          <path d="M9 7v3.3M9 12.2v.4" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
        </svg>
      );
  }
}

function shortDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}
