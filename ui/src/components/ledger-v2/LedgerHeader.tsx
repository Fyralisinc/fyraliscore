import type { LedgerHeaderData } from "@/api/ledger-v2-types";

interface Props {
  data: LedgerHeaderData;
  onAskClick?: () => void;
}

// Ledger header. Mirrors the screenshot copy:
//   "Ledger" / "What changed, what resolved, and what Fyralis learned."
//   Inline stats line. Top-right controls: Ask · Date range · Filters · User.
export function LedgerHeader({ data, onAskClick }: Props) {
  const stats = [
    plural(data.eventCount, "event", "events"),
    plural(data.chainCount, "chain", "chains"),
    `${data.resolvedCount} resolved`,
    plural(data.forecastsClosedCount, "forecast closed", "forecasts closed"),
    plural(data.correctionCount, "correction", "corrections"),
  ];
  return (
    <header className="lg-header" data-testid="ledger-header">
      <div className="lg-header__lede">
        <h1 className="lg-header__title">Ledger</h1>
        <p className="lg-header__subtitle">
          What changed, what resolved, and what Fyralis learned.
        </p>
        <ul className="lg-header__stats" aria-label="Period summary">
          {stats.filter(Boolean).map((s, i) => (
            <li key={i}>
              {i > 0 ? <span className="lg-header__dot">·</span> : null}
              {s}
            </li>
          ))}
          <li>
            <span className="lg-header__dot">·</span>
            <a className="lg-link" href="#change-log">
              View change log →
            </a>
          </li>
        </ul>
      </div>
      <div className="lg-header__controls">
        <button
          type="button"
          className="lg-header__ask"
          onClick={onAskClick}
          aria-label="Ask Fyralis"
        >
          <svg
            width="14"
            height="14"
            viewBox="0 0 14 14"
            aria-hidden="true"
            className="lg-header__ask-icon"
          >
            <circle cx="6" cy="6" r="4.2" fill="none" stroke="currentColor" strokeWidth="1.4" />
            <path d="m9.2 9.2 3 3" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
          </svg>
          <span>Ask Fyralis</span>
          <span className="lg-header__ask-key">⌘ K</span>
        </button>
        <button
          type="button"
          className="lg-header__chip"
          aria-label="Date range"
        >
          <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
            <rect x="1.5" y="2.5" width="11" height="9" rx="1.2" fill="none" stroke="currentColor" strokeWidth="1.3" />
            <path d="M1.5 5h11M4.5 1.5v2M9.5 1.5v2" stroke="currentColor" strokeWidth="1.3" />
          </svg>
          <span>{data.dateRange.label}</span>
          <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true">
            <path d="M2 4 5 7l3-3" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </button>
        <button type="button" className="lg-header__chip">
          <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
            <path d="M1.5 3h11M3 7h8M5 11h4" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
          </svg>
          <span>Filters</span>
        </button>
        <div className="lg-header__avatar" aria-label="Diana, CEO" title="Diana">
          D
          <span className="lg-header__avatar-status" aria-hidden="true" />
        </div>
      </div>
    </header>
  );
}

function plural(count: number, one: string, many: string): string | null {
  if (!count) return null;
  return `${count} ${count === 1 ? one : many}`;
}
