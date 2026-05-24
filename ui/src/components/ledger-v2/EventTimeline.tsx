import type { LedgerEvent, LedgerEventType } from "@/api/ledger-v2-types";

interface Props {
  events: LedgerEvent[];
  onOpenEvent?: (id: string) => void;
}

// Vertical event timeline inside the Chain Inspector. Each event has
// a colored dot (typed), timestamp + title + summary, and an inline
// "open detail" affordance.
export function EventTimeline({ events, onOpenEvent }: Props) {
  if (events.length === 0) {
    return (
      <div className="lg-empty" data-testid="event-timeline-empty">
        No events recorded for this chain yet.
      </div>
    );
  }
  return (
    <ol className="lg-events" aria-label="Event timeline">
      {events.map((e, i) => (
        <li
          key={e.id}
          className={`lg-event lg-event--${eventTone(e.type)}`}
          data-event-type={e.type}
        >
          <div className="lg-event__rail" aria-hidden="true">
            <span className={`lg-event__dot lg-event__dot--${eventTone(e.type)}`} />
            {i < events.length - 1 ? <span className="lg-event__line" /> : null}
          </div>
          <div className="lg-event__body">
            <div className="lg-event__time">{formatStamp(e.timestamp)}</div>
            <div className="lg-event__title">{e.title}</div>
            {e.description ? (
              <p className="lg-event__desc">{e.description}</p>
            ) : null}
            {typeof e.confidencePct === "number" ? (
              <button
                type="button"
                className="lg-event__chip"
                onClick={() => onOpenEvent?.(e.id)}
              >
                {e.confidencePct}% confidence
              </button>
            ) : null}
          </div>
          <button
            type="button"
            className="lg-event__open"
            aria-label="Open event detail"
            onClick={() => onOpenEvent?.(e.id)}
          >
            <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
              <path d="M5 2.5h6.5V9" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
              <path d="M11 2.8 5.5 8.3" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" />
              <path d="M2.5 5v6.5H9" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </button>
        </li>
      ))}
    </ol>
  );
}

function eventTone(t: LedgerEventType): string {
  switch (t) {
    case "model_item_created":
    case "model_item_updated":
    case "evidence_added":
      return "lapis";
    case "forecast_created":
    case "reevaluation_scheduled":
      return "iris";
    case "proposed_change_created":
    case "proposed_change_delegated":
      return "gold";
    case "proposed_change_accepted":
    case "owner_assigned":
    case "risk_downgraded":
    case "forecast_resolved_true":
      return "moss";
    case "forecast_resolved_false":
    case "claim_contested":
    case "risk_escalated":
      return "garnet";
    case "claim_corrected":
    case "proposed_change_corrected":
      return "gold";
    default:
      return "stone";
  }
}

function formatStamp(iso: string): string {
  const d = new Date(iso);
  const day = d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  const t = d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  return `${day}, ${t}`;
}
