import type { SpecLedgerEvent } from "@/api/ledger-event-types";

interface Props {
  event: SpecLedgerEvent;
  onSelect?: (id: string) => void;
  selected?: boolean;
}

const KIND_LABEL: Record<SpecLedgerEvent["kind"], string> = {
  observation_ingested: "Observation ingested",
  model_updated: "Model updated",
  thread_created: "Thread created",
  thread_status_changed: "Thread status changed",
  thread_split: "Thread split",
  thread_merged: "Thread merged",
  decision_delta_proposed: "Proposed change",
  decision_delta_accepted: "Decision accepted",
  decision_delta_delegated: "Decision delegated",
  decision_delta_contested: "Contested",
  commitment_created: "Commitment created",
  commitment_blocked: "Commitment blocked",
  forecast_created: "Forecast created",
  forecast_confidence_changed: "Forecast confidence changed",
  forecast_resolved: "Forecast resolved",
  user_context_added: "Context added",
  node_archived: "Node archived",
};

export function LedgerEventRow({ event, onSelect, selected }: Props) {
  const time = formatHM(event.occurredAt);
  return (
    <div
      className={`fx-ledger__event${selected ? " fx-card--selected" : ""}`}
      onClick={() => onSelect?.(event.id)}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect?.(event.id);
        }
      }}
    >
      <div className="fx-ledger__event-time">{time}</div>
      <div className="fx-ledger__event-body">
        <div className="fx-ledger__event-kind">{KIND_LABEL[event.kind]}</div>
        <div className="fx-ledger__event-summary">{event.summary}</div>
        {event.actionsTaken && event.actionsTaken.length > 0 ? (
          <div className="fx-ledger__event-refs">
            <span className="fx-muted">{event.actionsTaken.join(" · ")}</span>
          </div>
        ) : null}
        {event.relatedRefs.length > 0 ? (
          <div className="fx-ledger__event-refs">
            Related:{" "}
            {event.relatedRefs.slice(0, 4).map((r, i) => (
              <span key={`${r.id}-${i}`}>{i > 0 ? " · " : ""}{r.label}</span>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}

function formatHM(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", hour12: false });
  } catch {
    return iso;
  }
}
