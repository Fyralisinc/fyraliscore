import { useNavigate } from "react-router-dom";

import type { SpecLedgerEvent } from "@/api/ledger-event-types";

interface Props {
  event: SpecLedgerEvent;
  onClose: () => void;
}

export function LedgerEventInspector({ event, onClose }: Props) {
  const navigate = useNavigate();
  return (
    <div className="fx-inspector" data-testid="ledger-inspector">
      <header className="fx-inspector__head">
        <div>
          <div className="fx-delta__type">Ledger event</div>
          <h2 className="fx-inspector__title">{event.summary}</h2>
          <div className="fx-inspector__subtitle">
            {new Date(event.occurredAt).toLocaleString()}{" · "}
            {event.actor?.label ?? "Fyralis"}
          </div>
        </div>
        <button type="button" className="fx-inspector__close" aria-label="Close" onClick={onClose}>×</button>
      </header>

      {event.body ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Details</div>
          <div className="fx-inspector__body">{event.body}</div>
        </section>
      ) : null}

      {(event.before || event.after) ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Before → after</div>
          <div className="fx-inspector__body">
            <span className="fx-muted">{event.before ?? "—"}</span>
            <span style={{ margin: "0 8px" }}>→</span>
            <strong>{event.after ?? "—"}</strong>
          </div>
        </section>
      ) : null}

      {event.actionsTaken && event.actionsTaken.length > 0 ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Actions taken</div>
          <ul className="fx-inspector__body" style={{ paddingLeft: 18 }}>
            {event.actionsTaken.map((a, i) => <li key={i}>{a}</li>)}
          </ul>
        </section>
      ) : null}

      {event.outcome ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Outcome</div>
          <div className="fx-inspector__body">
            <strong>{event.outcome}</strong>
            {event.calibrationImpact != null ? (
              <span className="fx-muted"> · calibration {event.calibrationImpact > 0 ? "+" : ""}{event.calibrationImpact.toFixed(2)}</span>
            ) : null}
          </div>
        </section>
      ) : null}

      <div className="fx-inspector__actions">
        {event.affectedThreadId ? (
          <button type="button" className="fx-btn" onClick={() => navigate(`/model?thread=${event.affectedThreadId}`)}>
            Open thread →
          </button>
        ) : null}
        {event.affectedDeltaId ? (
          <button type="button" className="fx-btn fx-btn--gold" onClick={() => navigate(`/?delta=${event.affectedDeltaId}`)}>
            Open delta →
          </button>
        ) : null}
        {event.affectedForecastId ? (
          <button type="button" className="fx-btn" onClick={() => navigate(`/forecasts?forecast=${event.affectedForecastId}`)}>
            Open forecast →
          </button>
        ) : null}
      </div>
    </div>
  );
}
