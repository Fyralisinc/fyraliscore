import { useEffect, useRef, useState } from "react";
import type { Arc, HistoryEvent, Prediction } from "./types";

// Spec Part 10 — side panel renders either an event or a prediction.

type Selection =
  | { kind: "event"; event: HistoryEvent; arc?: Arc }
  | { kind: "prediction"; prediction: Prediction }
  | null;

type Props = {
  selection: Selection;
  onClose: () => void;
  onJumpToEntity: (kind: string, id: string) => void;
  onJumpToArc: (id: string) => void;
};

export function EventPanel({
  selection,
  onClose,
  onJumpToEntity,
  onJumpToArc,
}: Props) {
  const [visible, setVisible] = useState(false);
  const [renderedKey, setRenderedKey] = useState<string | null>(null);
  const [bodyHidden, setBodyHidden] = useState(false);
  const closeRef = useRef<HTMLButtonElement>(null);
  const swapTimer = useRef<number | null>(null);

  const currentKey = selection
    ? selection.kind === "event"
      ? `e:${selection.event.id}`
      : `p:${selection.prediction.id}`
    : null;

  useEffect(() => {
    if (selection) {
      setVisible(true);
      window.requestAnimationFrame(() => closeRef.current?.focus());
    } else {
      setVisible(false);
    }
  }, [selection]);

  useEffect(() => {
    if (!currentKey) {
      setRenderedKey(null);
      return;
    }
    if (renderedKey === null) {
      setRenderedKey(currentKey);
      return;
    }
    if (renderedKey !== currentKey) {
      setBodyHidden(true);
      if (swapTimer.current) window.clearTimeout(swapTimer.current);
      swapTimer.current = window.setTimeout(() => {
        setRenderedKey(currentKey);
        setBodyHidden(false);
      }, 150);
    }
  }, [currentKey, renderedKey]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape" && selection) {
        e.preventDefault();
        onClose();
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [selection, onClose]);

  if (!selection && !renderedKey) return null;
  if (!selection) return null;

  return (
    <aside
      className={"commitment-panel event-panel" + (visible ? " open" : "")}
      role="complementary"
      aria-label="Event detail"
    >
      {selection.kind === "event"
        ? renderEventHeader(selection.event, closeRef, onClose)
        : renderPredictionHeader(selection.prediction, closeRef, onClose)}

      <div className={"panel-body" + (bodyHidden ? " body-hidden" : "")}>
        {selection.kind === "event"
          ? renderEventBody(selection.event, selection.arc, onJumpToEntity, onJumpToArc)
          : renderPredictionBody(selection.prediction, onJumpToEntity, onJumpToArc)}
      </div>
    </aside>
  );
}

function renderEventHeader(
  e: HistoryEvent,
  closeRef: React.RefObject<HTMLButtonElement>,
  onClose: () => void
) {
  const dt = new Date(e.timestamp);
  const dateLabel = dt
    .toLocaleDateString("en-US", { month: "short", day: "numeric" })
    .toLowerCase();
  const timeLabel = dt.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  return (
    <header className="panel-header">
      <span className="panel-event-type panel-territory">
        {e.type.replace(/-/g, " ").toUpperCase()}
      </span>
      <span className="panel-event-time panel-id">
        {dateLabel} · {timeLabel}
      </span>
      <button
        ref={closeRef}
        type="button"
        className="panel-close"
        aria-label="Close"
        onClick={onClose}
      >
        ×
      </button>
    </header>
  );
}

function renderEventBody(
  e: HistoryEvent,
  arc: Arc | undefined,
  onJumpToEntity: (kind: string, id: string) => void,
  onJumpToArc: (id: string) => void
) {
  return (
    <>
      <h2 className="panel-title">{e.title || e.descriptor}</h2>
      {e.title ? <p className="panel-descriptor">{e.descriptor}</p> : null}

      {e.substrate_voice ? (
        <>
          <hr className="panel-divider" />
          <section className="panel-section">
            <span className="panel-section-label">Substrate context</span>
            <p className="panel-substrate-voice">{e.substrate_voice}</p>
          </section>
        </>
      ) : null}

      {e.links && e.links.length > 0 ? (
        <>
          <hr className="panel-divider" />
          <section className="panel-section">
            <span className="panel-section-label">Linked entities</span>
            <ul className="panel-link-list">
              {e.links.map((l) => (
                <li key={l.id}>
                  <a
                    className="panel-link"
                    data-target={l.id}
                    href={`#${l.id}`}
                    onClick={(ev) => {
                      ev.preventDefault();
                      onJumpToEntity(l.type, l.id);
                    }}
                  >
                    {(l.label ?? l.id) + ` (${l.type})`}
                  </a>
                </li>
              ))}
            </ul>
          </section>
        </>
      ) : null}

      {arc ? (
        <>
          <hr className="panel-divider" />
          <section className="panel-section">
            <span className="panel-section-label">Part of arc</span>
            <a
              className="panel-arc-link panel-link"
              data-arc={arc.id}
              href={`#arc-${arc.id}`}
              onClick={(ev) => {
                ev.preventDefault();
                onJumpToArc(arc.id);
              }}
            >
              {arc.name} · {arc.events.length} events
            </a>
          </section>
        </>
      ) : null}

      {e.today_card_id ? (
        <>
          <hr className="panel-divider" />
          <section className="panel-section">
            <span className="panel-section-label">In Today</span>
            <p className="panel-today-context">
              This appeared in Today as a critical card.
            </p>
            <button type="button" className="btn-text panel-show-today">
              Show original Today card
            </button>
          </section>
        </>
      ) : null}

      {e.structure_link ? (
        <>
          <hr className="panel-divider" />
          <section className="panel-section">
            <span className="panel-section-label">See in Structure</span>
            <a
              className="panel-link"
              href="/structure"
              onClick={(ev) => {
                ev.preventDefault();
                onJumpToEntity("structure", e.structure_link!);
              }}
            >
              {e.structure_link} in Structure →
            </a>
          </section>
        </>
      ) : null}
    </>
  );
}

function renderPredictionHeader(
  p: Prediction,
  closeRef: React.RefObject<HTMLButtonElement>,
  onClose: () => void
) {
  return (
    <header className="panel-header">
      <span className="panel-event-type panel-territory">PREDICTION</span>
      <span className="panel-event-id panel-id">{p.id}</span>
      <button
        ref={closeRef}
        type="button"
        className="panel-close"
        aria-label="Close"
        onClick={onClose}
      >
        ×
      </button>
    </header>
  );
}

function renderPredictionBody(
  p: Prediction,
  onJumpToEntity: (kind: string, id: string) => void,
  onJumpToArc: (id: string) => void
) {
  return (
    <>
      <h2 className="panel-title">{p.prediction_text}</h2>
      <hr className="panel-divider" />
      <dl className="panel-keyvalue">
        <dt>Made on</dt>
        <dd>{formatLongDate(p.made_on)}</dd>
        <dt>Domain</dt>
        <dd>{p.domain}</dd>
        <dt>Confidence</dt>
        <dd>{Math.round(p.confidence * 100)}%</dd>
        <dt>Status</dt>
        <dd className={"status pred-status-" + p.status}>
          {p.status === "correct"
            ? "RESOLVED CORRECTLY"
            : p.status === "wrong"
              ? "RESOLVED WRONG"
              : "PENDING"}
        </dd>
        {p.resolved_on ? (
          <>
            <dt>Resolved on</dt>
            <dd>{formatLongDate(p.resolved_on)}</dd>
          </>
        ) : null}
      </dl>

      {p.reasoning_at_time ? (
        <>
          <hr className="panel-divider" />
          <section className="panel-section">
            <span className="panel-section-label">My reasoning at the time</span>
            <p className="panel-substrate-voice">{p.reasoning_at_time}</p>
          </section>
        </>
      ) : null}

      {p.outcome_voice ? (
        <>
          <hr className="panel-divider" />
          <section className="panel-section">
            <span className="panel-section-label">What happened</span>
            <p className="panel-substrate-voice">{p.outcome_voice}</p>
          </section>
        </>
      ) : null}

      {p.calibration_impact ? (
        <>
          <hr className="panel-divider" />
          <section className="panel-section">
            <span className="panel-section-label">Calibration impact</span>
            <p className="panel-calibration-impact">
              {p.calibration_impact.domain} calibration:{" "}
              <strong>
                {p.calibration_impact.before.toFixed(2)} →{" "}
                {p.calibration_impact.after.toFixed(2)}
              </strong>
            </p>
          </section>
        </>
      ) : null}

      {p.links && p.links.length > 0 ? (
        <>
          <hr className="panel-divider" />
          <section className="panel-section">
            <span className="panel-section-label">Linked</span>
            <ul className="panel-link-list">
              {p.links.map((l) => (
                <li key={l.id}>
                  <a
                    className="panel-link"
                    data-target={l.id}
                    href={`#${l.id}`}
                    onClick={(e) => {
                      e.preventDefault();
                      if (l.type === "arc") onJumpToArc(l.id);
                      else onJumpToEntity(l.type, l.id);
                    }}
                  >
                    {(l.label ?? l.id) + ` (${l.type})`}
                  </a>
                </li>
              ))}
            </ul>
          </section>
        </>
      ) : null}
    </>
  );
}

function formatLongDate(iso: string): string {
  return new Date(iso).toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
  });
}
