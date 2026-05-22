import type { OperatingThread } from "@/api/operating-thread-types";

import { StatusPill, statusRailClass } from "./StatusPill";

interface Props {
  thread: OperatingThread;
  selected?: boolean;
  onSelect?: (id: string) => void;
  onTraceCause?: (id: string) => void;
  onTraceConsequence?: (id: string) => void;
  onViewDeltas?: (id: string) => void;
  onMarkWrong?: (id: string) => void;
}

// Operating Thread row — spec §4.5 anatomy. Compact executive case
// reading: status rail, title, current reading, causal ribbon (5 cells),
// semantic mass, trust line, optional actions row.
export function OperatingThreadRow({
  thread,
  selected,
  onSelect,
  onTraceCause,
  onTraceConsequence,
  onViewDeltas,
  onMarkWrong,
}: Props) {
  const mass = thread.semanticMass;
  const trust = thread.trust;

  return (
    <article
      className={`fx-card${selected ? " fx-card--selected" : ""}${
        thread.status === "contested" ? " fx-card--contested" : ""
      }`}
      onClick={() => onSelect?.(thread.id)}
      tabIndex={0}
      role="button"
      aria-label={thread.title}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect?.(thread.id);
        }
      }}
    >
      <div className={statusRailClass(thread.status)} aria-hidden="true" />
      <div className="fx-card__body">
        <header className="fx-thread__head">
          <h3 className="fx-thread__title">{thread.title}</h3>
          <StatusPill status={thread.status} />
        </header>

        <p className="fx-thread__reading">{thread.currentReading}</p>

        <div className="fx-thread__ribbon">
          {thread.causalRibbon.slice(0, 5).map((c, i) => (
            <div key={i} className="fx-thread__ribbon-cell">
              <div className="fx-thread__ribbon-label">{c.label}</div>
              <div
                className={`fx-thread__ribbon-value${
                  c.tone ? ` fx-thread__ribbon-value--${c.tone}` : ""
                }`}
              >
                {c.value}
              </div>
            </div>
          ))}
        </div>

        <footer className="fx-thread__footer">
          <div className="fx-thread__mass">
            <span className="fx-thread__mass-bullet">
              <strong>{mass.representedNodes}</strong> Nodes represented
            </span>
            {Object.entries(mass.typeCounts).slice(0, 4).map(([k, v]) => (
              <span key={k} className="fx-thread__mass-bullet">
                {v} {k}
              </span>
            ))}
          </div>
          <div className="fx-thread__trust">
            <span>Updated {formatRel(thread.lastUpdatedAt)}</span>
            {trust.confidence != null ? (
              <span>Confidence <strong>{Math.round(trust.confidence * 100)}%</strong></span>
            ) : null}
            <span>Evidence <strong>{trust.evidenceQuality}</strong></span>
          </div>
        </footer>

        {(onTraceCause || onTraceConsequence || onViewDeltas || onMarkWrong) ? (
          <div className="fx-thread__actions" onClick={(e) => e.stopPropagation()}>
            <button type="button" className="fx-btn fx-btn--sm" onClick={() => onSelect?.(thread.id)}>
              Open thread
            </button>
            {onTraceCause ? (
              <button type="button" className="fx-btn fx-btn--sm fx-btn--ghost" onClick={() => onTraceCause(thread.id)}>
                Trace cause
              </button>
            ) : null}
            {onTraceConsequence ? (
              <button type="button" className="fx-btn fx-btn--sm fx-btn--ghost" onClick={() => onTraceConsequence(thread.id)}>
                Trace consequence
              </button>
            ) : null}
            {onViewDeltas && thread.relatedDecisionDeltaIds.length > 0 ? (
              <button type="button" className="fx-btn fx-btn--sm fx-btn--ghost" onClick={() => onViewDeltas(thread.id)}>
                Decision Deltas ({thread.relatedDecisionDeltaIds.length})
              </button>
            ) : null}
            {onMarkWrong ? (
              <button type="button" className="fx-btn fx-btn--sm fx-btn--coral" onClick={() => onMarkWrong(thread.id)}>
                This looks wrong
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
    </article>
  );
}

function formatRel(iso: string): string {
  try {
    const d = new Date(iso);
    const ms = Date.now() - d.getTime();
    const m = Math.floor(ms / 60_000);
    if (m < 1) return "just now";
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    const days = Math.floor(h / 24);
    return `${days}d ago`;
  } catch {
    return iso;
  }
}
