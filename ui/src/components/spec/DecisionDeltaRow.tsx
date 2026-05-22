import type { SpecDelta } from "@/api/spec-delta-types";

import { StatusPill } from "./StatusPill";

interface Props {
  delta: SpecDelta;
  selected?: boolean;
  onSelect?: (id: string) => void;
  onAccept?: (id: string) => void;
  onDelegate?: (id: string) => void;
  onContest?: (id: string) => void;
}

// Decision Delta list row (Today page). Spec §11.8 anatomy:
//   [status rail] TYPE                                         Status
//                 Proposal sentence
//                 Current → Proposed (diff strip)
//                 Impact chips · confidence · from <Thread>
//                 [Review evidence] [Accept] [Delegate]
export function DecisionDeltaRow({
  delta,
  selected,
  onSelect,
  onAccept,
  onDelegate,
  onContest,
}: Props) {
  const rail =
    delta.severity === "critical"
      ? "fx-rail--critical"
      : delta.severity === "high"
        ? "fx-rail--needs-review"
        : delta.severity === "medium"
          ? "fx-rail--watch"
          : "fx-rail--healthy";

  const statusForPill = delta.queueSection === "requires_authority"
    ? "needs_review"
    : delta.queueSection === "needs_context"
      ? "contested"
      : delta.queueSection === "delegatable"
        ? "monitoring"
        : "watch";

  return (
    <article
      className={`fx-card${selected ? " fx-card--selected" : ""}`}
      onClick={() => onSelect?.(delta.id)}
      tabIndex={0}
      role="button"
      aria-label={delta.proposal}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect?.(delta.id);
        }
      }}
    >
      <div className={`fx-rail ${rail}`} aria-hidden="true" />
      <div className="fx-card__body">
        <header className="fx-row fx-row--between">
          <div className="fx-delta__type">{delta.userFacingType}</div>
          <StatusPill status={statusForPill as never} />
        </header>

        <div className="fx-delta__proposal">{delta.proposal}</div>

        <div className="fx-delta__diff">
          <span className="fx-delta__diff-label">Current</span>
          <span>{delta.currentState}</span>
          <span className="fx-delta__diff-arrow" aria-hidden="true">→</span>
          <span className="fx-delta__diff-label">Proposed</span>
          <span><strong>{delta.proposedState}</strong></span>
        </div>

        <div className="fx-delta__chips">
          {delta.impactChips.map((c, i) => (
            <span key={i}><strong>{c}</strong></span>
          ))}
          {delta.confidence != null ? (
            <span>· confidence <strong>{Math.round(delta.confidence * 100)}%</strong></span>
          ) : null}
          {delta.sourceThreadTitle ? (
            <span>· from <strong>{delta.sourceThreadTitle}</strong></span>
          ) : null}
          {delta.staleLabel ? <span>· {delta.staleLabel}</span> : null}
        </div>

        {(onAccept || onDelegate || onContest) ? (
          <div className="fx-delta__actions" onClick={(e) => e.stopPropagation()}>
            <button type="button" className="fx-btn fx-btn--sm" onClick={() => onSelect?.(delta.id)}>
              Review evidence
            </button>
            {onAccept ? (
              <button type="button" className="fx-btn fx-btn--sm fx-btn--gold" onClick={() => onAccept(delta.id)}>
                Accept change
              </button>
            ) : null}
            {onDelegate ? (
              <button type="button" className="fx-btn fx-btn--sm fx-btn--ghost" onClick={() => onDelegate(delta.id)}>
                Delegate
              </button>
            ) : null}
            {onContest ? (
              <button type="button" className="fx-btn fx-btn--sm fx-btn--coral" onClick={() => onContest(delta.id)}>
                This looks wrong
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
    </article>
  );
}
