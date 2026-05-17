// Other judgment items — spec §5.5 + §6 + §9.
// Each row is one of:
//   - Compact summary (default), or
//   - In-place Focused Review case when this row is the selected one.
//
// Per spec §6.3, expanding does not navigate; the selected card swaps
// into the same slot and surrounding compact rows stay visible above /
// below. The expanded form follows the §7 anatomy: card header
// (Reviewing N of M, Collapse review), identity, impact chips, deep
// review body, then a sticky 4-button action bar. Collapse is in the
// header, not the action bar (§2.1).

import type {
  DecisionDelta,
  DeltaAction,
  DeltaMetric,
  DeltaStatus,
} from "@/api/today-page-types";
import { StatusChip } from "./StatusChip";
import { InlineDetail } from "./InlineDetail";

interface Props {
  items: DecisionDelta[];
  expandedId: string | null;
  applyingId: string | null;
  // Position of the expanded card within the full Today queue
  // (primary + others). Used to render "Reviewing N of M".
  positionOf?: (id: string) => { index: number; total: number } | null;
  onToggle: (id: string) => void;
  onAccept: (id: string) => void;
  onDelegate: (delta: DecisionDelta) => void;
  onCorrect: (delta: DecisionDelta) => void;
  onOpenEvidence: (delta: DecisionDelta) => void;
}

function hasAction(d: DecisionDelta, action: DeltaAction): boolean {
  return d.availableActions.includes(action);
}

// Compact-row metric strip — up to 3 chips per spec §5.5 example.
function compactMetricsLine(metrics: DeltaMetric[]): string {
  return metrics.slice(0, 3).map((m) => m.label).join(" · ");
}

export function OtherJudgmentList({
  items,
  expandedId,
  applyingId,
  positionOf,
  onToggle,
  onAccept,
  onDelegate,
  onCorrect,
  onOpenEvidence,
}: Props) {
  if (items.length === 0) return null;
  return (
    <section className="tdv2-panel" data-testid="other-judgment-panel">
      <header className="tdv2-panel__head">
        <h3 className="tdv2-panel__title">Other judgment items</h3>
        <span className="tdv2-panel__count">{items.length}</span>
      </header>
      <div className="tdv2-other-list">
        {items.map((d) => {
          const expanded = expandedId === d.id;
          const applying = applyingId === d.id;
          return expanded ? (
            <ExpandedReviewCard
              key={d.id}
              delta={d}
              applying={applying}
              position={positionOf ? positionOf(d.id) : null}
              onCollapse={() => onToggle(d.id)}
              onAccept={() => onAccept(d.id)}
              onDelegate={() => onDelegate(d)}
              onCorrect={() => onCorrect(d)}
              onOpenEvidence={() => onOpenEvidence(d)}
            />
          ) : (
            <CompactRow
              key={d.id}
              delta={d}
              onOpen={() => onToggle(d.id)}
            />
          );
        })}
      </div>
    </section>
  );
}

function CompactRow({
  delta,
  onOpen,
}: {
  delta: DecisionDelta;
  onOpen: () => void;
}) {
  return (
    <article
      className="tdv2-other-card"
      data-testid={`other-card-${delta.id}`}
    >
      <button
        type="button"
        className="tdv2-other-row"
        onClick={onOpen}
        aria-expanded={false}
        aria-controls={`other-detail-${delta.id}`}
        data-testid={`other-row-${delta.id}`}
      >
        <div>
          <div className="tdv2-other-row__title">{delta.title}</div>
          {delta.summaryLine ? (
            <div className="tdv2-other-row__summary">{delta.summaryLine}</div>
          ) : null}
          {delta.keyMetrics.length > 0 ? (
            <div className="tdv2-other-row__metrics">
              {compactMetricsLine(delta.keyMetrics)}
            </div>
          ) : null}
        </div>
        <div className="tdv2-other-row__chev">
          <StatusChip status={delta.status} />
          <span className="tdv2-other-row__caret" aria-hidden="true">▾</span>
        </div>
      </button>
    </article>
  );
}

function statusModifier(status: DeltaStatus): string {
  if (status === "delegatable") return "tdv2-review-card--delegatable";
  if (status === "monitoring") return "tdv2-review-card--monitoring";
  if (status === "contested" || status === "correction_submitted") {
    return "tdv2-review-card--contested";
  }
  return "tdv2-review-card--critical";
}

function ExpandedReviewCard({
  delta,
  applying,
  position,
  onCollapse,
  onAccept,
  onDelegate,
  onCorrect,
  onOpenEvidence,
}: {
  delta: DecisionDelta;
  applying: boolean;
  position: { index: number; total: number } | null;
  onCollapse: () => void;
  onAccept: () => void;
  onDelegate: () => void;
  onCorrect: () => void;
  onOpenEvidence: () => void;
}) {
  const canAccept = hasAction(delta, "accept");
  const canDelegate = hasAction(delta, "delegate");
  const canCorrect = hasAction(delta, "report_correction");
  const isDelegatable = delta.status === "delegatable";

  return (
    <article
      className={`tdv2-review-card ${statusModifier(delta.status)}`}
      data-testid={`other-card-${delta.id}`}
      id={`other-detail-${delta.id}`}
    >
      <header className="tdv2-review-card__head">
        {position ? (
          <span className="tdv2-review-card__position">
            Reviewing {position.index + 1} of {position.total}
          </span>
        ) : (
          <span />
        )}
        <button
          type="button"
          className="tdv2-review-card__collapse"
          onClick={onCollapse}
          data-testid={`other-collapse-${delta.id}`}
        >
          Collapse review
        </button>
      </header>

      <div className="tdv2-review-card__identity">
        <div className="tdv2-label-row">
          <div className="tdv2-label">Proposed change</div>
          <StatusChip status={delta.status} />
        </div>
        <h2 className="tdv2-review-card__title">{delta.title}</h2>
        {delta.summaryLine ? (
          <p className="tdv2-review-card__summary">{delta.summaryLine}</p>
        ) : null}
        {delta.keyMetrics.length > 0 ? (
          <div className="tdv2-metrics" data-testid={`key-metrics-${delta.id}`}>
            {delta.keyMetrics.slice(0, 4).map((m, i) => (
              <span
                key={i}
                className={`tdv2-metric${
                  m.severity === "critical"
                    ? " tdv2-metric--critical"
                    : m.severity === "high"
                      ? " tdv2-metric--high"
                      : ""
                }`}
              >
                {m.label}
              </span>
            ))}
          </div>
        ) : null}
      </div>

      <InlineDetail
        delta={delta}
        onOpenEvidence={onOpenEvidence}
      />

      <div className="tdv2-review-card__action-bar" data-testid={`action-bar-${delta.id}`}>
        {isDelegatable ? (
          <>
            {canDelegate ? (
              <button
                type="button"
                className="tdv2-btn tdv2-btn--primary"
                onClick={onDelegate}
                data-testid={`other-delegate-${delta.id}`}
              >
                Delegate
              </button>
            ) : null}
            {canAccept ? (
              <button
                type="button"
                className="tdv2-btn tdv2-btn--secondary"
                onClick={onAccept}
                disabled={applying}
                data-testid={`other-accept-${delta.id}`}
              >
                {applying ? "Applying..." : "Accept change"}
              </button>
            ) : null}
          </>
        ) : (
          <>
            {canAccept ? (
              <button
                type="button"
                className="tdv2-btn tdv2-btn--primary"
                onClick={onAccept}
                disabled={applying}
                data-testid={`other-accept-${delta.id}`}
              >
                {applying ? "Applying..." : "Accept change"}
              </button>
            ) : null}
            {canDelegate ? (
              <button
                type="button"
                className="tdv2-btn tdv2-btn--secondary"
                onClick={onDelegate}
                data-testid={`other-delegate-${delta.id}`}
              >
                Delegate
              </button>
            ) : null}
          </>
        )}
        <button
          type="button"
          className="tdv2-btn tdv2-btn--tertiary"
          onClick={onOpenEvidence}
          data-testid={`other-review-evidence-${delta.id}`}
        >
          Review evidence
        </button>
        {canCorrect ? (
          <button
            type="button"
            className="tdv2-btn tdv2-btn--correction"
            onClick={onCorrect}
            data-testid={`other-correct-${delta.id}`}
          >
            Report correction
          </button>
        ) : null}
      </div>
    </article>
  );
}
