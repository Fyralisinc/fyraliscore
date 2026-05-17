// Primary judgment card — spec §5.4 + §7. The hero of Briefing Mode.
//
// Default state (collapsed): partial expansion — label, status, title,
// summary line, key impact chips, short Why, mini-diff, impact list,
// action bar. Less detail than a full focused review (§5.4).
//
// Expanded state: clicking the title swaps in the §7 anatomy in the
// same slot — header strip (Reviewing N of M, Collapse review),
// identity, chips, deep review body via InlineDetail, sticky action
// bar. No navigation; surrounding compact cards stay visible (§6.3).

import type { DecisionDelta } from "@/api/today-page-types";
import { StatusChip } from "./StatusChip";
import { MiniDiff } from "./MiniDiff";
import { InlineDetail } from "./InlineDetail";

interface Props {
  delta: DecisionDelta;
  onAccept: () => void;
  onDelegate: () => void;
  onCorrect: () => void;
  onOpenEvidence: () => void;
  onToggleExpand: () => void;
  expanded: boolean;
  applying?: boolean;
  // Position of this card within the full Today queue (primary +
  // others). Used to render "Reviewing N of M" in expanded mode.
  position?: { index: number; total: number };
}

export function PrimaryJudgmentCard({
  delta,
  onAccept,
  onDelegate,
  onCorrect,
  onOpenEvidence,
  onToggleExpand,
  expanded,
  applying = false,
  position,
}: Props) {
  const canAccept = delta.availableActions.includes("accept");
  const canDelegate = delta.availableActions.includes("delegate");
  const canCorrect = delta.availableActions.includes("report_correction");
  const isDelegatable = delta.status === "delegatable";
  const isMonitoring = delta.status === "monitoring";
  const isContested =
    delta.status === "contested" || delta.status === "correction_submitted";

  const cardClass = [
    "tdv2-primary",
    expanded ? "tdv2-primary--expanded" : "",
    isDelegatable ? "tdv2-primary--delegatable" : "",
    isMonitoring ? "tdv2-primary--monitoring" : "",
    isContested ? "tdv2-primary--contested" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <section className={cardClass} data-testid="primary-judgment">
      {expanded ? (
        <header className="tdv2-review-card__head">
          {position ? (
            <span className="tdv2-review-card__position">
              Reviewing {position.index + 1} of {position.total}
            </span>
          ) : (
            <span className="tdv2-label">Primary judgment</span>
          )}
          <button
            type="button"
            className="tdv2-review-card__collapse"
            onClick={onToggleExpand}
            data-testid="primary-collapse"
          >
            Collapse review
          </button>
        </header>
      ) : (
        <div className="tdv2-label-row">
          <div className="tdv2-label">Primary judgment</div>
          <StatusChip status={delta.status} />
        </div>
      )}

      <div className="tdv2-review-card__identity">
        {expanded ? (
          <div className="tdv2-label-row">
            <div className="tdv2-label">Proposed change</div>
            <StatusChip status={delta.status} />
          </div>
        ) : null}
        {expanded ? (
          <h2 className="tdv2-review-card__title">{delta.title}</h2>
        ) : (
          <button
            type="button"
            className="tdv2-primary__title-btn"
            onClick={onToggleExpand}
            aria-expanded={expanded}
            aria-controls={`primary-detail-${delta.id}`}
            data-testid="primary-judgment-open"
          >
            <h2 className="tdv2-primary__title">{delta.title}</h2>
          </button>
        )}

        {delta.summaryLine ? (
          <p
            className={
              expanded ? "tdv2-review-card__summary" : "tdv2-primary__summary"
            }
          >
            {delta.summaryLine}
          </p>
        ) : null}

        {delta.keyMetrics.length > 0 ? (
          <div className="tdv2-metrics" data-testid="key-metrics">
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

      {!expanded ? (
        <>
          {delta.whyThisMatters ? (
            <div className="tdv2-why">
              <p className="tdv2-why__label">Why this matters</p>
              <p className="tdv2-why__body">{delta.whyThisMatters}</p>
            </div>
          ) : null}

          <MiniDiff
            current={delta.currentState}
            proposed={delta.proposedState}
            maxRows={3}
          />

          {delta.impactIfAccepted.length > 0 ? (
            <div className="tdv2-impact">
              <p className="tdv2-impact__label">If you accept</p>
              <ul className="tdv2-impact__list">
                {delta.impactIfAccepted.slice(0, 5).map((i) => (
                  <li key={i.id} className="tdv2-impact__item">
                    <span className="tdv2-impact__check" aria-hidden="true">
                      <svg width="8" height="8" viewBox="0 0 8 8" fill="none">
                        <path
                          d="M1.5 4.2L3 5.7l3.5-3.5"
                          stroke="currentColor"
                          strokeWidth="1.3"
                          strokeLinecap="round"
                          strokeLinejoin="round"
                        />
                      </svg>
                    </span>
                    <span>{i.text}</span>
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </>
      ) : (
        <div id={`primary-detail-${delta.id}`}>
          <InlineDetail delta={delta} onOpenEvidence={onOpenEvidence} />
        </div>
      )}

      <div
        className={
          expanded
            ? "tdv2-review-card__action-bar"
            : "tdv2-actions"
        }
      >
        {isDelegatable ? (
          <>
            {canDelegate ? (
              <button
                type="button"
                className="tdv2-btn tdv2-btn--primary"
                onClick={onDelegate}
                data-testid="primary-delegate"
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
                data-testid="primary-accept"
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
                data-testid="primary-accept"
              >
                {applying ? "Applying..." : "Accept change"}
              </button>
            ) : null}
            {canDelegate ? (
              <button
                type="button"
                className="tdv2-btn tdv2-btn--secondary"
                onClick={onDelegate}
                data-testid="primary-delegate"
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
          data-testid="primary-review-evidence"
        >
          Review evidence
        </button>
        {canCorrect ? (
          <button
            type="button"
            className="tdv2-btn tdv2-btn--correction"
            onClick={onCorrect}
            data-testid="primary-correct"
          >
            Report correction
          </button>
        ) : null}
      </div>
    </section>
  );
}
