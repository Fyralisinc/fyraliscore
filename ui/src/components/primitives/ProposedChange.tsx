import type { ReactNode } from "react";

export type ProposedChangeLabel =
  | "PROPOSED CHANGE"
  | "NEEDS YOUR REVIEW"
  | "AUTHORITY REQUIRED"
  | "RECOMMENDED UPDATE";

export interface ProposedChangeAction {
  key: string;
  label: string;
  variant?: "primary" | "secondary" | "review";
  onClick?: () => void;
}

export interface ProposedChangeProps {
  label: ProposedChangeLabel;
  mainAssertion: ReactNode;
  currentState: ReactNode;
  suggestedUpdate: ReactNode;
  evidence?: ReactNode;
  whatMayBeMissing?: ReactNode;
  falsificationCondition?: ReactNode;
  consequencePreview?: ReactNode;
  actions?: ProposedChangeAction[];
}

export function ProposedChange({
  label,
  mainAssertion,
  currentState,
  suggestedUpdate,
  evidence,
  whatMayBeMissing,
  falsificationCondition,
  consequencePreview,
  actions,
}: ProposedChangeProps) {
  return (
    <article className="fy-proposed-change">
      <header className="fy-proposed-change__label">{label}</header>
      <div className="fy-proposed-change__assertion">{mainAssertion}</div>

      <div className="fy-proposed-change__state-row">
        <div>
          <div className="fy-proposed-change__state-label">Current</div>
          <div className="fy-proposed-change__state-value">{currentState}</div>
        </div>
        <div className="fy-proposed-change__arrow" aria-hidden="true">
          &rarr;
        </div>
        <div>
          <div className="fy-proposed-change__state-label">Suggested</div>
          <div className="fy-proposed-change__state-value">
            {suggestedUpdate}
          </div>
        </div>
      </div>

      {evidence ? (
        <section className="fy-proposed-change__section">
          <div className="fy-proposed-change__section-title">
            Evidence chain
          </div>
          {evidence}
        </section>
      ) : null}

      {whatMayBeMissing ? (
        <section className="fy-proposed-change__section">
          <div className="fy-proposed-change__section-title">
            What may be missing
          </div>
          {whatMayBeMissing}
        </section>
      ) : null}

      {falsificationCondition ? (
        <section className="fy-proposed-change__section">
          {falsificationCondition}
        </section>
      ) : null}

      {consequencePreview ? (
        <section className="fy-proposed-change__section">
          {consequencePreview}
        </section>
      ) : null}

      {actions && actions.length > 0 ? (
        <div className="fy-proposed-change__actions">
          {actions.map((action) => (
            <button
              key={action.key}
              type="button"
              className={`fy-btn fy-btn--${action.variant ?? "secondary"}`}
              onClick={action.onClick}
            >
              {action.label}
            </button>
          ))}
        </div>
      ) : null}
    </article>
  );
}

export default ProposedChange;
