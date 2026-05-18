// Focused review card — spec §9 + §10. The expanded judgment surface.
//
// Anatomy, top to bottom:
//   1. Utility row              — "Reviewing N of M" / "Collapse review"
//   2. Header                   — object-type label, status badge, title, subtitle, meta strip, grounded line
//   3. Current vs. proposed     — two panels joined by a quiet center axis
//   4. Review body (3 columns)  — Why this matters / Evidence / What may be missing
//   5. If accepted              — operational consequences
//   6. Ask Fyralis              — contextual reasoning strip
//   7. Action bar               — Accept · Delegate · Review evidence · Report correction

import type { DecisionDelta } from "@/api/today-page-types";
import { ChangeDiff } from "./ChangeDiff";
import { AskFyralisStrip } from "./AskFyralisStrip";

interface Props {
  delta: DecisionDelta;
  position?: { index: number; total: number } | null;
  applying?: boolean;
  onCollapse: () => void;
  onAccept: () => void;
  onDelegate: () => void;
  onCorrect: () => void;
  onOpenEvidence: () => void;
}

const CATEGORY_LABELS: Record<DecisionDelta["sourceCategory"], string> = {
  goals_priorities: "Goals & Priorities",
  commitments: "Commitments",
  decisions: "Decisions",
  risks_constraints: "Risks & Constraints",
  customers_revenue: "Customers & Revenue",
  people_teams: "People & Teams",
  systems_capacity: "Systems & Capacity",
  finance_capital: "Finance & Capital",
};

const STATUS_BADGES: Record<
  DecisionDelta["status"],
  { label: string; tone: "authority" | "delegate" | "monitor" | "contest" | "neutral" }
> = {
  needs_authority: { label: "Needs your authority", tone: "authority" },
  delegatable: { label: "Delegatable", tone: "delegate" },
  monitoring: { label: "Monitoring", tone: "monitor" },
  contested: { label: "Contested", tone: "contest" },
  correction_submitted: { label: "Correction submitted", tone: "contest" },
  accepted: { label: "Accepted", tone: "neutral" },
  delegated: { label: "Delegated", tone: "neutral" },
  archived: { label: "Archived", tone: "neutral" },
  failed_apply: { label: "Apply failed", tone: "contest" },
};

function relativeTime(iso: string): string {
  const created = new Date(iso).getTime();
  if (Number.isNaN(created)) return iso;
  const delta = Date.now() - created;
  const minutes = Math.floor(delta / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function proposedByLabel(p: DecisionDelta["proposedBy"]): string {
  if (p === "fyralis") return "Fyralis";
  if (p === "user") return "you";
  return "system";
}

function confidenceLabel(c?: number | null): {
  text: string;
  band: "low" | "moderate" | "high";
} | null {
  if (c == null) return null;
  const pct = Math.round(c * 100);
  let band: "low" | "moderate" | "high" = "moderate";
  if (pct >= 75) band = "high";
  else if (pct < 55) band = "low";
  const word = band === "high" ? "High" : band === "low" ? "Low" : "Moderate";
  return { text: `${word} confidence`, band };
}

export function FocusedReviewCard({
  delta,
  position,
  applying = false,
  onCollapse,
  onAccept,
  onDelegate,
  onCorrect,
  onOpenEvidence,
}: Props) {
  const badge = STATUS_BADGES[delta.status];
  const conf = confidenceLabel(delta.confidence);
  const zeroEvidence = delta.evidenceSummary.totalSignals === 0;
  const canAccept = delta.availableActions.includes("accept");
  const canDelegate = delta.availableActions.includes("delegate");
  const canCorrect = delta.availableActions.includes("report_correction");
  const isDelegatable = delta.status === "delegatable";

  return (
    <article
      className={`tdv2-review tdv2-review--${badge.tone}`}
      data-testid={`focused-review-${delta.id}`}
      data-state={badge.tone}
      id={`focused-${delta.id}`}
      aria-label={`Reviewing proposed change: ${delta.title}. ${badge.label}.`}
    >
      <div className="tdv2-review__utility">
        {position ? (
          <span className="tdv2-review__position">
            Reviewing {position.index + 1} of {position.total}
          </span>
        ) : (
          <span />
        )}
        <button
          type="button"
          className="tdv2-review__collapse"
          onClick={onCollapse}
          data-testid={`focused-collapse-${delta.id}`}
        >
          <span>Collapse review</span>
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true">
            <path
              d="M3 7.5L6 4.5L9 7.5"
              stroke="currentColor"
              strokeWidth="1.4"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </button>
      </div>

      <header className="tdv2-review__header">
        <div className="tdv2-review__header-top">
          <span className="tdv2-review__kind">Proposed change</span>
          <span className={`tdv2-badge tdv2-badge--${badge.tone}`}>
            <span className="tdv2-badge__dot" aria-hidden="true" />
            {badge.label}
          </span>
        </div>
        <h2 className="tdv2-review__title">{delta.title}</h2>
        {delta.summaryLine ? (
          <p className="tdv2-review__subtitle">{delta.summaryLine}</p>
        ) : null}
        <p className="tdv2-review__meta">
          <span className="tdv2-review__meta-item">
            <CategoryIcon />
            From {CATEGORY_LABELS[delta.sourceCategory] ?? delta.sourceCategory}
          </span>
          <span className="tdv2-review__meta-sep" aria-hidden="true">·</span>
          <span className="tdv2-review__meta-item">
            Proposed by {proposedByLabel(delta.proposedBy)}
          </span>
          <span className="tdv2-review__meta-sep" aria-hidden="true">·</span>
          <span className="tdv2-review__meta-item">
            Created {relativeTime(delta.createdAt)}
          </span>
          {conf ? (
            <>
              <span className="tdv2-review__meta-sep" aria-hidden="true">·</span>
              <span className={`tdv2-confidence tdv2-confidence--${conf.band}`}>
                {conf.text}
              </span>
            </>
          ) : null}
        </p>
        <p className="tdv2-review__grounded">
          <ShieldIcon />
          Grounded in existing model items
        </p>
      </header>

      <ChangeDiff current={delta.currentState} proposed={delta.proposedState} />

      <div className="tdv2-review__body">
        <section className="tdv2-section">
          <h3 className="tdv2-section__heading">Why this matters</h3>
          <p className="tdv2-section__body">{delta.whyThisMatters}</p>
          {delta.relatedModelLinks.length > 0 ? (
            <a
              className="tdv2-section__link"
              href={delta.relatedModelLinks[0].href}
            >
              Learn more →
            </a>
          ) : null}
        </section>

        <section className="tdv2-section">
          <h3 className="tdv2-section__heading">Evidence</h3>
          {zeroEvidence ? (
            <p className="tdv2-section__body tdv2-section__body--muted">
              No new signals since the last evaluation. This proposed change
              is grounded in existing model items and historical context.
            </p>
          ) : (
            <>
              <p className="tdv2-section__lede">
                Grounded in existing model items and recent updates.
              </p>
              <ul className="tdv2-evidence">
                {delta.evidenceSummary.groups.map((g) => (
                  <li key={g.id} className="tdv2-evidence__row">
                    <span className="tdv2-evidence__label">{g.label}</span>
                    <span className="tdv2-evidence__count">{g.count}</span>
                    <span
                      className={`tdv2-evidence__dot tdv2-evidence__dot--${g.quality}`}
                      aria-hidden="true"
                    />
                  </li>
                ))}
              </ul>
            </>
          )}
          <button
            type="button"
            className="tdv2-section__link"
            onClick={onOpenEvidence}
            data-testid={`focused-review-evidence-link-${delta.id}`}
          >
            Review all evidence →
          </button>
        </section>

        <div className="tdv2-review__col-stack">
          <section className="tdv2-section">
            <h3 className="tdv2-section__heading">What may be missing</h3>
            {delta.missingContext.length > 0 ? (
              <ul className="tdv2-bullets">
                {delta.missingContext.map((m) => (
                  <li key={m.id}>{m.text}</li>
                ))}
              </ul>
            ) : (
              <p className="tdv2-section__body tdv2-section__body--muted">
                No major context gaps identified from connected sources.
              </p>
            )}
          </section>

          {delta.impactIfAccepted.length > 0 ? (
            <section className="tdv2-section">
              <h3 className="tdv2-section__heading">If accepted</h3>
              <ul className="tdv2-bullets tdv2-bullets--check">
                {delta.impactIfAccepted.slice(0, 6).map((i) => (
                  <li key={i.id}>
                    <CheckIcon />
                    <span>{i.text}</span>
                  </li>
                ))}
              </ul>
            </section>
          ) : null}
        </div>
      </div>

      <AskFyralisStrip delta={delta} />

      <div className="tdv2-review__actions" data-testid={`action-bar-${delta.id}`}>
        {canAccept ? (
          <button
            type="button"
            className="tdv2-act tdv2-act--primary"
            onClick={onAccept}
            disabled={applying}
            data-testid={`focused-accept-${delta.id}`}
            aria-label="Accept change"
          >
            <CheckCircleIcon />
            <span>{applying ? "Applying..." : "Accept change"}</span>
          </button>
        ) : null}
        {canDelegate ? (
          <button
            type="button"
            className={`tdv2-act tdv2-act--secondary${
              isDelegatable && canAccept ? " tdv2-act--emphasis" : ""
            }`}
            onClick={onDelegate}
            data-testid={`focused-delegate-${delta.id}`}
            aria-label="Delegate"
          >
            <UsersIcon />
            <span>Delegate</span>
          </button>
        ) : null}
        <button
          type="button"
          className="tdv2-act tdv2-act--secondary"
          onClick={onOpenEvidence}
          data-testid={`focused-review-evidence-${delta.id}`}
          aria-label="Review evidence"
        >
          <DocIcon />
          <span>Review evidence</span>
        </button>
        {canCorrect ? (
          <button
            type="button"
            className="tdv2-act tdv2-act--correction"
            onClick={onCorrect}
            data-testid={`focused-correct-${delta.id}`}
            aria-label="Report correction"
          >
            <FlagIcon />
            <span>Report correction</span>
          </button>
        ) : null}
      </div>
    </article>
  );
}

function CategoryIcon() {
  return (
    <svg
      width="13"
      height="13"
      viewBox="0 0 13 13"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.3"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <rect x="2" y="2.5" width="9" height="8" rx="1.2" />
      <path d="M4.5 5.5h4M4.5 7.5h2.5" />
    </svg>
  );
}

function ShieldIcon() {
  return (
    <svg
      width="13"
      height="13"
      viewBox="0 0 13 13"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.3"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M6.5 1.8L11 3.4v3.2c0 2.3-1.8 4.2-4.5 5C3.8 10.8 2 8.9 2 6.6V3.4l4.5-1.6z" />
      <path d="M4.7 6.5l1.4 1.4 2.7-2.7" />
    </svg>
  );
}

function CheckIcon() {
  return (
    <svg
      width="11"
      height="11"
      viewBox="0 0 11 11"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M2 5.5L4.2 7.7L9 3" />
    </svg>
  );
}

function CheckCircleIcon() {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <circle cx="7.5" cy="7.5" r="6" />
      <path d="M4.8 7.6l2 2 3.4-3.7" />
    </svg>
  );
}

function UsersIcon() {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <circle cx="5.6" cy="6" r="2.1" />
      <circle cx="10.3" cy="6.6" r="1.6" />
      <path d="M1.8 12.5c.5-2 2-3 3.8-3s3.3 1 3.8 3" />
      <path d="M9.6 12.5c.3-1.4 1.2-2.2 2.4-2.2" />
    </svg>
  );
}

function DocIcon() {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M3.5 2h5.4L11.5 4.6V13H3.5z" />
      <path d="M5.5 6h4M5.5 8.2h4M5.5 10.4h2.5" />
    </svg>
  );
}

function FlagIcon() {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M3.5 13V2.6" />
      <path d="M3.5 2.6h7.3l-1.3 2.5 1.3 2.5h-7.3" />
    </svg>
  );
}
