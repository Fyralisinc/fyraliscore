// Primary Judgment Preview — spec §4.6.
//
// Bridge from Briefing Mode to Review Mode. NOT the full focused
// review sheet — this is a rich preview with one clear CTA. The label
// "Primary Judgment · 1 of N" sets context; the title + current→
// proposed summary + one-sentence "Why it's important" + compact
// "What happens if accepted" gives enough to decide whether to review.
//
// Clicking the CTA opens Review Mode for this proposed change.

import type { DecisionDelta } from "@/api/today-page-types";

interface Props {
  delta: DecisionDelta;
  total: number;
  onReview: () => void;
}

function confidenceLabel(
  c?: number | null,
): { pct: string; band: "low" | "moderate" | "high" } | null {
  if (c == null) return null;
  const pct = Math.round(c * 100);
  let band: "low" | "moderate" | "high" = "moderate";
  if (pct >= 75) band = "high";
  else if (pct < 55) band = "low";
  return { pct: `${pct}% confidence`, band };
}

function statusLabel(status: DecisionDelta["status"]): {
  label: string;
  tone: "authority" | "delegate" | "monitor" | "contest" | "neutral";
} {
  switch (status) {
    case "needs_authority": return { label: "Needs your authority", tone: "authority" };
    case "delegatable":     return { label: "Delegatable", tone: "delegate" };
    case "monitoring":      return { label: "Monitoring", tone: "monitor" };
    case "contested":       return { label: "Contested", tone: "contest" };
    case "correction_submitted": return { label: "Correction submitted", tone: "contest" };
    default:                return { label: status, tone: "neutral" };
  }
}

export function PrimaryJudgmentPreview({ delta, total, onReview }: Props) {
  const conf = confidenceLabel(delta.confidence);
  const status = statusLabel(delta.status);
  return (
    <section
      className={`tdv2-preview tdv2-preview--${status.tone}`}
      data-testid="primary-preview"
    >
      <div className="tdv2-preview__head">
        <span className="tdv2-preview__label">Primary judgment</span>
        <span className="tdv2-preview__count">1 of {total}</span>
        <span className={`tdv2-badge tdv2-badge--${status.tone}`}>
          <span className="tdv2-badge__dot" aria-hidden="true" />
          {status.label}
        </span>
      </div>
      <h2 className="tdv2-preview__title">{delta.title}</h2>
      <div className="tdv2-preview__line">
        {delta.summaryLine ? (
          <span className="tdv2-preview__summary">{delta.summaryLine}</span>
        ) : null}
        {conf ? (
          <span className={`tdv2-confidence tdv2-confidence--${conf.band}`}>
            {conf.pct}
          </span>
        ) : null}
      </div>
      {delta.whyThisMatters ? (
        <div className="tdv2-preview__why">
          <span className="tdv2-preview__why-label">Why it’s important</span>
          <p className="tdv2-preview__why-body">{delta.whyThisMatters}</p>
        </div>
      ) : null}
      {delta.impactIfAccepted.length > 0 ? (
        <div className="tdv2-preview__impact">
          <span className="tdv2-preview__impact-label">What happens if accepted</span>
          <p className="tdv2-preview__impact-body">
            {delta.impactIfAccepted
              .slice(0, 3)
              .map((i) => i.text)
              .join(" · ")}
          </p>
        </div>
      ) : null}
      <div className="tdv2-preview__cta-row">
        <button
          type="button"
          className="tdv2-preview__cta"
          onClick={onReview}
          data-testid="primary-preview-review"
        >
          Review this first
          <ArrowRight />
        </button>
      </div>
    </section>
  );
}

function ArrowRight() {
  return (
    <svg
      width="13"
      height="13"
      viewBox="0 0 13 13"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M2.5 6.5h8" />
      <path d="M7 3l3.5 3.5L7 10" />
    </svg>
  );
}
