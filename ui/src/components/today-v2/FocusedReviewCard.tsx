// Focused review sheet — the editorial review surface.
//
//   Reviewing 1 of N  ‹ ›                              Collapse · Needs your authority
//
//   PROPOSED CHANGE
//   Escalate customer risk for Salesforce sync instability
//   At watch → Critical
//   [78% confidence]
//   From Customers & Revenue · Proposed by Fyralis · Created May 17, 9:22 AM   View in Model →
//
//   WHY THIS MATTERS                                    POTENTIAL IMPACT
//   Three anchor customers are experiencing recurring   $2.04M at risk in renewal pipeline
//   sync failures. Renewal exposure is increasing.
//
//   CURRENT → PROPOSED
//   Risk level    At watch    →    Critical
//   Owner         Unassigned  →    VP Engineering
//   ...
//
//   EVIDENCE (4)                          WHAT MAY BE MISSING
//   ✓ 5 sync failure alerts in last 7d    ⚠ No RCA provided by Engineering yet
//   ✓ 3 support tickets from anchor ...   ⚠ No customer call transcripts this week
//   ...
//
//   WHAT HAPPENS IF ACCEPTED
//   [Create escalation] → [Notify VP Eng] → [Link 3 commitments] → [Re-evaluate 48h]
//
//   ASK FYRALIS ABOUT THIS CHANGE
//   [Why now?] [What if I wait?] [Who should own?] [What's weakest?] [What if we escalate?]
//   [Ask a question or request...                                            ↗]
//   Fyralis uses your company model and connected sources.   View conversation history

import type {
  DecisionDelta,
  ImpactItem,
  ModelCategoryKey,
} from "@/api/today-page-types";
import { AskFyralisStrip } from "./AskFyralisStrip";

interface Props {
  delta: DecisionDelta;
  position?: { index: number; total: number } | null;
  applying?: boolean;
  onOpenEvidence: () => void;
}

const CATEGORY_LABELS: Record<ModelCategoryKey, string> = {
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

function formatStamp(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

function proposedByLabel(p: DecisionDelta["proposedBy"]): string {
  if (p === "fyralis") return "Fyralis";
  if (p === "user") return "you";
  return "system";
}

function confLabel(
  c?: number | null,
): { pct: string; band: "low" | "moderate" | "high" } | null {
  if (c == null) return null;
  const pct = Math.round(c * 100);
  let band: "low" | "moderate" | "high" = "moderate";
  if (pct >= 75) band = "high";
  else if (pct < 55) band = "low";
  return { pct: `${pct}% confidence`, band };
}

export function FocusedReviewCard({ delta, position, onOpenEvidence }: Props) {
  const badge = STATUS_BADGES[delta.status];
  const conf = confLabel(delta.confidence);

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
            Reviewing <strong>{position.index + 1} of {position.total}</strong>
            <span className="tdv2-review__nav" aria-hidden="true">
              <button type="button" className="tdv2-review__nav-btn" disabled>
                <ChevLeft />
              </button>
              <button type="button" className="tdv2-review__nav-btn" disabled>
                <ChevRight />
              </button>
            </span>
          </span>
        ) : (
          <span />
        )}
        <span className="tdv2-review__util-right">
          <span className={`tdv2-badge tdv2-badge--${badge.tone}`}>
            <span className="tdv2-badge__dot" aria-hidden="true" />
            {badge.label}
          </span>
        </span>
      </div>

      <header className="tdv2-review__header">
        <span className="tdv2-review__kind">PROPOSED CHANGE</span>
        <h2
          className="tdv2-review__title"
          tabIndex={-1}
        >
          {delta.title}
        </h2>
        {delta.summaryLine ? (
          <p className="tdv2-review__subtitle">{delta.summaryLine}</p>
        ) : null}
        {conf ? (
          <span className={`tdv2-confidence tdv2-confidence--${conf.band}`}>
            {conf.pct}
          </span>
        ) : null}
        <div className="tdv2-review__meta">
          <span className="tdv2-review__meta-item">
            From {CATEGORY_LABELS[delta.sourceCategory] ?? delta.sourceCategory}
          </span>
          <Sep />
          <span className="tdv2-review__meta-item">
            Proposed by {proposedByLabel(delta.proposedBy)}
          </span>
          <Sep />
          <span className="tdv2-review__meta-item">
            Created {formatStamp(delta.createdAt)}
          </span>
          {delta.relatedModelLinks[0] ? (
            <a
              className="tdv2-review__meta-link"
              href={delta.relatedModelLinks[0].href}
            >
              View in Model →
            </a>
          ) : null}
        </div>
      </header>

      <WhyThisMatters delta={delta} />

      <CurrentProposedTable delta={delta} />

      <div className="tdv2-review__pair">
        <EvidenceSection delta={delta} onReview={onOpenEvidence} />
        <MissingSection delta={delta} />
      </div>

      <WhatHappensIfAccepted items={delta.impactIfAccepted} />

      <AskFyralisStrip delta={delta} />
    </article>
  );
}

// ---------------------------------------------------------------------

function WhyThisMatters({ delta }: { delta: DecisionDelta }) {
  // Pull a "potential impact" snippet from key metrics — the highest
  // severity money-like chip wins. Falls back to the first metric so
  // the right cell is never empty when there's any data.
  const impact = pickImpactMetric(delta);
  return (
    <section className="tdv2-review__why">
      <div className="tdv2-review__why-text">
        <h3 className="tdv2-section__heading">Why this matters</h3>
        <p className="tdv2-section__body">{delta.whyThisMatters}</p>
      </div>
      {impact ? (
        <aside className="tdv2-review__impact">
          <span className="tdv2-section__eyebrow">Potential impact</span>
          <span className="tdv2-review__impact-value">{impact.value}</span>
          <span className="tdv2-review__impact-sub">{impact.sub}</span>
        </aside>
      ) : null}
    </section>
  );
}

function pickImpactMetric(d: DecisionDelta): { value: string; sub: string } | null {
  const m = d.keyMetrics.find(
    (x) => x.severity === "critical" || x.severity === "high",
  );
  if (m) {
    const parts = String(m.label).split(/\s+/);
    const value = parts[0] ?? m.label;
    const sub = parts.slice(1).join(" ") || (m.unit ?? "at risk");
    return { value, sub: sub || "at risk" };
  }
  if (d.keyMetrics[0]) {
    const m0 = d.keyMetrics[0];
    const parts = String(m0.label).split(/\s+/);
    return {
      value: parts[0] ?? m0.label,
      sub: parts.slice(1).join(" ") || m0.unit || "",
    };
  }
  return null;
}

// ---------------------------------------------------------------------

function CurrentProposedTable({ delta }: { delta: DecisionDelta }) {
  const rows = buildRows(delta);
  if (rows.length === 0) return null;
  return (
    <section className="tdv2-review__diff" data-testid="change-diff">
      <h3 className="tdv2-section__heading">
        Current → proposed
      </h3>
      <div className="tdv2-diff" role="table">
        <div className="tdv2-diff__head" role="row">
          <span role="columnheader" />
          <span role="columnheader" className="tdv2-diff__head-cell">Current</span>
          <span role="columnheader" />
          <span role="columnheader" className="tdv2-diff__head-cell">Proposed</span>
        </div>
        {rows.map((r) => (
          <div key={r.key} className="tdv2-diff__row" role="row">
            <span role="cell" className="tdv2-diff__label">{r.label}</span>
            <span role="cell" className="tdv2-diff__from">{r.from || "—"}</span>
            <span role="cell" className="tdv2-diff__arrow" aria-hidden="true">→</span>
            <span
              role="cell"
              className={[
                "tdv2-diff__to",
                r.severity ? `tdv2-diff__to--${r.severity}` : "",
              ].join(" ").trim()}
            >
              {r.to || "—"}
            </span>
          </div>
        ))}
      </div>
    </section>
  );
}

interface DiffRow {
  key: string;
  label: string;
  from: string;
  to: string;
  severity?: string;
}

function buildRows(d: DecisionDelta): DiffRow[] {
  const map = new Map<string, DiffRow>();
  for (const f of d.currentState) {
    map.set(f.key, { key: f.key, label: f.label, from: f.value, to: "" });
  }
  for (const f of d.proposedState) {
    const prev = map.get(f.key);
    if (prev) {
      prev.to = f.value;
      prev.severity = f.severity;
    } else {
      map.set(f.key, {
        key: f.key,
        label: f.label,
        from: "",
        to: f.value,
        severity: f.severity,
      });
    }
  }
  return Array.from(map.values()).filter((r) => r.from !== r.to);
}

// ---------------------------------------------------------------------

function EvidenceSection({
  delta,
  onReview,
}: {
  delta: DecisionDelta;
  onReview: () => void;
}) {
  const total = delta.evidenceSummary.totalSignals;
  const groups = delta.evidenceSummary.groups;
  return (
    <section className="tdv2-section">
      <h3 className="tdv2-section__heading">
        Evidence <span className="tdv2-section__count">({total})</span>
      </h3>
      {total === 0 ? (
        <p className="tdv2-section__body tdv2-section__body--muted">
          No new signals since the last evaluation. This proposed change is
          grounded in existing model items.
        </p>
      ) : (
        <ul className="tdv2-evidence">
          {groups.map((g) => (
            <li key={g.id} className="tdv2-evidence__item">
              <CheckMark />
              <span className="tdv2-evidence__text">
                <span className="tdv2-evidence__main">
                  <strong>{g.count}</strong> {g.label.toLowerCase()}
                </span>
                <span className="tdv2-evidence__sub">{describeQuality(g.quality)}</span>
              </span>
            </li>
          ))}
        </ul>
      )}
      <button
        type="button"
        className="tdv2-section__link"
        onClick={onReview}
        data-testid={`focused-review-evidence-link-${delta.id}`}
      >
        Review all evidence →
      </button>
    </section>
  );
}

function describeQuality(q: string): string {
  switch (q) {
    case "strong":  return "Strong source quality";
    case "medium":  return "Medium source quality";
    case "partial": return "Partial source quality";
    case "weak":    return "Weak source quality";
    default:        return "";
  }
}

function MissingSection({ delta }: { delta: DecisionDelta }) {
  return (
    <section className="tdv2-section">
      <h3 className="tdv2-section__heading">What may be missing</h3>
      {delta.missingContext.length > 0 ? (
        <ul className="tdv2-missing">
          {delta.missingContext.map((m) => (
            <li key={m.id} className="tdv2-missing__item">
              <WarningMark />
              <span>{m.text}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="tdv2-section__body tdv2-section__body--muted">
          No major context gaps identified from connected sources.
        </p>
      )}
      {delta.relatedModelLinks[0] ? (
        <a className="tdv2-section__link" href={delta.relatedModelLinks[0].href}>
          Explore in Model →
        </a>
      ) : null}
    </section>
  );
}

// ---------------------------------------------------------------------

function WhatHappensIfAccepted({ items }: { items: ImpactItem[] }) {
  if (items.length === 0) return null;
  return (
    <section className="tdv2-section">
      <h3 className="tdv2-section__heading">What happens if accepted</h3>
      <ol className="tdv2-steps">
        {items.slice(0, 4).map((i, idx) => (
          <li key={i.id} className="tdv2-step">
            <span className="tdv2-step__icon" aria-hidden="true">
              <ImpactGlyph type={i.operationType} />
            </span>
            <span className="tdv2-step__text">{i.text}</span>
            {idx < Math.min(items.length, 4) - 1 ? (
              <span className="tdv2-step__sep" aria-hidden="true">→</span>
            ) : null}
          </li>
        ))}
      </ol>
    </section>
  );
}

function ImpactGlyph({ type }: { type: ImpactItem["operationType"] }) {
  const common = {
    width: 14,
    height: 14,
    viewBox: "0 0 14 14",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 1.4,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
  };
  switch (type) {
    case "create_node":
      return (
        <svg {...common}>
          <rect x="2" y="2" width="10" height="10" rx="1.5" />
          <path d="M7 4.5v5M4.5 7h5" />
        </svg>
      );
    case "update_node":
      return (
        <svg {...common}>
          <rect x="2" y="2" width="10" height="10" rx="1.5" />
          <path d="M4.5 7l1.5 1.5L9.5 5.5" />
        </svg>
      );
    case "notify_actor":
      return (
        <svg {...common}>
          <circle cx="5.5" cy="5.5" r="2" />
          <circle cx="10" cy="6" r="1.4" />
          <path d="M2 12c.5-2 1.8-3 3.5-3s3 1 3.5 3" />
        </svg>
      );
    case "link_nodes":
      return (
        <svg {...common}>
          <path d="M5 8a2 2 0 0 0 2 2h1a2 2 0 0 0 0-4" />
          <path d="M9 6a2 2 0 0 0-2-2H6a2 2 0 0 0 0 4" />
        </svg>
      );
    case "schedule_re_evaluation":
      return (
        <svg {...common}>
          <rect x="2.5" y="3" width="9" height="9" rx="1" />
          <path d="M2.5 6h9" />
          <path d="M5 2v2M9 2v2" />
        </svg>
      );
    default:
      return (
        <svg {...common}>
          <circle cx="7" cy="7" r="4" />
          <circle cx="7" cy="7" r="1.3" />
        </svg>
      );
  }
}

// ---------------------------------------------------------------------

function CheckMark() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      className="tdv2-check"
    >
      <circle cx="7" cy="7" r="6" />
      <path d="M4.3 7.2L6.2 9 9.8 5" />
    </svg>
  );
}

function WarningMark() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      className="tdv2-warning"
    >
      <path d="M7 2l5.5 10h-11z" />
      <path d="M7 6v3" />
      <circle cx="7" cy="10.5" r="0.4" fill="currentColor" />
    </svg>
  );
}

function ChevLeft() {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true">
      <path d="M7.5 2.5L4 6l3.5 3.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function ChevRight() {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true">
      <path d="M4.5 2.5L8 6l-3.5 3.5" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function Sep() {
  return <span className="tdv2-review__meta-sep" aria-hidden="true">·</span>;
}
