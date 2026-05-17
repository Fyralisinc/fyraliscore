// Focused-review body — the deep content rendered inside an expanded
// proposed-change card on Today. Maps to spec §7.4–§7.10:
// Current → Proposed diff, Why this matters, Evidence quality, What
// may be missing, Ask Fyralis strip, Impact if accepted, Related model
// context. The parent card supplies the §7.1 header (Reviewing N of M,
// Collapse review) and the §7.11 sticky action bar.
//
// Keeps the legacy testid (`inline-detail-${id}`) so existing tests
// continue to anchor on the expansion content.

import type { DecisionDelta } from "@/api/today-page-types";
import { MiniDiff } from "./MiniDiff";
import { AskFyralisStrip } from "./AskFyralisStrip";

interface Props {
  delta: DecisionDelta;
  onOpenEvidence: () => void;
  // Set when the parent already renders an abbreviated diff (e.g. the
  // primary-judgment preview) and we want to avoid stacking two diffs.
  hideDiff?: boolean;
  // Set when the parent already renders the Why-this-matters block in
  // its own header (e.g. the primary-judgment preview).
  hideWhy?: boolean;
}

function labelForCategory(c: DecisionDelta["sourceCategory"]): string {
  switch (c) {
    case "goals_priorities":  return "Goals & Priorities";
    case "commitments":       return "Commitments";
    case "decisions":         return "Decisions";
    case "risks_constraints": return "Risks & Constraints";
    case "customers_revenue": return "Customers & Revenue";
    case "people_teams":      return "People & Teams";
    case "systems_capacity":  return "Systems & Capacity";
    case "finance_capital":   return "Finance & Capital";
    default:                  return c;
  }
}

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

export function InlineDetail({
  delta,
  onOpenEvidence,
  hideDiff = false,
  hideWhy = false,
}: Props) {
  // Spec §7.6: never combine high confidence with unexplained zero
  // evidence. If the wire returns zero signals, prefer the
  // missing-context list as the explanation.
  const zeroEvidence = delta.evidenceSummary.totalSignals === 0;

  return (
    <div className="tdv2-inline-detail" data-testid={`inline-detail-${delta.id}`}>
      <div className="tdv2-inline-detail__source">
        <span>From {labelForCategory(delta.sourceCategory)}</span>
        <span aria-hidden="true">·</span>
        <span>Proposed by {proposedByLabel(delta.proposedBy)}</span>
        <span aria-hidden="true">·</span>
        <span>Created {relativeTime(delta.createdAt)}</span>
      </div>

      {!hideDiff ? (
        <div className="tdv2-focused__section">
          <h3 className="tdv2-focused__section-label">Current → Proposed</h3>
          <MiniDiff
            current={delta.currentState}
            proposed={delta.proposedState}
            showHeader
          />
        </div>
      ) : null}

      {!hideWhy && delta.whyThisMatters ? (
        <div className="tdv2-why">
          <p className="tdv2-why__label">Why this matters</p>
          <p className="tdv2-why__body">{delta.whyThisMatters}</p>
        </div>
      ) : null}

      <div className="tdv2-focused__grid tdv2-focused__grid--two">
        <div className="tdv2-focused__section">
          <h3 className="tdv2-focused__section-label">
            Evidence quality · {delta.evidenceSummary.totalSignals} signals
          </h3>
          {zeroEvidence ? (
            <p className="tdv2-evidence-empty">
              No new signals since the last evaluation. This proposed
              change is grounded in existing model items — review them
              before accepting.
            </p>
          ) : (
            <ul className="tdv2-evidence-list">
              {delta.evidenceSummary.groups.map((g) => (
                <li key={g.id} className="tdv2-evidence-list__item">
                  <span>
                    {g.label}
                    <span style={{ color: "var(--text-muted)", marginLeft: "4px" }}>
                      ×{g.count}
                    </span>
                  </span>
                  <span
                    className={`tdv2-evidence-list__quality tdv2-evidence-list__quality--${g.quality}`}
                  >
                    {g.quality}
                  </span>
                </li>
              ))}
            </ul>
          )}
          <button
            type="button"
            className="tdv2-btn tdv2-btn--tertiary"
            onClick={onOpenEvidence}
            data-testid={`inline-review-evidence-${delta.id}`}
            style={{ alignSelf: "flex-start", padding: "6px 0" }}
          >
            Review all evidence →
          </button>
        </div>

        <div className="tdv2-focused__section">
          <h3 className="tdv2-focused__section-label">What may be missing</h3>
          {delta.missingContext.length > 0 ? (
            <ul className="tdv2-missing">
              {delta.missingContext.map((m) => (
                <li key={m.id} className="tdv2-missing__item">{m.text}</li>
              ))}
            </ul>
          ) : (
            <p className="tdv2-missing__empty">No major context gaps identified.</p>
          )}
        </div>
      </div>

      <AskFyralisStrip delta={delta} />

      {delta.impactIfAccepted.length > 0 ? (
        <div className="tdv2-impact">
          <p className="tdv2-impact__label">Impact if accepted</p>
          <ul className="tdv2-impact__list">
            {delta.impactIfAccepted.slice(0, 6).map((i) => (
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

      {delta.relatedModelLinks.length > 0 ? (
        <div className="tdv2-focused__section">
          <h3 className="tdv2-focused__section-label">Related model context</h3>
          <div className="tdv2-related">
            {delta.relatedModelLinks.map((l) => (
              <a key={l.category} href={l.href}>
                {l.label}
              </a>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
