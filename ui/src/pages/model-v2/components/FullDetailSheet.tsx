// Full Detail slide-in sheet (design fix spec §4.6).
//
// Triggered explicitly by "Open full detail" on the NodeZoom toolbar.
// This is allowed to be a drawer / modal — unlike NodeZoom which must
// always preserve spatial context, Full Detail is a deliberate
// secondary state where the user has asked for everything we know
// about a single claim.

import { useEffect } from "react";
import type { ResolutionThread } from "@/api/resolution-thread-types";
import type { ItemDetail } from "../types";
import { StatusChip } from "./primitives";

export function FullDetailSheet({
  detail,
  onClose,
}: {
  detail: ItemDetail;
  onClose: () => void;
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const { item, neighbors, evidence, missingContext } = detail;
  const outgoing = neighbors.outgoing ?? [];
  const incoming = neighbors.incoming ?? [];
  return (
    <div
      className="fm-detail"
      role="dialog"
      aria-modal="true"
      aria-label="Full claim detail"
      data-testid="full-detail-sheet"
    >
      <div className="fm-detail__shade" onClick={onClose} aria-hidden="true" />
      <aside className="fm-detail__panel">
        <header className="fm-detail__head">
          <div className="fm-detail__category">
            {humanCategory(item.categoryId)}
          </div>
          <button
            type="button"
            className="fm-detail__close"
            onClick={onClose}
            aria-label="Close"
          >
            ×
          </button>
        </header>
        <h2 className="fm-detail__assertion">{item.assertion}</h2>
        <div className="fm-detail__meta">
          <StatusChip status={item.status} />
          {item.owner ? <span>Owner: {item.owner}</span> : null}
          {typeof item.confidence === "number" ? (
            <span>Confidence {Math.round((item.confidence ?? 0) * 100)}%</span>
          ) : null}
          {item.authority ? <span>Authority: {item.authority.replace(/_/g, " ")}</span> : null}
        </div>

        {item.dealReality ? <DealRealityBrief detail={detail} /> : null}

        <section className="fm-detail__section">
          <h3>Subject &amp; type</h3>
          <dl>
            <dt>Type</dt>
            <dd>{item.propositionKind ?? humanCategory(item.categoryId)}</dd>
            {item.lifecycle?.createdAt ? (
              <>
                <dt>Created</dt>
                <dd>{humanDate(item.lifecycle.createdAt)}</dd>
              </>
            ) : null}
            {item.lifecycle?.lastConfirmedAt ? (
              <>
                <dt>Last confirmed</dt>
                <dd>{humanDate(item.lifecycle.lastConfirmedAt)}</dd>
              </>
            ) : null}
          </dl>
        </section>

        {evidence.length > 0 ? (
          <section className="fm-detail__section">
            <h3>Supporting evidence</h3>
            <ul>
              {evidence.map((e) => (
                <li key={e.id}>
                  <strong>{e.source}:</strong> {e.summary}
                </li>
              ))}
            </ul>
          </section>
        ) : null}

        {item.falsificationConditions && item.falsificationConditions.length > 0 ? (
          <section className="fm-detail__section">
            <h3>Falsification conditions</h3>
            <ul>
              {item.falsificationConditions.map((f, i) => (
                <li key={i}>{f}</li>
              ))}
            </ul>
          </section>
        ) : null}

        <section className="fm-detail__section">
          <h3>Depends on ({incoming.length})</h3>
          {incoming.length === 0 ? (
            <p className="fm-detail__empty">No upstream dependencies recorded.</p>
          ) : (
            <ul>
              {incoming.slice(0, 8).map((r) => (
                <li key={r.id}>
                  <span className="fm-detail__verb">{r.verb}</span>{" "}
                  {r.sourceItem.shortLabel}
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="fm-detail__section">
          <h3>Supports / affects ({outgoing.length})</h3>
          {outgoing.length === 0 ? (
            <p className="fm-detail__empty">No downstream impact recorded.</p>
          ) : (
            <ul>
              {outgoing.slice(0, 8).map((r) => (
                <li key={r.id}>
                  <span className="fm-detail__verb">{r.verb}</span>{" "}
                  {r.targetItem.shortLabel}
                </li>
              ))}
            </ul>
          )}
        </section>

        {missingContext.length > 0 ? (
          <section className="fm-detail__section">
            <h3>Missing context</h3>
            <ul>
              {missingContext.map((m, i) => (
                <li key={i}>
                  <strong>{m.reason}</strong>
                  <br />
                  <span className="fm-muted">{m.impact}</span>
                </li>
              ))}
            </ul>
          </section>
        ) : null}

        <footer className="fm-detail__foot">
          <button type="button" className="fm-detail__btn">
            Report correction
          </button>
        </footer>
      </aside>
    </div>
  );
}

function DealRealityBrief({ detail }: { detail: ItemDetail }) {
  const deal = detail.item.dealReality;
  if (!deal) return null;
  return (
    <section className="fm-detail__section fm-deal-brief" data-testid="deal-reality-brief">
      <h3>Deal Reality</h3>
      <div className="fm-deal-brief__assessment">
        <span>Current assessment</span>
        <strong>{deal.stageAssessment}</strong>
      </div>
      <div className="fm-deal-brief__metrics">
        <Metric label="Consensus" value={`${deal.consensusScore}/100`} />
        <Metric label="Forecast" value={deal.forecastRecommendation} />
        <Metric label="Health" value={titleize(deal.dealHealth)} />
      </div>
      <div className="fm-deal-brief__callout">
        <span>Next best proof</span>
        <p>{deal.recommendedNextProof}</p>
      </div>
      {deal.managerRecommendation ? (
        <div className="fm-deal-brief__callout fm-deal-brief__callout--manager">
          <span>Manager recommendation</span>
          <p>{deal.managerRecommendation}</p>
        </div>
      ) : null}

      {deal.resolutionThread ? (
        <DealResolutionThread thread={deal.resolutionThread} />
      ) : null}

      <div className="fm-deal-brief__subsection">
        <h4>Buyer consensus</h4>
        <div className="fm-deal-brief__stakeholders">
          {deal.buyerConsensus.map((s) => (
            <div key={s.role} className="fm-deal-brief__stakeholder">
              <strong>{s.label}</strong>
              <span>{s.status}</span>
              {s.concern ? <small>{s.concern}</small> : null}
            </div>
          ))}
        </div>
      </div>

      {deal.proofRequirements.length > 0 ? (
        <div className="fm-deal-brief__subsection">
          <h4>Proof requirements</h4>
          <ul>
            {deal.proofRequirements.map((p) => (
              <li key={`${p.requirement}-${p.stakeholder}`}>
                <strong>{p.requirement}</strong>
                <br />
                <span className="fm-muted">
                  {p.status} · {p.stakeholder}
                  {p.owner ? ` · owner: ${p.owner}` : ""}
                </span>
                {p.recommendedAction ? (
                  <>
                    <br />
                    <span>{p.recommendedAction}</span>
                  </>
                ) : null}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {deal.internalBlockers.length > 0 ? (
        <div className="fm-deal-brief__subsection">
          <h4>Hidden internal blockers</h4>
          <ul>
            {deal.internalBlockers.map((b) => (
              <li key={`${b.blocker}-${b.source}`}>
                <strong>{b.blocker}</strong>
                <br />
                <span className="fm-muted">{b.source} · {b.impact}</span>
                {b.recommendedAction ? (
                  <>
                    <br />
                    <span>{b.recommendedAction}</span>
                  </>
                ) : null}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {deal.similarDeals && deal.similarDeals.length > 0 ? (
        <div className="fm-deal-brief__subsection">
          <h4>Similarity memory</h4>
          <ul>
            {deal.similarDeals.map((s) => (
              <li key={s.name}>
                <strong>{s.name}</strong>
                <br />
                <span>{s.pattern}</span>
                {s.appliedLesson ? (
                  <>
                    <br />
                    <span className="fm-muted">{s.appliedLesson}</span>
                  </>
                ) : null}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {deal.counterevidence && deal.counterevidence.length > 0 ? (
        <div className="fm-deal-brief__subsection">
          <h4>Counterevidence</h4>
          <ul>
            {deal.counterevidence.map((c, i) => (
              <li key={i}>{c}</li>
            ))}
          </ul>
        </div>
      ) : null}
    </section>
  );
}

function DealResolutionThread({ thread }: { thread: ResolutionThread }) {
  return (
    <div
      className="fm-deal-brief__subsection fm-resolution"
      data-testid={`model-resolution-thread-${thread.id}`}
    >
      <div className="fm-resolution__head">
        <div>
          <h4>Resolution tracker</h4>
          <strong>{thread.title}</strong>
        </div>
        <span className={`fm-resolution__status fm-resolution__status--${thread.status}`}>
          {titleize(thread.status)}
        </span>
      </div>

      <div className="fm-resolution__states">
        <div>
          <span>Current</span>
          <p>{thread.currentState}</p>
        </div>
        <div>
          <span>Target</span>
          <p>{thread.targetState}</p>
        </div>
      </div>

      <p className="fm-resolution__meta">
        Owner: {thread.owner}
        {thread.nextReviewAt ? ` · Review ${humanDate(thread.nextReviewAt)}` : ""}
      </p>

      <div className="fm-resolution__grid">
        <div>
          <span className="fm-resolution__eyebrow">Work to complete</span>
          <ul className="fm-resolution__list">
            {thread.steps.slice(0, 4).map((step) => (
              <li key={step.id}>
                <span className={`fm-resolution__dot fm-resolution__dot--${step.status}`} />
                <div>
                  <strong>{step.label}</strong>
                  <small>{titleize(step.status)} · {step.owner}</small>
                </div>
              </li>
            ))}
          </ul>
        </div>
        <div>
          <span className="fm-resolution__eyebrow">Fyralis watches</span>
          <ul className="fm-resolution__watch">
            {thread.watchedSignals.map((signal) => (
              <li key={signal.id}>
                <strong>{signal.label}</strong>
                <small>{titleize(signal.status)} · {signal.sourceType}</small>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="fm-deal-brief__metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function humanCategory(id: string): string {
  switch (id) {
    case "goals": return "Goal";
    case "commitments": return "Commitment";
    case "decisions": return "Decision";
    case "risks": return "Risk";
    case "customers": return "Customer";
    case "people": return "Team";
    case "systems": return "System";
    case "finance": return "Finance";
    default: return id;
  }
}

function humanDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

function titleize(value: string): string {
  return value.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
