// Full Detail slide-in sheet (design fix spec §4.6).
//
// Triggered explicitly by "Open full detail" on the NodeZoom toolbar.
// This is allowed to be a drawer / modal — unlike NodeZoom which must
// always preserve spatial context, Full Detail is a deliberate
// secondary state where the user has asked for everything we know
// about a single claim.

import { useEffect } from "react";
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
