import { useEffect, useRef, useState } from "react";
import type { Commitment } from "./types";
import { TERRITORY_LABELS } from "./positioning";

// Spec Part 7 — side panel that slides in when a dot is clicked.

type Props = {
  commitment: Commitment | null;
  onClose: () => void;
  onJumpToCommitment: (id: string) => void;
};

export function CommitmentPanel({
  commitment,
  onClose,
  onJumpToCommitment,
}: Props) {
  const [visible, setVisible] = useState(false);
  const [renderedId, setRenderedId] = useState<string | null>(null);
  const [bodyHidden, setBodyHidden] = useState(false);
  const closeRef = useRef<HTMLButtonElement | null>(null);
  const swapTimer = useRef<number | null>(null);

  // open / close transitions
  useEffect(() => {
    if (commitment) {
      setVisible(true);
      window.requestAnimationFrame(() => closeRef.current?.focus());
    } else {
      setVisible(false);
    }
  }, [commitment]);

  // when commitment id changes while panel is open, fade body out then in
  useEffect(() => {
    if (!commitment) {
      setRenderedId(null);
      return;
    }
    if (renderedId === null) {
      setRenderedId(commitment.id);
      return;
    }
    if (renderedId !== commitment.id) {
      setBodyHidden(true);
      if (swapTimer.current) window.clearTimeout(swapTimer.current);
      swapTimer.current = window.setTimeout(() => {
        setRenderedId(commitment.id);
        setBodyHidden(false);
      }, 150);
    }
  }, [commitment, renderedId]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape" && commitment) {
        e.preventDefault();
        onClose();
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [commitment, onClose]);

  if (!commitment && !renderedId) return null;
  const c = commitment ?? null;
  if (!c) return null;

  return (
    <aside
      className={"commitment-panel" + (visible ? " open" : "")}
      role="complementary"
      aria-label={`Commitment ${c.id} detail`}
    >
      <header className="panel-header">
        <span className="panel-territory">{TERRITORY_LABELS[c.territory]}</span>
        <span className="panel-id">{c.id}</span>
        <button
          ref={closeRef}
          type="button"
          className="panel-close"
          aria-label="Close"
          onClick={onClose}
        >
          ×
        </button>
      </header>

      <div className={"panel-body" + (bodyHidden ? " body-hidden" : "")}>
        <h2 className="panel-title">{c.label}</h2>

        <hr className="panel-divider" />

        <dl className="panel-keyvalue">
          <dt>Status</dt>
          <dd className="status" data-status={c.status}>
            {c.status.replace("-", " ").toUpperCase()}
          </dd>
          <dt>Owner</dt>
          <dd>{c.owner_display}</dd>
          <dt>Due</dt>
          <dd>{formatDate(c.due_date)}</dd>
          <dt>Created</dt>
          <dd>{formatDate(c.created_date)}</dd>
          {c.progress ? (
            <>
              <dt>Progress</dt>
              <dd>{c.progress}</dd>
            </>
          ) : null}
        </dl>

        {c.traces_to.length > 0 ? (
          <>
            <hr className="panel-divider" />
            <section className="panel-section">
              <span className="panel-section-label">Traces to</span>
              {c.traces_to.map((d) => (
                <a
                  key={d}
                  className="panel-link"
                  data-target={d}
                  href={`#${d}`}
                  onClick={(e) => e.preventDefault()}
                >
                  {d} · decision lineage
                </a>
              ))}
              {c.substrate_insight ? (
                <p className="panel-substrate-insight">↑ {c.substrate_insight}</p>
              ) : null}
            </section>
          </>
        ) : c.substrate_insight ? (
          <>
            <hr className="panel-divider" />
            <section className="panel-section">
              <p className="panel-substrate-insight">↑ {c.substrate_insight}</p>
            </section>
          </>
        ) : null}

        <hr className="panel-divider" />
        <section className="panel-section">
          <span className="panel-section-label">Stakeholder</span>
          <p>
            {c.stakeholder === "internal" ? "Internal" : "Customer"} ·{" "}
            {c.stakeholder_label}
          </p>
        </section>

        {c.related.length > 0 ? (
          <>
            <hr className="panel-divider" />
            <section className="panel-section">
              <span className="panel-section-label">Related commitments</span>
              {c.related.map((rid) => (
                <a
                  key={rid}
                  className="panel-link"
                  data-commitment={rid}
                  href={`#${rid}`}
                  onClick={(e) => {
                    e.preventDefault();
                    onJumpToCommitment(rid);
                  }}
                >
                  {rid}
                </a>
              ))}
            </section>
          </>
        ) : null}

        {c.activity.length > 0 ? (
          <>
            <hr className="panel-divider" />
            <section className="panel-section">
              <span className="panel-section-label">Recent activity</span>
              <ul className="activity-log">
                {c.activity.map((a, i) => (
                  <li key={i}>
                    <span className="activity-date">
                      {formatActivityDate(a.date)}
                    </span>
                    <span className="activity-desc">{a.desc}</span>
                  </li>
                ))}
              </ul>
            </section>
          </>
        ) : null}
      </div>
    </aside>
  );
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}
function formatActivityDate(iso: string): string {
  const d = new Date(iso);
  return d
    .toLocaleDateString("en-US", { month: "short", day: "numeric" })
    .toLowerCase();
}
