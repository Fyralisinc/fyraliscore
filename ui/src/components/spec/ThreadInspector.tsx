import { useNavigate } from "react-router-dom";

import type { OperatingThread } from "@/api/operating-thread-types";
import { useFyralisStore } from "@/lib/store";

import { CausalSpine } from "./CausalSpine";
import { Confidence } from "./Confidence";
import { ContextGapList } from "./ContextGaps";
import { SourceCoverageList } from "./SourceCoverage";
import { StatusPill } from "./StatusPill";

interface Props {
  thread: OperatingThread;
  onClose: () => void;
  onTraceCause?: () => void;
  onTraceConsequence?: () => void;
  onMarkWrong?: () => void;
  onCreateProposed?: () => void;
}

// Operating Thread inspector (spec §4.12 + §12.9).
// Sections: title · current reading · why this matters · causal spine ·
// what changed · hidden structure · accountability · evidence quality ·
// source coverage · context gaps · related deltas · related forecasts ·
// recent ledger events · actions.
export function ThreadInspector({
  thread,
  onClose,
  onTraceCause,
  onTraceConsequence,
  onMarkWrong,
  onCreateProposed,
}: Props) {
  const navigate = useNavigate();
  const allDeltas = useFyralisStore((s) => s.deltas);
  const allForecasts = useFyralisStore((s) => s.forecasts);
  const recent = useFyralisStore((s) => s.recentChanges);

  const relatedDeltas = allDeltas.filter((d) =>
    thread.relatedDecisionDeltaIds.includes(d.id)
  );
  const relatedForecasts = allForecasts.filter((f) =>
    thread.relatedForecastIds.includes(f.id)
  );
  const recentForThread = recent.filter((r) => r.threadId === thread.id);

  return (
    <div className="fx-inspector" data-testid="thread-inspector">
      <header className="fx-inspector__head">
        <div>
          <div className="fx-delta__type">Operating Thread</div>
          <h2 className="fx-inspector__title">{thread.title}</h2>
          <div className="fx-inspector__subtitle">
            <StatusPill status={thread.status} />
            <span style={{ marginLeft: 8 }}>Updated {formatRel(thread.lastUpdatedAt)}</span>
          </div>
        </div>
        <button type="button" className="fx-inspector__close" aria-label="Close" onClick={onClose}>×</button>
      </header>

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Current reading</div>
        <div className="fx-inspector__body">{thread.currentReading}</div>
      </section>

      {thread.whyThisMatters ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Why this matters</div>
          <div className="fx-inspector__body fx-muted" style={{ fontSize: 13 }}>
            {thread.whyThisMatters}
          </div>
        </section>
      ) : null}

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Causal spine</div>
        <CausalSpine cells={thread.causalRibbon} />
      </section>

      {thread.whatChanged && thread.whatChanged.length > 0 ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">What changed</div>
          <ul className="fx-inspector__body" style={{ paddingLeft: 18 }}>
            {thread.whatChanged.map((c, i) => (
              <li key={i}>
                <span className="fx-muted">{formatRel(c.at)}</span> — {c.note}
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {thread.hiddenStructure && thread.hiddenStructure.length > 0 ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Hidden structure</div>
          <ul className="fx-inspector__body" style={{ paddingLeft: 18 }}>
            {thread.hiddenStructure.map((s, i) => <li key={i}>{s}</li>)}
          </ul>
        </section>
      ) : null}

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Accountability</div>
        <div className="fx-inspector__body" style={{ fontSize: 13 }}>
          <div><strong>Owner:</strong> {thread.accountability.owner?.label ?? "Unassigned"}{thread.accountability.owner?.subtitle ? ` · ${thread.accountability.owner.subtitle}` : ""}</div>
          {thread.accountability.contributors.length > 0 ? (
            <div><strong>Contributors:</strong> {thread.accountability.contributors.map((c) => c.label).join(", ")}</div>
          ) : null}
          {thread.accountability.waitingOn.length > 0 ? (
            <div><strong>Waiting on:</strong> {thread.accountability.waitingOn.map((c) => c.label).join(", ")}</div>
          ) : null}
          {thread.accountability.loadSignal ? (
            <div className="fx-muted">{thread.accountability.loadSignal}</div>
          ) : null}
        </div>
      </section>

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Confidence & evidence</div>
        <Confidence value={thread.trust.confidence} previous={thread.trust.confidencePrevious} />
        <div className="fx-inspector__body fx-muted" style={{ fontSize: 12 }}>
          Evidence quality: <strong>{thread.trust.evidenceQuality}</strong>
        </div>
      </section>

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Source coverage</div>
        <SourceCoverageList items={thread.trust.sourceCoverage} />
      </section>

      <ContextGapList gaps={thread.trust.contextGaps} />

      {relatedDeltas.length > 0 ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Related Decision Deltas</div>
          <div className="fx-stack" style={{ gap: 6 }}>
            {relatedDeltas.map((d) => (
              <button
                key={d.id}
                type="button"
                className="fx-btn fx-btn--ghost fx-btn--sm"
                style={{ justifyContent: "flex-start", padding: "8px 10px", textAlign: "left", height: "auto" }}
                onClick={() => navigate(`/?delta=${d.id}`)}
              >
                {d.proposal}
              </button>
            ))}
          </div>
        </section>
      ) : null}

      {relatedForecasts.length > 0 ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Related Forecasts</div>
          <div className="fx-stack" style={{ gap: 6 }}>
            {relatedForecasts.map((f) => (
              <button
                key={f.id}
                type="button"
                className="fx-btn fx-btn--ghost fx-btn--sm"
                style={{ justifyContent: "flex-start", padding: "8px 10px", textAlign: "left", height: "auto" }}
                onClick={() => navigate(`/forecasts?forecast=${f.id}`)}
              >
                {f.statement}
              </button>
            ))}
          </div>
        </section>
      ) : null}

      {recentForThread.length > 0 ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Recent ledger events</div>
          <ul className="fx-inspector__body" style={{ paddingLeft: 18, fontSize: 13 }}>
            {recentForThread.map((r) => (
              <li key={r.id}>
                <span className="fx-muted">{formatRel(r.occurredAt)}</span> — {r.summary}
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      <div className="fx-inspector__actions">
        {onTraceCause ? (
          <button type="button" className="fx-btn" onClick={onTraceCause}>Trace cause</button>
        ) : null}
        {onTraceConsequence ? (
          <button type="button" className="fx-btn" onClick={onTraceConsequence}>Trace consequence</button>
        ) : null}
        {onCreateProposed ? (
          <button type="button" className="fx-btn fx-btn--gold" onClick={onCreateProposed}>Create proposed change</button>
        ) : null}
        {onMarkWrong ? (
          <button type="button" className="fx-btn fx-btn--coral" onClick={onMarkWrong}>This looks wrong</button>
        ) : null}
      </div>
    </div>
  );
}

function formatRel(iso: string): string {
  try {
    const d = new Date(iso);
    const ms = Date.now() - d.getTime();
    const m = Math.floor(ms / 60_000);
    if (m < 1) return "just now";
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    const days = Math.floor(h / 24);
    return `${days}d ago`;
  } catch {
    return iso;
  }
}
