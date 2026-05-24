import { useNavigate } from "react-router-dom";

import type { SpecDelta } from "@/api/spec-delta-types";

import { CausalSpine } from "./CausalSpine";
import { Confidence } from "./Confidence";
import { ConsequencePreviewView } from "./ConsequencePreview";
import { ContextGapList } from "./ContextGaps";
import { EvidenceTraceView } from "./EvidenceTraceView";
import { SourceCoverageList } from "./SourceCoverage";

interface Props {
  delta: SpecDelta;
  onClose: () => void;
  onAccept?: () => void;
  onDelegate?: () => void;
  onContest?: () => void;
  onAddContext?: () => void;
  onSnooze?: () => void;
}

// Decision Delta inspector — spec §11.9 sections:
//   Proposal · Current · Proposed · Why this surfaced · Evidence
//   · Source coverage · What may be missing · If accepted · Related
//   thread · Actions
export function DecisionDeltaInspector({
  delta,
  onClose,
  onAccept,
  onDelegate,
  onContest,
  onAddContext,
  onSnooze,
}: Props) {
  const navigate = useNavigate();

  return (
    <div className="fx-inspector" data-testid="delta-inspector">
      <header className="fx-inspector__head">
        <div>
          <div className="fx-delta__type">{delta.userFacingType}</div>
          <h2 className="fx-inspector__title">{delta.proposal}</h2>
          {delta.sourceThreadTitle ? (
            <div className="fx-inspector__subtitle">
              From{" "}
              <button
                type="button"
                className="fx-btn fx-btn--ghost fx-btn--sm"
                onClick={() => navigate(`/model?thread=${delta.sourceThreadId ?? ""}`)}
              >
                {delta.sourceThreadTitle} →
              </button>
            </div>
          ) : null}
        </div>
        <button type="button" className="fx-inspector__close" aria-label="Close" onClick={onClose}>×</button>
      </header>

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Current state</div>
        <div className="fx-inspector__body">{delta.currentState}</div>
      </section>

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Proposed state</div>
        <div className="fx-inspector__body"><strong>{delta.proposedState}</strong></div>
      </section>

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Why this surfaced</div>
        <ul className="fx-inspector__body" style={{ paddingLeft: 18 }}>
          {delta.whySurfaced.map((w, i) => <li key={i}>{w}</li>)}
        </ul>
      </section>

      {delta.confidence != null ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Confidence</div>
          <Confidence value={delta.confidence} basis={delta.confidenceBasis} />
        </section>
      ) : null}

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Evidence trace</div>
        <EvidenceTraceView trace={delta.evidenceTrace} />
      </section>

      <section className="fx-inspector__section">
        <div className="fx-inspector__section-label">Source coverage</div>
        <SourceCoverageList items={delta.sourceCoverage} />
      </section>

      <ContextGapList
        gaps={delta.contextGaps}
        onAddContext={onAddContext}
        onConnectSource={onAddContext}
        onAskOwner={onAddContext}
      />

      {delta.falsificationCondition ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Would revise if</div>
          <div className="fx-inspector__body fx-muted" style={{ fontSize: 13 }}>
            {delta.falsificationCondition}
          </div>
        </section>
      ) : null}

      <ConsequencePreviewView ops={delta.consequencePreview} />

      <div className="fx-inspector__actions">
        {onAccept ? (
          <button type="button" className="fx-btn fx-btn--gold" onClick={onAccept}>
            Accept change
          </button>
        ) : null}
        {onDelegate ? (
          <button type="button" className="fx-btn" onClick={onDelegate}>
            Delegate
          </button>
        ) : null}
        {onContest ? (
          <button type="button" className="fx-btn fx-btn--coral" onClick={onContest}>
            This looks wrong
          </button>
        ) : null}
        {onAddContext ? (
          <button type="button" className="fx-btn fx-btn--ghost" onClick={onAddContext}>
            Add context
          </button>
        ) : null}
        {onSnooze ? (
          <button type="button" className="fx-btn fx-btn--ghost" onClick={onSnooze}>
            Snooze
          </button>
        ) : null}
      </div>

      {delta.sourceThreadId ? (
        <section className="fx-inspector__section">
          <div className="fx-inspector__section-label">Source operating thread</div>
          <CausalSpine
            cells={[
              { label: "Thread", value: delta.sourceThreadTitle ?? "—" },
              { label: "Category", value: delta.category ?? "—" },
              { label: "Severity", value: delta.severity },
              { label: "Updated", value: formatRel(delta.updatedAt) },
              { label: "Source", value: delta.sourceCoverage[0]?.label ?? "—" },
            ]}
          />
        </section>
      ) : null}
    </div>
  );
}

function formatRel(iso: string): string {
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
