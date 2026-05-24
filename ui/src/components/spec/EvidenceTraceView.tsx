import type { EvidenceTrace, EvidenceStep } from "@/api/trust-types";

interface Props {
  trace: EvidenceTrace;
  collapsed?: boolean;       // show summary one-liner only
  maxSteps?: number;
}

const KIND_LABEL: Record<EvidenceStep["kind"], string> = {
  observation: "Observation",
  claim: "Claim",
  pattern: "Pattern",
  belief: "Belief",
  recommendation: "Recommendation",
  forecast: "Forecast",
  commitment: "Commitment",
};

// Evidence Trace — vertical stepper per spec §8.4. Use focused mini-chain,
// not full graph. Compact preview when `collapsed` is true.
export function EvidenceTraceView({ trace, collapsed, maxSteps }: Props) {
  if (collapsed) {
    return (
      <div className="fx-trace">
        <div className="fx-trace__summary">{trace.summary}</div>
      </div>
    );
  }
  const steps = maxSteps ? trace.steps.slice(0, maxSteps) : trace.steps;
  return (
    <div className="fx-trace" aria-label="Evidence trace">
      <div className="fx-trace__summary">{trace.summary}</div>
      {trace.contested && trace.contestationNote ? (
        <div className="fx-error" style={{ marginBottom: 8 }}>
          Evidence is mixed. {trace.contestationNote}
        </div>
      ) : null}
      {steps.map((s) => (
        <div key={s.id} className="fx-trace__step">
          <span className={`fx-trace__dot fx-trace__dot--${s.kind}`} aria-hidden="true" />
          <div className="fx-trace__step-body">
            <div className="fx-trace__step-kind">{KIND_LABEL[s.kind]}</div>
            <div className="fx-trace__step-title">{s.title}</div>
            {s.description ? <div className="fx-trace__step-desc">{s.description}</div> : null}
            <div className="fx-trace__step-meta">
              {s.sourceLabel ? <span>{s.sourceLabel}</span> : null}
              {s.occurredAt ? <span>{formatTime(s.occurredAt)}</span> : null}
              {s.trustTier ? <span>· {s.trustTier}</span> : null}
              {s.restricted ? <span>· restricted</span> : null}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function formatTime(iso: string): string {
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
