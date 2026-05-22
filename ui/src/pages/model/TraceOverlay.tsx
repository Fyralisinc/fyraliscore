import type { OperatingThread } from "@/api/operating-thread-types";
import { EvidenceTraceView } from "@/components/spec/EvidenceTraceView";
import { useFyralisStore } from "@/lib/store";

interface Props {
  mode: "cause" | "consequence";
  thread: OperatingThread;
  onClose: () => void;
}

// Trace overlay — where graph-like structure is allowed (spec §12.10).
// We compose the chain from the spec evidence trace; the legacy
// LayeredGraph is reachable below via "Show structural map" but no
// longer the default Model view (spec §1.1).
export function TraceOverlay({ mode, thread, onClose }: Props) {
  const deltas = useFyralisStore((s) => s.deltas);
  const forecasts = useFyralisStore((s) => s.forecasts);

  // Pull the first related delta's evidence trace as the "cause" chain;
  // for "consequence", concatenate forecasts → commitments → deltas.
  const sampleDelta = deltas.find((d) => thread.relatedDecisionDeltaIds.includes(d.id));
  const sampleForecast = forecasts.find((f) => thread.relatedForecastIds.includes(f.id));

  const trace =
    mode === "cause"
      ? sampleDelta?.evidenceTrace
      : sampleForecast?.evidenceTrace ?? sampleDelta?.evidenceTrace;

  return (
    <div className="fx-trace-overlay" role="dialog" aria-modal="true">
      <header className="fx-trace-overlay__head">
        <div>
          <div className="fx-delta__type" style={{ color: "rgba(255,252,246,0.6)" }}>
            Trace · {mode}
          </div>
          <h2 className="fx-trace-overlay__title">{thread.title}</h2>
        </div>
        <button type="button" className="fx-btn" onClick={onClose}>Close</button>
      </header>
      <div className="fx-trace-overlay__body">
        <div style={{ padding: 24, overflow: "auto", height: "100%" }}>
          {trace ? (
            <EvidenceTraceView trace={trace} />
          ) : (
            <div className="fx-empty">
              <strong>No traceable chain yet for this thread.</strong>
              <div style={{ marginTop: 6 }}>
                Connect more sources or wait for the next synthesis pass.
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
