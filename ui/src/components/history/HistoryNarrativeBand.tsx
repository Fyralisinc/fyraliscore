import type {
  Arc,
  CalibrationSummary,
  HistoryEvent,
  HistoryLayerId,
  ShapeToken,
} from "./types";

// Spec Part 3 — narrative band with content per layer.
type Props = {
  layer: HistoryLayerId;
  statement: ShapeToken[];
  events: HistoryEvent[];
  arcs: Arc[];
  calibration: CalibrationSummary;
  onArcChip: (arcId: string) => void;
  onRef: (kind: string, id: string) => void;
};

export function HistoryNarrativeBand({
  layer,
  statement,
  events,
  arcs,
  calibration,
  onArcChip,
  onRef,
}: Props) {
  return (
    <section className="narrative-band" aria-label="Period summary">
      <div className="shape-statement">
        <p className="shape-statement-text">
          {statement.map((tok, i) =>
            tok.kind === "text" ? (
              <span key={i}>{tok.text}</span>
            ) : (
              <button
                key={i}
                type="button"
                className="ref"
                data-ref-type={tok.ref.type}
                onClick={() => onRef(tok.ref.type, tok.ref.id)}
              >
                {tok.ref.text}
              </button>
            )
          )}
        </p>
      </div>
      <div className="shape-data">
        {layer === "chronicle"
          ? renderChronicleData(events, arcs, onArcChip)
          : layer === "predictions"
            ? renderPredictionsData(calibration)
            : renderArcsData(arcs)}
      </div>
    </section>
  );
}

function renderChronicleData(
  events: HistoryEvent[],
  arcs: Arc[],
  onArcChip: (id: string) => void
) {
  const counts = { major: 0, standard: 0, minor: 0 };
  for (const e of events) counts[e.prominence] += 1;
  // simple approximation for the "calibration in window" block
  const notable = arcs.slice(0, 3);
  return (
    <>
      <div className="shape-data-section">
        <span className="shape-data-label">Events this period</span>
        <div className="event-breakdown">
          <span className="event-count">{events.length}</span>
          <span className="event-breakdown-detail">
            <span className="event-tier major">{counts.major} major</span>
            <span className="event-tier standard">
              {counts.standard} standard
            </span>
            <span className="event-tier minor">{counts.minor} routine</span>
          </span>
        </div>
      </div>
      <div className="shape-data-section">
        <span className="shape-data-label">Calibration in this window</span>
        <div className="calibration-summary-inline">
          <span className="calibration-score">11 of 14</span>
          <span className="calibration-detail">resolved correctly · 0.79</span>
        </div>
      </div>
      <div className="shape-data-section">
        <span className="shape-data-label">Notable arcs</span>
        <div className="arc-chips">
          {notable.map((a) => (
            <button
              key={a.id}
              type="button"
              className="arc-chip"
              data-arc={a.id}
              onClick={() => onArcChip(a.id)}
            >
              {a.name}
            </button>
          ))}
        </div>
      </div>
    </>
  );
}

function renderPredictionsData(c: CalibrationSummary) {
  // `domains` is empty until at least one prediction resolves (correct
  // or wrong). On a fresh demo session there are predictions but no
  // resolutions yet, so we render an empty-state instead of crashing
  // on `sorted[0].name`.
  const sorted = [...c.domains].sort((a, b) => b.score - a.score);
  const strongest = sorted[0];
  const weakest = sorted.length > 1 ? sorted[sorted.length - 1] : undefined;
  const hasResolved = sorted.length > 0;
  return (
    <>
      <div className="shape-data-section">
        <span className="shape-data-label">Overall</span>
        <div className="event-breakdown">
          <span className="event-count">
            {hasResolved ? c.overall.toFixed(2) : "—"}
          </span>
        </div>
      </div>
      <div className="shape-data-section">
        <span className="shape-data-label">Strongest domain</span>
        <div className="calibration-summary-inline">
          <span className="calibration-score">
            {strongest
              ? `${strongest.name} (${strongest.correct} of ${strongest.total})`
              : "no resolved predictions yet"}
          </span>
        </div>
      </div>
      <div className="shape-data-section">
        <span className="shape-data-label">Weakest domain</span>
        <div className="calibration-summary-inline">
          <span className="calibration-score">
            {weakest
              ? `${weakest.name} (${weakest.correct} of ${weakest.total})`
              : "—"}
          </span>
        </div>
      </div>
    </>
  );
}

function renderArcsData(arcs: Arc[]) {
  const open = arcs.filter((a) => a.status === "open");
  const resolved = arcs.filter((a) => a.status === "resolved");
  const avgDuration = avgArcDays(arcs);
  return (
    <>
      <div className="shape-data-section">
        <span className="shape-data-label">Open arcs</span>
        <div className="event-breakdown">
          <span className="event-count">{open.length}</span>
        </div>
      </div>
      <div className="shape-data-section">
        <span className="shape-data-label">Resolved this quarter</span>
        <div className="event-breakdown">
          <span className="event-count">{resolved.length}</span>
        </div>
      </div>
      <div className="shape-data-section">
        <span className="shape-data-label">Avg arc duration</span>
        <div className="calibration-summary-inline">
          <span className="calibration-score">{avgDuration} days</span>
        </div>
      </div>
    </>
  );
}

function avgArcDays(arcs: Arc[]): number {
  const closed = arcs.filter((a) => a.ended);
  if (closed.length === 0) return 0;
  const ms = closed.reduce(
    (sum, a) =>
      sum + (new Date(a.ended!).getTime() - new Date(a.started).getTime()),
    0
  );
  return Math.round(ms / closed.length / (24 * 60 * 60 * 1000));
}
