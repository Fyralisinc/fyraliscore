import type { Commitment, ShapeStatementToken, ActiveRefFilter } from "./types";

// Spec Part 4 — two-zone narrative band: shape statement (62%) + data (38%).
type Props = {
  statement: ShapeStatementToken[];
  commitments: Commitment[]; // for status segments / pressure
  recentChange: { direction: "up" | "down" | "mixed" | "flat"; text: string };
  onRef: (ref: ActiveRefFilter) => void;
  activeRef: ActiveRefFilter;
};

export function NarrativeBand({
  statement,
  commitments,
  recentChange,
  onRef,
  activeRef,
}: Props) {
  const total = commitments.length || 1;
  const counts = {
    "on-track": 0,
    slipping: 0,
    "at-risk": 0,
    blocked: 0,
  } as Record<Commitment["status"], number>;
  for (const c of commitments) counts[c.status] += 1;

  const segments = [
    { key: "on-track", count: counts["on-track"], label: "on track" },
    { key: "slipping", count: counts["slipping"], label: "slipping" },
    { key: "at-risk", count: counts["at-risk"], label: "at risk" },
    { key: "blocked", count: counts["blocked"], label: "blocked" },
  ] as const;

  // Forward pressure — next 7 days.
  const now = Date.now();
  const sevenDays = 7 * 24 * 60 * 60 * 1000;
  const upcoming = commitments.filter((c) => {
    const t = new Date(c.due_date).getTime();
    return t >= now && t - now <= sevenDays;
  });
  const upcomingCustomerFacing = upcoming.filter(
    (c) => c.territory === "customer-facing"
  ).length;

  const directionGlyph: Record<typeof recentChange.direction, string> = {
    up: "↑",
    down: "↓",
    mixed: "∼",
    flat: "—",
  };

  function renderToken(tok: ShapeStatementToken, i: number) {
    if (tok.kind === "text") return <span key={i}>{tok.text}</span>;
    const r = tok.ref;
    const isActive =
      activeRef !== null &&
      ((r.type === "territory" &&
        activeRef.kind === "territory" &&
        activeRef.id === r.id) ||
        (r.type === "person" &&
          activeRef.kind === "person" &&
          activeRef.id === r.id) ||
        (r.type === "commitment" &&
          activeRef.kind === "commitment" &&
          activeRef.id === r.id) ||
        (r.type === "customer" &&
          activeRef.kind === "customer" &&
          activeRef.id === r.id));
    return (
      <button
        key={i}
        type="button"
        className={"ref" + (isActive ? " ref-active" : "")}
        data-ref-type={r.type}
        onClick={() => {
          if (r.type === "decision") return;
          if (isActive) {
            onRef(null);
            return;
          }
          if (r.type === "territory") onRef({ kind: "territory", id: r.id });
          else if (r.type === "person") onRef({ kind: "person", id: r.id });
          else if (r.type === "commitment")
            onRef({ kind: "commitment", id: r.id });
          else if (r.type === "customer")
            onRef({ kind: "customer", id: r.id });
        }}
      >
        {r.text}
      </button>
    );
  }

  return (
    <section className="narrative-band" aria-label="Current state summary">
      <div className="shape-statement">
        <p className="shape-statement-text">
          {statement.map(renderToken)}
        </p>
      </div>
      <div className="shape-data">
        <div className="shape-data-section">
          <span className="shape-data-label">Status</span>
          <div className="status-bar" role="img" aria-label="Status mix">
            {segments
              .filter((s) => s.count > 0)
              .map((s) => (
                <div
                  key={s.key}
                  className="status-segment"
                  data-status={s.key}
                  style={{ width: `${(s.count / total) * 100}%` }}
                  title={`${s.count} ${s.label}`}
                >
                  <span className="seg-count">{s.count}</span>
                  <span className="seg-label">{s.label}</span>
                </div>
              ))}
          </div>
        </div>
        <div className="shape-data-section">
          <span className="shape-data-label">Next 7 days</span>
          <div className="forward-pressure">
            <span className="pressure-count">{upcoming.length}</span>
            <span className="pressure-detail">
              due · {upcomingCustomerFacing} customer-facing
            </span>
          </div>
        </div>
        <div className="shape-data-section">
          <span className="shape-data-label">This week</span>
          <div className="recent-change">
            <span className={"change-direction " + recentChange.direction}>
              {directionGlyph[recentChange.direction]}
            </span>
            <span className="change-text">{recentChange.text}</span>
          </div>
        </div>
      </div>
    </section>
  );
}
