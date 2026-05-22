// TraceView state (spec §11).
//
// Full-canvas causal or consequence path. Each trace node is a narrow
// path card; the edges between them are verb-labeled. Trace cause runs
// upstream (observation → support → claim → current); trace
// consequence runs downstream.

import type { Trace, TraceEdge, TraceNode } from "../types";

export function TracePath({
  trace,
  depth,
  onDepthChange,
}: {
  trace: Trace;
  depth: number;
  onDepthChange: (d: number) => void;
}) {
  const isVertical = false; // currently always horizontal
  const layout = useLinearLayout(trace, isVertical);

  return (
    <div className="fm-trace" data-testid="trace-canvas">
      <header className="fm-trace__head">
        <div>
          <h2 className="fm-trace__title">
            {trace.direction === "cause" ? "Trace cause" : "Trace consequence"}
          </h2>
          <p className="fm-trace__hint">
            {trace.direction === "cause"
              ? "Upstream chain: observation → supporting claim → current item."
              : "Downstream chain: current item → dependent item → business consequence."}
          </p>
        </div>
        <div className="fm-trace__controls">
          <label className="fm-trace__depth">
            Depth
            <select
              value={depth}
              onChange={(e) => onDepthChange(Number(e.target.value))}
              data-testid="trace-depth"
            >
              {[2, 3, 4, 5, 6].map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </label>
        </div>
      </header>
      <div className="fm-trace__canvas">
        {layout.nodes.length === 0 ? (
          <div className="fm-trace__empty">
            Fyralis does not yet have a {trace.direction} chain rooted in this item.
          </div>
        ) : (
          <ol className="fm-trace__chain">
            {layout.nodes.map((n, i) => {
              const edge = i > 0 ? edgeForStep(trace.edges, layout.nodes[i - 1], n, trace.direction) : null;
              return (
                <li key={n.id} className="fm-trace__step">
                  {edge ? (
                    <div className="fm-trace__edge" aria-hidden="true">
                      <span className="fm-trace__verb">{edge.verb}</span>
                      <span className="fm-trace__arrow">↓</span>
                    </div>
                  ) : null}
                  <TraceCard node={n} />
                </li>
              );
            })}
          </ol>
        )}
      </div>
    </div>
  );
}

function TraceCard({ node }: { node: TraceNode }) {
  return (
    <article className={`fm-trace__card fm-trace__card--${node.kind}`}>
      <header className="fm-trace__card-head">
        <span className="fm-trace__card-kind">{node.kind}</span>
        {node.source ? <span className="fm-trace__card-source">{node.source}</span> : null}
      </header>
      <p className="fm-trace__card-assertion">{node.shortLabel}</p>
    </article>
  );
}

function useLinearLayout(trace: Trace, isVertical: boolean) {
  void isVertical;
  return { nodes: trace.nodes };
}

function edgeForStep(
  edges: TraceEdge[],
  prev: TraceNode,
  next: TraceNode,
  direction: "cause" | "consequence",
): TraceEdge | null {
  // For cause, edges run upstream (toward the root). For consequence,
  // edges run downstream. Either way we look up the edge connecting
  // these two adjacent nodes regardless of direction so we can label
  // the link with a verb.
  void direction;
  return (
    edges.find(
      (e) =>
        (e.source === prev.id && e.target === next.id) ||
        (e.source === next.id && e.target === prev.id),
    ) ?? null
  );
}
