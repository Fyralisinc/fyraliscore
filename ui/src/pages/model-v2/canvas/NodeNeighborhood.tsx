// NodeZoom state (design fix spec §4.4).
//
// The selected claim sits in the center. Incoming neighbors (things
// that affect / threaten / depend on this) arrange on the left
// half-circle. Outgoing neighbors (things this affects / serves /
// blocks) arrange on the right half-circle. This split is what makes
// the neighborhood read causally: cause flows left → right.
//
// Labels sit just outside the arrow midpoint, not on the line, so the
// verb is readable without overlapping the stroke.

import { useMemo } from "react";
import type { ItemDetail, ModelItemSummary, RelationshipInstance } from "../types";
import { FloatingToolbar, StatusChip } from "../components/primitives";

const CANVAS_W = 1200;
const CANVAS_H = 760;
const CENTER_W = 460;
const CENTER_H = 220;
const NEIGHBOR_W = 232;
const NEIGHBOR_H = 130;
const LABEL_W = 132;
const LABEL_H = 28;

export function NodeNeighborhood({
  detail,
  onNeighborClick,
  onTraceCause,
  onTraceConsequence,
  onCreateDecisionDelta,
  onOpenFullDetail,
  onReportCorrection,
}: {
  detail: ItemDetail;
  onNeighborClick: (id: string) => void;
  onTraceCause: () => void;
  onTraceConsequence: () => void;
  onCreateDecisionDelta?: () => void;
  onOpenFullDetail?: () => void;
  onReportCorrection?: () => void;
}) {
  const { item, neighbors } = detail;
  const incoming = neighbors.incoming.slice(0, 3);
  const outgoing = neighbors.outgoing.slice(0, 4);

  const layout = useMemo(
    () => computeLayout(incoming, outgoing),
    [incoming, outgoing],
  );

  const cx = CANVAS_W / 2;
  const cy = CANVAS_H * 0.46;

  return (
    <div className="fm-canvas fm-canvas--zoom" data-testid="nodezoom-canvas">
      <svg
        className="fm-canvas__svg"
        viewBox={`0 0 ${CANVAS_W} ${CANVAS_H}`}
        preserveAspectRatio="xMidYMid meet"
        aria-label={`${item.shortLabel} neighborhood`}
      >
        <g className="fm-edges">
          {layout.map((n) => {
            const startsAtCenter = n.direction === "out";
            const start = startsAtCenter
              ? anchorOnRect(cx, cy, CENTER_W, CENTER_H, n.x, n.y)
              : anchorOnRect(n.x, n.y, NEIGHBOR_W, NEIGHBOR_H, cx, cy);
            const end = startsAtCenter
              ? anchorOnRect(n.x, n.y, NEIGHBOR_W, NEIGHBOR_H, cx, cy)
              : anchorOnRect(cx, cy, CENTER_W, CENTER_H, n.x, n.y);
            // S-curve for organic feel.
            const dx = end.x - start.x;
            const dy = end.y - start.y;
            const bend = 22;
            const px = -dy;
            const py = dx;
            const norm = Math.hypot(px, py) || 1;
            const bx = (px / norm) * bend;
            const by = (py / norm) * bend;
            const c1 = { x: start.x + dx * 0.35 + bx * 0.4, y: start.y + dy * 0.35 + by * 0.4 };
            const c2 = { x: start.x + dx * 0.65 + bx * 0.4, y: start.y + dy * 0.65 + by * 0.4 };
            const mid = {
              x: start.x + dx * 0.5 + bx * 0.5,
              y: start.y + dy * 0.5 + by * 0.5,
            };
            const synth =
              (n.instance as RelationshipInstance & { synthesized?: boolean })
                .synthesized;
            const cls = [
              "fm-edge",
              `fm-edge--${verbColor(n.instance.verb)}`,
              "fm-edge--medium",
              synth ? "fm-edge--synth" : "",
            ]
              .filter(Boolean)
              .join(" ");
            return (
              <g key={n.instance.id} className={cls}>
                <path
                  d={`M ${start.x} ${start.y} C ${c1.x} ${c1.y}, ${c2.x} ${c2.y}, ${end.x} ${end.y}`}
                  fill="none"
                  className="fm-edge__path"
                  markerEnd={`url(#fm-arrow-${verbColor(n.instance.verb)})`}
                />
                <foreignObject
                  x={mid.x - LABEL_W / 2}
                  y={mid.y - LABEL_H - 6}
                  width={LABEL_W}
                  height={LABEL_H + 4}
                >
                  <div className="fm-edge-label-host">
                    <span className={`fm-edgelabel fm-edgelabel--compact fm-edgelabel--${verbColor(n.instance.verb)}${synth ? " fm-edgelabel--synth" : ""}`}>
                      <span className="fm-edgelabel__verb">{n.instance.verb}</span>
                    </span>
                  </div>
                </foreignObject>
              </g>
            );
          })}
        </g>
        <g className="fm-cards">
          <foreignObject
            x={cx - CENTER_W / 2}
            y={cy - CENTER_H / 2}
            width={CENTER_W}
            height={CENTER_H}
          >
            <CentralNodeCard item={item} onOpen={onOpenFullDetail} />
          </foreignObject>
          {layout.map((n) => {
            const side =
              n.direction === "out" ? n.instance.targetItem : n.instance.sourceItem;
            return (
              <foreignObject
                key={`n-${n.instance.id}`}
                x={n.x - NEIGHBOR_W / 2}
                y={n.y - NEIGHBOR_H / 2}
                width={NEIGHBOR_W}
                height={NEIGHBOR_H}
              >
                <NeighborCard
                  item={side}
                  direction={n.direction}
                  onClick={() => onNeighborClick(side.id)}
                />
              </foreignObject>
            );
          })}
        </g>
      </svg>
      <FloatingToolbar
        onTraceCause={onTraceCause}
        onTraceConsequence={onTraceConsequence}
        onCreateDecisionDelta={onCreateDecisionDelta}
        onOpenFullDetail={onOpenFullDetail}
        onReportCorrection={onReportCorrection}
      />
    </div>
  );
}

// Compute neighbor positions. When BOTH sides have neighbors, place
// incoming on the left, outgoing on the right — cause flows L→R.
// When only ONE side has neighbors (the common case on sparse demo
// tenants), fall back to a balanced semi-arc on the populated side
// so the central card has visual weight on both sides of the canvas
// instead of looking shoved off-center.
function computeLayout(
  incoming: RelationshipInstance[],
  outgoing: RelationshipInstance[],
): {
  instance: RelationshipInstance;
  direction: "in" | "out";
  x: number;
  y: number;
}[] {
  const cy = CANVAS_H * 0.46;
  const leftX = CANVAS_W * 0.17;
  const rightX = CANVAS_W * 0.83;
  const colYStep = 170;

  const placeColumn = (
    items: RelationshipInstance[],
    direction: "in" | "out",
    x: number,
  ) => {
    const n = items.length;
    if (n === 0) return [];
    const totalH = (n - 1) * colYStep;
    const startY = cy - totalH / 2;
    return items.map((instance, i) => ({
      instance,
      direction,
      x,
      y: startY + i * colYStep,
    }));
  };

  if (incoming.length > 0 && outgoing.length > 0) {
    return [
      ...placeColumn(incoming, "in", leftX),
      ...placeColumn(outgoing, "out", rightX),
    ];
  }

  // Single-sided layout: arrange the populated side in a semi-arc
  // hugging the central card so the composition still reads
  // intentionally and the central card stays visually centered.
  const direction: "in" | "out" = incoming.length > 0 ? "in" : "out";
  const items = incoming.length > 0 ? incoming : outgoing;
  const n = items.length;
  if (n === 0) return [];
  // Arc spans a 140° sweep centered on the horizontal axis on the
  // populated side. Radius is the canvas inner half.
  const radius = CANVAS_W * 0.34;
  // Left arc faces right (cards on the left, arrows pointing in);
  // right arc faces left (cards on the right, arrows pointing out).
  // The signed cos sign flips the arc to the correct side.
  const cosSign = direction === "in" ? -1 : 1;
  const sweepDeg = 110;
  const startDeg = -sweepDeg / 2;
  return items.map((instance, i) => {
    const t = n === 1 ? 0.5 : i / (n - 1);
    const deg = startDeg + t * sweepDeg;
    const rad = (deg * Math.PI) / 180;
    return {
      instance,
      direction,
      x: CANVAS_W / 2 + cosSign * Math.cos(rad) * radius,
      y: cy + Math.sin(rad) * radius * 0.7,
    };
  });
}

function anchorOnRect(
  rx: number,
  ry: number,
  rw: number,
  rh: number,
  tx: number,
  ty: number,
): { x: number; y: number } {
  const dx = tx - rx;
  const dy = ty - ry;
  if (dx === 0 && dy === 0) return { x: rx, y: ry };
  const halfW = rw / 2;
  const halfH = rh / 2;
  const txMag = Math.abs(dx) < 1e-6 ? Infinity : halfW / Math.abs(dx);
  const tyMag = Math.abs(dy) < 1e-6 ? Infinity : halfH / Math.abs(dy);
  const t = Math.min(txMag, tyMag);
  return { x: rx + dx * t, y: ry + dy * t };
}

// Map a verb to a color family so edges + labels carry semantic
// color without the caller threading colorToken explicitly.
function verbColor(verb: string): string {
  if (verb === "blocks" || verb === "exposes" || verb === "falsifies") return "garnet";
  if (verb === "constrains" || verb === "limits") return "blue";
  if (verb === "owns" || verb === "contributes to") return "ochre";
  if (verb === "funds") return "gold";
  if (verb === "contradicts") return "iris";
  if (verb === "evidences" || verb === "supports") return "lapis";
  return "moss";
}

function CentralNodeCard({
  item,
  onOpen,
}: {
  item: ItemDetail["item"];
  onOpen?: () => void;
}) {
  const relationshipLine = composeRelationshipLine(item);
  return (
    <article className="fm-node fm-node--central" data-testid="node-central">
      <header className="fm-node__head">
        <span className={`fm-node__category fm-node__category--${item.categoryId}`}>
          {humanCategory(item.categoryId)}
        </span>
        <StatusChip status={item.status} />
      </header>
      <h2 className="fm-node__assertion">{item.assertion}</h2>
      <div className="fm-node__meta">
        {item.owner ? <span>Owner: {item.owner}</span> : null}
        {typeof item.confidence === "number" ? (
          <span>Confidence {Math.round((item.confidence ?? 0) * 100)}%</span>
        ) : null}
        {item.lifecycle?.updatedAt ? (
          <span>Updated {humanAgo(item.lifecycle.updatedAt)}</span>
        ) : null}
      </div>
      {relationshipLine ? (
        <div className="fm-node__rels" data-testid="node-rel-counts">
          {relationshipLine}
        </div>
      ) : null}
      {item.metrics?.arrExposure ? (
        <div className="fm-node__metric">
          ${(item.metrics.arrExposure / 1_000_000).toFixed(2)}M ARR exposure
        </div>
      ) : null}
    </article>
  );
}

function composeRelationshipLine(item: ItemDetail["item"]): string | null {
  const counts = item.relationshipCounts ?? {};
  if (Object.keys(counts).length === 0) return null;
  const phrases: string[] = [];
  const blockedBy = counts["in_blocks"];
  if (blockedBy && blockedBy > 0) phrases.push(`Blocked by ${blockedBy}`);
  const affectsCustomers = counts["affects_customers"];
  if (affectsCustomers && affectsCustomers > 0) {
    phrases.push(
      `Affects ${affectsCustomers} customer${affectsCustomers === 1 ? "" : "s"}`,
    );
  }
  const servesGoals = counts["serves_goals"];
  if (servesGoals && servesGoals > 0) {
    phrases.push(`Serves ${servesGoals} goal${servesGoals === 1 ? "" : "s"}`);
  }
  const exposedBy = counts["in_exposes"];
  if (exposedBy && exposedBy > 0) phrases.push(`Exposed by ${exposedBy}`);
  const constrainedBy = counts["in_constrains"];
  if (constrainedBy && constrainedBy > 0) phrases.push(`Constrained by ${constrainedBy}`);
  const relDec = (counts["in_decisions"] ?? 0) + (counts["affects_decisions"] ?? 0);
  if (relDec > 0) phrases.push(`Related decision ${relDec}`);
  if (phrases.length === 0) return null;
  return phrases.slice(0, 3).join(" · ");
}

function NeighborCard({
  item,
  direction,
  onClick,
}: {
  item: ModelItemSummary;
  direction: "in" | "out";
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      className={`fm-node fm-node--neighbor fm-node--${direction}`}
      onClick={onClick}
      aria-label={`Neighbor: ${item.assertion}`}
    >
      <header className="fm-node__head fm-node__head--compact">
        <span className={`fm-node__category fm-node__category--${item.categoryId}`}>
          {humanCategory(item.categoryId)}
        </span>
        <StatusChip status={item.status} />
      </header>
      <p className="fm-node__assertion fm-node__assertion--compact">
        {item.shortLabel}
      </p>
      {item.impactMetric ? (
        <span className="fm-node__metric fm-node__metric--compact">
          {item.impactMetric}
        </span>
      ) : null}
    </button>
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

function humanAgo(iso: string): string {
  try {
    const ts = new Date(iso).getTime();
    const ms = Date.now() - ts;
    const s = Math.max(1, Math.floor(ms / 1000));
    if (s < 60) return `${s}s ago`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    const d = Math.floor(h / 24);
    return `${d}d ago`;
  } catch {
    return "recently";
  }
}
