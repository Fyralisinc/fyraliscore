// LayeredGraph — the spine of the Model page. Renders the five fixed
// horizontal bands (goal / commitment / decision / risk / customer)
// as SVG bands with HTML node tiles laid into them via foreignObject.
//
// Why SVG instead of cytoscape for Wave 2:
//   * The spec mandates ≤25 nodes in five exact rows. There's no
//     force-directed signal to render — positions are pure layout.
//   * Edges need three precise visual styles (solid moss / dashed
//     stone / solid garnet) plus a "traced" highlighted state.
//     Configuring cytoscape's stylesheet per-edge for that is
//     significantly more code than a small SVG path map.
//   * Selection, dimming, and trace animation are all CSS-driven via
//     class names, so they integrate cleanly with the rest of the
//     primitives layer.
//
// The packages cytoscape, cytoscape-cose-bilkent, react-cytoscapejs
// remain available for the freeform graph view that ships in Wave 3.

import { useMemo, useRef } from "react";
import type { MapBand, MapEdge, MapNode } from "@/api/map-types";
import type { NodeMetaV2 } from "@/api/map-mock-v2";
import { BAND_LABELS, BAND_ORDER } from "./types";
import { NodeTile } from "./NodeTile";

export interface LayeredGraphProps {
  nodes: MapNode[];
  edges: MapEdge[];
  meta: Record<string, NodeMetaV2>;
  selectedId: string | null;
  onSelect: (id: string | null) => void;
  highlightedEdgeKeys: Set<string>;
  zoom: number;
  showGrid: boolean;
  // Server-reported total node count per band BEFORE capping. Used to
  // size the "+N more" overflow card. Falls back to the visible count
  // (no overflow) when the server doesn't send it.
  bandTotals?: Partial<Record<MapBand, number>>;
  // Click handler for the +N more overflow card. Lets the parent
  // refetch the snapshot with ?lens=<band> for a band-focused view.
  onExpandBand?: (band: MapBand) => void;
}

interface Positioned {
  node: MapNode;
  x: number;
  y: number;
  width: number;
  height: number;
}

const CANVAS_WIDTH = 1100;
const CANVAS_HEIGHT = 720;
const BAND_HEIGHT = CANVAS_HEIGHT / BAND_ORDER.length;
const TILE_W_WIDE = 320;
const TILE_W = 220;
const TILE_W_NARROW = 170;
const TILE_H = 78;
const SIDE_PADDING = 40;

function pickTileWidth(count: number, band: MapBand): number {
  if (band === "goal") return TILE_W_WIDE;
  if (band === "customer") return TILE_W_NARROW;
  if (count <= 2) return TILE_W_WIDE - 40;
  if (count <= 3) return TILE_W;
  return TILE_W_NARROW;
}

export function buildPositions(
  nodes: MapNode[],
  bandTotals?: Partial<Record<MapBand, number>>
): Positioned[] {
  const byBand: Record<MapBand, MapNode[]> = {
    goal: [],
    commitment: [],
    decision: [],
    risk: [],
    customer: [],
  };
  for (const n of nodes) {
    const b = n.band;
    if (!b) continue;
    byBand[b].push(n);
  }
  const out: Positioned[] = [];
  BAND_ORDER.forEach((band, rowIdx) => {
    const arr = byBand[band];
    // Overflow = (server-reported total for the band) minus what's
    // actually visible. Falls back to 0 when the server doesn't
    // report band_totals (older snapshot endpoints).
    const total = bandTotals?.[band] ?? arr.length;
    const overflow = Math.max(0, total - arr.length);
    const cellCount = arr.length + (overflow > 0 ? 1 : 0);
    if (cellCount === 0) return;
    const tileW = pickTileWidth(cellCount, band);
    const totalW = cellCount * tileW + (cellCount - 1) * 24;
    const startX = (CANVAS_WIDTH - totalW) / 2;
    const y = rowIdx * BAND_HEIGHT + BAND_HEIGHT / 2 - TILE_H / 2;
    arr.forEach((node, i) => {
      out.push({
        node,
        x: startX + i * (tileW + 24),
        y,
        width: tileW,
        height: TILE_H,
      });
    });
    if (overflow > 0) {
      // Synthesize a placeholder node for the +N overflow cluster.
      const phantom: MapNode = {
        id: `__overflow_${band}`,
        natural: `+${overflow} more`,
        proposition_kind: band,
        neighborhood_id: null,
        confidence: 0,
        activation: 0.4,
        status: "active",
        archive_reason: null,
        health: "stable",
        in_degree: 0,
        out_degree: 0,
        topo_x: null,
        topo_y: null,
        created_at: new Date().toISOString(),
        band,
      };
      out.push({
        node: phantom,
        x: startX + arr.length * (tileW + 24),
        y,
        width: tileW,
        height: TILE_H,
      });
    }
  });
  return out;
}

function edgeStyle(
  edge: MapEdge,
  srcBand: MapBand | undefined,
  tgtBand: MapBand | undefined,
  meta?: NodeMetaV2
): { stroke: string; dash: string | null; kind: "supports" | "depends" | "blocks" } {
  // Risk → customer is the blocking edge (Deep Garnet). Risk → anything
  // else stays in the depends-on / contributes register.
  if (edge.kind === "supports") {
    return { stroke: "var(--color-moss-cipher)", dash: null, kind: "supports" };
  }
  if (srcBand === "risk" && tgtBand === "customer") {
    return { stroke: "var(--color-deep-garnet)", dash: null, kind: "blocks" };
  }
  if (meta?.critical && srcBand === "risk") {
    return { stroke: "var(--color-deep-garnet)", dash: null, kind: "blocks" };
  }
  return {
    stroke: "var(--color-weathered-sage)",
    dash: "4 3",
    kind: "depends",
  };
}

export function LayeredGraph({
  nodes,
  edges,
  meta,
  selectedId,
  onSelect,
  highlightedEdgeKeys,
  zoom,
  showGrid,
  bandTotals,
  onExpandBand,
}: LayeredGraphProps) {
  const positioned = useMemo(
    () => buildPositions(nodes, bandTotals),
    [nodes, bandTotals]
  );
  const posIndex = useMemo(() => {
    const m = new Map<string, Positioned>();
    positioned.forEach((p) => m.set(p.node.id, p));
    return m;
  }, [positioned]);

  // Neighbors map for selection dimming. Selecting a node fades nodes
  // not connected to it to ~25% opacity (spec §4.3).
  const neighbors = useMemo(() => {
    if (!selectedId) return new Set<string>();
    const set = new Set<string>([selectedId]);
    for (const e of edges) {
      if (e.source === selectedId) set.add(e.target);
      if (e.target === selectedId) set.add(e.source);
    }
    return set;
  }, [selectedId, edges]);

  const svgRef = useRef<SVGSVGElement>(null);

  const visibleEdges = useMemo(
    () =>
      edges.filter(
        (e) => posIndex.has(e.source) && posIndex.has(e.target)
      ),
    [edges, posIndex]
  );

  return (
    <div
      className="fy-model-graph"
      data-testid="layered-graph"
      onClick={(e) => {
        // Click on the canvas background (not a tile) clears selection.
        if ((e.target as HTMLElement).closest(".fy-node-tile")) return;
        if (selectedId) onSelect(null);
      }}
    >
      <svg
        ref={svgRef}
        className="fy-model-graph__svg"
        viewBox={`0 0 ${CANVAS_WIDTH} ${CANVAS_HEIGHT}`}
        preserveAspectRatio="xMidYMid meet"
        style={{ transform: `scale(${zoom})`, transformOrigin: "center top" }}
        data-testid="graph-svg"
      >
        <defs>
          <marker
            id="arrow-moss"
            viewBox="0 0 10 10"
            refX="8"
            refY="5"
            markerWidth="5"
            markerHeight="5"
            orient="auto-start-reverse"
          >
            <path d="M0 0 L10 5 L0 10 z" fill="var(--color-moss-cipher)" />
          </marker>
          <marker
            id="arrow-stone"
            viewBox="0 0 10 10"
            refX="8"
            refY="5"
            markerWidth="5"
            markerHeight="5"
            orient="auto-start-reverse"
          >
            <path d="M0 0 L10 5 L0 10 z" fill="var(--color-weathered-sage)" />
          </marker>
          <marker
            id="arrow-garnet"
            viewBox="0 0 10 10"
            refX="8"
            refY="5"
            markerWidth="5"
            markerHeight="5"
            orient="auto-start-reverse"
          >
            <path d="M0 0 L10 5 L0 10 z" fill="var(--color-deep-garnet)" />
          </marker>
        </defs>

        {showGrid ? <BandGrid /> : null}
        <BandLabels />

        <g className="fy-model-graph__edges">
          {visibleEdges.map((e, idx) => {
            const sp = posIndex.get(e.source)!;
            const tp = posIndex.get(e.target)!;
            const srcBand = sp.node.band;
            const tgtBand = tp.node.band;
            const style = edgeStyle(e, srcBand, tgtBand, meta[e.source]);
            const key = `${e.source}__${e.target}__${e.kind}`;
            const isHighlight = highlightedEdgeKeys.has(key);
            // Anchors: bottom-center of source → top-center of target
            // when going downward (source band higher up); otherwise
            // top→bottom. Source ABOVE target = bottom-out, top-in.
            const srcAbove = sp.y < tp.y;
            const x1 = sp.x + sp.width / 2;
            const y1 = srcAbove ? sp.y + sp.height : sp.y;
            const x2 = tp.x + tp.width / 2;
            const y2 = srcAbove ? tp.y : tp.y + tp.height;
            // Smooth s-curve between bands for legibility.
            const cy1 = y1 + (y2 - y1) * 0.5;
            const cy2 = y1 + (y2 - y1) * 0.5;
            const path = `M ${x1} ${y1} C ${x1} ${cy1}, ${x2} ${cy2}, ${x2} ${y2}`;
            const markerId =
              style.kind === "supports"
                ? "arrow-moss"
                : style.kind === "blocks"
                  ? "arrow-garnet"
                  : "arrow-stone";
            return (
              <path
                key={`${key}-${idx}`}
                d={path}
                fill="none"
                stroke={style.stroke}
                strokeWidth={isHighlight ? 2.4 : 1.4}
                strokeDasharray={style.dash ?? undefined}
                strokeOpacity={
                  selectedId &&
                  !(neighbors.has(e.source) && neighbors.has(e.target))
                    ? 0.18
                    : isHighlight
                      ? 1
                      : 0.85
                }
                markerEnd={`url(#${markerId})`}
                className={`fy-model-edge fy-model-edge--${style.kind}${
                  isHighlight ? " is-highlighted" : ""
                }`}
                data-edge-key={key}
                data-edge-kind={style.kind}
                data-testid={isHighlight ? "edge-highlighted" : undefined}
              />
            );
          })}
        </g>

        <g className="fy-model-graph__nodes">
          {positioned.map(({ node, x, y, width, height }) => (
            <foreignObject
              key={node.id}
              x={x}
              y={y}
              width={width}
              height={height}
            >
              {/* Wrap so flexbox can centre the tile inside the box */}
              <div
                className="fy-node-tile__wrap"
                style={{ width: "100%", height: "100%" }}
              >
                <NodeTile
                  node={node}
                  meta={meta[node.id]}
                  selected={selectedId === node.id}
                  dimmed={!!selectedId && !neighbors.has(node.id)}
                  onClick={(id) => {
                    if (id.startsWith("__overflow_")) {
                      const band = id.slice("__overflow_".length) as MapBand;
                      onExpandBand?.(band);
                      return;
                    }
                    onSelect(id === selectedId ? null : id);
                  }}
                />
              </div>
            </foreignObject>
          ))}
        </g>
      </svg>
    </div>
  );
}

function BandLabels() {
  return (
    <g className="fy-model-graph__bands">
      {BAND_ORDER.map((band, i) => (
        <text
          key={band}
          x={20}
          y={i * BAND_HEIGHT + 22}
          className="fy-model-graph__band-label"
          fill="var(--color-weathered-sage)"
          fontSize="11"
          fontFamily="ui-sans-serif, system-ui, -apple-system"
          letterSpacing="0.08em"
        >
          {BAND_LABELS[band]}
        </text>
      ))}
    </g>
  );
}

function BandGrid() {
  return (
    <g className="fy-model-graph__grid" aria-hidden="true">
      {BAND_ORDER.map((_, i) => (
        <line
          key={i}
          x1="0"
          x2={CANVAS_WIDTH}
          y1={i * BAND_HEIGHT}
          y2={i * BAND_HEIGHT}
          stroke="var(--color-stone-veil)"
          strokeDasharray="3 4"
          strokeOpacity="0.6"
        />
      ))}
    </g>
  );
}

export default LayeredGraph;
