// Default overview state (spec §6).
//
// Renders the 8 category modules on a stable semantic lattice and
// draws the top relationship bundles as labeled cubic-bezier paths
// between them. Categories are positioned via the layoutHints from the
// API (normalized 0..1 coordinates) and rescaled to the canvas size.
//
// Edges use side-anchored routing (see edgeGeometry.ts): each endpoint
// snaps to one of 4 cardinal sides on the card and the curve leaves
// perpendicular to that side. This gives a clean flowchart feel and
// avoids the "edges curling into each other" issue when multiple
// bundles terminate at the same card.

import { useMemo, useState } from "react";
import type {
  CategoryId,
  ModelCategory,
  RelationshipBundle,
} from "../types";
import { CategoryModule, RelationshipLabel } from "../components/primitives";
import { assignLateralOffsets, buildEdge } from "./edgeGeometry";

export type OverviewMapProps = {
  categories: ModelCategory[];
  bundles: RelationshipBundle[];
  onCategoryClick: (id: CategoryId) => void;
  onBundleClick: (bundleId: string) => void;
};

const CANVAS_W = 1200;
const CANVAS_H = 760;
const CARD_W = 244;
const CARD_H = 104;
// Visible card corner radius (matches CSS .fm-cat border-radius). The
// edge anchor sits at the side midpoint so the arrow tip lands at the
// card boundary, not at a rounded-corner area.
const LABEL_W = 168;
const LABEL_H = 46;

export function OverviewMap({
  categories,
  bundles,
  onCategoryClick,
  onBundleClick,
}: OverviewMapProps) {
  const positions = useMemo(() => {
    const out = new Map<CategoryId, { x: number; y: number }>();
    for (const c of categories) {
      out.set(c.id, {
        x: c.position.x * CANVAS_W,
        y: c.position.y * CANVAS_H,
      });
    }
    return out;
  }, [categories]);

  const [hoveredBundle, setHoveredBundle] = useState<string | null>(null);

  const edges = useMemo(() => {
    const offsets = assignLateralOffsets(
      bundles,
      positions as Map<string, { x: number; y: number }>,
      () => ({ w: CARD_W, h: CARD_H }),
    );
    return bundles
      .map((b) => {
        const src = positions.get(b.sourceCategoryId);
        const tgt = positions.get(b.targetCategoryId);
        if (!src || !tgt) return null;
        const lat = offsets.get(b.id) ?? { src: 0, tgt: 0 };
        const geom = buildEdge(
          src, CARD_W, CARD_H,
          tgt, CARD_W, CARD_H,
          { srcLateral: lat.src, tgtLateral: lat.tgt },
        );
        return { bundle: b, geom };
      })
      .filter((e): e is NonNullable<typeof e> => e !== null);
  }, [bundles, positions]);

  return (
    <div className="fm-canvas" data-testid="overview-canvas">
      <svg
        className="fm-canvas__svg"
        viewBox={`0 0 ${CANVAS_W} ${CANVAS_H}`}
        preserveAspectRatio="xMidYMid meet"
        aria-label="Company model map"
      >
        <g className="fm-edges">
          {edges.map((e) => {
            const dimmed = hoveredBundle !== null && hoveredBundle !== e.bundle.id;
            const cls = [
              "fm-edge",
              `fm-edge--${e.bundle.visual.colorToken}`,
              `fm-edge--${e.bundle.visual.strength}`,
              e.bundle.synthesized ? "fm-edge--synth" : "",
              dimmed ? "is-dimmed" : "",
              hoveredBundle === e.bundle.id ? "is-hovered" : "",
            ]
              .filter(Boolean)
              .join(" ");
            const { src, tgt, c1, c2 } = e.geom;
            return (
              <g
                key={e.bundle.id}
                className={cls}
                onMouseEnter={() => setHoveredBundle(e.bundle.id)}
                onMouseLeave={() => setHoveredBundle(null)}
                onClick={() => onBundleClick(e.bundle.id)}
                role="button"
                tabIndex={0}
                onKeyDown={(ev) => {
                  if (ev.key === "Enter") onBundleClick(e.bundle.id);
                }}
                aria-label={`${e.bundle.verb} relationship`}
              >
                {/* Invisible hit area so the edge stays clickable even
                    where the painted stroke is thin. */}
                <path
                  d={`M ${src.x} ${src.y} C ${c1.x} ${c1.y}, ${c2.x} ${c2.y}, ${tgt.x} ${tgt.y}`}
                  fill="none"
                  className="fm-edge__hit"
                />
                <path
                  d={`M ${src.x} ${src.y} C ${c1.x} ${c1.y}, ${c2.x} ${c2.y}, ${tgt.x} ${tgt.y}`}
                  fill="none"
                  className="fm-edge__path"
                  markerEnd={`url(#fm-arrow-${e.bundle.visual.colorToken})`}
                />
              </g>
            );
          })}
        </g>
        <g className="fm-edge-labels">
          {edges.map((e) => {
            // Place the label centered on the curve's analytic
            // midpoint. The label has a paper background so the line
            // visually breaks behind it — no awkward offset needed.
            const x = e.geom.mid.x - LABEL_W / 2;
            const y = e.geom.mid.y - LABEL_H / 2;
            return (
              <foreignObject
                key={`l-${e.bundle.id}`}
                x={x}
                y={y}
                width={LABEL_W}
                height={LABEL_H}
                onMouseEnter={() => setHoveredBundle(e.bundle.id)}
                onMouseLeave={() => setHoveredBundle(null)}
              >
                <div className="fm-edge-label-host">
                  <RelationshipLabel
                    bundle={e.bundle}
                    onClick={() => onBundleClick(e.bundle.id)}
                  />
                </div>
              </foreignObject>
            );
          })}
        </g>
        <g className="fm-cards">
          {categories.map((c) => {
            const p = positions.get(c.id);
            if (!p) return null;
            return (
              <foreignObject
                key={c.id}
                x={p.x - CARD_W / 2}
                y={p.y - CARD_H / 2}
                width={CARD_W}
                height={CARD_H}
              >
                <CategoryModule
                  category={c}
                  onClick={() => onCategoryClick(c.id)}
                />
              </foreignObject>
            );
          })}
        </g>
      </svg>
      {hoveredBundle
        ? (() => {
            const e = edges.find((x) => x.bundle.id === hoveredBundle);
            if (!e) return null;
            const left = `${(e.geom.mid.x / CANVAS_W) * 100}%`;
            const top = `${(e.geom.mid.y / CANVAS_H) * 100}%`;
            return (
              <div
                className="fm-edge-preview"
                style={{ left, top }}
                role="tooltip"
                data-testid={`bundle-preview-${e.bundle.id}`}
              >
                <div className="fm-edge-preview__title">
                  {e.bundle.sourceCategoryId} → {e.bundle.verb} → {e.bundle.targetCategoryId}
                </div>
                <div className="fm-edge-preview__count">
                  {e.bundle.instanceCount} relationship
                  {e.bundle.instanceCount === 1 ? "" : "s"}
                  {e.bundle.synthesized ? " · inferred" : ""}
                </div>
                {e.bundle.topExample ? (
                  <div className="fm-edge-preview__example">
                    {e.bundle.topExample.sourceShortLabel} →{" "}
                    {e.bundle.topExample.targetShortLabel}
                  </div>
                ) : null}
                {e.bundle.impactLabel ? (
                  <div className="fm-edge-preview__impact">
                    {e.bundle.impactLabel}
                  </div>
                ) : null}
                <div className="fm-edge-preview__hint">Click to inspect</div>
              </div>
            );
          })()
        : null}
    </div>
  );
}
