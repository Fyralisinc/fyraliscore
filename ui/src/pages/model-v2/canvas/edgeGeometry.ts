// Shared geometry for relationship-edge routing on the Model canvas.
//
// The previous routing anchored edges at the intersection of a ray from
// the card center with the card's bounding rectangle, then bent the
// curve along a fixed perpendicular. That produced two visible bugs in
// the overview:
//   - Edges entered cards at oblique angles; arrow heads landed on the
//     rounded-corner area outside the visible card edge.
//   - Curves all twisted the same direction, so multiple edges
//     terminating at the same card (Commitments, Customers) overlapped
//     and the labels stacked on top of each other.
//
// This module replaces that with side-anchored cubic beziers: each
// endpoint snaps to the midpoint of the nearest card side, and control
// handles extend perpendicular out of that side. The result reads like
// a clean flowchart — every edge enters and leaves a card along a
// cardinal direction, and the curvature is determined by the offset
// between the two card sides rather than an arbitrary perpendicular
// bend.
//
// Co-terminating edges (multiple bundles entering the same card side)
// are spread laterally along that side via the optional `lateral`
// offset returned by `assignLateralOffsets`.
//
// Label placement uses the analytic midpoint of the cubic bezier
// (t=0.5) so the label always sits on the curve itself rather than at
// a synthetic offset above the chord.

export type Pt = { x: number; y: number };
export type Side = "top" | "right" | "bottom" | "left";

export type Anchor = {
  point: Pt;
  side: Side;
  normal: Pt;
};

export type EdgeGeom = {
  src: Pt;
  tgt: Pt;
  c1: Pt;
  c2: Pt;
  mid: Pt;
  tangent: Pt; // unit tangent at t=0.5, points src→tgt direction
  normal: Pt;  // unit normal perpendicular to tangent (left-hand)
  srcSide: Side;
  tgtSide: Side;
};

// Pick the side of a card (centered at `from`, size w×h) that faces
// `to`. Uses the corner angle to decide whether the connection should
// exit through the vertical or horizontal sides.
export function pickSide(
  from: Pt, to: Pt, w: number, h: number,
): Side {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  // Angle of the diagonal from card center to corner. Edges within
  // this angle cone exit through left/right; outside it, top/bottom.
  const cornerAngle = Math.atan2(h, w);
  const angle = Math.atan2(Math.abs(dy), Math.abs(dx));
  if (angle < cornerAngle) {
    return dx >= 0 ? "right" : "left";
  }
  return dy >= 0 ? "bottom" : "top";
}

// Compute the socket point + outward normal for a given card side.
// `lateral` shifts the socket along the side (e.g. for parallel edges
// terminating at the same card side).
export function sideSocket(
  center: Pt, w: number, h: number, side: Side, lateral = 0,
): Anchor {
  const halfW = w / 2;
  const halfH = h / 2;
  switch (side) {
    case "top":
      return {
        point: { x: center.x + lateral, y: center.y - halfH },
        side,
        normal: { x: 0, y: -1 },
      };
    case "bottom":
      return {
        point: { x: center.x + lateral, y: center.y + halfH },
        side,
        normal: { x: 0, y: 1 },
      };
    case "left":
      return {
        point: { x: center.x - halfW, y: center.y + lateral },
        side,
        normal: { x: -1, y: 0 },
      };
    case "right":
      return {
        point: { x: center.x + halfW, y: center.y + lateral },
        side,
        normal: { x: 1, y: 0 },
      };
  }
}

// Build a full cubic-bezier geometry for an edge between two cards.
// Control handles extend perpendicular outward from each chosen side
// so the curve leaves and enters the cards cleanly.
export function buildEdge(
  srcCenter: Pt, srcW: number, srcH: number,
  tgtCenter: Pt, tgtW: number, tgtH: number,
  opts?: { srcLateral?: number; tgtLateral?: number },
): EdgeGeom {
  const srcSide = pickSide(srcCenter, tgtCenter, srcW, srcH);
  const tgtSide = pickSide(tgtCenter, srcCenter, tgtW, tgtH);
  const srcAnchor = sideSocket(srcCenter, srcW, srcH, srcSide, opts?.srcLateral ?? 0);
  const tgtAnchor = sideSocket(tgtCenter, tgtW, tgtH, tgtSide, opts?.tgtLateral ?? 0);

  const dx = tgtAnchor.point.x - srcAnchor.point.x;
  const dy = tgtAnchor.point.y - srcAnchor.point.y;
  const dist = Math.hypot(dx, dy);

  // Handle length controls curve smoothness. It must not exceed
  // roughly half the projected gap in either normal direction, or
  // the cubic bezier will loop back on itself (visible when two cards
  // are stacked closely, e.g. Goals immediately above Commitments).
  const projSrc = Math.abs(srcAnchor.normal.x * dx + srcAnchor.normal.y * dy);
  const projTgt = Math.abs(tgtAnchor.normal.x * dx + tgtAnchor.normal.y * dy);
  const projMin = Math.min(projSrc, projTgt);
  const handle = Math.max(
    12,
    Math.min(140, dist * 0.45, projMin * 0.5),
  );

  const c1: Pt = {
    x: srcAnchor.point.x + srcAnchor.normal.x * handle,
    y: srcAnchor.point.y + srcAnchor.normal.y * handle,
  };
  const c2: Pt = {
    x: tgtAnchor.point.x + tgtAnchor.normal.x * handle,
    y: tgtAnchor.point.y + tgtAnchor.normal.y * handle,
  };

  const mid = bezierPoint(srcAnchor.point, c1, c2, tgtAnchor.point, 0.5);
  const tan = bezierTangent(srcAnchor.point, c1, c2, tgtAnchor.point, 0.5);
  const tlen = Math.hypot(tan.x, tan.y) || 1;
  const tangent = { x: tan.x / tlen, y: tan.y / tlen };
  const normal = { x: -tangent.y, y: tangent.x };

  return {
    src: srcAnchor.point,
    tgt: tgtAnchor.point,
    c1, c2, mid, tangent, normal,
    srcSide: srcAnchor.side,
    tgtSide: tgtAnchor.side,
  };
}

// Given a set of bundles, return per-bundle lateral offsets so edges
// that co-terminate on the same card side fan out instead of stacking.
// Keyed by `${categoryId}::${side}`; offsets are centered around zero.
export function assignLateralOffsets<B extends {
  id: string;
  sourceCategoryId: string;
  targetCategoryId: string;
}>(
  bundles: B[],
  positions: Map<string, Pt>,
  cardSize: (id: string) => { w: number; h: number },
): Map<string, { src: number; tgt: number }> {
  // First pass: determine each endpoint's side.
  type Group = { categoryId: string; side: Side; cardLen: number };
  const sides: Array<{ b: B; srcG: Group; tgtG: Group }> = [];
  const buckets = new Map<string, Array<{ b: B; end: "src" | "tgt" }>>();

  for (const b of bundles) {
    const src = positions.get(b.sourceCategoryId);
    const tgt = positions.get(b.targetCategoryId);
    if (!src || !tgt) continue;
    const ss = cardSize(b.sourceCategoryId);
    const ts = cardSize(b.targetCategoryId);
    const srcSide = pickSide(src, tgt, ss.w, ss.h);
    const tgtSide = pickSide(tgt, src, ts.w, ts.h);
    const srcG: Group = {
      categoryId: b.sourceCategoryId,
      side: srcSide,
      cardLen: srcSide === "top" || srcSide === "bottom" ? ss.w : ss.h,
    };
    const tgtG: Group = {
      categoryId: b.targetCategoryId,
      side: tgtSide,
      cardLen: tgtSide === "top" || tgtSide === "bottom" ? ts.w : ts.h,
    };
    sides.push({ b, srcG, tgtG });
    const srcKey = `${srcG.categoryId}::${srcG.side}`;
    const tgtKey = `${tgtG.categoryId}::${tgtG.side}`;
    if (!buckets.has(srcKey)) buckets.set(srcKey, []);
    if (!buckets.has(tgtKey)) buckets.set(tgtKey, []);
    buckets.get(srcKey)!.push({ b, end: "src" });
    buckets.get(tgtKey)!.push({ b, end: "tgt" });
  }

  // Second pass: assign offsets within each bucket.
  const result = new Map<string, { src: number; tgt: number }>();
  for (const e of sides) result.set(e.b.id, { src: 0, tgt: 0 });
  for (const [key, entries] of buckets) {
    if (entries.length <= 1) continue;
    // Find the matching `Group` (any entry will do for `cardLen`).
    const first = entries[0];
    const side = key.split("::")[1] as Side;
    const e0 = sides.find((x) => x.b.id === first.b.id)!;
    const group = first.end === "src" ? e0.srcG : e0.tgtG;
    // Distribute evenly along the side, leaving margin from corners
    // (40% of the side is used so labels still fit on the card).
    const usable = group.cardLen * 0.4;
    const step = usable / Math.max(1, entries.length - 1);
    // Order entries deterministically — by the OTHER endpoint's
    // position so the fan-out is geometrically sensible: edges that
    // come from further left attach further left on the side.
    const ranked = entries.slice().sort((a, b) => {
      const otherA = otherEndpointXY(a);
      const otherB = otherEndpointXY(b);
      return side === "top" || side === "bottom"
        ? otherA.x - otherB.x
        : otherA.y - otherB.y;
    });
    ranked.forEach((entry, i) => {
      const offset = entries.length === 1
        ? 0
        : -usable / 2 + i * step;
      const cur = result.get(entry.b.id)!;
      if (entry.end === "src") result.set(entry.b.id, { ...cur, src: offset });
      else result.set(entry.b.id, { ...cur, tgt: offset });
    });
  }
  return result;

  function otherEndpointXY(entry: { b: B; end: "src" | "tgt" }): Pt {
    const otherId = entry.end === "src"
      ? entry.b.targetCategoryId
      : entry.b.sourceCategoryId;
    return positions.get(otherId) ?? { x: 0, y: 0 };
  }
}

function bezierPoint(p0: Pt, p1: Pt, p2: Pt, p3: Pt, t: number): Pt {
  const u = 1 - t;
  return {
    x: u * u * u * p0.x + 3 * u * u * t * p1.x + 3 * u * t * t * p2.x + t * t * t * p3.x,
    y: u * u * u * p0.y + 3 * u * u * t * p1.y + 3 * u * t * t * p2.y + t * t * t * p3.y,
  };
}

function bezierTangent(p0: Pt, p1: Pt, p2: Pt, p3: Pt, t: number): Pt {
  const u = 1 - t;
  return {
    x: 3 * u * u * (p1.x - p0.x) + 6 * u * t * (p2.x - p1.x) + 3 * t * t * (p3.x - p2.x),
    y: 3 * u * u * (p1.y - p0.y) + 6 * u * t * (p2.y - p1.y) + 3 * t * t * (p3.y - p2.y),
  };
}
