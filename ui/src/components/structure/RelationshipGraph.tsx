import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import type {
  Commitment,
  DecisionRef,
  FocusTarget,
  GoalLearnings,
  GoalRef,
  LearnedPattern,
  PatternEvidence,
  PersonProfile,
  ResourceRef,
} from "./types";

type Props = {
  commitments: Commitment[];
  goals: GoalRef[];
  decisions: DecisionRef[];
  resources: ResourceRef[];
  peopleIndex: Record<string, PersonProfile>;
  goalLearnings: Record<string, GoalLearnings>;
  ownerLabels: Record<string, string>;
  focus: FocusTarget | null;
  hoveredCommitmentId: string | null;
  onFocus: (target: FocusTarget | null) => void;
};

// Pan/zoom limits — applied uniformly to actor / commitment / goal /
// aggregate views.
const ZOOM_MIN = 0.5;
const ZOOM_MAX = 2.5;
const SIDE_PANEL_W = 360;

// Right canvas — three modes:
//   • Aggregate (focus = null): goal nodes on inner/outer rings, with
//     status-colored leaves orbiting each goal.
//   • Focus on commitment: classic 4-quadrant view (goals ↑, decisions ↓,
//     resources →, people ←, related commitments at corners).
//   • Focus on goal/decision/resource/actor: that artifact at center,
//     surrounded by all commitments that touch it.
//
// Every chip in the focus view is clickable — click drills into that
// chip's own focus view. A back button (top-left) returns to the
// previous step or all the way out to aggregate.
export function RelationshipGraph({
  commitments,
  goals,
  decisions,
  resources,
  peopleIndex,
  goalLearnings,
  ownerLabels,
  focus,
  hoveredCommitmentId,
  onFocus,
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const svgRef = useRef<SVGSVGElement | null>(null);
  const [size, setSize] = useState<{ w: number; h: number }>({ w: 800, h: 620 });

  // Pan/zoom transform applied to the inner <g>. tx/ty are in viewBox
  // units; scale clamps to [ZOOM_MIN, ZOOM_MAX].
  const [tx, setTx] = useState(0);
  const [ty, setTy] = useState(0);
  const [scale, setScale] = useState(1);
  const dragRef = useRef<{
    startX: number; startY: number; startTx: number; startTy: number;
  } | null>(null);
  const [dragging, setDragging] = useState(false);

  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      const r = el.getBoundingClientRect();
      setSize({ w: Math.max(560, r.width), h: Math.max(520, r.height) });
    });
    ro.observe(el);
    const r = el.getBoundingClientRect();
    setSize({ w: Math.max(560, r.width), h: Math.max(520, r.height) });
    return () => ro.disconnect();
  }, []);

  const idIndex = useMemo(() => {
    const m = new Map<string, Commitment>();
    for (const c of commitments) m.set(c.id, c);
    return m;
  }, [commitments]);
  const goalIndex = useMemo(() => new Map(goals.map((g) => [g.id, g])), [goals]);
  const decisionIndex = useMemo(
    () => new Map(decisions.map((d) => [d.id, d])),
    [decisions]
  );
  const resourceIndex = useMemo(
    () => new Map(resources.map((r) => [r.id, r])),
    [resources]
  );

  // Resolve the current focus and a fallback (hovered commitment) so the
  // graph reacts to mouse-over from the list when nothing is selected.
  const liveFocus: FocusTarget | null =
    focus ??
    (hoveredCommitmentId ? { kind: "commitment", id: hoveredCommitmentId } : null);

  // Decide whether a side panel is shown for the live focus. People,
  // commitments, and goals all get their own learnings panel.
  const sidePanelKind: "actor" | "commitment" | "goal" | null =
    liveFocus
      ? liveFocus.kind === "actor"
        ? "actor"
        : liveFocus.kind === "commitment"
        ? "commitment"
        : liveFocus.kind === "goal"
        ? "goal"
        : null
      : null;
  const sidePanelOpen = !!sidePanelKind;

  // Reset pan/zoom whenever focus changes — and pre-shift the graph
  // left when a side panel is open, so the focused artifact stays
  // visually centered in the remaining canvas.
  useEffect(() => {
    setTx(sidePanelOpen ? -SIDE_PANEL_W / 2 : 0);
    setTy(0);
    setScale(1);
  }, [focus?.kind, focus?.id, sidePanelOpen]);

  // Native wheel listener — passive: false so we can preventDefault
  // and zoom relative to the cursor position rather than scrolling
  // the page.
  useEffect(() => {
    const el = svgRef.current;
    if (!el) return;
    function onWheel(e: WheelEvent) {
      e.preventDefault();
      const rect = el!.getBoundingClientRect();
      const px = ((e.clientX - rect.left) / rect.width) * size.w;
      const py = ((e.clientY - rect.top) / rect.height) * size.h;
      const factor = e.deltaY > 0 ? 1 / 1.1 : 1.1;
      setScale((s) => {
        const next = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, s * factor));
        const real = next / s;
        setTx((t) => clampTx(px - (px - t) * real, size.w, next));
        setTy((t) => clampTy(py - (py - t) * real, size.h, next));
        return next;
      });
    }
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [size.w, size.h]);

  const onPanStart = (e: React.MouseEvent<SVGRectElement>) => {
    dragRef.current = { startX: e.clientX, startY: e.clientY, startTx: tx, startTy: ty };
    setDragging(true);
  };
  const onPanMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const d = dragRef.current;
    if (!d) return;
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    const dx = ((e.clientX - d.startX) / rect.width) * size.w;
    const dy = ((e.clientY - d.startY) / rect.height) * size.h;
    setTx(clampTx(d.startTx + dx, size.w, scale));
    setTy(clampTy(d.startTy + dy, size.h, scale));
  };
  const onPanEnd = () => {
    dragRef.current = null;
    setDragging(false);
  };

  const zoomBy = (factor: number) => {
    setScale((s) => {
      const next = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, s * factor));
      // Zoom around the geometric center of the canvas.
      const cx = size.w / 2;
      const cy = size.h / 2;
      const real = next / s;
      setTx((t) => clampTx(cx - (cx - t) * real, size.w, next));
      setTy((t) => clampTy(cy - (cy - t) * real, size.h, next));
      return next;
    });
  };
  const resetView = () => {
    setTx(sidePanelOpen ? -SIDE_PANEL_W / 2 : 0);
    setTy(0);
    setScale(1);
  };

  return (
    <div className="relgraph" ref={containerRef}>
      {focus ? (
        <button
          type="button"
          className="rg-back"
          onClick={() => onFocus(null)}
          aria-label="Back to overview"
          title="Back to overview"
        >
          <span className="rg-back-glyph">←</span>
          <span>All commitments</span>
        </button>
      ) : null}
      <svg
        ref={svgRef}
        className={"relgraph-svg" + (dragging ? " dragging" : "")}
        width="100%"
        height="100%"
        viewBox={`0 0 ${size.w} ${size.h}`}
        role="img"
        aria-label={
          liveFocus ? `Relationships for ${liveFocus.kind} ${liveFocus.id}` : "Aggregate relationship graph"
        }
        onMouseMove={onPanMove}
        onMouseUp={onPanEnd}
        onMouseLeave={onPanEnd}
      >
        <rect
          className="rg-pan-surface"
          x={0}
          y={0}
          width={size.w}
          height={size.h}
          onMouseDown={onPanStart}
        />
        <g transform={`translate(${tx}, ${ty}) scale(${scale})`}>
          {liveFocus ? (
            <FocusRouter
              focus={liveFocus}
              commitments={commitments}
              idIndex={idIndex}
              goalIndex={goalIndex}
              decisionIndex={decisionIndex}
              resourceIndex={resourceIndex}
              peopleIndex={peopleIndex}
              ownerLabels={ownerLabels}
              w={size.w}
              h={size.h}
              onFocus={onFocus}
            />
          ) : (
            <AggregateGraph
              commitments={commitments}
              goals={goals}
              w={size.w}
              h={size.h}
              onFocus={onFocus}
            />
          )}
        </g>
      </svg>

      {sidePanelKind === "actor" && liveFocus ? (
        <ActorPanel
          profile={peopleIndex[liveFocus.id]}
          fallbackLabel={ownerLabels[liveFocus.id] ?? liveFocus.id}
          commitments={commitments.filter(
            (c) => c.owner === liveFocus.id || (c.edges?.contributors ?? []).includes(liveFocus.id)
          )}
          onFocus={onFocus}
        />
      ) : null}
      {sidePanelKind === "commitment" && liveFocus ? (
        <CommitmentPanel
          commitment={idIndex.get(liveFocus.id)}
          goalIndex={goalIndex}
          decisionIndex={decisionIndex}
          resourceIndex={resourceIndex}
          ownerLabels={ownerLabels}
          onFocus={onFocus}
        />
      ) : null}
      {sidePanelKind === "goal" && liveFocus ? (
        <GoalPanel
          goal={goalIndex.get(liveFocus.id)}
          learnings={goalLearnings[liveFocus.id]}
          commitments={commitments.filter((c) =>
            (c.edges?.contributes_to ?? []).includes(liveFocus.id)
          )}
          onFocus={onFocus}
        />
      ) : null}

      <ZoomControls
        scale={scale}
        onZoomIn={() => zoomBy(1.2)}
        onZoomOut={() => zoomBy(1 / 1.2)}
        onReset={resetView}
      />
      <RelGraphLegend />
    </div>
  );
}

function clampTx(t: number, w: number, scale: number): number {
  const limit = Math.max(160, w * 0.6 * scale);
  return Math.max(-limit, Math.min(limit, t));
}
function clampTy(t: number, h: number, scale: number): number {
  const limit = Math.max(120, h * 0.5 * scale);
  return Math.max(-limit, Math.min(limit, t));
}

function ZoomControls({
  scale,
  onZoomIn,
  onZoomOut,
  onReset,
}: {
  scale: number;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onReset: () => void;
}) {
  return (
    <div className="rg-zoom-controls" role="toolbar" aria-label="Pan and zoom">
      <button
        type="button"
        onClick={onZoomOut}
        disabled={scale <= ZOOM_MIN + 0.001}
        aria-label="Zoom out"
        title="Zoom out"
      >
        −
      </button>
      <button
        type="button"
        className="rg-zoom-reset"
        onClick={onReset}
        aria-label="Reset view"
        title="Reset view (drag to pan, scroll to zoom)"
      >
        {Math.round(scale * 100)}%
      </button>
      <button
        type="button"
        onClick={onZoomIn}
        disabled={scale >= ZOOM_MAX - 0.001}
        aria-label="Zoom in"
        title="Zoom in"
      >
        +
      </button>
    </div>
  );
}

// ────────────────────────────────────────────────────────────────────
// FOCUS ROUTER — picks the right shape per focus kind
// ────────────────────────────────────────────────────────────────────

function FocusRouter(props: {
  focus: FocusTarget;
  commitments: Commitment[];
  idIndex: Map<string, Commitment>;
  goalIndex: Map<string, GoalRef>;
  decisionIndex: Map<string, DecisionRef>;
  resourceIndex: Map<string, ResourceRef>;
  peopleIndex: Record<string, PersonProfile>;
  ownerLabels: Record<string, string>;
  w: number;
  h: number;
  onFocus: (t: FocusTarget | null) => void;
}) {
  const { focus, idIndex, goalIndex, decisionIndex, resourceIndex } = props;

  if (focus.kind === "commitment") {
    const c = idIndex.get(focus.id);
    if (!c) return null;
    return <CommitmentFocus c={c} {...props} />;
  }
  if (focus.kind === "goal") {
    const g = goalIndex.get(focus.id);
    if (!g) return null;
    const list = props.commitments.filter((c) =>
      (c.edges?.contributes_to ?? []).includes(g.id)
    );
    return (
      <ArtifactFanout
        kind="goal"
        glyph="◆"
        title={g.label}
        sub={g.altitude.toUpperCase() + " GOAL"}
        commitments={list}
        emptyHint="No commitments contribute to this goal."
        w={props.w}
        h={props.h}
        onFocus={props.onFocus}
      />
    );
  }
  if (focus.kind === "decision") {
    const d = decisionIndex.get(focus.id);
    if (!d) return null;
    const list = props.commitments.filter((c) =>
      (c.edges?.constrained_by ?? c.traces_to ?? []).includes(d.id)
    );
    return (
      <ArtifactFanout
        kind="decision"
        glyph="⌃"
        title={d.label}
        sub={"DECISION · " + d.state.toUpperCase()}
        commitments={list}
        emptyHint="No commitments are constrained by this decision."
        w={props.w}
        h={props.h}
        onFocus={props.onFocus}
      />
    );
  }
  if (focus.kind === "resource") {
    const r = resourceIndex.get(focus.id);
    if (!r) return null;
    const list = props.commitments.filter((c) =>
      (c.edges?.consumes ?? []).includes(r.id)
    );
    return (
      <ArtifactFanout
        kind="resource"
        glyph="▤"
        title={r.label}
        sub={r.kind.toUpperCase() + " RESOURCE"}
        commitments={list}
        emptyHint="No commitments consume this resource."
        w={props.w}
        h={props.h}
        onFocus={props.onFocus}
      />
    );
  }
  if (focus.kind === "actor") {
    const profile = props.peopleIndex[focus.id];
    const label = profile?.label ?? props.ownerLabels[focus.id] ?? focus.id;
    const list = props.commitments.filter(
      (c) => c.owner === focus.id || (c.edges?.contributors ?? []).includes(focus.id)
    );
    return (
      <ActorFocus
        profile={profile}
        fallbackLabel={label}
        commitments={list}
        w={props.w}
        h={props.h}
        onFocus={props.onFocus}
      />
    );
  }
  return null;
}

// ────────────────────────────────────────────────────────────────────
// COMMITMENT FOCUS — 4 quadrant ring around a single commitment
// ────────────────────────────────────────────────────────────────────

const FOCUS_RADIUS = {
  goal: 170,
  decision: 170,
  resource: 200,
  people: 200,
  related: 240,
};

function CommitmentFocus({
  c,
  commitments,
  goalIndex,
  decisionIndex,
  resourceIndex,
  ownerLabels,
  w,
  h,
  onFocus,
}: {
  c: Commitment;
  commitments: Commitment[];
  goalIndex: Map<string, GoalRef>;
  decisionIndex: Map<string, DecisionRef>;
  resourceIndex: Map<string, ResourceRef>;
  ownerLabels: Record<string, string>;
  w: number;
  h: number;
  onFocus: (t: FocusTarget | null) => void;
}) {
  const cx = w / 2;
  const cy = h / 2;

  // Resolve + sort each quadrant. Sort criteria mirror the substrate's
  // priority signals: strategic goals first, in-force decisions first,
  // financial resources first, owner before contributors, and related
  // commitments off-track-first.
  const goals = useMemo(() => {
    const list = (c.edges?.contributes_to ?? [])
      .map((id) => goalIndex.get(id))
      .filter(Boolean) as GoalRef[];
    return list.sort((a, b) =>
      a.altitude === b.altitude ? 0 : a.altitude === "strategic" ? -1 : 1
    );
  }, [c, goalIndex]);

  const dec = useMemo(() => {
    const list = (c.edges?.constrained_by ?? c.traces_to ?? [])
      .map((id) => decisionIndex.get(id))
      .filter(Boolean) as DecisionRef[];
    const order: Record<DecisionRef["state"], number> = {
      "in-force": 0, drifting: 1, revisited: 2,
    };
    return list.sort((a, b) => (order[a.state] ?? 9) - (order[b.state] ?? 9));
  }, [c, decisionIndex]);

  const res = useMemo(() => {
    const list = (c.edges?.consumes ?? [])
      .map((id) => resourceIndex.get(id))
      .filter(Boolean) as ResourceRef[];
    const order: Record<ResourceRef["kind"], number> = {
      financial: 0, human: 1, technical: 2,
    };
    return list.sort((a, b) => (order[a.kind] ?? 9) - (order[b.kind] ?? 9));
  }, [c, resourceIndex]);

  const peopleIds = useMemo(() => [c.owner, ...(c.edges?.contributors ?? [])], [c]);
  const related = useMemo(() => {
    const list = (c.related ?? [])
      .map((rid) => commitments.find((x) => x.id === rid))
      .filter(Boolean) as Commitment[];
    const order: Record<Commitment["status"], number> = {
      "at-risk": 0, blocked: 1, slipping: 2, "on-track": 3,
    };
    return list.sort((a, b) => (order[a.status] ?? 9) - (order[b.status] ?? 9));
  }, [c, commitments]);

  // Compact mode kicks in if any quadrant exceeds 4 entries — chips
  // shrink so they have room to fit on the inner arc without overlap;
  // overflow then pushes onto a secondary arc.
  const maxInQuadrant = Math.max(
    goals.length, dec.length, res.length, peopleIds.length, related.length
  );
  const compact = maxInQuadrant > 4;
  const chipW = compact ? 132 : 168;
  const chipH = compact ? 38 : 50;
  const radii = compact
    ? { goal: 150, decision: 150, resource: 180, people: 180, related: 220 }
    : FOCUS_RADIUS;

  const goalPos = quadrantArcs(cx, cy, radii.goal, -Math.PI / 2, goals.length, chipW, 0.9);
  const decisionPos = quadrantArcs(cx, cy, radii.decision, Math.PI / 2, dec.length, chipW, 0.9);
  const resourcePos = quadrantArcs(cx, cy, radii.resource, 0, res.length, chipW, 0.7);
  const peoplePos = quadrantArcs(cx, cy, radii.people, Math.PI, peopleIds.length, chipW, 0.7);
  const relatedPos = corners(cx, cy, radii.related, related.length);

  return (
    <g className="rg-focus">
      {goals.map((g, i) => (
        <Edge key={"e-g-" + g.id} x1={cx} y1={cy} {...goalPos[i]} kind="goal" />
      ))}
      {dec.map((d, i) => (
        <Edge key={"e-d-" + d.id} x1={cx} y1={cy} {...decisionPos[i]} kind="decision" />
      ))}
      {res.map((r, i) => (
        <Edge key={"e-r-" + r.id} x1={cx} y1={cy} {...resourcePos[i]} kind="resource" />
      ))}
      {peopleIds.map((p, i) => (
        <Edge key={"e-p-" + p} x1={cx} y1={cy} {...peoplePos[i]} kind="people" />
      ))}
      {related.map((r, i) => (
        <Edge key={"e-rel-" + r.id} x1={cx} y1={cy} {...relatedPos[i]} kind="related" />
      ))}

      <CenterCommitment c={c} cx={cx} cy={cy} />

      {goals.map((g, i) => (
        <ChipNode
          key={"n-g-" + g.id}
          x={goalPos[i].x2}
          y={goalPos[i].y2}
          label={g.label}
          sub={g.altitude}
          kind="goal"
          glyph="◆"
          width={chipW}
          height={chipH}
          onClick={() => onFocus({ kind: "goal", id: g.id })}
        />
      ))}
      {dec.map((d, i) => (
        <ChipNode
          key={"n-d-" + d.id}
          x={decisionPos[i].x2}
          y={decisionPos[i].y2}
          label={d.label}
          sub={d.state}
          kind="decision"
          glyph="⌃"
          width={chipW}
          height={chipH}
          onClick={() => onFocus({ kind: "decision", id: d.id })}
        />
      ))}
      {res.map((r, i) => (
        <ChipNode
          key={"n-r-" + r.id}
          x={resourcePos[i].x2}
          y={resourcePos[i].y2}
          label={r.label}
          sub={r.kind}
          kind="resource"
          glyph="▤"
          width={chipW}
          height={chipH}
          onClick={() => onFocus({ kind: "resource", id: r.id })}
        />
      ))}
      {peopleIds.map((p, i) => (
        <ChipNode
          key={"n-p-" + p}
          x={peoplePos[i].x2}
          y={peoplePos[i].y2}
          label={ownerLabels[p] ?? p}
          sub={p === c.owner ? "owner" : "contributor"}
          kind="people"
          glyph="◯"
          width={chipW}
          height={chipH}
          onClick={() => onFocus({ kind: "actor", id: p })}
        />
      ))}
      {related.map((r, i) => (
        <ChipNode
          key={"n-rel-" + r.id}
          x={relatedPos[i].x2}
          y={relatedPos[i].y2}
          label={r.label}
          sub={r.id + " · " + r.status}
          kind="related"
          glyph="↔"
          width={chipW}
          height={chipH}
          onClick={() => onFocus({ kind: "commitment", id: r.id })}
        />
      ))}

      <QuadrantLabel x={cx} y={cy - radii.goal - 32} text="GOALS" />
      <QuadrantLabel x={cx} y={cy + radii.decision + 36} text="DECISIONS" />
      <QuadrantLabel x={cx + radii.resource + 60} y={cy + 4} text="RESOURCES" />
      <QuadrantLabel x={cx - radii.people - 60} y={cy + 4} text="PEOPLE" />
    </g>
  );
}

// ────────────────────────────────────────────────────────────────────
// ARTIFACT FANOUT — goal / decision / resource / actor at center,
// surrounded by the commitments that touch it
// ────────────────────────────────────────────────────────────────────

function ArtifactFanout({
  kind,
  glyph,
  title,
  sub,
  commitments,
  emptyHint,
  w,
  h,
  onFocus,
}: {
  kind: "goal" | "decision" | "resource" | "actor";
  glyph: string;
  title: string;
  sub: string;
  commitments: Commitment[];
  emptyHint: string;
  w: number;
  h: number;
  onFocus: (t: FocusTarget | null) => void;
}) {
  const cx = w / 2;
  const cy = h / 2;
  const count = commitments.length;

  // Sort: off-track first (at-risk → blocked → slipping → on-track),
  // then by due date. Higher-attention items end up on the inner ring.
  const sorted = useMemo(() => {
    const order: Record<Commitment["status"], number> = {
      "at-risk": 0, blocked: 1, slipping: 2, "on-track": 3,
    };
    return [...commitments].sort((a, b) => {
      const so = (order[a.status] ?? 4) - (order[b.status] ?? 4);
      if (so !== 0) return so;
      return new Date(a.due_date).getTime() - new Date(b.due_date).getTime();
    });
  }, [commitments]);

  // Ring layout: pack commitments onto concentric rings so chips never
  // overlap. Innermost ring carries the highest-priority status items.
  const compact = count > 8;
  const chipW = compact ? 132 : 168;
  const chipH = compact ? 38 : 50;
  // Approximate min spacing between chip centers along an arc.
  const minStep = chipW + 14;

  const positions = useMemo(() => {
    if (count === 0) return [];
    const baseR = Math.min(w * 0.22, h * 0.30, 200);
    const ringStep = compact ? 70 : 88;
    type Ring = { r: number; capacity: number };
    const rings: Ring[] = [];
    let placed = 0;
    while (placed < count) {
      const r = baseR + rings.length * ringStep;
      const circumference = 2 * Math.PI * r;
      const cap = Math.max(4, Math.floor(circumference / minStep));
      rings.push({ r, capacity: cap });
      placed += cap;
    }
    const out: { x: number; y: number; r: number }[] = [];
    let i = 0;
    for (const ring of rings) {
      const remaining = count - i;
      const inThisRing = Math.min(ring.capacity, remaining);
      // Distribute inThisRing items evenly around the ring, starting at
      // top (-π/2). Slight rotation per ring keeps chips from stacking
      // radially across rings.
      const rotate = rings.indexOf(ring) * 0.18;
      for (let k = 0; k < inThisRing; k++) {
        const angle =
          -Math.PI / 2 +
          rotate +
          (inThisRing === 1 ? 0 : (k / inThisRing) * Math.PI * 2);
        out.push({
          x: cx + Math.cos(angle) * ring.r,
          y: cy + Math.sin(angle) * ring.r,
          r: ring.r,
        });
        i++;
        if (i >= count) break;
      }
    }
    return out;
  }, [count, compact, cx, cy, h, w, minStep]);

  // Off-track count for the center-readout
  const offTrack = sorted.filter((c) => c.status !== "on-track").length;

  return (
    <g className="rg-fanout">
      {/* Edges (drawn under nodes) */}
      {sorted.map((c, i) => (
        <line
          key={"ef-" + c.id}
          x1={cx}
          y1={cy}
          x2={positions[i].x}
          y2={positions[i].y}
          className={"rg-fanout-edge s-" + c.status}
        />
      ))}

      {/* Center artifact */}
      <CenterArtifact glyph={glyph} title={title} sub={sub} cx={cx} cy={cy} kind={kind} />

      {/* Empty state */}
      {count === 0 ? (
        <text x={cx} y={cy + 80} textAnchor="middle" className="rg-fanout-empty">
          {emptyHint}
        </text>
      ) : null}

      {/* Surrounding commitments */}
      {sorted.map((c, i) => (
        <ChipNode
          key={"nf-" + c.id}
          x={positions[i].x}
          y={positions[i].y}
          label={c.label}
          sub={c.owner_display + " · " + c.status.replace("-", " ")}
          kind={statusToChipKind(c.status)}
          glyph="●"
          width={chipW}
          height={chipH}
          onClick={() => onFocus({ kind: "commitment", id: c.id })}
        />
      ))}

      {/* Counter underneath the center */}
      {count > 0 ? (
        <text x={cx} y={cy + 70} textAnchor="middle" className="rg-fanout-count">
          {count} commitment{count === 1 ? "" : "s"}
          {offTrack > 0 ? ` · ${offTrack} off-track` : ""}
        </text>
      ) : null}
    </g>
  );
}

// ────────────────────────────────────────────────────────────────────
// ACTOR FOCUS — person at center, their commitments fanned out. The
// learnings panel is rendered as an HTML overlay outside the SVG (see
// ActorPanel) so it is not affected by the graph's pan/zoom transform.
// ────────────────────────────────────────────────────────────────────

function ActorFocus({
  profile,
  fallbackLabel,
  commitments,
  w,
  h,
  onFocus,
}: {
  profile: PersonProfile | undefined;
  fallbackLabel: string;
  commitments: Commitment[];
  w: number;
  h: number;
  onFocus: (t: FocusTarget | null) => void;
}) {
  return (
    <ArtifactFanout
      kind="actor"
      glyph="◯"
      title={profile?.label ?? fallbackLabel}
      sub={profile?.role?.toUpperCase() ?? "PERSON"}
      commitments={commitments}
      emptyHint="No commitments link to this person."
      w={w}
      h={h}
      onFocus={onFocus}
    />
  );
}

// ────────────────────────────────────────────────────────────────────
// HTML side panels — rendered as siblings of the SVG so they are not
// scaled or panned with the graph content.
// ────────────────────────────────────────────────────────────────────

function PanelShell({
  accent,
  eyebrow,
  name,
  sub,
  children,
}: {
  accent: "actor" | "commitment-on-track" | "commitment-slipping" | "commitment-at-risk" | "commitment-blocked" | "goal-strategic" | "goal-operational";
  eyebrow: string;
  name: string;
  sub?: string;
  children: React.ReactNode;
}) {
  return (
    <aside className={"rg-side-panel rg-side-panel-" + accent} aria-label={eyebrow}>
      <div className="rg-actor-panel">
        <div className="rg-actor-panel-head">
          <span className="rg-actor-panel-eyebrow">{eyebrow}</span>
          <span className="rg-actor-panel-name">{name}</span>
          {sub ? <span className="rg-actor-panel-role">{sub}</span> : null}
        </div>
        {children}
      </div>
    </aside>
  );
}

function CalibrationBlock({ value }: { value: number }) {
  return (
    <div className="rg-actor-calibration">
      <div className="rg-actor-calibration-head">
        <span>Model calibration</span>
        <span>{(value * 100).toFixed(0)}%</span>
      </div>
      <div className="rg-actor-calibration-bar">
        <div
          className="rg-actor-calibration-fill"
          style={{ width: `${value * 100}%` }}
        />
      </div>
      <p className="rg-actor-calibration-hint">
        {value >= 0.85
          ? "High confidence — patterns are stable across many observations."
          : value >= 0.70
          ? "Moderate confidence — patterns are forming but still evolving."
          : "Low confidence — limited signal; patterns may shift as more is observed."}
      </p>
    </div>
  );
}

function PatternsBlock({
  patterns,
  onFocus,
  emptyHint,
}: {
  patterns: LearnedPattern[] | undefined;
  onFocus: (t: FocusTarget | null) => void;
  emptyHint: string;
}) {
  return (
    <section className="rg-actor-section">
      <h4>Patterns</h4>
      {patterns && patterns.length > 0 ? (
        <>
          <p className="rg-actor-patterns-hint">
            Click a pattern to see the evidence that supports it.
          </p>
          <ol className="rg-actor-patterns">
            {patterns.map((p) => (
              <PatternItem key={p.id} pattern={p} onFocus={onFocus} />
            ))}
          </ol>
        </>
      ) : (
        <p className="rg-actor-patterns-empty">{emptyHint}</p>
      )}
    </section>
  );
}

function ActorPanel({
  profile,
  fallbackLabel,
  commitments,
  onFocus,
}: {
  profile: PersonProfile | undefined;
  fallbackLabel: string;
  commitments: Commitment[];
  onFocus: (t: FocusTarget | null) => void;
}) {
  const offTrack = commitments.filter((c) => c.status !== "on-track").length;
  const high = commitments.filter((c) => c.priority === "high").length;
  const customers = Array.from(
    new Set(commitments.map((c) => c.customer).filter(Boolean) as string[])
  );
  if (!profile) {
    return (
      <PanelShell accent="actor" eyebrow="Person" name={fallbackLabel} sub="PERSON">
        <p className="rg-actor-recent">No profile data for this person yet.</p>
      </PanelShell>
    );
  }
  return (
    <PanelShell
      accent="actor"
      eyebrow="What I've learned"
      name={profile.label}
      sub={profile.role}
    >
      <div className="rg-actor-stats">
        <div className="rg-actor-stat">
          <span className="rg-actor-stat-num">{commitments.length}</span>
          <span className="rg-actor-stat-lbl">load</span>
        </div>
        <div className={"rg-actor-stat" + (offTrack > 0 ? " warn" : "")}>
          <span className="rg-actor-stat-num">{offTrack}</span>
          <span className="rg-actor-stat-lbl">off-track</span>
        </div>
        <div className="rg-actor-stat">
          <span className="rg-actor-stat-num">{high}</span>
          <span className="rg-actor-stat-lbl">high-pri</span>
        </div>
      </div>
      <CalibrationBlock value={profile.calibration} />
      <section className="rg-actor-section">
        <h4>Recent</h4>
        <p className="rg-actor-recent">{profile.recent_observation}</p>
      </section>
      <PatternsBlock
        patterns={profile.patterns}
        onFocus={onFocus}
        emptyHint="No patterns observed yet."
      />
      {customers.length > 0 ? (
        <section className="rg-actor-section">
          <h4>Active customers</h4>
          <p className="rg-actor-customers">
            {customers.map((c) => c.charAt(0).toUpperCase() + c.slice(1)).join(" · ")}
          </p>
        </section>
      ) : null}
    </PanelShell>
  );
}

function CommitmentPanel({
  commitment,
  goalIndex,
  decisionIndex,
  resourceIndex,
  ownerLabels,
  onFocus,
}: {
  commitment: Commitment | undefined;
  goalIndex: Map<string, GoalRef>;
  decisionIndex: Map<string, DecisionRef>;
  resourceIndex: Map<string, ResourceRef>;
  ownerLabels: Record<string, string>;
  onFocus: (t: FocusTarget | null) => void;
}) {
  if (!commitment) return null;
  const c = commitment;
  const goalsLinked = (c.edges?.contributes_to ?? [])
    .map((id) => goalIndex.get(id))
    .filter(Boolean) as GoalRef[];
  const decisionsLinked = (c.edges?.constrained_by ?? c.traces_to ?? [])
    .map((id) => decisionIndex.get(id))
    .filter(Boolean) as DecisionRef[];
  const resourcesLinked = (c.edges?.consumes ?? [])
    .map((id) => resourceIndex.get(id))
    .filter(Boolean) as ResourceRef[];
  const contributors = c.edges?.contributors ?? [];
  const accent =
    c.status === "on-track"
      ? "commitment-on-track"
      : c.status === "slipping"
      ? "commitment-slipping"
      : c.status === "at-risk"
      ? "commitment-at-risk"
      : "commitment-blocked";

  return (
    <PanelShell
      accent={accent}
      eyebrow="What I've noticed"
      name={c.label}
      sub={`${c.id.toUpperCase()} · ${c.status.replace("-", " ").toUpperCase()}`}
    >
      <div className="rg-actor-stats">
        <div className="rg-actor-stat">
          <span className="rg-actor-stat-num">{goalsLinked.length}</span>
          <span className="rg-actor-stat-lbl">goals</span>
        </div>
        <div className="rg-actor-stat">
          <span className="rg-actor-stat-num">{decisionsLinked.length}</span>
          <span className="rg-actor-stat-lbl">decisions</span>
        </div>
        <div className="rg-actor-stat">
          <span className="rg-actor-stat-num">{resourcesLinked.length}</span>
          <span className="rg-actor-stat-lbl">resources</span>
        </div>
      </div>

      <section className="rg-actor-section">
        <h4>Snapshot</h4>
        <dl className="rg-fact-list">
          <div><dt>Owner</dt><dd>
            <button
              type="button"
              className="rg-fact-link"
              onClick={() => onFocus({ kind: "actor", id: c.owner })}
            >
              {c.owner_display}
            </button>
          </dd></div>
          <div><dt>Due</dt><dd>{formatLongDate(c.due_date)}</dd></div>
          <div><dt>Priority</dt><dd>{c.priority}</dd></div>
          {c.customer ? (
            <div><dt>Customer</dt><dd>{cap(c.customer)}</dd></div>
          ) : null}
          <div><dt>Stakeholder</dt><dd>{c.stakeholder_label}</dd></div>
          {c.progress ? <div><dt>Progress</dt><dd>{c.progress}</dd></div> : null}
        </dl>
      </section>

      {c.substrate_insight ? (
        <section className="rg-actor-section">
          <h4>System note</h4>
          <p className="rg-actor-recent">{c.substrate_insight}</p>
        </section>
      ) : null}

      <PatternsBlock
        patterns={c.learnings}
        onFocus={onFocus}
        emptyHint="Thin signal — the system has not accumulated patterns specific to this commitment yet."
      />

      {contributors.length > 0 ? (
        <section className="rg-actor-section">
          <h4>Contributors</h4>
          <ul className="rg-link-list">
            {contributors.map((id) => (
              <li key={id}>
                <button
                  type="button"
                  className="rg-link-row"
                  onClick={() => onFocus({ kind: "actor", id })}
                >
                  <span className="rg-link-glyph">◯</span>
                  <span className="rg-link-label">{ownerLabels[id] ?? id}</span>
                </button>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {c.activity.length > 0 ? (
        <section className="rg-actor-section">
          <h4>Activity</h4>
          <ul className="rg-activity-list">
            {c.activity.map((a, i) => (
              <li key={i}>
                <span className="rg-evidence-when">{a.date}</span>
                <span className="rg-evidence-text">{a.desc}</span>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </PanelShell>
  );
}

function GoalPanel({
  goal,
  learnings,
  commitments,
  onFocus,
}: {
  goal: GoalRef | undefined;
  learnings: GoalLearnings | undefined;
  commitments: Commitment[];
  onFocus: (t: FocusTarget | null) => void;
}) {
  if (!goal) return null;
  const total = commitments.length;
  const off = commitments.filter((c) => c.status !== "on-track").length;
  const ownerCounts = new Map<string, number>();
  for (const c of commitments) {
    ownerCounts.set(c.owner, (ownerCounts.get(c.owner) ?? 0) + 1);
  }
  const topContributors = [...ownerCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 4);

  return (
    <PanelShell
      accent={goal.altitude === "strategic" ? "goal-strategic" : "goal-operational"}
      eyebrow="What I've noticed"
      name={goal.label}
      sub={`${goal.altitude.toUpperCase()} GOAL`}
    >
      <div className="rg-actor-stats">
        <div className="rg-actor-stat">
          <span className="rg-actor-stat-num">{total}</span>
          <span className="rg-actor-stat-lbl">commits</span>
        </div>
        <div className={"rg-actor-stat" + (off > 0 ? " warn" : "")}>
          <span className="rg-actor-stat-num">{off}</span>
          <span className="rg-actor-stat-lbl">off-track</span>
        </div>
        <div className="rg-actor-stat">
          <span className="rg-actor-stat-num">{topContributors.length}</span>
          <span className="rg-actor-stat-lbl">owners</span>
        </div>
      </div>

      {learnings ? (
        <>
          <CalibrationBlock value={learnings.calibration} />
          <section className="rg-actor-section">
            <h4>Recent</h4>
            <p className="rg-actor-recent">{learnings.recent_observation}</p>
          </section>
        </>
      ) : null}

      <PatternsBlock
        patterns={learnings?.patterns}
        onFocus={onFocus}
        emptyHint="Thin signal — the system has not accumulated goal-level patterns here yet."
      />

      {topContributors.length > 0 ? (
        <section className="rg-actor-section">
          <h4>Top contributors</h4>
          <ul className="rg-link-list">
            {topContributors.map(([id, count]) => (
              <li key={id}>
                <button
                  type="button"
                  className="rg-link-row"
                  onClick={() => onFocus({ kind: "actor", id })}
                >
                  <span className="rg-link-glyph">◯</span>
                  <span className="rg-link-label">{id.charAt(0).toUpperCase() + id.slice(1)}</span>
                  <span className="rg-link-meta">{count} commit{count === 1 ? "" : "s"}</span>
                </button>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </PanelShell>
  );
}

function formatLongDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}
function cap(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

// Single pattern row inside the side panel. Click toggles the evidence
// drawer; evidence items with a ref are themselves clickable and drill
// into the referenced artifact (commitment / decision / goal).
function PatternItem({
  pattern,
  onFocus,
}: {
  pattern: LearnedPattern;
  onFocus: (t: FocusTarget | null) => void;
}) {
  const [open, setOpen] = useState(false);
  const strengthPct = Math.round(pattern.strength * 100);
  return (
    <li className={"rg-pattern" + (open ? " open" : "")}>
      <button
        type="button"
        className="rg-pattern-row"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
      >
        <span className="rg-pattern-caret" aria-hidden>
          {open ? "▾" : "▸"}
        </span>
        <span className="rg-pattern-text">{pattern.statement}</span>
        <span
          className="rg-pattern-strength"
          title={`Pattern strength: ${strengthPct}% (${pattern.evidence.length} observation${pattern.evidence.length === 1 ? "" : "s"})`}
        >
          <span className="rg-pattern-strength-bar">
            <span
              className="rg-pattern-strength-fill"
              style={{ width: `${strengthPct}%` }}
            />
          </span>
          <span className="rg-pattern-strength-num">{strengthPct}</span>
        </span>
      </button>
      {open ? (
        <div className="rg-evidence">
          <div className="rg-evidence-head">
            <span>Evidence</span>
            <span>{pattern.evidence.length} observation{pattern.evidence.length === 1 ? "" : "s"}</span>
          </div>
          <ul className="rg-evidence-list">
            {pattern.evidence.map((e, i) => (
              <EvidenceRow key={i} ev={e} onFocus={onFocus} />
            ))}
          </ul>
        </div>
      ) : null}
    </li>
  );
}

function EvidenceRow({
  ev,
  onFocus,
}: {
  ev: PatternEvidence;
  onFocus: (t: FocusTarget | null) => void;
}) {
  const clickable = !!ev.ref;
  const onClick = () => {
    if (ev.ref) onFocus({ kind: ev.ref.kind, id: ev.ref.id });
  };
  return (
    <li
      className={"rg-evidence-item" + (clickable ? " clickable" : "")}
      onClick={clickable ? onClick : undefined}
      role={clickable ? "button" : undefined}
      tabIndex={clickable ? 0 : undefined}
      onKeyDown={
        clickable
          ? (e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                onClick();
              }
            }
          : undefined
      }
    >
      <span className="rg-evidence-when">{ev.when}</span>
      <span className="rg-evidence-text">{ev.text}</span>
      {ev.ref ? (
        <span className="rg-evidence-ref">
          {ev.ref.kind === "commitment" ? "↗ " + ev.ref.id : null}
          {ev.ref.kind === "decision" ? "⌃ " + ev.ref.id : null}
          {ev.ref.kind === "goal" ? "◆ " + ev.ref.id : null}
        </span>
      ) : null}
    </li>
  );
}

function statusToChipKind(s: Commitment["status"]) {
  if (s === "on-track") return "ok";
  if (s === "slipping") return "slip";
  if (s === "at-risk") return "risk";
  return "blocked";
}

function CenterArtifact({
  glyph,
  title,
  sub,
  cx,
  cy,
  kind,
}: {
  glyph: string;
  title: string;
  sub: string;
  cx: number;
  cy: number;
  kind: string;
}) {
  const w = 260;
  const h = 92;
  return (
    <g className={"rg-center-artifact rg-center-" + kind}>
      <rect
        x={cx - w / 2}
        y={cy - h / 2}
        width={w}
        height={h}
        rx="12"
        className="rg-center-bg"
      />
      <text x={cx - w / 2 + 18} y={cy - 6} className="rg-artifact-glyph">
        {glyph}
      </text>
      <text x={cx} y={cy - 14} textAnchor="middle" className="rg-artifact-sub">
        {sub}
      </text>
      <foreignObject x={cx - w / 2 + 14} y={cy + 0} width={w - 28} height={h / 2 - 8}>
        <div className="rg-artifact-title">{title}</div>
      </foreignObject>
    </g>
  );
}

// ────────────────────────────────────────────────────────────────────
// Shared SVG building blocks
// ────────────────────────────────────────────────────────────────────

function CenterCommitment({ c, cx, cy }: { c: Commitment; cx: number; cy: number }) {
  const w = 240;
  const h = 76;
  return (
    <g className="rg-center" data-status={c.status}>
      <rect
        x={cx - w / 2}
        y={cy - h / 2}
        width={w}
        height={h}
        rx="10"
        ry="10"
        className="rg-center-bg"
      />
      <text x={cx} y={cy - 14} textAnchor="middle" className="rg-center-id">
        {c.id} · {c.status.replace("-", " ")}
      </text>
      <foreignObject x={cx - w / 2 + 10} y={cy - 4} width={w - 20} height={32}>
        <div className="rg-center-label">{c.label}</div>
      </foreignObject>
      <text x={cx} y={cy + h / 2 - 8} textAnchor="middle" className="rg-center-meta">
        {c.owner_display} · due{" "}
        {new Date(c.due_date).toLocaleDateString("en-US", { month: "short", day: "numeric" })}
      </text>
    </g>
  );
}

function ChipNode({
  x,
  y,
  label,
  sub,
  kind,
  glyph,
  onClick,
  width = 168,
  height = 50,
}: {
  x: number;
  y: number;
  label: string;
  sub?: string;
  kind: "goal" | "decision" | "resource" | "people" | "related" | "ok" | "slip" | "risk" | "blocked";
  glyph: string;
  onClick?: () => void;
  width?: number;
  height?: number;
}) {
  const w = width;
  const h = height;
  return (
    <g
      className={"rg-chip rg-chip-" + kind + (onClick ? " clickable" : "")}
      transform={`translate(${x - w / 2}, ${y - h / 2})`}
      onClick={onClick}
      style={onClick ? { cursor: "pointer" } : undefined}
      role={onClick ? "button" : undefined}
      tabIndex={onClick ? 0 : undefined}
    >
      <rect className="rg-chip-bg" width={w} height={h} rx="8" ry="8" />
      <text x="10" y="20" className="rg-chip-glyph">{glyph}</text>
      <foreignObject x="26" y="6" width={w - 30} height={h - 8}>
        <div className="rg-chip-text">
          <div className="rg-chip-label">{label}</div>
          {sub ? <div className="rg-chip-sub">{sub}</div> : null}
        </div>
      </foreignObject>
    </g>
  );
}

function QuadrantLabel({ x, y, text }: { x: number; y: number; text: string }) {
  return (
    <text x={x} y={y} className="rg-quadrant-label" textAnchor="middle">
      {text}
    </text>
  );
}

function Edge({
  x1,
  y1,
  x2,
  y2,
  kind,
}: {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  kind: "goal" | "decision" | "resource" | "people" | "related";
}) {
  return <line className={"rg-edge rg-edge-" + kind} x1={x1} y1={y1} x2={x2} y2={y2} />;
}

function arcPositions(
  cx: number,
  cy: number,
  r: number,
  centerAngle: number,
  count: number,
  spread: number
) {
  if (count === 0) return [];
  if (count === 1) {
    return [{ x2: cx + Math.cos(centerAngle) * r, y2: cy + Math.sin(centerAngle) * r }];
  }
  const arc = Math.PI * spread;
  const step = arc / (count - 1);
  const start = centerAngle - arc / 2;
  return Array.from({ length: count }, (_, i) => {
    const a = start + step * i;
    return { x2: cx + Math.cos(a) * r, y2: cy + Math.sin(a) * r };
  });
}

// Multi-arc positioning: when more chips would fit on a single arc than
// its capacity allows (chip width + gap > chord length), push the
// remainder onto progressively larger concentric arcs. Keeps the
// commitment-focus quadrants readable when a commitment carries many
// relations of one kind.
function quadrantArcs(
  cx: number,
  cy: number,
  baseR: number,
  centerAngle: number,
  count: number,
  chipW: number,
  spread: number
) {
  if (count === 0) return [];
  const out: { x2: number; y2: number }[] = [];
  const minStep = chipW + 14;
  const ringStep = 78;
  let placed = 0;
  let ringIdx = 0;
  while (placed < count) {
    const r = baseR + ringIdx * ringStep;
    const arcLen = Math.PI * spread * r;
    const cap = Math.max(2, Math.floor(arcLen / minStep) + 1);
    const inThisArc = Math.min(cap, count - placed);
    const positions = arcPositions(cx, cy, r, centerAngle, inThisArc, spread);
    out.push(...positions);
    placed += inThisArc;
    ringIdx += 1;
  }
  return out;
}

function corners(cx: number, cy: number, r: number, count: number) {
  if (count === 0) return [];
  const angles = [-Math.PI / 4, -3 * Math.PI / 4, Math.PI / 4, 3 * Math.PI / 4];
  return Array.from({ length: count }, (_, i) => {
    const a = angles[i % angles.length] + Math.floor(i / angles.length) * 0.18;
    return { x2: cx + Math.cos(a) * r, y2: cy + Math.sin(a) * r };
  });
}

// ────────────────────────────────────────────────────────────────────
// AGGREGATE
// ────────────────────────────────────────────────────────────────────

function AggregateGraph({
  commitments,
  goals,
  w,
  h,
  onFocus,
}: {
  commitments: Commitment[];
  goals: GoalRef[];
  w: number;
  h: number;
  onFocus: (t: FocusTarget | null) => void;
}) {
  const counts = useMemo(() => {
    const m = new Map<string, number>();
    for (const c of commitments)
      for (const g of c.edges?.contributes_to ?? []) m.set(g, (m.get(g) ?? 0) + 1);
    return m;
  }, [commitments]);

  const cx = w / 2;
  const cy = h / 2;
  const innerR = Math.min(w, h) * 0.18;
  const outerR = Math.min(w, h) * 0.36;

  const strat = goals.filter((g) => g.altitude === "strategic");
  const op = goals.filter((g) => g.altitude === "operational");
  const stratPos = ringPositions(cx, cy, innerR, strat.length, -Math.PI / 2);
  const opPos = ringPositions(cx, cy, outerR, op.length, -Math.PI / 2 + 0.4);

  const goalCenter = new Map<string, { x: number; y: number }>();
  strat.forEach((g, i) => goalCenter.set(g.id, stratPos[i]));
  op.forEach((g, i) => goalCenter.set(g.id, opPos[i]));

  const byGoal = new Map<string, Commitment[]>();
  for (const c of commitments) {
    const gid = (c.edges?.contributes_to ?? [])[0];
    const key = gid ?? "__orphan__";
    const arr = byGoal.get(key) ?? [];
    arr.push(c);
    byGoal.set(key, arr);
  }

  return (
    <g className="rg-aggregate">
      <text x={cx} y={cy - 6} textAnchor="middle" className="rg-agg-center-pri">
        {commitments.length}
      </text>
      <text x={cx} y={cy + 12} textAnchor="middle" className="rg-agg-center-sec">
        active commitments
      </text>
      <text x={cx} y={cy + 26} textAnchor="middle" className="rg-agg-center-hint">
        click any node to drill in
      </text>

      {[...byGoal.entries()].map(([gid, list]) => {
        if (gid === "__orphan__") return null;
        const center = goalCenter.get(gid);
        if (!center) return null;
        // Sort: off-track items at the front of the orbit so the eye
        // catches them first.
        const order: Record<Commitment["status"], number> = {
          "at-risk": 0, blocked: 1, slipping: 2, "on-track": 3,
        };
        const sorted = [...list].sort(
          (a, b) => (order[a.status] ?? 9) - (order[b.status] ?? 9)
        );
        // Multi-ring leaves so dots never collide. Capacity per ring
        // scales with circumference; a wider base keeps leaves clear of
        // the goal-node label below the circle.
        const leafPos = orbitPositions(center.x, center.y, sorted.length);
        return (
          <g key={"agg-" + gid}>
            {sorted.map((c, i) => (
              <line
                key={"el-" + c.id}
                className={"rg-agg-edge s-" + c.status}
                x1={center.x}
                y1={center.y}
                x2={leafPos[i].x}
                y2={leafPos[i].y}
              />
            ))}
            {sorted.map((c, i) => (
              <circle
                key={"ll-" + c.id}
                className={"rg-agg-leaf s-" + c.status}
                cx={leafPos[i].x}
                cy={leafPos[i].y}
                r={c.priority === "high" ? 5 : c.priority === "low" ? 3 : 4}
                onClick={() => onFocus({ kind: "commitment", id: c.id })}
              >
                <title>
                  {c.id} · {c.label} · {c.status}
                </title>
              </circle>
            ))}
          </g>
        );
      })}

      {goals.map((g) => {
        const p = goalCenter.get(g.id);
        if (!p) return null;
        const cnt = counts.get(g.id) ?? 0;
        const r = 14 + Math.min(28, cnt * 1.6);
        return (
          <g
            key={"goal-" + g.id}
            className={"rg-agg-goal alt-" + g.altitude + " clickable"}
            onClick={() => onFocus({ kind: "goal", id: g.id })}
            style={{ cursor: "pointer" }}
          >
            <circle cx={p.x} cy={p.y} r={r} className="rg-agg-goal-bg" />
            <text x={p.x} y={p.y + 3} textAnchor="middle" className="rg-agg-goal-label">
              {g.label}
            </text>
            <text x={p.x} y={p.y + r + 14} textAnchor="middle" className="rg-agg-goal-count">
              {cnt} {cnt === 1 ? "commitment" : "commitments"}
            </text>
          </g>
        );
      })}

      {byGoal.get("__orphan__") && (byGoal.get("__orphan__") as Commitment[]).length > 0 ? (
        <g className="rg-agg-orphans">
          <text x={32} y={28} className="rg-agg-orphans-label">
            ⚠ {(byGoal.get("__orphan__") as Commitment[]).length} unlinked
          </text>
        </g>
      ) : null}
    </g>
  );
}

function ringPositions(
  cx: number,
  cy: number,
  r: number,
  count: number,
  startAngle: number
) {
  if (count === 0) return [];
  if (count === 1) return [{ x: cx, y: cy - r }];
  return Array.from({ length: count }, (_, i) => {
    const a = startAngle + (i / count) * Math.PI * 2;
    return { x: cx + Math.cos(a) * r, y: cy + Math.sin(a) * r };
  });
}

// Multi-ring orbit for the aggregate-graph leaves around each goal.
// Leaves are tiny circles (r ≈ 4) so each ring can carry 8-12; we open
// a second ring once the first fills, keeping leaves from stacking and
// avoiding overlap with the goal-node label that sits just below.
function orbitPositions(
  cx: number,
  cy: number,
  count: number
): { x: number; y: number }[] {
  if (count === 0) return [];
  const out: { x: number; y: number }[] = [];
  const baseR = 32;
  const ringStep = 14;
  const perRing = 9;
  let placed = 0;
  let ringIdx = 0;
  while (placed < count) {
    const r = baseR + ringIdx * ringStep;
    const inThisRing = Math.min(perRing, count - placed);
    // Skip the bottom 80° arc so leaves don't crowd the count label.
    const arcSpread = Math.PI * 1.55;
    const start = -Math.PI / 2 - arcSpread / 2;
    const step = inThisRing <= 1 ? 0 : arcSpread / (inThisRing - 1);
    for (let k = 0; k < inThisRing; k++) {
      const a = inThisRing === 1 ? -Math.PI / 2 : start + step * k;
      out.push({ x: cx + Math.cos(a) * r, y: cy + Math.sin(a) * r });
    }
    placed += inThisRing;
    ringIdx += 1;
  }
  return out;
}

function RelGraphLegend() {
  return (
    <div className="rg-legend">
      <span className="rg-legend-label">Edges</span>
      <span className="rg-legend-item rg-legend-goal">◆ goal</span>
      <span className="rg-legend-item rg-legend-decision">⌃ decision</span>
      <span className="rg-legend-item rg-legend-resource">▤ resource</span>
      <span className="rg-legend-item rg-legend-people">◯ people</span>
      <span className="rg-legend-item rg-legend-related">↔ related</span>
    </div>
  );
}
