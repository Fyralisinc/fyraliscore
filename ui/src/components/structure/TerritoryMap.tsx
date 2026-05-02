import { useEffect, useMemo, useRef, useState, useLayoutEffect } from "react";
import type {
  Commitment,
  ColorMode,
  DotPosition,
  LayoutMode,
  TerritoryId,
} from "./types";
import {
  ALL_TERRITORIES,
  TERRITORY_LABELS,
  computeTerritoryRects,
  computeTwoAxisPositions,
  dotRadius,
  isBeyondWindow,
  isOverdue,
  placeDots,
} from "./positioning";

// Spec Part 5 — territorial constellation map.

const STATUS_FILL: Record<Commitment["status"], string> = {
  "on-track": "var(--accent)",
  slipping: "var(--high)",
  "at-risk": "var(--critical)",
  blocked: "url(#hatch-blocked)",
};

const PALETTE = [
  "#0F766E",
  "#5B21B6",
  "#9F1239",
  "#854D0E",
  "#1E40AF",
  "#065F46",
  "#7C2D12",
  "#475569",
];

type Props = {
  commitments: Commitment[];
  layout: LayoutMode;
  color: ColorMode;
  maxDaysVisible: number;
  now: Date;
  selectedId: string | null;
  onSelect: (id: string | null) => void;
  dimNonMatching: Set<string> | null; // ids that should appear dimmed
  freshlyUpdatedIds?: Set<string> | null; // ids that just received a substrate update
  emptyState?: { reason: "filtered-zero" | "no-commitments"; onClear?: () => void };
};

export function TerritoryMap({
  commitments,
  layout,
  color,
  maxDaysVisible,
  now,
  selectedId,
  onSelect,
  dimNonMatching,
  freshlyUpdatedIds,
  emptyState,
}: Props) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState<{ w: number; h: number }>({
    w: 1000,
    h: 600,
  });

  // Resize observer (throttled to 100ms).
  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    let raf = 0;
    let lastTs = 0;
    const ro = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      const now = performance.now();
      if (now - lastTs < 100) {
        cancelAnimationFrame(raf);
        raf = requestAnimationFrame(() => {
          const r = entry.contentRect;
          setSize({ w: r.width, h: r.height });
        });
        return;
      }
      lastTs = now;
      const r = entry.contentRect;
      setSize({ w: r.width, h: r.height });
    });
    ro.observe(el);
    const r = el.getBoundingClientRect();
    setSize({ w: r.width, h: r.height });
    return () => {
      ro.disconnect();
      cancelAnimationFrame(raf);
    };
  }, []);

  const rects = useMemo(
    () => computeTerritoryRects(size.w, size.h),
    [size.w, size.h]
  );

  const dotPositions: DotPosition[] = useMemo(() => {
    if (layout === "territory") {
      return placeDots(commitments, rects, maxDaysVisible, now);
    }
    return computeTwoAxisPositions(
      commitments,
      size.w,
      size.h,
      maxDaysVisible,
      now
    );
  }, [commitments, rects, layout, maxDaysVisible, now, size.w, size.h]);

  const positionsById = useMemo(() => {
    const m = new Map<string, DotPosition>();
    for (const p of dotPositions) m.set(p.id, p);
    return m;
  }, [dotPositions]);

  // Color encoding lookup.
  const colorByEntity = useMemo(() => {
    if (color === "status") return null;
    const counts = new Map<string, number>();
    for (const c of commitments) {
      const key = colorKeyFor(c, color);
      if (!key) continue;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    const ranked = [...counts.entries()].sort((a, b) => b[1] - a[1]);
    const map = new Map<string, string>();
    ranked.slice(0, PALETTE.length).forEach(([k], i) => {
      map.set(k, PALETTE[i] ?? "#475569");
    });
    return map;
  }, [color, commitments]);

  function dotFill(c: Commitment): string {
    if (c.status === "blocked") return STATUS_FILL.blocked;
    if (color === "status") return STATUS_FILL[c.status];
    if (!colorByEntity) return STATUS_FILL[c.status];
    const k = colorKeyFor(c, color);
    if (!k) return "var(--ink-4)";
    return colorByEntity.get(k) ?? "var(--ink-4)";
  }

  // Tooltip state — single shared element, positioned via CSS.
  const [tooltip, setTooltip] = useState<{
    id: string;
    x: number;
    y: number;
  } | null>(null);
  const hoverTimer = useRef<number | null>(null);

  function onDotEnter(c: Commitment, x: number, y: number) {
    if (hoverTimer.current) window.clearTimeout(hoverTimer.current);
    hoverTimer.current = window.setTimeout(() => {
      setTooltip({ id: c.id, x, y });
    }, 60);
  }
  function onDotLeave() {
    if (hoverTimer.current) window.clearTimeout(hoverTimer.current);
    setTooltip(null);
  }

  // Keyboard: arrow keys move focus to nearest dot.
  const groupRefs = useRef<Map<string, SVGGElement | null>>(new Map());
  function onDotKey(
    e: React.KeyboardEvent,
    c: Commitment,
    pos: DotPosition
  ) {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      onSelect(c.id);
      return;
    }
    if (!["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown"].includes(e.key))
      return;
    e.preventDefault();
    let best: { id: string; d: number } | null = null;
    for (const other of dotPositions) {
      if (other.id === pos.id) continue;
      const dx = other.x - pos.x;
      const dy = other.y - pos.y;
      let directional = false;
      if (e.key === "ArrowLeft" && dx < -1 && Math.abs(dy) <= -dx + 60) directional = true;
      if (e.key === "ArrowRight" && dx > 1 && Math.abs(dy) <= dx + 60) directional = true;
      if (e.key === "ArrowUp" && dy < -1 && Math.abs(dx) <= -dy + 60) directional = true;
      if (e.key === "ArrowDown" && dy > 1 && Math.abs(dx) <= dy + 60) directional = true;
      if (!directional) continue;
      const d = dx * dx + dy * dy;
      if (!best || d < best.d) best = { id: other.id, d };
    }
    if (best) {
      const node = groupRefs.current.get(best.id);
      node?.focus();
    }
  }

  const tooltipCommitment = tooltip
    ? commitments.find((c) => c.id === tooltip.id)
    : null;

  const ariaLabel = `Commitment constellation map. ${commitments.length} active commitments across 5 territories. ${
    commitments.filter((c) => c.status !== "on-track").length
  } not on track.`;

  return (
    <div className="territory-map" ref={containerRef}>
      <svg
        className="territory-svg"
        width="100%"
        height="100%"
        viewBox={`0 0 ${size.w} ${size.h}`}
        role="img"
        aria-label={ariaLabel}
      >
        <defs>
          {ALL_TERRITORIES.map((id) => (
            <linearGradient
              key={id}
              id={`tint-${id}`}
              x1="0%"
              y1="0%"
              x2="0%"
              y2="100%"
            >
              <stop offset="0%" stopColor={tintTop(id)} />
              <stop offset="100%" stopColor={tintBottom(id)} />
            </linearGradient>
          ))}
          <pattern
            id="hatch-blocked"
            patternUnits="userSpaceOnUse"
            width="4"
            height="4"
            patternTransform="rotate(45)"
          >
            <line
              x1="0"
              y1="0"
              x2="0"
              y2="4"
              stroke="var(--ink-3)"
              strokeWidth="2"
            />
            <line
              x1="2"
              y1="0"
              x2="2"
              y2="4"
              stroke="var(--ink-5)"
              strokeWidth="2"
            />
          </pattern>
        </defs>

        {/* Territory backgrounds + labels (Territory mode only). */}
        {layout === "territory"
          ? ALL_TERRITORIES.map((id) => {
              const r = rects[id];
              if (!r) return null;
              return (
                <g
                  className="territory"
                  data-territory={id}
                  key={id}
                  style={{ animation: "fade-in 400ms var(--ease-out) both" }}
                >
                  <rect
                    className="territory-bg"
                    x={r.left}
                    y={r.top}
                    width={r.right - r.left}
                    height={r.bottom - r.top}
                    rx="12"
                    ry="12"
                    fill={`url(#tint-${id})`}
                  />
                  <rect
                    className="territory-border"
                    x={r.left}
                    y={r.top}
                    width={r.right - r.left}
                    height={r.bottom - r.top}
                    rx="12"
                    ry="12"
                    fill="none"
                    stroke="var(--rule-soft)"
                    strokeWidth="1"
                    strokeDasharray="4 4"
                  />
                  <text
                    className="territory-label"
                    x={r.left + 14}
                    y={r.top + 22}
                    fontFamily="var(--sans)"
                    fontSize="11"
                    fontWeight="600"
                    letterSpacing="1.2"
                    fill="var(--ink-3)"
                  >
                    {TERRITORY_LABELS[id]}
                  </text>
                </g>
              );
            })
          : renderAxes(size.w, size.h, maxDaysVisible)}

        {/* Dots */}
        <g className="dots-layer">
          {commitments.map((c, i) => {
            const pos = positionsById.get(c.id);
            if (!pos) return null;
            const r = pos.r;
            const overdue = isOverdue(c, now);
            const beyond = isBeyondWindow(c, now, maxDaysVisible);
            const isSelected = selectedId === c.id;
            const dimmed = dimNonMatching?.has(c.id) ?? false;
            const fresh = freshlyUpdatedIds?.has(c.id) ?? false;
            return (
              <g
                key={c.id}
                ref={(node) => {
                  if (node) groupRefs.current.set(c.id, node);
                }}
                className={
                  "dot-group" +
                  (isSelected ? " selected" : "") +
                  (dimmed ? " dimmed" : "") +
                  (fresh ? " fresh" : "")
                }
                data-status={c.status}
                data-priority={c.priority}
                tabIndex={0}
                role="button"
                aria-label={`${c.id}, ${c.label}, owned by ${c.owner_display}, due ${formatDate(c.due_date)}, ${c.status}`}
                transform={`translate(${pos.x}, ${pos.y})`}
                style={{ animationDelay: `${i * 12}ms` }}
                onClick={() => onSelect(c.id)}
                onMouseEnter={() => onDotEnter(c, pos.x, pos.y)}
                onMouseLeave={onDotLeave}
                onFocus={() => setTooltip({ id: c.id, x: pos.x, y: pos.y })}
                onBlur={() => setTooltip(null)}
                onKeyDown={(e) => onDotKey(e, c, pos)}
              >
                <circle
                  className="dot-fill"
                  cx="0"
                  cy="0"
                  r={r}
                  fill={dotFill(c)}
                />
                {c.status !== "blocked" ? (
                  <circle
                    className="dot-highlight"
                    cx={-r * 0.3}
                    cy={-r * 0.3}
                    r={Math.max(1.5, r * 0.3)}
                    fill="rgba(255,255,255,0.32)"
                  />
                ) : null}
                {overdue ? (
                  <circle
                    className="dot-overdue-ring"
                    cx="0"
                    cy="0"
                    r={r + 3}
                    fill="none"
                    stroke="var(--critical)"
                    strokeWidth="1.5"
                    strokeDasharray="2 2"
                  />
                ) : null}
                {beyond ? (
                  <circle
                    cx={r * 0.6}
                    cy="0"
                    r={r * 0.4}
                    fill="rgba(255,255,255,0.5)"
                  />
                ) : null}
                {isSelected ? (
                  <circle
                    className="dot-focus-ring"
                    cx="0"
                    cy="0"
                    r={r + 4}
                    fill="none"
                    stroke="var(--accent)"
                    strokeWidth="1.5"
                    opacity="0.7"
                  />
                ) : null}
                {fresh ? (
                  <circle
                    className="dot-fresh-ring"
                    cx="0"
                    cy="0"
                    r={r + 5}
                    fill="none"
                    stroke="var(--accent)"
                    strokeWidth="1.5"
                  />
                ) : null}
              </g>
            );
          })}
        </g>
      </svg>

      {tooltipCommitment ? (
        <DotTooltip
          c={tooltipCommitment}
          x={tooltip!.x}
          y={tooltip!.y}
          containerW={size.w}
        />
      ) : null}

      {emptyState ? (
        <EmptyOverlay
          reason={emptyState.reason}
          onClear={emptyState.onClear}
        />
      ) : null}

      {commitments.length > 0 && commitments.length <= 15 ? (
        <div className="sparse-note">
          Showing {commitments.length} commitments. The map becomes more useful
          as your work fills out.
        </div>
      ) : null}
    </div>
  );
}

function renderAxes(w: number, h: number, maxDays: number) {
  const padX = 96;
  const padY = 36;
  const ticks = [0, 0.33, 0.66, 1];
  return (
    <g className="two-axis-frame" style={{ animation: "fade-in 300ms var(--ease-out) both" }}>
      <line
        x1={padX}
        y1={h - padY}
        x2={w - padX}
        y2={h - padY}
        stroke="var(--rule-soft)"
      />
      <line
        x1={padX}
        y1={padY}
        x2={padX}
        y2={h - padY}
        stroke="var(--rule-soft)"
      />
      {ticks.map((t) => {
        const x = padX + t * (w - padX * 2);
        const days = Math.round(t * maxDays);
        return (
          <g key={t}>
            <line
              x1={x}
              y1={h - padY}
              x2={x}
              y2={h - padY + 4}
              stroke="var(--rule-soft)"
            />
            <text
              x={x}
              y={h - padY + 18}
              textAnchor="middle"
              fontFamily="var(--sans)"
              fontSize="10"
              fill="var(--ink-3)"
            >
              {days === 0 ? "due now" : `${days}d`}
            </text>
          </g>
        );
      })}
      {[
        { y: padY + (h - padY * 2) * 0.18, label: "high priority" },
        { y: padY + (h - padY * 2) * 0.5, label: "standard" },
        { y: padY + (h - padY * 2) * 0.82, label: "low priority" },
      ].map((row) => (
        <g key={row.label}>
          <text
            x={padX - 8}
            y={row.y + 4}
            textAnchor="end"
            fontFamily="var(--sans)"
            fontSize="10"
            fill="var(--ink-3)"
          >
            {row.label}
          </text>
          <line
            x1={padX}
            y1={row.y}
            x2={w - padX}
            y2={row.y}
            stroke="var(--rule-faint)"
            strokeDasharray="2 4"
          />
        </g>
      ))}
    </g>
  );
}

function DotTooltip({
  c,
  x,
  y,
  containerW,
}: {
  c: Commitment;
  x: number;
  y: number;
  containerW: number;
}) {
  // Default: tooltip 12px right of dot. Flip if it'd overflow.
  const flipLeft = x + 12 + 280 > containerW - 16;
  const style: React.CSSProperties = {
    left: flipLeft ? x - 12 : x + 12,
    top: y,
    transform: flipLeft ? "translate(-100%, -50%)" : "translateY(-50%)",
  };
  return (
    <div
      className="dot-tooltip visible"
      role="tooltip"
      style={style}
    >
      <div className="tooltip-title">
        <span className="tooltip-id">{c.id}</span> ·{" "}
        <span className="tooltip-label">{c.label}</span>
      </div>
      <div className="tooltip-meta">
        <span className="tooltip-owner">{c.owner_display}</span> ·{" "}
        <span className="tooltip-due">Due {formatDate(c.due_date)}</span> ·{" "}
        <span className="tooltip-status" data-status={c.status}>
          {c.status.replace("-", " ").toUpperCase()}
        </span>
      </div>
    </div>
  );
}

function EmptyOverlay({
  reason,
  onClear,
}: {
  reason: "filtered-zero" | "no-commitments";
  onClear?: () => void;
}) {
  if (reason === "no-commitments") {
    return (
      <div className="empty-state-overlay">
        <p className="empty-state-text">
          Your map will fill out as commitments get tracked. Right now I'm
          watching, but there's nothing to show.
        </p>
        <p className="empty-state-attribution">— Driftwood</p>
      </div>
    );
  }
  return (
    <div className="filter-zero">
      <p>No commitments match your filter.</p>
      {onClear ? (
        <button type="button" className="btn-text" onClick={onClear}>
          Clear filter
        </button>
      ) : null}
    </div>
  );
}

function colorKeyFor(c: Commitment, mode: ColorMode): string | null {
  if (mode === "owner") return c.owner;
  if (mode === "customer") return c.customer ?? null;
  if (mode === "decision") return c.traces_to[0] ?? null;
  return null;
}

function tintTop(id: TerritoryId): string {
  switch (id) {
    case "strategic":
      return "rgba(91, 33, 182, 0.04)";
    case "customer-facing":
      return "rgba(15, 118, 110, 0.04)";
    case "technical-infrastructure":
      return "rgba(90, 92, 102, 0.04)";
    case "internal-operations":
      return "rgba(133, 77, 14, 0.04)";
    case "personnel":
      return "rgba(91, 33, 182, 0.03)";
  }
}
function tintBottom(id: TerritoryId): string {
  switch (id) {
    case "strategic":
      return "rgba(91, 33, 182, 0.06)";
    case "customer-facing":
      return "rgba(15, 118, 110, 0.06)";
    case "technical-infrastructure":
      return "rgba(90, 92, 102, 0.06)";
    case "internal-operations":
      return "rgba(133, 77, 14, 0.06)";
    case "personnel":
      return "rgba(91, 33, 182, 0.05)";
  }
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

// Avoid unused import warnings during ts checks.
useEffect;
dotRadius;
