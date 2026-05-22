// Small visual primitives shared across forecast components.
// Kept dependency-light so each Mode file can import what it needs
// without pulling in unrelated logic.

import type {
  ForecastSeverity,
  ForecastTrend,
  PatternStatus,
} from "@/api/forecasts-types";

// -----------------------------------------------------------------------
// Tiny SVG sparkline. Renders a stroked polyline normalized to 0..1.
// -----------------------------------------------------------------------

export interface SparklineProps {
  points: number[] | undefined;
  width?: number;
  height?: number;
  stroke?: string;
  ariaLabel?: string;
}

export function Sparkline({
  points,
  width = 80,
  height = 22,
  stroke = "currentColor",
  ariaLabel,
}: SparklineProps) {
  if (!points || points.length === 0) {
    return <svg width={width} height={height} aria-hidden="true" />;
  }
  const lo = Math.min(...points);
  const hi = Math.max(...points);
  const range = hi - lo || 1;
  const step = points.length > 1 ? width / (points.length - 1) : width;
  const path = points
    .map((p, i) => {
      const x = i * step;
      const y = height - ((p - lo) / range) * height;
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role={ariaLabel ? "img" : undefined}
      aria-label={ariaLabel}
      aria-hidden={ariaLabel ? undefined : true}
      className="fc-sparkline"
    >
      <path d={path} fill="none" stroke={stroke} strokeWidth={1.4} />
    </svg>
  );
}

// -----------------------------------------------------------------------
// Confidence bar — pill rendering of a 0..1 confidence number.
// -----------------------------------------------------------------------

export function ConfidencePill({
  value,
  delta,
}: {
  value: number;
  delta?: number | null;
}) {
  const pct = Math.round(value * 100);
  const dir = (delta ?? 0) > 0.005 ? "up" : (delta ?? 0) < -0.005 ? "down" : "flat";
  return (
    <span className={`fc-confidence fc-confidence--${dir}`}>
      <span className="fc-confidence__value">{pct}%</span>
      {delta != null && Math.abs(delta) >= 0.005 ? (
        <span className="fc-confidence__delta" aria-label="confidence change">
          {dir === "up" ? "↑" : "↓"}{Math.abs(Math.round(delta * 100))}pp
        </span>
      ) : null}
    </span>
  );
}

// -----------------------------------------------------------------------
// Trend arrow icon — text-labeled, never color-only.
// -----------------------------------------------------------------------

export function TrendArrow({ trend }: { trend: ForecastTrend }) {
  const map: Record<ForecastTrend, { glyph: string; aria: string }> = {
    up: { glyph: "↑", aria: "trending up" },
    down: { glyph: "↓", aria: "trending down" },
    flat: { glyph: "→", aria: "trending flat" },
    volatile: { glyph: "↔", aria: "volatile" },
  };
  const { glyph, aria } = map[trend];
  return (
    <span className={`fc-trend fc-trend--${trend}`} aria-label={aria}>
      {glyph}
    </span>
  );
}

// -----------------------------------------------------------------------
// Pattern status badge.
// -----------------------------------------------------------------------

export function PatternStatusBadge({ status }: { status: PatternStatus }) {
  const label = {
    emerging: "Emerging",
    strengthening: "Strengthening",
    stable: "Stable",
    weakening: "Weakening",
    resolved: "Resolved",
    archived: "Archived",
  }[status];
  return (
    <span className={`fc-pattern-status fc-pattern-status--${status}`}>
      {label}
    </span>
  );
}

// -----------------------------------------------------------------------
// Severity rail — used by the inspector data-attr.
// -----------------------------------------------------------------------

export function severityTone(severity: ForecastSeverity | undefined): string {
  return severity ?? "medium";
}

// -----------------------------------------------------------------------
// Date helpers
// -----------------------------------------------------------------------

export function formatDate(iso: string | null | undefined): string {
  if (!iso) return "";
  try {
    const d = new Date(iso);
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  } catch {
    return "";
  }
}

export function relativeDays(iso: string | null | undefined): string {
  if (!iso) return "";
  try {
    const d = new Date(iso).getTime();
    const days = Math.round((d - Date.now()) / 86_400_000);
    if (days === 0) return "today";
    if (days === 1) return "tomorrow";
    if (days > 1) return `in ${days} days`;
    if (days === -1) return "yesterday";
    return `${Math.abs(days)} days ago`;
  } catch {
    return "";
  }
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null) return "—";
  return `${Math.round(value * 100)}%`;
}

export function formatPpDelta(value: number | null | undefined): string {
  if (value == null) return "";
  const sign = value > 0 ? "+" : "";
  return `${sign}${(value * 100).toFixed(0)}pp`;
}

// -----------------------------------------------------------------------
// Confidence series — mini trajectory chart with optional resolution mark.
// -----------------------------------------------------------------------

export interface ConfidenceChartProps {
  points: { timestamp: string; confidence: number }[];
  width?: number;
  height?: number;
}

export function ConfidenceChart({
  points,
  width = 340,
  height = 80,
}: ConfidenceChartProps) {
  if (!points || points.length === 0) {
    return <div className="fc-chart fc-chart--empty">No confidence history.</div>;
  }
  const values = points.map((p) => p.confidence);
  const lo = Math.min(0.2, Math.min(...values) - 0.05);
  const hi = Math.max(0.95, Math.max(...values) + 0.05);
  const range = hi - lo || 1;
  const step = points.length > 1 ? (width - 8) / (points.length - 1) : width;
  const path = points
    .map((p, i) => {
      const x = 4 + i * step;
      const y = height - 6 - ((p.confidence - lo) / range) * (height - 14);
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const last = points[points.length - 1];
  const lastY = height - 6 - ((last.confidence - lo) / range) * (height - 14);
  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className="fc-chart"
      role="img"
      aria-label={`Confidence trajectory: ${points
        .map((p) => `${Math.round(p.confidence * 100)}%`)
        .join(", ")}`}
    >
      <path d={path} fill="none" stroke="var(--accent-trust)" strokeWidth={1.6} />
      <circle
        cx={4 + (points.length - 1) * step}
        cy={lastY}
        r={3.2}
        fill="var(--accent-trust)"
      />
    </svg>
  );
}
