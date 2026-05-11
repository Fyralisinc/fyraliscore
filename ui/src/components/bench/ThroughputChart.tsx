import { useMemo } from "react";
import type { BenchMetric } from "@/api/bench-client";

// ThroughputChart — dual-axis line chart of signals/sec and p95 latency
// vs concurrency (8 / 16 / 32). Shows where the system saturates.

interface Point {
  concurrency: number;
  sps: number;
  p95: number;
  saturated: boolean;
}

export function ThroughputChart({ metrics }: { metrics: BenchMetric[] }) {
  const points = useMemo<Point[]>(() => buildPoints(metrics), [metrics]);
  if (points.length === 0)
    return <div className="text-sm text-neutral-500">No throughput data.</div>;

  const W = 700;
  const H = 280;
  const PAD = { left: 50, right: 50, top: 16, bottom: 50 };
  const innerW = W - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;

  const maxSps = Math.max(...points.map((p) => p.sps), 1);
  const maxP95 = Math.max(...points.map((p) => p.p95), 1);
  const cs = points.map((p) => p.concurrency);
  const minC = Math.min(...cs);
  const maxC = Math.max(...cs);
  const span = Math.max(1, maxC - minC);

  const xOf = (c: number) =>
    PAD.left + ((c - minC) / span) * innerW;
  const ySpsOf = (v: number) =>
    PAD.top + innerH - (v / maxSps) * innerH;
  const yP95Of = (v: number) =>
    PAD.top + innerH - (v / maxP95) * innerH;

  const spsPath = points
    .map((p, i) => `${i === 0 ? "M" : "L"} ${xOf(p.concurrency).toFixed(2)} ${ySpsOf(p.sps).toFixed(2)}`)
    .join(" ");
  const p95Path = points
    .map((p, i) => `${i === 0 ? "M" : "L"} ${xOf(p.concurrency).toFixed(2)} ${yP95Of(p.p95).toFixed(2)}`)
    .join(" ");

  return (
    <div>
      <div className="text-xs text-neutral-500 mb-2 flex items-center gap-4">
        <Legend swatch="#0f766e" label="signals/sec (left axis)" />
        <Legend swatch="#b91c1c" label="p95 latency ms (right axis)" />
        <Legend swatch="#fbbf24" label="saturated" filled={false} />
      </div>
      <svg width={W} height={H} className="block">
        {/* Left Y axis (sps) */}
        {[0, 0.25, 0.5, 0.75, 1].map((f) => {
          const y = PAD.top + innerH - innerH * f;
          return (
            <g key={`l-${f}`}>
              <line
                x1={PAD.left}
                x2={W - PAD.right}
                y1={y}
                y2={y}
                stroke="#e5e5e5"
                strokeWidth={0.5}
              />
              <text
                x={PAD.left - 6}
                y={y + 3}
                fontSize={10}
                textAnchor="end"
                fill="#0f766e"
              >
                {(maxSps * f).toFixed(0)}
              </text>
            </g>
          );
        })}
        {/* Right Y axis (p95) */}
        {[0, 0.25, 0.5, 0.75, 1].map((f) => {
          const y = PAD.top + innerH - innerH * f;
          return (
            <text
              key={`r-${f}`}
              x={W - PAD.right + 6}
              y={y + 3}
              fontSize={10}
              textAnchor="start"
              fill="#b91c1c"
            >
              {(maxP95 * f).toFixed(0)}
            </text>
          );
        })}
        {/* Lines */}
        <path d={spsPath} stroke="#0f766e" strokeWidth={2} fill="none" />
        <path d={p95Path} stroke="#b91c1c" strokeWidth={2} fill="none" strokeDasharray="4 3" />
        {/* Points */}
        {points.map((p, i) => (
          <g key={i}>
            <circle cx={xOf(p.concurrency)} cy={ySpsOf(p.sps)} r={4} fill="#0f766e">
              <title>{`c=${p.concurrency}: ${p.sps.toFixed(1)} signals/sec`}</title>
            </circle>
            <circle cx={xOf(p.concurrency)} cy={yP95Of(p.p95)} r={4} fill="#b91c1c">
              <title>{`c=${p.concurrency}: p95 ${p.p95.toFixed(1)} ms`}</title>
            </circle>
            {p.saturated ? (
              <circle
                cx={xOf(p.concurrency)}
                cy={yP95Of(p.p95)}
                r={9}
                fill="none"
                stroke="#fbbf24"
                strokeWidth={2}
              />
            ) : null}
          </g>
        ))}
        {/* X axis */}
        <line
          x1={PAD.left}
          x2={W - PAD.right}
          y1={PAD.top + innerH}
          y2={PAD.top + innerH}
          stroke="#171717"
        />
        {points.map((p, i) => (
          <text
            key={i}
            x={xOf(p.concurrency)}
            y={H - PAD.bottom + 16}
            fontSize={11}
            textAnchor="middle"
            fill="#171717"
          >
            c={p.concurrency}
          </text>
        ))}
        <text
          x={(PAD.left + W - PAD.right) / 2}
          y={H - 8}
          fontSize={11}
          textAnchor="middle"
          fill="#525252"
        >
          concurrency
        </text>
      </svg>
    </div>
  );
}

function buildPoints(metrics: BenchMetric[]): Point[] {
  const byKey: Map<string, number> = new Map();
  for (const m of metrics) byKey.set(m.metric, m.value);
  const concurrencies: number[] = [];
  for (const m of metrics) {
    const match = m.metric.match(/^signals_per_sec_at_c(\d+)$/);
    if (match) concurrencies.push(Number(match[1]));
  }
  concurrencies.sort((a, b) => a - b);
  return concurrencies.map((c) => ({
    concurrency: c,
    sps: byKey.get(`signals_per_sec_at_c${c}`) ?? 0,
    p95: byKey.get(`p95_latency_at_c${c}`) ?? 0,
    saturated: (byKey.get(`saturated_at_c${c}`) ?? 0) > 0,
  }));
}

function Legend({
  swatch,
  label,
  filled = true,
}: {
  swatch: string;
  label: string;
  filled?: boolean;
}) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span
        className="inline-block w-3 h-3 rounded-full"
        style={
          filled
            ? { background: swatch }
            : { border: `2px solid ${swatch}`, background: "transparent" }
        }
      />
      <span className="text-neutral-700">{label}</span>
    </span>
  );
}
