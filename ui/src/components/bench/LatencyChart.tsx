import { useMemo } from "react";
import type { BenchMetric } from "@/api/bench-client";

// LatencyChart — grouped bar chart of p50/p95/p99 per stage (ingest /
// retrieve / think / apply). When a baseline is present, each stage
// gets two adjacent groups (baseline vs current). Verdict colors the
// current bar.

interface StageData {
  stage: string;
  current: { p50: number; p95: number; p99: number };
  baseline: { p50: number; p95: number; p99: number } | null;
  verdicts: { p50: string; p95: string; p99: string };
}

export function LatencyChart({ metrics }: { metrics: BenchMetric[] }) {
  const data = useMemo<StageData[]>(() => buildStageData(metrics), [metrics]);
  if (data.length === 0)
    return <div className="text-sm text-neutral-500">No latency data.</div>;

  const allValues = data.flatMap((d) => [
    d.current.p50,
    d.current.p95,
    d.current.p99,
    ...(d.baseline ? [d.baseline.p50, d.baseline.p95, d.baseline.p99] : []),
  ]);
  const maxVal = Math.max(...allValues, 1);

  const W = 700;
  const H = 280;
  const PAD = { left: 50, right: 16, top: 16, bottom: 60 };
  const innerW = W - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;

  const stageW = innerW / data.length;
  // Within a stage: 3 percentile groups (p50/p95/p99), each with 1 or 2 bars.
  const hasBaseline = data.some((d) => d.baseline);
  const barsPerGroup = hasBaseline ? 2 : 1;
  const groupGap = 6;
  const stageInner = stageW - groupGap;
  const pctGroupW = stageInner / 3;
  const barW = (pctGroupW - 4) / barsPerGroup;

  const yOf = (v: number) => PAD.top + innerH - (v / maxVal) * innerH;
  const verdictColor = (v: string) =>
    v === "regression" ? "#dc2626" : v === "improvement" ? "#059669" : "#171717";

  return (
    <div>
      <div className="text-xs text-neutral-500 mb-2 flex items-center gap-4">
        <Legend swatch="#171717" label="current (ok)" />
        <Legend swatch="#dc2626" label="regression" />
        <Legend swatch="#059669" label="improvement" />
        {hasBaseline ? (
          <Legend swatch="#d4d4d4" label="baseline" />
        ) : (
          <span className="italic">no baseline saved yet</span>
        )}
      </div>
      <svg width={W} height={H} className="block">
        {/* Y axis grid + labels */}
        {[0, 0.25, 0.5, 0.75, 1].map((f) => {
          const y = PAD.top + innerH - innerH * f;
          return (
            <g key={f}>
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
                fill="#737373"
              >
                {(maxVal * f).toFixed(1)} ms
              </text>
            </g>
          );
        })}
        {/* Bars */}
        {data.map((d, si) => {
          const stageX = PAD.left + si * stageW;
          return (
            <g key={d.stage}>
              {(["p50", "p95", "p99"] as const).map((pct, pi) => {
                const groupX = stageX + pctGroupW * pi + 2;
                const curV = d.current[pct];
                const baseV = d.baseline?.[pct];
                const verdictKey = pct as keyof typeof d.verdicts;
                return (
                  <g key={pct}>
                    {baseV !== undefined ? (
                      <rect
                        x={groupX}
                        y={yOf(baseV)}
                        width={barW}
                        height={Math.max(1, PAD.top + innerH - yOf(baseV))}
                        fill="#d4d4d4"
                      />
                    ) : null}
                    <rect
                      x={groupX + (baseV !== undefined ? barW : 0)}
                      y={yOf(curV)}
                      width={barW}
                      height={Math.max(1, PAD.top + innerH - yOf(curV))}
                      fill={verdictColor(d.verdicts[verdictKey])}
                    >
                      <title>
                        {`${d.stage} ${pct}: ${curV.toFixed(2)} ms` +
                          (baseV !== undefined
                            ? ` (baseline ${baseV.toFixed(2)} ms)`
                            : "")}
                      </title>
                    </rect>
                    <text
                      x={groupX + pctGroupW / 2 - 4}
                      y={H - PAD.bottom + 14}
                      fontSize={9}
                      textAnchor="middle"
                      fill="#737373"
                    >
                      {pct}
                    </text>
                  </g>
                );
              })}
              <text
                x={stageX + stageInner / 2}
                y={H - PAD.bottom + 32}
                fontSize={11}
                textAnchor="middle"
                fill="#171717"
                className="font-medium"
              >
                {d.stage}
              </text>
            </g>
          );
        })}
        {/* Axis line */}
        <line
          x1={PAD.left}
          x2={W - PAD.right}
          y1={PAD.top + innerH}
          y2={PAD.top + innerH}
          stroke="#171717"
          strokeWidth={1}
        />
      </svg>
    </div>
  );
}

function buildStageData(metrics: BenchMetric[]): StageData[] {
  const stages = ["ingest", "retrieve", "think", "apply"];
  const byKey: Map<string, BenchMetric> = new Map();
  for (const m of metrics) byKey.set(m.metric, m);
  return stages
    .map((s) => {
      const p50 = byKey.get(`${s}_p50`);
      const p95 = byKey.get(`${s}_p95`);
      const p99 = byKey.get(`${s}_p99`);
      if (!p50 || !p95 || !p99) return null;
      return {
        stage: s,
        current: { p50: p50.value, p95: p95.value, p99: p99.value },
        baseline:
          p50.baseline !== null && p95.baseline !== null && p99.baseline !== null
            ? { p50: p50.baseline, p95: p95.baseline, p99: p99.baseline }
            : null,
        verdicts: { p50: p50.verdict, p95: p95.verdict, p99: p99.verdict },
      } as StageData;
    })
    .filter((x): x is StageData => x !== null);
}

function Legend({ swatch, label }: { swatch: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span
        className="inline-block w-3 h-3 rounded-sm"
        style={{ background: swatch }}
      />
      <span className="text-neutral-700">{label}</span>
    </span>
  );
}
