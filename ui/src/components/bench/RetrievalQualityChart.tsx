import { useMemo } from "react";
import type { BenchMetric } from "@/api/bench-client";

// RetrievalQualityChart — two modes:
//
// 1. Labeled-set mode (when recall_at_K metrics exist): line chart of
//    recall@k against k=10/20/80, plus a pathway-share donut.
//
// 2. Surrogate-timing mode (when labels missing): horizontal bar chart
//    of pathway_*_ms timings.

export function RetrievalQualityChart({ metrics }: { metrics: BenchMetric[] }) {
  const labeled = useMemo(() => hasLabeled(metrics), [metrics]);
  if (labeled) return <LabeledChart metrics={metrics} />;
  return <SurrogateChart metrics={metrics} />;
}

function hasLabeled(metrics: BenchMetric[]): boolean {
  return metrics.some((m) => m.metric.startsWith("recall_at_"));
}

function LabeledChart({ metrics }: { metrics: BenchMetric[] }) {
  const recallPoints = useMemo(() => {
    const ks = [10, 20, 80];
    const byKey: Map<string, BenchMetric> = new Map();
    for (const m of metrics) byKey.set(m.metric, m);
    return ks.map((k) => ({
      k,
      current: byKey.get(`recall_at_${k}`)?.value ?? 0,
      baseline: byKey.get(`recall_at_${k}`)?.baseline ?? null,
      verdict: byKey.get(`recall_at_${k}`)?.verdict ?? "ok",
    }));
  }, [metrics]);
  const ndcg = metrics.find((m) => m.metric === "ndcg_at_10");
  const pathways = ["a", "b", "c", "f"]
    .map((p) => ({
      key: p,
      value: metrics.find((m) => m.metric === `pathway_${p}_share`)?.value ?? 0,
    }))
    .filter((x) => x.value > 0);

  const W = 360;
  const H = 220;
  const PAD = { left: 40, right: 16, top: 16, bottom: 36 };

  const innerW = W - PAD.left - PAD.right;
  const innerH = H - PAD.top - PAD.bottom;
  const xOf = (i: number) =>
    PAD.left + (i / Math.max(1, recallPoints.length - 1)) * innerW;
  const yOf = (v: number) => PAD.top + innerH - v * innerH;

  const curPath = recallPoints
    .map((p, i) => `${i === 0 ? "M" : "L"} ${xOf(i).toFixed(2)} ${yOf(p.current).toFixed(2)}`)
    .join(" ");
  const basePath =
    recallPoints.every((p) => p.baseline !== null)
      ? recallPoints
          .map((p, i) => `${i === 0 ? "M" : "L"} ${xOf(i).toFixed(2)} ${yOf(p.baseline!).toFixed(2)}`)
          .join(" ")
      : "";

  return (
    <div className="grid grid-cols-2 gap-6">
      <div>
        <h4 className="text-xs uppercase tracking-wider text-neutral-500 mb-2">
          Recall@k
        </h4>
        <svg width={W} height={H} className="block">
          {[0, 0.25, 0.5, 0.75, 1].map((f) => (
            <g key={f}>
              <line
                x1={PAD.left}
                x2={W - PAD.right}
                y1={yOf(f)}
                y2={yOf(f)}
                stroke="#e5e5e5"
                strokeWidth={0.5}
              />
              <text
                x={PAD.left - 6}
                y={yOf(f) + 3}
                fontSize={10}
                textAnchor="end"
                fill="#737373"
              >
                {f.toFixed(2)}
              </text>
            </g>
          ))}
          {basePath ? (
            <path d={basePath} stroke="#a3a3a3" strokeDasharray="4 3" fill="none" />
          ) : null}
          <path d={curPath} stroke="#171717" strokeWidth={2} fill="none" />
          {recallPoints.map((p, i) => (
            <circle
              key={p.k}
              cx={xOf(i)}
              cy={yOf(p.current)}
              r={4}
              fill={
                p.verdict === "regression"
                  ? "#dc2626"
                  : p.verdict === "improvement"
                  ? "#059669"
                  : "#171717"
              }
            >
              <title>{`recall@${p.k}: ${p.current.toFixed(3)}`}</title>
            </circle>
          ))}
          {recallPoints.map((p, i) => (
            <text
              key={p.k}
              x={xOf(i)}
              y={H - PAD.bottom + 14}
              fontSize={11}
              textAnchor="middle"
              fill="#171717"
            >
              k={p.k}
            </text>
          ))}
        </svg>
        {ndcg ? (
          <div className="text-xs text-neutral-600 mt-2">
            NDCG@10: <span className="font-medium tabular-nums">{ndcg.value.toFixed(3)}</span>
            {ndcg.baseline !== null ? (
              <span className="text-neutral-400">
                {" "}
                (baseline {ndcg.baseline.toFixed(3)})
              </span>
            ) : null}
          </div>
        ) : null}
      </div>
      <div>
        <h4 className="text-xs uppercase tracking-wider text-neutral-500 mb-2">
          Top-10 pathway share
        </h4>
        <PathwayDonut pathways={pathways} />
      </div>
    </div>
  );
}

function PathwayDonut({ pathways }: { pathways: { key: string; value: number }[] }) {
  const total = pathways.reduce((acc, p) => acc + p.value, 0) || 1;
  const R = 70;
  const cx = 110;
  const cy = 100;
  let cursor = 0;
  const colors: Record<string, string> = {
    a: "#1e40af",
    b: "#0f766e",
    c: "#a16207",
    f: "#a21caf",
  };
  return (
    <svg width={220} height={200} className="block">
      {pathways.map((p) => {
        const frac = p.value / total;
        const startAng = cursor * 2 * Math.PI - Math.PI / 2;
        const endAng = (cursor + frac) * 2 * Math.PI - Math.PI / 2;
        cursor += frac;
        const large = frac > 0.5 ? 1 : 0;
        const x1 = cx + R * Math.cos(startAng);
        const y1 = cy + R * Math.sin(startAng);
        const x2 = cx + R * Math.cos(endAng);
        const y2 = cy + R * Math.sin(endAng);
        const path = `M ${cx} ${cy} L ${x1.toFixed(2)} ${y1.toFixed(2)} A ${R} ${R} 0 ${large} 1 ${x2.toFixed(2)} ${y2.toFixed(2)} Z`;
        return (
          <g key={p.key}>
            <path d={path} fill={colors[p.key] ?? "#737373"}>
              <title>{`pathway ${p.key.toUpperCase()}: ${(frac * 100).toFixed(1)}%`}</title>
            </path>
          </g>
        );
      })}
      <circle cx={cx} cy={cy} r={R * 0.55} fill="white" />
      {pathways.map((p, i) => (
        <g key={p.key} transform={`translate(${cx + R + 16}, ${cy - R + i * 18})`}>
          <rect width={10} height={10} fill={colors[p.key] ?? "#737373"} />
          <text x={14} y={9} fontSize={11} fill="#171717">
            {p.key.toUpperCase()} {(p.value * 100).toFixed(0)}%
          </text>
        </g>
      ))}
    </svg>
  );
}

function SurrogateChart({ metrics }: { metrics: BenchMetric[] }) {
  const rows = metrics.filter((m) => m.metric.endsWith("_ms"));
  const maxV = Math.max(...rows.map((r) => r.value), 1);
  const W = 600;
  const rowH = 32;
  const H = rows.length * rowH + 20;
  const labelW = 130;
  const barMaxW = W - labelW - 60;

  return (
    <div>
      <div className="text-xs text-neutral-500 mb-2 italic">
        Labeled-set mode is disabled (bench/fixtures/labeled_retrieval.jsonl is
        empty). Showing surrogate per-pathway query timings.
      </div>
      <svg width={W} height={H} className="block">
        {rows.map((m, i) => {
          const w = (m.value / maxV) * barMaxW;
          const y = i * rowH + 8;
          const color =
            m.verdict === "regression"
              ? "#dc2626"
              : m.verdict === "improvement"
              ? "#059669"
              : "#171717";
          return (
            <g key={m.metric}>
              <text x={6} y={y + 14} fontSize={11} fill="#171717">
                {m.metric}
              </text>
              <rect
                x={labelW}
                y={y}
                width={Math.max(1, w)}
                height={20}
                fill={color}
              />
              <text
                x={labelW + w + 6}
                y={y + 14}
                fontSize={11}
                fill="#525252"
                className="tabular-nums"
              >
                {m.value.toFixed(2)} ms
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
