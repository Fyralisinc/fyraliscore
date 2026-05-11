import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getTrends, type TrendPoint } from "@/api/bench-client";

// /bench/trends — sparkline view of every tracked metric across the
// last N runs. Helps spot slow drift that no single PR comparison
// would catch.
//
// Hardcodes a list of "headline" (dimension, metric) tuples for the
// initial view. A future enhancement can let the user pick which
// metrics to chart.

const HEADLINE_METRICS: { dimension: string; metric: string; label: string }[] = [
  { dimension: "latency", metric: "retrieve_p95", label: "Retrieve p95 (ms)" },
  { dimension: "latency", metric: "think_p95", label: "Think p95 (ms)" },
  { dimension: "latency", metric: "apply_p95", label: "Apply p95 (ms)" },
  { dimension: "throughput", metric: "saturation_concurrency", label: "Saturation concurrency" },
  { dimension: "retrieval_quality", metric: "recall_at_10", label: "Recall@10" },
  { dimension: "reasoning_quality", metric: "ece", label: "ECE" },
  { dimension: "reasoning_quality", metric: "pass_rate", label: "Pass rate" },
  { dimension: "cost", metric: "mean_usd_per_run", label: "$/Think run" },
];

export default function BenchTrends() {
  return (
    <div className="min-h-screen bg-neutral-50 text-neutral-900">
      <nav className="border-b border-neutral-200 bg-white">
        <div className="mx-auto max-w-7xl px-6 py-3 flex items-center gap-3">
          <Link to="/bench" className="font-semibold text-sm">
            ← Bench
          </Link>
          <span className="text-neutral-400">/</span>
          <span className="text-sm">Trends</span>
        </div>
      </nav>
      <main className="mx-auto max-w-7xl px-6 py-8">
        <h1 className="text-2xl font-semibold tracking-tight mb-1">
          Metric trends
        </h1>
        <p className="text-sm text-neutral-600 mb-8">
          Each metric across the last 50 completed runs. Use this to spot
          drift that no single PR comparison would catch.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {HEADLINE_METRICS.map((m) => (
            <TrendCard
              key={`${m.dimension}.${m.metric}`}
              dimension={m.dimension}
              metric={m.metric}
              label={m.label}
            />
          ))}
        </div>
      </main>
    </div>
  );
}

function TrendCard({
  dimension,
  metric,
  label,
}: {
  dimension: string;
  metric: string;
  label: string;
}) {
  const [points, setPoints] = useState<TrendPoint[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    getTrends(dimension, metric, 50, ctrl.signal)
      .then((p) => setPoints(p.reverse()))   // oldest → newest
      .catch((e) =>
        setError(e instanceof Error ? e.message : String(e))
      );
    return () => ctrl.abort();
  }, [dimension, metric]);

  return (
    <div className="rounded-md border border-neutral-200 bg-white p-4">
      <div className="text-sm font-medium mb-3">{label}</div>
      {error ? (
        <div className="text-xs text-red-700">{error}</div>
      ) : points.length === 0 ? (
        <div className="text-xs text-neutral-500">No data yet.</div>
      ) : (
        <Sparkline points={points} />
      )}
    </div>
  );
}

function Sparkline({ points }: { points: TrendPoint[] }) {
  const values = points.map((p) => p.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const w = 400;
  const h = 60;
  const path = points
    .map((p, i) => {
      const x = (i / Math.max(1, points.length - 1)) * w;
      const y = h - ((p.value - min) / span) * (h - 4) - 2;
      return `${i === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");

  return (
    <div>
      <svg width={w} height={h} className="block">
        <path d={path} fill="none" stroke="#171717" strokeWidth={1.5} />
        {points.map((p, i) => {
          const x = (i / Math.max(1, points.length - 1)) * w;
          const y = h - ((p.value - min) / span) * (h - 4) - 2;
          const fill =
            p.verdict === "regression"
              ? "#dc2626"
              : p.verdict === "improvement"
              ? "#059669"
              : "#737373";
          return <circle key={i} cx={x} cy={y} r={2.5} fill={fill} />;
        })}
      </svg>
      <div className="text-xs text-neutral-500 mt-2 flex justify-between tabular-nums">
        <span>min {min.toFixed(2)}</span>
        <span>
          latest {points[points.length - 1].value.toFixed(2)}
        </span>
        <span>max {max.toFixed(2)}</span>
      </div>
    </div>
  );
}
