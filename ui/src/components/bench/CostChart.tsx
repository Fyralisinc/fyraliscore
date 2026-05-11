import type { BenchMetric } from "@/api/bench-client";

// CostChart — three KPI cards (mean $/run, p95 input tokens, p95
// output tokens) plus a side-by-side bar chart of current vs baseline
// dollar and token usage.

export function CostChart({ metrics }: { metrics: BenchMetric[] }) {
  const mean = metrics.find((m) => m.metric === "mean_usd_per_run");
  const inTok = metrics.find((m) => m.metric === "p95_input_tokens");
  const outTok = metrics.find((m) => m.metric === "p95_output_tokens");
  const calls = metrics.find((m) => m.metric === "mean_llm_calls");
  const observed = metrics.find((m) => m.metric === "total_runs_observed");

  const empty = (observed?.value ?? 0) === 0;

  return (
    <div className="space-y-6">
      {empty ? (
        <div className="rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
          <strong>think_run_costs is empty.</strong> Cost metrics will populate
          once real Think runs execute against this DB. The bench wiring is
          working — every Think run writes a row via
          <code> services/think/observability.py:record_think_run_cost</code>.
        </div>
      ) : null}

      <div className="grid grid-cols-4 gap-3">
        <KPI label="$ / Think run" metric={mean} fmt={(v) => `$${v.toFixed(4)}`} />
        <KPI
          label="p95 input tokens"
          metric={inTok}
          fmt={(v) => v.toFixed(0)}
        />
        <KPI
          label="p95 output tokens"
          metric={outTok}
          fmt={(v) => v.toFixed(0)}
        />
        <KPI
          label="Mean LLM calls"
          metric={calls}
          fmt={(v) => v.toFixed(2)}
        />
      </div>

      {!empty ? <TokensVsCostChart inTok={inTok} outTok={outTok} mean={mean} /> : null}
    </div>
  );
}

function KPI({
  label,
  metric,
  fmt,
}: {
  label: string;
  metric: BenchMetric | undefined;
  fmt: (v: number) => string;
}) {
  const tone =
    metric?.verdict === "regression"
      ? "border-red-200 bg-red-50"
      : metric?.verdict === "improvement"
      ? "border-emerald-200 bg-emerald-50"
      : "border-neutral-200 bg-white";
  return (
    <div className={`rounded-md border p-4 ${tone}`}>
      <div className="text-xs uppercase tracking-wider text-neutral-500">
        {label}
      </div>
      <div className="text-2xl font-semibold tabular-nums mt-1">
        {metric ? fmt(metric.value) : "—"}
      </div>
      {metric?.baseline !== null && metric?.baseline !== undefined ? (
        <div className="text-xs text-neutral-500 mt-1">
          baseline {fmt(metric.baseline)}
          {metric.delta_pct !== null ? (
            <span className="ml-1">
              {" ("}
              {metric.delta_pct > 0 ? "+" : ""}
              {(metric.delta_pct * 100).toFixed(1)}%{")"}
            </span>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function TokensVsCostChart({
  inTok,
  outTok,
  mean,
}: {
  inTok: BenchMetric | undefined;
  outTok: BenchMetric | undefined;
  mean: BenchMetric | undefined;
}) {
  const series = [
    { label: "input tokens", metric: inTok, color: "#0f766e" },
    { label: "output tokens", metric: outTok, color: "#1e40af" },
  ].filter((s) => s.metric);
  if (series.length === 0) return null;

  const W = 600;
  const rowH = 50;
  const H = series.length * rowH + 50;
  const labelW = 110;
  const barMaxW = W - labelW - 110;
  const maxVal = Math.max(
    ...series.flatMap((s) => [s.metric!.value, s.metric!.baseline ?? 0]),
    1
  );

  return (
    <div className="rounded-md border border-neutral-200 bg-white p-4">
      <h4 className="text-xs uppercase tracking-wider text-neutral-500 mb-3">
        Token usage (current vs baseline)
      </h4>
      <svg width={W} height={H} className="block">
        {series.map((s, i) => {
          const m = s.metric!;
          const baseW =
            m.baseline !== null ? (m.baseline / maxVal) * barMaxW : 0;
          const curW = (m.value / maxVal) * barMaxW;
          const y = i * rowH + 6;
          return (
            <g key={s.label}>
              <text x={6} y={y + 14} fontSize={11} fill="#171717">
                {s.label}
              </text>
              {m.baseline !== null ? (
                <rect
                  x={labelW}
                  y={y}
                  width={Math.max(1, baseW)}
                  height={14}
                  fill="#d4d4d4"
                />
              ) : null}
              <rect
                x={labelW}
                y={y + 18}
                width={Math.max(1, curW)}
                height={14}
                fill={s.color}
              />
              <text
                x={labelW + Math.max(curW, baseW) + 6}
                y={y + 28}
                fontSize={11}
                fill="#525252"
                className="tabular-nums"
              >
                {m.value.toFixed(0)}
                {m.baseline !== null
                  ? ` (was ${m.baseline.toFixed(0)})`
                  : ""}
              </text>
            </g>
          );
        })}
      </svg>
      {mean && mean.baseline !== null ? (
        <div className="text-xs text-neutral-600 mt-2">
          Mean $/run moved from{" "}
          <span className="tabular-nums">${mean.baseline.toFixed(4)}</span> →{" "}
          <span className="tabular-nums font-medium">
            ${mean.value.toFixed(4)}
          </span>
        </div>
      ) : null}
    </div>
  );
}
