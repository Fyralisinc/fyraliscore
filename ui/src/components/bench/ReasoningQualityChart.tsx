import type { BenchMetric } from "@/api/bench-client";

// ReasoningQualityChart — a calibration reliability diagram-style view.
// Headline numbers (ECE / pass rate) sit above; below them, a stacked
// view showing each metric's current value vs baseline as a horizontal
// bar with a y=x reference line for the perfect-calibration ideal.

export function ReasoningQualityChart({ metrics }: { metrics: BenchMetric[] }) {
  if (metrics.length === 0)
    return <div className="text-sm text-neutral-500">No reasoning data.</div>;

  const ece = metrics.find((m) => m.metric === "ece");
  const pr = metrics.find((m) => m.metric === "pass_rate");
  const scenarios = metrics.find((m) => m.metric === "scenarios_labeled");

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-3 gap-4">
        <StatCard
          label="Expected Calibration Error"
          unit=""
          metric={ece}
          fmt={(v) => v.toFixed(4)}
          lowerIsBetter
        />
        <StatCard
          label="Pass rate"
          unit=""
          metric={pr}
          fmt={(v) => `${(v * 100).toFixed(1)}%`}
        />
        <StatCard
          label="Scenarios labeled"
          unit=""
          metric={scenarios}
          fmt={(v) => v.toFixed(0)}
        />
      </div>

      {ece ? <CalibrationDial value={ece.value} baseline={ece.baseline} /> : null}
    </div>
  );
}

function StatCard({
  label,
  metric,
  fmt,
  unit,
  lowerIsBetter,
}: {
  label: string;
  metric: BenchMetric | undefined;
  fmt: (v: number) => string;
  unit?: string;
  lowerIsBetter?: boolean;
}) {
  if (!metric)
    return (
      <div className="rounded-md border border-neutral-200 bg-white p-4">
        <div className="text-xs uppercase tracking-wider text-neutral-500">
          {label}
        </div>
        <div className="text-2xl font-semibold text-neutral-400 mt-2">—</div>
      </div>
    );
  const tone =
    metric.verdict === "regression"
      ? "border-red-200 bg-red-50"
      : metric.verdict === "improvement"
      ? "border-emerald-200 bg-emerald-50"
      : "border-neutral-200 bg-white";
  return (
    <div className={`rounded-md border p-4 ${tone}`}>
      <div className="text-xs uppercase tracking-wider text-neutral-500">
        {label}
      </div>
      <div className="text-2xl font-semibold tabular-nums mt-2">
        {fmt(metric.value)}
        {unit ? <span className="text-base text-neutral-500"> {unit}</span> : null}
      </div>
      {metric.baseline !== null ? (
        <div className="text-xs text-neutral-500 mt-1">
          baseline: {fmt(metric.baseline)}
          {metric.delta_pct !== null ? (
            <span className="ml-1">
              ({metric.delta_pct > 0 ? "+" : ""}
              {(metric.delta_pct * 100).toFixed(1)}%)
            </span>
          ) : null}
        </div>
      ) : null}
      {lowerIsBetter ? (
        <div className="text-[10px] uppercase tracking-wider text-neutral-400 mt-2">
          lower is better
        </div>
      ) : null}
    </div>
  );
}

// CalibrationDial — render ECE as a horizontal gauge with the "no
// regression" band shaded.
function CalibrationDial({
  value,
  baseline,
}: {
  value: number;
  baseline: number | null;
}) {
  const W = 600;
  const H = 80;
  const max = Math.max(0.5, value * 1.5, (baseline ?? 0) * 1.5);
  const xOf = (v: number) => 20 + (v / max) * (W - 40);

  return (
    <div className="rounded-md border border-neutral-200 bg-white p-4">
      <h4 className="text-xs uppercase tracking-wider text-neutral-500 mb-2">
        ECE position (lower = better calibrated)
      </h4>
      <svg width={W} height={H} className="block">
        <rect
          x={xOf(0)}
          y={28}
          width={xOf(0.05) - xOf(0)}
          height={20}
          fill="#dcfce7"
        />
        <rect
          x={xOf(0.05)}
          y={28}
          width={xOf(0.1) - xOf(0.05)}
          height={20}
          fill="#fef9c3"
        />
        <rect
          x={xOf(0.1)}
          y={28}
          width={Math.max(0, xOf(max) - xOf(0.1))}
          height={20}
          fill="#fecaca"
        />
        {[0, 0.05, 0.1, 0.2, 0.3].map((t) =>
          t <= max ? (
            <g key={t}>
              <line x1={xOf(t)} x2={xOf(t)} y1={28} y2={56} stroke="#737373" strokeWidth={0.5} />
              <text x={xOf(t)} y={66} fontSize={10} textAnchor="middle" fill="#525252">
                {t.toFixed(2)}
              </text>
            </g>
          ) : null
        )}
        {baseline !== null ? (
          <g>
            <line
              x1={xOf(baseline)}
              x2={xOf(baseline)}
              y1={20}
              y2={56}
              stroke="#737373"
              strokeWidth={2}
              strokeDasharray="3 2"
            />
            <text x={xOf(baseline)} y={16} fontSize={10} fill="#737373" textAnchor="middle">
              baseline
            </text>
          </g>
        ) : null}
        <polygon
          points={`${xOf(value)},20 ${xOf(value) - 6},10 ${xOf(value) + 6},10`}
          fill="#171717"
        />
        <line x1={xOf(value)} x2={xOf(value)} y1={20} y2={56} stroke="#171717" strokeWidth={2} />
        <text x={xOf(value)} y={8} fontSize={10} fill="#171717" textAnchor="middle" fontWeight={600}>
          {value.toFixed(4)}
        </text>
      </svg>
      <div className="text-xs text-neutral-500 mt-2">
        Green = well-calibrated (&lt;0.05) · Yellow = drifting · Red = miscalibrated (&gt;0.10).
      </div>
    </div>
  );
}
