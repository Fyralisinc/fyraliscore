import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  cancelRun,
  getRun,
  saveBaseline,
  type BenchRunDetail,
  type RunStatus,
  type Verdict,
} from "@/api/bench-client";
import { useBenchRunStream } from "@/hooks/useBenchRunStream";
import { LatencyChart } from "@/components/bench/LatencyChart";
import { ThroughputChart } from "@/components/bench/ThroughputChart";
import { RetrievalQualityChart } from "@/components/bench/RetrievalQualityChart";
import { ReasoningQualityChart } from "@/components/bench/ReasoningQualityChart";
import { CostChart } from "@/components/bench/CostChart";

// /bench/runs/:runId — single-run page. Dual-mode:
//   while status is queued/running, polls for live progress (WebSocket
//   replaces this in the next build step) and shows the progress bar +
//   current stage. When complete, switches to the results view with
//   per-dimension metric tables.
//
// The page polls every 1s while in-flight, every 30s when terminal.
export default function BenchRun() {
  const { runId } = useParams<{ runId: string }>();
  const [data, setData] = useState<BenchRunDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [actionMsg, setActionMsg] = useState<string | null>(null);

  // Live stream — pushes progress as it happens. Replaces the polling
  // path while the run is in-flight. When the WS sends a terminal
  // frame we re-fetch via REST so the completed metrics + profiles
  // arrive in one round-trip.
  const live = useBenchRunStream(runId);

  const status: RunStatus | undefined =
    (live.status !== "unknown" ? (live.status as RunStatus) : undefined) ??
    data?.run.status;
  const inFlight = status === "queued" || status === "running";

  // Always fetch once on mount, and again whenever the WS signals
  // a terminal status so the completed metrics + profiles arrive.
  useEffect(() => {
    if (!runId) return;
    const ctrl = new AbortController();
    let cancelled = false;
    async function load() {
      try {
        const d = await getRun(runId!, ctrl.signal);
        if (!cancelled) {
          setData(d);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    }
    load();
    // Fallback poll every 30s in case the WS is unavailable.
    const id = setInterval(load, 30_000);
    return () => {
      cancelled = true;
      ctrl.abort();
      clearInterval(id);
    };
  }, [runId]);

  // Refetch on terminal so we get the final metrics + profiles.
  useEffect(() => {
    if (!runId) return;
    if (!live.terminal) return;
    const ctrl = new AbortController();
    getRun(runId, ctrl.signal).then(setData).catch(() => {
      /* benign */
    });
    return () => ctrl.abort();
  }, [runId, live.terminal]);

  if (!runId) return <div className="p-8">Missing run id.</div>;
  if (error)
    return (
      <div className="p-8">
        <Link to="/bench" className="text-sm underline">
          ← back
        </Link>
        <div className="mt-4 text-red-700">Failed to load: {error}</div>
      </div>
    );
  if (!data) return <div className="p-8 text-sm text-neutral-500">Loading…</div>;

  const { run, metrics, profiles } = data;
  const dimNames = Array.from(new Set(metrics.map((m) => m.dimension)));

  const onCancel = async () => {
    try {
      await cancelRun(runId);
      setActionMsg("Cancellation requested.");
    } catch (e) {
      setActionMsg(`Cancel failed: ${e instanceof Error ? e.message : e}`);
    }
  };

  const onSaveBaseline = async () => {
    try {
      const resp = await saveBaseline(runId);
      setActionMsg(`Baselines written: ${resp.files.join(", ")}`);
    } catch (e) {
      setActionMsg(
        `Save baseline failed: ${e instanceof Error ? e.message : e}`
      );
    }
  };

  return (
    <div className="min-h-screen bg-neutral-50 text-neutral-900">
      <nav className="border-b border-neutral-200 bg-white">
        <div className="mx-auto max-w-7xl px-6 py-3 flex items-center gap-3">
          <Link to="/bench" className="font-semibold text-sm">
            ← Bench
          </Link>
          <span className="text-neutral-400">/</span>
          <span className="text-sm font-mono">{runId.slice(0, 8)}…</span>
        </div>
      </nav>

      <main className="mx-auto max-w-7xl px-6 py-8">
        <header className="flex items-start justify-between mb-6">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight mb-1">
              Bench run
            </h1>
            <div className="text-xs text-neutral-600 font-mono">
              {run.git_branch} @ {run.git_sha.slice(0, 10)}
              {run.git_dirty ? " (dirty)" : ""}
            </div>
            {run.notes ? (
              <div className="mt-2 text-sm text-neutral-700 italic">
                "{run.notes}"
              </div>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            {inFlight ? (
              <button
                onClick={onCancel}
                className="rounded-md border border-red-300 bg-white px-4 py-2 text-sm font-medium text-red-700 hover:bg-red-50"
              >
                Cancel run
              </button>
            ) : null}
            {status === "completed" ? (
              <button
                onClick={onSaveBaseline}
                className="rounded-md border border-neutral-300 bg-white px-4 py-2 text-sm font-medium text-neutral-900 hover:bg-neutral-100"
              >
                Save as baseline
              </button>
            ) : null}
          </div>
        </header>

        {actionMsg ? (
          <div className="mb-6 rounded-md border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-900">
            {actionMsg}
          </div>
        ) : null}

        {inFlight ? (
          <LiveProgress
            run={run}
            liveStage={live.current_stage}
            livePct={live.progress_pct}
            connected={live.connected}
          />
        ) : (
          <CompletedHeader run={run} />
        )}

        {dimNames.length > 0 ? (
          <section className="mt-8 space-y-8">
            {dimNames.map((dim) => (
              <DimensionTable
                key={dim}
                dimension={dim}
                metrics={metrics.filter((m) => m.dimension === dim)}
              />
            ))}
          </section>
        ) : inFlight ? (
          <div className="mt-8 text-sm text-neutral-500">
            Metrics will populate as dimensions complete.
          </div>
        ) : null}

        {profiles.length > 0 ? (
          <section className="mt-10">
            <h2 className="text-sm font-medium text-neutral-700 mb-3">
              Profiles
            </h2>
            <div className="flex flex-wrap gap-3">
              {profiles.map((p) => (
                <Link
                  key={p.kind}
                  to={`/bench/runs/${runId}/profile/${p.kind}`}
                  className="rounded-md border border-neutral-200 bg-white px-4 py-3 text-sm hover:border-neutral-400"
                >
                  <div className="font-medium">{p.kind} profile</div>
                  <div className="text-xs text-neutral-500 font-mono mt-1">
                    {p.artifact_path}
                  </div>
                </Link>
              ))}
            </div>
          </section>
        ) : null}
      </main>
    </div>
  );
}

function LiveProgress({
  run,
  liveStage,
  livePct,
  connected,
}: {
  run: BenchRunDetail["run"];
  liveStage: string | null;
  livePct: number;
  connected: boolean;
}) {
  // Prefer live values from the WebSocket; fall back to the REST snapshot.
  const stage = liveStage ?? run.current_stage ?? "starting…";
  const pct = livePct || run.progress_pct;
  return (
    <section className="rounded-md border border-blue-200 bg-blue-50 px-5 py-4">
      <div className="flex items-center justify-between mb-2">
        <div className="text-xs font-medium uppercase tracking-wider text-blue-900 flex items-center gap-2">
          <span className="inline-block w-2 h-2 rounded-full bg-blue-600 animate-pulse" />
          {run.status}
          {!connected ? (
            <span className="text-amber-700 normal-case font-normal">
              · live disconnected (polling)
            </span>
          ) : null}
        </div>
        <div className="text-sm font-mono text-blue-900 tabular-nums">
          {pct}%
        </div>
      </div>
      <div className="text-sm font-medium text-blue-950 mb-2">{stage}</div>
      <div className="w-full bg-blue-200 rounded h-2 overflow-hidden">
        <div
          className="h-full bg-blue-600 transition-all duration-300"
          style={{ width: `${Math.max(2, pct)}%` }}
        />
      </div>
      <div className="mt-3 text-xs text-blue-900/80">
        Triggered by {run.triggered_by} · dimensions: {run.dimensions.join(", ")}
        {run.profile_kinds.length > 0
          ? ` · profiles: ${run.profile_kinds.join(", ")}`
          : ""}
      </div>
    </section>
  );
}

function CompletedHeader({ run }: { run: BenchRunDetail["run"] }) {
  const tone =
    run.status === "completed"
      ? run.regressions > 0
        ? "bad"
        : run.improvements > 0
        ? "good"
        : "neutral"
      : run.status === "failed"
      ? "bad"
      : "neutral";
  const toneCls = {
    good: "border-emerald-200 bg-emerald-50 text-emerald-900",
    bad: "border-red-200 bg-red-50 text-red-900",
    neutral: "border-neutral-200 bg-white text-neutral-900",
  }[tone];

  let elapsed = "";
  try {
    if (run.started_at && run.ended_at) {
      const ms =
        new Date(run.ended_at).getTime() - new Date(run.started_at).getTime();
      elapsed = `${(ms / 1000).toFixed(1)}s`;
    }
  } catch {
    /* ignore */
  }

  return (
    <section className={`rounded-md border px-5 py-4 ${toneCls}`}>
      <div className="flex items-center gap-6">
        <div>
          <div className="text-xs uppercase tracking-wider opacity-70">
            Status
          </div>
          <div className="text-lg font-semibold capitalize">{run.status}</div>
        </div>
        {elapsed ? (
          <div>
            <div className="text-xs uppercase tracking-wider opacity-70">
              Elapsed
            </div>
            <div className="text-lg font-semibold tabular-nums">{elapsed}</div>
          </div>
        ) : null}
        <div>
          <div className="text-xs uppercase tracking-wider opacity-70">
            Regressions
          </div>
          <div className="text-lg font-semibold tabular-nums">
            {run.regressions}
          </div>
        </div>
        <div>
          <div className="text-xs uppercase tracking-wider opacity-70">
            Improvements
          </div>
          <div className="text-lg font-semibold tabular-nums">
            {run.improvements}
          </div>
        </div>
      </div>
      {run.error ? (
        <div className="mt-3 text-sm font-mono">⚠ {run.error}</div>
      ) : null}
    </section>
  );
}

function DimensionTable({
  dimension,
  metrics,
}: {
  dimension: string;
  metrics: BenchRunDetail["metrics"];
}) {
  const regressed = metrics.filter((m) => m.verdict === "regression").length;
  const improved = metrics.filter((m) => m.verdict === "improvement").length;

  return (
    <div className="rounded-md border border-neutral-200 bg-white p-5">
      <div className="flex items-baseline justify-between mb-4">
        <h3 className="text-sm font-semibold text-neutral-900 capitalize">
          {dimension.replace(/_/g, " ")}
        </h3>
        <div className="text-xs text-neutral-600">
          {metrics.length} metrics
          {regressed > 0 ? (
            <span className="ml-2 text-red-700 font-medium">
              · {regressed} regression{regressed === 1 ? "" : "s"}
            </span>
          ) : null}
          {improved > 0 ? (
            <span className="ml-2 text-emerald-700">
              · {improved} improvement{improved === 1 ? "" : "s"}
            </span>
          ) : null}
        </div>
      </div>
      <div className="mb-2">
        <DimensionChart dimension={dimension} metrics={metrics} />
      </div>
      <details className="mt-4 group">
        <summary className="cursor-pointer text-xs text-neutral-500 hover:text-neutral-900 select-none">
          Show raw metric table ▾
        </summary>
        <div className="mt-3 overflow-hidden rounded-md border border-neutral-100">
          <table className="min-w-full text-sm">
            <thead className="bg-neutral-50 text-neutral-600 text-xs uppercase tracking-wider">
              <tr>
                <th className="text-left px-4 py-2 font-medium">Metric</th>
                <th className="text-right px-4 py-2 font-medium">Baseline</th>
                <th className="text-right px-4 py-2 font-medium">Current</th>
                <th className="text-right px-4 py-2 font-medium">Δ abs</th>
                <th className="text-right px-4 py-2 font-medium">Δ %</th>
                <th className="text-right px-4 py-2 font-medium">Threshold</th>
                <th className="text-center px-4 py-2 font-medium">Verdict</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-100">
              {metrics.map((m, i) => (
                <tr key={`${m.metric}-${i}`} className="hover:bg-neutral-50">
                  <td className="px-4 py-2 font-mono text-xs">{m.metric}</td>
                  <td className="px-4 py-2 text-right tabular-nums text-neutral-600">
                    {fmt(m.baseline)}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums font-medium">
                    {fmt(m.value)}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-xs">
                    {fmt(m.delta_abs)}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-xs">
                    {fmtPct(m.delta_pct)}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-xs text-neutral-500">
                    {fmt(m.threshold)}
                  </td>
                  <td className="px-4 py-2 text-center">
                    <VerdictChip v={m.verdict} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </div>
  );
}

function DimensionChart({
  dimension,
  metrics,
}: {
  dimension: string;
  metrics: BenchRunDetail["metrics"];
}) {
  if (metrics.length === 0)
    return (
      <div className="text-sm text-neutral-500 italic">
        No metrics returned for this dimension.
      </div>
    );
  switch (dimension) {
    case "latency":
      return <LatencyChart metrics={metrics} />;
    case "throughput":
      return <ThroughputChart metrics={metrics} />;
    case "retrieval_quality":
      return <RetrievalQualityChart metrics={metrics} />;
    case "reasoning_quality":
      return <ReasoningQualityChart metrics={metrics} />;
    case "cost":
      return <CostChart metrics={metrics} />;
    default:
      return (
        <div className="text-sm text-neutral-500 italic">
          No chart configured for "{dimension}".
        </div>
      );
  }
}

function VerdictChip({ v }: { v: Verdict }) {
  const cfg = {
    ok: { label: "✓ ok", cls: "bg-neutral-100 text-neutral-700" },
    regression: { label: "✗ regression", cls: "bg-red-100 text-red-800" },
    improvement: { label: "↑ better", cls: "bg-emerald-100 text-emerald-800" },
  }[v];
  return (
    <span
      className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${cfg.cls}`}
    >
      {cfg.label}
    </span>
  );
}

function fmt(v: number | null): string {
  if (v === null || v === undefined) return "—";
  const abs = Math.abs(v);
  if (abs >= 100) return v.toLocaleString(undefined, { maximumFractionDigits: 1 });
  if (abs >= 1) return v.toFixed(2);
  return v.toFixed(4);
}

function fmtPct(v: number | null): string {
  if (v === null || v === undefined) return "—";
  const sign = v > 0 ? "+" : "";
  return `${sign}${(v * 100).toFixed(1)}%`;
}
