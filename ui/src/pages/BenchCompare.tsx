import { useEffect, useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { getRun, listRuns, type BenchRunDetail, type BenchRunSummary } from "@/api/bench-client";

// /bench/compare?a=:runId&b=:runId — side-by-side compare of any two
// completed runs across every metric. Defaults to the latest two
// completed runs if no query params are provided.

export default function BenchCompare() {
  const [params, setParams] = useSearchParams();
  const [runs, setRuns] = useState<BenchRunSummary[]>([]);
  const [aDetail, setADetail] = useState<BenchRunDetail | null>(null);
  const [bDetail, setBDetail] = useState<BenchRunDetail | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    listRuns(50, ctrl.signal).then((rs) => {
      const completed = rs.filter((r) => r.status === "completed");
      setRuns(completed);
      // Auto-seed if missing.
      if (completed.length >= 2 && (!params.get("a") || !params.get("b"))) {
        const next = new URLSearchParams(params);
        if (!params.get("a")) next.set("a", completed[1].id);
        if (!params.get("b")) next.set("b", completed[0].id);
        setParams(next, { replace: true });
      }
    });
    return () => ctrl.abort();
  }, []);   // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    const a = params.get("a");
    const b = params.get("b");
    if (a) getRun(a).then(setADetail).catch(() => setADetail(null));
    if (b) getRun(b).then(setBDetail).catch(() => setBDetail(null));
  }, [params]);

  const merged = useMemo(() => {
    if (!aDetail || !bDetail) return [];
    const byKey: Map<string, { a?: number; b?: number; dimension: string; metric: string }> = new Map();
    for (const m of aDetail.metrics) {
      const k = `${m.dimension}.${m.metric}`;
      byKey.set(k, { dimension: m.dimension, metric: m.metric, a: m.value });
    }
    for (const m of bDetail.metrics) {
      const k = `${m.dimension}.${m.metric}`;
      const cur = byKey.get(k) ?? { dimension: m.dimension, metric: m.metric };
      cur.b = m.value;
      byKey.set(k, cur);
    }
    return Array.from(byKey.values()).sort((x, y) => {
      if (x.dimension !== y.dimension)
        return x.dimension.localeCompare(y.dimension);
      return x.metric.localeCompare(y.metric);
    });
  }, [aDetail, bDetail]);

  return (
    <div className="min-h-screen bg-neutral-50 text-neutral-900">
      <nav className="border-b border-neutral-200 bg-white">
        <div className="mx-auto max-w-7xl px-6 py-3 flex items-center gap-3">
          <Link to="/bench" className="font-semibold text-sm">
            ← Bench
          </Link>
          <span className="text-neutral-400">/</span>
          <span className="text-sm">Compare</span>
        </div>
      </nav>
      <main className="mx-auto max-w-7xl px-6 py-8">
        <h1 className="text-2xl font-semibold tracking-tight mb-6">
          Compare runs
        </h1>
        <div className="grid grid-cols-2 gap-4 mb-8">
          <RunPicker
            label="A"
            value={params.get("a") ?? ""}
            runs={runs}
            onChange={(v) => {
              const next = new URLSearchParams(params);
              next.set("a", v);
              setParams(next);
            }}
          />
          <RunPicker
            label="B"
            value={params.get("b") ?? ""}
            runs={runs}
            onChange={(v) => {
              const next = new URLSearchParams(params);
              next.set("b", v);
              setParams(next);
            }}
          />
        </div>
        {!aDetail || !bDetail ? (
          <div className="text-sm text-neutral-500">
            Select two completed runs above to compare.
          </div>
        ) : (
          <div className="overflow-hidden rounded-md border border-neutral-200 bg-white">
            <table className="min-w-full text-sm">
              <thead className="bg-neutral-50 text-neutral-600 text-xs uppercase tracking-wider">
                <tr>
                  <th className="text-left px-3 py-2 font-medium">Dimension</th>
                  <th className="text-left px-3 py-2 font-medium">Metric</th>
                  <th className="text-right px-3 py-2 font-medium">A</th>
                  <th className="text-right px-3 py-2 font-medium">B</th>
                  <th className="text-right px-3 py-2 font-medium">Δ abs</th>
                  <th className="text-right px-3 py-2 font-medium">Δ %</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-100">
                {merged.map((m) => {
                  const delta =
                    m.a !== undefined && m.b !== undefined ? m.b - m.a : null;
                  const pct =
                    delta !== null && m.a
                      ? delta / Math.abs(m.a as number)
                      : null;
                  return (
                    <tr key={`${m.dimension}.${m.metric}`}>
                      <td className="px-3 py-2 text-xs">{m.dimension}</td>
                      <td className="px-3 py-2 font-mono text-xs">{m.metric}</td>
                      <td className="px-3 py-2 text-right tabular-nums">
                        {m.a !== undefined ? m.a.toFixed(3) : "—"}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums font-medium">
                        {m.b !== undefined ? m.b.toFixed(3) : "—"}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-xs">
                        {delta !== null ? delta.toFixed(3) : "—"}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums text-xs">
                        {pct !== null
                          ? `${pct > 0 ? "+" : ""}${(pct * 100).toFixed(1)}%`
                          : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </main>
    </div>
  );
}

function RunPicker({
  label,
  value,
  runs,
  onChange,
}: {
  label: string;
  value: string;
  runs: BenchRunSummary[];
  onChange: (v: string) => void;
}) {
  return (
    <div>
      <div className="text-sm font-medium mb-1">{label}</div>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full rounded border border-neutral-300 px-3 py-1.5 text-sm bg-white"
      >
        <option value="">— pick a run —</option>
        {runs.map((r) => (
          <option key={r.id} value={r.id}>
            {r.git_branch} @ {r.git_sha.slice(0, 8)} ·{" "}
            {new Date(r.started_at).toLocaleDateString()} ·{" "}
            r{r.regressions}/i{r.improvements}
          </option>
        ))}
      </select>
    </div>
  );
}
