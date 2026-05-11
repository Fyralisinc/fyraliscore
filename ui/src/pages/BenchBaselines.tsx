import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { ALL_DIMENSIONS, type DimensionName } from "@/api/bench-client";
import { getAuthHeader } from "@/api/auth";
import { ApiError } from "@/api/client";

// /bench/baselines — view the current committed baseline JSON files.
// Read-only — to update baselines you complete a run and click
// "Save as baseline" on the run detail page. This view exists so a
// developer can quickly see what the regression check is comparing
// against without leaving the browser.

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

interface BaselineFile {
  dimension: DimensionName;
  metrics: Record<string, number>;
  run_id?: string;
  raw_json: string;
  error?: string;
}

async function fetchBaseline(d: DimensionName): Promise<BaselineFile> {
  // The gateway doesn't yet expose a baseline-read endpoint; we read
  // the file directly via Vite's static-file proxy at /bench/baselines/.
  // In production, the same path is served by FastAPI's StaticFiles
  // mount (added in the polish step).
  const url = `/bench/baselines/${d}.json`;
  try {
    const res = await fetch(url, { headers: { ...getAuthHeader() } });
    if (!res.ok) {
      throw new ApiError(`${res.status} ${res.statusText}`, res.status);
    }
    const text = await res.text();
    const json = JSON.parse(text) as { metrics: Record<string, number>; run_id?: string };
    return {
      dimension: d,
      metrics: json.metrics ?? {},
      run_id: json.run_id,
      raw_json: text,
    };
  } catch (e) {
    return {
      dimension: d,
      metrics: {},
      raw_json: "",
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

export default function BenchBaselines() {
  const [files, setFiles] = useState<BaselineFile[]>([]);

  useEffect(() => {
    Promise.all(ALL_DIMENSIONS.map(fetchBaseline)).then(setFiles);
  }, []);

  return (
    <div className="min-h-screen bg-neutral-50 text-neutral-900">
      <nav className="border-b border-neutral-200 bg-white">
        <div className="mx-auto max-w-7xl px-6 py-3 flex items-center gap-3">
          <Link to="/bench" className="font-semibold text-sm">
            ← Bench
          </Link>
          <span className="text-neutral-400">/</span>
          <span className="text-sm">Baselines</span>
        </div>
      </nav>
      <main className="mx-auto max-w-7xl px-6 py-8">
        <h1 className="text-2xl font-semibold tracking-tight mb-1">
          Committed baselines
        </h1>
        <p className="text-sm text-neutral-600 mb-8">
          The regression check compares each run's metrics against these
          values. To update, complete a run on the baseline branch and click{" "}
          <strong>Save as baseline</strong> on its detail page — that writes
          to <code>bench/baselines/*.json</code> for you to commit.
        </p>
        <div className="space-y-6">
          {files.map((f) => (
            <div
              key={f.dimension}
              className="rounded-md border border-neutral-200 bg-white p-4"
            >
              <div className="flex items-baseline justify-between mb-3">
                <h2 className="text-sm font-semibold capitalize">
                  {f.dimension.replace(/_/g, " ")}
                </h2>
                {f.run_id ? (
                  <Link
                    to={`/bench/runs/${f.run_id}`}
                    className="text-xs text-neutral-500 font-mono hover:underline"
                  >
                    sourced from {f.run_id.slice(0, 8)}…
                  </Link>
                ) : null}
              </div>
              {f.error ? (
                <div className="text-xs text-neutral-500">
                  No baseline committed yet for this dimension.
                </div>
              ) : Object.keys(f.metrics).length === 0 ? (
                <div className="text-xs text-neutral-500">Empty baseline.</div>
              ) : (
                <table className="min-w-full text-sm">
                  <tbody className="divide-y divide-neutral-100">
                    {Object.entries(f.metrics).map(([k, v]) => (
                      <tr key={k}>
                        <td className="py-1 font-mono text-xs">{k}</td>
                        <td className="py-1 text-right tabular-nums">
                          {v.toFixed(4)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          ))}
        </div>
      </main>
    </div>
  );
}
