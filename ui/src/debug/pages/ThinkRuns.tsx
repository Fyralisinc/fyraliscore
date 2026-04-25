import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { dget, type ThinkRun, type Artifact } from "../api";
import { JsonView } from "../components/JsonView";
import { Loading, Empty, ErrorBox } from "../components/Loading";

const STAGE_LABELS: Record<string, string> = {
  trigger: "1. trigger",
  retrieval: "2. retrieval",
  response: "3. llm response",
  validation: "4. validation",
  apply: "5. apply",
  post_commit: "6. post-commit",
  cascade: "7. cascade",
  error: "! error",
};

export function ThinkRunsList() {
  const [rows, setRows] = useState<ThinkRun[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [status, setStatus] = useState("");
  const [triggerKind, setTriggerKind] = useState("");
  const [limit, setLimit] = useState(50);

  useEffect(() => {
    let alive = true;
    setRows(null);
    setErr(null);
    dget<{ runs: ThinkRun[] }>("/think-runs", { limit, status, trigger_kind: triggerKind })
      .then((d) => alive && setRows(d.runs))
      .catch((e) => alive && setErr(String(e)));
    return () => {
      alive = false;
    };
  }, [status, triggerKind, limit]);

  return (
    <div>
      <div className="debug-filters">
        <label>
          status
          <select value={status} onChange={(e) => setStatus(e.target.value)}>
            <option value="">all</option>
            <option value="success">success</option>
            <option value="failed">failed</option>
            <option value="skipped_idempotent">skipped</option>
          </select>
        </label>
        <label>
          trigger_kind
          <input
            value={triggerKind}
            placeholder="T1:event_arrival"
            onChange={(e) => setTriggerKind(e.target.value)}
          />
        </label>
        <label>
          limit
          <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={200}>200</option>
          </select>
        </label>
      </div>
      {err ? <ErrorBox message={err} /> : null}
      {rows === null ? (
        <Loading />
      ) : rows.length === 0 ? (
        <Empty what="no runs" />
      ) : (
        <table className="debug-table">
          <thead>
            <tr>
              <th>id</th>
              <th>started</th>
              <th>trigger_kind</th>
              <th>status</th>
              <th>llm_ms</th>
              <th>retr_m / o</th>
              <th>ops_applied</th>
              <th>cascade</th>
              <th>err</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const ops = r.ops_applied as Record<string, unknown[]> | null;
              const opsSummary = ops
                ? ["claim_ops", "act_ops", "resource_ops"]
                    .map((k) => `${k.replace("_ops", "")}=${Array.isArray(ops[k]) ? (ops[k] as unknown[]).length : 0}`)
                    .join(" ")
                : "—";
              return (
                <tr key={r.id} onClick={() => (window.location.href = `/debug/think-runs/${r.id}`)}>
                  <td>
                    <Link to={`/debug/think-runs/${r.id}`} className="mono">
                      {r.id.slice(0, 8)}
                    </Link>
                  </td>
                  <td className="muted">{new Date(r.started_at).toLocaleString()}</td>
                  <td>{r.trigger_kind}</td>
                  <td>
                    <span className={`pill ${r.status === "success" ? "ok" : r.status === "failed" ? "err" : ""}`}>
                      {r.status}
                    </span>
                  </td>
                  <td>{r.llm_latency_ms ?? "—"}</td>
                  <td className="muted">
                    {r.retrieval_model_count ?? 0} / {r.retrieval_observation_count ?? 0}
                  </td>
                  <td className="muted">{opsSummary}</td>
                  <td className="muted">{r.cascade_depth ?? 0}</td>
                  <td className="truncate muted">{r.error ?? ""}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

type RunDetailResp = {
  run: ThinkRun;
  trigger: { observation_id?: string; trigger_kind?: string } | null;
  observation: { id: string; source_channel: string; content_text: string; occurred_at: string } | null;
  artifacts: Artifact[];
};

export function ThinkRunDetail() {
  const { id } = useParams();
  const [data, setData] = useState<RunDetailResp | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setData(null);
    setErr(null);
    if (!id) return;
    dget<RunDetailResp>(`/think-runs/${id}`)
      .then((d) => alive && setData(d))
      .catch((e) => alive && setErr(String(e)));
    return () => {
      alive = false;
    };
  }, [id]);

  if (err) return <ErrorBox message={err} />;
  if (!data) return <Loading />;
  const r = data.run;

  return (
    <div>
      <Link to="/debug/think-runs" className="back">
        ← think runs
      </Link>
      <h1>
        <span className="mono">{r.id}</span>{" "}
        <span className={`pill ${r.status === "success" ? "ok" : r.status === "failed" ? "err" : ""}`}>{r.status}</span>
      </h1>
      <div className="detail">
        <div>
          <div className="card">
            <h2>run</h2>
            <dl className="kv">
              <dt>trigger_kind</dt>
              <dd>{r.trigger_kind}</dd>
              <dt>started_at</dt>
              <dd>{new Date(r.started_at).toLocaleString()}</dd>
              <dt>ended_at</dt>
              <dd>{r.ended_at ? new Date(r.ended_at).toLocaleString() : "—"}</dd>
              <dt>llm_latency_ms</dt>
              <dd>{r.llm_latency_ms ?? "—"}</dd>
              <dt>retrieval</dt>
              <dd>
                {r.retrieval_model_count ?? 0} models · {r.retrieval_observation_count ?? 0} observations
              </dd>
              <dt>cascade_depth</dt>
              <dd>{r.cascade_depth ?? 0}</dd>
              <dt>error</dt>
              <dd style={{ color: r.error ? "#ff7b72" : "inherit" }}>{r.error ?? "—"}</dd>
            </dl>
          </div>

          {data.observation ? (
            <div className="card">
              <h2>source signal</h2>
              <Link to={`/debug/signals/${data.observation.id}`} className="mono">
                {data.observation.id.slice(0, 8)}
              </Link>{" "}
              <span className="muted">· {data.observation.source_channel}</span>
              <div style={{ whiteSpace: "pre-wrap", marginTop: 6 }}>{data.observation.content_text}</div>
            </div>
          ) : null}

          <div className="card">
            <h2>ops summary</h2>
            <JsonView value={r.ops_applied} />
          </div>
        </div>

        <div>
          <h2>processing log · {data.artifacts.length} stages</h2>
          {data.artifacts.length === 0 ? (
            <div className="empty">
              no artifacts captured for this run. Set{" "}
              <span className="mono">DEBUG_ARTIFACT_CAPTURE=1</span> and re-run.
            </div>
          ) : (
            <div className="timeline">
              {data.artifacts.map((a) => (
                <div key={a.id} className="step">
                  <div className="step-header">
                    <span className="stage-badge">{STAGE_LABELS[a.stage] ?? a.stage}</span>
                    <span className="ts">{new Date(a.captured_at).toLocaleTimeString()}</span>
                  </div>
                  <div className="step-body">
                    <JsonView value={a.payload} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
