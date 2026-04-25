import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { dget, type SignalDetail } from "../api";
import { JsonView } from "../components/JsonView";
import { Loading, ErrorBox } from "../components/Loading";

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

export function SignalDetailPage() {
  const { id } = useParams();
  const [data, setData] = useState<SignalDetail | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setData(null);
    setErr(null);
    if (!id) return;
    dget<SignalDetail>(`/signals/${id}`)
      .then((d) => alive && setData(d))
      .catch((e) => alive && setErr(String(e)));
    return () => {
      alive = false;
    };
  }, [id]);

  if (err) return <ErrorBox message={err} />;
  if (!data) return <Loading />;

  const obs = data.observation;
  return (
    <div>
      <Link to="/debug/signals" className="back">
        ← signals
      </Link>
      <h1>
        <span className="mono">{obs.id}</span>
      </h1>

      <div className="detail">
        <div>
          <div className="card">
            <h2>observation</h2>
            <dl className="kv">
              <dt>channel</dt>
              <dd>{obs.source_channel}</dd>
              <dt>occurred_at</dt>
              <dd>{new Date(obs.occurred_at).toLocaleString()}</dd>
              <dt>kind</dt>
              <dd>{obs.kind ?? "—"}</dd>
              <dt>actor_id</dt>
              <dd className="mono">{String((obs as Record<string, unknown>).actor_id ?? "—")}</dd>
            </dl>
          </div>

          <div className="card">
            <h2>content</h2>
            <div style={{ whiteSpace: "pre-wrap" }}>
              {obs.content_text ?? "—"}
            </div>
          </div>

          <div className="card">
            <h2>triggers ({data.triggers.length})</h2>
            {data.triggers.length === 0 ? (
              <div className="muted">no triggers enqueued</div>
            ) : (
              data.triggers.map((t) => (
                <div key={t.id} style={{ marginBottom: 6, fontSize: 12 }}>
                  <span className="mono">{t.id.slice(0, 8)}</span>{" "}
                  <span className="pill">{t.trigger_kind}</span>{" "}
                  <span className="muted">attempts={t.attempts}</span>
                </div>
              ))
            )}
          </div>

          <div className="card">
            <h2>models born from this signal ({data.models_born.length})</h2>
            {data.models_born.length === 0 ? (
              <div className="muted">none</div>
            ) : (
              data.models_born.map((m) => (
                <div key={m.id} style={{ marginBottom: 6 }}>
                  <Link to={`/debug/models/${m.id}`} className="mono">
                    {m.id.slice(0, 8)}
                  </Link>{" "}
                  <span className="pill">{m.proposition_kind}</span>{" "}
                  <span className="muted">conf={m.confidence}</span>
                </div>
              ))
            )}
          </div>
        </div>

        <div>
          <h2>processing log</h2>
          {data.runs.length === 0 ? (
            <div className="empty">no think runs (yet)</div>
          ) : null}
          {data.runs.map((r) => {
            const runArtifacts = data.artifacts.filter(
              (a) => String((a as unknown as { run_id?: string }).run_id ?? "") === r.id
            );
            return (
              <div key={r.id} className="card">
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 10 }}>
                  <div>
                    <Link to={`/debug/think-runs/${r.id}`} className="mono">
                      run {r.id.slice(0, 8)}
                    </Link>{" "}
                    <span className={`pill ${r.status === "success" ? "ok" : r.status === "failed" ? "err" : ""}`}>
                      {r.status}
                    </span>{" "}
                    <span className="muted">
                      {r.trigger_kind} · llm {r.llm_latency_ms ?? "—"}ms · cascade {r.cascade_depth ?? 0}
                    </span>
                  </div>
                  <div className="muted">{new Date(r.started_at).toLocaleString()}</div>
                </div>

                <div className="timeline">
                  {runArtifacts.length === 0 ? (
                    <div className="muted">
                      no artifacts captured. set DEBUG_ARTIFACT_CAPTURE=1 and re-run.
                    </div>
                  ) : (
                    runArtifacts.map((a) => (
                      <div key={a.id} className="step">
                        <div className="step-header">
                          <span className="stage-badge">{STAGE_LABELS[a.stage] ?? a.stage}</span>
                          <span className="ts">{new Date(a.captured_at).toLocaleTimeString()}</span>
                        </div>
                        <div className="step-body">
                          <JsonView value={a.payload} />
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
