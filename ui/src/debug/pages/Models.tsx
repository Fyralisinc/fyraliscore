import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { dget, type ModelRow } from "../api";
import { JsonView } from "../components/JsonView";
import { Loading, Empty, ErrorBox } from "../components/Loading";

export function ModelsList() {
  const [rows, setRows] = useState<ModelRow[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [status, setStatus] = useState("active");
  const [kind, setKind] = useState("");
  const [minConf, setMinConf] = useState("");
  const [limit, setLimit] = useState(100);

  useEffect(() => {
    let alive = true;
    setRows(null);
    setErr(null);
    dget<{ models: ModelRow[] }>("/models", {
      limit,
      status,
      kind,
      min_confidence: minConf,
    })
      .then((d) => alive && setRows(d.models))
      .catch((e) => alive && setErr(String(e)));
    return () => {
      alive = false;
    };
  }, [status, kind, minConf, limit]);

  return (
    <div>
      <div className="debug-filters">
        <label>
          status
          <select value={status} onChange={(e) => setStatus(e.target.value)}>
            <option value="">all</option>
            <option value="active">active</option>
            <option value="archived">archived</option>
          </select>
        </label>
        <label>
          kind
          <select value={kind} onChange={(e) => setKind(e.target.value)}>
            <option value="">all</option>
            <option value="state">state</option>
            <option value="concern">concern</option>
            <option value="expectation">expectation</option>
            <option value="relationship">relationship</option>
            <option value="policy">policy</option>
          </select>
        </label>
        <label>
          min conf
          <input
            value={minConf}
            placeholder="0.7"
            onChange={(e) => setMinConf(e.target.value)}
            size={4}
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
        <Empty what="no models" />
      ) : (
        <table className="debug-table">
          <thead>
            <tr>
              <th>id</th>
              <th>kind</th>
              <th>status</th>
              <th>conf</th>
              <th>conf@assert</th>
              <th>±</th>
              <th>subject / assertion</th>
              <th>created</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((m) => {
              const p = m.proposition ?? {};
              const head =
                (p.subject as string | undefined) ??
                (p.about as string | undefined) ??
                (p.name as string | undefined) ??
                "";
              const body =
                (p.assertion as string | undefined) ??
                (p.nature as string | undefined) ??
                (p.description as string | undefined) ??
                "";
              return (
                <tr key={m.id} onClick={() => (window.location.href = `/debug/models/${m.id}`)}>
                  <td>
                    <Link to={`/debug/models/${m.id}`} className="mono">
                      {m.id.slice(0, 8)}
                    </Link>
                  </td>
                  <td>
                    <span className="pill">{m.proposition_kind}</span>
                  </td>
                  <td>
                    <span className={`pill ${m.status === "active" ? "ok" : ""}`}>{m.status}</span>
                  </td>
                  <td className="mono">{m.confidence?.toFixed?.(2) ?? m.confidence}</td>
                  <td className="muted">{m.confidence_at_assertion?.toFixed?.(2) ?? "—"}</td>
                  <td className="muted">
                    +{m.confirmed_count ?? 0} / -{m.contested_count ?? 0}
                  </td>
                  <td className="truncate">
                    <b>{head}</b>
                    {body ? <span className="muted"> · {body}</span> : null}
                  </td>
                  <td className="muted">{new Date(m.created_at).toLocaleString()}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

type ModelDetailResp = {
  model: ModelRow & {
    scope_actors?: string[];
    supporting_event_ids?: string[];
    supporting_model_ids?: string[];
    evidential_weight?: number;
    falsifier?: Record<string, unknown> | string;
  };
  status_notes: { id: string; note: string; authored_at: string; kind: string }[];
  supporting_events: { id: string; source_channel: string; kind: string; content_text: string; occurred_at: string }[];
  supporting_models: ModelRow[];
};

export function ModelDetail() {
  const { id } = useParams();
  const [data, setData] = useState<ModelDetailResp | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setData(null);
    setErr(null);
    if (!id) return;
    dget<ModelDetailResp>(`/models/${id}`)
      .then((d) => alive && setData(d))
      .catch((e) => alive && setErr(String(e)));
    return () => {
      alive = false;
    };
  }, [id]);

  if (err) return <ErrorBox message={err} />;
  if (!data) return <Loading />;
  const m = data.model;

  return (
    <div>
      <Link to="/debug/models" className="back">
        ← models
      </Link>
      <h1>
        <span className="mono">{m.id}</span>{" "}
        <span className="pill">{m.proposition_kind}</span>{" "}
        <span className={`pill ${m.status === "active" ? "ok" : ""}`}>{m.status}</span>
      </h1>
      <div className="detail">
        <div>
          <div className="card">
            <h2>core</h2>
            <dl className="kv">
              <dt>confidence</dt>
              <dd className="mono">{m.confidence}</dd>
              <dt>confidence@assert</dt>
              <dd className="mono">{m.confidence_at_assertion ?? "—"}</dd>
              <dt>confirmed / contested</dt>
              <dd>
                +{m.confirmed_count ?? 0} / −{m.contested_count ?? 0}
              </dd>
              <dt>evidential_weight</dt>
              <dd>{m.evidential_weight ?? "—"}</dd>
              <dt>last_confirmed_at</dt>
              <dd className="muted">
                {m.last_confirmed_at ? new Date(m.last_confirmed_at).toLocaleString() : "—"}
              </dd>
              <dt>born_from_event_id</dt>
              <dd>
                {m.born_from_event_id ? (
                  <Link to={`/debug/signals/${m.born_from_event_id}`} className="mono">
                    {m.born_from_event_id.slice(0, 8)}
                  </Link>
                ) : (
                  "—"
                )}
              </dd>
              <dt>created_at</dt>
              <dd className="muted">{new Date(m.created_at).toLocaleString()}</dd>
            </dl>
          </div>

          <div className="card">
            <h2>proposition</h2>
            <JsonView value={m.proposition} />
          </div>

          {m.falsifier ? (
            <div className="card">
              <h2>falsifier</h2>
              <JsonView value={m.falsifier} />
            </div>
          ) : null}

          <div className="card">
            <h2>status notes ({data.status_notes.length})</h2>
            {data.status_notes.length === 0 ? (
              <div className="muted">none</div>
            ) : (
              data.status_notes.map((n) => (
                <div key={n.id} style={{ marginBottom: 6 }}>
                  <span className="pill">{n.kind}</span>{" "}
                  <span className="muted">{new Date(n.authored_at).toLocaleString()}</span>
                  <div>{n.note}</div>
                </div>
              ))
            )}
          </div>
        </div>

        <div>
          <h2>supporting events ({data.supporting_events.length})</h2>
          {data.supporting_events.length === 0 ? (
            <div className="empty">none</div>
          ) : (
            <div className="card">
              {data.supporting_events.map((e) => (
                <div key={e.id} style={{ marginBottom: 10 }}>
                  <Link to={`/debug/signals/${e.id}`} className="mono">
                    {e.id.slice(0, 8)}
                  </Link>{" "}
                  <span className="muted">{e.source_channel}</span>{" "}
                  <span className="muted">{new Date(e.occurred_at).toLocaleString()}</span>
                  <div>{e.content_text}</div>
                </div>
              ))}
            </div>
          )}

          <h2>supporting models ({data.supporting_models.length})</h2>
          {data.supporting_models.length === 0 ? (
            <div className="empty">none</div>
          ) : (
            <div className="card">
              {data.supporting_models.map((sm) => (
                <div key={sm.id} style={{ marginBottom: 8 }}>
                  <Link to={`/debug/models/${sm.id}`} className="mono">
                    {sm.id.slice(0, 8)}
                  </Link>{" "}
                  <span className="pill">{sm.proposition_kind}</span>{" "}
                  <span className="muted">conf={sm.confidence}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
