import { useEffect, useState } from "react";
import { dget } from "../api";
import { Loading, Empty, ErrorBox } from "../components/Loading";
import { JsonView } from "../components/JsonView";

type ActKind = "commitment" | "goal" | "decision" | "resource";

export function Acts() {
  const [kind, setKind] = useState<ActKind>("commitment");
  const [rows, setRows] = useState<Record<string, unknown>[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [selected, setSelected] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    let alive = true;
    setRows(null);
    setErr(null);
    setSelected(null);
    dget<{ rows: Record<string, unknown>[] }>("/acts", { kind })
      .then((d) => alive && setRows(d.rows))
      .catch((e) => alive && setErr(String(e)));
    return () => {
      alive = false;
    };
  }, [kind]);

  const columns =
    kind === "commitment"
      ? ["id", "state", "owner_id", "description", "due_at", "created_at"]
      : kind === "goal"
      ? ["id", "state", "name", "target_date", "created_at"]
      : kind === "decision"
      ? ["id", "state", "question", "chosen_option", "created_at"]
      : ["id", "kind", "name", "status", "created_at"];

  return (
    <div>
      <div className="debug-filters">
        {(["commitment", "goal", "decision", "resource"] as ActKind[]).map((k) => (
          <button
            key={k}
            onClick={() => setKind(k)}
            style={kind === k ? { background: "#2a323d", color: "#e6edf3" } : undefined}
          >
            {k}s
          </button>
        ))}
      </div>
      {err ? <ErrorBox message={err} /> : null}
      {rows === null ? (
        <Loading />
      ) : rows.length === 0 ? (
        <Empty what={`no ${kind}s`} />
      ) : (
        <div style={{ display: "grid", gridTemplateColumns: selected ? "1fr 500px" : "1fr", gap: 16 }}>
          <table className="debug-table">
            <thead>
              <tr>
                {columns.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={i} onClick={() => setSelected(r)}>
                  {columns.map((c) => {
                    const v = r[c];
                    if (v === null || v === undefined) return <td key={c} className="muted">—</td>;
                    if (c === "id") return <td key={c} className="mono">{String(v).slice(0, 8)}</td>;
                    if (c.endsWith("_at") || c.endsWith("_date"))
                      return <td key={c} className="muted">{new Date(String(v)).toLocaleString()}</td>;
                    if (c === "state")
                      return (
                        <td key={c}>
                          <span className="pill">{String(v)}</span>
                        </td>
                      );
                    return <td key={c} className="truncate">{String(v)}</td>;
                  })}
                </tr>
              ))}
            </tbody>
          </table>
          {selected ? (
            <div>
              <div className="card">
                <h2>full row</h2>
                <JsonView value={selected} />
              </div>
              <button onClick={() => setSelected(null)}>close</button>
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}
