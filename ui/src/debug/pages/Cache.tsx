import { useEffect, useState } from "react";
import { dget, type CacheRow } from "../api";
import { JsonView } from "../components/JsonView";
import { Loading, Empty, ErrorBox } from "../components/Loading";

export function Cache() {
  const [rows, setRows] = useState<CacheRow[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const load = () => {
    setRows(null);
    setErr(null);
    dget<{ cache: CacheRow[] }>("/cache")
      .then((d) => setRows(d.cache))
      .catch((e) => setErr(String(e)));
  };
  useEffect(load, []);

  const forceRefresh = async () => {
    setRefreshing(true);
    try {
      const res = await fetch("/api/view/ceo/force-refresh", { method: "POST" });
      if (!res.ok) throw new Error(`${res.status}`);
      load();
    } catch (e) {
      setErr(String(e));
    } finally {
      setRefreshing(false);
    }
  };

  return (
    <div>
      <div className="debug-filters">
        <button onClick={load}>reload</button>
        <button onClick={forceRefresh} disabled={refreshing}>
          {refreshing ? "refreshing…" : "force-refresh cache"}
        </button>
      </div>
      {err ? <ErrorBox message={err} /> : null}
      {rows === null ? (
        <Loading />
      ) : rows.length === 0 ? (
        <Empty what="no cache rows" />
      ) : (
        <div style={{ display: "grid", gridTemplateColumns: "360px 1fr", gap: 16 }}>
          <table className="debug-table">
            <thead>
              <tr>
                <th>key</th>
                <th>age</th>
                <th>cached_at</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.cache_key} onClick={() => setSelected(r.cache_key)}>
                  <td>{r.cache_key}</td>
                  <td className="mono">{r.age_seconds}s</td>
                  <td className="muted">{new Date(r.cached_at).toLocaleTimeString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {selected ? (
            <div className="card">
              <h2>payload — {selected}</h2>
              <JsonView value={rows.find((r) => r.cache_key === selected)?.payload} />
            </div>
          ) : (
            <div className="card">
              <div className="muted">pick a row</div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
