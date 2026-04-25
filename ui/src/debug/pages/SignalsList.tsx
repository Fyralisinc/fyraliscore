import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { dget, type Signal } from "../api";
import { Loading, Empty, ErrorBox } from "../components/Loading";

export function SignalsList() {
  const [rows, setRows] = useState<Signal[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [channel, setChannel] = useState("");
  const [limit, setLimit] = useState(50);

  useEffect(() => {
    let alive = true;
    setRows(null);
    setErr(null);
    dget<{ signals: Signal[] }>("/signals", { limit, channel })
      .then((d) => alive && setRows(d.signals))
      .catch((e) => alive && setErr(String(e)));
    return () => {
      alive = false;
    };
  }, [channel, limit]);

  return (
    <div>
      <div className="debug-filters">
        <label>
          channel
          <input
            value={channel}
            placeholder="slack:eng"
            onChange={(e) => setChannel(e.target.value)}
          />
        </label>
        <label>
          limit
          <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={200}>200</option>
          </select>
        </label>
        <button onClick={() => setLimit((l) => l)}>refresh</button>
      </div>
      {err ? <ErrorBox message={err} /> : null}
      {rows === null ? (
        <Loading />
      ) : rows.length === 0 ? (
        <Empty what="no signals" />
      ) : (
        <table className="debug-table">
          <thead>
            <tr>
              <th>id</th>
              <th>occurred_at</th>
              <th>channel</th>
              <th>actor</th>
              <th>kind</th>
              <th>runs</th>
              <th>content</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((s) => (
              <tr key={s.id} onClick={() => (window.location.href = `/debug/signals/${s.id}`)}>
                <td>
                  <Link to={`/debug/signals/${s.id}`} className="mono">
                    {s.id.slice(0, 8)}
                  </Link>
                </td>
                <td className="muted">{new Date(s.occurred_at).toLocaleString()}</td>
                <td>{s.source_channel}</td>
                <td className="muted">{s.source_actor_ref ?? "—"}</td>
                <td className="muted">{s.kind ?? "—"}</td>
                <td>
                  <span className="pill">{s.run_count}</span>
                </td>
                <td className="truncate">{s.content_text ?? ""}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
