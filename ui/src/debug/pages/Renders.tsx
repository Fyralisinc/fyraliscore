import { useEffect, useState } from "react";
import { dget, type RenderRow } from "../api";
import { Loading, Empty, ErrorBox } from "../components/Loading";

type SummaryRow = { render_kind: string; count: number; total_usd: string; avg_ms: number };

export function Renders() {
  const [rows, setRows] = useState<RenderRow[] | null>(null);
  const [summary, setSummary] = useState<SummaryRow[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [kind, setKind] = useState("");

  useEffect(() => {
    let alive = true;
    setRows(null);
    setErr(null);
    dget<{ renders: RenderRow[]; summary: SummaryRow[] }>("/renders", { render_kind: kind, limit: 100 })
      .then((d) => {
        if (!alive) return;
        setRows(d.renders);
        setSummary(d.summary);
      })
      .catch((e) => alive && setErr(String(e)));
    return () => {
      alive = false;
    };
  }, [kind]);

  return (
    <div>
      <div className="debug-filters">
        <label>
          render_kind
          <select value={kind} onChange={(e) => setKind(e.target.value)}>
            <option value="">all</option>
            <option value="greeting">greeting</option>
            <option value="card_observation">card_observation</option>
            <option value="card_decision">card_decision</option>
            <option value="card_question">card_question</option>
            <option value="card_reasoning">card_reasoning</option>
            <option value="query_grid">query_grid</option>
            <option value="conversation_turn">conversation_turn</option>
            <option value="close_line">close_line</option>
          </select>
        </label>
      </div>

      {summary ? (
        <div className="card">
          <h2>summary</h2>
          <table className="debug-table">
            <thead>
              <tr>
                <th>render_kind</th>
                <th>count</th>
                <th>total $</th>
                <th>avg ms</th>
              </tr>
            </thead>
            <tbody>
              {summary.map((s) => (
                <tr key={s.render_kind}>
                  <td>{s.render_kind}</td>
                  <td>{s.count}</td>
                  <td className="mono">{s.total_usd}</td>
                  <td className="muted">{s.avg_ms}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}

      {err ? <ErrorBox message={err} /> : null}
      {rows === null ? (
        <Loading />
      ) : rows.length === 0 ? (
        <Empty what="no renders" />
      ) : (
        <table className="debug-table">
          <thead>
            <tr>
              <th>render_id</th>
              <th>time</th>
              <th>kind</th>
              <th>outcome</th>
              <th>model</th>
              <th>calls</th>
              <th>in / out tok</th>
              <th>$</th>
              <th>ms</th>
              <th>retries</th>
              <th>flagged</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.render_id}>
                <td className="mono">{r.render_id.slice(0, 8)}</td>
                <td className="muted">{new Date(r.computed_at).toLocaleString()}</td>
                <td>{r.render_kind}</td>
                <td>
                  <span className={`pill ${r.outcome === "success" ? "ok" : r.outcome.includes("flag") ? "warm" : "err"}`}>
                    {r.outcome}
                  </span>
                </td>
                <td className="muted">{r.model_name ?? "—"}</td>
                <td className="muted">{r.llm_calls_count}</td>
                <td className="muted">
                  {r.llm_input_tokens_total} / {r.llm_output_tokens_total}
                </td>
                <td className="mono">{r.llm_cost_usd}</td>
                <td className="muted">{r.latency_total_ms}</td>
                <td className="muted">{r.retry_count}</td>
                <td className="muted">{r.flagged ? "yes" : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
