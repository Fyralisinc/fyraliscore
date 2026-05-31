import { useCallback, useEffect, useRef, useState } from "react";
import {
  backfill,
  DEFAULT_TENANT_ID,
  financeTenant,
  install,
  liveEmit,
  setFinanceTenant,
  status,
  type FinanceSource,
  type StatusResult,
} from "../finance-api";

// Finance ingestion testing console (Mercury + QuickBooks).
//
// Drives both sources end-to-end from the browser:
//   1. Install   — provision install + accounts/entities + webhook + flag.
//   2. Backfill  — synthesize historical records through the REAL handler.
//   3. Live      — emit a fresh HMAC-signed webhook event (single or auto-loop,
//                  so live traffic runs CONCURRENTLY with a backfill).
//   4. Status    — polled observation counts + the latest rows.

const SOURCES: { id: FinanceSource; label: string; tint: string }[] = [
  { id: "mercury", label: "Mercury · banking / cash", tint: "#3fb950" },
  { id: "quickbooks", label: "QuickBooks · accounting / AR-AP", tint: "#58a6ff" },
];

type LogLine = { ts: string; level: "ok" | "warm" | "error"; text: string };

function nowHHMMSS(): string {
  return new Date().toLocaleTimeString();
}

export function Finance() {
  const [tenant, setTenant] = useState<string>(() => financeTenant());
  const [count, setCount] = useState(5);
  const [busy, setBusy] = useState<string | null>(null);
  const [log, setLog] = useState<LogLine[]>([]);
  const [statuses, setStatuses] = useState<Record<string, StatusResult | null>>({
    mercury: null,
    quickbooks: null,
  });
  // Per-source live auto-loop state.
  const [looping, setLooping] = useState<Record<string, boolean>>({});
  const seqRef = useRef<Record<string, number>>({ mercury: 0, quickbooks: 0 });
  const loopTimers = useRef<Record<string, number | undefined>>({});

  const emit = useCallback((level: LogLine["level"], text: string) => {
    setLog((prev) => [{ ts: nowHHMMSS(), level, text }, ...prev].slice(0, 200));
  }, []);

  const refresh = useCallback(async (source: FinanceSource) => {
    try {
      const st = await status(source);
      setStatuses((p) => ({ ...p, [source]: st }));
    } catch (e) {
      emit("error", `${source} status: ${String(e)}`);
    }
  }, [emit]);

  // Poll both sources' status every 2s so backfill + live progress is visible.
  useEffect(() => {
    let alive = true;
    const tick = async () => {
      if (!alive) return;
      await Promise.all(SOURCES.map((s) => refresh(s.id)));
    };
    void tick();
    const h = window.setInterval(tick, 2000);
    return () => {
      alive = false;
      window.clearInterval(h);
    };
  }, [refresh, tenant]);

  const onSaveTenant = () => {
    setFinanceTenant(tenant.trim() || DEFAULT_TENANT_ID);
    emit("ok", `tenant set to ${tenant.trim() || DEFAULT_TENANT_ID}`);
    SOURCES.forEach((s) => void refresh(s.id));
  };

  const doInstall = async (source: FinanceSource) => {
    setBusy(`${source}:install`);
    try {
      const r = await install(source);
      emit("ok", `[${source}] installed — ${r.sub_resources} sub-resources, ` +
        `webhook ${r.webhook_secret_registered ? "registered" : "skipped"}`);
      await refresh(source);
    } catch (e) {
      emit("error", `[${source}] install failed: ${String(e)}`);
    } finally {
      setBusy(null);
    }
  };

  const doBackfill = async (source: FinanceSource) => {
    setBusy(`${source}:backfill`);
    try {
      const seed = Math.floor(Math.random() * 100000);
      const r = await backfill(source, count, seed);
      emit("ok", `[${source}] backfill — ${r.ingested} ingested, ${r.deduped} deduped ` +
        `(${r.records} records)`);
      await refresh(source);
    } catch (e) {
      emit("error", `[${source}] backfill failed: ${String(e)}`);
    } finally {
      setBusy(null);
    }
  };

  const doLiveOnce = async (source: FinanceSource) => {
    try {
      const seq = (seqRef.current[source] = (seqRef.current[source] ?? 0) + 1);
      const r = await liveEmit(source, seq);
      emit(r.delivered_via === "webhook" ? "ok" : "warm",
        `[${source}] live ${r.payload_kind} via ${r.delivered_via}` +
        (r.webhook_status ? ` (HTTP ${r.webhook_status})` : ""));
      await refresh(source);
    } catch (e) {
      emit("error", `[${source}] live emit failed: ${String(e)}`);
    }
  };

  const toggleLoop = (source: FinanceSource) => {
    setLooping((prev) => {
      const next = !prev[source];
      if (next) {
        emit("ok", `[${source}] live auto-loop STARTED (every 1.5s)`);
        const run = async () => {
          await doLiveOnce(source);
          loopTimers.current[source] = window.setTimeout(run, 1500);
        };
        void run();
      } else {
        emit("warm", `[${source}] live auto-loop stopped`);
        const t = loopTimers.current[source];
        if (t) window.clearTimeout(t);
        loopTimers.current[source] = undefined;
      }
      return { ...prev, [source]: next };
    });
  };

  // Clean up loop timers on unmount.
  useEffect(() => () => {
    Object.values(loopTimers.current).forEach((t) => t && window.clearTimeout(t));
  }, []);

  return (
    <div className="finance-console">
      <div className="card">
        <h2>finance ingestion console — Mercury + QuickBooks</h2>
        <div className="kv" style={{ marginBottom: 8, alignItems: "center", flexWrap: "wrap" }}>
          <span className="muted">tenant</span>
          <input
            value={tenant}
            onChange={(e) => setTenant(e.target.value)}
            style={{
              width: 340, background: "#0d1117", color: "#c9d1d9",
              border: "1px solid #30363d", borderRadius: 6, padding: "4px 8px",
              fontFamily: "ui-monospace, monospace", fontSize: 12,
            }}
          />
          <button onClick={onSaveTenant}>set tenant</button>
          <span className="muted">records/backfill</span>
          <input
            type="number" min={1} max={50} value={count}
            onChange={(e) => setCount(Math.max(1, Math.min(50, Number(e.target.value) || 1)))}
            style={{
              width: 64, background: "#0d1117", color: "#c9d1d9",
              border: "1px solid #30363d", borderRadius: 6, padding: "4px 8px",
            }}
          />
        </div>
        <div className="note">
          Backfill ingests synthetic historical records through the real
          per-source handler. Live emit signs a webhook and posts it to the
          gateway edge. Start a backfill, then turn on live auto-loop to run
          both concurrently — the status panels update live.
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        {SOURCES.map((s) => {
          const st = statuses[s.id];
          const isLooping = !!looping[s.id];
          return (
            <div className="card" key={s.id} style={{ borderTop: `2px solid ${s.tint}` }}>
              <h2 style={{ color: s.tint }}>{s.label}</h2>

              <div className="debug-filters">
                <button
                  onClick={() => doInstall(s.id)}
                  disabled={busy === `${s.id}:install`}
                >
                  {busy === `${s.id}:install` ? "installing…" : "1 · install"}
                </button>
                <button
                  onClick={() => doBackfill(s.id)}
                  disabled={busy === `${s.id}:backfill` || !st?.installed}
                >
                  {busy === `${s.id}:backfill` ? "backfilling…" : "2 · start backfill"}
                </button>
                <button onClick={() => doLiveOnce(s.id)} disabled={!st?.installed}>
                  3 · emit 1 live
                </button>
                <button
                  onClick={() => toggleLoop(s.id)}
                  disabled={!st?.installed}
                  style={isLooping ? { background: "#1f6feb", color: "#fff", borderColor: "#1f6feb" } : undefined}
                >
                  {isLooping ? "■ stop live loop" : "▶ live auto-loop"}
                </button>
              </div>

              <div className="kv" style={{ gap: 18, marginBottom: 10 }}>
                <span>
                  installed{" "}
                  <b className={st?.installed ? "ok" : "muted"}>
                    {st?.installed ? "yes" : "no"}
                  </b>
                </span>
                <span>channel <span className="mono">{st?.channel ?? `${s.id}:…`}</span></span>
                <span>total <b>{st?.counts.total ?? 0}</b></span>
                <span>signal <b className="ok">{st?.counts.signal ?? 0}</b></span>
                <span>state_change <b className="warm">{st?.counts.state_change ?? 0}</b></span>
              </div>

              {st?.sub_resources && st.sub_resources.length > 0 ? (
                <div className="kv" style={{ flexWrap: "wrap", gap: 6, marginBottom: 10 }}>
                  {st.sub_resources.map((sub, i) => (
                    <span className="pill" key={i}>
                      {String(sub.account_id ?? sub.entity_type ?? "")}
                      {sub.account_name ? ` · ${sub.account_name}` : ""}
                    </span>
                  ))}
                </div>
              ) : null}

              <div className="scroll-x">
                <table className="debug-table">
                  <thead>
                    <tr>
                      <th>kind</th>
                      <th>external_id</th>
                      <th>summary</th>
                      <th>ingested</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(st?.recent ?? []).map((o) => (
                      <tr key={o.id}>
                        <td>
                          <span className={o.kind === "state_change" ? "warm" : "ok"}>
                            {o.kind}
                          </span>
                        </td>
                        <td className="mono truncate" title={o.external_id}>
                          {o.external_id}
                        </td>
                        <td className="truncate" title={o.content_text}>
                          {o.content_text}
                        </td>
                        <td className="muted">
                          {o.ingested_at ? new Date(o.ingested_at).toLocaleTimeString() : "—"}
                        </td>
                      </tr>
                    ))}
                    {(!st || st.recent.length === 0) ? (
                      <tr><td colSpan={4} className="empty">no observations yet</td></tr>
                    ) : null}
                  </tbody>
                </table>
              </div>
            </div>
          );
        })}
      </div>

      <div className="card" style={{ marginTop: 16 }}>
        <h2>activity log</h2>
        <div className="timeline" style={{ maxHeight: 260, overflowY: "auto" }}>
          {log.length === 0 ? (
            <div className="empty">no activity yet — install a source to begin</div>
          ) : (
            log.map((l, i) => (
              <div className="step" key={i}>
                <span className="ts">{l.ts}</span>{" "}
                <span className={l.level}>{l.text}</span>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
