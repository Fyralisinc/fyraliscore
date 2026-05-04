import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import App from "@/App";
import {
  DEMO_LS_KEYS,
  clearDemoSession,
  endDemoSession,
  getDemoSession,
  resetDemoSession,
  type DemoSessionInfo,
} from "@/api/demo-picker-client";

// Wraps the cockpit when a demo session is active. Renders <App /> for
// non-demo visitors so /debug and direct API consumers stay unaffected.
export default function DemoLanding() {
  const navigate = useNavigate();
  const [sessionId, setSessionId] = useState<string | null>(() =>
    typeof window !== "undefined"
      ? localStorage.getItem(DEMO_LS_KEYS.sessionId)
      : null
  );
  const [info, setInfo] = useState<DemoSessionInfo | null>(null);
  const [busy, setBusy] = useState<"reset" | "end" | null>(null);
  const [resetMsg, setResetMsg] = useState<string | null>(null);

  // Poll session info every 5s while mounted, so the bar tracks cost +
  // signal counts while the user works.
  useEffect(() => {
    if (!sessionId) return;
    let alive = true;
    let timer: number | null = null;
    async function tick() {
      try {
        const next = await getDemoSession(sessionId!);
        if (!alive) return;
        setInfo(next);
      } catch {
        // ignore — bar is best-effort
      }
      if (!alive) return;
      timer = window.setTimeout(tick, 5_000);
    }
    void tick();
    return () => {
      alive = false;
      if (timer !== null) window.clearTimeout(timer);
    };
  }, [sessionId]);

  const onReset = useCallback(async () => {
    if (!sessionId || busy) return;
    setBusy("reset");
    setResetMsg("Resetting demo… this takes a few seconds.");
    try {
      await resetDemoSession(sessionId);
      setResetMsg("Demo reset. Reloading…");
      window.setTimeout(() => window.location.reload(), 500);
    } catch (err) {
      setBusy(null);
      setResetMsg(
        err instanceof Error ? `Reset failed: ${err.message}` : "Reset failed."
      );
    }
  }, [sessionId, busy]);

  const onEnd = useCallback(async () => {
    if (!sessionId || busy) return;
    setBusy("end");
    try {
      await endDemoSession(sessionId);
    } catch {
      // proceed even if the call fails — wipe local and bounce to /demo
    }
    clearDemoSession();
    setSessionId(null);
    navigate("/demo");
  }, [sessionId, busy, navigate]);

  if (!sessionId) {
    navigate("/demo");
    return null;
  }

  const cost = info ? info.total_cost_usd.toFixed(2) : "0.00";
  const cap = info ? info.cost_cap_usd.toFixed(2) : "—";
  const signals = info ? info.signals_injected : 0;

  return (
    <>
      <App />
      <div className="demo-session-bar" role="status">
        <div className="demo-session-stat">
          <span className="demo-session-stat-label">Cost</span>
          <span className="demo-session-stat-value">
            ${cost} <span className="demo-session-stat-cap">/ ${cap}</span>
          </span>
        </div>
        <div className="demo-session-stat">
          <span className="demo-session-stat-label">Signals</span>
          <span className="demo-session-stat-value">{signals}</span>
        </div>
        <div className="demo-session-actions">
          <a
            className="demo-session-btn"
            href={`/simulation/slack_ui/?tenant_id=${sessionId ? localStorage.getItem("demoTenantId") ?? "" : ""}`}
            target="_blank"
            rel="noreferrer"
          >
            Slack sim
          </a>
          <button
            type="button"
            className="demo-session-btn"
            onClick={() => void onReset()}
            disabled={busy !== null}
          >
            {busy === "reset" ? "Resetting…" : "Reset"}
          </button>
          <button
            type="button"
            className="demo-session-btn demo-session-btn-end"
            onClick={() => void onEnd()}
            disabled={busy !== null}
          >
            {busy === "end" ? "Ending…" : "End demo"}
          </button>
        </div>
      </div>
      {resetMsg ? <div className="demo-session-toast">{resetMsg}</div> : null}
    </>
  );
}
