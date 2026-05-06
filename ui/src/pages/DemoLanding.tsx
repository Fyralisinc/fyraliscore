import { useCallback, useEffect, useState } from "react";
import App from "@/App";
import {
  DEMO_LS_KEYS,
  clearDemoSession,
  endDemoSession,
  listDemoCompanies,
  resetDemoSession,
  saveDemoSession,
  startDemoSession,
  type DemoCompany,
} from "@/api/demo-picker-client";

// Root route. With no active demo session, render the start-demo
// picker; otherwise render the cockpit with the reset/end controls.
// /debug and direct API consumers are unaffected.
export default function DemoLanding() {
  const [sessionId, setSessionId] = useState<string | null>(() =>
    typeof window !== "undefined"
      ? localStorage.getItem(DEMO_LS_KEYS.sessionId)
      : null
  );
  const [busy, setBusy] = useState<"reset" | "end" | null>(null);
  const [resetMsg, setResetMsg] = useState<string | null>(null);

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
      // proceed even if the call fails — wipe local state and bounce
      // back to the picker
    }
    clearDemoSession();
    setSessionId(null);
    setBusy(null);
  }, [sessionId, busy]);

  if (!sessionId) {
    return <DemoPicker onSessionStarted={(sid) => setSessionId(sid)} />;
  }

  return (
    <>
      <App />
      <div className="demo-session-bar" role="status">
        <div className="demo-session-actions">
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

function DemoPicker({
  onSessionStarted,
}: {
  onSessionStarted: (sessionId: string) => void;
}) {
  const [companies, setCompanies] = useState<DemoCompany[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [startingId, setStartingId] = useState<string | null>(null);
  const [startError, setStartError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const items = await listDemoCompanies();
        if (!alive) return;
        setCompanies(items);
      } catch (err) {
        if (!alive) return;
        setLoadError(err instanceof Error ? err.message : "load failed");
      }
    })();
    return () => {
      alive = false;
    };
  }, []);

  async function onStart(companyId: string): Promise<void> {
    setStartingId(companyId);
    setStartError(null);
    try {
      const session = await startDemoSession(companyId);
      saveDemoSession(session);
      onSessionStarted(session.session_id);
    } catch (err) {
      setStartingId(null);
      setStartError(err instanceof Error ? err.message : "start failed");
    }
  }

  if (startingId) {
    return (
      <div className="demo-picker-shell">
        <div className="demo-picker-loading">
          <div className="demo-picker-loading-pulse" aria-hidden />
          <h1 className="demo-picker-loading-title">
            Setting up your demo environment…
          </h1>
          <p className="demo-picker-loading-body">
            Loading the company snapshot. This usually takes 5 to 15 seconds.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="demo-picker-shell">
      <header className="demo-picker-head">
        <div className="demo-picker-mark" aria-hidden>D</div>
        <h1 className="demo-picker-title">Fyralis demo</h1>
        <p className="demo-picker-subtitle">
          Start the demo to land in the action list as the CEO.
        </p>
      </header>

      {loadError ? (
        <div className="demo-picker-error">
          Could not load demo companies — {loadError}
        </div>
      ) : null}

      {startError ? (
        <div className="demo-picker-error">
          Could not start the demo — {startError}
        </div>
      ) : null}

      <div className="demo-picker-grid">
        {companies === null && !loadError ? (
          <div className="demo-picker-skeleton" aria-busy="true">
            Loading companies…
          </div>
        ) : null}

        {companies?.map((c) => (
          <article key={c.company_id} className="demo-picker-card">
            <div>
              <div className="demo-picker-card-tagline">{c.tagline}</div>
              <h2 className="demo-picker-card-name">{c.name}</h2>
              <p className="demo-picker-card-desc">{c.description}</p>
            </div>
            <div className="demo-picker-card-cta-wrap">
              <button
                type="button"
                className="demo-picker-card-cta"
                onClick={() => void onStart(c.company_id)}
                data-testid={`start-${c.company_id}`}
              >
                Start demo
              </button>
              <p className="demo-picker-card-cta-hint">
                Simulated company based on common organizational patterns.
                You will land in the action list as the CEO.
              </p>
            </div>
          </article>
        ))}
      </div>
    </div>
  );
}
