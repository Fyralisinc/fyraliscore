import { useCallback, useState } from "react";
import { useNavigate } from "react-router-dom";
import App from "@/App";
import {
  DEMO_LS_KEYS,
  clearDemoSession,
  endDemoSession,
  resetDemoSession,
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
