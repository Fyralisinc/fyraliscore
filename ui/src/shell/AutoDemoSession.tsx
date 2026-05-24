import { useEffect, useState } from "react";
import {
  startDemoSession,
  saveDemoSession,
  DEMO_LS_KEYS,
} from "@/api/demo-picker-client";

const PELAGO = "pelago";

// Drops a Pelago demo session token in localStorage on first visit so
// the four primary pages render against a real tenant. No company
// picker — the founder always lands inside Pelago.
export function AutoDemoSession({ children }: { children: React.ReactNode }) {
  const [ready, setReady] = useState<boolean>(() =>
    Boolean(localStorage.getItem(DEMO_LS_KEYS.authToken))
  );
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (ready) return;
    let cancelled = false;
    (async () => {
      try {
        const s = await startDemoSession(PELAGO);
        if (cancelled) return;
        saveDemoSession(s);
        setReady(true);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to start session");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [ready]);

  if (error) {
    return (
      <div
        style={{
          minHeight: "100vh",
          display: "grid",
          placeItems: "center",
          background: "var(--bg-canvas)",
          color: "var(--text-primary)",
          fontFamily: "var(--font-sans-v2)",
          padding: "2rem",
          textAlign: "center",
        }}
      >
        <div>
          <div style={{ fontSize: "1.25rem", marginBottom: "0.5rem" }}>
            Could not start Pelago session
          </div>
          <div style={{ color: "var(--text-muted)", fontSize: "0.875rem" }}>{error}</div>
        </div>
      </div>
    );
  }

  if (!ready) {
    return (
      <div
        style={{
          minHeight: "100vh",
          display: "grid",
          placeItems: "center",
          background: "var(--bg-canvas)",
          color: "var(--text-muted)",
          fontFamily: "var(--font-sans-v2)",
          fontSize: "0.875rem",
        }}
      >
        Loading Pelago…
      </div>
    );
  }

  return <>{children}</>;
}
