import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useEffect, useState } from "react";
import { dget, type Stats } from "./api";
import "./debug.css";

export function DebugLayout() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const loc = useLocation();

  useEffect(() => {
    let alive = true;
    const load = () =>
      dget<Stats>("/stats")
        .then((s) => alive && setStats(s))
        .catch((e) => alive && setErr(String(e)));
    load();
    const h = window.setInterval(load, 10000);
    return () => {
      alive = false;
      window.clearInterval(h);
    };
  }, [loc.pathname]);

  return (
    <div className="debug-app">
      <header className="debug-header">
        <div className="debug-title">
          <strong>company os</strong> <span>· inspector</span>
        </div>
        <nav className="debug-nav">
          <NavLink to="/debug/signals" end>signals</NavLink>
          <NavLink to="/debug/think-runs" end>think runs</NavLink>
          <NavLink to="/debug/models" end>models</NavLink>
          <NavLink to="/debug/acts" end>acts</NavLink>
          <NavLink to="/debug/renders" end>renders</NavLink>
          <NavLink to="/debug/cache" end>cache</NavLink>
        </nav>
        <div className="debug-stats">
          {err ? (
            <span className="err">err: {err.slice(0, 80)}</span>
          ) : stats ? (
            <>
              <span>obs <b>{stats.stats.observations}</b></span>
              <span>active models <b>{stats.stats.active_models}</b></span>
              <span>commits <b>{stats.stats.commitments}</b></span>
              <span>runs <b>{stats.stats.think_runs}</b></span>
              <span>queue <b>{stats.stats.trigger_queue_depth}</b></span>
              <span>artifacts <b>{stats.stats.artifacts}</b></span>
              <a className="back" href="/">← ceo view</a>
            </>
          ) : (
            <span>loading…</span>
          )}
        </div>
      </header>
      <main className="debug-main">
        <Outlet />
      </main>
    </div>
  );
}
