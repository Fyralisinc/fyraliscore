import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  listDemoCompanies,
  startDemoSession,
  saveDemoSession,
  type DemoCompany,
} from "@/api/demo-picker-client";

// /demo — public landing. Lists the three preloaded companies and drops
// the visitor into the cockpit (/) as that company's CEO on Start.
export default function DemoPicker() {
  const navigate = useNavigate();
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
      navigate("/");
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
          Pick a company. You will land in their action list as the CEO.
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
            <div className="demo-picker-card-tagline">{c.tagline}</div>
            <h2 className="demo-picker-card-name">{c.name}</h2>
            <p className="demo-picker-card-desc">{c.description}</p>
            <button
              type="button"
              className="demo-picker-card-cta"
              onClick={() => void onStart(c.company_id)}
              data-testid={`start-${c.company_id}`}
            >
              Start demo
            </button>
          </article>
        ))}
      </div>

      <footer className="demo-picker-foot">
        These are simulated companies based on common organizational patterns.
        Pick one to explore Company OS as that company&rsquo;s CEO.
      </footer>
    </div>
  );
}
