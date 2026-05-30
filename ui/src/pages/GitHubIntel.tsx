// GitHub Intelligence panel — a browser-testable view over the /github-intel/*
// read API. Self-contained single route (/github) with internal view state:
//   repos list -> repo (tabs: state | signals | blast radius | code search)
//   -> per-signal "explain" panel (cause/effect/why + blast radius).
//
// Auth: the gateway derives the tenant from the Bearer token. Demo sessions use
// a fresh per-session tenant, so GitHub-intel data seeded under the dogfood
// tenant is reached by pasting that tenant's token here (see the token bar +
// scripts/github_intel_dev_session.py). Pasting a token also clears any stale
// demoTenantId so authHeaders() doesn't send a mismatched X-Tenant-Id (the
// gateway 403s on mismatch).
import { useCallback, useEffect, useState } from "react";

import { ApiError } from "../api/client";
import { getDemoAuthToken, setDemoAuthToken } from "../api/auth";
import {
  listRepos,
  getRepoState,
  getSignals,
  explainSignal,
  getBlastRadius,
  codeSearch,
  type RepoSummary,
  type RepoStateResponse,
  type SignalItem,
  type ExplainResponse,
  type BlastRadiusResponse,
  type CodeSearchResponse,
} from "../api/github-intel-client";

type RepoTab = "state" | "signals" | "blast" | "search";

// ---- small presentational helpers ---------------------------------------
function Pill({ value, tone }: { value: string | null | undefined; tone?: string }) {
  if (!value) return <span className="gi-pill gi-pill--muted">—</span>;
  return <span className={`gi-pill${tone ? ` gi-pill--${tone}` : ""}`}>{value}</span>;
}

function lifecycleTone(s: string): string {
  if (s === "merged") return "ok";
  if (s === "closed") return "muted";
  if (s === "approved") return "ok";
  if (s === "changes_requested") return "warn";
  return "accent";
}
function ciTone(s: string): string {
  if (s === "passing") return "ok";
  if (s === "failing" || s === "error") return "crit";
  if (s === "pending") return "warn";
  return "muted";
}
function shortSha(s: string | null | undefined): string {
  return s ? s.slice(0, 8) : "—";
}
function ago(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

// ---- token bar ----------------------------------------------------------
function TokenBar({ onConnect }: { onConnect: () => void }) {
  const [value, setValue] = useState("");
  const hasToken = !!getDemoAuthToken();

  function connect() {
    const t = value.trim();
    if (!t) return;
    setDemoAuthToken(t);
    // Drop any stale demo tenant so authHeaders() sends only the bearer; the
    // gateway resolves the tenant from the token (and 403s on a mismatch).
    try {
      window.localStorage.removeItem("demoTenantId");
    } catch {
      /* ignore */
    }
    setValue("");
    onConnect();
  }

  return (
    <div className="gi-tokenbar">
      <span className="gi-tokenbar__status" data-on={hasToken}>
        {hasToken ? "● token set" : "○ no token"}
      </span>
      <input
        className="gi-input gi-tokenbar__input"
        type="password"
        placeholder="paste a bearer token (from scripts/github_intel_dev_session.py)"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") connect();
        }}
      />
      <button className="gi-btn gi-btn--primary" onClick={connect}>
        Connect
      </button>
    </div>
  );
}

// ---- repos list ---------------------------------------------------------
function ReposView({ onPick }: { onPick: (repo: string) => void }) {
  const [repos, setRepos] = useState<RepoSummary[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const ctrl = new AbortController();
    let alive = true;
    setLoading(true);
    (async () => {
      try {
        const r = await listRepos(ctrl.signal);
        if (!alive) return;
        setRepos(r.repos);
        setError(null);
      } catch (err) {
        if (!alive || (err instanceof Error && err.name === "AbortError")) return;
        setError(err instanceof ApiError ? err.message : String(err));
      } finally {
        if (alive) setLoading(false);
      }
    })();
    return () => {
      alive = false;
      ctrl.abort();
    };
  }, []);

  if (loading) return <div className="gi-muted">Loading repos…</div>;
  if (error)
    return error.startsWith("401") ? (
      <div className="gi-empty">
        Not authenticated — paste a bearer token in the bar above and click Connect.
        Generate one with <code className="gi-code">python scripts/github_intel_dev_session.py</code>.
      </div>
    ) : (
      <div className="gi-error">Failed to load repos: {error}</div>
    );
  if (!repos || repos.length === 0)
    return (
      <div className="gi-empty">
        No repositories with intelligence yet. Run{" "}
        <code className="gi-code">python scripts/demo_github_intel.py</code> to seed the
        dogfood repo, then Connect with that tenant's token.
      </div>
    );

  return (
    <table className="gi-table">
      <thead>
        <tr>
          <th>Repository</th>
          <th>Signals</th>
          <th>Indexed</th>
          <th>Symbols</th>
          <th>Last signal</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {repos.map((r) => (
          <tr key={r.repo} className="gi-row" onClick={() => onPick(r.repo)}>
            <td className="gi-mono">{r.repo}</td>
            <td>{r.signal_count}</td>
            <td>
              {r.indexed ? (
                <Pill value={shortSha(r.head_commit_sha)} tone="ok" />
              ) : (
                <Pill value={null} />
              )}
            </td>
            <td>{r.symbol_count}</td>
            <td className="gi-muted">{ago(r.last_signal_at)}</td>
            <td className="gi-chev">›</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ---- repo: state tab ----------------------------------------------------
function StateTab({ repo }: { repo: string }) {
  const [state, setState] = useState<RepoStateResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    let alive = true;
    (async () => {
      try {
        const s = await getRepoState(repo, ctrl.signal);
        if (alive) setState(s);
      } catch (err) {
        if (!alive || (err instanceof Error && err.name === "AbortError")) return;
        setError(err instanceof ApiError ? err.message : String(err));
      }
    })();
    return () => {
      alive = false;
      ctrl.abort();
    };
  }, [repo]);

  if (error) return <div className="gi-error">{error}</div>;
  if (!state) return <div className="gi-muted">Loading state…</div>;

  return (
    <div className="gi-stack">
      <div className="gi-cards">
        <div className="gi-card">
          <div className="gi-card__label">Default branch</div>
          <div className="gi-card__value">{state.default_branch ?? "—"}</div>
          <div className="gi-card__sub gi-mono">HEAD {shortSha(state.head_sha)}</div>
        </div>
        <div className="gi-card">
          <div className="gi-card__label">Code index</div>
          <div className="gi-card__value">
            {state.code_index.indexed ? `${state.code_index.symbol_count} symbols` : "not indexed"}
          </div>
          <div className="gi-card__sub gi-mono">
            {state.code_index.file_count} files · {state.code_index.edge_count} edges ·{" "}
            {shortSha(state.code_index.commit_sha)}
          </div>
        </div>
        <div className="gi-card">
          <div className="gi-card__label">Open PRs</div>
          <div className="gi-card__value">
            {state.pull_requests.filter((p) => !["merged", "closed"].includes(p.lifecycle)).length}
          </div>
          <div className="gi-card__sub">{state.pull_requests.length} total tracked</div>
        </div>
      </div>

      <h3 className="gi-h3">Pull requests</h3>
      {state.pull_requests.length === 0 ? (
        <div className="gi-muted">none</div>
      ) : (
        <table className="gi-table">
          <thead>
            <tr><th>#</th><th>Title</th><th>Lifecycle</th><th>CI</th><th>Base</th><th>Author</th></tr>
          </thead>
          <tbody>
            {state.pull_requests.map((p) => (
              <tr key={p.pr_number}>
                <td className="gi-mono">#{p.pr_number}</td>
                <td>{p.title ?? "—"}</td>
                <td><Pill value={p.lifecycle} tone={lifecycleTone(p.lifecycle)} /></td>
                <td><Pill value={p.ci_state} tone={ciTone(p.ci_state)} /></td>
                <td className="gi-mono">{p.base_ref ?? "—"}</td>
                <td>{p.author ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="gi-two-col">
        <div>
          <h3 className="gi-h3">Issues</h3>
          {state.issues.length === 0 ? (
            <div className="gi-muted">none</div>
          ) : (
            <ul className="gi-list">
              {state.issues.map((i) => (
                <li key={i.issue_number}>
                  <span className="gi-mono">#{i.issue_number}</span>{" "}
                  <Pill value={i.status} tone={i.status === "closed" ? "muted" : "accent"} />{" "}
                  {i.title ?? ""}
                </li>
              ))}
            </ul>
          )}
        </div>
        <div>
          <h3 className="gi-h3">Branches</h3>
          {state.branches.length === 0 ? (
            <div className="gi-muted">none</div>
          ) : (
            <ul className="gi-list">
              {state.branches.map((b) => (
                <li key={b.branch}>
                  <span className="gi-mono">{b.branch}</span>{" "}
                  <span className="gi-muted gi-mono">@{shortSha(b.head_sha)}</span>
                  {b.is_deleted ? " (deleted)" : ""}
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}

// ---- repo: signals tab + explain panel ----------------------------------
function SignalsTab({ repo }: { repo: string }) {
  const [signals, setSignals] = useState<SignalItem[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    let alive = true;
    (async () => {
      try {
        const s = await getSignals(repo, { limit: 50 }, ctrl.signal);
        if (alive) setSignals(s.signals);
      } catch (err) {
        if (!alive || (err instanceof Error && err.name === "AbortError")) return;
        setError(err instanceof ApiError ? err.message : String(err));
      }
    })();
    return () => {
      alive = false;
      ctrl.abort();
    };
  }, [repo]);

  if (error) return <div className="gi-error">{error}</div>;
  if (!signals) return <div className="gi-muted">Loading signals…</div>;
  if (signals.length === 0) return <div className="gi-muted">No signals for this repo.</div>;

  return (
    <div className="gi-two-pane">
      <div className="gi-feed">
        {signals.map((s) => (
          <button
            key={s.observation_id}
            className={`gi-feed-row${selected === s.observation_id ? " is-selected" : ""}`}
            onClick={() => setSelected(s.observation_id)}
          >
            <div className="gi-feed-row__top">
              <span className="gi-tag">{s.event_type}{s.action ? `.${s.action}` : ""}</span>
              {s.state_changed && s.state_after ? (
                <span className="gi-mono gi-delta">
                  {stateLabel(s.state_before)}→{stateLabel(s.state_after)}
                </span>
              ) : null}
              {s.blast_radius_count > 0 ? (
                <span className="gi-blast">⤳ {s.blast_radius_count}</span>
              ) : null}
            </div>
            <div className="gi-feed-row__text">{s.effect ?? s.content_text ?? s.entity_ref}</div>
            <div className="gi-feed-row__meta gi-muted">{s.entity_ref} · {ago(s.occurred_at)}</div>
          </button>
        ))}
      </div>
      <div className="gi-explain-wrap">
        {selected ? (
          <ExplainPanel observationId={selected} />
        ) : (
          <div className="gi-muted gi-explain-empty">Select a signal to see why it happened.</div>
        )}
      </div>
    </div>
  );
}

function stateLabel(o: Record<string, unknown> | null): string {
  if (!o) return "·";
  for (const k of ["lifecycle", "status", "ci_state", "head_sha"]) {
    if (k in o) {
      const v = o[k];
      return v == null ? "·" : k === "head_sha" ? String(v).slice(0, 6) : String(v);
    }
  }
  return "·";
}

function ExplainPanel({ observationId }: { observationId: string }) {
  const [data, setData] = useState<ExplainResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    let alive = true;
    setData(null);
    setError(null);
    (async () => {
      try {
        const r = await explainSignal(observationId, ctrl.signal);
        if (alive) setData(r);
      } catch (err) {
        if (!alive || (err instanceof Error && err.name === "AbortError")) return;
        setError(err instanceof ApiError ? err.message : String(err));
      }
    })();
    return () => {
      alive = false;
      ctrl.abort();
    };
  }, [observationId]);

  if (error) return <div className="gi-error">{error}</div>;
  if (!data) return <div className="gi-muted">Loading…</div>;
  const e = data.enrichment;

  return (
    <div className="gi-explain">
      <div className="gi-explain__headline">{data.content_text}</div>
      {e ? (
        <>
          <dl className="gi-kv">
            <dt>Cause</dt><dd>{e.cause ?? "—"}</dd>
            <dt>Effect</dt><dd>{e.effect ?? "—"}</dd>
            <dt>Why</dt><dd>{e.explanation ?? "—"}</dd>
            <dt>State change</dt>
            <dd className="gi-mono">
              {e.state_changed
                ? `${stateLabel(e.state_before)} → ${stateLabel(e.state_after)}`
                : "none"}
            </dd>
            <dt>Confidence</dt>
            <dd>
              {e.confidence != null ? e.confidence.toFixed(2) : "—"}{" "}
              <span className="gi-muted">via {e.reasoning_path ?? "rule"}</span>
            </dd>
          </dl>

          {e.affected_files && e.affected_files.length > 0 ? (
            <div className="gi-affected">
              <div className="gi-card__label">Changed files</div>
              <div className="gi-chips">
                {e.affected_files.map((f) => <span key={f} className="gi-chip gi-mono">{f}</span>)}
              </div>
            </div>
          ) : null}

          <BlastSummary blast={e.blast_radius} />

          {e.related_entities && e.related_entities.length > 0 ? (
            <div className="gi-affected">
              <div className="gi-card__label">Related</div>
              <div className="gi-chips">
                {e.related_entities.map((r, i) => (
                  <span key={i} className="gi-chip">
                    {String((r as Record<string, unknown>).ref ?? "")}
                    {(r as Record<string, unknown>).relation
                      ? ` (${String((r as Record<string, unknown>).relation)})`
                      : ""}
                  </span>
                ))}
              </div>
            </div>
          ) : null}

          {e.code_snapshot_sha ? (
            <div className="gi-muted gi-mono gi-snap">code @ {shortSha(e.code_snapshot_sha)}</div>
          ) : null}
        </>
      ) : (
        <div className="gi-muted">
          Raw signal — no intelligence was attached (enrichment disabled or it timed out).
        </div>
      )}
    </div>
  );
}

function BlastSummary({ blast }: { blast: Record<string, unknown> | null }) {
  if (!blast || typeof blast !== "object") return null;
  const depFiles = (blast.dependent_files as Array<{ path: string; hops: number }>) ?? [];
  const depSyms = (blast.dependent_symbols as Array<{ qualified_name: string }>) ?? [];
  if (depFiles.length === 0 && depSyms.length === 0) return null;
  return (
    <div className="gi-affected">
      <div className="gi-card__label">
        Blast radius — {depFiles.length + depSyms.length} dependents
      </div>
      <div className="gi-chips">
        {depFiles.map((d) => (
          <span key={d.path} className="gi-chip gi-mono">
            {d.path}<span className="gi-muted"> ·{d.hops}</span>
          </span>
        ))}
      </div>
      {depSyms.length > 0 ? (
        <div className="gi-chips">
          {depSyms.slice(0, 12).map((s) => (
            <span key={s.qualified_name} className="gi-chip gi-chip--sym gi-mono">
              {s.qualified_name}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

// ---- repo: blast-radius tab --------------------------------------------
function BlastTab({ repo }: { repo: string }) {
  const [path, setPath] = useState("");
  const [result, setResult] = useState<BlastRadiusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const run = useCallback(async () => {
    const p = path.trim();
    if (!p) return;
    setLoading(true);
    setError(null);
    try {
      setResult(await getBlastRadius(repo, [p]));
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [repo, path]);

  return (
    <div className="gi-stack">
      <div className="gi-runbar">
        <input
          className="gi-input"
          placeholder="changed file path, e.g. app/db.py"
          value={path}
          onChange={(e) => setPath(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && run()}
        />
        <button className="gi-btn gi-btn--primary" onClick={run} disabled={loading}>
          {loading ? "…" : "What does this affect?"}
        </button>
      </div>
      {error ? <div className="gi-error">{error}</div> : null}
      {result ? (
        result.indexed === false ? (
          <div className="gi-empty">Repo not indexed yet — no code graph to traverse.</div>
        ) : (
          <div className="gi-stack">
            <div className="gi-muted gi-mono">
              changed {(result.changed_files ?? []).join(", ") || "—"} @ {shortSha(result.commit_sha)}
            </div>
            <BlastSummary blast={result as unknown as Record<string, unknown>} />
            {(result.unknown_files ?? []).length > 0 ? (
              <div className="gi-muted">
                not found in index: {(result.unknown_files ?? []).join(", ")}
              </div>
            ) : null}
          </div>
        )
      ) : null}
    </div>
  );
}

// ---- repo: code-search tab ---------------------------------------------
function SearchTab({ repo }: { repo: string }) {
  const [q, setQ] = useState("");
  const [result, setResult] = useState<CodeSearchResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const run = useCallback(async () => {
    const query = q.trim();
    if (!query) return;
    setLoading(true);
    setError(null);
    try {
      setResult(await codeSearch(repo, query));
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [repo, q]);

  return (
    <div className="gi-stack">
      <div className="gi-runbar">
        <input
          className="gi-input"
          placeholder="describe code, e.g. verify an auth token"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && run()}
        />
        <button className="gi-btn gi-btn--primary" onClick={run} disabled={loading}>
          {loading ? "…" : "Search code"}
        </button>
      </div>
      {error ? <div className="gi-error">{error}</div> : null}
      {result ? (
        result.results.length === 0 ? (
          <div className="gi-empty">
            No matches{result.indexed === false ? " (repo not indexed)" : ""} — or the embedder
            is unavailable.
          </div>
        ) : (
          <table className="gi-table">
            <thead>
              <tr><th>Score</th><th>Symbol</th><th>Kind</th><th>File</th></tr>
            </thead>
            <tbody>
              {result.results.map((h) => (
                <tr key={`${h.path}:${h.qualified_name}`}>
                  <td className="gi-mono">{h.score.toFixed(3)}</td>
                  <td className="gi-mono">{h.qualified_name}</td>
                  <td>{h.kind}</td>
                  <td className="gi-mono gi-muted">{h.path}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )
      ) : null}
    </div>
  );
}

// ---- repo container -----------------------------------------------------
function RepoView({ repo, onBack }: { repo: string; onBack: () => void }) {
  const [tab, setTab] = useState<RepoTab>("state");
  const tabs: Array<{ id: RepoTab; label: string }> = [
    { id: "state", label: "State" },
    { id: "signals", label: "Signals" },
    { id: "blast", label: "Blast radius" },
    { id: "search", label: "Code search" },
  ];
  return (
    <div className="gi-stack">
      <div className="gi-repohead">
        <button className="gi-back" onClick={onBack}>← repos</button>
        <h2 className="gi-mono gi-repohead__name">{repo}</h2>
      </div>
      <div className="gi-tabs">
        {tabs.map((t) => (
          <button
            key={t.id}
            className={`gi-tab${tab === t.id ? " is-active" : ""}`}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>
      {tab === "state" && <StateTab repo={repo} />}
      {tab === "signals" && <SignalsTab repo={repo} />}
      {tab === "blast" && <BlastTab repo={repo} />}
      {tab === "search" && <SearchTab repo={repo} />}
    </div>
  );
}

// ---- page root ----------------------------------------------------------
export default function GitHubIntel() {
  const [repo, setRepo] = useState<string | null>(null);
  // bump to remount the repos list after a token is connected
  const [authTick, setAuthTick] = useState(0);

  return (
    <div className="gi-page">
      <header className="gi-header">
        <div>
          <h1 className="gi-title">GitHub Intelligence</h1>
          <p className="gi-subtitle">
            Every GitHub signal, enriched: state change, cause &amp; effect, and the code it
            touches.
          </p>
        </div>
      </header>
      <TokenBar onConnect={() => { setRepo(null); setAuthTick((t) => t + 1); }} />
      <main className="gi-main">
        {repo ? (
          <RepoView repo={repo} onBack={() => setRepo(null)} />
        ) : (
          <ReposView key={authTick} onPick={setRepo} />
        )}
      </main>
    </div>
  );
}
