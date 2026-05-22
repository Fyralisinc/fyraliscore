import { useEffect, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import type { ModelLens } from "@/api/operating-thread-types";
import {
  CommandPalette,
  LensBar,
  OperatingThreadRow,
  RecentChangesStrip,
  SpecShell,
  SpecSidebar,
  ThreadInspector,
} from "@/components/spec";
import { useOperatingThreads, useRecentModelChanges } from "@/hooks/useSpecData";
import { useFyralisStore } from "@/lib/store";

// Model page — spec §12. Default = compressed thread board, not graph.
// Trace mode (cause/consequence) is what surfaces the underlying graph;
// the previous LayeredGraph is reachable from the Trace overlay so we
// don't lose the work, but it is no longer the primary view.
import { TraceOverlay } from "./TraceOverlay";

export default function ModelSpec() {
  const [lens, setLens] = useState<ModelLens>("company");
  const { phase, response } = useOperatingThreads(lens);
  useRecentModelChanges();

  const threads = useFyralisStore((s) => s.threads);
  const setSelection = useFyralisStore((s) => s.setSelection);
  const setPaletteOpen = useFyralisStore((s) => s.setPaletteOpen);

  const [params, setParams] = useSearchParams();
  const navigate = useNavigate();
  const [selectedId, setSelectedId] = useState<string | null>(params.get("thread"));
  const [traceMode, setTraceMode] = useState<"cause" | "consequence" | null>(null);
  const [search, setSearch] = useState("");

  useEffect(() => {
    const id = params.get("thread");
    if (id) setSelectedId(id);
  }, [params]);

  // Filter threads by lens + search query. When the network response
  // hasn't landed yet we synthesize groups from the store so the board
  // never flashes empty — the seeded fixture renders immediately and
  // the real server payload swaps in once it arrives.
  const visibleGroups = useMemo(() => {
    const q = search.trim().toLowerCase();
    const match = (title: string, body: string) =>
      !q ? true : `${title} ${body}`.toLowerCase().includes(q);

    if (response) {
      return response.groups.map((g) => ({
        ...g,
        threads: g.threads.filter((t) => match(t.title, t.currentReading)),
      }));
    }

    const NEEDS = new Set([
      "under_pressure",
      "needs_review",
      "critical",
      "contested",
      "stale",
    ]);
    return [
      {
        id: "needs-attention",
        label: "Needs attention",
        threads: threads.filter((t) => NEEDS.has(t.status)).filter((t) => match(t.title, t.currentReading)),
      },
      {
        id: "stable",
        label: "Stable / watching",
        threads: threads.filter((t) => !NEEDS.has(t.status)).filter((t) => match(t.title, t.currentReading)),
      },
    ];
  }, [response, lens, search, threads]);

  const selectedThread = selectedId ? threads.find((t) => t.id === selectedId) ?? null : null;

  return (
    <>
      <SpecShell
        sidebar={<SpecSidebar active="model" />}
        main={
          <div className="fx-stack--xl">
            <header className="fx-pageheader">
              <div>
                <h1 className="fx-pageheader__title">Model</h1>
                <p className="fx-pageheader__compression">
                  {response?.compressionSentence ??
                    "Fyralis has condensed the company into operating threads."}
                </p>
                {response ? (
                  <div className="fx-pageheader__counters">
                    <span className="fx-pageheader__counter">
                      <strong>{response.statusCounters.changedToday}</strong> changed today
                    </span>
                    <span className="fx-pageheader__counter">
                      <strong>{response.statusCounters.contested}</strong> contested
                    </span>
                    <span className="fx-pageheader__counter">
                      <strong>{response.statusCounters.blockedCommitments}</strong> blocked commitments
                    </span>
                    {response.statusCounters.arrAtRisk ? (
                      <span className="fx-pageheader__counter">
                        <strong>${(response.statusCounters.arrAtRisk / 1_000_000).toFixed(2)}M</strong> at risk
                      </span>
                    ) : null}
                  </div>
                ) : null}
              </div>
              <div className="fx-pageheader__right">
                <input
                  className="fx-btn"
                  style={{ minWidth: 240, fontSize: 13 }}
                  placeholder="Ask or search the model…"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  onFocus={() => setPaletteOpen(true)}
                />
                <span className="fx-muted" style={{ fontSize: 12 }}>
                  Last updated {response?.lastUpdatedAt ? formatRel(response.lastUpdatedAt) : "—"}
                </span>
              </div>
            </header>

            <LensBar active={lens} onChange={setLens} />

            <div style={{ display: "grid", gridTemplateColumns: "1fr 280px", gap: 18 }}>
              <div className="fx-stack--lg">
                {phase === "loading" && threads.length === 0 ? (
                  <div className="fx-empty">Loading model…</div>
                ) : visibleGroups.length === 0 ? (
                  <div className="fx-empty">
                    <strong>No active threads under this lens.</strong>
                    <div style={{ marginTop: 6 }}>Try a different lens, or connect more sources.</div>
                  </div>
                ) : (
                  visibleGroups.map((group) => (
                    <section key={group.id} className="fx-section">
                      <header className="fx-section__head">
                        <div className="fx-section__title">{group.label}</div>
                        <div className="fx-section__sub">{group.threads.length} threads</div>
                      </header>
                      <div className="fx-stack--lg">
                        {group.threads.map((t) => (
                          <OperatingThreadRow
                            key={t.id}
                            thread={t}
                            selected={selectedId === t.id}
                            onSelect={(id) => {
                              setSelectedId(id);
                              setSelection({ threadId: id });
                              setParams({ thread: id });
                            }}
                            onTraceCause={(id) => {
                              setSelectedId(id);
                              setTraceMode("cause");
                            }}
                            onTraceConsequence={(id) => {
                              setSelectedId(id);
                              setTraceMode("consequence");
                            }}
                            onViewDeltas={(id) => {
                              const thread = threads.find((tt) => tt.id === id);
                              const firstDelta = thread?.relatedDecisionDeltaIds[0];
                              if (firstDelta) navigate(`/?delta=${firstDelta}`);
                            }}
                            onMarkWrong={() => {}}
                          />
                        ))}
                      </div>
                    </section>
                  ))
                )}
              </div>
              <div className="fx-stack--lg">
                <RecentChangesStrip
                  onSelectThread={(id) => {
                    setSelectedId(id);
                    setParams({ thread: id });
                  }}
                />
              </div>
            </div>
          </div>
        }
        inspector={
          selectedThread ? (
            <ThreadInspector
              thread={selectedThread}
              onClose={() => {
                setSelectedId(null);
                setParams({});
              }}
              onTraceCause={() => setTraceMode("cause")}
              onTraceConsequence={() => setTraceMode("consequence")}
              onMarkWrong={() => {}}
              onCreateProposed={() => navigate("/")}
            />
          ) : undefined
        }
      />
      <CommandPalette />
      {traceMode && selectedThread ? (
        <TraceOverlay
          mode={traceMode}
          thread={selectedThread}
          onClose={() => setTraceMode(null)}
        />
      ) : null}
    </>
  );
}

function formatRel(iso: string): string {
  try {
    const d = new Date(iso);
    const ms = Date.now() - d.getTime();
    const s = Math.max(1, Math.floor(ms / 1000));
    if (s < 60) return `${s}s ago`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    return d.toLocaleDateString();
  } catch {
    return iso;
  }
}
