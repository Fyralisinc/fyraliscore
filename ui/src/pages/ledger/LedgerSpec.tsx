import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import type { LedgerEventCategory } from "@/api/ledger-event-types";
import {
  CommandPalette,
  LedgerEventInspector,
  LedgerEventRow,
  SpecShell,
  SpecSidebar,
} from "@/components/spec";
import { useLedgerEvents } from "@/hooks/useSpecData";
import { useFyralisStore } from "@/lib/store";

const CATEGORY_FILTERS: Array<{ id: LedgerEventCategory | "all"; label: string }> = [
  { id: "all", label: "All" },
  { id: "model_update", label: "Model updates" },
  { id: "decision_action", label: "Decision actions" },
  { id: "contestation", label: "Contestations" },
  { id: "forecast", label: "Forecasts" },
  { id: "commitment_state", label: "Commitments" },
  { id: "observation", label: "Observations" },
];

// Ledger page — spec §14. Memory of model updates, decisions,
// forecasts, contestations, resolutions. Group by day.
export default function LedgerSpec() {
  const [category, setCategory] = useState<LedgerEventCategory | "all">("all");
  const [search, setSearch] = useState("");

  const params = useMemo(
    () =>
      category === "all"
        ? search
          ? { search }
          : undefined
        : { categories: [category], search: search || undefined },
    [category, search]
  );

  const { phase, rangeLabel } = useLedgerEvents(params);
  const events = useFyralisStore((s) => s.ledgerEvents);
  const [params2, setParams] = useSearchParams();
  const [selectedId, setSelectedId] = useState<string | null>(params2.get("event"));
  const setPaletteOpen = useFyralisStore((s) => s.setPaletteOpen);

  useEffect(() => {
    const id = params2.get("event");
    if (id) setSelectedId(id);
  }, [params2]);

  const grouped = useMemo(() => {
    const groups: Record<string, typeof events> = {};
    for (const e of events) {
      const day = new Date(e.occurredAt).toLocaleDateString(undefined, {
        weekday: "long",
        month: "long",
        day: "numeric",
      });
      if (!groups[day]) groups[day] = [];
      groups[day].push(e);
    }
    return groups;
  }, [events]);

  const selectedEvent = selectedId ? events.find((e) => e.id === selectedId) ?? null : null;

  return (
    <>
      <SpecShell
        sidebar={<SpecSidebar active="ledger" />}
        main={
          <div className="fx-stack--xl">
            <header className="fx-pageheader">
              <div>
                <h1 className="fx-pageheader__title">Ledger</h1>
                <p className="fx-pageheader__compression">
                  Company memory across observations, model updates, decisions, forecasts, and resolutions.
                </p>
                <div className="fx-pageheader__counters">
                  <span className="fx-pageheader__counter">
                    <strong>{rangeLabel || "Last 30 days"}</strong>
                  </span>
                  <span className="fx-pageheader__counter">
                    <strong>{events.length}</strong> events
                  </span>
                </div>
              </div>
              <div className="fx-pageheader__right">
                <input
                  className="fx-btn"
                  style={{ minWidth: 240, fontSize: 13 }}
                  placeholder="Search the ledger…"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                />
                <button type="button" className="fx-btn" onClick={() => setPaletteOpen(true)}>
                  Ask Fyralis <span style={{ opacity: 0.6, marginLeft: 4 }}>⌘K</span>
                </button>
              </div>
            </header>

            <div className="fx-row" style={{ gap: 6, flexWrap: "wrap" }}>
              {CATEGORY_FILTERS.map((c) => (
                <button
                  key={c.id}
                  type="button"
                  className={`fx-pill${category === c.id ? " fx-pill--evidence" : " fx-pill--ghost"}`}
                  onClick={() => setCategory(c.id as never)}
                >
                  {c.label}
                </button>
              ))}
            </div>

            {phase === "loading" && events.length === 0 ? (
              <div className="fx-empty">Loading ledger…</div>
            ) : events.length === 0 ? (
              <div className="fx-empty">
                <strong>No Ledger events yet.</strong>
                <div style={{ marginTop: 6 }}>
                  Fyralis will record model changes, decisions, forecasts, contestations, and resolutions here.
                </div>
              </div>
            ) : (
              Object.entries(grouped).map(([day, evs]) => (
                <section key={day}>
                  <div className="fx-ledger__day">{day}</div>
                  <div className="fx-stack" style={{ gap: 0 }}>
                    {evs.map((e) => (
                      <LedgerEventRow
                        key={e.id}
                        event={e}
                        selected={selectedId === e.id}
                        onSelect={(id) => {
                          setSelectedId(id);
                          setParams({ event: id });
                        }}
                      />
                    ))}
                  </div>
                </section>
              ))
            )}
          </div>
        }
        inspector={
          selectedEvent ? (
            <LedgerEventInspector
              event={selectedEvent}
              onClose={() => {
                setSelectedId(null);
                setParams({});
              }}
            />
          ) : undefined
        }
      />
      <CommandPalette />
    </>
  );
}
