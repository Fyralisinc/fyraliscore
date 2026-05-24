import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AppShell } from "@/shell/AppShell";
import { Sidebar } from "@/shell/Sidebar";
import "@/components/ledger/ledger.css";
import type { LedgerEvent, LedgerEventType, LedgerSummary } from "@/api/history-types";
import {
  ApiError,
  getHistorySummary,
  getLedgerHistory,
} from "@/api/history-client";
import { LedgerHeader } from "@/components/ledger/LedgerHeader";
import { LedgerTabs } from "@/components/ledger/LedgerTabs";
import type { LedgerTabId } from "@/components/ledger/LedgerTabs";
import { LedgerSummary as LedgerSummaryStrip } from "@/components/ledger/LedgerSummary";
import { LedgerTimeline } from "@/components/ledger/LedgerTimeline";
import { LedgerInspector } from "@/components/ledger/LedgerInspector";
import { tabToTypes } from "@/components/ledger/event-taxonomy";
import {
  LEDGER_EVENTS_FIXTURE,
  LEDGER_SUMMARY_FIXTURE,
} from "@/api/ledger-mock";

const PAGE_SIZE = 10;
// Fixture anchor — when the fixture is in play, the "today" date should
// align with the screenshot copy.
const FIXTURE_NOW = new Date("2025-05-15T12:00:00.000Z");

type LoadState = "idle" | "loading" | "ready" | "error";

export default function LedgerPage() {
  const [events, setEvents] = useState<LedgerEvent[]>([]);
  const [summary, setSummary] = useState<LedgerSummary | null>(null);
  const [tab, setTab] = useState<LedgerTabId>("all");
  const [filterTypes, setFilterTypes] = useState<LedgerEventType[]>([]);
  const [search, setSearch] = useState("");
  const [pageSize, setPageSize] = useState(PAGE_SIZE);
  const [state, setState] = useState<LoadState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [history, setHistory] = useState<string[]>([]);
  const [usingFixture, setUsingFixture] = useState(false);

  const searchRef = useRef<HTMLInputElement | null>(null);

  // Effective canonical type filter = intersection of (tab) and (filter
  // multiselect). When the user multi-selects via Filters, that wins
  // over the tab category — tabs are a convenience shortcut.
  const effectiveTypes = useMemo<LedgerEventType[] | undefined>(() => {
    const tabTypes = tabToTypes(tab);
    if (filterTypes.length > 0) return filterTypes;
    return tabTypes;
  }, [tab, filterTypes]);

  const load = useCallback(async () => {
    setState("loading");
    setError(null);
    try {
      const [events, summary] = await Promise.all([
        getLedgerHistory({ period: "30d", types: effectiveTypes }),
        getHistorySummary({ range_days: 30 }),
      ]);
      // If the response looks like the legacy History payload (events
      // with non-canonical types), fall back to the fixture so the
      // page remains usable while the backend ledger surface is being
      // wired through. An empty array is treated as a legitimate
      // "no results" response — not a fallback signal.
      const evs = (events.events ?? []) as LedgerEvent[];
      const canonical = new Set([
        "action_taken",
        "model_update",
        "prediction_made",
        "prediction_resolved",
        "observation_ingested",
        "contestation",
      ]);
      const looksLegacy =
        evs.length > 0 &&
        !evs.some((e) => canonical.has((e as { type?: string }).type ?? ""));
      if (looksLegacy) {
        useFixture();
        return;
      }
      setEvents(evs);
      setSummary(summary);
      setUsingFixture(false);
      setState("ready");
    } catch (err) {
      if (err instanceof ApiError) {
        setError(`${err.status} ${err.message}`);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError("Unknown error");
      }
      setState("error");
    }
  }, [effectiveTypes]);

  const useFixture = useCallback(() => {
    const filtered =
      effectiveTypes && effectiveTypes.length > 0
        ? LEDGER_EVENTS_FIXTURE.filter((e) =>
            effectiveTypes.includes(e.type)
          )
        : LEDGER_EVENTS_FIXTURE;
    setEvents(filtered);
    setSummary(LEDGER_SUMMARY_FIXTURE);
    setUsingFixture(true);
    setState("ready");
    setError(null);
  }, [effectiveTypes]);

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [effectiveTypes]);

  // Reset pagination when the filter context changes.
  useEffect(() => {
    setPageSize(PAGE_SIZE);
  }, [effectiveTypes, search]);

  // Search filter (client-side per spec).
  const searchedEvents = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return events;
    return events.filter((e) => {
      const haystack = [
        e.title,
        e.summary,
        e.body ?? "",
        ...(e.tags ?? []),
        e.actor.name,
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(q);
    });
  }, [events, search]);

  const pagedEvents = useMemo(
    () => searchedEvents.slice(0, pageSize),
    [searchedEvents, pageSize]
  );

  const selectedEvent = useMemo(
    () =>
      selectedId
        ? pagedEvents.find((e) => e.id === selectedId) ??
          events.find((e) => e.id === selectedId) ??
          null
        : null,
    [selectedId, pagedEvents, events]
  );

  const onSelect = useCallback(
    (event: LedgerEvent) => {
      setSelectedId(event.id);
      setHistory((prev) => {
        // Append to history if it's a new (not back/forward) selection.
        if (prev[prev.length - 1] === event.id) return prev;
        return [...prev, event.id];
      });
    },
    []
  );

  const onTabChange = useCallback((next: LedgerTabId) => {
    setTab(next);
    // Switching tabs clears the multi-select filter chip so the tab
    // category is the dominant signal (matches screenshot behaviour).
    setFilterTypes([]);
    setSelectedId(null);
  }, []);

  const onFiltersChange = useCallback((next: LedgerEventType[]) => {
    setFilterTypes(next);
    setSelectedId(null);
  }, []);

  const onClose = useCallback(() => setSelectedId(null), []);

  const historyIndex = selectedId
    ? history.lastIndexOf(selectedId)
    : -1;
  const canBack = historyIndex > 0;
  const canForward = historyIndex >= 0 && historyIndex < history.length - 1;
  const onBack = useCallback(() => {
    if (historyIndex <= 0) return;
    setSelectedId(history[historyIndex - 1]);
  }, [history, historyIndex]);
  const onForward = useCallback(() => {
    if (historyIndex < 0 || historyIndex >= history.length - 1) return;
    setSelectedId(history[historyIndex + 1]);
  }, [history, historyIndex]);

  return (
    <AppShell
      sidebar={<Sidebar activeRoute="ledger" />}
      main={
        <div className="fy-ledger" data-testid="ledger-root">
          <LedgerHeader
            dateRangeLabel="Apr 15 – May 15, 2025"
            searchValue={search}
            onSearchChange={setSearch}
            activeFilters={filterTypes}
            onFiltersChange={onFiltersChange}
            searchInputRef={searchRef}
          />
          <LedgerTabs active={tab} onChange={onTabChange} />
          <LedgerSummaryStrip
            summary={summary}
            loading={state === "loading" && !summary}
          />
          <div className="fy-ledger__list-toolbar">
            <div className="fy-ledger__sort">
              Newest first
              <svg
                width="10"
                height="10"
                viewBox="0 0 10 10"
                aria-hidden="true"
              >
                <path
                  d="M2 4 5 7l3-3"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.4"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </div>
            {usingFixture ? (
              <span
                className="fy-ledger__fixture-note"
                data-testid="ledger-fixture-note"
              >
                Fixture data — wire /v1/history?surface=ledger to switch
              </span>
            ) : null}
          </div>
          <LedgerTimeline
            events={pagedEvents}
            selectedEventId={selectedId}
            onSelect={onSelect}
            onLoadMore={() => setPageSize((n) => n + PAGE_SIZE)}
            hasMore={searchedEvents.length > pageSize}
            loading={state === "loading"}
            error={state === "error" ? error : null}
            now={usingFixture ? FIXTURE_NOW : undefined}
          />
        </div>
      }
      inspector={
        selectedEvent ? (
          <LedgerInspector
            event={selectedEvent}
            onClose={onClose}
            onBack={onBack}
            onForward={onForward}
            canBack={canBack}
            canForward={canForward}
            now={usingFixture ? FIXTURE_NOW : undefined}
          />
        ) : undefined
      }
    />
  );
}
