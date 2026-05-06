import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Sidebar } from "@/components/Sidebar";
import { ShortcutsOverlay } from "@/components/ShortcutsOverlay";
import { HistoryLayerStrip } from "@/components/history/HistoryLayerStrip";
import { HistoryNarrativeBand } from "@/components/history/HistoryNarrativeBand";
import { Chronicle } from "@/components/history/Chronicle";
import { Predictions } from "@/components/history/Predictions";
import { Arcs } from "@/components/history/Arcs";
import { EventPanel } from "@/components/history/EventPanel";
import { useHistory } from "@/hooks/useHistory";
import type { HistoryPeriod } from "@/api/history-client";
import type {
  Arc,
  CalibrationSummary,
  EventType,
  HistoryEvent,
  HistoryFilters,
  HistoryLayerId,
  LayerStripCounts,
  Prediction,
  ShapeToken,
} from "@/components/history/types";

const EMPTY_LAYER_COUNTS: LayerStripCounts = {
  chronicle: { events: 0, period_label: "this period" },
  predictions: { calibration: 0, correct: 0, total: 0 },
  arcs: { active: 0, resolved: 0 },
};
const EMPTY_CALIBRATION: CalibrationSummary = { overall: 0, domains: [] };
const EMPTY_EVENTS: HistoryEvent[] = [];
const EMPTY_PREDICTIONS: Prediction[] = [];
const EMPTY_ARCS: Arc[] = [];

// Driftwood — History page (DRIFTWOOD_HISTORY_SPEC.md).
// Three layers: Chronicle (default), Predictions, Arcs.
// Data is sourced from the gateway /v1/history endpoint
// (services.history.aggregator) — events, predictions, arcs, calibration,
// layer counts, and the narrative-band statements all derive from the
// substrate.
export default function History() {
  const navigate = useNavigate();
  const [layer, setLayer] = useState<HistoryLayerId>("chronicle");
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const [filters, setFilters] = useState<HistoryFilters>(() => ({
    period: "90d",
    types: new Set<EventType>(),
    significance: "all",
    arcsOn: true,
    search: "",
    arcId: null,
  }));
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);
  const [selectedPredictionId, setSelectedPredictionId] = useState<
    string | null
  >(null);
  const [selectedArcId, setSelectedArcId] = useState<string | null>(null);
  const searchRef = useRef<HTMLInputElement>(null);

  const { history, loading, error, offline, setPeriod } = useHistory(
    filters.period as HistoryPeriod
  );

  // Keep the backend period query in sync with the filters dropdown.
  useEffect(() => {
    setPeriod(filters.period as HistoryPeriod);
  }, [filters.period, setPeriod]);

  const events = history?.events ?? EMPTY_EVENTS;
  const predictions = history?.predictions ?? EMPTY_PREDICTIONS;
  const arcs = history?.arcs ?? EMPTY_ARCS;
  const calibration = history?.calibration ?? EMPTY_CALIBRATION;
  const layerCounts = history?.layer_counts ?? EMPTY_LAYER_COUNTS;
  const chronicleStatement: ShapeToken[] = history?.chronicle_statement ?? [];
  const predictionsStatement: ShapeToken[] = history?.predictions_statement ?? [];
  const arcsStatement: ShapeToken[] = history?.arcs_statement ?? [];

  const selectedEvent = useMemo<HistoryEvent | null>(
    () =>
      selectedEventId
        ? events.find((e) => e.id === selectedEventId) ?? null
        : null,
    [events, selectedEventId]
  );
  const selectedPrediction = useMemo<Prediction | null>(
    () =>
      selectedPredictionId
        ? predictions.find((p) => p.id === selectedPredictionId) ?? null
        : null,
    [predictions, selectedPredictionId]
  );
  const selection = selectedEvent
    ? {
        kind: "event" as const,
        event: selectedEvent,
        arc: selectedEvent.arc
          ? arcs.find((a) => a.id === selectedEvent.arc)
          : undefined,
      }
    : selectedPrediction
      ? { kind: "prediction" as const, prediction: selectedPrediction }
      : null;

  function closePanel() {
    setSelectedEventId(null);
    setSelectedPredictionId(null);
  }

  const onSwitchLayer = useCallback((id: HistoryLayerId) => {
    setLayer(id);
    closePanel();
    setFilters({
      period: "90d",
      types: new Set<EventType>(),
      significance: "all",
      arcsOn: true,
      search: "",
      arcId: null,
    });
  }, []);

  // ? opens shortcuts; 1-3 switches layers; / focuses search; Esc closes.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const tag = (document.activeElement as HTMLElement | null)?.tagName;
      const isInput =
        tag === "INPUT" ||
        tag === "TEXTAREA" ||
        (document.activeElement as HTMLElement | null)?.isContentEditable;
      if (e.key === "Escape") {
        if (shortcutsOpen) {
          setShortcutsOpen(false);
          e.preventDefault();
          return;
        }
        if (selection) {
          closePanel();
          e.preventDefault();
          return;
        }
        if (filters.arcId) {
          setFilters({ ...filters, arcId: null });
          e.preventDefault();
        }
        return;
      }
      if (isInput) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      if (e.key === "?") {
        e.preventDefault();
        setShortcutsOpen(true);
        return;
      }
      if (e.key === "/") {
        e.preventDefault();
        searchRef.current?.focus();
        return;
      }
      const map: Record<string, HistoryLayerId> = {
        "1": "chronicle",
        "2": "predictions",
        "3": "arcs",
      };
      if (map[e.key]) {
        e.preventDefault();
        onSwitchLayer(map[e.key]);
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [filters, onSwitchLayer, selection, shortcutsOpen]);

  const nav = useMemo(
    () => [
      {
        id: "primary",
        label: "Surfaces",
        items: [
          { id: "today", label: "Today", active: false },
          { id: "structure", label: "Structure", active: false },
          { id: "history", label: "History", active: true },
        ],
      },
    ],
    []
  );

  function handleRef(kind: string, id: string) {
    if (kind === "arc") {
      setLayer("arcs");
      setSelectedArcId(id);
    } else if (kind === "decision" || kind === "commitment") {
      // toggle arcId filter on the chronicle for events related to this entity
      setLayer("chronicle");
    }
  }

  function handleArcChip(arcId: string) {
    setLayer("arcs");
    setSelectedArcId(arcId);
  }

  return (
    <>
      <div className="cockpit">
        <Sidebar
          brand={{ name: "Fyralis", mark: "F", pulse_day: 4 }}
          nav={nav}
          onBrandClick={() => {
            // Reset History to its default view: chronicle layer, no
            // filter, no panel.
            setLayer("chronicle");
            setSelectedEventId(null);
            setSelectedPredictionId(null);
            setSelectedArcId(null);
            setFilters({
              period: "90d",
              types: new Set<EventType>(),
              significance: "all",
              arcsOn: true,
              search: "",
              arcId: null,
            });
          }}
          onNavigate={(_s, item) => {
            if (item === "today") navigate("/");
            else if (item === "structure") navigate("/structure");
            else if (item === "history") navigate("/history");
          }}
        />

        <main className="structure-main history-main">
          <HistoryLayerStrip
            active={layer}
            counts={layerCounts}
            onSwitch={onSwitchLayer}
            onShortcuts={() => setShortcutsOpen(true)}
          />

          <HistoryNarrativeBand
            layer={layer}
            statement={
              layer === "chronicle"
                ? chronicleStatement
                : layer === "predictions"
                  ? predictionsStatement
                  : arcsStatement
            }
            events={events}
            arcs={arcs}
            calibration={calibration}
            onArcChip={handleArcChip}
            onRef={handleRef}
          />

          {layer === "chronicle" ? (
            <ChronicleControls
              filters={filters}
              onFiltersChange={setFilters}
              searchRef={searchRef}
            />
          ) : null}

          <div className="history-layer-content">
            {loading && history === null ? (
              <div className="history-state-msg">Loading history…</div>
            ) : offline && history === null ? (
              <div className="history-state-msg">
                Couldn't reach the server.
                {error ? <span className="history-state-detail"> {error}</span> : null}
              </div>
            ) : layer === "chronicle" ? (
              <Chronicle
                events={events}
                arcs={arcs}
                filters={filters}
                onEventClick={(id) => {
                  setSelectedPredictionId(null);
                  setSelectedEventId(id);
                }}
                onArcClick={(id) =>
                  setFilters({ ...filters, arcId: filters.arcId === id ? null : id })
                }
                onFiltersChange={setFilters}
              />
            ) : layer === "predictions" ? (
              <Predictions
                predictions={predictions}
                calibration={calibration}
                onRowClick={(id) => {
                  setSelectedEventId(null);
                  setSelectedPredictionId(id);
                }}
              />
            ) : (
              <Arcs
                arcs={arcs}
                events={events}
                selectedArcId={selectedArcId}
                onSelect={setSelectedArcId}
                onEventClick={(id) => {
                  setSelectedPredictionId(null);
                  setSelectedEventId(id);
                }}
              />
            )}
          </div>
        </main>
      </div>

      <EventPanel
        selection={selection}
        onClose={closePanel}
        onJumpToEntity={(kind, id) => {
          if (kind === "structure") navigate("/structure");
          else if (kind === "decision") {
            setLayer("chronicle");
            setFilters({ ...filters, search: id });
          }
        }}
        onJumpToArc={(id) => {
          setLayer("arcs");
          setSelectedArcId(id);
          closePanel();
        }}
      />

      {shortcutsOpen ? (
        <ShortcutsOverlay onClose={() => setShortcutsOpen(false)} />
      ) : null}
    </>
  );
}

function ChronicleControls({
  filters,
  onFiltersChange,
  searchRef,
}: {
  filters: HistoryFilters;
  onFiltersChange: (f: HistoryFilters) => void;
  searchRef: React.RefObject<HTMLInputElement>;
}) {
  return (
    <div className="chronicle-controls map-controls">
      <select
        className="filter-select"
        value={filters.period}
        onChange={(e) =>
          onFiltersChange({
            ...filters,
            period: e.target.value as HistoryFilters["period"],
          })
        }
        aria-label="Period"
      >
        <option value="7d">Period: Last 7 days</option>
        <option value="30d">Period: Last 30 days</option>
        <option value="90d">Period: Last 90 days</option>
        <option value="365d">Period: Last 365 days</option>
        <option value="all">Period: All time</option>
      </select>

      <select
        className="filter-select"
        value={filters.significance}
        onChange={(e) =>
          onFiltersChange({
            ...filters,
            significance: e.target
              .value as HistoryFilters["significance"],
          })
        }
        aria-label="Significance"
      >
        <option value="all">Show: All</option>
        <option value="major-standard">Show: Major + Standard</option>
        <option value="major">Show: Major only</option>
      </select>

      <button
        type="button"
        className="control-toggle"
        data-active={filters.arcsOn ? "true" : "false"}
        onClick={() => onFiltersChange({ ...filters, arcsOn: !filters.arcsOn })}
        aria-pressed={filters.arcsOn}
      >
        <span className="control-icon">◆</span>
        Arcs {filters.arcsOn ? "on" : "off"}
      </button>

      {filters.arcId ? (
        <button
          type="button"
          className="control-toggle"
          data-active="true"
          onClick={() => onFiltersChange({ ...filters, arcId: null })}
          aria-label="Clear arc filter"
        >
          Arc filter active · ×
        </button>
      ) : null}

      <input
        ref={searchRef}
        type="search"
        className="search-input"
        placeholder="Search events…"
        value={filters.search}
        onChange={(e) =>
          onFiltersChange({ ...filters, search: e.target.value })
        }
      />
    </div>
  );
}
