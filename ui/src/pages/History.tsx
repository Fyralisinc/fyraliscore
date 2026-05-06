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
import {
  ARCS_NARRATIVE_STATEMENT,
  CHRONICLE_PERIOD_STATEMENT,
  PREDICTIONS_NARRATIVE_STATEMENT,
  SAMPLE_ARCS,
  SAMPLE_CALIBRATION,
  SAMPLE_EVENTS,
  SAMPLE_LAYER_COUNTS,
  SAMPLE_PREDICTIONS,
} from "@/components/history/sample-data";
import type {
  EventType,
  HistoryEvent,
  HistoryFilters,
  HistoryLayerId,
  Prediction,
} from "@/components/history/types";

// Driftwood — History page (DRIFTWOOD_HISTORY_SPEC.md).
// Three layers: Chronicle (default), Predictions, Arcs.
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

  const selectedEvent = useMemo<HistoryEvent | null>(
    () =>
      selectedEventId
        ? SAMPLE_EVENTS.find((e) => e.id === selectedEventId) ?? null
        : null,
    [selectedEventId]
  );
  const selectedPrediction = useMemo<Prediction | null>(
    () =>
      selectedPredictionId
        ? SAMPLE_PREDICTIONS.find((p) => p.id === selectedPredictionId) ?? null
        : null,
    [selectedPredictionId]
  );
  const selection = selectedEvent
    ? {
        kind: "event" as const,
        event: selectedEvent,
        arc: selectedEvent.arc
          ? SAMPLE_ARCS.find((a) => a.id === selectedEvent.arc)
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
            counts={SAMPLE_LAYER_COUNTS}
            onSwitch={onSwitchLayer}
            onShortcuts={() => setShortcutsOpen(true)}
          />

          <HistoryNarrativeBand
            layer={layer}
            statement={
              layer === "chronicle"
                ? CHRONICLE_PERIOD_STATEMENT
                : layer === "predictions"
                  ? PREDICTIONS_NARRATIVE_STATEMENT
                  : ARCS_NARRATIVE_STATEMENT
            }
            events={SAMPLE_EVENTS}
            arcs={SAMPLE_ARCS}
            calibration={SAMPLE_CALIBRATION}
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
            {layer === "chronicle" ? (
              <Chronicle
                events={SAMPLE_EVENTS}
                arcs={SAMPLE_ARCS}
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
                predictions={SAMPLE_PREDICTIONS}
                calibration={SAMPLE_CALIBRATION}
                onRowClick={(id) => {
                  setSelectedEventId(null);
                  setSelectedPredictionId(id);
                }}
              />
            ) : (
              <Arcs
                arcs={SAMPLE_ARCS}
                events={SAMPLE_EVENTS}
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
