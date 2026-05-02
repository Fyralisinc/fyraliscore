import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Sidebar } from "@/components/Sidebar";
import { ShortcutsOverlay } from "@/components/ShortcutsOverlay";
import { JustUpdated } from "@/components/JustUpdated";
import { LayerStrip } from "@/components/structure/LayerStrip";
import { NarrativeBand } from "@/components/structure/NarrativeBand";
import { MapControls } from "@/components/structure/MapControls";
import { TerritoryMap } from "@/components/structure/TerritoryMap";
import { CommitmentPanel } from "@/components/structure/CommitmentPanel";
import { useToday } from "@/hooks/useToday";
import {
  SAMPLE_COMMITMENTS,
  SAMPLE_CUSTOMERS,
  SAMPLE_LAYER_COUNTS,
  SAMPLE_OWNERS,
  SAMPLE_RECENT_CHANGE,
  SAMPLE_SHAPE_STATEMENT,
} from "@/components/structure/sample-data";
import { computeFreshlyUpdatedIds } from "@/components/structure/fresh-match";
import type {
  ActiveRefFilter,
  ColorMode,
  CommitmentStatus,
  Filters,
  LayerId,
  LayoutMode,
} from "@/components/structure/types";

const DAY_MS = 24 * 60 * 60 * 1000;

// Driftwood — Structure page (Part 1-15 of DRIFTWOOD_STRUCTURE_SPEC.md).
// Only the Commitments layer is interactive in v1; layers 2-5 render
// "Coming soon" per Part 2.4.
export default function Structure() {
  const navigate = useNavigate();
  const now = useMemo(() => new Date(), []);
  // Reuse Today's hook just for the just-updated banner. The Structure
  // map itself still renders SAMPLE_COMMITMENTS per the v1 spec, but
  // the banner gives the user feedback that an injected signal landed
  // in the substrate even when no card is warranted.
  const { today, dismissJustUpdated } = useToday();
  const [layer, setLayer] = useState<LayerId>("commits");
  const [layout, setLayout] = useState<LayoutMode>("territory");
  const [color, setColor] = useState<ColorMode>("status");
  const [filters, setFilters] = useState<Filters>(() => ({
    time: "quarter",
    statuses: new Set<CommitmentStatus>([
      "on-track",
      "slipping",
      "at-risk",
      "blocked",
    ]),
    owner: null,
    customer: null,
  }));
  const [activeRef, setActiveRef] = useState<ActiveRefFilter>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [shortcutsOpen, setShortcutsOpen] = useState(false);

  const maxDaysVisible =
    filters.time === "next-7"
      ? 7
      : filters.time === "all"
        ? 365
        : 90;

  // Apply filter pipeline → visible commitments.
  const visibleCommitments = useMemo(() => {
    return SAMPLE_COMMITMENTS.filter((c) => {
      if (!filters.statuses.has(c.status)) return false;
      if (filters.owner && c.owner !== filters.owner) return false;
      if (filters.customer && c.customer !== filters.customer) return false;
      const daysToDue =
        (new Date(c.due_date).getTime() - now.getTime()) / DAY_MS;
      // include overdue regardless of time window
      if (daysToDue > maxDaysVisible) return false;
      return true;
    });
  }, [filters, maxDaysVisible, now]);

  // Pulse a ring on dots whose owner / customer is mentioned in the
  // most recent inbound signal, so the user sees the substrate change
  // reflected on the map (not just in the banner above it).
  const freshlyUpdatedIds = useMemo(
    () => computeFreshlyUpdatedIds(today?.just_updated?.text_html, SAMPLE_COMMITMENTS),
    [today?.just_updated?.text_html]
  );

  // The activeRef from the narrative band dims rather than removes — so
  // it doesn't mutate the visible list, just shades non-matching dots.
  const dimSet = useMemo(() => {
    if (!activeRef) return null;
    const dim = new Set<string>();
    for (const c of visibleCommitments) {
      let match = false;
      if (activeRef.kind === "territory") {
        match = c.territory === activeRef.id;
      } else if (activeRef.kind === "person") {
        match = c.owner === activeRef.id;
      } else if (activeRef.kind === "commitment") {
        match = c.id === activeRef.id;
      } else if (activeRef.kind === "customer") {
        match = c.customer === activeRef.id;
      }
      if (!match) dim.add(c.id);
    }
    return dim;
  }, [activeRef, visibleCommitments]);

  // When a person ref becomes active, color-by switches to Owner per spec.
  useEffect(() => {
    if (activeRef?.kind === "person" && color !== "owner") setColor("owner");
  }, [activeRef, color]);

  // When ref points at a specific commitment, also open the side panel.
  useEffect(() => {
    if (activeRef?.kind === "commitment") setSelectedId(activeRef.id);
  }, [activeRef]);

  const onSwitchLayer = useCallback((id: LayerId) => {
    setLayer(id);
    setSelectedId(null);
    // reset filters per spec 8.4
    setFilters({
      time: "quarter",
      statuses: new Set<CommitmentStatus>([
        "on-track",
        "slipping",
        "at-risk",
        "blocked",
      ]),
      owner: null,
      customer: null,
    });
    setActiveRef(null);
  }, []);

  // Keyboard model: ? shortcuts, 1-5 layers, Esc closes things.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const tag = (document.activeElement as HTMLElement | null)?.tagName;
      const isInput =
        tag === "INPUT" || tag === "TEXTAREA" ||
        (document.activeElement as HTMLElement | null)?.isContentEditable;
      if (e.key === "Escape") {
        if (shortcutsOpen) {
          setShortcutsOpen(false);
          e.preventDefault();
          return;
        }
        if (selectedId) {
          setSelectedId(null);
          e.preventDefault();
          return;
        }
        if (activeRef) {
          setActiveRef(null);
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
      const layerKeys: Record<string, LayerId> = {
        "1": "commits",
        "2": "decisions",
        "3": "people",
        "4": "customers",
        "5": "model",
      };
      if (layerKeys[e.key]) {
        e.preventDefault();
        onSwitchLayer(layerKeys[e.key]);
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [activeRef, onSwitchLayer, selectedId, shortcutsOpen]);

  const selectedCommitment =
    selectedId !== null
      ? SAMPLE_COMMITMENTS.find((c) => c.id === selectedId) ?? null
      : null;

  // Sidebar shell — same nav as Today, but mark Structure active.
  const nav = useMemo(
    () => [
      {
        id: "primary",
        label: "Surfaces",
        items: [
          { id: "today", label: "Today", active: false, href: "/" },
          { id: "structure", label: "Structure", active: true },
          { id: "history", label: "History", active: false },
          { id: "mind", label: "My Mind", shortcut: "M" },
          { id: "communicate", label: "Communicate", disabled: true, badge: "soon" },
        ],
      },
    ],
    []
  );

  // Map-empty conditions
  const mapEmpty =
    visibleCommitments.length === 0
      ? SAMPLE_COMMITMENTS.length === 0
        ? { reason: "no-commitments" as const }
        : {
            reason: "filtered-zero" as const,
            onClear: () => {
              setFilters({
                time: "quarter",
                statuses: new Set([
                  "on-track",
                  "slipping",
                  "at-risk",
                  "blocked",
                ]),
                owner: null,
                customer: null,
              });
              setActiveRef(null);
            },
          }
      : undefined;

  return (
    <>
      <div className="cockpit">
        <Sidebar
          brand={{ name: "Driftwood", mark: "D", pulse_day: 3 }}
          nav={nav}
          vitals={[]}
          onNavigate={(_s, item) => {
            if (item === "today") navigate("/");
            else if (item === "structure") navigate("/structure");
            else if (item === "history") navigate("/history");
            else if (item === "mind") navigate("/mind");
          }}
        />

        <main className="structure-main">
          {today?.just_updated ? (
            <JustUpdated
              text_html={today.just_updated.text_html}
              onDismiss={dismissJustUpdated}
            />
          ) : null}
          <LayerStrip
            active={layer}
            counts={SAMPLE_LAYER_COUNTS}
            onSwitch={onSwitchLayer}
            onShortcuts={() => setShortcutsOpen(true)}
          />

          {layer === "commits" ? (
            <>
              <NarrativeBand
                statement={SAMPLE_SHAPE_STATEMENT}
                commitments={visibleCommitments}
                recentChange={SAMPLE_RECENT_CHANGE}
                onRef={setActiveRef}
                activeRef={activeRef}
              />
              <MapControls
                layout={layout}
                color={color}
                filters={filters}
                ownerOptions={SAMPLE_OWNERS}
                customerOptions={SAMPLE_CUSTOMERS}
                onLayoutChange={setLayout}
                onColorChange={setColor}
                onFiltersChange={setFilters}
              />
              <TerritoryMap
                commitments={visibleCommitments}
                layout={layout}
                color={color}
                maxDaysVisible={maxDaysVisible}
                now={now}
                selectedId={selectedId}
                onSelect={setSelectedId}
                dimNonMatching={dimSet}
                freshlyUpdatedIds={freshlyUpdatedIds}
                emptyState={mapEmpty}
              />
            </>
          ) : (
            <ComingSoonLayer />
          )}
        </main>
      </div>

      <CommitmentPanel
        commitment={selectedCommitment}
        onClose={() => {
          setSelectedId(null);
          if (activeRef?.kind === "commitment") setActiveRef(null);
        }}
        onJumpToCommitment={(id) => setSelectedId(id)}
      />

      {shortcutsOpen ? (
        <ShortcutsOverlay onClose={() => setShortcutsOpen(false)} />
      ) : null}
    </>
  );
}

function ComingSoonLayer() {
  return (
    <div className="layer-coming-soon">
      <p>This layer is coming soon.</p>
      <p>For now, Commitments is the primary view.</p>
    </div>
  );
}
