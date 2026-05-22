// Lightweight global store for the spec-aligned product. Keeps cross-
// page state (selected entity, palette open/closed, trust stage, last
// fetched threads) in one place so deep-linking between Today and Model
// doesn't have to re-fetch.

import { create } from "zustand";
import { subscribeWithSelector } from "zustand/middleware";

import type { ID } from "@/api/common-types";
import type {
  OperatingThread,
  RecentModelChange,
} from "@/api/operating-thread-types";
import type { SpecDelta } from "@/api/spec-delta-types";
import type { SpecForecast } from "@/api/spec-forecast-types";
import type { SpecLedgerEvent } from "@/api/ledger-event-types";
import {
  RECENT_MODEL_CHANGES_FIXTURE,
  SPEC_DELTAS_FIXTURE,
  SPEC_FORECASTS_FIXTURE,
  SPEC_LEDGER_EVENTS_FIXTURE,
  SPEC_THREADS_FIXTURE,
} from "@/api/spec-mocks";

// Tenant trust stage drives the user-facing naming of Decision Deltas
// (spec §5.5). Defaults to `low` per the user's instruction.
export type TrustStage = "low" | "intermediate" | "mature";

export interface CrossPageSelection {
  // Last clicked thread (used when /model is opened from /today).
  threadId?: ID;
  deltaId?: ID;
  forecastId?: ID;
  ledgerEventId?: ID;
}

interface FyralisState {
  // ── Cross-page selection ──
  selection: CrossPageSelection;
  setSelection: (s: Partial<CrossPageSelection>) => void;
  clearSelection: () => void;

  // ── Palette ──
  paletteOpen: boolean;
  setPaletteOpen: (open: boolean) => void;

  // ── Trust stage ──
  trustStage: TrustStage;
  setTrustStage: (s: TrustStage) => void;

  // ── Cached data (last fetched). These are seeded with fixtures so
  // the UI never flashes empty during route transitions. Background
  // hooks rehydrate on mount.
  threads: OperatingThread[];
  deltas: SpecDelta[];
  forecasts: SpecForecast[];
  ledgerEvents: SpecLedgerEvent[];
  recentChanges: RecentModelChange[];
  setThreads: (xs: OperatingThread[]) => void;
  setDeltas: (xs: SpecDelta[]) => void;
  setForecasts: (xs: SpecForecast[]) => void;
  setLedgerEvents: (xs: SpecLedgerEvent[]) => void;
  setRecentChanges: (xs: RecentModelChange[]) => void;

  // Optimistic delta mutation — used by Today actions to immediately
  // update the queue while the backend mutation lands.
  patchDelta: (id: string, patch: Partial<SpecDelta>) => void;
  removeDelta: (id: string) => void;
}

export const useFyralisStore = create<FyralisState>()(
  subscribeWithSelector((set) => ({
    selection: {},
    setSelection: (s) => set((state) => ({ selection: { ...state.selection, ...s } })),
    clearSelection: () => set({ selection: {} }),

    paletteOpen: false,
    setPaletteOpen: (open) => set({ paletteOpen: open }),

    trustStage: "low",
    setTrustStage: (s) => set({ trustStage: s }),

    threads: SPEC_THREADS_FIXTURE,
    deltas: SPEC_DELTAS_FIXTURE,
    forecasts: SPEC_FORECASTS_FIXTURE,
    ledgerEvents: SPEC_LEDGER_EVENTS_FIXTURE,
    recentChanges: RECENT_MODEL_CHANGES_FIXTURE,

    setThreads: (xs) => set({ threads: xs }),
    setDeltas: (xs) => set({ deltas: xs }),
    setForecasts: (xs) => set({ forecasts: xs }),
    setLedgerEvents: (xs) => set({ ledgerEvents: xs }),
    setRecentChanges: (xs) => set({ recentChanges: xs }),

    patchDelta: (id, patch) =>
      set((state) => ({
        deltas: state.deltas.map((d) => (d.id === id ? { ...d, ...patch } : d)),
      })),

    removeDelta: (id) =>
      set((state) => ({
        deltas: state.deltas.filter((d) => d.id !== id),
      })),
  }))
);

// Adaptive label per trust stage (spec §5.5). Internal name is always
// "Decision Delta"; the surface name varies.
export function deltaFacingType(stage: TrustStage): SpecDelta["userFacingType"] {
  if (stage === "mature") return "Decision Delta";
  if (stage === "intermediate") return "Recommended Change";
  return "Proposed Change";
}
