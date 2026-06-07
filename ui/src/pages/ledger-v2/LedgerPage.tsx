// Ledger page — spec v1.0 (Memory River + Chain Inspector).
// Spec source: /fyralis_ledger_page_implementation_spec_v1.md
//
// Page shape:
//   AppShell.sidebar  → primary nav
//   AppShell.main     → LedgerHeader
//                       LedgerBrief
//                       ModeSelector
//                       <mode body>
//
// Mode bodies:
//   timeline    → MemoryRiver | ChainInspector
//   resolutions → outcome-bucketed chain lists
//   accuracy    → calibration view
//   audit       → raw event table

import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { AppShell } from "@/shell/AppShell";
import { Sidebar } from "@/shell/Sidebar";
import { useAskOverlay } from "@/ask/AskOverlayProvider";

import { LEDGER_PAGE_FIXTURE } from "@/api/ledger-v2-mock";
import type {
  LedgerChainDetail,
  LedgerMode,
} from "@/api/ledger-v2-types";

import { LedgerHeader } from "@/components/ledger-v2/LedgerHeader";
import { LedgerBrief } from "@/components/ledger-v2/LedgerBrief";
import { ModeSelector } from "@/components/ledger-v2/ModeSelector";
import { MemoryRiver } from "@/components/ledger-v2/MemoryRiver";
import { ChainInspector } from "@/components/ledger-v2/ChainInspector";
import { ResolutionsMode } from "@/components/ledger-v2/ResolutionsMode";
import { AccuracyMode } from "@/components/ledger-v2/AccuracyMode";
import { AuditMode } from "@/components/ledger-v2/AuditMode";

import "@/styles/ledger.css";

const VALID_MODES: ReadonlySet<LedgerMode> = new Set([
  "timeline",
  "resolutions",
  "accuracy",
  "audit",
]);

// Fixture anchor — keep "Today" aligned to the screenshot copy.
const FIXTURE_NOW = new Date("2026-05-18T12:00:00.000Z");

export default function LedgerPage() {
  const [params, setParams] = useSearchParams();
  const { openAsk } = useAskOverlay();
  const payload = LEDGER_PAGE_FIXTURE;

  const mode = useMemo<LedgerMode>(() => {
    const raw = params.get("mode") as LedgerMode | null;
    return raw && VALID_MODES.has(raw) ? raw : "timeline";
  }, [params]);

  const setMode = useCallback(
    (m: LedgerMode) => {
      const next = new URLSearchParams(params);
      if (m === "timeline") next.delete("mode");
      else next.set("mode", m);
      setParams(next, { replace: false });
    },
    [params, setParams],
  );

  const urlChain = params.get("chain");
  const [selectedId, setSelectedId] = useState<string>(
    urlChain && payload.chainDetails[urlChain]
      ? urlChain
      : payload.defaultSelectedChainId,
  );

  useEffect(() => {
    if (urlChain && urlChain !== selectedId && payload.chainDetails[urlChain]) {
      setSelectedId(urlChain);
    }
  }, [urlChain, selectedId, payload.chainDetails]);

  const onSelectChain = useCallback(
    (id: string) => {
      setSelectedId(id);
      const next = new URLSearchParams(params);
      next.set("chain", id);
      setParams(next, { replace: false });
      requestAnimationFrame(() => {
        const heading = document.getElementById("lg-inspector-title");
        heading?.focus?.();
      });
    },
    [params, setParams],
  );

  const detail: LedgerChainDetail = payload.chainDetails[selectedId] ??
    payload.chainDetails[payload.defaultSelectedChainId];

  return (
    <div className="lg-page" data-mode={mode}>
      <AppShell
        sidebarMode="collapsed"
        sidebar={<Sidebar activeRoute="ledger" mode="collapsed" />}
        main={
          <div className="lg-main">
            <LedgerHeader data={payload.header} onAskClick={() => openAsk()} />
            <LedgerBrief data={payload.brief} />
            <ModeSelector mode={mode} onChange={setMode} />

            {mode === "timeline" ? (
              <div className="lg-workspace">
                <MemoryRiver
                  chains={payload.chains}
                  selectedId={selectedId}
                  onSelect={onSelectChain}
                  now={FIXTURE_NOW}
                />
                <ChainInspector
                  detail={detail}
                  onOpenAccuracy={() => setMode("accuracy")}
                />
              </div>
            ) : null}

            {mode === "resolutions" ? (
              <ResolutionsMode
                chains={payload.chains}
                selectedId={selectedId}
                onSelect={(id) => {
                  onSelectChain(id);
                  setMode("timeline");
                }}
              />
            ) : null}

            {mode === "accuracy" ? (
              <AccuracyMode
                summary={payload.accuracy}
                onOpenChain={(id) => {
                  onSelectChain(id);
                  setMode("timeline");
                }}
              />
            ) : null}

            {mode === "audit" ? (
              <AuditMode
                events={payload.auditEvents}
                onOpenChain={(id) => {
                  onSelectChain(id);
                  setMode("timeline");
                }}
              />
            ) : null}
          </div>
        }
      />
    </div>
  );
}
