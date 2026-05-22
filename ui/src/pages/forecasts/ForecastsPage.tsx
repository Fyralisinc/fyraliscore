// Forecasts page root — spec v1.0 implementation.
// Spec source: /fyralis_forecasts_page_implementation_complete_spec_v1.md
//
// Page shape (Horizon mode default):
//   AppShell.sidebar  → primary nav
//   AppShell.main     → ForecastsHeader
//                       ForesightBrief
//                       ModeSelector
//                       <ModeBody>
//                       AccuracyStrip
//
// State: load the page payload once, drive mode + selection from URL.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { AppShell } from "@/shell/AppShell";
import { Sidebar } from "@/shell/Sidebar";
import { useForecastsPage } from "@/hooks/useForecastsPage";
import type { ForecastMode } from "@/api/forecasts-types";
import { ForecastsHeader } from "@/components/forecasts/ForecastsHeader";
import { ForesightBrief } from "@/components/forecasts/ForesightBrief";
import { ModeSelector } from "@/components/forecasts/ModeSelector";
import { HorizonMode } from "@/components/forecasts/HorizonMode";
import { PatternsMode } from "@/components/forecasts/PatternsMode";
import { ScenariosMode } from "@/components/forecasts/ScenariosMode";
import { AccuracyMode } from "@/components/forecasts/AccuracyMode";
import { AccuracyStrip } from "@/components/forecasts/AccuracyStrip";

const VALID_MODES: ReadonlySet<ForecastMode> = new Set([
  "horizon", "patterns", "scenarios", "accuracy",
]);

export default function ForecastsPage() {
  const [params, setParams] = useSearchParams();
  const [horizonDays, setHorizonDays] = useState(() => {
    const v = Number(params.get("horizon") ?? "90");
    return Number.isFinite(v) && v >= 14 ? v : 90;
  });

  const page = useForecastsPage(horizonDays);

  // Mode driven by URL ?mode= with safe fallback. Switching mode
  // updates the URL so deep-linking + reload preserve state.
  const mode = useMemo<ForecastMode>(() => {
    const raw = params.get("mode") as ForecastMode | null;
    return raw && VALID_MODES.has(raw) ? raw : "horizon";
  }, [params]);

  const setMode = useCallback(
    (m: ForecastMode) => {
      const next = new URLSearchParams(params);
      if (m === "horizon") next.delete("mode");
      else next.set("mode", m);
      setParams(next, { replace: false });
    },
    [params, setParams],
  );

  // Selected forecast id — URL-driven for share-ability.
  const urlSelected = params.get("forecast");
  useEffect(() => {
    if (urlSelected && urlSelected !== page.selectedId) {
      page.selectForecast(urlSelected);
    }
    // We deliberately don't depend on page.selectedId — that's the
    // initial server-side default, set once. URL is the source of
    // truth from then on.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [urlSelected]);

  const handleSelect = useCallback(
    (id: string) => {
      page.selectForecast(id);
      const next = new URLSearchParams(params);
      next.set("forecast", id);
      setParams(next, { replace: false });
    },
    [page, params, setParams],
  );

  const handleHorizonChange = useCallback(
    (days: number) => {
      setHorizonDays(days);
      const next = new URLSearchParams(params);
      if (days === 90) next.delete("horizon");
      else next.set("horizon", String(days));
      setParams(next, { replace: true });
    },
    [params, setParams],
  );

  const handleAskClick = useCallback(() => {
    // For now: jump to the inspector by selecting the default
    // forecast if none is selected. The inspector itself contains
    // the Ask Fyralis input.
    if (!page.selectedId && page.payload?.selected_forecast_id) {
      handleSelect(page.payload.selected_forecast_id);
    }
    // Try to focus the ask input.
    requestAnimationFrame(() => {
      const el = document.querySelector<HTMLInputElement>(
        ".fc-inspector .fc-ask__input",
      );
      el?.focus();
    });
  }, [page.selectedId, page.payload, handleSelect]);

  const selectedDetail =
    page.selectedId ? page.detailById[page.selectedId] ?? null : null;

  // Render based on state.
  const body = renderBody({
    mode,
    page,
    selectedDetail,
    onSelectForecast: handleSelect,
    onSelectPattern: (_id: string) => setMode("patterns"),
    horizonDays,
  });

  return (
    <div className="fc-page" data-mode={mode}>
      <AppShell
        sidebarMode="collapsed"
        sidebar={<Sidebar activeRoute="forecasts" mode="collapsed" />}
        main={
          <div className="fc-main">
            <ForecastsHeader
              header={page.payload?.header ?? null}
              horizonDays={horizonDays}
              onHorizonChange={handleHorizonChange}
              onAskClick={handleAskClick}
            />

            {page.phase === "error" ? (
              <ErrorState message={page.error} onRetry={page.refresh} />
            ) : (
              <>
                <ForesightBrief
                  brief={page.payload?.foresight_brief ?? null}
                  onSelectForecast={handleSelect}
                  onSelectInterventionForecast={handleSelect}
                />

                <ModeSelector mode={mode} onChange={setMode} />

                {page.phase === "empty" ? (
                  <EmptyState />
                ) : (
                  body
                )}

                <AccuracyStrip
                  summary={page.payload?.accuracy ?? null}
                  full={page.accuracy}
                  onOpenAccuracy={() => setMode("accuracy")}
                />
              </>
            )}
          </div>
        }
      />
    </div>
  );
}

interface BodyArgs {
  mode: ForecastMode;
  page: ReturnType<typeof useForecastsPage>;
  selectedDetail: ReturnType<typeof useForecastsPage>["detailById"][string] | null;
  onSelectForecast: (id: string) => void;
  onSelectPattern: (id: string) => void;
  horizonDays: number;
}

function renderBody({
  mode,
  page,
  selectedDetail,
  onSelectForecast,
  onSelectPattern,
  horizonDays,
}: BodyArgs) {
  switch (mode) {
    case "horizon":
      return (
        <HorizonMode
          payload={page.payload}
          selectedId={page.selectedId}
          detail={selectedDetail}
          detailPending={page.detailPending}
          patterns={page.patterns}
          onSelect={onSelectForecast}
          onSelectPattern={onSelectPattern}
          horizonDays={horizonDays}
        />
      );
    case "patterns":
      return (
        <PatternsMode
          payload={page.payload}
          patterns={page.patterns}
          onSelectForecast={onSelectForecast}
          horizonDays={horizonDays}
        />
      );
    case "scenarios":
      return (
        <ScenariosMode
          selectedForecast={selectedDetail}
          onSelectForecast={onSelectForecast}
          horizonDays={horizonDays}
        />
      );
    case "accuracy":
      return (
        <AccuracyMode
          summary={page.payload?.accuracy ?? null}
          accuracy={page.accuracy}
          payload={page.payload}
        />
      );
  }
}

function EmptyState() {
  return (
    <section className="fc-empty-state">
      <h2>No active forecasts right now.</h2>
      <p>
        Fyralis is still monitoring leading indicators. Patterns will appear
        here as evidence accumulates across sources.
      </p>
      <div className="fc-empty-state__actions">
        <a className="fc-link" href="/model">Open Model</a>
        <a className="fc-link" href="/ledger">View Ledger</a>
      </div>
    </section>
  );
}

function ErrorState({
  message,
  onRetry,
}: {
  message: string | null;
  onRetry: () => void;
}) {
  return (
    <section className="fc-error-state">
      <h2>Forecasts could not be loaded.</h2>
      {message ? <p className="fc-error-state__detail">{message}</p> : null}
      <div className="fc-error-state__actions">
        <button type="button" className="fc-btn" onClick={onRetry}>
          Try again
        </button>
        <a className="fc-link" href="/model">Open Model</a>
      </div>
    </section>
  );
}
