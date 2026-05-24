// Horizon Mode (default). Composes the Forecast Horizon Matrix +
// Foresight Inspector + Pattern Field. The Accuracy Strip is rendered
// by the page root since it sits at the very bottom across modes.

import type { ForecastsPagePayload, ForecastDetail, PatternCard } from "@/api/forecasts-types";
import { HorizonMatrix } from "./HorizonMatrix";
import { ForesightInspector } from "./ForesightInspector";
import { PatternField } from "./PatternField";

export interface HorizonModeProps {
  payload: ForecastsPagePayload | null;
  selectedId: string | null;
  detail: ForecastDetail | null;
  detailPending: boolean;
  patterns: PatternCard[];
  onSelect: (id: string) => void;
  onSelectPattern: (id: string) => void;
  horizonDays: number;
}

export function HorizonMode({
  payload,
  selectedId,
  detail,
  detailPending,
  patterns,
  onSelect,
  onSelectPattern,
  horizonDays,
}: HorizonModeProps) {
  const visibleIds: string[] =
    payload?.horizon.domains.flatMap((d) =>
      d.cells.flatMap((c) => c.forecasts.map((f) => f.id)),
    ) ?? [];

  return (
    <>
      <div className="fc-workspace">
        <div className="fc-workspace__left">
          <HorizonMatrix
            data={payload?.horizon ?? null}
            selectedId={selectedId}
            onSelect={onSelect}
          />
          <PatternField
            patterns={patterns}
            onSelectPattern={onSelectPattern}
          />
        </div>
        <div className="fc-workspace__right">
          <ForesightInspector
            detail={detail}
            pending={detailPending}
            visibleForecastIds={visibleIds}
            horizonDays={horizonDays}
          />
        </div>
      </div>
    </>
  );
}
