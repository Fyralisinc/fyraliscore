// Forecast Horizon Matrix (spec §12) — domains × horizons grid of
// forecast cards. Domain rows are fixed; horizon columns come from
// the payload so the time window can be changed without UI churn.

import type {
  ForecastHorizonData,
  ForecastSummaryCard,
} from "@/api/forecasts-types";
import { ForecastCard } from "./ForecastCard";

export interface HorizonMatrixProps {
  data: ForecastHorizonData | null;
  selectedId: string | null;
  onSelect: (id: string) => void;
}

export function HorizonMatrix({ data, selectedId, onSelect }: HorizonMatrixProps) {
  if (!data) {
    return (
      <section className="fc-horizon fc-horizon--loading" aria-label="Forecast Horizon Matrix">
        <div className="fc-horizon__skeleton" />
      </section>
    );
  }
  return (
    <section className="fc-horizon" aria-label="Forecast Horizon Matrix">
      <header className="fc-horizon__head">
        <span className="fc-micro-label">Forecast Horizon</span>
        <h2 className="fc-horizon__title">
          Where the future is forming, by domain and time
        </h2>
      </header>

      <div
        className="fc-horizon__grid"
        style={{
          gridTemplateColumns: `132px repeat(${data.horizons.length}, minmax(0, 1fr))`,
        }}
        role="grid"
      >
        <div className="fc-horizon__corner" role="presentation" />
        {data.horizons.map((h) => (
          <div key={h.id} className="fc-horizon__colhead" role="columnheader">
            {h.label}
          </div>
        ))}

        {data.domains.map((row) => (
          <DomainRow
            key={row.id}
            row={row}
            selectedId={selectedId}
            onSelect={onSelect}
          />
        ))}
      </div>
    </section>
  );
}

function DomainRow({
  row,
  selectedId,
  onSelect,
}: {
  row: { id: string; label: string; cells: { horizon_id: string; forecasts: ForecastSummaryCard[]; hidden_count: number }[] };
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  return (
    <>
      <div className="fc-horizon__rowhead" role="rowheader">
        {row.label}
      </div>
      {row.cells.map((cell) => (
        <div key={cell.horizon_id} className="fc-horizon__cell" role="gridcell">
          {cell.forecasts.length === 0 ? (
            <div className="fc-horizon__cell-empty">—</div>
          ) : (
            <>
              {cell.forecasts.map((f) => (
                <ForecastCard
                  key={f.id}
                  forecast={f}
                  selected={selectedId === f.id}
                  onSelect={onSelect}
                />
              ))}
              {cell.hidden_count > 0 ? (
                <div className="fc-horizon__more">+{cell.hidden_count} more</div>
              ) : null}
            </>
          )}
        </div>
      ))}
    </>
  );
}
