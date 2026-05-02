import { useMemo, useState } from "react";
import type {
  CalibrationSummary,
  Prediction,
  PredictionDomain,
  PredictionFilter,
} from "./types";

// Spec Part 8 — Predictions layer: filter chips + sortable table +
// calibration summary.

type SortKey = "domain" | "confidence" | "status" | "resolved-date";

type Props = {
  predictions: Prediction[];
  calibration: CalibrationSummary;
  onRowClick: (id: string) => void;
};

export function Predictions({
  predictions,
  calibration,
  onRowClick,
}: Props) {
  const [chip, setChip] = useState<PredictionFilter>("all");
  const [domain, setDomain] = useState<PredictionDomain | "all">("all");
  const [confidenceBand, setConfidenceBand] = useState<
    "all" | "above-80" | "50-80" | "below-50"
  >("all");
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("resolved-date");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const counts = useMemo(() => {
    const total = predictions.length;
    const pending = predictions.filter((p) => p.status === "pending").length;
    const correct = predictions.filter((p) => p.status === "correct").length;
    const wrong = predictions.filter((p) => p.status === "wrong").length;
    return { all: total, pending, correct, wrong };
  }, [predictions]);

  const filtered = useMemo(() => {
    let out = predictions.slice();
    if (chip !== "all") out = out.filter((p) => p.status === chip);
    if (domain !== "all") out = out.filter((p) => p.domain === domain);
    if (confidenceBand !== "all") {
      out = out.filter((p) => {
        const c = p.confidence;
        if (confidenceBand === "above-80") return c >= 0.8;
        if (confidenceBand === "50-80") return c >= 0.5 && c < 0.8;
        return c < 0.5;
      });
    }
    if (search.trim()) {
      const q = search.toLowerCase();
      out = out.filter(
        (p) =>
          p.prediction_text.toLowerCase().includes(q) ||
          p.domain.toLowerCase().includes(q)
      );
    }
    out.sort((a, b) => {
      let cmp = 0;
      if (sortKey === "domain") cmp = a.domain.localeCompare(b.domain);
      else if (sortKey === "confidence") cmp = a.confidence - b.confidence;
      else if (sortKey === "status") cmp = a.status.localeCompare(b.status);
      else if (sortKey === "resolved-date") {
        const ar = a.resolved_on ? new Date(a.resolved_on).getTime() : 0;
        const br = b.resolved_on ? new Date(b.resolved_on).getTime() : 0;
        cmp = ar - br;
      }
      return sortDir === "asc" ? cmp : -cmp;
    });
    return out;
  }, [predictions, chip, domain, confidenceBand, search, sortKey, sortDir]);

  function handleSort(k: SortKey) {
    if (k === sortKey) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortKey(k);
      setSortDir(k === "domain" ? "asc" : "desc");
    }
  }

  if (predictions.length === 0) {
    return (
      <div className="predictions-layer">
        <div className="empty-state-overlay">
          <p className="empty-state-text">
            I haven't made enough predictions yet to interrogate. Predictions
            accumulate as I surface patterns and you act on them.
          </p>
          <p className="empty-state-attribution">— Driftwood</p>
        </div>
      </div>
    );
  }

  return (
    <div className="predictions-layer">
      <div className="predictions-filters">
        <div className="filter-chips">
          {(["all", "pending", "correct", "wrong"] as PredictionFilter[]).map(
            (f) => (
              <button
                key={f}
                type="button"
                className={"filter-chip" + (chip === f ? " active" : "")}
                data-filter={f}
                onClick={() => setChip(f)}
              >
                {f.charAt(0).toUpperCase() + f.slice(1)}{" "}
                <span className="count">{counts[f]}</span>
              </button>
            )
          )}
        </div>
        <div className="filter-secondary">
          <select
            className="filter-select"
            value={domain}
            onChange={(e) => setDomain(e.target.value as PredictionDomain | "all")}
            aria-label="Filter by domain"
          >
            <option value="all">Domain: All</option>
            <option value="patterns">Patterns</option>
            <option value="decisions">Decisions</option>
            <option value="personnel">Personnel</option>
            <option value="customer health">Customer health</option>
            <option value="predictions">Predictions</option>
          </select>
          <select
            className="filter-select"
            value={confidenceBand}
            onChange={(e) => setConfidenceBand(e.target.value as typeof confidenceBand)}
            aria-label="Filter by confidence"
          >
            <option value="all">Confidence: All</option>
            <option value="above-80">Above 80%</option>
            <option value="50-80">50–80%</option>
            <option value="below-50">Below 50%</option>
          </select>
          <input
            type="search"
            className="filter-search"
            placeholder="Search predictions…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </div>

      <table className="predictions-table">
        <thead>
          <tr>
            <SortableHeader
              label="Domain"
              k="domain"
              sortKey={sortKey}
              sortDir={sortDir}
              onSort={handleSort}
            />
            <th>Prediction</th>
            <SortableHeader
              label="Confidence"
              k="confidence"
              sortKey={sortKey}
              sortDir={sortDir}
              onSort={handleSort}
            />
            <SortableHeader
              label="Status"
              k="status"
              sortKey={sortKey}
              sortDir={sortDir}
              onSort={handleSort}
            />
            <SortableHeader
              label="Resolved"
              k="resolved-date"
              sortKey={sortKey}
              sortDir={sortDir}
              onSort={handleSort}
            />
          </tr>
        </thead>
        <tbody>
          {filtered.map((p) => (
            <tr
              key={p.id}
              className="prediction-row"
              data-id={p.id}
              data-status={p.status}
              onClick={() => onRowClick(p.id)}
            >
              <td className="cell-domain">{p.domain}</td>
              <td className="cell-prediction">{p.prediction_text}</td>
              <td className="cell-confidence">
                {Math.round(p.confidence * 100)}%
              </td>
              <td className={"cell-status " + p.status}>
                {p.status === "correct" ? (
                  <>
                    <span className="status-icon">✓</span>correct
                  </>
                ) : p.status === "wrong" ? (
                  <>
                    <span className="status-icon">✗</span>wrong
                  </>
                ) : (
                  "pending"
                )}
              </td>
              <td className="cell-date">
                {p.resolved_on ? formatShortDate(p.resolved_on) : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <CalibrationSummaryBlock cal={calibration} />
    </div>
  );
}

function SortableHeader({
  label,
  k,
  sortKey,
  sortDir,
  onSort,
}: {
  label: string;
  k: SortKey;
  sortKey: SortKey;
  sortDir: "asc" | "desc";
  onSort: (k: SortKey) => void;
}) {
  const active = sortKey === k;
  const cls = active ? (sortDir === "asc" ? "sort-asc" : "sort-desc") : "";
  return (
    <th
      data-sort={k}
      className={cls}
      aria-sort={active ? (sortDir === "asc" ? "ascending" : "descending") : "none"}
      onClick={() => onSort(k)}
    >
      {label}
    </th>
  );
}

function CalibrationSummaryBlock({ cal }: { cal: CalibrationSummary }) {
  return (
    <section className="calibration-summary">
      <header>
        <h2>Calibration summary</h2>
        <span className="cal-overall">
          Overall: <strong>{cal.overall.toFixed(2)}</strong>
        </span>
      </header>
      <div className="cal-domain-list">
        {cal.domains.map((d) => (
          <div className="cal-domain-row" key={d.name}>
            <span className="cal-domain-label">{d.name}</span>
            <div className="cal-bar-track">
              <div
                className="cal-bar-fill"
                style={{ width: `${Math.round(d.score * 100)}%` }}
              />
            </div>
            <span className="cal-fraction">
              {d.correct} / {d.total}
            </span>
            <span className="cal-score">({d.score.toFixed(2)})</span>
          </div>
        ))}
      </div>
      {cal.trend ? (
        <p className="cal-trend">
          Recent trend: <strong>{cal.trend.direction}</strong> (
          {cal.trend.from_score.toFixed(2)} in{" "}
          {new Date(cal.trend.from_date).toLocaleDateString("en-US", {
            month: "long",
          })}{" "}
          → {cal.trend.to_score.toFixed(2)} today)
        </p>
      ) : null}
    </section>
  );
}

function formatShortDate(iso: string): string {
  return new Date(iso)
    .toLocaleDateString("en-US", { month: "short", day: "numeric" })
    .toLowerCase();
}
