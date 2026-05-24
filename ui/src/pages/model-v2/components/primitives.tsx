// Shared visual primitives for the Model page (spec §22.3).
//
// Everything here is presentational and prop-driven — no data
// fetching, no global state. Higher-level canvas components compose
// these into the state-specific views (OverviewMap,
// RelationshipCorridor, NodeNeighborhood, TracePath).

import type { ReactNode } from "react";
import type {
  CategoryId,
  ModelCategory,
  ModelItemSummary,
  ModelItemStatus,
  RelationshipBundle,
  RelationshipMode,
  SemanticColorToken,
} from "../types";

// ---------------------------------------------------------------------
// Status helpers
// ---------------------------------------------------------------------

export function statusLabel(s: ModelItemStatus): string {
  switch (s) {
    case "healthy": return "Healthy";
    case "watch": return "Watch";
    case "at_risk": return "At risk";
    case "blocked": return "Blocked";
    case "critical": return "Critical";
    case "contested": return "Contested";
    case "stale": return "Stale";
  }
}

// ---------------------------------------------------------------------
// StatusBeads — distribution glyph (spec §5.6).
// ---------------------------------------------------------------------

export function StatusBeads({
  distribution,
  max = 5,
}: {
  distribution: ModelCategory["statusDistribution"] | undefined;
  max?: number;
}) {
  // We render up to `max` beads, scaled by share of items. If no items
  // in any bucket we still render a placeholder row to keep cards from
  // jumping in height. Defensive against partial API shapes — an
  // undefined distribution becomes empty placeholder beads instead of
  // crashing the page.
  const dist = distribution ?? [];
  const total = dist.reduce((s, b) => s + b.count, 0);
  const ordered = [...dist].sort((a, b) => b.count - a.count);
  const beads: { status: ModelItemStatus; filled: boolean }[] = [];
  let remaining = max;
  for (const bucket of ordered) {
    if (bucket.count <= 0) continue;
    const share = total === 0 ? 0 : Math.round((bucket.count / total) * max);
    const slot = Math.max(1, Math.min(remaining, share || 1));
    for (let i = 0; i < slot; i++) {
      beads.push({ status: bucket.status, filled: true });
      remaining -= 1;
      if (remaining <= 0) break;
    }
    if (remaining <= 0) break;
  }
  while (beads.length < max) {
    beads.push({ status: "healthy", filled: false });
  }
  return (
    <span
      className="fm-beads"
      role="img"
      aria-label={`Status distribution: ${dist
        .filter((b) => b.count > 0)
        .map((b) => `${b.count} ${statusLabel(b.status)}`)
        .join(", ") || "no items"}`}
    >
      {beads.map((b, i) => (
        <span
          key={i}
          className={`fm-beads__dot fm-beads__dot--${b.status} ${
            b.filled ? "is-filled" : "is-empty"
          }`}
        />
      ))}
    </span>
  );
}

// ---------------------------------------------------------------------
// StatusChip — single inline status for micro-cards.
// ---------------------------------------------------------------------

export function StatusChip({ status }: { status: ModelItemStatus }) {
  return (
    <span
      className={`fm-status fm-status--${status}`}
      role="status"
      aria-label={statusLabel(status)}
    >
      <span className="fm-status__dot" aria-hidden="true" />
      <span className="fm-status__label">{statusLabel(status)}</span>
    </span>
  );
}

// ---------------------------------------------------------------------
// Category module — the persistent anchor card (spec §5.3).
// ---------------------------------------------------------------------

export type CategoryModuleProps = {
  category: ModelCategory;
  state?: "normal" | "selected" | "ghosted" | "expanded" | "related";
  onClick?: () => void;
};

export function CategoryModule({
  category,
  state = "normal",
  onClick,
}: CategoryModuleProps) {
  const cls = [
    "fm-cat",
    `fm-cat--${category.colorToken}`,
    state !== "normal" ? `fm-cat--${state}` : "",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <button
      type="button"
      className={cls}
      onClick={onClick}
      aria-label={`${category.label}. ${category.itemCount} active items. ${
        category.blockedCount ?? 0
      } blocked. ${category.changedTodayCount} changed today.`}
      data-testid={`category-${category.id}`}
    >
      <header className="fm-cat__head">
        <CategoryIcon id={category.id} />
        <span className="fm-cat__title">{category.label}</span>
      </header>
      <div className="fm-cat__counts">
        <span>{category.itemCount} active</span>
        {category.blockedCount ? <span>· {category.blockedCount} blocked</span> : null}
        {category.atRiskCount ? <span>· {category.atRiskCount} at risk</span> : null}
      </div>
      <footer className="fm-cat__foot">
        <StatusBeads distribution={category.statusDistribution} />
        <span className="fm-cat__changed">
          {category.changedTodayCount > 0
            ? `${category.changedTodayCount} changed`
            : "stable"}
        </span>
      </footer>
    </button>
  );
}

// ---------------------------------------------------------------------
// Model item micro-card (spec §8.6).
// ---------------------------------------------------------------------

export function ModelItemMicroCard({
  item,
  onClick,
  size = "default",
}: {
  item: ModelItemSummary;
  onClick?: () => void;
  size?: "default" | "compact";
}) {
  return (
    <button
      type="button"
      className={`fm-micro fm-micro--${size}`}
      onClick={onClick}
      data-testid={`micro-${item.id}`}
    >
      <div className="fm-micro__primary">
        <span className="fm-micro__assertion">{item.shortLabel}</span>
        <StatusChip status={item.status} />
      </div>
      <div className="fm-micro__meta">
        {item.owner ? <span>{item.owner}</span> : null}
        {item.relationshipHint ? <span>· {item.relationshipHint}</span> : null}
      </div>
      {item.impactMetric ? (
        <span className="fm-micro__impact">{item.impactMetric}</span>
      ) : null}
    </button>
  );
}

// ---------------------------------------------------------------------
// Relationship label (spec §5.5).
// ---------------------------------------------------------------------

export function RelationshipLabel({
  bundle,
  onClick,
}: {
  bundle: RelationshipBundle;
  onClick?: () => void;
}) {
  // Pre-render a tight "verb · N" line; the inferred-bundle case gets
  // a softer treatment so users can distinguish observed relationships
  // from category-population inferences.
  const countText =
    bundle.synthesized
      ? `~${bundle.instanceCount} ${bundle.targetCategoryId}`
      : `${bundle.instanceCount} ${bundle.targetCategoryId}`;
  return (
    <button
      type="button"
      className={[
        "fm-edgelabel",
        `fm-edgelabel--${bundle.visual.colorToken}`,
        bundle.synthesized ? "fm-edgelabel--synth" : "",
      ]
        .filter(Boolean)
        .join(" ")}
      onClick={onClick}
      data-testid={`bundle-${bundle.id}`}
      aria-label={`${bundle.verb}, ${bundle.instanceCount}${
        bundle.synthesized ? " inferred" : ""
      } ${bundle.targetCategoryId}. ${bundle.impactLabel ?? ""}`.trim()}
    >
      <span className="fm-edgelabel__verb">{bundle.verb}</span>
      <span className="fm-edgelabel__count">{countText}</span>
    </button>
  );
}

// ---------------------------------------------------------------------
// Floating toolbar (spec §10.7).
// ---------------------------------------------------------------------

export function FloatingToolbar({
  onTraceCause,
  onTraceConsequence,
  onCreateDecisionDelta,
  onOpenFullDetail,
  onReportCorrection,
}: {
  onTraceCause?: () => void;
  onTraceConsequence?: () => void;
  onCreateDecisionDelta?: () => void;
  onOpenFullDetail?: () => void;
  onReportCorrection?: () => void;
}) {
  return (
    <div className="fm-toolbar" role="toolbar" aria-label="Model item actions">
      <button type="button" className="fm-toolbar__btn" onClick={onTraceCause}>
        Trace cause
      </button>
      <button type="button" className="fm-toolbar__btn" onClick={onTraceConsequence}>
        Trace consequence
      </button>
      <button type="button" className="fm-toolbar__btn" onClick={onCreateDecisionDelta}>
        Create Decision Delta
      </button>
      <button type="button" className="fm-toolbar__btn" onClick={onOpenFullDetail}>
        Open full detail
      </button>
      <button
        type="button"
        className="fm-toolbar__btn fm-toolbar__btn--ghost"
        onClick={onReportCorrection}
      >
        Report correction
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------
// Breadcrumb
// ---------------------------------------------------------------------

export function Breadcrumb({
  trail,
  onJump,
}: {
  trail: { id: string; label: string }[];
  onJump: (idx: number) => void;
}) {
  return (
    <nav className="fm-crumbs" aria-label="Breadcrumb">
      {trail.map((c, i) => (
        <span key={`${c.id}-${i}`} className="fm-crumbs__item">
          {i > 0 ? <span className="fm-crumbs__sep" aria-hidden="true">›</span> : null}
          <button
            type="button"
            className={`fm-crumbs__link${i === trail.length - 1 ? " is-current" : ""}`}
            onClick={() => onJump(i)}
            disabled={i === trail.length - 1}
          >
            {c.label}
          </button>
        </span>
      ))}
    </nav>
  );
}

// ---------------------------------------------------------------------
// Mode bar — segmented control for relationship modes (spec §4.4).
// ---------------------------------------------------------------------

const MODE_LABELS: Record<RelationshipMode, string> = {
  impact: "Impact",
  dependencies: "Dependencies",
  ownership: "Ownership",
  evidence: "Evidence",
};

const MODE_HINTS: Record<RelationshipMode, string> = {
  impact: "Where is value exposed or created?",
  dependencies: "What blocks, constrains, or supports what?",
  ownership: "Who owns what?",
  evidence: "How grounded is the model?",
};

export function RelationshipModeBar({
  mode,
  onChange,
}: {
  mode: RelationshipMode;
  onChange: (m: RelationshipMode) => void;
}) {
  const modes: RelationshipMode[] = ["impact", "dependencies", "ownership", "evidence"];
  return (
    <div
      className="fm-modebar"
      role="tablist"
      aria-label="Relationship mode"
      data-testid="model-modebar"
    >
      {modes.map((m) => (
        <button
          key={m}
          type="button"
          role="tab"
          aria-selected={mode === m}
          className={`fm-modebar__btn${mode === m ? " is-active" : ""}`}
          onClick={() => onChange(m)}
          data-testid={`mode-${m}`}
          title={MODE_HINTS[m]}
        >
          <ModeIcon id={m} />
          <span>{MODE_LABELS[m]}</span>
        </button>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------
// Header — page title + summary counters + search (spec §4.3).
// ---------------------------------------------------------------------

export function ModelHeader({
  summary,
  searchValue,
  onSearchChange,
  onSearchFocus,
  onHelp,
}: {
  summary: {
    activeItemCount: number;
    changedTodayCount: number;
    blockedCount: number;
    contestedCount: number;
    exposureAtRisk?: number | null;
    lastUpdatedAt: string;
  };
  searchValue: string;
  onSearchChange: (v: string) => void;
  onSearchFocus?: () => void;
  onHelp?: () => void;
}) {
  return (
    <header className="fm-header" data-testid="model-header">
      <div className="fm-header__hero">
        <div className="fm-header__title-block">
          <h1 className="fm-header__title">Model</h1>
          <p className="fm-header__subtitle">
            {summary.activeItemCount > 0
              ? `${summary.activeItemCount.toLocaleString()} active claims across 8 categories. Last updated ${humanRelative(summary.lastUpdatedAt)}.`
              : "Fyralis is building your company model."}
          </p>
        </div>
        <div className="fm-header__controls">
          <label className="fm-search">
            <SearchIcon />
            <input
              type="search"
              value={searchValue}
              onChange={(e) => onSearchChange(e.target.value)}
              onFocus={onSearchFocus}
              placeholder="Ask or search the model…"
              aria-label="Search the model"
              data-testid="model-search"
            />
            <kbd className="fm-search__kbd" aria-hidden="true">⌘K</kbd>
          </label>
          <button
            type="button"
            className="fm-header__help"
            onClick={onHelp}
            aria-label="How to read this model"
            title="How to read this model"
          >
            ?
          </button>
        </div>
      </div>
      <div className="fm-header__counters" aria-label="Model summary">
        <Counter label="active items" value={summary.activeItemCount.toLocaleString()} />
        <Counter label="changed today" value={summary.changedTodayCount} muted={summary.changedTodayCount === 0} />
        <Counter label="blocked" value={summary.blockedCount} muted={summary.blockedCount === 0} tone={summary.blockedCount > 0 ? "garnet" : undefined} />
        <Counter label="contested" value={summary.contestedCount} muted={summary.contestedCount === 0} tone={summary.contestedCount > 0 ? "iris" : undefined} />
        {typeof summary.exposureAtRisk === "number" ? (
          <Counter
            label="exposure"
            value={`$${(summary.exposureAtRisk / 1_000_000).toFixed(2)}M`}
            tone="garnet"
          />
        ) : null}
      </div>
    </header>
  );
}

function humanRelative(iso: string): string {
  try {
    const ts = new Date(iso).getTime();
    const ms = Date.now() - ts;
    if (ms < 60_000) return "just now";
    const m = Math.floor(ms / 60_000);
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    const d = Math.floor(h / 24);
    return `${d}d ago`;
  } catch {
    return "recently";
  }
}

function Counter({
  label,
  value,
  muted,
  tone,
}: {
  label: string;
  value: number | string;
  muted?: boolean;
  tone?: SemanticColorToken;
}) {
  const cls = [
    "fm-counter",
    muted ? "fm-counter--muted" : "",
    tone ? `fm-counter--${tone}` : "",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <div className={cls}>
      <span className="fm-counter__value">{value}</span>
      <span className="fm-counter__label">{label}</span>
    </div>
  );
}

// ---------------------------------------------------------------------
// Tiny icons (no external dep). Keep them small and visually quiet —
// the spec is anti-decorative.
// ---------------------------------------------------------------------

export function CategoryIcon({ id }: { id: CategoryId }): ReactNode {
  const stroke = "currentColor";
  switch (id) {
    case "goals":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <circle cx="8" cy="8" r="5.5" fill="none" stroke={stroke} strokeWidth="1.3" />
          <circle cx="8" cy="8" r="2" fill={stroke} />
        </svg>
      );
    case "commitments":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <path d="M3 4.5h10v7H3z" fill="none" stroke={stroke} strokeWidth="1.3" />
          <path d="M5 7.5h6M5 9.5h4" stroke={stroke} strokeWidth="1.3" />
        </svg>
      );
    case "decisions":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <path d="M8 2.5l5 5-5 5-5-5z" fill="none" stroke={stroke} strokeWidth="1.3" />
          <circle cx="8" cy="7.5" r="0.8" fill={stroke} />
        </svg>
      );
    case "risks":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <path d="M8 2.5l6 10.5H2z" fill="none" stroke={stroke} strokeWidth="1.3" />
          <path d="M8 6v3.5" stroke={stroke} strokeWidth="1.3" />
          <circle cx="8" cy="11" r="0.7" fill={stroke} />
        </svg>
      );
    case "customers":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <circle cx="6" cy="6" r="2.2" fill="none" stroke={stroke} strokeWidth="1.3" />
          <path d="M2.5 12c.5-2 2-3 3.5-3s3 1 3.5 3" fill="none" stroke={stroke} strokeWidth="1.3" />
          <circle cx="11.5" cy="7" r="1.5" fill="none" stroke={stroke} strokeWidth="1.3" />
        </svg>
      );
    case "people":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <circle cx="5" cy="6" r="2" fill="none" stroke={stroke} strokeWidth="1.3" />
          <circle cx="11" cy="6" r="2" fill="none" stroke={stroke} strokeWidth="1.3" />
          <path d="M2 12.5c.6-1.6 2-2.5 3.5-2.5M14 12.5c-.6-1.6-2-2.5-3.5-2.5" fill="none" stroke={stroke} strokeWidth="1.3" />
        </svg>
      );
    case "systems":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <rect x="3" y="3.5" width="10" height="3" rx="0.6" fill="none" stroke={stroke} strokeWidth="1.3" />
          <rect x="3" y="9.5" width="10" height="3" rx="0.6" fill="none" stroke={stroke} strokeWidth="1.3" />
          <circle cx="5" cy="5" r="0.6" fill={stroke} />
          <circle cx="5" cy="11" r="0.6" fill={stroke} />
        </svg>
      );
    case "finance":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <circle cx="8" cy="8" r="5.5" fill="none" stroke={stroke} strokeWidth="1.3" />
          <path d="M6 5.5h3.5c1 0 1.5.6 1.5 1.5s-.5 1.5-1.5 1.5H6m0 0v2h3M6 8.5h3" stroke={stroke} strokeWidth="1.3" fill="none" />
        </svg>
      );
  }
}

function ModeIcon({ id }: { id: RelationshipMode }): ReactNode {
  switch (id) {
    case "impact":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <circle cx="5" cy="8" r="2.5" fill="currentColor" opacity="0.55" />
          <circle cx="11" cy="8" r="3.5" fill="currentColor" opacity="0.25" />
        </svg>
      );
    case "dependencies":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <path d="M3 4l5 4 5-4M3 12l5-4 5 4" fill="none" stroke="currentColor" strokeWidth="1.4" />
        </svg>
      );
    case "ownership":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <circle cx="8" cy="5" r="2" fill="none" stroke="currentColor" strokeWidth="1.3" />
          <path d="M3 13c.5-2.5 2.5-3.5 5-3.5s4.5 1 5 3.5" fill="none" stroke="currentColor" strokeWidth="1.3" />
        </svg>
      );
    case "evidence":
      return (
        <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
          <path d="M3 4h10v8H3z" fill="none" stroke="currentColor" strokeWidth="1.3" />
          <path d="M5 7h6M5 9.5h4" stroke="currentColor" strokeWidth="1.2" />
        </svg>
      );
  }
}

function SearchIcon() {
  return (
    <svg className="fm-icon" viewBox="0 0 16 16" aria-hidden="true">
      <circle cx="7" cy="7" r="4" fill="none" stroke="currentColor" strokeWidth="1.4" />
      <path d="M10.5 10.5L13.5 13.5" stroke="currentColor" strokeWidth="1.4" />
    </svg>
  );
}
