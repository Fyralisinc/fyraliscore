import { useMemo } from "react";
import type {
  StructureResourceAggregate,
  ResourceHealth,
} from "@/api/structure-client";

// Resource portfolio dashboard. Replaces the relational graph when the
// user picks "Resources only" in the entity-kind filter and nothing is
// focused. Shows three synthesizing callouts (over-allocated, under-
// utilized, customer-pressure) and per-kind utilization cards.

type Props = {
  resources: StructureResourceAggregate[];
  onFocus: (resourceId: string) => void;
};

const KIND_LABEL: Record<StructureResourceAggregate["kind"], string> = {
  human: "Human pods",
  financial: "Financial pools",
  technical: "Technical platforms",
  time: "Time pools",
};

const KIND_ORDER: StructureResourceAggregate["kind"][] = [
  "human",
  "technical",
  "financial",
  "time",
];

const HEALTH_LABEL: Record<ResourceHealth, string> = {
  "available": "Available",
  "under-utilized": "Under-utilized",
  "deployed": "Deployed",
  "constrained": "Constrained",
  "over-allocated": "Over-allocated",
};

function formatQuantity(value: number, unit: string): string {
  const u = (unit || "").toLowerCase();
  if (u.includes("usd")) {
    if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
    if (value >= 1_000) return `$${Math.round(value / 1_000)}k`;
    return `$${Math.round(value)}`;
  }
  if (u.includes("fte")) return `${value.toFixed(1)} FTE`;
  if (!unit) return value.toFixed(2);
  return `${Math.round(value)} ${unit}`;
}

export function ResourceAggregateView({ resources, onFocus }: Props) {
  // Synthesizing callouts — pre-compute up to 3 punchy bullet items.
  const callouts = useMemo(() => {
    const overAllocated = resources
      .filter((r) => r.utilization_pct >= 100)
      .sort((a, b) => b.utilization_pct - a.utilization_pct)
      .slice(0, 2);
    const underUtilized = resources
      .filter((r) => r.utilization_pct > 0 && r.utilization_pct < 50 && r.kind !== "financial")
      .sort((a, b) => a.utilization_pct - b.utilization_pct)
      .slice(0, 2);
    const constrained = resources
      .filter((r) => r.utilization_pct >= 85 && r.utilization_pct < 100)
      .sort((a, b) => b.utilization_pct - a.utilization_pct)
      .slice(0, 2);
    return { overAllocated, underUtilized, constrained };
  }, [resources]);

  const grouped = useMemo(() => {
    const m: Record<StructureResourceAggregate["kind"], StructureResourceAggregate[]> = {
      human: [], financial: [], technical: [], time: [],
    };
    for (const r of resources) m[r.kind].push(r);
    for (const k of Object.keys(m) as (keyof typeof m)[]) {
      m[k].sort((a, b) => b.utilization_pct - a.utilization_pct);
    }
    return m;
  }, [resources]);

  const totalActive = resources.reduce(
    (acc, r) => acc + r.deployments_count, 0,
  );
  const avgUtil =
    resources.length === 0
      ? 0
      : resources.reduce((acc, r) => acc + r.utilization_pct, 0) / resources.length;

  return (
    <div className="resource-agg" aria-label="Resource portfolio">
      <header className="resource-agg-head">
        <div>
          <h2>Resource portfolio</h2>
          <p className="resource-agg-sub">
            {resources.length} capacity resources · {totalActive} active deployments ·
            {" "}
            {avgUtil.toFixed(0)}% avg utilization
          </p>
        </div>
      </header>

      {(callouts.overAllocated.length > 0 ||
        callouts.underUtilized.length > 0 ||
        callouts.constrained.length > 0) ? (
        <section className="resource-agg-callouts">
          {callouts.overAllocated.length > 0 ? (
            <CalloutCard
              tone="warn"
              title="Over-allocated"
              hint="Capacity claimed exceeds what the pool can sustain. Pause new commitments or rebalance."
              items={callouts.overAllocated}
              onFocus={onFocus}
            />
          ) : null}
          {callouts.constrained.length > 0 ? (
            <CalloutCard
              tone="caution"
              title="Constrained"
              hint="Tight headroom — a single unplanned ask will spill into over-allocation."
              items={callouts.constrained}
              onFocus={onFocus}
            />
          ) : null}
          {callouts.underUtilized.length > 0 ? (
            <CalloutCard
              tone="opportunity"
              title="Under-utilized"
              hint="Idle capacity. Roadmap items waiting on the over-allocated pools could shift here."
              items={callouts.underUtilized}
              onFocus={onFocus}
            />
          ) : null}
        </section>
      ) : null}

      <div className="resource-agg-groups">
        {KIND_ORDER.map((kind) => {
          const list = grouped[kind];
          if (list.length === 0) return null;
          return (
            <section key={kind} className="resource-agg-group">
              <h3>{KIND_LABEL[kind]}</h3>
              <ul className="resource-agg-list">
                {list.map((r) => (
                  <ResourceRow key={r.id} r={r} onFocus={onFocus} />
                ))}
              </ul>
            </section>
          );
        })}
      </div>
    </div>
  );
}

function CalloutCard({
  tone, title, hint, items, onFocus,
}: {
  tone: "warn" | "caution" | "opportunity";
  title: string;
  hint: string;
  items: StructureResourceAggregate[];
  onFocus: (id: string) => void;
}) {
  return (
    <article className={`resource-callout resource-callout-${tone}`}>
      <h4>{title}</h4>
      <p className="resource-callout-hint">{hint}</p>
      <ul>
        {items.map((r) => (
          <li key={r.id}>
            <button
              type="button"
              className="resource-callout-link"
              onClick={() => onFocus(r.id)}
            >
              <span className="resource-callout-label">{r.label}</span>
              <span className="resource-callout-pct">
                {Math.round(r.utilization_pct)}%
              </span>
            </button>
          </li>
        ))}
      </ul>
    </article>
  );
}

function ResourceRow({
  r, onFocus,
}: {
  r: StructureResourceAggregate;
  onFocus: (id: string) => void;
}) {
  const pct = Math.max(0, Math.min(150, r.utilization_pct));
  const barWidth = Math.min(100, pct);
  const overflow = pct > 100 ? Math.min(50, pct - 100) : 0;
  const health = r.health;

  return (
    <li>
      <button
        type="button"
        className="resource-row"
        onClick={() => onFocus(r.id)}
        title={r.description}
      >
        <div className="resource-row-head">
          <span className="resource-row-label">{r.label}</span>
          <span className={`resource-row-health resource-row-health-${health}`}>
            {HEALTH_LABEL[health]}
          </span>
        </div>
        <div className="resource-row-meta">
          <span>
            {formatQuantity(r.deployed, r.unit)} of {formatQuantity(r.capacity, r.unit)}
          </span>
          <span>·</span>
          <span>{r.deployments_count} commitment{r.deployments_count === 1 ? "" : "s"}</span>
          <span>·</span>
          <span className="resource-row-pct">{Math.round(r.utilization_pct)}%</span>
        </div>
        <div className="resource-row-bar" aria-hidden>
          <div
            className={`resource-row-bar-fill resource-row-bar-${health}`}
            style={{ width: `${barWidth}%` }}
          />
          {overflow > 0 ? (
            <div
              className="resource-row-bar-overflow"
              style={{ width: `${overflow}%` }}
            />
          ) : null}
        </div>
      </button>
    </li>
  );
}
