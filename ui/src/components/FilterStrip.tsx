type FilterId = "all" | "ops" | "strategic";

type Props = {
  active: FilterId;
  counts: { all: number; ops: number; strategic: number };
  cleared: number;
  onChange: (id: FilterId) => void;
};

// Per spec §3.4 — three tabs (All / Operational / Strategic) with live
// counts. Right side shows "<n> open · <m> cleared today" where cleared
// increments on every triage action.
export function FilterStrip({ active, counts, cleared, onChange }: Props) {
  return (
    <div className="filter-strip">
      <div className="filter-tabs">
        <Tab id="all" label="All" count={counts.all} active={active === "all"} onClick={onChange} />
        <Tab id="ops" label="Operational" count={counts.ops} active={active === "ops"} onClick={onChange} />
        <Tab id="strategic" label="Strategic" count={counts.strategic} active={active === "strategic"} onClick={onChange} />
      </div>
      <div className="filter-state">
        <span><b className="clear-count">{counts.all}</b> open</span>
        <span>·</span>
        <span><b className="clear-count">{cleared}</b> cleared today</span>
      </div>
    </div>
  );
}

function Tab({
  id, label, count, active, onClick,
}: {
  id: FilterId; label: string; count: number; active: boolean; onClick: (id: FilterId) => void;
}) {
  return (
    <button
      className={"filter-tab" + (active ? " active" : "")}
      onClick={() => onClick(id)}
      type="button"
    >
      {label} <span className="count">{count}</span>
    </button>
  );
}

export type { FilterId };
