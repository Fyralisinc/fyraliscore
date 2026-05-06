import { useMemo } from "react";
import type {
  Commitment,
  EntityKind,
  FocusTarget,
  GoalRef,
  PersonProfile,
} from "./types";

type Props = {
  commitments: Commitment[];
  goals: GoalRef[];
  people: PersonProfile[];
  entityKind: EntityKind;
  focus: FocusTarget | null;
  onFocus: (target: FocusTarget) => void;
  onHover: (id: string | null) => void;
};

const STATUS_GLYPH: Record<Commitment["status"], string> = {
  "on-track": "●",
  slipping: "●",
  "at-risk": "●",
  blocked: "▨",
};

// Left rail. Two flat sections: Goals + Commitments. No territory
// grouping. Either section can be hidden by the entity-kind filter.
// Selecting a row drives the relational graph's focus.
export function CommitmentList({
  commitments,
  goals,
  people,
  entityKind,
  focus,
  onFocus,
  onHover,
}: Props) {
  // Per-goal aggregates so each goal row can show its load.
  const goalAgg = useMemo(() => {
    const map = new Map<string, { total: number; off: number }>();
    for (const g of goals) map.set(g.id, { total: 0, off: 0 });
    for (const c of commitments) {
      for (const gid of c.edges?.contributes_to ?? []) {
        const a = map.get(gid);
        if (!a) continue;
        a.total += 1;
        if (c.status !== "on-track") a.off += 1;
      }
    }
    return map;
  }, [goals, commitments]);

  // Sort commitments off-track first, then by due date.
  const sortedCommits = useMemo(() => {
    return [...commitments].sort((a, b) => {
      const aw = a.status === "on-track" ? 1 : 0;
      const bw = b.status === "on-track" ? 1 : 0;
      if (aw !== bw) return aw - bw;
      return new Date(a.due_date).getTime() - new Date(b.due_date).getTime();
    });
  }, [commitments]);

  // Build the goal tree: strategic at the top, operational children
  // grouped under their parent, orphan operationals (no parent) at the
  // end. Strategic agg rolls up its operational children's totals so
  // the parent row shows the full load, not just direct contributes_to
  // (which strategics rarely have).
  type GoalTreeRow = {
    goal: GoalRef;
    children: GoalRef[];
    agg: { total: number; off: number };
  };
  const goalTree = useMemo<GoalTreeRow[]>(() => {
    const byId = new Map(goals.map((g) => [g.id, g]));
    const childrenByParent = new Map<string, GoalRef[]>();
    const strategicGoals: GoalRef[] = [];
    const orphanOperational: GoalRef[] = [];
    for (const g of goals) {
      if (g.altitude === "strategic") {
        strategicGoals.push(g);
      } else if (g.parent_goal_id && byId.has(g.parent_goal_id)) {
        const list = childrenByParent.get(g.parent_goal_id) ?? [];
        list.push(g);
        childrenByParent.set(g.parent_goal_id, list);
      } else {
        orphanOperational.push(g);
      }
    }
    function rolledAgg(g: GoalRef, children: GoalRef[]) {
      const own = goalAgg.get(g.id) ?? { total: 0, off: 0 };
      let total = own.total;
      let off = own.off;
      for (const ch of children) {
        const a = goalAgg.get(ch.id) ?? { total: 0, off: 0 };
        total += a.total;
        off += a.off;
      }
      return { total, off };
    }
    function sortChildren(arr: GoalRef[]) {
      return [...arr].sort((a, b) => {
        const ao = goalAgg.get(a.id)?.off ?? 0;
        const bo = goalAgg.get(b.id)?.off ?? 0;
        if (ao !== bo) return bo - ao;
        return a.label.localeCompare(b.label);
      });
    }
    const rows: GoalTreeRow[] = [];
    const sortedStrategic = [...strategicGoals].sort((a, b) => {
      const ac = childrenByParent.get(a.id) ?? [];
      const bc = childrenByParent.get(b.id) ?? [];
      const aa = rolledAgg(a, ac);
      const bb = rolledAgg(b, bc);
      if (aa.off !== bb.off) return bb.off - aa.off;
      return a.label.localeCompare(b.label);
    });
    for (const s of sortedStrategic) {
      const ch = sortChildren(childrenByParent.get(s.id) ?? []);
      rows.push({ goal: s, children: ch, agg: rolledAgg(s, ch) });
    }
    for (const o of sortChildren(orphanOperational)) {
      rows.push({
        goal: o,
        children: [],
        agg: goalAgg.get(o.id) ?? { total: 0, off: 0 },
      });
    }
    return rows;
  }, [goals, goalAgg]);
  const goalCount = goals.length;

  // Per-person aggregates derived from currently-visible commitments,
  // so the Team rail reacts to active filters (owner/customer/time/status).
  const personAgg = useMemo(() => {
    const map = new Map<string, { total: number; off: number; high: number }>();
    for (const p of people) map.set(p.id, { total: 0, off: 0, high: 0 });
    for (const c of commitments) {
      const seen = new Set<string>();
      seen.add(c.owner);
      for (const cid of c.edges?.contributors ?? []) seen.add(cid);
      for (const id of seen) {
        const a = map.get(id);
        if (!a) continue;
        a.total += 1;
        if (c.status !== "on-track") a.off += 1;
        if (c.priority === "high") a.high += 1;
      }
    }
    return map;
  }, [people, commitments]);

  // Sort: people with off-track commitments first, then by load,
  // then alphabetically — so attention-worthy folks float up.
  const sortedPeople = useMemo(() => {
    return [...people].sort((a, b) => {
      const aa = personAgg.get(a.id) ?? { total: 0, off: 0, high: 0 };
      const ba = personAgg.get(b.id) ?? { total: 0, off: 0, high: 0 };
      if (aa.off !== ba.off) return ba.off - aa.off;
      if (aa.total !== ba.total) return ba.total - aa.total;
      return a.label.localeCompare(b.label);
    });
  }, [people, personAgg]);

  const showGoals = entityKind === "all" || entityKind === "goals";
  const showCommits = entityKind === "all" || entityKind === "commitments";
  const showPeople = entityKind === "all" || entityKind === "people";

  return (
    <aside className="commitment-list" aria-label="Goals and commitments">
      {showGoals ? (
        <section className="cl-section">
          <header className="cl-section-head">
            <span className="cl-territory-name">Goals</span>
            <span className="cl-territory-count">{goalCount}</span>
          </header>
          <ul className="cl-rows">
            {goalTree.flatMap((row) => {
              const g = row.goal;
              const agg = row.agg;
              const isSelected = focus?.kind === "goal" && focus.id === g.id;
              const items: JSX.Element[] = [];
              items.push(
                  <li
                    key={g.id}
                    className={
                      "cl-row cl-row-goal" +
                      (isSelected ? " selected" : "") +
                      (row.children.length > 0 ? " parent" : "")
                    }
                    onClick={() => onFocus({ kind: "goal", id: g.id })}
                  >
                    <span
                      className={
                        "cl-goal-glyph" +
                        (g.altitude === "strategic" ? " strategic" : " operational")
                      }
                      aria-hidden
                    >
                      ◆
                    </span>
                    <div className="cl-row-body">
                      <div className="cl-row-line1">
                        <span className="cl-label">{g.label}</span>
                      </div>
                      <div className="cl-row-line2">
                        <span className="cl-altitude">{g.altitude}</span>
                        {row.children.length > 0 ? (
                          <>
                            <span className="cl-sep">·</span>
                            <span className="cl-goal-count">
                              {row.children.length} sub-goal
                              {row.children.length === 1 ? "" : "s"}
                            </span>
                          </>
                        ) : null}
                        <span className="cl-sep">·</span>
                        <span className="cl-goal-count">
                          {agg.total} commitment{agg.total === 1 ? "" : "s"}
                        </span>
                        {agg.off > 0 ? (
                          <>
                            <span className="cl-sep">·</span>
                            <span className="cl-goal-off">{agg.off} off-track</span>
                          </>
                        ) : null}
                      </div>
                    </div>
                    <span className="cl-priority p-standard" />
                  </li>
              );
              for (const ch of row.children) {
                const cAgg = goalAgg.get(ch.id) ?? { total: 0, off: 0 };
                const cSelected =
                  focus?.kind === "goal" && focus.id === ch.id;
                items.push(
                  <li
                    key={ch.id}
                    className={
                      "cl-row cl-row-goal cl-row-goal-sub" +
                      (cSelected ? " selected" : "")
                    }
                    onClick={() => onFocus({ kind: "goal", id: ch.id })}
                  >
                    <span className="cl-goal-glyph operational" aria-hidden>
                      ◇
                    </span>
                    <div className="cl-row-body">
                      <div className="cl-row-line1">
                        <span className="cl-label">{ch.label}</span>
                      </div>
                      <div className="cl-row-line2">
                        <span className="cl-altitude">sub-goal</span>
                        <span className="cl-sep">·</span>
                        <span className="cl-goal-count">
                          {cAgg.total} commitment
                          {cAgg.total === 1 ? "" : "s"}
                        </span>
                        {cAgg.off > 0 ? (
                          <>
                            <span className="cl-sep">·</span>
                            <span className="cl-goal-off">
                              {cAgg.off} off-track
                            </span>
                          </>
                        ) : null}
                      </div>
                    </div>
                    <span className="cl-priority p-standard" />
                  </li>
                );
              }
              return items;
            })}
          </ul>
        </section>
      ) : null}

      {showPeople ? (
        <section className="cl-section">
          <header className="cl-section-head">
            <span className="cl-territory-name">Team</span>
            <span className="cl-territory-count">{sortedPeople.length}</span>
          </header>
          <ul className="cl-rows">
            {sortedPeople.map((p) => {
              const agg = personAgg.get(p.id) ?? { total: 0, off: 0, high: 0 };
              const isSelected = focus?.kind === "actor" && focus.id === p.id;
              const lead = p.patterns[0];
              return (
                <li
                  key={p.id}
                  className={
                    "cl-row cl-row-person" + (isSelected ? " selected" : "")
                  }
                  onClick={() => onFocus({ kind: "actor", id: p.id })}
                >
                  <span className="cl-person-glyph" aria-hidden>◯</span>
                  <div className="cl-row-body">
                    <div className="cl-row-line1">
                      <span className="cl-label">{p.label}</span>
                      <span className="cl-person-role">{p.role}</span>
                    </div>
                    <div className="cl-row-line2">
                      <span className="cl-glyphs" aria-label="load">
                        <span
                          className="cl-glyph g-people"
                          title={`${agg.total} commitment${agg.total === 1 ? "" : "s"}`}
                        >
                          ◯ {agg.total}
                        </span>
                        {agg.off > 0 ? (
                          <span
                            className="cl-glyph g-off"
                            title={`${agg.off} off-track`}
                          >
                            ▲ {agg.off}
                          </span>
                        ) : null}
                        {agg.high > 0 ? (
                          <span
                            className="cl-glyph g-high"
                            title={`${agg.high} high-priority`}
                          >
                            ★ {agg.high}
                          </span>
                        ) : null}
                        <span
                          className="cl-glyph g-calibration"
                          title={`Model calibration: ${(p.calibration * 100).toFixed(0)}%`}
                        >
                          {calibrationGlyph(p.calibration)} {(p.calibration * 100).toFixed(0)}%
                        </span>
                      </span>
                    </div>
                    {lead ? (
                      <div className="cl-person-pattern" title={p.recent_observation}>
                        “{lead.statement}”
                      </div>
                    ) : null}
                  </div>
                  <span className="cl-priority p-standard" />
                </li>
              );
            })}
          </ul>
        </section>
      ) : null}

      {showCommits ? (
        <section className="cl-section">
          <header className="cl-section-head">
            <span className="cl-territory-name">Commitments</span>
            <span className="cl-territory-count">{sortedCommits.length}</span>
          </header>
          <ul className="cl-rows">
            {sortedCommits.map((c) => {
              const isSelected =
                focus?.kind === "commitment" && focus.id === c.id;
              const goalsCount = (c.edges?.contributes_to ?? []).length;
              const decisionsCount = (c.edges?.constrained_by ?? c.traces_to ?? []).length;
              const resourcesCount = (c.edges?.consumes ?? []).length;
              const peopleCount = (c.edges?.contributors ?? []).length + 1;
              const hasCustomer = !!c.customer;
              return (
                <li
                  key={c.id}
                  className={"cl-row" + (isSelected ? " selected" : "")}
                  onClick={() => onFocus({ kind: "commitment", id: c.id })}
                  onMouseEnter={() => onHover(c.id)}
                  onMouseLeave={() => onHover(null)}
                >
                  <span
                    className={"cl-status cl-status-" + c.status}
                    aria-label={c.status}
                  >
                    {STATUS_GLYPH[c.status]}
                  </span>
                  <div className="cl-row-body">
                    <div className="cl-row-line1">
                      <span className="cl-label">{c.label}</span>
                    </div>
                    <div className="cl-row-line2">
                      <span className="cl-owner">{c.owner_display}</span>
                      <span className="cl-sep">·</span>
                      <span className="cl-due">due {formatDate(c.due_date)}</span>
                      {goalsCount || decisionsCount || resourcesCount || peopleCount > 1 || hasCustomer ? (
                        <>
                          <span className="cl-sep">·</span>
                          <span className="cl-glyphs" aria-label="relations">
                            {goalsCount > 0 ? (
                              <span className="cl-glyph g-goal" title={`${goalsCount} goal${goalsCount === 1 ? "" : "s"}`}>
                                ◆ {goalsCount}
                              </span>
                            ) : null}
                            {decisionsCount > 0 ? (
                              <span className="cl-glyph g-decision" title={`${decisionsCount} decision${decisionsCount === 1 ? "" : "s"}`}>
                                ⌃ {decisionsCount}
                              </span>
                            ) : null}
                            {resourcesCount > 0 ? (
                              <span className="cl-glyph g-resource" title={`${resourcesCount} resource${resourcesCount === 1 ? "" : "s"}`}>
                                ▤ {resourcesCount}
                              </span>
                            ) : null}
                            {peopleCount > 1 ? (
                              <span className="cl-glyph g-people" title={`${peopleCount} people`}>
                                ◯ {peopleCount}
                              </span>
                            ) : null}
                            {hasCustomer ? (
                              <span className="cl-glyph g-customer" title={`Customer: ${c.customer}`}>
                                ☆ {c.customer}
                              </span>
                            ) : null}
                          </span>
                        </>
                      ) : (
                        <>
                          <span className="cl-sep">·</span>
                          <span className="cl-orphan" title="No goal linkage — review">
                            ⚠ no goal
                          </span>
                        </>
                      )}
                    </div>
                  </div>
                  <span className={"cl-priority p-" + c.priority} aria-label={`${c.priority} priority`} />
                </li>
              );
            })}
          </ul>
        </section>
      ) : null}
    </aside>
  );
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function calibrationGlyph(c: number): string {
  if (c >= 0.85) return "▮▮▮";
  if (c >= 0.70) return "▮▮▯";
  if (c >= 0.55) return "▮▯▯";
  return "▯▯▯";
}
