import { useState } from "react";

// Renders a top-50 slow-query table where each row expands into the
// EXPLAIN ANALYZE JSON plan tree.

export interface DBPlansDoc {
  plans: PlanEntry[];
  total_queries: number;
  total_db_ms: number;
}
export interface PlanEntry {
  sql: string;
  calls: number;
  total_ms: number;
  mean_ms: number;
  max_ms: number;
  plan: PlanNode[] | null;
  plan_error: string | null;
}
interface PlanNode {
  "Node Type"?: string;
  Plan?: PlanNode;
  Plans?: PlanNode[];
  [k: string]: unknown;
}

export function QueryPlan({ doc }: { doc: DBPlansDoc | null }) {
  if (!doc) return <div className="text-sm text-neutral-500">Loading…</div>;
  if (!doc.plans?.length)
    return (
      <div className="text-sm text-neutral-500">
        No queries captured for this run.
      </div>
    );

  return (
    <div>
      <div className="text-sm mb-3 text-neutral-700">
        Captured <strong>{doc.total_queries.toLocaleString()}</strong>{" "}
        queries totalling <strong>{doc.total_db_ms.toLocaleString()}ms</strong>.
        Top 50 by total time shown below.
      </div>
      <div className="space-y-2">
        {doc.plans.map((p, i) => (
          <PlanCard key={i} plan={p} rank={i + 1} />
        ))}
      </div>
    </div>
  );
}

function PlanCard({ plan, rank }: { plan: PlanEntry; rank: number }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-md border border-neutral-200 bg-white">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full text-left px-4 py-2 flex items-baseline justify-between hover:bg-neutral-50"
      >
        <div className="flex items-baseline gap-3 min-w-0">
          <span className="text-xs text-neutral-400 tabular-nums w-6">
            #{rank}
          </span>
          <code className="text-xs font-mono truncate flex-1">
            {plan.sql.length > 200 ? plan.sql.slice(0, 200) + "…" : plan.sql}
          </code>
        </div>
        <div className="text-xs tabular-nums text-neutral-600 shrink-0 ml-4">
          {plan.calls}× · total {plan.total_ms.toFixed(1)}ms · mean{" "}
          {plan.mean_ms.toFixed(2)}ms · max {plan.max_ms.toFixed(2)}ms
        </div>
      </button>
      {open ? (
        <div className="border-t border-neutral-100 px-4 py-3">
          {plan.plan_error ? (
            <div className="text-xs text-red-700 font-mono">
              EXPLAIN failed: {plan.plan_error}
            </div>
          ) : plan.plan && Array.isArray(plan.plan) && plan.plan.length > 0 ? (
            <PlanTree node={plan.plan[0]?.Plan ?? (plan.plan[0] as PlanNode)} />
          ) : (
            <div className="text-xs text-neutral-500">
              No plan captured (write query or EXPLAIN skipped).
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}

function PlanTree({ node, depth = 0 }: { node: PlanNode | undefined; depth?: number }) {
  if (!node) return null;
  const children = (node["Plans"] as PlanNode[] | undefined) ?? [];
  const interestingKeys = [
    "Node Type",
    "Relation Name",
    "Index Name",
    "Total Cost",
    "Actual Total Time",
    "Actual Rows",
    "Rows Removed by Filter",
    "Shared Hit Blocks",
    "Shared Read Blocks",
  ];
  return (
    <div style={{ marginLeft: depth * 16 }} className="mt-1">
      <div className="text-xs font-mono">
        <span className="font-semibold text-neutral-900">
          {(node["Node Type"] as string) ?? "Node"}
        </span>{" "}
        {interestingKeys
          .filter((k) => k !== "Node Type" && node[k] !== undefined)
          .map((k) => (
            <span key={k} className="text-neutral-500">
              · {k.toLowerCase().replace(/ /g, "_")}={String(node[k])}
            </span>
          ))}
      </div>
      {children.map((c, i) => (
        <PlanTree key={i} node={c} depth={depth + 1} />
      ))}
    </div>
  );
}
