import { useMemo, useState } from "react";
import type { LedgerAuditEvent, LedgerEventType } from "@/api/ledger-v2-types";

interface Props {
  events: LedgerAuditEvent[];
  onOpenChain: (id: string) => void;
}

// Audit mode — compliance/power-user view. Raw event table with a
// search + type filter and CSV export.
const TYPE_LABELS: Record<LedgerEventType, string> = {
  model_item_created: "model · created",
  model_item_updated: "model · updated",
  forecast_created: "forecast · created",
  forecast_resolved_true: "forecast · resolved true",
  forecast_resolved_false: "forecast · resolved false",
  proposed_change_created: "proposal · created",
  proposed_change_accepted: "proposal · accepted",
  proposed_change_delegated: "proposal · delegated",
  proposed_change_corrected: "proposal · corrected",
  owner_assigned: "owner · assigned",
  risk_escalated: "risk · escalated",
  risk_downgraded: "risk · downgraded",
  evidence_added: "evidence · added",
  claim_contested: "claim · contested",
  claim_corrected: "claim · corrected",
  reevaluation_scheduled: "re-evaluation · scheduled",
};

export function AuditMode({ events, onOpenChain }: Props) {
  const [query, setQuery] = useState("");
  const [type, setType] = useState<LedgerEventType | "all">("all");

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return events.filter((e) => {
      if (type !== "all" && e.type !== type) return false;
      if (!q) return true;
      const hay = `${e.chainTitle} ${e.actor.name ?? ""} ${e.object.label}`.toLowerCase();
      return hay.includes(q);
    });
  }, [events, query, type]);

  return (
    <section className="lg-audit" aria-label="Audit">
      <header className="lg-audit__head">
        <h2 className="lg-audit__title">Audit log</h2>
        <p className="lg-audit__sub">
          {filtered.length} of {events.length} events
        </p>
      </header>

      <div className="lg-audit__toolbar">
        <input
          type="search"
          className="lg-audit__search"
          placeholder="Search event, actor, or chain..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <select
          className="lg-audit__select"
          value={type}
          onChange={(e) =>
            setType((e.target.value as LedgerEventType) ?? "all")
          }
        >
          <option value="all">All event types</option>
          {(Object.keys(TYPE_LABELS) as LedgerEventType[]).map((k) => (
            <option key={k} value={k}>
              {TYPE_LABELS[k]}
            </option>
          ))}
        </select>
        <button
          type="button"
          className="lg-audit__btn"
          onClick={() => exportCsv(filtered)}
        >
          Export CSV
        </button>
        <button
          type="button"
          className="lg-audit__btn lg-audit__btn--ghost"
          onClick={() => copyAuditLink()}
        >
          Copy link
        </button>
      </div>

      <div className="lg-table-scroll">
        <table className="lg-table lg-audit__table">
          <thead>
            <tr>
              <th>Time</th>
              <th>Event type</th>
              <th>Actor</th>
              <th>Object</th>
              <th>Before</th>
              <th>After</th>
              <th>Chain</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((e) => (
              <tr key={e.id}>
                <td>{formatStamp(e.timestamp)}</td>
                <td>{TYPE_LABELS[e.type] ?? e.type}</td>
                <td>{e.actor.name ?? e.actor.type}</td>
                <td>{e.object.label}</td>
                <td>
                  <code className="lg-audit__code">{e.before ?? "—"}</code>
                </td>
                <td>
                  <code className="lg-audit__code">{e.after ?? "—"}</code>
                </td>
                <td>
                  <button
                    type="button"
                    className="lg-link"
                    onClick={() => onOpenChain(e.chainId)}
                  >
                    {e.chainTitle} →
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function formatStamp(iso: string): string {
  const d = new Date(iso);
  const day = d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  const t = d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  return `${day} ${t}`;
}

function exportCsv(events: LedgerAuditEvent[]) {
  const rows = [
    ["Time", "Type", "Actor", "Object", "Before", "After", "Chain"],
    ...events.map((e) => [
      e.timestamp,
      e.type,
      e.actor.name ?? e.actor.type,
      e.object.label,
      e.before ?? "",
      e.after ?? "",
      e.chainTitle,
    ]),
  ];
  const csv = rows
    .map((r) => r.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(","))
    .join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "ledger-audit.csv";
  a.click();
  URL.revokeObjectURL(url);
}

function copyAuditLink() {
  try {
    const url = `${window.location.origin}/ledger?mode=audit`;
    void navigator.clipboard?.writeText(url);
  } catch {
    /* clipboard unavailable */
  }
}
