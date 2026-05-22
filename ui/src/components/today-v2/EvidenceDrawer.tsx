// Evidence drawer — spec §7.1. Side-drawer revealing the underlying
// evidence supporting a Proposed Change, grouped by source.

import { useEffect, useMemo, useState } from "react";

import type {
  EvidenceItem,
  EvidenceQuality,
  EvidenceResponse,
} from "@/api/today-page-types";

interface Props {
  data: EvidenceResponse;
  deltaTitle: string;
  onClose: () => void;
}

const QUALITY_ORDER: Record<EvidenceQuality, number> = {
  strong: 3, medium: 2, partial: 1, weak: 0,
};

export function EvidenceDrawer({ data, deltaTitle, onClose }: Props) {
  const [sourceFilter, setSourceFilter] = useState<string | null>(null);
  const [minQuality, setMinQuality] = useState<EvidenceQuality | null>(null);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  const filtered = useMemo(() => {
    return data.items.filter((ev) => {
      if (sourceFilter && ev.source !== sourceFilter) return false;
      if (
        minQuality &&
        ev.quality &&
        QUALITY_ORDER[ev.quality] < QUALITY_ORDER[minQuality]
      ) {
        return false;
      }
      return true;
    });
  }, [data.items, sourceFilter, minQuality]);

  return (
    <div
      className="tdv2-drawer-backdrop"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
      role="dialog"
      aria-modal="true"
    >
      <aside className="tdv2-drawer" data-testid="evidence-drawer">
        <header className="tdv2-drawer__head">
          <div>
            <div style={{ fontSize: "11px", color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: "4px" }}>
              Evidence for
            </div>
            <h2 className="tdv2-drawer__title">{deltaTitle}</h2>
          </div>
          <button
            type="button"
            className="tdv2-drawer__close"
            onClick={onClose}
            aria-label="Close"
          >
            ×
          </button>
        </header>
        <div className="tdv2-drawer__body">
          <div style={{ display: "flex", gap: "var(--space-3)", marginBottom: "var(--space-4)", flexWrap: "wrap" }}>
            <SourceFilter
              groups={data.evidenceGroups}
              value={sourceFilter}
              onChange={setSourceFilter}
            />
            <QualityFilter value={minQuality} onChange={setMinQuality} />
          </div>
          <p style={{ fontSize: "13px", color: "var(--text-muted)", marginBottom: "var(--space-4)" }}>
            {filtered.length} of {data.totalSignals} signals shown
          </p>
          <ul style={{ listStyle: "none", padding: 0, margin: 0, display: "flex", flexDirection: "column", gap: "var(--space-3)" }}>
            {filtered.map((ev) => (
              <EvidenceCard key={ev.id} ev={ev} />
            ))}
          </ul>
        </div>
        <footer className="tdv2-drawer__foot">
          <button
            type="button"
            className="tdv2-btn tdv2-btn--secondary"
            onClick={onClose}
          >
            Close
          </button>
        </footer>
      </aside>
    </div>
  );
}

function SourceFilter({
  groups, value, onChange,
}: {
  groups: EvidenceResponse["evidenceGroups"];
  value: string | null;
  onChange: (v: string | null) => void;
}) {
  return (
    <select
      className="tdv2-select"
      value={value ?? ""}
      onChange={(e) => onChange(e.target.value || null)}
    >
      <option value="">All sources</option>
      {groups.map((g) => (
        <option key={g.id} value={g.sourceType}>
          {g.label} ({g.count})
        </option>
      ))}
    </select>
  );
}

function QualityFilter({
  value, onChange,
}: {
  value: EvidenceQuality | null;
  onChange: (v: EvidenceQuality | null) => void;
}) {
  return (
    <select
      className="tdv2-select"
      value={value ?? ""}
      onChange={(e) => onChange((e.target.value || null) as EvidenceQuality | null)}
    >
      <option value="">All trust tiers</option>
      <option value="strong">Strong only</option>
      <option value="medium">Medium and stronger</option>
      <option value="partial">Partial and stronger</option>
    </select>
  );
}

function EvidenceCard({ ev }: { ev: EvidenceItem }) {
  return (
    <li
      style={{
        background: "var(--bg-elevated)",
        border: "1px solid var(--border-default)",
        borderRadius: "var(--radius-md)",
        padding: "var(--space-3) var(--space-4)",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: "var(--space-2)", marginBottom: "4px" }}>
        <div style={{ fontSize: "11px", color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.06em" }}>
          {ev.sourceLabel ?? ev.source} · {ev.occurredAt.split("T")[0]}
        </div>
        {ev.quality ? (
          <span
            className={`tdv2-evidence-list__quality tdv2-evidence-list__quality--${ev.quality}`}
          >
            {ev.quality}
          </span>
        ) : null}
      </div>
      <div style={{ fontSize: "14px", fontWeight: 500, color: "var(--text-primary)", marginBottom: "2px" }}>
        {ev.title}
      </div>
      {ev.excerpt ? (
        <div style={{ fontSize: "13px", color: "var(--text-muted)", lineHeight: 1.5 }}>
          {ev.excerpt}
        </div>
      ) : null}
    </li>
  );
}
