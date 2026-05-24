import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { useFyralisStore } from "@/lib/store";

interface PaletteItem {
  id: string;
  kind: "thread" | "delta" | "forecast" | "ledger" | "action";
  label: string;
  detail?: string;
  onSelect: () => void;
}

// Ask Fyralis palette — ⌘K overlay (spec §2.3 utility surface). Searches
// across operating threads, decision deltas, forecasts, and ledger
// events. Open via the sidebar button or the global ⌘K / Ctrl+K binding.
// Suggested prompts come straight from the spec.
const SUGGESTED: string[] = [
  "Show me commitments blocked by pricing.",
  "Why is Beacon renewal at risk?",
  "What changed in Customer Reliability today?",
  "Show owner gaps older than 30 days.",
  "Which forecasts changed confidence this week?",
];

export function CommandPalette() {
  const open = useFyralisStore((s) => s.paletteOpen);
  const setOpen = useFyralisStore((s) => s.setPaletteOpen);
  const threads = useFyralisStore((s) => s.threads);
  const deltas = useFyralisStore((s) => s.deltas);
  const forecasts = useFyralisStore((s) => s.forecasts);
  const ledger = useFyralisStore((s) => s.ledgerEvents);
  const setSelection = useFyralisStore((s) => s.setSelection);

  const navigate = useNavigate();
  const [query, setQuery] = useState("");
  const [activeIdx, setActiveIdx] = useState(0);

  useEffect(() => {
    if (!open) {
      setQuery("");
      setActiveIdx(0);
    }
  }, [open]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen(!open);
      }
      if (e.key === "Escape" && open) {
        setOpen(false);
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [open, setOpen]);

  const items = useMemo<PaletteItem[]>(() => {
    const q = query.trim().toLowerCase();
    const out: PaletteItem[] = [];

    // Threads
    for (const t of threads) {
      const hay = `${t.title} ${t.currentReading}`.toLowerCase();
      if (!q || hay.includes(q)) {
        out.push({
          id: `thr-${t.id}`,
          kind: "thread",
          label: t.title,
          detail: t.currentReading,
          onSelect: () => {
            setSelection({ threadId: t.id });
            navigate(`/model?thread=${t.id}`);
            setOpen(false);
          },
        });
      }
    }

    // Deltas
    for (const d of deltas) {
      const hay = `${d.proposal} ${d.category ?? ""}`.toLowerCase();
      if (!q || hay.includes(q)) {
        out.push({
          id: `dlt-${d.id}`,
          kind: "delta",
          label: d.proposal,
          detail: `${d.currentState} → ${d.proposedState}`,
          onSelect: () => {
            setSelection({ deltaId: d.id });
            navigate(`/?delta=${d.id}`);
            setOpen(false);
          },
        });
      }
    }

    // Forecasts
    for (const f of forecasts) {
      const hay = `${f.statement}`.toLowerCase();
      if (!q || hay.includes(q)) {
        out.push({
          id: `fcs-${f.id}`,
          kind: "forecast",
          label: f.statement,
          detail: f.resolutionDate ? `Resolves ${f.resolutionDate}` : undefined,
          onSelect: () => {
            setSelection({ forecastId: f.id });
            navigate(`/forecasts?forecast=${f.id}`);
            setOpen(false);
          },
        });
      }
    }

    // Ledger
    for (const e of ledger) {
      const hay = `${e.summary}`.toLowerCase();
      if (!q || hay.includes(q)) {
        out.push({
          id: `led-${e.id}`,
          kind: "ledger",
          label: e.summary,
          detail: new Date(e.occurredAt).toLocaleString(),
          onSelect: () => {
            setSelection({ ledgerEventId: e.id });
            navigate(`/ledger?event=${e.id}`);
            setOpen(false);
          },
        });
      }
    }

    // Actions
    out.push({
      id: "act-today",
      kind: "action",
      label: "Go to Today",
      onSelect: () => {
        navigate("/");
        setOpen(false);
      },
    });
    out.push({
      id: "act-model",
      kind: "action",
      label: "Go to Model",
      onSelect: () => {
        navigate("/model");
        setOpen(false);
      },
    });

    return out.slice(0, 40);
  }, [query, threads, deltas, forecasts, ledger, navigate, setOpen, setSelection]);

  useEffect(() => {
    if (activeIdx >= items.length) setActiveIdx(0);
  }, [items.length, activeIdx]);

  if (!open) return null;

  return (
    <div
      className="fx-palette-backdrop"
      role="dialog"
      aria-modal="true"
      aria-label="Ask Fyralis"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) setOpen(false);
      }}
    >
      <div className="fx-palette">
        <input
          autoFocus
          className="fx-palette__input"
          placeholder="Ask Fyralis or search the model…"
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setActiveIdx(0);
          }}
          onKeyDown={(e) => {
            if (e.key === "ArrowDown") {
              e.preventDefault();
              setActiveIdx((i) => Math.min(items.length - 1, i + 1));
            } else if (e.key === "ArrowUp") {
              e.preventDefault();
              setActiveIdx((i) => Math.max(0, i - 1));
            } else if (e.key === "Enter") {
              e.preventDefault();
              items[activeIdx]?.onSelect();
            }
          }}
        />
        <div className="fx-palette__list">
          {query.trim() === "" ? (
            <>
              <div className="fx-palette__group-label">Suggested</div>
              {SUGGESTED.map((s, i) => (
                <div
                  key={i}
                  className="fx-palette__item"
                  onClick={() => setQuery(s)}
                >
                  <span className="fx-palette__item-kind">prompt</span>
                  {s}
                </div>
              ))}
              <div className="fx-palette__group-label">Threads & deltas</div>
              {items.slice(0, 8).map((it, idx) => (
                <Row key={it.id} item={it} active={idx === activeIdx} onClick={() => it.onSelect()} />
              ))}
            </>
          ) : (
            items.map((it, idx) => (
              <Row key={it.id} item={it} active={idx === activeIdx} onClick={() => it.onSelect()} />
            ))
          )}
        </div>
        <div className="fx-palette__hint">
          <span><span className="fx-palette__kbd">↑↓</span> navigate · <span className="fx-palette__kbd">↵</span> open · <span className="fx-palette__kbd">Esc</span> close</span>
          <span>Powered by Fyralis</span>
        </div>
      </div>
    </div>
  );
}

function Row({ item, active, onClick }: { item: PaletteItem; active: boolean; onClick: () => void }) {
  return (
    <div
      className={`fx-palette__item${active ? " fx-palette__item--active" : ""}`}
      onClick={onClick}
    >
      <span className="fx-palette__item-kind">{item.kind}</span>
      <span style={{ flex: 1 }}>{item.label}</span>
      {item.detail ? <span className="fx-muted" style={{ fontSize: 12 }}>{item.detail}</span> : null}
    </div>
  );
}
