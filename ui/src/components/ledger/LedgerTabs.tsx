import type { LedgerEventType } from "@/api/history-types";
import { TAB_ORDER } from "./event-taxonomy";

export type LedgerTabId = "all" | LedgerEventType;

export interface LedgerTabsProps {
  active: LedgerTabId;
  onChange: (next: LedgerTabId) => void;
}

export function LedgerTabs({ active, onChange }: LedgerTabsProps) {
  return (
    <nav
      className="fy-ledger__tabs"
      role="tablist"
      aria-label="Ledger views"
    >
      {TAB_ORDER.map((tab) => {
        const isActive = tab.id === active;
        return (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={isActive}
            data-tab-id={tab.id}
            className={
              "fy-ledger__tab" +
              (isActive ? " fy-ledger__tab--active" : "")
            }
            onClick={() => onChange(tab.id)}
          >
            {tab.label}
          </button>
        );
      })}
    </nav>
  );
}
