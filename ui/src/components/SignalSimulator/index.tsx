import { useCallback, useEffect, useState } from "react";
import {
  getSuggestedSignals,
  injectSignal,
  type SuggestedSignals as SuggestedSignalsModel,
  type SuggestedSignalItem,
} from "@/api/demo-client";
import { SlackTab } from "./SlackTab";
import { EmailTab } from "./EmailTab";
import { GitHubTab } from "./GitHubTab";
import { CalendarTab } from "./CalendarTab";
import { StripeTab } from "./StripeTab";
import { CustomTab } from "./CustomTab";
import type { SendStatus, TabId } from "./types";

type Props = {
  token: string;
  sessionId: string;
};

const TAB_ORDER: { id: TabId; label: string }[] = [
  { id: "slack", label: "Slack" },
  { id: "email", label: "Email" },
  { id: "github", label: "GitHub" },
  { id: "calendar", label: "Calendar" },
  { id: "stripe", label: "Stripe" },
  { id: "custom", label: "Custom" },
];

export function SignalSimulator({ token }: Props) {
  // Default to collapsed — the panel is fixed-position and easily
  // covers content on narrow viewports if forced open. The side handle
  // is the discoverable toggle.
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<TabId>("slack");
  const [suggested, setSuggested] = useState<SuggestedSignalsModel | null>(null);
  const [status, setStatus] = useState<SendStatus>({ kind: "idle" });

  // Fetch the per-company suggested signals once on mount.
  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const data = await getSuggestedSignals(token);
        if (alive) setSuggested(data);
      } catch {
        // Suggestions are a non-critical convenience.
      }
    })();
    return () => {
      alive = false;
    };
  }, [token]);

  // Reset transient status when switching tabs.
  useEffect(() => {
    setStatus({ kind: "idle" });
  }, [tab]);

  const send = useCallback(
    async (channel: string, payload: Record<string, unknown>) => {
      setStatus({ kind: "sending" });
      try {
        const res = await injectSignal(token, channel, payload);
        setStatus({
          kind: "sent",
          deduped: !!res.deduped,
          observation_id: res.observation_id,
        });
      } catch (err) {
        setStatus({
          kind: "error",
          message: err instanceof Error ? err.message : "send failed",
        });
      }
    },
    [token]
  );

  if (!open) {
    return (
      <button
        type="button"
        className="sim-tab-handle"
        onClick={() => setOpen(true)}
        aria-label="Open signal simulator"
        data-testid="sim-open-handle"
      >
        Inject signals
      </button>
    );
  }

  const itemsForTab = (id: TabId): SuggestedSignalItem[] => {
    const tabs = suggested?.tabs ?? {};
    return (tabs[id] ?? []) as SuggestedSignalItem[];
  };

  return (
    <aside className="sim-panel" data-testid="signal-simulator">
      <header className="sim-header">
        <span className="sim-title">Signal simulator</span>
        <button
          type="button"
          className="sim-collapse"
          onClick={() => setOpen(false)}
          aria-label="Minimize simulator"
          title="Minimize"
          data-testid="sim-minimize"
        >
          <span className="sim-collapse-label">Minimize</span>
          <span className="sim-collapse-x">×</span>
        </button>
      </header>
      <nav className="sim-tabs">
        {TAB_ORDER.map((t) => (
          <button
            key={t.id}
            type="button"
            className={"sim-tab" + (tab === t.id ? " active" : "")}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </nav>
      <div className="sim-body">
        {tab === "slack" ? (
          <SlackTab
            suggestions={itemsForTab("slack")}
            send={send}
            status={status}
          />
        ) : null}
        {tab === "email" ? (
          <EmailTab
            suggestions={itemsForTab("email")}
            send={send}
            status={status}
          />
        ) : null}
        {tab === "github" ? (
          <GitHubTab
            suggestions={itemsForTab("github")}
            send={send}
            status={status}
          />
        ) : null}
        {tab === "calendar" ? (
          <CalendarTab
            suggestions={itemsForTab("calendar")}
            send={send}
            status={status}
          />
        ) : null}
        {tab === "stripe" ? (
          <StripeTab
            suggestions={itemsForTab("stripe")}
            send={send}
            status={status}
          />
        ) : null}
        {tab === "custom" ? (
          <CustomTab
            suggestions={itemsForTab("custom")}
            send={send}
            status={status}
          />
        ) : null}
      </div>
    </aside>
  );
}
