"use client";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

import { sourceStatusLabel } from "../state/onboarding-store";
import type { Source, SourceConnection } from "../types";

export type SourceAutomationCardState = {
  status: "idle" | "connecting" | "waiting_admin" | "connected" | "blocked" | "error";
  label: string;
  message?: string;
};

export function SourceMarketplace({
  sources,
  connections,
  selectedSourceId,
  automationStates = {},
  onSelect,
  onConnect
}: {
  sources: Source[];
  connections: SourceConnection[];
  selectedSourceId: string;
  automationStates?: Record<string, SourceAutomationCardState>;
  onSelect: (sourceId: string) => void;
  onConnect: (sourceId: string) => void;
}) {
  return (
    <div className="grid w-full min-w-0 max-w-full">
      {sources.length ? (
        <div className="w-full min-w-0 overflow-hidden rounded-lg border border-border bg-card">
          {sources.map((source) => {
            const connection = connections.find(
              (item) => item.sourceId === source.id
            );
            const status = connection?.status ?? "not-configured";
            const automation = automationStates[source.id];
            const effectiveStatus = automation?.status ?? status;
            const selected = selectedSourceId === source.id;
            const waiting =
              automation?.status === "waiting_admin" || status === "waiting-admin";
            const busy = automation?.status === "connecting" || waiting;
            const connected =
              automation?.status === "connected" || status === "connected";

            return (
              <div
                key={source.id}
                className={cn(
                  "grid min-h-16 min-w-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3 border-b border-border px-3 py-3 last:border-b-0 sm:grid-cols-[minmax(0,1fr)_auto_auto]",
                  selected ? "bg-success/5" : "bg-card"
                )}
              >
                <button
                  type="button"
                  className="min-w-0 text-left"
                  onClick={() => onSelect(source.id)}
                >
                  <span className="block min-w-0 truncate text-sm font-medium">
                    {source.name}
                  </span>
                  {automation?.message ? (
                    <span className="mt-1 block min-w-0 break-words text-xs leading-5 text-muted-foreground">
                      {automation.message}
                    </span>
                  ) : null}
                </button>

                {automation || status !== "not-configured" ? (
                  <Badge
                    tone={statusTone(effectiveStatus)}
                    className="w-fit max-w-full justify-self-end"
                  >
                    {automation?.label ?? sourceStatusLabel(status)}
                  </Badge>
                ) : null}

                <Button
                  type="button"
                  className="col-span-2 w-full sm:col-span-1 sm:w-32"
                  variant={connected ? "secondary" : "primary"}
                  disabled={busy || connected}
                  onClick={() => onConnect(source.id)}
                  aria-label={sourceActionLabel({
                    sourceName: source.name,
                    waiting,
                    busy,
                    connected
                  })}
                >
                  {waiting
                    ? "Waiting"
                    : busy
                      ? "Connecting..."
                      : connected
                        ? "Connected"
                        : "Connect"}
                </Button>
              </div>
            );
          })}
        </div>
      ) : (
        <div className="rounded-lg border border-border bg-card p-6 text-center">
          <strong>No sources available.</strong>
        </div>
      )}
    </div>
  );
}

function statusTone(status: SourceConnection["status"] | SourceAutomationCardState["status"]) {
  if (status === "connected" || status === "ready") {
    return "success" as const;
  }
  if (
    status === "validating" ||
    status === "draft" ||
    status === "connecting" ||
    status === "waiting_admin" ||
    status === "waiting-admin"
  ) {
    return "info" as const;
  }
  if (status === "error" || status === "blocked") {
    return "error" as const;
  }
  return "muted" as const;
}

function sourceActionLabel({
  sourceName,
  waiting,
  busy,
  connected
}: {
  sourceName: string;
  waiting: boolean;
  busy: boolean;
  connected: boolean;
}) {
  if (waiting) {
    return `${sourceName} waiting for approval`;
  }
  if (busy) {
    return `${sourceName} connecting`;
  }
  if (connected) {
    return `${sourceName} connected`;
  }
  return `Connect ${sourceName}`;
}
