"use client";

import { useEffect, useState, type ReactNode } from "react";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  ExternalLink,
  Hash,
  LockKeyhole,
  Server,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

import { sourceStatusLabel } from "../state/onboarding-store";
import type {
  SourceAccessResource,
  SourceAccessSummary,
  SourceConnectAccessMode,
  SourceInstallation,
} from "../services/onboarding-service";
import type { Source, SourceConnection } from "../types";

export type SourceAutomationCardState = {
  status:
    "idle" | "connecting" | "waiting_admin" | "connected" | "blocked" | "error";
  label: string;
  message?: string;
  actionUrl?: string | null;
  actionLabel?: string;
  receiptPathHint?: string | null;
  installations?: SourceInstallation[];
  accessSummary?: SourceAccessSummary;
  accessResources?: SourceAccessResource[];
  accessNextActions?: string[];
  syncStartedAt?: string | null;
  installUrl?: string | null;
};

export function SourceMarketplace({
  sources,
  connections,
  selectedSourceId,
  automationStates = {},
  onSelect,
  onConnect,
  onRegisterAwsRuntimeRole,
  onFinalizeAwsSourceRole,
  onRetryAwsFirstSync,
  onAuthorize,
}: {
  sources: Source[];
  connections: SourceConnection[];
  selectedSourceId: string;
  automationStates?: Record<string, SourceAutomationCardState>;
  onSelect: (sourceId: string) => void;
  onConnect: (
    sourceId: string,
    options?: { discordAccessMode?: SourceConnectAccessMode },
  ) => void;
  onRegisterAwsRuntimeRole?: (roleArn: string) => void | Promise<void>;
  onFinalizeAwsSourceRole?: (roleArn: string) => void | Promise<void>;
  onRetryAwsFirstSync?: () => void | Promise<void>;
  onAuthorize?: (sourceId: string, installUrl: string) => void;
}) {
  const [awsRuntimeRoleInput, setAwsRuntimeRoleInput] = useState("");
  const [awsRuntimeRoleError, setAwsRuntimeRoleError] = useState<string | null>(
    null,
  );
  const [awsSourceRoleInput, setAwsSourceRoleInput] = useState("");
  const [awsSourceRoleError, setAwsSourceRoleError] = useState<string | null>(
    null,
  );

  async function registerAwsRuntimeRole(value: string) {
    const roleArn = awsRuntimeRoleArnFromInput(value);
    if (!roleArn) {
      setAwsRuntimeRoleError(
        "Use a 12-digit AWS account id or a valid IAM role ARN.",
      );
      return;
    }
    setAwsRuntimeRoleError(null);
    await onRegisterAwsRuntimeRole?.(roleArn);
    setAwsRuntimeRoleInput("");
  }

  async function finalizeAwsSourceRole(value: string) {
    const roleArn = awsSourceRoleArnFromInput(value);
    if (!roleArn) {
      setAwsSourceRoleError(
        "Use the RoleArn output or a 12-digit AWS account id.",
      );
      return;
    }
    setAwsSourceRoleError(null);
    await onFinalizeAwsSourceRole?.(roleArn);
    setAwsSourceRoleInput("");
  }

  return (
    <div className="grid w-full min-w-0 max-w-full">
      {sources.length ? (
        <div className="w-full min-w-0 overflow-hidden rounded-lg border border-border bg-card">
          {sources.map((source) => {
            const connection = connections.find(
              (item) => item.sourceId === source.id,
            );
            const status = connection?.status ?? "not-configured";
            const automation = automationStates[source.id];
            const effectiveStatus = automation?.status ?? status;
            const selected = selectedSourceId === source.id;
            const waiting = automation
              ? automation.status === "waiting_admin"
              : status === "waiting-admin";
            const blocked = automation?.status === "blocked";
            const connecting = automation?.status === "connecting";
            const connected = automation
              ? automation.status === "connected"
              : status === "connected";
            const canAddDiscordServer = source.id === "discord" && connected;
            const approvalActionUrl =
              waiting || blocked ? automation?.actionUrl : null;
            const canOpenApproval = Boolean(approvalActionUrl);
            const actionDisabled =
              connecting || (connected && !canAddDiscordServer);
            const approvalInstruction = sourceApprovalInstruction(source);
            const approvalActionLabel =
              automation?.actionLabel ?? sourceApprovalActionLabel(source);
            const syncBlocked =
              source.id === "aws" &&
              automation?.status === "error" &&
              connected;
            const showDiscordAccess =
              source.id === "discord" &&
              Boolean(
                (automation?.accessSummary?.total ?? 0) > 0 ||
                automation?.accessResources?.length ||
                automation?.accessNextActions?.length,
              );
            const authorizeUrl =
              automation?.installUrl && !connected ? automation.installUrl : null;

            return (
              <div
                key={source.id}
                className={cn(
                  "grid min-h-16 min-w-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3 border-b border-border px-3 py-3 last:border-b-0 sm:grid-cols-[minmax(0,1fr)_auto_auto]",
                  selected ? "bg-success/5" : "bg-card",
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
                  {waiting || blocked ? (
                    <span className="mt-1 block min-w-0 break-words text-xs leading-5 text-foreground">
                      {blocked && source.id === "aws"
                        ? "Next: create the Fyralis BYOC source runtime role, then connect AWS again."
                        : approvalInstruction}
                    </span>
                  ) : null}
                  {waiting && automation?.receiptPathHint ? (
                    <span className="mt-1 block min-w-0 break-words text-xs leading-5 text-muted-foreground">
                      Receipt: {automation.receiptPathHint}
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
                  disabled={actionDisabled}
                  onClick={() => {
                    if (authorizeUrl) {
                      if (onAuthorize) {
                        onAuthorize(source.id, authorizeUrl);
                      } else {
                        window.open(authorizeUrl, "_blank", "noopener,noreferrer");
                      }
                      return;
                    }
                    if (canOpenApproval && approvalActionUrl) {
                      window.open(
                        approvalActionUrl,
                        "_blank",
                        "noopener,noreferrer",
                      );
                      return;
                    }
                    if (source.id === "discord") {
                      onConnect(source.id, {
                        discordAccessMode: "full_server_sync",
                      });
                      return;
                    }
                    onConnect(source.id);
                  }}
                  aria-label={sourceActionLabel({
                    sourceId: source.id,
                    sourceName: source.name,
                    authorize: Boolean(authorizeUrl),
                    waiting,
                    blocked,
                    connecting,
                    connected,
                    canAddDiscordServer,
                    canOpenApproval,
                    actionLabel: approvalActionLabel,
                  })}
                >
                  {authorizeUrl ? (
                    "Authorize"
                  ) : waiting || blocked ? (
                    canOpenApproval ? (
                      <>
                        <ExternalLink className="h-4 w-4" aria-hidden="true" />
                        {approvalActionLabel}
                      </>
                    ) : (
                      "Get link"
                    )
                  ) : connecting ? (
                    "Connecting..."
                  ) : source.id === "discord" && !connected ? (
                    "Full sync"
                  ) : canAddDiscordServer ? (
                    "Add server"
                  ) : connected ? (
                    "Connected"
                  ) : (
                    "Connect"
                  )}
                </Button>
                {source.id === "discord" && !waiting && !blocked ? (
                  <div className="col-span-2 grid min-w-0 gap-2 rounded-md border border-info/30 bg-info/10 p-3 text-xs leading-5 text-foreground sm:col-span-3 sm:grid-cols-[auto_minmax(0,1fr)_auto] sm:items-center">
                    <LockKeyhole
                      className="mt-0.5 h-4 w-4 text-info sm:mt-0"
                      aria-hidden="true"
                    />
                    <span className="min-w-0 break-words">
                      Full Server Sync: Fyralis can read every channel this
                      server has, including private channels.
                    </span>
                    <Badge tone="warning" className="w-fit">
                      Administrator
                    </Badge>
                  </div>
                ) : null}
                {blocked && source.id === "aws" && onRegisterAwsRuntimeRole ? (
                  <form
                    className="col-span-2 grid min-w-0 gap-2 rounded-md border border-border bg-background/50 p-3 sm:col-span-3 sm:grid-cols-[minmax(0,1fr)_auto]"
                    onSubmit={(event) => {
                      event.preventDefault();
                      void registerAwsRuntimeRole(awsRuntimeRoleInput);
                    }}
                  >
                    <label
                      className="grid min-w-0 gap-1 text-xs text-muted-foreground"
                      htmlFor="aws-runtime-role-input"
                    >
                      SourceRuntimeRoleArn or AWS account id
                      <input
                        id="aws-runtime-role-input"
                        className="min-h-10 min-w-0 rounded-md border border-input bg-background px-3 text-sm text-foreground outline-none transition-colors placeholder:text-muted-foreground focus:border-ring"
                        value={awsRuntimeRoleInput}
                        onChange={(event) => {
                          setAwsRuntimeRoleInput(event.target.value);
                          setAwsRuntimeRoleError(null);
                        }}
                        placeholder="587628268464"
                      />
                    </label>
                    <Button
                      type="submit"
                      className="self-end"
                      variant="secondary"
                    >
                      Use runtime role
                    </Button>
                    {awsRuntimeRoleError ? (
                      <span className="text-xs text-destructive sm:col-span-2">
                        {awsRuntimeRoleError}
                      </span>
                    ) : (
                      <span className="text-xs text-muted-foreground sm:col-span-2">
                        After the runtime stack is created, Fyralis uses role
                        name fyralis-source-runtime.
                      </span>
                    )}
                  </form>
                ) : null}
                {waiting && source.id === "aws" && onFinalizeAwsSourceRole ? (
                  <form
                    className="col-span-2 grid min-w-0 gap-2 rounded-md border border-border bg-background/50 p-3 sm:col-span-3 sm:grid-cols-[minmax(0,1fr)_auto]"
                    onSubmit={(event) => {
                      event.preventDefault();
                      void finalizeAwsSourceRole(awsSourceRoleInput);
                    }}
                  >
                    <label
                      className="grid min-w-0 gap-1 text-xs text-muted-foreground"
                      htmlFor="aws-source-role-input"
                    >
                      RoleArn from CloudFormation Outputs
                      <input
                        id="aws-source-role-input"
                        className="min-h-10 min-w-0 rounded-md border border-input bg-background px-3 text-sm text-foreground outline-none transition-colors placeholder:text-muted-foreground focus:border-ring"
                        value={awsSourceRoleInput}
                        onChange={(event) => {
                          setAwsSourceRoleInput(event.target.value);
                          setAwsSourceRoleError(null);
                        }}
                        placeholder="arn:aws:iam::587628268464:role/fyralis-source-readonly"
                      />
                    </label>
                    <Button
                      type="submit"
                      className="self-end"
                      variant="secondary"
                    >
                      Finalize AWS
                    </Button>
                    {awsSourceRoleError ? (
                      <span className="text-xs text-destructive sm:col-span-2">
                        {awsSourceRoleError}
                      </span>
                    ) : (
                      <span className="text-xs text-muted-foreground sm:col-span-2">
                        Open the stack Outputs tab and copy RoleArn.
                      </span>
                    )}
                  </form>
                ) : null}
                {syncBlocked && onRetryAwsFirstSync ? (
                  <div className="col-span-2 grid min-w-0 gap-2 rounded-md border border-border bg-background/50 p-3 sm:col-span-3 sm:grid-cols-[minmax(0,1fr)_auto]">
                    <span className="min-w-0 text-xs leading-5 text-muted-foreground">
                      Fix the Fyralis runtime AWS identity, then retry the first
                      CloudTrail sync.
                    </span>
                    <Button
                      type="button"
                      className="self-center"
                      variant="secondary"
                      onClick={() => {
                        void onRetryAwsFirstSync();
                      }}
                    >
                      Retry first sync
                    </Button>
                  </div>
                ) : null}
                {showDiscordAccess ? (
                  <DiscordAccessPanel
                    summary={automation?.accessSummary}
                    installations={automation?.installations ?? []}
                    resources={automation?.accessResources ?? []}
                    nextActions={automation?.accessNextActions ?? []}
                    syncStartedAt={automation?.syncStartedAt ?? null}
                  />
                ) : null}
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

function statusTone(
  status: SourceConnection["status"] | SourceAutomationCardState["status"],
) {
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

function DiscordAccessPanel({
  summary,
  installations,
  resources,
  nextActions,
  syncStartedAt,
}: {
  summary?: SourceAccessSummary;
  installations: SourceInstallation[];
  resources: SourceAccessResource[];
  nextActions: string[];
  syncStartedAt?: string | null;
}) {
  const servers = buildDiscordServers(installations, resources);
  const [selectedServerId, setSelectedServerId] = useState<string | null>(null);
  const [nowMs, setNowMs] = useState(() => Date.now());
  const selectedServer =
    servers.find((server) => server.installationId === selectedServerId) ??
    servers[0];
  const selectedResources = selectedServer?.resources ?? [];
  const total = summary?.total ?? resources.length;
  const serverCount = servers.filter((item) => item.enabled).length;
  const selectedSummary = summarizeAccessResources(selectedResources);
  const privateReadable = selectedResources.filter(
    (resource) =>
      resource.visibility === "private" &&
      resource.permissionStatus === "ready",
  ).length;
  const landingChannels = selectedResources.filter(
    (resource) => resource.observationCount > 0,
  ).length;
  const syncElapsed = syncStartedAt
    ? formatElapsedDuration(nowMs - Date.parse(syncStartedAt))
    : null;

  useEffect(() => {
    if (!syncStartedAt) {
      return;
    }
    setNowMs(Date.now());
    const timer = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [syncStartedAt]);

  return (
    <div className="col-span-2 grid min-w-0 gap-4 border-t border-border bg-background/60 px-3 py-4 sm:col-span-3">
      <div className="grid min-w-0 grid-cols-2 gap-2 sm:grid-cols-6">
        <AccessMetric
          label="Servers"
          value={String(serverCount)}
          icon="server"
        />
        <AccessMetric label="Ready" value={`${summary?.ready ?? 0}/${total}`} />
        <AccessMetric
          label="Blocked"
          value={String(summary?.missingAccess ?? 0)}
          tone={summary?.missingAccess ? "warning" : "default"}
        />
        <AccessMetric
          label="Landing"
          value={String(summary?.observed ?? 0)}
          tone={summary?.observed ? "success" : "default"}
        />
        <AccessMetric label="Selected" value={String(summary?.selected ?? 0)} />
        <AccessMetric
          label="Sync time"
          value={syncElapsed ?? "--"}
          tone={syncElapsed ? "success" : "default"}
        />
      </div>

      {nextActions.length ? (
        <div className="grid min-w-0 gap-1 rounded-md border border-warning/40 bg-warning/10 p-3 text-xs leading-5 text-foreground">
          {nextActions.slice(0, 2).map((action) => (
            <span key={action} className="min-w-0 break-words">
              {action}
            </span>
          ))}
        </div>
      ) : null}

      {servers.length ? (
        <div className="grid min-w-0 gap-3">
          <div className="grid min-w-0 gap-2 lg:grid-cols-2">
            {servers.map((server) => {
              const active =
                server.installationId === selectedServer?.installationId;
              const serverSummary = summarizeAccessResources(server.resources);
              return (
                <button
                  key={server.installationId}
                  type="button"
                  aria-pressed={active}
                  aria-label={`Open ${server.name} Discord server`}
                  className={cn(
                    "grid min-w-0 gap-2 rounded-md border px-3 py-3 text-left transition-colors",
                    active
                      ? "border-ring bg-accent"
                      : "border-border bg-card hover:border-ring hover:bg-accent/70",
                  )}
                  onClick={() => setSelectedServerId(server.installationId)}
                >
                  <span className="flex min-w-0 items-center justify-between gap-3">
                    <span className="flex min-w-0 items-center gap-2">
                      <Server
                        className="h-4 w-4 shrink-0 text-muted-foreground"
                        aria-hidden="true"
                      />
                      <strong className="min-w-0 truncate text-sm text-foreground">
                        {server.name}
                      </strong>
                    </span>
                    <Badge tone={server.enabled ? "success" : "muted"}>
                      {server.enabled ? "Connected" : "Disabled"}
                    </Badge>
                  </span>
                  <span className="grid min-w-0 gap-2 sm:grid-cols-3">
                    <span className="truncate text-xs text-muted-foreground">
                      {serverSummary.ready}/{serverSummary.total} readable
                    </span>
                    <span className="truncate text-xs text-muted-foreground">
                      {serverSummary.missingAccess} needs access
                    </span>
                    <span className="truncate text-xs text-muted-foreground">
                      {serverSummary.observed} landing
                    </span>
                  </span>
                  <ProgressBar
                    value={serverBackfillPercent(server.resources)}
                    tone={serverSummary.observed ? "success" : "info"}
                  />
                </button>
              );
            })}
          </div>

          {selectedServer ? (
            <div className="grid min-w-0 gap-3 border-t border-border pt-3">
              <div className="flex min-w-0 flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                <span className="min-w-0">
                  <strong className="block min-w-0 truncate text-sm text-foreground">
                    {selectedServer.name}
                  </strong>
                  <span className="block text-xs leading-5 text-muted-foreground">
                    {selectedSummary.ready} readable channel
                    {selectedSummary.ready === 1 ? "" : "s"} · {privateReadable}{" "}
                    readable private · {landingChannels} with observations
                    landing
                  </span>
                </span>
                <Badge
                  tone={selectedSummary.missingAccess ? "warning" : "success"}
                  className="w-fit"
                >
                  {selectedSummary.missingAccess
                    ? "Private access needed"
                    : "Access healthy"}
                </Badge>
              </div>

              <div className="grid min-w-0 gap-2 rounded-md border border-info/30 bg-info/10 p-3 text-xs leading-5 text-foreground md:grid-cols-[auto_minmax(0,1fr)]">
                <LockKeyhole
                  className="mt-0.5 h-4 w-4 text-info"
                  aria-hidden="true"
                />
                <span className="min-w-0 break-words">
                  Use Full Server Sync to authorize Fyralis with Administrator
                  access. Fyralis can then read every channel this server has,
                  including private channels, without per-channel role setup.
                </span>
              </div>

              {selectedResources.length ? (
                <div className="max-h-[28rem] min-w-0 overflow-auto rounded-md border border-border bg-card">
                  {selectedResources.map((resource) => (
                    <DiscordChannelAccessRow
                      key={`${resource.installationId}:${resource.resourceId}`}
                      resource={resource}
                    />
                  ))}
                </div>
              ) : (
                <div className="rounded-md border border-border bg-card p-4 text-sm text-muted-foreground">
                  No message channels or threads were returned by Discord for
                  this server.
                </div>
              )}
            </div>
          ) : null}
        </div>
      ) : resources.length ? (
        <div className="rounded-md border border-border bg-card p-4 text-sm text-muted-foreground">
          Discord channel status is available, but no server installation row
          was returned.
        </div>
      ) : null}
    </div>
  );
}

function DiscordChannelAccessRow({
  resource,
}: {
  resource: SourceAccessResource;
}) {
  const backfill = channelBackfillState(resource);
  return (
    <div className="grid min-w-0 gap-3 border-b border-border px-3 py-3 last:border-b-0 lg:grid-cols-[minmax(0,1.35fr)_auto_minmax(10rem,0.75fr)_auto] lg:items-center">
      <span className="min-w-0">
        <strong className="flex min-w-0 items-center gap-2 text-sm text-foreground">
          {resource.visibility === "private" ? (
            <LockKeyhole
              className="h-4 w-4 shrink-0 text-muted-foreground"
              aria-hidden="true"
            />
          ) : (
            <Hash
              className="h-4 w-4 shrink-0 text-muted-foreground"
              aria-hidden="true"
            />
          )}
          <span className="min-w-0 truncate">
            {formatDiscordChannelName(resource.displayName)}
          </span>
        </strong>
        <span className="mt-1 block min-w-0 truncate text-xs text-muted-foreground">
          {resource.parentName ?? "No category"}
        </span>
      </span>

      <span className="flex flex-wrap items-center gap-2">
        <Badge tone={discordVisibilityTone(resource.visibility)}>
          {discordVisibilityLabel(resource)}
        </Badge>
        <Badge tone={discordAccessTone(resource.permissionStatus)}>
          {discordAccessLabel(resource.permissionStatus)}
        </Badge>
      </span>

      <span className="grid min-w-0 gap-1">
        <span className="flex min-w-0 items-center justify-between gap-2 text-xs">
          <span className="inline-flex min-w-0 items-center gap-1 truncate font-medium text-foreground">
            {backfill.icon}
            {backfill.label}
          </span>
          <span className="shrink-0 text-muted-foreground">
            {resource.observationCount > 0
              ? `${resource.observationCount} landed`
              : "Waiting"}
          </span>
        </span>
        <ProgressBar value={backfill.percent} tone={backfill.tone} />
      </span>

      <span className="text-xs text-muted-foreground lg:text-right">
        {resource.lastObservationAt
          ? `Last ${formatShortDateTime(resource.lastObservationAt)}`
          : resource.canBackfill
            ? "Queued"
            : "Paused"}
      </span>

      {typeof resource.diagnostics.message === "string" ? (
        <span className="min-w-0 break-words text-xs leading-5 text-muted-foreground lg:col-span-4">
          {resource.diagnostics.message}
        </span>
      ) : null}
    </div>
  );
}

function ProgressBar({
  value,
  tone = "info",
}: {
  value: number;
  tone?: "success" | "warning" | "info" | "muted";
}) {
  return (
    <span className="block h-2 min-w-0 overflow-hidden rounded-full bg-muted">
      <span
        className={cn(
          "block h-full rounded-full transition-all duration-500",
          tone === "success" && "bg-success",
          tone === "warning" && "bg-warning",
          tone === "info" && "bg-info",
          tone === "muted" && "bg-muted-foreground/40",
        )}
        style={{ width: `${clampPercent(value)}%` }}
      />
    </span>
  );
}

function AccessMetric({
  label,
  value,
  icon,
  tone = "default",
}: {
  label: string;
  value: string;
  icon?: "server";
  tone?: "default" | "success" | "warning";
}) {
  return (
    <span
      className={cn(
        "grid min-w-0 gap-1 rounded-md border border-border bg-card px-3 py-2",
        tone === "success" && "border-success/30 bg-success/10",
        tone === "warning" && "border-warning/40 bg-warning/10",
      )}
    >
      <span className="flex min-w-0 items-center gap-1 text-[11px] font-medium uppercase tracking-normal text-muted-foreground">
        {icon === "server" ? (
          <Server className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
        ) : null}
        <span className="truncate">{label}</span>
      </span>
      <strong className="truncate text-base font-semibold text-foreground">
        {value}
      </strong>
    </span>
  );
}

type DiscordServerAccess = {
  installationId: string;
  name: string;
  enabled: boolean;
  installedAt: string | null;
  resources: SourceAccessResource[];
};

function buildDiscordServers(
  installations: SourceInstallation[],
  resources: SourceAccessResource[],
): DiscordServerAccess[] {
  const byId = new Map<string, DiscordServerAccess>();
  for (const installation of installations) {
    byId.set(installation.installationId, {
      installationId: installation.installationId,
      name: discordServerName(installation, resources),
      enabled: installation.enabled,
      installedAt: installation.installedAt,
      resources: [],
    });
  }
  for (const resource of resources) {
    const existing = byId.get(resource.installationId);
    if (existing) {
      existing.resources.push(resource);
      if (!existing.name || existing.name.startsWith("Server ")) {
        existing.name =
          resource.installationName ??
          discordServerFallback(resource.installationId);
      }
      continue;
    }
    byId.set(resource.installationId, {
      installationId: resource.installationId,
      name:
        resource.installationName ??
        discordServerFallback(resource.installationId),
      enabled: true,
      installedAt: null,
      resources: [resource],
    });
  }
  return Array.from(byId.values()).sort((left, right) => {
    if (left.enabled !== right.enabled) {
      return left.enabled ? -1 : 1;
    }
    return left.name.localeCompare(right.name);
  });
}

function discordServerName(
  installation: SourceInstallation,
  resources: SourceAccessResource[],
) {
  const fromDetails = detailString(installation.details, [
    "server_name",
    "guild_name",
    "name",
  ]);
  if (fromDetails) {
    return fromDetails;
  }
  const fromResource = resources.find(
    (resource) =>
      resource.installationId === installation.installationId &&
      resource.installationName,
  )?.installationName;
  return fromResource ?? discordServerFallback(installation.installationId);
}

function detailString(details: Record<string, unknown>, keys: string[]) {
  for (const key of keys) {
    const value = details[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function discordServerFallback(installationId: string) {
  const suffix =
    installationId.length > 6 ? installationId.slice(-6) : installationId;
  return suffix ? `Server ${suffix}` : "Discord server";
}

function summarizeAccessResources(resources: SourceAccessResource[]) {
  return resources.reduce(
    (summary, resource) => {
      summary.total += 1;
      if (resource.permissionStatus === "ready") {
        summary.ready += 1;
      } else if (resource.permissionStatus === "missing_access") {
        summary.missingAccess += 1;
      } else if (resource.permissionStatus === "needs_admin") {
        summary.needsAdmin += 1;
      } else {
        summary.unknown += 1;
      }
      if (resource.observationCount > 0) {
        summary.observed += 1;
      }
      return summary;
    },
    {
      total: 0,
      ready: 0,
      missingAccess: 0,
      needsAdmin: 0,
      unknown: 0,
      observed: 0,
    },
  );
}

function serverBackfillPercent(resources: SourceAccessResource[]) {
  const ready = resources.filter(
    (resource) => resource.permissionStatus === "ready",
  );
  if (!ready.length) {
    return 0;
  }
  const observed = ready.filter((resource) => resource.observationCount > 0);
  if (!observed.length) {
    return 35;
  }
  return Math.max(35, Math.round((observed.length / ready.length) * 100));
}

function channelBackfillState(resource: SourceAccessResource): {
  label: string;
  percent: number;
  tone: "success" | "warning" | "info" | "muted";
  icon: ReactNode;
} {
  if (resource.permissionStatus === "missing_access") {
    return {
      label: "Paused",
      percent: 0,
      tone: "warning",
      icon: <AlertTriangle className="h-3.5 w-3.5" aria-hidden="true" />,
    };
  }
  if (resource.permissionStatus === "needs_admin") {
    return {
      label: "Admin",
      percent: 0,
      tone: "warning",
      icon: <AlertTriangle className="h-3.5 w-3.5" aria-hidden="true" />,
    };
  }
  if (resource.observationCount > 0) {
    return {
      label: "Landing",
      percent: 100,
      tone: "success",
      icon: <CheckCircle2 className="h-3.5 w-3.5" aria-hidden="true" />,
    };
  }
  if (resource.canBackfill) {
    return {
      label: "Scanning",
      percent: 45,
      tone: "info",
      icon: <Activity className="h-3.5 w-3.5" aria-hidden="true" />,
    };
  }
  return {
    label: "Waiting",
    percent: 10,
    tone: "muted",
    icon: <Activity className="h-3.5 w-3.5" aria-hidden="true" />,
  };
}

function clampPercent(value: number) {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.min(100, value));
}

function discordVisibilityTone(
  visibility: SourceAccessResource["visibility"],
): "info" | "muted" | "warning" {
  if (visibility === "private") {
    return "warning";
  }
  if (visibility === "public") {
    return "info";
  }
  return "muted";
}

function discordVisibilityLabel(resource: SourceAccessResource) {
  const visibility = resource.visibility;
  if (visibility === "private") {
    if (resource.permissionStatus === "ready") {
      return "Private, accessible";
    }
    return "Private";
  }
  if (visibility === "public") {
    return "Public";
  }
  return "Visibility unknown";
}

function discordAccessTone(
  status: SourceAccessResource["permissionStatus"],
): "success" | "warning" | "info" | "muted" | "error" {
  if (status === "ready") {
    return "success";
  }
  if (status === "missing_access") {
    return "warning";
  }
  if (status === "needs_admin") {
    return "error";
  }
  if (status === "not_selected") {
    return "muted";
  }
  return "info";
}

function discordAccessLabel(status: SourceAccessResource["permissionStatus"]) {
  if (status === "ready") {
    return "Ready";
  }
  if (status === "missing_access") {
    return "Needs access";
  }
  if (status === "needs_admin") {
    return "Admin";
  }
  if (status === "not_selected") {
    return "Not selected";
  }
  return "Check";
}

function formatDiscordChannelName(name: string) {
  const trimmed = name.trim();
  if (!trimmed) {
    return "#unknown";
  }
  return trimmed.startsWith("#") ? trimmed : `#${trimmed}`;
}

function formatShortDateTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatElapsedDuration(ms: number) {
  if (!Number.isFinite(ms) || ms < 0) {
    return "0:00";
  }
  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function sourceApprovalInstruction(source: Source) {
  if (source.id === "aws") {
    return "Next: sign in to AWS, review the generated read-only CloudFormation role stack, approve IAM creation, then return here.";
  }
  if (source.id === "figma") {
    return "Next: a deployment administrator completes the one-time private Figma app setup in Control Panel.";
  }
  return "Next: approve the provider prompt or create the requested least-privilege credential, then return here.";
}

function sourceApprovalActionLabel(source: Source) {
  if (source.id === "aws") {
    return "Open AWS approval";
  }
  if (source.id === "github") {
    return "Open GitHub approval";
  }
  if (source.id === "discord") {
    return "Open Discord";
  }
  return "Open settings";
}

function sourceActionLabel({
  sourceId,
  sourceName,
  authorize,
  waiting,
  blocked,
  connecting,
  connected,
  canAddDiscordServer,
  canOpenApproval,
  actionLabel,
}: {
  sourceId?: string;
  sourceName: string;
  authorize: boolean;
  waiting: boolean;
  blocked: boolean;
  connecting: boolean;
  connected: boolean;
  canAddDiscordServer: boolean;
  canOpenApproval: boolean;
  actionLabel?: string;
}) {
  if (authorize) {
    return `Authorize ${sourceName}`;
  }
  if (blocked && canOpenApproval && actionLabel) {
    return `${actionLabel} for ${sourceName}`;
  }
  if (sourceId === "figma" && canOpenApproval && actionLabel) {
    return `${actionLabel} for ${sourceName}`;
  }
  if (waiting || blocked) {
    if (canOpenApproval) {
      return `Open ${sourceName} provider settings for approval`;
    }
    return `Get ${sourceName} provider settings link for approval`;
  }
  if (connecting) {
    return `${sourceName} connecting`;
  }
  if (canAddDiscordServer) {
    return `Connect another ${sourceName} server with Full Server Sync`;
  }
  if (connected) {
    return `${sourceName} connected`;
  }
  if (sourceId === "discord") {
    return `Connect ${sourceName} with Full Server Sync`;
  }
  return `Connect ${sourceName}`;
}

function awsRuntimeRoleArnFromInput(value: string) {
  const trimmed = value.trim();
  if (/^arn:aws[a-zA-Z-]*:iam::\d{12}:role\/[\w+=,.@/-]+$/.test(trimmed)) {
    return trimmed;
  }
  const accountDigits = Array.from(trimmed)
    .filter((char) => char >= "0" && char <= "9")
    .join("");
  if (accountDigits.length === 12) {
    return `arn:aws:iam::${accountDigits}:role/fyralis-source-runtime`;
  }
  return null;
}

function awsSourceRoleArnFromInput(value: string) {
  const trimmed = value.trim();
  if (/^arn:aws[a-zA-Z-]*:iam::\d{12}:role\/[\w+=,.@/-]+$/.test(trimmed)) {
    return trimmed;
  }
  const accountDigits = Array.from(trimmed)
    .filter((char) => char >= "0" && char <= "9")
    .join("");
  if (accountDigits.length === 12) {
    return `arn:aws:iam::${accountDigits}:role/fyralis-source-readonly`;
  }
  return null;
}
