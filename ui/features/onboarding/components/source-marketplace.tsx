"use client";

import { useState } from "react";
import { ExternalLink } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

import { sourceStatusLabel } from "../state/onboarding-store";
import type { Source, SourceConnection } from "../types";

export type SourceAutomationCardState = {
  status: "idle" | "connecting" | "waiting_admin" | "connected" | "blocked" | "error";
  label: string;
  message?: string;
  actionUrl?: string | null;
  actionLabel?: string;
  receiptPathHint?: string | null;
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
  onRetryAwsFirstSync
}: {
  sources: Source[];
  connections: SourceConnection[];
  selectedSourceId: string;
  automationStates?: Record<string, SourceAutomationCardState>;
  onSelect: (sourceId: string) => void;
  onConnect: (sourceId: string) => void;
  onRegisterAwsRuntimeRole?: (roleArn: string) => void | Promise<void>;
  onFinalizeAwsSourceRole?: (roleArn: string) => void | Promise<void>;
  onRetryAwsFirstSync?: () => void | Promise<void>;
}) {
  const [awsRuntimeRoleInput, setAwsRuntimeRoleInput] = useState("");
  const [awsRuntimeRoleError, setAwsRuntimeRoleError] = useState<string | null>(
    null
  );
  const [awsSourceRoleInput, setAwsSourceRoleInput] = useState("");
  const [awsSourceRoleError, setAwsSourceRoleError] = useState<string | null>(
    null
  );

  async function registerAwsRuntimeRole(value: string) {
    const roleArn = awsRuntimeRoleArnFromInput(value);
    if (!roleArn) {
      setAwsRuntimeRoleError("Use a 12-digit AWS account id or a valid IAM role ARN.");
      return;
    }
    setAwsRuntimeRoleError(null);
    await onRegisterAwsRuntimeRole?.(roleArn);
    setAwsRuntimeRoleInput("");
  }

  async function finalizeAwsSourceRole(value: string) {
    const roleArn = awsSourceRoleArnFromInput(value);
    if (!roleArn) {
      setAwsSourceRoleError("Use the RoleArn output or a 12-digit AWS account id.");
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
              (item) => item.sourceId === source.id
            );
            const status = connection?.status ?? "not-configured";
            const automation = automationStates[source.id];
            const effectiveStatus = automation?.status ?? status;
            const selected = selectedSourceId === source.id;
            const waiting =
              automation?.status === "waiting_admin" || status === "waiting-admin";
            const blocked = automation?.status === "blocked";
            const connecting = automation?.status === "connecting";
            const connected =
              automation?.status === "connected" || status === "connected";
            const approvalActionUrl =
              waiting || blocked ? automation?.actionUrl : null;
            const canOpenApproval = Boolean(approvalActionUrl);
            const actionDisabled = connecting || connected;
            const approvalInstruction = sourceApprovalInstruction(source);
            const approvalActionLabel =
              automation?.actionLabel ?? sourceApprovalActionLabel(source);
            const syncBlocked =
              source.id === "aws" && automation?.status === "error" && connected;

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
                    if (canOpenApproval && approvalActionUrl) {
                      window.open(approvalActionUrl, "_blank", "noopener,noreferrer");
                      return;
                    }
                    onConnect(source.id);
                  }}
                  aria-label={sourceActionLabel({
                    sourceName: source.name,
                    waiting,
                    blocked,
                    connecting,
                    connected,
                    canOpenApproval,
                    actionLabel: approvalActionLabel
                  })}
                >
                  {waiting || blocked
                    ? canOpenApproval
                      ? (
                          <>
                            <ExternalLink className="h-4 w-4" aria-hidden="true" />
                            {approvalActionLabel}
                          </>
                        )
                      : "Get link"
                    : connecting
                      ? "Connecting..."
                      : connected
                        ? "Connected"
                        : "Connect"}
                </Button>
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
                        After the runtime stack is created, Fyralis uses role name fyralis-source-runtime.
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
                    <Button type="submit" className="self-end" variant="secondary">
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
                      Fix the Fyralis runtime AWS identity, then retry the first CloudTrail sync.
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

function sourceApprovalInstruction(source: Source) {
  if (source.id === "aws") {
    return "Next: sign in to AWS, review the generated read-only CloudFormation role stack, approve IAM creation, then return here.";
  }
  return "Next: approve the provider prompt or create the requested least-privilege credential, then return here.";
}

function sourceApprovalActionLabel(source: Source) {
  if (source.id === "aws") {
    return "Open AWS approval";
  }
  return "Open settings";
}

function sourceActionLabel({
  sourceName,
  waiting,
  blocked,
  connecting,
  connected,
  canOpenApproval,
  actionLabel
}: {
  sourceName: string;
  waiting: boolean;
  blocked: boolean;
  connecting: boolean;
  connected: boolean;
  canOpenApproval: boolean;
  actionLabel?: string;
}) {
  if (blocked && canOpenApproval && actionLabel) {
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
  if (connected) {
    return `${sourceName} connected`;
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
