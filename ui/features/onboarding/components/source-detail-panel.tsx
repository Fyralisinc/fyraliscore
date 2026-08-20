"use client";

import { ExternalLink, KeyRound, RadioTower, Shield } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

import type { Source, SourceConnection, Workspace } from "../types";

export function SourceDetailPanel({
  source,
  connection,
  workspace,
  onOpenSetup
}: {
  source: Source;
  connection?: SourceConnection;
  workspace: Workspace;
  onOpenSetup: () => void;
}) {
  const endpointRows = source.providerIngressPaths.length
    ? source.providerIngressPaths.map((path) => `${workspace.providerIngressUrl}${path}`)
    : [source.noIngressReason ?? "No provider ingress required."];

  return (
    <Card className="lg:sticky lg:top-6">
      <CardHeader>
        <div>
          <CardTitle>{source.name}</CardTitle>
          <p className="mt-1 text-sm text-muted-foreground">
            {source.description}
          </p>
        </div>
        <Badge tone={connection?.status === "connected" ? "success" : "info"}>
          {source.method}
        </Badge>
      </CardHeader>
      <CardContent className="grid gap-5">
        <section>
          <h3 className="flex items-center gap-2 text-sm font-semibold">
            <Shield className="h-4 w-4 text-success" aria-hidden="true" />
            Customer-owned endpoints
          </h3>
          <div className="mt-3 grid gap-2 rounded-lg border border-border bg-background/70 p-3">
            <span>
              <span className="block text-xs font-semibold text-muted-foreground">
                Admin console
              </span>
              <code className="mt-1 block break-all text-xs">
                {workspace.localConsoleUrl}
              </code>
            </span>
            <span>
              <span className="block text-xs font-semibold text-muted-foreground">
                Provider ingress
              </span>
              {endpointRows.map((endpoint) => (
                <code key={endpoint} className="mt-1 block break-all text-xs">
                  {endpoint}
                </code>
              ))}
            </span>
          </div>
        </section>

        <section>
          <h3 className="flex items-center gap-2 text-sm font-semibold">
            <KeyRound className="h-4 w-4 text-info" aria-hidden="true" />
            Required permissions
          </h3>
          <div className="mt-3 flex flex-wrap gap-2">
            {source.requiredPermissions.map((permission) => (
              <Badge key={permission} tone="muted">
                {permission}
              </Badge>
            ))}
          </div>
        </section>

        <section>
          <h3 className="flex items-center gap-2 text-sm font-semibold">
            <RadioTower className="h-4 w-4 text-info" aria-hidden="true" />
            Supported sync modes
          </h3>
          <div className="mt-3 flex flex-wrap gap-2">
            {source.supportedSyncModes.map((mode) => (
              <Badge key={mode} tone="info">
                {mode}
              </Badge>
            ))}
          </div>
        </section>

        <div className="rounded-lg border border-success/25 bg-success/10 p-3 text-sm text-muted-foreground">
          Source credentials, OAuth tokens, webhook secrets, sessions, and raw
          payloads stay in the customer cloud. Fyralis receives sanitized status
          and bounded issue codes only.
        </div>

        <Button type="button" onClick={onOpenSetup}>
          <ExternalLink className="h-4 w-4" aria-hidden="true" />
          Open source setup
        </Button>
      </CardContent>
    </Card>
  );
}
