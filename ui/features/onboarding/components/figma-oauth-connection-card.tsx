"use client";

import { useMemo } from "react";
import {
  CheckCircle2,
  CircleAlert,
  ExternalLink,
  FileText,
  LoaderCircle,
  RefreshCw,
  ShieldCheck,
  Unplug,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

import {
  useFigmaOAuthConnection,
  type FigmaOAuthConnectionController,
} from "../hooks/use-figma-oauth-connection";
import type {
  FigmaConnectionObservation,
  FigmaConnectionStatus,
  FigmaOAuthClientOptions,
} from "../services/figma-oauth-service";

export type FigmaOAuthConnectionCardProps = FigmaOAuthClientOptions & {
  className?: string;
  /** Non-secret file URLs to show initially, one per line. */
  initialFileUrls?: string[];
  /** Relative callback return path; defaults to the current route. */
  returnPath?: string;
  pollIntervalMs?: number;
  /**
   * Test/integration seam. Without it, authorization uses a top-level browser
   * navigation to Figma.
   */
  onAuthorize?: (authorizationUrl: string) => void | Promise<void>;
  onStatusChange?: (status: FigmaConnectionStatus) => void;
  /** Lets the host surface link to the observation workspace without coupling this card to it. */
  onOpenObservation?: (
    observation: FigmaConnectionObservation,
    status: FigmaConnectionStatus,
  ) => void;
};

/**
 * A self-contained Figma OAuth onboarding panel. It only receives Figma file
 * URLs and displays sanitized gateway status; no credential field or browser
 * storage is used anywhere in this flow.
 */
export function FigmaOAuthConnectionCard({
  className,
  initialFileUrls,
  apiBase,
  gatewayToken,
  returnPath,
  pollIntervalMs,
  onAuthorize,
  onStatusChange,
  onOpenObservation,
}: FigmaOAuthConnectionCardProps) {
  const controller = useFigmaOAuthConnection({
    apiBase,
    gatewayToken,
    initialFileUrlInput: initialFileUrls?.join("\n"),
    returnPath,
    pollIntervalMs,
    onAuthorize,
    onStatusChange,
  });
  const status = controller.status;
  const deploymentSetupRequired =
    status?.state === "deployment_setup_required";
  const canDisconnect = Boolean(status?.installationId) &&
    status?.state !== "disconnected";

  return (
    <Card className={cn("w-full", className)} data-testid="figma-oauth-connection-card">
      <CardHeader className="items-start">
        <div className="min-w-0">
          <CardTitle className="flex items-center gap-2">
            <span>Connect Figma</span>
            {status ? <ConnectionStateBadge state={status.state} /> : null}
          </CardTitle>
          <p className="mt-1 text-sm leading-6 text-muted-foreground">
            {deploymentSetupRequired
              ? "A deployment administrator must finish this customer-cloud Figma app setup before files can be connected."
              : "Select Figma files, approve read access, and Fyralis will create a design snapshot observation for each file."}
          </p>
        </div>
        <ShieldCheck className="h-5 w-5 shrink-0 text-success" aria-hidden="true" />
      </CardHeader>
      <CardContent className="grid gap-5">
        <ConnectionFeedback controller={controller} />

        {controller.installationIds.length > 1 ? (
          <FigmaInstallationSelector controller={controller} />
        ) : null}

        {deploymentSetupRequired ? (
          <DeploymentSetupRequiredNotice
            loading={controller.loading}
            onRefresh={() => void controller.refresh()}
          />
        ) : (
          <>
            <form
              className="grid gap-3 rounded-lg border border-border bg-background/40 p-4"
              onSubmit={(event) => {
                event.preventDefault();
                void controller.connect();
              }}
            >
              <label className="grid gap-1.5 text-sm font-semibold" htmlFor="figma-file-urls">
                Figma file URLs
                <span className="font-normal text-xs leading-5 text-muted-foreground">
                  Paste one or more file, design, or prototype URLs. This
                  deployment’s Figma OAuth app connects only the files you
                  explicitly select.
                </span>
              </label>
              <textarea
                id="figma-file-urls"
                name="figma-file-urls"
                aria-label="Figma file URLs"
                className="min-h-28 w-full resize-y rounded-md border border-input bg-background px-3 py-2 text-sm text-foreground outline-none transition-colors placeholder:text-muted-foreground focus:border-ring"
                value={controller.fileUrlInput}
                onChange={(event) => {
                  controller.setFileUrlInput(event.target.value);
                  controller.clearError();
                }}
                placeholder={"https://www.figma.com/design/FILE_KEY/Checkout\nhttps://www.figma.com/file/ANOTHER_KEY/Design-system"}
                disabled={controller.starting || controller.disconnecting}
              />
              <div className="flex flex-wrap items-center justify-between gap-3">
                <p className="max-w-xl text-xs leading-5 text-muted-foreground">
                  Figma tokens never enter this browser. Fyralis stores OAuth
                  credentials encrypted in the customer cloud after you approve
                  Figma’s consent screen.
                </p>
                <Button
                  type="submit"
                  disabled={controller.starting || controller.disconnecting || !controller.fileUrlInput.trim()}
                >
                  {controller.starting ? (
                    <>
                      <LoaderCircle className="h-4 w-4 animate-spin" aria-hidden="true" />
                      Opening Figma…
                    </>
                  ) : (
                    <>
                      <ExternalLink className="h-4 w-4" aria-hidden="true" />
                      Continue with Figma
                    </>
                  )}
                </Button>
              </div>
            </form>

            <ConnectionProof
              status={status}
              loading={controller.loading}
              onRefresh={() => void controller.refresh()}
              onRetry={() => void controller.retry()}
              onDisconnect={() => void controller.disconnect()}
              retrying={controller.retrying}
              disconnecting={controller.disconnecting}
              canDisconnect={canDisconnect}
              onOpenObservation={onOpenObservation}
            />
          </>
        )}
      </CardContent>
    </Card>
  );
}

function FigmaInstallationSelector({
  controller,
}: {
  controller: FigmaOAuthConnectionController;
}) {
  return (
    <section className="grid gap-2 rounded-lg border border-border bg-background/40 p-4">
      <label
        className="text-sm font-semibold"
        htmlFor="figma-installation-selector"
      >
        Figma installation
      </label>
      <p className="text-xs leading-5 text-muted-foreground">
        This tenant has more than one Figma connection. Choose the exact
        connection whose status and actions you want to manage.
      </p>
      <select
        id="figma-installation-selector"
        aria-label="Figma installation"
        className="h-10 w-full rounded-md border border-input bg-background px-3 text-sm text-foreground outline-none transition-colors focus:border-ring"
        value={controller.selectedInstallationId ?? ""}
        onChange={(event) => {
          void controller.selectInstallation(event.target.value);
        }}
        disabled={
          controller.selectingInstallation ||
          controller.retrying ||
          controller.disconnecting
        }
      >
        <option value="">Select an installation</option>
        {controller.installationIds.map((installationId) => (
          <option key={installationId} value={installationId}>
            {installationLabel(installationId)}
          </option>
        ))}
      </select>
      {controller.selectingInstallation ? (
        <span
          className="flex items-center gap-2 text-xs text-muted-foreground"
          role="status"
        >
          <LoaderCircle
            className="h-3.5 w-3.5 animate-spin"
            aria-hidden="true"
          />
          Loading selected installation…
        </span>
      ) : null}
    </section>
  );
}

function ConnectionFeedback({
  controller,
}: {
  controller: FigmaOAuthConnectionController;
}) {
  const message = useMemo(() => {
    if (controller.status?.state === "deployment_setup_required") {
      return null;
    }
    if (controller.error) {
      return { tone: "error" as const, value: controller.error };
    }
    if (controller.callbackOutcome === "error") {
      return {
        tone: "error" as const,
        value: "Figma did not complete the connection. You can try again when you are ready.",
      };
    }
    if (controller.callbackOutcome === "connected") {
      return {
        tone: "success" as const,
        value: "Figma authorization completed. Fyralis is validating the selected files and starting the first sync.",
      };
    }
    if (controller.status?.nextAction) {
      return { tone: "info" as const, value: controller.status.nextAction };
    }
    return null;
  }, [
    controller.callbackOutcome,
    controller.error,
    controller.status?.nextAction,
    controller.status?.state,
  ]);

  if (!message) {
    return null;
  }
  return (
    <div
      className={cn(
        "flex gap-2 rounded-md border p-3 text-sm leading-6",
        message.tone === "error" && "border-destructive/30 bg-destructive/10 text-foreground",
        message.tone === "success" && "border-success/30 bg-success/10 text-foreground",
        message.tone === "info" && "border-info/30 bg-info/10 text-foreground",
      )}
      role={message.tone === "error" ? "alert" : "status"}
      aria-live="polite"
    >
      {message.tone === "error" ? (
        <CircleAlert className="mt-0.5 h-4 w-4 shrink-0 text-destructive" aria-hidden="true" />
      ) : (
        <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-success" aria-hidden="true" />
      )}
      <span>{message.value}</span>
    </div>
  );
}

/**
 * The customer chose the isolated BYOC model: this deployment owns the
 * provider app. Deliberately show no client ID, callback URL, missing-setting
 * list, or secret-entry controls to ordinary source users.
 */
function DeploymentSetupRequiredNotice({
  loading,
  onRefresh,
}: {
  loading: boolean;
  onRefresh: () => void;
}) {
  return (
    <section
      className="grid gap-3 rounded-lg border border-warning/40 bg-warning/10 p-4"
      aria-labelledby="figma-deployment-setup-title"
      data-testid="figma-deployment-setup-required"
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 id="figma-deployment-setup-title" className="text-sm font-semibold">
            Deployment administrator setup required
          </h3>
          <p className="mt-1 text-sm leading-6 text-foreground">
            This customer-cloud deployment uses its own Figma OAuth app. Ask a
            deployment administrator to finish the app setup before anyone
            connects Figma.
          </p>
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            The administrator configures the deployment privately. No Figma
            credentials or configuration values are shown or requested here.
          </p>
          <a
            className="mt-2 inline-flex text-xs font-semibold text-primary underline underline-offset-4"
            href="/host/control-panel"
          >
            Deployment administrators: check setup in Control Panel
          </a>
        </div>
        <Badge tone="warning">Setup required</Badge>
      </div>
      <div>
        <Button type="button" variant="secondary" onClick={onRefresh} disabled={loading}>
          {loading ? (
            <LoaderCircle className="h-4 w-4 animate-spin" aria-hidden="true" />
          ) : (
            <RefreshCw className="h-4 w-4" aria-hidden="true" />
          )}
          Refresh status
        </Button>
      </div>
    </section>
  );
}

function ConnectionProof({
  status,
  loading,
  onRefresh,
  onRetry,
  onDisconnect,
  retrying,
  disconnecting,
  canDisconnect,
  onOpenObservation,
}: {
  status: FigmaConnectionStatus | null;
  loading: boolean;
  onRefresh: () => void;
  onRetry: () => void;
  onDisconnect: () => void;
  retrying: boolean;
  disconnecting: boolean;
  canDisconnect: boolean;
  onOpenObservation?: (
    observation: FigmaConnectionObservation,
    status: FigmaConnectionStatus,
  ) => void;
}) {
  if (loading && !status) {
    return (
      <div className="flex items-center gap-2 rounded-lg border border-border bg-muted/40 p-4 text-sm text-muted-foreground" role="status">
        <LoaderCircle className="h-4 w-4 animate-spin" aria-hidden="true" />
        Checking Figma connection status…
      </div>
    );
  }

  if (!status || ["not_connected", "disconnected", "unknown"].includes(status.state)) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-muted/30 p-4 text-sm text-muted-foreground">
        No Figma connection yet. After approval, this panel will show the first
        snapshot observation as it lands.
      </div>
    );
  }

  if (status.state === "multiple_installations") {
    return (
      <div className="rounded-lg border border-info/30 bg-info/10 p-4 text-sm text-foreground">
        Select a Figma installation above to load its files, observations, and
        management actions.
      </div>
    );
  }

  const inProgress = [
    "ready_for_provider_approval",
    "authorizing",
    "finalizing",
    "syncing",
  ].includes(status.state);
  const needsRetry = ["error", "degraded", "reauthorization_required"].includes(
    status.state,
  );

  return (
    <section className="grid gap-4 rounded-lg border border-border bg-background/40 p-4" aria-labelledby="figma-connection-proof-title">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 id="figma-connection-proof-title" className="text-sm font-semibold">
            Connection and first observation
          </h3>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            {inProgress
              ? "Fyralis is automatically polling while Figma authorization and the first sync finish."
              : "This status is returned by the customer-cloud gateway."}
          </p>
        </div>
        <ConnectionStateBadge state={status.state} />
      </div>

      <dl className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <ProofMetric label="Selected files" value={String(status.selectedFileCount || status.fileCount)} />
        <ProofMetric
          label="Synced files"
          value={status.fileCount ? `${status.syncedFileCount}/${status.fileCount}` : String(status.syncedFileCount)}
        />
        <ProofMetric label="Observations landed" value={String(status.observationCount)} />
        <ProofMetric
          label="Latest landing"
          value={formatDate(status.latestObservationAt)}
          title={status.latestObservationAt ?? undefined}
        />
      </dl>

      {status.lastError ? (
        <div className="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-foreground" role="alert">
          {status.lastError}
        </div>
      ) : null}

      {status.latestObservation ? (
        <article className="grid gap-2 rounded-md border border-success/30 bg-success/10 p-3" data-testid="figma-latest-observation">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <span className="flex items-center gap-2 text-sm font-semibold">
              <FileText className="h-4 w-4 text-success" aria-hidden="true" />
              Observation landed
            </span>
            <Badge tone="success">{status.latestObservation.kind}</Badge>
          </div>
          <p className="text-sm text-foreground">
            {status.latestObservation.contentText ?? "Figma design snapshot is ready for intelligence processing."}
          </p>
          <p className="break-all text-xs text-muted-foreground">
            Observation {status.latestObservation.id}
            {status.latestObservation.artifactId
              ? ` · artifact ${status.latestObservation.artifactId}`
              : ""}
          </p>
          {onOpenObservation ? (
            <div>
              <Button
                type="button"
                variant="secondary"
                onClick={() => onOpenObservation(status.latestObservation!, status)}
              >
                Open observation
              </Button>
            </div>
          ) : null}
        </article>
      ) : null}

      {status.files.length ? (
        <div className="grid gap-2">
          <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Connected files
          </h4>
          <ul className="grid gap-2" aria-label="Connected Figma files">
            {status.files.map((file) => (
              <li key={file.fileKey} className="flex min-w-0 items-center justify-between gap-3 rounded-md border border-border px-3 py-2 text-sm">
                <span className="min-w-0 truncate">{file.fileName ?? file.fileKey}</span>
                <span className="shrink-0 text-xs text-muted-foreground">
                  {file.observationCount} observations
                </span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="flex flex-wrap gap-2">
        <Button type="button" variant="secondary" onClick={onRefresh} disabled={disconnecting}>
          <RefreshCw className="h-4 w-4" aria-hidden="true" />
          Refresh status
        </Button>
        {needsRetry ? (
          <Button type="button" onClick={onRetry} disabled={retrying || disconnecting}>
            {retrying ? <LoaderCircle className="h-4 w-4 animate-spin" aria-hidden="true" /> : <RefreshCw className="h-4 w-4" aria-hidden="true" />}
            {status.state === "reauthorization_required" ? "Reconnect Figma" : "Retry sync"}
          </Button>
        ) : null}
        {canDisconnect ? (
          <Button type="button" variant="danger" onClick={onDisconnect} disabled={disconnecting}>
            {disconnecting ? <LoaderCircle className="h-4 w-4 animate-spin" aria-hidden="true" /> : <Unplug className="h-4 w-4" aria-hidden="true" />}
            Disconnect
          </Button>
        ) : null}
      </div>
    </section>
  );
}

function ProofMetric({
  label,
  value,
  title,
}: {
  label: string;
  value: string;
  title?: string;
}) {
  return (
    <div className="rounded-md border border-border bg-card px-3 py-2">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="mt-1 truncate text-sm font-semibold" title={title}>
        {value}
      </dd>
    </div>
  );
}

function ConnectionStateBadge({
  state,
}: {
  state: FigmaConnectionStatus["state"];
}) {
  const display = state.replaceAll("_", " ");
  const tone =
    state === "connected"
      ? "success"
      : state === "deployment_setup_required"
        ? "warning"
      : ["error", "degraded", "reauthorization_required"].includes(state)
        ? "error"
        : [
              "syncing",
              "authorizing",
              "finalizing",
              "ready_for_provider_approval",
              "multiple_installations",
            ].includes(state)
          ? "info"
          : "muted";
  return <Badge tone={tone}>{display}</Badge>;
}

function installationLabel(installationId: string): string {
  const suffix =
    installationId.length > 12
      ? installationId.slice(-12)
      : installationId;
  return `Installation …${suffix}`;
}

function formatDate(value: string | null): string {
  if (!value) {
    return "Waiting";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}
