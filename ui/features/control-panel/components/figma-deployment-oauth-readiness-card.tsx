"use client";

import {
  CheckCircle2,
  CircleAlert,
  ExternalLink,
  Loader2,
  RefreshCw,
  ShieldCheck,
} from "lucide-react";
import { useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

import {
  fetchFigmaDeploymentOAuthReadiness,
  type FigmaDeploymentOAuthReadiness,
} from "../api";

type FigmaDeploymentOAuthReadinessCardProps = {
  apiBase: string;
  bearerToken: string;
};

/**
 * Separate from end-user source onboarding: this is the one-time BYOC
 * deployment administrator surface. It displays only the gateway's explicit
 * safe setup contract and never renders secret values or references.
 */
export function FigmaDeploymentOAuthReadinessCard({
  apiBase,
  bearerToken,
}: FigmaDeploymentOAuthReadinessCardProps) {
  const [readiness, setReadiness] =
    useState<FigmaDeploymentOAuthReadiness | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    if (!bearerToken.trim()) {
      setError("Enter a tenant-admin bearer token to check Figma deployment setup.");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      setReadiness(
        await fetchFigmaDeploymentOAuthReadiness({ apiBase, bearerToken }),
      );
    } catch (caught) {
      setError(
        caught instanceof Error
          ? caught.message
          : "Could not load Figma deployment setup.",
      );
    } finally {
      setLoading(false);
    }
  }

  return (
    <Card data-testid="figma-deployment-oauth-readiness-card">
      <CardHeader className="items-start">
        <div>
          <CardTitle className="flex items-center gap-2">
            <ShieldCheck className="h-5 w-5 text-success" aria-hidden="true" />
            Figma deployment setup
          </CardTitle>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            One tenant administrator configures this customer deployment’s
            private Figma OAuth app once. Source users then only approve access
            and select files.
          </p>
        </div>
        <Badge tone={readiness?.runtime_ready ? "success" : "muted"}>
          {readiness?.runtime_ready ? "Ready" : "Admin check"}
        </Badge>
      </CardHeader>
      <CardContent className="grid gap-4">
        <div className="flex flex-wrap gap-2">
          <Button type="button" onClick={() => void load()} disabled={loading}>
            {loading ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
            ) : (
              <RefreshCw className="h-4 w-4" aria-hidden="true" />
            )}
            Check Figma setup
          </Button>
          {readiness?.provider_console_url ? (
            <a
              className="inline-flex min-h-10 items-center justify-center gap-2 rounded-md border border-border bg-card px-4 text-sm font-semibold text-foreground transition-colors hover:border-ring hover:bg-accent"
              href={readiness.provider_console_url}
              target="_blank"
              rel="noreferrer"
            >
              <ExternalLink className="h-4 w-4" aria-hidden="true" />
              Open Figma developer apps
            </a>
          ) : null}
        </div>

        {error ? (
          <div className="flex gap-2 rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-foreground" role="alert">
            <CircleAlert className="mt-0.5 h-4 w-4 shrink-0 text-destructive" aria-hidden="true" />
            <span>{error}</span>
          </div>
        ) : null}

        {readiness ? <ReadinessDetails readiness={readiness} /> : null}
      </CardContent>
    </Card>
  );
}

function ReadinessDetails({
  readiness,
}: {
  readiness: FigmaDeploymentOAuthReadiness;
}) {
  const checks = Object.entries(readiness.checks);
  return (
    <section className="grid gap-4 rounded-lg border border-border bg-background/40 p-4" aria-live="polite">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold">
            {readiness.runtime_ready
              ? "Gateway configuration is ready"
              : "Gateway configuration needs attention"}
          </h3>
          <p className="mt-1 text-sm leading-6 text-muted-foreground">
            Figma app registration itself cannot be verified through an API.
            Confirm the callback and scopes in Figma before enabling users.
          </p>
        </div>
        <Badge tone={readiness.runtime_ready ? "success" : "warning"}>
          {readiness.runtime_ready ? "Runtime ready" : "Setup needed"}
        </Badge>
      </div>

      <dl className="grid gap-3 text-sm md:grid-cols-2">
        <Detail label="Recommended app mode" value={readiness.recommended_app_mode} />
        <Detail label="Figma source enabled" value={readiness.source_enabled ? "Yes" : "No"} />
        <Detail label="OAuth callback" value={readiness.redirect_uri ?? "Not configured"} code />
        <Detail label="UI return origin" value={readiness.ui_return_origin ?? "Not configured"} code />
      </dl>

      <div className="grid gap-2">
        <h4 className="text-sm font-semibold">Required Figma scopes</h4>
        <div className="flex flex-wrap gap-2">
          {readiness.required_scopes.map((scope) => (
            <Badge key={scope} tone="info">{scope}</Badge>
          ))}
        </div>
      </div>

      <div className="grid gap-2">
        <h4 className="text-sm font-semibold">Runtime checks</h4>
        <ul className="grid gap-2 md:grid-cols-2">
          {checks.map(([check, passed]) => (
            <li key={check} className="flex items-center gap-2 text-sm">
              {passed ? (
                <CheckCircle2 className="h-4 w-4 shrink-0 text-success" aria-hidden="true" />
              ) : (
                <CircleAlert className="h-4 w-4 shrink-0 text-warning" aria-hidden="true" />
              )}
              <span>{checkLabel(check)}</span>
            </li>
          ))}
        </ul>
      </div>

      <ol className="grid list-decimal gap-2 pl-5 text-sm leading-6 text-muted-foreground">
        {readiness.setup_checklist.map((step) => <li key={step}>{step}</li>)}
      </ol>
    </section>
  );
}

function Detail({
  label,
  value,
  code = false,
}: {
  label: string;
  value: string;
  code?: boolean;
}) {
  return (
    <div className="grid gap-1">
      <dt className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{label}</dt>
      <dd className={code ? "break-all rounded bg-muted px-2 py-1 font-mono text-xs" : "font-medium"}>{value}</dd>
    </div>
  );
}

function checkLabel(value: string): string {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}
