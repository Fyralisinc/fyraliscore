import { BarChart3, ExternalLink, Gauge, ShieldCheck } from "lucide-react";
import type { Metadata } from "next";
import Link from "next/link";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LinkButton, ProductShell } from "@/features/platform/components/product-shell";
import { OBSERVABILITY_SURFACES } from "@/features/platform/data/surfaces";

export const metadata: Metadata = {
  title: "Fyralis Host Observability",
  description: "Internal Grafana, Prometheus, and dashboard-as-code surfaces."
};

export default function HostObservabilityPage() {
  return (
    <ProductShell
      active="/host/observability"
      eyebrow="Host observability"
      title="Grafana, Prometheus, and dashboard-as-code for Fyralis operators."
      description="Internal production-readiness and BYOC operations surfaces. These are not part of the customer-facing landing site or customer-cloud source setup flow."
      actions={
        <>
          <LinkButton href="/host/control-panel" variant="secondary">
            Control panel
          </LinkButton>
          <LinkButton href="/host/surfaces" variant="secondary">
            API surfaces
          </LinkButton>
        </>
      }
    >
      <div className="grid gap-5">
        <section className="grid gap-4 md:grid-cols-3">
          <StatusTile
            label="Grafana"
            value="127.0.0.1:${GRAFANA_HOST_PORT:-3000}"
            detail="Provisioned dashboards in the Fyralis folder."
          />
          <StatusTile
            label="Prometheus"
            value="127.0.0.1:${PROMETHEUS_HOST_PORT:-9090}"
            detail="Metrics query UI and scrape backend."
          />
          <StatusTile
            label="Dashboard source"
            value="observability/grafana/dashboards"
            detail="Repository-owned dashboard-as-code."
          />
        </section>

        <section className="grid gap-4 lg:grid-cols-2">
          {OBSERVABILITY_SURFACES.map((surface) => (
            <Card key={surface.name}>
              <CardHeader className="items-start">
                <div>
                  <div className="flex items-center gap-2">
                    <BarChart3 className="h-5 w-5 text-info" aria-hidden="true" />
                    <CardTitle>{surface.name}</CardTitle>
                  </div>
                  <p className="mt-2 text-sm leading-6 text-muted-foreground">
                    {surface.purpose}
                  </p>
                </div>
                <Badge tone={surface.href ? "success" : "muted"}>
                  {surface.href ? "Openable" : "Code"}
                </Badge>
              </CardHeader>
              <CardContent className="grid gap-4">
                <code className="break-words rounded-md border border-border bg-background/70 px-3 py-2 text-sm">
                  {surface.location}
                </code>
                <div className="rounded-lg border border-border bg-background/70 p-4">
                  <div className="flex items-center gap-2 text-sm font-semibold">
                    <ShieldCheck className="h-4 w-4 text-success" aria-hidden="true" />
                    Boundary
                  </div>
                  <p className="mt-2 text-sm leading-6 text-muted-foreground">
                    {surface.boundary}
                  </p>
                </div>
                {surface.href ? (
                  <Link
                    href={surface.href}
                    className="inline-flex min-h-10 w-fit items-center gap-2 rounded-md border border-border bg-card px-4 text-sm font-semibold transition hover:border-ring hover:bg-accent"
                  >
                    Open surface
                    <ExternalLink className="h-4 w-4" aria-hidden="true" />
                  </Link>
                ) : null}
              </CardContent>
            </Card>
          ))}
        </section>
      </div>
    </ProductShell>
  );
}

function StatusTile({
  label,
  value,
  detail
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-5 shadow-panel">
      <div className="flex items-center gap-2 text-xs font-semibold text-muted-foreground">
        <Gauge className="h-4 w-4 text-info" aria-hidden="true" />
        {label}
      </div>
      <strong className="mt-3 block break-words text-lg tracking-normal">
        {value}
      </strong>
      <p className="mt-2 text-sm leading-6 text-muted-foreground">{detail}</p>
    </div>
  );
}
