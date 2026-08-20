import { Braces, ShieldCheck } from "lucide-react";
import type { Metadata } from "next";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LinkButton, ProductShell } from "@/features/platform/components/product-shell";
import { API_SURFACES } from "@/features/platform/data/surfaces";

export const metadata: Metadata = {
  title: "Fyralis Host API Surfaces",
  description: "Internal backend route families that power Fyralis UI surfaces."
};

export default function HostSurfacesPage() {
  return (
    <ProductShell
      active="/host/surfaces"
      eyebrow="Host API surfaces"
      title="Backend contracts for hosted control and customer-cloud handoff."
      description="Internal map of route families that power BYOC operations, provider ingress, customer-cloud evidence, and metadata-only control-panel status."
      actions={
        <>
          <LinkButton href="/host/control-panel" variant="secondary">
            Control panel
          </LinkButton>
          <LinkButton href="/host/observability" variant="secondary">
            Observability
          </LinkButton>
        </>
      }
    >
      <div className="grid gap-5">
        <section className="grid gap-4 xl:grid-cols-4">
          {[
            ["Control-panel metadata", "/byoc/control-panel/*"],
            ["Control-plane evidence", "/byoc/control-plane/*"],
            ["Customer-cloud agent", "/byoc/agent/*"],
            ["Provider ingress", "/integrations/* + /webhooks/*"]
          ].map(([label, value]) => (
            <div
              key={label}
              className="rounded-lg border border-border bg-card p-5 shadow-panel"
            >
              <div className="flex items-center gap-2 text-xs font-semibold text-muted-foreground">
                <Braces className="h-4 w-4 text-info" aria-hidden="true" />
                {label}
              </div>
              <strong className="mt-3 block break-words text-lg tracking-normal">
                {value}
              </strong>
            </div>
          ))}
        </section>

        <section className="grid gap-4">
          {API_SURFACES.map((surface) => (
            <Card key={surface.name}>
              <CardHeader className="items-start">
                <div>
                  <CardTitle>{surface.name}</CardTitle>
                  <p className="mt-2 max-w-4xl text-sm leading-6 text-muted-foreground">
                    {surface.purpose}
                  </p>
                </div>
                <Badge tone={surface.boundary === "Customer data plane" ? "success" : "info"}>
                  {surface.boundary}
                </Badge>
              </CardHeader>
              <CardContent className="grid gap-5 lg:grid-cols-[1.1fr_0.9fr]">
                <div className="grid gap-2">
                  {surface.routes.map((route) => (
                    <code
                      key={route}
                      className="rounded-md border border-border bg-background/70 px-3 py-2 text-sm"
                    >
                      {route}
                    </code>
                  ))}
                </div>
                <div className="rounded-lg border border-success/20 bg-success/10 p-4">
                  <div className="flex items-center gap-2 text-sm font-semibold text-success">
                    <ShieldCheck className="h-4 w-4" aria-hidden="true" />
                    Payload boundary
                  </div>
                  <p className="mt-3 text-sm leading-6 text-muted-foreground">
                    {surface.payloadBoundary}
                  </p>
                </div>
              </CardContent>
            </Card>
          ))}
        </section>
      </div>
    </ProductShell>
  );
}
