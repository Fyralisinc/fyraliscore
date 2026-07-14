import { Activity, ShieldCheck } from "lucide-react";
import type { Metadata } from "next";

import { ControlPanelApp } from "@/features/control-panel/components/control-panel-app";
import { LinkButton, ProductShell } from "@/features/platform/components/product-shell";

export const metadata: Metadata = {
  title: "Fyralis Host Control Panel",
  description: "Internal metadata-only BYOC control panel."
};

export default function HostControlPanelPage() {
  return (
    <ProductShell
      active="/host/control-panel"
      eyebrow="Host control panel"
      title="Operate BYOC deployments without leaving the sanitized metadata boundary."
      description="Internal Fyralis view for deployment access, customer-cloud health, evidence receipts, and product health. The customer-facing landing site does not expose this surface."
      actions={
        <>
          <LinkButton href="/host/surfaces" variant="secondary">
            <ShieldCheck className="h-4 w-4" aria-hidden="true" />
            API boundaries
          </LinkButton>
          <LinkButton href="/host/observability" variant="secondary">
            <Activity className="h-4 w-4" aria-hidden="true" />
            Observability
          </LinkButton>
        </>
      }
    >
      <ControlPanelApp />
    </ProductShell>
  );
}
