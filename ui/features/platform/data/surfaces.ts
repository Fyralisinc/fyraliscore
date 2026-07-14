import {
  Activity,
  BarChart3,
  CloudCog,
  DatabaseZap,
  Gauge,
  KeyRound,
  LayoutDashboard,
  ListChecks,
  Network,
  PackageCheck,
  Rocket,
  ShieldCheck,
  TerminalSquare,
  Waypoints
} from "lucide-react";
import type { LucideIcon } from "lucide-react";

export type NavItem = {
  label: string;
  href: string;
  description: string;
  icon: LucideIcon;
};

export type SurfaceCard = {
  title: string;
  label: string;
  href?: string;
  description: string;
  owner: "Fyralis hosted" | "Customer cloud" | "Operator";
  status: "active" | "preserved" | "api-contract" | "operator";
  icon: LucideIcon;
};

export type ApiSurface = {
  name: string;
  boundary: "Fyralis control plane" | "Customer data plane" | "Provider edge";
  routes: string[];
  purpose: string;
  payloadBoundary: string;
};

export type ObservabilitySurface = {
  name: string;
  href?: string;
  location: string;
  purpose: string;
  boundary: string;
};

export const HOST_NAV: NavItem[] = [
  {
    label: "Control Panel",
    href: "/host/control-panel",
    description: "Modernized metadata-only BYOC operations view.",
    icon: LayoutDashboard
  },
  {
    label: "API Surfaces",
    href: "/host/surfaces",
    description: "Backend contracts that power browser surfaces.",
    icon: Waypoints
  },
  {
    label: "Observability",
    href: "/host/observability",
    description: "Grafana, Prometheus, and operator dashboards.",
    icon: BarChart3
  }
];

export const HOSTED_PORTAL_FLOW = [
  {
    title: "Get Fyralis",
    description: "Select design-partner BYOC or commercial BYOC rollout.",
    href: "/onboarding/get-fyralis",
    icon: Rocket
  },
  {
    title: "Customer intake",
    description: "Capture company, setup owner, and target cloud metadata.",
    href: "/onboarding/customer-intake",
    icon: ListChecks
  },
  {
    title: "Cloud readiness",
    description: "Confirm required deployment shape without secrets.",
    href: "/onboarding/cloud-readiness",
    icon: CloudCog
  },
  {
    title: "Setup package",
    description: "Prepare manifests, permission skeletons, and commands.",
    href: "/onboarding/setup-package",
    icon: PackageCheck
  },
  {
    title: "Boundary handoff",
    description: "Move into the customer cloud before secrets or source data.",
    href: "/onboarding/trust-boundary",
    icon: ShieldCheck
  }
];

export const CUSTOMER_CONSOLE_FLOW = [
  {
    title: "Preflight",
    description: "Run readiness checks from inside the customer cloud.",
    href: "/onboarding/preflight",
    icon: TerminalSquare
  },
  {
    title: "Deployment",
    description: "Install and validate the Fyralis data plane locally.",
    href: "/onboarding/deployment",
    icon: Network
  },
  {
    title: "Connect sources",
    description: "Configure provider apps, secret refs, scopes, and sync.",
    href: "/onboarding/source-catalog",
    icon: KeyRound
  },
  {
    title: "Operate workspace",
    description: "Launch, monitor ingestion, and operate from local console.",
    href: "/onboarding/workspace-home",
    icon: Activity
  }
];

export const PRESERVED_SURFACES: SurfaceCard[] = [
  {
    title: "BYOC Onboarding App",
    label: "Active product UI",
    href: "/onboarding/get-fyralis",
    description:
      "Modern Next.js App Router flow for getting Fyralis, preparing BYOC setup, deploying locally, connecting sources, and launching the workspace.",
    owner: "Fyralis hosted",
    status: "active",
    icon: Rocket
  },
  {
    title: "BYOC Control Panel",
    label: "Legacy UI preserved",
    href: "/host/control-panel",
    description:
      "The old control-panel contract is preserved and rendered through the modern shell using sanitized metadata-only endpoints.",
    owner: "Fyralis hosted",
    status: "preserved",
    icon: LayoutDashboard
  },
  {
    title: "UI-Facing Backend APIs",
    label: "Contracts preserved",
    href: "/host/surfaces",
    description:
      "Route families that power browser views remain visible as first-class API surfaces with owner and trust-boundary context.",
    owner: "Fyralis hosted",
    status: "api-contract",
    icon: Waypoints
  },
  {
    title: "Operator Observability",
    label: "Infrastructure UI",
    href: "/host/observability",
    description:
      "Grafana dashboards and Prometheus endpoints are preserved as operator surfaces, separate from customer onboarding UI.",
    owner: "Operator",
    status: "operator",
    icon: BarChart3
  }
];

export const API_SURFACES: ApiSurface[] = [
  {
    name: "BYOC control panel",
    boundary: "Fyralis control plane",
    routes: [
      "GET /byoc/control-panel/deployments",
      "GET /byoc/control-panel/state",
      "GET /byoc/control-panel/product-health"
    ],
    purpose:
      "Powers the modern control panel with deployment access, sanitized state, sections, actions, and product health.",
    payloadBoundary:
      "Sanitized metadata only; no raw payloads, prompts, source records, logs, vectors, or credentials."
  },
  {
    name: "BYOC control plane",
    boundary: "Fyralis control plane",
    routes: [
      "POST /byoc/control-plane/preflight-reports",
      "GET /byoc/control-plane/preflight-reports",
      "POST /byoc/control-plane/runner-evidence",
      "GET /byoc/control-plane/runner-evidence",
      "GET /byoc/control-plane/control-panel-state"
    ],
    purpose:
      "Receives signed customer-cloud evidence and exposes bounded operational state for hosted control-plane views.",
    payloadBoundary:
      "Evidence receipts and summarized status; customer-cloud secrets and source data remain local."
  },
  {
    name: "BYOC agent",
    boundary: "Customer data plane",
    routes: [
      "POST /byoc/agent/enroll",
      "POST /byoc/agent/heartbeat",
      "POST /byoc/agent/desired-state"
    ],
    purpose:
      "Lets customer-cloud data-plane agents enroll, heartbeat, and receive desired state without exposing raw tenant data.",
    payloadBoundary:
      "Agent identity, revision, health, and desired-state metadata only."
  },
  {
    name: "Provider ingress and source install",
    boundary: "Provider edge",
    routes: [
      "GET /integrations/slack/callback",
      "GET /integrations/github/callback",
      "GET /integrations/notion/callback",
      "POST /webhooks/*",
      "POST /integrations/*/connect/preflight",
      "POST /integrations/*/connect/finalize"
    ],
    purpose:
      "Supports OAuth callbacks, provider webhooks, and source connection flows that the customer-cloud console initiates.",
    payloadBoundary:
      "Provider callbacks must terminate at the customer-owned ingress when secrets or tokens are involved."
  }
];

export const OBSERVABILITY_SURFACES: ObservabilitySurface[] = [
  {
    name: "Grafana",
    location: "127.0.0.1:${GRAFANA_HOST_PORT:-3000}",
    href: "http://127.0.0.1:3000",
    purpose:
      "Operator dashboard workspace with provisioned Fyralis dashboards.",
    boundary:
      "Loopback-only compose surface; production exposure should stay behind operator access controls."
  },
  {
    name: "Prometheus",
    location: "127.0.0.1:${PROMETHEUS_HOST_PORT:-9090}",
    href: "http://127.0.0.1:9090",
    purpose:
      "Metrics query UI and scrape target backing Grafana dashboards.",
    boundary:
      "Loopback-only compose surface; label sets are designed to avoid tenant data leakage."
  },
  {
    name: "Data-plane infra dashboard",
    location: "observability/grafana/dashboards/data-plane-infra.json",
    purpose:
      "Kafka, Postgres, Redis, MinIO, gateway, and Fyralis service health.",
    boundary:
      "Dashboard-as-code provisioned into Grafana from the repository."
  },
  {
    name: "System health dashboard",
    location: "observability/grafana/dashboards/system-health.json",
    purpose:
      "Core service health, availability, and resource signals.",
    boundary:
      "Dashboard-as-code provisioned into Grafana from the repository."
  },
  {
    name: "Ingestion funnel dashboard",
    location: "observability/grafana/dashboards/ingestion-funnel.json",
    purpose:
      "Source ingress, normalization, summarization, embedding, and write path health.",
    boundary:
      "Operational metrics only; raw source material is not stored as dashboard data."
  },
  {
    name: "Webhook ingress dashboard",
    location: "observability/grafana/dashboards/webhook-ingress.json",
    purpose:
      "Provider webhook volume, failures, retry pressure, and signature path signals.",
    boundary:
      "Aggregated telemetry with bounded labels."
  },
  {
    name: "Product workflow health dashboard",
    location: "observability/grafana/dashboards/product-workflow-health.json",
    purpose:
      "Today, map, dashboard, integrations, finance, Slack, and product route health.",
    boundary:
      "Route-template metrics; raw paths and tenant-specific labels are avoided."
  }
];

export function statusLabel(status: SurfaceCard["status"]) {
  switch (status) {
    case "active":
      return "Active";
    case "preserved":
      return "Preserved";
    case "api-contract":
      return "API contract";
    case "operator":
      return "Operator";
  }
}
