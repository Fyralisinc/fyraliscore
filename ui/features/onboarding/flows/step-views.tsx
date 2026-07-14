"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  ClipboardCheck,
  CloudCog,
  Download,
  Play,
  RefreshCw,
  Rocket,
  ShieldCheck,
  TerminalSquare,
} from "lucide-react";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { useForm, type UseFormRegisterReturn } from "react-hook-form";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Field, Input, Select } from "@/components/ui/form";
import { cn } from "@/lib/utils";

import {
  cloudReadinessSchema,
  customerSchema,
  sourceScopeSchema,
  type CloudReadinessFormValues,
  type CustomerFormValues,
  type SourceScopeFormValues,
} from "../schemas/onboarding-schemas";
import type {
  CloudReadiness,
  Customer,
  Deployment,
  OnboardingIntent,
  OnboardingSnapshot,
  PlanId,
  Source,
  SourceConnection,
  SourceObservation,
  StepId,
  SyncJob,
  Validation,
  Workspace,
} from "../types";
import {
  FigmaOAuthConnectionCard,
} from "../components/figma-oauth-connection-card";
import type {
  FigmaConnectionStatus,
} from "../services/figma-oauth-service";
import {
  SourceMarketplace,
  type SourceAutomationCardState,
} from "../components/source-marketplace";
import {
  autoConnectSourceRehearsal,
  createDesignPartnerOnboardingIntent,
  fetchGatewaySourceObservations,
  fetchSourceRehearsalStatus,
  finalizeAwsSourceRehearsal,
  retryAwsFirstSyncRehearsal,
  type SourceAutoConnectResponse,
  type SourceConnectAccessMode,
  type SourceRehearsalPrepareResponse,
  type SourceRehearsalStatus,
  submitDesignPartnerIntake,
} from "../services/onboarding-service";

export type StepViewProps = {
  snapshot: OnboardingSnapshot;
  selectedPlan: PlanId;
  onboardingIntent: OnboardingIntent | null;
  customer: Customer;
  readiness: CloudReadiness;
  workspace: Workspace;
  selectedSource: Source;
  selectedConnection?: SourceConnection;
  connections: SourceConnection[];
  sourceValidation: Validation;
  syncJobs: SyncJob[];
  sourceObservations: SourceObservation[];
  launchReady: boolean;
  choosePlan: (plan: PlanId) => void;
  setOnboardingIntent: (intent: OnboardingIntent) => void;
  updateCustomer: (customer: Customer) => void;
  updateReadiness: (readiness: CloudReadiness) => void;
  selectSource: (sourceId: string) => void;
  updateConnection: (
    sourceId: string,
    patch: Partial<SourceConnection>,
  ) => void;
  setSourceValidation: (validation: Validation) => void;
  upsertSyncJob: (job: SyncJob) => void;
  landSourceObservations: (
    sourceId: string,
    observations: SourceObservation[],
  ) => void;
  setLaunchReady: (ready: boolean) => void;
  goTo: (step: StepId) => void;
  advance: () => void;
};

export function StepView({
  stepId,
  props,
}: {
  stepId: StepId;
  props: StepViewProps;
}) {
  switch (stepId) {
    case "get-fyralis":
      return <PlanSelection {...props} />;
    case "customer-intake":
      return <CustomerIntake {...props} />;
    case "cloud-readiness":
      return <CloudReadinessStep {...props} />;
    case "setup-package":
      return <SetupPackageStep {...props} />;
    case "trust-boundary":
      return <TrustBoundaryStep {...props} />;
    case "preflight":
      return <PreflightStep {...props} />;
    case "deployment":
      return <DeploymentStep {...props} />;
    case "deployment-validation":
      return <DeploymentValidationStep {...props} />;
    case "source-catalog":
    case "source-setup":
    case "source-validation":
    case "source-scope":
    case "first-sync":
    case "ingestion-health":
    case "activation":
      return <SourceCatalogStep {...props} />;
    case "workspace-launch":
      return <WorkspaceLaunchStep {...props} />;
    case "workspace-home":
      return <WorkspaceHomeStep {...props} />;
  }
}

type CapabilityKey = Exclude<keyof CloudReadiness, "region" | "environment">;
type CapabilityStatus = "existing" | "provision" | "guidance" | "unknown";

type CapabilityDefinition = {
  key: CapabilityKey;
  label: string;
  existingValue: CloudReadiness[CapabilityKey];
  provisionValue: CloudReadiness[CapabilityKey];
  module: string;
  requiredFor: string;
  provisionAction: string;
  existingAction: string;
};

const BYOC_CAPABILITIES: CapabilityDefinition[] = [
  {
    key: "kubernetes",
    label: "Kubernetes runtime",
    existingValue: "available",
    provisionValue: "provision-eks",
    module: "terraform/aws/eks + helm/fyralis",
    requiredFor:
      "Running the gateway, workers, and local customer-cloud console.",
    provisionAction:
      "Provision a dedicated EKS runtime and install Fyralis with Helm.",
    existingAction: "Attach Fyralis to the approved Kubernetes runtime.",
  },
  {
    key: "network",
    label: "Network/VPC",
    existingValue: "existing-ready",
    provisionValue: "provision-isolated-vpc",
    module: "terraform/aws/network",
    requiredFor:
      "Keeping ingress, egress, private DNS, and service access in the approved boundary.",
    provisionAction:
      "Create an isolated VPC, subnets, security groups, and DNS shape.",
    existingAction:
      "Use the approved VPC and generate network policy references.",
  },
  {
    key: "secrets",
    label: "AWS Secrets Manager",
    existingValue: "aws-secrets-manager",
    provisionValue: "provision-secret-refs",
    module: "terraform/aws/secrets",
    requiredFor:
      "Storing customer-owned source tokens, database URLs, and provider secrets locally.",
    provisionAction:
      "Create secret names, IAM access, and placeholder refs without secret values.",
    existingAction: "Reference existing customer-owned secret names only.",
  },
  {
    key: "postgres",
    label: "Postgres with pgvector",
    existingValue: "pgvector-ready",
    provisionValue: "provision-rds-pgvector",
    module: "terraform/aws/rds-pgvector",
    requiredFor: "Tenant data, model state, embeddings, and vector search.",
    provisionAction:
      "Provision Postgres/RDS and enable the pgvector extension.",
    existingAction:
      "Use the existing Postgres endpoint and pgvector validation check.",
  },
  {
    key: "objectStorage",
    label: "S3-compatible storage",
    existingValue: "s3-compatible-ready",
    provisionValue: "provision-s3",
    module: "terraform/aws/s3",
    requiredFor:
      "Raw payload tier, deployment artifacts, and durable evidence receipts.",
    provisionAction:
      "Create S3 buckets, retention policy, and least-privilege access.",
    existingAction: "Use the approved bucket and generate object-storage refs.",
  },
  {
    key: "kafka",
    label: "Kafka/MSK",
    existingValue: "kafka-msk-ready",
    provisionValue: "provision-msk",
    module: "terraform/aws/msk",
    requiredFor: "Kafka-first ingestion durability and worker orchestration.",
    provisionAction:
      "Provision MSK/Kafka topics, ACL shape, and worker connectivity.",
    existingAction:
      "Use the existing Kafka/MSK cluster and generate topic/ACL checks.",
  },
];

type SourceRehearsalConfig = {
  sourceId: string;
  providerKind: string;
  needsPublicUrl: boolean;
  providerConsoleUrl: string;
  gatewayRoutes: Array<{
    label: string;
    method: "GET" | "POST";
    path: string;
    access:
      | "bearer session"
      | "state-token callback"
      | "provider signed"
      | "local only";
  }>;
  generatedArtifacts: string[];
  envKeys: string[];
  manualGates: string[];
};

type SourceSignalConfig = {
  historicalTrigger: string;
  liveIngress: string;
  liveWorker: string;
  landedChannel: string;
};

const SOURCE_REHEARSAL_CONFIG: Record<string, SourceRehearsalConfig> = {
  slack: {
    sourceId: "slack",
    providerKind: "OAuth app",
    needsPublicUrl: true,
    providerConsoleUrl: "https://api.slack.com/apps",
    gatewayRoutes: [
      {
        label: "Install",
        method: "GET",
        path: "/integrations/slack/install",
        access: "bearer session",
      },
      {
        label: "OAuth callback",
        method: "GET",
        path: "/integrations/slack/callback",
        access: "state-token callback",
      },
      {
        label: "Events API webhook",
        method: "POST",
        path: "/webhooks/slack/events",
        access: "provider signed",
      },
    ],
    generatedArtifacts: [
      "fyralis-slack-app-manifest.yaml",
      "fyralis-slack-app-events-manifest.yaml",
      "slack.env.example",
      "slack-provider-setup.json",
    ],
    envKeys: [
      "SLACK_APP_CONFIG_TOKEN",
      "SLACK_REDIRECT_URI",
      "OAUTH_STATE_HMAC_KEY",
    ],
    manualGates: [
      "Slack app configuration/admin approval",
      "Slack OAuth consent",
    ],
  },
  jira: {
    sourceId: "jira",
    providerKind: "API token connect",
    needsPublicUrl: true,
    providerConsoleUrl:
      "https://id.atlassian.com/manage-profile/security/api-tokens",
    gatewayRoutes: [
      {
        label: "Credential preflight",
        method: "POST",
        path: "/integrations/jira/connect/preflight",
        access: "bearer session",
      },
      {
        label: "Finalize connection",
        method: "POST",
        path: "/integrations/jira/connect/finalize",
        access: "bearer session",
      },
      {
        label: "Webhook",
        method: "POST",
        path: "/webhooks/jira/events",
        access: "provider signed",
      },
    ],
    generatedArtifacts: [
      "jira-connect-payload.example.json",
      "jira.env.example",
      "jira-provider-setup.json",
    ],
    envKeys: ["JIRA_BASE_URL", "JIRA_ACCOUNT_EMAIL", "JIRA_API_TOKEN"],
    manualGates: [
      "Atlassian API token creation",
      "Project scope selection",
      "Jira webhook registration",
    ],
  },
  github: {
    sourceId: "github",
    providerKind: "GitHub App",
    needsPublicUrl: true,
    providerConsoleUrl: "https://github.com/settings/apps",
    gatewayRoutes: [
      {
        label: "Install",
        method: "GET",
        path: "/integrations/github/install",
        access: "bearer session",
      },
      {
        label: "GitHub callback",
        method: "GET",
        path: "/integrations/github/callback",
        access: "state-token callback",
      },
      {
        label: "Webhook",
        method: "POST",
        path: "/webhooks/github",
        access: "provider signed",
      },
    ],
    generatedArtifacts: [
      "fyralis-github-app-manifest.json",
      "github.env.example",
      "github-provider-setup.json",
    ],
    envKeys: [
      "GITHUB_APP_SLUG",
      "GITHUB_APP_ID",
      "GITHUB_APP_PRIVATE_KEY",
      "WEBHOOK_SECRET_GITHUB",
      "OAUTH_STATE_HMAC_KEY",
    ],
    manualGates: ["GitHub App creation/update", "Org installation approval"],
  },
  discord: {
    sourceId: "discord",
    providerKind: "OAuth app + gateway bot",
    needsPublicUrl: true,
    providerConsoleUrl: "https://discord.com/developers/applications",
    gatewayRoutes: [
      {
        label: "Install",
        method: "GET",
        path: "/integrations/discord/install",
        access: "bearer session",
      },
      {
        label: "OAuth callback",
        method: "GET",
        path: "/integrations/discord/callback",
        access: "state-token callback",
      },
      {
        label: "Interactions/webhook",
        method: "POST",
        path: "/webhooks/discord",
        access: "provider signed",
      },
    ],
    generatedArtifacts: [
      "fyralis-discord-app-setup.json",
      "discord.env.example",
      "discord-provider-setup.json",
    ],
    envKeys: [
      "DISCORD_CLIENT_ID",
      "DISCORD_CLIENT_SECRET",
      "DISCORD_APPLICATION_ID",
      "DISCORD_BOT_TOKEN",
      "WEBHOOK_SECRET_DISCORD",
      "OAUTH_STATE_HMAC_KEY",
    ],
    manualGates: ["Discord app/bot setup", "Server admin bot approval"],
  },
  notion: {
    sourceId: "notion",
    providerKind: "OAuth integration",
    needsPublicUrl: true,
    providerConsoleUrl: "https://www.notion.so/my-integrations",
    gatewayRoutes: [
      {
        label: "Install",
        method: "GET",
        path: "/integrations/notion/install",
        access: "bearer session",
      },
      {
        label: "OAuth callback",
        method: "GET",
        path: "/integrations/notion/callback",
        access: "state-token callback",
      },
      {
        label: "Webhook",
        method: "POST",
        path: "/webhooks/notion/events",
        access: "provider signed",
      },
    ],
    generatedArtifacts: [
      "fyralis-notion-app-setup.json",
      "notion.env.example",
      "notion-provider-setup.json",
    ],
    envKeys: [
      "NOTION_CLIENT_ID",
      "NOTION_CLIENT_SECRET",
      "NOTION_REDIRECT_URI",
      "OAUTH_STATE_HMAC_KEY",
    ],
    manualGates: [
      "Notion integration setup",
      "Workspace OAuth consent",
      "Webhook verification token copy",
    ],
  },
  telegram: {
    sourceId: "telegram",
    providerKind: "Local MTProto gateway session",
    needsPublicUrl: false,
    providerConsoleUrl: "https://my.telegram.org/apps",
    gatewayRoutes: [
      {
        label: "Local session",
        method: "POST",
        path: ".fyralis/local-rehearsal/telegram/telegram.env",
        access: "local only",
      },
    ],
    generatedArtifacts: [
      "telegram-session-plan.json",
      "telegram.env.example",
      "telegram-provider-setup.json",
    ],
    envKeys: [
      "TELEGRAM_ACCOUNT_LABEL",
      "TELEGRAM_API_ID",
      "TELEGRAM_API_HASH",
      "TELEGRAM_SESSION",
    ],
    manualGates: [
      "Telegram API ID/hash creation",
      "MTProto login code",
      "Dialog scope approval",
    ],
  },
};

const SOURCE_SIGNAL_CONFIG: Record<string, SourceSignalConfig> = {
  slack: {
    historicalTrigger:
      "OAuth callback writes provider_installations and onboarding_triggers(source='slack').",
    liveIngress: "/webhooks/slack/events",
    liveWorker: "Slack Events API -> webhook router -> ingestion pipeline.",
    landedChannel: "slack:message",
  },
  jira: {
    historicalTrigger:
      "Jira finalize writes jira_installations/projects and onboarding_triggers(source='jira').",
    liveIngress: "/webhooks/jira/events",
    liveWorker: "Jira webhook -> signed webhook router -> ingestion pipeline.",
    landedChannel: "jira:issue",
  },
  github: {
    historicalTrigger:
      "GitHub callback writes provider_installations and onboarding_triggers(source='github').",
    liveIngress: "/webhooks/github",
    liveWorker:
      "GitHub webhook -> signed webhook router -> ingestion pipeline.",
    landedChannel: "github:webhook",
  },
  discord: {
    historicalTrigger:
      "Discord callback writes provider_installations and onboarding_triggers(source='discord').",
    liveIngress: "/webhooks/discord",
    liveWorker:
      "Discord gateway/bot events -> gateway dispatcher -> observations.",
    landedChannel: "discord:message",
  },
  notion: {
    historicalTrigger:
      "Notion callback writes provider_installations and onboarding_triggers(source='notion').",
    liveIngress: "/webhooks/notion/events",
    liveWorker:
      "Notion webhook -> verified webhook router -> ingestion pipeline.",
    landedChannel: "notion:object",
  },
  telegram: {
    historicalTrigger:
      "Telegram finalize writes telegram_installations/dialogs and onboarding_triggers(source='telegram').",
    liveIngress: "Customer-cloud MTProto session",
    liveWorker:
      "Telegram gateway worker reads updates and writes observations locally.",
    landedChannel: "telegram:message",
  },
};

function sourceEnvPrefix(sourceId: string) {
  return sourceId.replaceAll("-", "_").toUpperCase();
}

function genericEnvKeys(source: Source) {
  const prefix = sourceEnvPrefix(source.id);
  if (source.method === "OAuth") {
    return [
      `${prefix}_OAUTH_CLIENT_ID`,
      `${prefix}_OAUTH_CLIENT_SECRET`,
      `${prefix}_TOKEN_REF`,
    ];
  }
  if (source.method === "Workspace DWD") {
    return [
      `${prefix}_WORKSPACE_DOMAIN`,
      `${prefix}_ADMIN_EMAIL`,
      `${prefix}_DWD_CLIENT_ID`,
    ];
  }
  if (source.method === "API token") {
    return [`${prefix}_API_TOKEN`];
  }
  if (source.method === "Gateway") {
    return [`${prefix}_SESSION_REF`];
  }
  if (source.method === "IAM role") {
    return [`${prefix}_ROLE_ARN`];
  }
  if (source.method === "Webhook") {
    if (source.id === "whatsapp") {
      return [
        `${prefix}_PHONE_NUMBER_ID`,
        `${prefix}_APP_SECRET`,
        `${prefix}_VERIFY_TOKEN`,
      ];
    }
    return [`${prefix}_ACCESS_TOKEN`, `${prefix}_WEBHOOK_SECRET`];
  }
  return [`${prefix}_TOKEN_REF`];
}

function sourceRehearsalConfig(source: Source): SourceRehearsalConfig {
  const explicit = SOURCE_REHEARSAL_CONFIG[source.id];
  if (explicit) {
    return explicit;
  }
  const gatewayRoutes = source.providerIngressPaths.map((path) => ({
    label: path.includes("callback") ? "Callback" : "Webhook",
    method: (path.includes("callback") ? "GET" : "POST") as "GET" | "POST",
    path,
    access: (path.includes("callback")
      ? "state-token callback"
      : "provider signed") as SourceRehearsalConfig["gatewayRoutes"][number]["access"],
  }));
  return {
    sourceId: source.id,
    providerKind: `${source.method} connection`,
    needsPublicUrl: source.providerIngressPaths.length > 0,
    providerConsoleUrl: "Customer provider admin console",
    gatewayRoutes,
    generatedArtifacts: [
      `${source.id}-provider-setup.json`,
      `${source.id}.env.example`,
      `${source.id}-connection-checklist.json`,
    ],
    envKeys: genericEnvKeys(source),
    manualGates: [
      "Provider/admin approval",
      "Scope selection",
      source.providerIngressPaths.length
        ? "Webhook registration"
        : "Customer-local ref approval",
    ],
  };
}

function sourceSignalConfig(source: Source): SourceSignalConfig {
  const channelSourceId = source.id.replaceAll("-", "_");
  return (
    SOURCE_SIGNAL_CONFIG[source.id] ?? {
      historicalTrigger: `${source.name} setup writes local install refs and onboarding_triggers(source='${source.id}').`,
      liveIngress:
        source.providerIngressPaths[0] ??
        source.noIngressReason ??
        "Customer-cloud local worker",
      liveWorker: `${source.name} worker reads approved resources and writes sanitized observations locally.`,
      landedChannel: `${channelSourceId}:*`,
    }
  );
}

function capabilityStatus(
  readiness: CloudReadiness,
  capability: CapabilityDefinition,
): CapabilityStatus {
  const value = readiness[capability.key];
  if (value === capability.existingValue) {
    return "existing";
  }
  if (value === capability.provisionValue) {
    return "provision";
  }
  if (value === "unknown") {
    return "unknown";
  }
  return "guidance";
}

function buildAutomationPlan(readiness: CloudReadiness) {
  const rows = BYOC_CAPABILITIES.map((capability) => ({
    ...capability,
    status: capabilityStatus(readiness, capability),
  }));
  return {
    rows,
    existing: rows.filter((row) => row.status === "existing"),
    provision: rows.filter((row) => row.status === "provision"),
    needsDecision: rows.filter(
      (row) => row.status === "guidance" || row.status === "unknown",
    ),
  };
}

function normalizeReadiness(readiness: CloudReadiness): CloudReadiness {
  const partial = readiness as Partial<CloudReadiness>;
  return {
    region: partial.region ?? "us-east-1",
    environment: partial.environment ?? "pilot",
    setupAutomation: "agent-managed",
    agentAccess: partial.agentAccess ?? "customer-cloud-agent",
    agentPermissionProfile:
      partial.agentPermissionProfile ?? "byoc-bootstrap-provisioner",
    agentApprovalMode: partial.agentApprovalMode ?? "approval-required",
    setupRoleArn: partial.setupRoleArn ?? "",
    kubernetes: partial.kubernetes ?? "provision-eks",
    network: partial.network ?? "provision-isolated-vpc",
    secrets: partial.secrets ?? "provision-secret-refs",
    postgres: partial.postgres ?? "provision-rds-pgvector",
    objectStorage: partial.objectStorage ?? "provision-s3",
    kafka: partial.kafka ?? "provision-msk",
  };
}

function PlanSelection({
  snapshot,
  selectedPlan,
  onboardingIntent,
  choosePlan,
  setOnboardingIntent,
  advance,
}: StepViewProps) {
  const [starting, setStarting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function startDesignPartner() {
    setStarting(true);
    setError(null);
    try {
      choosePlan("design-partner-byoc");
      const intent =
        onboardingIntent?.plan_code === "design_partner_byoc_pilot"
          ? onboardingIntent
          : await createDesignPartnerOnboardingIntent();
      setOnboardingIntent(intent);
      advance();
    } catch (caught) {
      setError(
        caught instanceof Error
          ? caught.message
          : "Could not create the Design Partner BYOC intent.",
      );
    } finally {
      setStarting(false);
    }
  }

  return (
    <div className="rounded-lg border border-border bg-card p-5 text-card-foreground md:p-7">
      <div className="mx-auto max-w-5xl">
        <div className="mb-6">
          <p className="text-sm font-semibold text-muted-foreground">
            Get Fyralis
          </p>
          <h2 className="mt-2 text-3xl font-semibold tracking-normal">
            Choose how you want to start.
          </h2>
          <p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground">
            Start with the design-partner BYOC path or move toward an enterprise
            BYOC rollout. We do not collect source credentials here.
          </p>
        </div>
        <div className="grid gap-4 lg:grid-cols-2">
          {snapshot.plans.map((plan) => (
            <article
              key={plan.id}
              className={cn(
                "flex min-h-[33rem] flex-col rounded-lg border p-8 transition",
                plan.id === "design-partner-byoc"
                  ? "border-success/40 bg-success/10"
                  : "border-info/40 bg-info/10",
                selectedPlan === plan.id && "ring-2 ring-ring/25",
              )}
            >
              <div>
                <h3 className="text-2xl font-semibold tracking-normal">
                  {plan.label}
                </h3>
                <div className="mt-5 flex items-end gap-2">
                  <strong className="text-5xl font-semibold tracking-normal">
                    {plan.terms[0]?.value ?? "Custom"}
                  </strong>
                  <span className="pb-2 text-sm text-muted-foreground">
                    {plan.badge}
                  </span>
                </div>
              </div>
              <p className="mt-6 min-h-16 text-base leading-7 text-foreground">
                {plan.description}
              </p>
              <ul className="mt-6 grid gap-3 text-sm text-foreground">
                {plan.features.map((feature) => (
                  <li key={feature} className="flex gap-2">
                    <CheckCircle2
                      className={cn(
                        "mt-0.5 h-4 w-4 shrink-0 rounded-full",
                        plan.id === "design-partner-byoc"
                          ? "text-success"
                          : "text-info",
                      )}
                      aria-hidden="true"
                    />
                    {feature}
                  </li>
                ))}
              </ul>
              <Button
                type="button"
                className={cn(
                  "mt-auto w-full",
                  plan.id === "design-partner-byoc"
                    ? "border-success bg-success text-success-foreground hover:bg-success/90"
                    : "border-info bg-info text-info-foreground hover:bg-info/90",
                )}
                disabled={starting || plan.id !== "design-partner-byoc"}
                onClick={() => {
                  if (plan.id === "design-partner-byoc") {
                    void startDesignPartner();
                  }
                }}
              >
                {plan.id === "design-partner-byoc"
                  ? starting
                    ? "Creating intent..."
                    : "Start Design Partner BYOC"
                  : "Enterprise path later"}
              </Button>
            </article>
          ))}
        </div>
        {error ? (
          <p className="mt-4 rounded-lg border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive">
            {error}
          </p>
        ) : null}
      </div>
    </div>
  );
}

function CustomerIntake({
  onboardingIntent,
  customer,
  setOnboardingIntent,
  updateCustomer,
  advance,
}: StepViewProps) {
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const form = useForm<CustomerFormValues>({
    resolver: zodResolver(customerSchema),
    defaultValues: customer,
    mode: "onChange",
  });

  useEffect(() => {
    const subscription = form.watch((value) => {
      const parsed = customerSchema.safeParse(value);
      if (parsed.success) {
        updateCustomer(parsed.data);
      }
    });
    return () => subscription.unsubscribe();
  }, [form, updateCustomer]);

  return (
    <form
      className="grid gap-5"
      onSubmit={form.handleSubmit(async (values) => {
        setSubmitting(true);
        setSubmitError(null);
        try {
          const intent =
            onboardingIntent?.plan_code === "design_partner_byoc_pilot"
              ? onboardingIntent
              : await createDesignPartnerOnboardingIntent();
          const updatedIntent = await submitDesignPartnerIntake(
            intent.intent_id,
            values,
          );
          setOnboardingIntent(updatedIntent);
          updateCustomer(values);
          advance();
        } catch (caught) {
          setSubmitError(
            caught instanceof Error
              ? caught.message
              : "Could not submit Design Partner BYOC intake.",
          );
        } finally {
          setSubmitting(false);
        }
      })}
    >
      <Card>
        <CardHeader>
          <CardTitle>What we need from the customer</CardTitle>
          <Badge tone="info">No secrets</Badge>
        </CardHeader>
        <CardContent className="grid gap-5 md:grid-cols-2">
          <Field
            label="Company"
            help="Used for the customer record, tenant name, and commercial handoff."
            error={form.formState.errors.company?.message}
          >
            <Input {...form.register("company")} />
          </Field>
          <Field
            label="Fyralis setup owner"
            help="Technical owner who can coordinate cloud access and validation."
            error={form.formState.errors.setupOwnerEmail?.message}
          >
            <Input {...form.register("setupOwnerEmail")} />
          </Field>
          <Field
            label="Target cloud"
            help="Determines the first BYOC manifest and permission shape."
            error={form.formState.errors.targetCloud?.message}
          >
            <Select {...form.register("targetCloud")}>
              <option>AWS</option>
              <option>GCP future profile</option>
              <option>Azure future profile</option>
            </Select>
          </Field>
        </CardContent>
      </Card>
      {onboardingIntent ? (
        <Card>
          <CardContent className="grid gap-2 p-4 text-sm text-muted-foreground md:grid-cols-3">
            <span>
              <strong className="block text-foreground">Intent</strong>
              {onboardingIntent.intent_id}
            </span>
            <span>
              <strong className="block text-foreground">Customer</strong>
              {onboardingIntent.customer_id ?? "Created after intake"}
            </span>
            <span>
              <strong className="block text-foreground">Deployment</strong>
              {onboardingIntent.deployment_id ?? "Created after intake"}
            </span>
          </CardContent>
        </Card>
      ) : null}
      {submitError ? (
        <p className="rounded-lg border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {submitError}
        </p>
      ) : null}
      <ActionBar
        primaryLabel={
          submitting ? "Creating workspace..." : "Continue to cloud readiness"
        }
        disabled={submitting}
        submit
      />
    </form>
  );
}

function CloudReadinessStep({
  readiness,
  updateReadiness,
  advance,
}: StepViewProps) {
  const initialReadiness = normalizeReadiness(readiness);
  const form = useForm<CloudReadinessFormValues>({
    resolver: zodResolver(cloudReadinessSchema),
    defaultValues: initialReadiness,
    mode: "onChange",
  });

  useEffect(() => {
    const subscription = form.watch((value) => {
      const parsed = cloudReadinessSchema.safeParse(
        normalizeReadiness(value as CloudReadiness),
      );
      if (parsed.success) {
        updateReadiness(parsed.data);
      }
    });
    return () => subscription.unsubscribe();
  }, [form, updateReadiness]);

  const currentReadiness = normalizeReadiness(form.watch() as CloudReadiness);
  const accessLabel =
    currentReadiness.agentAccess === "customer-cloud-agent"
      ? "Local agent"
      : "Assume setup role";
  const approvalLabel =
    currentReadiness.agentApprovalMode === "approval-required"
      ? "Approval required"
      : "Plan only";

  return (
    <form
      className="grid gap-5"
      onSubmit={form.handleSubmit((values) => {
        updateReadiness(normalizeReadiness(values));
        advance();
      })}
    >
      <Card>
        <CardHeader>
          <CardTitle>BYOC setup agent</CardTitle>
          <Badge tone="info">Agent mode</Badge>
        </CardHeader>
        <CardContent className="grid gap-5 md:grid-cols-2">
          <SelectField
            label="Cloud region"
            register={form.register("region")}
            help="Used for deployment location and residency."
          >
            <option>us-east-1</option>
            <option>us-west-2</option>
            <option>eu-west-1</option>
            <option>ap-south-1</option>
          </SelectField>
          <SelectField
            label="Deployment environment"
            register={form.register("environment")}
            help="Controls naming, safety gates, and rollout strictness."
          >
            <option>pilot</option>
            <option>staging</option>
            <option>production</option>
          </SelectField>
          <SelectField
            label="Agent access"
            register={form.register("agentAccess")}
            help="The agent runs with scoped setup access, not customer source credentials."
          >
            <option value="customer-cloud-agent">
              Install agent inside customer cloud
            </option>
            <option value="aws-cross-account-role">
              Allow Fyralis agent to assume setup role
            </option>
          </SelectField>
          <Field
            label="Source runtime role"
            help="Filled from BYOC deployment output SourceRuntimeRoleArn. No secret keys or source tokens."
            error={form.formState.errors.setupRoleArn?.message}
          >
            <Input
              {...form.register("setupRoleArn")}
              placeholder="Detected after BYOC deployment"
            />
          </Field>
          <SelectField
            label="Permission profile"
            register={form.register("agentPermissionProfile")}
            help="Controls whether the agent can only discover or can also provision BYOC infra."
          >
            <option value="byoc-bootstrap-provisioner">
              BYOC bootstrap provisioner
            </option>
            <option value="discovery-only">Discovery only</option>
          </SelectField>
          <SelectField
            label="Apply policy"
            register={form.register("agentApprovalMode")}
            help="Recommended: require the customer setup owner to approve the plan before apply."
          >
            <option value="approval-required">
              Customer approves before apply
            </option>
            <option value="plan-only">Plan only, no apply</option>
          </SelectField>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Agent runbook</CardTitle>
          <Badge tone="success">No source data access</Badge>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-3">
          <Metric
            label="Access mode"
            value={accessLabel}
            detail="Agent uses scoped customer-approved setup access."
          />
          <Metric
            label="Approval"
            value={approvalLabel}
            detail="Apply is gated by the setup owner."
          />
          <Metric
            label="Discovery scope"
            value="6 capabilities"
            detail="Kubernetes, VPC, Secrets, Postgres, S3, and Kafka/MSK."
          />
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>What the agent manages</CardTitle>
          <Badge tone="info">Customer cloud only</Badge>
        </CardHeader>
        <CardContent className="grid gap-3 lg:grid-cols-2">
          {BYOC_CAPABILITIES.map((capability) => (
            <div
              key={capability.key}
              className="rounded-lg border border-border bg-background/70 p-4"
            >
              <div className="flex items-start justify-between gap-3">
                <span>
                  <strong className="block">{capability.label}</strong>
                  <span className="mt-1 block text-sm text-muted-foreground">
                    Agent discovers existing resources, then provisions only
                    what is missing and approved.
                  </span>
                </span>
                <Badge tone="info">Agent-managed</Badge>
              </div>
              <code className="mt-3 block break-all text-xs text-muted-foreground">
                {capability.module}
              </code>
            </div>
          ))}
        </CardContent>
      </Card>

      <div className="grid gap-5 lg:grid-cols-2">
        <BoundaryPanel
          title="Agent can"
          items={[
            "Discover required BYOC resources",
            "Generate CloudFormation and Helm execution artifacts",
            "Create missing approved infrastructure",
            "Install Fyralis with Helm",
            "Emit sanitized readiness status",
          ]}
          strong
        />
        <BoundaryPanel
          title="Agent cannot"
          items={[
            "Read source credentials",
            "Upload raw customer data",
            "Export prompts, logs, or embeddings",
            "Apply changes without approval",
            "Create resources outside the setup policy",
          ]}
        />
      </div>

      <ActionBar primaryLabel="Generate agent setup package" submit />
    </form>
  );
}

function SetupPackageStep({ snapshot, readiness, advance }: StepViewProps) {
  const effectiveReadiness = normalizeReadiness(readiness);
  const automationPlan = buildAutomationPlan(effectiveReadiness);
  const packageMode =
    effectiveReadiness.agentApprovalMode === "plan-only"
      ? "Discovery and plan only"
      : "Agent setup package";
  const accessCommand =
    effectiveReadiness.agentAccess === "customer-cloud-agent"
      ? `fyralis byoc agent install --bundle fyralis-byoc-acme-finance.zip --region ${effectiveReadiness.region}`
      : `fyralis byoc agent register-role --role-arn ${effectiveReadiness.setupRoleArn} --external-id fyralis-acme-finance-pilot`;
  const applyCommand =
    effectiveReadiness.agentApprovalMode === "plan-only"
      ? "fyralis byoc agent plan --no-apply --emit-review-bundle"
      : "fyralis byoc agent apply --requires-approval --plan latest";
  const providerExecutorCommand = `fyralis byoc agent provider-executor --cloud aws --region ${effectiveReadiness.region} --stack-name fyralis-byoc-acme-finance --create-change-set --execute-change-set --execute-helm --confirm-cost-and-mutation --json`;
  const autopilotCommand = [
    "fyralis byoc agent autopilot",
    "--cloud aws",
    `--region ${effectiveReadiness.region}`,
    "--external-id fyralis-acme-finance-pilot",
    "--bundle fyralis-byoc-acme-finance.zip",
    "--capabilities kubernetes,network,secrets,postgres,s3,kafka",
    effectiveReadiness.agentApprovalMode === "plan-only"
      ? "--plan-only"
      : "--auto-approve",
    "--run-readonly-api-probes",
    "--run-provider-executor",
    "--stack-name fyralis-byoc-acme-finance",
    "--json",
  ].join(" ");
  const commands = [
    {
      label: "Zero-spend local rehearsal",
      command: `fyralis byoc agent local-rehearsal --region ${effectiveReadiness.region} --workdir .fyralis/local-rehearsal --json`,
    },
    {
      label: "Create setup role template",
      command: `fyralis byoc agent role-template --cloud aws --region ${effectiveReadiness.region} --external-id fyralis-acme-finance-pilot`,
    },
    {
      label: "Register setup agent",
      command: accessCommand,
    },
    {
      label: "Discover and plan",
      command: `fyralis byoc agent discover --region ${effectiveReadiness.region} --capabilities kubernetes,network,secrets,postgres,s3,kafka --emit-plan`,
    },
    {
      label: "Apply approved plan",
      command: applyCommand,
    },
    {
      label: "Execute AWS and Helm setup",
      command: providerExecutorCommand,
    },
    {
      label: "Validate",
      command:
        "fyralis byoc agent validate --json --emit-sanitized-readiness-report",
    },
  ];

  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Generated BYOC agent package</CardTitle>
          <Badge tone="success">{packageMode}</Badge>
        </CardHeader>
        <CardContent className="grid gap-5">
          <div className="rounded-lg border border-info/30 bg-info/10 p-4">
            <div className="flex items-start gap-3">
              <CloudCog
                className="mt-0.5 h-5 w-5 shrink-0 text-info"
                aria-hidden="true"
              />
              <span>
                <strong className="block text-sm">
                  Agent runs with scoped setup access
                </strong>
                <span className="mt-1 block text-sm leading-6 text-muted-foreground">
                  The setup owner installs the agent or grants the setup role,
                  reviews the discovered plan, approves apply, and sends back
                  only sanitized readiness status.
                </span>
              </span>
            </div>
          </div>

          <div className="grid gap-3 lg:grid-cols-2">
            {automationPlan.rows.map((row) => (
              <div
                key={row.key}
                className="rounded-lg border border-border bg-background/70 p-4"
              >
                <div className="flex items-start justify-between gap-3">
                  <span>
                    <strong className="block">{row.label}</strong>
                    <span className="mt-1 block text-sm text-muted-foreground">
                      Agent discovers current state, reuses approved resources,
                      and provisions missing resources only after approval.
                    </span>
                  </span>
                  <Badge tone="info">Agent-managed</Badge>
                </div>
                <code className="mt-3 block break-all text-xs text-muted-foreground">
                  {row.module}
                </code>
              </div>
            ))}
          </div>

          <div>
            <h3 className="text-sm font-semibold">Safe artifacts generated</h3>
            <div className="mt-3 grid gap-3 md:grid-cols-2">
              {snapshot.setupPackage.artifacts.map((artifact) => (
                <div
                  key={artifact.filename}
                  className="rounded-lg border border-border bg-background/70 p-4"
                >
                  <div className="flex items-start justify-between gap-3">
                    <span>
                      <strong className="block">{artifact.name}</strong>
                      <span className="mt-1 block text-sm text-muted-foreground">
                        {artifact.description}
                      </span>
                    </span>
                    <Badge tone={artifact.safeToShare ? "success" : "warning"}>
                      {artifact.safeToShare ? "Safe" : "Review"}
                    </Badge>
                  </div>
                  <code className="mt-3 block break-all text-xs text-muted-foreground">
                    {artifact.filename}
                  </code>
                </div>
              ))}
            </div>
          </div>

          <div>
            <h3 className="text-sm font-semibold">Customer-cloud autopilot</h3>
            <div className="mt-3 rounded-lg border border-success/30 bg-success/10 p-4">
              <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
                <TerminalSquare className="h-4 w-4" aria-hidden="true" />
                Setup Fyralis command
              </div>
              <code className="block break-all text-xs text-muted-foreground">
                {autopilotCommand}
              </code>
            </div>
          </div>

          <div>
            <h3 className="text-sm font-semibold">Advanced manual commands</h3>
            <div className="mt-3 grid gap-3">
              {commands.map((command) => (
                <div
                  key={command.label}
                  className="rounded-lg border border-border bg-primary p-4 text-primary-foreground"
                >
                  <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
                    <TerminalSquare className="h-4 w-4" aria-hidden="true" />
                    {command.label} command
                  </div>
                  <code className="block break-all text-xs text-primary-foreground/80">
                    {command.command}
                  </code>
                </div>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>
      <ActionBar
        primaryLabel="Continue to trust boundary"
        onPrimary={advance}
        secondaryLabel="Download artifacts"
        secondaryIcon={<Download className="h-4 w-4" aria-hidden="true" />}
      />
    </div>
  );
}

function TrustBoundaryStep({ workspace, advance }: StepViewProps) {
  return (
    <div className="grid gap-5">
      <Card className="overflow-hidden">
        <CardHeader>
          <CardTitle>Hosted portal to customer cloud</CardTitle>
          <Badge tone="success">Security-sensitive handoff</Badge>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 lg:grid-cols-[1fr_auto_1fr] lg:items-stretch">
            <BoundaryPanel
              title="Fyralis hosted portal"
              items={[
                "Commercial intake",
                "Setup package generation",
                "Safe readiness status",
                "Bounded issue codes",
              ]}
            />
            <div className="hidden items-center justify-center lg:flex">
              <ArrowRight
                className="h-8 w-8 text-muted-foreground"
                aria-hidden="true"
              />
            </div>
            <BoundaryPanel
              title="Customer cloud"
              items={[
                "Runtime deployment",
                "Source credentials",
                "Raw source payloads",
                "Private logs and prompts",
              ]}
              strong
            />
          </div>
          <div className="mt-5 grid gap-3 md:grid-cols-2">
            <Endpoint label="Admin console" value={workspace.localConsoleUrl} />
            <Endpoint
              label="Provider ingress"
              value={workspace.providerIngressUrl}
            />
          </div>
        </CardContent>
      </Card>
      <ActionBar primaryLabel="Continue in local console" onPrimary={advance} />
    </div>
  );
}

function PreflightStep({ snapshot, advance }: StepViewProps) {
  return (
    <OperationalStep
      icon={<ClipboardCheck className="h-5 w-5" aria-hidden="true" />}
      title="Preflight checks"
      badge="Run locally"
      description="The setup owner runs preflight from the customer-controlled environment. Fyralis receives only sanitized readiness evidence."
      cards={[
        ["Manifest validity", "Data-plane shape matches the BYOC contract."],
        ["Permission/IAM shape", "Least-privilege boundaries are present."],
        [
          "Required components",
          "Kubernetes, Postgres, S3, Kafka/MSK, and Secrets Manager are reachable.",
        ],
        [
          "Privacy posture",
          "No raw logs, prompts, embeddings, source data, or credentials are submitted.",
        ],
      ]}
      command={snapshot.setupPackage.commands[0]?.command}
      primaryLabel="Mark preflight complete"
      onPrimary={advance}
    />
  );
}

function DeploymentStep({ snapshot, advance }: StepViewProps) {
  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Deployment timeline</CardTitle>
          <Badge tone="info">Customer Kubernetes</Badge>
        </CardHeader>
        <CardContent className="grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
          <div className="grid gap-3">
            {snapshot.deployment.timeline.map((event) => (
              <div
                key={event.label}
                className="grid grid-cols-[1.5rem_minmax(0,1fr)] gap-3"
              >
                <span
                  className={cn(
                    "mt-1 h-3 w-3 rounded-full",
                    event.status === "done" ? "bg-success" : "bg-info",
                  )}
                />
                <span>
                  <strong className="block">{event.label}</strong>
                  <span className="text-sm text-muted-foreground">
                    {event.detail}
                  </span>
                </span>
              </div>
            ))}
          </div>
          <LogPanel logs={snapshot.deployment.logs} />
        </CardContent>
      </Card>
      <ActionBar
        primaryLabel="Continue to deployment validation"
        onPrimary={advance}
      />
    </div>
  );
}

function DeploymentValidationStep({ snapshot, advance }: StepViewProps) {
  return (
    <ValidationCard
      title="Post-deploy validation"
      validation={snapshot.deploymentValidation}
      primaryLabel="Mark validation complete"
      onPrimary={advance}
    />
  );
}

function sourceFirstSyncCommand({
  sourceId,
  workspace,
  syncMode,
  backfillWindow,
}: {
  sourceId: string;
  workspace: Workspace;
  syncMode: SourceConnection["syncMode"];
  backfillWindow: SourceConnection["backfillWindow"];
}) {
  return [
    "fyralis byoc source activate",
    `--source ${sourceId}`,
    "--requires-approval",
    "--start-first-sync",
    `--sync-mode ${syncModeToCli(syncMode)}`,
    `--backfill-window ${backfillWindowToCli(backfillWindow)}`,
    `--admin-console-url ${workspace.localConsoleUrl}`,
    `--provider-ingress-url ${workspace.providerIngressUrl}`,
    "--provider-authorization-mode preauthorized-ref",
    "--preauthorized-ref-manifest ./customer-source-refs.json",
    "--json",
  ].join(" ");
}

function syncModeToCli(syncMode: SourceConnection["syncMode"]) {
  const modes: Record<SourceConnection["syncMode"], string> = {
    "Dry run": "dry-run",
    "Limited backfill": "limited-backfill",
    "Live events": "live-events",
    "Backfill plus live": "backfill-plus-live",
    "Backfill plus polling": "backfill-plus-polling",
  };
  return modes[syncMode];
}

function backfillWindowToCli(
  backfillWindow: SourceConnection["backfillWindow"],
) {
  const windows: Record<SourceConnection["backfillWindow"], string> = {
    "Last 7 days": "7d",
    "Last 30 days": "30d",
    "Last 90 days": "90d",
    "All available history": "all-available",
    "No historical backfill": "none",
  };
  return windows[backfillWindow];
}

function errorMessage(caught: unknown) {
  if (caught instanceof Error) {
    return caught.message;
  }
  return "Unexpected error while reading the customer gateway.";
}

function sourceObservationSamples(
  source: Source,
  syncMode: SourceConnection["syncMode"],
): SourceObservation[] {
  const summaries: Record<
    string,
    Array<[string, SourceObservation["kind"], string]>
  > = {
    slack: [
      [
        "Finance launch thread",
        "message",
        "Approved Slack channels produced launch-readiness signals.",
      ],
      [
        "Customer-success escalation",
        "message",
        "A pilot allowlisted channel produced support-priority metadata.",
      ],
    ],
    jira: [
      [
        "Pilot issue movement",
        "issue",
        "Approved Jira projects produced issue status and comment metadata.",
      ],
      [
        "Release blocker changed",
        "task",
        "A tracked Jira issue moved into review during the first sync.",
      ],
    ],
    github: [
      [
        "Repository rollout activity",
        "pull-request",
        "Selected repositories produced pull-request and issue metadata.",
      ],
      [
        "Deployment branch updated",
        "deployment",
        "A tracked branch update landed as a code-workflow observation.",
      ],
    ],
    discord: [
      [
        "Community feedback channel",
        "message",
        "Approved Discord channels produced event metadata through the gateway.",
      ],
      [
        "Moderator follow-up",
        "message",
        "A scoped Discord thread produced a follow-up observation.",
      ],
    ],
    notion: [
      [
        "Launch checklist updates",
        "page",
        "Shared Notion pages produced page-change observations.",
      ],
      [
        "Runbook database edited",
        "page",
        "An approved Notion database update landed as a knowledge observation.",
      ],
    ],
    telegram: [
      [
        "Partner readiness chat",
        "message",
        "Approved Telegram dialogs produced MTProto session observations.",
      ],
      [
        "Pilot decision note",
        "message",
        "A scoped Telegram chat produced a decision-tracking observation.",
      ],
    ],
  };
  const rows = summaries[source.id] ?? [
    [
      `${source.name} pilot event`,
      "task" as const,
      `${source.name} produced a sanitized pilot observation.`,
    ],
  ];
  const previewOccurredAt = "2026-07-05T09:00:00.000Z";
  return rows.map(([title, kind, summary], index) => ({
    id: `obs_${source.id}_${index + 1}`,
    sourceId: source.id,
    title,
    kind,
    occurredAt: previewOccurredAt,
    summary: `${summary} Sync mode: ${syncMode}.`,
    evidencePath: `s3://fyralis-byoc-pilot/raw/${source.id}/obs_${index + 1}.jsonl`,
    status: "landed",
    origin: "preview",
    syncTrack:
      syncMode === "Live events"
        ? "live"
        : syncMode === "Backfill plus live" || syncMode === "Backfill plus polling"
          ? "mixed"
          : "historical",
    sourceChannel: sourceSignalConfig(source).landedChannel,
  }));
}

type SourceConnectRun = SourceAutomationCardState & {
  apiBase?: string;
  installUrl?: string | null;
  observationCount?: number;
  syncStartedAt?: string | null;
};

function sourceBackgroundRunView(
  run: SourceRehearsalStatus["autoConnectRun"] | null | undefined,
): Pick<SourceConnectRun, "status" | "label" | "message" | "actionUrl"> | null {
  // A browser agent can remain queued/running while it waits for a provider
  // admin. The durable run status is authoritative for that human gate; do
  // not let the background progress label hide the approval action.
  const runStatus =
    run?.status === "waiting_for_admin" || run?.status === "admin_gate"
      ? run.status
      : run?.backgroundStatus ?? run?.status;
  if (!runStatus) {
    return null;
  }
  if (
    run?.backgroundRunnerMode === "artifact_materialization" &&
    (runStatus === "waiting_for_admin" || runStatus === "admin_gate")
  ) {
    return {
      status: "blocked",
      label: "Not connected",
      message:
        "Source setup did not complete. Retry after enabling browser automation or use the fallback credential field."
    };
  }
  if (runStatus === "queued") {
    return {
      status: "connecting",
      label: "Queued",
      message: "Source setup is queued in the customer cloud.",
    };
  }
  if (runStatus === "running") {
    return {
      status: "connecting",
      label: "Running",
      message: "Source setup is running in the customer cloud.",
    };
  }
  if (runStatus === "waiting_for_admin" || runStatus === "admin_gate") {
    return {
      status: "waiting_admin",
      label: "Approval needed",
      message:
        "Provider admin approval is blocking completion. Fyralis keeps checking.",
      actionUrl: run?.handoffUrl,
    };
  }
  if (
    runStatus === "failed" ||
    runStatus === "blocked" ||
    runStatus === "error"
  ) {
    return {
      status: "blocked",
      label: "Needs attention",
      message:
        "Source setup needs attention. Retry when the provider task is ready.",
    };
  }
  if (runStatus === "connected" || runStatus === "completed") {
    return {
      status: "connecting",
      label: "Finalizing",
      message:
        "Source setup finished. Fyralis is waiting for connection proof.",
    };
  }
  return null;
}

function discordConnectionMessage(status: SourceRehearsalStatus) {
  const serverCount = status.installations.filter(
    (installation) => installation.enabled,
  ).length;
  const landingChannels = status.accessResources.filter(
    (resource) => resource.observationCount > 0,
  ).length;
  const readableChannels = status.accessSummary.ready;
  const blockedChannels = status.accessSummary.missingAccess;
  if (landingChannels > 0) {
    return `${serverCount} Discord server${serverCount === 1 ? "" : "s"} connected. Observations landing from ${landingChannels} message stream${landingChannels === 1 ? "" : "s"}.`;
  }
  if (readableChannels > 0) {
    return `${serverCount} Discord server${serverCount === 1 ? "" : "s"} connected. ${readableChannels} readable message stream${readableChannels === 1 ? "" : "s"} queued for backfill.`;
  }
  if (blockedChannels > 0) {
    return `${serverCount} Discord server${serverCount === 1 ? "" : "s"} connected. Private channel or thread access is needed.`;
  }
  return `${serverCount} Discord server${serverCount === 1 ? "" : "s"} connected. Fyralis is checking channel and thread access.`;
}

function SourceCatalogStep(props: StepViewProps) {
  const {
    snapshot,
    selectedSource,
    selectedConnection,
    selectSource,
    updateConnection,
    updateReadiness,
    landSourceObservations,
    connections,
    readiness,
    workspace,
  } = props;
  const effectiveReadiness = normalizeReadiness(readiness);
  const [automationStates, setAutomationStates] = useState<
    Record<string, SourceConnectRun>
  >({});
  const validatedConnectedSources = useRef(new Set<string>());
  const [sourceInputs, setSourceInputs] = useState<
    Record<string, Record<string, string>>
  >({});

  function patchSourceInput(sourceId: string, name: string, value: string) {
    setSourceInputs((current) => ({
      ...current,
      [sourceId]: {
        ...(current[sourceId] ?? {}),
        [name]: value
      }
    }));
  }

  function openProviderAuthorization(sourceId: string, installUrl: string) {
    selectSource(sourceId);
    window.open(installUrl, "_blank", "noopener,noreferrer");
  }

  function maybeOpenSlackAuthorization(sourceId: string, installUrl?: string | null) {
    if (
      sourceId === "slack" &&
      installUrl &&
      isExternalUrl(installUrl) &&
      installUrl.includes("slack.com/oauth")
    ) {
      openProviderAuthorization(sourceId, installUrl);
    }
  }

  function patchAutomationState(
    sourceId: string,
    patch: Partial<SourceConnectRun>,
  ) {
    setAutomationStates((current) => {
      const existing = current[sourceId] ?? {
        status: "idle" as const,
        label: "Ready",
      };
      return {
        ...current,
        [sourceId]: {
          ...existing,
          ...patch,
        },
      };
    });
  }

  function clearAutomationState(sourceId: string) {
    setAutomationStates((current) => {
      if (!(sourceId in current)) {
        return current;
      }
      const { [sourceId]: _removed, ...next } = current;
      return next;
    });
  }

  function applyFigmaOAuthStatus(status: FigmaConnectionStatus) {
    if (status.state === "deployment_setup_required") {
      updateConnection("figma", {
        status: "waiting-admin",
        receiptId: undefined,
        lastIssueCode: "deployment_setup_required",
      });
      patchAutomationState("figma", {
        status: "waiting_admin",
        label: "Admin setup required",
        message:
          status.nextAction ??
          "A deployment administrator must configure this deployment's Figma OAuth app.",
        actionUrl: "/host/control-panel",
        actionLabel: "Open Control Panel",
        observationCount: 0,
        syncStartedAt: null,
      });
      return;
    }
    if (["not_connected", "disconnected", "unknown"].includes(status.state)) {
      clearAutomationState("figma");
      if (status.state === "disconnected") {
        updateConnection("figma", {
          status: "not-configured",
          receiptId: undefined,
          lastIssueCode: undefined,
        });
      }
      return;
    }
    const isFailure = [
      "error",
      "degraded",
      "reauthorization_required",
    ].includes(status.state);
    const isConnected = status.state === "connected";
    const isConnecting = [
      "ready_for_provider_approval",
      "authorizing",
      "finalizing",
      "syncing",
    ].includes(status.state);

    if (isConnected || isFailure) {
      updateConnection("figma", {
        status: isFailure ? "error" : "connected",
        selectedScopes: [
          "OAuth file allowlist",
          "Design snapshots",
          "Comments and versions",
        ],
        backfillWindow: "All available history",
        syncMode: "Backfill plus polling",
        lastIssueCode: isFailure ? status.state : undefined,
        receiptId: status.installationId
          ? `figma:${status.installationId}`
          : undefined,
      });
    }

    patchAutomationState("figma", {
      status: isFailure ? "error" : isConnecting ? "connecting" : "connected",
      label: isFailure
        ? "Needs attention"
        : isConnecting
          ? "Syncing"
          : "Connected",
      message:
        status.lastError ??
        status.nextAction ??
        (status.observationCount
          ? `${status.observationCount} Figma observation${status.observationCount === 1 ? "" : "s"} landed.`
          : "Connect selected Figma files to create the first design snapshot."),
      observationCount: status.observationCount,
      syncStartedAt: status.latestObservationAt,
    });

    if (status.latestObservation?.occurredAt) {
      landSourceObservations("figma", [
        {
          id: status.latestObservation.id,
          sourceId: "figma",
          title: status.latestObservation.contentText ?? "Figma design snapshot",
          kind: "page",
          occurredAt: status.latestObservation.occurredAt,
          summary:
            status.latestObservation.contentText ??
            "Figma design snapshot is ready for intelligence processing.",
          evidencePath: `/observations/${status.latestObservation.id}`,
          status: "landed",
          origin: "gateway",
          syncTrack: "historical",
          sourceChannel: "figma:file_snapshot",
        },
      ]);
    }
  }

  function applyCatalogSourceStatus({
    source,
    status,
    apiBase,
    installUrl,
    waitingForAdmin,
    resetWhenMissingInstall,
  }: {
    source: Source;
    status: SourceRehearsalStatus;
    apiBase?: string;
    installUrl?: string | null;
    waitingForAdmin?: boolean;
    resetWhenMissingInstall?: boolean;
  }) {
    if (status.sourceId !== "discord" && status.observations.length) {
      landSourceObservations(status.sourceId, status.observations);
    }
    if (status.installed) {
      validatedConnectedSources.current.add(source.id);
      const failedShardCount = status.shardStateCounts.failed?.count ?? 0;
      const activeRunCount =
        (status.runStatusCounts.pending ?? 0) +
        (status.runStatusCounts.running ?? 0) +
        (status.runStatusCounts.in_progress ?? 0);
      const replayQueued = status.triggerCount > status.consumedTriggerCount;
      const firstSyncActive =
        status.observationCount === 0 && (activeRunCount > 0 || replayQueued);
      const hasFirstSyncFailure =
        !firstSyncActive &&
        (failedShardCount > 0 ||
          (status.runStatusCounts.failed ?? 0) > 0 ||
          Boolean(status.latestFailure));
      updateConnection(source.id, {
        status: "connected",
        selectedScopes: sourceScopeChoices(source.id).slice(0, 3),
        backfillWindow:
          source.id === "facebook_pages" ? "All available history" : "Last 30 days",
        syncMode:
          source.id === "facebook_pages" ? "Backfill plus live" : "Limited backfill",
        receiptId:
          status.autoConnectRun?.receiptPathHint ??
          `source_agent_${source.id}_connected`,
      });
      patchAutomationState(source.id, {
        status: hasFirstSyncFailure
          ? "error"
          : firstSyncActive
            ? "connecting"
            : "connected",
        label: hasFirstSyncFailure
          ? "Sync blocked"
          : firstSyncActive
            ? "Sync running"
            : "Connected",
        message:
          source.id === "discord"
            ? discordConnectionMessage(status)
            : status.observationCount
              ? `${status.observationCount} sanitized observation${status.observationCount === 1 ? "" : "s"} landed.`
              : firstSyncActive
                ? "AWS first sync is queued or running. Fyralis is checking for CloudTrail proof."
                : hasFirstSyncFailure
                  ? status.nextAction ||
                    status.latestFailure ||
                    "AWS first sync failed."
                  : "Install created. Fyralis is waiting for the first sync proof.",
        apiBase,
        installUrl,
        observationCount: status.observationCount,
        syncStartedAt: status.syncStartedAt,
        installations: status.installations,
        accessSummary: status.accessSummary,
        accessResources: status.accessResources,
        accessNextActions: status.accessNextActions,
      });
      return;
    }
    const persistedConnection = connections.find(
      (connection) => connection.sourceId === source.id,
    );
    if (
      persistedConnection?.status === "connected" &&
      !status.autoConnectRun
    ) {
      validatedConnectedSources.current.delete(source.id);
      updateConnection(source.id, {
        status: "not-configured",
        receiptId: undefined,
      });
      clearAutomationState(source.id);
      return;
    }
    const backgroundView = sourceBackgroundRunView(status.autoConnectRun);
    const receiptPathHint = status.autoConnectRun?.receiptPathHint;
    const handoffUrl = status.autoConnectRun?.handoffUrl ?? installUrl ?? null;
    const nextRunStatus =
      backgroundView?.status ??
      (waitingForAdmin ? "waiting_admin" : "connecting");
    if (resetWhenMissingInstall && !backgroundView && !waitingForAdmin) {
      updateConnection(source.id, {
        status: "draft",
        selectedScopes: [],
        receiptId: undefined,
      });
      clearAutomationState(source.id);
      return;
    }
    if (nextRunStatus === "blocked") {
      updateConnection(source.id, {
        status: "error",
        ...(receiptPathHint ? { receiptId: receiptPathHint } : {}),
      });
    } else if (nextRunStatus === "waiting_admin") {
      updateConnection(source.id, {
        status: "waiting-admin",
        ...(receiptPathHint ? { receiptId: receiptPathHint } : {}),
      });
    }
    patchAutomationState(source.id, {
      status: nextRunStatus,
      label:
        backgroundView?.label ??
        (waitingForAdmin ? "Approval needed" : "Running"),
      message:
        backgroundView?.message ??
        (waitingForAdmin
          ? "Provider admin approval is blocking completion. Fyralis keeps checking."
          : status.nextAction),
      apiBase,
      installUrl,
      actionUrl: backgroundView?.actionUrl ?? handoffUrl,
      actionLabel:
        nextRunStatus === "waiting_admin"
          ? sourceApprovalActionLabel(source)
          : undefined,
      observationCount: status.observationCount,
      syncStartedAt: status.syncStartedAt,
      installations: status.installations,
      accessSummary: status.accessSummary,
      accessResources: status.accessResources,
      accessNextActions: status.accessNextActions,
      ...(receiptPathHint ? { receiptPathHint } : {}),
    });
  }

  async function connectSource(
    sourceId: string,
    options?: {
      awsAssumingPrincipalArn?: string;
      discordAccessMode?: SourceConnectAccessMode;
    },
  ) {
    const source = snapshot.sources.find((item) => item.id === sourceId);
    if (!source) {
      return;
    }
    if (source.id === "figma") {
      // Figma has a native OAuth card below the source marketplace.  It owns
      // the top-level provider navigation and its own server-backed status
      // polling; generic rehearsal automation is intentionally never used.
      selectSource("figma");
      return;
    }
    const providerHandoffWindow = openImmediateProviderHandoffWindow(source);
    selectSource(sourceId);
    updateConnection(sourceId, { status: "validating" });
    patchAutomationState(sourceId, {
      status: "connecting",
      label: "Connecting",
      message: "Starting customer-cloud connection...",
    });
    try {
      const prepared = await autoConnectSourceRehearsal({
        sourceId,
        apiBase: workspace.providerIngressUrl,
        deploymentContext:
          source.id === "aws"
            ? {
                awsRegion: effectiveReadiness.region,
                awsAssumingPrincipalArn:
                  options?.awsAssumingPrincipalArn ??
                  effectiveReadiness.setupRoleArn,
              }
            : undefined,
        accessMode:
          source.id === "discord" ? options?.discordAccessMode : undefined,
        inputs: sourceInputs[sourceId] ?? {},
      });
      const preparedApiBase =
        prepared.gatewayApiBase || workspace.providerIngressUrl;
      applyCatalogSourceStatus({
        source,
        status: prepared.status,
        apiBase: preparedApiBase,
        installUrl: prepared.installUrl,
      });
      const installUrl = prepared.autoConnect.installUrl ?? prepared.installUrl;
      const providerConsoleUrl =
        prepared.autoConnect.browserAgent?.providerConsoleUrl ??
        prepared.browserAgent?.providerConsoleUrl ??
        prepared.providerConsoleUrl;
      const handoffUrl =
        prepared.autoConnect.browserAgentRun?.handoffUrl ??
        prepared.browserAgentRun?.handoffUrl ??
        installUrl ??
        (isExternalUrl(providerConsoleUrl) ? providerConsoleUrl : null);
      if (
        source.id === "discord" &&
        prepared.installUrl &&
        prepared.status.installed
      ) {
        patchAutomationState(sourceId, {
          status: "waiting_admin",
          label: "Add server",
          message: "Select another Discord server to grant Fyralis access.",
          apiBase: preparedApiBase,
          installUrl: prepared.installUrl,
          actionUrl: prepared.installUrl,
          actionLabel: "Open Discord",
          installations: prepared.status.installations,
          accessSummary: prepared.status.accessSummary,
          accessResources: prepared.status.accessResources,
          accessNextActions: prepared.status.accessNextActions,
        });
        completeImmediateProviderHandoffWindow(
          providerHandoffWindow,
          prepared.installUrl,
        );
        return;
      }
      if (
        prepared.autoConnect.state === "blocked" ||
        prepared.autoConnect.state === "error"
      ) {
        const providerConsoleUrl =
          prepared.autoConnect.browserAgent?.providerConsoleUrl ??
          prepared.browserAgent?.providerConsoleUrl ??
          prepared.providerConsoleUrl;
        const handoffUrl =
          prepared.autoConnect.browserAgentRun?.handoffUrl ??
          prepared.browserAgentRun?.handoffUrl ??
          prepared.autoConnect.installUrl ??
          prepared.installUrl ??
          (isExternalUrl(providerConsoleUrl) ? providerConsoleUrl : null);
        updateConnection(sourceId, { status: "error" });
        patchAutomationState(sourceId, {
          status:
            prepared.autoConnect.state === "blocked" ? "blocked" : "error",
          label: prepared.autoConnect.label,
          message: prepared.autoConnect.message,
          apiBase: preparedApiBase,
          installUrl: handoffUrl,
          actionUrl:
            prepared.autoConnect.state === "blocked" ? handoffUrl : null,
          actionLabel:
            prepared.autoConnect.state === "blocked" && source.id === "aws"
              ? "Create runtime role"
              : undefined,
        });
        if (
          sourceId === "slack" &&
          prepared.autoConnect.state === "blocked" &&
          prepared.autoConnect.browserAgentRun?.canStart &&
          handoffUrl &&
          isExternalUrl(handoffUrl)
        ) {
          openProviderAuthorization(
            sourceId,
            slackBrowserAgentHandoffUrl(handoffUrl, preparedApiBase) ??
              handoffUrl,
          );
        }
        if (prepared.autoConnect.state === "blocked") {
          completeImmediateProviderHandoffWindow(
            providerHandoffWindow,
            handoffUrl,
          );
        } else {
          closeImmediateProviderHandoffWindow(providerHandoffWindow);
        }
        return;
      }
      if (
        prepared.autoConnect.state === "connected" ||
        prepared.status.installed
      ) {
        closeImmediateProviderHandoffWindow(providerHandoffWindow);
        return;
      }
      if (prepared.autoConnect.state === "admin_gate") {
        const waiting = sourceWaitingAdminCard(prepared);
        updateConnection(sourceId, {
          status: "waiting-admin",
          receiptId:
            prepared.autoConnect.automationRun?.receiptPathHint ??
            `source_agent_${sourceId}_waiting_admin`,
        });
        patchAutomationState(sourceId, {
          status: "waiting_admin",
          label: waiting.label,
          message: waiting.message,
          apiBase: preparedApiBase,
          installUrl: handoffUrl,
          actionUrl: handoffUrl,
          actionLabel: sourceApprovalActionLabel(source),
          receiptPathHint: prepared.autoConnect.automationRun?.receiptPathHint,
        });
        completeImmediateProviderHandoffWindow(
          providerHandoffWindow,
          handoffUrl,
        );
        maybeOpenSlackAuthorization(sourceId, installUrl);
        return;
      }
      patchAutomationState(sourceId, {
        status: "connecting",
        label: sourceRunningLabel(prepared),
        message: sourceRunningMessage(prepared),
        apiBase: preparedApiBase,
        installUrl: handoffUrl,
        actionUrl: handoffUrl,
        receiptPathHint: prepared.autoConnect.automationRun?.receiptPathHint,
      });
      completeImmediateProviderHandoffWindow(providerHandoffWindow, handoffUrl);
      maybeOpenSlackAuthorization(sourceId, installUrl);
    } catch (caught) {
      closeImmediateProviderHandoffWindow(providerHandoffWindow);
      updateConnection(sourceId, { status: "error" });
      patchAutomationState(sourceId, {
        status: "error",
        label: "Retry",
        message: errorMessage(caught),
      });
    }
  }

  async function registerAwsRuntimeRole(roleArn: string) {
    const nextReadiness = normalizeReadiness({
      ...effectiveReadiness,
      setupRoleArn: roleArn,
    });
    updateReadiness(nextReadiness);
    await connectSource("aws", { awsAssumingPrincipalArn: roleArn });
  }

  async function finalizeAwsSourceRole(roleArn: string) {
    const source = snapshot.sources.find((item) => item.id === "aws");
    if (!source) {
      return;
    }
    patchAutomationState("aws", {
      status: "connecting",
      label: "Finalizing",
      message: "Registering the approved AWS source role...",
    });
    try {
      const status = await finalizeAwsSourceRehearsal({
        apiBase: workspace.providerIngressUrl,
        roleArn,
        region: effectiveReadiness.region,
      });
      applyCatalogSourceStatus({
        source,
        status,
        apiBase: workspace.providerIngressUrl,
      });
      updateConnection("aws", { status: "connected" });
    } catch (caught) {
      patchAutomationState("aws", {
        status: "error",
        label: "Retry",
        message: errorMessage(caught),
      });
      updateConnection("aws", { status: "error" });
    }
  }

  async function retryAwsFirstSync() {
    const source = snapshot.sources.find((item) => item.id === "aws");
    if (!source) {
      return;
    }
    patchAutomationState("aws", {
      status: "connecting",
      label: "Retrying",
      message: "Queueing a fresh AWS first sync...",
    });
    try {
      const status = await retryAwsFirstSyncRehearsal({
        apiBase: workspace.providerIngressUrl,
      });
      applyCatalogSourceStatus({
        source,
        status,
        apiBase: workspace.providerIngressUrl,
      });
    } catch (caught) {
      patchAutomationState("aws", {
        status: "error",
        label: "Retry",
        message: errorMessage(caught),
      });
    }
  }

  useEffect(() => {
    const activeRunMap = new Map<string, SourceConnectRun>();
    for (const connection of connections) {
      // Figma uses its native connection status endpoint. Polling the generic
      // rehearsal route here would be production-disabled and could overwrite
      // a truthful OAuth/sync state with an unrelated error.
      if (connection.sourceId === "figma") {
        continue;
      }
      if (connection.status === "waiting-admin") {
        activeRunMap.set(connection.sourceId, {
          status: "waiting_admin",
          label: "Approval needed",
          message:
            "Provider admin approval is blocking completion. Fyralis keeps checking.",
          apiBase: workspace.providerIngressUrl,
        });
      }
      if (connection.status === "validating") {
        activeRunMap.set(connection.sourceId, {
          status: "connecting",
          label: "Connecting",
          message: "Fyralis is checking source setup status.",
          apiBase: workspace.providerIngressUrl,
        });
      }
      if (
        connection.status === "connected" &&
        !validatedConnectedSources.current.has(connection.sourceId)
      ) {
        activeRunMap.set(connection.sourceId, {
          status: "connecting",
          label: "Checking connection",
          message: "Fyralis is confirming the persisted connection.",
          apiBase:
            automationStates[connection.sourceId]?.apiBase ??
            workspace.providerIngressUrl,
        });
      }
    }
    for (const [sourceId, state] of Object.entries(automationStates)) {
      // Figma's native OAuth card owns its status lifecycle. Never enqueue its
      // deployment setup / consent state for the generic rehearsal poller.
      if (sourceId === "figma") {
        continue;
      }
      if (state.status === "connecting" || state.status === "waiting_admin") {
        activeRunMap.set(sourceId, state);
      }
    }
    const activeRuns = Array.from(activeRunMap.entries());
    if (!activeRuns.length) {
      return;
    }
    let cancelled = false;
    let pollTimer: number | null = null;
    async function pollActiveSources() {
      if (cancelled) {
        return;
      }
      for (const [sourceId, state] of activeRuns) {
        const source = snapshot.sources.find((item) => item.id === sourceId);
        if (!source) {
          continue;
        }
        try {
          const status = await fetchSourceRehearsalStatus({
            sourceId,
            apiBase: state.apiBase ?? workspace.providerIngressUrl,
          });
          if (!cancelled) {
            applyCatalogSourceStatus({
              source,
              status,
              apiBase: state.apiBase,
              installUrl: state.installUrl,
              waitingForAdmin:
                state.status === "waiting_admin" &&
                Boolean(status.autoConnectRun),
              resetWhenMissingInstall: state.status === "waiting_admin",
            });
          }
        } catch (caught) {
          if (!cancelled) {
            patchAutomationState(sourceId, {
              status: "error",
              label: "Retry",
              message: errorMessage(caught),
            });
          }
        }
      }
      if (!cancelled) {
        pollTimer = window.setTimeout(pollActiveSources, 8000);
      }
    }
    pollTimer = window.setTimeout(pollActiveSources, 1000);
    return () => {
      cancelled = true;
      if (pollTimer !== null) {
        window.clearTimeout(pollTimer);
      }
    };
  }, [
    automationStates,
    connections,
    snapshot.sources,
    workspace.providerIngressUrl,
  ]);

  const selectedAutomationState = automationStates[selectedSource.id];
  const showSlackManualTokenFallback =
    selectedSource.id === "slack" &&
    selectedConnection?.status !== "connected" &&
    Boolean(
      sourceInputs.slack?.slack_app_config_token ||
        selectedAutomationState?.status === "error"
    );

  return (
    <div className="grid w-full min-w-0 max-w-full gap-4">
      <div className="flex min-w-0 flex-col gap-2 md:flex-row md:items-end md:justify-between">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold tracking-normal">Sources</h1>
        </div>
      </div>
      {showSlackManualTokenFallback ? (
        <div className="grid w-full max-w-lg gap-3 rounded-lg border border-border bg-card p-3">
          <Field label="Slack app configuration token">
            <Input
              type="password"
              autoComplete="off"
              inputMode="text"
              spellCheck={false}
              placeholder="xoxe..."
              value={sourceInputs.slack?.slack_app_config_token ?? ""}
              onChange={(event) =>
                patchSourceInput(
                  "slack",
                  "slack_app_config_token",
                  event.target.value
                )
              }
            />
          </Field>
        </div>
      ) : null}
      <SourceMarketplace
        sources={snapshot.sources}
        connections={connections}
        selectedSourceId={selectedSource.id}
        automationStates={automationStates}
        onSelect={selectSource}
        onConnect={(sourceId, options) => void connectSource(sourceId, options)}
        onRegisterAwsRuntimeRole={(roleArn) =>
          void registerAwsRuntimeRole(roleArn)
        }
        onFinalizeAwsSourceRole={(roleArn) =>
          void finalizeAwsSourceRole(roleArn)
        }
        onRetryAwsFirstSync={() => void retryAwsFirstSync()}
        onAuthorize={openProviderAuthorization}
      />
      {selectedSource.id === "figma" ? (
        <FigmaOAuthConnectionCard
          apiBase={workspace.providerIngressUrl}
          className="mt-2"
          onStatusChange={applyFigmaOAuthStatus}
        />
      ) : null}
    </div>
  );
}

function sourceWaitingAdminCard(prepared: SourceAutoConnectResponse) {
  const run = prepared.autoConnect.automationRun;
  if (run) {
    const backgroundVerb =
      run.backgroundStatus === "queued" ? "queued" : "started";
    return {
      label: "Approval needed",
      message:
        run.automatedActionCount > 0
          ? `Fyralis ${backgroundVerb} ${run.automatedActionCount} background step${run.automatedActionCount === 1 ? "" : "s"}. Provider admin approval is blocking completion.`
          : "Provider admin approval is blocking completion. Fyralis keeps checking.",
    };
  }
  return {
    label: "Approval needed",
    message: prepared.autoConnect.message,
  };
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

function sourceRunningLabel(prepared: SourceAutoConnectResponse) {
  return prepared.autoConnect.automationRun?.canStart === false
    ? prepared.autoConnect.label
    : "Running";
}

function sourceRunningMessage(prepared: SourceAutoConnectResponse) {
  const run = prepared.autoConnect.automationRun;
  if (!run) {
    return prepared.autoConnect.message;
  }
  if (run.receiptPathHint) {
    return "Fyralis is running source setup in the customer cloud.";
  }
  return prepared.autoConnect.message;
}

function WorkspaceLaunchStep({
  selectedSource,
  selectedConnection,
  setLaunchReady,
  advance,
}: StepViewProps) {
  return (
    <OperationalStep
      icon={<ShieldCheck className="h-5 w-5" aria-hidden="true" />}
      title="Pilot launch checklist"
      badge="Launch gates"
      description="Confirm the deployment, source sync, observability, support, and access controls are ready."
      cards={[
        ["Deployment health", "Green"],
        [
          "Active source",
          selectedConnection?.status === "connected"
            ? `${selectedSource.name} active`
            : "Pending activation",
        ],
        ["Observability", "Customer-local logs and metrics ready"],
        ["Support", "Launch-day owner confirmed"],
      ]}
      primaryLabel="Mark pilot launch ready"
      onPrimary={() => {
        setLaunchReady(true);
        advance();
      }}
    />
  );
}

function WorkspaceHomeStep({
  snapshot,
  connections,
  launchReady,
  selectedSource,
  selectedConnection,
  sourceObservations,
}: StepViewProps) {
  const activeSources = snapshot.sources.filter((source) => {
    const connection = connections.find((item) => item.sourceId === source.id);
    return (
      connection?.status === "connected" || source.id === selectedSource.id
    );
  });
  const activeSourceIds = new Set(activeSources.map((source) => source.id));
  const landedObservations = sourceObservations.filter((observation) =>
    activeSourceIds.has(observation.sourceId),
  );
  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Workspace status</CardTitle>
          <Badge tone={launchReady ? "success" : "info"}>
            {launchReady ? "Running in customer cloud" : "Launch review"}
          </Badge>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-3">
          <Metric
            label="Deployment"
            value="Healthy"
            detail="Post-deploy validation complete."
          />
          <Metric
            label="Active sources"
            value={String(activeSources.length)}
            detail={`${selectedSource.name} ${selectedConnection?.status ?? "ready"}.`}
          />
          <Metric
            label="Data boundary"
            value="BYOC"
            detail="No source data leaves the customer cloud."
          />
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Connected sources</CardTitle>
          <Badge tone="info">Pilot scope</Badge>
        </CardHeader>
        <CardContent className="grid gap-3">
          {activeSources.map((source) => (
            <div
              key={source.id}
              className="flex items-center justify-between gap-4 rounded-lg border border-border bg-background/70 p-4"
            >
              <span>
                <strong className="block">{source.name}</strong>
                <span className="text-sm text-muted-foreground">
                  {source.description}
                </span>
              </span>
              <Badge tone="success">Active</Badge>
            </div>
          ))}
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Recent observations landed</CardTitle>
          <Badge tone="success">{landedObservations.length} in BYOC</Badge>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-2">
          {landedObservations.map((observation) => (
            <ObservationCard
              key={observation.id}
              observation={observation}
              sourceName={
                snapshot.sources.find(
                  (source) => source.id === observation.sourceId,
                )?.name
              }
            />
          ))}
        </CardContent>
      </Card>
    </div>
  );
}

function OperationalStep({
  icon,
  title,
  badge,
  description,
  cards,
  command,
  primaryLabel,
  onPrimary,
}: {
  icon: ReactNode;
  title: string;
  badge: string;
  description: string;
  cards: Array<[string, string]>;
  command?: string;
  primaryLabel: string;
  onPrimary: () => void;
}) {
  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader>
          <div className="flex items-center gap-3">
            <span className="flex h-10 w-10 items-center justify-center rounded-lg bg-accent text-accent-foreground">
              {icon}
            </span>
            <div>
              <CardTitle>{title}</CardTitle>
              <p className="mt-1 text-sm text-muted-foreground">
                {description}
              </p>
            </div>
          </div>
          <Badge tone="info">{badge}</Badge>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 md:grid-cols-2">
            {cards.map(([label, detail]) => (
              <InfoBlock key={label} title={label} value={detail} />
            ))}
          </div>
          {command ? <LogPanel logs={[command]} command /> : null}
        </CardContent>
      </Card>
      <ActionBar primaryLabel={primaryLabel} onPrimary={onPrimary} />
    </div>
  );
}

function ValidationCard({
  title,
  validation,
  primaryLabel,
  onPrimary,
  secondaryLabel,
  onSecondary,
  footer,
}: {
  title: string;
  validation: Validation;
  primaryLabel: string;
  onPrimary: () => void;
  secondaryLabel?: string;
  onSecondary?: () => void;
  footer?: ReactNode;
}) {
  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader>
          <CardTitle>{title}</CardTitle>
          <Badge
            tone={
              validation.status === "failed"
                ? "error"
                : validation.status === "passed"
                  ? "success"
                  : "info"
            }
          >
            {validation.status}
          </Badge>
        </CardHeader>
        <CardContent className="grid gap-3">
          {validation.checks.map((check) => (
            <div
              key={check.label}
              className="flex gap-3 rounded-lg border border-border bg-background/70 p-4"
            >
              {check.status === "failed" ? (
                <AlertTriangle
                  className="mt-0.5 h-4 w-4 text-destructive"
                  aria-hidden="true"
                />
              ) : (
                <CheckCircle2
                  className="mt-0.5 h-4 w-4 text-success"
                  aria-hidden="true"
                />
              )}
              <span>
                <strong className="block">{check.label}</strong>
                <span className="text-sm text-muted-foreground">
                  {check.detail}
                </span>
              </span>
            </div>
          ))}
          <div className="mt-2 flex flex-wrap gap-2">
            <Button type="button" onClick={onPrimary}>
              {primaryLabel}
            </Button>
            {secondaryLabel && onSecondary ? (
              <Button type="button" variant="secondary" onClick={onSecondary}>
                {secondaryLabel}
              </Button>
            ) : null}
          </div>
        </CardContent>
      </Card>
      {footer}
    </div>
  );
}

function ActionBar({
  primaryLabel,
  onPrimary,
  secondaryLabel,
  secondaryIcon,
  onSecondary,
  submit,
  disabled,
  compact,
}: {
  primaryLabel: string;
  onPrimary?: () => void;
  secondaryLabel?: string;
  secondaryIcon?: ReactNode;
  onSecondary?: () => void;
  submit?: boolean;
  disabled?: boolean;
  compact?: boolean;
}) {
  return (
    <div
      className={cn(
        "flex flex-wrap items-center gap-3",
        !compact && "rounded-lg border border-border bg-card p-4",
      )}
    >
      <Button
        type={submit ? "submit" : "button"}
        onClick={onPrimary}
        disabled={disabled}
      >
        {primaryLabel}
        {!submit ? <ArrowRight className="h-4 w-4" aria-hidden="true" /> : null}
      </Button>
      {secondaryLabel && onSecondary ? (
        <Button type="button" variant="secondary" onClick={onSecondary}>
          {secondaryIcon}
          {secondaryLabel}
        </Button>
      ) : null}
      <p className="text-sm text-muted-foreground">
        Autosaved locally. Customer secrets never enter this workflow.
      </p>
    </div>
  );
}

function SelectField({
  label,
  help,
  register,
  children,
}: {
  label: string;
  help?: string;
  register: UseFormRegisterReturn;
  children: ReactNode;
}) {
  return (
    <Field label={label} help={help}>
      <Select {...register}>{children}</Select>
    </Field>
  );
}

function BoundaryPanel({
  title,
  items,
  strong,
}: {
  title: string;
  items: string[];
  strong?: boolean;
}) {
  return (
    <div
      className={cn(
        "rounded-lg border p-5",
        strong
          ? "border-success/30 bg-success/10"
          : "border-border bg-background/70",
      )}
    >
      <h3 className="text-lg font-semibold">{title}</h3>
      <ul className="mt-4 grid gap-2 text-sm text-muted-foreground">
        {items.map((item) => (
          <li key={item} className="flex gap-2">
            <CheckCircle2
              className="mt-0.5 h-4 w-4 shrink-0 text-success"
              aria-hidden="true"
            />
            {item}
          </li>
        ))}
      </ul>
    </div>
  );
}

function Endpoint({ label, value }: { label: string; value: string }) {
  return <InfoBlock title={label} value={value} code />;
}

function InfoBlock({
  title,
  value,
  code,
}: {
  title: string;
  value: string;
  code?: boolean;
}) {
  return (
    <div className="rounded-lg border border-border bg-background/70 p-4">
      <strong className="block text-sm">{title}</strong>
      {code ? (
        <code className="mt-2 block whitespace-pre-wrap break-all text-xs text-muted-foreground">
          {value}
        </code>
      ) : (
        <span className="mt-2 block whitespace-pre-wrap text-sm text-muted-foreground">
          {value}
        </span>
      )}
    </div>
  );
}

function SignalPathCard({
  title,
  detail,
  command,
}: {
  title: string;
  detail: string;
  command: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-background/70 p-4">
      <strong className="block text-sm">{title}</strong>
      <span className="mt-2 block text-sm leading-6 text-muted-foreground">
        {detail}
      </span>
      <code className="mt-3 block break-all text-xs text-muted-foreground">
        {command}
      </code>
    </div>
  );
}

function ObservationCard({
  observation,
  sourceName,
}: {
  observation: SourceObservation;
  sourceName?: string;
}) {
  const isGateway = observation.origin === "gateway";
  return (
    <div
      className={cn(
        "rounded-lg border p-4",
        isGateway
          ? "border-success/25 bg-success/10"
          : "border-info/25 bg-info/10",
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <span>
          <strong className="block">{observation.title}</strong>
          {sourceName ? (
            <span className="mt-1 block text-xs font-semibold text-muted-foreground">
              {sourceName}
            </span>
          ) : null}
        </span>
        <span className="flex shrink-0 flex-wrap justify-end gap-2">
          <Badge tone={isGateway ? "success" : "info"}>
            {isGateway ? "Gateway" : "Preview"}
          </Badge>
          <Badge tone="success">{observation.status}</Badge>
        </span>
      </div>
      <p className="mt-3 text-sm leading-6 text-muted-foreground">
        {observation.summary}
      </p>
      <div className="mt-3 grid gap-2">
        <code className="break-all text-xs text-muted-foreground">
          {observation.evidencePath}
        </code>
        {observation.sourceChannel ? (
          <code className="break-all text-xs text-muted-foreground">
            {observation.sourceChannel}
          </code>
        ) : null}
        <span className="text-xs text-muted-foreground">
          {observation.kind} · {observation.syncTrack ?? "mixed"} ·{" "}
          {observation.occurredAt.replace("T", " ").replace("Z", " UTC")}
        </span>
      </div>
    </div>
  );
}

function LogPanel({ logs, command }: { logs: string[]; command?: boolean }) {
  return (
    <div className="mt-5 rounded-lg border border-border bg-primary p-4 text-primary-foreground">
      <div className="mb-3 flex items-center gap-2 text-sm font-semibold">
        <TerminalSquare className="h-4 w-4" aria-hidden="true" />
        {command ? "Command" : "Deployment log"}
      </div>
      <div className="grid gap-2">
        {logs.map((log) => (
          <code
            key={log}
            className="break-all text-xs text-primary-foreground/80"
          >
            {log}
          </code>
        ))}
      </div>
    </div>
  );
}

function Metric({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="rounded-lg border border-border bg-background/70 p-4">
      <span className="text-xs font-semibold text-muted-foreground">
        {label}
      </span>
      <strong className="mt-2 block text-2xl tracking-tight">{value}</strong>
      <span className="mt-1 block text-sm text-muted-foreground">{detail}</span>
    </div>
  );
}

function isExternalUrl(value: string | null | undefined): value is string {
  return Boolean(value && /^https?:\/\//.test(value));
}

function openImmediateProviderHandoffWindow(source: Source): Window | null {
  if (source.id !== "discord" || typeof window === "undefined") {
    return null;
  }
  const opened = window.open("about:blank", "_blank");
  if (!opened) {
    return null;
  }
  try {
    opened.opener = null;
    opened.document.title = `Opening ${source.name}`;
    opened.document.body.textContent = `Opening ${source.name}...`;
  } catch {
    // Some browser contexts do not allow touching the blank window document.
  }
  return opened;
}

function completeImmediateProviderHandoffWindow(
  opened: Window | null,
  url: string | null | undefined,
) {
  if (!opened) {
    return;
  }
  if (!isExternalUrl(url)) {
    closeImmediateProviderHandoffWindow(opened);
    return;
  }
  try {
    opened.location.href = url;
  } catch {
    closeImmediateProviderHandoffWindow(opened);
  }
}

function closeImmediateProviderHandoffWindow(opened: Window | null) {
  if (!opened) {
    return;
  }
  try {
    opened.close();
  } catch {
    // The handoff window may already be controlled by the browser.
  }
}

function slackBrowserAgentHandoffUrl(
  handoffUrl: string,
  gatewayApiBase: string | undefined
) {
  if (!gatewayApiBase) {
    return null;
  }
  try {
    const endpoint = new URL(
      "/platform/onboarding/sources/slack/rehearsal/browser-agent/configuration",
      gatewayApiBase
    ).toString();
    const payload = {
      schema_version: "fyralis.browser_agent.handoff.slack.v1",
      source: "slack",
      endpoint
    };
    const url = new URL(handoffUrl);
    url.hash = `fyralis_agent=${base64UrlEncode(JSON.stringify(payload))}`;
    return url.toString();
  } catch {
    return null;
  }
}

function base64UrlEncode(value: string) {
  if (typeof window === "undefined") {
    return "";
  }
  return window
    .btoa(value)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function sourceScopeChoices(sourceId: string) {
  const scopes: Record<string, string[]> = {
    slack: [
      "#leadership",
      "#finance-ops",
      "#customer-success",
      "#engineering",
      "Consented DMs",
      "Pinned files",
    ],
    github: [
      "Core repos",
      "Infrastructure repos",
      "Open pull requests",
      "Issues",
      "Release branches",
      "Security repos excluded",
    ],
    gmail: [
      "Pilot executives",
      "Finance mailbox",
      "Customer-success mailbox",
      "Last 30 days",
      "Exclude personal labels",
      "Metadata-first crawl",
    ],
    facebook_pages: [
      "Selected Facebook Page",
      "Messenger conversations",
      "All available history",
      "Live messages webhook",
      "Page-authored echoes",
      "Unavailable/deleted gaps noted",
    ],
  };
  return (
    scopes[sourceId] ?? [
      "Pilot workspace",
      "Approved entities",
      "Last 30 days",
      "Live updates",
      "Metadata-first crawl",
      "Sensitive records excluded",
    ]
  );
}
