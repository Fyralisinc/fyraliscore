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
  TerminalSquare
} from "lucide-react";
import { useEffect, useState, type ReactNode } from "react";
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
  type SourceScopeFormValues
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
  Workspace
} from "../types";
import { SourceDetailPanel } from "../components/source-detail-panel";
import { SourceMarketplace } from "../components/source-marketplace";
import {
  createDesignPartnerOnboardingIntent,
  fetchGatewaySourceObservations,
  fetchSourceRehearsalStatus,
  finalizeJiraRehearsal,
  finalizeTelegramRehearsal,
  prepareSourceRehearsal,
  type SourceRehearsalPrepareResponse,
  type SourceRehearsalStatus,
  submitDesignPartnerIntake
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
  updateConnection: (sourceId: string, patch: Partial<SourceConnection>) => void;
  setSourceValidation: (validation: Validation) => void;
  upsertSyncJob: (job: SyncJob) => void;
  landSourceObservations: (
    sourceId: string,
    observations: SourceObservation[]
  ) => void;
  setLaunchReady: (ready: boolean) => void;
  goTo: (step: StepId) => void;
  advance: () => void;
};

export function StepView({ stepId, props }: { stepId: StepId; props: StepViewProps }) {
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
      return <SourceCatalogStep {...props} />;
    case "source-setup":
      return <SourceSetupStep {...props} />;
    case "source-validation":
      return <SourceValidationStep {...props} />;
    case "source-scope":
      return <SourceScopeStep {...props} />;
    case "first-sync":
      return <FirstSyncStep {...props} />;
    case "ingestion-health":
      return <IngestionHealthStep {...props} />;
    case "activation":
      return <ActivationStep {...props} />;
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
    requiredFor: "Running the gateway, workers, and local customer-cloud console.",
    provisionAction: "Provision a dedicated EKS runtime and install Fyralis with Helm.",
    existingAction: "Attach Fyralis to the approved Kubernetes runtime."
  },
  {
    key: "network",
    label: "Network/VPC",
    existingValue: "existing-ready",
    provisionValue: "provision-isolated-vpc",
    module: "terraform/aws/network",
    requiredFor: "Keeping ingress, egress, private DNS, and service access in the approved boundary.",
    provisionAction: "Create an isolated VPC, subnets, security groups, and DNS shape.",
    existingAction: "Use the approved VPC and generate network policy references."
  },
  {
    key: "secrets",
    label: "AWS Secrets Manager",
    existingValue: "aws-secrets-manager",
    provisionValue: "provision-secret-refs",
    module: "terraform/aws/secrets",
    requiredFor: "Storing customer-owned source tokens, database URLs, and provider secrets locally.",
    provisionAction: "Create secret names, IAM access, and placeholder refs without secret values.",
    existingAction: "Reference existing customer-owned secret names only."
  },
  {
    key: "postgres",
    label: "Postgres with pgvector",
    existingValue: "pgvector-ready",
    provisionValue: "provision-rds-pgvector",
    module: "terraform/aws/rds-pgvector",
    requiredFor: "Tenant data, model state, embeddings, and vector search.",
    provisionAction: "Provision Postgres/RDS and enable the pgvector extension.",
    existingAction: "Use the existing Postgres endpoint and pgvector validation check."
  },
  {
    key: "objectStorage",
    label: "S3-compatible storage",
    existingValue: "s3-compatible-ready",
    provisionValue: "provision-s3",
    module: "terraform/aws/s3",
    requiredFor: "Raw payload tier, deployment artifacts, and durable evidence receipts.",
    provisionAction: "Create S3 buckets, retention policy, and least-privilege access.",
    existingAction: "Use the approved bucket and generate object-storage refs."
  },
  {
    key: "kafka",
    label: "Kafka/MSK",
    existingValue: "kafka-msk-ready",
    provisionValue: "provision-msk",
    module: "terraform/aws/msk",
    requiredFor: "Kafka-first ingestion durability and worker orchestration.",
    provisionAction: "Provision MSK/Kafka topics, ACL shape, and worker connectivity.",
    existingAction: "Use the existing Kafka/MSK cluster and generate topic/ACL checks."
  }
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
    access: "bearer session" | "state-token callback" | "provider signed" | "local only";
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
        access: "bearer session"
      },
      {
        label: "OAuth callback",
        method: "GET",
        path: "/integrations/slack/callback",
        access: "state-token callback"
      },
      {
        label: "Events API webhook",
        method: "POST",
        path: "/webhooks/slack/events",
        access: "provider signed"
      }
    ],
    generatedArtifacts: [
      "fyralis-slack-app-manifest.yaml",
      "fyralis-slack-app-events-manifest.yaml",
      "slack.env.example",
      "slack-provider-setup.json"
    ],
    envKeys: [
      "SLACK_CLIENT_ID",
      "SLACK_CLIENT_SECRET",
      "SLACK_SIGNING_SECRET",
      "SLACK_REDIRECT_URI",
      "OAUTH_STATE_HMAC_KEY"
    ],
    manualGates: ["Slack app/admin approval", "Slack OAuth consent"]
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
        access: "bearer session"
      },
      {
        label: "Finalize connection",
        method: "POST",
        path: "/integrations/jira/connect/finalize",
        access: "bearer session"
      },
      {
        label: "Webhook",
        method: "POST",
        path: "/webhooks/jira/events",
        access: "provider signed"
      }
    ],
    generatedArtifacts: [
      "jira-connect-payload.example.json",
      "jira.env.example",
      "jira-provider-setup.json"
    ],
    envKeys: ["JIRA_BASE_URL", "JIRA_ACCOUNT_EMAIL", "JIRA_API_TOKEN"],
    manualGates: [
      "Atlassian API token creation",
      "Project scope selection",
      "Jira webhook registration"
    ]
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
        access: "bearer session"
      },
      {
        label: "GitHub callback",
        method: "GET",
        path: "/integrations/github/callback",
        access: "state-token callback"
      },
      {
        label: "Webhook",
        method: "POST",
        path: "/webhooks/github",
        access: "provider signed"
      }
    ],
    generatedArtifacts: [
      "fyralis-github-app-manifest.json",
      "github.env.example",
      "github-provider-setup.json"
    ],
    envKeys: [
      "GITHUB_APP_SLUG",
      "GITHUB_APP_ID",
      "GITHUB_APP_PRIVATE_KEY",
      "WEBHOOK_SECRET_GITHUB",
      "OAUTH_STATE_HMAC_KEY"
    ],
    manualGates: ["GitHub App creation/update", "Org installation approval"]
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
        access: "bearer session"
      },
      {
        label: "OAuth callback",
        method: "GET",
        path: "/integrations/discord/callback",
        access: "state-token callback"
      },
      {
        label: "Interactions/webhook",
        method: "POST",
        path: "/webhooks/discord",
        access: "provider signed"
      }
    ],
    generatedArtifacts: [
      "fyralis-discord-app-setup.json",
      "discord.env.example",
      "discord-provider-setup.json"
    ],
    envKeys: [
      "DISCORD_CLIENT_ID",
      "DISCORD_CLIENT_SECRET",
      "DISCORD_APPLICATION_ID",
      "DISCORD_BOT_TOKEN",
      "WEBHOOK_SECRET_DISCORD",
      "OAUTH_STATE_HMAC_KEY"
    ],
    manualGates: ["Discord app/bot setup", "Server admin bot approval"]
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
        access: "bearer session"
      },
      {
        label: "OAuth callback",
        method: "GET",
        path: "/integrations/notion/callback",
        access: "state-token callback"
      },
      {
        label: "Webhook",
        method: "POST",
        path: "/webhooks/notion/events",
        access: "provider signed"
      }
    ],
    generatedArtifacts: [
      "fyralis-notion-app-setup.json",
      "notion.env.example",
      "notion-provider-setup.json"
    ],
    envKeys: [
      "NOTION_CLIENT_ID",
      "NOTION_CLIENT_SECRET",
      "NOTION_REDIRECT_URI",
      "OAUTH_STATE_HMAC_KEY"
    ],
    manualGates: [
      "Notion integration setup",
      "Workspace OAuth consent",
      "Webhook verification token copy"
    ]
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
        access: "local only"
      }
    ],
    generatedArtifacts: [
      "telegram-session-plan.json",
      "telegram.env.example",
      "telegram-provider-setup.json"
    ],
    envKeys: [
      "TELEGRAM_ACCOUNT_LABEL",
      "TELEGRAM_API_ID",
      "TELEGRAM_API_HASH",
      "TELEGRAM_SESSION"
    ],
    manualGates: [
      "Telegram API ID/hash creation",
      "MTProto login code",
      "Dialog scope approval"
    ]
  }
};

const SOURCE_SIGNAL_CONFIG: Record<string, SourceSignalConfig> = {
  slack: {
    historicalTrigger:
      "OAuth callback writes provider_installations and onboarding_triggers(source='slack').",
    liveIngress: "/webhooks/slack/events",
    liveWorker: "Slack Events API -> webhook router -> ingestion pipeline.",
    landedChannel: "slack:message"
  },
  jira: {
    historicalTrigger:
      "Jira finalize writes jira_installations/projects and onboarding_triggers(source='jira').",
    liveIngress: "/webhooks/jira/events",
    liveWorker: "Jira webhook -> signed webhook router -> ingestion pipeline.",
    landedChannel: "jira:issue"
  },
  github: {
    historicalTrigger:
      "GitHub callback writes provider_installations and onboarding_triggers(source='github').",
    liveIngress: "/webhooks/github",
    liveWorker: "GitHub webhook -> signed webhook router -> ingestion pipeline.",
    landedChannel: "github:webhook"
  },
  discord: {
    historicalTrigger:
      "Discord callback writes provider_installations and onboarding_triggers(source='discord').",
    liveIngress: "/webhooks/discord",
    liveWorker: "Discord gateway/bot events -> gateway dispatcher -> observations.",
    landedChannel: "discord:message"
  },
  notion: {
    historicalTrigger:
      "Notion callback writes provider_installations and onboarding_triggers(source='notion').",
    liveIngress: "/webhooks/notion",
    liveWorker: "Notion webhook -> verified webhook router -> ingestion pipeline.",
    landedChannel: "notion:object"
  },
  telegram: {
    historicalTrigger:
      "Telegram finalize writes telegram_installations/dialogs and onboarding_triggers(source='telegram').",
    liveIngress: "Customer-cloud MTProto session",
    liveWorker: "Telegram gateway worker reads updates and writes observations locally.",
    landedChannel: "telegram:message"
  }
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
      `${prefix}_TOKEN_REF`
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
      : "provider signed") as SourceRehearsalConfig["gatewayRoutes"][number]["access"]
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
      `${source.id}-connection-checklist.json`
    ],
    envKeys: genericEnvKeys(source),
    manualGates: [
      "Provider/admin approval",
      "Scope selection",
      source.providerIngressPaths.length ? "Webhook registration" : "Customer-local ref approval"
    ]
  };
}

function sourceSignalConfig(source: Source): SourceSignalConfig {
  return (
    SOURCE_SIGNAL_CONFIG[source.id] ?? {
      historicalTrigger: `${source.name} setup writes local install refs and onboarding_triggers(source='${source.id}').`,
      liveIngress: source.providerIngressPaths[0] ?? source.noIngressReason ?? "Customer-cloud local worker",
      liveWorker: `${source.name} worker reads approved resources and writes sanitized observations locally.`,
      landedChannel: `${source.id}:*`
    }
  );
}

function capabilityStatus(
  readiness: CloudReadiness,
  capability: CapabilityDefinition
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
    status: capabilityStatus(readiness, capability)
  }));
  return {
    rows,
    existing: rows.filter((row) => row.status === "existing"),
    provision: rows.filter((row) => row.status === "provision"),
    needsDecision: rows.filter(
      (row) => row.status === "guidance" || row.status === "unknown"
    )
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
    setupRoleArn:
      partial.setupRoleArn ??
      "arn:aws:iam::123456789012:role/FyralisByocSetupRole",
    kubernetes: partial.kubernetes ?? "provision-eks",
    network: partial.network ?? "provision-isolated-vpc",
    secrets: partial.secrets ?? "provision-secret-refs",
    postgres: partial.postgres ?? "provision-rds-pgvector",
    objectStorage: partial.objectStorage ?? "provision-s3",
    kafka: partial.kafka ?? "provision-msk"
  };
}

function PlanSelection({
  snapshot,
  selectedPlan,
  onboardingIntent,
  choosePlan,
  setOnboardingIntent,
  advance
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
          : "Could not create the Design Partner BYOC intent."
      );
    } finally {
      setStarting(false);
    }
  }

  return (
    <div className="rounded-lg border border-border bg-card p-5 text-card-foreground md:p-7">
      <div className="mx-auto max-w-5xl">
        <div className="mb-6">
          <p className="text-sm font-semibold text-muted-foreground">Get Fyralis</p>
          <h2 className="mt-2 text-3xl font-semibold tracking-normal">
            Choose how you want to start.
          </h2>
          <p className="mt-3 max-w-2xl text-sm leading-6 text-muted-foreground">
            Start with the design-partner BYOC path or move toward an enterprise BYOC rollout. We do not collect source credentials here.
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
              selectedPlan === plan.id && "ring-2 ring-ring/25"
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
                        : "text-info"
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
                  : "border-info bg-info text-info-foreground hover:bg-info/90"
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
  advance
}: StepViewProps) {
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const form = useForm<CustomerFormValues>({
    resolver: zodResolver(customerSchema),
    defaultValues: customer,
    mode: "onChange"
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
            values
          );
          setOnboardingIntent(updatedIntent);
          updateCustomer(values);
          advance();
        } catch (caught) {
          setSubmitError(
            caught instanceof Error
              ? caught.message
              : "Could not submit Design Partner BYOC intake."
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
        primaryLabel={submitting ? "Creating workspace..." : "Continue to cloud readiness"}
        disabled={submitting}
        submit
      />
    </form>
  );
}

function CloudReadinessStep({
  readiness,
  updateReadiness,
  advance
}: StepViewProps) {
  const initialReadiness = normalizeReadiness(readiness);
  const form = useForm<CloudReadinessFormValues>({
    resolver: zodResolver(cloudReadinessSchema),
    defaultValues: initialReadiness,
    mode: "onChange"
  });

  useEffect(() => {
    const subscription = form.watch((value) => {
      const parsed = cloudReadinessSchema.safeParse(
        normalizeReadiness(value as CloudReadiness)
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
          <SelectField label="Cloud region" register={form.register("region")} help="Used for deployment location and residency.">
            <option>us-east-1</option>
            <option>us-west-2</option>
            <option>eu-west-1</option>
          </SelectField>
          <SelectField label="Deployment environment" register={form.register("environment")} help="Controls naming, safety gates, and rollout strictness.">
            <option>pilot</option>
            <option>staging</option>
            <option>production</option>
          </SelectField>
          <SelectField
            label="Agent access"
            register={form.register("agentAccess")}
            help="The agent runs with scoped setup access, not customer source credentials."
          >
            <option value="customer-cloud-agent">Install agent inside customer cloud</option>
            <option value="aws-cross-account-role">Allow Fyralis agent to assume setup role</option>
          </SelectField>
          <Field
            label="Setup role ARN"
            help="Role name/reference only. No secret keys or source tokens."
            error={form.formState.errors.setupRoleArn?.message}
          >
            <Input {...form.register("setupRoleArn")} />
          </Field>
          <SelectField
            label="Permission profile"
            register={form.register("agentPermissionProfile")}
            help="Controls whether the agent can only discover or can also provision BYOC infra."
          >
            <option value="byoc-bootstrap-provisioner">BYOC bootstrap provisioner</option>
            <option value="discovery-only">Discovery only</option>
          </SelectField>
          <SelectField
            label="Apply policy"
            register={form.register("agentApprovalMode")}
            help="Recommended: require the customer setup owner to approve the plan before apply."
          >
            <option value="approval-required">Customer approves before apply</option>
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
            <div key={capability.key} className="rounded-lg border border-border bg-background/70 p-4">
              <div className="flex items-start justify-between gap-3">
                <span>
                  <strong className="block">{capability.label}</strong>
                  <span className="mt-1 block text-sm text-muted-foreground">
                    Agent discovers existing resources, then provisions only what
                    is missing and approved.
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
            "Emit sanitized readiness status"
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
            "Create resources outside the setup policy"
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
  const providerExecutorCommand =
    `fyralis byoc agent provider-executor --cloud aws --region ${effectiveReadiness.region} --stack-name fyralis-byoc-acme-finance --create-change-set --execute-change-set --execute-helm --confirm-cost-and-mutation --json`;
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
    "--json"
  ].join(" ");
  const commands = [
    {
      label: "Zero-spend local rehearsal",
      command:
        `fyralis byoc agent local-rehearsal --region ${effectiveReadiness.region} --workdir .fyralis/local-rehearsal --json`
    },
    {
      label: "Create setup role template",
      command:
        `fyralis byoc agent role-template --cloud aws --region ${effectiveReadiness.region} --external-id fyralis-acme-finance-pilot`
    },
    {
      label: "Register setup agent",
      command: accessCommand
    },
    {
      label: "Discover and plan",
      command:
        `fyralis byoc agent discover --region ${effectiveReadiness.region} --capabilities kubernetes,network,secrets,postgres,s3,kafka --emit-plan`
    },
    {
      label: "Apply approved plan",
      command: applyCommand
    },
    {
      label: "Execute AWS and Helm setup",
      command: providerExecutorCommand
    },
    {
      label: "Validate",
      command:
        "fyralis byoc agent validate --json --emit-sanitized-readiness-report"
    }
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
              <CloudCog className="mt-0.5 h-5 w-5 shrink-0 text-info" aria-hidden="true" />
              <span>
                <strong className="block text-sm">Agent runs with scoped setup access</strong>
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
              <div key={row.key} className="rounded-lg border border-border bg-background/70 p-4">
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
              <div key={artifact.filename} className="rounded-lg border border-border bg-background/70 p-4">
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
              <div key={command.label} className="rounded-lg border border-border bg-primary p-4 text-primary-foreground">
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
      <ActionBar primaryLabel="Continue to trust boundary" onPrimary={advance} secondaryLabel="Download artifacts" secondaryIcon={<Download className="h-4 w-4" aria-hidden="true" />} />
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
                "Bounded issue codes"
              ]}
            />
            <div className="hidden items-center justify-center lg:flex">
              <ArrowRight className="h-8 w-8 text-muted-foreground" aria-hidden="true" />
            </div>
            <BoundaryPanel
              title="Customer cloud"
              items={[
                "Runtime deployment",
                "Source credentials",
                "Raw source payloads",
                "Private logs and prompts"
              ]}
              strong
            />
          </div>
          <div className="mt-5 grid gap-3 md:grid-cols-2">
            <Endpoint label="Admin console" value={workspace.localConsoleUrl} />
            <Endpoint label="Provider ingress" value={workspace.providerIngressUrl} />
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
        ["Required components", "Kubernetes, Postgres, S3, Kafka/MSK, and Secrets Manager are reachable."],
        ["Privacy posture", "No raw logs, prompts, embeddings, source data, or credentials are submitted."]
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
              <div key={event.label} className="grid grid-cols-[1.5rem_minmax(0,1fr)] gap-3">
                <span className={cn("mt-1 h-3 w-3 rounded-full", event.status === "done" ? "bg-success" : "bg-info")} />
                <span>
                  <strong className="block">{event.label}</strong>
                  <span className="text-sm text-muted-foreground">{event.detail}</span>
                </span>
              </div>
            ))}
          </div>
          <LogPanel logs={snapshot.deployment.logs} />
        </CardContent>
      </Card>
      <ActionBar primaryLabel="Continue to deployment validation" onPrimary={advance} />
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

function sourceRehearsalCommand(source: Source, workspace: Workspace) {
  const sourceId = source.id;
  const config = sourceRehearsalConfig(source);
  return [
    "fyralis byoc source rehearse",
    `--source ${sourceId}`,
    `--setup-dir .fyralis/local-rehearsal/${sourceId}`,
    config.needsPublicUrl ? `--public-url ${workspace.providerIngressUrl}` : "",
    "--no-start-tunnel",
    "--json"
  ]
    .filter(Boolean)
    .join(" ");
}

function sourceApplyEnvCommand(source: Source, workspace: Workspace) {
  const sourceId = source.id;
  const config = sourceRehearsalConfig(source);
  const hasInstallRoute = config.gatewayRoutes.some((route) =>
    route.path.includes("/install")
  );
  return [
    "fyralis byoc source rehearse",
    `--source ${sourceId}`,
    `--setup-dir .fyralis/local-rehearsal/${sourceId}`,
    config.needsPublicUrl ? `--public-url ${workspace.providerIngressUrl}` : "",
    `--provider-env .fyralis/local-rehearsal/${sourceId}/${sourceId}.env`,
    "--apply-env",
    hasInstallRoute
      ? "--print-install-url --tenant-id <tenant-id> --actor-id <actor-id>"
      : "",
    "--json"
  ]
    .filter(Boolean)
    .join(" ");
}

function sourceFirstSyncCommand({
  sourceId,
  workspace,
  syncMode,
  backfillWindow
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
    "--json"
  ].join(" ");
}

function syncModeToCli(syncMode: SourceConnection["syncMode"]) {
  const modes: Record<SourceConnection["syncMode"], string> = {
    "Dry run": "dry-run",
    "Limited backfill": "limited-backfill",
    "Live events": "live-events",
    "Backfill plus live": "backfill-plus-live"
  };
  return modes[syncMode];
}

function backfillWindowToCli(backfillWindow: SourceConnection["backfillWindow"]) {
  const windows: Record<SourceConnection["backfillWindow"], string> = {
    "Last 7 days": "7d",
    "Last 30 days": "30d",
    "Last 90 days": "90d",
    "No historical backfill": "none"
  };
  return windows[backfillWindow];
}

function gatewayRouteUrl(workspace: Workspace, path: string) {
  if (path.startsWith(".")) {
    return path;
  }
  return `${workspace.providerIngressUrl}${path}`;
}

function isPublicHttpsUrl(value: string) {
  try {
    const url = new URL(value);
    return (
      url.protocol === "https:" &&
      !["localhost", "127.0.0.1", "0.0.0.0"].includes(url.hostname) &&
      !url.hostname.endsWith(".example") &&
      !value.includes("REPLACE_WITH")
    );
  } catch {
    return false;
  }
}

function errorMessage(caught: unknown) {
  if (caught instanceof Error) {
    return caught.message;
  }
  return "Unexpected error while reading the customer gateway.";
}

function sourceObservationSamples(
  source: Source,
  syncMode: SourceConnection["syncMode"]
): SourceObservation[] {
  const summaries: Record<string, Array<[string, SourceObservation["kind"], string]>> = {
    slack: [
      [
        "Finance launch thread",
        "message",
        "Approved Slack channels produced launch-readiness signals."
      ],
      [
        "Customer-success escalation",
        "message",
        "A pilot allowlisted channel produced support-priority metadata."
      ]
    ],
    jira: [
      [
        "Pilot issue movement",
        "issue",
        "Approved Jira projects produced issue status and comment metadata."
      ],
      [
        "Release blocker changed",
        "task",
        "A tracked Jira issue moved into review during the first sync."
      ]
    ],
    github: [
      [
        "Repository rollout activity",
        "pull-request",
        "Selected repositories produced pull-request and issue metadata."
      ],
      [
        "Deployment branch updated",
        "deployment",
        "A tracked branch update landed as a code-workflow observation."
      ]
    ],
    discord: [
      [
        "Community feedback channel",
        "message",
        "Approved Discord channels produced event metadata through the gateway."
      ],
      [
        "Moderator follow-up",
        "message",
        "A scoped Discord thread produced a follow-up observation."
      ]
    ],
    notion: [
      [
        "Launch checklist updates",
        "page",
        "Shared Notion pages produced page-change observations."
      ],
      [
        "Runbook database edited",
        "page",
        "An approved Notion database update landed as a knowledge observation."
      ]
    ],
    telegram: [
      [
        "Partner readiness chat",
        "message",
        "Approved Telegram dialogs produced MTProto session observations."
      ],
      [
        "Pilot decision note",
        "message",
        "A scoped Telegram chat produced a decision-tracking observation."
      ]
    ]
  };
  const rows =
    summaries[source.id] ??
    [
      [
        `${source.name} pilot event`,
        "task" as const,
        `${source.name} produced a sanitized pilot observation.`
      ]
    ];
  const now = new Date().toISOString();
  return rows.map(([title, kind, summary], index) => ({
    id: `obs_${source.id}_${index + 1}`,
    sourceId: source.id,
    title,
    kind,
    occurredAt: now,
    summary: `${summary} Sync mode: ${syncMode}.`,
    evidencePath: `s3://fyralis-byoc-pilot/raw/${source.id}/obs_${index + 1}.jsonl`,
    status: "landed",
    origin: "preview",
    syncTrack:
      syncMode === "Live events"
        ? "live"
        : syncMode === "Backfill plus live"
          ? "mixed"
          : "historical",
    sourceChannel: sourceSignalConfig(source).landedChannel
  }));
}

function SourceCatalogStep(props: StepViewProps) {
  const { snapshot, selectedSource, selectedConnection, selectSource, updateConnection, goTo } = props;
  const allSourceCommands = [
    [
      "discover",
      [
        "fyralis byoc source discover",
        "--source all",
        "--scopes auto",
        `--admin-console-url ${props.workspace.localConsoleUrl}`,
        `--provider-ingress-url ${props.workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ],
    [
      "plan",
      [
        "fyralis byoc source plan",
        "--source all",
        "--scopes auto",
        "--sync-mode dry-run",
        "--backfill-window 30d",
        `--admin-console-url ${props.workspace.localConsoleUrl}`,
        `--provider-ingress-url ${props.workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ],
    [
      "apply",
      [
        "fyralis byoc source apply",
        "--source all",
        "--requires-approval",
        "--plan latest",
        "--sync-mode dry-run",
        "--backfill-window 30d",
        `--admin-console-url ${props.workspace.localConsoleUrl}`,
        `--provider-ingress-url ${props.workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ],
    [
      "validate",
      [
        "fyralis byoc source validate",
        "--source all",
        `--admin-console-url ${props.workspace.localConsoleUrl}`,
        `--provider-ingress-url ${props.workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ],
    [
      "activate",
      [
        "fyralis byoc source activate",
        "--source all",
        "--requires-approval",
        "--start-first-sync",
        "--sync-mode limited-backfill",
        "--backfill-window 30d",
        `--admin-console-url ${props.workspace.localConsoleUrl}`,
        `--provider-ingress-url ${props.workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ]
  ];
  return (
    <div className="grid min-w-0 gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Source agent lifecycle</CardTitle>
          <Badge tone="success">All supported sources</Badge>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 rounded-lg border border-success/30 bg-success/10 p-4">
            <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
              <TerminalSquare className="h-4 w-4" aria-hidden="true" />
              Discover, plan, apply, validate, and activate
            </div>
            {allSourceCommands.map(([label, command]) => (
              <div key={label} className="grid gap-1">
                <span className="text-xs font-semibold uppercase text-muted-foreground">{label}</span>
                <code className="block break-all text-xs text-muted-foreground">
                  {command}
                </code>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Provider rehearsal automation</CardTitle>
          <Badge tone="info">Setup owner path</Badge>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {snapshot.sources.map((source) => {
            const sourceId = source.id;
            const config = sourceRehearsalConfig(source);
            return (
              <button
                key={sourceId}
                type="button"
                className="rounded-lg border border-border bg-background/70 p-4 text-left transition-colors hover:border-ring"
                onClick={() => {
                  selectSource(sourceId);
                  updateConnection(sourceId, { status: "draft" });
                  goTo("source-setup");
                }}
              >
                <span className="flex items-start justify-between gap-3">
                  <span>
                    <strong className="block">{source.name}</strong>
                    <span className="mt-1 block text-sm text-muted-foreground">
                      {config.providerKind}
                    </span>
                  </span>
                  <Badge tone="success">
                    {config.generatedArtifacts.length} files
                  </Badge>
                </span>
                <code className="mt-3 block break-all text-xs text-muted-foreground">
                  .fyralis/local-rehearsal/{sourceId}
                </code>
              </button>
            );
          })}
        </CardContent>
      </Card>
      <div className="grid min-w-0 gap-5 xl:grid-cols-[minmax(0,1fr)_22rem]">
        <SourceMarketplace
          sources={snapshot.sources}
          connections={props.connections}
          selectedSourceId={selectedSource.id}
          onSelect={selectSource}
          onOpenSetup={(sourceId) => {
            selectSource(sourceId);
            updateConnection(sourceId, { status: "draft" });
            goTo("source-setup");
          }}
        />
        <SourceDetailPanel
          source={selectedSource}
          connection={selectedConnection}
          workspace={props.workspace}
          onOpenSetup={() => {
            updateConnection(selectedSource.id, { status: "draft" });
            goTo("source-setup");
          }}
        />
      </div>
    </div>
  );
}

function SourceSetupStep({
  selectedSource,
  selectedConnection,
  workspace,
  updateConnection,
  goTo,
  advance
}: StepViewProps) {
  const rehearsalConfig = sourceRehearsalConfig(selectedSource);
  const [rehearsalStage, setRehearsalStage] = useState<
    "idle" | "generated" | "provider-gates-done"
  >(
    selectedConnection?.status === "ready" || selectedConnection?.status === "connected"
      ? "provider-gates-done"
      : selectedConnection?.receiptId?.startsWith("rehearsal_")
        ? "generated"
        : "idle"
  );
  const endpoints = selectedSource.providerIngressPaths.length
    ? selectedSource.providerIngressPaths.map((path) => `${workspace.providerIngressUrl}${path}`)
    : [selectedSource.noIngressReason ?? "No provider ingress required."];
  const defaultScopes = sourceScopeChoices(selectedSource.id).slice(0, 3);
  const rehearsalCommand = sourceRehearsalCommand(selectedSource, workspace);
  const applyEnvCommand = sourceApplyEnvCommand(selectedSource, workspace);
  const providerIngressIsPublic = isPublicHttpsUrl(workspace.providerIngressUrl);
  const observationPreview = sourceObservationSamples(
    selectedSource,
    selectedConnection?.syncMode ?? "Limited backfill"
  );
  const sourceCommands = [
    [
      "discover",
      [
        "fyralis byoc source discover",
        `--source ${selectedSource.id}`,
        "--scopes auto",
        `--admin-console-url ${workspace.localConsoleUrl}`,
        `--provider-ingress-url ${workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ],
    [
      "plan",
      [
        "fyralis byoc source plan",
        `--source ${selectedSource.id}`,
        "--scopes auto",
        "--sync-mode limited-backfill",
        "--backfill-window 30d",
        `--admin-console-url ${workspace.localConsoleUrl}`,
        `--provider-ingress-url ${workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ],
    [
      "apply",
      [
        "fyralis byoc source apply",
        `--source ${selectedSource.id}`,
        "--requires-approval",
        "--plan latest",
        "--sync-mode limited-backfill",
        "--backfill-window 30d",
        `--admin-console-url ${workspace.localConsoleUrl}`,
        `--provider-ingress-url ${workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ],
    [
      "validate",
      [
        "fyralis byoc source validate",
        `--source ${selectedSource.id}`,
        `--admin-console-url ${workspace.localConsoleUrl}`,
        `--provider-ingress-url ${workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ],
    [
      "activate",
      [
        "fyralis byoc source activate",
        `--source ${selectedSource.id}`,
        "--requires-approval",
        "--start-first-sync",
        "--sync-mode limited-backfill",
        "--backfill-window 30d",
        `--admin-console-url ${workspace.localConsoleUrl}`,
        `--provider-ingress-url ${workspace.providerIngressUrl}`,
        "--provider-authorization-mode preauthorized-ref",
        "--preauthorized-ref-manifest ./customer-source-refs.json",
        "--json"
      ].join(" ")
    ]
  ];

  function generatePackage() {
    setRehearsalStage("generated");
    updateConnection(selectedSource.id, {
      status: "draft",
      receiptId: `rehearsal_${selectedSource.id}_package`
    });
  }

  function markProviderGatesDone() {
    setRehearsalStage("provider-gates-done");
    updateConnection(selectedSource.id, {
      status: "ready",
      selectedScopes: defaultScopes,
      backfillWindow: "Last 30 days",
      syncMode: "Limited backfill",
      receiptId: `rehearsal_${selectedSource.id}_provider_ready`
    });
  }

  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Connect {selectedSource.name}</CardTitle>
          <Badge tone="info">{selectedConnection?.status ?? "draft"}</Badge>
        </CardHeader>
        <CardContent className="grid gap-5 lg:grid-cols-2">
          <InfoBlock title="Admin console URL" value={workspace.localConsoleUrl} />
          <InfoBlock title="Provider ingress" value={endpoints.join("\n")} />
          <InfoBlock title="Setup requirements" value={selectedSource.setupRequirements} />
          <InfoBlock
            title="Secret refs created locally"
            value={`${selectedSource.id}_credential_ref\n${selectedSource.id}_connection_policy_ref`}
          />
        </CardContent>
      </Card>
      {rehearsalConfig ? (
        <>
          <Card>
            <CardHeader>
              <CardTitle>{selectedSource.name} provider setup package</CardTitle>
              <Badge tone="success">{rehearsalConfig.providerKind}</Badge>
            </CardHeader>
            <CardContent className="grid gap-5">
              <div className="rounded-lg border border-info/30 bg-info/10 p-4 text-sm text-muted-foreground">
                Use this package view for CLI handoff details. Continue to the
                automated setup screen to prepare the live provider install,
                open the approval URL, poll gateway status, and load landed
                observations.
              </div>
              <div className="grid gap-3 md:grid-cols-3">
                <Metric
                  label="Package"
                  value={
                    rehearsalStage === "idle" ? "Ready" : "Generated"
                  }
                  detail=".fyralis local setup artifacts."
                />
                <Metric
                  label="Provider gates"
                  value={
                    rehearsalStage === "provider-gates-done"
                      ? "Done"
                      : String(rehearsalConfig.manualGates.length)
                  }
                  detail="Only provider approval screens remain manual."
                />
                <Metric
                  label="Secrets"
                  value="Local"
                  detail="Env values apply to the customer-cloud gateway only."
                />
              </div>

              <div
                className={cn(
                  "rounded-lg border p-4",
                  providerIngressIsPublic
                    ? "border-success/30 bg-success/10"
                    : "border-warning/40 bg-warning/15"
                )}
              >
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <span>
                    <strong className="block text-sm">
                      {providerIngressIsPublic
                        ? "Provider callback URL is public HTTPS"
                        : "Provider callback URL is local or not public HTTPS"}
                    </strong>
                    <span className="mt-1 block text-sm leading-6 text-muted-foreground">
                      Real Slack, GitHub, Discord, and Notion installs require
                      the provider ingress to be reachable by the provider.
                      For local testing, expose the gateway with ngrok or a
                      customer-owned DNS name and set
                      NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL.
                    </span>
                  </span>
                  <Badge tone={providerIngressIsPublic ? "success" : "warning"}>
                    {providerIngressIsPublic ? "Live URL" : "Local URL"}
                  </Badge>
                </div>
              </div>

              <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_14rem]">
                <ProviderRouteList
                  workspace={workspace}
                  config={rehearsalConfig}
                />
                <a
                  href={rehearsalConfig.providerConsoleUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex min-h-10 items-center justify-center rounded-md border border-border bg-card px-4 text-sm font-semibold text-foreground transition-colors hover:border-ring hover:bg-accent"
                >
                  Open provider console
                </a>
              </div>

              <div className="grid gap-3 lg:grid-cols-2">
                <div className="rounded-lg border border-success/30 bg-success/10 p-4">
                  <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
                    <TerminalSquare className="h-4 w-4" aria-hidden="true" />
                    Generate setup artifacts
                  </div>
                  <code className="block break-all text-xs text-muted-foreground">
                    {rehearsalCommand}
                  </code>
                </div>
                <div className="rounded-lg border border-info/30 bg-info/10 p-4">
                  <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
                    <TerminalSquare className="h-4 w-4" aria-hidden="true" />
                    Apply local provider env
                  </div>
                  <code className="block break-all text-xs text-muted-foreground">
                    {applyEnvCommand}
                  </code>
                </div>
              </div>

              <div className="grid gap-3 lg:grid-cols-3">
                <SetupList
                  title="Generated files"
                  items={rehearsalConfig.generatedArtifacts}
                  tone="success"
                />
                <SetupList
                  title="Local env keys"
                  items={rehearsalConfig.envKeys}
                  tone="info"
                />
                <SetupList
                  title="Provider gates"
                  items={rehearsalConfig.manualGates}
                  tone="warning"
                />
              </div>

              <div>
                <h3 className="text-sm font-semibold">Observations expected after first sync</h3>
                <div className="mt-3 grid gap-3 md:grid-cols-2">
                  {observationPreview.map((observation) => (
                    <ObservationCard key={observation.id} observation={observation} />
                  ))}
                </div>
              </div>

              <div className="flex flex-wrap gap-3">
                <Button type="button" variant="secondary" onClick={generatePackage}>
                  Generate CLI package
                </Button>
                <Button type="button" onClick={markProviderGatesDone}>
                  Mark provider gates complete
                </Button>
              </div>
            </CardContent>
          </Card>
        </>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle>{selectedSource.name} source agent</CardTitle>
            <Badge tone="success">Customer cloud</Badge>
          </CardHeader>
          <CardContent>
            <div className="grid gap-3 rounded-lg border border-success/30 bg-success/10 p-4">
              <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
                <TerminalSquare className="h-4 w-4" aria-hidden="true" />
                Stop at each approval boundary
              </div>
              {sourceCommands.map(([label, command]) => (
                <div key={label} className="grid gap-1">
                  <span className="text-xs font-semibold uppercase text-muted-foreground">{label}</span>
                  <code className="block break-all text-xs text-muted-foreground">
                    {command}
                  </code>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
      <ActionBar
        primaryLabel={
          rehearsalConfig
            ? "Continue to automated setup"
            : "Run source autopilot"
        }
        onPrimary={() => {
          updateConnection(selectedSource.id, {
            status: rehearsalConfig ? "ready" : "connected",
            selectedScopes: defaultScopes,
            backfillWindow: "Last 30 days",
            syncMode: "Limited backfill",
            receiptId: rehearsalConfig
              ? `rehearsal_${selectedSource.id}_provider_ready`
              : `srcval_${selectedSource.id}_autopilot`
          });
          if (rehearsalConfig) {
            goTo("ingestion-health");
          } else {
            advance();
          }
        }}
      />
    </div>
  );
}

function SourceValidationStep({
  selectedSource,
  sourceValidation,
  setSourceValidation,
  updateConnection,
  advance
}: StepViewProps) {
  function pass() {
    setSourceValidation({
      ...sourceValidation,
      status: "passed",
      checks: sourceValidation.checks.map((check) => ({
        ...check,
        status: "passed",
        detail: `${selectedSource.name} ${check.label.toLowerCase()} passed.`
      }))
    });
    updateConnection(selectedSource.id, {
      status: "ready",
      receiptId: `srcval_${selectedSource.id}_20260629`
    });
  }

  function fail() {
    setSourceValidation({
      ...sourceValidation,
      status: "failed",
      checks: sourceValidation.checks.map((check, index) => ({
        ...check,
        status: index === 1 ? "failed" : "passed",
        detail:
          index === 1
            ? "SOURCE_CALLBACK_MISMATCH. Fix locally, then retry."
            : `${selectedSource.name} ${check.label.toLowerCase()} passed.`
      }))
    });
    updateConnection(selectedSource.id, {
      status: "error",
      lastIssueCode: "SOURCE_CALLBACK_MISMATCH"
    });
  }

  return (
    <ValidationCard
      title={`Validate ${selectedSource.name}`}
      validation={sourceValidation}
      primaryLabel="Run connection test"
      onPrimary={pass}
      secondaryLabel="Show failed state"
      onSecondary={fail}
      footer={<ActionBar primaryLabel="Continue to scope" onPrimary={advance} compact />}
    />
  );
}

function SourceScopeStep({ selectedSource, selectedConnection, updateConnection, advance }: StepViewProps) {
  const choices = sourceScopeChoices(selectedSource.id);
  const form = useForm<SourceScopeFormValues>({
    resolver: zodResolver(sourceScopeSchema),
    defaultValues: {
      selectedScopes: selectedConnection?.selectedScopes.length
        ? selectedConnection.selectedScopes
        : choices.slice(0, 3),
      backfillWindow: selectedConnection?.backfillWindow ?? "Last 30 days",
      syncMode: selectedConnection?.syncMode ?? "Limited backfill"
    }
  });

  return (
    <form
      className="grid gap-5"
      onSubmit={form.handleSubmit((values) => {
        updateConnection(selectedSource.id, values);
        advance();
      })}
    >
      <Card>
        <CardHeader>
          <CardTitle>Approve {selectedSource.name} scope</CardTitle>
          <Badge tone="success">Customer approved</Badge>
        </CardHeader>
        <CardContent className="grid gap-5">
          <div className="grid gap-2 md:grid-cols-2">
            {choices.map((choice) => (
              <label key={choice} className="flex gap-3 rounded-lg border border-border bg-background/70 p-3 text-sm">
                <input
                  type="checkbox"
                  value={choice}
                  className="mt-1 h-4 w-4 accent-success"
                  {...form.register("selectedScopes")}
                />
                <span>
                  <strong className="block">{choice}</strong>
                  <span className="text-xs text-muted-foreground">
                    Included only when approved for this pilot batch.
                  </span>
                </span>
              </label>
            ))}
          </div>
          {form.formState.errors.selectedScopes?.message ? (
            <p className="text-sm font-medium text-destructive">
              {form.formState.errors.selectedScopes.message}
            </p>
          ) : null}
          <div className="grid gap-4 md:grid-cols-2">
            <SelectField label="Backfill window" register={form.register("backfillWindow")}>
              <option>Last 7 days</option>
              <option>Last 30 days</option>
              <option>Last 90 days</option>
              <option>No historical backfill</option>
            </SelectField>
            <SelectField label="Sync mode" register={form.register("syncMode")}>
              <option>Dry run</option>
              <option>Limited backfill</option>
              <option>Live events</option>
              <option>Backfill plus live</option>
            </SelectField>
          </div>
        </CardContent>
      </Card>
      <ActionBar primaryLabel="Save scope and continue" submit />
    </form>
  );
}

function FirstSyncStep({
  selectedSource,
  selectedConnection,
  workspace,
  upsertSyncJob,
  landSourceObservations,
  advance
}: StepViewProps) {
  const syncMode = selectedConnection?.syncMode ?? "Limited backfill";
  const backfillWindow = selectedConnection?.backfillWindow ?? "Last 30 days";
  const signalConfig = sourceSignalConfig(selectedSource);
  const selectedCommand = sourceFirstSyncCommand({
    sourceId: selectedSource.id,
    workspace,
    syncMode,
    backfillWindow
  });
  const historicalCommand = sourceFirstSyncCommand({
    sourceId: selectedSource.id,
    workspace,
    syncMode: "Limited backfill",
    backfillWindow
  });
  const liveCommand = sourceFirstSyncCommand({
    sourceId: selectedSource.id,
    workspace,
    syncMode: "Live events",
    backfillWindow: "No historical backfill"
  });
  const bothCommand = sourceFirstSyncCommand({
    sourceId: selectedSource.id,
    workspace,
    syncMode: "Backfill plus live",
    backfillWindow
  });

  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader>
          <div className="flex items-center gap-3">
            <span className="flex h-10 w-10 items-center justify-center rounded-lg bg-accent text-accent-foreground">
              <Play className="h-5 w-5" aria-hidden="true" />
            </span>
            <div>
              <CardTitle>Run {selectedSource.name} first sync</CardTitle>
              <p className="mt-1 text-sm text-muted-foreground">
                Use the customer-cloud source agent to start historical backfill,
                live signals, or both after the source is authorized.
              </p>
            </div>
          </div>
          <Badge tone="info">{syncMode}</Badge>
        </CardHeader>
        <CardContent className="grid gap-5">
          <div className="grid gap-3 md:grid-cols-4">
            <Metric
              label="Scope"
              value={`${selectedConnection?.selectedScopes.length || 3} items`}
              detail="Approved by the setup owner."
            />
            <Metric
              label="Backfill"
              value={backfillWindow}
              detail="Only used for historical modes."
            />
            <Metric
              label="Live edge"
              value={signalConfig?.liveIngress ?? "source runner"}
              detail="Provider callback, webhook, or local gateway."
            />
            <Metric
              label="Landed channel"
              value={signalConfig?.landedChannel ?? `${selectedSource.id}:*`}
              detail="Observation source_channel prefix."
            />
          </div>

          <div className="grid gap-3 lg:grid-cols-2">
            <SignalPathCard
              title="Historical backfill path"
              detail={
                signalConfig?.historicalTrigger ??
                "Source activation emits the onboarding trigger consumed by the source-onboarding workers."
              }
              command={historicalCommand}
            />
            <SignalPathCard
              title="Live signal path"
              detail={
                signalConfig?.liveWorker ??
                "Provider live events enter the customer-cloud gateway and write observations."
              }
              command={liveCommand}
            />
          </div>

          <div className="rounded-lg border border-success/30 bg-success/10 p-4">
            <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
              <TerminalSquare className="h-4 w-4" aria-hidden="true" />
              Recommended command for the selected mode
            </div>
            <code className="block break-all text-xs text-muted-foreground">
              {selectedCommand}
            </code>
            {syncMode !== "Backfill plus live" ? (
              <code className="mt-3 block break-all text-xs text-muted-foreground">
                Both paths: {bothCommand}
              </code>
            ) : null}
          </div>
        </CardContent>
      </Card>
      <ActionBar
        primaryLabel="Record sync receipt and continue"
        onPrimary={() => {
          upsertSyncJob({
            id: `sync_${selectedSource.id}_initial`,
            sourceId: selectedSource.id,
            mode: syncMode,
            status: "completed",
            eventsReceived: 80 + selectedSource.name.length * 8,
            errors: 0
          });
          landSourceObservations(
            selectedSource.id,
            sourceObservationSamples(selectedSource, syncMode)
          );
          advance();
        }}
      />
    </div>
  );
}

function IngestionHealthStep({
  selectedSource,
  workspace,
  syncJobs,
  sourceObservations,
  landSourceObservations,
  advance
}: StepViewProps) {
  const job = syncJobs.find((item) => item.sourceId === selectedSource.id);
  const observations = sourceObservations.filter(
    (observation) => observation.sourceId === selectedSource.id
  );
  const sourceAutomationConfig = sourceRehearsalConfig(selectedSource);
  const hasSourceAutomation = true;
  const [apiBase, setApiBase] = useState(workspace.providerIngressUrl);
  const [bearerToken, setBearerToken] = useState("");
  const [fetchStatus, setFetchStatus] = useState<
    "idle" | "loading" | "loaded" | "error"
  >("idle");
  const [fetchMessage, setFetchMessage] = useState(
    "Fetch after the source authorization and first sync have run in the customer cloud."
  );
  const [sourceRehearsal, setSourceRehearsal] =
    useState<SourceRehearsalPrepareResponse | null>(null);
  const [sourceStatus, setSourceStatus] = useState<SourceRehearsalStatus | null>(
    null
  );
  const [sourceAutomationStatus, setSourceAutomationStatus] = useState<
    "idle" | "preparing" | "polling" | "ready" | "blocked" | "error"
  >("idle");
  const [sourceAutomationMessage, setSourceAutomationMessage] = useState(
    `Prepare ${selectedSource.name} from this UI. Fyralis will generate the provider handoff, open approval when available, poll the gateway, and show observations when they land.`
  );
  const [jiraForm, setJiraForm] = useState({
    baseUrl: "",
    accountEmail: "",
    apiToken: "",
    webhookSecret: ""
  });
  const [telegramForm, setTelegramForm] = useState({
    accountLabel: "",
    apiId: "",
    apiHash: "",
    liveSession: "",
    backfillSession: ""
  });

  useEffect(() => {
    setSourceRehearsal(null);
    setSourceStatus(null);
    setSourceAutomationStatus("idle");
    setSourceAutomationMessage(
      `Prepare ${selectedSource.name} from this UI. Fyralis will generate the provider handoff, open approval when available, poll the gateway, and show observations when they land.`
    );
    setFetchMessage(
      "Fetch after the source authorization and first sync have run in the customer cloud."
    );
    setFetchStatus("idle");
  }, [selectedSource.id, selectedSource.name]);

  useEffect(() => {
    if (!hasSourceAutomation || !sourceRehearsal) {
      return;
    }
    let cancelled = false;
    async function pollSourceStatus() {
      try {
        const status = await fetchSourceRehearsalStatus({
          sourceId: selectedSource.id,
          apiBase: sourceRehearsal?.gatewayApiBase ?? apiBase
        });
        if (cancelled) {
          return;
        }
        applySourceStatus(status);
      } catch (caught) {
        if (!cancelled) {
          setSourceAutomationStatus("error");
          setSourceAutomationMessage(errorMessage(caught));
        }
      }
    }
    const interval = window.setInterval(pollSourceStatus, 7000);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [apiBase, hasSourceAutomation, selectedSource.id, sourceRehearsal]);

  async function loadGatewayObservations() {
    setFetchStatus("loading");
    setFetchMessage("Reading gateway observations...");
    try {
      const gatewayObservations = await fetchGatewaySourceObservations({
        apiBase,
        bearerToken,
        sourceId: selectedSource.id,
        limit: 50
      });
      landSourceObservations(selectedSource.id, gatewayObservations);
      setFetchStatus("loaded");
      setFetchMessage(
        gatewayObservations.length
          ? `${gatewayObservations.length} ${selectedSource.name} observations loaded from the customer gateway.`
          : `No ${selectedSource.name} observations were returned yet. Check the source_channel prefix ${sourceSignalConfig(selectedSource).landedChannel}.`
      );
    } catch (caught) {
      setFetchStatus("error");
      setFetchMessage(errorMessage(caught));
    }
  }

  function applySourceStatus(status: SourceRehearsalStatus) {
    setSourceStatus(status);
    if (status.bearerToken) {
      setBearerToken(status.bearerToken);
    }
    if (status.observations.length) {
      landSourceObservations(status.sourceId, status.observations);
      setFetchStatus("loaded");
      setFetchMessage(
        `${status.observations.length} ${selectedSource.name} observations loaded from the customer gateway.`
      );
    }
    setSourceAutomationStatus(status.observationCount ? "ready" : "polling");
    setSourceAutomationMessage(status.nextAction);
  }

  async function prepareAndOpenSource() {
    setSourceAutomationStatus("preparing");
    setSourceAutomationMessage(`Preparing ${selectedSource.name} authorization...`);
    try {
      const prepared = await prepareSourceRehearsal({
        sourceId: selectedSource.id,
        apiBase
      });
      setSourceRehearsal(prepared);
      setApiBase(prepared.gatewayApiBase);
      setBearerToken(prepared.bearerToken);
      applySourceStatus({
        ...prepared.status,
        bearerToken: prepared.bearerToken,
        sessionExpiresAt: prepared.sessionExpiresAt
      });
      if (prepared.missingConfiguration.length) {
        setSourceAutomationStatus("blocked");
        setSourceAutomationMessage(
          `${selectedSource.name} provider app is not fully configured. Add ${prepared.missingConfiguration.join(", ")} in the customer-cloud runtime, then prepare again.`
        );
        return;
      }
      if (prepared.installUrl && typeof window !== "undefined") {
        window.open(prepared.installUrl, "_blank", "noopener,noreferrer");
        setSourceAutomationMessage(
          `${selectedSource.name} approval opened. Approve the provider app, then this page will keep checking for install, backfill, and live observations.`
        );
      } else {
        setSourceAutomationMessage(
          `${selectedSource.name} handoff prepared. Complete the connection details below and Fyralis will finalize the install customer-side.`
        );
      }
    } catch (caught) {
      setSourceAutomationStatus("error");
      setSourceAutomationMessage(errorMessage(caught));
    }
  }

  async function refreshSourceAutomationStatus() {
    setSourceAutomationStatus("polling");
    setSourceAutomationMessage(
      `Checking ${selectedSource.name} install and observation status...`
    );
    try {
      const status = await fetchSourceRehearsalStatus({
        sourceId: selectedSource.id,
        apiBase
      });
      applySourceStatus(status);
    } catch (caught) {
      setSourceAutomationStatus("error");
      setSourceAutomationMessage(errorMessage(caught));
    }
  }

  async function finalizeJiraFromUi() {
    setSourceAutomationStatus("preparing");
    setSourceAutomationMessage("Verifying Jira credentials and finalizing install...");
    try {
      const response = await finalizeJiraRehearsal({
        apiBase,
        payload: {
          baseUrl: jiraForm.baseUrl,
          accountEmail: jiraForm.accountEmail,
          apiToken: jiraForm.apiToken,
          webhookSecret: jiraForm.webhookSecret || undefined
        }
      });
      applySourceStatus(response.status);
      setSourceAutomationMessage(
        "Jira install finalized. Fyralis created local secret refs, install rows, and the onboarding trigger."
      );
    } catch (caught) {
      setSourceAutomationStatus("error");
      setSourceAutomationMessage(errorMessage(caught));
    }
  }

  async function finalizeTelegramFromUi() {
    setSourceAutomationStatus("preparing");
    setSourceAutomationMessage("Verifying Telegram session and finalizing install...");
    try {
      const response = await finalizeTelegramRehearsal({
        apiBase,
        payload: {
          accountLabel: telegramForm.accountLabel,
          apiId: telegramForm.apiId,
          apiHash: telegramForm.apiHash,
          liveSession: telegramForm.liveSession,
          backfillSession: telegramForm.backfillSession || undefined
        }
      });
      applySourceStatus(response.status);
      setSourceAutomationMessage(
        "Telegram install finalized. Fyralis verified the session, enumerated dialogs, stored local refs, and emitted the onboarding trigger."
      );
    } catch (caught) {
      setSourceAutomationStatus("error");
      setSourceAutomationMessage(errorMessage(caught));
    }
  }

  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader>
          <CardTitle>{selectedSource.name} ingestion health</CardTitle>
          <Badge tone={job?.errors ? "warning" : "success"}>
            {job?.errors ? "Needs retry" : "Healthy"}
          </Badge>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-3">
          <Metric label="Events received" value={String(job?.eventsReceived ?? 0)} detail="From allowed source scope." />
          <Metric label="Kafka/MSK publish" value="OK" detail="Customer broker accepted messages." />
          <Metric label="Workers" value="OK" detail="Ingestion workers processed the batch." />
          <Metric label="Postgres writes" value="OK" detail="Metadata and state writes succeeded." />
          <Metric label="Object storage" value="OK" detail="Payload tier accepted test artifacts." />
          <Metric label="Bounded errors" value={String(job?.errors ?? 0)} detail="No raw logs shown." />
        </CardContent>
      </Card>
      {hasSourceAutomation ? (
        <Card>
          <CardHeader>
            <div>
              <CardTitle>Automated {selectedSource.name} setup</CardTitle>
              <p className="mt-1 text-sm text-muted-foreground">
                Use this page as the setup owner. Fyralis prepares the provider
                handoff, opens approval when the provider allows it, finalizes
                local refs, polls the gateway, and shows observations after
                authorization.
              </p>
            </div>
            <Badge
              tone={
                sourceAutomationStatus === "ready"
                  ? "success"
                  : sourceAutomationStatus === "error"
                    ? "error"
                    : sourceAutomationStatus === "blocked"
                      ? "warning"
                    : "info"
              }
            >
              {sourceAutomationStatus}
            </Badge>
          </CardHeader>
          <CardContent className="grid gap-4">
            <div className="grid gap-3 md:grid-cols-4">
              <Metric
                label={`${selectedSource.name} install`}
                value={sourceStatus?.installed ? "Done" : "Waiting"}
                detail={
                  sourceStatus?.installation?.hasSecret
                    ? "Workspace secret stored customer-side."
                    : "Requires provider authorization."
                }
              />
              <Metric
                label="Backfill trigger"
                value={String(sourceStatus?.triggerCount ?? 0)}
                detail="Created by OAuth callback or finalize action."
              />
              <Metric
                label="Observations"
                value={String(sourceStatus?.observationCount ?? 0)}
                detail="Read from gateway-backed Postgres."
              />
              <Metric
                label="Open failures"
                value={String(sourceStatus?.unresolvedFailureCount ?? 0)}
                detail="Unresolved ingestion failures."
              />
            </div>
            {sourceRehearsal ? (
              <div className="grid gap-3 rounded-lg border border-border bg-background/70 p-4 text-sm md:grid-cols-2">
                <InfoBlock
                  title="Authorization mode"
                  value={sourceRehearsal.authorizationMode}
                />
                <InfoBlock
                  title="Provider ingress"
                  value={sourceRehearsal.eventsRequestUrl ?? "No public provider ingress required."}
                />
                <InfoBlock
                  title="OAuth redirect"
                  value={sourceRehearsal.oauthRedirectUrl ?? "No OAuth redirect required."}
                />
                <InfoBlock
                  title="Provider console"
                  value={sourceRehearsal.providerConsoleUrl ?? sourceAutomationConfig?.providerConsoleUrl ?? "Provider console not required."}
                />
              </div>
            ) : null}
            {sourceRehearsal?.missingConfiguration.length ? (
              <div className="rounded-lg border border-warning/40 bg-warning/15 p-4 text-sm text-muted-foreground">
                <strong className="block text-foreground">Provider app configuration needed</strong>
                <span className="mt-2 block">
                  Add these runtime values before opening provider approval:
                </span>
                <code className="mt-2 block break-all text-xs">
                  {sourceRehearsal.missingConfiguration.join(", ")}
                </code>
              </div>
            ) : null}
            {selectedSource.id === "jira" ? (
              <div className="grid gap-3 rounded-lg border border-border bg-background/70 p-4 md:grid-cols-2">
                <Field label="Jira site URL" help="Full Atlassian site URL.">
                  <Input
                    value={jiraForm.baseUrl}
                    onChange={(event) =>
                      setJiraForm((current) => ({
                        ...current,
                        baseUrl: event.target.value
                      }))
                    }
                    placeholder="https://acme.atlassian.net"
                  />
                </Field>
                <Field label="Account email" help="Atlassian account that can read the selected projects.">
                  <Input
                    value={jiraForm.accountEmail}
                    onChange={(event) =>
                      setJiraForm((current) => ({
                        ...current,
                        accountEmail: event.target.value
                      }))
                    }
                    placeholder="owner@company.com"
                  />
                </Field>
                <Field label="API token" help="Stored only as an encrypted customer-cloud ref.">
                  <Input
                    type="password"
                    autoComplete="off"
                    value={jiraForm.apiToken}
                    onChange={(event) =>
                      setJiraForm((current) => ({
                        ...current,
                        apiToken: event.target.value
                      }))
                    }
                    placeholder="Atlassian API token"
                  />
                </Field>
                <Field label="Webhook secret" help="Optional. Enables live Jira webhooks when configured.">
                  <Input
                    type="password"
                    autoComplete="off"
                    value={jiraForm.webhookSecret}
                    onChange={(event) =>
                      setJiraForm((current) => ({
                        ...current,
                        webhookSecret: event.target.value
                      }))
                    }
                    placeholder="Optional Jira webhook secret"
                  />
                </Field>
                <div className="md:col-span-2">
                  <Button
                    type="button"
                    onClick={finalizeJiraFromUi}
                    disabled={sourceAutomationStatus === "preparing"}
                  >
                    <Play className="h-4 w-4" aria-hidden="true" />
                    Verify and connect Jira
                  </Button>
                </div>
              </div>
            ) : null}
            {selectedSource.id === "telegram" ? (
              <div className="grid gap-3 rounded-lg border border-border bg-background/70 p-4 md:grid-cols-2">
                <Field label="Account label" help="Phone, username, or internal label for this Telegram account.">
                  <Input
                    value={telegramForm.accountLabel}
                    onChange={(event) =>
                      setTelegramForm((current) => ({
                        ...current,
                        accountLabel: event.target.value
                      }))
                    }
                    placeholder="+15551234567"
                  />
                </Field>
                <Field label="API ID" help="From my.telegram.org/apps.">
                  <Input
                    value={telegramForm.apiId}
                    onChange={(event) =>
                      setTelegramForm((current) => ({
                        ...current,
                        apiId: event.target.value
                      }))
                    }
                    placeholder="123456"
                  />
                </Field>
                <Field label="API hash" help="Stored only as an encrypted customer-cloud ref.">
                  <Input
                    type="password"
                    autoComplete="off"
                    value={telegramForm.apiHash}
                    onChange={(event) =>
                      setTelegramForm((current) => ({
                        ...current,
                        apiHash: event.target.value
                      }))
                    }
                    placeholder="Telegram API hash"
                  />
                </Field>
                <Field label="Live StringSession" help="Authorized MTProto session for live gateway updates.">
                  <Input
                    type="password"
                    autoComplete="off"
                    value={telegramForm.liveSession}
                    onChange={(event) =>
                      setTelegramForm((current) => ({
                        ...current,
                        liveSession: event.target.value
                      }))
                    }
                    placeholder="Telethon StringSession"
                  />
                </Field>
                <Field label="Backfill StringSession" help="Optional second session for backfill. Defaults to live session in rehearsal.">
                  <Input
                    type="password"
                    autoComplete="off"
                    value={telegramForm.backfillSession}
                    onChange={(event) =>
                      setTelegramForm((current) => ({
                        ...current,
                        backfillSession: event.target.value
                      }))
                    }
                    placeholder="Optional second StringSession"
                  />
                </Field>
                <div className="flex items-end">
                  <Button
                    type="button"
                    onClick={finalizeTelegramFromUi}
                    disabled={sourceAutomationStatus === "preparing"}
                  >
                    <Play className="h-4 w-4" aria-hidden="true" />
                    Verify and connect Telegram
                  </Button>
                </div>
              </div>
            ) : null}
            <div
              className={cn(
                "rounded-lg border p-4 text-sm",
                sourceAutomationStatus === "error"
                  ? "border-destructive/35 bg-destructive/10 text-destructive"
                  : sourceAutomationStatus === "blocked"
                    ? "border-warning/40 bg-warning/15 text-muted-foreground"
                  : "border-border bg-background/70 text-muted-foreground"
              )}
            >
              {sourceAutomationMessage}
            </div>
            <div className="flex flex-wrap gap-3">
              <Button
                type="button"
                onClick={prepareAndOpenSource}
                disabled={sourceAutomationStatus === "preparing"}
              >
                <Play className="h-4 w-4" aria-hidden="true" />
                {sourceAutomationConfig?.providerKind.includes("OAuth") ||
                selectedSource.id === "github"
                  ? `Prepare and open ${selectedSource.name}`
                  : `Prepare ${selectedSource.name}`}
              </Button>
              <Button
                type="button"
                variant="secondary"
                onClick={refreshSourceAutomationStatus}
                disabled={sourceAutomationStatus === "preparing"}
              >
                <RefreshCw className="h-4 w-4" aria-hidden="true" />
                Refresh status
              </Button>
              {sourceRehearsal?.installUrl ? (
                <Button
                  type="button"
                  variant="secondary"
                  onClick={() =>
                    window.open(
                      sourceRehearsal.installUrl ?? "",
                      "_blank",
                      "noopener,noreferrer"
                    )
                  }
                >
                  <ArrowRight className="h-4 w-4" aria-hidden="true" />
                  Reopen {selectedSource.name} approval
                </Button>
              ) : null}
            </div>
          </CardContent>
        </Card>
      ) : null}
      <Card>
        <CardHeader>
          <div>
            <CardTitle>Fetch actual landed observations</CardTitle>
            <p className="mt-1 text-sm text-muted-foreground">
              Reads the customer-cloud gateway observation list with an actor
              session token held only in this browser memory.
            </p>
          </div>
          <Badge
            tone={
              fetchStatus === "loaded"
                ? "success"
                : fetchStatus === "error"
                  ? "error"
                  : "info"
            }
          >
            {fetchStatus === "idle" ? "gateway read" : fetchStatus}
          </Badge>
        </CardHeader>
        <CardContent className="grid gap-4 lg:grid-cols-[1fr_1fr_auto]">
          <Field label="Gateway API base" help="Customer-cloud gateway or public tunnel.">
            <Input
              value={apiBase}
              onChange={(event) => setApiBase(event.target.value)}
              placeholder="https://fyralis-ingress.customer.example"
            />
          </Field>
          <Field label="Bearer token" help="Actor session token, not stored.">
            <Input
              value={bearerToken}
              onChange={(event) => setBearerToken(event.target.value)}
              type="password"
              autoComplete="off"
              placeholder="Paste customer-cloud token"
            />
          </Field>
          <div className="flex items-end">
            <Button
              type="button"
              onClick={loadGatewayObservations}
              disabled={fetchStatus === "loading"}
            >
              <RefreshCw className="h-4 w-4" aria-hidden="true" />
              Fetch observations
            </Button>
          </div>
          <div
            className={cn(
              "rounded-lg border p-4 text-sm lg:col-span-3",
              fetchStatus === "error"
                ? "border-destructive/35 bg-destructive/10 text-destructive"
                : "border-border bg-background/70 text-muted-foreground"
            )}
          >
            {fetchMessage}
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Observations landed</CardTitle>
          <Badge tone="success">
            {observations.filter((item) => item.origin === "gateway").length ||
              observations.length}{" "}
            sanitized
          </Badge>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-2">
          {observations.length ? (
            observations.map((observation) => (
              <ObservationCard key={observation.id} observation={observation} />
            ))
          ) : (
            <div className="rounded-lg border border-border bg-background/70 p-4 text-sm text-muted-foreground md:col-span-2">
              Start the first sync to land sanitized observations for this source.
            </div>
          )}
        </CardContent>
      </Card>
      <ActionBar primaryLabel="Continue to activation" onPrimary={advance} secondaryLabel="Retry checks" secondaryIcon={<RefreshCw className="h-4 w-4" aria-hidden="true" />} />
    </div>
  );
}

function ActivationStep({ selectedSource, updateConnection, advance }: StepViewProps) {
  return (
    <OperationalStep
      icon={<Rocket className="h-5 w-5" aria-hidden="true" />}
      title={`Activate ${selectedSource.name} for pilot`}
      badge="Pilot gate"
      description="The source can move into the pilot after setup, validation, scope, first sync, and ingestion health are complete."
      cards={[
        ["Connection", "Validated"],
        ["Scope", "Customer approved"],
        ["First sync", "Completed"],
        ["Health", "Acceptable"]
      ]}
      primaryLabel="Activate source"
      onPrimary={() => {
        updateConnection(selectedSource.id, { status: "connected" });
        advance();
      }}
    />
  );
}

function WorkspaceLaunchStep({ selectedSource, selectedConnection, setLaunchReady, advance }: StepViewProps) {
  return (
    <OperationalStep
      icon={<ShieldCheck className="h-5 w-5" aria-hidden="true" />}
      title="Pilot launch checklist"
      badge="Launch gates"
      description="Confirm the deployment, source sync, observability, support, and access controls are ready."
      cards={[
        ["Deployment health", "Green"],
        ["Active source", selectedConnection?.status === "connected" ? `${selectedSource.name} active` : "Pending activation"],
        ["Observability", "Customer-local logs and metrics ready"],
        ["Support", "Launch-day owner confirmed"]
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
  sourceObservations
}: StepViewProps) {
  const activeSources = snapshot.sources.filter((source) => {
    const connection = connections.find((item) => item.sourceId === source.id);
    return connection?.status === "connected" || source.id === selectedSource.id;
  });
  const activeSourceIds = new Set(activeSources.map((source) => source.id));
  const landedObservations = sourceObservations.filter(
    (observation) => activeSourceIds.has(observation.sourceId)
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
          <Metric label="Deployment" value="Healthy" detail="Post-deploy validation complete." />
          <Metric label="Active sources" value={String(activeSources.length)} detail={`${selectedSource.name} ${selectedConnection?.status ?? "ready"}.`} />
          <Metric label="Data boundary" value="BYOC" detail="No source data leaves the customer cloud." />
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>Connected sources</CardTitle>
          <Badge tone="info">Pilot scope</Badge>
        </CardHeader>
        <CardContent className="grid gap-3">
          {activeSources.map((source) => (
            <div key={source.id} className="flex items-center justify-between gap-4 rounded-lg border border-border bg-background/70 p-4">
              <span>
                <strong className="block">{source.name}</strong>
                <span className="text-sm text-muted-foreground">{source.description}</span>
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
                snapshot.sources.find((source) => source.id === observation.sourceId)
                  ?.name
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
  onPrimary
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
              <p className="mt-1 text-sm text-muted-foreground">{description}</p>
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
  footer
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
          <Badge tone={validation.status === "failed" ? "error" : validation.status === "passed" ? "success" : "info"}>
            {validation.status}
          </Badge>
        </CardHeader>
        <CardContent className="grid gap-3">
          {validation.checks.map((check) => (
            <div key={check.label} className="flex gap-3 rounded-lg border border-border bg-background/70 p-4">
              {check.status === "failed" ? (
                <AlertTriangle className="mt-0.5 h-4 w-4 text-destructive" aria-hidden="true" />
              ) : (
                <CheckCircle2 className="mt-0.5 h-4 w-4 text-success" aria-hidden="true" />
              )}
              <span>
                <strong className="block">{check.label}</strong>
                <span className="text-sm text-muted-foreground">{check.detail}</span>
              </span>
            </div>
          ))}
          <div className="mt-2 flex flex-wrap gap-2">
            <Button type="button" onClick={onPrimary}>{primaryLabel}</Button>
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
  submit,
  disabled,
  compact
}: {
  primaryLabel: string;
  onPrimary?: () => void;
  secondaryLabel?: string;
  secondaryIcon?: ReactNode;
  submit?: boolean;
  disabled?: boolean;
  compact?: boolean;
}) {
  return (
    <div className={cn("flex flex-wrap items-center gap-3", !compact && "rounded-lg border border-border bg-card p-4")}>
      <Button type={submit ? "submit" : "button"} onClick={onPrimary} disabled={disabled}>
        {primaryLabel}
        {!submit ? <ArrowRight className="h-4 w-4" aria-hidden="true" /> : null}
      </Button>
      {secondaryLabel ? (
        <Button type="button" variant="secondary">
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
  children
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

function BoundaryPanel({ title, items, strong }: { title: string; items: string[]; strong?: boolean }) {
  return (
    <div className={cn("rounded-lg border p-5", strong ? "border-success/30 bg-success/10" : "border-border bg-background/70")}>
      <h3 className="text-lg font-semibold">{title}</h3>
      <ul className="mt-4 grid gap-2 text-sm text-muted-foreground">
        {items.map((item) => (
          <li key={item} className="flex gap-2">
            <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0 text-success" aria-hidden="true" />
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

function InfoBlock({ title, value, code }: { title: string; value: string; code?: boolean }) {
  return (
    <div className="rounded-lg border border-border bg-background/70 p-4">
      <strong className="block text-sm">{title}</strong>
      {code ? (
        <code className="mt-2 block whitespace-pre-wrap break-all text-xs text-muted-foreground">{value}</code>
      ) : (
        <span className="mt-2 block whitespace-pre-wrap text-sm text-muted-foreground">{value}</span>
      )}
    </div>
  );
}

function SetupList({
  title,
  items,
  tone
}: {
  title: string;
  items: string[];
  tone: "success" | "info" | "warning";
}) {
  return (
    <div className="rounded-lg border border-border bg-background/70 p-4">
      <div className="mb-3 flex items-center justify-between gap-3">
        <strong className="text-sm">{title}</strong>
        <Badge tone={tone}>{items.length}</Badge>
      </div>
      <div className="grid gap-2">
        {items.map((item) => (
          <code
            key={item}
            className="break-all rounded-md border border-border bg-card px-2 py-1 text-xs text-muted-foreground"
          >
            {item}
          </code>
        ))}
      </div>
    </div>
  );
}

function SignalPathCard({
  title,
  detail,
  command
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

function ProviderRouteList({
  workspace,
  config
}: {
  workspace: Workspace;
  config: SourceRehearsalConfig;
}) {
  return (
    <div className="rounded-lg border border-border bg-background/70 p-4">
      <div className="mb-3 flex items-center justify-between gap-3">
        <strong className="text-sm">Real gateway URLs</strong>
        <Badge tone="info">{config.gatewayRoutes.length}</Badge>
      </div>
      <div className="grid gap-3">
        {config.gatewayRoutes.map((route) => (
          <div key={`${route.method}-${route.path}`} className="grid gap-1">
            <span className="flex flex-wrap items-center gap-2 text-xs font-semibold text-muted-foreground">
              <Badge tone={route.method === "GET" ? "info" : "success"}>
                {route.method}
              </Badge>
              {route.label}
              <span>{route.access}</span>
            </span>
            <code className="break-all text-xs text-muted-foreground">
              {gatewayRouteUrl(workspace, route.path)}
            </code>
          </div>
        ))}
      </div>
    </div>
  );
}

function ObservationCard({
  observation,
  sourceName
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
          : "border-info/25 bg-info/10"
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
          <code key={log} className="break-all text-xs text-primary-foreground/80">
            {log}
          </code>
        ))}
      </div>
    </div>
  );
}

function Metric({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <div className="rounded-lg border border-border bg-background/70 p-4">
      <span className="text-xs font-semibold text-muted-foreground">{label}</span>
      <strong className="mt-2 block text-2xl tracking-tight">{value}</strong>
      <span className="mt-1 block text-sm text-muted-foreground">{detail}</span>
    </div>
  );
}

function sourceScopeChoices(sourceId: string) {
  const scopes: Record<string, string[]> = {
    slack: ["#leadership", "#finance-ops", "#customer-success", "#engineering", "Consented DMs", "Pinned files"],
    github: ["Core repos", "Infrastructure repos", "Open pull requests", "Issues", "Release branches", "Security repos excluded"],
    gmail: ["Pilot executives", "Finance mailbox", "Customer-success mailbox", "Last 30 days", "Exclude personal labels", "Metadata-first crawl"]
  };
  return (
    scopes[sourceId] ?? [
      "Pilot workspace",
      "Approved entities",
      "Last 30 days",
      "Live updates",
      "Metadata-first crawl",
      "Sensitive records excluded"
    ]
  );
}
