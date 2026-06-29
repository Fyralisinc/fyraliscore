"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import {
  AlertTriangle,
  ArrowRight,
  CheckCircle2,
  ClipboardCheck,
  CloudCog,
  Download,
  KeyRound,
  LockKeyhole,
  Play,
  RefreshCw,
  Rocket,
  ShieldCheck,
  TerminalSquare
} from "lucide-react";
import { useEffect, type ReactNode } from "react";
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
  OnboardingSnapshot,
  PlanId,
  Source,
  SourceConnection,
  StepId,
  SyncJob,
  Validation,
  Workspace
} from "../types";
import { SourceDetailPanel } from "../components/source-detail-panel";
import { SourceMarketplace } from "../components/source-marketplace";

export type StepViewProps = {
  snapshot: OnboardingSnapshot;
  selectedPlan: PlanId;
  customer: Customer;
  readiness: CloudReadiness;
  workspace: Workspace;
  selectedSource: Source;
  selectedConnection?: SourceConnection;
  connections: SourceConnection[];
  sourceValidation: Validation;
  syncJobs: SyncJob[];
  launchReady: boolean;
  choosePlan: (plan: PlanId) => void;
  updateCustomer: (customer: Customer) => void;
  updateReadiness: (readiness: CloudReadiness) => void;
  selectSource: (sourceId: string) => void;
  updateConnection: (sourceId: string, patch: Partial<SourceConnection>) => void;
  setSourceValidation: (validation: Validation) => void;
  upsertSyncJob: (job: SyncJob) => void;
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

function PlanSelection({ snapshot, selectedPlan, choosePlan, advance }: StepViewProps) {
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
              onClick={() => {
                choosePlan(plan.id);
                advance();
              }}
            >
              {plan.id === "design-partner-byoc"
                ? "Start Design Partner BYOC"
                : "Start Enterprise"}
            </Button>
          </article>
        ))}
        </div>
      </div>
    </div>
  );
}

function CustomerIntake({
  customer,
  updateCustomer,
  advance
}: StepViewProps) {
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
      onSubmit={form.handleSubmit((values) => {
        updateCustomer(values);
        advance();
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
      <ActionBar primaryLabel="Continue to cloud readiness" submit />
    </form>
  );
}

function CloudReadinessStep({
  readiness,
  updateReadiness,
  advance
}: StepViewProps) {
  const form = useForm<CloudReadinessFormValues>({
    resolver: zodResolver(cloudReadinessSchema),
    defaultValues: readiness,
    mode: "onChange"
  });

  useEffect(() => {
    const subscription = form.watch((value) => {
      const parsed = cloudReadinessSchema.safeParse(value);
      if (parsed.success) {
        updateReadiness(parsed.data);
      }
    });
    return () => subscription.unsubscribe();
  }, [form, updateReadiness]);

  return (
    <form
      className="grid gap-5"
      onSubmit={form.handleSubmit((values) => {
        updateReadiness(values);
        advance();
      })}
    >
      <Card>
        <CardHeader>
          <CardTitle>Required BYOC components</CardTitle>
          <Badge tone="info">No credentials</Badge>
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
          <SelectField label="Kubernetes runtime" register={form.register("kubernetes")} help="Fyralis first supports the Kubernetes BYOC install path.">
            <option value="available">Kubernetes available</option>
            <option value="needs-guidance">Need Kubernetes setup guidance</option>
            <option value="unknown">Not sure yet</option>
          </SelectField>
          <SelectField label="Network/VPC" register={form.register("network")} help="Confirms whether Fyralis fits into an approved network boundary.">
            <option value="existing-ready">Existing VPC/network ready</option>
            <option value="needs-isolated-guidance">Need isolated VPC guidance</option>
            <option value="unknown">Not sure yet</option>
          </SelectField>
          <SelectField label="AWS Secrets Manager" register={form.register("secrets")} help="Fyralis uses secret refs later, never secret values.">
            <option value="aws-secrets-manager">AWS Secrets Manager available</option>
            <option value="needs-guidance">Need setup guidance</option>
            <option value="unknown">Not sure yet</option>
          </SelectField>
          <SelectField label="Postgres with pgvector" register={form.register("postgres")} help="Required for tenant data, model state, and vector search.">
            <option value="pgvector-ready">Postgres with pgvector available</option>
            <option value="needs-guidance">Need setup guidance</option>
            <option value="unknown">Not sure yet</option>
          </SelectField>
          <SelectField label="S3-compatible storage" register={form.register("objectStorage")} help="Required for raw payload tier and artifacts.">
            <option value="s3-compatible-ready">S3-compatible object storage available</option>
            <option value="needs-guidance">Need bucket setup guidance</option>
            <option value="unknown">Not sure yet</option>
          </SelectField>
          <SelectField label="Kafka/MSK" register={form.register("kafka")} help="Required for Kafka-first ingestion durability.">
            <option value="kafka-msk-ready">Kafka/MSK available</option>
            <option value="needs-guidance">Need Kafka/MSK setup guidance</option>
            <option value="unknown">Not sure yet</option>
          </SelectField>
        </CardContent>
      </Card>
      <ActionBar primaryLabel="Prepare setup package" submit />
    </form>
  );
}

function SetupPackageStep({ snapshot, advance }: StepViewProps) {
  return (
    <div className="grid gap-5">
      <Card>
        <CardHeader>
          <CardTitle>Generated BYOC package</CardTitle>
          <Badge tone="success">Package ready</Badge>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 md:grid-cols-2">
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
          <div className="mt-5 grid gap-3">
            {snapshot.setupPackage.commands.map((command) => (
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

function SourceCatalogStep(props: StepViewProps) {
  const { snapshot, selectedSource, selectedConnection, selectSource, updateConnection, goTo } = props;
  return (
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
  );
}

function SourceSetupStep({ selectedSource, selectedConnection, workspace, updateConnection, advance }: StepViewProps) {
  const endpoints = selectedSource.providerIngressPaths.length
    ? selectedSource.providerIngressPaths.map((path) => `${workspace.providerIngressUrl}${path}`)
    : [selectedSource.noIngressReason ?? "No provider ingress required."];
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
      <ActionBar
        primaryLabel="Mark source prepared"
        onPrimary={() => {
          updateConnection(selectedSource.id, { status: "ready" });
          advance();
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

function FirstSyncStep({ selectedSource, selectedConnection, upsertSyncJob, advance }: StepViewProps) {
  return (
    <OperationalStep
      icon={<Play className="h-5 w-5" aria-hidden="true" />}
      title={`Run ${selectedSource.name} first sync`}
      badge={selectedConnection?.syncMode ?? "Limited backfill"}
      description="Run a bounded source sync inside the customer data plane before activation."
      cards={[
        ["Scope", `${selectedConnection?.selectedScopes.length || 3} approved items`],
        ["Backfill", selectedConnection?.backfillWindow ?? "Last 30 days"],
        ["Mode", selectedConnection?.syncMode ?? "Limited backfill"],
        ["Boundary", "Raw source payloads remain in BYOC"]
      ]}
      primaryLabel="Start first sync"
      onPrimary={() => {
        upsertSyncJob({
          id: `sync_${selectedSource.id}_initial`,
          sourceId: selectedSource.id,
          mode: selectedConnection?.syncMode ?? "Limited backfill",
          status: "completed",
          eventsReceived: 80 + selectedSource.name.length * 8,
          errors: 0
        });
        advance();
      }}
    />
  );
}

function IngestionHealthStep({ selectedSource, syncJobs, advance }: StepViewProps) {
  const job = syncJobs.find((item) => item.sourceId === selectedSource.id);
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

function WorkspaceHomeStep({ snapshot, connections, launchReady, selectedSource, selectedConnection }: StepViewProps) {
  const activeSources = snapshot.sources.filter((source) => {
    const connection = connections.find((item) => item.sourceId === source.id);
    return connection?.status === "connected" || source.id === selectedSource.id;
  });
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
  compact
}: {
  primaryLabel: string;
  onPrimary?: () => void;
  secondaryLabel?: string;
  secondaryIcon?: ReactNode;
  submit?: boolean;
  compact?: boolean;
}) {
  return (
    <div className={cn("flex flex-wrap items-center gap-3", !compact && "rounded-lg border border-border bg-card p-4")}>
      <Button type={submit ? "submit" : "button"} onClick={onPrimary}>
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
