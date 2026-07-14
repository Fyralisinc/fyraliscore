export type StepId =
  | "get-fyralis"
  | "customer-intake"
  | "cloud-readiness"
  | "setup-package"
  | "trust-boundary"
  | "preflight"
  | "deployment"
  | "deployment-validation"
  | "source-catalog"
  | "source-setup"
  | "source-validation"
  | "source-scope"
  | "first-sync"
  | "ingestion-health"
  | "activation"
  | "workspace-launch"
  | "workspace-home";

export type OnboardingPhase =
  | "Commercial"
  | "Cloud"
  | "Boundary"
  | "Deployment"
  | "Sources"
  | "Launch";

export type Boundary = "hosted-portal" | "customer-cloud";

export type StepState = "completed" | "current" | "available" | "locked";

export type StepDefinition = {
  id: StepId;
  title: string;
  eyebrow: string;
  phase: OnboardingPhase;
  boundary: Boundary;
  estimateMinutes: number;
  description: string;
};

export type PlanId = "design-partner-byoc" | "commercial-byoc";

export type OnboardingIntent = {
  schema_version: "fyralis.platform.onboarding_intent.v1";
  intent_id: string;
  plan_code: "design_partner_byoc_pilot" | "enterprise_byoc";
  procurement_channel:
    | "design_partner"
    | "sales"
    | "direct"
    | "aws_marketplace"
    | "private_offer";
  entrypoint: string;
  status:
    | "draft"
    | "intake_submitted"
    | "workspace_created"
    | "commercial_review"
    | "cancelled";
  customer_id: string | null;
  tenant_id: string | null;
  deployment_id: string | null;
  company_name: string | null;
  setup_owner_email: string | null;
  target_cloud: "aws" | null;
  created_at: string;
  updated_at: string;
  stored_scope: "sanitized_onboarding_metadata_only";
};

export type Plan = {
  id: PlanId;
  label: string;
  badge: string;
  description: string;
  terms: Array<{ label: string; value: string }>;
  features: string[];
};

export type Customer = {
  company: string;
  setupOwnerEmail: string;
  targetCloud: "AWS" | "GCP future profile" | "Azure future profile";
};

export type Workspace = {
  id: string;
  customerName: string;
  localConsoleUrl: string;
  providerIngressUrl: string;
};

export type CloudReadiness = {
  region: "us-east-1" | "us-west-2" | "eu-west-1" | "ap-south-1";
  environment: "pilot" | "staging" | "production";
  setupAutomation: "agent-managed";
  agentAccess: "customer-cloud-agent" | "aws-cross-account-role";
  agentPermissionProfile: "byoc-bootstrap-provisioner" | "discovery-only";
  agentApprovalMode: "approval-required" | "plan-only";
  setupRoleArn: string;
  kubernetes: "available" | "provision-eks" | "needs-guidance" | "unknown";
  network:
    | "existing-ready"
    | "provision-isolated-vpc"
    | "needs-isolated-guidance"
    | "unknown";
  secrets:
    | "aws-secrets-manager"
    | "provision-secret-refs"
    | "needs-guidance"
    | "unknown";
  postgres:
    | "pgvector-ready"
    | "provision-rds-pgvector"
    | "needs-guidance"
    | "unknown";
  objectStorage:
    | "s3-compatible-ready"
    | "provision-s3"
    | "needs-guidance"
    | "unknown";
  kafka:
    | "kafka-msk-ready"
    | "provision-msk"
    | "needs-guidance"
    | "unknown";
};

export type SetupPackage = {
  id: string;
  generatedAt: string;
  artifacts: Array<{
    name: string;
    description: string;
    filename: string;
    safeToShare: boolean;
  }>;
  commands: Array<{
    label: string;
    command: string;
  }>;
};

export type DeploymentStatus =
  | "not-started"
  | "running"
  | "ready"
  | "needs-attention";

export type Deployment = {
  id: string;
  status: DeploymentStatus;
  region: string;
  runtime: "Kubernetes";
  timeline: Array<{
    label: string;
    status: "done" | "running" | "pending" | "error";
    detail: string;
  }>;
  logs: string[];
};

export type SourceCategory =
  | "Communication"
  | "Engineering"
  | "Productivity"
  | "Knowledge"
  | "Meetings"
  | "Finance"
  | "People"
  | "Cloud"
  | "Design"
  | "Operations"
  | "CRM";

export type ConnectionMethod =
  | "OAuth"
  | "Webhook"
  | "API token"
  | "Gateway"
  | "IAM role"
  | "Workspace DWD"
  | "Poll";

export type SourceStatus =
  | "not-configured"
  | "draft"
  | "ready"
  | "validating"
  | "waiting-admin"
  | "connected"
  | "error";

export type Source = {
  id: string;
  name: string;
  category: SourceCategory;
  description: string;
  method: ConnectionMethod;
  requiredPermissions: string[];
  setupRequirements: string;
  supportedSyncModes: Array<
    | "Dry run"
    | "Limited backfill"
    | "Live events"
    | "Backfill plus live"
    | "Backfill plus polling"
  >;
  providerIngressPaths: string[];
  noIngressReason?: string;
};

export type SourceConnection = {
  sourceId: string;
  status: SourceStatus;
  selectedScopes: string[];
  backfillWindow:
    | "Last 7 days"
    | "Last 30 days"
    | "Last 90 days"
    | "All available history"
    | "No historical backfill";
  syncMode:
    | "Dry run"
    | "Limited backfill"
    | "Live events"
    | "Backfill plus live"
    | "Backfill plus polling";
  lastIssueCode?: string;
  receiptId?: string;
};

export type Validation = {
  id: string;
  target: "deployment" | "source";
  status: "not-run" | "running" | "passed" | "failed";
  checks: Array<{
    label: string;
    status: "pending" | "passed" | "failed";
    detail: string;
  }>;
};

export type SyncJob = {
  id: string;
  sourceId: string;
  mode: SourceConnection["syncMode"];
  status: "idle" | "running" | "completed" | "failed";
  eventsReceived: number;
  errors: number;
};

export type SourceObservation = {
  id: string;
  sourceId: string;
  title: string;
  kind: "message" | "issue" | "pull-request" | "page" | "deployment" | "task";
  occurredAt: string;
  summary: string;
  evidencePath: string;
  status: "landed" | "queued" | "rejected";
  origin?: "preview" | "gateway";
  syncTrack?: "historical" | "live" | "mixed";
  sourceChannel?: string;
};

export type OnboardingProgress = {
  currentStep: StepId;
  completedSteps: StepId[];
  lastSavedAt: string | null;
  dirty: boolean;
  launchReady: boolean;
};

export type OnboardingSnapshot = {
  plans: Plan[];
  steps: StepDefinition[];
  customer: Customer;
  workspace: Workspace;
  readiness: CloudReadiness;
  setupPackage: SetupPackage;
  deployment: Deployment;
  sources: Source[];
  connections: SourceConnection[];
  sourceValidation: Validation;
  deploymentValidation: Validation;
  syncJobs: SyncJob[];
  sourceObservations: SourceObservation[];
  progress: OnboardingProgress;
};
