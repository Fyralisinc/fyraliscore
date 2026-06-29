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
  region: "us-east-1" | "us-west-2" | "eu-west-1";
  environment: "pilot" | "staging" | "production";
  kubernetes: "available" | "needs-guidance" | "unknown";
  network: "existing-ready" | "needs-isolated-guidance" | "unknown";
  secrets: "aws-secrets-manager" | "needs-guidance" | "unknown";
  postgres: "pgvector-ready" | "needs-guidance" | "unknown";
  objectStorage: "s3-compatible-ready" | "needs-guidance" | "unknown";
  kafka: "kafka-msk-ready" | "needs-guidance" | "unknown";
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
  | "Poll";

export type SourceStatus =
  | "not-configured"
  | "draft"
  | "ready"
  | "validating"
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
  supportedSyncModes: Array<"Dry run" | "Limited backfill" | "Live events" | "Backfill plus live">;
  providerIngressPaths: string[];
  noIngressReason?: string;
};

export type SourceConnection = {
  sourceId: string;
  status: SourceStatus;
  selectedScopes: string[];
  backfillWindow: "Last 7 days" | "Last 30 days" | "Last 90 days" | "No historical backfill";
  syncMode: "Dry run" | "Limited backfill" | "Live events" | "Backfill plus live";
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
  progress: OnboardingProgress;
};
