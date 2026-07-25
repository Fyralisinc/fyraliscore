import type {
  CloudReadiness,
  Customer,
  Deployment,
  OnboardingProgress,
  OnboardingSnapshot,
  Plan,
  SetupPackage,
  Source,
  SourceConnection,
  SourceObservation,
  StepDefinition,
  SyncJob,
  Validation,
  Workspace
} from "../types";
import sourceCatalogArtifact from "./source-catalog.generated.json";

export const ONBOARDING_STEPS: StepDefinition[] = [
  {
    id: "get-fyralis",
    title: "Get Fyralis",
    eyebrow: "Commercial start",
    phase: "Commercial",
    boundary: "hosted-portal",
    estimateMinutes: 2,
    description: "Choose the design-partner or commercial BYOC path."
  },
  {
    id: "customer-intake",
    title: "Customer intake",
    eyebrow: "No secrets",
    phase: "Commercial",
    boundary: "hosted-portal",
    estimateMinutes: 3,
    description: "Capture customer and setup-owner context."
  },
  {
    id: "cloud-readiness",
    title: "Cloud readiness",
    eyebrow: "BYOC shape",
    phase: "Cloud",
    boundary: "hosted-portal",
    estimateMinutes: 6,
    description: "Confirm the required customer-cloud deployment components."
  },
  {
    id: "setup-package",
    title: "Setup package",
    eyebrow: "Generated artifacts",
    phase: "Cloud",
    boundary: "hosted-portal",
    estimateMinutes: 5,
    description: "Review the generated manifest, permissions, and commands."
  },
  {
    id: "trust-boundary",
    title: "Trust boundary",
    eyebrow: "Boundary handoff",
    phase: "Boundary",
    boundary: "hosted-portal",
    estimateMinutes: 4,
    description: "Move from Fyralis hosted portal into the customer cloud."
  },
  {
    id: "preflight",
    title: "Preflight",
    eyebrow: "Local console",
    phase: "Deployment",
    boundary: "customer-cloud",
    estimateMinutes: 8,
    description: "Run readiness checks from inside the customer cloud."
  },
  {
    id: "deployment",
    title: "Deployment",
    eyebrow: "Runtime install",
    phase: "Deployment",
    boundary: "customer-cloud",
    estimateMinutes: 12,
    description: "Install the Fyralis data plane."
  },
  {
    id: "deployment-validation",
    title: "Deployment validation",
    eyebrow: "Live checks",
    phase: "Deployment",
    boundary: "customer-cloud",
    estimateMinutes: 7,
    description: "Prove gateway, workers, database, broker, and storage health."
  },
  {
    id: "source-catalog",
    title: "Source catalog",
    eyebrow: "Integrations",
    phase: "Sources",
    boundary: "customer-cloud",
    estimateMinutes: 4,
    description: "Choose sources from the integration marketplace."
  },
  {
    id: "workspace-launch",
    title: "Workspace launch",
    eyebrow: "Launch gates",
    phase: "Launch",
    boundary: "customer-cloud",
    estimateMinutes: 7,
    description: "Confirm operational, support, and source readiness."
  },
  {
    id: "workspace-home",
    title: "Workspace home",
    eyebrow: "Operate",
    phase: "Launch",
    boundary: "customer-cloud",
    estimateMinutes: 0,
    description: "Operate Fyralis from the customer-cloud workspace."
  }
];

export const PLANS: Plan[] = [
  {
    id: "design-partner-byoc",
    label: "Design Partner BYOC",
    badge: "First deployment",
    description:
      "For the first design partner deployment where Fyralis is installed in the customer cloud and the BYOC operating model is hardened together.",
    terms: [
      { label: "Plan", value: "Pilot" },
      { label: "Cloud", value: "Customer BYOC" }
    ],
    features: [
      "Deploy Fyralis inside the partner cloud",
      "Use the hosted portal for setup metadata only",
      "Move source credentials to the customer-cloud console",
      "Start with narrow approved source scope",
      "Harden readiness, security, and operations"
    ]
  },
  {
    id: "commercial-byoc",
    label: "Enterprise",
    badge: "Commercial BYOC",
    description:
      "For production enterprise customers that need a customer-cloud Fyralis deployment with security review, support terms, and rollout controls.",
    terms: [
      { label: "Plan", value: "Custom" },
      { label: "Cloud", value: "Customer BYOC" }
    ],
    features: [
      "Production BYOC workspace setup",
      "Security review and rollout plan",
      "Customer-cloud source connection path",
      "Support and implementation gates",
      "Sanitized status back to Fyralis control plane"
    ]
  }
];

export const CUSTOMER: Customer = {
  company: "Acme Finance",
  setupOwnerEmail: "platform-owner@acme.example",
  targetCloud: "AWS"
};

export const WORKSPACE: Workspace = {
  id: "wks_acme_finance_byoc",
  customerName: "Acme Finance",
  localConsoleUrl: "http://localhost:3003",
  providerIngressUrl: "http://localhost:8000"
};

export const READINESS: CloudReadiness = {
  region: "us-east-1",
  environment: "pilot",
  setupAutomation: "agent-managed",
  agentAccess: "customer-cloud-agent",
  agentPermissionProfile: "byoc-bootstrap-provisioner",
  agentApprovalMode: "approval-required",
  setupRoleArn: "",
  kubernetes: "provision-eks",
  network: "provision-isolated-vpc",
  secrets: "provision-secret-refs",
  postgres: "provision-rds-pgvector",
  objectStorage: "provision-s3",
  kafka: "provision-msk"
};

export const SETUP_PACKAGE: SetupPackage = {
  id: "pkg_acme_2026_06_29",
  generatedAt: "2026-06-29T12:30:00Z",
  artifacts: [
    {
      name: "Data-plane manifest",
      description: "Runtime, telemetry posture, service shape, and component refs.",
      filename: "customer-dataplane.yaml",
      safeToShare: true
    },
    {
      name: "Terraform/OpenTofu plan",
      description: "Customer-cloud modules for missing BYOC capabilities.",
      filename: "terraform/fyralis-byoc.auto.tfvars.json",
      safeToShare: true
    },
    {
      name: "Setup agent manifest",
      description: "Customer-cloud agent install, discovery, plan, apply, and validation policy.",
      filename: "agent/fyralis-setup-agent.yaml",
      safeToShare: true
    },
    {
      name: "AWS setup role template",
      description: "Least-privilege setup role with external ID and approval boundary.",
      filename: "iam/fyralis-byoc-setup-role.template.yaml",
      safeToShare: true
    },
    {
      name: "Helm values",
      description: "Kubernetes install values that reference local customer resources.",
      filename: "helm/fyralis-values.customer.yaml",
      safeToShare: true
    },
    {
      name: "Permission manifest",
      description: "Least-privilege IAM shape and customer-owned boundary assumptions.",
      filename: "customer-permissions.yaml",
      safeToShare: true
    },
    {
      name: "Bootstrap bundle",
      description: "Digest-pinned setup inputs and artifact references.",
      filename: "customer-bootstrap-bundle.yaml",
      safeToShare: true
    },
    {
      name: "Post-deploy validation checklist",
      description: "Gateway, workers, Postgres, Kafka/MSK, and object storage checks.",
      filename: "customer-validation-checklist.yaml",
      safeToShare: true
    }
  ],
  commands: [
    {
      label: "Preflight",
      command:
        "fyralis byoc preflight --bundle fyralis-byoc-acme-finance.zip --json"
    },
    {
      label: "Validation",
      command:
        "fyralis byoc validate --json --emit-sanitized-readiness-report"
    }
  ]
};

export const DEPLOYMENT: Deployment = {
  id: "dep_acme_byoc_pilot",
  status: "ready",
  region: "us-east-1",
  runtime: "Kubernetes",
  timeline: [
    {
      label: "Bundle verified",
      status: "done",
      detail: "Signatures, digests, and SBOM policy accepted."
    },
    {
      label: "Runtime applied",
      status: "done",
      detail: "Kubernetes manifests accepted by the customer cluster."
    },
    {
      label: "Data plane healthy",
      status: "done",
      detail: "Gateway, workers, database, Kafka/MSK, and object storage are reachable."
    },
    {
      label: "Status egress active",
      status: "running",
      detail: "Only heartbeat, version, and readiness receipts flow to Fyralis."
    }
  ],
  logs: [
    "12:41:08 bundle signature verified",
    "12:42:13 applied namespace fyralis-system",
    "12:44:29 gateway readiness probe passed",
    "12:45:02 kafka-msk connectivity verified",
    "12:46:18 sanitized status receipt emitted"
  ]
};

type SourceCatalogArtifact = {
  schemaVersion: "fyralis.source-catalog.ui.v1";
  canonicalSourceIds: string[];
  canonicalProviderIds: string[];
  sources: Source[];
};

const sourceCatalog =
  sourceCatalogArtifact as SourceCatalogArtifact;

export const SOURCES: Source[] = sourceCatalog.sources;

export const CONNECTIONS: SourceConnection[] = [];

export const DEPLOYMENT_VALIDATION: Validation = {
  id: "val_deployment_acme",
  target: "deployment",
  status: "passed",
  checks: [
    { label: "Gateway health", status: "passed", detail: "Gateway responds inside the data plane." },
    { label: "Worker health", status: "passed", detail: "Ingestion and background workers are healthy." },
    { label: "Postgres with pgvector", status: "passed", detail: "Reachable with production role checks." },
    { label: "Kafka/MSK", status: "passed", detail: "Broker publish and consume checks passed." },
    { label: "S3-compatible storage", status: "passed", detail: "Raw payload tier is reachable." }
  ]
};

export const SOURCE_VALIDATION: Validation = {
  id: "val_source_slack",
  target: "source",
  status: "not-run",
  checks: [
    { label: "Secret refs reachable", status: "pending", detail: "No local source test has run." },
    { label: "Provider endpoint reachable", status: "pending", detail: "Waiting for provider callback or local path test." },
    { label: "Required scopes present", status: "pending", detail: "Waiting for provider scope check." },
    { label: "Rate limits acceptable", status: "pending", detail: "Waiting for pilot batch estimate." }
  ]
};

export const SYNC_JOBS: SyncJob[] = [
  {
    id: "sync_slack_initial",
    sourceId: "slack",
    mode: "Limited backfill",
    status: "completed",
    eventsReceived: 128,
    errors: 0
  }
];

export const SOURCE_OBSERVATIONS: SourceObservation[] = [
  observation(
    "slack_obs_001",
    "slack",
    "Finance launch thread",
    "message",
    "Leadership and finance-ops pilot channels produced bounded launch-readiness signals."
  ),
  observation(
    "github_obs_001",
    "github",
    "Repository rollout activity",
    "pull-request",
    "Selected repositories produced pull-request and issue metadata for the first sync."
  ),
  observation(
    "jira_obs_001",
    "jira",
    "Pilot issue movement",
    "issue",
    "Approved Jira projects produced issue status and comment metadata without raw attachments."
  ),
  observation(
    "discord_obs_001",
    "discord",
    "Community feedback channel",
    "message",
    "Approved Discord channels produced event metadata through the customer-cloud gateway."
  ),
  observation(
    "notion_obs_001",
    "notion",
    "Launch checklist updates",
    "page",
    "Shared Notion pages and databases produced page-change observations for the pilot."
  ),
  observation(
    "telegram_obs_001",
    "telegram",
    "Partner readiness chat",
    "message",
    "Approved Telegram dialogs produced MTProto session observations inside the customer boundary."
  )
];

export const PROGRESS: OnboardingProgress = {
  currentStep: "get-fyralis",
  completedSteps: [],
  lastSavedAt: null,
  dirty: false,
  launchReady: false
};

export const ONBOARDING_SNAPSHOT: OnboardingSnapshot = {
  plans: PLANS,
  steps: ONBOARDING_STEPS,
  customer: CUSTOMER,
  workspace: WORKSPACE,
  readiness: READINESS,
  setupPackage: SETUP_PACKAGE,
  deployment: DEPLOYMENT,
  sources: SOURCES,
  connections: CONNECTIONS,
  sourceValidation: SOURCE_VALIDATION,
  deploymentValidation: DEPLOYMENT_VALIDATION,
  syncJobs: SYNC_JOBS,
  sourceObservations: SOURCE_OBSERVATIONS,
  progress: PROGRESS
};

function observation(
  id: SourceObservation["id"],
  sourceId: SourceObservation["sourceId"],
  title: SourceObservation["title"],
  kind: SourceObservation["kind"],
  summary: SourceObservation["summary"]
): SourceObservation {
  return {
    id,
    sourceId,
    title,
    kind,
    occurredAt: "2026-07-01T09:30:00Z",
    summary,
    evidencePath: `s3://fyralis-byoc-pilot/raw/${sourceId}/${id}.jsonl`,
    status: "landed",
    origin: "preview",
    syncTrack: "mixed",
    sourceChannel: `${sourceId}:sample`
  };
}
