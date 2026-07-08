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

export const SOURCES: Source[] = [
  source("slack", "Slack", "Communication", "Channels, events, and consented DMs.", "OAuth", ["channels:history", "groups:history", "users:read", "team:read"], "Slack workspace admin approval, app/OAuth install, signing secret, channel allowlist, optional DM consent.", ["Dry run", "Limited backfill", "Live events", "Backfill plus live"], ["/integrations/slack/callback", "/webhooks/slack/events"]),
  source("gmail", "Gmail", "Productivity", "Workspace email with watch and history polling.", "Workspace DWD", ["gmail.readonly", "pubsub.topics.attachSubscription"], "Google Workspace admin approval, domain-wide delegation setup, mailbox scope, Pub/Sub watch topic, history-poller access.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/gmail/pubsub"]),
  source("google-calendar", "Google Calendar", "Productivity", "Calendar events and shared calendars.", "Workspace DWD", ["calendar.readonly"], "Google Workspace admin approval, domain-wide delegation setup, calendar scope, allowlist, and local polling access.", ["Dry run", "Limited backfill"], [], "Google Calendar DWD install is poll-only; no webhook or push watch is configured."),
  source("google-drive", "Google Drive", "Knowledge", "Files, metadata, and Drive watches.", "Workspace DWD", ["drive.metadata.readonly", "drive.readonly"], "Google Workspace admin approval, domain-wide delegation setup, Drive scopes, shared-drive scope, change watch setup, large-file policy.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/google_drive/push"]),
  source("github", "GitHub", "Engineering", "Repositories, pull requests, issues, and code intelligence.", "OAuth", ["repository metadata", "pull requests", "issues", "webhooks"], "GitHub App installation, repository selection, webhook secret, org admin approval, installation ID mapping.", ["Dry run", "Limited backfill", "Live events", "Backfill plus live"], ["/integrations/github/callback", "/webhooks/github"]),
  source("jira", "Jira", "Engineering", "Issues, projects, and work tracking signals.", "API token", ["read:jira-work", "read:jira-user", "webhook registration"], "Jira site URL, project scope, API token or OAuth app, webhook callback approval, issue/comment permissions.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/jira/events"]),
  source("notion", "Notion", "Knowledge", "Pages, databases, and workspace knowledge.", "OAuth", ["read content", "read users", "database access"], "Workspace integration token, pages and databases shared to integration, workspace owner approval, selector scope.", ["Dry run", "Limited backfill"], ["/integrations/notion/callback", "/webhooks/notion/events"]),
  source("discord", "Discord", "Communication", "Community and team message streams.", "Gateway", ["bot token", "message content intent", "guild read"], "Discord app or bot token, guild/channel allowlist, gateway intents, single-worker lease readiness.", ["Dry run", "Limited backfill", "Live events"], ["/integrations/discord/callback", "/webhooks/discord"]),
  source("telegram", "Telegram", "Communication", "MTProto user-account backfill and live updates.", "Gateway", ["api id", "api hash", "approved chats"], "Telegram API ID/hash, authorized user session, chats allowlist, backfill approval, gateway worker readiness.", ["Dry run", "Limited backfill", "Live events"], [], "Local MTProto gateway session runs from the customer cloud."),
  source("signal", "Signal", "Communication", "Linked-device message ingestion.", "Gateway", ["linked device session", "approved contacts", "approved groups"], "Linked-device session, account approval, contact or group scope, gateway worker readiness.", ["Dry run", "Live events"], [], "Linked-device gateway session runs from the customer cloud."),
  source("whatsapp", "WhatsApp", "Communication", "Cloud API live webhook ingestion.", "Webhook", ["business account read", "messages webhook", "phone id"], "WhatsApp Cloud API app, business account and phone IDs, generated verify token, app secret, and optional customer-local access token.", ["Live events"], ["/integrations/whatsapp/webhook"]),
  source("fireflies", "Fireflies", "Meetings", "Meeting transcripts and conversation records.", "API token", ["transcripts read", "meetings read"], "Workspace approval, API token, transcript scope, meeting history window, webhook or poll setup.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/fireflies"]),
  source("figma", "Figma", "Design", "Design files, teams, and file update events.", "API token", ["file read", "team read", "webhook read"], "Team ID, file keys or team scope, org/team access token, optional webhook secret, file visibility confirmation.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/figma"]),
  source("miro", "Miro", "Design", "Boards, items, and collaboration artifacts.", "API token", ["boards read", "team read"], "Workspace admin approval, bearer token, board allowlist, polling setup, API base confirmation.", ["Dry run", "Limited backfill"], [], "Miro is poll-only from the customer data plane; provider webhooks are not configured."),
  source("grafana", "Grafana", "Operations", "Dashboards, alerts, and operations signals.", "API token", ["dashboards read", "alerts read", "folders read"], "Grafana instance URL, service account token, dashboard/folder scope, alert scope, network reachability from BYOC.", ["Dry run", "Limited backfill"], ["/webhooks/grafana/events"]),
  source("aws", "AWS", "Cloud", "Cloud inventory and operational events.", "IAM role", ["inventory read", "cloudtrail read", "eventbridge read"], "AWS account and region, customer IAM role or access ref, inventory scope, CloudTrail/EventBridge scope if enabled.", ["Dry run", "Limited backfill", "Live events"], [], "Fyralis polls customer-authorized AWS APIs from the local data plane."),
  source("mercury", "Mercury", "Finance", "Banking, cash accounts, and transactions.", "API token", ["accounts read", "transactions read"], "Mercury organization ID, account IDs, API token, webhook secret if live events are enabled, account scope.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/mercury/events"]),
  source("quickbooks", "QuickBooks", "Finance", "Accounting, company, and ledger signals.", "OAuth", ["accounting read", "company info", "webhooks"], "QuickBooks company realm ID, OAuth token path, sandbox or production base URL, webhook verifier.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/quickbooks/events"]),
  source("brex", "Brex", "Finance", "Corporate cards, cash, and transactions.", "API token", ["accounts read", "transactions read", "cards read"], "Brex organization ID, account IDs, API token, webhook secret if live events are enabled, transaction scope.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/brex"]),
  source("ramp", "Ramp", "Finance", "Spend management and finance events.", "OAuth", ["transactions read", "reimbursements read", "cards read", "users read", "business read"], "Ramp business scope, access token or OAuth client credentials, entity allowlist, webhook verifier, and poll schedule approval.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/ramp"]),
  source("carta", "Carta", "Finance", "Cap table, grants, and equity-management signals.", "OAuth", ["issuer read", "securities read", "stakeholders read"], "Carta firm or issuer ID, OAuth access token or client-credentials path, entity scope, token re-mint process.", ["Dry run", "Limited backfill"], [], "Carta is poll-only from the customer data plane."),
  source("gusto", "Gusto", "People", "Payroll and company HR finance records.", "OAuth", ["company read", "employee read", "payroll read"], "Gusto company UUID, OAuth app approval, access/refresh token path, payroll and employee scopes.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/gusto"]),
  source("hibob", "HiBob", "People", "People directory and HRIS signals.", "API token", ["people read", "fields read", "reports read"], "HiBob company ID, service user/API credentials, people-field scope, employee directory approval.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/hibob"]),
  source("ashby", "Ashby", "People", "Recruiting pipeline and hiring signals.", "API token", ["jobs read", "candidates read", "interviews read"], "Ashby organization access, API token, jobs/candidates scope, recruiting data approval.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/ashby/{install-id}"]),
  source("deel", "Deel", "People", "Contractor, payroll, and workforce records.", "API token", ["workers read", "contracts read", "payments read"], "Deel organization access, API token, worker/contract scope, payroll or contractor data approval.", ["Dry run", "Limited backfill", "Live events"], ["/webhooks/deel"]),
  source("linkedin", "LinkedIn", "CRM", "Company and professional-network signals.", "Poll", ["organization read", "profile read", "rate-limit approval"], "LinkedIn organization/page access, OAuth app access token, optional refresh token, company/profile scope, rate-limit approval.", ["Dry run", "Limited backfill"], [], "LinkedIn is poll-only from the customer data plane.")
];

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

function source(
  id: Source["id"],
  name: Source["name"],
  category: Source["category"],
  description: Source["description"],
  method: Source["method"],
  requiredPermissions: Source["requiredPermissions"],
  setupRequirements: Source["setupRequirements"],
  supportedSyncModes: Source["supportedSyncModes"],
  providerIngressPaths: Source["providerIngressPaths"],
  noIngressReason?: Source["noIngressReason"]
): Source {
  return {
    id,
    name,
    category,
    description,
    method,
    requiredPermissions,
    setupRequirements,
    supportedSyncModes,
    providerIngressPaths,
    noIngressReason
  };
}

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
