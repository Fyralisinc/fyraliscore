export type ControlPanelAccessGrant = {
  schema_version: "fyralis.byoc.control_panel_access_grant.v1";
  tenant_id: string;
  customer_id: string;
  deployment_ids: string[];
  role: "viewer" | "operator" | "admin";
  enabled: boolean;
  granted_at: string;
  expires_at: string | null;
  stored_scope: "sanitized_control_panel_access_metadata_only";
};

export type ControlPanelAccessGrantList = {
  schema_version: "fyralis.byoc.control_panel_access_grant_list.v1";
  tenant_id: string;
  customer_id: string | null;
  active_only: boolean;
  result_count: number;
  generated_at: string;
  items: ControlPanelAccessGrant[];
  stored_scope: "sanitized_control_panel_access_metadata_only";
};

export type ControlPanelStatus =
  | "ready"
  | "action_required"
  | "degraded"
  | "unknown"
  | "empty";

export type DeploymentOverviewStatus =
  | "ready"
  | "action_required"
  | "degraded"
  | "unknown";

export type ControlPanelSection = {
  key:
    | "deployment_overview"
    | "agent_fleet"
    | "product_health"
    | "evidence_packages"
    | "preflight_reports"
    | "runner_evidence";
  status: ControlPanelStatus;
  item_count: number;
  latest_observed_at: string | null;
  source_schema_version: string;
};

export type ControlPanelAction = {
  code:
    | "enroll_agent"
    | "restore_agent_health"
    | "submit_evidence_package"
    | "review_evidence_failures"
    | "review_preflight_failures"
    | "review_runner_failures"
    | "review_desired_state_drift"
    | "review_product_health";
  priority: "critical" | "warning" | "info";
  source: ControlPanelSection["key"];
  target_section: ControlPanelSection["key"];
  deployment_id: string;
  customer_id: string | null;
  status: "open";
};

export type DeploymentOverview = {
  schema_version: "fyralis.byoc.deployment_overview.v1";
  deployment_id: string;
  customer_id: string | null;
  generated_at: string;
  status: DeploymentOverviewStatus;
  next_action:
    | "none"
    | "enroll_agent"
    | "restore_agent_health"
    | "submit_evidence_package"
    | "review_evidence_failures"
    | "review_preflight_failures"
    | "review_runner_failures";
  agent_summary: {
    enrolled_count: number;
    passing_count: number;
    degraded_count: number;
    failing_count: number;
    unknown_count: number;
    connected_count: number;
    disconnected_count: number;
    heartbeat_observed_count: number;
    evidence_package_required_count: number;
    highest_desired_config_epoch: number;
    current_desired_revision: string | null;
    mixed_desired_revisions: boolean;
    latest_heartbeat_accepted_at: string | null;
    latest_desired_state_updated_at: string | null;
  };
  evidence_summary: {
    receipt_count: number;
    passed_receipt_count: number;
    failed_receipt_count: number;
    skipped_receipt_count: number;
    latest_receipt_id: string | null;
    latest_ledger_status: string;
    latest_required_evidence_passed: boolean | null;
    latest_package_accepted_at: string | null;
  };
  preflight_summary: {
    receipt_count: number;
    passed_receipt_count: number;
    failed_receipt_count: number;
    skipped_receipt_count: number;
    latest_receipt_id: string | null;
    latest_preflight_status: string;
    latest_required_sections_passed: boolean | null;
    latest_failed_section_count: number | null;
    latest_report_accepted_at: string | null;
  };
  runner_summary: {
    receipt_count: number;
    passed_receipt_count: number;
    failed_receipt_count: number;
    latest_receipt_id: string | null;
    latest_runner_status: string;
    latest_required_checks_passed: boolean | null;
    latest_rollout_action: string | null;
    latest_apply_plan_count: number | null;
    latest_artifact_verification_count: number | null;
    latest_digest_pinned_artifact_count: number | null;
    latest_local_digest_checked_count: number | null;
    latest_evidence_accepted_at: string | null;
  };
  stored_scope: "sanitized_deployment_metadata_only";
};

export type AgentFleetItem = {
  agent_id: string;
  agent_version: string;
  artifact_revision: string;
  cloud_provider: string;
  region: string;
  desired_revision: string;
  desired_config_epoch: number;
  evidence_package_required: boolean;
  latest_validation_status: string | null;
  latest_control_plane_connected: boolean | null;
  latest_component_count: number | null;
  latest_ok_component_count: number | null;
  latest_degraded_component_count: number | null;
  latest_failed_component_count: number | null;
  latest_unknown_component_count: number | null;
  latest_queued_batches: number | null;
  latest_dropped_batches: number | null;
  latest_heartbeat_accepted_at: string | null;
  stored_scope: "sanitized_agent_metadata_only";
};

export type ReceiptRecord = {
  receipt?: {
    receipt_id?: string;
    status?: string;
    accepted_at?: string;
    ledger_overall_status?: string;
    preflight_status?: string;
    runner_status?: string;
    required_evidence_passed?: boolean;
    required_sections_passed?: boolean;
    required_checks_passed?: boolean;
  };
  cloud_provider?: string;
  region?: string;
  submitted_at?: string;
};

export type ReceiptList = {
  schema_version: string;
  result_count: number;
  items: ReceiptRecord[];
  stored_scope: "sanitized_metadata_only";
};

export type ProductHealthStatus =
  | "ready"
  | "action_required"
  | "degraded"
  | "unknown";

export type ProductSourceHealth = {
  source: string;
  status: "ready" | "degraded" | "failing" | "disabled" | "unknown";
  auth_status: "ready" | "action_required" | "not_configured" | "unknown";
  backfill_status: "idle" | "running" | "blocked" | "unknown";
  items_ingested_count: number;
  items_failed_count: number;
  queue_depth_count: number;
  lag_seconds: number | null;
  last_success_at: string | null;
};

export type ProductHealthIssue = {
  code: string;
  severity: "critical" | "warning" | "info";
  component:
    | "source_ingestion"
    | "source_auth"
    | "pipeline"
    | "think"
    | "models"
    | "vector_index"
    | "runtime";
  observed_count: number;
  first_observed_at: string | null;
  latest_observed_at: string | null;
};

export type ProductHealth = {
  schema_version: "fyralis.byoc.product_health.v1";
  deployment_id: string;
  customer_id: string | null;
  generated_at: string;
  observed: boolean;
  latest_snapshot_id: string | null;
  latest_collected_at: string | null;
  overall_status: ProductHealthStatus;
  sources: ProductSourceHealth[];
  pipeline: {
    status: ProductHealthStatus;
    queue_lag_count: number;
    dead_letter_count: number;
    retry_backlog_count: number;
    dropped_item_count: number;
  };
  think: {
    status: ProductHealthStatus;
    run_count: number;
    failed_run_count: number;
    queued_run_count: number;
    latest_run_at: string | null;
    breaker_status: "closed" | "open" | "unknown";
  };
  models: {
    status: ProductHealthStatus;
    model_count: number;
    model_build_count: number;
    failed_build_count: number;
    model_relation_count: number;
    orphan_model_count: number;
    stale_relation_count: number;
    latest_build_at: string | null;
    graph_status: ProductHealthStatus;
  };
  vector_index: {
    status: ProductHealthStatus;
    vector_count: number;
    backlog_count: number;
    failed_job_count: number;
    latest_job_at: string | null;
    retrieval_status: ProductHealthStatus;
  };
  issues: ProductHealthIssue[];
  privacy_boundary: {
    raw_payloads_included: false;
    raw_prompts_included: false;
    raw_logs_included: false;
    pii_included: false;
    source_records_included: false;
    model_contents_included: false;
    vector_values_included: false;
  };
  stored_scope: "sanitized_product_health_metadata_only";
};

export type ControlPanelState = {
  schema_version: "fyralis.byoc.control_panel_state.v1";
  deployment_id: string;
  customer_id: string | null;
  generated_at: string;
  overview: DeploymentOverview;
  sections: ControlPanelSection[];
  actions: ControlPanelAction[];
  agent_fleet: {
    schema_version: "fyralis.byoc.agent_fleet_list.v1";
    result_count: number;
    items: AgentFleetItem[];
    stored_scope: "sanitized_agent_metadata_only";
  };
  product_health: ProductHealth;
  evidence_packages: ReceiptList;
  preflight_reports: ReceiptList;
  runner_evidence: ReceiptList;
  stored_scope: "sanitized_control_panel_metadata_only";
};

export type DeploymentOption = {
  customerId: string;
  deploymentId: string;
  role: ControlPanelAccessGrant["role"];
  expiresAt: string | null;
};
