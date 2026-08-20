import type {
  ControlPanelAccessGrantList,
  ControlPanelState,
  DeploymentOption
} from "@/src/types";

const GENERATED_AT = "2026-06-29T12:45:00.000Z";

export const SAMPLE_DEPLOYMENTS: ControlPanelAccessGrantList = {
  schema_version: "fyralis.byoc.control_panel_access_grant_list.v1",
  tenant_id: "018ffc55-79e0-7dc1-9f66-5a0625465110",
  customer_id: "cus_acme_finance",
  active_only: true,
  result_count: 1,
  generated_at: GENERATED_AT,
  items: [
    {
      schema_version: "fyralis.byoc.control_panel_access_grant.v1",
      tenant_id: "018ffc55-79e0-7dc1-9f66-5a0625465110",
      customer_id: "cus_acme_finance",
      deployment_ids: ["dep_acme_byoc_pilot"],
      role: "operator",
      enabled: true,
      granted_at: "2026-06-28T10:00:00.000Z",
      expires_at: null,
      stored_scope: "sanitized_control_panel_access_metadata_only"
    }
  ],
  stored_scope: "sanitized_control_panel_access_metadata_only"
};

export const SAMPLE_CONTROL_PANEL_STATE: ControlPanelState = {
  schema_version: "fyralis.byoc.control_panel_state.v1",
  deployment_id: "dep_acme_byoc_pilot",
  customer_id: "cus_acme_finance",
  generated_at: GENERATED_AT,
  overview: {
    schema_version: "fyralis.byoc.deployment_overview.v1",
    deployment_id: "dep_acme_byoc_pilot",
    customer_id: "cus_acme_finance",
    generated_at: GENERATED_AT,
    status: "ready",
    next_action: "none",
    agent_summary: {
      enrolled_count: 2,
      passing_count: 2,
      degraded_count: 0,
      failing_count: 0,
      unknown_count: 0,
      connected_count: 2,
      disconnected_count: 0,
      heartbeat_observed_count: 2,
      evidence_package_required_count: 0,
      highest_desired_config_epoch: 8,
      current_desired_revision: "rev_2026_06_29_01",
      mixed_desired_revisions: false,
      latest_heartbeat_accepted_at: "2026-06-29T12:44:10.000Z",
      latest_desired_state_updated_at: "2026-06-29T12:30:00.000Z"
    },
    evidence_summary: {
      receipt_count: 6,
      passed_receipt_count: 6,
      failed_receipt_count: 0,
      skipped_receipt_count: 0,
      latest_receipt_id: "evd_20260629_1242",
      latest_ledger_status: "passed",
      latest_required_evidence_passed: true,
      latest_package_accepted_at: "2026-06-29T12:42:20.000Z"
    },
    preflight_summary: {
      receipt_count: 3,
      passed_receipt_count: 3,
      failed_receipt_count: 0,
      skipped_receipt_count: 0,
      latest_receipt_id: "pre_20260629_1219",
      latest_preflight_status: "passed",
      latest_required_sections_passed: true,
      latest_failed_section_count: 0,
      latest_report_accepted_at: "2026-06-29T12:19:00.000Z"
    },
    runner_summary: {
      receipt_count: 4,
      passed_receipt_count: 4,
      failed_receipt_count: 0,
      latest_receipt_id: "run_20260629_1244",
      latest_runner_status: "passed",
      latest_required_checks_passed: true,
      latest_rollout_action: "validate",
      latest_apply_plan_count: 2,
      latest_artifact_verification_count: 4,
      latest_digest_pinned_artifact_count: 4,
      latest_local_digest_checked_count: 4,
      latest_evidence_accepted_at: "2026-06-29T12:44:40.000Z"
    },
    stored_scope: "sanitized_deployment_metadata_only"
  },
  sections: [
    {
      key: "deployment_overview",
      status: "ready",
      item_count: 1,
      latest_observed_at: "2026-06-29T12:44:40.000Z",
      source_schema_version: "fyralis.byoc.deployment_overview.v1"
    },
    {
      key: "agent_fleet",
      status: "ready",
      item_count: 2,
      latest_observed_at: "2026-06-29T12:44:10.000Z",
      source_schema_version: "fyralis.byoc.agent_fleet_list.v1"
    },
    {
      key: "product_health",
      status: "ready",
      item_count: 1,
      latest_observed_at: "2026-06-29T12:43:30.000Z",
      source_schema_version: "fyralis.byoc.product_health.v1"
    },
    {
      key: "evidence_packages",
      status: "ready",
      item_count: 6,
      latest_observed_at: "2026-06-29T12:42:20.000Z",
      source_schema_version: "fyralis.byoc.evidence_package_receipt.v1"
    },
    {
      key: "preflight_reports",
      status: "ready",
      item_count: 3,
      latest_observed_at: "2026-06-29T12:19:00.000Z",
      source_schema_version: "fyralis.byoc.preflight_report_receipt.v1"
    },
    {
      key: "runner_evidence",
      status: "ready",
      item_count: 4,
      latest_observed_at: "2026-06-29T12:44:40.000Z",
      source_schema_version: "fyralis.byoc.runner_evidence_receipt.v1"
    }
  ],
  actions: [],
  agent_fleet: {
    schema_version: "fyralis.byoc.agent_fleet_list.v1",
    result_count: 2,
    items: [
      {
        agent_id: "agent-acme-gateway-01",
        agent_version: "2026.06.29",
        artifact_revision: "sha256:40cafe",
        cloud_provider: "aws",
        region: "us-east-1",
        desired_revision: "rev_2026_06_29_01",
        desired_config_epoch: 8,
        evidence_package_required: false,
        latest_validation_status: "passing",
        latest_control_plane_connected: true,
        latest_component_count: 9,
        latest_ok_component_count: 9,
        latest_degraded_component_count: 0,
        latest_failed_component_count: 0,
        latest_unknown_component_count: 0,
        latest_queued_batches: 0,
        latest_dropped_batches: 0,
        latest_heartbeat_accepted_at: "2026-06-29T12:44:10.000Z",
        stored_scope: "sanitized_agent_metadata_only"
      },
      {
        agent_id: "agent-acme-worker-01",
        agent_version: "2026.06.29",
        artifact_revision: "sha256:40cafe",
        cloud_provider: "aws",
        region: "us-east-1",
        desired_revision: "rev_2026_06_29_01",
        desired_config_epoch: 8,
        evidence_package_required: false,
        latest_validation_status: "passing",
        latest_control_plane_connected: true,
        latest_component_count: 11,
        latest_ok_component_count: 11,
        latest_degraded_component_count: 0,
        latest_failed_component_count: 0,
        latest_unknown_component_count: 0,
        latest_queued_batches: 3,
        latest_dropped_batches: 0,
        latest_heartbeat_accepted_at: "2026-06-29T12:43:51.000Z",
        stored_scope: "sanitized_agent_metadata_only"
      }
    ],
    stored_scope: "sanitized_agent_metadata_only"
  },
  product_health: {
    schema_version: "fyralis.byoc.product_health.v1",
    deployment_id: "dep_acme_byoc_pilot",
    customer_id: "cus_acme_finance",
    generated_at: GENERATED_AT,
    observed: true,
    latest_snapshot_id: "health_20260629_1243",
    latest_collected_at: "2026-06-29T12:43:30.000Z",
    overall_status: "ready",
    sources: [
      {
        source: "slack",
        status: "ready",
        auth_status: "ready",
        backfill_status: "idle",
        items_ingested_count: 128,
        items_failed_count: 0,
        queue_depth_count: 0,
        lag_seconds: 3,
        last_success_at: "2026-06-29T12:42:58.000Z"
      },
      {
        source: "github",
        status: "ready",
        auth_status: "ready",
        backfill_status: "idle",
        items_ingested_count: 74,
        items_failed_count: 0,
        queue_depth_count: 2,
        lag_seconds: 11,
        last_success_at: "2026-06-29T12:41:44.000Z"
      },
      {
        source: "gmail",
        status: "unknown",
        auth_status: "not_configured",
        backfill_status: "unknown",
        items_ingested_count: 0,
        items_failed_count: 0,
        queue_depth_count: 0,
        lag_seconds: null,
        last_success_at: null
      }
    ],
    pipeline: {
      status: "ready",
      queue_lag_count: 2,
      dead_letter_count: 0,
      retry_backlog_count: 0,
      dropped_item_count: 0
    },
    think: {
      status: "ready",
      run_count: 16,
      failed_run_count: 0,
      queued_run_count: 1,
      latest_run_at: "2026-06-29T12:40:00.000Z",
      breaker_status: "closed"
    },
    models: {
      status: "ready",
      model_count: 412,
      model_build_count: 18,
      failed_build_count: 0,
      model_relation_count: 923,
      orphan_model_count: 0,
      stale_relation_count: 1,
      latest_build_at: "2026-06-29T12:39:12.000Z",
      graph_status: "ready"
    },
    vector_index: {
      status: "ready",
      vector_count: 11024,
      backlog_count: 18,
      failed_job_count: 0,
      latest_job_at: "2026-06-29T12:42:00.000Z",
      retrieval_status: "ready"
    },
    issues: [],
    privacy_boundary: {
      raw_payloads_included: false,
      raw_prompts_included: false,
      raw_logs_included: false,
      pii_included: false,
      source_records_included: false,
      model_contents_included: false,
      vector_values_included: false
    },
    stored_scope: "sanitized_product_health_metadata_only"
  },
  evidence_packages: {
    schema_version: "fyralis.byoc.evidence_package_receipt_list.v1",
    result_count: 2,
    items: [
      {
        receipt: {
          receipt_id: "evd_20260629_1242",
          status: "accepted",
          accepted_at: "2026-06-29T12:42:20.000Z",
          ledger_overall_status: "passed",
          required_evidence_passed: true
        },
        cloud_provider: "aws",
        region: "us-east-1",
        submitted_at: "2026-06-29T12:42:08.000Z"
      },
      {
        receipt: {
          receipt_id: "evd_20260629_1200",
          status: "accepted",
          accepted_at: "2026-06-29T12:00:22.000Z",
          ledger_overall_status: "passed",
          required_evidence_passed: true
        },
        cloud_provider: "aws",
        region: "us-east-1",
        submitted_at: "2026-06-29T12:00:10.000Z"
      }
    ],
    stored_scope: "sanitized_metadata_only"
  },
  preflight_reports: {
    schema_version: "fyralis.byoc.preflight_report_receipt_list.v1",
    result_count: 1,
    items: [
      {
        receipt: {
          receipt_id: "pre_20260629_1219",
          status: "accepted",
          accepted_at: "2026-06-29T12:19:00.000Z",
          preflight_status: "passed",
          required_sections_passed: true
        },
        cloud_provider: "aws",
        region: "us-east-1",
        submitted_at: "2026-06-29T12:18:41.000Z"
      }
    ],
    stored_scope: "sanitized_metadata_only"
  },
  runner_evidence: {
    schema_version: "fyralis.byoc.runner_evidence_receipt_list.v1",
    result_count: 1,
    items: [
      {
        receipt: {
          receipt_id: "run_20260629_1244",
          status: "accepted",
          accepted_at: "2026-06-29T12:44:40.000Z",
          runner_status: "passed",
          required_checks_passed: true
        },
        cloud_provider: "aws",
        region: "us-east-1",
        submitted_at: "2026-06-29T12:44:22.000Z"
      }
    ],
    stored_scope: "sanitized_metadata_only"
  },
  stored_scope: "sanitized_control_panel_metadata_only"
};

export const SAMPLE_DEPLOYMENT_OPTIONS: DeploymentOption[] = [
  {
    customerId: "cus_acme_finance",
    deploymentId: "dep_acme_byoc_pilot",
    role: "operator",
    expiresAt: null
  }
];
