import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import type { ControlPanelAccessGrantList, ControlPanelState } from "./types";

const deploymentsResponse: ControlPanelAccessGrantList = {
  schema_version: "fyralis.byoc.control_panel_access_grant_list.v1",
  tenant_id: "11111111-1111-4111-8111-111111111111",
  customer_id: "cus_control01",
  active_only: true,
  result_count: 1,
  generated_at: "2026-06-27T12:00:00Z",
  stored_scope: "sanitized_control_panel_access_metadata_only",
  items: [
    {
      schema_version: "fyralis.byoc.control_panel_access_grant.v1",
      tenant_id: "11111111-1111-4111-8111-111111111111",
      customer_id: "cus_control01",
      deployment_ids: ["dep_control01"],
      role: "operator",
      enabled: true,
      granted_at: "2026-06-27T11:00:00Z",
      expires_at: null,
      stored_scope: "sanitized_control_panel_access_metadata_only"
    }
  ]
};

const stateResponse: ControlPanelState = {
  schema_version: "fyralis.byoc.control_panel_state.v1",
  deployment_id: "dep_control01",
  customer_id: "cus_control01",
  generated_at: "2026-06-27T12:00:00Z",
  stored_scope: "sanitized_control_panel_metadata_only",
  overview: {
    schema_version: "fyralis.byoc.deployment_overview.v1",
    deployment_id: "dep_control01",
    customer_id: "cus_control01",
    generated_at: "2026-06-27T12:00:00Z",
    status: "ready",
    next_action: "none",
    stored_scope: "sanitized_deployment_metadata_only",
    agent_summary: {
      enrolled_count: 1,
      passing_count: 1,
      degraded_count: 0,
      failing_count: 0,
      unknown_count: 0,
      connected_count: 1,
      disconnected_count: 0,
      heartbeat_observed_count: 1,
      evidence_package_required_count: 1,
      highest_desired_config_epoch: 2,
      current_desired_revision: "2026.06.27-2",
      mixed_desired_revisions: false,
      latest_heartbeat_accepted_at: "2026-06-27T11:59:00Z",
      latest_desired_state_updated_at: "2026-06-27T11:55:00Z"
    },
    evidence_summary: {
      receipt_count: 1,
      passed_receipt_count: 1,
      failed_receipt_count: 0,
      skipped_receipt_count: 0,
      latest_receipt_id: "evpkg_0123456789abcdef0123456789abcdef",
      latest_ledger_status: "pass",
      latest_required_evidence_passed: true,
      latest_package_accepted_at: "2026-06-27T11:59:00Z"
    },
    preflight_summary: {
      receipt_count: 1,
      passed_receipt_count: 1,
      failed_receipt_count: 0,
      skipped_receipt_count: 0,
      latest_receipt_id: "pfrep_0123456789abcdef0123456789abcdef",
      latest_preflight_status: "pass",
      latest_required_sections_passed: true,
      latest_failed_section_count: 0,
      latest_report_accepted_at: "2026-06-27T11:57:00Z"
    },
    runner_summary: {
      receipt_count: 1,
      passed_receipt_count: 1,
      failed_receipt_count: 0,
      latest_receipt_id: "runev_0123456789abcdef0123456789abcdef",
      latest_runner_status: "pass",
      latest_required_checks_passed: true,
      latest_rollout_action: "apply_revision",
      latest_apply_plan_count: 1,
      latest_artifact_verification_count: 1,
      latest_digest_pinned_artifact_count: 7,
      latest_local_digest_checked_count: 1,
      latest_evidence_accepted_at: "2026-06-27T11:58:00Z"
    }
  },
  sections: [
    {
      key: "deployment_overview",
      status: "ready",
      item_count: 1,
      latest_observed_at: "2026-06-27T12:00:00Z",
      source_schema_version: "fyralis.byoc.deployment_overview.v1"
    },
    {
      key: "agent_fleet",
      status: "ready",
      item_count: 1,
      latest_observed_at: "2026-06-27T11:59:00Z",
      source_schema_version: "fyralis.byoc.agent_fleet_list.v1"
    }
  ],
  actions: [],
  agent_fleet: {
    schema_version: "fyralis.byoc.agent_fleet_list.v1",
    result_count: 1,
    stored_scope: "sanitized_agent_metadata_only",
    items: [
      {
        agent_id: "agt_control01",
        agent_version: "2026.06.27",
        artifact_revision: "2026.06.27-1",
        cloud_provider: "aws",
        region: "us-east-1",
        desired_revision: "2026.06.27-2",
        desired_config_epoch: 2,
        evidence_package_required: true,
        latest_validation_status: "passing",
        latest_control_plane_connected: true,
        latest_component_count: 3,
        latest_ok_component_count: 3,
        latest_degraded_component_count: 0,
        latest_failed_component_count: 0,
        latest_unknown_component_count: 0,
        latest_queued_batches: 0,
        latest_dropped_batches: 0,
        latest_heartbeat_accepted_at: "2026-06-27T11:59:00Z",
        stored_scope: "sanitized_agent_metadata_only"
      }
    ]
  },
  evidence_packages: receiptList("fyralis.byoc.evidence_package_receipt_list.v1"),
  preflight_reports: receiptList("fyralis.byoc.preflight_report_receipt_list.v1"),
  runner_evidence: receiptList("fyralis.byoc.runner_evidence_receipt_list.v1")
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe("BYOC control panel UI", () => {
  it("loads deployments and selected state through bearer-authenticated routes", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(deploymentsResponse))
      .mockResolvedValueOnce(jsonResponse(stateResponse));
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    await userEvent.type(screen.getByLabelText("Bearer token"), "bearer-value");
    await userEvent.type(screen.getByLabelText("Customer"), "cus_control01");
    await userEvent.click(screen.getByRole("button", { name: "Refresh" }));

    expect((await screen.findAllByText("dep_control01")).length).toBeGreaterThan(1);
    expect(screen.getByText("agt_control01")).toBeInTheDocument();
    expect(screen.getByText("Evidence Packages")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0][0]).toContain(
      "/byoc/control-panel/deployments?customer_id=cus_control01"
    );
    expect(fetchMock.mock.calls[1][0]).toContain(
      "/byoc/control-panel/state?deployment_id=dep_control01"
    );
    expect(fetchMock.mock.calls[0][1]?.headers.Authorization).toBe(
      "Bearer bearer-value"
    );
    expect(JSON.stringify(document.body.textContent)).not.toContain(
      "bearer-value"
    );
  });

  it("shows backend errors without rendering stale state", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: false,
        status: 403,
        statusText: "Forbidden",
        text: async () => "control_panel_access: grant_missing"
      })
    );

    render(<App />);
    await userEvent.click(screen.getByRole("button", { name: "Refresh" }));

    expect(
      await screen.findByText(/control_panel_access: grant_missing/)
    ).toBeInTheDocument();
    expect(screen.queryByText("Agent Fleet")).not.toBeInTheDocument();
  });
});

function jsonResponse(payload: unknown): Response {
  return {
    ok: true,
    status: 200,
    statusText: "OK",
    json: async () => payload,
    text: async () => JSON.stringify(payload)
  } as Response;
}

function receiptList(schemaVersion: string) {
  return {
    schema_version: schemaVersion,
    result_count: 1,
    stored_scope: "sanitized_metadata_only" as const,
    items: [
      {
        receipt: {
          receipt_id: "receipt_0123456789abcdef0123456789abcdef",
          status: "accepted",
          ledger_overall_status: "pass",
          accepted_at: "2026-06-27T11:59:00Z"
        }
      }
    ]
  };
}
