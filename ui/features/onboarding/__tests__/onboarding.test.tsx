import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { SourceMarketplace } from "../components/source-marketplace";
import { ONBOARDING_SNAPSHOT, ONBOARDING_STEPS, SOURCES } from "../data/mock-data";
import { StepView, type StepViewProps } from "../flows/step-views";
import {
  autoConnectSourceRehearsal,
  fetchGatewaySourceObservations,
  fetchSourceRehearsalStatus,
  submitDesignPartnerIntake
} from "../services/onboarding-service";
import type { Customer, SourceConnection } from "../types";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("onboarding workflow contract", () => {
  it("keeps source connection as one clean catalog step", () => {
    expect(ONBOARDING_STEPS.map((step) => step.id)).toEqual([
      "get-fyralis",
      "customer-intake",
      "cloud-readiness",
      "setup-package",
      "trust-boundary",
      "preflight",
      "deployment",
      "deployment-validation",
      "source-catalog",
      "workspace-launch",
      "workspace-home"
    ]);
  });

  it("shows sources as a direct connect list", async () => {
    const user = userEvent.setup();
    const onSelect = vi.fn();
    const onConnect = vi.fn();

    render(
      <SourceMarketplace
        sources={SOURCES.filter((source) => source.id === "github")}
        connections={[]}
        selectedSourceId="github"
        onSelect={onSelect}
        onConnect={onConnect}
      />
    );

    expect(screen.getByText("GitHub")).toBeInTheDocument();
    expect(screen.queryByLabelText("Search integrations")).not.toBeInTheDocument();
    expect(screen.queryByText("Engineering")).not.toBeInTheDocument();
    expect(
      screen.queryByText("Repositories, pull requests, issues, and code intelligence.")
    ).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Connect GitHub" }));
    expect(onConnect).toHaveBeenCalledWith("github");
  });

  it("lets persisted waiting approval state recover the provider handoff link", async () => {
    const user = userEvent.setup();
    const onSelect = vi.fn();
    const onConnect = vi.fn();
    const connections: SourceConnection[] = [
      {
        sourceId: "ramp",
        status: "waiting-admin",
        selectedScopes: [],
        backfillWindow: "Last 30 days",
        syncMode: "Limited backfill"
      }
    ];

    render(
      <SourceMarketplace
        sources={SOURCES.filter((source) => source.id === "ramp")}
        connections={connections}
        selectedSourceId="ramp"
        onSelect={onSelect}
        onConnect={onConnect}
      />
    );

    expect(screen.getByText("Approval needed")).toBeInTheDocument();
    await user.click(
      screen.getByRole("button", {
        name: "Get Ramp provider settings link for approval"
      })
    );
    expect(onConnect).toHaveBeenCalledWith("ramp");
  });

  it("turns approval waiting state into an open settings action when a handoff URL exists", async () => {
    const user = userEvent.setup();
    const openMock = vi.fn();
    vi.spyOn(window, "open").mockImplementation(openMock);

    render(
      <SourceMarketplace
        sources={SOURCES.filter((source) => source.id === "ramp")}
        connections={[]}
        selectedSourceId="ramp"
        automationStates={{
          ramp: {
            status: "waiting_admin",
            label: "Approval needed",
            message: "Provider admin approval is blocking completion.",
            actionUrl: "https://developers.ramp.com/",
            actionLabel: "Open settings",
            receiptPathHint: ".fyralis/sources/ramp/browser-agent-receipt.json"
          }
        }}
        onSelect={vi.fn()}
        onConnect={vi.fn()}
      />
    );

    expect(
      screen.getByText(
        "Next: approve the provider prompt or create the requested least-privilege credential, then return here."
      )
    ).toBeInTheDocument();

    await user.click(
      screen.getByRole("button", {
        name: "Open Ramp provider settings for approval"
      })
    );

    expect(openMock).toHaveBeenCalledWith(
      "https://developers.ramp.com/",
      "_blank",
      "noopener,noreferrer"
    );
  });

  it("shows AWS IAM role instructions instead of a generic approval wait", async () => {
    const user = userEvent.setup();
    const openMock = vi.fn();
    vi.spyOn(window, "open").mockImplementation(openMock);

    render(
      <SourceMarketplace
        sources={SOURCES.filter((source) => source.id === "aws")}
        connections={[]}
        selectedSourceId="aws"
        automationStates={{
          aws: {
            status: "waiting_admin",
            label: "Approval needed",
            message:
              "Fyralis queued 22 background steps. Provider admin approval is blocking completion.",
            actionUrl:
              "https://console.aws.amazon.com/cloudformation/home#/stacks/create/template"
          }
        }}
        onSelect={vi.fn()}
        onConnect={vi.fn()}
      />
    );

    expect(
      screen.getByText(
        "Next: sign in to AWS, review the generated read-only CloudFormation role stack, approve IAM creation, then return here."
      )
    ).toBeInTheDocument();

    await user.click(
      screen.getByRole("button", {
        name: "Open AWS provider settings for approval"
      })
    );

    expect(openMock).toHaveBeenCalledWith(
      "https://console.aws.amazon.com/cloudformation/home#/stacks/create/template",
      "_blank",
      "noopener,noreferrer"
    );
    expect(screen.getByText("Open AWS approval")).toBeInTheDocument();
  });

  it("turns missing AWS BYOC runtime into a create-runtime-role action", async () => {
    const user = userEvent.setup();
    const openMock = vi.fn();
    const runtimeRoleUrl =
      "https://ap-south-1.console.aws.amazon.com/cloudformation/home?region=ap-south-1#/stacks/create/template";
    vi.spyOn(window, "open").mockImplementation(openMock);

    render(
      <SourceMarketplace
        sources={SOURCES.filter((source) => source.id === "aws")}
        connections={[]}
        selectedSourceId="aws"
        automationStates={{
          aws: {
            status: "blocked",
            label: "BYOC runtime missing",
            message:
              "Create the Fyralis BYOC source runtime role first. Use the generated CloudFormation template.",
            actionUrl: runtimeRoleUrl,
            actionLabel: "Create runtime role"
          }
        }}
        onSelect={vi.fn()}
        onConnect={vi.fn()}
      />
    );

    expect(screen.getByText("BYOC runtime missing")).toBeInTheDocument();
    expect(
      screen.getByText(
        "Next: create the Fyralis BYOC source runtime role, then connect AWS again."
      )
    ).toBeInTheDocument();
    expect(screen.getByText("Create runtime role")).toBeInTheDocument();

    await user.click(
      screen.getByRole("button", {
        name: "Create runtime role for AWS"
      })
    );

    expect(openMock).toHaveBeenCalledWith(
      runtimeRoleUrl,
      "_blank",
      "noopener,noreferrer"
    );
  });

  it("registers an AWS runtime role from account id after stack approval", async () => {
    const user = userEvent.setup();
    const onRegisterAwsRuntimeRole = vi.fn();

    render(
      <SourceMarketplace
        sources={SOURCES.filter((source) => source.id === "aws")}
        connections={[]}
        selectedSourceId="aws"
        automationStates={{
          aws: {
            status: "blocked",
            label: "BYOC runtime missing",
            message:
              "Create the Fyralis BYOC source runtime role first. Use the generated CloudFormation template.",
            actionUrl:
              "https://ap-south-1.console.aws.amazon.com/cloudformation/home?region=ap-south-1#/stacks/create/template",
            actionLabel: "Create runtime role"
          }
        }}
        onSelect={vi.fn()}
        onConnect={vi.fn()}
        onRegisterAwsRuntimeRole={onRegisterAwsRuntimeRole}
      />
    );

    await user.type(
      screen.getByLabelText("SourceRuntimeRoleArn or AWS account id"),
      "587628268464"
    );
    await user.click(screen.getByRole("button", { name: "Use runtime role" }));

    expect(onRegisterAwsRuntimeRole).toHaveBeenCalledWith(
      "arn:aws:iam::587628268464:role/fyralis-source-runtime"
    );
  });

  it("finalizes AWS from the CloudFormation RoleArn output", async () => {
    const user = userEvent.setup();
    const onFinalizeAwsSourceRole = vi.fn();

    render(
      <SourceMarketplace
        sources={SOURCES.filter((source) => source.id === "aws")}
        connections={[]}
        selectedSourceId="aws"
        automationStates={{
          aws: {
            status: "waiting_admin",
            label: "Approval needed",
            message: "AWS stack approval is complete.",
            actionUrl:
              "https://us-east-1.console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/template",
            actionLabel: "Open AWS approval"
          }
        }}
        onSelect={vi.fn()}
        onConnect={vi.fn()}
        onFinalizeAwsSourceRole={onFinalizeAwsSourceRole}
      />
    );

    await user.type(
      screen.getByLabelText("RoleArn from CloudFormation Outputs"),
      "587628268464"
    );
    await user.click(screen.getByRole("button", { name: "Finalize AWS" }));

    expect(onFinalizeAwsSourceRole).toHaveBeenCalledWith(
      "arn:aws:iam::587628268464:role/fyralis-source-readonly"
    );
  });

  it("keeps idle source rows minimal", () => {
    render(
      <SourceMarketplace
        sources={SOURCES.filter((source) => source.id === "ramp")}
        connections={[]}
        selectedSourceId="ramp"
        onSelect={vi.fn()}
        onConnect={vi.fn()}
      />
    );

    expect(screen.getByText("Ramp")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Connect Ramp" })).toBeInTheDocument();
    expect(
      screen.queryByText("Spend management and finance events.")
    ).not.toBeInTheDocument();
  });

  it("does not seed fake source connection statuses", () => {
    render(
      <StepView
        stepId="source-catalog"
        props={stepViewProps({
          selectedSource: ONBOARDING_SNAPSHOT.sources[0]
        })}
      />
    );

    expect(screen.queryByText("Draft")).not.toBeInTheDocument();
    expect(screen.queryByText("Ready")).not.toBeInTheDocument();
    expect(screen.queryByText("Connected")).not.toBeInTheDocument();
    expect(screen.queryByText("Channels, events, and consented DMs.")).not.toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: /^Connect / })).toHaveLength(
      ONBOARDING_SNAPSHOT.sources.length
    );
  });

  it("starts source automation from the minimal source catalog", async () => {
    const user = userEvent.setup();
    const props = stepViewProps({
      selectedSource: ONBOARDING_SNAPSHOT.sources[0]
    });
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(rampNativePreparePayload()), { status: 200 })
    );
    const openMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    vi.spyOn(window, "open").mockImplementation(openMock);

    render(
      <StepView
        stepId="source-catalog"
        props={props}
      />
    );

    await user.click(screen.getByRole("button", { name: "Connect Ramp" }));

    expect(fetchMock).toHaveBeenCalledWith(
      "http://localhost:8000/platform/onboarding/sources/ramp/rehearsal/auto-connect",
      expect.objectContaining({ method: "POST" })
    );
    expect(await screen.findByText("Approval needed")).toBeInTheDocument();
    expect(await screen.findByText(/background steps/)).toBeInTheDocument();
    expect(openMock).not.toHaveBeenCalled();
    expect(props.updateConnection).toHaveBeenCalledWith(
      "ramp",
      expect.objectContaining({
        status: "waiting-admin",
        receiptId: ".fyralis/sources/ramp/browser-agent-receipt.json"
      })
    );
  });

  it("shows retry when source automation returns an error state", async () => {
    const user = userEvent.setup();
    const props = stepViewProps({
      selectedSource: ONBOARDING_SNAPSHOT.sources[0]
    });
    const payload = rampNativePreparePayload();
    payload.auto_connect.state = "error";
    payload.auto_connect.label = "Retry";
    payload.auto_connect.message = "Ramp setup needs attention.";
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(payload), { status: 200 })
    );
    const openMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    vi.spyOn(window, "open").mockImplementation(openMock);

    render(<StepView stepId="source-catalog" props={props} />);

    await user.click(screen.getByRole("button", { name: "Connect Ramp" }));

    expect(await screen.findByText("Retry")).toBeInTheDocument();
    expect(await screen.findByText("Ramp setup needs attention.")).toBeInTheDocument();
    expect(props.updateConnection).toHaveBeenCalledWith(
      "ramp",
      expect.objectContaining({ status: "error" })
    );
    expect(openMock).not.toHaveBeenCalled();
  });

  it("resumes polling for persisted waiting source connections", async () => {
    vi.useFakeTimers();
    const selectedSource = ONBOARDING_SNAPSHOT.sources.find(
      (source) => source.id === "ramp"
    );
    expect(selectedSource).toBeDefined();
    const waitingConnection: SourceConnection = {
      sourceId: "ramp",
      status: "waiting-admin",
      selectedScopes: [],
      backfillWindow: "Last 30 days",
      syncMode: "Limited backfill",
      receiptId: ".fyralis/sources/ramp/browser-agent-receipt.json"
    };
    const props = stepViewProps({
      selectedSource: selectedSource!,
      connections: [waitingConnection]
    });
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(rampConnectedStatusPayload()), { status: 200 })
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<StepView stepId="source-catalog" props={props} />);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(8000);
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "http://localhost:8000/platform/onboarding/sources/ramp/rehearsal/status",
      expect.objectContaining({ method: "GET" })
    );
    expect(props.updateConnection).toHaveBeenCalledWith(
      "ramp",
      expect.objectContaining({
        status: "connected",
        receiptId: ".fyralis/sources/ramp/browser-agent-receipt.json"
      })
    );
    expect(props.landSourceObservations).toHaveBeenCalledWith(
      "ramp",
      [
        expect.objectContaining({
          id: "obs_ramp_connected",
          sourceChannel: "gateway:ramp:connection-proof"
        })
      ]
    );
  });

  it("shows background browser-agent progress while polling", async () => {
    vi.useFakeTimers();
    const selectedSource = ONBOARDING_SNAPSHOT.sources.find(
      (source) => source.id === "ramp"
    );
    expect(selectedSource).toBeDefined();
    const waitingConnection: SourceConnection = {
      sourceId: "ramp",
      status: "waiting-admin",
      selectedScopes: [],
      backfillWindow: "Last 30 days",
      syncMode: "Limited backfill",
      receiptId: ".fyralis/sources/ramp/browser-agent-receipt.json"
    };
    const props = stepViewProps({
      selectedSource: selectedSource!,
      connections: [waitingConnection]
    });
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(rampWaitingStatusPayload("running")), {
        status: 200
      })
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<StepView stepId="source-catalog" props={props} />);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(8000);
    });

    expect(screen.getByText("Running")).toBeInTheDocument();
    expect(
      screen.getByText("Source setup is running in the customer cloud.")
    ).toBeInTheDocument();
    expect(props.updateConnection).not.toHaveBeenCalledWith(
      "ramp",
      expect.objectContaining({ status: "connected" })
    );
  });

  it("keeps legacy source lifecycle views on the clean source catalog", () => {
    const selectedSource = ONBOARDING_SNAPSHOT.sources.find(
      (source) => source.id === "ramp"
    );
    expect(selectedSource).toBeDefined();

    render(
      <StepView
        stepId="ingestion-health"
        props={stepViewProps({
          selectedSource: selectedSource!,
          selectedConnection: ONBOARDING_SNAPSHOT.connections.find(
            (connection) => connection.sourceId === "ramp"
          )
        })}
      />
    );

    expect(
      screen.getByRole("heading", { name: "Sources" })
    ).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: /^Connect / }).length).toBeGreaterThan(
      0
    );
    expect(
      screen.queryByRole("button", { name: "Run Ramp setup agent" })
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText(/admin-present browser agent/)
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Manual override")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("checkbox", { name: /Enable live Ramp webhooks/ })
    ).not.toBeInTheDocument();
  });
});

function stepViewProps({
  selectedSource,
  selectedConnection,
  connections
}: {
  selectedSource: StepViewProps["selectedSource"];
  selectedConnection?: StepViewProps["selectedConnection"];
  connections?: StepViewProps["connections"];
}): StepViewProps {
  return {
    snapshot: ONBOARDING_SNAPSHOT,
    selectedPlan: "design-partner-byoc",
    onboardingIntent: null,
    customer: ONBOARDING_SNAPSHOT.customer,
    readiness: ONBOARDING_SNAPSHOT.readiness,
    workspace: ONBOARDING_SNAPSHOT.workspace,
    selectedSource,
    selectedConnection,
    connections: connections ?? ONBOARDING_SNAPSHOT.connections,
    sourceValidation: ONBOARDING_SNAPSHOT.sourceValidation,
    syncJobs: ONBOARDING_SNAPSHOT.syncJobs,
    sourceObservations: ONBOARDING_SNAPSHOT.sourceObservations,
    launchReady: false,
    choosePlan: vi.fn(),
    setOnboardingIntent: vi.fn(),
    updateCustomer: vi.fn(),
    updateReadiness: vi.fn(),
    selectSource: vi.fn(),
    updateConnection: vi.fn(),
    setSourceValidation: vi.fn(),
    upsertSyncJob: vi.fn(),
    landSourceObservations: vi.fn(),
    setLaunchReady: vi.fn(),
    goTo: vi.fn(),
    advance: vi.fn()
  };
}

function rampNativePreparePayload() {
  return {
    enabled: true,
    source: "ramp",
    tenant_id: "00000000-0000-7000-8000-000000000001",
    actor_id: "00000000-0000-7000-8000-000000000002",
    gateway_api_base: "http://localhost:8000",
    provider_ingress_url: "https://fyralis-ingress.acme.example",
    oauth_redirect_url: null,
    events_request_url: "https://fyralis-ingress.acme.example/webhooks/ramp",
    install_url: null,
    provider_console_url: "https://developers.ramp.com/",
    authorization_mode: "customer_local_provider_refs",
    missing_configuration: [],
    required_inputs: ["access_token_or_client_credentials"],
    optional_inputs: ["business_id", "base_url", "entity_scope", "webhook_verifier_token"],
    finalize_mode: "native_finalizer_required",
    automation_profile: {
      automation_level: "fully_automated_after_customer_ref",
      method: "oauth_client_credentials",
      minimum_human_inputs: ["access_token_or_client_credentials"],
      optional_hints: ["business_id", "base_url", "entity_scope", "webhook_verifier_token"],
      automated_actions: [
        "prepare provider handoff and gateway routes",
        "validate required customer-owned refs are present",
        "discover business scope, transactions, reimbursements, cards, and users",
        "generate least-privilege connection contract",
        "create encrypted secret refs in the customer cloud"
      ],
      human_steps: [
        {
          id: "create_provider_client_credentials",
          label: "Create a least-privilege Ramp OAuth client credential or access token.",
          reason:
            "The provider requires customer-approved credential material before Fyralis can verify access.",
          can_agent_complete: false
        }
      ],
      agent_discovery_target:
        "business scope, cards, transactions, vendors, and reimbursements",
      post_connect_actions: [
        "store encrypted customer-cloud refs",
        "register source installation metadata",
        "emit onboarding trigger"
      ],
      human_step_count: 1
    },
    browser_agent: {
      source: "ramp",
      provider_console_url: "https://developers.ramp.com/",
      settings_targets: [
        "OAuth app settings",
        "business settings",
        "webhook settings"
      ],
      agent_collects: ["business id", "transaction/reimbursement/card/user streams"],
      agent_generates: ["webhook verifier token ref", "Ramp spend scope contract"],
      human_gates: [
        "admin signs in and completes MFA when prompted",
        "admin creates or approves a least-privilege service credential"
      ],
      completion_checks: ["source install status is pollable"]
    },
    browser_agent_run: rampBrowserAgentRun(),
    bearer_token: "session-token",
    session_expires_at: "2026-07-01T10:30:00Z",
    state_expires_in_seconds: null,
    status: {
      source: "ramp",
      installed: false,
      installation: null,
      trigger_count: 0,
      consumed_trigger_count: 0,
      run_status_counts: {},
      shard_state_counts: {},
      observation_count: 0,
      observations: [],
      unresolved_failure_count: 0,
      bearer_token: "session-token",
      session_expires_at: "2026-07-01T10:30:00Z",
      next_action: "Submit the required Ramp connection details."
    },
    auto_connect: {
      state: "admin_gate",
      label: "Admin gate",
      message:
        "Fyralis prepared Ramp. Only provider-required approval or credential creation remains.",
      human_step_count: 1,
      human_steps: [
        {
          id: "create_provider_token",
          label: "Create a least-privilege Ramp token or service user.",
          reason:
            "Fyralis cannot mint provider-owned credentials without customer approval.",
          can_agent_complete: false
        }
      ],
      automated_actions: [
        "prepare provider handoff and gateway routes",
        "validate required customer-owned refs are present"
      ],
      browser_agent: {
        source: "ramp",
        provider_console_url: "https://developers.ramp.com/",
        settings_targets: [
          "developer app settings",
          "business settings",
          "webhook settings"
        ],
        agent_collects: ["business id", "spend entity streams"],
        agent_generates: [
          "webhook verifier token ref",
          "Ramp spend scope contract"
        ],
        human_gates: [
          "admin signs in and completes MFA when prompted",
          "admin creates or approves a least-privilege service credential"
        ],
        completion_checks: ["source install status is pollable"]
      },
      browser_agent_run: rampBrowserAgentRun(),
      automation_run: {
        schema_version: "fyralis.byoc.source.auto_connect_run.v1",
        source: "ramp",
        status: "waiting_for_admin",
        launch_mode: "customer_cloud_admin_present_browser",
        can_start: true,
        handoff_url: "https://developers.ramp.com/",
        current_action_id: "open_provider_settings",
        automated_action_count: 7,
        human_action_count: 1,
        native_connect_kind: "ramp_native_connect",
        native_payload_template_path_hint:
          ".fyralis/sources/ramp/browser-agent-provider-setup/native-payload-template.json",
        provider_setup_output_dir_hint:
          ".fyralis/sources/ramp/browser-agent-provider-setup",
        receipt_path_hint: ".fyralis/sources/ramp/browser-agent-receipt.json",
        background_status: "queued",
        background_queued_at: "2026-07-01T10:29:00Z",
        background_started_at: null,
        background_finished_at: null,
        background_runner_mode: "admin_present_browser_dom",
        run_artifact_path_hint: ".fyralis/sources/ramp/connection.json",
        command_preview:
          "fyralis byoc source browser-agent --source ramp --execute-browser-dom --interactive-admin --execute-native",
        command_args: [
          "fyralis",
          "byoc",
          "source",
          "browser-agent",
          "--source",
          "ramp",
          "--execute-browser-dom",
          "--interactive-admin",
          "--execute-native"
        ]
      },
      install_url: null
    }
  };
}

function rampConnectedStatusPayload() {
  return {
    source: "ramp",
    installed: true,
    installation: {
      installation_id: "019f4000-0000-7000-8000-000000000123",
      enabled: true,
      has_secret: true,
      installed_at: "2026-07-01T10:35:00Z",
      details: {}
    },
    trigger_count: 1,
    consumed_trigger_count: 0,
    run_status_counts: {},
    shard_state_counts: {},
    observation_count: 1,
    observations: [
      {
        id: "obs_ramp_connected",
        kind: "connection_proof",
        source_channel: "gateway:ramp:connection-proof",
        occurred_at: "2026-07-01T10:35:00Z",
        content_text: "Ramp connection proof landed."
      }
    ],
    unresolved_failure_count: 0,
    bearer_token: "session-token",
    session_expires_at: "2026-07-01T10:30:00Z",
    auto_connect_run: rampAutoConnectRun({
      status: "connected",
      backgroundStatus: "connected",
      currentActionId: null
    }),
    next_action: "Ramp is connected."
  };
}

function rampWaitingStatusPayload(backgroundStatus: string) {
  return {
    source: "ramp",
    installed: false,
    installation: null,
    trigger_count: 0,
    consumed_trigger_count: 0,
    run_status_counts: {},
    shard_state_counts: {},
    observation_count: 0,
    observations: [],
    unresolved_failure_count: 0,
    bearer_token: "session-token",
    session_expires_at: "2026-07-01T10:30:00Z",
    auto_connect_run: rampAutoConnectRun({
      status: "waiting_for_admin",
      backgroundStatus
    }),
    next_action: "Ramp setup is running."
  };
}

function rampAutoConnectRun({
  status,
  backgroundStatus,
  currentActionId = "open_provider_settings"
}: {
  status: string;
  backgroundStatus: string;
  currentActionId?: string | null;
}) {
  return {
    schema_version: "fyralis.byoc.source.auto_connect_run.v1",
    source: "ramp",
    status,
    launch_mode: "customer_cloud_admin_present_browser",
    can_start: true,
    handoff_url: "https://developers.ramp.com/",
    current_action_id: currentActionId,
    automated_action_count: 7,
    human_action_count: 1,
    native_connect_kind: "ramp_native_connect",
    native_payload_template_path_hint:
      ".fyralis/sources/ramp/browser-agent-provider-setup/native-payload-template.json",
    provider_setup_output_dir_hint:
      ".fyralis/sources/ramp/browser-agent-provider-setup",
    receipt_path_hint: ".fyralis/sources/ramp/browser-agent-receipt.json",
    background_status: backgroundStatus,
    background_queued_at: "2026-07-01T10:29:00Z",
    background_started_at:
      backgroundStatus === "queued" ? null : "2026-07-01T10:29:05Z",
    background_finished_at:
      backgroundStatus === "connected" ? "2026-07-01T10:34:00Z" : null,
    background_runner_mode: "admin_present_browser_dom",
    run_artifact_path_hint: ".fyralis/sources/ramp/connection.json",
    command_preview:
      "fyralis byoc source browser-agent --source ramp --execute-browser-dom --interactive-admin --execute-native",
    command_args: [
      "fyralis",
      "byoc",
      "source",
      "browser-agent",
      "--source",
      "ramp",
      "--execute-browser-dom",
      "--interactive-admin",
      "--execute-native"
    ]
  };
}

function rampBrowserAgentRun() {
  return {
    schema_version: "fyralis.byoc.source.browser_agent_run.v1",
    source: "ramp",
    state: "waiting_for_admin",
    launch_mode: "customer_cloud_admin_present_browser",
    can_start: true,
    handoff_url: "https://developers.ramp.com/",
    handoff_kind: "provider_console",
    provider_console_url: "https://developers.ramp.com/",
    oauth_redirect_url: null,
    events_request_url: "https://fyralis-ingress.acme.example/webhooks/ramp",
    settings_targets: [
      "developer app settings",
      "business settings",
      "webhook settings"
    ],
    agent_collects: ["business id", "spend entity streams"],
    agent_generates: ["webhook verifier token ref", "Ramp spend scope contract"],
    human_gates: [
      {
        id: "create_provider_token",
        label: "Create a least-privilege Ramp token or service user.",
        reason:
          "Fyralis cannot mint provider-owned credentials without customer approval.",
        status: "waiting",
        can_agent_complete: false
      }
    ],
    completion_checks: [
      {
        name: "source install status is pollable",
        status: "pending"
      }
    ],
    action_queue: [
      {
        id: "open_provider_settings",
        owner: "fyralis_agent",
        status: "ready",
        label: "Open provider settings in the customer-cloud browser."
      }
    ],
    current_action: {
      id: "open_provider_settings",
      owner: "fyralis_agent",
      status: "ready",
      label: "Open provider settings in the customer-cloud browser."
    },
    automated_action_count: 1,
    human_action_count: 1
  };
}

describe("onboarding service recovery", () => {
  it("reads gateway observations with an in-memory bearer token", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          items: [
            {
              id: "obs_slack_1",
              kind: "signal",
              source_channel: "slack:message",
              occurred_at: "2026-07-01T09:30:00Z",
              content_text: "historical Slack backfill landed"
            },
            {
              id: "obs_github_1",
              kind: "signal",
              source_channel: "github:event",
              occurred_at: "2026-07-01T09:31:00Z",
              content_text: "GitHub event landed"
            }
          ],
          stub: false,
          source: "substrate"
        }),
        { status: 200 }
      )
    );
    vi.stubGlobal("fetch", fetchMock);

    const observations = await fetchGatewaySourceObservations({
      apiBase: "https://fyralis-ingress.acme.example",
      bearerToken: "session-token",
      sourceId: "slack"
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "https://fyralis-ingress.acme.example/observations?limit=50&source=slack",
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer session-token"
        })
      })
    );
    expect(observations).toHaveLength(1);
    expect(observations[0]).toMatchObject({
      id: "obs_slack_1",
      sourceId: "slack",
      origin: "gateway",
      sourceChannel: "slack:message",
      evidencePath: "gateway:/observations/obs_slack_1"
    });
  });

  it("normalizes source ids at the gateway boundary", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          items: [
            {
              id: "obs_calendar_1",
              kind: "event",
              source_channel: "google_calendar:event",
              occurred_at: "2026-07-01T09:30:00Z",
              content_text: "calendar backfill landed"
            }
          ],
          stub: false,
          source: "substrate"
        }),
        { status: 200 }
      )
    );
    vi.stubGlobal("fetch", fetchMock);

    const observations = await fetchGatewaySourceObservations({
      apiBase: "https://fyralis-ingress.acme.example",
      bearerToken: "session-token",
      sourceId: "google-calendar"
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "https://fyralis-ingress.acme.example/observations?limit=50&source=google_calendar",
      expect.anything()
    );
    expect(observations).toHaveLength(1);
    expect(observations[0]).toMatchObject({
      id: "obs_calendar_1",
      sourceId: "google-calendar",
      sourceChannel: "google_calendar:event"
    });
  });

  it("maps background source run state from rehearsal status", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(rampWaitingStatusPayload("running")), {
        status: 200
      })
    );
    vi.stubGlobal("fetch", fetchMock);

    const status = await fetchSourceRehearsalStatus({
      apiBase: "https://fyralis-ingress.acme.example",
      sourceId: "ramp"
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "https://fyralis-ingress.acme.example/platform/onboarding/sources/ramp/rehearsal/status",
      expect.objectContaining({ method: "GET" })
    );
    expect(status.autoConnectRun).toMatchObject({
      sourceId: "ramp",
      backgroundStatus: "running",
      receiptPathHint: ".fyralis/sources/ramp/browser-agent-receipt.json"
    });
  });

  it("preserves ui source ids when starting source auto-connect", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          enabled: true,
          source: "google_calendar",
          tenant_id: "00000000-0000-7000-8000-000000000001",
          actor_id: "00000000-0000-7000-8000-000000000002",
          gateway_api_base: "https://fyralis-ingress.acme.example",
          provider_ingress_url: "https://fyralis-ingress.acme.example",
          oauth_redirect_url: null,
          events_request_url: null,
          install_url: null,
          provider_console_url: "https://admin.google.com/ac/owl/domainwidedelegation",
          authorization_mode: "customer_local_provider_refs",
          missing_configuration: [],
          required_inputs: ["workspace_domain"],
          optional_inputs: [],
          finalize_mode: "native_finalizer_required",
          automation_profile: {
            automation_level: "automated_after_workspace_dwd_authorization",
            method: "dwd",
            minimum_human_inputs: ["workspace_domain"],
            optional_hints: [],
            automated_actions: [],
            human_steps: [],
            agent_discovery_target: "calendars",
            post_connect_actions: [],
            human_step_count: 0
          },
          bearer_token: "session-token",
          session_expires_at: "2026-07-01T10:30:00Z",
          state_expires_in_seconds: null,
          status: {
            source: "google_calendar",
            installed: false,
            installation: null,
            trigger_count: 0,
            consumed_trigger_count: 0,
            run_status_counts: {},
            shard_state_counts: {},
            observation_count: 0,
            observations: [],
            unresolved_failure_count: 0,
            bearer_token: "session-token",
            session_expires_at: "2026-07-01T10:30:00Z",
            next_action: "Submit the required Google Calendar connection details."
          },
          auto_connect: {
            state: "running",
            label: "Running",
            message: "Fyralis is running Google Calendar setup.",
            human_step_count: 0,
            human_steps: [],
            automated_actions: ["open provider settings"],
            install_url: null
          }
        }),
        { status: 200 }
      )
    );
    vi.stubGlobal("fetch", fetchMock);

    const prepared = await autoConnectSourceRehearsal({
      apiBase: "https://fyralis-ingress.acme.example",
      sourceId: "google-calendar"
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "https://fyralis-ingress.acme.example/platform/onboarding/sources/google_calendar/rehearsal/auto-connect",
      expect.anything()
    );
    expect(prepared.sourceId).toBe("google-calendar");
    expect(prepared.status.sourceId).toBe("google-calendar");
    expect(prepared.finalizeMode).toBe("native_finalizer_required");
    expect(prepared.autoConnect.state).toBe("running");
    expect(prepared.autoConnect.automatedActions).toEqual([
      "open provider settings"
    ]);
  });

  it("sends AWS deployment context when starting AWS auto-connect", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          enabled: true,
          source: "aws",
          tenant_id: "00000000-0000-7000-8000-000000000001",
          actor_id: "00000000-0000-7000-8000-000000000002",
          gateway_api_base: "https://fyralis-ingress.acme.example",
          provider_ingress_url: "https://fyralis-ingress.acme.example",
          oauth_redirect_url: null,
          events_request_url: null,
          install_url: null,
          provider_console_url:
            "https://console.aws.amazon.com/cloudformation/home#/stacks/create/template",
          authorization_mode: "customer_local_provider_refs",
          missing_configuration: [],
          required_inputs: ["role_arn"],
          optional_inputs: [],
          finalize_mode: "native_finalizer_required",
          automation_profile: {
            automation_level: "admin_present_browser_agent",
            method: "iam_role",
            minimum_human_inputs: ["aws_console_approval"],
            optional_hints: [],
            automated_actions: [],
            human_steps: [],
            agent_discovery_target: "account inventory",
            post_connect_actions: [],
            human_step_count: 0
          },
          bearer_token: "session-token",
          session_expires_at: "2026-07-01T10:30:00Z",
          state_expires_in_seconds: null,
          status: {
            source: "aws",
            installed: false,
            installation: null,
            trigger_count: 0,
            consumed_trigger_count: 0,
            run_status_counts: {},
            shard_state_counts: {},
            observation_count: 0,
            observations: [],
            unresolved_failure_count: 0,
            bearer_token: "session-token",
            session_expires_at: "2026-07-01T10:30:00Z",
            next_action: "Approve the AWS source role stack."
          },
          auto_connect: {
            state: "running",
            label: "Running",
            message: "Fyralis is preparing AWS approval.",
            human_step_count: 0,
            human_steps: [],
            automated_actions: [],
            install_url: null
          }
        }),
        { status: 200 }
      )
    );
    vi.stubGlobal("fetch", fetchMock);

    await autoConnectSourceRehearsal({
      apiBase: "https://fyralis-ingress.acme.example",
      sourceId: "aws",
      deploymentContext: {
        awsRegion: "us-west-2",
        awsAssumingPrincipalArn:
          "arn:aws:iam::587628268464:role/fyralis-runtime"
      }
    });

    expect(fetchMock).toHaveBeenCalledWith(
      "https://fyralis-ingress.acme.example/platform/onboarding/sources/aws/rehearsal/auto-connect",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          "Content-Type": "application/json"
        }),
        body: JSON.stringify({
          deployment_context: {
            aws_region: "us-west-2",
            aws_assuming_principal_arn:
              "arn:aws:iam::587628268464:role/fyralis-runtime"
          }
        })
      })
    );
  });

  it("recreates stale backend intent records before resubmitting intake", async () => {
    vi.stubEnv("NEXT_PUBLIC_FYRALIS_API_BASE", "http://localhost:8000");
    const customer: Customer = {
      company: "Alpen Labs",
      setupOwnerEmail: "alpen-owner@google.com",
      targetCloud: "AWS"
    };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({ detail: { error: "onboarding_intent_not_found" } }),
          { status: 404 }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            schema_version: "fyralis.platform.onboarding_intent.v1",
            intent_id: "ofi_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            plan_code: "design_partner_byoc_pilot",
            procurement_channel: "design_partner",
            entrypoint: "get_fyralis",
            status: "draft",
            customer_id: null,
            tenant_id: null,
            deployment_id: null,
            company_name: null,
            setup_owner_email: null,
            target_cloud: null,
            created_at: "2026-07-01T00:00:00Z",
            updated_at: "2026-07-01T00:00:00Z",
            stored_scope: "sanitized_onboarding_metadata_only"
          }),
          { status: 201 }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            schema_version: "fyralis.platform.onboarding_intent.v1",
            intent_id: "ofi_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            plan_code: "design_partner_byoc_pilot",
            procurement_channel: "design_partner",
            entrypoint: "get_fyralis",
            status: "workspace_created",
            customer_id: "cus_alpen01",
            tenant_id: "00000000-0000-7000-8000-000000000001",
            deployment_id: "dep_alpen01",
            company_name: "Alpen Labs",
            setup_owner_email: "alpen-owner@google.com",
            target_cloud: "aws",
            created_at: "2026-07-01T00:00:00Z",
            updated_at: "2026-07-01T00:01:00Z",
            stored_scope: "sanitized_onboarding_metadata_only"
          }),
          { status: 200 }
        )
      );
    vi.stubGlobal("fetch", fetchMock);

    const result = await submitDesignPartnerIntake(
      "ofi_11111111111111111111111111111111",
      customer
    );

    expect(result.status).toBe("workspace_created");
    expect(result.intent_id).toBe("ofi_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock.mock.calls[0][0]).toContain(
      "/platform/onboarding/intents/ofi_11111111111111111111111111111111/design-partner-intake"
    );
    expect(fetchMock.mock.calls[2][0]).toContain(
      "/platform/onboarding/intents/ofi_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/design-partner-intake"
    );
  });
});
