import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { SourceMarketplace } from "../components/source-marketplace";
import { ONBOARDING_STEPS, SOURCES } from "../data/mock-data";
import {
  fetchGatewaySourceObservations,
  submitDesignPartnerIntake
} from "../services/onboarding-service";
import type { Customer, SourceConnection } from "../types";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
});

describe("onboarding workflow contract", () => {
  it("preserves the complete production onboarding flow", () => {
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
      "source-setup",
      "source-validation",
      "source-scope",
      "first-sync",
      "ingestion-health",
      "activation",
      "workspace-launch",
      "workspace-home"
    ]);
  });

  it("filters the source marketplace and opens setup for the selected source", async () => {
    const user = userEvent.setup();
    const onSelect = vi.fn();
    const onOpenSetup = vi.fn();
    const connections: SourceConnection[] = [
      {
        sourceId: "slack",
        status: "connected",
        selectedScopes: ["#leadership"],
        backfillWindow: "Last 30 days",
        syncMode: "Limited backfill"
      }
    ];

    render(
      <SourceMarketplace
        sources={SOURCES}
        connections={connections}
        selectedSourceId="slack"
        onSelect={onSelect}
        onOpenSetup={onOpenSetup}
      />
    );

    await user.type(screen.getByLabelText("Search integrations"), "github");

    expect(screen.getByText("GitHub")).toBeInTheDocument();
    expect(screen.queryByText("Slack")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Open setup" }));
    expect(onOpenSetup).toHaveBeenCalledWith("github");
  });
});

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
