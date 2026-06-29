import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { describe, expect, it, vi } from "vitest";

import { SourceMarketplace } from "../components/source-marketplace";
import { ONBOARDING_STEPS, SOURCES } from "../data/mock-data";
import type { SourceConnection } from "../types";

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
