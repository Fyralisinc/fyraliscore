import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { FigmaOAuthConnectionCard } from "../components/figma-oauth-connection-card";
import {
  canonicalFigmaFileUrl,
  normalizeFileUrls,
  startFigmaOAuth,
} from "../services/figma-oauth-service";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe("Figma OAuth connection card", () => {
  it("accepts selected Figma URLs, starts OAuth, and uses only an in-memory gateway token", async () => {
    const user = userEvent.setup();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({ ok: true, state: "not_connected", file_count: 0 }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          ok: true,
          state: "ready_for_provider_approval",
          authorization_url: "https://www.figma.com/oauth?state=safe-state",
          state_expires_in_seconds: 600,
          requested_file_count: 1,
        }),
      );
    vi.stubGlobal("fetch", fetchMock);
    const authorize = vi.fn();

    render(
      <FigmaOAuthConnectionCard
        apiBase="https://gateway.example.test"
        gatewayToken="short-lived-session-token"
        onAuthorize={authorize}
      />,
    );

    await screen.findByText(/No Figma connection yet/i);
    await user.type(
      screen.getByLabelText("Figma file URLs"),
      "https://www.figma.com/design/CheckoutKey/Checkout",
    );
    await user.click(
      screen.getByRole("button", { name: /Continue with Figma/i }),
    );

    await waitFor(() => {
      expect(authorize).toHaveBeenCalledWith(
        "https://www.figma.com/oauth?state=safe-state",
      );
    });
    expect(fetchMock).toHaveBeenLastCalledWith(
      "https://gateway.example.test/integrations/figma/oauth/start",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          Authorization: "Bearer short-lived-session-token",
        }),
      }),
    );
    expect(localStorage.getItem("short-lived-session-token")).toBeNull();
    expect(sessionStorage.getItem("short-lived-session-token")).toBeNull();
  });

  it("shows snapshot observation proof returned by the customer-cloud gateway", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        ok: true,
        state: "connected",
        installation_id: "figma-install-1",
        file_count: 1,
        selected_file_count: 1,
        synced_file_count: 1,
        observation_count: 1,
        latest_observation_at: "2026-07-12T08:00:00Z",
        latest_observation: {
          id: "obs-figma-snapshot",
          kind: "figma:file_snapshot",
          content_text: "Checkout Redesign snapshot is ready.",
          artifact_id: "blob-figma-document",
        },
        files: [
          {
            file_key: "CheckoutKey",
            file_name: "Checkout Redesign",
            observation_count: 1,
          },
        ],
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<FigmaOAuthConnectionCard apiBase="https://gateway.example.test" />);

    expect(await screen.findByText("Observation landed")).toBeInTheDocument();
    expect(
      screen.getByText("Checkout Redesign snapshot is ready."),
    ).toBeInTheDocument();
    expect(screen.getByText("Checkout Redesign")).toBeInTheDocument();
    expect(screen.getByText("1 observations")).toBeInTheDocument();
  });

  it("keeps BYOC-owned Figma app setup with the deployment administrator", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        ok: true,
        state: "deployment_setup_required",
        setup_owner: "deployment_admin",
        next_action: "Ask a deployment administrator to configure Figma OAuth.",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<FigmaOAuthConnectionCard apiBase="https://gateway.example.test" />);

    expect(
      await screen.findByRole("heading", {
        name: "Deployment administrator setup required",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/uses its own Figma OAuth app/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/No Figma credentials or configuration values are shown/i),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", {
        name: "Deployment administrators: check setup in Control Panel",
      }),
    ).toHaveAttribute("href", "/host/control-panel");
    expect(screen.queryByLabelText("Figma file URLs")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /Continue with Figma/i }),
    ).not.toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("never uses the public local token bridge in production", async () => {
    vi.stubEnv("NODE_ENV", "production");
    vi.stubEnv("NEXT_PUBLIC_FYRALIS_GATEWAY_TOKEN", "must-not-be-sent");
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ ok: true, state: "not_connected", file_count: 0 }),
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<FigmaOAuthConnectionCard apiBase="https://gateway.example.test" />);

    await screen.findByText(/No Figma connection yet/i);
    expect(fetchMock).toHaveBeenCalledWith(
      "https://gateway.example.test/integrations/figma/connect/status",
      expect.objectContaining({
        headers: expect.not.objectContaining({
          Authorization: "Bearer must-not-be-sent",
        }),
      }),
    );
  });

  it("canonicalizes selected file URLs and rejects a non-Figma URL", async () => {
    expect(
      canonicalFigmaFileUrl(
        "https://www.figma.com/design/CheckoutKey/Checkout?node-id=1-2",
      ),
    ).toBe("https://www.figma.com/design/CheckoutKey");
    expect(
      normalizeFileUrls([
        "https://www.figma.com/file/ABC123/One, https://www.figma.com/file/ABC123/Duplicate",
      ]),
    ).toEqual(["https://www.figma.com/file/ABC123"]);
    expect(() => normalizeFileUrls(["https://example.com/not-figma"])).toThrow(
      /not a valid Figma file URL/i,
    );
  });

  it("sends the documented OAuth-start payload", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        ok: true,
        state: "ready_for_provider_approval",
        authorization_url: "https://www.figma.com/oauth?state=test",
        requested_file_count: 1,
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await startFigmaOAuth(
      {
        fileUrls: ["https://www.figma.com/file/ABC123/One"],
        returnPath: "/onboarding/source-catalog",
      },
      { apiBase: "https://gateway.example.test" },
    );

    expect(fetchMock).toHaveBeenCalledWith(
      "https://gateway.example.test/integrations/figma/oauth/start",
      expect.objectContaining({
        body: JSON.stringify({
          file_urls: ["https://www.figma.com/file/ABC123"],
          return_path: "/onboarding/source-catalog",
        }),
        credentials: "include",
      }),
    );
  });
});

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: async () => JSON.stringify(body),
  } as Response;
}
