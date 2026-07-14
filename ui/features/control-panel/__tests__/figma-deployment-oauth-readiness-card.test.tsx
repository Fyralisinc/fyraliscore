import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { FigmaDeploymentOAuthReadinessCard } from "../components/figma-deployment-oauth-readiness-card";
import { defaultControlPanelApiBase } from "../api";

describe("FigmaDeploymentOAuthReadinessCard", () => {
  it("uses the provider ingress as the control-panel API fallback", () => {
    vi.stubEnv("NEXT_PUBLIC_FYRALIS_API_BASE", "");
    vi.stubEnv(
      "NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL",
      "https://gateway.customer.test",
    );

    expect(defaultControlPanelApiBase()).toBe("https://gateway.customer.test");
  });

  it("renders only the safe admin setup contract", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        runtime_ready: true,
        source_enabled: true,
        checks: {
          figma_oauth_enabled: true,
          client_id_configured: true,
          client_secret_configured: true,
        },
        redirect_uri:
          "https://gateway.customer.test/integrations/figma/oauth/callback",
        ui_return_origin: "https://console.customer.test",
        required_scopes: ["current_user:read", "file_content:read"],
        configured_scopes: ["current_user:read", "file_content:read"],
        provider_console_url: "https://www.figma.com/developers/apps",
        recommended_app_mode: "private",
        provider_app_registration_unverified: true,
        setup_checklist: ["Create the private customer-owned app."],
        client_secret: "must-never-render",
        client_secret_ref: "must-never-render-ref",
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    render(
      <FigmaDeploymentOAuthReadinessCard
        apiBase="https://gateway.customer.test"
        bearerToken="tenant-admin-token"
      />,
    );

    await user.click(screen.getByRole("button", { name: "Check Figma setup" }));

    expect(await screen.findByText("Gateway configuration is ready")).toBeInTheDocument();
    expect(
      screen.getByText(
        "https://gateway.customer.test/integrations/figma/oauth/callback",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("current_user:read")).toBeInTheDocument();
    expect(screen.queryByText("must-never-render")).not.toBeInTheDocument();
    expect(screen.queryByText("must-never-render-ref")).not.toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Open Figma developer apps" }),
    ).toHaveAttribute("href", "https://www.figma.com/developers/apps");

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "https://gateway.customer.test/api/admin/integrations/figma/oauth/readiness",
        expect.objectContaining({
          headers: expect.objectContaining({
            Authorization: "Bearer tenant-admin-token",
          }),
        }),
      );
    });
  });
});

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as Response;
}
