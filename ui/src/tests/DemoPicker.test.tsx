import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import DemoPicker from "../pages/DemoPicker";

const COMPANIES = {
  items: [
    { company_id: "truss", name: "Truss", tagline: "Series A AI infra", description: "Forty-person AI-native company." },
    { company_id: "northwind", name: "Northwind", tagline: "Series B SaaS", description: "180-person HR platform." },
    { company_id: "meridian", name: "Meridian", tagline: "Series C enterprise", description: "1100-person supply chain." },
  ],
};

const startSpy = vi.fn();

beforeEach(() => {
  localStorage.clear();
  startSpy.mockReset();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      const method = (init?.method ?? "GET").toUpperCase();
      if (url.endsWith("/v1/demo/companies") && method === "GET") {
        return new Response(JSON.stringify(COMPANIES), { status: 200 });
      }
      if (url.endsWith("/v1/demo/sessions/start") && method === "POST") {
        startSpy(JSON.parse((init?.body as string) ?? "{}"));
        return new Response(
          JSON.stringify({
            session_id: "s-1",
            tenant_id: "t-1",
            auth_token: "tok-1",
            auth_token_expires_at: "2026-04-29T00:00:00Z",
            ceo_actor_id: "a-1",
            company_id: "truss",
          }),
          { status: 201 }
        );
      }
      return new Response("not found", { status: 404 });
    })
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("DemoPicker", () => {
  it("renders three cards from the API and starts a session on click", async () => {
    const user = userEvent.setup();
    render(
      <MemoryRouter initialEntries={["/demo"]}>
        <DemoPicker />
      </MemoryRouter>
    );
    await waitFor(() => expect(screen.getByText("Truss")).toBeInTheDocument());
    expect(screen.getByText("Northwind")).toBeInTheDocument();
    expect(screen.getByText("Meridian")).toBeInTheDocument();
    expect(document.querySelectorAll(".demo-picker-card").length).toBe(3);

    await user.click(screen.getByTestId("start-truss"));
    await waitFor(() => expect(startSpy).toHaveBeenCalledWith({ company_id: "truss" }));
    await waitFor(() =>
      expect(localStorage.getItem("demoAuthToken")).toBe("tok-1")
    );
    expect(localStorage.getItem("demoSessionId")).toBe("s-1");
  });
});
