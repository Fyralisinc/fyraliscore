import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { SignalSimulator } from "../components/SignalSimulator";

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();
    if (url.endsWith("/v1/demo/simulator/suggested")) {
      return new Response(
        JSON.stringify({ company_id: "truss", tabs: { slack: [] } }),
        { status: 200 }
      );
    }
    if (url.endsWith("/v1/demo/simulator/inject")) {
      const body = JSON.parse((init?.body as string) ?? "{}");
      // Echo body back so tests can assert payload shape.
      return new Response(
        JSON.stringify({
          observation_id: "obs-1",
          deduped: false,
          _echo: body,
        }),
        { status: 200 }
      );
    }
    return new Response("not found", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("SignalSimulator", () => {
  it("renders all tabs", async () => {
    render(<SignalSimulator token="t" sessionId="s" />);
    await waitFor(() => screen.getByText("Slack"));
    for (const t of ["Slack", "Email", "GitHub", "Calendar", "Stripe", "Custom"]) {
      expect(screen.getByText(t)).toBeInTheDocument();
    }
  });

  it("posts the slack payload with the right channel mapping", async () => {
    const user = userEvent.setup();
    render(<SignalSimulator token="t" sessionId="s" />);
    await waitFor(() => screen.getByText("Slack"));

    const channel = screen.getByPlaceholderText("#sales") as HTMLInputElement;
    await user.clear(channel);
    await user.type(channel, "#deals");
    const message = screen.getByPlaceholderText(
      /Linear just asked about SSO/
    ) as HTMLTextAreaElement;
    await user.type(message, "hello");
    await user.click(screen.getByRole("button", { name: "Send" }));

    await waitFor(() => {
      const injectCall = fetchMock.mock.calls.find((args: unknown[]) =>
        String(args[0]).endsWith("/v1/demo/simulator/inject")
      );
      expect(injectCall).toBeTruthy();
      const body = JSON.parse((injectCall as any[])[1].body);
      expect(body.channel).toBe("slack:message");
      expect(body.payload.channel).toBe("#deals");
      expect(body.payload.message).toBe("hello");
    });
  });
});
