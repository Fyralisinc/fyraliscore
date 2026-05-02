import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import App from "../App";
import { TODAY_FIXTURE } from "../api/today-mock";
import { HOME_FIXTURE, mockAsk } from "../api/mock-data";

// Fetch-mock covers /v1/today, /v1/recommendations/<id>/triage, plus the
// legacy /view/ceo/* surface used by the Ask Zone. The WS stream fails
// to connect in jsdom; useToday falls back to the HTTP snapshot.
beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      const method = (init?.method ?? "GET").toUpperCase();
      if (url.endsWith("/v1/today") && method === "GET") {
        return new Response(JSON.stringify(TODAY_FIXTURE), { status: 200 });
      }
      if (/\/v1\/recommendations\/[^/]+\/triage$/.test(url) && method === "POST") {
        return new Response(
          JSON.stringify({ ok: true, recommendation_id: "rec-x", action: "act" }),
          { status: 200 }
        );
      }
      if (url.endsWith("/v1/today/brand") && method === "POST") {
        return new Response(JSON.stringify({ ok: true, name: "X" }), { status: 200 });
      }
      if (url.endsWith("/view/ceo/home")) {
        return new Response(JSON.stringify(HOME_FIXTURE), { status: 200 });
      }
      if (url.endsWith("/view/ceo/ask")) {
        const body = JSON.parse((init?.body as string) ?? "{}");
        return new Response(JSON.stringify(mockAsk(body.query)), { status: 200 });
      }
      if (url.endsWith("/view/ceo/turn-action")) {
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      return new Response("not found", { status: 404 });
    })
  );
  // Silence the WS connection attempt cleanly.
  class FakeWS {
    readyState = 0;
    onopen: ((e: Event) => void) | null = null;
    onclose: ((e: Event) => void) | null = null;
    onmessage: ((e: MessageEvent) => void) | null = null;
    onerror: ((e: Event) => void) | null = null;
    constructor(_url: string) {}
    close() {}
    send() {}
  }
  vi.stubGlobal("WebSocket", FakeWS);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Today page", () => {
  it("renders date header, signal strip, and the seven cards", async () => {
    render(<App />);
    await waitFor(() =>
      expect(screen.getByText(TODAY_FIXTURE.page.date_label)).toBeInTheDocument()
    );
    // Sidebar brand
    expect(screen.getByText("Fyralis")).toBeInTheDocument();
    // Signal strip — 4 metric labels
    const labels = Array.from(document.querySelectorAll(".signal-label")).map(
      (el) => el.textContent?.trim()
    );
    expect(labels).toEqual(expect.arrayContaining(["ARR", "Runway", "Commitments", "My calibration"]));
    // Cards (kind labels)
    expect(screen.getAllByText(/Decision drift/i).length).toBeGreaterThan(0);
    expect(document.querySelectorAll("article.card").length).toBe(
      TODAY_FIXTURE.cards.length
    );
  });

  it("filters strategic only when pressing 3", async () => {
    const user = userEvent.setup();
    render(<App />);
    await waitFor(() =>
      expect(screen.getByText(TODAY_FIXTURE.page.date_label)).toBeInTheDocument()
    );
    await user.keyboard("3");
    const visible = document.querySelectorAll("article.card[data-kind='strategic']");
    expect(visible.length).toBeGreaterThan(0);
    expect(document.querySelectorAll("article.card[data-kind='operational']").length).toBe(0);
  });

  it("opens shortcuts overlay when ? is pressed and closes on Esc", async () => {
    const user = userEvent.setup();
    render(<App />);
    await waitFor(() =>
      expect(screen.getByText(TODAY_FIXTURE.page.date_label)).toBeInTheDocument()
    );
    await user.keyboard("?");
    expect(screen.getByText(/Keyboard shortcuts/)).toBeInTheDocument();
    await user.keyboard("{Escape}");
    await waitFor(() =>
      expect(screen.queryByText(/Keyboard shortcuts/)).not.toBeInTheDocument()
    );
  });

  it("focuses the ask field when / is pressed", async () => {
    const user = userEvent.setup();
    render(<App />);
    await waitFor(() =>
      expect(screen.getByText(TODAY_FIXTURE.page.date_label)).toBeInTheDocument()
    );
    const input = screen.getByPlaceholderText(/What did we decide about pricing/);
    (input as HTMLInputElement).blur();
    expect(document.activeElement).not.toBe(input);
    await user.keyboard("/");
    expect(document.activeElement).toBe(input);
  });
});
