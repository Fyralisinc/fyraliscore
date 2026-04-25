import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { render, screen, waitFor, act } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import App from "../App";
import { HOME_FIXTURE, mockAsk } from "../api/mock-data";

// Fetch-mock covers /view/ceo/home, /view/ceo/ask, /view/ceo/turn-action.
// The WS stream fails to connect in jsdom; that's fine — useHome falls
// back to the HTTP snapshot and flags offline=false because the initial
// fetch succeeded.
beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.endsWith("/view/ceo/home")) {
        return new Response(JSON.stringify(HOME_FIXTURE), { status: 200 });
      }
      if (url.endsWith("/view/ceo/ask")) {
        const body = JSON.parse((init?.body as string) ?? "{}");
        return new Response(JSON.stringify(mockAsk(body.query)), {
          status: 200,
        });
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

describe("CEO view", () => {
  it("renders greeting, query grid, cards, and close line", async () => {
    render(<App />);
    await waitFor(() =>
      expect(screen.getByTestId("greeting")).toBeInTheDocument()
    );
    expect(screen.getByText(/Company OS/i)).toBeInTheDocument();
    expect(screen.getByText(/view \//)).toBeInTheDocument();
    expect(screen.getByTestId("query-grid")).toBeInTheDocument();
    // Six chips in the grid.
    expect(
      screen.getByTestId("query-grid").querySelectorAll("button.q")
    ).toHaveLength(6);
    // Three cards (observation / decision / question).
    expect(screen.getByTestId("card-observation")).toBeInTheDocument();
    expect(screen.getByTestId("card-decision")).toBeInTheDocument();
    expect(screen.getByTestId("card-question")).toBeInTheDocument();
    expect(screen.getByTestId("close-line")).toBeInTheDocument();
  });

  it("expands a card and closes on Esc", async () => {
    const user = userEvent.setup();
    render(<App />);
    await waitFor(() =>
      expect(screen.getByTestId("greeting")).toBeInTheDocument()
    );
    const card = screen.getByTestId("card-observation");
    await user.click(card);
    expect(card).toHaveAttribute("aria-expanded", "true");
    // Esc closes.
    await user.keyboard("{Escape}");
    expect(card).toHaveAttribute("aria-expanded", "false");
  });

  it("submits a query from the ground input and shows a turn", async () => {
    const user = userEvent.setup();
    render(<App />);
    await waitFor(() =>
      expect(screen.getByTestId("greeting")).toBeInTheDocument()
    );
    const input = screen.getByPlaceholderText(/Ask anything else/);
    await user.type(input, "Show me why Acme became unsafe{Enter}");
    await waitFor(() =>
      expect(screen.getByTestId("turn")).toBeInTheDocument()
    );
    expect(screen.getByTestId("turn")).toHaveTextContent(
      /Show me why Acme became unsafe/
    );
  });

  it("tapping a query chip opens a turn with the chip's label", async () => {
    const user = userEvent.setup();
    render(<App />);
    await waitFor(() =>
      expect(screen.getByTestId("greeting")).toBeInTheDocument()
    );
    const chip = screen.getByRole("button", {
      name: /Show me why Acme became unsafe/,
    });
    await user.click(chip);
    await waitFor(() =>
      expect(screen.getByTestId("turn")).toBeInTheDocument()
    );
  });

  it("pressing / focuses the ground input from anywhere", async () => {
    const user = userEvent.setup();
    render(<App />);
    await waitFor(() =>
      expect(screen.getByTestId("greeting")).toBeInTheDocument()
    );
    const input = screen.getByPlaceholderText(/Ask anything else/);
    // Ensure focus is not already there.
    (input as HTMLInputElement).blur();
    expect(document.activeElement).not.toBe(input);
    await act(async () => {
      await user.keyboard("/");
    });
    expect(document.activeElement).toBe(input);
  });

  it("tapping a card verb sends the query template and collapses the card", async () => {
    const user = userEvent.setup();
    render(<App />);
    await waitFor(() =>
      expect(screen.getByTestId("greeting")).toBeInTheDocument()
    );
    const card = screen.getByTestId("card-observation");
    await user.click(card);
    const verb = await screen.findByRole("button", { name: /Full reasoning/ });
    await user.click(verb);
    await waitFor(() =>
      expect(screen.getByTestId("turn")).toBeInTheDocument()
    );
    // Card collapsed.
    expect(card).toHaveAttribute("aria-expanded", "false");
  });
});
