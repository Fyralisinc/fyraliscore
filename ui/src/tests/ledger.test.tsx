import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import LedgerPage from "../pages/ledger/Ledger";
import {
  LEDGER_EVENTS_FIXTURE,
  LEDGER_SUMMARY_FIXTURE,
  SALESFORCE_ESCALATION_EVENT,
} from "../api/ledger-mock";

// Mirror the canonical ledger event surface: the page calls
//  - GET /v1/history?surface=ledger[&types=...]
//  - GET /v1/history/summary?range_days=30
//
// We capture every call so tests can assert against the requested URL
// and respond with our fixture in the canonical shape.

type FetchCall = { url: string; method: string };
let calls: FetchCall[];
let serveMode: "fixture" | "empty" | "error";

function urlFor(input: RequestInfo | URL): string {
  if (typeof input === "string") return input;
  if (input instanceof URL) return input.toString();
  return (input as Request).url;
}

function fakeFetch(input: RequestInfo | URL, init?: RequestInit) {
  const url = urlFor(input);
  const method = (init?.method ?? "GET").toUpperCase();
  calls.push({ url, method });
  if (serveMode === "error") {
    return Promise.resolve(new Response("server failure", { status: 500 }));
  }
  if (url.includes("/v1/history/summary")) {
    return Promise.resolve(
      new Response(JSON.stringify(LEDGER_SUMMARY_FIXTURE), { status: 200 })
    );
  }
  if (url.includes("/v1/history")) {
    const parsed = new URL(url, "http://localhost");
    const typesRaw = parsed.searchParams.get("types");
    const types = typesRaw ? typesRaw.split(",") : null;
    const events =
      serveMode === "empty"
        ? []
        : types
          ? LEDGER_EVENTS_FIXTURE.filter((e) => types.includes(e.type))
          : LEDGER_EVENTS_FIXTURE;
    return Promise.resolve(
      new Response(
        JSON.stringify({ events, period: "30d", types }),
        { status: 200 }
      )
    );
  }
  return Promise.resolve(new Response("not found", { status: 404 }));
}

function renderLedger() {
  return render(
    <MemoryRouter initialEntries={["/ledger"]}>
      <LedgerPage />
    </MemoryRouter>
  );
}

beforeEach(() => {
  calls = [];
  serveMode = "fixture";
  vi.stubGlobal("fetch", vi.fn(fakeFetch));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Ledger page", () => {
  it("renders the page title and subtitle", async () => {
    renderLedger();
    expect(
      await screen.findByRole("heading", { level: 1, name: "Ledger" })
    ).toBeInTheDocument();
    expect(
      screen.getByText(/history of what changed/i)
    ).toBeInTheDocument();
  });

  it("renders summary strip with 6 counters from the API", async () => {
    renderLedger();
    await waitFor(() =>
      expect(
        screen.getByText(LEDGER_SUMMARY_FIXTURE.events.value.toLocaleString())
      ).toBeInTheDocument()
    );
    const labels = Array.from(
      document.querySelectorAll(".fy-summary-cell__label")
    ).map((el) => el.textContent?.trim());
    expect(labels).toEqual(
      expect.arrayContaining([
        "Events",
        "Model updates",
        "Predictions made",
        "Predictions accuracy",
        "Actions taken",
        "Contestations",
      ])
    );
    expect(labels.length).toBe(6);
  });

  it("renders all 6 tabs", async () => {
    renderLedger();
    expect(await screen.findByRole("tab", { name: "All activity" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Model changes" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Predictions" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Actions" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Contestations" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Observations" })).toBeInTheDocument();
  });

  it("switches tab and calls API with correct types param", async () => {
    const user = userEvent.setup();
    renderLedger();
    await screen.findByRole("tab", { name: "All activity" });
    // Wait for first load to settle.
    await waitFor(() =>
      expect(
        calls.some((c) =>
          c.url.includes("/v1/history?") && c.url.includes("surface=ledger")
        )
      ).toBe(true)
    );
    calls.length = 0;
    await user.click(screen.getByRole("tab", { name: "Actions" }));
    await waitFor(() =>
      expect(
        calls.some((c) =>
          c.url.includes("/v1/history?") && c.url.includes("types=action_taken")
        )
      ).toBe(true)
    );
  });

  it("groups events by date with sticky date headers", async () => {
    renderLedger();
    await screen.findByTestId("ledger-timeline");
    const headers = screen.getAllByTestId("ledger-day-header");
    expect(headers.length).toBeGreaterThanOrEqual(2);
    // First header should mention "Today" (fixture anchored to May 15)
    expect(headers[0].textContent).toMatch(/Today/);
  });

  it("clicking an event row opens the inspector with title + timestamp", async () => {
    const user = userEvent.setup();
    renderLedger();
    const row = await screen.findByText(
      SALESFORCE_ESCALATION_EVENT.title
    );
    await user.click(row);
    expect(await screen.findByTestId("ledger-inspector-title")).toHaveTextContent(
      SALESFORCE_ESCALATION_EVENT.title
    );
    expect(screen.getByTestId("ledger-inspector-time").textContent).toMatch(
      /Today at/
    );
    expect(screen.getByTestId("ledger-inspector-class").textContent).toMatch(
      /ACTION TAKEN/
    );
  });

  it("search filters event rows", async () => {
    const user = userEvent.setup();
    renderLedger();
    await screen.findByTestId("ledger-timeline");
    const input = screen.getByTestId("ledger-search-input");
    await user.type(input, "salesforce");
    await waitFor(() => {
      const rows = document.querySelectorAll(".fy-ledger__event-row");
      // every visible row should mention salesforce somewhere
      for (const row of rows) {
        expect(row.textContent?.toLowerCase()).toContain("salesforce");
      }
    });
  });

  it("renders empty state when API returns no events", async () => {
    serveMode = "empty";
    renderLedger();
    await waitFor(() => {
      const empty = document.querySelector("[data-empty]");
      expect(empty).not.toBeNull();
    });
  });

  it("renders error state when API returns 500", async () => {
    serveMode = "error";
    renderLedger();
    await waitFor(() =>
      expect(screen.getByText(/Could not load ledger/i)).toBeInTheDocument()
    );
  });

  it("shows the loading state before the first response", async () => {
    type Resolver = (v: Response) => void;
    const pending: { fn: Resolver | null } = { fn: null };
    vi.stubGlobal(
      "fetch",
      vi.fn(
        (input: RequestInfo | URL) =>
          new Promise<Response>((resolve) => {
            const url = urlFor(input);
            if (url.includes("/v1/history/summary")) {
              resolve(
                new Response(JSON.stringify(LEDGER_SUMMARY_FIXTURE), {
                  status: 200,
                })
              );
              return;
            }
            // hold the ledger request open
            pending.fn = resolve;
          })
      )
    );
    renderLedger();
    expect(await screen.findByText(/Loading the ledger/i)).toBeInTheDocument();
    if (pending.fn) {
      pending.fn(
        new Response(
          JSON.stringify({ events: LEDGER_EVENTS_FIXTURE, period: "30d" }),
          { status: 200 }
        )
      );
    }
  });

  it("filters dropdown multi-selects and triggers fresh fetch", async () => {
    const user = userEvent.setup();
    renderLedger();
    await screen.findByTestId("ledger-timeline");
    calls.length = 0;
    await user.click(screen.getByTestId("ledger-filters-toggle"));
    // Find the model_update option and toggle it
    const modelUpdateOption = screen.getByText("Model update");
    await user.click(modelUpdateOption);
    await waitFor(() =>
      expect(
        calls.some((c) => c.url.includes("types=model_update"))
      ).toBe(true)
    );
  });

  it("inspector contains 'View in model' link", async () => {
    const user = userEvent.setup();
    renderLedger();
    const row = await screen.findByText(
      SALESFORCE_ESCALATION_EVENT.title
    );
    await user.click(row);
    const link = await screen.findByTestId("ledger-link-view-in-model");
    expect(link.textContent).toMatch(/View in model/);
  });

  it("⌘K focuses the search input", async () => {
    renderLedger();
    const input = (await screen.findByTestId(
      "ledger-search-input"
    )) as HTMLInputElement;
    expect(document.activeElement).not.toBe(input);
    const ev = new KeyboardEvent("keydown", {
      key: "k",
      metaKey: true,
      bubbles: true,
    });
    window.dispatchEvent(ev);
    await waitFor(() => expect(document.activeElement).toBe(input));
  });

  it("load more reveals additional events", async () => {
    const user = userEvent.setup();
    renderLedger();
    await screen.findByTestId("ledger-timeline");
    const initialRows = document.querySelectorAll(".fy-ledger__event-row").length;
    const btn = screen.queryByTestId("ledger-load-more");
    if (!btn) {
      // The fixture has more than the page size — sanity check.
      throw new Error("expected a Load more button");
    }
    await user.click(btn);
    await waitFor(() => {
      const after = document.querySelectorAll(".fy-ledger__event-row").length;
      expect(after).toBeGreaterThan(initialRows);
    });
    void within;
  });
});
