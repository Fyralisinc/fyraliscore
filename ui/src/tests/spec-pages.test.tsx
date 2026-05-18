import { describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";

import TodayBriefing from "@/pages/today-v2/Briefing";
import { TODAY_PAGE_FIXTURE } from "@/api/today-page-mock";
import ModelSpec from "@/pages/model/ModelSpec";
import ForecastsSpec from "@/pages/forecasts/ForecastsSpec";
import LedgerSpec from "@/pages/ledger/LedgerSpec";

// Today v2 talks to /api/today; back the page with the same mock the
// dedicated today-v2.test.tsx suite uses so the smoke test renders the
// real component tree without a network round-trip.
vi.mock("@/api/today-page-client", () => ({
  ApiError: class ApiError extends Error {
    status: number;
    constructor(m: string, s: number) {
      super(m);
      this.status = s;
    }
  },
  getTodayPage: vi.fn(async () => ({ ...TODAY_PAGE_FIXTURE })),
  getDeltaDetail: vi.fn(async () => null),
  getDeltaEvidence: vi.fn(async () => null),
  applyDelta: vi.fn(async () => null),
  delegateDelta: vi.fn(async () => null),
  submitCorrection: vi.fn(async () => null),
}));

// Smoke tests for the spec-aligned pages. We rely on the in-store
// fixtures (SPEC_THREADS_FIXTURE etc.) so the pages can render before
// any network round-trip lands. These tests validate the "page boots
// without crashing and shows the headline product surface" contract.

function wrap(node: React.ReactElement, initialPath = "/") {
  return (
    <MemoryRouter initialEntries={[initialPath]}>
      {node}
    </MemoryRouter>
  );
}

describe("Today (spec)", () => {
  it("renders the page heading and at least one Decision Delta", async () => {
    render(wrap(<TodayBriefing />, "/today"));
    // The briefing renders a loading skeleton first; wait for the
    // hydrated header.
    await waitFor(() =>
      expect(screen.getByRole("heading", { name: /^Today$/ })).toBeTruthy(),
    );
    await waitFor(() =>
      expect(
        screen.getByText(/Salesforce sync instability/i),
      ).toBeInTheDocument(),
    );
  });

  it("labels the focused review surface as a Proposed change", async () => {
    render(wrap(<TodayBriefing />, "/today"));
    await waitFor(() =>
      expect(screen.getAllByText(/Proposed change/i).length).toBeGreaterThan(0),
    );
  });
});

describe("Model (spec)", () => {
  it("renders Operating Thread rows with causal ribbons", () => {
    render(wrap(<ModelSpec />));
    expect(screen.getAllByText("Customer Reliability").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Engineering Capacity").length).toBeGreaterThan(0);
    // Causal ribbon labels (default Company lens)
    expect(screen.getAllByText(/Intent/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Promise/i).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Friction/i).length).toBeGreaterThan(0);
  });

  it("renders the 8-lens bar", () => {
    render(wrap(<ModelSpec />));
    for (const lens of ["Company", "Commitments", "Decisions", "Customers", "Teams", "Risks", "Owners", "Predictions"]) {
      expect(screen.getByText(lens)).toBeTruthy();
    }
  });
});

describe("Forecasts (spec)", () => {
  it("renders a forecast statement and confidence", () => {
    render(wrap(<ForecastsSpec />));
    expect(screen.getAllByText(/Beacon renewal risk/i).length).toBeGreaterThan(0);
  });
});

describe("Ledger (spec)", () => {
  it("renders ledger events grouped by day", () => {
    render(wrap(<LedgerSpec />));
    expect(screen.getByRole("heading", { name: /Ledger/i })).toBeTruthy();
    expect(screen.getAllByText(/Customer Reliability moved/i).length).toBeGreaterThan(0);
  });
});
