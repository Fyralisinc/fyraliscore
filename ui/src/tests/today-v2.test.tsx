import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import TodayBriefing from "@/pages/today-v2/Briefing";
import {
  TODAY_PAGE_FIXTURE,
  mockApply,
  mockCorrection,
  mockDelegate,
  mockGetDelta,
  mockGetEvidence,
  _resetTodayPageMock,
} from "@/api/today-page-mock";

// Mock the today-page-client module. We intercept at the module
// boundary so the hook stays untouched and we can assert on the wire
// calls. The implementation delegates to the shared mock fixture so
// status transitions look exactly like prod.
vi.mock("@/api/today-page-client", () => ({
  ApiError: class ApiError extends Error {
    status: number;
    body?: unknown;
    constructor(m: string, s: number) {
      super(m);
      this.status = s;
    }
  },
  getTodayPage: vi.fn(async () => ({ ...TODAY_PAGE_FIXTURE })),
  getDeltaDetail: vi.fn(async (id: string) => mockGetDelta(id)),
  getDeltaEvidence: vi.fn(async (id: string) => mockGetEvidence(id)),
  applyDelta: vi.fn(async (id: string) => mockApply(id)),
  delegateDelta: vi.fn(async (id: string, body) => mockDelegate(id, body)),
  submitCorrection: vi.fn(async (id: string, body) => mockCorrection(id, body)),
}));

function renderBriefing() {
  return render(
    <MemoryRouter initialEntries={["/today"]}>
      <Routes>
        <Route path="/today" element={<TodayBriefing />} />
      </Routes>
    </MemoryRouter>,
  );
}

beforeEach(() => {
  _resetTodayPageMock();
});

afterEach(() => {
  vi.clearAllMocks();
});

// =====================================================================
// Today Briefing — in-place focused review (spec §6 + §7)
// =====================================================================

describe("Today Briefing", () => {
  it("renders header + summary strip + primary judgment + other items + handled panel", async () => {
    renderBriefing();
    await waitFor(() =>
      expect(screen.getByTestId("today-page")).toBeInTheDocument(),
    );
    await waitFor(() =>
      expect(screen.getByTestId("briefing-header")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("today-summary-strip")).toBeInTheDocument();
    expect(screen.getByTestId("primary-judgment")).toBeInTheDocument();
    expect(screen.getByTestId("other-judgment-panel")).toBeInTheDocument();
    expect(screen.getByTestId("handled-without-you-panel")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("primary-judgment")).getByText(/Salesforce sync instability/i),
    ).toBeInTheDocument();
  });

  it("shows summary metrics from the wire fixture", async () => {
    renderBriefing();
    await waitFor(() => screen.getByTestId("today-summary-strip"));
    const strip = screen.getByTestId("today-summary-strip");
    expect(within(strip).getByText("98")).toBeInTheDocument();
    expect(within(strip).getByText("94")).toBeInTheDocument();
    expect(within(strip).getByText("4")).toBeInTheDocument();
    expect(within(strip).getByText("$2.04M")).toBeInTheDocument();
  });

  it("shows the right status chip for the primary judgment", async () => {
    renderBriefing();
    await waitFor(() => screen.getByTestId("primary-judgment"));
    expect(screen.getByTestId("status-chip-needs_authority")).toBeInTheDocument();
  });

  it("clicking the primary judgment title expands the card inline (no navigation)", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await waitFor(() => screen.getByTestId("primary-judgment-open"));
    expect(screen.queryByTestId(/^inline-detail-/)).not.toBeInTheDocument();
    await user.click(screen.getByTestId("primary-judgment-open"));
    await waitFor(() =>
      expect(
        screen.getByTestId("inline-detail-delta-primary-001"),
      ).toBeInTheDocument(),
    );
    // No navigation away from Today and no "Reviewing N of M" header
    // would appear on a separate page — the strip lives inside the
    // same primary judgment card.
    expect(screen.getByTestId("today-page")).toBeInTheDocument();
    expect(screen.getByTestId("primary-collapse")).toBeInTheDocument();
  });

  it("clicking an Other Judgment row expands it inline into a focused review case", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await waitFor(() => screen.getByTestId("other-judgment-panel"));
    const pricingRow = screen.getByTestId("other-row-delta-other-pricing");
    await user.click(pricingRow);
    await waitFor(() =>
      expect(
        screen.getByTestId("inline-detail-delta-other-pricing"),
      ).toBeInTheDocument(),
    );
    // Still on Today.
    expect(screen.getByTestId("today-page")).toBeInTheDocument();
    // Title is rendered inside the expanded review card.
    expect(
      within(screen.getByTestId("other-card-delta-other-pricing")).getByText(
        /Assign owner for pricing model decision/i,
      ),
    ).toBeInTheDocument();
    // "Reviewing N of M" header is present.
    expect(
      within(screen.getByTestId("other-card-delta-other-pricing")).getByText(
        /Reviewing \d+ of \d+/i,
      ),
    ).toBeInTheDocument();
  });

  it("Collapse review (in card header) returns the card to compact form", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await waitFor(() => screen.getByTestId("other-judgment-panel"));
    await user.click(screen.getByTestId("other-row-delta-other-pricing"));
    await waitFor(() =>
      expect(
        screen.getByTestId("inline-detail-delta-other-pricing"),
      ).toBeInTheDocument(),
    );
    // Collapse lives in the card header, not the action bar (spec §2.1).
    await user.click(screen.getByTestId("other-collapse-delta-other-pricing"));
    await waitFor(() =>
      expect(
        screen.queryByTestId("inline-detail-delta-other-pricing"),
      ).not.toBeInTheDocument(),
    );
    // Compact row is back.
    expect(
      screen.getByTestId("other-row-delta-other-pricing"),
    ).toBeInTheDocument();
  });

  it("expanded card surfaces Ask Fyralis suggestions and renders a stubbed typed answer", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await waitFor(() => screen.getByTestId("primary-judgment-open"));
    await user.click(screen.getByTestId("primary-judgment-open"));
    await waitFor(() =>
      expect(
        screen.getByTestId("ask-strip-delta-primary-001"),
      ).toBeInTheDocument(),
    );
    // Suggested prompts from spec §7.8.
    expect(screen.getByTestId("ask-suggestion-why_now")).toBeInTheDocument();
    expect(screen.getByTestId("ask-suggestion-what_if_wait")).toBeInTheDocument();
    expect(screen.getByTestId("ask-suggestion-who_owns")).toBeInTheDocument();
    expect(
      screen.getByTestId("ask-suggestion-evidence_weakest"),
    ).toBeInTheDocument();
    await user.click(screen.getByTestId("ask-suggestion-why_now"));
    await waitFor(() =>
      expect(
        screen.getByTestId("ask-answer-delta-primary-001"),
      ).toBeInTheDocument(),
    );
    // Stubbed answer uses the spec's "Why now" typed response.
    expect(
      within(screen.getByTestId("ask-answer-delta-primary-001"))
        .getByRole("heading", { name: /Why now/i }),
    ).toBeInTheDocument();
  });

  it("Primary 'Review evidence' opens the evidence drawer in place (no navigation)", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await waitFor(() => screen.getByTestId("primary-review-evidence"));
    await user.click(screen.getByTestId("primary-review-evidence"));
    await waitFor(() =>
      expect(screen.getByTestId("evidence-drawer")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("today-page")).toBeInTheDocument();
  });

  it("Accept change button triggers applyDelta", async () => {
    const user = userEvent.setup();
    const client = await import("@/api/today-page-client");
    renderBriefing();
    await waitFor(() => screen.getByTestId("primary-accept"));
    await user.click(screen.getByTestId("primary-accept"));
    await waitFor(() =>
      expect(client.applyDelta).toHaveBeenCalledWith("delta-primary-001"),
    );
    await waitFor(() =>
      expect(screen.getByTestId("today-toast")).toBeInTheDocument(),
    );
  });

  it("Delegate button opens the delegation sheet", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await waitFor(() => screen.getByTestId("primary-delegate"));
    await user.click(screen.getByTestId("primary-delegate"));
    expect(screen.getByTestId("delegation-sheet")).toBeInTheDocument();
  });

  it("Report correction button opens the correction sheet", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await waitFor(() => screen.getByTestId("primary-correct"));
    await user.click(screen.getByTestId("primary-correct"));
    expect(screen.getByTestId("correction-sheet")).toBeInTheDocument();
  });
});
