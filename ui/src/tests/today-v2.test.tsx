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

const PRIMARY_ID = "delta-primary-001";
const PRICING_ID = "delta-other-pricing";

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
// Today Briefing — in-place focused review (spec §6 + §9 + §10)
// =====================================================================

describe("Today Briefing", () => {
  it("renders briefing header and a stream of judgment cards", async () => {
    renderBriefing();
    await waitFor(() =>
      expect(screen.getByTestId("today-page")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("briefing-header")).toBeInTheDocument();
    expect(screen.getByTestId("today-stream")).toBeInTheDocument();
    // Primary judgment auto-expands into a Focused Review card.
    expect(screen.getByTestId(`focused-review-${PRIMARY_ID}`)).toBeInTheDocument();
    // Other items render as compact rows that can be opened.
    expect(screen.getByTestId(`compact-card-${PRICING_ID}`)).toBeInTheDocument();
    // Page-level chrome from the old Briefing (SummaryStrip,
    // Handled-without-you) is gone per the locked direction.
    expect(screen.queryByTestId("today-summary-strip")).toBeNull();
    expect(screen.queryByTestId("handled-without-you-panel")).toBeNull();
  });

  it("briefing header surfaces the absorbed-vs-judgment line from the wire fixture", async () => {
    renderBriefing();
    await waitFor(() => screen.getByTestId("briefing-header"));
    const header = screen.getByTestId("briefing-header");
    expect(within(header).getByText(/Fyralis reviewed 98 signals/i)).toBeInTheDocument();
    expect(
      within(header).getByText(/4 require judgment; 94 were absorbed/i),
    ).toBeInTheDocument();
  });

  it("shows the status badge for the expanded judgment", async () => {
    renderBriefing();
    const card = await screen.findByTestId(`focused-review-${PRIMARY_ID}`);
    expect(within(card).getByText(/Needs your authority/i)).toBeInTheDocument();
  });

  it("clicking a compact card switches focused review without leaving Today", async () => {
    const user = userEvent.setup();
    renderBriefing();
    // Primary is expanded by default; pricing is compact.
    await screen.findByTestId(`focused-review-${PRIMARY_ID}`);
    expect(screen.queryByTestId(`focused-review-${PRICING_ID}`)).toBeNull();

    await user.click(screen.getByTestId(`compact-row-${PRICING_ID}`));

    // Pricing card is now the expanded one; primary collapsed to compact.
    await waitFor(() =>
      expect(screen.getByTestId(`focused-review-${PRICING_ID}`)).toBeInTheDocument(),
    );
    expect(screen.queryByTestId(`focused-review-${PRIMARY_ID}`)).toBeNull();
    expect(screen.getByTestId(`compact-card-${PRIMARY_ID}`)).toBeInTheDocument();
    // Still on Today.
    expect(screen.getByTestId("today-page")).toBeInTheDocument();
    // "Reviewing N of M" is present in the header.
    expect(
      within(screen.getByTestId(`focused-review-${PRICING_ID}`)).getByText(
        /Reviewing \d+ of \d+/i,
      ),
    ).toBeInTheDocument();
  });

  it("Collapse review collapses the expanded card to compact form", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`focused-review-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-collapse-${PRIMARY_ID}`));
    await waitFor(() =>
      expect(screen.queryByTestId(`focused-review-${PRIMARY_ID}`)).toBeNull(),
    );
    expect(screen.getByTestId(`compact-card-${PRIMARY_ID}`)).toBeInTheDocument();
  });

  it("expanded card surfaces Ask Fyralis suggestions and renders a stubbed typed answer", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`ask-strip-${PRIMARY_ID}`);
    expect(screen.getByTestId("ask-suggestion-why_now")).toBeInTheDocument();
    expect(screen.getByTestId("ask-suggestion-what_if_wait")).toBeInTheDocument();
    expect(screen.getByTestId("ask-suggestion-who_owns")).toBeInTheDocument();
    expect(screen.getByTestId("ask-suggestion-evidence_weakest")).toBeInTheDocument();
    await user.click(screen.getByTestId("ask-suggestion-why_now"));
    await waitFor(() =>
      expect(screen.getByTestId(`ask-answer-${PRIMARY_ID}`)).toBeInTheDocument(),
    );
    expect(
      within(screen.getByTestId(`ask-answer-${PRIMARY_ID}`)).getByRole("heading", {
        name: /Why now/i,
      }),
    ).toBeInTheDocument();
  });

  it("Review evidence opens the evidence drawer in place (no navigation)", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`focused-review-evidence-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-review-evidence-${PRIMARY_ID}`));
    await waitFor(() =>
      expect(screen.getByTestId("evidence-drawer")).toBeInTheDocument(),
    );
    expect(screen.getByTestId("today-page")).toBeInTheDocument();
  });

  it("Accept change button triggers applyDelta", async () => {
    const user = userEvent.setup();
    const client = await import("@/api/today-page-client");
    renderBriefing();
    await screen.findByTestId(`focused-accept-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-accept-${PRIMARY_ID}`));
    await waitFor(() =>
      expect(client.applyDelta).toHaveBeenCalledWith(PRIMARY_ID),
    );
    await waitFor(() =>
      expect(screen.getByTestId("today-toast")).toBeInTheDocument(),
    );
  });

  it("Delegate button opens the delegation sheet", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`focused-delegate-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-delegate-${PRIMARY_ID}`));
    expect(screen.getByTestId("delegation-sheet")).toBeInTheDocument();
  });

  it("Report correction button opens the correction sheet", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`focused-correct-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-correct-${PRIMARY_ID}`));
    expect(screen.getByTestId("correction-sheet")).toBeInTheDocument();
  });
});
