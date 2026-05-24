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

function renderBriefing(initialEntry = "/today") {
  return render(
    <MemoryRouter initialEntries={[initialEntry]}>
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
// Today (combined Briefing + Review layout)
// =====================================================================

describe("Today page", () => {
  it("renders header, Fyralis Brief, rail, and the focused review sheet for the primary judgment", async () => {
    renderBriefing();
    await screen.findByTestId(`focused-review-${PRIMARY_ID}`);
    expect(screen.getByTestId("today-page")).toBeInTheDocument();
    expect(screen.getByTestId("briefing-header")).toBeInTheDocument();
    expect(screen.getByTestId("fyralis-brief")).toBeInTheDocument();
    expect(screen.getByTestId("review-rail")).toBeInTheDocument();
    expect(screen.getByTestId(`focused-review-${PRIMARY_ID}`)).toBeInTheDocument();
    expect(screen.getByTestId(`action-bar-${PRIMARY_ID}`)).toBeInTheDocument();
  });

  it("briefing header surfaces the attention receipt", async () => {
    renderBriefing();
    await screen.findByTestId("briefing-header");
    const header = screen.getByTestId("briefing-header");
    expect(within(header).getByText(/Fyralis reviewed the company/i)).toBeInTheDocument();
    expect(within(header).getByText(/signals processed/i)).toBeInTheDocument();
    expect(within(header).getByText(/absorbed/i)).toBeInTheDocument();
    expect(within(header).getByText(/need your judgment/i)).toBeInTheDocument();
  });

  it("rail lists primary + other items + handled stats", async () => {
    renderBriefing();
    await screen.findByTestId("review-rail");
    const rail = screen.getByTestId("review-rail");
    expect(within(rail).getByTestId(`rail-row-${PRIMARY_ID}`)).toBeInTheDocument();
    expect(within(rail).getByTestId(`rail-row-${PRICING_ID}`)).toBeInTheDocument();
    expect(within(rail).getByText(/Primary judgment/i)).toBeInTheDocument();
    expect(within(rail).getByText(/Other items needing your judgment/i)).toBeInTheDocument();
    expect(within(rail).getByText(/Handled without you/i)).toBeInTheDocument();
  });

  it("clicking another rail row swaps the focused review sheet", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`focused-review-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`rail-row-${PRICING_ID}`));
    await waitFor(() =>
      expect(screen.getByTestId(`focused-review-${PRICING_ID}`)).toBeInTheDocument(),
    );
    expect(screen.queryByTestId(`focused-review-${PRIMARY_ID}`)).toBeNull();
  });

  it("deep link /today?review=<id> selects that delta", async () => {
    renderBriefing(`/today?review=${PRICING_ID}`);
    await screen.findByTestId(`focused-review-${PRICING_ID}`);
    expect(screen.queryByTestId(`focused-review-${PRIMARY_ID}`)).toBeNull();
  });

  it("focused review header shows PROPOSED CHANGE label, title, and badge", async () => {
    renderBriefing();
    const sheet = await screen.findByTestId(`focused-review-${PRIMARY_ID}`);
    expect(within(sheet).getByText(/PROPOSED CHANGE/i)).toBeInTheDocument();
    expect(within(sheet).getByText(/Salesforce sync instability/i)).toBeInTheDocument();
    expect(within(sheet).getByText(/Needs your authority/i)).toBeInTheDocument();
  });

  it("Ask Fyralis suggestions render and a stubbed typed answer appears inline", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`ask-strip-${PRIMARY_ID}`);
    expect(screen.getByTestId("ask-suggestion-why_now")).toBeInTheDocument();
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

  it("Review all evidence opens the evidence drawer in place", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`focused-review-evidence-link-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-review-evidence-link-${PRIMARY_ID}`));
    await waitFor(() =>
      expect(screen.getByTestId("evidence-drawer")).toBeInTheDocument(),
    );
  });

  it("Accept change in the bottom action bar triggers applyDelta", async () => {
    const user = userEvent.setup();
    const client = await import("@/api/today-page-client");
    renderBriefing();
    await screen.findByTestId(`focused-accept-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-accept-${PRIMARY_ID}`));
    await waitFor(() =>
      expect(client.applyDelta).toHaveBeenCalledWith(PRIMARY_ID),
    );
  });

  it("Delegate opens the delegation sheet", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`focused-delegate-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-delegate-${PRIMARY_ID}`));
    expect(screen.getByTestId("delegation-sheet")).toBeInTheDocument();
  });

  it("Report correction opens the correction sheet", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`focused-correct-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-correct-${PRIMARY_ID}`));
    expect(screen.getByTestId("correction-sheet")).toBeInTheDocument();
  });

  it("Request changes opens the correction sheet (same payload)", async () => {
    const user = userEvent.setup();
    renderBriefing();
    await screen.findByTestId(`focused-request-changes-${PRIMARY_ID}`);
    await user.click(screen.getByTestId(`focused-request-changes-${PRIMARY_ID}`));
    expect(screen.getByTestId("correction-sheet")).toBeInTheDocument();
  });

  it("header has the global Ask Fyralis search trigger", async () => {
    renderBriefing();
    await screen.findByTestId("briefing-header");
    expect(screen.getByTestId("header-ask")).toBeInTheDocument();
  });
});
