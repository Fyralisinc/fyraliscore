import { describe, expect, it } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import LedgerPage from "../pages/ledger-v2/LedgerPage";

function renderLedger(initialPath = "/ledger") {
  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <LedgerPage />
    </MemoryRouter>,
  );
}

describe("Ledger page", () => {
  it("renders the v2 Memory River and selected Chain Inspector", () => {
    renderLedger();

    expect(screen.getByRole("heading", { level: 1, name: "Ledger" })).toBeInTheDocument();
    expect(screen.getByText(/what changed, what resolved/i)).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Memory river" })).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { level: 2, name: "Customer Reliability Escalation" }),
    ).toBeInTheDocument();
  });

  it("switches modes from the tab control", async () => {
    const user = userEvent.setup();
    renderLedger();

    await user.click(screen.getByRole("tab", { name: "Accuracy" }));
    expect(screen.getByRole("heading", { level: 2, name: "Accuracy" })).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Audit" }));
    expect(screen.getByRole("heading", { level: 2, name: "Audit log" })).toBeInTheDocument();
  });

  it("filters audit rows by event text", async () => {
    const user = userEvent.setup();
    renderLedger("/ledger?mode=audit");

    await user.type(
      screen.getByPlaceholderText("Search event, actor, or chain..."),
      "pricing",
    );

    const audit = screen.getByRole("region", { name: "Audit" });
    expect(within(audit).getAllByText(/Pricing Ownership Delay/).length).toBeGreaterThan(0);
    expect(within(audit).queryByText(/Customer Reliability Escalation/)).not.toBeInTheDocument();
  });
});
