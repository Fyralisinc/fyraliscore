import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import ModelPage from "../pages/model/Model";
import { MAP_SNAPSHOT_V2_FIXTURE } from "../api/map-mock-v2";
import { mockSupports, mockDependsOn } from "../api/model-trace-mock";

// Patch fetch so the Model page loads against the banded fixture rather
// than the un-banded legacy snapshot.
beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/map/snapshot")) {
        return new Response(JSON.stringify(MAP_SNAPSHOT_V2_FIXTURE), {
          status: 200,
        });
      }
      const supMatch = url.match(/\/v1\/model\/([^/]+)\/supports/);
      if (supMatch) {
        return new Response(JSON.stringify(mockSupports(supMatch[1])), {
          status: 200,
        });
      }
      const depMatch = url.match(/\/v1\/model\/([^/]+)\/depends_on/);
      if (depMatch) {
        return new Response(JSON.stringify(mockDependsOn(depMatch[1])), {
          status: 200,
        });
      }
      const traceMatch = url.match(/\/v1\/model\/([^/]+)\/trace/);
      if (traceMatch) {
        return new Response(
          JSON.stringify({
            node_id: traceMatch[1],
            direction: "back",
            max_depth: 4,
            chain: [],
          }),
          { status: 200 }
        );
      }
      return new Response("not found", { status: 404 });
    })
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function renderPage() {
  return render(
    <MemoryRouter initialEntries={["/model"]}>
      <ModelPage />
    </MemoryRouter>
  );
}

describe("Model page", () => {
  it("renders the six-chip metrics strip", async () => {
    renderPage();
    await waitFor(() =>
      expect(screen.getByTestId("metric-active-nodes")).toBeInTheDocument()
    );
    expect(screen.getByTestId("metric-changed-today")).toBeInTheDocument();
    expect(screen.getByTestId("metric-contested")).toBeInTheDocument();
    expect(screen.getByTestId("metric-awaiting")).toBeInTheDocument();
    expect(screen.getByTestId("metric-blocked")).toBeInTheDocument();
    expect(screen.getByTestId("metric-arr-at-risk")).toBeInTheDocument();
    expect(screen.getByText(/At-risk ARR/)).toBeInTheDocument();
  });

  it("renders all five bands with the seeded fixture", async () => {
    renderPage();
    await waitFor(() =>
      expect(screen.getByTestId("layered-graph")).toBeInTheDocument()
    );
    // Bands are SVG <text> labels, not headings, so query via the
    // rendered SVG textContent. All five band labels must be present.
    const svg = screen.getByTestId("graph-svg");
    const text = svg.textContent ?? "";
    expect(text).toMatch(/GOALS/);
    expect(text).toMatch(/COMMITMENTS/);
    expect(text).toMatch(/DECISIONS/);
    expect(text).toMatch(/CONSTRAINTS \/ RISKS/);
    expect(text).toMatch(/CUSTOMER IMPACT/);
  });

  it("clicking a node opens the inspector with the node title", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() =>
      expect(screen.getByTestId("node-r-1")).toBeInTheDocument()
    );
    await user.click(screen.getByTestId("node-r-1"));
    await waitFor(() =>
      expect(screen.getByTestId("node-inspector")).toBeInTheDocument()
    );
    const inspector = screen.getByTestId("node-inspector");
    expect(
      within(inspector).getByText(
        /Salesforce sync instability threatens anchor renewals/
      )
    ).toBeInTheDocument();
    expect(screen.getByTestId("inspector-critical")).toBeInTheDocument();
  });

  it("LensRail show filter hides goal nodes when toggled off", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() =>
      expect(screen.getByTestId("node-g-1")).toBeInTheDocument()
    );
    // Goal node renders initially.
    expect(screen.queryByTestId("node-g-1")).toBeInTheDocument();
    // Uncheck the Show > Goals toggle.
    const goalToggle = within(
      screen.getByTestId("show-goal")
    ).getByRole("checkbox");
    await user.click(goalToggle);
    await waitFor(() =>
      expect(screen.queryByTestId("node-g-1")).not.toBeInTheDocument()
    );
    // Commitments still visible.
    expect(screen.getByTestId("node-c-1")).toBeInTheDocument();
  });

  it("search filters nodes to those whose title contains the term", async () => {
    const user = userEvent.setup();
    renderPage();
    await waitFor(() =>
      expect(screen.getByTestId("node-r-1")).toBeInTheDocument()
    );
    const search = screen.getByTestId("model-search");
    await user.type(search, "Salesforce");
    await waitFor(() => {
      // The risk and commitment about Salesforce remain.
      expect(screen.getByTestId("node-r-1")).toBeInTheDocument();
      expect(screen.getByTestId("node-c-1")).toBeInTheDocument();
      // Unrelated nodes (goal, pricing decision) are gone.
      expect(screen.queryByTestId("node-g-1")).not.toBeInTheDocument();
      expect(screen.queryByTestId("node-d-1")).not.toBeInTheDocument();
    });
  });
});
