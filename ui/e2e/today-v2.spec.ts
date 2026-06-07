import { test, expect } from "@playwright/test";
import { TODAY_PAGE_FIXTURE } from "../src/api/today-page-mock";

const PRIMARY_ID = "delta-primary-001";
const ACME_ID = "delta-other-acme-deal-reality";

async function routeTodayPage(page: import("@playwright/test").Page, body: unknown, status = 200) {
  await page.route("**/api/today", async (route) => {
    await route.fulfill({
      status,
      contentType: "application/json",
      body: typeof body === "string" ? body : JSON.stringify(body),
    });
  });
}

test.describe("Today v2", () => {
  test("renders sidebar, briefing, review rail, focused review, and action bar", async ({ page }) => {
    await page.goto("/today");

    await expect(page.getByTestId("today-page")).toBeVisible();
    await expect(page.locator(".fy-sidebar__wordmark")).toContainText("Fyralis");
    await expect(page.getByTestId("briefing-header")).toBeVisible();
    await expect(page.getByTestId("fyralis-brief")).toBeVisible();
    await expect(page.getByTestId("review-rail")).toBeVisible();
    await expect(page.getByTestId(`rail-row-${PRIMARY_ID}`)).toHaveAttribute("aria-current", "true");
    await expect(page.getByTestId(`focused-review-${PRIMARY_ID}`)).toBeVisible();
    await expect(page.getByTestId(`action-bar-${PRIMARY_ID}`)).toBeVisible();
  });

  test("selecting Acme shows Deal Reality resolution tracking", async ({ page }) => {
    await page.goto("/today");
    await page.getByTestId(`rail-row-${ACME_ID}`).click();

    const review = page.getByTestId(`focused-review-${ACME_ID}`);
    await expect(review).toBeVisible();
    await expect(review.getByText("Commit → Best Case")).toBeVisible();
    await expect(review.getByText(/economic buyer has not validated ROI/i)).toBeVisible();
    await expect(page.getByTestId("resolution-thread-rt-acme-deal-reality")).toBeVisible();
    await expect(review.getByText("Schedule CFO + security alignment call")).toBeVisible();
  });

  test("J and K move through the review queue", async ({ page }) => {
    await page.goto("/today");
    await expect(page.getByTestId(`focused-review-${PRIMARY_ID}`)).toBeVisible();

    await page.keyboard.press("j");
    await expect(page.getByTestId(`focused-review-${ACME_ID}`)).toBeVisible();
    await expect(page.getByTestId(`rail-row-${ACME_ID}`)).toHaveAttribute("aria-current", "true");

    await page.keyboard.press("k");
    await expect(page.getByTestId(`focused-review-${PRIMARY_ID}`)).toBeVisible();
  });

  test("evidence drawer opens from the focused review and closes with Escape", async ({ page }) => {
    await page.goto("/today");
    await page.getByTestId(`focused-review-evidence-link-${PRIMARY_ID}`).click();

    await expect(page.getByTestId("evidence-drawer")).toBeVisible();
    await expect(page.getByText("Sync failure alert #441")).toBeVisible();

    await page.keyboard.press("Escape");
    await expect(page.getByTestId("evidence-drawer")).toHaveCount(0);
  });

  test("Delegate and correction sheets open and close", async ({ page }) => {
    await page.goto("/today");

    await page.getByTestId(`focused-delegate-${PRIMARY_ID}`).click();
    await expect(page.getByTestId("delegation-sheet")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByTestId("delegation-sheet")).toHaveCount(0);

    await page.getByTestId(`focused-correct-${PRIMARY_ID}`).click();
    await expect(page.getByTestId("correction-sheet")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByTestId("correction-sheet")).toHaveCount(0);
  });

  test("Ask strip returns an answer for the selected change", async ({ page }) => {
    await page.goto("/today");

    await page.getByTestId(`ask-input-${PRIMARY_ID}`).fill("Who should own this?");
    await page.getByTestId(`ask-submit-${PRIMARY_ID}`).click();

    await expect(page.getByTestId(`ask-answer-${PRIMARY_ID}`)).toBeVisible();
    await expect(page.getByTestId(`ask-answer-${PRIMARY_ID}`)).toContainText(/Recommended owner/i);
  });

  test("all-clear state when no changes need judgment", async ({ page }) => {
    await routeTodayPage(page, {
      ...TODAY_PAGE_FIXTURE,
      summary: {
        ...TODAY_PAGE_FIXTURE.summary,
        needJudgment: 0,
        requiresAuthority: 0,
        delegatable: 0,
        monitoring: 0,
      },
      primaryJudgment: null,
      otherChanges: [],
    });

    await page.goto("/today");
    await expect(page.getByTestId("today-all-clear")).toBeVisible();
  });

  test("error state when Today route returns 500", async ({ page }) => {
    await routeTodayPage(page, "boom", 500);

    await page.goto("/today");
    await expect(page.getByTestId("today-error")).toBeVisible();
  });

  test("accept action calls apply endpoint and refreshes", async ({ page }) => {
    let called = false;
    await page.route(`**/api/today/deltas/${PRIMARY_ID}/apply`, async (route) => {
      called = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          status: "applied",
          deltaId: PRIMARY_ID,
          resultMessage: "Applied from e2e",
          ledgerEventId: "ledger-e2e",
        }),
      });
    });

    await page.goto("/today");
    await page.getByTestId(`focused-accept-${PRIMARY_ID}`).click();

    await expect.poll(() => called).toBe(true);
    await expect(page.getByTestId("today-toast")).toContainText("Applied from e2e");
  });
});
