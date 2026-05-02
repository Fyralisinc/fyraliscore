import { test, expect } from "@playwright/test";

const BASE = "http://127.0.0.1:5173";

test("expanded card shows multi-section reasoning, not just the headline", async ({ page }) => {
  await page.goto(`${BASE}/demo`);
  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await page.getByTestId("start-truss").click();
  await page.waitForURL(`${BASE}/`);
  await page.waitForTimeout(2500);

  // Expand the SSO design partners card (top of Truss list).
  const topCard = page.locator("article.card").first();
  const headline = (await topCard.locator(".card-headline, .headline").innerText())
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  console.log(`headline: ${headline.slice(0, 80)}`);

  await topCard.locator(".expand-cta").click();
  await page.waitForTimeout(500);

  const reasoning = topCard.locator(".detail-voice");
  await expect(reasoning).toBeVisible();
  const reasoningText = (await reasoning.innerText()).replace(/\s+/g, " ").trim();
  console.log(`reasoning length: ${reasoningText.length}`);
  console.log(`reasoning preview: ${reasoningText.slice(0, 400)}`);

  // It should be substantially longer than the headline (the bug was
  // reasoning ≈ headline; now it should be a multi-section walkthrough).
  expect(reasoningText.length).toBeGreaterThan(200);

  // Should contain section headings from the reasoning chain.
  const headings = await reasoning.locator(".reasoning-heading").allInnerTexts();
  console.log(`section headings: ${JSON.stringify(headings)}`);
  expect(headings.length).toBeGreaterThanOrEqual(2);
  expect(headings.join(" ").toLowerCase()).toContain("asking you to do");

  // Should cite confidence percentages from the supporting models.
  const confs = await reasoning.locator(".reasoning-conf").count();
  console.log(`confidence chips: ${confs}`);
  expect(confs).toBeGreaterThan(0);
});
