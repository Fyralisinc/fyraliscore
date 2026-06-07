import { defineConfig } from "@playwright/test";

const port = Number(process.env.E2E_PORT ?? "5173");
const baseURL =
  process.env.PLAYWRIGHT_BASE_URL ?? `http://localhost:${port}`;
const reuseExistingServer = process.env.E2E_REUSE_SERVER !== "0";

// Playwright config for the Company OS UI. The E2E suite assumes that
// the Gateway + Postgres + Ollama are running on the developer's
// machine (documented in e2e/alice-merges-pr.spec.ts). The Vite dev
// server is started by the `webServer` block below.
export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  fullyParallel: false,
  retries: 0,
  reporter: [["list"]],
  use: {
    baseURL,
    trace: "retain-on-failure",
  },
  webServer: {
    command: `USE_MOCK=1 npm run dev -- --host localhost --port ${port} --strictPort`,
    url: baseURL,
    reuseExistingServer,
    timeout: 60_000,
  },
});
