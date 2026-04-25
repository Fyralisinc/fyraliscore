import { defineConfig } from "@playwright/test";

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
    baseURL: "http://localhost:5173",
    trace: "retain-on-failure",
  },
  webServer: {
    command: "USE_MOCK=1 npm run dev -- --port 5173",
    url: "http://localhost:5173",
    reuseExistingServer: true,
    timeout: 60_000,
  },
});
