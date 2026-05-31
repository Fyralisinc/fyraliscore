import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";
import { mockBackend } from "./mock-server";

// Vite config for the Company OS CEO view.
//
// Two run modes:
// 1. `USE_MOCK=1 npm run dev` — serves /api/* and /stream/* locally from
//    src/api/mock-data.ts. Used while Agent-GRT + Agent-QRY are still
//    landing the real services. Zero external deps.
// 2. `npm run dev` (default) — proxies /api/* and /stream/* to
//    http://localhost:8000 where the FastAPI gateway is expected.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const useMock = env.USE_MOCK === "1" || env.VITE_USE_MOCK === "1";
  // Where /api and /stream proxy to. Defaults to the dev-stack gateway on :8000;
  // override (e.g. VITE_GATEWAY_TARGET=http://localhost:8010) to point at an
  // isolated gateway such as scripts/run_finance_demo_gateway.py.
  const gatewayTarget = env.VITE_GATEWAY_TARGET || "http://localhost:8000";
  const wsTarget = env.VITE_GATEWAY_WS_TARGET || gatewayTarget.replace(/^http/, "ws");
  return {
    plugins: [react(), ...(useMock ? [mockBackend()] : [])],
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      port: 5173,
      proxy: useMock
        ? undefined
        : {
            "/api": {
              target: gatewayTarget,
              changeOrigin: true,
              rewrite: (p) => p.replace(/^\/api/, ""),
            },
            "/stream": {
              target: wsTarget,
              ws: true,
              rewrite: (p) => p.replace(/^\/stream/, ""),
            },
          },
    },
    build: {
      outDir: "dist",
      sourcemap: true,
    },
  };
});
