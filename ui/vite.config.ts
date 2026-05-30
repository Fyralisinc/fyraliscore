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
// 2. `npm run dev` (default) — proxies /api/* and /stream/* to the FastAPI
//    gateway. Origin defaults to http://localhost:8000 but can be overridden
//    with GATEWAY_URL — needed when running the gateway from source on another
//    port to bypass a stale prebuilt container holding :8000:
//      GATEWAY_URL=http://127.0.0.1:8011 npm run dev
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const useMock = env.USE_MOCK === "1" || env.VITE_USE_MOCK === "1";
  const gatewayHttp = env.GATEWAY_URL || "http://localhost:8000";
  const gatewayWs = gatewayHttp.replace(/^http/, "ws");
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
              target: gatewayHttp,
              changeOrigin: true,
              rewrite: (p) => p.replace(/^\/api/, ""),
            },
            "/stream": {
              target: gatewayWs,
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
