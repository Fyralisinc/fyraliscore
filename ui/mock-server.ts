/*
 * Vite plugin that serves the CEO view HTTP + WS contract against the
 * fixture in src/api/mock-data.ts. Activated when the `/api` proxy target
 * isn't available — Agent-GRT and Agent-QRY are still landing backend
 * services, so the UI can't block on them.
 *
 * Switch with `USE_MOCK=1 npm run dev` (or set it in .env.local).
 */

import type { Plugin } from "vite";
import { HOME_FIXTURE, mockAsk } from "./src/api/mock-data";
import type { TurnActionResponse } from "./src/api/types";
import { WebSocketServer, WebSocket } from "ws";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";

function json(res: import("node:http").ServerResponse, body: unknown, status = 200) {
  res.statusCode = status;
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify(body));
}

async function readJson(req: IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    return {};
  }
}

export function mockBackend(): Plugin {
  let wss: WebSocketServer | null = null;
  return {
    name: "company-os-mock-backend",
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        const url = req.url ?? "";
        if (req.method === "GET" && url.startsWith("/api/view/ceo/home")) {
          json(res, HOME_FIXTURE);
          return;
        }
        if (req.method === "POST" && url.startsWith("/api/view/ceo/ask")) {
          const body = await readJson(req);
          json(res, mockAsk(String(body.query ?? "")));
          return;
        }
        if (
          req.method === "POST" &&
          url.startsWith("/api/view/ceo/turn-action")
        ) {
          const r: TurnActionResponse = { ok: true };
          json(res, r);
          return;
        }
        next();
      });

      // Attach a WS server at /stream/view/ceo/stream. Vite's http server
      // already multiplexes upgrades, so we just install an upgrade hook.
      wss = new WebSocketServer({ noServer: true });
      const onUpgrade = (
        req: IncomingMessage,
        socket: Duplex,
        head: Buffer
      ) => {
        const path = req.url ?? "";
        if (!path.startsWith("/stream/view/ceo/stream")) return;
        wss!.handleUpgrade(req, socket, head, (ws: WebSocket) => {
          // Send snapshot immediately so reconnecting clients re-hydrate.
          ws.send(
            JSON.stringify({
              type: "greeting_updated",
              greeting: HOME_FIXTURE.greeting,
            })
          );
          // Heartbeat every 30s per contract.
          const hb = setInterval(() => {
            if (ws.readyState === ws.OPEN) ws.ping();
          }, 30_000);
          ws.on("close", () => clearInterval(hb));
        });
      };
      server.httpServer?.on("upgrade", onUpgrade);
    },
  };
}
