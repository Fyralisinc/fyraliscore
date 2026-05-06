/*
 * Vite plugin that serves the Fyralis Today contract against the
 * fixture in src/api/today-mock.ts. Activated when the `/api` proxy
 * target isn't available (so the UI never has to block on backend
 * services that haven't landed yet).
 *
 * Switch with `USE_MOCK=1 npm run dev` (or set it in .env.local).
 *
 * The legacy /view/ceo/* HTTP+WS contract is also served here so the
 * Today page's Ask Zone (which still uses /view/ceo/ask + turn-action)
 * keeps working end-to-end in mock mode.
 */

import type { Plugin } from "vite";
import { HOME_FIXTURE, mockAsk } from "./src/api/mock-data";
import { TODAY_FIXTURE, mockTriage } from "./src/api/today-mock";
import {
  ARCS_NARRATIVE_STATEMENT,
  CHRONICLE_PERIOD_STATEMENT,
  PREDICTIONS_NARRATIVE_STATEMENT,
  SAMPLE_ARCS,
  SAMPLE_CALIBRATION,
  SAMPLE_EVENTS,
  SAMPLE_LAYER_COUNTS,
  SAMPLE_PREDICTIONS,
} from "./src/components/history/sample-data";
import type {
  TriageAction,
  TriageResponse,
  CardConversation,
  CardExchange,
  ProbeRequest,
  ProbeResponse,
  RecCard,
} from "./src/api/today-types";
import type { TurnActionResponse } from "./src/api/types";

// Driftwood revision: in-memory conversation store keyed by card id.
const CONVERSATIONS = new Map<string, CardConversation>();

// Decorate fixture cards on the fly so we don't have to hand-edit every
// `<em>` into a `<probe>`. The decorator wraps the first <em>...</em>
// in headline/supporting with a probe id derived from its text and
// attaches a default probe_chips array per card kind.
function decorateCardsForRevision(today: typeof TODAY_FIXTURE) {
  const cards = today.cards.map((card: RecCard) => {
    const conversation_id = `conv-${card.id}`;
    const headline_html = wrapEmAsProbe(card.headline_html, card.id, "h");
    const supporting_html = card.supporting_html
      ? wrapEmAsProbe(card.supporting_html, card.id, "s")
      : card.supporting_html;
    const probe_chips = defaultChipsFor(card);
    return {
      ...card,
      headline_html,
      supporting_html,
      detail: {
        ...(card.detail ?? {}),
        probe_chips,
        conversation_id,
      },
    };
  });
  return { ...today, cards };
}

function wrapEmAsProbe(html: string, cardId: string, prefix: string): string {
  let i = 0;
  return html.replace(/<em>([^<]+)<\/em>/g, (_m, text: string) => {
    i += 1;
    const slug = text
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "")
      .slice(0, 32) || `p${i}`;
    return `<span data-probe-id="${prefix}-${cardId}-${slug}-${i}">${text}</span>`;
  });
}

function defaultChipsFor(card: RecCard) {
  const id = card.id;
  // Card-kind-specific chips per spec §4.1.
  if (card.kind_label.toLowerCase().includes("decision")) {
    return [
      { id: `${id}:why`, text: "Why this decision specifically?" },
      { id: `${id}:contradicting`, text: "What's contradicting it?" },
      { id: `${id}:history`, text: "Have we ratified before?" },
      { id: `${id}:drift-cost`, text: "What if I let it drift?" },
    ];
  }
  if (card.category === "strategic") {
    return [
      { id: `${id}:why-pattern`, text: "Why this pattern matters?" },
      { id: `${id}:customer-asks`, text: "Show me the customer asks" },
      { id: `${id}:cost`, text: "What's the engineering cost?" },
      { id: `${id}:change-mind`, text: "What would change your mind?" },
    ];
  }
  return [
    { id: `${id}:why`, text: "Why are you flagging this?" },
    { id: `${id}:evidence`, text: "Show me the evidence" },
    { id: `${id}:options`, text: "What are my options?" },
  ];
}

function getOrCreateConversation(cardId: string): CardConversation {
  let c = CONVERSATIONS.get(cardId);
  if (!c) {
    c = {
      conversation_id: `conv-${cardId}`,
      card_id: cardId,
      exchanges: [],
      probed_phrase_ids: [],
      used_chip_ids: [],
      archived: false,
    };
    CONVERSATIONS.set(cardId, c);
  }
  return c;
}

function mockProbeResponse(cardId: string, body: ProbeRequest): ProbeResponse {
  const conv = getOrCreateConversation(cardId);
  const now = new Date().toISOString();
  const exchangeId = `exch-${cardId}-${conv.exchanges.length + 1}`;
  let probe_action = "You probed";
  let probe_text = "";
  let probe_id: string | undefined;
  let probe_kind: "phrase" | "chip" | "ask" = "chip";
  let response_html = "";
  if (body.kind === "phrase") {
    probe_kind = "phrase";
    probe_id = body.probe_id;
    probe_action = "You clicked";
    probe_text = `"${body.probe_id.split("-").slice(2).join(" ")}"`;
    response_html =
      `<p>Here's what's behind that phrase. The substrate keeps a ` +
      `provenance trail for every claim — this one resolves to a ` +
      `cluster of <span data-probe-id="${cardId}:cluster">three signals</span> ` +
      `over the last 14 days.</p>`;
  } else if (body.kind === "chip") {
    probe_kind = "chip";
    probe_id = body.probe_id;
    probe_action = "You probed";
    // Find the chip text from the fixture so the header reads naturally.
    const card = TODAY_FIXTURE.cards.find((c) => c.id === cardId);
    const chips = card ? defaultChipsFor(card) : [];
    probe_text = chips.find((c) => c.id === body.probe_id)?.text ?? body.probe_id;
    response_html =
      `<p>Short answer: yes — and here's the reasoning. ` +
      `<span data-probe-id="${cardId}:why-d5">d-5</span> is the only ` +
      `structurally-load-bearing decision in this domain, which is why ` +
      `the cluster matters.</p>`;
  } else {
    probe_kind = "ask";
    probe_action = "You asked";
    probe_text = body.query;
    response_html =
      `<p>Here's how I'd think about that. ${escapeHtml(body.query)} — ` +
      `interesting framing. My current read is that the ` +
      `<span data-probe-id="${cardId}:tradeoff">primary tradeoff</span> ` +
      `is between speed of resolution and information value.</p>`;
  }
  const exchange: CardExchange = {
    id: exchangeId,
    conversation_id: conv.conversation_id,
    probe_kind,
    probe_id,
    probe_action,
    probe_text,
    response_html,
    follow_ups: [
      { id: `${cardId}:fu1:${exchangeId}`, text: "Show me the conversations" },
      { id: `${cardId}:fu2:${exchangeId}`, text: "Compare to other patterns" },
    ],
    created_at: now,
  };
  conv.exchanges.push(exchange);
  conv.last_probed_at = now;
  if (probe_kind === "phrase" && probe_id && !conv.probed_phrase_ids.includes(probe_id)) {
    conv.probed_phrase_ids.push(probe_id);
  }
  if (probe_kind === "chip" && probe_id && !conv.used_chip_ids.includes(probe_id)) {
    conv.used_chip_ids.push(probe_id);
  }
  return { exchange };
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}
import { WebSocketServer, WebSocket } from "ws";
import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";

function json(
  res: import("node:http").ServerResponse,
  body: unknown,
  status = 200
) {
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
  let brandName = TODAY_FIXTURE.brand.name;
  let brandMark = TODAY_FIXTURE.brand.mark;
  return {
    name: "fyralis-mock-backend",
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        const url = req.url ?? "";
        const method = req.method ?? "GET";

        // ---- Fyralis Today surface ------------------------------
        if (method === "GET" && url.startsWith("/api/v1/today")) {
          const decorated = decorateCardsForRevision(TODAY_FIXTURE);
          json(res, { ...decorated, brand: { ...TODAY_FIXTURE.brand, name: brandName, mark: brandMark } });
          return;
        }

        // ---- Driftwood revision: card conversation surface ------
        const conversationMatch = url.match(
          /^\/api\/v1\/cards\/([^/]+)\/conversation(?:\?.*)?$/
        );
        if (method === "GET" && conversationMatch) {
          const id = conversationMatch[1];
          const conv = CONVERSATIONS.get(id);
          if (!conv) {
            res.statusCode = 404;
            res.setHeader("content-type", "application/json");
            res.end(JSON.stringify({ error: "no_conversation" }));
            return;
          }
          json(res, conv);
          return;
        }
        if (method === "DELETE" && conversationMatch) {
          CONVERSATIONS.delete(conversationMatch[1]);
          json(res, { ok: true });
          return;
        }
        const probeMatch = url.match(
          /^\/api\/v1\/cards\/([^/]+)\/probe(?:\?.*)?$/
        );
        // Test-only escape hatch: wipe all in-memory conversations so
        // Playwright specs start with a clean slate between tests.
        if (method === "POST" && url.startsWith("/api/__test__/reset-conversations")) {
          CONVERSATIONS.clear();
          json(res, { ok: true });
          return;
        }
        if (method === "POST" && probeMatch) {
          const id = probeMatch[1];
          const body = (await readJson(req)) as ProbeRequest;
          // Tiny artificial latency so the thinking indicator is
          // visible in interactive testing.
          await new Promise((r) => setTimeout(r, 150));
          json(res, mockProbeResponse(id, body));
          return;
        }

        // ---- History page surface --------------------------------
        if (method === "GET" && url.startsWith("/api/v1/history")) {
          json(res, {
            events: SAMPLE_EVENTS,
            predictions: SAMPLE_PREDICTIONS,
            arcs: SAMPLE_ARCS,
            calibration: SAMPLE_CALIBRATION,
            layer_counts: SAMPLE_LAYER_COUNTS,
            chronicle_statement: CHRONICLE_PERIOD_STATEMENT,
            predictions_statement: PREDICTIONS_NARRATIVE_STATEMENT,
            arcs_statement: ARCS_NARRATIVE_STATEMENT,
            period: new URLSearchParams(url.split("?")[1] ?? "").get("period") ?? "90d",
          });
          return;
        }

        if (method === "POST" && url.startsWith("/api/v1/today/brand")) {
          const body = await readJson(req);
          const next = String(body?.name ?? "").trim();
          if (next) {
            brandName = next;
            brandMark = next.charAt(0).toUpperCase();
          }
          json(res, { ok: true, name: brandName });
          return;
        }

        const triageMatch = url.match(
          /^\/api\/v1\/recommendations\/([^/]+)\/triage(?:\?.*)?$/
        );
        if (method === "POST" && triageMatch) {
          const id = triageMatch[1];
          const body = await readJson(req);
          const action = (body?.action ?? "act") as TriageAction;
          const resp: TriageResponse = mockTriage(id, action);
          json(res, resp);
          return;
        }

        // ---- Legacy CEO view (Ask Zone still uses these) ---------
        if (method === "GET" && url.startsWith("/api/view/ceo/home")) {
          json(res, HOME_FIXTURE);
          return;
        }
        if (method === "POST" && url.startsWith("/api/view/ceo/ask")) {
          const body = await readJson(req);
          json(res, mockAsk(String(body.query ?? "")));
          return;
        }
        if (method === "POST" && url.startsWith("/api/view/ceo/turn-action")) {
          const r: TurnActionResponse = { ok: true };
          json(res, r);
          return;
        }
        next();
      });

      // WS server at /stream/view/ceo/stream. Vite multiplexes upgrades.
      wss = new WebSocketServer({ noServer: true });
      const onUpgrade = (
        req: IncomingMessage,
        socket: Duplex,
        head: Buffer
      ) => {
        const path = req.url ?? "";
        if (!path.startsWith("/stream/view/ceo/stream")) return;
        wss!.handleUpgrade(req, socket, head, (ws: WebSocket) => {
          // Send the Today snapshot immediately so reconnecting clients
          // re-hydrate without an extra HTTP round-trip.
          // The WS hot-replaces the in-memory `today` state, so it must
          // ship the same Driftwood-decorated payload that the HTTP
          // /v1/today endpoint serves. Without this, the cards arrive
          // via HTTP with probe markup, then the WS overwrites them
          // with bare-em fixture cards and the probe row vanishes.
          const decorated = decorateCardsForRevision(TODAY_FIXTURE);
          ws.send(
            JSON.stringify({
              type: "today_updated",
              today: { ...decorated, brand: { ...TODAY_FIXTURE.brand, name: brandName, mark: brandMark } },
            })
          );
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
