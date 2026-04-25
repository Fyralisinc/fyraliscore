// WebSocket client with auto-reconnect for /view/ceo/stream.
// Contract source: CONTRACTS.md §1.4. Reconnect uses capped exponential
// backoff; heartbeats are handled server-side (30s per the contract) so
// the client only needs to notice dead sockets through `onclose`.

import type { StreamMessage } from "./types";

type Listener = (msg: StreamMessage) => void;
type ConnListener = (state: "open" | "closed" | "reconnecting") => void;

export interface StreamClient {
  start(): void;
  stop(): void;
  subscribe(fn: Listener): () => void;
  onConnectionChange(fn: ConnListener): () => void;
}

const DEFAULT_PATH = "/stream/view/ceo/stream";

export function createStreamClient(
  path: string = DEFAULT_PATH
): StreamClient {
  let ws: WebSocket | null = null;
  let stopped = false;
  let retry = 0;
  let retryTimer: number | null = null;
  const listeners = new Set<Listener>();
  const connListeners = new Set<ConnListener>();

  function url(): string {
    const loc = window.location;
    const proto = loc.protocol === "https:" ? "wss:" : "ws:";
    if (path.startsWith("ws://") || path.startsWith("wss://")) return path;
    return `${proto}//${loc.host}${path}`;
  }

  function notifyConn(state: "open" | "closed" | "reconnecting") {
    connListeners.forEach((fn) => fn(state));
  }

  function connect() {
    if (stopped) return;
    try {
      ws = new WebSocket(url());
    } catch {
      schedule();
      return;
    }
    ws.onopen = () => {
      retry = 0;
      notifyConn("open");
    };
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data) as StreamMessage;
        listeners.forEach((fn) => fn(msg));
      } catch {
        // ignore malformed frames; the contract says every frame is JSON
      }
    };
    ws.onerror = () => {
      // onclose follows; handle there
    };
    ws.onclose = () => {
      notifyConn("closed");
      if (!stopped) schedule();
    };
  }

  function schedule() {
    notifyConn("reconnecting");
    const delay = Math.min(30_000, 500 * 2 ** retry);
    retry += 1;
    retryTimer = window.setTimeout(connect, delay);
  }

  return {
    start() {
      stopped = false;
      connect();
    },
    stop() {
      stopped = true;
      if (retryTimer !== null) {
        window.clearTimeout(retryTimer);
        retryTimer = null;
      }
      if (ws) {
        ws.onclose = null;
        ws.close();
        ws = null;
      }
    },
    subscribe(fn) {
      listeners.add(fn);
      return () => listeners.delete(fn);
    },
    onConnectionChange(fn) {
      connListeners.add(fn);
      return () => connListeners.delete(fn);
    },
  };
}
