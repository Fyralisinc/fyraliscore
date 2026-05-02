// SSE client for /v1/recommendations/stream.
// Uses fetch + ReadableStream rather than EventSource because the backend
// only accepts the bearer token via the Authorization header, which the
// browser EventSource API cannot set.

export type RecStreamEvent =
  | { event: "ready"; data: Record<string, unknown> }
  | {
      event: "created" | "updated" | "archived";
      recommendation_id: string;
      summary: Record<string, unknown>;
    };

type EventHandler = (ev: RecStreamEvent) => void;
type ErrorHandler = (err: Error) => void;

const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "/api";
const PATH = "/v1/recommendations/stream";

export class RecommendationStream {
  private controller: AbortController | null = null;
  private retry = 0;
  private retryTimer: number | null = null;
  private closed = false;
  private token = "";
  private onEvent: EventHandler = () => {};
  private onError: ErrorHandler = () => {};

  connect(token: string, onEvent: EventHandler, onError: ErrorHandler): void {
    this.token = token;
    this.onEvent = onEvent;
    this.onError = onError;
    this.closed = false;
    this.retry = 0;
    void this.openOnce();
  }

  close(): void {
    this.closed = true;
    if (this.retryTimer !== null) {
      window.clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.controller) {
      this.controller.abort();
      this.controller = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.closed) return;
    const delay = Math.min(30_000, 1_000 * 2 ** this.retry);
    this.retry += 1;
    this.retryTimer = window.setTimeout(() => {
      void this.openOnce();
    }, delay);
  }

  private async openOnce(): Promise<void> {
    if (this.closed) return;
    this.controller = new AbortController();
    try {
      const res = await fetch(`${BASE}${PATH}`, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${this.token}`,
          Accept: "text/event-stream",
        },
        signal: this.controller.signal,
      });
      if (!res.ok || !res.body) {
        throw new Error(`stream ${res.status}`);
      }
      this.retry = 0;
      const reader = res.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let buf = "";
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        // SSE frames are separated by a blank line.
        let idx: number;
        while ((idx = buf.indexOf("\n\n")) !== -1) {
          const frame = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          this.handleFrame(frame);
        }
      }
      // Reader closed — server hung up; reconnect.
      this.scheduleReconnect();
    } catch (err) {
      if (this.closed) return;
      this.onError(err instanceof Error ? err : new Error(String(err)));
      this.scheduleReconnect();
    }
  }

  private handleFrame(frame: string): void {
    let eventName: string | null = null;
    const dataLines: string[] = [];
    for (const line of frame.split("\n")) {
      if (!line || line.startsWith(":")) continue; // heartbeat/comment
      if (line.startsWith("event:")) {
        eventName = line.slice(6).trim();
      } else if (line.startsWith("data:")) {
        dataLines.push(line.slice(5).trim());
      }
    }
    if (dataLines.length === 0) return;
    let parsed: any;
    try {
      parsed = JSON.parse(dataLines.join("\n"));
    } catch {
      return;
    }
    if (eventName === "ready") {
      this.onEvent({ event: "ready", data: parsed });
      return;
    }
    const ev = (parsed?.event as string) ?? eventName;
    if (ev === "created" || ev === "updated" || ev === "archived") {
      this.onEvent({
        event: ev,
        recommendation_id: String(parsed.recommendation_id ?? ""),
        summary: parsed.summary ?? {},
      });
    }
  }
}
