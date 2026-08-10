"""Lightweight health + metrics HTTP surface for the ingestion consumers.

The Kafka consumers are bare asyncio processes with no health surface, so
an orchestrator could detect a *dead* process (docker `restart`) but not a
*hung* one (wedged poll / stuck S3 or DB call), and the in-process metric
dicts were never exported. This module adds an opt-in stdlib HTTP server
(daemon thread; no new dependency, no coupling to the consumer's event
loop, safe for the normalizer's Path B no-DB invariant) exposing:

  GET /healthz  — 200 if the worker reported progress within
                  INGESTION_HEALTH_STALE_SEC (default 120s), else 503.
                  A hung consumer goes 503 and the orchestrator restarts
                  it. The worker `touch()`es the heartbeat each loop tick;
                  an idle consumer also touches on each empty poll, so
                  "stale" means genuinely wedged, not merely idle.
  GET /metrics  — the worker's in-process counters in Prometheus text
                  format. Scrapeable; harmless if unused.

Enable by setting INGESTION_HEALTH_PORT in the process env. Disabled
(no server) when unset/0, so tests and local runs are unaffected.
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable


class Heartbeat:
    """Monotonic last-progress marker the worker touches each tick."""

    def __init__(self) -> None:
        self._last = time.monotonic()
        self.started_at = time.time()

    def touch(self) -> None:
        self._last = time.monotonic()

    def age(self) -> float:
        return time.monotonic() - self._last


async def run_heartbeat_ticker(
    heartbeat: Heartbeat,
    stop_event: asyncio.Event,
    *,
    interval: float = 5.0,
) -> None:
    """Touch the heartbeat every `interval` seconds while the event loop
    is responsive. A consumer blocked in an idle (but healthy) poll keeps
    ticking; a wedged loop (blocked thread / deadlock) stops ticking and
    /healthz goes 503 so the orchestrator can restart it. Exits cleanly
    when the stop event is set."""
    while not stop_event.is_set():
        heartbeat.touch()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


def render_prometheus(metrics: dict[str, float], heartbeat: Heartbeat) -> str:
    """Render an in-process metric dict as Prometheus text exposition.

    Appends the shared lib.observability registry (ollama, db pool, kafka
    producer, oauth, connector counters) so every worker that
    serves /metrics exposes whatever shared-library instrumentation fired
    in its process — no per-worker wiring.
    """
    lines: list[str] = []
    for key, value in sorted(metrics.items()):
        name = "ingestion_" + key.replace(".", "_").replace("-", "_")
        lines.append(f"{name} {value}")
    lines.append(f"ingestion_heartbeat_age_seconds {heartbeat.age():.3f}")
    lines.append(
        f"ingestion_uptime_seconds {time.time() - heartbeat.started_at:.0f}"
    )
    text = "\n".join(lines) + "\n"
    try:
        from lib.observability.metrics import render_default

        text += render_default()
    except Exception:  # noqa: BLE001 — scrape must not 500 over shared metrics
        pass
    return text


def start_health_server(
    *,
    get_metrics: Callable[[], dict[str, float]],
    heartbeat: Heartbeat,
    port: int | None = None,
    stale_sec: float | None = None,
) -> ThreadingHTTPServer | None:
    """Start the /healthz + /metrics server in a daemon thread.

    Returns the server (so callers can shut it down) or None when
    INGESTION_HEALTH_PORT is unset/0 (the disabled default).
    """
    if port is None:
        # Disabled unless INGESTION_HEALTH_PORT is set non-zero. (An
        # explicitly-passed port — including 0 for an OS-assigned
        # ephemeral port in tests — is always honored.)
        port = int(os.environ.get("INGESTION_HEALTH_PORT", "0"))
        if not port:
            return None
    if stale_sec is None:
        stale_sec = float(os.environ.get("INGESTION_HEALTH_STALE_SEC", "120"))

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args) -> None:  # silence access logging
            pass

        def _send(self, code: int, body: bytes, content_type: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 (stdlib casing)
            if self.path.startswith("/healthz"):
                age = heartbeat.age()
                ok = age <= stale_sec
                body = json.dumps({
                    "status": "ok" if ok else "stale",
                    "heartbeat_age_s": round(age, 3),
                    "stale_threshold_s": stale_sec,
                }).encode()
                self._send(200 if ok else 503, body, "application/json")
            elif self.path.startswith("/metrics"):
                body = render_prometheus(get_metrics(), heartbeat).encode()
                self._send(200, body, "text/plain; version=0.0.4")
            else:
                self._send(404, b"not found\n", "text/plain")

    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name=f"ingestion-health-{port}",
    )
    thread.start()
    return server


__all__ = [
    "Heartbeat",
    "render_prometheus",
    "run_heartbeat_ticker",
    "start_health_server",
]
