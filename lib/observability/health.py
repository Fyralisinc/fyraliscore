"""lib/observability/health.py — generic /healthz + /metrics worker server.

Generalization of services/ingest/ingestion/observability.py (which stays
canonical for the ingestion consumers and keeps its `ingestion_` families)
for workers OUTSIDE the ingest layer — think_worker, post_commit_worker —
where importing services.ingest would invert the layer dependency.

Same operational contract:
  GET /healthz  — 200 while the heartbeat is fresh, 503 when stale, so a
                  hung (not just dead) worker is restarted by compose.
  GET /metrics  — caller-supplied Prometheus text + heartbeat/uptime gauges.

Heartbeat lines are emitted as `worker_heartbeat_age_seconds{worker=...}` /
`worker_uptime_seconds{worker=...}`; alert rules OR these with the legacy
`ingestion_heartbeat_age_seconds`.

Enabled by INGESTION_HEALTH_PORT — deliberately the same env var the
ingestion consumers use, because the compose `x-app-env` anchor already
sets it for every container; wiring a worker makes it scrapeable with no
compose env change.
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
    """Touch the heartbeat while the event loop is responsive; a wedged
    loop stops ticking and /healthz goes 503 (see the ingestion twin)."""
    while not stop_event.is_set():
        heartbeat.touch()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


def start_health_server(
    *,
    worker_name: str,
    render_metrics: Callable[[], str],
    heartbeat: Heartbeat,
    port: int | None = None,
    stale_sec: float | None = None,
) -> ThreadingHTTPServer | None:
    """Start the /healthz + /metrics daemon-thread server.

    Returns the server (callers shut it down on exit) or None when
    INGESTION_HEALTH_PORT is unset/0 — the disabled default, so tests and
    bare local runs are unaffected. An explicitly-passed port (including 0
    for an OS-assigned test port) is always honored.
    """
    if port is None:
        port = int(os.environ.get("INGESTION_HEALTH_PORT", "0"))
        if not port:
            return None
    if stale_sec is None:
        stale_sec = float(os.environ.get("INGESTION_HEALTH_STALE_SEC", "120"))

    def _render() -> bytes:
        body = render_metrics() or ""
        if body and not body.endswith("\n"):
            body += "\n"
        body += (
            f'worker_heartbeat_age_seconds{{worker="{worker_name}"}} '
            f"{heartbeat.age():.3f}\n"
            f'worker_uptime_seconds{{worker="{worker_name}"}} '
            f"{time.time() - heartbeat.started_at:.0f}\n"
        )
        return body.encode()

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
                    "worker": worker_name,
                    "heartbeat_age_s": round(age, 3),
                    "stale_threshold_s": stale_sec,
                }).encode()
                self._send(200 if ok else 503, body, "application/json")
            elif self.path.startswith("/metrics"):
                try:
                    body = _render()
                except Exception:  # noqa: BLE001 — scrape must not crash
                    body = b""
                self._send(200, body, "text/plain; version=0.0.4")
            else:
                self._send(404, b"not found\n", "text/plain")

    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name=f"{worker_name}-health-{port}",
    )
    thread.start()
    return server


__all__ = ["Heartbeat", "run_heartbeat_ticker", "start_health_server"]
