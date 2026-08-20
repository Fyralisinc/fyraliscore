#!/usr/bin/env python3
"""demo-dataplane / metrics_stub.py — a tiny stand-in DATA PLANE for the
testable control-plane bring-up.

WHY THIS EXISTS
---------------
The real Fyralis data plane (ingestion + reasoning + product + Postgres + Kafka
+ ~29 scrape targets) is a heavyweight stack that runs INSIDE the customer VPC.
For a one-command, CTO-runnable demo of the *control plane* we do not want to
stand the whole thing up — we only need a realistic SCRAPE TARGET so the
boundary OTel Collector has something to scrape, redact, identity-stamp, and
remote-write through the auth-proxy into central Mimir. That end-to-end path
(data plane -> boundary -> mTLS auth-proxy -> Mimir -> Grafana fleet view) is
the thing under test; the data plane's internals are not.

So this module exposes the **golden-12 SLI** ``fyralis_*`` metric families on
``:9300`` in Prometheus text format — exactly the names the boundary collector's
allowlist keeps (boundary/otel-collector-config.yaml) and the fleet-sli rules
aggregate (fleet-sli/recording_rules.yml). One worker target, healthy-looking
values, gently jittered each scrape so the Grafana panels move.

This is a STUB, not the product. It emits plausible-but-synthetic numbers and
opens no database/Kafka. In PRODUCTION the installer points the boundary
collector at the REAL data-plane targets (workers :9300, gateway :8000,
postgres-exporter :9187, kafka-exporter :9308, …); see
``installer/`` and ``boundary/otel-collector-config.yaml``. Nothing here ships
to a customer — it is a demo fixture for the bring-up only.

Dependency-light: stdlib only (http.server). No prometheus_client needed.

Endpoints
---------
  GET /metrics   Prometheus text exposition of the golden-12 families.
  GET /healthz   {"status":"ok"} liveness (also what the agent SLI probe hits).
  GET /          short human note.

Env
---
  DEMO_DP_HOST            (default 0.0.0.0)
  DEMO_DP_PORT            (default 9300)
  DEMO_DP_TENANT          (default acme)        informational label
  DEMO_DP_DEPLOYMENT      (default acme-use1-0001)
  DEMO_DP_REGION          (default us-east-1)
  DEMO_DP_SCENARIO        (default healthy)     healthy|degraded — flips a few
                          SLIs into the yellow band so the fleet roll-up colour
                          and alerts can be demoed.
"""

from __future__ import annotations

import json
import os
import random
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --------------------------------------------------------------------------- #
# Config                                                                      #
# --------------------------------------------------------------------------- #
HOST = os.environ.get("DEMO_DP_HOST", "0.0.0.0")
PORT = int(os.environ.get("DEMO_DP_PORT", "9300"))
TENANT = os.environ.get("DEMO_DP_TENANT", "acme")
DEPLOYMENT = os.environ.get("DEMO_DP_DEPLOYMENT", "acme-use1-0001")
REGION = os.environ.get("DEMO_DP_REGION", "us-east-1")
SCENARIO = os.environ.get("DEMO_DP_SCENARIO", "healthy").strip().lower()

_T0 = time.time()


def _jitter(base: float, frac: float = 0.15) -> float:
    """A small deterministic-ish wobble so dashboards aren't flat lines."""
    span = base * frac
    return max(0.0, base + random.uniform(-span, span))


# --------------------------------------------------------------------------- #
# The golden-12 SLI families.                                                 #
# Names + label shapes mirror what the boundary allowlist keeps and the       #
# fleet-sli recording rules aggregate. We do NOT stamp tenant/deployment      #
# labels here — the boundary collector's `resource` processor adds the        #
# authoritative tenant_id/deployment_id/region (C4). We DO emit the bounded   #
# enum labels (job, source, pool, provider, state, status) the rules group by.#
# --------------------------------------------------------------------------- #
WORKERS = [
    "normalizer",
    "observation-writer",
    "reconciler",
    "think-worker",
    "post-commit-worker",
    "embedding-worker",
]
SOURCES = ["github", "slack", "jira", "notion"]


def _line(name: str, value, labels: dict | None = None) -> str:
    if labels:
        lbl = ",".join(f'{k}="{v}"' for k, v in labels.items())
        return f"{name}{{{lbl}}} {value}"
    return f"{name} {value}"


def render_metrics() -> str:
    uptime = time.time() - _T0
    degraded = SCENARIO == "degraded"
    out: list[str] = []

    def emit(name: str, help_: str, typ: str, samples: list[str]) -> None:
        out.append(f"# HELP {name} {help_}")
        out.append(f"# TYPE {name} {typ}")
        out.extend(samples)

    # --- SLI #1 — worker liveness ----------------------------------------- #
    emit(
        "worker_heartbeat_age_seconds",
        "Seconds since each worker last heartbeated (hung-loop detector).",
        "gauge",
        [
            _line(
                "worker_heartbeat_age_seconds",
                round(_jitter(8.0 if not degraded else 45.0), 2),
                {"worker": w, "job": "fyralis-workers"},
            )
            for w in WORKERS
        ],
    )
    emit(
        "worker_uptime_seconds",
        "Per-worker uptime in seconds.",
        "gauge",
        [
            _line("worker_uptime_seconds", round(uptime, 1), {"worker": w, "job": "fyralis-workers"})
            for w in WORKERS
        ],
    )
    # G5 — expected-vs-present worker classes (anomaly/deadline not running).
    emit(
        "fyralis_worker_expected_present",
        "Count of worker classes expected to be present.",
        "gauge",
        [_line("fyralis_worker_expected_present", 8)],
    )
    emit(
        "fyralis_worker_expected_running",
        "Count of expected worker classes actually running.",
        "gauge",
        [_line("fyralis_worker_expected_running", 8 if not degraded else 6)],
    )

    # --- SLI #2 — Kafka data plane ---------------------------------------- #
    emit(
        "kafka_consumergroup_lag_sum",
        "Total consumer-group lag (messages behind).",
        "gauge",
        [
            _line("kafka_consumergroup_lag_sum", int(_jitter(120 if not degraded else 4200)),
                  {"consumergroup": "normalizer", "topic": "signals.raw"}),
        ],
    )
    emit(
        "normalizer_consumer_lag_seconds",
        "Seconds the normalizer is behind ingress.",
        "gauge",
        [_line("normalizer_consumer_lag_seconds", round(_jitter(1.5 if not degraded else 22.0), 2))],
    )
    emit(
        "breaker_trips_total",
        "Kafka->inline cutover breaker trips (cumulative).",
        "counter",
        [_line("breaker_trips_total", 0 if not degraded else int(uptime // 90))],
    )

    # --- SLI #3 + #6 — DLQ / poison / silent-loss ------------------------- #
    emit(
        "fyralis_dlq_unresolved",
        "Unresolved dead-letter queue depth.",
        "gauge",
        [_line("fyralis_dlq_unresolved", 0 if not degraded else 7)],
    )
    emit(
        "fyralis_dead_letter_rows",
        "Total dead-letter (poison) rows across tables.",
        "gauge",
        [_line("fyralis_dead_letter_rows", 0 if not degraded else 3)],
    )
    # writer.shadow_drop MUST be zero (silent-data-loss invariant, SLI #6).
    emit(
        "writer_shadow_drop_total",
        "Backfill-path shadow drops (must be zero).",
        "counter",
        [_line("writer_shadow_drop_total", 0)],
    )
    emit(
        "writer_poison_attempts_total",
        "Poison-cap burn counter (migration 0137).",
        "counter",
        [_line("writer_poison_attempts_total", 0 if not degraded else int(uptime // 300))],
    )

    # --- SLI #4 + #5 — ingestion rate & backfill progress ----------------- #
    emit(
        "writer_full_mode_writes_total",
        "Observation writes per source (cumulative).",
        "counter",
        [
            _line("writer_full_mode_writes_total",
                  int((uptime + 1) * (3.0 + SOURCES.index(s))),
                  {"source": s})
            for s in SOURCES
        ],
    )
    emit(
        "writer_full_mode_dedup_hits_total",
        "Dedup hits per source (retry-storm detector).",
        "counter",
        [_line("writer_full_mode_dedup_hits_total", int(uptime * 0.2), {"source": s}) for s in SOURCES],
    )
    emit(
        "fyralis_onboarding_shards",
        "Backfill shards by status.",
        "gauge",
        [
            _line("fyralis_onboarding_shards", 24 if not degraded else 18, {"status": "complete"}),
            _line("fyralis_onboarding_shards", 0 if not degraded else 6, {"status": "pending"}),
            _line("fyralis_onboarding_shards", 0, {"status": "failed"}),
        ],
    )
    emit(
        "reconciliation_pass_count_total",
        "Reconciler pass count (cumulative).",
        "counter",
        [_line("reconciliation_pass_count_total", int(uptime // 60))],
    )

    # --- SLI #7 + #8 — reasoning / think pipeline ------------------------- #
    emit(
        "fyralis_think_queue_pending",
        "Think queue depth (backpressure).",
        "gauge",
        [_line("fyralis_think_queue_pending", int(_jitter(12 if not degraded else 640)))],
    )
    emit(
        "think_runs_total",
        "Think runs (cumulative).",
        "counter",
        [_line("think_runs_total", int(uptime * 0.5))],
    )
    emit(
        "think_runs_failed_total",
        "Failed think runs (cumulative).",
        "counter",
        [_line("think_runs_failed_total", 0 if not degraded else int(uptime * 0.02))],
    )

    # --- SLI #9 — embedding dependency ------------------------------------ #
    emit(
        "fyralis_embedding_backlog_pending",
        "Embedding backlog depth.",
        "gauge",
        [_line("fyralis_embedding_backlog_pending", int(_jitter(5 if not degraded else 320)))],
    )
    emit(
        "embedding_attempts_total",
        "Embedding attempts (cumulative).",
        "counter",
        [_line("embedding_attempts_total", int(uptime * 0.8))],
    )
    emit(
        "embedding_failures_total",
        "Embedding failures (cumulative).",
        "counter",
        [_line("embedding_failures_total", 0 if not degraded else int(uptime * 0.05))],
    )

    # --- SLI #10 — LLM circuit-breaker + spend ---------------------------- #
    emit(
        "fyralis_llm_breaker_state",
        "Per-provider breaker state (1 = in that state).",
        "gauge",
        [
            _line("fyralis_llm_breaker_state", 1, {"provider": "deepseek", "state": "closed"}),
            _line("fyralis_llm_breaker_state", 0, {"provider": "deepseek", "state": "open"}),
            _line("fyralis_llm_breaker_state", 1 if not degraded else 0,
                  {"provider": "openai", "state": "closed"}),
            _line("fyralis_llm_breaker_state", 0 if not degraded else 1,
                  {"provider": "openai", "state": "open"}),
        ],
    )
    emit(
        "think_cost_recent_usd_1h",
        "Rolling 1h LLM spend (USD).",
        "gauge",
        [_line("think_cost_recent_usd_1h", round(_jitter(1.85), 4))],
    )

    # --- SLI #11 — database & schema integrity ---------------------------- #
    emit(
        "fyralis_db_pool_in_use",
        "DB pool connections in use.",
        "gauge",
        [
            _line("fyralis_db_pool_in_use", int(_jitter(6 if not degraded else 19)), {"pool": "default"}),
            _line("fyralis_db_pool_in_use", int(_jitter(2)), {"pool": "think"}),
        ],
    )
    emit(
        "fyralis_db_pool_max_size",
        "DB pool max size.",
        "gauge",
        [
            _line("fyralis_db_pool_max_size", 20, {"pool": "default"}),
            _line("fyralis_db_pool_max_size", 10, {"pool": "think"}),
        ],
    )
    emit(
        "fyralis_schema_version",
        "Applied schema migration version (G1).",
        "gauge",
        [_line("fyralis_schema_version", 145)],
    )
    emit(
        "fyralis_partition_coverage_months",
        "Forward partition coverage in months.",
        "gauge",
        [_line("fyralis_partition_coverage_months", 6 if not degraded else 1)],
    )

    # --- SLI #12 — auth / OAuth / ingress --------------------------------- #
    emit(
        "fyralis_oauth_token_refresh_failures_total",
        "OAuth source-token refresh failures (G2, cumulative).",
        "counter",
        [
            _line("fyralis_oauth_token_refresh_failures_total",
                  0 if not degraded else int(uptime // 120), {"source": "github"})
        ],
    )
    emit(
        "fyralis_oauth_token_expiring_soon",
        "Count of source tokens expiring soon (G2).",
        "gauge",
        [_line("fyralis_oauth_token_expiring_soon", 0 if not degraded else 1)],
    )
    emit(
        "webhook_verification_failures_total",
        "Webhook signature-verification failures (cumulative).",
        "counter",
        [_line("webhook_verification_failures_total", 0)],
    )
    emit(
        "http_requests_total",
        "Gateway HTTP requests by status (cumulative).",
        "counter",
        [
            _line("http_requests_total", int(uptime * 4), {"status": "200"}),
            _line("http_requests_total", 0 if not degraded else int(uptime * 0.1), {"status": "503"}),
        ],
    )

    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------- #
# HTTP server                                                                 #
# --------------------------------------------------------------------------- #
class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 (stdlib naming)
        if self.path.startswith("/metrics"):
            body = render_metrics().encode("utf-8")
            self._send(200, body, "text/plain; version=0.0.4; charset=utf-8")
        elif self.path.startswith("/healthz"):
            body = json.dumps({"status": "ok", "scenario": SCENARIO}).encode()
            self._send(200, body, "application/json")
        else:
            body = (
                "fyralis demo-dataplane metrics stub — golden-12 SLIs at /metrics, "
                "liveness at /healthz. This is a DEMO fixture, not the real data plane.\n"
            ).encode()
            self._send(200, body, "text/plain; charset=utf-8")

    def log_message(self, *_args):  # silence per-request noise
        return


def main() -> int:
    random.seed()  # nondeterministic wobble across restarts
    srv = ThreadingHTTPServer((HOST, PORT), _Handler)
    print(
        f"[demo-dataplane] golden-12 SLI stub on {HOST}:{PORT} "
        f"(tenant={TENANT} deployment={DEPLOYMENT} region={REGION} scenario={SCENARIO})",
        flush=True,
    )
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
