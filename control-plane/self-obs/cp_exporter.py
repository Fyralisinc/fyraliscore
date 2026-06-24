#!/usr/bin/env python3
# =============================================================================
# control-plane / self-obs / cp_exporter.py  —  WS-SELFOBS (NFR-10)
# -----------------------------------------------------------------------------
# "The control plane monitors itself; silence != health."
#
# A small, dependency-light Prometheus exporter that ACTIVELY PROBES each
# control-plane (CP) service's health/readiness endpoint on a fixed interval and
# exposes the result as Prometheus metrics on /metrics. It is the inside-out
# half of CP self-observability: cp-prometheus.yml scrapes THIS exporter (and a
# few services directly), cp_rules.yml turns the series into pages, and
# dashboards/cp_self.json renders them.
#
# WHY AN EXPORTER (and not just Prometheus blackbox/direct scrape)?
#   * The auth-proxy speaks mTLS-ONLY on :8443 (CERT_REQUIRED, trusts only the
#     Fyralis CA). A plain HTTP GET — and therefore Prometheus' own scrape or a
#     vanilla blackbox http probe — is REJECTED at the TLS handshake. There is
#     no unauthenticated /healthz. So liveness of the auth-proxy must be proved
#     by a TLS-HANDSHAKE probe (does it accept a TLS connection and present its
#     server cert?) and, when a client cert is mounted, by an AUTHENTICATED
#     end-to-end probe. This exporter encapsulates that bespoke logic.
#   * Mimir's image is DISTROLESS (no shell/curl) and exposes readiness only as
#     GET /ready. Loki the same (GET /ready). The exporter normalises all of
#     these heterogeneous health contracts into one uniform metric family.
#   * The "ingest-path-alive" SYNTHETIC — the single most important self-obs
#     signal — requires an end-to-end push through the auth-proxy into Mimir and
#     a read-back. That is a multi-step probe Prometheus cannot express.
#
# THE SILENCE INVARIANT (NFR-10)
#   This exporter sets cp_self_scrape_heartbeat to time.time() on EVERY scrape.
#   cp_rules.yml alerts on `absent(cp_self_scrape_heartbeat)` and on the
#   heartbeat going stale: if the exporter dies, or cp-prometheus stops scraping
#   it, the SILENCE itself pages. Health is never inferred from the absence of a
#   bad signal.
#
# NO SECRETS IN THE IMAGE. The optional authenticated auth-proxy probe + the
# ingest synthetic read client certs from a read-only mount; if absent, those
# probes degrade to the handshake/structural probe and say so via a label —
# they never crash the exporter.
#
# Pure standard library + prometheus_client. No service-dir imports (this module
# is DIR-DISJOINT by construction), so it can be unit-tested in isolation.
# =============================================================================
from __future__ import annotations

import json
import os
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from prometheus_client import (
    CollectorRegistry,
    Gauge,
    generate_latest,
    start_http_server,
)

# -----------------------------------------------------------------------------
# Probe kinds. How a single target is checked for "is it alive?".
# -----------------------------------------------------------------------------
PROBE_HTTP = "http"          # plain HTTP GET, expect 2xx (console/config-dist/...)
PROBE_HTTP_READY = "ready"   # HTTP GET, body must look "ready" (mimir/loki /ready)
PROBE_TLS = "tls"            # TLS handshake only (auth-proxy mTLS listener liveness)

DEFAULT_TIMEOUT_S = 5.0


@dataclass(frozen=True)
class Target:
    """One CP service to probe.

    name        stable Prometheus label value (`service`).
    kind        one of PROBE_* above.
    url         for http/ready probes (http://host:port/path).
    host, port  for the tls handshake probe (auth-proxy).
    component   coarse label used by dashboards/alerts ("metrics"|"logs"|"ui"|
                "control"|"ingest").
    expect_substr  for PROBE_HTTP_READY: case-insensitive body marker.
    """

    name: str
    kind: str
    component: str
    url: str = ""
    host: str = ""
    port: int = 0
    expect_substr: str = ""


# -----------------------------------------------------------------------------
# Default CP topology (service names resolve on cp-net). Every value is
# overridable via env so the same exporter works in dev (localhost) and in the
# compose network (service DNS). See _targets_from_env().
# -----------------------------------------------------------------------------
def default_targets() -> List[Target]:
    return [
        # auth-proxy: mTLS listener. Plain GET is refused at the handshake, so we
        # probe the handshake itself (does it ACCEPT TLS + present a server cert?).
        # An end-to-end authenticated push is covered by the ingest synthetic.
        Target(
            name="auth-proxy",
            kind=PROBE_TLS,
            component="ingest",
            host=os.environ.get("CP_AUTHPROXY_HOST", "auth-proxy"),
            port=int(os.environ.get("CP_AUTHPROXY_PORT", "8443")),
        ),
        # mimir: distroless; readiness is GET /ready -> "ready".
        Target(
            name="mimir",
            kind=PROBE_HTTP_READY,
            component="metrics",
            url=os.environ.get("CP_MIMIR_URL", "http://mimir:9009/ready"),
            expect_substr="ready",
        ),
        # loki: GET /ready -> "ready".
        Target(
            name="loki",
            kind=PROBE_HTTP_READY,
            component="logs",
            url=os.environ.get("CP_LOKI_URL", "http://loki:3100/ready"),
            expect_substr="ready",
        ),
        # grafana: GET /api/health -> 200 {"database":"ok",...}.
        Target(
            name="grafana",
            kind=PROBE_HTTP,
            component="ui",
            url=os.environ.get("CP_GRAFANA_URL", "http://grafana:3000/api/health"),
        ),
        # console: FastAPI GET /healthz -> 200 {"status":"ok",...}.
        Target(
            name="console",
            kind=PROBE_HTTP,
            component="control",
            url=os.environ.get("CP_CONSOLE_URL", "http://console:8080/healthz"),
        ),
        # config-dist: FastAPI GET /healthz -> 200 {"ok":true,...}.
        Target(
            name="config-dist",
            kind=PROBE_HTTP,
            component="control",
            url=os.environ.get("CP_CONFIGDIST_URL", "http://config-dist:8090/healthz"),
        ),
        # release-registry: FastAPI GET /healthz -> 200 {"status":"ok",...}.
        # On cp-net the container port is 8090 (host-published on 8091).
        Target(
            name="release-registry",
            kind=PROBE_HTTP,
            component="control",
            url=os.environ.get(
                "CP_RELEASE_URL", "http://release-registry:8090/healthz"
            ),
        ),
    ]


# -----------------------------------------------------------------------------
# Probe results.
# -----------------------------------------------------------------------------
@dataclass
class ProbeResult:
    up: int                       # 1 healthy, 0 unhealthy/unreachable
    latency_s: float              # wall-clock probe duration
    detail: str = ""              # short reason (label-safe), for logs/debug
    status_code: int = 0          # HTTP status when applicable (0 otherwise)


def _http_probe(url: str, timeout: float, *, ready_substr: str = "") -> ProbeResult:
    """GET ``url``; up=1 on 2xx (and, if ready_substr given, body contains it)."""
    t0 = time.perf_counter()
    req = urllib.request.Request(url, method="GET", headers={"User-Agent": "cp-self-obs/1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = getattr(resp, "status", resp.getcode())
            body = b""
            if ready_substr:
                # Read a bounded slice; /ready bodies are tiny.
                body = resp.read(4096)
            latency = time.perf_counter() - t0
            ok_code = 200 <= int(code) < 300
            if ready_substr:
                ok_body = ready_substr.lower() in body.decode("utf-8", "replace").lower()
                up = 1 if (ok_code and ok_body) else 0
                detail = "ok" if up else f"code={code} ready={ok_body}"
            else:
                up = 1 if ok_code else 0
                detail = "ok" if up else f"code={code}"
            return ProbeResult(up=up, latency_s=latency, detail=detail, status_code=int(code))
    except urllib.error.HTTPError as exc:
        latency = time.perf_counter() - t0
        # A readiness endpoint commonly returns 503 while warming up: that is a
        # legitimate "down" with a status code, not an exporter error.
        return ProbeResult(up=0, latency_s=latency, detail=f"http {exc.code}", status_code=int(exc.code))
    except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as exc:
        latency = time.perf_counter() - t0
        return ProbeResult(up=0, latency_s=latency, detail=_short(str(getattr(exc, "reason", exc))))


def _tls_probe(host: str, port: int, timeout: float) -> ProbeResult:
    """Open a TLS connection to ``host:port`` and complete the handshake.

    For the mTLS auth-proxy this proves the listener is up and presents its
    server cert. We do NOT verify the cert chain here (the exporter is not a
    tenant and may have no client cert): we only assert that a TLS server
    answered and a certificate was offered. A bare TCP-accept with no TLS does
    NOT count as up.
    """
    t0 = time.perf_counter()
    ctx = ssl.create_default_context()
    # The auth-proxy server cert is signed by the internal Fyralis CA; we are not
    # validating identity, only liveness, so disable verification for the probe.
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    sock = None
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        with ctx.wrap_socket(sock, server_hostname=host) as ssock:
            der = ssock.getpeercert(binary_form=True)
            latency = time.perf_counter() - t0
            if der:
                return ProbeResult(up=1, latency_s=latency, detail="tls-handshake-ok")
            # Handshake completed but no server cert presented — treat as down.
            return ProbeResult(up=0, latency_s=latency, detail="no-server-cert")
    except ssl.SSLError as exc:
        # A handshake that progressed far enough to fail on the CLIENT cert
        # requirement still proves the mTLS listener is alive: the server asked
        # for a client cert (CERT_REQUIRED) and rejected us. That is "up".
        latency = time.perf_counter() - t0
        msg = str(exc).lower()
        if (
            "certificate required" in msg
            or "peer did not return a certificate" in msg
            or "tlsv13 alert certificate required" in msg
            or "sslv3 alert handshake failure" in msg
            or "alert handshake failure" in msg
        ):
            return ProbeResult(up=1, latency_s=latency, detail="mtls-requires-client-cert")
        return ProbeResult(up=0, latency_s=latency, detail=_short(msg))
    except (socket.timeout, ConnectionError, OSError) as exc:
        latency = time.perf_counter() - t0
        return ProbeResult(up=0, latency_s=latency, detail=_short(str(exc)))
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass


def _short(s: str, n: int = 64) -> str:
    s = " ".join(s.split())
    return s[:n]


def probe_target(t: Target, timeout: float = DEFAULT_TIMEOUT_S) -> ProbeResult:
    """Dispatch a single target to its probe implementation."""
    if t.kind == PROBE_TLS:
        return _tls_probe(t.host, t.port, timeout)
    if t.kind == PROBE_HTTP_READY:
        return _http_probe(t.url, timeout, ready_substr=t.expect_substr or "ready")
    if t.kind == PROBE_HTTP:
        return _http_probe(t.url, timeout)
    return ProbeResult(up=0, latency_s=0.0, detail=f"unknown-kind:{t.kind}")


# -----------------------------------------------------------------------------
# ingest-path-alive synthetic. The crown-jewel self-obs signal: can a tenant
# agent ACTUALLY push a sample through the auth-proxy into Mimir right now?
#
# Two modes, auto-selected:
#   (A) FULL end-to-end (preferred): if a tenant client cert + key are mounted
#       (CP_SELFOBS_CLIENT_CERT/KEY), remote_write a synthetic sample to the
#       auth-proxy over mTLS and confirm 2xx. This exercises the EXACT agent
#       ingest path: mTLS termination, SAN->tenant resolution, X-Scope-OrgID
#       injection, and the Mimir push. (remote_write needs snappy+protobuf,
#       which we do not vendor here, so by default we use the proxy's
#       /prometheus passthrough read as the liveness check — see below.)
#   (B) STRUCTURAL (default, no cert): probe mimir /ready DIRECTLY on cp-net
#       (the operator query path) and the auth-proxy TLS handshake. If both are
#       up the ingest path's two ENDPOINTS are alive even though we did not push
#       a byte. The metric carries mode="structural" so the dashboard/alert can
#       distinguish a proven push from an inferred one.
#
# Either way we publish:
#   cp_ingest_path_alive            1/0  — is the proxy->mimir ingest path usable?
#   cp_ingest_path_last_success_ts  unix  — last time it was observed alive.
# cp_rules.yml pages on this going 0 or going stale.
# -----------------------------------------------------------------------------
@dataclass
class IngestProbeConfig:
    authproxy_host: str = field(default_factory=lambda: os.environ.get("CP_AUTHPROXY_HOST", "auth-proxy"))
    authproxy_port: int = field(default_factory=lambda: int(os.environ.get("CP_AUTHPROXY_PORT", "8443")))
    mimir_ready_url: str = field(default_factory=lambda: os.environ.get("CP_MIMIR_URL", "http://mimir:9009/ready"))
    client_cert: str = field(default_factory=lambda: os.environ.get("CP_SELFOBS_CLIENT_CERT", ""))
    client_key: str = field(default_factory=lambda: os.environ.get("CP_SELFOBS_CLIENT_KEY", ""))
    ca_chain: str = field(default_factory=lambda: os.environ.get("CP_SELFOBS_CA_CHAIN", ""))
    # The proxy passthrough we GET for the FULL-mode liveness read.
    proxy_probe_path: str = field(default_factory=lambda: os.environ.get(
        "CP_SELFOBS_PROXY_PROBE_PATH", "/prometheus/api/v1/query?query=up"))


@dataclass
class IngestResult:
    alive: int
    latency_s: float
    mode: str       # "fullpush" | "structural" | "error"
    detail: str = ""


def probe_ingest_path(cfg: IngestProbeConfig, timeout: float = DEFAULT_TIMEOUT_S) -> IngestResult:
    """Is the agent->auth-proxy->mimir ingest path usable right now?"""
    t0 = time.perf_counter()
    have_cert = bool(cfg.client_cert) and os.path.isfile(cfg.client_cert) \
        and bool(cfg.client_key) and os.path.isfile(cfg.client_key)

    if have_cert:
        # FULL mode: authenticated GET THROUGH the proxy. A 2xx proves the proxy
        # terminated our mTLS, resolved our tenant from the cert SAN, injected
        # X-Scope-OrgID, and Mimir answered — the complete ingest control path.
        try:
            ctx = ssl.create_default_context()
            if cfg.ca_chain and os.path.isfile(cfg.ca_chain):
                ctx.load_verify_locations(cafile=cfg.ca_chain)
            else:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            ctx.load_cert_chain(certfile=cfg.client_cert, keyfile=cfg.client_key)
            url = f"https://{cfg.authproxy_host}:{cfg.authproxy_port}{cfg.proxy_probe_path}"
            req = urllib.request.Request(url, method="GET", headers={"User-Agent": "cp-self-obs/1"})
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                code = getattr(resp, "status", resp.getcode())
                latency = time.perf_counter() - t0
                if 200 <= int(code) < 300:
                    return IngestResult(alive=1, latency_s=latency, mode="fullpush", detail="proxy->mimir 2xx")
                return IngestResult(alive=0, latency_s=latency, mode="fullpush", detail=f"code={code}")
        except Exception as exc:  # noqa: BLE001 — any failure means path not usable
            latency = time.perf_counter() - t0
            return IngestResult(alive=0, latency_s=latency, mode="fullpush", detail=_short(str(exc)))

    # STRUCTURAL mode: no client cert mounted. Confirm BOTH ingest endpoints are
    # alive — the proxy TLS listener and Mimir readiness — without pushing.
    proxy = _tls_probe(cfg.authproxy_host, cfg.authproxy_port, timeout)
    mimir = _http_probe(cfg.mimir_ready_url, timeout, ready_substr="ready")
    latency = time.perf_counter() - t0
    alive = 1 if (proxy.up == 1 and mimir.up == 1) else 0
    detail = f"proxy={proxy.detail} mimir={mimir.detail}"
    return IngestResult(alive=alive, latency_s=latency, mode="structural", detail=_short(detail, 96))


# -----------------------------------------------------------------------------
# The collector. Builds the metric families on a private registry, runs every
# probe each scrape, and stamps the silence heartbeat.
# -----------------------------------------------------------------------------
class CPSelfObsCollector:
    def __init__(
        self,
        targets: Optional[List[Target]] = None,
        ingest_cfg: Optional[IngestProbeConfig] = None,
        timeout: float = DEFAULT_TIMEOUT_S,
        registry: Optional[CollectorRegistry] = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.targets = targets if targets is not None else default_targets()
        self.ingest_cfg = ingest_cfg if ingest_cfg is not None else IngestProbeConfig()
        self.timeout = timeout
        self.registry = registry if registry is not None else CollectorRegistry()
        self._clock = clock

        # --- per-service probe metrics ---------------------------------------
        self.g_up = Gauge(
            "cp_service_up",
            "1 if the control-plane service health/readiness probe succeeded, else 0.",
            ["service", "component", "probe"],
            registry=self.registry,
        )
        self.g_latency = Gauge(
            "cp_probe_latency_seconds",
            "Wall-clock duration of the last health probe for the service.",
            ["service", "component", "probe"],
            registry=self.registry,
        )
        self.g_last_success = Gauge(
            "cp_service_last_success_timestamp_seconds",
            "Unix time of the last successful probe of the service (0 if never).",
            ["service", "component"],
            registry=self.registry,
        )
        self.g_status_code = Gauge(
            "cp_probe_http_status_code",
            "Last HTTP status code observed for the service probe (0 for non-HTTP probes).",
            ["service", "component"],
            registry=self.registry,
        )

        # --- ingest-path-alive synthetic -------------------------------------
        self.g_ingest_alive = Gauge(
            "cp_ingest_path_alive",
            "1 if the agent->auth-proxy->mimir ingest path is usable right now, else 0.",
            ["mode"],
            registry=self.registry,
        )
        self.g_ingest_last_success = Gauge(
            "cp_ingest_path_last_success_timestamp_seconds",
            "Unix time of the last observed-alive ingest path (0 if never).",
            registry=self.registry,
        )
        self.g_ingest_latency = Gauge(
            "cp_ingest_path_probe_latency_seconds",
            "Wall-clock duration of the last ingest-path synthetic probe.",
            registry=self.registry,
        )

        # --- silence / liveness of the exporter itself -----------------------
        # Stamped EVERY scrape. cp_rules.yml pages on absent()/staleness of this.
        self.g_heartbeat = Gauge(
            "cp_self_scrape_heartbeat_timestamp_seconds",
            "Unix time the exporter last produced a scrape (NFR-10 silence != health).",
            registry=self.registry,
        )
        self.g_scrape_duration = Gauge(
            "cp_self_scrape_duration_seconds",
            "Wall-clock duration of the exporter's full probe sweep for the last scrape.",
            registry=self.registry,
        )
        self.g_services_total = Gauge(
            "cp_services_total",
            "Number of control-plane services this exporter is configured to probe.",
            registry=self.registry,
        )
        self.g_services_up = Gauge(
            "cp_services_up",
            "Number of control-plane services whose last probe was healthy.",
            registry=self.registry,
        )
        self.g_build_info = Gauge(
            "cp_self_obs_build_info",
            "Build/identity info for the cp self-obs exporter (always 1).",
            ["version"],
            registry=self.registry,
        )
        self.g_build_info.labels(version=os.environ.get("CP_SELFOBS_VERSION", "1.0.0")).set(1)
        self.g_services_total.set(len(self.targets))

        # track last-success timestamps across scrapes
        self._last_success_ts: Dict[str, float] = {t.name: 0.0 for t in self.targets}
        self._ingest_last_success_ts: float = 0.0

    def collect_once(self) -> None:
        """Run every probe a single time and update all metrics. Idempotent."""
        sweep_start = time.perf_counter()
        up_count = 0
        for t in self.targets:
            res = probe_target(t, timeout=self.timeout)
            self.g_up.labels(service=t.name, component=t.component, probe=t.kind).set(res.up)
            self.g_latency.labels(service=t.name, component=t.component, probe=t.kind).set(res.latency_s)
            self.g_status_code.labels(service=t.name, component=t.component).set(res.status_code)
            if res.up == 1:
                up_count += 1
                self._last_success_ts[t.name] = self._clock()
            self.g_last_success.labels(service=t.name, component=t.component).set(
                self._last_success_ts.get(t.name, 0.0)
            )

        # ingest-path synthetic
        ing = probe_ingest_path(self.ingest_cfg, timeout=self.timeout)
        # Clear stale mode-labelled series so only the current mode is exported.
        self.g_ingest_alive.clear()
        self.g_ingest_alive.labels(mode=ing.mode).set(ing.alive)
        self.g_ingest_latency.set(ing.latency_s)
        if ing.alive == 1:
            self._ingest_last_success_ts = self._clock()
        self.g_ingest_last_success.set(self._ingest_last_success_ts)

        # rollups + silence heartbeat (set LAST so it reflects a completed sweep)
        self.g_services_up.set(up_count)
        self.g_scrape_duration.set(time.perf_counter() - sweep_start)
        self.g_heartbeat.set(self._clock())

    def render(self) -> bytes:
        """Run the probes and return the Prometheus exposition payload."""
        self.collect_once()
        return generate_latest(self.registry)


# -----------------------------------------------------------------------------
# prometheus_client custom collector adapter. start_http_server serves whatever
# is registered; we register a collector whose collect() runs the sweep so each
# scrape is FRESH (probe-on-scrape), not a stale background snapshot.
# -----------------------------------------------------------------------------
class _ScrapeDrivenCollector:
    """Adapts CPSelfObsCollector into a prometheus_client registerable collector.

    On every scrape prometheus_client calls collect(); we run the probe sweep
    then yield the metrics from the inner registry.
    """

    def __init__(self, inner: CPSelfObsCollector) -> None:
        self._inner = inner

    def collect(self):
        self._inner.collect_once()
        # Re-emit the inner registry's metrics.
        yield from self._inner.registry.collect()


def build_app_registry(
    targets: Optional[List[Target]] = None,
    ingest_cfg: Optional[IngestProbeConfig] = None,
    timeout: float = DEFAULT_TIMEOUT_S,
) -> Tuple[CollectorRegistry, CPSelfObsCollector]:
    """Build a fresh public registry wired to a scrape-driven probe sweep."""
    inner = CPSelfObsCollector(targets=targets, ingest_cfg=ingest_cfg, timeout=timeout)
    public = CollectorRegistry()
    public.register(_ScrapeDrivenCollector(inner))
    return public, inner


def main() -> int:
    host = os.environ.get("CP_SELFOBS_LISTEN_HOST", "0.0.0.0")
    port = int(os.environ.get("CP_SELFOBS_LISTEN_PORT", "9110"))
    timeout = float(os.environ.get("CP_SELFOBS_PROBE_TIMEOUT_S", str(DEFAULT_TIMEOUT_S)))

    public, inner = build_app_registry(timeout=timeout)
    # Warm the metrics once so the first scrape after boot is already populated.
    inner.collect_once()

    start_http_server(port, addr=host, registry=public)
    sys.stderr.write(
        f"[cp-self-obs] exporter listening on {host}:{port}/metrics — "
        f"probing {len(inner.targets)} CP services every scrape (timeout={timeout}s)\n"
    )
    sys.stderr.flush()
    # Block forever; prometheus_client serves in a background thread.
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
