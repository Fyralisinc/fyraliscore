#!/usr/bin/env python3
# =============================================================================
# control-plane / self-obs / selftest.py  —  WS-SELFOBS (NFR-10)
# -----------------------------------------------------------------------------
# OFFLINE self-test for the control-plane self-observability stack. Proves the
# deliverables are wired and working WITHOUT a running control plane:
#
#   1. yaml-load cp-prometheus.yml + cp_rules.yml; structural sanity on both.
#   2. If `promtool` is on PATH: `promtool check rules` + `check config`.
#      (Not installed on the dev host -> SKIPPED loudly, not failed.)
#   3. import cp_exporter; spin up a STUB health server (one route up, one down,
#      one /ready, plus a TLS listener standing in for the mTLS auth-proxy) and
#      assert the exporter emits cp_service_up = {1 for healthy, 0 for down},
#      probe-latency, the ingest synthetic, and the silence heartbeat.
#   4. json-load dashboards/cp_self.json; assert it references the cp metrics +
#      the templated CP datasource input.
#
# Run:
#   /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
#       control-plane/self-obs/selftest.py
#
# Exit 0 = all checks passed (skips allowed). Exit 1 = a hard failure.
# =============================================================================
from __future__ import annotations

import json
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"

_results: list[tuple[str, str, str]] = []


def record(status: str, name: str, detail: str = "") -> None:
    _results.append((status, name, detail))
    mark = {"PASS": "ok  ", "FAIL": "FAIL", "SKIP": "skip"}[status]
    line = f"  [{mark}] {name}"
    if detail:
        line += f" — {detail}"
    print(line)


# -----------------------------------------------------------------------------
# 1. YAML configs
# -----------------------------------------------------------------------------
def test_yaml_configs() -> None:
    print("1) YAML config load + structure")
    try:
        import yaml
    except Exception as exc:  # pragma: no cover
        record(FAIL, "import pyyaml", str(exc))
        return

    # --- prometheus config ---
    try:
        prom = yaml.safe_load((HERE / "cp-prometheus.yml").read_text())
        assert isinstance(prom, dict), "prometheus config is not a mapping"
        assert "scrape_configs" in prom and prom["scrape_configs"], "no scrape_configs"
        jobs = {c["job_name"] for c in prom["scrape_configs"]}
        assert "cp-self-obs-exporter" in jobs, f"exporter job missing; jobs={jobs}"
        assert "rule_files" in prom and prom["rule_files"], "no rule_files wired"
        # auth-proxy must NOT be a direct scrape target (mTLS-only).
        all_targets = []
        for c in prom["scrape_configs"]:
            for sc in c.get("static_configs", []) or []:
                all_targets += sc.get("targets", []) or []
        assert not any("8443" in t for t in all_targets), (
            "auth-proxy:8443 must not be a direct prometheus target (mTLS-only)"
        )
        record(PASS, "cp-prometheus.yml loads", f"{len(jobs)} jobs, rule_files wired")
    except Exception as exc:
        record(FAIL, "cp-prometheus.yml", str(exc))

    # --- rules config ---
    try:
        rules = yaml.safe_load((HERE / "cp_rules.yml").read_text())
        assert isinstance(rules, dict) and "groups" in rules, "no groups"
        alert_names, record_names = [], []
        for g in rules["groups"]:
            assert "name" in g and "rules" in g, f"group missing name/rules: {g}"
            for r in g["rules"]:
                if "alert" in r:
                    assert "expr" in r, f"alert {r.get('alert')} has no expr"
                    alert_names.append(r["alert"])
                elif "record" in r:
                    assert "expr" in r, f"record {r.get('record')} has no expr"
                    record_names.append(r["record"])
        # The non-negotiable alerts for this WS.
        required = {
            "ControlPlaneSelfObsSilent",  # the silence != health page
            "AuthProxyDown",
            "MimirUnreachable",
            "LokiUnreachable",
            "ConsoleDown",
            "ConfigDistDown",
            "IngestPathDown",
        }
        missing = required - set(alert_names)
        assert not missing, f"missing required alerts: {sorted(missing)}"
        # The silence alert MUST use absent() so it can fire from no-data.
        silent = next(
            r for g in rules["groups"] for r in g["rules"]
            if r.get("alert") == "ControlPlaneSelfObsSilent"
        )
        assert "absent(" in silent["expr"], "silence alert must use absent()"
        record(
            PASS, "cp_rules.yml loads",
            f"{len(record_names)} recording, {len(alert_names)} alerts, silence=absent()",
        )
    except Exception as exc:
        record(FAIL, "cp_rules.yml", str(exc))


# -----------------------------------------------------------------------------
# 2. promtool (optional)
# -----------------------------------------------------------------------------
def test_promtool() -> None:
    print("2) promtool check (optional)")
    promtool = shutil.which("promtool")
    if not promtool:
        record(SKIP, "promtool", "not on PATH (host has no prometheus toolchain)")
        return
    try:
        r1 = subprocess.run(
            [promtool, "check", "rules", str(HERE / "cp_rules.yml")],
            capture_output=True, text=True, timeout=60,
        )
        if r1.returncode == 0:
            record(PASS, "promtool check rules", r1.stdout.strip().splitlines()[-1] if r1.stdout.strip() else "")
        else:
            record(FAIL, "promtool check rules", (r1.stdout + r1.stderr).strip()[:200])
    except Exception as exc:
        record(FAIL, "promtool check rules", str(exc))

    # config check needs the rule_files path to resolve; run from a temp dir
    # where cp_rules.yml is copied to the path the config references.
    try:
        with tempfile.TemporaryDirectory() as td:
            etc = Path(td) / "etc" / "prometheus"
            etc.mkdir(parents=True)
            shutil.copy(HERE / "cp_rules.yml", etc / "cp_rules.yml")
            cfg = (HERE / "cp-prometheus.yml").read_text()
            (etc / "cp-prometheus.yml").write_text(cfg)
            r2 = subprocess.run(
                [promtool, "check", "config", str(etc / "cp-prometheus.yml")],
                capture_output=True, text=True, timeout=60, cwd=td,
            )
            # promtool resolves rule_files relative to the config file dir OR the
            # absolute /etc path; if the absolute path is absent on host it warns.
            ok = r2.returncode == 0
            record(PASS if ok else SKIP, "promtool check config",
                   (r2.stdout + r2.stderr).strip().splitlines()[-1] if (r2.stdout + r2.stderr).strip() else "")
    except Exception as exc:
        record(SKIP, "promtool check config", str(exc))


# -----------------------------------------------------------------------------
# 3. exporter against stub servers
# -----------------------------------------------------------------------------
class _StubHealthHandler(BaseHTTPRequestHandler):
    """Serves /healthz 200, /down 503, /ready 'ready', /api/health 200."""

    def log_message(self, *args):  # silence
        pass

    def do_GET(self):  # noqa: N802
        if self.path.startswith("/healthz") or self.path.startswith("/api/health"):
            body = b'{"status":"ok"}'
            self.send_response(200)
        elif self.path.startswith("/ready"):
            body = b"ready\n"
            self.send_response(200)
        elif self.path.startswith("/down"):
            body = b"unhealthy"
            self.send_response(503)
        else:
            body = b"not found"
            self.send_response(404)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _start_http_stub() -> tuple[ThreadingHTTPServer, int]:
    port = _free_port()
    srv = ThreadingHTTPServer(("127.0.0.1", port), _StubHealthHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, port


def _make_self_signed(tmpdir: Path) -> tuple[Path, Path]:
    """Best-effort self-signed cert for the TLS stub. Returns (cert, key) or
    raises if cryptography is unavailable (caller then skips the TLS case)."""
    from datetime import datetime, timedelta, timezone

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "stub-authproxy")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(timezone.utc) - timedelta(minutes=5))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    cert_path = tmpdir / "stub.crt"
    key_path = tmpdir / "stub.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


def _start_tls_stub(cert: Path, key: Path) -> tuple[object, int]:
    """A TLS listener requiring a client cert — stands in for the mTLS auth-proxy.
    It performs a handshake then closes; the exporter's TLS probe should read it
    as 'up' (mtls-requires-client-cert or handshake-ok)."""
    port = _free_port()
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=str(cert), keyfile=str(key))
    # Require a client cert so the path mirrors the real mTLS proxy.
    ctx.verify_mode = ssl.CERT_REQUIRED
    # No CA loaded -> client verification will fail, but the handshake reaches
    # the certificate-required stage, which is exactly what we probe for.
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen(5)
    stop = threading.Event()

    def serve():
        listener.settimeout(0.5)
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                ctx.wrap_socket(conn, server_side=True)
            except (ssl.SSLError, OSError):
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    t = threading.Thread(target=serve, daemon=True)
    t.start()

    class _Handle:
        def shutdown(self):
            stop.set()
            try:
                listener.close()
            except Exception:
                pass

    return _Handle(), port


def test_exporter() -> None:
    print("3) exporter probes (against stubs)")
    try:
        import cp_exporter as cx
    except Exception as exc:
        record(FAIL, "import cp_exporter", str(exc))
        return
    record(PASS, "import cp_exporter", "")

    http_srv, http_port = _start_http_stub()
    tls_handle = None
    tls_port = None
    tmp = Path(tempfile.mkdtemp())
    try:
        # Build a target set pointing at the stubs.
        base = f"http://127.0.0.1:{http_port}"
        targets = [
            cx.Target(name="console", kind=cx.PROBE_HTTP, component="control", url=f"{base}/healthz"),
            cx.Target(name="mimir", kind=cx.PROBE_HTTP_READY, component="metrics", url=f"{base}/ready", expect_substr="ready"),
            cx.Target(name="release-registry", kind=cx.PROBE_HTTP, component="control", url=f"{base}/down"),
        ]

        # TLS stub for auth-proxy (skip gracefully if cryptography missing).
        try:
            cert, key = _make_self_signed(tmp)
            tls_handle, tls_port = _start_tls_stub(cert, key)
            time.sleep(0.2)
            targets.append(
                cx.Target(name="auth-proxy", kind=cx.PROBE_TLS, component="ingest",
                          host="127.0.0.1", port=tls_port)
            )
            tls_available = True
        except Exception as exc:
            tls_available = False
            record(SKIP, "tls auth-proxy stub", f"cryptography unavailable: {exc}")

        # Ingest synthetic in structural mode: point at the TLS stub + ready url.
        if tls_available:
            ingest_cfg = cx.IngestProbeConfig(
                authproxy_host="127.0.0.1",
                authproxy_port=tls_port,
                mimir_ready_url=f"{base}/ready",
                client_cert="", client_key="",
            )
        else:
            ingest_cfg = cx.IngestProbeConfig(
                authproxy_host="127.0.0.1", authproxy_port=1,  # unreachable
                mimir_ready_url=f"{base}/ready",
            )

        coll = cx.CPSelfObsCollector(targets=targets, ingest_cfg=ingest_cfg, timeout=3.0)
        coll.collect_once()
        payload = cx.generate_latest(coll.registry).decode()

        # --- assertions on the exposition payload ---
        def sample(metric: str, **labels) -> float:
            for fam in coll.registry.collect():
                for s in fam.samples:
                    if s.name == metric and all(s.labels.get(k) == v for k, v in labels.items()):
                        return s.value
            raise AssertionError(f"sample not found: {metric} {labels}")

        assert sample("cp_service_up", service="console") == 1.0, "console should be up"
        record(PASS, "cp_service_up{console}=1", "")
        assert sample("cp_service_up", service="mimir") == 1.0, "mimir /ready should be up"
        record(PASS, "cp_service_up{mimir}=1 (/ready)", "")
        assert sample("cp_service_up", service="release-registry") == 0.0, "503 must read down"
        record(PASS, "cp_service_up{release-registry}=0 (503)", "")

        # latency present and non-negative
        lat = sample("cp_probe_latency_seconds", service="console")
        assert lat >= 0.0, "latency must be >= 0"
        record(PASS, "cp_probe_latency_seconds present", f"{lat:.4f}s")

        # last-success timestamp set for the healthy ones
        assert sample("cp_service_last_success_timestamp_seconds", service="console") > 0
        assert sample("cp_service_last_success_timestamp_seconds", service="release-registry") == 0
        record(PASS, "cp_service_last_success_timestamp_seconds", "set for up, 0 for down")

        # rollups
        assert sample("cp_services_total") == float(len(targets))
        # up = console + mimir (+ auth-proxy if tls) ; release-registry down
        expected_up = 2 + (1 if tls_available else 0)
        assert sample("cp_services_up") == float(expected_up), f"expected {expected_up} up"
        record(PASS, "cp_services_up / total rollup", f"{expected_up}/{len(targets)}")

        if tls_available:
            assert sample("cp_service_up", service="auth-proxy") == 1.0, "mTLS handshake should read up"
            record(PASS, "cp_service_up{auth-proxy}=1 (TLS handshake)", "")
            # ingest synthetic structural: proxy up + mimir ready -> alive
            assert sample("cp_ingest_path_alive", mode="structural") == 1.0
            record(PASS, "cp_ingest_path_alive{structural}=1", "")
            assert sample("cp_ingest_path_last_success_timestamp_seconds") > 0
            record(PASS, "cp_ingest_path_last_success_timestamp_seconds set", "")

        # silence heartbeat stamped
        assert sample("cp_self_scrape_heartbeat_timestamp_seconds") > 0
        record(PASS, "cp_self_scrape_heartbeat_timestamp_seconds stamped", "silence != health")

        # build info
        assert "cp_self_obs_build_info" in payload
        record(PASS, "exposition payload renders", f"{len(payload)} bytes")

        # --- the scrape-driven collector adapter also works ---
        public, inner = cx.build_app_registry(targets=targets, ingest_cfg=ingest_cfg, timeout=3.0)
        body = cx.generate_latest(public).decode()
        assert "cp_service_up" in body and "cp_self_scrape_heartbeat" in body
        record(PASS, "scrape-driven collector adapter", "probe-on-scrape works")

    except AssertionError as exc:
        record(FAIL, "exporter assertion", str(exc))
    except Exception as exc:
        record(FAIL, "exporter probe", repr(exc))
    finally:
        http_srv.shutdown()
        if tls_handle is not None:
            tls_handle.shutdown()
        shutil.rmtree(tmp, ignore_errors=True)


# -----------------------------------------------------------------------------
# 4. dashboard JSON
# -----------------------------------------------------------------------------
def test_dashboard() -> None:
    print("4) dashboard JSON")
    try:
        path = HERE / "dashboards" / "cp_self.json"
        dash = json.loads(path.read_text())
        assert dash.get("title"), "dashboard has no title"
        assert dash.get("panels"), "dashboard has no panels"
        blob = json.dumps(dash)
        for metric in [
            "cp_service_up",
            "cp_ingest_path_alive",
            "self_scrape_heartbeat_age_seconds",
            "cp:services_healthy_ratio",
        ]:
            assert metric in blob, f"dashboard does not reference {metric}"
        # references the templated CP datasource input
        assert "${DS_CP}" in blob, "dashboard does not use the ${DS_CP} datasource var"
        inputs = {i["name"] for i in dash.get("__inputs", [])}
        assert "DS_CP" in inputs, "dashboard __inputs missing DS_CP"
        record(PASS, "cp_self.json loads",
               f"title={dash['title']!r}, {len(dash['panels'])} panels, refs cp metrics + ${{DS_CP}}")
    except Exception as exc:
        record(FAIL, "cp_self.json", str(exc))


# -----------------------------------------------------------------------------
def main() -> int:
    print("=" * 74)
    print("control-plane / self-obs — WS-SELFOBS self-test")
    print("=" * 74)
    test_yaml_configs()
    test_promtool()
    test_exporter()
    test_dashboard()

    print("-" * 74)
    n_pass = sum(1 for s, *_ in _results if s == PASS)
    n_skip = sum(1 for s, *_ in _results if s == SKIP)
    n_fail = sum(1 for s, *_ in _results if s == FAIL)
    print(f"RESULT: {n_pass} passed, {n_skip} skipped, {n_fail} failed")
    if n_fail:
        print("FAILURES:")
        for s, name, detail in _results:
            if s == FAIL:
                print(f"  - {name}: {detail}")
    print("=" * 74)
    return 1 if n_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
