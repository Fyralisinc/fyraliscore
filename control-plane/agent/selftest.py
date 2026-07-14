#!/usr/bin/env python3
"""selftest — drive the agent end-to-end against a fake in-process console.

Runs the exact scenario the WS-AGENT spec calls for, with REAL code paths
(the production ``requests_sender`` over a loopback socket, the real signing
verify, the real buffer):

  1. heartbeat a valid DeploymentRecord to a live console;
  2. kill the console -> assert the agent BUFFERS + RETRIES (does NOT crash, I3),
     then bring it back -> assert the backlog flushes;
  3. expired license -> ``is_licensed()`` is False (license gate);
  4. tampered config -> rejected, not applied (I6);
  5. assert NO socket is opened for LISTENING anywhere in the agent (I2).

Exit code 0 = all assertions passed. Run::

    python selftest.py
"""

from __future__ import annotations

import datetime as _dt
import json
import socket
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import _bootstrap  # noqa: F401
import signing_lib as sl

from agent import Agent, requests_sender
from config import load_agent_config
from config_pull import ConfigPuller
from health_probe import HealthProbe, static_probe
from license_check import LicenseChecker
from lib import DeploymentRecord, Health


# --------------------------------------------------------------------------- #
# Tiny signing fabric (throwaway key + trust root, never touches the repo's)   #
# --------------------------------------------------------------------------- #


class _Fabric:
    def __init__(self, base: Path) -> None:
        self.base = base
        self.ring = sl.Keyring()
        self.ring.generate_active_key("selftest-key")
        self.trust_root = base / "trust_root.json"
        self.trust_root.write_text(json.dumps(self.ring.to_trust_root()), encoding="utf-8")

    def sign(self, path: Path, kind: str, version: str = "1") -> None:
        sb = sl.canonical_bytes_for_file(str(path), kind)
        key_id = self.ring.active_key_id
        # I6 (sig_binding v2): sign the canonical manifest BINDING, not the raw bytes,
        # matching production sign_bundle.py so verify_bundle (which requires v2) passes.
        payload = sl.signed_payload_for(
            artifact_kind=kind, version=version, key_id=key_id, signed_bytes=sb
        )
        _, raw = self.ring.sign_with_active(payload)
        (path.parent / (path.name + ".sig")).write_text(sl.b64e(raw) + "\n", encoding="utf-8")
        man = sl.build_manifest(artifact_kind=kind, version=version, signed_bytes=sb, key_id=key_id)
        (path.parent / (path.name + ".manifest.json")).write_text(json.dumps(man), encoding="utf-8")


def _rfc3339(dt: _dt.datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _make_license(fab: _Fabric, path: Path, *, expires_in_days: int) -> Path:
    now = _dt.datetime.now(_dt.timezone.utc)
    body = {
        "tenant_id": "acme",
        "deployment_id": "acme-use1-0001",
        "plan": "enterprise",
        "issued_at": _rfc3339(now - _dt.timedelta(days=1)),
        "expires_at": _rfc3339(now + _dt.timedelta(days=expires_in_days)),
        "features": ["fleet", "anomaly"],
    }
    path.write_text(json.dumps(body, indent=2), encoding="utf-8")
    fab.sign(path, "license", version=body["expires_at"])
    return path


# --------------------------------------------------------------------------- #
# Fake console over a real loopback socket                                     #
# --------------------------------------------------------------------------- #


class _ConsoleState:
    def __init__(self) -> None:
        self.received: list[dict] = []
        self.up = True
        self.lock = threading.Lock()


def _handler(state: _ConsoleState):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *_a):
            pass

        def do_POST(self):
            if self.path != "/api/v1/heartbeat":
                self.send_response(404); self.end_headers(); return
            if not state.up:
                self.send_response(503); self.end_headers(); self.wfile.write(b"down"); return
            n = int(self.headers.get("Content-Length", 0))
            rec = json.loads(self.rfile.read(n) or b"{}")
            with state.lock:
                state.received.append(rec)
            self.send_response(200); self.end_headers(); self.wfile.write(b'{"ok":true}')

    return H


# --------------------------------------------------------------------------- #
# The scenario                                                                 #
# --------------------------------------------------------------------------- #


def run() -> int:
    checks: list[tuple[str, bool]] = []

    def check(name: str, cond: bool) -> None:
        checks.append((name, bool(cond)))
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}")

    # I2 trap: any listen() anywhere fails the run.
    listen_calls: list = []
    real_listen = socket.socket.listen

    def trap_listen(self, *a, **k):
        listen_calls.append("listen")
        return real_listen(self, *a, **k)

    socket.socket.listen = trap_listen
    try:
        with tempfile.TemporaryDirectory(prefix="agent-selftest-") as td:
            base = Path(td)
            fab = _Fabric(base)

            # --- start the fake console -----------------------------------
            state = _ConsoleState()
            server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(state))
            _, port = server.server_address
            console_listen_seen = len(listen_calls)  # the CONSOLE listens; the agent must not add more
            t = threading.Thread(target=server.serve_forever, daemon=True)
            t.start()
            console_url = f"http://127.0.0.1:{port}"

            # --- build the agent with a VALID license ---------------------
            (base / "VERSION").write_text("1.4.2\n", encoding="utf-8")
            lic = _make_license(fab, base / "license.json", expires_in_days=365)
            cfg = load_agent_config(
                console_url=console_url,
                tenant_id="acme",
                deployment_id="acme-use1-0001",
                region="us-east-1",
                version_file=base / "VERSION",
                license_path=lic,
                trust_root_path=fab.trust_root,
                config_dir=base / "applied",
                healthz_url="http://127.0.0.1:9/healthz",
                buffer_path=base / "buffer.jsonl",
                interval_s=0.02,
                heartbeat_timeout_s=1.0,
                backoff_base_s=0.01,
                backoff_max_s=0.2,
            )
            agent = Agent(
                cfg,
                sender=requests_sender(cfg.heartbeat_url, timeout_s=1.0),
                probe=HealthProbe(static_probe(True)),
                license_checker=LicenseChecker(lic, trust_root_path=str(fab.trust_root)),
            )

            # === 1. heartbeats a valid DeploymentRecord ===================
            agent.tick()
            with state.lock:
                got = list(state.received)
            valid_record = False
            if got:
                try:
                    rec = DeploymentRecord(**got[0])
                    valid_record = (
                        rec.deployment_id == "acme-use1-0001"
                        and rec.version == "1.4.2"
                        and rec.health == Health.GREEN
                    )
                except Exception:
                    valid_record = False
            check("heartbeats a valid DeploymentRecord (green, v1.4.2)", valid_record)

            # === 2. kill console -> buffers + retries, does not crash =====
            state.up = False
            crashed = False
            try:
                agent.tick(); agent.tick()
            except Exception:
                crashed = True
            check("does NOT crash when console is unreachable (I3)", not crashed)
            check("BUFFERS heartbeats while console down (I3)", agent.buffer.count() == 2)

            # bring it back -> retry flushes the backlog
            state.up = True
            agent.tick()
            check("RETRIES + flushes backlog on reconnect", agent.buffer.is_empty())
            with state.lock:
                total = len(state.received)
            check("all heartbeats eventually delivered (1 + 2 buffered + 1)", total == 4)

            # === 3. expired license -> is_licensed() False ================
            exp_lic = _make_license(fab, base / "expired.json", expires_in_days=-1)
            exp_chk = LicenseChecker(exp_lic, trust_root_path=str(fab.trust_root))
            check("expired license -> is_licensed() is False", exp_chk.is_licensed() is False)

            # valid license sanity
            check(
                "valid license -> is_licensed() is True",
                LicenseChecker(lic, trust_root_path=str(fab.trust_root)).is_licensed() is True,
            )

            # === 4. tampered config -> rejected, not applied (I6) =========
            bundle = base / "bundle.json"
            bundle.write_text(json.dumps({"interval_s": 30}), encoding="utf-8")
            fab.sign(bundle, "config", version="9")
            # tamper AFTER signing
            bundle.write_text(json.dumps({"interval_s": 999999}), encoding="utf-8")
            sig = bundle.parent / (bundle.name + ".sig")
            man = bundle.parent / (bundle.name + ".manifest.json")
            puller = ConfigPuller(
                config_dir=base / "applied2",
                trust_root_path=str(fab.trust_root),
                fetcher=lambda _u: (bundle.read_bytes(), sig.read_text(), man.read_bytes()),
            )
            res = puller.pull_and_apply("https://console/config")
            check("tampered config -> REJECTED (I6)", not res.ok and not res.applied)
            check("tampered config -> NOT applied to disk", puller.load_applied_config() is None)

            # a clean config DOES verify+apply
            good = base / "good.json"
            good.write_text(json.dumps({"interval_s": 45}), encoding="utf-8")
            fab.sign(good, "config", version="10")
            gsig = good.parent / (good.name + ".sig")
            gman = good.parent / (good.name + ".manifest.json")
            gpuller = ConfigPuller(
                config_dir=base / "applied3",
                trust_root_path=str(fab.trust_root),
                fetcher=lambda _u: (good.read_bytes(), gsig.read_text(), gman.read_bytes()),
            )
            gres = gpuller.pull_and_apply("https://console/config")
            check("verified config -> applied", gres.ok and gpuller.load_applied_config() == {"interval_s": 45})

            # === 5. I2: agent opened NO listening socket ==================
            # The only listen() seen should be the fake console's own server.
            agent_listens = len(listen_calls) - console_listen_seen
            check("agent opened NO listening socket (I2)", agent_listens == 0)

            server.shutdown()
            server.server_close()
    finally:
        socket.socket.listen = real_listen

    passed = sum(1 for _, ok in checks if ok)
    total = len(checks)
    print(f"\nselftest: {passed}/{total} checks passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(run())
