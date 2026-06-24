"""End-to-end loopback test: the REAL agent sender against a REAL in-process console.

This exercises the agent's production ``requests_sender`` over an actual TCP
socket (loopback), then kills the console mid-run and asserts the agent buffers
and retries without crashing (I3) — and that it recovers when the console returns.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from agent import Agent, requests_sender
from config import load_agent_config
from conftest import make_license
from health_probe import HealthProbe, static_probe
from license_check import LicenseChecker
from lib import DeploymentRecord


class _ConsoleState:
    def __init__(self, *, required_token: str | None = None) -> None:
        self.received: list[dict] = []
        self.auth_headers: list[str | None] = []
        self.up = True
        self.required_token = required_token  # if set, enforce Bearer auth (I4)
        self.lock = threading.Lock()


def _make_handler(state: _ConsoleState):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_a):  # silence
            pass

        def do_POST(self):
            if self.path != "/api/v1/heartbeat":
                self.send_response(404)
                self.end_headers()
                return
            if not state.up:
                # Simulate an unreachable / failing console.
                self.send_response(503)
                self.end_headers()
                self.wfile.write(b'{"error":"down"}')
                return
            auth = self.headers.get("Authorization")
            with state.lock:
                state.auth_headers.append(auth)
            # I4: when the console requires a token, reject a missing/wrong bearer.
            if state.required_token is not None:
                expected = f"Bearer {state.required_token}"
                if auth != expected:
                    self.send_response(401)
                    self.end_headers()
                    self.wfile.write(b'{"error":"unauthorized"}')
                    return
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else b"{}"
            try:
                rec = json.loads(body)
            except ValueError:
                self.send_response(400)
                self.end_headers()
                return
            with state.lock:
                state.received.append(rec)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')

    return Handler


def _agent_for(console_url: str, fabric, tmp_path: Path, *, console_token: str | None = None) -> Agent:
    version_file = tmp_path / "VERSION"
    version_file.write_text("2.0.0\n", encoding="utf-8")
    lic = make_license(fabric, tmp_path / "license.json", expires_in_days=365)
    cfg = load_agent_config(
        console_url=console_url,
        console_token=console_token,
        tenant_id="acme",
        deployment_id="acme-use1-0001",
        region="us-east-1",
        version_file=version_file,
        license_path=lic,
        trust_root_path=fabric.trust_root_path,
        config_dir=tmp_path / "applied",
        healthz_url="http://127.0.0.1:9/healthz",
        buffer_path=tmp_path / "buffer.jsonl",
        interval_s=0.05,
        heartbeat_timeout_s=1.0,
        backoff_base_s=0.01,
        backoff_max_s=0.2,
    )
    return Agent(
        cfg,
        sender=requests_sender(
            cfg.heartbeat_url, timeout_s=cfg.heartbeat_timeout_s, token=cfg.console_token
        ),
        probe=HealthProbe(static_probe(True)),
        license_checker=LicenseChecker(lic, trust_root_path=str(fabric.trust_root_path)),
    )


def test_loopback_heartbeat_buffer_and_recover(signing_fabric, tmp_path):
    state = _ConsoleState()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(state))
    host, port = server.server_address
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        console_url = f"http://127.0.0.1:{port}"
        agent = _agent_for(console_url, signing_fabric, tmp_path)

        # 1. Console up: a real outbound POST lands a valid C4 record.
        agent.tick()
        with state.lock:
            assert len(state.received) == 1
            DeploymentRecord(**state.received[0])  # strict parse == valid contract

        # 2. Console "down": agent buffers, does NOT crash.
        state.up = False
        agent.tick()
        agent.tick()
        assert agent.buffer.count() == 2
        with state.lock:
            assert len(state.received) == 1  # nothing new landed

        # 3. Console back: backlog flushes oldest-first + the live tick.
        state.up = True
        agent.tick()
        assert agent.buffer.is_empty()
        with state.lock:
            assert len(state.received) == 4  # 1 + (2 flushed) + 1 live
    finally:
        server.shutdown()
        server.server_close()


def test_console_never_started_does_not_crash(signing_fabric, tmp_path):
    # Point at a closed port: connection refused on every send. Must buffer, not crash.
    agent = _agent_for("http://127.0.0.1:1", signing_fabric, tmp_path)
    ticks = agent.run_forever(max_ticks=3)
    assert ticks == 3
    assert agent.buffer.count() == 3


# --------------------------------------------------------------------------- #
# I4: the agent carries the console write token; an authed console accepts it   #
# --------------------------------------------------------------------------- #


def test_agent_heartbeat_carries_token_and_is_accepted(signing_fabric, tmp_path):
    """The agent presents `Authorization: Bearer <token>` and a console that
    REQUIRES that token accepts the heartbeat (200, record landed)."""
    token = "secret-console-token-xyz"
    state = _ConsoleState(required_token=token)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(state))
    _host, port = server.server_address
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        agent = _agent_for(
            f"http://127.0.0.1:{port}", signing_fabric, tmp_path, console_token=token
        )
        result = agent.tick()
        assert result.delivered and not result.buffered
        with state.lock:
            assert len(state.received) == 1
            DeploymentRecord(**state.received[0])  # valid C4 record landed
            assert state.auth_headers == [f"Bearer {token}"]  # the token was sent
    finally:
        server.shutdown()
        server.server_close()


def test_agent_without_token_is_rejected_and_buffers(signing_fabric, tmp_path):
    """A console that requires a token rejects an agent that sends NONE (401); the
    agent treats it as undelivered, buffers, and never crashes (I3)."""
    token = "secret-console-token-xyz"
    state = _ConsoleState(required_token=token)
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(state))
    _host, port = server.server_address
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        # Agent built with NO console_token -> sends no Authorization header.
        agent = _agent_for(
            f"http://127.0.0.1:{port}", signing_fabric, tmp_path, console_token=None
        )
        result = agent.tick()
        assert not result.delivered and result.buffered
        assert agent.buffer.count() == 1
        with state.lock:
            assert state.received == []          # nothing was accepted
            assert state.auth_headers == [None]  # no bearer presented -> 401
    finally:
        server.shutdown()
        server.server_close()
