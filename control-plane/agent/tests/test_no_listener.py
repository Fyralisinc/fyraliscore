"""I2 enforcement: the agent NEVER opens an inbound listening socket.

Two complementary checks:

1. **Behavioral trap** — monkeypatch ``socket.socket.listen`` (and ``bind`` to a
   non-loopback wildcard) so that if the agent's code path ever tries to *listen*,
   the test fails loudly. We then drive a full ``run_forever`` cycle (collect ->
   deliver -> buffer) and assert ``listen`` was never called.

2. **Source guard** — assert no agent module imports a server framework or calls a
   listen/serve primitive (uvicorn/socketserver/HTTPServer/asyncio.start_server/
   .listen()). This catches a regression that adds a listener even if a particular
   test path doesn't hit it.
"""

from __future__ import annotations

import socket
from pathlib import Path


from agent import Agent
from config import load_agent_config
from conftest import make_license
from health_probe import HealthProbe, static_probe
from license_check import LicenseChecker

AGENT_DIR = Path(__file__).resolve().parent.parent


def _agent(fabric, tmp_path, fake_console):
    version_file = tmp_path / "VERSION"
    version_file.write_text("1.0.0\n", encoding="utf-8")
    lic = make_license(fabric, tmp_path / "license.json", expires_in_days=365)
    cfg = load_agent_config(
        console_url="https://console:8080",
        tenant_id="acme",
        deployment_id="d1",
        region="us-east-1",
        version_file=version_file,
        license_path=lic,
        trust_root_path=fabric.trust_root_path,
        config_dir=tmp_path / "applied",
        healthz_url="http://127.0.0.1:9/healthz",
        buffer_path=tmp_path / "buffer.jsonl",
        interval_s=0.01,
        backoff_base_s=0.001,
        backoff_max_s=0.05,
    )
    return Agent(
        cfg,
        sender=fake_console.sender,
        probe=HealthProbe(static_probe(True)),
        license_checker=LicenseChecker(lic, trust_root_path=str(fabric.trust_root_path)),
    )


def test_agent_never_calls_listen(signing_fabric, tmp_path, fake_console, monkeypatch):
    listen_calls: list = []
    bind_calls: list = []

    real_listen = socket.socket.listen
    real_bind = socket.socket.bind

    def trap_listen(self, *a, **k):
        listen_calls.append(self.getsockname() if self.fileno() != -1 else "?")
        return real_listen(self, *a, **k)

    def trap_bind(self, address, *a, **k):
        bind_calls.append(address)
        return real_bind(self, address, *a, **k)

    monkeypatch.setattr(socket.socket, "listen", trap_listen)
    monkeypatch.setattr(socket.socket, "bind", trap_bind)

    agent = _agent(signing_fabric, tmp_path, fake_console)
    # Drive a few full ticks while the console is up AND down (buffer path too).
    agent.run_forever(max_ticks=2)
    fake_console.up = False
    agent.run_forever(max_ticks=2)

    assert listen_calls == [], f"agent opened a LISTENING socket (I2 violation): {listen_calls}"
    # We do not forbid bind outright (clients may bind ephemeral source ports),
    # but a bind to a wildcard server address paired with listen is the smell —
    # and listen never happened, so any bind here is a client source bind.


def test_no_inbound_listener_socket_is_open(signing_fabric, tmp_path, fake_console):
    """After running the agent, the process holds no LISTEN-state TCP socket of ours.

    We snapshot listening sockets before and after; the agent must not have opened
    a new one. (Uses /proc when available; otherwise falls back to the listen trap
    which is covered by the test above.)
    """
    def _listening_ports() -> set[int]:
        ports: set[int] = set()
        try:
            # Linux: parse /proc/net/tcp for sockets in LISTEN (st == 0A).
            for fname in ("/proc/net/tcp", "/proc/net/tcp6"):
                p = Path(fname)
                if not p.is_file():
                    continue
                for line in p.read_text().splitlines()[1:]:
                    cols = line.split()
                    if len(cols) < 4:
                        continue
                    local, st = cols[1], cols[3]
                    if st == "0A":  # TCP_LISTEN
                        ports.add(int(local.split(":")[1], 16))
        except OSError:
            pass
        return ports

    before = _listening_ports()
    agent = _agent(signing_fabric, tmp_path, fake_console)
    agent.run_forever(max_ticks=3)
    after = _listening_ports()

    new_listeners = after - before
    assert not new_listeners, f"agent opened new LISTEN port(s) (I2 violation): {new_listeners}"


def test_agent_source_has_no_server_primitives():
    """Static guard: no PRODUCTION agent module imports/calls a server primitive.

    Scope is the modules that ship in the agent runtime (those copied by the
    Dockerfile). The test harness — ``selftest.py`` and ``tests/`` — deliberately
    stands up a *fake console* (an ``HTTPServer``) to exercise the agent against a
    real loopback socket, so it is excluded; it is not part of the shipped agent.
    """
    # The production daemon surface (what `Dockerfile` runs / `agent.py` imports).
    PRODUCTION_MODULES = (
        "agent.py",
        "config.py",
        "config_pull.py",
        "license_check.py",
        "buffer.py",
        "health_probe.py",
        "_bootstrap.py",
    )
    forbidden = (
        "uvicorn",
        "ThreadingHTTPServer",
        "HTTPServer",
        "socketserver",
        "start_server",      # asyncio.start_server
        "create_server",     # asyncio loop.create_server / websockets
        ".listen(",
        "FastAPI(",
        "Flask(",
    )
    for name in PRODUCTION_MODULES:
        py = AGENT_DIR / name
        assert py.is_file(), f"expected production module missing: {name}"
        text = py.read_text(encoding="utf-8")
        for needle in forbidden:
            assert needle not in text, (
                f"{py.name} references server/listen primitive {needle!r} — "
                "the agent must be OUTBOUND-ONLY (I2)"
            )
