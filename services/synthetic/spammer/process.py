"""`SpammerProcess` — run the source-mock spammer as a real subprocess on
a TCP port, seeded from a fixture registry, with readiness polling.

This is what lets the backfill harness drive the REAL source clients
against the spammer over a real socket (not just in-process ASGITransport):
the subprocess services resolve `*_API_BASE_URL` → this server, mint
tokens, page, and back off on 429s exactly as in production.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.request


def free_port() -> int:
    """Reserve an ephemeral port (bind→read→close). Small TOCTOU race
    window, acceptable for a local test harness."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class SpammerProcess:
    """Spawns `python -m services.synthetic.spammer.server` on `port`,
    seeded from `registry_path` (the harness registry.json). Use as a
    context manager or call `start()` / `stop()` explicitly."""

    def __init__(
        self,
        *,
        port: int | None = None,
        registry_path: str | None = None,
        rate_limit_every: int = 0,
        retry_after_s: int = 1,
        startup_timeout_s: float = 15.0,
        extra_env: dict[str, str] | None = None,
    ) -> None:
        self.port = port or free_port()
        self._registry_path = registry_path
        self._rate_limit_every = rate_limit_every
        self._retry_after_s = retry_after_s
        self._startup_timeout_s = startup_timeout_s
        self._extra_env = extra_env or {}
        self._proc: subprocess.Popen | None = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> "SpammerProcess":
        env = os.environ.copy()
        env["SPAMMER_PORT"] = str(self.port)
        env["SPAMMER_429_EVERY"] = str(self._rate_limit_every)
        env["SPAMMER_RETRY_AFTER"] = str(self._retry_after_s)
        if self._registry_path:
            env["SPAMMER_FIXTURE_REGISTRY"] = self._registry_path
        env.update(self._extra_env)
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "services.synthetic.spammer.server"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self._await_ready()
        return self

    def _await_ready(self) -> None:
        deadline = time.monotonic() + self._startup_timeout_s
        url = f"{self.base_url}/healthz"
        while time.monotonic() < deadline:
            if self._proc is not None and self._proc.poll() is not None:
                err = (self._proc.stderr.read().decode(errors="replace")
                       if self._proc.stderr else "")
                raise RuntimeError(
                    f"spammer exited early (rc={self._proc.returncode}):\n"
                    f"{err[-2000:]}"
                )
            try:
                with urllib.request.urlopen(url, timeout=1) as resp:
                    if resp.status == 200:
                        return
            except Exception:  # noqa: BLE001 — not up yet
                time.sleep(0.1)
        raise RuntimeError(
            f"spammer did not become ready within {self._startup_timeout_s}s",
        )

    def stop(self) -> str:
        if self._proc is None:
            return ""
        import signal
        try:
            self._proc.send_signal(signal.SIGTERM)
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)
        except Exception:  # noqa: BLE001
            pass
        tail = (self._proc.stderr.read().decode(errors="replace")[-2000:]
                if self._proc.stderr else "")
        self._proc = None
        return tail

    def __enter__(self) -> "SpammerProcess":
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()


__all__ = ["SpammerProcess", "free_port"]
