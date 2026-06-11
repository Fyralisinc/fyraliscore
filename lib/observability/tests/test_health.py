"""Tests for lib/observability/health.py — Heartbeat + /healthz + /metrics.

start_health_server is exercised with an explicit port=0 (OS-assigned)
so tests never collide on a fixed port; the bound port is read back from
server.server_address[1]. Every started server is shut down in finally.
"""
from __future__ import annotations

import http.client
import json
import time

import pytest

from lib.observability.health import Heartbeat, start_health_server


def _get(port: int, path: str) -> tuple[int, str]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return resp.status, resp.read().decode()
    finally:
        conn.close()


class TestHeartbeat:
    def test_age_grows_and_touch_resets(self) -> None:
        hb = Heartbeat()
        assert hb.age() < 1.0
        time.sleep(0.05)
        assert hb.age() >= 0.05
        hb.touch()
        assert hb.age() < 0.05


class TestStartHealthServer:
    def test_serves_metrics_and_healthz(self) -> None:
        hb = Heartbeat()
        server = start_health_server(
            worker_name="testworker",
            render_metrics=lambda: "caller_metric 1\n",
            heartbeat=hb,
            port=0,
        )
        assert server is not None
        try:
            port = server.server_address[1]

            status, body = _get(port, "/metrics")
            assert status == 200
            assert "caller_metric 1" in body
            assert 'worker_heartbeat_age_seconds{worker="testworker"}' in body
            assert 'worker_uptime_seconds{worker="testworker"}' in body

            status, body = _get(port, "/healthz")
            assert status == 200
            payload = json.loads(body)
            assert payload["status"] == "ok"
            assert payload["worker"] == "testworker"
        finally:
            server.shutdown()
            server.server_close()

    def test_healthz_503_when_heartbeat_stale(self) -> None:
        hb = Heartbeat()
        server = start_health_server(
            worker_name="staleworker",
            render_metrics=lambda: "",
            heartbeat=hb,
            port=0,
            stale_sec=0.01,
        )
        assert server is not None
        try:
            port = server.server_address[1]
            time.sleep(0.05)  # let the untouched heartbeat go stale
            status, body = _get(port, "/healthz")
            assert status == 503
            payload = json.loads(body)
            assert payload["status"] == "stale"
            assert payload["heartbeat_age_s"] > 0.01
        finally:
            server.shutdown()
            server.server_close()

    def test_returns_none_when_env_unset_and_no_port(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("INGESTION_HEALTH_PORT", raising=False)
        server = start_health_server(
            worker_name="disabledworker",
            render_metrics=lambda: "",
            heartbeat=Heartbeat(),
            port=None,
        )
        assert server is None
