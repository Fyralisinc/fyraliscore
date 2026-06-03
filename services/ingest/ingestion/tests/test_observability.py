"""Unit tests for the consumer health/metrics server."""
from __future__ import annotations

import asyncio
import time
import urllib.request

import pytest

from services.ingest.ingestion.observability import (
    Heartbeat,
    render_prometheus,
    run_heartbeat_ticker,
    start_health_server,
)


def _get(url: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:  # 503 etc.
        return e.code, e.read().decode()


def test_render_prometheus_shapes_names() -> None:
    hb = Heartbeat()
    text = render_prometheus({"normalizer.messages_consumed": 7.0}, hb)
    assert "ingestion_normalizer_messages_consumed 7.0" in text
    assert "ingestion_heartbeat_age_seconds" in text


def test_health_server_healthz_and_metrics() -> None:
    hb = Heartbeat()
    metrics = {"writer.full_mode_writes": 3.0}
    server = start_health_server(
        get_metrics=lambda: metrics, heartbeat=hb, port=0, stale_sec=60,
    )
    # port=0 → OS-assigned; read the bound port.
    assert server is not None
    port = server.server_address[1]
    try:
        code, body = _get(f"http://127.0.0.1:{port}/healthz")
        assert code == 200 and "ok" in body
        code, body = _get(f"http://127.0.0.1:{port}/metrics")
        assert code == 200
        assert "ingestion_writer_full_mode_writes 3.0" in body
    finally:
        server.shutdown()


def test_health_server_goes_stale() -> None:
    hb = Heartbeat()
    # Force the heartbeat to look old.
    hb._last = time.monotonic() - 1000
    server = start_health_server(
        get_metrics=dict, heartbeat=hb, port=0, stale_sec=1,
    )
    assert server is not None
    port = server.server_address[1]
    try:
        code, body = _get(f"http://127.0.0.1:{port}/healthz")
        assert code == 503 and "stale" in body
    finally:
        server.shutdown()


def test_disabled_when_port_zero(monkeypatch) -> None:
    monkeypatch.delenv("INGESTION_HEALTH_PORT", raising=False)
    assert start_health_server(get_metrics=dict, heartbeat=Heartbeat()) is None


@pytest.mark.asyncio
async def test_ticker_touches_until_stopped() -> None:
    hb = Heartbeat()
    hb._last = time.monotonic() - 100  # stale to start
    stop = asyncio.Event()
    task = asyncio.ensure_future(
        run_heartbeat_ticker(hb, stop, interval=0.01)
    )
    await asyncio.sleep(0.05)
    assert hb.age() < 1.0  # ticker refreshed it
    stop.set()
    await task
