"""The gateway's GET /metrics Prometheus scrape endpoint (FR-011).

Exercises the real route registered by `_register_routes`, delegating to
`services.app.webhooks.metrics.render_prometheus`. This is the scrape path
ops dashboards previously lacked.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from services.app.gateway.main import _PUBLIC_PATHS, _register_routes
from services.app.webhooks import metrics


@pytest.fixture
def client():
    app = FastAPI()
    _register_routes(app)
    metrics.reset()
    try:
        yield TestClient(app)
    finally:
        metrics.reset()


def test_metrics_endpoint_serves_prometheus_text(client) -> None:
    metrics.record_failure("github", "signature_mismatch")

    resp = client.get("/metrics")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain; version=0.0.4")
    assert (
        'webhook_verification_failures_total'
        '{provider="github",reason="signature_mismatch"} 1'
    ) in resp.text


def test_metrics_endpoint_is_public() -> None:
    # The Bearer middleware must skip /metrics — scrapers carry no token.
    assert "/metrics" in _PUBLIC_PATHS


def test_metrics_endpoint_ok_when_no_failures(client) -> None:
    resp = client.get("/metrics")
    assert resp.status_code == 200
    # Family header is advertised even with zero samples.
    assert "# TYPE webhook_verification_failures_total counter" in resp.text
