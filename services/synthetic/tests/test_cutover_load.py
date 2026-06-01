"""Tests for services/synthetic/cutover_load.py (M-Load).

Unit-level tests against a fake target URL. The full 1-hour
cutover dry run lives in tests/load/test_cutover_dryrun.py and runs
against a real gateway in staging — see docs/ingestion/m-load-runbook.md.
"""
from __future__ import annotations

import hashlib
import hmac
from uuid import uuid4

import pytest
import respx
import httpx

from services.synthetic.cutover_load import (
    LoadConfig,
    _build_tenant_pool,
    _github_payload,
    _github_sign,
    _slack_payload,
    _slack_sign,
    _zipf_pick,
    run,
)


# pytestmark intentionally NOT pytest.mark.asyncio — most tests in
# this file are synchronous. Async tests below get an explicit
# @pytest.mark.asyncio decorator.


def test_tenant_pool_deterministic():
    a = _build_tenant_pool(10)
    b = _build_tenant_pool(10)
    assert a == b


def test_zipf_picks_skew_toward_top():
    import random
    rng = random.Random(0)
    pool = _build_tenant_pool(100)
    counts: dict = {}
    for _ in range(10000):
        t = _zipf_pick(rng, pool)
        counts[t] = counts.get(t, 0) + 1
    # Top-20 tenants should get ~80% of traffic.
    top_20 = sum(counts.get(t, 0) for t in pool[:20])
    assert top_20 > 6500, f"top-20 share too low: {top_20}/10000"


def test_slack_signature_matches_canonical():
    body = b'{"type":"event"}'
    ts = "1700000000"
    sig = _slack_sign("secret", ts, body)
    expected = hmac.new(
        b"secret", b"v0:1700000000:" + body, hashlib.sha256,
    ).hexdigest()
    assert sig == f"v0={expected}"


def test_github_signature_matches_canonical():
    body = b'{"action":"opened"}'
    sig = _github_sign("secret", body)
    expected = hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    assert sig == f"sha256={expected}"


def test_slack_payload_contains_expected_fields():
    p = _slack_payload(uuid4(), "abc123")
    assert p["type"] == "event_callback"
    assert "team_id" in p
    assert p["event"]["text"].endswith("abc123")


def test_github_payload_contains_expected_fields():
    p = _github_payload(uuid4(), "abc123")
    assert p["action"] == "opened"
    assert p["_synthetic_seed"] == "abc123"


@pytest.mark.asyncio
@respx.mock
async def test_run_short_smoke():
    """Run for 1 second at high QPS against a respx mock; assert
    sent_total > 0 and errors are bounded."""
    respx.post("http://fake/webhooks/slack/events").mock(
        return_value=httpx.Response(200, json={"ok": True}),
    )
    respx.post("http://fake/webhooks/github/events").mock(
        return_value=httpx.Response(200, json={"ok": True}),
    )
    config = LoadConfig(
        target_url="http://fake",
        slack_signing_secret="s", github_webhook_secret="g",
        qps=50, duration_s=1, tenant_count=10,
    )
    metrics = await run(config)
    assert metrics["sent_total"] > 0
    assert metrics["errors"] == {}


@pytest.mark.asyncio
@respx.mock
async def test_run_handles_error_responses():
    """5xx responses bump the errors bucket; sender continues."""
    respx.post("http://fake/webhooks/slack/events").mock(
        return_value=httpx.Response(503, text="overloaded"),
    )
    respx.post("http://fake/webhooks/github/events").mock(
        return_value=httpx.Response(503, text="overloaded"),
    )
    config = LoadConfig(
        target_url="http://fake",
        slack_signing_secret="s", github_webhook_secret="g",
        qps=20, duration_s=1, tenant_count=5,
    )
    metrics = await run(config)
    assert metrics["sent_total"] == 0
    assert sum(metrics["errors"].values()) > 0
