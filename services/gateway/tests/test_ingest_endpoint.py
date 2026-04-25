"""Gateway HTTP tests for POST /ingest/{channel}.

Complements services/ingestion/tests/test_ingest_core.py by exercising
the full HTTP layer: Slack signature verification, 404 on unknown
channels, 413 on oversized payload, 400 on malformed JSON.
"""
from __future__ import annotations

import json
import time

import httpx
import pytest

from services.ingestion.core import MAX_PAYLOAD_BYTES


@pytest.mark.asyncio
async def test_ingest_slack_happy_path_http(
    client: httpx.AsyncClient, valid_session, build_slack_payload, sign_slack
):
    token, _ = valid_session
    payload = build_slack_payload(text="hi from http")
    body = json.dumps(payload).encode()
    sig_headers = sign_slack(body)
    resp = await client.post(
        "/ingest/slack:message",
        content=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            **sig_headers,
        },
    )
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["observation_id"]
    assert data["deduped"] is False


@pytest.mark.asyncio
async def test_ingest_unknown_channel_returns_404(
    client: httpx.AsyncClient, valid_session
):
    token, _ = valid_session
    resp = await client.post(
        "/ingest/mars:webhook",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"] == "handler_not_found"


@pytest.mark.asyncio
async def test_ingest_slack_missing_signature_returns_403(
    client: httpx.AsyncClient, valid_session, build_slack_payload
):
    token, _ = valid_session
    resp = await client.post(
        "/ingest/slack:message",
        json=build_slack_payload(),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403
    assert resp.json()["error"] == "slack_signature"


@pytest.mark.asyncio
async def test_ingest_slack_tampered_body_returns_403(
    client: httpx.AsyncClient, valid_session, build_slack_payload, sign_slack
):
    token, _ = valid_session
    original = build_slack_payload(text="original")
    body = json.dumps(original).encode()
    sig_headers = sign_slack(body)
    # Send a DIFFERENT body with the signature of `body`.
    tampered = json.dumps(build_slack_payload(text="tampered")).encode()
    resp = await client.post(
        "/ingest/slack:message",
        content=tampered,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            **sig_headers,
        },
    )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_ingest_oversized_payload_returns_413(
    client: httpx.AsyncClient, valid_session
):
    token, _ = valid_session
    # Just over the 1 MiB limit; the gateway checks body length before
    # dispatching to the handler.
    big = b"x" * (MAX_PAYLOAD_BYTES + 100)
    resp = await client.post(
        "/ingest/slack:message",
        content=big,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        },
    )
    assert resp.status_code == 413


@pytest.mark.asyncio
async def test_ingest_malformed_json_returns_400(
    client: httpx.AsyncClient, valid_session, sign_slack
):
    token, _ = valid_session
    bad = b"{not json"
    sig_headers = sign_slack(bad)
    resp = await client.post(
        "/ingest/slack:message",
        content=bad,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            **sig_headers,
        },
    )
    assert resp.status_code == 400


# build_slack_payload / sign_slack are plain module-level helpers.
# Import them as fixtures via an indirection.
from services.gateway.tests.conftest import (
    build_slack_payload as _build_slack_payload,
    sign_slack as _sign_slack,
)


@pytest.fixture
def build_slack_payload():
    return _build_slack_payload


@pytest.fixture
def sign_slack():
    return _sign_slack
