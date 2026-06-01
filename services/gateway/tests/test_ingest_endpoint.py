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
    body = resp.json()
    # IN-01: structured error includes the decoder reason in `detail`.
    assert body["error"] == "invalid_json"
    assert isinstance(body.get("detail"), str) and body["detail"]


# ---------------------------------------------------------------------
# IN-01 — body-size precheck via the FastAPI dependency
# ---------------------------------------------------------------------


async def _post_via_asgi(
    app, *, path: str, headers: list[tuple[bytes, bytes]], body: bytes = b""
) -> tuple[int, dict]:
    """Drive the ASGI app directly so we can craft headers (e.g. an
    oversize Content-Length or `Transfer-Encoding: chunked`) without
    httpx normalising them. Returns (status_code, dict_body).
    """
    import json as _json

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": ("test", 0),
        "server": ("testserver", 80),
    }
    body_sent = {"done": False}

    async def receive():
        if body_sent["done"]:
            return {"type": "http.disconnect"}
        body_sent["done"] = True
        return {"type": "http.request", "body": body, "more_body": False}

    captured: dict = {"status": None, "chunks": bytearray()}

    async def send(message):
        if message["type"] == "http.response.start":
            captured["status"] = message["status"]
        elif message["type"] == "http.response.body":
            captured["chunks"].extend(message.get("body", b"") or b"")

    await app(scope, receive, send)
    try:
        parsed = _json.loads(bytes(captured["chunks"]) or b"null")
    except Exception:
        parsed = {}
    return captured["status"], parsed


@pytest.mark.asyncio
async def test_ingest_rejects_oversize_content_length(
    client: httpx.AsyncClient, valid_session
):
    """A1/A2: header advertises >MAX even with an empty body — 413,
    no body read."""
    token, _ = valid_session
    app = client._transport.app  # type: ignore[attr-defined]
    headers = [
        (b"authorization", f"Bearer {token}".encode()),
        (b"content-type", b"application/json"),
        (b"content-length", str(MAX_PAYLOAD_BYTES + 1).encode()),
    ]
    status_code, body = await _post_via_asgi(
        app, path="/ingest/slack:message", headers=headers, body=b""
    )
    assert status_code == 413
    assert body["error"] == "payload_too_large"
    assert body["max_bytes"] == MAX_PAYLOAD_BYTES


@pytest.mark.asyncio
async def test_ingest_rejects_chunked_transfer_encoding(
    client: httpx.AsyncClient, valid_session
):
    """A3: Transfer-Encoding: chunked is unsupported on ingest."""
    token, _ = valid_session
    app = client._transport.app  # type: ignore[attr-defined]
    headers = [
        (b"authorization", f"Bearer {token}".encode()),
        (b"content-type", b"application/json"),
        (b"transfer-encoding", b"chunked"),
    ]
    status_code, body = await _post_via_asgi(
        app, path="/ingest/slack:message", headers=headers, body=b"{}"
    )
    assert status_code == 413
    assert body["error"] == "payload_too_large"
    assert body["reason"] == "chunked_unsupported"


@pytest.mark.asyncio
async def test_ingest_streamed_body_exceeds_limit(
    client: httpx.AsyncClient, valid_session
):
    """A4: no Content-Length header; streaming counter trips at the
    same MAX_PAYLOAD_BYTES limit."""
    token, _ = valid_session

    async def _gen():
        # Yield > MAX in two chunks; the dependency aborts mid-stream.
        yield b"x" * MAX_PAYLOAD_BYTES
        yield b"y" * 256

    resp = await client.post(
        "/ingest/slack:message",
        content=_gen(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        },
    )
    assert resp.status_code == 413
    assert resp.json()["error"] == "payload_too_large"


@pytest.mark.asyncio
async def test_ingest_oversize_before_auth_still_401(
    client: httpx.AsyncClient,
):
    """A8: middleware order — auth fires before the dependency, so an
    unauthenticated oversize request returns 401, not 413."""
    app = client._transport.app  # type: ignore[attr-defined]
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(MAX_PAYLOAD_BYTES + 1).encode()),
    ]
    status_code, body = await _post_via_asgi(
        app, path="/ingest/slack:message", headers=headers, body=b""
    )
    assert status_code == 401
    assert body["error"] == "unauthorized"
    assert body["reason"] == "missing_bearer"


@pytest.mark.asyncio
async def test_ingest_slack_signature_validates_after_bounded_read(
    client: httpx.AsyncClient,
    valid_session,
    build_slack_payload,
    sign_slack,
):
    """A5: the dependency must hand the route the exact bytes Slack
    signed — otherwise HMAC fails on a 500 KB payload."""
    token, _ = valid_session
    big_text = "x" * (500 * 1024)  # 500 KB body
    payload = build_slack_payload(text=big_text)
    body = json.dumps(payload).encode()
    assert len(body) < MAX_PAYLOAD_BYTES  # sanity
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
    assert resp.status_code in (200, 201), resp.text


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
