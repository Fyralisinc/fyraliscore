"""Contract test: HiBob webhook path parses a REAL Bob Webhooks V2 payload.

Confirms Phase-2 finding: #20 ("HiBob uses synthetic body fields absent from
real payloads") is a FALSE POSITIVE for HiBob. Official docs show every Bob V2
delivery carries a top-level `companyId` (a JSON NUMBER) as the tenant key, and
is signed HMAC-SHA512/base64 in `Bob-Signature` — exactly what the code already
does. This test locks the real contract in so it can't silently drift.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json

import httpx
import pytest

from services.app.webhooks.signatures.hibob import verifier as hibob_verifier
from services.app.webhooks.tenant_resolver import _extract_hibob
from services.app.webhooks.verifier import Secret, WebhookVerificationError
from services.ingest.integrations.hibob.client import HibobClient
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_TOKEN = "test-hibob-webhook-secret"


def _fixture():
    return load_fixture("hibob", "webhook", "event")


def _raw(body: dict) -> bytes:
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def _sign(raw: bytes) -> str:
    return base64.b64encode(
        hmac.new(_TOKEN.encode("utf-8"), raw, hashlib.sha512).digest()
    ).decode("ascii")


def test_hibob_tenant_resolution_reads_numeric_companyid():
    """companyId is a JSON number in real deliveries; the resolver must
    stringify it to the install key (finding #20 was a false positive)."""
    body = _fixture().body
    assert isinstance(body["companyId"], int)  # real payloads send a number
    assert _extract_hibob(body, {}) == str(body["companyId"])


async def test_hibob_signature_verifies_sha512_base64():
    body = _fixture().body
    raw = _raw(body)
    ctx = await hibob_verifier.verify(
        body=raw,
        headers={"Bob-Signature": _sign(raw)},
        secrets=[Secret("hibob", _TOKEN)],
    )
    assert ctx.provider == "hibob"


async def test_hibob_tampered_signature_rejected():
    body = _fixture().body
    raw = _raw(body)
    with pytest.raises(WebhookVerificationError) as exc:
        await hibob_verifier.verify(
            body=raw,
            headers={"Bob-Signature": _sign(raw)},
            secrets=[Secret("hibob", "wrong-secret")],
        )
    assert exc.value.reason == "signature_mismatch"


@pytest.mark.asyncio
async def test_hibob_people_search_uses_post_and_basic_auth():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={
            "employees": [{"/root/id": 1, "/root/displayName": "Ada"}],
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = HibobClient(
            base_url="https://api.hibob.com",
            company_id="co-1",
            service_user_id="svc",
            token="secret",
            http_client=http,
        )
        rows, next_cursor = await client.list_entities("employee")

    assert seen[0].method == "POST"
    assert seen[0].url.path == "/v1/people/search"
    assert seen[0].headers["Authorization"].startswith("Basic ")
    assert rows[0]["id"] == 1
    assert rows[0]["displayName"] == "Ada"
    assert next_cursor is None


@pytest.mark.asyncio
async def test_hibob_bulk_salary_cursor_shape():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={
            "results": [{"payrollId": "p1", "modified": "2026-05-01T00:00:00Z"}],
            "response_metadata": {"next_cursor": "next-page"},
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = HibobClient(
            base_url="https://api.hibob.com",
            company_id="co-1",
            service_user_id="svc",
            token="secret",
            http_client=http,
        )
        rows, next_cursor = await client.list_entities("payroll", page_cursor="abc")

    assert seen[0].method == "GET"
    assert seen[0].url.path == "/v1/bulk/people/salaries"
    assert dict(httpx.QueryParams(seen[0].url.query))["cursor"] == "abc"
    assert rows[0]["payrollId"] == "p1"
    assert next_cursor == "next-page"
