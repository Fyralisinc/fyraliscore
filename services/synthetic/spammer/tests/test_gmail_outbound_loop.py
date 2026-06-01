"""End-to-end proof of the outbound-API shift for gmail.

A REAL GmailClient (real DwdTokenMinter → real token exchange → real authed
HTTP request/response) drives messages_list / get_message / get_profile
against the local spammer through the real httpx + FastAPI stack — no
respx, no monkeypatched mock client. Pointing the client at the spammer is
pure config (GMAIL_API_BASE_URL + the SA JSON's token_uri). Also proves the
spammer's 429 maps to GoogleRateLimited and is absorbed by
retry_with_backoff_on_429.

The transport here is httpx.ASGITransport (hermetic + deterministic for CI);
the spammer's `main()` runs the SAME app on a real TCP port for actual load
runs (`python -m services.synthetic.spammer.server`). What's being proven is
the outbound wiring — real client → real HTTP semantics (routing, status,
429/Retry-After) → spammer → retry — which ASGITransport exercises fully.

This is the "we can plug the real API endpoints" verification.
"""
from __future__ import annotations

import json
import tempfile

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from services.synthetic.spammer.server import build_spammer_app


def _fake_sa_json(token_uri: str) -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    sa = {
        "type": "service_account", "project_id": "spammer-test",
        "private_key_id": "k1", "private_key": pem,
        "client_email": "spammer@spammer-test.iam.gserviceaccount.com",
        "client_id": "1", "token_uri": token_uri,
    }
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(sa, f)
    f.close()
    return f.name


# A fixed dummy host; ASGITransport routes by PATH, ignoring the host, so the
# resolver's URLs (GMAIL_API_BASE_URL + token_uri) still drive path selection.
_HOST = "http://spammer"


async def _real_gmail_client(app):
    """Real GoogleHttpClient + GmailClient + real DwdTokenMinter, all sharing
    one httpx client whose transport is the spammer app. Base URLs come from
    env (the resolver), exactly as in production."""
    from services.integrations.gmail import dwd as dwd_mod
    from services.integrations.gmail.client import GmailClient, GoogleHttpClient

    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                               base_url=_HOST)
    dwd_mod._MINTER = None  # reset singleton so it re-reads env
    minter = dwd_mod.get_minter()
    minter._client = client  # inject ASGI transport into token exchange
    minter._owns_client = False
    http = GoogleHttpClient(minter, http_client=client)
    return GmailClient(http), client


async def test_real_gmail_client_hits_spammer(monkeypatch):
    from services.integrations.gmail.client import GMAIL_METADATA_SCOPE

    monkeypatch.setenv("GMAIL_API_BASE_URL", f"{_HOST}/gmail/gmail/v1")
    monkeypatch.setenv("GMAIL_SERVICE_ACCOUNT_JSON_FILE",
                       _fake_sa_json(f"{_HOST}/gmail/token"))

    app = build_spammer_app(gmail_messages_per_mailbox=4, rate_limit_every=0)
    gmail, client = await _real_gmail_client(app)
    try:
        email = "loadtest@val.example"
        # 1. messages_list over the real HTTP stack → 4 ids.
        lst = await gmail.messages_list(
            user_email=email, scope=GMAIL_METADATA_SCOPE)
        ids = [m["id"] for m in lst["messages"]]
        assert len(ids) == 4

        # 2. get_message hydrates a real resource (Message-ID header present).
        msg = await gmail.get_message(
            user_email=email, scope=GMAIL_METADATA_SCOPE, message_id=ids[0])
        headers = {h["name"]: h["value"] for h in msg["payload"]["headers"]}
        assert "Message-ID" in headers

        # 3. profile (last-page watermark) round-trips.
        prof = await gmail.get_profile(
            user_email=email, scope=GMAIL_METADATA_SCOPE)
        assert "historyId" in prof
    finally:
        await client.aclose()


async def test_spammer_429_maps_to_rate_limited_and_retry_absorbs(monkeypatch):
    from services.integrations.gmail.client import (
        GMAIL_METADATA_SCOPE,
        GoogleRateLimited,
    )
    from services.ingestion.workflows.retry import retry_with_backoff_on_429

    monkeypatch.setenv("GMAIL_API_BASE_URL", f"{_HOST}/gmail/gmail/v1")
    monkeypatch.setenv("GMAIL_SERVICE_ACCOUNT_JSON_FILE",
                       _fake_sa_json(f"{_HOST}/gmail/token"))

    # 429 on every 2nd data request (token endpoint exempt).
    app = build_spammer_app(gmail_messages_per_mailbox=2, rate_limit_every=2,
                            retry_after_s=0)
    gmail, client = await _real_gmail_client(app)
    try:
        email = "rl@val.example"
        # First data call: 200. Second: 429 → raises GoogleRateLimited
        # (proves the HTTP-429 → exception mapping over the real stack).
        await gmail.messages_list(user_email=email, scope=GMAIL_METADATA_SCOPE)
        with pytest.raises(GoogleRateLimited):
            await gmail.messages_list(user_email=email, scope=GMAIL_METADATA_SCOPE)

        # Wrapped in the fetcher's retry helper, the 429 is absorbed and the
        # call ultimately succeeds — the production backoff path.
        result = await retry_with_backoff_on_429(
            lambda: gmail.messages_list(
                user_email=email, scope=GMAIL_METADATA_SCOPE),
            retry_on=GoogleRateLimited,
        )
        assert "messages" in result
    finally:
        await client.aclose()
