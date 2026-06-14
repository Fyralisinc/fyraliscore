"""Unit tests for extension OAuth2 identity (M1 / DP1.4) — no DB.

Covers the security-critical logic: PBKDF2 secret hashing, bearer-JWT mint/verify
(incl. expiry + tamper + wrong-type), client-credential verification (valid /
wrong-secret / revoked / missing) via a fake pool, and the /ext/oauth/token +
/ext/whoami round-trip through the router over ASGI.
"""
from __future__ import annotations

import time

import jwt
import pytest

from services.platform.extensions import identity
from services.platform.extensions.identity import (
    ExtensionOAuthClientsRepo, ExtensionPrincipal, IdentityError,
    hash_secret, mint_access_token, verify_access_token, verify_secret,
)


@pytest.fixture(autouse=True)
def _stable_secret(monkeypatch):
    monkeypatch.setenv("EXTENSION_JWT_SECRET", "test-signing-secret")


# --- secret hashing ---------------------------------------------------------------
def test_hash_secret_roundtrip():
    stored = hash_secret("s3cr3t")
    assert stored.startswith("pbkdf2$")
    assert verify_secret("s3cr3t", stored) is True
    assert verify_secret("wrong", stored) is False
    # distinct salt each time
    assert hash_secret("s3cr3t") != stored


def test_verify_secret_rejects_garbage():
    assert verify_secret("x", "not-a-valid-format") is False
    assert verify_secret("x", "bcrypt$...") is False


# --- access tokens ----------------------------------------------------------------
def test_mint_and_verify_token():
    tok = mint_access_token("github_intel", environment="sandbox")
    assert tok["token_type"] == "Bearer" and tok["expires_in"] > 0
    principal = verify_access_token(tok["access_token"])
    assert principal == ExtensionPrincipal("github_intel", "sandbox")


def test_expired_token_rejected(monkeypatch):
    monkeypatch.setattr(identity, "_TTL", -1)  # already expired on mint
    tok = mint_access_token("ext-x")
    with pytest.raises(IdentityError):
        verify_access_token(tok["access_token"])


def test_tampered_token_rejected():
    tok = mint_access_token("ext-x")["access_token"]
    # forge with a wrong (but length-valid) key -> signature fails
    forged = jwt.encode({"sub": "evil", "typ": "ext", "exp": int(time.time()) + 99},
                        b"x" * 32, algorithm="HS256")
    with pytest.raises(IdentityError):
        verify_access_token(forged)
    # correctly-signed (real derived key) but wrong token type -> rejected
    nonext = jwt.encode({"sub": "x", "typ": "user", "exp": int(time.time()) + 99},
                        identity._signing_key(), algorithm="HS256")
    with pytest.raises(IdentityError):
        verify_access_token(nonext)
    assert verify_access_token(tok).extension_id == "ext-x"  # the real one still works


# --- client-credential verification (fake pool) -----------------------------------
class _FakeConn:
    def __init__(self, row=None, execute_result="UPDATE 1"):
        self._row = row
        self._execute_result = execute_result

    async def fetchrow(self, q, *a):
        return self._row

    async def fetchval(self, q, *a):
        # KillSwitch.is_killed issues a SELECT EXISTS(...) → must read as a bool,
        # not the canned client row.
        if "EXISTS" in q.upper():
            return False
        return self._row

    async def execute(self, q, *a):
        return self._execute_result


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *e):
                return False

        return _Ctx()


async def test_verify_credentials_valid():
    secret = "topsecret"
    row = {"extension_id": "github_intel", "environment": "production",
           "client_secret_hash": hash_secret(secret), "revoked_at": None}
    repo = ExtensionOAuthClientsRepo(_FakePool(_FakeConn(row)))
    principal = await repo.verify_credentials("ext_abc", secret)
    assert principal == ExtensionPrincipal("github_intel", "production")


async def test_verify_credentials_wrong_secret_revoked_missing():
    good = {"extension_id": "e", "environment": "production",
            "client_secret_hash": hash_secret("right"), "revoked_at": None}
    assert await ExtensionOAuthClientsRepo(_FakePool(_FakeConn(good))).verify_credentials("c", "wrong") is None

    revoked = {**good, "revoked_at": "2026-01-01"}
    assert await ExtensionOAuthClientsRepo(_FakePool(_FakeConn(revoked))).verify_credentials("c", "right") is None

    assert await ExtensionOAuthClientsRepo(_FakePool(_FakeConn(None))).verify_credentials("c", "right") is None


async def test_rotate_and_revoke_report_status():
    repo = ExtensionOAuthClientsRepo(_FakePool(_FakeConn(execute_result="UPDATE 1")))
    assert await repo.rotate_secret("ext_abc") is not None
    assert await repo.revoke("ext_abc") is True
    repo0 = ExtensionOAuthClientsRepo(_FakePool(_FakeConn(execute_result="UPDATE 0")))
    assert await repo0.rotate_secret("missing") is None
    assert await repo0.revoke("missing") is False


# --- router round-trip (ASGI) -----------------------------------------------------
async def test_token_and_whoami_over_http():
    import httpx
    from types import SimpleNamespace
    from fastapi import FastAPI
    from services.app.gateway.extension_router import build_extension_router

    secret = "rt-secret"
    row = {"extension_id": "github_intel", "environment": "production",
           "client_secret_hash": hash_secret(secret), "revoked_at": None}
    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=_FakePool(_FakeConn(row)))
    app.include_router(build_extension_router())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        # bad creds -> 401
        bad = await c.post("/ext/oauth/token",
                           data={"grant_type": "client_credentials",
                                 "client_id": "ext_abc", "client_secret": "nope"})
        assert bad.status_code == 401

        # wrong-secret row? use a matching one: override conn to reject
        ok = await c.post("/ext/oauth/token",
                          data={"grant_type": "client_credentials",
                                "client_id": "ext_abc", "client_secret": secret})
        assert ok.status_code == 200
        token = ok.json()["access_token"]

        who = await c.get("/ext/whoami", headers={"authorization": f"Bearer {token}"})
        assert who.status_code == 200 and who.json()["extension_id"] == "github_intel"

        no = await c.get("/ext/whoami")
        assert no.status_code == 401
