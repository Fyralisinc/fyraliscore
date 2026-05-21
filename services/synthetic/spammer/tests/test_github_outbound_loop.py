"""End-to-end proof of the outbound-API shift for GitHub.

A REAL GithubClient (real App-JWT mint → real installation-token exchange
→ real authed REST request) drives list_repo_events / head_repo_events
against the local spammer through the real httpx + FastAPI stack — no
respx, no monkeypatched mock client. Pointing the client at the spammer
is pure config (api_base_url / GITHUB_API_BASE_URL). Proves pagination
(Link header → next_page), the ETag conditional fast-path (304 →
has_changes=False), and the 429 → GithubApiError mapping over real HTTP.

Transport is httpx.ASGITransport (hermetic for CI); the spammer's main()
runs the same app on a real port for load runs.
"""
from __future__ import annotations

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from lib.shared.errors import GithubApiError
from services.synthetic.fixtures import make_github_repos
from services.synthetic.spammer.server import build_spammer_app


_HOST = "http://spammer"
_INSTALL = "inst-1"


def _set_github_app_env(monkeypatch) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    monkeypatch.setenv("GITHUB_APP_ID", "424242")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", pem)


def _client(app, monkeypatch, **kwargs):
    from services.integrations.github.client import GithubClient

    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url=_HOST)
    gh = GithubClient(
        pool=None, http_client=http,
        api_base_url=f"{_HOST}/github",
        backfill_installation_id=_INSTALL,
        **kwargs,
    )
    return gh, http


async def test_real_github_client_paginates_against_spammer(monkeypatch):
    _set_github_app_env(monkeypatch)
    fx = make_github_repos(
        org_or_user="octo", repos=1, events_per_repo=3,
        installation_id=_INSTALL,
    )
    owner, repo = fx["repos"][0]["full_name"].split("/", 1)
    app = build_spammer_app(fixtures={"github": [fx]}, rate_limit_every=0)
    gh, http = _client(app, monkeypatch)
    try:
        # list_installation_repositories resolves repos from the minted
        # token's installation (the full real App→installation flow).
        repos = await gh.list_installation_repositories(_INSTALL)
        assert repos == [fx["repos"][0]["full_name"]]

        # Page through issues at per_page=2 → 2 + 1, next_page advances.
        page1, etag1, next1 = await gh.list_repo_events(
            owner=owner, repo=repo, event_type="issues", page=1, per_page=2,
        )
        assert len(page1) == 2 and next1 == 2 and etag1
        page2, _etag2, next2 = await gh.list_repo_events(
            owner=owner, repo=repo, event_type="issues", page=2, per_page=2,
        )
        assert len(page2) == 1 and next2 is None
        # node_id parity (external_id is derived from it downstream).
        assert all("node_id" in r for r in page1 + page2)
    finally:
        await http.aclose()


async def test_github_etag_conditional_fast_path(monkeypatch):
    _set_github_app_env(monkeypatch)
    fx = make_github_repos(
        org_or_user="acme", repos=1, events_per_repo=2,
        installation_id=_INSTALL,
    )
    owner, repo = fx["repos"][0]["full_name"].split("/", 1)
    app = build_spammer_app(fixtures={"github": [fx]}, rate_limit_every=0)
    gh, http = _client(app, monkeypatch)
    try:
        changed, etag = await gh.head_repo_events(
            owner=owner, repo=repo, event_type="issues",
        )
        assert changed is True and etag
        # Same etag → 304 → no changes.
        changed2, etag2 = await gh.head_repo_events(
            owner=owner, repo=repo, event_type="issues", etag=etag,
        )
        assert changed2 is False and etag2 == etag
    finally:
        await http.aclose()


async def test_github_429_maps_to_api_error(monkeypatch):
    _set_github_app_env(monkeypatch)
    fx = make_github_repos(
        org_or_user="rl", repos=1, events_per_repo=2,
        installation_id=_INSTALL,
    )
    owner, repo = fx["repos"][0]["full_name"].split("/", 1)
    # 429 on every 2nd data request (token mint is exempt).
    app = build_spammer_app(fixtures={"github": [fx]}, rate_limit_every=2,
                            retry_after_s=0)
    gh, http = _client(app, monkeypatch)
    try:
        await gh.list_repo_events(
            owner=owner, repo=repo, event_type="issues")  # #1 → 200
        with pytest.raises(GithubApiError):
            await gh.list_repo_events(
                owner=owner, repo=repo, event_type="issues")  # #2 → 429
    finally:
        await http.aclose()
