"""Regression: the backfill opener reuses ONE GithubClient per
installation_id (process-wide) instead of building a fresh one per fetch.

A fresh client per fetch threw away the in-process installation-token cache,
so Fyralis re-minted an App installation token before nearly every REST call
(a `POST /app/installations/{id}/access_tokens` storm — wasteful and a
secondary-rate-limit risk that scales with the pr_reviews fan-out). With the
memo, the token is minted once and reused until near expiry.
"""
from __future__ import annotations

import pytest

import services.ingestion.fetchers._clients as clients


@pytest.mark.asyncio
async def test_build_github_client_is_memoized_per_installation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Spammer mode: token is preset (no real App-JWT mint), pool not touched.
    monkeypatch.setenv("SYNTHETIC_SOURCE_API_BASE", "http://localhost:7003")
    clients._GITHUB_CLIENTS.clear()

    inst_a = {"installation_id": "111", "tenant_id": "t", "id": "row-a"}
    inst_a2 = {"installation_id": "111", "tenant_id": "t", "id": "row-a"}
    inst_b = {"installation_id": "222", "tenant_id": "t", "id": "row-b"}

    c1 = await clients.build_github_client(inst_a)
    c2 = await clients.build_github_client(inst_a2)
    c3 = await clients.build_github_client(inst_b)

    # Same installation_id → SAME client object (token cache survives).
    assert c1 is c2
    # Different installation_id → distinct client.
    assert c3 is not c1
    # The preset spammer token is present and reused (no re-mint).
    assert c1._installation_tokens["111"].token == "spam-gh::111"


@pytest.mark.asyncio
async def test_explicit_pool_is_not_memoized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The planner factory passes an explicit pool and must get a fresh
    (non-shared) client, preserving its semantics."""
    monkeypatch.setenv("SYNTHETIC_SOURCE_API_BASE", "http://localhost:7003")
    clients._GITHUB_CLIENTS.clear()

    inst = {"installation_id": "333", "tenant_id": "t", "id": "row-c"}
    memoized = await clients.build_github_client(inst)
    explicit = await clients.build_github_client(inst, pool=None)  # opener path
    assert explicit is memoized  # pool=None → memoized path

    # A truthy explicit pool sentinel bypasses the memo (fresh client).
    sentinel_pool = object()
    # _effective_pool returns the provided pool verbatim, so a fresh client
    # is constructed; assert it's a different object than the memoized one.
    fresh = await clients.build_github_client(inst, pool=sentinel_pool)  # type: ignore[arg-type]
    assert fresh is not memoized
