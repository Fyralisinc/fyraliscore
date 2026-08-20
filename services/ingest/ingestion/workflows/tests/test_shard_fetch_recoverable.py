"""Regression (IN-13 hardening): the backfill fetch loop PARKS a shard
(leaves it in_progress for the orphan-scan to retry) on recoverable faults
instead of terminal-failing it.

Parked (NOT terminal-failed):
  - a recoverable source API error (rate limit / 5xx) raised by the fetcher
  - a disabled install (suspended/revoked) — resumes on unsuspend

Still terminal-failed:
  - a non-recoverable error (genuine bug / bad input)

Fully mocked — no DB, no Kafka, no subprocess (so it never touches the dev
DATABASE_URL). It patches the module-level helpers the loop calls.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

import services.ingest.ingestion.workflows.shard_fetch as sf
from lib.shared.errors import GithubApiError
from services.ingest.ingestion.workflows.shard_fetch import ShardFetch, ShardFetchConfig


def _shard():
    return {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "source": "github",
        "shard_identifier": {"shard_kind": "github_repo_events",
                             "event_type": "pull_requests",
                             "owner": "o", "repo": "r"},
    }


def _make_service(*, connector_router=None):
    svc = ShardFetch(
        MagicMock(), MagicMock(),
        config=ShardFetchConfig(), s3_client=MagicMock(),
        connector_router=connector_router,
    )
    svc._terminate_shard = AsyncMock()  # spy
    return svc


def _patch_loadable(monkeypatch, *, install):
    """Make _load_install return `install` and load_state return a cursor."""
    monkeypatch.setattr(sf, "_load_install", AsyncMock(return_value=install))
    state = MagicMock()
    state.state_data = {"cursor": None}
    monkeypatch.setattr(sf, "load_state", AsyncMock(return_value=state))


@pytest.mark.asyncio
async def test_recoverable_api_error_parks_shard(monkeypatch):
    async def _raise(*_a, **_k):
        raise GithubApiError("primary rate limit", code="github_api_rate_limited",
                             recoverable=True)
    router = MagicMock()
    router.fetch = AsyncMock(side_effect=_raise)
    svc = _make_service(connector_router=router)
    _patch_loadable(monkeypatch, install={"id": uuid4()})

    await svc._run_fetch_loop(_shard())
    # Parked: NOT terminal-failed.
    svc._terminate_shard.assert_not_called()


@pytest.mark.asyncio
async def test_disabled_install_parks_shard(monkeypatch):
    svc = _make_service()
    monkeypatch.setattr(sf, "_load_install", AsyncMock(return_value=None))

    await svc._run_fetch_loop(_shard())
    # Disabled install → parked (resumes on unsuspend), NOT terminal-failed.
    svc._terminate_shard.assert_not_called()


@pytest.mark.asyncio
async def test_nonrecoverable_error_terminal_fails(monkeypatch):
    async def _raise(*_a, **_k):
        raise ValueError("genuine bug")
    router = MagicMock()
    router.fetch = AsyncMock(side_effect=_raise)
    svc = _make_service(connector_router=router)
    _patch_loadable(monkeypatch, install={"id": uuid4()})

    await svc._run_fetch_loop(_shard())
    # Non-recoverable → terminal-failed.
    svc._terminate_shard.assert_awaited_once()
    assert svc._terminate_shard.await_args.kwargs["state"] == "failed"
