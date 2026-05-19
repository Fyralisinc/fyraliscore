"""services/ingestion/fetchers/github.py — GitHub backfill fetcher (M6.4).

Per ingestion LLD §4 + §3.1 (N1) + A18 (per-source backfill = new
code) + A18.4 (shard_kind mirrored into shard_identifier).

============================================================
ENDPOINT DISPATCH
============================================================
The fetcher dispatches on `shard_identifier["event_type"]`:
  - `issues` → /repos/{owner}/{repo}/issues
  - `pull_requests` → /repos/{owner}/{repo}/pulls

Cursor schema (per-source Pydantic, opaque to ShardFetch):
    GithubCursor:
      - page: int      — 1-indexed; advances with each page
      - etag: str|None — captured from response; used by reconciler
        for the fast-path "did anything change?" check.
      - last_seen_updated_at: ISO timestamp of the most recent
        record observed; used by reconciler for cursor-based gap
        detection.

Paging is plain offset paging via `?per_page=N&page=K`. End-of-data
when the response is an empty list (no more pages).

============================================================
WIRE-IN
============================================================
This module assigns into `FETCHER_DISPATCH['github']` at module-
import time.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_REPO_EVENTS = "github_repo_events"
_DEFAULT_PER_PAGE = 100


class GithubCursor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page: int = 1
    etag: str | None = None
    last_seen_updated_at: str | None = None


# Test seam — production opens a real GithubClient against the
# install's auth; tests rebind to return a fake.
async def _open_github_client(install: asyncpg.Record):  # noqa: ANN202
    from services.integrations.github.client import GithubClient
    # Production uses the SAME GithubClient instance the planner used
    # (shared per-process). For the fetcher, we build a new one if no
    # shared instance is available. Tests override.
    raise RuntimeError(
        "fetchers.github._open_github_client not configured; tests must "
        "rebind via monkeypatch. Production wiring should provide a "
        "shared GithubClient instance via the substrate (M-Load work)."
    )


def _decode_cursor(cursor: dict[str, Any] | None) -> GithubCursor:
    if cursor is None:
        return GithubCursor()
    return GithubCursor.model_validate(cursor)


def _encode_cursor(cursor: GithubCursor) -> dict[str, Any]:
    return cursor.model_dump(mode="json")


def _build_record(
    *, event_type: str, repo_full_name: str,
    installation_id: str, payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "repo_full_name": repo_full_name,
        "installation_id": installation_id,
        "payload": payload,
        "read_path": "backfill",
    }


async def fetch_page_github(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of records via Octokit + cursor advance."""
    event_type = shard_identifier.get("event_type")
    owner = shard_identifier.get("owner")
    repo = shard_identifier.get("repo")
    repo_full_name = shard_identifier.get(
        "repo_full_name", f"{owner}/{repo}",
    )
    installation_id = str(shard_identifier.get("installation_id") or "")

    if event_type not in ("issues", "pull_requests"):
        raise ValueError(
            f"github fetcher: unknown event_type={event_type!r}"
        )

    cur = _decode_cursor(cursor)
    client, close = await _open_github_client(install)
    try:
        page_records, etag, next_page = await client.list_repo_events(
            owner=owner, repo=repo, event_type=event_type,
            page=cur.page, per_page=_DEFAULT_PER_PAGE,
            etag=cur.etag,
        )
        records = [
            _build_record(
                event_type=event_type, repo_full_name=repo_full_name,
                installation_id=installation_id, payload=item,
            )
            for item in page_records
        ]
        last_seen = cur.last_seen_updated_at
        for item in page_records:
            ts = item.get("updated_at")
            if ts and (last_seen is None or ts > last_seen):
                last_seen = ts

        is_end = (
            next_page is None
            or len(page_records) < _DEFAULT_PER_PAGE
        )
        next_cursor = GithubCursor(
            page=next_page if next_page is not None else cur.page + 1,
            etag=etag,
            last_seen_updated_at=last_seen,
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(next_cursor),
            end_of_data=is_end,
        )
    finally:
        await close()


FETCHER_DISPATCH["github"] = fetch_page_github


__all__ = [
    "GithubCursor",
    "SHARD_KIND_REPO_EVENTS",
    "fetch_page_github",
]
