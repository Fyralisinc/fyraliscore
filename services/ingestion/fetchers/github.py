"""services/ingestion/fetchers/github.py — GitHub backfill fetcher (M6.4).

Per ingestion LLD §4 + §3.1 (N1) + A18 (per-source backfill = new
code) + A18.4 (shard_kind mirrored into shard_identifier) + A27.3
(handler conformance).

============================================================
HANDLER CONFORMANCE (A27.3) + EXTERNAL_ID PARITY (HLD §02 L278)
============================================================
The REST list endpoints return bare issue / PR objects, but the
`github:webhook` handler consumes the webhook *event body*
(`{action, issue|pull_request, repository, sender}`) and reads the
event TYPE from the `X-GitHub-Event` header. So the fetcher reshapes
each REST item into that event-body shape and emits the header under
the reserved `webhook_metadata` key (lifted into the RawEnvelope blob
by the producer; replayed to the handler by the normalizer). The
handler derives `external_id` from the object's `node_id`, which is
identical in the REST item and the webhook payload — so a backfilled
event and its live webhook twin dedup to one observation. Backfill is
authenticated by the REST call, so no signature is attached.

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
    # Builds a real GithubClient pointed at the resolver's github_api base
    # (production, or the local spammer when *_API_BASE_URL points at it).
    # The X3 mock harness monkeypatches this symbol to inject a fixture
    # client instead.
    from services.ingestion.fetchers._clients import open_github_client
    return await open_github_client(install)


def _decode_cursor(cursor: dict[str, Any] | None) -> GithubCursor:
    if cursor is None:
        return GithubCursor()
    return GithubCursor.model_validate(cursor)


def _encode_cursor(cursor: GithubCursor) -> dict[str, Any]:
    return cursor.model_dump(mode="json")


# Maps the shard's REST event_type to the webhook `X-GitHub-Event`
# header value the handler dispatches on. The REST endpoint "issues"
# and the webhook event "issues" coincide; "pull_requests" (REST) maps
# to "pull_request" (webhook, singular).
_GH_EVENT_NAME = {"issues": "issues", "pull_requests": "pull_request"}


def _derive_action(event_type: str, item: dict[str, Any]) -> str:
    """Synthesize the webhook `action` from the REST item's state.

    The REST list objects carry `state` ("open"/"closed") but no
    `action`. external_id parity does NOT depend on `action` (it's
    derived from `node_id`); this only shapes content/trust_tier so the
    backfilled observation reads sensibly.
    """
    if event_type == "pull_requests" and bool(item.get("merged")):
        return "closed"
    return "closed" if item.get("state") == "closed" else "opened"


def _build_record(
    *, event_type: str, repo_full_name: str, payload: dict[str, Any],
) -> dict[str, Any]:
    """Reshape one REST item into the webhook event body the
    `github:webhook` handler consumes, plus the `webhook_metadata`
    header (A27.3). `payload` is the bare issue / PR object.
    """
    gh_event = _GH_EVENT_NAME[event_type]
    user = payload.get("user") or {}
    body: dict[str, Any] = {
        "action": _derive_action(event_type, payload),
        "repository": {"full_name": repo_full_name},
        "sender": {"login": user.get("login", "unknown")},
        # The reserved key the producer lifts into the blob's
        # webhook_metadata; the normalizer replays it as the handler's
        # X-GitHub-Event header.
        "webhook_metadata": {"X-GitHub-Event": gh_event},
    }
    body["pull_request" if event_type == "pull_requests" else "issue"] = payload
    return body


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
                payload=item,
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
