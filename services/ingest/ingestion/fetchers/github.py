"""services/ingest/ingestion/fetchers/github.py — GitHub backfill fetcher (M6.4).

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
handler derives the lifecycle `external_id` from the object's stable
`node_id` plus the event action. The REST shaper deterministically maps
the current state to the equivalent action, so a backfilled event and its
live webhook twin dedup while later lifecycle transitions remain distinct.
Backfill is authenticated by the REST call, so no signature is attached.

============================================================
ENDPOINT DISPATCH
============================================================
The fetcher dispatches on `shard_identifier["event_type"]`:
  - `issues` → /repos/{owner}/{repo}/issues
  - `pull_requests` → /repos/{owner}/{repo}/pulls
  - `issue_comments` → /repos/{owner}/{repo}/issues/comments
  - `commits` → /repos/{owner}/{repo}/commits

All four are repo-level list endpoints (Class A — plain offset/Link
paging). Per-PR / per-commit fan-out signals (pr_reviews, check_runs)
are Class B and live in a separate fetch path; see
docs/ingestion/github-backfill-gap-closure.md.

DEDUP-KEY PARITY:
  - issue_comments → `issue_comment` webhook; external_id = comment.node_id.
    The repo-level endpoint omits the parent issue object, so we reshape
    `issue = {"number": <parsed from issue_url>}` — the handler only uses
    issue.node_id for an optional entity hint, never the dedup key.
  - commits → `push` webhook; external_id = `{repo}@{sha}`. We reshape each
    commit as a single-commit push with `after=sha` and inject
    `head_commit.timestamp` from the commit's author date so the backfilled
    observation keeps its true time. The live push tip-SHA collides with the
    backfilled tip commit (dedups); intermediate commits are backfill-only.

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
SOURCE CONTRACT
============================================================
`SourceDefinition.fetcher_binding` points directly to
`fetch_page_github`; importing this module has no registration side effect.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from services.ingest.ingestion.fetchers import FetchResult


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
    # (production, or Provider Lab when the explicit API base points at it).
    # The X3 mock harness monkeypatches this symbol to inject a fixture
    # client instead.
    from services.ingest.ingestion.fetchers._clients import open_github_client
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
# to "pull_request" (webhook, singular); the comment/commit collections
# map to their respective webhook events.
_GH_EVENT_NAME = {
    "issues": "issues",
    "pull_requests": "pull_request",
    "issue_comments": "issue_comment",
    "commits": "push",
    # Class B (fan-out — nested child collections):
    "pr_reviews": "pull_request_review",
    "check_runs": "check_run",
}

# Fan-out event types: no repo-level list endpoint exists; the fetcher
# enumerates PR parents and drains each parent's children one page per call.
_FANOUT_EVENT_TYPES = frozenset({"pr_reviews", "check_runs"})

_SUPPORTED_EVENT_TYPES = frozenset(_GH_EVENT_NAME)


def _derive_action(event_type: str, item: dict[str, Any]) -> str:
    """Synthesize the webhook `action` from the REST item's state.

    The REST list objects carry `state` ("open"/"closed") but no
    `action`. external_id parity depends on this action because mutable
    PR/issue lifecycle observations use ``{node_id}:{action}``; the REST
    shaper must therefore synthesize the same action as the equivalent
    webhook.
    """
    if event_type == "pull_requests" and bool(item.get("merged")):
        return "closed"
    return "closed" if item.get("state") == "closed" else "opened"


def _issue_number_from_url(issue_url: str | None) -> int | None:
    """Parse the trailing issue number from a REST `issue_url`
    (e.g. ".../repos/o/r/issues/42" → 42). The repo-level comments
    endpoint omits the parent issue object; the handler only needs the
    number for the content sentence, never for the dedup key."""
    if not issue_url:
        return None
    tail = issue_url.rstrip("/").rsplit("/", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _build_record(
    *, event_type: str, repo_full_name: str, payload: dict[str, Any],
) -> dict[str, Any]:
    """Reshape one REST item into the webhook event body the
    `github:webhook` handler consumes, plus the `webhook_metadata`
    header (A27.3). `payload` is the bare REST object for `event_type`.
    """
    gh_event = _GH_EVENT_NAME[event_type]
    meta = {"X-GitHub-Event": gh_event}
    repo = {"full_name": repo_full_name}

    if event_type == "commits":
        # Reshape one commit into a single-commit push body. external_id
        # parity = `{repo}@{after}` with after=sha; head_commit.timestamp
        # carries the real author date so occurred_at is correct.
        sha = payload.get("sha")
        commit = payload.get("commit") or {}
        author_obj = payload.get("author") or {}
        login = author_obj.get("login") if isinstance(author_obj, dict) else None
        commit_author = commit.get("author") or {}
        ts = commit_author.get("date") if isinstance(commit_author, dict) else None
        head_commit = {"id": sha, "timestamp": ts, "message": commit.get("message")}
        return {
            "ref": "",
            "after": sha,
            "commits": [head_commit],
            "head_commit": head_commit,
            "repository": repo,
            "sender": {"login": login or "unknown"},
            "webhook_metadata": meta,
        }

    if event_type == "issue_comments":
        user = payload.get("user") or {}
        return {
            "action": "created",
            "comment": payload,
            "issue": {"number": _issue_number_from_url(payload.get("issue_url"))},
            "repository": repo,
            "sender": {"login": user.get("login", "unknown")},
            "webhook_metadata": meta,
        }

    # issues / pull_requests — bare object under its event key.
    user = payload.get("user") or {}
    body: dict[str, Any] = {
        "action": _derive_action(event_type, payload),
        "repository": repo,
        "sender": {"login": user.get("login", "unknown")},
        "webhook_metadata": meta,
    }
    body["pull_request" if event_type == "pull_requests" else "issue"] = payload
    return body


def _is_pull_request(item: dict[str, Any]) -> bool:
    """True if an item from the `/issues` list is actually a pull request.
    GitHub returns PRs in the issues stream and documents that consumers
    identify them by the presence of a `pull_request` key."""
    return isinstance(item, dict) and item.get("pull_request") is not None


def _item_updated_at(event_type: str, item: dict[str, Any]) -> str | None:
    """The timestamp used to advance `last_seen_updated_at`. Commits
    carry it under `commit.author.date`; everything else uses
    `updated_at`."""
    if event_type == "commits":
        commit = item.get("commit") or {}
        author = commit.get("author") or {}
        return author.get("date") if isinstance(author, dict) else None
    return item.get("updated_at")


# ---------------------------------------------------------------------
# Fan-out (Class B) — pr_reviews / check_runs.
# ---------------------------------------------------------------------
class GithubFanoutCursor(BaseModel):
    """Resumable two-level cursor for nested child collections.

    Parents are PRs (enumerated via the pull_requests list); children are
    that PR's reviews (pr_reviews) or the check-runs for the PR's head SHA
    (check_runs). Each `_fetch_page_fanout` call does exactly ONE HTTP
    fetch — either advance parent enumeration or drain one child page — so
    the whole walk is restorable from this dict under the N1 invariant.
    """

    model_config = ConfigDict(extra="forbid")

    parent_page: int = 1
    parents_exhausted: bool = False
    parent_queue: list[dict[str, Any]] = Field(default_factory=list)
    current_parent: dict[str, Any] | None = None
    child_page: int = 1
    last_seen_updated_at: str | None = None


def _parent_entry(event_type: str, pr: dict[str, Any]) -> dict[str, Any] | None:
    """Project a PR list item into the minimal parent context the child
    fetch + reshape need. Returns None to skip a parent (e.g. a PR with no
    head SHA when fanning out to check_runs)."""
    if event_type == "check_runs":
        head = pr.get("head") or {}
        sha = head.get("sha") if isinstance(head, dict) else None
        return {"sha": sha} if sha else None
    # pr_reviews: keep number (child endpoint) + node_id (reshape hint).
    return {"number": pr.get("number"), "node_id": pr.get("node_id")}


def _build_review_record(
    repo_full_name: str, parent: dict[str, Any], review: dict[str, Any],
) -> dict[str, Any]:
    """Reshape one review into the `pull_request_review` webhook body.
    external_id parity = review.node_id."""
    user = review.get("user") or {}
    return {
        "action": "submitted",
        "review": review,
        "pull_request": {
            "number": parent.get("number"),
            "node_id": parent.get("node_id"),
        },
        "repository": {"full_name": repo_full_name},
        "sender": {"login": user.get("login", "unknown")},
        "webhook_metadata": {"X-GitHub-Event": "pull_request_review"},
    }


def _build_check_run_record(
    repo_full_name: str, check: dict[str, Any],
) -> dict[str, Any]:
    """Reshape one check-run into the `check_run` webhook body. external_id
    parity = check.node_id; check runs are bot-originated (no sender)."""
    return {
        "action": "completed",
        "check_run": check,
        "repository": {"full_name": repo_full_name},
        "webhook_metadata": {"X-GitHub-Event": "check_run"},
    }


async def _fetch_page_fanout(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One unit of fan-out work: enumerate the next PR page, or drain one
    child page for the parent currently in flight."""
    event_type = shard_identifier["event_type"]
    owner = shard_identifier.get("owner")
    repo = shard_identifier.get("repo")
    repo_full_name = shard_identifier.get(
        "repo_full_name", f"{owner}/{repo}",
    )
    cur = (
        GithubFanoutCursor()
        if cursor is None
        else GithubFanoutCursor.model_validate(cursor)
    )
    client, close = await _open_github_client(install)
    try:
        # 1. Ensure a parent is in flight, enumerating PRs if needed.
        if cur.current_parent is None:
            if not cur.parent_queue:
                if cur.parents_exhausted:
                    return FetchResult(
                        records=[],
                        next_cursor=cur.model_dump(mode="json"),
                        end_of_data=True,
                    )
                prs, _etag, next_page = await client.list_repo_events(
                    owner=owner, repo=repo, event_type="pull_requests",
                    page=cur.parent_page, per_page=_DEFAULT_PER_PAGE,
                    etag=None,
                )
                for pr in prs:
                    entry = _parent_entry(event_type, pr)
                    if entry is not None:
                        cur.parent_queue.append(entry)
                if next_page is None:
                    cur.parents_exhausted = True
                else:
                    cur.parent_page = next_page
                end = cur.parents_exhausted and not cur.parent_queue
                return FetchResult(
                    records=[],
                    next_cursor=cur.model_dump(mode="json"),
                    end_of_data=end,
                )
            cur.current_parent = cur.parent_queue.pop(0)
            cur.child_page = 1

        # 2. Drain one child page for the current parent.
        parent = cur.current_parent
        if event_type == "pr_reviews":
            children, _etag, next_child = await client.list_pr_reviews(
                owner=owner, repo=repo, pull_number=parent["number"],
                page=cur.child_page, per_page=_DEFAULT_PER_PAGE, etag=None,
            )
            records = [
                _build_review_record(repo_full_name, parent, c)
                for c in children
            ]
            ts_field = "submitted_at"
        else:  # check_runs
            children, _etag, next_child = await client.list_check_runs(
                owner=owner, repo=repo, ref=parent["sha"],
                page=cur.child_page, per_page=_DEFAULT_PER_PAGE, etag=None,
            )
            records = [
                _build_check_run_record(repo_full_name, c) for c in children
            ]
            ts_field = "completed_at"

        for c in children:
            ts = c.get(ts_field)
            if ts and (
                cur.last_seen_updated_at is None
                or ts > cur.last_seen_updated_at
            ):
                cur.last_seen_updated_at = ts

        if next_child is None:
            cur.current_parent = None
            cur.child_page = 1
        else:
            cur.child_page = next_child

        end = (
            cur.current_parent is None
            and not cur.parent_queue
            and cur.parents_exhausted
        )
        return FetchResult(
            records=records,
            next_cursor=cur.model_dump(mode="json"),
            end_of_data=end,
        )
    finally:
        await close()


async def fetch_page_github(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of records via Octokit + cursor advance.

    Class A (repo-level list: issues/pull_requests/issue_comments/commits)
    pages here directly; Class B (pr_reviews/check_runs) is delegated to the
    fan-out walker.
    """
    event_type = shard_identifier.get("event_type")
    owner = shard_identifier.get("owner")
    repo = shard_identifier.get("repo")
    repo_full_name = shard_identifier.get(
        "repo_full_name", f"{owner}/{repo}",
    )

    if event_type not in _SUPPORTED_EVENT_TYPES:
        raise ValueError(
            f"github fetcher: unknown event_type={event_type!r}"
        )
    if event_type in _FANOUT_EVENT_TYPES:
        return await _fetch_page_fanout(install, shard_identifier, cursor)

    cur = _decode_cursor(cursor)
    client, close = await _open_github_client(install)
    try:
        page_records, etag, next_page = await client.list_repo_events(
            owner=owner, repo=repo, event_type=event_type,
            page=cur.page, per_page=_DEFAULT_PER_PAGE,
            etag=cur.etag,
        )
        # GitHub's REST `/repos/{o}/{r}/issues` returns pull requests as well
        # as issues (every PR is an issue), each carrying a `pull_request` key
        # — and the PR's issue-endpoint `node_id` is an *issue* id distinct
        # from the `PullRequest_*` id the `pull_requests` shard sees, so dedup
        # can't collapse them. Skip them here (GitHub's documented consumer
        # guard) so PRs aren't double-ingested as bogus `issues` observations;
        # they're already covered by the dedicated `pull_requests` shard. The
        # cursor below still advances over the full page (PRs included) so
        # pagination / last_seen stays correct.
        record_items = (
            [item for item in page_records if not _is_pull_request(item)]
            if event_type == "issues"
            else page_records
        )
        records = [
            _build_record(
                event_type=event_type, repo_full_name=repo_full_name,
                payload=item,
            )
            for item in record_items
        ]
        last_seen = cur.last_seen_updated_at
        for item in page_records:
            ts = _item_updated_at(event_type, item)
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




__all__ = [
    "GithubCursor",
    "GithubFanoutCursor",
    "SHARD_KIND_REPO_EVENTS",
    "fetch_page_github",
]
