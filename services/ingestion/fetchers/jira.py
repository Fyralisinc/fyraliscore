"""services/ingestion/fetchers/jira.py — Jira backfill/poll fetcher (IN-17).

Per A18 (per-source backfill = net-new code) + A16/N1 (cursor advanced by
ShardFetch, opaque to it) + A27.3 (records shaped for the handler).

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `jira_project_issues` shard streams one project's issues. ShardFetch calls
this fetcher in a loop, persisting the returned cursor between calls. Two
modes share the cursor:

  - FULL (initial backfill): JQL `project = "KEY" ORDER BY updated ASC`,
    `expand=changelog`, paged via `startAt`. `updated ASC` makes the high-water
    `updated` monotonic, so a crash mid-walk resumes cleanly.
  - INCREMENTAL (poll): when the shard is warm-started with an `updated_cursor`
    (or the cursor carries `high_water_updated`), the JQL adds
    `AND updated >= "<cursor>"` so only changed issues come back. Jira JQL has
    minute precision; the `>=` overlap re-fetches the boundary minute, which
    dedups via the versioned external_id.

`end_of_data=True` when the page is the last (`startAt + len >= total`).

============================================================
FAN-OUT: ONE ISSUE -> N RECORDS (A27.3 handler conformance)
============================================================
The `jira:issue` handler produces ONE observation per record. To preserve full
historical fidelity (the per-transition *timing* is the velocity/flow signal),
the fetcher fans each issue out into separate records, each tagged with a
private `_fyralis_record_type` the handler branches on:

  - "issue"      : the issue's current field snapshot.
  - "transition" : one changelog history entry (a field-change event; a STATUS
                   change becomes a `state_change` observation).
  - "comment"    : one comment.

external_id parity (set by the handler) collapses a backfilled record and its
live-webhook twin to one observation. Because issues + comments MUTATE, their
external_id is versioned by the `updated` timestamp (per the IN-15 mutable-
source dedup lesson); changelog histories are immutable (history id is stable).

NOTE (documented v1 scope): `expand=changelog` and `fields.comment` from the
search endpoint return the most-recent histories/comments inline, which is
sufficient for the live + recent-history reasoning signal. Deep history beyond
the inline window would need the per-issue `/changelog` + `/comment` endpoints;
the reconciler's incremental re-walk catches anything the inline window missed.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import JiraApiError
from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_PROJECT_ISSUES = "jira_project_issues"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(100, int(os.environ.get("JIRA_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class JiraCursor(BaseModel):
    """Cursor for one project shard. Round-trips through the opaque dict in
    workflow_states.state_data per the M6.2a contract.

    - start_at            : the next `startAt` offset within this run.
    - high_water_updated  : max issue `updated` (ISO) observed — the warm-start
                            / incremental lower bound AND the reconciler's gap
                            reference point.
    - incremental_floor   : the `updated >=` lower bound frozen for this run
                            (None in FULL mode).
    - issues_seen         : diagnostic.
    - seeded              : whether the first-call setup has run.
    """

    model_config = ConfigDict(extra="forbid")

    start_at: int = 0
    high_water_updated: str | None = None
    incremental_floor: str | None = None
    issues_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> JiraCursor:
    if c is None:
        return JiraCursor()
    return JiraCursor.model_validate(c)


def _encode_cursor(c: JiraCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real JiraClient against the install's auth;
# the mock harness / tests rebind this symbol to inject a fake.
async def _open_jira_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingestion.fetchers._clients import open_jira_client
    return await open_jira_client(install)


def _to_jql_datetime(iso: str | None) -> str | None:
    """Convert an ISO-8601 `updated` timestamp to the JQL datetime literal
    Jira accepts (`yyyy/MM/dd HH:mm`, minute precision). Returns None if it
    can't be parsed (caller then runs a FULL walk)."""
    if not isinstance(iso, str) or not iso:
        return None
    s = iso
    # Jira returns e.g. 2026-05-24T10:30:00.000+0000 — normalise the offset.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    elif len(s) >= 5 and (s[-5] in "+-") and s[-3] != ":":
        s = s[:-2] + ":" + s[-2:]  # +0000 -> +00:00
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y/%m/%d %H:%M")


def _build_jql(project_key: str, floor: str | None) -> str:
    safe_key = project_key.replace('"', "")
    jql = f'project = "{safe_key}"'
    floor_literal = _to_jql_datetime(floor)
    if floor_literal:
        jql += f' AND updated >= "{floor_literal}"'
    return jql + " ORDER BY updated ASC"


def _bump_high_water(cur: JiraCursor, updated: Any) -> None:
    if isinstance(updated, str) and (
        cur.high_water_updated is None or updated > cur.high_water_updated
    ):
        cur.high_water_updated = updated


def _explode_issue(issue: dict[str, Any], *, site: str) -> list[dict[str, Any]]:
    """Fan one Jira issue out into the tagged records the handler consumes:
    one "issue" record, one "transition" per changelog history, one "comment"
    per inline comment. The site (base host) is injected for external_id
    namespacing."""
    records: list[dict[str, Any]] = []
    issue_id = str(issue.get("id") or "")
    issue_key = issue.get("key")
    fields = issue.get("fields") or {}

    # 1. The issue snapshot.
    issue_rec = dict(issue)
    issue_rec["_fyralis_record_type"] = "issue"
    issue_rec["_fyralis_site"] = site
    records.append(issue_rec)

    # 2. Changelog histories -> transitions.
    changelog = issue.get("changelog") or {}
    histories = changelog.get("histories")
    if isinstance(histories, list):
        for hist in histories:
            if not isinstance(hist, dict):
                continue
            records.append({
                "_fyralis_record_type": "transition",
                "_fyralis_site": site,
                "_fyralis_issue_id": issue_id,
                "_fyralis_issue_key": issue_key,
                "history": hist,
            })

    # 3. Inline comments.
    comment_field = fields.get("comment") or {}
    comments = comment_field.get("comments")
    if isinstance(comments, list):
        for comment in comments:
            if not isinstance(comment, dict):
                continue
            records.append({
                "_fyralis_record_type": "comment",
                "_fyralis_site": site,
                "_fyralis_issue_id": issue_id,
                "_fyralis_issue_key": issue_key,
                "comment": comment,
            })

    return records


def _site_of(install: asyncpg.Record) -> str:
    """The site host used in external_id. MUST match what the live webhook
    handler derives from the issue's `self` URL host, so a backfilled record
    and its webhook twin dedup (the handler reads `_fyralis_site` for backfill
    records and the `issue.self` host for webhooks — both are the site host)."""
    base = str(install["base_url"]) if "base_url" in install else ""
    return base.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0]


async def fetch_page_jira(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of issues (fanned out into records) + next cursor."""
    project_key = shard_identifier.get("project_key")
    if not isinstance(project_key, str) or not project_key:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)

    if not cur.seeded:
        warm = shard_identifier.get("updated_cursor")
        if isinstance(warm, str) and warm:
            cur.incremental_floor = warm  # warm start -> incremental
            cur.high_water_updated = warm
        cur.seeded = True

    site = _site_of(install)
    jql = _build_jql(project_key, cur.incremental_floor)
    page_size = _page_size()

    client, close = await _open_jira_client(install)
    try:
        try:
            issues, next_start, total = await client.search_issues(
                jql=jql, start_at=cur.start_at, max_results=page_size,
            )
        except JiraApiError as exc:
            if (exc.context or {}).get("code") == "jira_api_rate_limited" or \
               getattr(exc, "_code", None) == "jira_api_rate_limited":
                # Retry budget spent — leave the cursor unadvanced, end this
                # round empty so ShardFetch re-enters next tick.
                log.info(
                    "jira_backfill_rate_limited",
                    extra={"project_key": project_key},
                )
                return FetchResult(
                    records=[], next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        records: list[dict[str, Any]] = []
        for issue in issues:
            records.extend(_explode_issue(issue, site=site))
            fields = issue.get("fields") or {}
            _bump_high_water(cur, fields.get("updated"))

        cur.issues_seen += len(issues)
        is_last = next_start is None
        cur.start_at = next_start if next_start is not None else cur.start_at

        log.info(
            "jira_backfill_page",
            extra={
                "project_key": project_key,
                "issues": len(issues),
                "records": len(records),
                "total": total,
                "is_last": is_last,
            },
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()


FETCHER_DISPATCH["jira"] = fetch_page_jira


__all__ = [
    "SHARD_KIND_PROJECT_ISSUES",
    "JiraCursor",
    "fetch_page_jira",
]
