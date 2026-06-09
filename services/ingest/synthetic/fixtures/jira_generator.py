"""Jira project/issue fixture generator (IN-17).

`make_jira(site_host=..., projects=N, issues_per_project=M,
transitions_per_issue=T, comments_per_issue=C)` produces a deterministic set
of Jira projects + issues shaped to feed `MockJiraClient`.

The issue shape mirrors the REAL `/rest/api/3/search/jql` response the
production fetcher (`services/ingest/ingestion/fetchers/jira.py`) consumes:
each issue carries `id`, `key`, `self`, a `fields` object (with `updated`,
`status`, `comment.comments`, ...) and an inline `changelog.histories` list
(the `expand=changelog` surface). The fetcher fans each issue out into:

  - 1 "issue" record   (the fields snapshot),
  - T "transition" records (one per `changelog.histories` entry),
  - C "comment" records   (one per `fields.comment.comments` entry),

so the per-project observation count is
`issues_per_project * (1 + transitions_per_issue + comments_per_issue)`.

Determinism: every id / timestamp / name is derived from a SHA-256 of its
coordinates (project index, issue index, ...), so a given call always yields
byte-identical output. Timestamps land in the 2026-01 observations partition
window, spaced minutes apart so `ORDER BY updated ASC` is well-defined and the
handler's `occurred_at` parses into 2026.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


# Jira's wire datetime format: millis + numeric offset WITHOUT a colon
# (e.g. "2026-01-05T00:00:00.000+0000"), the exact shape the production
# handler/fetcher normalise (`_parse_iso` / `_to_jql_datetime`).
def _jira_ts(base: datetime, minutes: int) -> str:
    dt = (base + timedelta(minutes=minutes)).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}+0000"


def _parse_base(base_iso: str) -> datetime:
    s = base_iso
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def make_jira(
    *,
    site_host: str = "acme.atlassian.net",
    projects: int = 1,
    issues_per_project: int = 3,
    transitions_per_issue: int = 0,
    comments_per_issue: int = 0,
    page_size: int = 50,
    base_iso: str = "2026-01-05T00:00:00Z",
) -> dict[str, Any]:
    """Build a Jira site fixture consumable by `MockJiraClient(fixture=...)`.

    Args:
      site_host: The Atlassian site host (used in `issue.self` + external_id
        namespacing parity with the live webhook path).
      projects: Number of projects (each becomes one searchable project_key).
      issues_per_project: Issues per project.
      transitions_per_issue: `changelog.histories` entries per issue (each
        fans out to one "transition" record). The first transition is always
        a STATUS change (the handler emits it as kind="state_change").
      comments_per_issue: `fields.comment.comments` entries per issue (each
        fans out to one "comment" record).
      page_size: The mock client's per-page cap for `search_issues`.
      base_iso: ISO-8601 anchor for the oldest issue's `updated`; everything
        is offset forward from here so it lands in the 2026-01 partition.

    Returns:
      Fixture dict:
        {
          "site_host": "acme.atlassian.net",
          "page_size": 50,
          "projects": [
            {
              "project_key": "ENG",
              "project_id": "10000",
              "issues": [ <raw /search/jql issue dict>, ... ],  # ORDER BY updated ASC
            },
            ...
          ],
        }
    """
    base = _parse_base(base_iso)
    project_list: list[dict[str, Any]] = []

    for p in range(projects):
        project_key = _project_key(site_host, p)
        project_id = str(10000 + p)
        issues: list[dict[str, Any]] = []
        for i in range(issues_per_project):
            issues.append(
                _issue(
                    site_host=site_host,
                    project_key=project_key,
                    project_id=project_id,
                    p=p,
                    i=i,
                    base=base,
                    transitions=transitions_per_issue,
                    comments=comments_per_issue,
                )
            )
        # The fetcher's JQL is `ORDER BY updated ASC`; issues are emitted oldest
        # first (issue index 0 is the oldest) so the contract holds.
        project_list.append({
            "project_key": project_key,
            "project_id": project_id,
            "issues": issues,
        })

    return {
        "site_host": site_host,
        "page_size": page_size,
        "projects": project_list,
    }


# ---------------------------------------------------------------------
# Per-entity builders
# ---------------------------------------------------------------------

def _issue(
    *,
    site_host: str,
    project_key: str,
    project_id: str,
    p: int,
    i: int,
    base: datetime,
    transitions: int,
    comments: int,
) -> dict[str, Any]:
    # Each issue is 60 minutes apart so transitions/comments (minutes within
    # the issue) never collide with the next issue's `updated`.
    issue_minute = i * 60
    issue_id = str(10000 + p * 1000 + i)
    issue_key = f"{project_key}-{i + 1}"
    self_url = f"https://{site_host}/rest/api/3/issue/{issue_id}"
    updated = _jira_ts(base, issue_minute + 30)  # mutated after creation
    created = _jira_ts(base, issue_minute)

    reporter = _user(site_host, p, i, "reporter")
    assignee = _user(site_host, p, i, "assignee")

    fields: dict[str, Any] = {
        "summary": f"{issue_key}: {_digest(site_host, p, i, 'summary')[:12]}",
        "description": _adf_paragraph(
            f"Description for {issue_key} ({_digest(site_host, p, i)[:8]})"
        ),
        "issuetype": {"name": "Task" if i % 2 == 0 else "Bug"},
        "status": {"name": "In Progress" if i % 2 == 0 else "Open"},
        "priority": {"name": "High" if i % 3 == 0 else "Medium"},
        "assignee": assignee,
        "reporter": reporter,
        "creator": reporter,
        "created": created,
        "updated": updated,
        "resolution": None,
        "labels": [f"label-{_digest(site_host, p, i, 'lbl')[:6]}"],
        "project": {"key": project_key, "id": project_id, "name": f"Project {project_key}"},
    }

    if comments > 0:
        fields["comment"] = {
            "comments": [
                _comment(site_host, p, i, c, base, issue_minute)
                for c in range(comments)
            ],
            "total": comments,
        }

    issue: dict[str, Any] = {
        "id": issue_id,
        "key": issue_key,
        "self": self_url,
        "fields": fields,
    }

    if transitions > 0:
        issue["changelog"] = {
            "histories": [
                _history(site_host, p, i, h, base, issue_minute)
                for h in range(transitions)
            ],
            "total": transitions,
        }

    return issue


def _history(
    site_host: str, p: int, i: int, h: int, base: datetime, issue_minute: int,
) -> dict[str, Any]:
    history_id = str(900000 + p * 10000 + i * 100 + h)
    # The first transition is a STATUS change → handler emits state_change.
    if h == 0:
        items = [{
            "field": "status",
            "fieldtype": "jira",
            "fromString": "To Do",
            "toString": "In Progress",
        }]
    else:
        items = [{
            "field": "assignee",
            "fieldtype": "jira",
            "fromString": None,
            "toString": _digest(site_host, p, i, h, "asgn")[:8],
        }]
    return {
        "id": history_id,
        "author": _user(site_host, p, i, f"actor-{h}"),
        # Transitions happen after creation, before the issue's `updated`.
        "created": _jira_ts(base, issue_minute + 5 + h),
        "items": items,
    }


def _comment(
    site_host: str, p: int, i: int, c: int, base: datetime, issue_minute: int,
) -> dict[str, Any]:
    comment_id = str(700000 + p * 10000 + i * 100 + c)
    ts = _jira_ts(base, issue_minute + 20 + c)
    return {
        "id": comment_id,
        "author": _user(site_host, p, i, f"commenter-{c}"),
        "updateAuthor": _user(site_host, p, i, f"commenter-{c}"),
        "body": _adf_paragraph(
            f"Comment {c} on {p}-{i}: {_digest(site_host, p, i, c, 'cmt')[:10]}"
        ),
        "created": ts,
        "updated": ts,
    }


def _user(site_host: str, p: int, i: int, role: str) -> dict[str, Any]:
    acct = _digest(site_host, p, i, role)[:24]
    handle = _digest(site_host, role)[:8]
    return {
        "accountId": acct,
        "emailAddress": f"{role}-{handle}@example.com",
        "displayName": f"{role.title()} {handle}",
    }


def _adf_paragraph(text: str) -> dict[str, Any]:
    """A minimal Atlassian Document Format doc (what the v3 API returns for
    rich-text bodies); the handler's `_adf_to_text` flattens it."""
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": text}]},
        ],
    }


def _project_key(site_host: str, p: int) -> str:
    # Stable, uppercase, alphabetic project key (Jira keys are A-Z).
    digest = _digest(site_host, p, "projkey")
    letters = "".join(
        chr(ord("A") + (b % 26)) for b in bytes.fromhex(digest[:6])
    )
    return letters[:3] or "PRJ"


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update(str(part).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_jira"]
