"""GitHub repos fixture generator.

`make_github_repos(org_or_user=..., repos=N, events_per_repo=M)`
produces a deterministic set of repositories shaped to feed
`MockGithubClient`.
"""
from __future__ import annotations

import hashlib
from typing import Any


EVENT_TYPES = ("issues", "pull_requests")


def make_github_repos(
    *,
    org_or_user: str,
    repos: int = 5,
    events_per_repo: int = 20,
    installation_id: str = "12345",
    per_page: int = 30,
) -> dict[str, Any]:
    """Build a GitHub installation fixture.

    Args:
      org_or_user: Owner login (used in `full_name`).
      repos: Number of repositories.
      events_per_repo: Events of EACH type per repo. Total per repo =
        `events_per_repo * len(EVENT_TYPES)`.
      installation_id: GitHub App installation id.
      per_page: Mock client's per-page cap for `list_repo_events`.

    Returns:
      Fixture dict consumable by `MockGithubClient(fixture=...)`.
    """
    repo_list: list[dict[str, Any]] = []
    for r in range(repos):
        repo_name = f"repo-{_digest(org_or_user, r)[:8]}"
        full_name = f"{org_or_user}/{repo_name}"
        events_by_type: dict[str, list[dict[str, Any]]] = {}
        for et in EVENT_TYPES:
            events_by_type[et] = [
                _event(full_name, et, idx) for idx in range(events_per_repo)
            ]
        repo_list.append({
            "full_name": full_name,
            "events_by_type": events_by_type,
        })

    return {
        "installation_id": installation_id,
        "repos": repo_list,
        "per_page": per_page,
    }


def _event(
    full_name: str, event_type: str, idx: int,
) -> dict[str, Any]:
    # ISO timestamps spaced 1 minute apart, oldest first.
    minute = 1_700_000 + idx
    iso = f"2026-01-01T00:{minute % 60:02d}:00Z"
    record_id = f"{event_type}-{idx}-{_digest(full_name, idx)[:8]}"
    return {
        "id": record_id,
        "number": idx + 1,
        "title": f"{event_type} #{idx + 1} for {full_name}",
        "state": "open" if idx % 2 == 0 else "closed",
        "updated_at": iso,
        "html_url": f"https://github.com/{full_name}/{event_type}/{idx + 1}",
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()
