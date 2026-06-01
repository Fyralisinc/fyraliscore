"""services/integrations/gmail/directory.py — Directory-based mailbox enumeration.

Resolves an inclusion_spec like:

    {
      "users":      ["alice@acme.com", "bob@acme.com"],
      "groups":     ["sales@acme.com"],
      "org_units":  ["/Sales", "/Engineering"]
    }

into the concrete list of email addresses that should be watched.
Honors gmail_mailbox_optouts.

Notes:
  - This is a *resolution* layer, not a permissions layer. The
    inclusion_spec is admin-authored and trusted; opt-out is the only
    subtraction.
  - Domain alias users (multi-domain Workspace) are surfaced as their
    primary email. Aliases never end up in the watch table.
  - Suspended / archived users are filtered out (their primaryEmail
    may still receive mail but a watch on them would surface old
    history we don't want).
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import structlog

from services.integrations.gmail.client import DirectoryClient


log = structlog.get_logger("integrations.gmail.directory")


def _is_active_user(user: dict[str, Any]) -> bool:
    """Heuristic: include users who can actually receive mail."""
    if user.get("suspended"):
        return False
    if user.get("archived"):
        return False
    # Some users (delegated admin without mailbox) report no primaryEmail
    # via `isMailboxSetup=false`. Filter them.
    if user.get("isMailboxSetup") is False:
        return False
    return bool(user.get("primaryEmail"))


async def resolve_inclusion(
    directory: DirectoryClient,
    *,
    workspace_domain: str,
    inclusion_spec: dict[str, Any],
    optouts: set[str],
) -> list[str]:
    """Resolve an inclusion_spec → sorted list of unique primary email addresses.

    The result is the candidate set BEFORE any per-mailbox checks
    (e.g. mailbox already watched). The watch lifecycle layer
    reconciles against `gmail_mailbox_watches`.
    """
    emails: set[str] = set()

    # 1. Explicit users.
    for email in inclusion_spec.get("users") or []:
        if isinstance(email, str) and email:
            emails.add(email.lower())

    # 2. Groups → resolve members. Nested groups not expanded in v1;
    #    most orgs use shallow groups for mail distribution.
    for group in inclusion_spec.get("groups") or []:
        if not isinstance(group, str) or not group:
            continue
        page_token: str | None = None
        while True:
            page = await directory.list_group_members(group_key=group, page_token=page_token)
            for member in page.items:
                if member.get("type") == "USER" and member.get("email"):
                    emails.add(member["email"].lower())
            page_token = page.next_page_token
            if not page_token:
                break

    # 3. Org units → list users whose orgUnitPath matches.
    for ou in inclusion_spec.get("org_units") or []:
        if not isinstance(ou, str) or not ou:
            continue
        page_token = None
        while True:
            page = await directory.list_users_in_orgunit(
                org_unit_path=ou, page_token=page_token,
            )
            for user in page.items:
                if _is_active_user(user):
                    emails.add(user["primaryEmail"].lower())
            page_token = page.next_page_token
            if not page_token:
                break

    # 4. Apply opt-outs.
    emails -= {e.lower() for e in optouts}

    # 5. Sort for determinism.
    return sorted(emails)


async def enumerate_domain(
    directory: DirectoryClient,
    *,
    workspace_domain: str,
) -> dict[str, list[dict[str, Any]]]:
    """Return ALL users/groups/OUs in the domain for the admin-pick UI.

    Used only by the connect wizard's selector. Not used in the hot path.
    """
    users: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        page = await directory.list_users(domain=workspace_domain, page_token=page_token)
        for u in page.items:
            if _is_active_user(u):
                users.append({
                    "email": u["primaryEmail"],
                    "name": u.get("name", {}).get("fullName"),
                    "org_unit_path": u.get("orgUnitPath"),
                })
        page_token = page.next_page_token
        if not page_token:
            break

    groups: list[dict[str, Any]] = []
    page_token = None
    while True:
        page = await directory.list_groups(domain=workspace_domain, page_token=page_token)
        for g in page.items:
            groups.append({
                "email": g.get("email"),
                "name": g.get("name"),
                "description": g.get("description"),
                "members_count": int(g.get("directMembersCount") or 0),
            })
        page_token = page.next_page_token
        if not page_token:
            break

    org_units = await directory.list_org_units()
    org_unit_paths = [
        {"path": ou.get("orgUnitPath"), "name": ou.get("name")}
        for ou in org_units
        if ou.get("orgUnitPath")
    ]

    return {
        "users": users,
        "groups": groups,
        "org_units": org_unit_paths,
    }


__all__ = ["enumerate_domain", "resolve_inclusion"]
