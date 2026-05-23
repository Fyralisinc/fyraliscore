"""Tests for services/integrations/google_calendar/onboarding.py (IN-15).

resolve_calendar_targets reuses the shared Gmail DirectoryClient resolver,
so these tests drive it with a fake directory. finalize_install / connect
touch the DB (RLS) and are exercised by the pipeline integration test.
"""
from __future__ import annotations

import pytest

from services.integrations.gmail.client import PagedResult
from services.integrations.google_calendar.onboarding import resolve_calendar_targets


pytestmark = pytest.mark.asyncio


class _FakeDirectory:
    def __init__(self, *, members=None, ou_users=None):
        self._members = members or {}
        self._ou_users = ou_users or {}

    async def list_group_members(self, *, group_key, page_token=None):
        return PagedResult(items=self._members.get(group_key, []), next_page_token=None)

    async def list_users_in_orgunit(self, *, org_unit_path, page_token=None, customer_id="my_customer"):
        return PagedResult(items=self._ou_users.get(org_unit_path, []), next_page_token=None)


async def test_explicit_users_and_group_expansion():
    directory = _FakeDirectory(members={
        "eng@acme.com": [
            {"type": "USER", "email": "carol@acme.com"},
            {"type": "USER", "email": "dave@acme.com"},
        ],
    })
    targets = await resolve_calendar_targets(
        directory,
        workspace_domain="acme.com",
        inclusion_spec={"users": ["alice@acme.com"], "groups": ["eng@acme.com"]},
        optouts=set(),
    )
    assert targets == ["alice@acme.com", "carol@acme.com", "dave@acme.com"]


async def test_optout_subtracted():
    directory = _FakeDirectory()
    targets = await resolve_calendar_targets(
        directory,
        workspace_domain="acme.com",
        inclusion_spec={"users": ["alice@acme.com", "bob@acme.com"]},
        optouts={"bob@acme.com"},
    )
    assert targets == ["alice@acme.com"]


async def test_org_unit_filters_inactive_users():
    directory = _FakeDirectory(ou_users={
        "/Sales": [
            {"primaryEmail": "ed@acme.com", "isMailboxSetup": True},
            {"primaryEmail": "suspended@acme.com", "suspended": True},
        ],
    })
    targets = await resolve_calendar_targets(
        directory,
        workspace_domain="acme.com",
        inclusion_spec={"org_units": ["/Sales"]},
        optouts=set(),
    )
    assert targets == ["ed@acme.com"]
