"""Tests for services/ingest/integrations/google_drive/onboarding.py (IN-16).

resolve_drive_targets reuses the shared Gmail DirectoryClient resolver (for
My-Drive per user) plus the Drive client's drives.list (for Shared Drives), so
these tests drive both with fakes. finalize_install / connect touch the DB
(RLS) and are exercised by the pipeline integration / sandbox path.
"""
from __future__ import annotations

import pytest

from services.ingest.integrations.gmail.client import PagedResult
from services.ingest.integrations.google_drive.onboarding import resolve_drive_targets


pytestmark = pytest.mark.asyncio


class _FakeDirectory:
    def __init__(self, *, members=None):
        self._members = members or {}

    async def list_group_members(self, *, group_key, page_token=None):
        return PagedResult(items=self._members.get(group_key, []), next_page_token=None)


class _FakeDriveClient:
    def __init__(self, drives):
        self._drives = drives
        self.calls = []

    async def list_shared_drives(self, *, user_email, page_token=None):
        self.calls.append(user_email)
        return {"drives": self._drives}


async def test_my_drive_targets_per_user():
    directory = _FakeDirectory()
    resolved = await resolve_drive_targets(
        directory,
        workspace_domain="acme.com",
        inclusion_spec={"users": ["alice@acme.com", "bob@acme.com"]},
        include_shared_drives=False,
    )
    assert [t.owner_email for t in resolved.my_drives] == ["alice@acme.com", "bob@acme.com"]
    assert all(t.drive_kind == "my_drive" and t.drive_id == "my-drive"
               for t in resolved.my_drives)
    assert resolved.shared_drives == []


async def test_shared_drives_enumerated():
    directory = _FakeDirectory()
    drive_client = _FakeDriveClient([
        {"id": "0ABC", "name": "Engineering"},
        {"id": "0DEF", "name": "Sales"},
    ])
    resolved = await resolve_drive_targets(
        directory,
        workspace_domain="acme.com",
        inclusion_spec={"users": ["alice@acme.com"]},
        include_shared_drives=True,
        drive_client=drive_client,
    )
    assert len(resolved.my_drives) == 1
    shared_ids = {t.drive_id for t in resolved.shared_drives}
    assert shared_ids == {"0ABC", "0DEF"}
    assert all(t.drive_kind == "shared_drive" for t in resolved.shared_drives)
    # Shared-drive enumeration impersonates the first resolved user as admin.
    assert drive_client.calls == ["alice@acme.com"]


async def test_shared_drives_skipped_when_no_client():
    directory = _FakeDirectory()
    resolved = await resolve_drive_targets(
        directory,
        workspace_domain="acme.com",
        inclusion_spec={"users": ["alice@acme.com"]},
        include_shared_drives=True,
        drive_client=None,
    )
    assert resolved.shared_drives == []
    assert len(resolved.all()) == 1
