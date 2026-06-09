"""Self-verifying synthetic Google Drive backfill test (IN-16, X2/X3 infra).

Drives the REAL `fetch_page_google_drive` fetcher against `MockGoogleDriveClient`
(a fixture from `make_google_drive`) through the `_open_drive_client` seam, then
runs EVERY emitted record through the REAL `google_drive:file` handler. No
database / network — the mock + fixture are the only test doubles; the fetcher,
cursor logic, FULL-mode windowed paging, comment/revision fan-out, and handler
are all production code.

Asserted invariants:
  - default fan-out: a non-folder file -> EXACTLY 1 record (comments=0,
    revisions=0) so count == files_per_target,
  - fan-out count == files * (1 + comments_per_file + revisions_per_file) when
    GOOGLE_DRIVE_FETCH_COMMENTS / GOOGLE_DRIVE_FETCH_REVISIONS are on,
  - every record yields a draft with a non-null external_id + an occurred_at in
    2026 (the observations partition window),
  - pagination: files_per_target > page_size triggers a multi-page fetch,
  - faults: a rate-limit FaultProfile surfaces the production fallback (empty,
    non-terminal page).
"""
from __future__ import annotations

import asyncio

import pytest

from services.ingest.ingestion.fetchers import google_drive as drive_fetcher
from services.ingest.ingestion.fetchers.google_drive import fetch_page_google_drive
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.ingestion.workflows import retry as retry_mod
from services.ingest.integrations.gmail.client import GoogleRateLimited
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.fixtures.google_drive_generator import make_google_drive
from services.ingest.synthetic.mock_clients.google_drive import MockGoogleDriveClient


# The fetcher passes `install` straight to `_open_drive_client` (which we
# replace) and never reads it directly, so a minimal dict suffices.
def _install() -> dict[str, object]:
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "scope": "drive.readonly",
    }


def _shard(
    *,
    owner_email: str = "alice@acme.com",
    drive_id: str = "my-drive",
    drive_kind: str = "my_drive",
    start_page_token: str | None = None,  # None -> FULL backfill via list_files
) -> dict[str, object]:
    return {
        "shard_kind": "google_drive_files",
        "drive_kind": drive_kind,
        "drive_id": drive_id,
        "owner_email": owner_email,
        "installation_id": "00000000-0000-0000-0000-000000000001",
        "start_page_token": start_page_token,
    }


def _patch_client(monkeypatch, client: MockGoogleDriveClient) -> None:
    """Rebind the fetcher's `_open_drive_client` seam to yield the mock."""
    async def _open(_install):  # noqa: ANN001, ANN202
        async def _close() -> None:
            return None
        return client, _close

    monkeypatch.setattr(drive_fetcher, "_open_drive_client", _open)


async def _drive_backfill(
    install: dict[str, object], shard: dict[str, object],
) -> list[dict[str, object]]:
    """Run the real fetch loop to completion, collecting all records. Threads
    `next_cursor` back each iteration exactly like ShardFetch."""
    records: list[dict[str, object]] = []
    cursor: dict[str, object] | None = None
    for _ in range(1000):  # generous guard against a runaway loop
        result = await fetch_page_google_drive(install, shard, cursor)
        records.extend(result.records)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    else:  # pragma: no cover — only on a genuine non-terminating fetcher bug
        raise AssertionError("fetch loop did not reach end_of_data")
    return records


def _run_through_handler(records: list[dict[str, object]]):
    """Drive every fetched record through the REAL google_drive handler."""
    channel = resolve_channel("google_drive", "backfill")
    assert channel == "google_drive:file"
    handler = get_handler(channel)

    async def _run(rec: dict[str, object]):
        return await handler(dict(rec), {})

    return asyncio.run(_gather([_run(r) for r in records]))


# ---------------------------------------------------------------------------
# 1. Default backfill: file -> exactly 1 record (comments=0, revisions=0).
# ---------------------------------------------------------------------------
def test_synthetic_drive_backfill_drives_real_fetcher_and_handler(monkeypatch):
    files_per_target = 3
    fixture = make_google_drive(
        files_per_target=files_per_target,
        comments_per_file=0,
        revisions_per_file=0,
    )
    client = MockGoogleDriveClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    records = asyncio.run(_drive_backfill(_install(), _shard()))

    # comments=0/revisions=0 -> each file fans out to exactly 1 record.
    assert len(records) == files_per_target == 3
    assert all(r.get("_fyralis_record_type") == "file" for r in records)

    drafts = _run_through_handler(records)
    assert len(drafts) == files_per_target
    external_ids = set()
    for draft in drafts:
        assert draft.external_id is not None and draft.external_id != ""
        assert draft.source_channel == "google_drive:file"
        assert draft.occurred_at is not None
        assert draft.occurred_at.year == 2026
        external_ids.add(draft.external_id)
    assert len(external_ids) == files_per_target  # no accidental collapse


# ---------------------------------------------------------------------------
# 2. Fan-out: comments + revisions per file (gating flags ON).
# ---------------------------------------------------------------------------
def test_synthetic_drive_fanout_comments_and_revisions(monkeypatch):
    """With both gating env flags on, a file -> 1 file + N comments + M
    revisions. VERIFIED formula: files * (1 + comments_per_file +
    revisions_per_file)."""
    # Default-on in production, but make the dependency explicit.
    monkeypatch.setenv("GOOGLE_DRIVE_FETCH_COMMENTS", "1")
    monkeypatch.setenv("GOOGLE_DRIVE_FETCH_REVISIONS", "1")

    files_per_target = 3
    comments_per_file = 2
    revisions_per_file = 1
    fixture = make_google_drive(
        files_per_target=files_per_target,
        comments_per_file=comments_per_file,
        revisions_per_file=revisions_per_file,
    )
    client = MockGoogleDriveClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    records = asyncio.run(_drive_backfill(_install(), _shard()))

    expected = files_per_target * (1 + comments_per_file + revisions_per_file)
    assert expected == 12
    assert len(records) == expected

    types = {r.get("_fyralis_record_type") for r in records}
    assert types == {"file", "comment", "revision"}

    drafts = _run_through_handler(records)
    assert len(drafts) == expected
    external_ids = set()
    object_types = set()
    for draft in drafts:
        assert draft.external_id is not None and draft.external_id != ""
        assert draft.source_channel == "google_drive:file"
        assert draft.occurred_at.year == 2026
        external_ids.add(draft.external_id)
        object_types.add(draft.content.get("object_type"))
    assert len(external_ids) == expected  # every fanned record is distinct
    assert object_types == {"file", "comment", "revision"}


# ---------------------------------------------------------------------------
# 3. Gating: with the env flags OFF, fan-out collapses to file records only.
# ---------------------------------------------------------------------------
def test_synthetic_drive_fanout_gated_off(monkeypatch):
    monkeypatch.setenv("GOOGLE_DRIVE_FETCH_COMMENTS", "0")
    monkeypatch.setenv("GOOGLE_DRIVE_FETCH_REVISIONS", "0")

    files_per_target = 3
    fixture = make_google_drive(
        files_per_target=files_per_target,
        comments_per_file=5,   # present in the fixture but gated out
        revisions_per_file=5,
    )
    client = MockGoogleDriveClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    records = asyncio.run(_drive_backfill(_install(), _shard()))
    assert len(records) == files_per_target
    assert all(r.get("_fyralis_record_type") == "file" for r in records)


# ---------------------------------------------------------------------------
# 4. Pagination: files_per_target > page_size triggers multi-page fetch.
# ---------------------------------------------------------------------------
def test_synthetic_drive_pagination_multi_page(monkeypatch):
    page_size = 2
    files_per_target = 5  # > page_size -> ceil(5/2) = 3 pages
    fixture = make_google_drive(
        files_per_target=files_per_target,
        comments_per_file=0,
        revisions_per_file=0,
        page_size=page_size,
    )
    client = MockGoogleDriveClient(fixture=fixture, profile=HAPPY_PATH)

    call_count = {"n": 0}
    orig_list_files = client.list_files

    async def _counting_list_files(**kwargs):
        call_count["n"] += 1
        return await orig_list_files(**kwargs)

    client.list_files = _counting_list_files  # type: ignore[method-assign]
    _patch_client(monkeypatch, client)

    records = asyncio.run(_drive_backfill(_install(), _shard()))

    assert call_count["n"] >= 3  # ceil(5/2) = 3 pages
    assert len(records) == files_per_target
    file_ids = {r["id"] for r in records}
    assert len(file_ids) == files_per_target  # every file walked exactly once


# ---------------------------------------------------------------------------
# 5. Fault: a rate-limit FaultProfile surfaces the production fallback.
# ---------------------------------------------------------------------------
def test_synthetic_drive_rate_limit_fault(monkeypatch):
    """A rate-limit FaultProfile makes the FIRST call (get_start_page_token)
    raise GoogleRateLimited; after the retry budget is spent the fetcher catches
    it and ends the round empty WITHOUT advancing (end_of_data False) so
    ShardFetch re-enters next tick."""
    fixture = make_google_drive(files_per_target=3)

    # rate_limit_after_n_requests=0 -> the very first call raises.
    profile = FaultProfile(rate_limit_after_n_requests=0)

    # 1. Raw client surface raises the shared production rate-limit type.
    raw = MockGoogleDriveClient(fixture=fixture, profile=profile)
    with pytest.raises(GoogleRateLimited):
        asyncio.run(raw.get_start_page_token(user_email="alice@acme.com",
                                             drive_id="my-drive"))

    # Make retry backoff instant so the test doesn't sleep through the budget.
    async def _no_sleep(_seconds):  # noqa: ANN001, ANN202
        return None
    monkeypatch.setattr(retry_mod.asyncio, "sleep", _no_sleep)

    # 2. Through the fetcher: the rate-limit fallback returns an empty,
    #    non-terminal page (cursor preserved) so ShardFetch re-enters.
    fetch_client = MockGoogleDriveClient(fixture=fixture, profile=profile)
    _patch_client(monkeypatch, fetch_client)
    result = asyncio.run(
        fetch_page_google_drive(_install(), _shard(), None)
    )
    assert result.records == []
    assert result.end_of_data is False


# ---------------------------------------------------------------------------
# 6. Surface check: the mock implements every method the fetcher/reconciler
#    call.
# ---------------------------------------------------------------------------
def test_mock_drive_implements_methods_called_by_fetcher_and_reconciler():
    import inspect

    client = MockGoogleDriveClient(fixture=make_google_drive(files_per_target=1))
    for name in (
        "get_start_page_token", "list_files", "list_changes", "export_text",
        "list_comments", "list_revisions", "has_changes_since",
    ):
        assert hasattr(client, name)
        assert inspect.iscoroutinefunction(getattr(client, name))


# ---------------------------------------------------------------------------
# Local async helper (avoids creating multiple event loops per record).
# ---------------------------------------------------------------------------
async def _gather(coros):
    return await asyncio.gather(*coros)
