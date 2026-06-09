"""Self-verifying synthetic Grafana backfill test (IN-GRAFANA, X2/X3 infra).

Drives the REAL `fetch_page_grafana` fetcher against `MockGrafanaClient` (a
fixture from `make_grafana`) through the `_open_grafana_client` seam, then runs
EVERY emitted record through the REAL `grafana:annotation` handler. No database /
network — the mock + fixture are the only test doubles; the fetcher, cursor
logic, 1:1 fan-out, and handler are all production code.

Asserted invariants:
  - fan-out count == annotations (Grafana annotations are 1:1, no sub-fan-out),
  - every record yields a draft with a non-null external_id + an occurred_at in
    2026 (the observations partition window),
  - backward pagination: annotations > per_page triggers a multi-page walk yet
    still yields every annotation exactly once,
  - faults: a rate-limit FaultProfile surfaces GrafanaApiError and the fetcher's
    rate-limit fallback returns an empty, non-terminal page (end_of_data False).

The 90-day backfill window floor is disabled (GRAFANA_BACKFILL_WINDOW_DAYS=0) so
the 2026-01 fixture annotations are not filtered out by a wall-clock floor.
"""
from __future__ import annotations

import asyncio

import pytest

from lib.shared.errors import GrafanaApiError
from services.ingest.ingestion.fetchers import grafana as grafana_fetcher
from services.ingest.ingestion.fetchers.grafana import fetch_page_grafana
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.synthetic.fault_profiles import FaultProfile, HAPPY_PATH
from services.ingest.synthetic.fixtures.grafana_generator import make_grafana
from services.ingest.synthetic.mock_clients.grafana import MockGrafanaClient


_BASE_MS = 1767571200000  # 2026-01-05T00:00:00Z
_BASE_URL = "https://acme.grafana.net"


@pytest.fixture(autouse=True)
def _all_time_window(monkeypatch):
    """Disable the wall-clock backfill floor so the 2026-01 fixture is in
    range regardless of when the suite runs."""
    monkeypatch.setenv("GRAFANA_BACKFILL_WINDOW_DAYS", "0")


# The fetcher reads `install["base_url"]` (via `_instance_of`) and the shard's
# `updated_cursor`. Model the install + shard as plain dicts (the fetcher only
# does dict-style access).
def _install(base_url: str = _BASE_URL) -> dict[str, object]:
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "base_url": base_url,
    }


def _shard(*, updated_cursor: int | None = None) -> dict[str, object]:
    return {
        "shard_kind": "grafana_org_annotations",
        "installation_id": "00000000-0000-0000-0000-000000000001",
        "base_url": _BASE_URL,
        "org_id": "1",
        "updated_cursor": updated_cursor,
    }


def _patch_client(monkeypatch, client: MockGrafanaClient) -> None:
    """Rebind the fetcher's `_open_grafana_client` seam to yield (mock, close)."""
    async def _open(_install):  # noqa: ANN001, ANN202
        async def _close() -> None:
            return None
        return client, _close

    monkeypatch.setattr(grafana_fetcher, "_open_grafana_client", _open)


async def _drive_backfill(
    install: dict[str, object], shard: dict[str, object],
) -> list[dict[str, object]]:
    """Run the real fetch loop to completion, collecting all records.
    Threads `next_cursor` back each iteration exactly like ShardFetch."""
    records: list[dict[str, object]] = []
    cursor: dict[str, object] | None = None
    for _ in range(1000):  # generous guard against a runaway loop
        result = await fetch_page_grafana(install, shard, cursor)
        records.extend(result.records)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    else:  # pragma: no cover - only on a genuine non-terminating fetcher bug
        raise AssertionError("fetch loop did not reach end_of_data")
    return records


def test_synthetic_grafana_backfill_drives_real_fetcher_and_handler(monkeypatch):
    annotations = 5
    fixture = make_grafana(
        annotations=annotations,
        base_ms=_BASE_MS,
        base_url=_BASE_URL,
    )

    client = MockGrafanaClient(fixture=fixture, profile=HAPPY_PATH)
    _patch_client(monkeypatch, client)

    records = asyncio.run(_drive_backfill(_install(), _shard()))

    # 1:1 fan-out: one record per annotation.
    assert len(records) == annotations == 5

    # Every record is tagged as an annotation + carries the instance host.
    for rec in records:
        assert rec.get("_fyralis_record_type") == "annotation"
        assert rec.get("_fyralis_instance") == "acme.grafana.net"

    # Drive each record through the REAL handler.
    channel = resolve_channel("grafana", "backfill")
    assert channel == "grafana:annotation"
    handler = get_handler(channel)

    drafts = asyncio.run(_gather([handler(dict(r), {}) for r in records]))

    assert len(drafts) == annotations
    external_ids = set()
    for draft in drafts:
        assert draft.external_id is not None and draft.external_id != ""
        assert draft.source_channel == "grafana:annotation"
        assert draft.occurred_at is not None
        assert draft.occurred_at.year == 2026
        external_ids.add(draft.external_id)
    # external_ids are unique across the fan-out (no accidental collapse).
    assert len(external_ids) == annotations

    # The fixture mixes plain annotations (signal) and alert-state annotations
    # (state_change); both kinds must appear.
    kinds = {d.kind for d in drafts}
    assert "signal" in kinds
    assert "state_change" in kinds


def test_synthetic_grafana_backward_pagination(monkeypatch):
    """annotations > per_page must trigger a multi-page backward walk yet still
    yield every annotation exactly once."""
    annotations = 7
    per_page = 3  # -> ceil(7/3) = 3 pages
    # The fetcher's page `limit` is driven by env (GRAFANA_ANNOTATIONS_PAGE_SIZE,
    # default 100), NOT the fixture; the mock caps at min(limit, per_page). For
    # the fetcher's `is_last = len(page) < limit` test to drive a multi-page
    # backward walk, the requested limit must equal the mock's per_page.
    monkeypatch.setenv("GRAFANA_ANNOTATIONS_PAGE_SIZE", str(per_page))
    fixture = make_grafana(
        annotations=annotations,
        base_ms=_BASE_MS,
        per_page=per_page,
        base_url=_BASE_URL,
    )

    # Count the actual list_annotations calls to prove multi-page paging.
    client = MockGrafanaClient(fixture=fixture, profile=HAPPY_PATH)
    call_count = {"n": 0}
    orig = client.list_annotations

    async def _counting(**kwargs):
        call_count["n"] += 1
        return await orig(**kwargs)

    client.list_annotations = _counting  # type: ignore[method-assign]
    _patch_client(monkeypatch, client)

    records = asyncio.run(_drive_backfill(_install(), _shard()))

    assert call_count["n"] >= 3  # multi-page backward walk happened
    assert len(records) == annotations

    # No duplicates: the backward walk must not re-emit a boundary annotation.
    ids = [r.get("id") for r in records]
    assert len(set(ids)) == annotations


def test_synthetic_grafana_rate_limit_fault(monkeypatch):
    """A rate-limit FaultProfile makes `list_annotations` raise
    GrafanaApiError(grafana_api_rate_limited); the fetcher catches it and ends
    the round empty WITHOUT advancing (end_of_data False)."""
    fixture = make_grafana(annotations=5, base_ms=_BASE_MS, base_url=_BASE_URL)

    # rate_limit_after_n_requests=0 -> the very first call raises.
    profile = FaultProfile(rate_limit_after_n_requests=0)

    # 1. Raw client surface raises the production error type + code.
    raw_client = MockGrafanaClient(fixture=fixture, profile=profile)
    with pytest.raises(GrafanaApiError) as exc_info:
        asyncio.run(raw_client.list_annotations(from_ms=None, to_ms=None))
    assert exc_info.value.code == "grafana_api_rate_limited"

    # 2. Through the fetcher: the rate-limit fallback returns an empty,
    #    non-terminal page (cursor unadvanced) so ShardFetch re-enters.
    fetch_client = MockGrafanaClient(fixture=fixture, profile=profile)
    _patch_client(monkeypatch, fetch_client)
    result = asyncio.run(fetch_page_grafana(_install(), _shard(), None))
    assert result.records == []
    assert result.end_of_data is False


def test_mock_grafana_implements_methods_called_by_fetcher_and_reconciler():
    """Surface check: the mock implements list_annotations + has_annotations_since
    (+ get_org) the production fetcher/reconciler/seed call."""
    import inspect

    client = MockGrafanaClient(fixture=make_grafana(annotations=1))
    for name in ("list_annotations", "has_annotations_since", "get_org"):
        assert hasattr(client, name)
        assert inspect.iscoroutinefunction(getattr(client, name))


# ---------------------------------------------------------------------
# Local async helper (avoids creating multiple event loops per record).
# ---------------------------------------------------------------------
async def _gather(coros):
    return await asyncio.gather(*coros)
