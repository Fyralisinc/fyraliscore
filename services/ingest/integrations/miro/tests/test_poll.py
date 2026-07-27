"""Miro poll ingress and exact-installation tests."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import orjson
import pytest

from services.ingest.integrations.miro import poll
from services.ingest.synthetic.live_generators.miro_poll import MiroPollGenerator


_TENANT_ID = UUID("00000000-0000-4000-8000-000000000123")


def _deps(**overrides: Any) -> poll.PollDeps:
    values: dict[str, Any] = {
        "pool": object(),
        "tenant_id": _TENANT_ID,
        "installation_id": "miro-installation",
        "org_id": "miro-org",
        "board_id": "miro-board",
    }
    values.update(overrides)
    return poll.PollDeps(**values)


def test_build_change_record_preserves_provider_item_and_scope() -> None:
    item = {
        "id": "item-1",
        "type": "sticky_note",
        "data": {"content": "contract-owned validation"},
    }

    record = poll.build_change_record(
        item,
        org_id="miro-org",
        board_id="miro-board",
    )

    assert record == {
        "_fyralis_record_type": "item",
        "_fyralis_board_id": "miro-board",
        "_fyralis_org_id": "miro-org",
        "item": item,
    }
    assert poll.build_change_record(
        {"type": "sticky_note"},
        org_id="miro-org",
        board_id="miro-board",
    ) is None


@pytest.mark.asyncio
async def test_handle_polled_change_uses_kafka_first_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    flags = AsyncMock()
    flags.kafka_path_enabled.return_value = True
    shadow_write = AsyncMock()
    inline_ingest = AsyncMock()
    monkeypatch.setattr(poll, "shadow_write_raw", shadow_write)
    monkeypatch.setattr(poll, "ingest", inline_ingest)

    await poll.handle_polled_change(
        {"id": "item-1", "type": "sticky_note"},
        _deps(
            tenant_flags=flags,
            kafka_producer=object(),
            s3_raw_client=object(),
        ),
    )

    flags.kafka_path_enabled.assert_awaited_once_with(_TENANT_ID)
    shadow_write.assert_awaited_once()
    kwargs = shadow_write.await_args.kwargs
    assert kwargs["source"] == "miro"
    assert kwargs["ingress_kind"] == "poll"
    assert kwargs["tenant_id"] == _TENANT_ID
    assert orjson.loads(kwargs["raw_body"])["item"]["id"] == "item-1"
    inline_ingest.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_polled_change_falls_back_inline_after_cutover_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    flags = AsyncMock()
    flags.kafka_path_enabled.return_value = True
    monkeypatch.setattr(
        poll,
        "shadow_write_raw",
        AsyncMock(side_effect=RuntimeError("provider lab kafka unavailable")),
    )
    inline_ingest = AsyncMock()
    monkeypatch.setattr(poll, "ingest", inline_ingest)
    deps = _deps(
        tenant_flags=flags,
        kafka_producer=object(),
        s3_raw_client=object(),
        actor_repo=object(),
        alias_repo=object(),
        embedder=object(),
    )

    await poll.handle_polled_change(
        {"id": "item-2", "type": "shape"},
        deps,
    )

    inline_ingest.assert_awaited_once()
    args = inline_ingest.await_args.args
    kwargs = inline_ingest.await_args.kwargs
    assert args[0] == poll.CHANNEL
    assert args[1]["item"]["id"] == "item-2"
    assert kwargs == {
        "pool": deps.pool,
        "tenant_id": _TENANT_ID,
        "actor_repo": deps.actor_repo,
        "alias_repo": deps.alias_repo,
        "embedder": deps.embedder,
    }


class _InstallationPool:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetches.append((query, args))
        return self.rows


@pytest.mark.asyncio
async def test_generator_resolves_one_exact_tenant_org_installation() -> None:
    pool = _InstallationPool([{"id": "exact-installation"}])
    generator = MiroPollGenerator(pool=pool)  # type: ignore[arg-type]

    first = await generator._installation_id(_TENANT_ID, "miro-org")
    second = await generator._installation_id(_TENANT_ID, "miro-org")

    assert first == second == "exact-installation"
    assert len(pool.fetches) == 1
    query, args = pool.fetches[0]
    assert "tenant_id = $1 AND org_id = $2" in query
    assert "LIMIT 1" not in query
    assert args == (_TENANT_ID, "miro-org")


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [{"id": "one"}, {"id": "two"}]])
async def test_generator_rejects_ambiguous_installation_scope(
    rows: list[dict[str, Any]],
) -> None:
    generator = MiroPollGenerator(  # type: ignore[arg-type]
        pool=_InstallationPool(rows),
    )

    with pytest.raises(ValueError, match="exactly one active installation"):
        await generator._installation_id(_TENANT_ID, "miro-org")
