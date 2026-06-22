from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

import services.reasoning.retrieval.assembler as assembler
from services.reasoning.retrieval.assembler import AccessContext, assemble_context
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


class _FakeAcquire:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(self, *_args: object) -> bool:
        return False


class _FakePool:
    def __init__(self) -> None:
        self.acquire_count = 0

    def get_max_size(self) -> int:
        return 4

    def acquire(self) -> _FakeAcquire:
        self.acquire_count += 1
        return _FakeAcquire(_FakeConn())


class _FakeConn:
    def __init__(self, *, in_transaction: bool = False) -> None:
        self._in_transaction = in_transaction

    def is_in_transaction(self) -> bool:
        return self._in_transaction


def _empty_model_selection() -> dict[str, object]:
    return {
        "models": [],
        "visible_models": [],
        "redactions": 0,
        "redaction_reasons": {},
        "cross_tenant_redactions": 0,
        "mmr": {"used": False},
    }


def _result() -> RetrievalResult:
    return RetrievalResult(
        trigger=TriggerContext(
            kind="T1",
            tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        )
    )


@pytest.mark.asyncio
async def test_assemble_context_overlaps_independent_db_facets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    models_started = asyncio.Event()
    resources_started = asyncio.Event()
    customer_started = asyncio.Event()

    async def fake_models(*_args: object, **_kwargs: object):
        models_started.set()
        await resources_started.wait()
        await customer_started.wait()
        return _empty_model_selection()

    async def fake_resources(*_args: object, **_kwargs: object):
        resources_started.set()
        await models_started.wait()
        return []

    async def fake_customer(*_args: object, **_kwargs: object):
        customer_started.set()
        await models_started.wait()
        return None

    monkeypatch.setattr(assembler, "_select_context_models", fake_models)
    monkeypatch.setattr(assembler, "_select_context_resources", fake_resources)
    monkeypatch.setattr(assembler, "_compute_customer_context", fake_customer)

    bundle = await asyncio.wait_for(
        assemble_context(
            _result(),
            AccessContext(tenant_id=UUID("00000000-0000-0000-0000-000000000001")),
            _FakeConn(),
            read_pool=_FakePool(),
        ),
        timeout=1.0,
    )

    assert bundle.models == []
    assert bundle.resources_summary == []


@pytest.mark.asyncio
async def test_assemble_context_disables_read_fanout_in_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _FakePool()

    async def fake_models(*_args: object, **_kwargs: object):
        return _empty_model_selection()

    async def fake_resources(*_args: object, **_kwargs: object):
        return []

    async def fake_customer(*_args: object, **_kwargs: object):
        return None

    monkeypatch.setattr(assembler, "_select_context_models", fake_models)
    monkeypatch.setattr(assembler, "_select_context_resources", fake_resources)
    monkeypatch.setattr(assembler, "_compute_customer_context", fake_customer)

    await assemble_context(
        _result(),
        AccessContext(tenant_id=UUID("00000000-0000-0000-0000-000000000001")),
        _FakeConn(in_transaction=True),
        read_pool=pool,
    )

    assert pool.acquire_count == 0
