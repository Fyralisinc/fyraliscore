from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

import services.reasoning.retrieval.second_pass as second_pass
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.retrieval.second_pass import second_pass_expand


class _FakeAcquire:
    def __init__(self, pool: _FakePool) -> None:
        self._pool = pool

    async def __aenter__(self) -> object:
        self._pool.current += 1
        self._pool.peak = max(self._pool.peak, self._pool.current)
        return _FakeConn()

    async def __aexit__(self, *_args: object) -> bool:
        self._pool.current -= 1
        return False


class _FakePool:
    def __init__(self, max_size: int = 4) -> None:
        self.max_size = max_size
        self.current = 0
        self.peak = 0

    def get_max_size(self) -> int:
        return self.max_size

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self)


class _FakeConn:
    def is_in_transaction(self) -> bool:
        return False


def _result() -> RetrievalResult:
    return RetrievalResult(
        trigger=TriggerContext(
            kind="T1",
            tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        )
    )


@pytest.mark.asyncio
async def test_second_pass_dimensions_fan_out_and_preserve_processed_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependency_started = asyncio.Event()
    support_started = asyncio.Event()
    events: list[str] = []

    async def fake_dependency(*_args: object, **_kwargs: object) -> int:
        events.append("dependency_started")
        dependency_started.set()
        await support_started.wait()
        return 2

    async def fake_support(*_args: object, **_kwargs: object) -> None:
        await dependency_started.wait()
        events.append("support_started")
        support_started.set()

    async def fake_adjacent(*_args: object, **_kwargs: object) -> None:
        events.append("adjacent_started")

    monkeypatch.setattr(
        second_pass,
        "_expand_dependency_context",
        fake_dependency,
    )
    monkeypatch.setattr(
        second_pass,
        "_expand_supporting_evidence",
        fake_support,
    )
    monkeypatch.setattr(
        second_pass,
        "_expand_adjacent_commitments",
        fake_adjacent,
    )

    pool = _FakePool(max_size=2)
    expanded = await asyncio.wait_for(
        second_pass_expand(
            _result(),
            [
                "dependency_context",
                "supporting_evidence",
                "adjacent_commitments",
            ],
            _FakeConn(),
            read_pool=pool,
        ),
        timeout=1.0,
    )

    assert events[:2] == ["dependency_started", "support_started"]
    assert expanded.notes["second_pass"]["dimensions_processed"] == [
        "dependency_context",
        "supporting_evidence",
        "adjacent_commitments",
    ]
    assert expanded.notes["second_pass"]["hops_used"] == {
        "dependency_context": 2,
        "supporting_evidence": 1,
        "adjacent_commitments": 1,
    }
    assert expanded.notes["second_pass"]["read_fanout_budget"] == {
        "max_concurrency": 2,
        "peak_in_use": 2,
        "acquired": 3,
        "denied": 0,
    }
    assert pool.peak == 2
