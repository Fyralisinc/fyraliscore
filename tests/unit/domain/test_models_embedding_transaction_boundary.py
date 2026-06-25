from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from lib.embeddings.ollama import EMBEDDING_DIM
from lib.shared.errors import ValidationError
from lib.shared.types import ModelCreate
from services.domain.models.repo import ModelsRepo


class _FakeTransaction:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn

    async def __aenter__(self) -> None:
        self.conn.in_transaction = True
        self.conn.transaction_enters += 1

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.conn.in_transaction = False


class _FakeConn:
    def __init__(self) -> None:
        self.in_transaction = False
        self.transaction_enters = 0

    def is_in_transaction(self) -> bool:
        return self.in_transaction

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self.conn

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


class _RecordingEmbedder:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn
        self.calls: list[bool] = []

    async def embed(self, _text: str) -> list[float]:
        self.calls.append(self.conn.is_in_transaction())
        return [0.25] * EMBEDDING_DIM


def _model_without_embedding() -> ModelCreate:
    return ModelCreate(
        tenant_id=uuid4(),
        born_from_event_id=uuid4(),
        proposition={
            "kind": "belief",
            "claim_role": "fact",
            "subject": "Atlas renewal risk",
            "assertion": "is rising",
            "summary": "Atlas renewal risk is rising",
        },
        natural="Atlas renewal risk is rising",
        embedding=[],
        scope_temporal={"valid_from": "2026-01-01T00:00:00Z", "valid_until": None},
        confidence=0.6,
        confidence_at_assertion=0.6,
    )


@pytest.mark.asyncio
async def test_repo_owned_insert_precomputes_embedding_before_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConn()
    embedder = _RecordingEmbedder(conn)
    repo = ModelsRepo(_FakePool(conn), embedder=embedder)  # type: ignore[arg-type]

    async def fake_insert_core(
        self: ModelsRepo,
        core_conn: _FakeConn,
        proposed: ModelCreate,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        assert core_conn.is_in_transaction()
        assert len(proposed.embedding) == EMBEDDING_DIM
        return "inserted"

    monkeypatch.setattr(ModelsRepo, "_insert_core", fake_insert_core)

    result = await repo.insert(_model_without_embedding())

    assert result == "inserted"
    assert embedder.calls == [False]
    assert conn.transaction_enters == 1


@pytest.mark.asyncio
async def test_transactional_insert_refuses_to_call_embedder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConn()
    embedder = _RecordingEmbedder(conn)
    repo = ModelsRepo(None, embedder=embedder)

    async def fail_insert_core(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("_insert_core should not run")

    monkeypatch.setattr(ModelsRepo, "_insert_core", fail_insert_core)

    async with conn.transaction():
        with pytest.raises(ValidationError, match="precomputed"):
            await repo.insert(_model_without_embedding(), conn=conn)  # type: ignore[arg-type]

    assert embedder.calls == []
