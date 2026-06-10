from __future__ import annotations

from uuid import uuid4

import pytest

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think import reason as reason_mod
from services.reasoning.think.reason import ThinkRunOutcome, think


class _FakeTransaction:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn

    async def __aenter__(self) -> None:
        self.conn.transaction_enters += 1
        self.conn.in_transaction = True

    async def __aexit__(self, exc_type, exc, tb) -> None:
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

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


def _trigger() -> TriggerContext:
    obs_id = uuid4()
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        subkind="event_arrival",
        observation_id=obs_id,
        seed_natural_text="new customer risk signal",
        seed_signature={"trigger_id": str(obs_id)},
    )


@pytest.mark.asyncio
async def test_inferential_think_enters_run_once_without_wide_transaction(
    monkeypatch,
):
    conn = _FakeConn()
    trigger = _trigger()
    saw_in_transaction: list[bool] = []

    async def fake_run_once(**kwargs):
        saw_in_transaction.append(kwargs["conn"].is_in_transaction())
        record = kwargs["record"]
        return ThinkRunOutcome(
            run_id=record.id,
            trigger_id=record.trigger_id,
            trigger_kind=record.trigger_kind,
            status="success",
        )

    async def fake_record_cost(*args, **kwargs):
        return None

    monkeypatch.setattr(reason_mod, "is_authoritative", lambda _trigger: False)
    monkeypatch.setattr(reason_mod, "_run_once", fake_run_once)
    monkeypatch.setattr(reason_mod, "_record_cost_for_outcome", fake_record_cost)

    outcome = await think(trigger, _FakePool(conn))  # type: ignore[arg-type]

    assert outcome.succeeded
    assert saw_in_transaction == [False]
    assert conn.transaction_enters == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("authoritative", "narrow_env"),
    [(True, "1"), (False, "0")],
)
async def test_authoritative_and_disabled_narrow_mode_keep_wide_transaction(
    monkeypatch,
    authoritative: bool,
    narrow_env: str,
):
    conn = _FakeConn()
    trigger = _trigger()
    saw_in_transaction: list[bool] = []

    async def fake_run_once(**kwargs):
        saw_in_transaction.append(kwargs["conn"].is_in_transaction())
        record = kwargs["record"]
        return ThinkRunOutcome(
            run_id=record.id,
            trigger_id=record.trigger_id,
            trigger_kind=record.trigger_kind,
            status="success",
        )

    async def fake_record_cost(*args, **kwargs):
        return None

    monkeypatch.setenv("THINK_NARROW_INFERENTIAL_TX", narrow_env)
    monkeypatch.setattr(
        reason_mod, "is_authoritative", lambda _trigger: authoritative
    )
    monkeypatch.setattr(reason_mod, "_run_once", fake_run_once)
    monkeypatch.setattr(reason_mod, "_record_cost_for_outcome", fake_record_cost)

    outcome = await think(trigger, _FakePool(conn))  # type: ignore[arg-type]

    assert outcome.succeeded
    assert saw_in_transaction == [True]
    assert conn.transaction_enters == 1
