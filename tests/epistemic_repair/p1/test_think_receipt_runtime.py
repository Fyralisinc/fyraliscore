"""Runtime wiring tests for task-local, durable Think LLM receipts."""

from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from pydantic import BaseModel

from lib.llm.provider import LLMConfig, LLMProvider
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think import reason
from services.reasoning.think.llm_receipts import ReceiptIntegrityError
from services.reasoning.think.observability import ThinkRunRecord


class _Answer(BaseModel):
    answer: str


class _ScriptedProvider(LLMProvider):
    def __init__(self, responses: list[str | BaseException]):
        super().__init__(
            LLMConfig(provider="test", model="test", api_key="test", max_retries=0)
        )
        self._responses = iter(responses)

    async def _raw_call(self, **_: object) -> str:
        response = next(self._responses)
        if isinstance(response, BaseException):
            raise response
        return response


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: object):
        return False


class _RollbackTransaction:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.snapshot = list(self.conn.domain_effects)
        return self

    async def __aexit__(self, exc_type, *_: object):
        if exc_type is not None:
            self.conn.domain_effects[:] = self.snapshot
        return False


class _Connection:
    def __init__(self, *, reject: bool = False):
        self.reject = reject
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.domain_effects: list[str] = []

    def transaction(self):
        return _Transaction()

    async def fetchval(self, sql: str, *args: object):
        self.calls.append((sql, args))
        return None if self.reject else args[1]


class _RollbackConnection(_Connection):
    def transaction(self):
        return _RollbackTransaction(self)


class _Pool:
    def __init__(self, conn: _Connection):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def _trigger(*, batch_id: str = "batch-45") -> TriggerContext:
    return TriggerContext(
        kind="T4",
        tenant_id=uuid4(),
        subkind="test",
        seed_signature={"batch_id": batch_id},
    )


def _install_run_shell(monkeypatch: pytest.MonkeyPatch, trigger: TriggerContext):
    trigger_id = uuid4()
    run_id = uuid4()
    record = ThinkRunRecord(
        id=run_id,
        tenant_id=trigger.tenant_id,
        trigger_id=trigger_id,
        trigger_kind="T4",
    )
    monkeypatch.setattr(
        reason,
        "_start_think_record",
        lambda *_args, **_kwargs: (trigger_id, "T4", record),
    )

    async def _not_skipped(*_args, **_kwargs):
        return None

    monkeypatch.setattr(reason, "_try_early_idempotency_skip", _not_skipped)
    monkeypatch.setattr(reason, "_install_usage_aggregator", lambda _provider: (None, None))
    monkeypatch.setattr(reason, "_detach_usage_and_trace", lambda *_args: None)
    return trigger_id, run_id


@pytest.mark.asyncio
async def test_success_receipts_are_scoped_enriched_and_persisted(monkeypatch):
    trigger = _trigger()
    trigger_id, run_id = _install_run_shell(monkeypatch, trigger)
    provider = _ScriptedProvider(['{"answer":"yes"}'])
    conn = _Connection()

    async def _execute(*_args, **_kwargs):
        await provider.structured(system="system", user="user", schema=_Answer)
        return reason.ThinkRunOutcome(
            run_id=run_id,
            trigger_id=trigger_id,
            trigger_kind="T4",
            status="success",
        )

    monkeypatch.setattr(reason, "_execute_think_run", _execute)

    outcome = await reason.think(trigger, _Pool(conn), llm_provider=provider)

    assert outcome.status == "success"
    assert len(conn.calls) == 2
    logical_args = conn.calls[0][1]
    assert logical_args[0] == trigger.tenant_id
    assert logical_args[2] == trigger_id
    assert logical_args[3] == run_id
    assert logical_args[4] == "batch-45"
    assert logical_args[15:17] == ("accepted", "applied")
    assert provider._receipt_sink is None


@pytest.mark.asyncio
async def test_failed_provider_attempt_is_still_persisted(monkeypatch):
    trigger = _trigger(batch_id="failure-batch")
    trigger_id, run_id = _install_run_shell(monkeypatch, trigger)
    provider = _ScriptedProvider([RuntimeError("provider unavailable")])
    conn = _Connection()

    async def _execute(*_args, **_kwargs):
        failure = None
        try:
            await provider.structured(system="system", user="user", schema=_Answer)
        except RuntimeError as exc:
            failure = exc
        return reason.ThinkRunOutcome(
            run_id=run_id,
            trigger_id=trigger_id,
            trigger_kind="T4",
            status="failed",
            error="provider unavailable",
            exception=failure,
        )

    monkeypatch.setattr(reason, "_execute_think_run", _execute)

    outcome = await reason.think(trigger, _Pool(conn), llm_provider=provider)

    assert outcome.status == "failed"
    assert len(conn.calls) == 2
    assert conn.calls[0][1][13] == "provider_error"
    assert conn.calls[0][1][15:17] == ("RuntimeError", "not_applied")
    assert conn.calls[1][1][10] == "provider_error"


@pytest.mark.asyncio
async def test_receipt_integrity_failure_prevents_clean_success(monkeypatch):
    trigger = _trigger()
    trigger_id, run_id = _install_run_shell(monkeypatch, trigger)
    provider = _ScriptedProvider(['{"answer":"yes"}'])

    async def _execute(*_args, **_kwargs):
        await provider.structured(system="system", user="user", schema=_Answer)
        return reason.ThinkRunOutcome(
            run_id=run_id,
            trigger_id=trigger_id,
            trigger_kind="T4",
            status="success",
        )

    monkeypatch.setattr(reason, "_execute_think_run", _execute)

    with pytest.raises(ReceiptIntegrityError, match="logical receipt conflict"):
        await reason.think(trigger, _Pool(_Connection(reject=True)), llm_provider=provider)

    assert provider._receipt_sink is None


@pytest.mark.asyncio
async def test_receipts_survive_an_orchestration_exception(monkeypatch):
    trigger = _trigger()
    _trigger_id, _run_id = _install_run_shell(monkeypatch, trigger)
    provider = _ScriptedProvider([RuntimeError("wire failure")])
    conn = _Connection()

    async def _execute(*_args, **_kwargs):
        try:
            await provider.structured(system="system", user="user", schema=_Answer)
        finally:
            raise LookupError("failure handler also failed")

    monkeypatch.setattr(reason, "_execute_think_run", _execute)

    with pytest.raises(LookupError, match="failure handler also failed"):
        await reason.think(trigger, _Pool(conn), llm_provider=provider)

    assert len(conn.calls) == 2
    assert conn.calls[0][1][15:17] == ("LookupError", "orchestration_failed")
    assert conn.calls[1][1][10] == "provider_error"


def test_batch_identity_prefers_explicit_then_uses_batched_trigger_identity():
    trigger_id = uuid4()
    explicit = _trigger(batch_id="explicit")
    assert reason._receipt_batch_id(explicit, trigger_id) == "explicit"

    batched = TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        observation_ids=[uuid4(), uuid4()],
    )
    assert reason._receipt_batch_id(batched, trigger_id) == str(trigger_id)
    assert reason._receipt_batch_id(
        TriggerContext(kind="T1", tenant_id=uuid4(), observation_id=uuid4()),
        trigger_id,
    ) is None


@pytest.mark.asyncio
async def test_receipt_failure_rolls_back_the_surrounding_semantic_effects():
    trigger = _trigger()
    provider = _ScriptedProvider(['{"answer":"yes"}'])
    collector = reason._new_receipt_collector(
        trigger,
        trigger_id=uuid4(),
        run_id=uuid4(),
        llm_provider=provider,
    )
    assert collector is not None
    with collector.capture():
        await provider.structured(system="system", user="user", schema=_Answer)
    outcome = reason.ThinkRunOutcome(
        run_id=collector.think_run_id,
        trigger_id=collector.trigger_id,
        trigger_kind="T4",
        status="success",
    )
    conn = _RollbackConnection(reject=True)

    with pytest.raises(ReceiptIntegrityError):
        async with conn.transaction():
            conn.domain_effects.append("canonical-model-write")
            await reason._persist_receipts_in_semantic_transaction(
                conn, collector, outcome
            )

    assert conn.domain_effects == []
