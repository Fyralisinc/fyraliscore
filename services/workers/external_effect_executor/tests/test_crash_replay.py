from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from lib.contracts.execution import (
    ActionAdapterCapabilities,
    ExternalEffectState,
)
from lib.shared.ids import uuid7
from services.domain.effect_execution import EffectExecutionRepo
from services.domain.effect_execution.tests import test_repo as repo_test_support
from services.workers.external_effect_executor.adapters import (
    ActionAdapterRequest,
    ActionDispatchFate,
    ActionDispatchResult,
    ActionPreflightResult,
    StaticActionAdapterRegistry,
)
from services.workers.external_effect_executor.worker import (
    ExternalEffectExecutorWorker,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@dataclass(slots=True)
class _RecordingAdapter:
    result: ActionDispatchResult
    adapter_name: str = "slack-message-delivery"
    provider_name: str = "slack"
    preflight_requests: list[ActionAdapterRequest] = field(default_factory=list)
    dispatch_requests: list[ActionAdapterRequest] = field(default_factory=list)

    async def preflight(
        self,
        request: ActionAdapterRequest,
    ) -> ActionPreflightResult:
        self.preflight_requests.append(request)
        return ActionPreflightResult(
            evidence_refs=(
                "provider-preflight:target-exists",
                "provider-preflight:preconditions-live",
            )
        )

    async def dispatch(
        self,
        request: ActionAdapterRequest,
    ) -> ActionDispatchResult:
        self.dispatch_requests.append(request)
        return self.result


@dataclass(frozen=True, slots=True)
class _CrashState:
    tenant_id: UUID
    work_item_id: UUID
    obligation_id: UUID
    effect_attempt_id: UUID
    provider_idempotency_key: str


def _worker(
    pool: asyncpg.Pool,
    *,
    worker_id: str,
    adapter: _RecordingAdapter,
) -> ExternalEffectExecutorWorker:
    return ExternalEffectExecutorWorker(
        pool=pool,
        worker_id=worker_id,
        adapter_registry=StaticActionAdapterRegistry((adapter,)),
        lease_duration=timedelta(minutes=1),
        retry_delay=timedelta(seconds=5),
        max_attempts=3,
    )


async def _leased_work(
    pool: asyncpg.Pool,
    *,
    reconciliation_only: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[UUID, UUID]:
    tenant_id = uuid7()
    start = datetime.now(timezone.utc) - timedelta(minutes=15)
    async with pool.acquire() as conn, conn.transaction():
        if reconciliation_only:
            original = repo_test_support.ActionAdapterCapabilities

            def capability_factory(**kwargs: Any) -> ActionAdapterCapabilities:
                kwargs.update(
                    idempotency_supported=False,
                    idempotency_scope=None,
                    idempotency_retention_until=None,
                    reconciliation_supported=True,
                    reconciliation_consistency_window_seconds=30,
                )
                return original(**kwargs)

            with monkeypatch.context() as patch:
                patch.setattr(
                    repo_test_support,
                    "ActionAdapterCapabilities",
                    capability_factory,
                )
                event_id, _capabilities = (
                    await repo_test_support._leased_work_fixture(
                        conn,
                        tenant_id=tenant_id,
                        start=start,
                    )
                )
        else:
            event_id, _capabilities = await repo_test_support._leased_work_fixture(
                conn,
                tenant_id=tenant_id,
                start=start,
            )
    return tenant_id, event_id


async def _seed_crash_after_dispatch_intent(
    pool: asyncpg.Pool,
    *,
    reconciliation_only: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> _CrashState:
    tenant_id, event_id = await _leased_work(
        pool,
        reconciliation_only=reconciliation_only,
        monkeypatch=monkeypatch,
    )
    repo = EffectExecutionRepo()
    discovery_at = datetime.now(timezone.utc) - timedelta(minutes=2)
    async with pool.acquire() as conn, conn.transaction():
        item = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=discovery_at,
        )
        assert item is not None
        (claimed,) = await repo.claim_ready_work(
            conn,
            worker_id="effect-executor:crashed",
            now=discovery_at,
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
        assert claimed.claim_token is not None
        context = await repo.load_claimed_context(
            conn,
            tenant_id=tenant_id,
            work_item_id=claimed.id,
            worker_id="effect-executor:crashed",
            claim_token=claimed.claim_token,
            now=discovery_at + timedelta(seconds=1),
        )
        attempt = await repo_test_support._reserve_effect(
            conn,
            tenant_id=tenant_id,
            context=context,
        )
        await repo_test_support._transition_effect(
            conn,
            tenant_id=tenant_id,
            attempt=attempt,
            expected_version=1,
            from_state=ExternalEffectState.RESERVED,
            to_state=ExternalEffectState.DISPATCH_INTENT_RECORDED,
            at=discovery_at + timedelta(seconds=2),
        )
    return _CrashState(
        tenant_id=tenant_id,
        work_item_id=item.id,
        obligation_id=item.plan.obligation_id,
        effect_attempt_id=item.plan.effect_attempt_id,
        provider_idempotency_key=item.plan.provider_idempotency_key,
    )


async def _states(
    pool: asyncpg.Pool,
    *,
    crash: _CrashState,
) -> tuple[str, str, str]:
    async with pool.acquire() as conn:
        queue_state = await conn.fetchval(
            """
            SELECT status
            FROM leased_work_effect_execution_items
            WHERE tenant_id=$1 AND id=$2
            """,
            crash.tenant_id,
            crash.work_item_id,
        )
        effect_state = await conn.fetchval(
            """
            SELECT current_state
            FROM external_effect_attempt_heads
            WHERE tenant_id=$1 AND effect_attempt_id=$2
            """,
            crash.tenant_id,
            crash.effect_attempt_id,
        )
        work_state = await conn.fetchval(
            """
            SELECT current_state
            FROM work_obligation_heads
            WHERE tenant_id=$1 AND obligation_id=$2
            """,
            crash.tenant_id,
            crash.obligation_id,
        )
    return str(queue_state), str(effect_state), str(work_state)


async def test_idempotent_reclaimed_dispatch_uses_same_key_and_replay_is_noop(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crash = await _seed_crash_after_dispatch_intent(
        fresh_db,
        reconciliation_only=False,
        monkeypatch=monkeypatch,
    )
    adapter = _RecordingAdapter(
        result=ActionDispatchResult(
            fate=ActionDispatchFate.SUCCEEDED,
            reason="provider persisted the exact idempotent request",
            provider_observation_refs=("provider:accepted",),
            external_state_evidence_refs=("provider-message:1717.001",),
        )
    )
    worker = _worker(
        fresh_db,
        worker_id="effect-executor:recovered-idempotent",
        adapter=adapter,
    )

    assert await worker.process_batch(limit=10) == 1
    assert len(adapter.preflight_requests) == 1
    assert len(adapter.dispatch_requests) == 1
    assert (
        adapter.dispatch_requests[0].provider_idempotency_key
        == crash.provider_idempotency_key
    )
    assert await _states(fresh_db, crash=crash) == (
        "dispatched",
        "succeeded",
        "completed",
    )

    assert await worker.process_batch(limit=10) == 0
    assert len(adapter.dispatch_requests) == 1


async def test_reconciliation_only_reclaimed_dispatch_is_not_redispatched(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crash = await _seed_crash_after_dispatch_intent(
        fresh_db,
        reconciliation_only=True,
        monkeypatch=monkeypatch,
    )
    adapter = _RecordingAdapter(
        result=ActionDispatchResult(
            fate=ActionDispatchFate.SUCCEEDED,
            reason="must never be used for a blind reconciliation-only replay",
            provider_observation_refs=("provider:unexpected-call",),
            external_state_evidence_refs=("provider-message:unexpected",),
        )
    )
    worker = _worker(
        fresh_db,
        worker_id="effect-executor:recovered-reconciliation-only",
        adapter=adapter,
    )

    assert await worker.process_batch(limit=10) == 1
    assert len(adapter.preflight_requests) == 1
    assert adapter.dispatch_requests == []
    assert await _states(fresh_db, crash=crash) == (
        "unknown",
        "unknown",
        "reconciliation_required",
    )

    assert await worker.process_batch(limit=10) == 0
    assert adapter.dispatch_requests == []


async def test_provider_unknown_routes_effect_and_work_to_reconciliation(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id, _event_id = await _leased_work(
        fresh_db,
        reconciliation_only=False,
        monkeypatch=monkeypatch,
    )
    adapter = _RecordingAdapter(
        result=ActionDispatchResult(
            fate=ActionDispatchFate.UNKNOWN,
            reason="provider timed out after accepting the request body",
            provider_observation_refs=("provider:timeout-after-send",),
        )
    )
    worker = _worker(
        fresh_db,
        worker_id="effect-executor:provider-unknown",
        adapter=adapter,
    )

    assert await worker.process_batch(limit=10) == 1
    assert len(adapter.dispatch_requests) == 1
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT q.id, q.obligation_id, q.effect_attempt_id, q.status,
                   e.current_state AS effect_state,
                   w.current_state AS work_state
            FROM leased_work_effect_execution_items q
            JOIN external_effect_attempt_heads e
              ON e.tenant_id=q.tenant_id
             AND e.effect_attempt_id=q.effect_attempt_id
            JOIN work_obligation_heads w
              ON w.tenant_id=q.tenant_id
             AND w.obligation_id=q.obligation_id
            WHERE q.tenant_id=$1
            """,
            tenant_id,
        )
    assert row is not None
    assert row["status"] == "unknown"
    assert row["effect_state"] == "unknown"
    assert row["work_state"] == "reconciliation_required"

    assert await worker.process_batch(limit=10) == 0
    assert len(adapter.dispatch_requests) == 1
