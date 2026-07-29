from __future__ import annotations

import datetime as dt
from collections.abc import Sequence
from typing import Any
from uuid import UUID

import httpx

from services.ingest.ingestion.workflows.credential_renewal_scheduler import (
    CredentialRenewalCandidate,
    CredentialRenewalScheduler,
    CredentialRenewalSchedulerConfig,
    credential_renewal_sources,
    credential_renewal_tick_interval_seconds,
    fair_credential_renewal_candidates,
    load_active_credential_renewal_candidates,
)
from services.ingest.integrations.bounded_renewal import RenewalOutcome
from services.ingest.source_contract.catalog import source_definition


def _uuid(value: int) -> UUID:
    return UUID(f"00000000-0000-0000-0000-{value:012d}")


def _now() -> dt.datetime:
    return dt.datetime(2026, 7, 29, 12, tzinfo=dt.timezone.utc)


def test_credential_renewal_subset_and_cadence_are_contract_derived() -> None:
    sources = credential_renewal_sources()

    assert len(sources) == 5
    assert all(source.renewal is not None for source in sources)
    assert all(source.renewal.kind == "credential" for source in sources)
    assert credential_renewal_tick_interval_seconds() == min(
        source.renewal.cadence_seconds
        for source in sources
        if source.renewal is not None
    )


def test_fair_candidate_selection_interleaves_tenants_and_oldest_installations() -> None:
    tenant_a = _uuid(1)
    tenant_b = _uuid(2)
    candidates = (
        CredentialRenewalCandidate(
            source_id="quickbooks",
            tenant_id=tenant_a,
            installation_id=_uuid(3),
            last_claimed_at=_now() - dt.timedelta(minutes=10),
        ),
        CredentialRenewalCandidate(
            source_id="quickbooks",
            tenant_id=tenant_a,
            installation_id=_uuid(1),
        ),
        CredentialRenewalCandidate(
            source_id="quickbooks",
            tenant_id=tenant_b,
            installation_id=_uuid(2),
        ),
    )

    selected = fair_credential_renewal_candidates(candidates, limit=3)

    # Unclaimed work wins first.  The second tenant gets one turn before the
    # first tenant's next-oldest candidate, rather than being starved by it.
    assert [candidate.installation_id for candidate in selected] == [
        _uuid(1),
        _uuid(2),
        _uuid(3),
    ]


def test_fair_candidate_selection_prioritizes_due_work_over_sampled_work() -> None:
    tenant_due = _uuid(4)
    tenant_sampled = _uuid(5)
    due = CredentialRenewalCandidate(
        source_id="quickbooks",
        tenant_id=tenant_due,
        installation_id=_uuid(6),
        last_claimed_at=_now(),
        durable_due=True,
    )
    sampled = CredentialRenewalCandidate(
        source_id="quickbooks",
        tenant_id=tenant_sampled,
        installation_id=_uuid(7),
    )

    selected = fair_credential_renewal_candidates((sampled, due), limit=1)

    # The sampled installation is otherwise a better fairness candidate
    # because it has never been claimed.  It still cannot displace durable
    # work that is due now.
    assert selected == (due,)


async def test_scheduler_invokes_contract_source_with_exact_identity_and_lease_inputs() -> None:
    tenant_a = _uuid(10)
    tenant_b = _uuid(11)
    installation_a = _uuid(12)
    installation_b = _uuid(13)
    source = source_definition("quickbooks")
    pool = object()
    invocations: list[Any] = []
    resolved_sources: list[str] = []

    async def load_candidates(
        loaded_pool: Any,
        sources: tuple[Any, ...],
        onboarding_scan_limit: int,
        due_limit: int,
        now: dt.datetime,
    ) -> Sequence[CredentialRenewalCandidate]:
        assert loaded_pool is pool
        assert sources == (source,)
        assert onboarding_scan_limit == 8
        assert due_limit == 2
        assert now == _now()
        return (
            CredentialRenewalCandidate(
                source_id="quickbooks",
                tenant_id=tenant_a,
                installation_id=installation_a,
            ),
            CredentialRenewalCandidate(
                source_id="quickbooks",
                tenant_id=tenant_b,
                installation_id=installation_b,
            ),
        )

    def resolve(source_id: str):
        resolved_sources.append(source_id)

        async def invoke(invocation: Any) -> RenewalOutcome:
            invocations.append(invocation)
            return RenewalOutcome(
                source_id=source_id,
                state="not_due",
                next_attempt_at=_now() + dt.timedelta(minutes=5),
            )

        return invoke

    http = httpx.AsyncClient()
    try:
        scheduler = CredentialRenewalScheduler(
            pool,
            secret_store=object(),
            http=http,
            config=CredentialRenewalSchedulerConfig(
                batch_size=2,
                candidate_scan_limit=8,
                max_concurrency=1,
                instance_name="unit-renewal",
            ),
            source_definitions=(source,),
            candidate_loader=load_candidates,
            invoker_resolver=resolve,
            clock=_now,
        )
        await scheduler.tick()
    finally:
        await http.aclose()

    assert resolved_sources == ["quickbooks", "quickbooks"]
    assert [(item.tenant_id, item.installation_id) for item in invocations] == [
        (tenant_a, installation_a),
        (tenant_b, installation_b),
    ]
    assert all(item.pool is pool for item in invocations)
    assert all(item.target_key == "installation" for item in invocations)
    assert all(item.lease_timeout_seconds == 60.0 for item in invocations)
    assert all(item.worker_id and item.worker_id.startswith("unit-renewal@") for item in invocations)
    assert scheduler.last_tick_result is not None
    assert scheduler.last_tick_result.selected_count == 2
    assert scheduler.last_tick_result.invocation_error_count == 0


async def test_scheduler_runs_due_work_before_sampled_onboarding_work() -> None:
    source = source_definition("quickbooks")
    due_tenant = _uuid(14)
    due_installation = _uuid(15)
    sampled_tenant = _uuid(16)
    sampled_installation = _uuid(17)
    invoked_installations: list[UUID] = []

    async def load_candidates(
        loaded_pool: Any,
        sources: tuple[Any, ...],
        onboarding_scan_limit: int,
        due_limit: int,
        now: dt.datetime,
    ) -> Sequence[CredentialRenewalCandidate]:
        assert loaded_pool is pool
        assert sources == (source,)
        assert onboarding_scan_limit == 1
        assert due_limit == 1
        assert now == _now()
        return (
            CredentialRenewalCandidate(
                source_id="quickbooks",
                tenant_id=sampled_tenant,
                installation_id=sampled_installation,
            ),
            CredentialRenewalCandidate(
                source_id="quickbooks",
                tenant_id=due_tenant,
                installation_id=due_installation,
                durable_due=True,
            ),
        )

    def resolve(source_id: str):
        assert source_id == "quickbooks"

        async def invoke(invocation: Any) -> RenewalOutcome:
            invoked_installations.append(invocation.installation_id)
            return RenewalOutcome(
                source_id=source_id,
                state="not_due",
                next_attempt_at=_now() + dt.timedelta(minutes=5),
            )

        return invoke

    pool = object()
    http = httpx.AsyncClient()
    try:
        scheduler = CredentialRenewalScheduler(
            pool,
            secret_store=object(),
            http=http,
            config=CredentialRenewalSchedulerConfig(
                batch_size=1,
                candidate_scan_limit=1,
                instance_name="unit-due-priority",
            ),
            source_definitions=(source,),
            candidate_loader=load_candidates,
            invoker_resolver=resolve,
            clock=_now,
        )
        await scheduler.tick()
    finally:
        await http.aclose()

    assert invoked_installations == [due_installation]


class _AsyncContext:
    def __init__(self, value: Any) -> None:
        self._value = value

    async def __aenter__(self) -> Any:
        return self._value

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None


class _CandidateConnection:
    def __init__(
        self,
        *,
        active_ids: dict[UUID, set[UUID]],
        claim_times: dict[tuple[UUID, UUID], dt.datetime],
        discovery_rows: Sequence[dict[str, object]],
        due_rows: Sequence[dict[str, object]] = (),
    ) -> None:
        self.active_ids = active_ids
        self.claim_times = claim_times
        self.discovery_rows = discovery_rows
        self.due_rows = due_rows
        self.current_tenant: UUID | None = None
        self.queries: list[str] = []

    def transaction(self) -> _AsyncContext:
        return _AsyncContext(self)

    async def execute(self, query: str, *args: object) -> str:
        self.queries.append(query)
        if "set_config('app.current_tenant'" in query:
            self.current_tenant = UUID(str(args[0]))
        return "SELECT 1"

    async def fetch(self, query: str, *args: object) -> Sequence[dict[str, object]]:
        self.queries.append(query)
        if "SELECT DISTINCT tenant_id" in query:
            return [
                {"tenant_id": tenant_id}
                for tenant_id in sorted(
                    {UUID(str(row["tenant_id"])) for row in self.due_rows},
                    key=str,
                )
            ]
        if (
            "FROM source_renewal_jobs" in query
            and "target_key = 'installation'" in query
            and "tenant_id = $1" in query
            and "state IN ('pending', 'retry_scheduled')" in query
        ):
            assert self.current_tenant == UUID(str(args[0]))
            assert "lease_expires_at <= $3" in query
            assert "reauthorization_required" not in query
            assert "manual_reconciliation_required" not in query
            now = args[2]
            assert isinstance(now, dt.datetime)
            due_rows = [
                row
                for row in self.due_rows
                if UUID(str(row["tenant_id"])) == self.current_tenant
                and _row_is_claimable_for_test(row, now)
            ]
            return sorted(
                due_rows,
                key=lambda row: (
                    row.get("last_claimed_at") is not None,
                    row.get("last_claimed_at") or dt.datetime.min.replace(
                        tzinfo=dt.timezone.utc,
                    ),
                    str(row["source_id"]),
                    str(row["installation_id"]),
                ),
            )[: int(args[3])]
        if "WITH installed AS MATERIALIZED" in query:
            return self.discovery_rows[: int(args[2])]
        if "FROM quickbooks_installations" in query:
            assert self.current_tenant is not None
            requested = set(args[1])
            return [
                {"id": installation_id}
                for installation_id in sorted(
                    self.active_ids.get(self.current_tenant, set()) & requested,
                    key=str,
                )
            ]
        if "FROM source_renewal_jobs" in query:
            assert self.current_tenant is not None
            return [
                {
                    "source_id": "quickbooks",
                    "installation_id": installation_id,
                    "last_claimed_at": claimed_at,
                }
                for (tenant_id, installation_id), claimed_at in self.claim_times.items()
                if tenant_id == self.current_tenant
            ]
        raise AssertionError(query)


class _CandidatePool:
    def __init__(self, connection: _CandidateConnection) -> None:
        self._connection = connection

    def acquire(self) -> _AsyncContext:
        return _AsyncContext(self._connection)


def _row_is_claimable_for_test(row: dict[str, object], now: dt.datetime) -> bool:
    """Small Provider-free model of the due-job SQL predicate above."""

    state = str(row.get("state", "pending"))
    if state in {"pending", "retry_scheduled"}:
        next_attempt_at = row.get("next_attempt_at", now - dt.timedelta(seconds=1))
        return isinstance(next_attempt_at, dt.datetime) and next_attempt_at <= now
    if state == "leased":
        lease_expires_at = row.get("lease_expires_at")
        return isinstance(lease_expires_at, dt.datetime) and lease_expires_at <= now
    return False


async def test_candidate_discovery_rechecks_active_installs_under_each_tenant() -> None:
    tenant_active = _uuid(20)
    tenant_disabled = _uuid(21)
    installation_active = _uuid(22)
    installation_disabled = _uuid(23)
    connection = _CandidateConnection(
        active_ids={tenant_active: {installation_active}},
        claim_times={
            (tenant_active, installation_active): _now() - dt.timedelta(minutes=1),
        },
        discovery_rows=(
            {
                "source": "quickbooks",
                "tenant_id": tenant_active,
                "installation_row_id": installation_active,
            },
            {
                "source": "quickbooks",
                "tenant_id": tenant_disabled,
                "installation_row_id": installation_disabled,
            },
        ),
    )

    candidates = await load_active_credential_renewal_candidates(
        _CandidatePool(connection),  # type: ignore[arg-type]
        (source_definition("quickbooks"),),
        8,
        1,
        _now(),
    )

    assert candidates == (
        CredentialRenewalCandidate(
            source_id="quickbooks",
            tenant_id=tenant_active,
            installation_id=installation_active,
            last_claimed_at=_now() - dt.timedelta(minutes=1),
        ),
    )
    assert all("secret_ref" not in query for query in connection.queries)
    assert connection.current_tenant == tenant_disabled


async def test_due_job_beyond_onboarding_scan_is_selected_first_and_rechecked() -> None:
    tenant_sampled = _uuid(30)
    installation_sampled = _uuid(31)
    tenant_due = _uuid(32)
    installation_due = _uuid(33)
    connection = _CandidateConnection(
        active_ids={
            tenant_sampled: {installation_sampled},
            tenant_due: {installation_due},
        },
        claim_times={},
        # The direct durable query below finds this second identity even though
        # a one-row onboarding sample cannot reach it.
        discovery_rows=(
            {
                "source": "quickbooks",
                "tenant_id": tenant_sampled,
                "installation_row_id": installation_sampled,
            },
            {
                "source": "quickbooks",
                "tenant_id": tenant_due,
                "installation_row_id": installation_due,
            },
        ),
        due_rows=(
            {
                "source_id": "quickbooks",
                "tenant_id": tenant_due,
                "installation_id": installation_due,
                "last_claimed_at": _now() - dt.timedelta(minutes=1),
            },
        ),
    )

    candidates = await load_active_credential_renewal_candidates(
        _CandidatePool(connection),  # type: ignore[arg-type]
        (source_definition("quickbooks"),),
        1,
        1,
        _now(),
    )

    assert candidates == (
        CredentialRenewalCandidate(
            source_id="quickbooks",
            tenant_id=tenant_due,
            installation_id=installation_due,
            last_claimed_at=_now() - dt.timedelta(minutes=1),
            durable_due=True,
        ),
    )
    assert any(
        "FROM source_renewal_jobs" in query and "tenant_id = $1" in query
        for query in connection.queries
    )
    assert not any(
        "WITH installed AS MATERIALIZED" in query for query in connection.queries
    )


async def test_due_discovery_excludes_cooldowns_and_terminal_jobs() -> None:
    tenant_id = _uuid(40)
    installation_pending = _uuid(41)
    installation_expired_lease = _uuid(42)
    installation_cooldown = _uuid(43)
    installation_reauthorization = _uuid(44)
    installation_manual = _uuid(45)
    installation_live_lease = _uuid(46)
    connection = _CandidateConnection(
        active_ids={
            tenant_id: {
                installation_pending,
                installation_expired_lease,
                installation_cooldown,
                installation_reauthorization,
                installation_manual,
                installation_live_lease,
            },
        },
        claim_times={},
        discovery_rows=(),
        due_rows=(
            {
                "source_id": "quickbooks",
                "tenant_id": tenant_id,
                "installation_id": installation_pending,
                "state": "pending",
                "next_attempt_at": _now() - dt.timedelta(seconds=1),
                "last_claimed_at": None,
            },
            {
                "source_id": "quickbooks",
                "tenant_id": tenant_id,
                "installation_id": installation_expired_lease,
                "state": "leased",
                "lease_expires_at": _now() - dt.timedelta(seconds=1),
                "last_claimed_at": None,
            },
            {
                "source_id": "quickbooks",
                "tenant_id": tenant_id,
                "installation_id": installation_cooldown,
                "state": "retry_scheduled",
                "next_attempt_at": _now() + dt.timedelta(minutes=1),
                "last_claimed_at": None,
            },
            {
                "source_id": "quickbooks",
                "tenant_id": tenant_id,
                "installation_id": installation_reauthorization,
                "state": "reauthorization_required",
                "last_claimed_at": None,
            },
            {
                "source_id": "quickbooks",
                "tenant_id": tenant_id,
                "installation_id": installation_manual,
                "state": "manual_reconciliation_required",
                "last_claimed_at": None,
            },
            {
                "source_id": "quickbooks",
                "tenant_id": tenant_id,
                "installation_id": installation_live_lease,
                "state": "leased",
                "lease_expires_at": _now() + dt.timedelta(minutes=1),
                "last_claimed_at": None,
            },
        ),
    )

    candidates = await load_active_credential_renewal_candidates(
        _CandidatePool(connection),  # type: ignore[arg-type]
        (source_definition("quickbooks"),),
        8,
        8,
        _now(),
    )

    assert {
        candidate.installation_id for candidate in candidates
    } == {installation_pending, installation_expired_lease}
    assert all(candidate.durable_due for candidate in candidates)
