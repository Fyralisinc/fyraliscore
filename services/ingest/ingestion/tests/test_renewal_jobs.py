"""Focused integration and validation gates for durable renewal jobs."""
from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migration
from services.ingest.ingestion.renewal_jobs import (
    RenewalJobKey,
    RenewalJobNotResumable,
    RenewalLeaseLost,
    RenewalScheduleError,
    claim_due_renewal_job,
    complete_renewal_job,
    defer_renewal_job,
    get_renewal_job,
    heartbeat_renewal_job,
    mark_renewal_provider_call_started,
    require_renewal_manual_reconciliation,
    require_renewal_reauthorization,
    resume_renewal_job,
)


pytestmark = pytest.mark.integration

_MIGRATION = (
    Path(__file__).resolve().parents[4]
    / "db/migrations/0200_source_renewal_jobs.sql"
)


async def _seed_tenant(pool: asyncpg.Pool, label: str) -> UUID:
    tenant_id = uuid7()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"renewal-jobs-{label}-{tenant_id}",
    )
    return tenant_id


def _key(tenant_id: UUID, *, target_key: str = "installation") -> RenewalJobKey:
    return RenewalJobKey(
        source_id="github",
        tenant_id=tenant_id,
        installation_id=uuid7(),
        target_key=target_key,
    )


def _past() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(seconds=1)


def _future(seconds: int = 60) -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc) + dt.timedelta(seconds=seconds)


async def test_renewal_job_migration_is_idempotent_and_strict_rls(
    fresh_db: asyncpg.Pool,
) -> None:
    sql = _MIGRATION.read_text(encoding="utf-8")
    async with fresh_db.acquire() as conn:
        await apply_migration(conn, sql, name=_MIGRATION.name)
        await apply_migration(conn, sql, name=_MIGRATION.name)

        relation = await conn.fetchrow(
            """
            SELECT relrowsecurity, relforcerowsecurity
              FROM pg_class
             WHERE oid = 'source_renewal_jobs'::regclass
            """,
        )
        assert relation is not None
        assert relation["relrowsecurity"] is True
        assert relation["relforcerowsecurity"] is True

        policy = await conn.fetchrow(
            """
            SELECT pg_get_expr(polqual, polrelid) AS using_expression,
                   pg_get_expr(polwithcheck, polrelid) AS check_expression
              FROM pg_policy
             WHERE polrelid = 'source_renewal_jobs'::regclass
               AND polname = 'tenant_isolation'
            """,
        )
        assert policy is not None
        assert "app.current_tenant" in policy["using_expression"]
        assert "IS NULL" not in policy["using_expression"]
        assert "app.current_tenant" in policy["check_expression"]

        manual_column = await conn.fetchval(
            """
            SELECT 1
              FROM information_schema.columns
             WHERE table_schema = 'public'
               AND table_name = 'source_renewal_jobs'
               AND column_name = 'manual_reconciliation_required_at'
            """,
        )
        assert manual_column == 1


async def test_renewal_job_claim_heartbeat_and_complete_are_durable(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db, "complete")
    key = _key(tenant_id)

    lease = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-a",
        initial_not_before=_past(),
        lease_timeout_seconds=5,
    )
    assert lease is not None
    assert lease.version == 1

    held = await get_renewal_job(fresh_db, key)
    assert held is not None
    assert held.state == "leased"
    assert held.lease_owner == "renewal-worker-a"
    assert held.attempt_count == 1

    heartbeated = await heartbeat_renewal_job(
        fresh_db,
        lease,
        lease_timeout_seconds=5,
    )
    assert heartbeated.version == lease.version
    assert heartbeated.expires_at.tzinfo is not None

    completed = await complete_renewal_job(
        fresh_db,
        heartbeated,
        next_attempt_at=_future(120),
        expires_at=_future(3600),
    )
    assert completed.state == "pending"
    assert completed.lease_owner is None
    assert completed.lease_expires_at is None
    assert completed.last_success_at is not None
    assert completed.last_error_code is None

    # The durable future schedule prevents an immediate hot-loop re-claim.
    assert await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-b",
    ) is None


async def test_reactive_force_claim_only_overrides_a_normal_pending_schedule(
    fresh_db: asyncpg.Pool,
) -> None:
    """A 401 may pull a normal credential renewal forward, never a cooldown."""

    tenant_id = await _seed_tenant(fresh_db, "reactive-force")
    key = _key(tenant_id)
    initial = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-scheduled",
        initial_not_before=_past(),
    )
    assert initial is not None
    await complete_renewal_job(
        fresh_db,
        initial,
        next_attempt_at=_future(600),
        expires_at=_future(3600),
    )

    # A reactive authentication failure may claim a future *pending* cadence.
    forced = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-reactive",
        force_pending=True,
    )
    assert forced is not None
    assert forced.version == initial.version + 1

    await defer_renewal_job(
        fresh_db,
        forced,
        not_before=_future(600),
        error_code="retry_later:provider_rate_limited",
    )

    # ``RetryLater`` is a provider cooldown, not a normal cadence. Reactive
    # callers must wait for its durable not-before rather than bypass it.
    assert await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-reactive-again",
        force_pending=True,
    ) is None


async def test_expired_lease_after_unsafe_provider_boundary_requires_repair(
    fresh_db: asyncpg.Pool,
) -> None:
    """Recovery must not repeat an OAuth rotation or watch creation blindly."""

    tenant_id = await _seed_tenant(fresh_db, "unsafe-lease-recovery")
    key = _key(tenant_id)
    lease = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-a",
        initial_not_before=_past(),
    )
    assert lease is not None
    assert await mark_renewal_provider_call_started(fresh_db, lease)

    await fresh_db.execute(
        """
        UPDATE source_renewal_jobs
           SET lease_expires_at = now() - interval '1 second'
         WHERE source_id = $1
           AND tenant_id = $2
           AND installation_id = $3
           AND target_key = $4
        """,
        key.source_id,
        key.tenant_id,
        key.installation_id,
        key.target_key,
    )

    # A new worker converts an unknown remote outcome into explicit repair;
    # it never receives a new lease for a potentially repeated unsafe call.
    assert await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-b",
    ) is None
    record = await get_renewal_job(fresh_db, key)
    assert record is not None
    assert record.state == "manual_reconciliation_required"
    assert record.last_error_code == "lease_lost_during_provider_call"
    assert record.provider_call_started_at is None


async def test_sibling_installations_and_concurrent_claims_stay_isolated(
    fresh_db: asyncpg.Pool,
) -> None:
    """One renewal target has one writer; sibling installations do not share it."""

    tenant_id = await _seed_tenant(fresh_db, "siblings-and-concurrency")
    first = RenewalJobKey(
        source_id="quickbooks",
        tenant_id=tenant_id,
        installation_id=uuid7(),
        target_key="installation",
    )
    sibling = RenewalJobKey(
        source_id="quickbooks",
        tenant_id=tenant_id,
        installation_id=uuid7(),
        target_key="installation",
    )

    claimant_a, claimant_b = await asyncio.gather(
        claim_due_renewal_job(
            fresh_db,
            first,
            owner="renewal-worker-a",
            initial_not_before=_past(),
        ),
        claim_due_renewal_job(
            fresh_db,
            first,
            owner="renewal-worker-b",
            initial_not_before=_past(),
        ),
    )
    assert sum(lease is not None for lease in (claimant_a, claimant_b)) == 1

    sibling_lease = await claim_due_renewal_job(
        fresh_db,
        sibling,
        owner="renewal-worker-sibling",
        initial_not_before=_past(),
    )
    assert sibling_lease is not None
    assert sibling_lease.key.installation_id != first.installation_id

    first_record = await get_renewal_job(fresh_db, first)
    sibling_record = await get_renewal_job(fresh_db, sibling)
    assert first_record is not None
    assert sibling_record is not None
    assert first_record.state == "leased"
    assert sibling_record.state == "leased"
    assert first_record.lease_owner in {"renewal-worker-a", "renewal-worker-b"}
    assert sibling_record.lease_owner == "renewal-worker-sibling"


async def test_renewal_job_fences_stale_worker_and_persists_retry_and_reauth(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db, "fencing")
    key = _key(tenant_id, target_key="watch-resource-1")
    lease_a = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-a",
        initial_not_before=_past(),
    )
    assert lease_a is not None

    # Simulate worker A being partitioned past its lease. Worker B receives a
    # new generation; A cannot persist any outcome after that handoff.
    await fresh_db.execute(
        """
        UPDATE source_renewal_jobs
           SET lease_expires_at = now() - interval '1 second'
         WHERE source_id = $1
           AND tenant_id = $2
           AND installation_id = $3
           AND target_key = $4
        """,
        key.source_id,
        key.tenant_id,
        key.installation_id,
        key.target_key,
    )
    lease_b = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-b",
    )
    assert lease_b is not None
    assert lease_b.version == lease_a.version + 1

    with pytest.raises(RenewalLeaseLost):
        await defer_renewal_job(
            fresh_db,
            lease_a,
            not_before=_future(),
            error_code="provider.rate_limited",
        )

    # Unsafe/manual terminal writes use the same exact owner/version fence as
    # success, retry, and reauthorization writes.
    with pytest.raises(RenewalLeaseLost):
        await require_renewal_manual_reconciliation(
            fresh_db,
            lease_a,
            error_code="provider.unknown_side_effect",
        )

    deferred = await defer_renewal_job(
        fresh_db,
        lease_b,
        not_before=_future(),
        error_code="provider.rate_limited",
    )
    assert deferred.state == "retry_scheduled"
    assert deferred.last_error_code == "provider.rate_limited"
    assert deferred.lease_owner is None
    assert await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-c",
    ) is None

    # A retry is only eligible once its durable not-before passes. Make that
    # state transition explicit here; runtime code normally reaches it by
    # waiting for the periodic scheduler rather than sleeping in-process.
    await fresh_db.execute(
        """
        UPDATE source_renewal_jobs
           SET next_attempt_at = now() - interval '1 second'
         WHERE source_id = $1
           AND tenant_id = $2
           AND installation_id = $3
           AND target_key = $4
        """,
        key.source_id,
        key.tenant_id,
        key.installation_id,
        key.target_key,
    )
    lease_c = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-c",
    )
    assert lease_c is not None
    reauth = await require_renewal_reauthorization(
        fresh_db,
        lease_c,
        error_code="oauth.invalid_grant",
    )
    assert reauth.state == "reauthorization_required"
    assert reauth.next_attempt_at is None
    assert reauth.reauthorization_required_at is not None
    assert reauth.lease_owner is None
    assert await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-d",
    ) is None


@pytest.mark.parametrize(
    "terminal_state",
    ["reauthorization_required", "manual_reconciliation_required"],
)
async def test_terminal_jobs_require_explicit_exact_resume_before_claim(
    fresh_db: asyncpg.Pool,
    terminal_state: str,
) -> None:
    tenant_id = await _seed_tenant(fresh_db, f"resume-{terminal_state}")
    key = _key(tenant_id, target_key=f"{terminal_state}-target")
    lease = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-a",
        initial_not_before=_past(),
    )
    assert lease is not None

    if terminal_state == "reauthorization_required":
        terminal = await require_renewal_reauthorization(
            fresh_db,
            lease,
            error_code="oauth.invalid_grant",
        )
        assert terminal.reauthorization_required_at is not None
        assert terminal.manual_reconciliation_required_at is None
    else:
        terminal = await require_renewal_manual_reconciliation(
            fresh_db,
            lease,
            error_code="provider.unknown_side_effect",
        )
        assert terminal.reauthorization_required_at is None
        assert terminal.manual_reconciliation_required_at is not None

    assert terminal.state == terminal_state
    assert terminal.next_attempt_at is None
    assert terminal.lease_owner is None
    assert terminal.lease_expires_at is None
    assert await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-b",
    ) is None

    # Repair cannot silently create or reset a different target, nor can it
    # schedule this one in the past and immediately re-enter the worker loop.
    wrong_target = RenewalJobKey(
        source_id=key.source_id,
        tenant_id=key.tenant_id,
        installation_id=key.installation_id,
        target_key="different-target",
    )
    with pytest.raises(RenewalJobNotResumable):
        await resume_renewal_job(
            fresh_db,
            wrong_target,
            not_before=_future(),
        )
    with pytest.raises(RenewalScheduleError, match="strictly in the future"):
        await resume_renewal_job(
            fresh_db,
            key,
            not_before=_past(),
        )

    resumed = await resume_renewal_job(
        fresh_db,
        key,
        not_before=_future(90),
    )
    assert resumed.state == "pending"
    assert resumed.next_attempt_at is not None
    assert resumed.last_error_code is None
    assert resumed.reauthorization_required_at is None
    assert resumed.manual_reconciliation_required_at is None
    assert resumed.lease_owner is None
    assert resumed.lease_expires_at is None
    assert resumed.lease_version == terminal.lease_version + 1

    with pytest.raises(RenewalJobNotResumable):
        await resume_renewal_job(
            fresh_db,
            key,
            not_before=_future(),
        )
    assert await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-b",
    ) is None

    # The scheduler only claims after the explicit repair schedule is due;
    # the subsequent generation also proves the repair fenced the old lease.
    await fresh_db.execute(
        """
        UPDATE source_renewal_jobs
           SET next_attempt_at = now() - interval '1 second'
         WHERE source_id = $1
           AND tenant_id = $2
           AND installation_id = $3
           AND target_key = $4
        """,
        key.source_id,
        key.tenant_id,
        key.installation_id,
        key.target_key,
    )
    repaired_lease = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-c",
    )
    assert repaired_lease is not None
    assert repaired_lease.version == resumed.lease_version + 1


async def test_past_complete_and_defer_deadlines_leave_lease_owned(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = await _seed_tenant(fresh_db, "past-deadline")
    key = _key(tenant_id)
    lease = await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-worker-a",
        initial_not_before=_past(),
    )
    assert lease is not None

    with pytest.raises(RenewalScheduleError, match="strictly in the future"):
        await complete_renewal_job(
            fresh_db,
            lease,
            next_attempt_at=_past(),
        )
    with pytest.raises(RenewalScheduleError, match="strictly in the future"):
        await defer_renewal_job(
            fresh_db,
            lease,
            not_before=_past(),
            error_code="provider.rate_limited",
        )

    held = await get_renewal_job(fresh_db, key)
    assert held is not None
    assert held.state == "leased"
    assert held.lease_owner == lease.owner
    assert held.lease_version == lease.version

    completed = await complete_renewal_job(
        fresh_db,
        lease,
        next_attempt_at=_future(),
    )
    assert completed.state == "pending"


def test_renewal_job_identity_and_error_codes_cannot_contain_secret_like_text() -> None:
    tenant_id = uuid7()
    with pytest.raises(ValueError, match="surrounding whitespace"):
        RenewalJobKey(
            source_id=" github",
            tenant_id=tenant_id,
            installation_id=uuid7(),
            target_key="installation",
        )

    # The database field accepts controlled codes only. Provider response text
    # is rejected before it can ever be persisted by `defer_renewal_job`.
    from services.ingest.ingestion.renewal_jobs import _validate_error_code

    with pytest.raises(ValueError, match="controlled lowercase code"):
        _validate_error_code("Bearer token=very-secret")
