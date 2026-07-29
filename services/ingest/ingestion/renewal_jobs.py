"""Source-neutral durable scheduling for bounded renewal work.

This module owns only the durable job lifecycle.  It deliberately does *not*
load an installation, resolve a secret, call a provider, or decide whether a
credential/watch is due.  A source-specific renewal callable performs that
provider work between ``claim_due_renewal_job`` and one fenced terminal
operation below.

Every public operation opens one short transaction, binds the exact tenant for
RLS, and performs only database I/O.  In particular, callers must never hold a
transaction or row lock while they make provider requests.  A lease owner and
monotonic generation fence prevent a stale worker from committing a result
after another replica has recovered an expired job.
"""
from __future__ import annotations

import datetime as dt
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

import asyncpg

from lib.shared.tenant_context import TenantContext, bind_tenant


DEFAULT_RENEWAL_LEASE_TIMEOUT_SECONDS = 60.0
MIN_EFFECTIVE_RENEWAL_LEASE_TIMEOUT_SECONDS = 1.0

RenewalJobState = Literal[
    "pending",
    "leased",
    "retry_scheduled",
    "reauthorization_required",
    "manual_reconciliation_required",
]

_TEXT_LIMITS = {
    "source_id": 64,
    "target_key": 1024,
    "owner": 256,
    "error_code": 128,
}
_ERROR_CODE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,127}$")


class RenewalJobError(RuntimeError):
    """Base error for the source-neutral renewal-job substrate."""


class RenewalLeaseLost(RenewalJobError):
    """The caller no longer owns the exact renewal job lease generation."""


class RenewalJobNotResumable(RenewalJobError):
    """An explicit repair attempted to resume a non-terminal renewal job."""


class RenewalScheduleError(RenewalJobError):
    """A terminal write would create an immediately due renewal hot loop."""


@dataclass(frozen=True, slots=True)
class RenewalJobKey:
    """Exact durable identity for one source renewal target."""

    source_id: str
    tenant_id: UUID
    installation_id: UUID
    target_key: str

    def __post_init__(self) -> None:
        _validate_text(self.source_id, "source_id")
        _validate_uuid(self.tenant_id, "tenant_id")
        _validate_uuid(self.installation_id, "installation_id")
        _validate_text(self.target_key, "target_key")


@dataclass(frozen=True, slots=True)
class RenewalLease:
    """One owned generation, valid only until a fenced terminal mutation."""

    key: RenewalJobKey
    owner: str
    version: int
    expires_at: dt.datetime

    def __post_init__(self) -> None:
        if not isinstance(self.key, RenewalJobKey):
            raise TypeError("key must be a RenewalJobKey")
        _validate_text(self.owner, "owner")
        if not isinstance(self.version, int) or self.version < 1:
            raise ValueError("version must be a positive integer")
        _aware_utc(self.expires_at, "expires_at")


@dataclass(frozen=True, slots=True)
class RenewalJobRecord:
    """Non-secret durable state exposed to source-specific renewal callers."""

    key: RenewalJobKey
    state: RenewalJobState
    next_attempt_at: dt.datetime | None
    attempt_count: int
    last_attempt_at: dt.datetime | None
    last_success_at: dt.datetime | None
    expires_at: dt.datetime | None
    last_error_code: str | None
    reauthorization_required_at: dt.datetime | None
    manual_reconciliation_required_at: dt.datetime | None
    lease_owner: str | None
    lease_version: int
    lease_expires_at: dt.datetime | None
    provider_call_started_at: dt.datetime | None
    last_claimed_at: dt.datetime | None
    created_at: dt.datetime
    updated_at: dt.datetime


_INSERT_IF_ABSENT_SQL = """
INSERT INTO source_renewal_jobs (
    source_id,
    tenant_id,
    installation_id,
    target_key,
    state,
    next_attempt_at
)
VALUES ($1, $2, $3, $4, 'pending', $5)
ON CONFLICT (source_id, tenant_id, installation_id, target_key) DO NOTHING
"""

_CLAIM_DUE_SQL = """
UPDATE source_renewal_jobs
   SET state = 'leased',
       attempt_count = attempt_count + 1,
       last_attempt_at = now(),
       last_error_code = NULL,
       lease_owner = $5,
       lease_version = lease_version + 1,
       lease_expires_at = now() + ($6 * interval '1 second'),
       last_claimed_at = now(),
       updated_at = now()
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
   AND (
       (
           state IN ('pending', 'retry_scheduled')
           AND next_attempt_at <= now()
       )
       OR (
           state = 'pending'
           AND $7
       )
       OR (
           state = 'leased'
           AND lease_expires_at <= now()
       )
   )
RETURNING lease_version, lease_expires_at
"""

_ABANDON_UNCERTAIN_EXPIRED_LEASE_SQL = """
UPDATE source_renewal_jobs
   SET state = 'manual_reconciliation_required',
       next_attempt_at = NULL,
       last_error_code = 'lease_lost_during_provider_call',
       reauthorization_required_at = NULL,
       manual_reconciliation_required_at = now(),
       lease_owner = NULL,
       lease_expires_at = NULL,
       provider_call_started_at = NULL,
       updated_at = now()
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
   AND state = 'leased'
   AND lease_expires_at <= now()
   AND provider_call_started_at IS NOT NULL
RETURNING 1
"""

_MARK_PROVIDER_CALL_STARTED_SQL = """
UPDATE source_renewal_jobs
   SET provider_call_started_at = COALESCE(provider_call_started_at, now()),
       updated_at = now()
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
   AND state = 'leased'
   AND lease_owner = $5
   AND lease_version = $6
   AND lease_expires_at > now()
RETURNING provider_call_started_at
"""

_HEARTBEAT_SQL = """
UPDATE source_renewal_jobs
   SET lease_expires_at = now() + ($7 * interval '1 second'),
       updated_at = now()
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
   AND state = 'leased'
   AND lease_owner = $5
   AND lease_version = $6
RETURNING lease_expires_at
"""

_COMPLETE_SQL = """
UPDATE source_renewal_jobs
   SET state = 'pending',
       next_attempt_at = $5,
       last_success_at = now(),
       expires_at = $6,
       last_error_code = NULL,
       reauthorization_required_at = NULL,
       manual_reconciliation_required_at = NULL,
       lease_owner = NULL,
       lease_expires_at = NULL,
       provider_call_started_at = NULL,
       updated_at = now()
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
   AND state = 'leased'
   AND lease_owner = $7
   AND lease_version = $8
   AND lease_expires_at > now()
   AND $5 > now()
RETURNING
    source_id, tenant_id, installation_id, target_key, state,
    next_attempt_at, attempt_count, last_attempt_at, last_success_at,
    expires_at, last_error_code, reauthorization_required_at,
    manual_reconciliation_required_at, lease_owner, lease_version,
    lease_expires_at, provider_call_started_at, last_claimed_at, created_at, updated_at
"""

_DEFER_SQL = """
UPDATE source_renewal_jobs
   SET state = 'retry_scheduled',
       next_attempt_at = $5,
       last_error_code = $6,
       reauthorization_required_at = NULL,
       manual_reconciliation_required_at = NULL,
       lease_owner = NULL,
       lease_expires_at = NULL,
       provider_call_started_at = NULL,
       updated_at = now()
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
   AND state = 'leased'
   AND lease_owner = $7
   AND lease_version = $8
   AND lease_expires_at > now()
   AND $5 > now()
RETURNING
    source_id, tenant_id, installation_id, target_key, state,
    next_attempt_at, attempt_count, last_attempt_at, last_success_at,
    expires_at, last_error_code, reauthorization_required_at,
    manual_reconciliation_required_at, lease_owner, lease_version,
    lease_expires_at, provider_call_started_at, last_claimed_at, created_at, updated_at
"""

_REQUIRE_REAUTHORIZATION_SQL = """
UPDATE source_renewal_jobs
   SET state = 'reauthorization_required',
       next_attempt_at = NULL,
       last_error_code = $5,
       reauthorization_required_at = now(),
       manual_reconciliation_required_at = NULL,
       lease_owner = NULL,
       lease_expires_at = NULL,
       provider_call_started_at = NULL,
       updated_at = now()
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
   AND state = 'leased'
   AND lease_owner = $6
   AND lease_version = $7
   AND lease_expires_at > now()
RETURNING
    source_id, tenant_id, installation_id, target_key, state,
    next_attempt_at, attempt_count, last_attempt_at, last_success_at,
    expires_at, last_error_code, reauthorization_required_at,
    manual_reconciliation_required_at, lease_owner, lease_version,
    lease_expires_at, provider_call_started_at, last_claimed_at, created_at, updated_at
"""

_REQUIRE_MANUAL_RECONCILIATION_SQL = """
UPDATE source_renewal_jobs
   SET state = 'manual_reconciliation_required',
       next_attempt_at = NULL,
       last_error_code = $5,
       reauthorization_required_at = NULL,
       manual_reconciliation_required_at = now(),
       lease_owner = NULL,
       lease_expires_at = NULL,
       provider_call_started_at = NULL,
       updated_at = now()
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
   AND state = 'leased'
   AND lease_owner = $6
   AND lease_version = $7
   AND lease_expires_at > now()
RETURNING
    source_id, tenant_id, installation_id, target_key, state,
    next_attempt_at, attempt_count, last_attempt_at, last_success_at,
    expires_at, last_error_code, reauthorization_required_at,
    manual_reconciliation_required_at, lease_owner, lease_version,
    lease_expires_at, provider_call_started_at, last_claimed_at, created_at, updated_at
"""

_RESUME_TERMINAL_JOB_SQL = """
UPDATE source_renewal_jobs
   SET state = 'pending',
       next_attempt_at = $5,
       last_error_code = NULL,
       reauthorization_required_at = NULL,
       manual_reconciliation_required_at = NULL,
       lease_owner = NULL,
       lease_version = lease_version + 1,
       lease_expires_at = NULL,
       provider_call_started_at = NULL,
       updated_at = now()
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
   AND state IN (
       'reauthorization_required',
       'manual_reconciliation_required'
   )
   AND lease_owner IS NULL
   AND lease_expires_at IS NULL
   AND $5 > now()
RETURNING
    source_id, tenant_id, installation_id, target_key, state,
    next_attempt_at, attempt_count, last_attempt_at, last_success_at,
    expires_at, last_error_code, reauthorization_required_at,
    manual_reconciliation_required_at, lease_owner, lease_version,
    lease_expires_at, provider_call_started_at, last_claimed_at, created_at, updated_at
"""

_GET_SQL = """
SELECT
    source_id, tenant_id, installation_id, target_key, state,
    next_attempt_at, attempt_count, last_attempt_at, last_success_at,
    expires_at, last_error_code, reauthorization_required_at,
    manual_reconciliation_required_at, lease_owner, lease_version,
    lease_expires_at, provider_call_started_at, last_claimed_at, created_at, updated_at
  FROM source_renewal_jobs
 WHERE source_id = $1
   AND tenant_id = $2
   AND installation_id = $3
   AND target_key = $4
"""


async def claim_due_renewal_job(
    pool: asyncpg.Pool,
    key: RenewalJobKey,
    *,
    owner: str,
    lease_timeout_seconds: float = DEFAULT_RENEWAL_LEASE_TIMEOUT_SECONDS,
    initial_not_before: dt.datetime | None = None,
    force_pending: bool = False,
) -> RenewalLease | None:
    """Create an absent job then acquire it only when it is due.

    ``initial_not_before`` applies only to the first insert.  Existing retry,
    reauthorization, and lease state is never silently reset by a caller that
    happens to invoke this function again.  ``force_pending`` is reserved for
    a reactive credential failure: it may claim an otherwise future *pending*
    schedule, but never a provider cooldown, terminal repair state, or an
    active lease.  An expired lease may be recovered by a different owner;
    that increments the generation and fences the old worker's later result.
    """

    _validate_key(key)
    _validate_text(owner, "owner")
    if not isinstance(force_pending, bool):
        raise TypeError("force_pending must be a boolean")
    timeout = _effective_lease_timeout_seconds(lease_timeout_seconds)
    first_due = (
        dt.datetime.now(tz=dt.timezone.utc)
        if initial_not_before is None
        else _aware_utc(initial_not_before, "initial_not_before")
    )

    async with _tenant_transaction(pool, key.tenant_id) as conn:
        await conn.execute(
            _INSERT_IF_ABSENT_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
            first_due,
        )
        # An expired lease that had already crossed an unsafe provider-call
        # boundary is ambiguous: the old process may have rotated a credential
        # or created a channel after losing its lease. Do not let the next
        # worker repeat it. It must be explicitly reconciled instead.
        abandoned = await conn.fetchval(
            _ABANDON_UNCERTAIN_EXPIRED_LEASE_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
        )
        if abandoned is not None:
            return None
        row = await conn.fetchrow(
            _CLAIM_DUE_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
            owner,
            timeout,
            force_pending,
        )
    if row is None:
        return None
    return RenewalLease(
        key=key,
        owner=owner,
        version=int(row["lease_version"]),
        expires_at=_aware_utc(row["lease_expires_at"], "lease_expires_at"),
    )


async def mark_renewal_provider_call_started(
    pool: asyncpg.Pool,
    lease: RenewalLease,
) -> dt.datetime:
    """Fence and record the boundary before one unsafe provider operation.

    The timestamp contains no provider material. It is deliberately retained
    only while the lease is active; all fenced terminal transitions clear it.
    If the lease expires first, the next claimant changes the target to manual
    reconciliation rather than replaying an unknown remote side effect.
    """

    _validate_lease(lease)
    key = lease.key
    async with _tenant_transaction(pool, key.tenant_id) as conn:
        row = await conn.fetchrow(
            _MARK_PROVIDER_CALL_STARTED_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
            lease.owner,
            lease.version,
        )
    if row is None:
        raise _lease_lost(lease)
    return _aware_utc(row["provider_call_started_at"], "provider_call_started_at")


async def heartbeat_renewal_job(
    pool: asyncpg.Pool,
    lease: RenewalLease,
    *,
    lease_timeout_seconds: float = DEFAULT_RENEWAL_LEASE_TIMEOUT_SECONDS,
) -> RenewalLease:
    """Extend the current owner/version lease or fail closed on takeover.

    A generation can extend just after its nominal expiry if another worker
    has not won the race to increment the version.  This mirrors the shard
    lease semantics: the owner/version fence, rather than a local clock race,
    decides which worker is authoritative.
    """

    _validate_lease(lease)
    timeout = _effective_lease_timeout_seconds(lease_timeout_seconds)
    key = lease.key
    async with _tenant_transaction(pool, key.tenant_id) as conn:
        row = await conn.fetchrow(
            _HEARTBEAT_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
            lease.owner,
            lease.version,
            timeout,
        )
    if row is None:
        raise _lease_lost(lease)
    return RenewalLease(
        key=key,
        owner=lease.owner,
        version=lease.version,
        expires_at=_aware_utc(row["lease_expires_at"], "lease_expires_at"),
    )


async def complete_renewal_job(
    pool: asyncpg.Pool,
    lease: RenewalLease,
    *,
    next_attempt_at: dt.datetime,
    expires_at: dt.datetime | None = None,
) -> RenewalJobRecord:
    """Record a successful bounded renewal and schedule its next attempt."""

    _validate_lease(lease)
    next_due = _require_strictly_future(next_attempt_at, "next_attempt_at")
    expiry = None if expires_at is None else _aware_utc(expires_at, "expires_at")
    key = lease.key
    async with _tenant_transaction(pool, key.tenant_id) as conn:
        row = await conn.fetchrow(
            _COMPLETE_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
            next_due,
            expiry,
            lease.owner,
            lease.version,
        )
    if row is None:
        raise _lease_lost(lease)
    return _record_from_row(row)


async def defer_renewal_job(
    pool: asyncpg.Pool,
    lease: RenewalLease,
    *,
    not_before: dt.datetime,
    error_code: str,
) -> RenewalJobRecord:
    """Persist a retry deadline and relinquish the owned lease.

    The error is a controlled code, not provider text.  The caller returns to
    its worker loop immediately; it must not sleep or spin while the job is
    waiting for a provider cooldown.
    """

    _validate_lease(lease)
    retry_at = _require_strictly_future(not_before, "not_before")
    _validate_error_code(error_code)
    key = lease.key
    async with _tenant_transaction(pool, key.tenant_id) as conn:
        row = await conn.fetchrow(
            _DEFER_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
            retry_at,
            error_code,
            lease.owner,
            lease.version,
        )
    if row is None:
        raise _lease_lost(lease)
    return _record_from_row(row)


async def require_renewal_reauthorization(
    pool: asyncpg.Pool,
    lease: RenewalLease,
    *,
    error_code: str,
) -> RenewalJobRecord:
    """Stop automatic renewal until the exact installation is reauthorized."""

    _validate_lease(lease)
    _validate_error_code(error_code)
    key = lease.key
    async with _tenant_transaction(pool, key.tenant_id) as conn:
        row = await conn.fetchrow(
            _REQUIRE_REAUTHORIZATION_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
            error_code,
            lease.owner,
            lease.version,
        )
    if row is None:
        raise _lease_lost(lease)
    return _record_from_row(row)


async def require_renewal_manual_reconciliation(
    pool: asyncpg.Pool,
    lease: RenewalLease,
    *,
    error_code: str,
) -> RenewalJobRecord:
    """Stop a job after an unsafe provider outcome until explicit repair.

    This is for outcomes whose remote side effect is unknown or cannot be
    retried safely. It is deliberately distinct from a normal retry deadline:
    the exact target remains terminal until a source repair path calls
    :func:`resume_renewal_job`.
    """

    _validate_lease(lease)
    _validate_error_code(error_code)
    key = lease.key
    async with _tenant_transaction(pool, key.tenant_id) as conn:
        row = await conn.fetchrow(
            _REQUIRE_MANUAL_RECONCILIATION_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
            error_code,
            lease.owner,
            lease.version,
        )
    if row is None:
        raise _lease_lost(lease)
    return _record_from_row(row)


async def resume_renewal_job(
    pool: asyncpg.Pool,
    key: RenewalJobKey,
    *,
    not_before: dt.datetime,
) -> RenewalJobRecord:
    """Explicitly resume one repaired terminal target at a future deadline.

    The complete source/tenant/installation/target identity is required. A
    regular pending, leased, or retry job cannot be reset through this API;
    that prevents repair tooling from bypassing an active lease or durable
    provider cooldown. Incrementing ``lease_version`` fences any stale worker
    generation even before the next worker claims the repaired job.
    """

    _validate_key(key)
    resume_at = _require_strictly_future(not_before, "not_before")
    async with _tenant_transaction(pool, key.tenant_id) as conn:
        row = await conn.fetchrow(
            _RESUME_TERMINAL_JOB_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
            resume_at,
        )
    if row is None:
        raise RenewalJobNotResumable(
            "renewal job is not in a terminal repair state for "
            f"{key.source_id}/{key.tenant_id}/{key.installation_id}/"
            f"{key.target_key}",
        )
    return _record_from_row(row)


async def get_renewal_job(
    pool: asyncpg.Pool,
    key: RenewalJobKey,
) -> RenewalJobRecord | None:
    """Return metadata for one exact job; there is no cross-tenant lookup."""

    _validate_key(key)
    async with _tenant_transaction(pool, key.tenant_id) as conn:
        row = await conn.fetchrow(
            _GET_SQL,
            key.source_id,
            key.tenant_id,
            key.installation_id,
            key.target_key,
        )
    return None if row is None else _record_from_row(row)


@asynccontextmanager
async def _tenant_transaction(
    pool: asyncpg.Pool,
    tenant_id: UUID,
) -> AsyncIterator[TenantContext]:
    """Open the smallest possible transaction with strict tenant RLS bound."""

    if not isinstance(pool, asyncpg.Pool):
        # asyncpg pools are normally concrete, but this detects accidental
        # connection/provider-client injection before the code reaches SQL.
        raise TypeError("pool must be an asyncpg.Pool")
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                yield tctx


def _effective_lease_timeout_seconds(configured_seconds: float) -> float:
    if not isinstance(configured_seconds, (int, float)):
        raise TypeError("lease_timeout_seconds must be a number")
    if configured_seconds <= 0:
        raise ValueError("lease_timeout_seconds must be > 0")
    return max(MIN_EFFECTIVE_RENEWAL_LEASE_TIMEOUT_SECONDS, float(configured_seconds))


def _record_from_row(row: Mapping[str, object]) -> RenewalJobRecord:
    key = RenewalJobKey(
        source_id=str(row["source_id"]),
        tenant_id=_row_uuid(row, "tenant_id"),
        installation_id=_row_uuid(row, "installation_id"),
        target_key=str(row["target_key"]),
    )
    raw_state = str(row["state"])
    if raw_state not in {
        "pending",
        "leased",
        "retry_scheduled",
        "reauthorization_required",
        "manual_reconciliation_required",
    }:
        raise RenewalJobError(f"stored renewal job has invalid state {raw_state!r}")
    return RenewalJobRecord(
        key=key,
        state=raw_state,  # type: ignore[arg-type]
        next_attempt_at=_optional_aware_row_value(row, "next_attempt_at"),
        attempt_count=int(row["attempt_count"]),
        last_attempt_at=_optional_aware_row_value(row, "last_attempt_at"),
        last_success_at=_optional_aware_row_value(row, "last_success_at"),
        expires_at=_optional_aware_row_value(row, "expires_at"),
        last_error_code=(
            None if row["last_error_code"] is None else str(row["last_error_code"])
        ),
        reauthorization_required_at=_optional_aware_row_value(
            row,
            "reauthorization_required_at",
        ),
        manual_reconciliation_required_at=_optional_aware_row_value(
            row,
            "manual_reconciliation_required_at",
        ),
        lease_owner=(
            None if row["lease_owner"] is None else str(row["lease_owner"])
        ),
        lease_version=int(row["lease_version"]),
        lease_expires_at=_optional_aware_row_value(row, "lease_expires_at"),
        provider_call_started_at=_optional_aware_row_value(
            row,
            "provider_call_started_at",
        ),
        last_claimed_at=_optional_aware_row_value(row, "last_claimed_at"),
        created_at=_aware_utc(row["created_at"], "created_at"),
        updated_at=_aware_utc(row["updated_at"], "updated_at"),
    )


def _row_uuid(row: Mapping[str, object], field: str) -> UUID:
    value = row[field]
    if not isinstance(value, UUID):
        raise RenewalJobError(f"stored renewal job {field} is not a UUID")
    return value


def _optional_aware_row_value(
    row: Mapping[str, object],
    field: str,
) -> dt.datetime | None:
    value = row[field]
    return None if value is None else _aware_utc(value, field)


def _validate_key(key: RenewalJobKey) -> None:
    if not isinstance(key, RenewalJobKey):
        raise TypeError("key must be a RenewalJobKey")


def _validate_lease(lease: RenewalLease) -> None:
    if not isinstance(lease, RenewalLease):
        raise TypeError("lease must be a RenewalLease")


def _validate_text(value: object, field: str) -> str:
    maximum = _TEXT_LIMITS[field]
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    if value != value.strip():
        raise ValueError(f"{field} must not contain surrounding whitespace")
    if len(value) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{field} must not contain control characters")
    return value


def _validate_error_code(value: object) -> str:
    _validate_text(value, "error_code")
    if not _ERROR_CODE_PATTERN.fullmatch(value):
        raise ValueError(
            "error_code must be a controlled lowercase code using "
            "letters, digits, '.', '_', ':', or '-'",
        )
    return value


def _validate_uuid(value: object, field: str) -> UUID:
    if not isinstance(value, UUID):
        raise ValueError(f"{field} must be a UUID")
    return value


def _aware_utc(value: object, field: str) -> dt.datetime:
    if (
        not isinstance(value, dt.datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{field} must be a timezone-aware datetime")
    return value.astimezone(dt.timezone.utc)


def _require_strictly_future(value: object, field: str) -> dt.datetime:
    """Reject an immediate redelivery schedule before it can become a hot loop."""

    moment = _aware_utc(value, field)
    if moment <= dt.datetime.now(tz=dt.timezone.utc):
        raise RenewalScheduleError(f"{field} must be strictly in the future")
    return moment


def _lease_lost(lease: RenewalLease) -> RenewalLeaseLost:
    key = lease.key
    return RenewalLeaseLost(
        "renewal lease lost for "
        f"{key.source_id}/{key.tenant_id}/{key.installation_id}/{key.target_key} "
        f"(owner={lease.owner!r}, version={lease.version})",
    )


__all__ = [
    "DEFAULT_RENEWAL_LEASE_TIMEOUT_SECONDS",
    "MIN_EFFECTIVE_RENEWAL_LEASE_TIMEOUT_SECONDS",
    "RenewalJobError",
    "RenewalJobKey",
    "RenewalJobNotResumable",
    "RenewalJobRecord",
    "RenewalScheduleError",
    "RenewalJobState",
    "RenewalLease",
    "RenewalLeaseLost",
    "claim_due_renewal_job",
    "complete_renewal_job",
    "defer_renewal_job",
    "get_renewal_job",
    "heartbeat_renewal_job",
    "mark_renewal_provider_call_started",
    "require_renewal_manual_reconciliation",
    "require_renewal_reauthorization",
    "resume_renewal_job",
]
