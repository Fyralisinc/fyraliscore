"""Bounded, contract-owned renewal execution.

The source catalog declares *which* lifecycle operation a source owns.  This
module supplies the common one-shot execution envelope for those operations:
an exact tenant/installation key, a durable owner/version lease, bounded
heartbeats while a provider request is in flight, and a terminal persisted
schedule.  Provider-specific algorithms remain in their source modules.

It intentionally has no source-dispatch registry.  Each catalog binding calls
one small source wrapper which supplies its immutable source identity and its
provider-specific attempt callable.
"""
from __future__ import annotations

import asyncio
import contextlib
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import UUID

import httpx

from lib.shared.provider_transport import ProviderRetryForbiddenError, RetryLater
from lib.shared.tenant_context import bind_tenant
from services.ingest.ingestion.renewal_jobs import (
    RenewalJobKey,
    RenewalLease,
    RenewalLeaseLost,
    claim_due_renewal_job,
    complete_renewal_job,
    defer_renewal_job,
    heartbeat_renewal_job,
    mark_renewal_provider_call_started,
    require_renewal_manual_reconciliation,
    require_renewal_reauthorization,
)
from services.ingest.integrations.oauth_refresh import (
    OAuthRefreshError,
    refresh_and_persist,
)
from services.ingest.source_contract.catalog import source_definition


RenewalAttemptState = Literal["not_due", "renewed"]
RenewalOutcomeState = Literal[
    "not_due",
    "renewed",
    "retry_scheduled",
    "reauthorization_required",
    "manual_reconciliation_required",
    "lease_unavailable",
]


class RenewalInvocationError(ValueError):
    """A contract-owned renewal call lacks exact, safe execution inputs."""


class RenewalReauthorizationRequired(RuntimeError):
    """The provider's credential/channel material requires user repair.

    Only a stable diagnostic code is retained in the durable job table.  The
    exception message must therefore stay generic and never include provider
    credentials, token payloads, or response bodies.
    """

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class RenewalManualRepairRequired(RuntimeError):
    """A renewal outcome is unsafe to retry without operator reconciliation.

    This is intentionally distinct from reauthorization: an unknown outcome
    after an unsafe token rotation or channel creation may leave valid
    credentials in place while requiring the provider-side state to be checked.
    The durable job stores only the controlled error code.
    """

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class RenewalInvocation:
    """Dependencies and identity for exactly one bounded renewal attempt.

    ``target_key`` is ``"installation"`` for credential renewal and the
    resource-row UUID string for a watch.  It is part of the durable job key;
    a sibling installation can never share the same renewal lease.
    """

    pool: Any = field(repr=False)
    tenant_id: UUID
    installation_id: UUID
    target_key: str
    secret_store: Any | None = field(default=None, repr=False)
    http: httpx.AsyncClient | None = field(default=None, repr=False)
    request_binding: Any | None = field(default=None, repr=False)
    worker_id: str | None = None
    watch_address: str | None = None
    now: datetime | None = None
    lease_timeout_seconds: float = 60.0
    force: bool = False

    def __post_init__(self) -> None:
        for field_name in ("tenant_id", "installation_id"):
            value = getattr(self, field_name)
            if not isinstance(value, UUID):
                try:
                    value = UUID(str(value))
                except (TypeError, ValueError) as exc:
                    raise RenewalInvocationError(
                        f"{field_name} must be an exact UUID"
                    ) from exc
                object.__setattr__(self, field_name, value)
        if not isinstance(self.target_key, str) or not self.target_key.strip():
            raise RenewalInvocationError("target_key must be non-empty")
        if len(self.target_key) > 256:
            raise RenewalInvocationError("target_key must be at most 256 characters")
        if self.now is not None and (
            self.now.tzinfo is None or self.now.utcoffset() is None
        ):
            raise RenewalInvocationError("now must be timezone-aware")
        if (
            isinstance(self.lease_timeout_seconds, bool)
            or not isinstance(self.lease_timeout_seconds, (int, float))
            or self.lease_timeout_seconds <= 0
        ):
            raise RenewalInvocationError(
                "lease_timeout_seconds must be positive"
            )
        if not isinstance(self.force, bool):
            raise RenewalInvocationError("force must be a boolean")

    @property
    def owner(self) -> str:
        return self.worker_id or f"renewal@{socket.gethostname()}"

    @property
    def current_time(self) -> datetime:
        return (self.now or datetime.now(timezone.utc)).astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class RenewalAttempt:
    """Provider-specific result which is safe to persist as scheduling state."""

    state: RenewalAttemptState
    next_attempt_at: datetime
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.state not in {"not_due", "renewed"}:
            raise RenewalInvocationError(f"invalid renewal attempt state {self.state!r}")
        for field_name in ("next_attempt_at", "expires_at"):
            value = getattr(self, field_name)
            if value is not None and (
                value.tzinfo is None or value.utcoffset() is None
            ):
                raise RenewalInvocationError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class RenewalOutcome:
    """Secret-free result of one bounded lifecycle execution."""

    source_id: str
    state: RenewalOutcomeState
    next_attempt_at: datetime | None
    expires_at: datetime | None = None
    error_code: str | None = None


RenewalAttemptCallable = Callable[
    [RenewalInvocation, RenewalLease],
    Awaitable[RenewalAttempt],
]


def renewal_next_attempt_at(
    expires_at: datetime,
    *,
    now: datetime,
    renewal_window_seconds: int,
    error_code: str,
) -> datetime:
    """Return a safe next renewal point or require explicit repair.

    A provider may respond successfully while returning an already-expired or
    implausibly short-lived credential/channel. Scheduling that response at a
    past timestamp would create a durable hot loop; treating it as success
    would leave the installation silently stale. Both conditions instead stop
    automation until the provider-side state is reconciled.
    """

    if expires_at.tzinfo is None or expires_at.utcoffset() is None:
        raise RenewalManualRepairRequired(error_code)
    if now.tzinfo is None or now.utcoffset() is None:
        raise RenewalInvocationError("now must be timezone-aware")
    normalized_expiry = expires_at.astimezone(timezone.utc)
    normalized_now = now.astimezone(timezone.utc)
    if normalized_expiry <= normalized_now + timedelta(seconds=renewal_window_seconds):
        raise RenewalManualRepairRequired(error_code)
    return normalized_expiry - timedelta(seconds=renewal_window_seconds)


def _ensure_contract(
    source_id: str,
    invocation: RenewalInvocation,
    *,
    expected_kind: Literal["watch", "credential"],
) -> RenewalJobKey:
    source = source_definition(source_id)
    renewal = source.renewal
    if renewal is None or renewal.kind != expected_kind:
        raise RenewalInvocationError(
            f"{source_id} has no {expected_kind} renewal contract"
        )
    if renewal.lease_scope == "installation" and invocation.target_key != "installation":
        raise RenewalInvocationError(
            "installation renewal target_key must be 'installation'"
        )
    if renewal.lease_scope == "resource" and invocation.target_key == "installation":
        raise RenewalInvocationError(
            "resource renewal requires an exact resource target_key"
        )
    return RenewalJobKey(
        source_id=source.source_id,
        tenant_id=invocation.tenant_id,
        installation_id=invocation.installation_id,
        target_key=invocation.target_key,
    )


async def _heartbeat_until_cancelled(
    invocation: RenewalInvocation,
    lease: RenewalLease,
) -> None:
    """Keep a lease alive without retaining any provider response data."""

    # A third of the lease period leaves two opportunities to recover from a
    # transient database delay before another worker may reclaim the job.
    interval = max(0.05, float(invocation.lease_timeout_seconds) / 3.0)
    while True:
        await asyncio.sleep(interval)
        await heartbeat_renewal_job(
            invocation.pool,
            lease,
            lease_timeout_seconds=float(invocation.lease_timeout_seconds),
        )


async def _await_attempt_with_lease_heartbeat(
    attempt: RenewalAttemptCallable,
    invocation: RenewalInvocation,
    lease: RenewalLease,
    heartbeat: asyncio.Task[None],
) -> RenewalAttempt:
    """Stop a bounded attempt as soon as its fenced lease is lost.

    A provider call that already crossed its unsafe boundary is recorded by the
    source-specific attempt. Cancelling its local task cannot undo a remote
    side effect, but it prevents subsequent calls/persistence by the stale
    worker. The durable job recovery path then requires manual reconciliation
    instead of allowing a replacement worker to repeat that marked operation.
    """

    attempt_task = asyncio.ensure_future(attempt(invocation, lease))
    try:
        done, _pending = await asyncio.wait(
            {attempt_task, heartbeat},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if heartbeat in done:
            heartbeat_error = heartbeat.exception()
            if heartbeat_error is not None:
                if not attempt_task.done():
                    attempt_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await attempt_task
                # Treat a simultaneous provider result conservatively. The
                # generic terminal mutation is not yet durable at this point,
                # so an expired fence makes the remote outcome ambiguous.
                raise heartbeat_error
        return await attempt_task
    finally:
        if not attempt_task.done():
            attempt_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await attempt_task


async def run_bounded_renewal(
    invocation: RenewalInvocation,
    *,
    source_id: str,
    expected_kind: Literal["watch", "credential"],
    attempt: RenewalAttemptCallable,
) -> RenewalOutcome:
    """Claim, execute, and durably settle one source-owned renewal attempt.

    The transaction used by the job helper ends before ``attempt`` reaches a
    provider.  A ``RetryLater`` therefore becomes an exact durable
    ``next_attempt_at`` rather than an in-process sleep or a zero-progress
    loop.  Permanent authorization failures become explicit repair work.
    """

    key = _ensure_contract(source_id, invocation, expected_kind=expected_kind)
    lease = await claim_due_renewal_job(
        invocation.pool,
        key,
        owner=invocation.owner,
        lease_timeout_seconds=float(invocation.lease_timeout_seconds),
        initial_not_before=invocation.current_time,
        # A reactive credential failure may legitimately precede the normal
        # cadence.  The durable job helper limits this override to a normal
        # ``pending`` schedule; it cannot bypass a provider cooldown,
        # terminal repair state, or another worker's live lease.
        force_pending=(expected_kind == "credential" and invocation.force),
    )
    if lease is None:
        return RenewalOutcome(
            source_id=source_id,
            state="lease_unavailable",
            next_attempt_at=None,
        )

    heartbeat = asyncio.create_task(
        _heartbeat_until_cancelled(invocation, lease),
        name=f"renewal-heartbeat:{source_id}:{invocation.target_key}",
    )
    try:
        result = await _await_attempt_with_lease_heartbeat(
            attempt,
            invocation,
            lease,
            heartbeat,
        )
    except RetryLater as exc:
        if exc.not_before <= invocation.current_time:
            await require_renewal_manual_reconciliation(
                invocation.pool,
                lease,
                error_code="renewal_retry_deadline_invalid",
            )
            return RenewalOutcome(
                source_id=source_id,
                state="manual_reconciliation_required",
                next_attempt_at=None,
                error_code="renewal_retry_deadline_invalid",
            )
        await defer_renewal_job(
            invocation.pool,
            lease,
            not_before=exc.not_before,
            error_code=f"retry_later:{exc.reason.value}",
        )
        return RenewalOutcome(
            source_id=source_id,
            state="retry_scheduled",
            next_attempt_at=exc.not_before,
            error_code=f"retry_later:{exc.reason.value}",
        )
    except ProviderRetryForbiddenError:
        await require_renewal_manual_reconciliation(
            invocation.pool,
            lease,
            error_code="provider_retry_forbidden",
        )
        return RenewalOutcome(
            source_id=source_id,
            state="manual_reconciliation_required",
            next_attempt_at=None,
            error_code="provider_retry_forbidden",
        )
    except RenewalManualRepairRequired as exc:
        await require_renewal_manual_reconciliation(
            invocation.pool,
            lease,
            error_code=exc.error_code,
        )
        return RenewalOutcome(
            source_id=source_id,
            state="manual_reconciliation_required",
            next_attempt_at=None,
            error_code=exc.error_code,
        )
    except RenewalReauthorizationRequired as exc:
        await require_renewal_reauthorization(
            invocation.pool,
            lease,
            error_code=exc.error_code,
        )
        return RenewalOutcome(
            source_id=source_id,
            state="reauthorization_required",
            next_attempt_at=None,
            error_code=exc.error_code,
        )
    except RenewalLeaseLost:
        # An in-flight provider call was fenced out by another worker. Its
        # durable provider-call marker makes recovery manual; this stale
        # process must not attempt another terminal mutation.
        raise
    except Exception:
        # Only ProviderTransport's typed RetryLater is safe for an automatic
        # repeat. A catch-all retry would repeat an unsafe token rotation or
        # watch creation after an unknown provider-side outcome.
        await require_renewal_manual_reconciliation(
            invocation.pool,
            lease,
            error_code="unexpected_renewal_failure",
        )
        return RenewalOutcome(
            source_id=source_id,
            state="manual_reconciliation_required",
            next_attempt_at=None,
            error_code="unexpected_renewal_failure",
        )
    else:
        await complete_renewal_job(
            invocation.pool,
            lease,
            next_attempt_at=result.next_attempt_at,
            expires_at=result.expires_at,
        )
        return RenewalOutcome(
            source_id=source_id,
            state=result.state,
            next_attempt_at=result.next_attempt_at,
            expires_at=result.expires_at,
        )
    finally:
        heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError, RenewalLeaseLost):
            await heartbeat


async def _load_active_credential_installation(
    invocation: RenewalInvocation,
    *,
    source_id: str,
) -> Any:
    """Load only the exact active installation declared by the catalog."""

    source = source_definition(source_id)
    refresh = source.credential_refresh
    if refresh is None:
        raise RenewalInvocationError(
            f"{source_id} has no credential refresh declaration"
        )
    async with invocation.pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, invocation.tenant_id) as tctx:
                return await tctx.fetchrow(
                    f"""
                    SELECT id, tenant_id, secret_ref, refresh_secret_ref,
                           token_expires_at
                      FROM {refresh.install_table}
                     WHERE id = $1
                       AND tenant_id = $2
                       AND disabled_at IS NULL
                    """,
                    invocation.installation_id,
                    invocation.tenant_id,
                )


async def run_credential_renewal(
    invocation: RenewalInvocation,
    *,
    source_id: str,
) -> RenewalOutcome:
    """Run a source's catalog-owned OAuth refresh or client-credential mint."""

    source = source_definition(source_id)
    renewal = source.renewal
    refresh = source.credential_refresh
    if renewal is None or renewal.kind != "credential" or refresh is None:
        raise RenewalInvocationError(
            f"{source_id} has no credential renewal contract"
        )
    if invocation.secret_store is None or invocation.http is None:
        raise RenewalInvocationError(
            "credential renewal requires secret_store and http"
        )

    async def attempt(
        call: RenewalInvocation,
        lease: RenewalLease,
    ) -> RenewalAttempt:
        install = await _load_active_credential_installation(
            call,
            source_id=source_id,
        )
        if install is None:
            raise RenewalReauthorizationRequired("installation_unavailable")
        expires_at = install["token_expires_at"]
        now = call.current_time
        if (
            not call.force
            and
            expires_at is not None
            and expires_at > now + timedelta(seconds=renewal.renewal_window_seconds)
        ):
            return RenewalAttempt(
                state="not_due",
                next_attempt_at=renewal_next_attempt_at(
                    expires_at,
                    now=now,
                    renewal_window_seconds=renewal.renewal_window_seconds,
                    error_code="credential_expiry_invalid",
                ),
                expires_at=expires_at,
            )
        await mark_renewal_provider_call_started(call.pool, lease)
        try:
            refreshed = await refresh_and_persist(
                provider=source_id,
                pool=call.pool,
                secret_store=call.secret_store,
                http=call.http,
                tenant_id=call.tenant_id,
                install_row_id=call.installation_id,
                refresh_secret_ref=install["refresh_secret_ref"],
                now=now,
                renewal_lease=lease,
                minimum_expires_in_seconds=renewal.renewal_window_seconds,
                request_binding=call.request_binding,
            )
        except OAuthRefreshError as exc:
            if exc.status in {400, 401, 403}:
                raise RenewalReauthorizationRequired(
                    "credential_reauthorization_required"
                ) from exc
            if exc.status == 422:
                raise RenewalManualRepairRequired(
                    "credential_expiry_invalid"
                ) from exc
            raise
        return RenewalAttempt(
            state="renewed",
            next_attempt_at=renewal_next_attempt_at(
                refreshed.expires_at,
                now=now,
                renewal_window_seconds=renewal.renewal_window_seconds,
                error_code="credential_expiry_invalid",
            ),
            expires_at=refreshed.expires_at,
        )

    return await run_bounded_renewal(
        invocation,
        source_id=source_id,
        expected_kind="credential",
        attempt=attempt,
    )


__all__ = [
    "RenewalAttempt",
    "RenewalAttemptCallable",
    "RenewalInvocation",
    "RenewalInvocationError",
    "RenewalManualRepairRequired",
    "RenewalOutcome",
    "RenewalReauthorizationRequired",
    "run_bounded_renewal",
    "run_credential_renewal",
    "renewal_next_attempt_at",
]
