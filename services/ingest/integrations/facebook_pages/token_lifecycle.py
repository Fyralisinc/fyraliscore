"""Exact-installation Facebook Page access-token recovery.

Meta does not expose a refresh grant for Page access tokens.  The supported
recovery is:

1. retain the long-lived User access token returned during Facebook Login;
2. call ``/me/accounts`` with that still-valid User token;
3. select the *same* Page id as the installation;
4. write a replacement Page secret before atomically swapping the DB ref.

If the User token is missing, expired, invalid, or no longer owns the Page,
Fyralis records ``reauthorization_required``.  It never attempts to exchange an
expired token and never falls back to another Page or installation.
"""
from __future__ import annotations

import inspect
import os
import socket
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable
from uuid import UUID

import structlog

from lib.shared.errors import SecretNotFoundError, SecretStoreError
from lib.shared.provider_transport import (
    ProviderPermanentError,
    ProviderTransientError,
    RequestContext,
    RetryLater,
    RetryReason,
)


log = structlog.get_logger("integrations.facebook_pages.token_lifecycle")

SOURCE = "facebook_pages"
CONNECTED = "connected"
DEGRADED = "degraded"
REAUTHORIZATION_REQUIRED = "reauthorization_required"

_LEASE_SECONDS = 120
_BASE_RETRY_SECONDS = 30
_MAX_RETRY_SECONDS = 60 * 60
_REAUTHORIZATION_POLL_SECONDS = 6 * 60 * 60

ClientFactory = Callable[[Any, str], Any | Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class RecoverySchedule:
    state: str
    not_before: datetime | None
    stale_page_token_ref: bool = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _worker_id() -> str:
    return (
        f"facebook-page-token@{socket.gethostname()}:{os.getpid()}"
    )


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, TypeError):
        return default


def _aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Facebook token timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _request_context(
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    operation: str,
) -> RequestContext:
    return RequestContext(
        source=SOURCE,
        operation=operation,
        tenant_id=str(tenant_id),
        installation_id=str(installation_row_id),
    )


def _retry_later(
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    operation: str,
    not_before: datetime,
    now: datetime,
    cause_code: str,
) -> RetryLater:
    not_before = _aware_utc(not_before) or now
    delay = max(0.0, (not_before - now).total_seconds())
    return RetryLater.after(
        request_context=_request_context(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
        ),
        delay_seconds=delay,
        reason=RetryReason.TRANSIENT,
        now=now,
        cause_code=cause_code,
    )


def _reauthorization_retry(
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    operation: str,
    now: datetime,
) -> RetryLater:
    return _retry_later(
        tenant_id=tenant_id,
        installation_row_id=installation_row_id,
        operation=operation,
        not_before=now + timedelta(seconds=_REAUTHORIZATION_POLL_SECONDS),
        now=now,
        cause_code="facebook_pages_reauthorization_required",
    )


async def _load_exact_installation(
    pool: Any,
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
) -> Any | None:
    return await pool.fetchrow(
        """
        SELECT id, tenant_id, page_id, page_access_token_ref,
               user_access_token_ref, user_token_expires_at,
               connection_state, enabled,
               page_token_recovery_next_attempt_at,
               page_token_recovery_attempts,
               page_recovery_lease_owner,
               page_token_recovery_lease_until
          FROM facebook_page_installations
         WHERE id = $1
           AND tenant_id = $2
        """,
        installation_row_id,
        tenant_id,
    )


def _require_enabled_exact_installation(
    row: Any | None,
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    operation: str,
) -> Any:
    if row is None or not bool(_row_value(row, "enabled", False)):
        raise ProviderPermanentError(
            "Facebook Page installation is missing or disabled",
            source=SOURCE,
            operation=operation,
            tenant_id=str(tenant_id),
            installation_id=str(installation_row_id),
        )
    return row


async def _resolve_secret(
    secret_store: Any,
    ref: str,
    *,
    tenant_id: UUID,
) -> str:
    raw = await secret_store.get(ref, tenant_id=tenant_id)
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


async def _mark_reauthorization_required(
    pool: Any,
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    error_code: str,
    now: datetime,
) -> None:
    await pool.execute(
        """
        UPDATE facebook_page_installations
           SET connection_state = 'reauthorization_required',
               reauthorization_required_at =
                   COALESCE(reauthorization_required_at, $3),
               page_token_recovery_next_attempt_at = NULL,
               page_token_recovery_last_attempt_at = $3,
               page_recovery_last_error_code = $4,
               page_recovery_lease_owner = NULL,
               page_token_recovery_lease_until = NULL,
               updated_at = $3
         WHERE id = $1
           AND tenant_id = $2
           AND enabled = TRUE
        """,
        installation_row_id,
        tenant_id,
        now,
        error_code,
    )


def _backoff_seconds(attempts: int) -> int:
    exponent = min(max(attempts - 1, 0), 8)
    return min(_MAX_RETRY_SECONDS, _BASE_RETRY_SECONDS * (2**exponent))


async def _record_retry(
    pool: Any,
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    not_before: datetime,
    error_code: str,
    now: datetime,
) -> None:
    await pool.execute(
        """
        UPDATE facebook_page_installations
           SET connection_state = 'degraded',
               page_token_recovery_next_attempt_at = $3,
               page_token_recovery_last_attempt_at = $4,
               page_recovery_last_error_code = $5,
               page_recovery_lease_owner = NULL,
               page_token_recovery_lease_until = NULL,
               updated_at = $4
         WHERE id = $1
           AND tenant_id = $2
           AND enabled = TRUE
        """,
        installation_row_id,
        tenant_id,
        _aware_utc(not_before),
        now,
        error_code,
    )


async def schedule_page_token_recovery(
    pool: Any,
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    expected_page_token_ref: str,
    graph_error_subcode: int | None,
    now: datetime | None = None,
) -> RecoverySchedule:
    """Schedule recovery for the exact installation whose token failed.

    The expected secret ref is part of the compare condition.  A late response
    from an old request therefore cannot degrade a concurrently rotated token.
    """

    now = _aware_utc(now) or _utcnow()
    controlled_error = (
        "graph_access_token_invalid"
        if graph_error_subcode is None
        else f"graph_access_token_invalid_subcode_{graph_error_subcode}"
    )
    row = await pool.fetchrow(
        """
        UPDATE facebook_page_installations
           SET connection_state = CASE
                   WHEN user_access_token_ref IS NULL
                     OR user_token_expires_at IS NULL
                     OR user_token_expires_at <= $4
                   THEN 'reauthorization_required'
                   ELSE 'degraded'
               END,
               reauthorization_required_at = CASE
                   WHEN user_access_token_ref IS NULL
                     OR user_token_expires_at IS NULL
                     OR user_token_expires_at <= $4
                   THEN COALESCE(reauthorization_required_at, $4)
                   ELSE NULL
               END,
               page_token_recovery_next_attempt_at = CASE
                   WHEN user_access_token_ref IS NULL
                     OR user_token_expires_at IS NULL
                     OR user_token_expires_at <= $4
                   THEN NULL
                   ELSE LEAST(
                       COALESCE(page_token_recovery_next_attempt_at, $4),
                       $4
                   )
               END,
               page_recovery_last_error_code = $5,
               page_recovery_lease_owner = NULL,
               page_token_recovery_lease_until = NULL,
               updated_at = $4
         WHERE id = $1
           AND tenant_id = $2
           AND page_access_token_ref = $3
           AND enabled = TRUE
        RETURNING connection_state, page_token_recovery_next_attempt_at
        """,
        installation_row_id,
        tenant_id,
        expected_page_token_ref,
        now,
        controlled_error,
    )
    if row is not None:
        return RecoverySchedule(
            state=str(row["connection_state"]),
            not_before=_aware_utc(
                _row_value(row, "page_token_recovery_next_attempt_at"),
            ),
        )

    current = _require_enabled_exact_installation(
        await _load_exact_installation(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
        ),
        tenant_id=tenant_id,
        installation_row_id=installation_row_id,
        operation="pages.list",
    )
    stale = _row_value(current, "page_access_token_ref") != expected_page_token_ref
    return RecoverySchedule(
        state=str(_row_value(current, "connection_state")),
        not_before=_aware_utc(
            _row_value(current, "page_token_recovery_next_attempt_at"),
        ),
        stale_page_token_ref=stale,
    )


async def _claim_recovery(
    pool: Any,
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    worker_id: str,
    now: datetime,
) -> Any | None:
    return await pool.fetchrow(
        """
        UPDATE facebook_page_installations
           SET page_recovery_lease_owner = $3,
               page_token_recovery_lease_until =
                   $4 + make_interval(secs => $5),
               page_token_recovery_attempts =
                   page_token_recovery_attempts + 1,
               page_token_recovery_last_attempt_at = $4,
               updated_at = $4
         WHERE id = $1
           AND tenant_id = $2
           AND enabled = TRUE
           AND connection_state = 'degraded'
           AND page_token_recovery_next_attempt_at IS NOT NULL
           AND page_token_recovery_next_attempt_at <= $4
           AND (
               page_token_recovery_lease_until IS NULL
               OR page_token_recovery_lease_until <= $4
               OR page_recovery_lease_owner = $3
           )
        RETURNING id, tenant_id, page_id, page_access_token_ref,
                  user_access_token_ref, user_token_expires_at,
                  connection_state, enabled,
                  page_token_recovery_next_attempt_at,
                  page_token_recovery_attempts,
                  page_recovery_lease_owner,
                  page_token_recovery_lease_until
        """,
        installation_row_id,
        tenant_id,
        worker_id,
        now,
        _LEASE_SECONDS,
    )


async def _default_client_factory(row: Any, user_token: str) -> Any:
    from services.ingest.integrations.facebook_pages.client import (
        FacebookPagesClient,
    )
    from services.ingest.integrations.provider_transport_runtime import (
        get_provider_transport_runtime,
    )

    runtime = get_provider_transport_runtime()
    return FacebookPagesClient(
        access_token=user_token,
        tenant_id=row["tenant_id"],
        installation_row_id=row["id"],
        provider_transport=(
            runtime.transport if runtime is not None else None
        ),
        quota_resolver=(
            runtime.quota_resolver if runtime is not None else None
        ),
        allow_unlimited_local=runtime is None,
    )


async def _make_client(
    factory: ClientFactory | None,
    row: Any,
    user_token: str,
) -> Any:
    value = (factory or _default_client_factory)(row, user_token)
    return await value if inspect.isawaitable(value) else value


async def _close_client(client: Any) -> None:
    close = getattr(client, "aclose", None)
    if close is not None:
        await close()


async def _cleanup_new_secret(
    secret_store: Any,
    ref: str,
    *,
    tenant_id: UUID,
) -> None:
    try:
        await secret_store.delete(ref, tenant_id=tenant_id)
    except Exception as exc:  # noqa: BLE001 - old DB ref remains authoritative
        log.warning(
            "facebook_pages_new_secret_cleanup_failed",
            installation_id=None,
            error_type=type(exc).__name__,
        )


async def recover_page_access_token(
    pool: Any,
    secret_store: Any,
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    operation: str,
    now: datetime | None = None,
    worker_id: str | None = None,
    client_factory: ClientFactory | None = None,
) -> str:
    """Re-derive and atomically install one exact Page token.

    A replacement secret is written first.  The DB swap is compare-and-set on
    tenant, installation id, Page ref, and User ref.  The prior Page secret is
    deleted only after that swap commits; every failure path keeps it intact.
    """

    now = _aware_utc(now) or _utcnow()
    worker_id = worker_id or _worker_id()
    row = await _claim_recovery(
        pool,
        tenant_id=tenant_id,
        installation_row_id=installation_row_id,
        worker_id=worker_id,
        now=now,
    )
    if row is None:
        current = _require_enabled_exact_installation(
            await _load_exact_installation(
                pool,
                tenant_id=tenant_id,
                installation_row_id=installation_row_id,
            ),
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
        )
        state = str(_row_value(current, "connection_state"))
        if state == CONNECTED:
            return await _resolve_secret(
                secret_store,
                str(current["page_access_token_ref"]),
                tenant_id=tenant_id,
            )
        if state == REAUTHORIZATION_REQUIRED:
            raise _reauthorization_retry(
                tenant_id=tenant_id,
                installation_row_id=installation_row_id,
                operation=operation,
                now=now,
            )
        not_before = max(
            filter(
                None,
                (
                    _aware_utc(
                        _row_value(
                            current,
                            "page_token_recovery_next_attempt_at",
                        ),
                    ),
                    _aware_utc(
                        _row_value(current, "page_token_recovery_lease_until"),
                    ),
                ),
            ),
            default=now + timedelta(seconds=_BASE_RETRY_SECONDS),
        )
        raise _retry_later(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            not_before=not_before,
            now=now,
            cause_code="facebook_pages_token_recovery_not_due",
        )

    user_ref = _row_value(row, "user_access_token_ref")
    user_expires_at = _aware_utc(_row_value(row, "user_token_expires_at"))
    if not user_ref or user_expires_at is None or user_expires_at <= now:
        await _mark_reauthorization_required(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            error_code=(
                "long_lived_user_token_missing"
                if not user_ref or user_expires_at is None
                else "long_lived_user_token_expired"
            ),
            now=now,
        )
        raise _reauthorization_retry(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            now=now,
        )

    try:
        user_token = await _resolve_secret(
            secret_store,
            str(user_ref),
            tenant_id=tenant_id,
        )
    except SecretNotFoundError:
        await _mark_reauthorization_required(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            error_code="long_lived_user_token_missing",
            now=now,
        )
        raise _reauthorization_retry(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            now=now,
        )
    except SecretStoreError:
        delay = _backoff_seconds(int(row["page_token_recovery_attempts"]))
        not_before = now + timedelta(seconds=delay)
        await _record_retry(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            not_before=not_before,
            error_code="secret_store_unavailable",
            now=now,
        )
        raise _retry_later(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            not_before=not_before,
            now=now,
            cause_code="facebook_pages_secret_store_unavailable",
        )

    client = await _make_client(client_factory, row, user_token)
    try:
        pages = await client.list_pages(user_token)
    except RetryLater as exc:
        await _record_retry(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            not_before=exc.not_before,
            error_code="provider_cooldown",
            now=now,
        )
        raise
    except ProviderTransientError:
        delay = _backoff_seconds(int(row["page_token_recovery_attempts"]))
        not_before = now + timedelta(seconds=delay)
        await _record_retry(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            not_before=not_before,
            error_code="provider_transient_error",
            now=now,
        )
        raise _retry_later(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            not_before=not_before,
            now=now,
            cause_code="facebook_pages_provider_transient_error",
        )
    except ProviderPermanentError as exc:
        if getattr(exc, "graph_error_code", None) == 190:
            await _mark_reauthorization_required(
                pool,
                tenant_id=tenant_id,
                installation_row_id=installation_row_id,
                error_code="long_lived_user_token_invalid",
                now=now,
            )
            raise _reauthorization_retry(
                tenant_id=tenant_id,
                installation_row_id=installation_row_id,
                operation=operation,
                now=now,
            )
        delay = _MAX_RETRY_SECONDS
        not_before = now + timedelta(seconds=delay)
        await _record_retry(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            not_before=not_before,
            error_code="page_list_permanent_error",
            now=now,
        )
        raise _retry_later(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            not_before=not_before,
            now=now,
            cause_code="facebook_pages_page_list_permanent_error",
        )
    finally:
        await _close_client(client)

    page_id = str(row["page_id"])
    exact_page = next(
        (
            page
            for page in pages
            if str(page.get("id") or "") == page_id
            and isinstance(page.get("access_token"), str)
            and bool(page["access_token"])
        ),
        None,
    )
    if exact_page is None:
        await _mark_reauthorization_required(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            error_code="owning_page_not_returned",
            now=now,
        )
        raise _reauthorization_retry(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            now=now,
        )

    replacement_token = str(exact_page["access_token"])
    try:
        replacement_ref = await secret_store.put(
            replacement_token,
            label=f"facebook_pages_page_token:{page_id}",
            tenant_id=tenant_id,
        )
    except SecretStoreError:
        delay = _backoff_seconds(int(row["page_token_recovery_attempts"]))
        not_before = now + timedelta(seconds=delay)
        await _record_retry(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            not_before=not_before,
            error_code="secret_store_unavailable",
            now=now,
        )
        raise _retry_later(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            not_before=not_before,
            now=now,
            cause_code="facebook_pages_secret_store_unavailable",
        )

    old_page_ref = str(row["page_access_token_ref"])
    try:
        swapped = await pool.fetchrow(
            """
            UPDATE facebook_page_installations
               SET page_access_token_ref = $3,
                   connection_state = 'connected',
                   reauthorization_required_at = NULL,
                   page_token_recovery_next_attempt_at = NULL,
                   page_token_recovery_attempts = 0,
                   page_recovery_last_error_code = NULL,
                   page_recovery_lease_owner = NULL,
                   page_token_recovery_lease_until = NULL,
                   page_token_rotated_at = $6,
                   updated_at = $6
             WHERE id = $1
               AND tenant_id = $2
               AND page_access_token_ref = $4
               AND user_access_token_ref = $5
               AND enabled = TRUE
            RETURNING page_access_token_ref
            """,
            installation_row_id,
            tenant_id,
            replacement_ref,
            old_page_ref,
            user_ref,
            now,
        )
    except Exception:
        await _cleanup_new_secret(
            secret_store,
            replacement_ref,
            tenant_id=tenant_id,
        )
        raise

    if swapped is None:
        await _cleanup_new_secret(
            secret_store,
            replacement_ref,
            tenant_id=tenant_id,
        )
        current = _require_enabled_exact_installation(
            await _load_exact_installation(
                pool,
                tenant_id=tenant_id,
                installation_row_id=installation_row_id,
            ),
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
        )
        if (
            _row_value(current, "connection_state") == CONNECTED
            and _row_value(current, "page_access_token_ref") != old_page_ref
        ):
            return await _resolve_secret(
                secret_store,
                str(current["page_access_token_ref"]),
                tenant_id=tenant_id,
            )
        not_before = now + timedelta(seconds=_BASE_RETRY_SECONDS)
        await _record_retry(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            not_before=not_before,
            error_code="page_token_compare_and_set_lost",
            now=now,
        )
        raise _retry_later(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            not_before=not_before,
            now=now,
            cause_code="facebook_pages_page_token_compare_and_set_lost",
        )

    if old_page_ref != replacement_ref:
        try:
            await secret_store.delete(old_page_ref, tenant_id=tenant_id)
        except Exception as exc:  # noqa: BLE001 - new ref is already committed
            log.warning(
                "facebook_pages_prior_secret_cleanup_failed",
                installation_id=str(installation_row_id),
                error_type=type(exc).__name__,
            )
    log.info(
        "facebook_pages_page_token_recovered",
        tenant_id=str(tenant_id),
        installation_id=str(installation_row_id),
        page_id=page_id,
    )
    return replacement_token


async def page_access_token_for_request(
    pool: Any,
    secret_store: Any,
    *,
    tenant_id: UUID,
    installation_row_id: UUID,
    operation: str,
    now: datetime | None = None,
    client_factory: ClientFactory | None = None,
) -> tuple[str, str]:
    """Return the current Page token or honor its durable recovery schedule."""

    now = _aware_utc(now) or _utcnow()
    row = _require_enabled_exact_installation(
        await _load_exact_installation(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
        ),
        tenant_id=tenant_id,
        installation_row_id=installation_row_id,
        operation=operation,
    )
    state = str(_row_value(row, "connection_state"))
    if (
        state == CONNECTED
        and (
            not _row_value(row, "user_access_token_ref")
            or _row_value(row, "user_token_expires_at") is None
        )
    ):
        await _mark_reauthorization_required(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            error_code="long_lived_user_token_missing",
            now=now,
        )
        raise _reauthorization_retry(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            now=now,
        )
    if state == REAUTHORIZATION_REQUIRED:
        raise _reauthorization_retry(
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            now=now,
        )
    if state == DEGRADED:
        token = await recover_page_access_token(
            pool,
            secret_store,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
            now=now,
            client_factory=client_factory,
        )
        refreshed = _require_enabled_exact_installation(
            await _load_exact_installation(
                pool,
                tenant_id=tenant_id,
                installation_row_id=installation_row_id,
            ),
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            operation=operation,
        )
        return token, str(refreshed["page_access_token_ref"])
    if state != CONNECTED:
        raise ProviderPermanentError(
            "Facebook Page installation has an unknown credential state",
            source=SOURCE,
            operation=operation,
            tenant_id=str(tenant_id),
            installation_id=str(installation_row_id),
            connection_state=state,
        )
    page_ref = str(row["page_access_token_ref"])
    return (
        await _resolve_secret(secret_store, page_ref, tenant_id=tenant_id),
        page_ref,
    )


__all__ = [
    "CONNECTED",
    "DEGRADED",
    "REAUTHORIZATION_REQUIRED",
    "RecoverySchedule",
    "page_access_token_for_request",
    "recover_page_access_token",
    "schedule_page_token_recovery",
]
