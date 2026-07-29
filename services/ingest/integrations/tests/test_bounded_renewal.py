"""Database-backed R2 checks for contract-owned bounded renewal invokers."""
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import httpx
import pytest

from lib.shared.ids import uuid7
from lib.shared.errors import SecretNotFoundError
from lib.shared.provider_transport import (
    ProviderRetryForbiddenError,
    ProviderTransport,
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.ingestion.renewal_jobs import (
    RenewalJobKey,
    RenewalLease,
    RenewalLeaseLost,
    claim_due_renewal_job,
    defer_renewal_job,
    get_renewal_job,
    mark_renewal_provider_call_started,
    require_renewal_manual_reconciliation,
)
from services.ingest.integrations import _google_watch as google_watch
from services.ingest.integrations.bounded_renewal import (
    RenewalAttempt,
    RenewalInvocation,
    RenewalManualRepairRequired,
    run_bounded_renewal,
)
from services.ingest.integrations.gmail import dwd as gmail_dwd
from services.ingest.integrations.gmail import watch_scheduler as gmail_watch_scheduler
from services.ingest.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GmailClient,
    GoogleHttpClient,
)
from services.ingest.integrations.google_calendar import watch as calendar_watch
from services.ingest.integrations.google_calendar.client import (
    CALENDAR_READONLY_SCOPE,
    GoogleCalendarClient,
)
from services.ingest.integrations.google_drive import watch as drive_watch
from services.ingest.integrations.google_drive.client import (
    DRIVE_READONLY_SCOPE,
    GoogleDriveClient,
)
from services.ingest.integrations.oauth_refresh import (
    OAuthRefreshError,
    REFRESH_CONFIGS,
    ensure_fresh_access_token,
    refresh_and_persist,
)
from services.ingest.integrations.provider_transport import ProviderRequestBinding
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS, source_definition
from services.ingest.source_contract.runtime import resolve_renewal_invoker
from services.ingest.synthetic.provider_lab import build_provider_lab_app


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_CREDENTIAL_RENEWAL_SOURCES = tuple(
    source.source_id
    for source in SOURCE_DEFINITIONS
    if source.renewal is not None and source.renewal.kind == "credential"
)
_R2_RENEWAL_SOURCES = tuple(
    source.source_id
    for source in SOURCE_DEFINITIONS
    if source.renewal is not None
)
_INSTALL_IDENTIFIER_COLUMNS = (
    "realm_id",
    "business_id",
    "company_uuid",
    "firm_id",
    "organization_urn",
)


class _MemorySecretStore:
    def __init__(self, initial: dict[str, str]) -> None:
        self._values = dict(initial)
        self._counter = 0
        self.deleted: list[str] = []

    async def get(self, ref: str, *, tenant_id: Any) -> bytes:  # noqa: ARG002
        try:
            value = self._values[ref]
        except KeyError as exc:
            raise SecretNotFoundError("test secret ref is unavailable") from exc
        return value.encode("utf-8")

    async def put(
        self,
        plaintext: bytes | str,
        *,
        label: str,
        tenant_id: Any,  # noqa: ARG002
    ) -> str:
        self._counter += 1
        ref = f"renewed-ref-{self._counter}"
        self._values[ref] = (
            plaintext.decode("utf-8")
            if isinstance(plaintext, bytes)
            else plaintext
        )
        return ref

    async def delete(self, ref: str, *, tenant_id: Any) -> None:  # noqa: ARG002
        self.deleted.append(ref)
        self._values.pop(ref, None)


class _RecordingProviderTransport:
    """Observe real universal-transport calls without replacing its behavior."""

    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []
        self._delegate = ProviderTransport()

    async def execute(
        self,
        request_context: RequestContext,
        policy: Any,
        call: Any,
    ) -> Any:
        self.contexts.append(request_context)
        return await self._delegate.execute(request_context, policy, call)


async def _seed_credential_installation(
    pool: asyncpg.Pool,
    *,
    source_id: str,
    expires_at: datetime,
    tenant_id: Any | None = None,
) -> tuple[Any, Any, _MemorySecretStore]:
    source = source_definition(source_id)
    declaration = source.credential_refresh
    assert declaration is not None
    config = REFRESH_CONFIGS[source_id]
    tenant_id = tenant_id or uuid7()
    install_id = uuid7()
    existing_tenant = await pool.fetchval(
        "SELECT 1 FROM tenants WHERE id = $1",
        tenant_id,
    )
    if existing_tenant is None:
        await pool.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2)",
            tenant_id,
            f"bounded-renewal-{source_id}-{tenant_id}",
        )
    columns = {
        str(row["column_name"])
        for row in await pool.fetch(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = $1
            """,
            declaration.install_table,
        )
    }
    identifier_column = next(
        column for column in _INSTALL_IDENTIFIER_COLUMNS if column in columns
    )
    refresh_ref = "initial-refresh-ref"
    refresh_value = "initial-refresh-token"
    if config.client_credentials_from_install:
        refresh_value = '{"client_id":"installed-client","client_secret":"installed-secret"}'
    elif config.client_secret_from_install:
        refresh_value = "installed-client-secret"
    store = _MemorySecretStore(
        {
            "initial-access-ref": "initial-access-token",
            refresh_ref: refresh_value,
        }
    )
    await pool.execute(
        f"""
        INSERT INTO {declaration.install_table} (
            id, tenant_id, {identifier_column}, base_url, secret_ref,
            refresh_secret_ref, token_expires_at
        )
        VALUES ($1, $2, $3, 'https://provider-lab.invalid', $4, $5, $6)
        """,
        install_id,
        tenant_id,
        f"renewal-{source_id}-{install_id}",
        "initial-access-ref",
        refresh_ref,
        expires_at,
    )
    return tenant_id, install_id, store


def _token_http(
    source_id: str,
    calls: list[dict[str, str]],
    *,
    expires_in: int | None = None,
) -> httpx.AsyncClient:
    config = REFRESH_CONFIGS[source_id]

    def handler(request: httpx.Request) -> httpx.Response:
        form = dict(httpx.QueryParams(request.content.decode("utf-8")))
        calls.append(form)
        payload: dict[str, Any] = {
            "access_token": f"renewed-access-{source_id}",
            "expires_in": (
                config.default_expires_in if expires_in is None else expires_in
            ),
            "token_type": "Bearer",
        }
        if config.grant_type == "refresh_token":
            payload["refresh_token"] = f"renewed-refresh-{source_id}"
        return httpx.Response(200, json=payload)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _blocking_token_http(
    source_id: str,
    *,
    entered: asyncio.Event,
    release: asyncio.Event,
    calls: list[dict[str, str]],
) -> httpx.AsyncClient:
    """Hold one real renewal exchange open to prove lease single-writer flow."""

    config = REFRESH_CONFIGS[source_id]

    async def handler(request: httpx.Request) -> httpx.Response:
        form = dict(httpx.QueryParams(request.content.decode("utf-8")))
        calls.append(form)
        entered.set()
        await release.wait()
        payload: dict[str, Any] = {
            "access_token": f"renewed-access-{source_id}",
            "expires_in": config.default_expires_in,
            "token_type": "Bearer",
        }
        if config.grant_type == "refresh_token":
            payload["refresh_token"] = f"renewed-refresh-{source_id}"
        return httpx.Response(200, json=payload)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def _superseded_lease(
    pool: asyncpg.Pool,
    key: RenewalJobKey,
) -> RenewalLease:
    """Return a real old lease after a replacement generation has won it."""

    stale = await claim_due_renewal_job(
        pool,
        key,
        owner="renewal-test-stale-owner",
        initial_not_before=datetime.now(timezone.utc),
    )
    assert stale is not None
    await pool.execute(
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
    replacement = await claim_due_renewal_job(
        pool,
        key,
        owner="renewal-test-replacement-owner",
    )
    assert replacement is not None
    assert replacement.version > stale.version
    return stale


@pytest.mark.parametrize("source_id", _CREDENTIAL_RENEWAL_SOURCES)
async def test_credential_renewal_is_exact_due_only_and_secret_free(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
) -> None:
    """All five catalog bindings obey before/due scheduling and lease fencing."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id, install_id, store = await _seed_credential_installation(
        fresh_db,
        source_id=source_id,
        expires_at=now + timedelta(hours=1),
    )
    monkeypatch.setenv(f"{source_id.upper()}_CLIENT_ID", "test-client-id")
    monkeypatch.setenv(f"{source_id.upper()}_CLIENT_SECRET", "test-client-secret")
    calls: list[dict[str, str]] = []
    async with _token_http(source_id, calls) as http:
        invocation = RenewalInvocation(
            pool=fresh_db,
            tenant_id=tenant_id,
            installation_id=install_id,
            target_key="installation",
            secret_store=store,
            http=http,
            worker_id=f"renewal-test-{source_id}",
            now=now,
        )
        invoker = resolve_renewal_invoker(source_id)

        # First invocation seeds durable state but is before the renewal window.
        before = await invoker(invocation)
        assert before.state == "not_due"
        assert calls == []

        source = source_definition(source_id)
        declaration = source.credential_refresh
        assert declaration is not None
        await fresh_db.execute(
            f"""
            UPDATE {declaration.install_table}
               SET token_expires_at = $1
             WHERE id = $2 AND tenant_id = $3
            """,
            now + timedelta(seconds=30),
            install_id,
            tenant_id,
        )
        await fresh_db.execute(
            """
            UPDATE source_renewal_jobs
               SET next_attempt_at = now() - interval '1 second'
             WHERE source_id = $1
               AND tenant_id = $2
               AND installation_id = $3
               AND target_key = 'installation'
            """,
            source_id,
            tenant_id,
            install_id,
        )

        renewed = await invoker(invocation)

    assert renewed.state == "renewed"
    assert len(calls) == 1
    if REFRESH_CONFIGS[source_id].grant_type == "refresh_token":
        assert calls[0]["grant_type"] == "refresh_token"
        assert calls[0]["refresh_token"] == "initial-refresh-token"
    else:
        assert calls[0]["grant_type"] == "client_credentials"
        assert "refresh_token" not in calls[0]
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=install_id,
            target_key="installation",
        ),
    )
    assert job is not None
    assert job.state == "pending"
    assert job.last_success_at is not None
    assert job.last_error_code is None
    assert job.next_attempt_at is not None
    assert "renewed-access" not in repr(job)
    assert "renewed-access" not in repr(renewed)
    source = source_definition(source_id)
    declaration = source.credential_refresh
    assert declaration is not None
    installed = await fresh_db.fetchrow(
        f"""
        SELECT secret_ref, refresh_secret_ref, token_expires_at
          FROM {declaration.install_table}
         WHERE id = $1 AND tenant_id = $2
        """,
        install_id,
        tenant_id,
    )
    assert installed is not None
    assert installed["secret_ref"] != "initial-access-ref"
    assert installed["token_expires_at"] is not None
    if REFRESH_CONFIGS[source_id].rotates_refresh_token:
        assert installed["refresh_secret_ref"] != "initial-refresh-ref"
    else:
        assert installed["refresh_secret_ref"] == "initial-refresh-ref"
    assert store.deleted == []


@pytest.mark.parametrize("source_id", _CREDENTIAL_RENEWAL_SOURCES)
async def test_credential_renewal_never_mutates_a_sibling_installation(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
) -> None:
    """A source renewal changes only the exact bound installation row."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id = uuid7()
    first_tenant, first_installation, first_store = (
        await _seed_credential_installation(
            fresh_db,
            source_id=source_id,
            expires_at=now + timedelta(seconds=30),
            tenant_id=tenant_id,
        )
    )
    sibling_tenant, sibling_installation, _sibling_store = (
        await _seed_credential_installation(
            fresh_db,
            source_id=source_id,
            expires_at=now + timedelta(seconds=30),
            tenant_id=tenant_id,
        )
    )
    assert first_tenant == sibling_tenant == tenant_id
    monkeypatch.setenv(f"{source_id.upper()}_CLIENT_ID", "test-client-id")
    monkeypatch.setenv(
        f"{source_id.upper()}_CLIENT_SECRET",
        "test-client-secret",
    )
    calls: list[dict[str, str]] = []
    async with _token_http(source_id, calls) as http:
        outcome = await resolve_renewal_invoker(source_id)(
            RenewalInvocation(
                pool=fresh_db,
                tenant_id=tenant_id,
                installation_id=first_installation,
                target_key="installation",
                secret_store=first_store,
                http=http,
                worker_id=f"renewal-test-sibling-{source_id}",
                now=now,
            ),
        )

    assert outcome.state == "renewed"
    assert len(calls) == 1
    source = source_definition(source_id)
    declaration = source.credential_refresh
    assert declaration is not None
    first = await fresh_db.fetchrow(
        f"""
        SELECT secret_ref, token_expires_at
          FROM {declaration.install_table}
         WHERE id = $1 AND tenant_id = $2
        """,
        first_installation,
        tenant_id,
    )
    sibling = await fresh_db.fetchrow(
        f"""
        SELECT secret_ref, token_expires_at
          FROM {declaration.install_table}
         WHERE id = $1 AND tenant_id = $2
        """,
        sibling_installation,
        tenant_id,
    )
    assert first is not None
    assert sibling is not None
    assert first["secret_ref"] != "initial-access-ref"
    assert sibling["secret_ref"] == "initial-access-ref"
    assert sibling["token_expires_at"] == now + timedelta(seconds=30)
    assert await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=sibling_installation,
            target_key="installation",
        ),
    ) is None


@pytest.mark.parametrize("source_id", _CREDENTIAL_RENEWAL_SOURCES)
async def test_reactive_credential_failure_can_pull_forward_only_pending_renewal(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
) -> None:
    """A real 401 path reuses the lease without waiting for normal cadence."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id, install_id, store = await _seed_credential_installation(
        fresh_db,
        source_id=source_id,
        expires_at=now + timedelta(hours=1),
    )
    monkeypatch.setenv(f"{source_id.upper()}_CLIENT_ID", "test-client-id")
    monkeypatch.setenv(
        f"{source_id.upper()}_CLIENT_SECRET",
        "test-client-secret",
    )
    calls: list[dict[str, str]] = []
    async with _token_http(source_id, calls) as http:
        scheduled = await resolve_renewal_invoker(source_id)(
            RenewalInvocation(
                pool=fresh_db,
                tenant_id=tenant_id,
                installation_id=install_id,
                target_key="installation",
                secret_store=store,
                http=http,
                worker_id=f"renewal-test-reactive-schedule-{source_id}",
                now=now,
            ),
        )
        refreshed = await ensure_fresh_access_token(
            provider=source_id,
            pool=fresh_db,
            secret_store=store,
            http=http,
            tenant_id=tenant_id,
            install_row_id=install_id,
            current_access_ref="initial-access-ref",
            refresh_secret_ref="initial-refresh-ref",
            token_expires_at=now + timedelta(hours=1),
            force=True,
            now=now,
        )

    assert scheduled.state == "not_due"
    assert refreshed == f"renewed-access-{source_id}"
    assert len(calls) == 1
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=install_id,
            target_key="installation",
        ),
    )
    assert job is not None
    assert job.state == "pending"
    assert job.attempt_count == 2


@pytest.mark.parametrize("source_id", _CREDENTIAL_RENEWAL_SOURCES)
async def test_concurrent_credential_renewal_makes_one_provider_exchange(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
) -> None:
    """Two workers on one installation share exactly one fenced exchange."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id, install_id, store = await _seed_credential_installation(
        fresh_db,
        source_id=source_id,
        expires_at=now + timedelta(seconds=30),
    )
    monkeypatch.setenv(f"{source_id.upper()}_CLIENT_ID", "test-client-id")
    monkeypatch.setenv(
        f"{source_id.upper()}_CLIENT_SECRET",
        "test-client-secret",
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    calls: list[dict[str, str]] = []
    async with _blocking_token_http(
        source_id,
        entered=entered,
        release=release,
        calls=calls,
    ) as http:
        first_invocation = RenewalInvocation(
            pool=fresh_db,
            tenant_id=tenant_id,
            installation_id=install_id,
            target_key="installation",
            secret_store=store,
            http=http,
            worker_id=f"renewal-test-concurrent-a-{source_id}",
            now=now,
        )
        invoker = resolve_renewal_invoker(source_id)
        first_task = asyncio.create_task(invoker(first_invocation))
        await asyncio.wait_for(entered.wait(), timeout=5)
        second = await invoker(
            replace(
                first_invocation,
                worker_id=f"renewal-test-concurrent-b-{source_id}",
            ),
        )
        release.set()
        first = await first_task

    assert first.state == "renewed"
    assert second.state == "lease_unavailable"
    assert len(calls) == 1
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=install_id,
            target_key="installation",
        ),
    )
    assert job is not None
    assert job.attempt_count == 1
    assert job.state == "pending"


@pytest.mark.parametrize(
    "source_id",
    ("quickbooks", "gusto", "linkedin"),
)
async def test_missing_rotating_refresh_credential_requires_reauthorization(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
) -> None:
    """A missing rotating refresh ref stops only its exact installation.

    A missing credential is a repairable authorization condition, not an
    unsafe provider-side outcome.  The renewal job must therefore become
    terminal ``reauthorization_required`` without making a token-endpoint
    request or exposing the unavailable ref in durable state.
    """

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id, install_id, store = await _seed_credential_installation(
        fresh_db,
        source_id=source_id,
        expires_at=now + timedelta(seconds=30),
    )
    store._values.pop("initial-refresh-ref")
    monkeypatch.setenv(f"{source_id.upper()}_CLIENT_ID", "test-client-id")
    monkeypatch.setenv(
        f"{source_id.upper()}_CLIENT_SECRET",
        "test-client-secret",
    )
    calls: list[dict[str, str]] = []
    async with _token_http(source_id, calls) as http:
        outcome = await resolve_renewal_invoker(source_id)(
            RenewalInvocation(
                pool=fresh_db,
                tenant_id=tenant_id,
                installation_id=install_id,
                target_key="installation",
                secret_store=store,
                http=http,
                worker_id=f"renewal-test-missing-refresh-{source_id}",
                now=now,
            ),
        )

    assert outcome.state == "reauthorization_required"
    assert outcome.next_attempt_at is None
    assert outcome.error_code == "credential_reauthorization_required"
    assert calls == []
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=install_id,
            target_key="installation",
        ),
    )
    assert job is not None
    assert job.state == "reauthorization_required"
    assert job.next_attempt_at is None
    assert job.reauthorization_required_at is not None
    assert "initial-refresh-ref" not in repr(job)


@pytest.mark.parametrize("source_id", _CREDENTIAL_RENEWAL_SOURCES)
async def test_too_short_credential_response_requires_manual_reconciliation(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
) -> None:
    """A nominal HTTP success cannot install a token inside the safety window."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id, install_id, store = await _seed_credential_installation(
        fresh_db,
        source_id=source_id,
        expires_at=now + timedelta(seconds=30),
    )
    monkeypatch.setenv(f"{source_id.upper()}_CLIENT_ID", "test-client-id")
    monkeypatch.setenv(
        f"{source_id.upper()}_CLIENT_SECRET",
        "test-client-secret",
    )
    calls: list[dict[str, str]] = []
    async with _token_http(source_id, calls, expires_in=60) as http:
        outcome = await resolve_renewal_invoker(source_id)(
            RenewalInvocation(
                pool=fresh_db,
                tenant_id=tenant_id,
                installation_id=install_id,
                target_key="installation",
                secret_store=store,
                http=http,
                worker_id=f"renewal-test-short-credential-{source_id}",
                now=now,
            ),
        )

    assert outcome.state == "manual_reconciliation_required"
    assert outcome.next_attempt_at is None
    assert outcome.error_code == "credential_expiry_invalid"
    assert len(calls) == 1
    source = source_definition(source_id)
    declaration = source.credential_refresh
    assert declaration is not None
    installed = await fresh_db.fetchrow(
        f"""
        SELECT secret_ref, refresh_secret_ref, token_expires_at
          FROM {declaration.install_table}
         WHERE id = $1 AND tenant_id = $2
        """,
        install_id,
        tenant_id,
    )
    assert installed is not None
    assert installed["secret_ref"] == "initial-access-ref"
    assert installed["refresh_secret_ref"] == "initial-refresh-ref"
    assert installed["token_expires_at"] == now + timedelta(seconds=30)
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=install_id,
            target_key="installation",
        ),
    )
    assert job is not None
    assert job.state == "manual_reconciliation_required"
    assert job.next_attempt_at is None
    assert job.last_error_code == "credential_expiry_invalid"
    assert job.manual_reconciliation_required_at is not None


async def _seed_google_watch_resource(
    pool: asyncpg.Pool,
    *,
    source_id: str,
    expiration: datetime,
    tenant_id: Any | None = None,
) -> tuple[Any, Any, Any]:
    tenant_id = tenant_id or uuid7()
    install_id = uuid7()
    resource_id = uuid7()
    installation_suffix = str(install_id)
    workspace_domain = f"renewal-{source_id}-{installation_suffix}.test"
    existing_tenant = await pool.fetchval(
        "SELECT 1 FROM tenants WHERE id = $1",
        tenant_id,
    )
    if existing_tenant is None:
        await pool.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2)",
            tenant_id,
            f"bounded-renewal-{source_id}-{tenant_id}",
        )
    if source_id == "google_calendar":
        await pool.execute(
            """
            INSERT INTO google_calendar_installations (
                id, tenant_id, workspace_domain, service_account_email, scope,
                inclusion_spec
            )
            VALUES ($1, $2, $3, $4,
                    'calendar.readonly', '{}'::jsonb)
            """,
            install_id,
            tenant_id,
            workspace_domain,
            f"svc-{installation_suffix}@{workspace_domain}",
        )
        await pool.execute(
            """
            INSERT INTO google_calendar_calendars (
                id, tenant_id, google_calendar_installation_id, calendar_id,
                owner_email, sync_token, state, watch_state, watch_expiration
            )
            VALUES ($1, $2, $3, 'renewal@calendar.test',
                    'renewal@calendar.test', 'sync-1', 'active', 'active', $4)
            """,
            resource_id,
            tenant_id,
            install_id,
            expiration,
        )
    elif source_id == "google_drive":
        await pool.execute(
            """
            INSERT INTO google_drive_installations (
                id, tenant_id, workspace_domain, service_account_email, scope,
                inclusion_spec
            )
            VALUES ($1, $2, $3, $4,
                    'drive.readonly', '{}'::jsonb)
            """,
            install_id,
            tenant_id,
            workspace_domain,
            f"svc-{installation_suffix}@{workspace_domain}",
        )
        await pool.execute(
            """
            INSERT INTO google_drive_targets (
                id, tenant_id, google_drive_installation_id, drive_kind, drive_id,
                owner_email, start_page_token, state, watch_state, watch_expiration
            )
            VALUES ($1, $2, $3, 'my_drive', 'my-drive', 'renewal@drive.test',
                    'page-1', 'active', 'active', $4)
            """,
            resource_id,
            tenant_id,
            install_id,
            expiration,
        )
    else:  # pragma: no cover - test declarations below are exhaustive
        raise AssertionError(source_id)
    return tenant_id, install_id, resource_id


async def _seed_gmail_watch_resource(
    pool: asyncpg.Pool,
    *,
    expiration: datetime,
    tenant_id: Any | None = None,
) -> tuple[Any, Any, Any, str, str]:
    """Seed one exact Gmail installation, active topic, and mailbox watch."""

    tenant_id = tenant_id or uuid7()
    installation_id = uuid7()
    watch_id = uuid7()
    # Gmail permits sibling installations under one tenant. Keep the fixture's
    # provider scope and mailbox identity distinct so exact-installation tests
    # exercise that supported production topology instead of tripping a
    # source-table uniqueness constraint during setup.
    installation_suffix = str(installation_id)
    workspace_domain = f"renewal-gmail-{installation_suffix}.test"
    email = f"renewal-mailbox-{installation_suffix}@provider-lab.test"
    topic_name = f"projects/provider-lab/topics/gmail-{installation_id}"
    existing_tenant = await pool.fetchval(
        "SELECT 1 FROM tenants WHERE id = $1",
        tenant_id,
    )
    if existing_tenant is None:
        await pool.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2)",
            tenant_id,
            f"bounded-renewal-gmail-{tenant_id}",
        )
    await pool.execute(
        """
        INSERT INTO gmail_installations (
            id, tenant_id, workspace_domain, service_account_email, scope,
            inclusion_spec
        )
        VALUES ($1, $2, $3, $4,
                'gmail.metadata', '{}'::jsonb)
        """,
        installation_id,
        tenant_id,
        workspace_domain,
        f"svc-{installation_suffix}@{workspace_domain}",
    )
    await pool.execute(
        """
        INSERT INTO gmail_pubsub_topics (
            id, tenant_id, gmail_installation_id, topic_name, subscription_name
        )
        VALUES ($1, $2, $3, $4, $5)
        """,
        uuid7(),
        tenant_id,
        installation_id,
        topic_name,
        f"projects/provider-lab/subscriptions/gmail-{installation_id}",
    )
    await pool.execute(
        """
        INSERT INTO gmail_mailbox_watches (
            id, tenant_id, gmail_installation_id, email_address, state,
            history_id, watch_expiration
        )
        VALUES ($1, $2, $3, $4, 'active', '800', $5)
        """,
        watch_id,
        tenant_id,
        installation_id,
        email,
        expiration,
    )
    return tenant_id, installation_id, watch_id, email, topic_name


@pytest.mark.parametrize(
    ("source_id", "watch_module"),
    (
        ("gmail", None),
        ("google_calendar", calendar_watch),
        ("google_drive", drive_watch),
    ),
)
async def test_watch_scheduler_skips_cooldown_and_terminal_jobs_without_starvation(
    fresh_db: asyncpg.Pool,
    source_id: str,
    watch_module: Any | None,
) -> None:
    """Durable renewal state, not source-table order, controls watch eligibility.

    The two earliest resources receive a future retry and a terminal repair
    state. Both must be omitted so due work behind them (including another
    tenant) is selected in the same bounded tick. This protects the watch
    schedulers from repeatedly sampling an unclaimable first batch.
    """

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_a = uuid7()
    tenant_b = uuid7()

    async def seed(tenant_id: Any) -> tuple[Any, Any, Any]:
        if source_id == "gmail":
            seeded_tenant, installation_id, resource_id, _email, _topic = (
                await _seed_gmail_watch_resource(
                    fresh_db,
                    expiration=now + timedelta(seconds=30),
                    tenant_id=tenant_id,
                )
            )
            return seeded_tenant, installation_id, resource_id
        return await _seed_google_watch_resource(
            fresh_db,
            source_id=source_id,
            expiration=now + timedelta(seconds=30),
            tenant_id=tenant_id,
        )

    blocked_retry = await seed(tenant_a)
    blocked_terminal = await seed(tenant_a)
    due_a = await seed(tenant_a)
    due_b = await seed(tenant_b)

    async def claim(source: str, seeded: tuple[Any, Any, Any]) -> RenewalLease:
        tenant_id, installation_id, resource_id = seeded
        lease = await claim_due_renewal_job(
            fresh_db,
            RenewalJobKey(
                source_id=source,
                tenant_id=tenant_id,
                installation_id=installation_id,
                target_key=str(resource_id),
            ),
            owner=f"watch-scheduler-selection-{source}",
            initial_not_before=now - timedelta(seconds=1),
        )
        assert lease is not None
        return lease

    retry_lease = await claim(source_id, blocked_retry)
    await defer_renewal_job(
        fresh_db,
        retry_lease,
        not_before=now + timedelta(hours=1),
        error_code="retry_later:rate_limit",
    )
    terminal_lease = await claim(source_id, blocked_terminal)
    await require_renewal_manual_reconciliation(
        fresh_db,
        terminal_lease,
        error_code="watch_response_invalid",
    )

    if source_id == "gmail":
        rows = await gmail_watch_scheduler._lease_due_watches(fresh_db, limit=2)
        installation_column = "gmail_installation_id"
    else:
        assert watch_module is not None
        rows = await google_watch._lease_due_watches(
            fresh_db,
            watch_module.SPEC,
            limit=2,
        )
        installation_column = "installation_id"

    selected = {
        (row["tenant_id"], row[installation_column], row["id"])
        for row in rows
    }
    assert selected == {due_a, due_b}
    assert (blocked_retry[0], blocked_retry[1], blocked_retry[2]) not in selected
    assert (
        blocked_terminal[0],
        blocked_terminal[1],
        blocked_terminal[2],
    ) not in selected

    if source_id == "gmail":
        # Renewal scheduling no longer writes poll bookkeeping before it wins
        # the exact durable lease. A skipped cooldown cannot suppress later
        # work merely by updating this legacy polling timestamp.
        assert await fresh_db.fetchval(
            "SELECT last_poll_at FROM gmail_mailbox_watches WHERE id = $1",
            due_a[2],
        ) is None


@pytest.mark.parametrize(
    ("source_id", "watch_module"),
    (
        ("gmail", None),
        ("google_calendar", calendar_watch),
        ("google_drive", drive_watch),
    ),
)
async def test_watch_renewal_invoker_crosses_provider_lab_and_durable_lifecycle(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
    watch_module: Any | None,
) -> None:
    """Exercise the real watch invoker through transport, Lab, and renewal DB.

    This is intentionally stronger than the Provider Lab's direct lifecycle
    route checks: it starts at the source contract invoker, retains the exact
    tenant/installation resource identity through the durable lease, and uses
    the production Google clients against the Lab's strict used surface.
    """

    now = datetime.now(timezone.utc).replace(microsecond=0)
    if source_id == "gmail":
        tenant_id, installation_id, resource_id, _email, _topic = (
            await _seed_gmail_watch_resource(
                fresh_db,
                expiration=now + timedelta(hours=1),
            )
        )
        scope = "gmail-renewal-runtime"
        token_url = "http://provider-lab/gmail/token"
        api_base_url = "http://provider-lab/gmail/gmail/v1"
        primary_operation = "watch.create"
        primary_route = "gmail.watch"
    else:
        tenant_id, installation_id, resource_id = await _seed_google_watch_resource(
            fresh_db,
            source_id=source_id,
            expiration=now + timedelta(hours=1),
        )
        scope = f"{source_id}-renewal-runtime"
        token_url = (
            "http://provider-lab/gcal/token"
            if source_id == "google_calendar"
            else "http://provider-lab/gdrive/token"
        )
        api_base_url = (
            "http://provider-lab/gcal/calendar/v3"
            if source_id == "google_calendar"
            else "http://provider-lab/gdrive/drive/v3"
        )
        primary_operation = (
            "events.watch"
            if source_id == "google_calendar"
            else "changes.watch"
        )
        primary_route = (
            "google_calendar.events_watch"
            if source_id == "google_calendar"
            else "google_drive.changes_watch"
        )

    app = build_provider_lab_app(clock_start=now)
    provider_transport = _RecordingProviderTransport()
    lifecycle_state = {
        "renewal_lifecycle": {
            "enabled": True,
            # Production renewal rejects returns that do not leave a full
            # renewal window. The Lab TTLs therefore model real watch/token
            # spans rather than the five-second direct-fixture diagnostic.
            "access_ttl_seconds": 8 * 24 * 60 * 60,
            "refresh_ttl_seconds": 14 * 24 * 60 * 60,
            "watch_ttl_seconds": 7 * 24 * 60 * 60,
        }
    }
    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 43129)),
        base_url="http://provider-lab",
        headers={"X-Provider-Lab-Scope": scope},
    )
    monkeypatch.setattr(
        gmail_dwd,
        "_rs256_sign",
        lambda _key, _data: b"provider-lab-test-signature",
    )
    minter = gmail_dwd.DwdTokenMinter(
        gmail_dwd.ServiceAccountKey(
            client_email="renewal-service@provider-lab.test",
            private_key_pem="unused-in-provider-lab-test",
            private_key_id="provider-lab-test-key",
            token_uri=token_url,
        ),
        http_client=http,
        provider_transport=provider_transport,
        allow_unlimited_local=True,
    )

    async def make_client(
        requested_scope: str,
        *,
        tenant_id: Any,
        installation_id: Any,
    ) -> tuple[Any, Any]:
        assert tenant_id == locals_tenant_id
        assert installation_id == locals_installation_id
        google_http = GoogleHttpClient(
            minter,
            http_client=http,
            source=source_id,
            tenant_id=str(tenant_id),
            installation_id=str(installation_id),
            provider_transport=provider_transport,
            allow_unlimited_local=True,
        )
        if source_id == "google_calendar":
            client: Any = GoogleCalendarClient(
                google_http,
                scope=CALENDAR_READONLY_SCOPE,
                base_url=api_base_url,
            )
        elif source_id == "google_drive":
            client = GoogleDriveClient(
                google_http,
                scope=DRIVE_READONLY_SCOPE,
                base_url=api_base_url,
            )
        else:  # pragma: no cover - Gmail uses the dedicated branch below.
            raise AssertionError(source_id)
        assert requested_scope in {"calendar.readonly", "drive.readonly"}

        async def close() -> None:
            return None

        return client, close

    locals_tenant_id = tenant_id
    locals_installation_id = installation_id
    if source_id == "gmail":
        def make_google_http(
            supplied_minter: Any,
            *,
            tenant_id: str,
            installation_id: str,
        ) -> GoogleHttpClient:
            assert supplied_minter is minter
            assert tenant_id == str(locals_tenant_id)
            assert installation_id == str(locals_installation_id)
            return GoogleHttpClient(
                supplied_minter,
                http_client=http,
                source="gmail",
                tenant_id=tenant_id,
                installation_id=installation_id,
                provider_transport=provider_transport,
                allow_unlimited_local=True,
            )

        monkeypatch.setattr(gmail_watch_scheduler, "get_minter", lambda: minter)
        monkeypatch.setattr(
            gmail_watch_scheduler,
            "build_google_http_client",
            make_google_http,
        )
        monkeypatch.setattr(
            gmail_watch_scheduler,
            "GmailClient",
            lambda google_http: GmailClient(google_http, base_url=api_base_url),
        )
    else:
        assert watch_module is not None
        monkeypatch.setattr(
            watch_module,
            "SPEC",
            replace(watch_module.SPEC, make_client=make_client),
        )

    try:
        configured = await http.put(
            f"/_lab/sources/{source_id}/state",
            json=lifecycle_state,
        )
        assert configured.status_code == 200
        invocation = RenewalInvocation(
            pool=fresh_db,
            tenant_id=tenant_id,
            installation_id=installation_id,
            target_key=str(resource_id),
            worker_id=f"provider-lab-renewal-{source_id}",
            watch_address=(
                None
                if source_id == "gmail"
                else f"https://renewal.provider-lab.test/{source_id}"
            ),
            now=now,
        )
        invoker = resolve_renewal_invoker(source_id)
        first = await invoker(invocation)
        assert first.state == "renewed"

        first_lifecycle = await http.get(
            f"/_lab/sources/{source_id}/watch-lifecycle",
            params={"scope": scope},
        )
        assert first_lifecycle.status_code == 200
        assert first_lifecycle.json()["watches"][0]["state"] == "active"

        # The first channel is active just before its renewal window. The
        # durable scheduler owns the actual deadline, so the test advances the
        # Lab clock and makes that exact already-persisted job eligible rather
        # than inventing a second source path.
        renewal_now = now + timedelta(days=6)
        assert (
            await http.post(
                "/_lab/clock/advance",
                json={"seconds": 6 * 24 * 60 * 60},
            )
        ).status_code == 200
        before_second = await http.get(
            f"/_lab/sources/{source_id}/watch-lifecycle",
            params={"scope": scope},
        )
        assert before_second.json()["watches"][0]["state"] == "active"
        await fresh_db.execute(
            """
            UPDATE source_renewal_jobs
               SET next_attempt_at = now() - interval '1 second'
             WHERE source_id = $1
               AND tenant_id = $2
               AND installation_id = $3
               AND target_key = $4
            """,
            source_id,
            tenant_id,
            installation_id,
            str(resource_id),
        )
        second = await invoker(replace(invocation, now=renewal_now))
        assert second.state == "renewed"

        renewed_lifecycle = await http.get(
            f"/_lab/sources/{source_id}/watch-lifecycle",
            params={"scope": scope},
        )
        watches = renewed_lifecycle.json()["watches"]
        assert len(watches) == 2
        assert watches[1]["state"] == "active"
        assert watches[0]["state"] == (
            "replaced" if source_id == "gmail" else "stopped"
        )

        assert (
            await http.post(
                "/_lab/clock/advance",
                json={"seconds": 7 * 24 * 60 * 60},
            )
        ).status_code == 200
        expired_lifecycle = await http.get(
            f"/_lab/sources/{source_id}/watch-lifecycle",
            params={"scope": scope},
        )
        assert expired_lifecycle.json()["watches"][1]["state"] == "expired"

        job = await get_renewal_job(
            fresh_db,
            RenewalJobKey(
                source_id=source_id,
                tenant_id=tenant_id,
                installation_id=installation_id,
                target_key=str(resource_id),
            ),
        )
        assert job is not None
        assert job.state == "pending"
        assert job.expires_at is not None and job.expires_at > renewal_now
        assert job.provider_call_started_at is None
        assert "plr1" not in repr(job)

        contexts = tuple(provider_transport.contexts)
        assert all(context.source == source_id for context in contexts)
        assert all(context.tenant_id == str(tenant_id) for context in contexts)
        assert all(
            context.installation_id == str(installation_id)
            for context in contexts
        )
        operation_ids = [context.operation for context in contexts]
        assert operation_ids.count(primary_operation) == 2
        assert "dwd.token.exchange" in operation_ids
        if source_id != "gmail":
            assert operation_ids.count("channels.stop") == 1

        ledger = await http.get(
            "/_lab/ledger",
            params={"source": source_id, "scope": scope},
        )
        route_ids = [entry["route_id"] for entry in ledger.json()["entries"]]
        assert route_ids.count(primary_route) == 2
        if source_id != "gmail":
            assert route_ids.count(
                f"{source_id}.channels_stop"
            ) == 1
    finally:
        await http.aclose()


@pytest.mark.parametrize(
    ("source_id", "watch_module", "scope"),
    (
        ("gmail", None, GMAIL_METADATA_SCOPE),
        ("google_calendar", calendar_watch, CALENDAR_READONLY_SCOPE),
        ("google_drive", drive_watch, DRIVE_READONLY_SCOPE),
    ),
)
async def test_dwd_authorization_rejection_requires_exact_reauthorization(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
    watch_module: Any | None,
    scope: str,
) -> None:
    """Map definite DWD authorization rejection before unsafe watch creation.

    Each status is exercised through the production DWD minter, universal
    transport, and the contract-resolved source invoker.  A provider response
    body is intentionally secret-shaped to prove that neither the durable job
    nor the returned outcome carries it while the exact source resource remains
    unchanged.
    """

    now = datetime.now(timezone.utc).replace(microsecond=0)
    original_spec = watch_module.SPEC if watch_module is not None else None
    expected_error = (
        "gmail_dwd_reauthorization_required"
        if source_id == "gmail"
        else "dwd_reauthorization_required"
    )
    monkeypatch.setattr(
        gmail_dwd,
        "_rs256_sign",
        lambda _key, _data: b"dwd-authorization-test-signature",
    )

    for status in (400, 401, 403):
        if source_id == "gmail":
            tenant_id, installation_id, resource_id, _email, _topic = (
                await _seed_gmail_watch_resource(
                    fresh_db,
                    expiration=now + timedelta(seconds=30),
                )
            )
            table = "gmail_mailbox_watches"
            installation_column = "gmail_installation_id"
            snapshot_columns = (
                "state, history_id, watch_expiration, "
                "consecutive_poll_failures, last_error"
            )
        else:
            tenant_id, installation_id, resource_id = await _seed_google_watch_resource(
                fresh_db,
                source_id=source_id,
                expiration=now + timedelta(seconds=30),
            )
            assert watch_module is not None
            table = watch_module.SPEC.table
            installation_column = watch_module.SPEC.install_fk
            snapshot_columns = (
                "state, watch_state, watch_channel_id, watch_resource_id, "
                "watch_token, watch_expiration"
            )

        before = await fresh_db.fetchrow(
            f"""
            SELECT {snapshot_columns}
              FROM {table}
             WHERE id = $1
               AND tenant_id = $2
               AND {installation_column} = $3
            """,
            resource_id,
            tenant_id,
            installation_id,
        )
        assert before is not None

        provider_body = f"dwd-provider-body-{source_id}-{status}-must-not-persist"
        requests: list[httpx.Request] = []

        def token_failure(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            assert request.method == "POST"
            assert str(request.url) == "https://dwd-renewal.test/token"
            return httpx.Response(status, text=provider_body)

        http = httpx.AsyncClient(transport=httpx.MockTransport(token_failure))
        provider_transport = _RecordingProviderTransport()
        minter = gmail_dwd.DwdTokenMinter(
            gmail_dwd.ServiceAccountKey(
                client_email="renewal-dwd@provider-lab.test",
                private_key_pem="unused-in-test",
                private_key_id="dwd-renewal-test-key",
                token_uri="https://dwd-renewal.test/token",
            ),
            http_client=http,
            provider_transport=provider_transport,
            allow_unlimited_local=True,
        )

        if source_id == "gmail":
            def make_google_http(
                supplied_minter: Any,
                *,
                tenant_id: str,
                installation_id: str,
            ) -> GoogleHttpClient:
                assert supplied_minter is minter
                assert tenant_id == str(locals_tenant_id)
                assert installation_id == str(locals_installation_id)
                return GoogleHttpClient(
                    supplied_minter,
                    http_client=http,
                    source="gmail",
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                    provider_transport=provider_transport,
                    allow_unlimited_local=True,
                )

            locals_tenant_id = tenant_id
            locals_installation_id = installation_id
            monkeypatch.setattr(gmail_watch_scheduler, "get_minter", lambda: minter)
            monkeypatch.setattr(
                gmail_watch_scheduler,
                "build_google_http_client",
                make_google_http,
            )
        else:
            assert watch_module is not None
            assert original_spec is not None

            async def make_client(
                requested_scope: str,
                *,
                tenant_id: Any,
                installation_id: Any,
            ) -> tuple[Any, Any]:
                assert requested_scope in {"calendar.readonly", "drive.readonly"}
                assert tenant_id == locals_tenant_id
                assert installation_id == locals_installation_id
                google_http = GoogleHttpClient(
                    minter,
                    http_client=http,
                    source=source_id,
                    tenant_id=str(tenant_id),
                    installation_id=str(installation_id),
                    provider_transport=provider_transport,
                    allow_unlimited_local=True,
                )
                client: Any
                if source_id == "google_calendar":
                    client = GoogleCalendarClient(google_http, scope=scope)
                else:
                    client = GoogleDriveClient(google_http, scope=scope)

                async def close() -> None:
                    return None

                return client, close

            locals_tenant_id = tenant_id
            locals_installation_id = installation_id
            monkeypatch.setattr(
                watch_module,
                "SPEC",
                replace(original_spec, make_client=make_client),
            )

        try:
            outcome = await resolve_renewal_invoker(source_id)(
                RenewalInvocation(
                    pool=fresh_db,
                    tenant_id=tenant_id,
                    installation_id=installation_id,
                    target_key=str(resource_id),
                    worker_id=f"dwd-authorization-{source_id}-{status}",
                    watch_address=(
                        None
                        if source_id == "gmail"
                        else f"https://renewal.test/{source_id}"
                    ),
                    now=now,
                )
            )
        finally:
            await http.aclose()

        assert outcome.state == "reauthorization_required"
        assert outcome.next_attempt_at is None
        assert outcome.error_code == expected_error
        assert len(requests) == 1
        assert provider_body not in repr(outcome)
        assert [
            (context.source, context.operation, context.tenant_id, context.installation_id)
            for context in provider_transport.contexts
        ] == [
            (
                source_id,
                "dwd.token.exchange",
                str(tenant_id),
                str(installation_id),
            )
        ]

        after = await fresh_db.fetchrow(
            f"""
            SELECT {snapshot_columns}
              FROM {table}
             WHERE id = $1
               AND tenant_id = $2
               AND {installation_column} = $3
            """,
            resource_id,
            tenant_id,
            installation_id,
        )
        assert after is not None
        assert dict(after) == dict(before)
        job = await get_renewal_job(
            fresh_db,
            RenewalJobKey(
                source_id=source_id,
                tenant_id=tenant_id,
                installation_id=installation_id,
                target_key=str(resource_id),
            ),
        )
        assert job is not None
        assert job.state == "reauthorization_required"
        assert job.next_attempt_at is None
        assert job.last_error_code == expected_error
        assert job.provider_call_started_at is None
        assert provider_body not in repr(job)


async def _credential_lab_resource_status(
    http: httpx.AsyncClient,
    *,
    source_id: str,
    scope: str,
    access_token: str,
) -> int:
    """Call one declared Lab resource to prove token validity after renewal."""

    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Provider-Lab-Scope": scope,
    }
    if source_id == "quickbooks":
        response = await http.get(
            "/quickbooks/v3/company/provider-lab-realm/"
            "companyinfo/provider-lab-realm",
            headers=headers,
        )
    elif source_id == "gusto":
        response = await http.get(
            "/gusto/v1/companies/provider-lab-company",
            headers=headers,
        )
    elif source_id == "linkedin":
        response = await http.get(
            "/linkedin/posts",
            params={
                "q": "author",
                "author": "urn:li:organization:1",
            },
            headers={
                **headers,
                "LinkedIn-Version": "202501",
                "X-Restli-Protocol-Version": "2.0.0",
            },
        )
    elif source_id == "ramp":
        response = await http.get("/ramp/business", headers=headers)
    elif source_id == "carta":
        response = await http.get("/carta/v1alpha1/issuers", headers=headers)
    else:  # pragma: no cover - closed contract-derived test parameterization
        raise AssertionError(source_id)
    return response.status_code


_CREDENTIAL_LAB_CASES = (
    (
        "quickbooks",
        "/quickbooks/oauth2/v1/tokens/bearer",
        "quickbooks.oauth_token",
    ),
    ("ramp", "/ramp/token", "ramp.token"),
    ("gusto", "/gusto/oauth/token", "gusto.oauth_token"),
    ("carta", "/carta/o/access_token/", "carta.oauth_token"),
    ("linkedin", "/linkedin/oauth/v2/accessToken", "linkedin.oauth_token"),
)


@pytest.mark.parametrize(
    ("source_id", "token_path", "token_route"),
    _CREDENTIAL_LAB_CASES,
)
async def test_credential_renewal_invoker_crosses_provider_lab_and_persists_rotation(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
    token_path: str,
    token_route: str,
) -> None:
    """Run every contract credential invoker against its real Lab token route.

    The test starts with an expiring exact installation, executes the
    contract-selected source invoker twice across a virtual expiry boundary,
    and derives success from the durable installation/job rows plus a strict
    Provider Lab resource call. It deliberately uses the universal transport
    wrapper, not a hand-written token HTTP call.
    """

    assert {case[0] for case in _CREDENTIAL_LAB_CASES} == set(
        _CREDENTIAL_RENEWAL_SOURCES
    )
    now = datetime.now(timezone.utc).replace(microsecond=0)
    source = source_definition(source_id)
    refresh = source.credential_refresh
    renewal = source.renewal
    assert refresh is not None
    assert renewal is not None and renewal.kind == "credential"
    monkeypatch.setitem(
        REFRESH_CONFIGS,
        source_id,
        replace(
            REFRESH_CONFIGS[source_id],
            token_url=f"http://provider-lab{token_path}",
        ),
    )
    # These are disposable test-only app credentials. Sources that load
    # client credentials from their exact installation override them below,
    # which also proves that path preserves the source-specific behavior.
    monkeypatch.setenv(f"{source_id.upper()}_CLIENT_ID", "provider-lab-client")
    monkeypatch.setenv(f"{source_id.upper()}_CLIENT_SECRET", "provider-lab-secret")
    tenant_id, installation_id, store = await _seed_credential_installation(
        fresh_db,
        source_id=source_id,
        expires_at=now + timedelta(seconds=30),
    )

    app = build_provider_lab_app(clock_start=now)
    scope = f"{source_id}-credential-runtime"
    lifecycle: dict[str, Any] = {
        "enabled": True,
        "access_ttl_seconds": 7 * 24 * 60 * 60,
        "refresh_ttl_seconds": 14 * 24 * 60 * 60,
        "watch_ttl_seconds": 7 * 24 * 60 * 60,
    }
    if refresh.grant_type == "refresh_token":
        lifecycle["initial_refresh_token"] = "initial-refresh-token"
        lifecycle["initial_refresh_expires_at"] = (
            now + timedelta(days=14)
        ).isoformat().replace("+00:00", "Z")
    provider_transport = _RecordingProviderTransport()
    request_binding = ProviderRequestBinding(
        source=source_id,
        tenant_id=str(tenant_id),
        installation_id=str(installation_id),
        transport=provider_transport,
        request_policy=None,
        quota_resolver=None,
        allow_unlimited_local=True,
    )
    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 43130)),
        base_url="http://provider-lab",
        headers={"X-Provider-Lab-Scope": scope},
    )
    try:
        configured = await http.put(
            f"/_lab/sources/{source_id}/state",
            json={"renewal_lifecycle": lifecycle},
        )
        assert configured.status_code == 200
        invocation = RenewalInvocation(
            pool=fresh_db,
            tenant_id=tenant_id,
            installation_id=installation_id,
            target_key="installation",
            secret_store=store,
            http=http,
            request_binding=request_binding,
            worker_id=f"provider-lab-credential-{source_id}",
            now=now,
        )
        invoker = resolve_renewal_invoker(source_id)
        first = await invoker(invocation)
        assert first.state == "renewed"
        first_install = await fresh_db.fetchrow(
            f"""
            SELECT secret_ref, refresh_secret_ref, token_expires_at
              FROM {refresh.install_table}
             WHERE id = $1 AND tenant_id = $2
            """,
            installation_id,
            tenant_id,
        )
        assert first_install is not None
        first_access = (
            await store.get(first_install["secret_ref"], tenant_id=tenant_id)
        ).decode("utf-8")
        assert await _credential_lab_resource_status(
            http,
            source_id=source_id,
            scope=scope,
            access_token=first_access,
        ) == 200

        # Move to the source-owned renewal window. The DB job remains the
        # scheduler authority, so the test only makes that exact persisted
        # target due after the virtual provider clock reaches the window.
        renewal_now = now + timedelta(days=7, seconds=-60)
        assert (
            await http.post(
                "/_lab/clock/advance",
                json={"seconds": 7 * 24 * 60 * 60 - 60},
            )
        ).status_code == 200
        assert await _credential_lab_resource_status(
            http,
            source_id=source_id,
            scope=scope,
            access_token=first_access,
        ) == 200
        await fresh_db.execute(
            """
            UPDATE source_renewal_jobs
               SET next_attempt_at = now() - interval '1 second'
             WHERE source_id = $1
               AND tenant_id = $2
               AND installation_id = $3
               AND target_key = 'installation'
            """,
            source_id,
            tenant_id,
            installation_id,
        )
        second = await invoker(replace(invocation, now=renewal_now))
        assert second.state == "renewed"
        second_install = await fresh_db.fetchrow(
            f"""
            SELECT secret_ref, refresh_secret_ref, token_expires_at
              FROM {refresh.install_table}
             WHERE id = $1 AND tenant_id = $2
            """,
            installation_id,
            tenant_id,
        )
        assert second_install is not None
        second_access = (
            await store.get(second_install["secret_ref"], tenant_id=tenant_id)
        ).decode("utf-8")
        assert second_access != first_access

        # The original Lab access token has now expired; the renewed source
        # installation's token still reaches the declared provider resource.
        assert (
            await http.post("/_lab/clock/advance", json={"seconds": 61})
        ).status_code == 200
        assert await _credential_lab_resource_status(
            http,
            source_id=source_id,
            scope=scope,
            access_token=first_access,
        ) == 401
        assert await _credential_lab_resource_status(
            http,
            source_id=source_id,
            scope=scope,
            access_token=second_access,
        ) == 200

        job = await get_renewal_job(
            fresh_db,
            RenewalJobKey(
                source_id=source_id,
                tenant_id=tenant_id,
                installation_id=installation_id,
                target_key="installation",
            ),
        )
        assert job is not None
        assert job.state == "pending"
        assert job.expires_at is not None and job.expires_at > renewal_now
        assert job.provider_call_started_at is None
        assert "initial-refresh-token" not in repr(job)
        assert "plr1" not in repr(job)

        contexts = tuple(provider_transport.contexts)
        assert len(contexts) == 2
        assert all(context.source == source_id for context in contexts)
        assert all(context.operation == refresh.operation_id for context in contexts)
        assert all(context.tenant_id == str(tenant_id) for context in contexts)
        assert all(
            context.installation_id == str(installation_id)
            for context in contexts
        )
        ledger = await http.get(
            "/_lab/ledger",
            params={
                "source": source_id,
                "scope": scope,
                "route_id": token_route,
            },
        )
        assert ledger.json()["count"] == 2
    finally:
        await http.aclose()


def _patch_gmail_watch_provider(
    monkeypatch: pytest.MonkeyPatch,
    *,
    response: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Replace Gmail's network boundary with one deterministic watch result."""

    calls: list[dict[str, Any]] = []
    contexts: list[dict[str, str]] = []
    minter = object()

    class _GoogleHttpContext:
        async def __aenter__(self) -> object:
            return self

        async def __aexit__(
            self,
            exc_type: object,
            exc_value: object,
            traceback: object,
        ) -> None:
            return None

    def build_google_http_client(
        supplied_minter: object,
        *,
        tenant_id: str,
        installation_id: str,
    ) -> _GoogleHttpContext:
        assert supplied_minter is minter
        contexts.append(
            {
                "tenant_id": tenant_id,
                "installation_id": installation_id,
            }
        )
        return _GoogleHttpContext()

    class _GmailClient:
        def __init__(self, http: object) -> None:
            assert isinstance(http, _GoogleHttpContext)

        async def watch(
            self,
            *,
            user_email: str,
            scope: str,
            topic_name: str,
        ) -> dict[str, Any]:
            calls.append(
                {
                    "user_email": user_email,
                    "scope": scope,
                    "topic_name": topic_name,
                }
            )
            return dict(response)

    monkeypatch.setattr(gmail_watch_scheduler, "get_minter", lambda: minter)
    monkeypatch.setattr(
        gmail_watch_scheduler,
        "build_google_http_client",
        build_google_http_client,
    )
    monkeypatch.setattr(gmail_watch_scheduler, "GmailClient", _GmailClient)
    return calls, contexts


async def test_gmail_watch_renewal_is_due_only_exact_and_secret_free(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Gmail renewal uses only its bound installation/topic/watch row."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id, installation_id, watch_id, email, topic_name = (
        await _seed_gmail_watch_resource(
            fresh_db,
            expiration=now + timedelta(days=2),
        )
    )
    renewed_expiration = now + timedelta(days=7)
    provider_only_marker = "provider-response-secret-must-not-persist"
    calls, contexts = _patch_gmail_watch_provider(
        monkeypatch,
        response={
            "historyId": "900",
            "expiration": str(int(renewed_expiration.timestamp() * 1000)),
            "provider_only_marker": provider_only_marker,
        },
    )

    invocation = RenewalInvocation(
        pool=fresh_db,
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key=str(watch_id),
        worker_id="renewal-test-gmail",
        now=now,
    )
    invoker = resolve_renewal_invoker("gmail")

    before = await invoker(invocation)

    assert before.state == "not_due"
    assert calls == []
    assert contexts == []

    await fresh_db.execute(
        """
        UPDATE gmail_mailbox_watches
           SET watch_expiration = $1
         WHERE id = $2 AND tenant_id = $3 AND gmail_installation_id = $4
        """,
        now + timedelta(seconds=30),
        watch_id,
        tenant_id,
        installation_id,
    )
    await fresh_db.execute(
        """
        UPDATE source_renewal_jobs
           SET next_attempt_at = now() - interval '1 second'
         WHERE source_id = 'gmail'
           AND tenant_id = $1
           AND installation_id = $2
           AND target_key = $3
        """,
        tenant_id,
        installation_id,
        str(watch_id),
    )

    renewed = await invoker(invocation)

    assert renewed.state == "renewed"
    assert calls == [
        {
            "user_email": email,
            "scope": GMAIL_METADATA_SCOPE,
            "topic_name": topic_name,
        }
    ]
    assert contexts == [
        {
            "tenant_id": str(tenant_id),
            "installation_id": str(installation_id),
        }
    ]
    watch = await fresh_db.fetchrow(
        """
        SELECT history_id, watch_expiration
          FROM gmail_mailbox_watches
         WHERE id = $1 AND tenant_id = $2 AND gmail_installation_id = $3
        """,
        watch_id,
        tenant_id,
        installation_id,
    )
    assert watch is not None
    assert watch["history_id"] == "900"
    assert watch["watch_expiration"] == renewed_expiration

    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id="gmail",
            tenant_id=tenant_id,
            installation_id=installation_id,
            target_key=str(watch_id),
        ),
    )
    assert job is not None
    assert job.state == "pending"
    assert job.attempt_count == 2
    assert job.last_success_at is not None
    assert job.last_error_code is None
    assert job.expires_at == renewed_expiration
    assert job.lease_owner is None
    assert job.lease_expires_at is None
    assert provider_only_marker not in repr(job)
    assert provider_only_marker not in repr(renewed)


async def test_gmail_watch_renewal_never_mutates_a_sibling_installation(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Gmail mailbox watch renewal cannot update a sibling installation."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id = uuid7()
    first_tenant, first_installation, first_watch, _email, _topic = (
        await _seed_gmail_watch_resource(
            fresh_db,
            expiration=now + timedelta(seconds=30),
            tenant_id=tenant_id,
        )
    )
    sibling_tenant, sibling_installation, sibling_watch, _email, _topic = (
        await _seed_gmail_watch_resource(
            fresh_db,
            expiration=now + timedelta(seconds=30),
            tenant_id=tenant_id,
        )
    )
    assert first_tenant == sibling_tenant == tenant_id
    calls, contexts = _patch_gmail_watch_provider(
        monkeypatch,
        response={
            "historyId": "900",
            "expiration": str(int((now + timedelta(days=7)).timestamp() * 1000)),
        },
    )
    outcome = await resolve_renewal_invoker("gmail")(
        RenewalInvocation(
            pool=fresh_db,
            tenant_id=tenant_id,
            installation_id=first_installation,
            target_key=str(first_watch),
            worker_id="renewal-test-sibling-gmail",
            now=now,
        ),
    )

    assert outcome.state == "renewed"
    assert len(calls) == 1
    assert contexts == [
        {
            "tenant_id": str(tenant_id),
            "installation_id": str(first_installation),
        }
    ]
    sibling = await fresh_db.fetchrow(
        """
        SELECT state, history_id, watch_expiration, consecutive_poll_failures
          FROM gmail_mailbox_watches
         WHERE id = $1
           AND tenant_id = $2
           AND gmail_installation_id = $3
        """,
        sibling_watch,
        tenant_id,
        sibling_installation,
    )
    assert sibling is not None
    assert dict(sibling) == {
        "state": "active",
        "history_id": "800",
        "watch_expiration": now + timedelta(seconds=30),
        "consecutive_poll_failures": 0,
    }
    assert await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id="gmail",
            tenant_id=tenant_id,
            installation_id=sibling_installation,
            target_key=str(sibling_watch),
        ),
    ) is None


@pytest.mark.parametrize(
    ("source_id", "watch_module"),
    (
        ("google_calendar", calendar_watch),
        ("google_drive", drive_watch),
    ),
)
async def test_google_watch_renewal_uses_one_exact_resource_lease(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
    watch_module: Any,
) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id, install_id, resource_id = await _seed_google_watch_resource(
        fresh_db,
        source_id=source_id,
        expiration=now + timedelta(days=2),
    )
    calls: list[dict[str, Any]] = []

    class _Client:
        async def watch_events(self, **kwargs: Any) -> dict[str, str]:
            calls.append(kwargs)
            return {
                "resourceId": "renewed-calendar-resource",
                "expiration": str(int((now + timedelta(days=7)).timestamp() * 1000)),
            }

        async def watch_changes(self, **kwargs: Any) -> dict[str, str]:
            calls.append(kwargs)
            return {
                "resourceId": "renewed-drive-resource",
                "expiration": str(int((now + timedelta(days=7)).timestamp() * 1000)),
            }

        async def stop_channel(self, **kwargs: Any) -> None:
            return None

    bound: list[dict[str, Any]] = []

    async def make_client(
        scope: str,
        *,
        tenant_id: Any,
        installation_id: Any,
    ) -> tuple[_Client, Any]:
        bound.append(
            {
                "scope": scope,
                "tenant_id": tenant_id,
                "installation_id": installation_id,
            }
        )

        async def close() -> None:
            return None

        return _Client(), close

    monkeypatch.setattr(
        watch_module,
        "SPEC",
        replace(watch_module.SPEC, make_client=make_client),
    )
    invocation = RenewalInvocation(
        pool=fresh_db,
        tenant_id=tenant_id,
        installation_id=install_id,
        target_key=str(resource_id),
        worker_id=f"renewal-test-{source_id}",
        watch_address=f"https://renewal.test/webhooks/{source_id}",
        now=now,
    )
    invoker = resolve_renewal_invoker(source_id)

    before = await invoker(invocation)
    assert before.state == "not_due"
    assert calls == []

    spec = watch_module.SPEC
    await fresh_db.execute(
        f"""
        UPDATE {spec.table}
           SET watch_expiration = $1
         WHERE id = $2 AND tenant_id = $3
        """,
        now + timedelta(seconds=30),
        resource_id,
        tenant_id,
    )
    await fresh_db.execute(
        """
        UPDATE source_renewal_jobs
           SET next_attempt_at = now() - interval '1 second'
         WHERE source_id = $1
           AND tenant_id = $2
           AND installation_id = $3
           AND target_key = $4
        """,
        source_id,
        tenant_id,
        install_id,
        str(resource_id),
    )
    renewed = await invoker(invocation)

    assert renewed.state == "renewed"
    assert len(calls) == 1
    assert bound == [
        {
            "scope": "calendar.readonly" if source_id == "google_calendar" else "drive.readonly",
            "tenant_id": tenant_id,
            "installation_id": install_id,
        }
    ]
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=install_id,
            target_key=str(resource_id),
        ),
    )
    assert job is not None
    assert job.state == "pending"
    assert job.expires_at is not None
    assert "renewed" not in repr(job)


@pytest.mark.parametrize(
    ("source_id", "watch_module"),
    (
        ("google_calendar", calendar_watch),
        ("google_drive", drive_watch),
    ),
)
async def test_google_watch_renewal_never_mutates_a_sibling_installation(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
    source_id: str,
    watch_module: Any,
) -> None:
    """A Calendar/Drive channel write remains scoped to one installation."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id = uuid7()
    first_tenant, first_installation, first_resource = (
        await _seed_google_watch_resource(
            fresh_db,
            source_id=source_id,
            expiration=now + timedelta(seconds=30),
            tenant_id=tenant_id,
        )
    )
    sibling_tenant, sibling_installation, sibling_resource = (
        await _seed_google_watch_resource(
            fresh_db,
            source_id=source_id,
            expiration=now + timedelta(seconds=30),
            tenant_id=tenant_id,
        )
    )
    assert first_tenant == sibling_tenant == tenant_id

    class _Client:
        async def watch_events(self, **kwargs: Any) -> dict[str, str]:
            return {
                "resourceId": "renewed-calendar-resource",
                "expiration": str(
                    int((now + timedelta(days=7)).timestamp() * 1000)
                ),
            }

        async def watch_changes(self, **kwargs: Any) -> dict[str, str]:
            return {
                "resourceId": "renewed-drive-resource",
                "expiration": str(
                    int((now + timedelta(days=7)).timestamp() * 1000)
                ),
            }

        async def stop_channel(self, **kwargs: Any) -> None:
            return None

    async def make_client(
        scope: str,
        *,
        tenant_id: Any,
        installation_id: Any,
    ) -> tuple[_Client, Any]:
        assert scope in {"calendar.readonly", "drive.readonly"}
        assert tenant_id == first_tenant
        assert installation_id == first_installation

        async def close() -> None:
            return None

        return _Client(), close

    monkeypatch.setattr(
        watch_module,
        "SPEC",
        replace(watch_module.SPEC, make_client=make_client),
    )
    outcome = await resolve_renewal_invoker(source_id)(
        RenewalInvocation(
            pool=fresh_db,
            tenant_id=tenant_id,
            installation_id=first_installation,
            target_key=str(first_resource),
            worker_id=f"renewal-test-sibling-{source_id}",
            watch_address=f"https://renewal.test/webhooks/{source_id}",
            now=now,
        ),
    )

    assert outcome.state == "renewed"
    spec = watch_module.SPEC
    sibling = await fresh_db.fetchrow(
        f"""
        SELECT watch_channel_id, watch_resource_id, watch_token,
               watch_expiration, watch_state
          FROM {spec.table}
         WHERE id = $1
           AND tenant_id = $2
           AND {spec.install_fk} = $3
        """,
        sibling_resource,
        tenant_id,
        sibling_installation,
    )
    assert sibling is not None
    assert sibling["watch_channel_id"] is None
    assert sibling["watch_resource_id"] is None
    assert sibling["watch_token"] is None
    assert sibling["watch_expiration"] == now + timedelta(seconds=30)
    assert await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=sibling_installation,
            target_key=str(sibling_resource),
        ),
    ) is None


async def test_post_create_watch_cleanup_retry_cannot_repeat_unsafe_create(
    fresh_db: asyncpg.Pool,
) -> None:
    """A prior-channel stop cooldown cannot schedule another `events.watch`."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id, install_id, resource_id = await _seed_google_watch_resource(
        fresh_db,
        source_id="google_calendar",
        expiration=now + timedelta(seconds=30),
    )
    await fresh_db.execute(
        """
        UPDATE google_calendar_calendars
           SET watch_channel_id = 'old-calendar-channel',
               watch_resource_id = 'old-calendar-resource',
               watch_token = 'old-calendar-token',
               watch_state = 'active'
         WHERE id = $1 AND tenant_id = $2
        """,
        resource_id,
        tenant_id,
    )
    create_calls = 0
    stop_calls = 0

    async def make_client(
        scope: str,
        *,
        tenant_id: Any,
        installation_id: Any,
    ) -> tuple[object, Any]:
        assert scope == "calendar.readonly"
        assert tenant_id is not None
        assert installation_id is not None

        async def close() -> None:
            return None

        return object(), close

    async def do_watch(
        client: object,  # noqa: ARG001
        *,
        row: Any,  # noqa: ARG001
        channel_id: str,
        address: str,
        token: str,
        ttl_seconds: int,
    ) -> dict[str, str]:
        nonlocal create_calls
        create_calls += 1
        assert channel_id and address and token and ttl_seconds
        return {
            "resourceId": "new-calendar-resource",
            "expiration": str(int((now + timedelta(days=7)).timestamp() * 1000)),
        }

    async def do_stop(client: object, *, row: Any) -> None:  # noqa: ARG001
        nonlocal stop_calls
        stop_calls += 1
        raise RetryLater.after(
            request_context=RequestContext(
                source="google_calendar",
                operation="channels.stop",
            ),
            delay_seconds=60,
            reason=RetryReason.RATE_LIMIT,
            now=now,
        )

    spec = replace(
        calendar_watch.SPEC,
        make_client=make_client,
        do_watch=do_watch,
        do_stop=do_stop,
    )
    invocation = RenewalInvocation(
        pool=fresh_db,
        tenant_id=tenant_id,
        installation_id=install_id,
        target_key=str(resource_id),
        worker_id="renewal-test-post-create-cleanup",
        watch_address="https://renewal.test/webhooks/google-calendar",
        now=now,
    )

    first = await google_watch.renew_exact_resource(invocation, spec)
    second = await google_watch.renew_exact_resource(invocation, spec)

    assert first.state == "renewed"
    assert second.state == "lease_unavailable"
    assert create_calls == 1
    assert stop_calls == 1
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id="google_calendar",
            tenant_id=tenant_id,
            installation_id=install_id,
            target_key=str(resource_id),
        ),
    )
    assert job is not None
    assert job.state == "pending"
    assert job.next_attempt_at is not None


async def test_stale_credential_lease_cannot_overwrite_installation(
    fresh_db: asyncpg.Pool,
) -> None:
    """An old credential lease fails before it can rotate stored refs."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    tenant_id, installation_id, store = await _seed_credential_installation(
        fresh_db,
        source_id="quickbooks",
        expires_at=now + timedelta(seconds=30),
    )
    key = RenewalJobKey(
        source_id="quickbooks",
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key="installation",
    )
    stale_lease = await _superseded_lease(fresh_db, key)
    calls: list[dict[str, str]] = []

    async with _token_http("quickbooks", calls) as http:
        with pytest.raises(
            OAuthRefreshError,
            match="installation unavailable before credential renewal",
        ):
            await refresh_and_persist(
                provider="quickbooks",
                pool=fresh_db,
                secret_store=store,
                http=http,
                tenant_id=tenant_id,
                install_row_id=installation_id,
                refresh_secret_ref="initial-refresh-ref",
                now=now,
                renewal_lease=stale_lease,
            )

    assert calls == []
    installed = await fresh_db.fetchrow(
        """
        SELECT secret_ref, refresh_secret_ref, token_expires_at
          FROM quickbooks_installations
         WHERE id = $1 AND tenant_id = $2
        """,
        installation_id,
        tenant_id,
    )
    assert installed is not None
    assert installed["secret_ref"] == "initial-access-ref"
    assert installed["refresh_secret_ref"] == "initial-refresh-ref"
    assert installed["token_expires_at"] == now + timedelta(seconds=30)
    assert store.deleted == []


async def test_stale_google_watch_lease_cannot_overwrite_active_state(
    fresh_db: asyncpg.Pool,
) -> None:
    """A replacement lease fences the final Calendar channel persistence."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    old_expiration = now + timedelta(seconds=30)
    tenant_id, installation_id, resource_id = await _seed_google_watch_resource(
        fresh_db,
        source_id="google_calendar",
        expiration=old_expiration,
    )
    await fresh_db.execute(
        """
        UPDATE google_calendar_calendars
           SET watch_channel_id = 'old-calendar-channel',
               watch_resource_id = 'old-calendar-resource',
               watch_token = 'old-calendar-token',
               watch_state = 'active'
         WHERE id = $1 AND tenant_id = $2
        """,
        resource_id,
        tenant_id,
    )
    key = RenewalJobKey(
        source_id="google_calendar",
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key=str(resource_id),
    )
    stale_lease = await _superseded_lease(fresh_db, key)
    calls: list[dict[str, Any]] = []
    stopped: list[dict[str, Any]] = []

    async def make_client(
        scope: str,
        *,
        tenant_id: Any,
        installation_id: Any,
    ) -> tuple[object, Any]:
        calls.append(
            {
                "scope": scope,
                "tenant_id": tenant_id,
                "installation_id": installation_id,
            }
        )

        async def close() -> None:
            return None

        return object(), close

    async def do_watch(
        client: object,  # noqa: ARG001
        *,
        row: Any,  # noqa: ARG001
        channel_id: str,
        address: str,
        token: str,
        ttl_seconds: int,
    ) -> dict[str, str]:
        assert address == "https://renewal.test/webhooks/google-calendar"
        assert token
        assert ttl_seconds > 0
        return {
            "resourceId": "replacement-calendar-resource",
            "expiration": str(
                int((now + timedelta(days=7)).timestamp() * 1000)
            ),
        }

    async def do_stop(client: object, *, row: Any) -> None:  # noqa: ARG001
        stopped.append(
            {
                "channel_id": row["watch_channel_id"],
                "resource_id": row["watch_resource_id"],
            }
        )

    spec = replace(
        calendar_watch.SPEC,
        make_client=make_client,
        do_watch=do_watch,
        do_stop=do_stop,
    )
    row = {
        "id": resource_id,
        "tenant_id": tenant_id,
        "installation_id": installation_id,
        "scope": "calendar.readonly",
        "watch_channel_id": "old-calendar-channel",
        "watch_resource_id": "old-calendar-resource",
    }

    with pytest.raises(
        RenewalManualRepairRequired,
        match="watch_renewal_lease_lost",
    ):
        await google_watch.register_watch(
            fresh_db,
            spec,
            row,
            address="https://renewal.test/webhooks/google-calendar",
            renewal_lease=stale_lease,
            raise_on_failure=True,
            minimum_expiration=now + timedelta(days=1),
        )

    assert calls == [
        {
            "scope": "calendar.readonly",
            "tenant_id": tenant_id,
            "installation_id": installation_id,
        }
    ]
    assert len(stopped) == 1
    assert stopped[0]["channel_id"] != "old-calendar-channel"
    assert stopped[0]["resource_id"] == "replacement-calendar-resource"
    stored = await fresh_db.fetchrow(
        """
        SELECT watch_channel_id, watch_resource_id, watch_token,
               watch_expiration, watch_state
          FROM google_calendar_calendars
         WHERE id = $1 AND tenant_id = $2
        """,
        resource_id,
        tenant_id,
    )
    assert stored is not None
    assert dict(stored) == {
        "watch_channel_id": "old-calendar-channel",
        "watch_resource_id": "old-calendar-resource",
        "watch_token": "old-calendar-token",
        "watch_expiration": old_expiration,
        "watch_state": "active",
    }


async def test_stale_gmail_watch_lease_cannot_overwrite_active_state(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replaced Gmail resource lease cannot advance its watch state."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    old_expiration = now + timedelta(seconds=30)
    tenant_id, installation_id, watch_id, email, topic_name = (
        await _seed_gmail_watch_resource(
            fresh_db,
            expiration=old_expiration,
        )
    )
    key = RenewalJobKey(
        source_id="gmail",
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key=str(watch_id),
    )
    stale_lease = await _superseded_lease(fresh_db, key)
    calls, contexts = _patch_gmail_watch_provider(
        monkeypatch,
        response={
            "historyId": "900",
            "expiration": str(int((now + timedelta(days=7)).timestamp() * 1000)),
        },
    )
    row = await fresh_db.fetchrow(
        """
        SELECT id, tenant_id, gmail_installation_id, email_address, state,
               history_id, watch_expiration, consecutive_poll_failures
          FROM gmail_mailbox_watches
         WHERE id = $1 AND tenant_id = $2
        """,
        watch_id,
        tenant_id,
    )
    assert row is not None

    with pytest.raises(
        RenewalManualRepairRequired,
        match="gmail_watch_renewal_lease_lost",
    ):
        await gmail_watch_scheduler.renew_one(
            fresh_db,
            row,
            renewal_lease=stale_lease,
            raise_on_failure=True,
            minimum_expiration=now + timedelta(days=1),
        )

    assert calls == [
        {
            "user_email": email,
            "scope": GMAIL_METADATA_SCOPE,
            "topic_name": topic_name,
        }
    ]
    assert contexts == [
        {
            "tenant_id": str(tenant_id),
            "installation_id": str(installation_id),
        }
    ]
    stored = await fresh_db.fetchrow(
        """
        SELECT state, history_id, watch_expiration, consecutive_poll_failures
          FROM gmail_mailbox_watches
         WHERE id = $1 AND tenant_id = $2
        """,
        watch_id,
        tenant_id,
    )
    assert stored is not None
    assert dict(stored) == {
        "state": "active",
        "history_id": "800",
        "watch_expiration": old_expiration,
        "consecutive_poll_failures": 0,
    }


async def test_malformed_google_watch_response_requires_manual_reconciliation(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed Calendar renewal never overwrites the active channel."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    old_expiration = now + timedelta(seconds=30)
    tenant_id, installation_id, resource_id = await _seed_google_watch_resource(
        fresh_db,
        source_id="google_calendar",
        expiration=old_expiration,
    )
    await fresh_db.execute(
        """
        UPDATE google_calendar_calendars
           SET watch_channel_id = 'old-calendar-channel',
               watch_resource_id = 'old-calendar-resource',
               watch_token = 'old-calendar-token',
               watch_state = 'active'
         WHERE id = $1 AND tenant_id = $2
        """,
        resource_id,
        tenant_id,
    )
    calls: list[dict[str, Any]] = []

    async def make_client(
        scope: str,
        *,
        tenant_id: Any,
        installation_id: Any,
    ) -> tuple[object, Any]:
        calls.append(
            {
                "scope": scope,
                "tenant_id": tenant_id,
                "installation_id": installation_id,
            }
        )

        async def close() -> None:
            return None

        return object(), close

    async def malformed_watch(
        client: object,  # noqa: ARG001
        *,
        row: Any,  # noqa: ARG001
        channel_id: str,
        address: str,
        token: str,
        ttl_seconds: int,
    ) -> dict[str, str]:
        assert channel_id and address and token and ttl_seconds
        return {
            "expiration": str(int((now + timedelta(days=7)).timestamp() * 1000)),
        }

    async def do_stop(client: object, *, row: Any) -> None:  # noqa: ARG001
        return None

    monkeypatch.setattr(
        calendar_watch,
        "SPEC",
        replace(
            calendar_watch.SPEC,
            make_client=make_client,
            do_watch=malformed_watch,
            do_stop=do_stop,
        ),
    )
    invocation = RenewalInvocation(
        pool=fresh_db,
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key=str(resource_id),
        worker_id="renewal-test-calendar-invalid-response",
        watch_address="https://renewal.test/webhooks/google-calendar",
        now=now,
    )

    outcome = await resolve_renewal_invoker("google_calendar")(invocation)

    assert outcome.state == "manual_reconciliation_required"
    assert outcome.next_attempt_at is None
    assert outcome.error_code == "watch_response_invalid"
    assert len(calls) == 1
    stored = await fresh_db.fetchrow(
        """
        SELECT watch_channel_id, watch_resource_id, watch_token,
               watch_expiration, watch_state
          FROM google_calendar_calendars
         WHERE id = $1 AND tenant_id = $2
        """,
        resource_id,
        tenant_id,
    )
    assert stored is not None
    assert dict(stored) == {
        "watch_channel_id": "old-calendar-channel",
        "watch_resource_id": "old-calendar-resource",
        "watch_token": "old-calendar-token",
        "watch_expiration": old_expiration,
        "watch_state": "active",
    }
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id="google_calendar",
            tenant_id=tenant_id,
            installation_id=installation_id,
            target_key=str(resource_id),
        ),
    )
    assert job is not None
    assert job.state == "manual_reconciliation_required"
    assert job.next_attempt_at is None
    assert job.last_error_code == "watch_response_invalid"
    assert job.manual_reconciliation_required_at is not None


async def test_too_short_gmail_watch_response_requires_manual_reconciliation(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A too-short Gmail watch response leaves the active watch untouched."""

    now = datetime.now(timezone.utc).replace(microsecond=0)
    old_expiration = now + timedelta(seconds=30)
    tenant_id, installation_id, watch_id, email, topic_name = (
        await _seed_gmail_watch_resource(
            fresh_db,
            expiration=old_expiration,
        )
    )
    calls, contexts = _patch_gmail_watch_provider(
        monkeypatch,
        response={
            "historyId": "900",
            "expiration": str(
                int((now + timedelta(minutes=10)).timestamp() * 1000)
            ),
        },
    )
    invocation = RenewalInvocation(
        pool=fresh_db,
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key=str(watch_id),
        worker_id="renewal-test-gmail-short-response",
        now=now,
    )

    outcome = await resolve_renewal_invoker("gmail")(invocation)

    assert outcome.state == "manual_reconciliation_required"
    assert outcome.next_attempt_at is None
    assert outcome.error_code == "gmail_watch_response_invalid"
    assert calls == [
        {
            "user_email": email,
            "scope": GMAIL_METADATA_SCOPE,
            "topic_name": topic_name,
        }
    ]
    assert contexts == [
        {
            "tenant_id": str(tenant_id),
            "installation_id": str(installation_id),
        }
    ]
    stored = await fresh_db.fetchrow(
        """
        SELECT state, history_id, watch_expiration, consecutive_poll_failures
          FROM gmail_mailbox_watches
         WHERE id = $1 AND tenant_id = $2
        """,
        watch_id,
        tenant_id,
    )
    assert stored is not None
    assert dict(stored) == {
        "state": "active",
        "history_id": "800",
        "watch_expiration": old_expiration,
        "consecutive_poll_failures": 0,
    }
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id="gmail",
            tenant_id=tenant_id,
            installation_id=installation_id,
            target_key=str(watch_id),
        ),
    )
    assert job is not None
    assert job.state == "manual_reconciliation_required"
    assert job.next_attempt_at is None
    assert job.last_error_code == "gmail_watch_response_invalid"
    assert job.manual_reconciliation_required_at is not None


@pytest.mark.parametrize("source_id", _R2_RENEWAL_SOURCES)
async def test_retry_later_is_durable_and_secret_free_for_every_renewal_contract(
    fresh_db: asyncpg.Pool,
    source_id: str,
) -> None:
    """Preflight/cooldown retries persist once without leaking provider text.

    The shared envelope is deliberately tested through every declared source
    contract because its source/kind/lease binding is what determines whether
    a future scheduler may retry the same exact target.
    """

    tenant_id = uuid7()
    installation_id = uuid7()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"bounded-renewal-retry-later-{source_id}-{tenant_id}",
    )
    source = source_definition(source_id)
    renewal = source.renewal
    assert renewal is not None
    target_key = (
        "installation"
        if renewal.lease_scope == "installation"
        else str(uuid7())
    )
    invocation = RenewalInvocation(
        pool=fresh_db,
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key=target_key,
        worker_id=f"renewal-test-retry-later-{source_id}",
        now=now,
    )
    marker = f"provider-secret-marker-{source_id}"
    attempts = 0

    async def attempt(
        call: RenewalInvocation,  # noqa: ARG001
        lease: RenewalLease,  # noqa: ARG001
    ) -> Any:
        nonlocal attempts
        attempts += 1
        raise RetryLater.after(
            request_context=RequestContext(
                source=source_id,
                operation=renewal.operation_id,
            ),
            delay_seconds=60,
            reason=RetryReason.RATE_LIMIT,
            now=now,
            message=marker,
        )

    first = await run_bounded_renewal(
        invocation,
        source_id=source_id,
        expected_kind=renewal.kind,
        attempt=attempt,
    )
    second = await run_bounded_renewal(
        invocation,
        source_id=source_id,
        expected_kind=renewal.kind,
        attempt=attempt,
    )

    assert first.state == "retry_scheduled"
    assert first.next_attempt_at is not None
    assert first.error_code == "retry_later:rate_limit"
    assert second.state == "lease_unavailable"
    assert attempts == 1
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=installation_id,
            target_key=target_key,
        ),
    )
    assert job is not None
    assert job.state == "retry_scheduled"
    assert job.next_attempt_at == first.next_attempt_at
    assert job.last_error_code == "retry_later:rate_limit"
    assert marker not in repr(job)
    assert marker not in repr(first)


@pytest.mark.parametrize("source_id", _R2_RENEWAL_SOURCES)
async def test_concurrent_contract_renewal_envelope_has_one_owned_attempt(
    fresh_db: asyncpg.Pool,
    source_id: str,
) -> None:
    """Every source contract gives concurrent workers one exact lease winner."""

    tenant_id = uuid7()
    installation_id = uuid7()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"bounded-renewal-concurrent-envelope-{source_id}-{tenant_id}",
    )
    source = source_definition(source_id)
    renewal = source.renewal
    assert renewal is not None
    target_key = (
        "installation"
        if renewal.lease_scope == "installation"
        else str(uuid7())
    )
    invocation = RenewalInvocation(
        pool=fresh_db,
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key=target_key,
        worker_id=f"renewal-test-concurrent-envelope-a-{source_id}",
        now=now,
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    attempts = 0

    async def attempt(
        call: RenewalInvocation,  # noqa: ARG001
        lease: RenewalLease,  # noqa: ARG001
    ) -> RenewalAttempt:
        nonlocal attempts
        attempts += 1
        entered.set()
        await release.wait()
        return RenewalAttempt(
            state="renewed",
            next_attempt_at=now + timedelta(hours=1),
            expires_at=now + timedelta(hours=2),
        )

    first_task = asyncio.create_task(
        run_bounded_renewal(
            invocation,
            source_id=source_id,
            expected_kind=renewal.kind,
            attempt=attempt,
        ),
    )
    await asyncio.wait_for(entered.wait(), timeout=5)
    second = await run_bounded_renewal(
        replace(
            invocation,
            worker_id=f"renewal-test-concurrent-envelope-b-{source_id}",
        ),
        source_id=source_id,
        expected_kind=renewal.kind,
        attempt=attempt,
    )
    release.set()
    first = await first_task

    assert first.state == "renewed"
    assert second.state == "lease_unavailable"
    assert attempts == 1


async def test_heartbeat_loss_cancels_marked_attempt_and_blocks_replay(
    fresh_db: asyncpg.Pool,
) -> None:
    """A stale worker cannot continue an unsafe provider call after takeover."""

    tenant_id = uuid7()
    installation_id = uuid7()
    now = datetime.now(timezone.utc).replace(microsecond=0)
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"bounded-renewal-heartbeat-loss-{tenant_id}",
    )
    invocation = RenewalInvocation(
        pool=fresh_db,
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key="installation",
        worker_id="renewal-test-heartbeat-loss-a",
        now=now,
        # Leave enough time to make the deterministic replacement claim below
        # before the original worker's next heartbeat. The real lease helper
        # deliberately lets the same owner recover a narrowly expired lease
        # when no replacement has won.
        lease_timeout_seconds=3,
    )
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def attempt(
        call: RenewalInvocation,
        lease: RenewalLease,
    ) -> RenewalAttempt:
        await mark_renewal_provider_call_started(call.pool, lease)
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")

    running = asyncio.create_task(
        run_bounded_renewal(
            invocation,
            source_id="quickbooks",
            expected_kind="credential",
            attempt=attempt,
        ),
    )
    await asyncio.wait_for(entered.wait(), timeout=5)
    key = RenewalJobKey(
        source_id="quickbooks",
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key="installation",
    )
    await fresh_db.execute(
        """
        UPDATE source_renewal_jobs
           SET lease_expires_at = now() - interval '1 second'
         WHERE source_id = 'quickbooks'
           AND tenant_id = $1
           AND installation_id = $2
           AND target_key = 'installation'
        """,
        tenant_id,
        installation_id,
    )
    # A prospective replacement first observes the marker placed immediately
    # before the unsafe provider boundary. It must terminalize the exact job
    # for reconciliation rather than acquire a new generation and replay the
    # provider operation. The original heartbeat then loses its fenced lease
    # and the running attempt must be cancelled.
    assert await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-test-heartbeat-loss-b",
    ) is None

    with pytest.raises(RenewalLeaseLost):
        await asyncio.wait_for(running, timeout=5)
    assert cancelled.is_set()

    assert await claim_due_renewal_job(
        fresh_db,
        key,
        owner="renewal-test-heartbeat-loss-b",
    ) is None
    job = await get_renewal_job(fresh_db, key)
    assert job is not None
    assert job.state == "manual_reconciliation_required"
    assert job.last_error_code == "lease_lost_during_provider_call"


@pytest.mark.parametrize("source_id", _R2_RENEWAL_SOURCES)
async def test_provider_retry_forbidden_is_terminal_without_second_attempt(
    fresh_db: asyncpg.Pool,
    source_id: str,
) -> None:
    """Unsafe provider failures become manual repair, never a retry schedule."""

    tenant_id = uuid7()
    installation_id = uuid7()
    target_key = str(uuid7())
    now = datetime.now(timezone.utc).replace(microsecond=0)
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"bounded-renewal-provider-retry-forbidden-{tenant_id}",
    )
    source = source_definition(source_id)
    renewal = source.renewal
    assert renewal is not None
    invocation = RenewalInvocation(
        pool=fresh_db,
        tenant_id=tenant_id,
        installation_id=installation_id,
        target_key=("installation" if renewal.lease_scope == "installation" else target_key),
        worker_id="renewal-test-provider-retry-forbidden",
        now=now,
    )
    attempts = 0

    async def attempt(
        call: RenewalInvocation,  # noqa: ARG001
        lease: RenewalLease,  # noqa: ARG001
    ) -> Any:
        nonlocal attempts
        attempts += 1
        raise ProviderRetryForbiddenError(
            "provider retry policy forbids a second watch request",
            source=source_id,
            operation=renewal.operation_id,
        )

    first = await run_bounded_renewal(
        invocation,
        source_id=source_id,
        expected_kind=renewal.kind,
        attempt=attempt,
    )
    second = await run_bounded_renewal(
        invocation,
        source_id=source_id,
        expected_kind=renewal.kind,
        attempt=attempt,
    )

    assert first.state == "manual_reconciliation_required"
    assert first.next_attempt_at is None
    assert first.error_code == "provider_retry_forbidden"
    assert second.state == "lease_unavailable"
    assert attempts == 1
    job = await get_renewal_job(
        fresh_db,
        RenewalJobKey(
            source_id=source_id,
            tenant_id=tenant_id,
            installation_id=installation_id,
            target_key=invocation.target_key,
        ),
    )
    assert job is not None
    assert job.state == "manual_reconciliation_required"
    assert job.next_attempt_at is None
    assert job.last_error_code == "provider_retry_forbidden"
    assert job.manual_reconciliation_required_at is not None
