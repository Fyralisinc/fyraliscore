"""Gateway router tests for the Google Calendar DWD connect wizard.

Covers the production install surface added in
`services/ingest/integrations/google_calendar/oauth.py` — the gap that
previously made Calendar install reachable only via
`scripts/sandbox_google_calendar.py`. Mirrors the Gmail connect/finalize
tests (`test_oauth_onboarding_triggers_retrofit.py`).

Marked `integration` (real Postgres, auto-skipped when DATABASE_URL is unset).
The router is mounted directly in `services/app/gateway/main.py` (not part of
`build_integrations_router()`), so these tests include the router module
explicitly on a minimal app, inject auth via middleware, and pin
`app.state.pool` — the same shape the gateway pins at runtime.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from fastapi import FastAPI

from services.ingest.integrations.gmail.client import GoogleApiError


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
async def _clean_gcal_rows(fresh_db: asyncpg.Pool):
    """Remove any `google_calendar` rows these tests leave behind.

    The conftest `db_pool` fixture re-applies ALL migrations at the start of
    every test, BEFORE `fresh_db` truncates. Migration 0059's source CHECK
    predates `google_calendar`, so a surviving `google_calendar` row makes the
    next test's `ADD CONSTRAINT` re-run fail validation. Production is
    forward-only and unaffected; this teardown keeps the shared test DB
    re-migratable (same guard as test_onboarding_google_calendar_db.py).
    """
    yield
    await fresh_db.execute(
        "DELETE FROM onboarding_triggers WHERE source = 'google_calendar'",
    )
    await fresh_db.execute("DELETE FROM google_calendar_calendars")
    await fresh_db.execute("DELETE FROM google_calendar_installations")


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'gcal-router-test')", tid,
    )
    return tid


class _StubMinter:
    service_account_email = "svc@acme.iam.gserviceaccount.com"


def _make_app(pool: asyncpg.Pool, tenant_id: UUID) -> FastAPI:
    """Minimal app: Calendar oauth router + injected auth + pinned pool."""
    from services.ingest.integrations.google_calendar.oauth import router

    app = FastAPI()
    app.state.pool = pool

    @app.middleware("http")
    async def _inject_auth(request, call_next):  # type: ignore[no-untyped-def]
        class _A:
            pass

        a = _A()
        a.tenant_id = tenant_id
        request.state.auth = a
        return await call_next(request)

    app.include_router(router)
    return app


async def test_finalize_writes_install_calendars_and_trigger(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /connect/finalize resolves targets and persists the install +
    per-calendar rows + an onboarding trigger so the M6 backfill chain fires."""
    from services.ingest.integrations.google_calendar import oauth as gcal_oauth
    from services.ingest.integrations.google_calendar import onboarding

    monkeypatch.setattr(gcal_oauth, "get_minter", lambda: _StubMinter())

    async def _fake_resolve(directory, *, workspace_domain, inclusion_spec, optouts=None):
        return ["alice@acme.com", "bob@acme.com"]

    # connect() calls resolve_calendar_targets by module-global name.
    monkeypatch.setattr(onboarding, "resolve_calendar_targets", _fake_resolve)

    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/google_calendar/connect/finalize",
            json={
                "workspace_domain": "acme.com",
                "admin_email": "admin@acme.com",
                "inclusion_spec": {"users": ["alice@acme.com"]},
            },
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["calendar_count"] == 2
    install_id = UUID(body["installation_id"])

    # Install row.
    install = await fresh_db.fetchrow(
        "SELECT workspace_domain, scope, resolved_calendar_count "
        "FROM google_calendar_installations WHERE id = $1",
        install_id,
    )
    assert install["workspace_domain"] == "acme.com"
    assert install["scope"] == "calendar.readonly"
    assert install["resolved_calendar_count"] == 2

    # One calendar row per resolved email.
    cals = await fresh_db.fetch(
        "SELECT calendar_id FROM google_calendar_calendars "
        "WHERE google_calendar_installation_id = $1 ORDER BY calendar_id",
        install_id,
    )
    assert [c["calendar_id"] for c in cals] == ["alice@acme.com", "bob@acme.com"]

    # Onboarding trigger emitted (source='google_calendar', DWD carries the
    # install id in installation_row_id, not gmail_installation_id).
    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id, gmail_installation_id "
        "FROM onboarding_triggers WHERE tenant_id = $1 AND source = 'google_calendar'",
        tenant,
    )
    assert trig is not None
    assert trig["trigger_kind"] == "install"
    assert trig["installation_row_id"] == install_id
    assert trig["gmail_installation_id"] is None


async def test_finalize_is_idempotent_on_reinstall(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two finalize calls for the same (tenant, workspace) produce one install
    and exactly one trigger row (UPSERT + ON CONFLICT DO NOTHING)."""
    from services.ingest.integrations.google_calendar import oauth as gcal_oauth
    from services.ingest.integrations.google_calendar import onboarding

    monkeypatch.setattr(gcal_oauth, "get_minter", lambda: _StubMinter())

    async def _fake_resolve(directory, *, workspace_domain, inclusion_spec, optouts=None):
        return ["alice@acme.com"]

    monkeypatch.setattr(onboarding, "resolve_calendar_targets", _fake_resolve)

    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, tenant)
    payload = {
        "workspace_domain": "acme.com",
        "admin_email": "admin@acme.com",
        "inclusion_spec": {"users": ["alice@acme.com"]},
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r1 = await c.post(
            "/integrations/google_calendar/connect/finalize", json=payload,
        )
        r2 = await c.post(
            "/integrations/google_calendar/connect/finalize", json=payload,
        )
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json()["installation_id"] == r2.json()["installation_id"]

    n_triggers = await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = 'google_calendar'",
        tenant,
    )
    assert n_triggers == 1


async def test_preflight_returns_dwd_remediation_when_grant_missing(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the Directory API rejects the impersonation (DWD grant missing),
    preflight returns a structured 400 with the client_id + scopes to paste
    into the Admin Console."""
    from services.ingest.integrations.google_calendar import oauth as gcal_oauth

    monkeypatch.setenv("GMAIL_SERVICE_ACCOUNT_CLIENT_ID", "test-sa-client-id")
    monkeypatch.setattr(gcal_oauth, "get_minter", lambda: _StubMinter())

    async def _boom(directory, *, workspace_domain):
        raise GoogleApiError("403 caller does not have permission")

    monkeypatch.setattr(gcal_oauth, "enumerate_domain", _boom)

    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/google_calendar/connect/preflight",
            json={"workspace_domain": "acme.com", "admin_email": "admin@acme.com"},
        )
    assert r.status_code == 400, r.text
    body = r.json()
    assert body["ok"] is False
    assert body["error_code"] == "dwd_grant_invalid"
    assert body["remediation"]["client_id"] == "test-sa-client-id"
    assert (
        "https://www.googleapis.com/auth/calendar.readonly"
        in body["remediation"]["required_scopes"]
    )


async def test_finalize_rejects_bad_scope(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unknown scope alias is rejected up-front (no install written)."""
    from services.ingest.integrations.google_calendar import oauth as gcal_oauth

    monkeypatch.setattr(gcal_oauth, "get_minter", lambda: _StubMinter())

    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/google_calendar/connect/finalize",
            json={
                "workspace_domain": "acme.com",
                "admin_email": "admin@acme.com",
                "scope": "calendar.write",
                "inclusion_spec": {"users": ["alice@acme.com"]},
            },
        )
    assert r.status_code == 400
    n = await fresh_db.fetchval(
        "SELECT count(*) FROM google_calendar_installations WHERE tenant_id = $1",
        tenant,
    )
    assert n == 0


async def test_unauthenticated_request_is_rejected(
    fresh_db: asyncpg.Pool,
) -> None:
    """No injected auth → 401 (tenant is required for install)."""
    from services.ingest.integrations.google_calendar.oauth import router

    app = FastAPI()
    app.state.pool = fresh_db
    app.include_router(router)  # NB: no auth middleware

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/google_calendar/connect/finalize",
            json={"workspace_domain": "acme.com", "admin_email": "a@acme.com"},
        )
    assert r.status_code == 401
