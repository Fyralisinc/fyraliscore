"""Gateway router tests for the Google Drive DWD connect wizard.

Covers the production install surface added in
`services/ingest/integrations/google_drive/oauth.py` — the gap that previously
made Drive install reachable only via `scripts/sandbox_google_drive.py`.
Mirrors the Calendar connect/finalize tests (`test_oauth_google_calendar.py`),
adding the Shared-Drive dimension Drive carries over Calendar.

Marked `integration` (real Postgres, auto-skipped when DATABASE_URL is unset).
The router is mounted directly in `services/app/gateway/main.py`, so these
tests include it explicitly on a minimal app, inject auth via middleware, and
pin `app.state.pool` — the same shape the gateway pins at runtime.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from fastapi import FastAPI

from services.ingest.integrations.gmail.client import GoogleApiError
from services.ingest.integrations.google_drive.onboarding import (
    DriveTarget,
    ResolvedTargets,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
async def _clean_gdrive_rows(fresh_db: asyncpg.Pool):
    """Remove any `google_drive` rows these tests leave behind.

    The conftest `db_pool` fixture re-applies ALL migrations at the start of
    every test, BEFORE `fresh_db` truncates. Migration 0059's source CHECK
    predates `google_drive`, so a surviving `google_drive` row makes the next
    test's `ADD CONSTRAINT` re-run fail validation. Production is forward-only
    and unaffected; this teardown keeps the shared test DB re-migratable (same
    guard as test_onboarding_google_calendar_db.py).
    """
    yield
    await fresh_db.execute(
        "DELETE FROM onboarding_triggers WHERE source = 'google_drive'",
    )
    await fresh_db.execute("DELETE FROM google_drive_targets")
    await fresh_db.execute("DELETE FROM google_drive_installations")


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'gdrive-router-test')", tid,
    )
    return tid


class _StubMinter:
    service_account_email = "svc@acme.iam.gserviceaccount.com"


def _make_app(pool: asyncpg.Pool, tenant_id: UUID) -> FastAPI:
    """Minimal app: Drive oauth router + injected auth + pinned pool."""
    from services.ingest.integrations.google_drive.oauth import router

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


async def test_finalize_writes_install_targets_and_trigger(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /connect/finalize resolves My-Drive + Shared-Drive targets and
    persists the install + per-target rows + an onboarding trigger."""
    from services.ingest.integrations.google_drive import oauth as gdrive_oauth
    from services.ingest.integrations.google_drive import onboarding

    monkeypatch.setattr(gdrive_oauth, "get_minter", lambda: _StubMinter())

    async def _fake_resolve(
        directory, *, workspace_domain, inclusion_spec,
        optouts=None, include_shared_drives=True, drive_client=None,
    ):
        my = [
            DriveTarget("my_drive", "my-drive", "alice@acme.com", "alice (My Drive)"),
            DriveTarget("my_drive", "my-drive", "bob@acme.com", "bob (My Drive)"),
        ]
        shared = (
            [DriveTarget("shared_drive", "0ABC", "alice@acme.com", "Engineering")]
            if include_shared_drives else []
        )
        return ResolvedTargets(my_drives=my, shared_drives=shared)

    # connect() calls resolve_drive_targets by module-global name.
    monkeypatch.setattr(onboarding, "resolve_drive_targets", _fake_resolve)

    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/google_drive/connect/finalize",
            json={
                "workspace_domain": "acme.com",
                "admin_email": "admin@acme.com",
                "inclusion_spec": {"users": ["alice@acme.com", "bob@acme.com"]},
            },
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["include_shared_drives"] is True
    assert body["target_count"] == 3  # 2 my-drive + 1 shared
    install_id = UUID(body["installation_id"])

    # Install row.
    install = await fresh_db.fetchrow(
        "SELECT workspace_domain, scope, include_shared_drives, "
        "resolved_target_count FROM google_drive_installations WHERE id = $1",
        install_id,
    )
    assert install["workspace_domain"] == "acme.com"
    assert install["scope"] == "drive.readonly"
    assert install["include_shared_drives"] is True
    assert install["resolved_target_count"] == 3

    # One target row per resolved drive (my_drive + shared_drive).
    targets = await fresh_db.fetch(
        "SELECT drive_kind, drive_id, owner_email FROM google_drive_targets "
        "WHERE google_drive_installation_id = $1 "
        "ORDER BY drive_kind, owner_email",
        install_id,
    )
    kinds = sorted({t["drive_kind"] for t in targets})
    assert kinds == ["my_drive", "shared_drive"]
    assert len(targets) == 3

    # Onboarding trigger emitted (source='google_drive', DWD carries the
    # install id in installation_row_id, not gmail_installation_id).
    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id, gmail_installation_id "
        "FROM onboarding_triggers WHERE tenant_id = $1 AND source = 'google_drive'",
        tenant,
    )
    assert trig is not None
    assert trig["trigger_kind"] == "install"
    assert trig["installation_row_id"] == install_id
    assert trig["gmail_installation_id"] is None


async def test_finalize_without_shared_drives(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """include_shared_drives=False → only My-Drive targets, and no Drive
    client is constructed for Shared-Drive enumeration."""
    from services.ingest.integrations.google_drive import oauth as gdrive_oauth
    from services.ingest.integrations.google_drive import onboarding

    monkeypatch.setattr(gdrive_oauth, "get_minter", lambda: _StubMinter())

    # Guard: if the handler builds a Drive client despite the opt-out, fail.
    def _no_drive_client(*args, **kwargs):  # pragma: no cover - guard
        raise AssertionError("GoogleDriveClient should not be built when opted out")

    monkeypatch.setattr(gdrive_oauth, "GoogleDriveClient", _no_drive_client)

    seen = {}

    async def _fake_resolve(
        directory, *, workspace_domain, inclusion_spec,
        optouts=None, include_shared_drives=True, drive_client=None,
    ):
        seen["include_shared_drives"] = include_shared_drives
        seen["drive_client"] = drive_client
        return ResolvedTargets(
            my_drives=[DriveTarget("my_drive", "my-drive", "alice@acme.com", None)],
            shared_drives=[],
        )

    monkeypatch.setattr(onboarding, "resolve_drive_targets", _fake_resolve)

    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/google_drive/connect/finalize",
            json={
                "workspace_domain": "acme.com",
                "admin_email": "admin@acme.com",
                "inclusion_spec": {"users": ["alice@acme.com"]},
                "include_shared_drives": False,
            },
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["include_shared_drives"] is False
    assert body["target_count"] == 1
    assert seen["include_shared_drives"] is False
    assert seen["drive_client"] is None


async def test_finalize_is_idempotent_on_reinstall(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two finalize calls for the same (tenant, workspace) produce one install
    and exactly one trigger row (UPSERT + ON CONFLICT DO NOTHING)."""
    from services.ingest.integrations.google_drive import oauth as gdrive_oauth
    from services.ingest.integrations.google_drive import onboarding

    monkeypatch.setattr(gdrive_oauth, "get_minter", lambda: _StubMinter())

    async def _fake_resolve(
        directory, *, workspace_domain, inclusion_spec,
        optouts=None, include_shared_drives=True, drive_client=None,
    ):
        return ResolvedTargets(
            my_drives=[DriveTarget("my_drive", "my-drive", "alice@acme.com", None)],
            shared_drives=[],
        )

    monkeypatch.setattr(onboarding, "resolve_drive_targets", _fake_resolve)

    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, tenant)
    payload = {
        "workspace_domain": "acme.com",
        "admin_email": "admin@acme.com",
        "inclusion_spec": {"users": ["alice@acme.com"]},
        "include_shared_drives": False,
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r1 = await c.post(
            "/integrations/google_drive/connect/finalize", json=payload,
        )
        r2 = await c.post(
            "/integrations/google_drive/connect/finalize", json=payload,
        )
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json()["installation_id"] == r2.json()["installation_id"]

    n_triggers = await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = 'google_drive'",
        tenant,
    )
    assert n_triggers == 1


async def test_preflight_returns_dwd_remediation_when_grant_missing(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the Directory API rejects the impersonation (DWD grant missing),
    preflight returns a structured 400 with the client_id + Drive scope to
    paste into the Admin Console."""
    from services.ingest.integrations.google_drive import oauth as gdrive_oauth

    monkeypatch.setenv("GMAIL_SERVICE_ACCOUNT_CLIENT_ID", "test-sa-client-id")
    monkeypatch.setattr(gdrive_oauth, "get_minter", lambda: _StubMinter())

    async def _boom(directory, *, workspace_domain):
        raise GoogleApiError("403 caller does not have permission")

    monkeypatch.setattr(gdrive_oauth, "enumerate_domain", _boom)

    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/google_drive/connect/preflight",
            json={"workspace_domain": "acme.com", "admin_email": "admin@acme.com"},
        )
    assert r.status_code == 400, r.text
    body = r.json()
    assert body["ok"] is False
    assert body["error_code"] == "dwd_grant_invalid"
    assert body["remediation"]["client_id"] == "test-sa-client-id"
    assert (
        "https://www.googleapis.com/auth/drive.readonly"
        in body["remediation"]["required_scopes"]
    )


async def test_finalize_rejects_bad_scope(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unknown scope alias is rejected up-front (no install written)."""
    from services.ingest.integrations.google_drive import oauth as gdrive_oauth

    monkeypatch.setattr(gdrive_oauth, "get_minter", lambda: _StubMinter())

    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/google_drive/connect/finalize",
            json={
                "workspace_domain": "acme.com",
                "admin_email": "admin@acme.com",
                "scope": "drive.write",
                "inclusion_spec": {"users": ["alice@acme.com"]},
            },
        )
    assert r.status_code == 400
    n = await fresh_db.fetchval(
        "SELECT count(*) FROM google_drive_installations WHERE tenant_id = $1",
        tenant,
    )
    assert n == 0


async def test_unauthenticated_request_is_rejected(
    fresh_db: asyncpg.Pool,
) -> None:
    """No injected auth → 401 (tenant is required for install)."""
    from services.ingest.integrations.google_drive.oauth import router

    app = FastAPI()
    app.state.pool = fresh_db
    app.include_router(router)  # NB: no auth middleware

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/google_drive/connect/finalize",
            json={"workspace_domain": "acme.com", "admin_email": "a@acme.com"},
        )
    assert r.status_code == 401
