"""services/ingest/integrations/google_calendar/oauth.py — admin DWD connect wizard.

Google Calendar reuses the Gmail DWD substrate (D1), so its install surface
mirrors `gmail/oauth.py` rather than the OAuth install/callback shape of
slack/github/discord/notion. This is the production gateway router that the
onboarding module flagged as an additive follow-up
(`onboarding.py`: "The HTTP/UI surface … is an additive follow-up"): it exposes
the already-built `connect()` / `finalize_install()` callables over HTTP so
install is reachable outside `scripts/sandbox_google_calendar.py`.

Flow:

    POST /integrations/google_calendar/connect/preflight
        body: { workspace_domain, admin_email, scope? }
        → impersonate admin_email at directory scopes
        → list users + groups + org_units for the selector UI
        → if the DWD grant is missing: a structured error carrying the exact
          client_id + scope strings to paste into the Admin Console

    POST /integrations/google_calendar/connect/finalize
        body: { workspace_domain, admin_email, scope?, inclusion_spec }
        → resolve inclusion_spec → calendar emails (shared DirectoryClient)
        → one transaction (in finalize_install): UPSERT
          google_calendar_installations + per-calendar google_calendar_calendars
          rows + an onboarding_triggers row (source='google_calendar') so the
          existing M6 backfill chain (oauth_poller → tenant_onboarding →
          source_onboarding) fires
        → 200 OK with the new google_calendar_installations.id

No OAuth state token is needed (the user never bounces through Google for
consent — DWD is pre-granted in the Admin Console). Calendar is poll-only (no
Pub/Sub topics or push watches), so unlike Gmail there is no out-of-band
provisioning step: resolution + persistence complete inline and the response
carries the resolved calendar count.
"""
from __future__ import annotations

import os
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from services.ingest.integrations.gmail.client import (
    DIRECTORY_READ_SCOPES,
    DirectoryClient,
    GoogleApiError,
    build_google_onboarding_http_client,
)
from services.ingest.integrations.gmail.directory import enumerate_domain
from services.ingest.integrations.gmail.dwd import DwdError, get_minter
from services.ingest.integrations.google_calendar.client import resolve_scope
from services.ingest.integrations.google_calendar.onboarding import connect


log = structlog.get_logger("integrations.google_calendar.oauth")


# Calendar exposes a single read scope today; default it so the one-scope
# source doesn't force callers to echo it back on every request.
_DEFAULT_SCOPE_ALIAS = "calendar.readonly"


router = APIRouter(
    prefix="/integrations/google_calendar", tags=["google_calendar"],
)


def _tenant_from_request(request: Request) -> UUID:
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    tid = auth.tenant_id
    return tid if isinstance(tid, UUID) else UUID(str(tid))


def _pool_from_request(request: Request) -> asyncpg.Pool:
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="database pool unavailable")
    return pool


def _validate_scope(raw: str | None) -> str:
    """Normalise + validate the install scope alias (raises 400 if unknown)."""
    alias = (raw or _DEFAULT_SCOPE_ALIAS).strip()
    try:
        resolve_scope(alias)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="scope must be 'calendar.readonly'",
        )
    return alias


def _dwd_remediation(scope_alias: str, exc: Exception) -> JSONResponse:
    """The shared DWD-grant-missing payload: the exact client_id + scopes to
    paste into the customer's Admin Console. Mirrors gmail/oauth.py so the
    connect wizard renders the same remediation for both Google sources."""
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "dwd_grant_invalid",
            "message": (
                "Directory API call failed. The most common cause is a "
                "missing or mis-scoped Domain-Wide Delegation grant in your "
                "Workspace Admin Console."
            ),
            "remediation": {
                "step1": "Open Admin Console → Security → API controls → Domain-wide Delegation",
                "step2": "Add a new entry with Client ID:",
                "client_id": _service_account_client_id(),
                "step3": "Authorize these OAuth scopes (comma-separated):",
                "required_scopes": [resolve_scope(scope_alias), *DIRECTORY_READ_SCOPES],
            },
            "underlying_error": str(exc)[:300],
        },
    )


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify DWD is set up and enumerate the domain for the selector."""
    tenant_id = _tenant_from_request(request)
    body = await request.json()
    workspace_domain = (body.get("workspace_domain") or "").strip().lower()
    admin_email = (body.get("admin_email") or "").strip().lower()
    scope_alias = _validate_scope(body.get("scope"))

    if not workspace_domain or "." not in workspace_domain:
        raise HTTPException(status_code=400, detail="workspace_domain is required")
    if not admin_email or "@" not in admin_email:
        raise HTTPException(status_code=400, detail="admin_email is required")

    minter = get_minter()
    async with build_google_onboarding_http_client(
        minter,
        source="google_calendar",
        tenant_id=str(tenant_id),
        quota_dimensions={"workspace": workspace_domain},
    ) as http:
        directory = DirectoryClient(http, admin_email)
        try:
            enumeration = await enumerate_domain(
                directory, workspace_domain=workspace_domain,
            )
        except (GoogleApiError, DwdError) as exc:
            return _dwd_remediation(scope_alias, exc)

    return JSONResponse(content={
        "ok": True,
        "workspace_domain": workspace_domain,
        "admin_email": admin_email,
        "scope": resolve_scope(scope_alias),
        "users": enumeration["users"],
        "groups": enumeration["groups"],
        "org_units": enumeration["org_units"],
    })


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    """Resolve the inclusion_spec and persist the install atomically.

    `connect()` resolves the admin's inclusion_spec to concrete calendar
    emails via the shared Directory API and then writes the install +
    per-calendar rows + onboarding trigger in one tenant-scoped transaction.
    """
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    body = await request.json()
    workspace_domain = (body.get("workspace_domain") or "").strip().lower()
    admin_email = (body.get("admin_email") or "").strip().lower()
    scope_alias = _validate_scope(body.get("scope"))
    inclusion_spec = body.get("inclusion_spec") or {}

    if not workspace_domain or "." not in workspace_domain:
        raise HTTPException(status_code=400, detail="workspace_domain is required")
    if not admin_email or "@" not in admin_email:
        raise HTTPException(status_code=400, detail="admin_email is required")
    if not isinstance(inclusion_spec, dict):
        raise HTTPException(status_code=400, detail="inclusion_spec must be an object")

    minter = get_minter()
    try:
        async with build_google_onboarding_http_client(
            minter,
            source="google_calendar",
            tenant_id=str(tenant_id),
            quota_dimensions={"workspace": workspace_domain},
        ) as http:
            directory = DirectoryClient(http, admin_email)
            install_id = await connect(
                pool, directory,
                tenant_id=tenant_id,
                workspace_domain=workspace_domain,
                service_account_email=minter.service_account_email,
                inclusion_spec=inclusion_spec,
                scope_alias=scope_alias,
            )
    except (GoogleApiError, DwdError) as exc:
        return _dwd_remediation(scope_alias, exc)

    calendar_count = await pool.fetchval(
        "SELECT resolved_calendar_count FROM google_calendar_installations "
        "WHERE id = $1",
        install_id,
    )
    log.info(
        "google_calendar.connect.finalized",
        installation_id=str(install_id),
        calendar_count=calendar_count,
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "scope": scope_alias,
        "calendar_count": int(calendar_count or 0),
    })


def _service_account_client_id() -> str:
    """The DWD client ID (numeric) is needed to authorize scopes in the
    customer's Admin Console. Calendar shares Gmail's service account, so it
    reads the same env var as gmail/oauth.py."""
    return os.environ.get(
        "GMAIL_SERVICE_ACCOUNT_CLIENT_ID", "(set GMAIL_SERVICE_ACCOUNT_CLIENT_ID)",
    )


__all__ = ["router"]
