"""services/ingest/integrations/google_drive/oauth.py — admin DWD connect wizard.

Google Drive reuses the Gmail DWD substrate (D1), so its install surface
mirrors `gmail/oauth.py` / `google_calendar/oauth.py` rather than the OAuth
install/callback shape of slack/github/discord/notion. This is the production
gateway router that makes install reachable outside
`scripts/sandbox_google_drive.py`, exposing the already-built `connect()` /
`finalize_install()` callables over HTTP.

Flow:

    POST /integrations/google_drive/connect/preflight
        body: { workspace_domain, admin_email, scope? }
        → impersonate admin_email at directory scopes
        → list users + groups + org_units for the selector UI
        → if the DWD grant is missing: a structured error carrying the exact
          client_id + scope strings to paste into the Admin Console

    POST /integrations/google_drive/connect/finalize
        body: { workspace_domain, admin_email, scope?, inclusion_spec,
                include_shared_drives? }
        → resolve inclusion_spec → per-user My-Drive targets and (optionally)
          enumerate the org's Shared Drives via drives.list
        → one transaction (in finalize_install): UPSERT
          google_drive_installations + per-target google_drive_targets rows +
          an onboarding_triggers row (source='google_drive') so the existing M6
          backfill chain fires
        → 200 OK with the new google_drive_installations.id

No OAuth state token is needed (DWD is pre-granted in the Admin Console). Like
Calendar, Drive is poll-only (changes-API delta, no push channel), so there is
no out-of-band provisioning step: resolution + persistence complete inline and
the response carries the resolved target count.
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
    GoogleHttpClient,
)
from services.ingest.integrations.gmail.directory import enumerate_domain
from services.ingest.integrations.gmail.dwd import DwdError, get_minter
from services.ingest.integrations.google_drive.client import (
    GoogleDriveClient,
    resolve_scope,
)
from services.ingest.integrations.google_drive.onboarding import connect


log = structlog.get_logger("integrations.google_drive.oauth")


# Drive exposes a single read scope today; default it so the one-scope source
# doesn't force callers to echo it back on every request.
_DEFAULT_SCOPE_ALIAS = "drive.readonly"


router = APIRouter(
    prefix="/integrations/google_drive", tags=["google_drive"],
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
            status_code=400, detail="scope must be 'drive.readonly'",
        )
    return alias


def _dwd_remediation(scope_alias: str, exc: Exception) -> JSONResponse:
    """The shared DWD-grant-missing payload: the exact client_id + scopes to
    paste into the customer's Admin Console. Mirrors gmail/oauth.py so the
    connect wizard renders the same remediation for every Google source."""
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
    _tenant_from_request(request)  # auth check
    body = await request.json()
    workspace_domain = (body.get("workspace_domain") or "").strip().lower()
    admin_email = (body.get("admin_email") or "").strip().lower()
    scope_alias = _validate_scope(body.get("scope"))

    if not workspace_domain or "." not in workspace_domain:
        raise HTTPException(status_code=400, detail="workspace_domain is required")
    if not admin_email or "@" not in admin_email:
        raise HTTPException(status_code=400, detail="admin_email is required")

    minter = get_minter()
    async with GoogleHttpClient(minter) as http:
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
    """Resolve the inclusion_spec (+ Shared Drives) and persist the install.

    `connect()` resolves the admin's inclusion_spec to per-user My-Drive
    targets via the shared Directory API, optionally enumerates the org's
    Shared Drives (impersonating the first resolved user), then writes the
    install + per-target rows + onboarding trigger in one tenant-scoped
    transaction.
    """
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    body = await request.json()
    workspace_domain = (body.get("workspace_domain") or "").strip().lower()
    admin_email = (body.get("admin_email") or "").strip().lower()
    scope_alias = _validate_scope(body.get("scope"))
    inclusion_spec = body.get("inclusion_spec") or {}
    include_shared_drives = bool(body.get("include_shared_drives", True))

    if not workspace_domain or "." not in workspace_domain:
        raise HTTPException(status_code=400, detail="workspace_domain is required")
    if not admin_email or "@" not in admin_email:
        raise HTTPException(status_code=400, detail="admin_email is required")
    if not isinstance(inclusion_spec, dict):
        raise HTTPException(status_code=400, detail="inclusion_spec must be an object")

    minter = get_minter()
    try:
        async with GoogleHttpClient(minter) as http:
            directory = DirectoryClient(http, admin_email)
            # The Drive client is only needed to enumerate Shared Drives; skip
            # building it when the caller opts out.
            drive_client = (
                GoogleDriveClient(http) if include_shared_drives else None
            )
            install_id = await connect(
                pool, directory,
                tenant_id=tenant_id,
                workspace_domain=workspace_domain,
                service_account_email=minter.service_account_email,
                inclusion_spec=inclusion_spec,
                include_shared_drives=include_shared_drives,
                drive_client=drive_client,
                scope_alias=scope_alias,
            )
    except (GoogleApiError, DwdError) as exc:
        return _dwd_remediation(scope_alias, exc)

    target_count = await pool.fetchval(
        "SELECT resolved_target_count FROM google_drive_installations "
        "WHERE id = $1",
        install_id,
    )
    log.info(
        "google_drive.connect.finalized",
        installation_id=str(install_id),
        target_count=target_count,
        include_shared_drives=include_shared_drives,
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "scope": scope_alias,
        "target_count": int(target_count or 0),
        "include_shared_drives": include_shared_drives,
    })


def _service_account_client_id() -> str:
    """The DWD client ID (numeric) is needed to authorize scopes in the
    customer's Admin Console. Drive shares Gmail's service account, so it reads
    the same env var as gmail/oauth.py."""
    return os.environ.get(
        "GMAIL_SERVICE_ACCOUNT_CLIENT_ID", "(set GMAIL_SERVICE_ACCOUNT_CLIENT_ID)",
    )


__all__ = ["router"]
