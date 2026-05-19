"""services/integrations/gmail/oauth.py — admin DWD connect wizard HTTP handlers.

Flow:

    GET /integrations/gmail/connect
        → render: enter workspace domain + super-admin email

    POST /integrations/gmail/connect/preflight
        body: { workspace_domain, admin_email, scope }
        → impersonate admin_email at directory scopes
        → list users + groups + org_units
        → returns enumeration JSON for the selector UI
        → if DWD grant missing: returns structured error with the
          exact client_id + scope strings to paste into Admin Console

    POST /integrations/gmail/connect/finalize
        body: { workspace_domain, admin_email, scope, inclusion_spec }
        → single transaction:
            - INSERT gmail_installations
            - INSERT gmail_install_audit (action='gmail.install')
        → background task:
            - resolve_inclusion(inclusion_spec)
            - PubsubAdmin.provision(tenant_id)
            - INSERT gmail_pubsub_topics
            - upsert_pending_watch / activate_watch per mailbox
        → 200 OK with the new gmail_installations.id

No OAuth state token is needed (the user never bounces through
Google for consent — DWD is pre-granted in the Admin Console). The
"connect" wizard is a pure first-party form with backend validation.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import CompanyOSError
from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction

from services.integrations.gmail.audit import write_install_audit
from services.integrations.gmail.client import (
    DIRECTORY_READ_SCOPES,
    GMAIL_METADATA_SCOPE,
    GMAIL_READONLY_SCOPE,
    DirectoryClient,
    GmailClient,
    GoogleApiError,
    GoogleHttpClient,
)
from services.integrations.gmail.directory import enumerate_domain, resolve_inclusion
from services.integrations.gmail.dwd import DwdError, get_minter
from services.integrations.gmail.optout import fetch_optout_emails
from services.integrations.gmail.pubsub import PubsubAdmin
from services.integrations.gmail.watch import activate_watch, upsert_pending_watch


log = structlog.get_logger("integrations.gmail.oauth")


SCOPE_ALIAS = {
    "gmail.metadata": GMAIL_METADATA_SCOPE,
    "gmail.readonly": GMAIL_READONLY_SCOPE,
}


class GmailConnectError(CompanyOSError):
    default_code = "gmail_connect_error"


router = APIRouter(prefix="/integrations/gmail", tags=["gmail"])


def _tenant_from_request(request: Request) -> UUID:
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    tid = auth.tenant_id
    return tid if isinstance(tid, UUID) else UUID(str(tid))


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Verify DWD is set up and enumerate the domain for the selector."""
    _tenant_from_request(request)  # auth check
    body = await request.json()
    workspace_domain = (body.get("workspace_domain") or "").strip().lower()
    admin_email = (body.get("admin_email") or "").strip().lower()
    scope_alias = (body.get("scope") or "").strip()

    if not workspace_domain or "." not in workspace_domain:
        raise HTTPException(status_code=400, detail="workspace_domain is required")
    if not admin_email or "@" not in admin_email:
        raise HTTPException(status_code=400, detail="admin_email is required")
    if scope_alias not in SCOPE_ALIAS:
        raise HTTPException(
            status_code=400,
            detail=f"scope must be one of {sorted(SCOPE_ALIAS)}",
        )

    minter = get_minter()
    async with GoogleHttpClient(minter) as http:
        directory = DirectoryClient(http, admin_email)
        try:
            enumeration = await enumerate_domain(
                directory, workspace_domain=workspace_domain,
            )
        except (GoogleApiError, DwdError) as exc:
            return JSONResponse(
                status_code=400,
                content={
                    "ok": False,
                    "error_code": "dwd_grant_invalid",
                    "message": (
                        "Directory API call failed. The most common cause is a "
                        "missing or mis-scoped Domain-Wide Delegation grant in "
                        "your Workspace Admin Console."
                    ),
                    "remediation": {
                        "step1": "Open Admin Console → Security → API controls → Domain-wide Delegation",
                        "step2": "Add a new entry with Client ID:",
                        "client_id": _service_account_client_id(),
                        "step3": "Authorize these OAuth scopes (comma-separated):",
                        "required_scopes": [
                            SCOPE_ALIAS[scope_alias],
                            *DIRECTORY_READ_SCOPES,
                        ],
                    },
                    "underlying_error": str(exc)[:300],
                },
            )

    return JSONResponse(content={
        "ok": True,
        "workspace_domain": workspace_domain,
        "admin_email": admin_email,
        "scope": SCOPE_ALIAS[scope_alias],
        "users": enumeration["users"],
        "groups": enumeration["groups"],
        "org_units": enumeration["org_units"],
    })


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    """Create the installation row and kick off async provisioning."""
    tenant_id = _tenant_from_request(request)
    body = await request.json()
    workspace_domain = (body.get("workspace_domain") or "").strip().lower()
    admin_email = (body.get("admin_email") or "").strip().lower()
    scope_alias = (body.get("scope") or "").strip()
    inclusion_spec = body.get("inclusion_spec") or {}

    if scope_alias not in SCOPE_ALIAS:
        raise HTTPException(status_code=400, detail="invalid scope")
    if not isinstance(inclusion_spec, dict):
        raise HTTPException(status_code=400, detail="inclusion_spec must be an object")

    scope_long = SCOPE_ALIAS[scope_alias]

    minter = get_minter()

    async with tenant_transaction(tenant_id) as tctx:
        # Upsert install row (idempotent on (tenant_id, workspace_domain)).
        install_id = await tctx.fetchval(
            """
            INSERT INTO gmail_installations (
              id, tenant_id, workspace_domain, service_account_email,
              scope, inclusion_spec
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            ON CONFLICT (tenant_id, workspace_domain) DO UPDATE
              SET scope = EXCLUDED.scope,
                  inclusion_spec = EXCLUDED.inclusion_spec,
                  disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, workspace_domain,
            minter.service_account_email, scope_alias,
            __dumps(inclusion_spec),
        )
        await write_install_audit(
            tctx,
            gmail_installation_id=install_id,
            action="gmail.install",
            actor_email=admin_email,
            details={
                "scope": scope_alias,
                "workspace_domain": workspace_domain,
                "inclusion_spec": inclusion_spec,
            },
        )
        # F4 / A20: atomic onboarding_triggers write. The retrofit makes
        # the M6 backfill chain fire on every completed OAuth install.
        # Idempotency: migration 0057 added a partial unique index on
        # (tenant_id, source, gmail_installation_id) WHERE
        # gmail_installation_id IS NOT NULL; ON CONFLICT DO NOTHING means
        # OAuth retries / browser refreshes / reinstalls produce at most
        # one trigger row per (tenant, gmail_install) tuple.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                gmail_installation_id, payload
            ) VALUES ($1, $2, 'gmail', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, gmail_installation_id)
                WHERE gmail_installation_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            __dumps({"scope": scope_alias,
                     "workspace_domain": workspace_domain}),
        )

    # Provisioning runs out-of-band: it makes external API calls
    # (Pub/Sub admin, users.watch per mailbox) that we don't want to
    # tie up the request thread on. Failures are recoverable via the
    # watch_scheduler.
    asyncio.create_task(
        _provision_install(
            tenant_id=tenant_id,
            gmail_installation_id=install_id,
            admin_email=admin_email,
            scope_alias=scope_alias,
        )
    )

    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "scope": scope_alias,
        "provisioning": "started",
    })


async def _provision_install(
    *,
    tenant_id: UUID,
    gmail_installation_id: UUID,
    admin_email: str,
    scope_alias: str,
) -> None:
    """Background work: provision Pub/Sub + resolve inclusion + start watches.

    Designed so partial failures are safe to re-run — every step is
    idempotent and the scheduler reconciles whatever it finds.
    """
    scope_long = SCOPE_ALIAS[scope_alias]
    try:
        async with PubsubAdmin() as admin:
            resources = await admin.provision(tenant_id)

        async with tenant_transaction(tenant_id) as tctx:
            await tctx.execute(
                """
                INSERT INTO gmail_pubsub_topics (
                  id, tenant_id, gmail_installation_id, topic_name, subscription_name
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (topic_name) DO NOTHING
                """,
                uuid7(), tenant_id, gmail_installation_id,
                resources.topic_name, resources.subscription_name,
            )

            install = await tctx.fetchrow(
                "SELECT workspace_domain, inclusion_spec FROM gmail_installations WHERE id = $1",
                gmail_installation_id,
            )
            optouts = await fetch_optout_emails(
                tctx, gmail_installation_id=gmail_installation_id,
            )

        minter = get_minter()
        async with GoogleHttpClient(minter) as http:
            directory = DirectoryClient(http, admin_email)
            emails = await resolve_inclusion(
                directory,
                workspace_domain=install["workspace_domain"],
                inclusion_spec=dict(install["inclusion_spec"]),
                optouts=optouts,
            )

        async with tenant_transaction(tenant_id) as tctx:
            await tctx.execute(
                """
                UPDATE gmail_installations
                   SET resolved_user_count = $2, resolved_at = now()
                 WHERE id = $1
                """,
                gmail_installation_id, len(emails),
            )
            for email in emails:
                await upsert_pending_watch(
                    tctx,
                    gmail_installation_id=gmail_installation_id,
                    email_address=email,
                )

        # Activate watches outside the transaction so a slow API call
        # doesn't hold a DB connection. Activation per-mailbox uses its
        # own short transaction.
        async with GoogleHttpClient(minter) as http:
            gmail = GmailClient(http)
            for email in emails:
                try:
                    async with tenant_transaction(tenant_id) as tctx:
                        await activate_watch(
                            tctx, gmail,
                            gmail_installation_id=gmail_installation_id,
                            email_address=email,
                            scope=scope_long,
                            topic_name=resources.topic_name,
                        )
                except GoogleApiError as exc:
                    log.warning(
                        "gmail.provision.watch_failed",
                        email=email, error=str(exc)[:200],
                    )

        log.info(
            "gmail.provision.completed",
            gmail_installation_id=str(gmail_installation_id),
            mailbox_count=len(emails),
        )
    except Exception as exc:
        log.error(
            "gmail.provision.failed",
            gmail_installation_id=str(gmail_installation_id),
            error=str(exc)[:300],
        )


def _service_account_client_id() -> str:
    """The DWD client ID (numeric) is needed to authorize scopes in the
    customer's Admin Console. Surfaced via env to avoid hard-coding."""
    return os.environ.get("GMAIL_SERVICE_ACCOUNT_CLIENT_ID", "(set GMAIL_SERVICE_ACCOUNT_CLIENT_ID)")


def __dumps(d: Any) -> str:
    import json
    return json.dumps(d, default=str)


__all__ = ["GmailConnectError", "router"]
