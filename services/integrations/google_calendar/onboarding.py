"""services/integrations/google_calendar/onboarding.py — install + provision.

Google Calendar reuses the Gmail DWD substrate (D1), so onboarding mirrors
Gmail's connect/finalize shape (services/integrations/gmail/oauth.py) rather
than the OAuth install/callback shape of slack/github/discord/notion:

  1. resolve_calendar_targets() — expand the admin inclusion_spec
     ({"users","groups","org_units"}) to concrete user emails via the SHARED
     gmail DirectoryClient + resolve_inclusion. Each user's primary calendar
     is addressed by their email (D6).
  2. finalize_install() — UPSERT a google_calendar_installations row, INSERT
     one google_calendar_calendars row per resolved calendar, and emit an
     onboarding_triggers row (source='google_calendar') so the existing M6
     backfill chain (oauth_poller -> tenant_onboarding -> source_onboarding)
     fires. All in one tenant-scoped transaction.

`connect()` ties the two together for the common case.

The HTTP/UI surface (a preflight enumerate + finalize wizard like
gmail/oauth.py) is an additive follow-up; these callables make the source
fully driveable and testable today.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.integrations.google_calendar import metrics
from services.integrations.google_calendar.client import resolve_scope


log = structlog.get_logger("integrations.google_calendar.onboarding")


_DEFAULT_SCOPE_ALIAS = "calendar.readonly"


async def resolve_calendar_targets(
    directory: Any,
    *,
    workspace_domain: str,
    inclusion_spec: dict[str, Any],
    optouts: set[str] | None = None,
) -> list[str]:
    """Expand the inclusion_spec to a sorted list of user emails whose
    primary calendars should be ingested. Reuses the Gmail DirectoryClient
    resolver verbatim — calendars and mailboxes share the user namespace."""
    from services.integrations.gmail.directory import resolve_inclusion

    return await resolve_inclusion(
        directory,
        workspace_domain=workspace_domain,
        inclusion_spec=inclusion_spec,
        optouts=optouts or set(),
    )


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    workspace_domain: str,
    service_account_email: str,
    calendar_emails: list[str],
    inclusion_spec: dict[str, Any] | None = None,
    scope_alias: str = _DEFAULT_SCOPE_ALIAS,
) -> UUID:
    """UPSERT the install + its calendars + an onboarding trigger atomically.

    `calendar_emails` is the resolved target list (see
    resolve_calendar_targets). Returns the google_calendar_installations id.
    Idempotent on (tenant_id, workspace_domain) and per (install, calendar).
    """
    # Validate the scope alias up-front (raises ValueError on unknown).
    resolve_scope(scope_alias)
    inclusion_spec = inclusion_spec or {}
    targets = sorted({e.lower() for e in calendar_emails if e})

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO google_calendar_installations (
                id, tenant_id, workspace_domain, service_account_email,
                scope, inclusion_spec, resolved_calendar_count, resolved_at
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, now())
            ON CONFLICT (tenant_id, workspace_domain) DO UPDATE
                SET scope = EXCLUDED.scope,
                    inclusion_spec = EXCLUDED.inclusion_spec,
                    service_account_email = EXCLUDED.service_account_email,
                    resolved_calendar_count = EXCLUDED.resolved_calendar_count,
                    resolved_at = now(),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, workspace_domain, service_account_email,
            scope_alias, json.dumps(inclusion_spec), len(targets),
        )

        for email in targets:
            await tctx.execute(
                """
                INSERT INTO google_calendar_calendars (
                    id, tenant_id, google_calendar_installation_id,
                    calendar_id, owner_email, state
                ) VALUES ($1, $2, $3, $4, $5, 'active')
                ON CONFLICT (google_calendar_installation_id, calendar_id)
                    DO UPDATE SET state = 'active', owner_email = EXCLUDED.owner_email
                """,
                uuid7(), tenant_id, install_id, email, email,
            )

        # Emit the onboarding trigger so the M6 backfill chain fires. Like
        # Gmail this is a DWD source (not in provider_installations); we
        # carry the install id in installation_row_id purely for the
        # idempotency dedup index (no FK). source='google_calendar' is
        # admitted by the CHECK widening in migration 0060.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'google_calendar', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"workspace_domain": workspace_domain,
                        "scope": scope_alias}),
        )

    if targets:
        metrics.record_provision_outcome("success")
    else:
        metrics.record_provision_outcome("no_calendars")
    log.info(
        "google_calendar_install_finalized",
        workspace_domain=workspace_domain,
        calendar_count=len(targets),
    )
    return install_id


async def connect(
    pool: asyncpg.Pool,
    directory: Any,
    *,
    tenant_id: UUID,
    workspace_domain: str,
    service_account_email: str,
    inclusion_spec: dict[str, Any],
    optouts: set[str] | None = None,
    scope_alias: str = _DEFAULT_SCOPE_ALIAS,
) -> UUID:
    """Resolve targets via the Directory API, then finalize the install."""
    try:
        targets = await resolve_calendar_targets(
            directory,
            workspace_domain=workspace_domain,
            inclusion_spec=inclusion_spec,
            optouts=optouts,
        )
    except Exception:
        metrics.record_provision_outcome("directory_error")
        raise
    return await finalize_install(
        pool,
        tenant_id=tenant_id,
        workspace_domain=workspace_domain,
        service_account_email=service_account_email,
        calendar_emails=targets,
        inclusion_spec=inclusion_spec,
        scope_alias=scope_alias,
    )


__all__ = ["connect", "finalize_install", "resolve_calendar_targets"]
