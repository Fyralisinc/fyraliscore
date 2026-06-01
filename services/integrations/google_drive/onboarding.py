"""services/integrations/google_drive/onboarding.py — install + provision.

Google Drive reuses the Gmail DWD substrate (D1), so onboarding mirrors the
Calendar connect/finalize shape (services/integrations/google_calendar/
onboarding.py) rather than the OAuth install/callback shape of
slack/github/discord/notion:

  1. resolve_drive_targets() — expand the admin inclusion_spec to concrete user
     emails via the SHARED gmail DirectoryClient + resolve_inclusion (each
     user's My Drive), and OPTIONALLY enumerate the org's Shared Drives via
     drives.list (impersonating the first resolved user as admin).
  2. finalize_install() — UPSERT a google_drive_installations row, INSERT one
     google_drive_targets row per resolved target (my_drive + shared_drive),
     and emit an onboarding_triggers row (source='google_drive') so the
     existing M6 backfill chain fires. All in one tenant-scoped transaction.

`connect()` ties the two together for the common case.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.integrations.google_drive import metrics
from services.integrations.google_drive.client import (
    MY_DRIVE_SENTINEL,
    resolve_scope,
)


log = structlog.get_logger("integrations.google_drive.onboarding")


_DEFAULT_SCOPE_ALIAS = "drive.readonly"


@dataclass(frozen=True)
class DriveTarget:
    """One drive to ingest. `drive_kind` is 'my_drive' or 'shared_drive'."""

    drive_kind: str
    drive_id: str
    owner_email: str
    display_name: str | None = None


@dataclass
class ResolvedTargets:
    my_drives: list[DriveTarget] = field(default_factory=list)
    shared_drives: list[DriveTarget] = field(default_factory=list)

    def all(self) -> list[DriveTarget]:
        return [*self.my_drives, *self.shared_drives]


async def resolve_drive_targets(
    directory: Any,
    *,
    workspace_domain: str,
    inclusion_spec: dict[str, Any],
    optouts: set[str] | None = None,
    include_shared_drives: bool = True,
    drive_client: Any | None = None,
) -> ResolvedTargets:
    """Expand the inclusion_spec to per-user My-Drive targets and (optionally)
    enumerate org Shared-Drive targets.

    `drive_client` (a GoogleDriveClient) is only needed when
    `include_shared_drives` is True; it is impersonated as the first resolved
    user to enumerate shared drives via drives.list?useDomainAdminAccess.
    """
    from services.integrations.gmail.directory import resolve_inclusion

    emails = await resolve_inclusion(
        directory,
        workspace_domain=workspace_domain,
        inclusion_spec=inclusion_spec,
        optouts=optouts or set(),
    )
    my_drives = [
        DriveTarget(
            drive_kind="my_drive",
            drive_id=MY_DRIVE_SENTINEL,
            owner_email=e,
            display_name=f"{e} (My Drive)",
        )
        for e in emails
    ]

    shared: list[DriveTarget] = []
    if include_shared_drives and drive_client is not None and emails:
        admin_email = emails[0]
        page_token: str | None = None
        while True:
            body = await drive_client.list_shared_drives(
                user_email=admin_email, page_token=page_token,
            )
            for d in body.get("drives") or []:
                drive_id = d.get("id")
                if isinstance(drive_id, str) and drive_id:
                    shared.append(DriveTarget(
                        drive_kind="shared_drive",
                        drive_id=drive_id,
                        owner_email=admin_email,
                        display_name=d.get("name"),
                    ))
            page_token = body.get("nextPageToken")
            if not page_token:
                break

    return ResolvedTargets(my_drives=my_drives, shared_drives=shared)


async def finalize_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    workspace_domain: str,
    service_account_email: str,
    targets: list[DriveTarget],
    inclusion_spec: dict[str, Any] | None = None,
    include_shared_drives: bool = True,
    scope_alias: str = _DEFAULT_SCOPE_ALIAS,
) -> UUID:
    """UPSERT the install + its targets + an onboarding trigger atomically.

    Returns the google_drive_installations id. Idempotent on
    (tenant_id, workspace_domain) and per (install, drive_kind, drive_id,
    owner_email).
    """
    # Validate the scope alias up-front (raises ValueError on unknown).
    resolve_scope(scope_alias)
    inclusion_spec = inclusion_spec or {}
    # Dedup targets defensively on the natural key.
    seen: set[tuple[str, str, str]] = set()
    deduped: list[DriveTarget] = []
    for t in targets:
        key = (t.drive_kind, t.drive_id, t.owner_email.lower())
        if t.owner_email and key not in seen:
            seen.add(key)
            deduped.append(t)

    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        install_id = await tctx.fetchval(
            """
            INSERT INTO google_drive_installations (
                id, tenant_id, workspace_domain, service_account_email,
                scope, inclusion_spec, include_shared_drives,
                resolved_target_count, resolved_at
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, now())
            ON CONFLICT (tenant_id, workspace_domain) DO UPDATE
                SET scope = EXCLUDED.scope,
                    inclusion_spec = EXCLUDED.inclusion_spec,
                    include_shared_drives = EXCLUDED.include_shared_drives,
                    service_account_email = EXCLUDED.service_account_email,
                    resolved_target_count = EXCLUDED.resolved_target_count,
                    resolved_at = now(),
                    disabled_at = NULL
            RETURNING id
            """,
            uuid7(), tenant_id, workspace_domain, service_account_email,
            scope_alias, json.dumps(inclusion_spec), include_shared_drives,
            len(deduped),
        )

        for t in deduped:
            await tctx.execute(
                """
                INSERT INTO google_drive_targets (
                    id, tenant_id, google_drive_installation_id,
                    drive_kind, drive_id, owner_email, display_name, state
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'active')
                ON CONFLICT (google_drive_installation_id, drive_kind, drive_id, owner_email)
                    DO UPDATE SET state = 'active',
                                  display_name = EXCLUDED.display_name
                """,
                uuid7(), tenant_id, install_id, t.drive_kind, t.drive_id,
                t.owner_email.lower(), t.display_name,
            )

        # Emit the onboarding trigger so the M6 backfill chain fires (DWD
        # source: install id carried in installation_row_id for the
        # idempotency dedup index, no FK). source='google_drive' is admitted
        # by the CHECK widening in migration 0061.
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind,
                installation_row_id, payload
            ) VALUES ($1, $2, 'google_drive', 'install', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO NOTHING
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"workspace_domain": workspace_domain,
                        "scope": scope_alias}),
        )

    if deduped:
        metrics.record_provision_outcome("success")
    else:
        metrics.record_provision_outcome("no_targets")
    log.info(
        "google_drive_install_finalized",
        workspace_domain=workspace_domain,
        target_count=len(deduped),
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
    include_shared_drives: bool = True,
    drive_client: Any | None = None,
    scope_alias: str = _DEFAULT_SCOPE_ALIAS,
) -> UUID:
    """Resolve targets via the Directory API (+ Shared-Drive enumeration),
    then finalize the install."""
    try:
        resolved = await resolve_drive_targets(
            directory,
            workspace_domain=workspace_domain,
            inclusion_spec=inclusion_spec,
            optouts=optouts,
            include_shared_drives=include_shared_drives,
            drive_client=drive_client,
        )
    except Exception:
        metrics.record_provision_outcome("directory_error")
        raise
    return await finalize_install(
        pool,
        tenant_id=tenant_id,
        workspace_domain=workspace_domain,
        service_account_email=service_account_email,
        targets=resolved.all(),
        inclusion_spec=inclusion_spec,
        include_shared_drives=include_shared_drives,
        scope_alias=scope_alias,
    )


__all__ = [
    "DriveTarget",
    "ResolvedTargets",
    "connect",
    "finalize_install",
    "resolve_drive_targets",
]
