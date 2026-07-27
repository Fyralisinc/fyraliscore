"""Exact installation adapters used by contract-driven historical ingestion.

Every public ``load_<source>_installation`` callable has the same contract:
load one enabled installation by its Fyralis row UUID and tenant UUID.  There
is deliberately no "latest", tenant-only, or ``LIMIT 1`` path.  The named
callables are referenced by :class:`SourceDefinition`; this module contains no
source registry or runtime dispatch map.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID


class InstallationIdentityError(ValueError):
    """Historical work is missing a valid Fyralis installation-row UUID."""


def require_installation_row_id(value: Any) -> UUID:
    """Parse one exact row UUID; never infer an installation from a tenant."""

    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise InstallationIdentityError(
            "historical ingestion requires an exact installation row UUID"
        ) from exc


async def load_source_installation(
    executor: Any,
    *,
    source: str,
    tenant_id: UUID,
    installation_id: UUID | str,
) -> Any | None:
    """Resolve the catalog adapter and load exactly one enabled row."""

    from services.ingest.source_contract.runtime import (
        resolve_installation_loader,
    )

    loader = resolve_installation_loader(source)
    return await loader(
        executor,
        tenant_id=tenant_id,
        installation_id=require_installation_row_id(installation_id),
    )


_PROVIDER_INSTALL_SQL = """
SELECT id, tenant_id, provider, installation_id, secret_ref, enabled
  FROM provider_installations
 WHERE id = $1
   AND tenant_id = $2
   AND provider = $3
   AND enabled = TRUE
"""


async def _load_provider_installation(
    executor: Any,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    provider: str,
) -> Any | None:
    return await executor.fetchrow(
        _PROVIDER_INSTALL_SQL,
        installation_id,
        tenant_id,
        provider,
    )


async def load_slack_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await _load_provider_installation(
        executor,
        tenant_id=tenant_id,
        installation_id=installation_id,
        provider="slack",
    )


async def load_github_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await _load_provider_installation(
        executor,
        tenant_id=tenant_id,
        installation_id=installation_id,
        provider="github",
    )


async def load_discord_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await _load_provider_installation(
        executor,
        tenant_id=tenant_id,
        installation_id=installation_id,
        provider="discord",
    )


async def load_notion_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await _load_provider_installation(
        executor,
        tenant_id=tenant_id,
        installation_id=installation_id,
        provider="notion",
    )


_GMAIL_INSTALL_SQL = """
SELECT gi.id, gi.tenant_id, gi.workspace_domain, gi.service_account_email,
       gi.scope, gi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'email_address', mw.email_address,
             'google_user_id', mw.google_user_id,
             'history_id', mw.history_id
           ) ORDER BY mw.email_address
         ) FILTER (WHERE mw.id IS NOT NULL),
         '[]'::json
       ) AS mailboxes
  FROM gmail_installations gi
  LEFT JOIN gmail_mailbox_watches mw
    ON mw.gmail_installation_id = gi.id AND mw.state = 'active'
 WHERE gi.id = $1 AND gi.tenant_id = $2 AND gi.disabled_at IS NULL
 GROUP BY gi.id
"""


async def load_gmail_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(
        _GMAIL_INSTALL_SQL,
        installation_id,
        tenant_id,
    )


_GOOGLE_CALENDAR_INSTALL_SQL = """
SELECT gi.id, gi.tenant_id, gi.workspace_domain, gi.service_account_email,
       gi.scope, gi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'calendar_id', cc.calendar_id,
             'owner_email', cc.owner_email,
             'sync_token', cc.sync_token
           ) ORDER BY cc.calendar_id
         ) FILTER (WHERE cc.id IS NOT NULL),
         '[]'::json
       ) AS calendars
  FROM google_calendar_installations gi
  LEFT JOIN google_calendar_calendars cc
    ON cc.google_calendar_installation_id = gi.id AND cc.state = 'active'
 WHERE gi.id = $1 AND gi.tenant_id = $2 AND gi.disabled_at IS NULL
 GROUP BY gi.id
"""


async def load_google_calendar_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(
        _GOOGLE_CALENDAR_INSTALL_SQL,
        installation_id,
        tenant_id,
    )


_GOOGLE_DRIVE_INSTALL_SQL = """
SELECT gi.id, gi.tenant_id, gi.workspace_domain, gi.service_account_email,
       gi.scope, gi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'drive_kind', dt.drive_kind,
             'drive_id', dt.drive_id,
             'owner_email', dt.owner_email,
             'start_page_token', dt.start_page_token
           ) ORDER BY dt.drive_kind, dt.drive_id, dt.owner_email
         ) FILTER (WHERE dt.id IS NOT NULL),
         '[]'::json
       ) AS targets
  FROM google_drive_installations gi
  LEFT JOIN google_drive_targets dt
    ON dt.google_drive_installation_id = gi.id AND dt.state = 'active'
 WHERE gi.id = $1 AND gi.tenant_id = $2 AND gi.disabled_at IS NULL
 GROUP BY gi.id
"""


async def load_google_drive_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(
        _GOOGLE_DRIVE_INSTALL_SQL,
        installation_id,
        tenant_id,
    )


_JIRA_INSTALL_SQL = """
SELECT ji.id, ji.tenant_id, ji.base_url, ji.account_email, ji.secret_ref,
       ji.cloud_id, ji.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'project_key', jp.project_key,
             'project_id', jp.project_id,
             'project_name', jp.project_name,
             'updated_cursor', jp.updated_cursor
           ) ORDER BY jp.project_key
         ) FILTER (WHERE jp.id IS NOT NULL),
         '[]'::json
       ) AS projects
  FROM jira_installations ji
  LEFT JOIN jira_projects jp
    ON jp.jira_installation_id = ji.id AND jp.state = 'active'
 WHERE ji.id = $1 AND ji.tenant_id = $2 AND ji.disabled_at IS NULL
 GROUP BY ji.id
"""


async def load_jira_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_JIRA_INSTALL_SQL, installation_id, tenant_id)


_MERCURY_INSTALL_SQL = """
SELECT mi.id, mi.tenant_id, mi.base_url, mi.secret_ref, mi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'account_id', ma.account_id,
             'account_name', ma.account_name,
             'account_kind', ma.account_kind,
             'txn_cursor', ma.txn_cursor
           ) ORDER BY ma.account_id
         ) FILTER (WHERE ma.id IS NOT NULL),
         '[]'::json
       ) AS accounts
  FROM mercury_installations mi
  LEFT JOIN mercury_accounts ma
    ON ma.mercury_installation_id = mi.id AND ma.state = 'active'
 WHERE mi.id = $1 AND mi.tenant_id = $2 AND mi.disabled_at IS NULL
 GROUP BY mi.id
"""


async def load_mercury_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(
        _MERCURY_INSTALL_SQL,
        installation_id,
        tenant_id,
    )


_QUICKBOOKS_INSTALL_SQL = """
SELECT qi.id, qi.tenant_id, qi.realm_id, qi.base_url, qi.secret_ref,
       qi.refresh_secret_ref, qi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', qe.entity_type,
             'updated_cursor', qe.updated_cursor
           ) ORDER BY qe.entity_type
         ) FILTER (WHERE qe.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM quickbooks_installations qi
  LEFT JOIN quickbooks_entities qe
    ON qe.quickbooks_installation_id = qi.id AND qe.state = 'active'
 WHERE qi.id = $1 AND qi.tenant_id = $2 AND qi.disabled_at IS NULL
 GROUP BY qi.id
"""


async def load_quickbooks_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(
        _QUICKBOOKS_INSTALL_SQL,
        installation_id,
        tenant_id,
    )


async def _load_simple_installation(
    executor: Any,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    table: str,
    columns: str,
) -> Any | None:
    # ``table`` and ``columns`` are private source-adapter constants, never
    # request data. Values remain positional parameters.
    query = (
        f"SELECT {columns} FROM {table} "
        "WHERE id = $1 AND tenant_id = $2 AND disabled_at IS NULL"
    )
    return await executor.fetchrow(query, installation_id, tenant_id)


async def load_grafana_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await _load_simple_installation(
        executor,
        tenant_id=tenant_id,
        installation_id=installation_id,
        table="grafana_installations",
        columns=(
            "id, tenant_id, base_url, org_id, secret_ref, "
            "annotations_cursor_ms, disabled_at"
        ),
    )


_TELEGRAM_INSTALL_SQL = """
SELECT ti.id, ti.tenant_id, ti.account_label, ti.api_id,
       ti.api_hash_secret_ref, ti.session_secret_ref,
       ti.backfill_session_secret_ref, ti.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'dialog_id', td.dialog_id,
             'dialog_kind', td.dialog_kind,
             'access_hash', td.access_hash,
             'title', td.title,
             'offset_id_cursor', td.offset_id_cursor
           ) ORDER BY td.dialog_id
         ) FILTER (WHERE td.id IS NOT NULL),
         '[]'::json
       ) AS dialogs
  FROM telegram_installations ti
  LEFT JOIN telegram_dialogs td
    ON td.telegram_installation_id = ti.id AND td.state = 'active'
 WHERE ti.id = $1 AND ti.tenant_id = $2 AND ti.disabled_at IS NULL
 GROUP BY ti.id
"""


async def load_telegram_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(
        _TELEGRAM_INSTALL_SQL,
        installation_id,
        tenant_id,
    )


_BREX_INSTALL_SQL = """
SELECT bi.id, bi.tenant_id, bi.base_url, bi.secret_ref, bi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'account_id', ba.account_id,
             'account_name', ba.account_name,
             'account_kind', ba.account_kind,
             'txn_cursor', ba.txn_cursor
           ) ORDER BY ba.account_id
         ) FILTER (WHERE ba.id IS NOT NULL),
         '[]'::json
       ) AS accounts
  FROM brex_installations bi
  LEFT JOIN brex_accounts ba
    ON ba.brex_installation_id = bi.id AND ba.state = 'active'
 WHERE bi.id = $1 AND bi.tenant_id = $2 AND bi.disabled_at IS NULL
 GROUP BY bi.id
"""


async def load_brex_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_BREX_INSTALL_SQL, installation_id, tenant_id)


_RAMP_INSTALL_SQL = """
SELECT ri.id, ri.tenant_id, ri.business_id, ri.base_url, ri.secret_ref,
       ri.refresh_secret_ref, ri.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', re.entity_type,
             'updated_cursor', re.updated_cursor
           ) ORDER BY re.entity_type
         ) FILTER (WHERE re.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM ramp_installations ri
  LEFT JOIN ramp_entities re
    ON re.ramp_installation_id = ri.id AND re.state = 'active'
 WHERE ri.id = $1 AND ri.tenant_id = $2 AND ri.disabled_at IS NULL
 GROUP BY ri.id
"""


async def load_ramp_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_RAMP_INSTALL_SQL, installation_id, tenant_id)


_GUSTO_INSTALL_SQL = """
SELECT gi.id, gi.tenant_id, gi.company_uuid, gi.base_url, gi.secret_ref,
       gi.refresh_secret_ref, gi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', ge.entity_type,
             'updated_cursor', ge.updated_cursor
           ) ORDER BY ge.entity_type
         ) FILTER (WHERE ge.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM gusto_installations gi
  LEFT JOIN gusto_entities ge
    ON ge.gusto_installation_id = gi.id AND ge.state = 'active'
 WHERE gi.id = $1 AND gi.tenant_id = $2 AND gi.disabled_at IS NULL
 GROUP BY gi.id
"""


async def load_gusto_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_GUSTO_INSTALL_SQL, installation_id, tenant_id)


_DEEL_INSTALL_SQL = """
SELECT di.id, di.tenant_id, di.base_url, di.secret_ref, di.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'contract_id', dc.contract_id,
             'payment_cursor', dc.payment_cursor
           ) ORDER BY dc.contract_id
         ) FILTER (WHERE dc.id IS NOT NULL),
         '[]'::json
       ) AS contracts
  FROM deel_installations di
  LEFT JOIN deel_contracts dc
    ON dc.deel_installation_id = di.id AND dc.state = 'active'
 WHERE di.id = $1 AND di.tenant_id = $2 AND di.disabled_at IS NULL
 GROUP BY di.id
"""


async def load_deel_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_DEEL_INSTALL_SQL, installation_id, tenant_id)


async def load_fireflies_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await _load_simple_installation(
        executor,
        tenant_id=tenant_id,
        installation_id=installation_id,
        table="fireflies_installations",
        columns=(
            "id, tenant_id, base_url, workspace_id, transcript_cursor, "
            "secret_ref, disabled_at"
        ),
    )


_SIGNAL_INSTALL_SQL = """
SELECT si.id, si.tenant_id, si.account_label, si.session_secret_ref,
       si.backfill_session_secret_ref, si.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'thread_id', st.thread_id,
             'thread_kind', st.thread_kind,
             'title', st.title,
             'offset_id_cursor', st.offset_id_cursor
           ) ORDER BY st.thread_id
         ) FILTER (WHERE st.id IS NOT NULL),
         '[]'::json
       ) AS threads
  FROM signal_installations si
  LEFT JOIN signal_threads st
    ON st.signal_installation_id = si.id AND st.state = 'active'
 WHERE si.id = $1 AND si.tenant_id = $2 AND si.disabled_at IS NULL
 GROUP BY si.id
"""


async def load_signal_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(
        _SIGNAL_INSTALL_SQL,
        installation_id,
        tenant_id,
    )


async def load_aws_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await _load_simple_installation(
        executor,
        tenant_id=tenant_id,
        installation_id=installation_id,
        table="aws_installations",
        columns=(
            "id, tenant_id, account_id, region, credential_kind, secret_ref, "
            "events_cursor_ms, disabled_at"
        ),
    )


_MIRO_INSTALL_SQL = """
SELECT mi.id, mi.tenant_id, mi.base_url, mi.org_id, mi.secret_ref,
       mi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'board_id', mb.board_id,
             'board_name', mb.board_name,
             'board_kind', mb.board_kind,
             'item_cursor', mb.item_cursor
           ) ORDER BY mb.board_id
         ) FILTER (WHERE mb.id IS NOT NULL),
         '[]'::json
       ) AS boards
  FROM miro_installations mi
  LEFT JOIN miro_boards mb
    ON mb.miro_installation_id = mi.id AND mb.state = 'active'
 WHERE mi.id = $1 AND mi.tenant_id = $2 AND mi.disabled_at IS NULL
 GROUP BY mi.id
"""


async def load_miro_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_MIRO_INSTALL_SQL, installation_id, tenant_id)


_FIGMA_INSTALL_SQL = """
SELECT fi.id, fi.tenant_id, fi.base_url, fi.team_id, fi.secret_ref,
       fi.auth_kind, fi.refresh_secret_ref, fi.token_expires_at,
       fi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'file_key', ff.file_key,
             'file_name', ff.file_name,
             'project_name', ff.project_name,
             'event_cursor', ff.event_cursor,
             'snapshot_version', ff.snapshot_version
           ) ORDER BY ff.file_key
         ) FILTER (WHERE ff.id IS NOT NULL),
         '[]'::json
       ) AS files
  FROM figma_installations fi
  LEFT JOIN figma_files ff
    ON ff.figma_installation_id = fi.id AND ff.state = 'active'
 WHERE fi.id = $1 AND fi.tenant_id = $2 AND fi.disabled_at IS NULL
 GROUP BY fi.id
"""


async def load_figma_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_FIGMA_INSTALL_SQL, installation_id, tenant_id)


_RECORD_FIGMA_ONBOARDING_FAILURE_SQL = """
UPDATE figma_installations
   SET connection_state = 'degraded', last_error = $3
 WHERE id = $1
   AND tenant_id = $2
   AND disabled_at IS NULL
   AND connection_state NOT IN ('reauthorization_required', 'disconnected')
"""


async def record_figma_onboarding_failure(
    executor: Any,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    failure_reason: str,
) -> None:
    await executor.execute(
        _RECORD_FIGMA_ONBOARDING_FAILURE_SQL,
        installation_id,
        tenant_id,
        failure_reason,
    )


_CARTA_INSTALL_SQL = """
SELECT ci.id, ci.tenant_id, ci.firm_id, ci.base_url, ci.secret_ref,
       ci.refresh_secret_ref, ci.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', ce.entity_type,
             'updated_cursor', ce.updated_cursor
           ) ORDER BY ce.entity_type
         ) FILTER (WHERE ce.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM carta_installations ci
  LEFT JOIN carta_entities ce
    ON ce.carta_installation_id = ci.id AND ce.state = 'active'
 WHERE ci.id = $1 AND ci.tenant_id = $2 AND ci.disabled_at IS NULL
 GROUP BY ci.id
"""


async def load_carta_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_CARTA_INSTALL_SQL, installation_id, tenant_id)


_HIBOB_INSTALL_SQL = """
SELECT hi.id, hi.tenant_id, hi.company_id, hi.service_user_id, hi.base_url,
       hi.secret_ref, hi.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', he.entity_type,
             'updated_cursor', he.updated_cursor
           ) ORDER BY he.entity_type
         ) FILTER (WHERE he.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM hibob_installations hi
  LEFT JOIN hibob_entities he
    ON he.hibob_installation_id = hi.id AND he.state = 'active'
 WHERE hi.id = $1 AND hi.tenant_id = $2 AND hi.disabled_at IS NULL
 GROUP BY hi.id
"""


async def load_hibob_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_HIBOB_INSTALL_SQL, installation_id, tenant_id)


_ASHBY_INSTALL_SQL = """
SELECT ai.id, ai.tenant_id, ai.org_id, ai.base_url, ai.secret_ref,
       ai.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', ae.entity_type,
             'sync_cursor', ae.sync_cursor
           ) ORDER BY ae.entity_type
         ) FILTER (WHERE ae.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM ashby_installations ai
  LEFT JOIN ashby_entities ae
    ON ae.ashby_installation_id = ai.id AND ae.state = 'active'
 WHERE ai.id = $1 AND ai.tenant_id = $2 AND ai.disabled_at IS NULL
 GROUP BY ai.id
"""


async def load_ashby_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(_ASHBY_INSTALL_SQL, installation_id, tenant_id)


_LINKEDIN_INSTALL_SQL = """
SELECT li.id, li.tenant_id, li.organization_urn, li.base_url, li.secret_ref,
       li.refresh_secret_ref, li.disabled_at,
       COALESCE(
         json_agg(
           json_build_object(
             'entity_type', le.entity_type,
             'updated_cursor', le.updated_cursor
           ) ORDER BY le.entity_type
         ) FILTER (WHERE le.id IS NOT NULL),
         '[]'::json
       ) AS entities
  FROM linkedin_installations li
  LEFT JOIN linkedin_entities le
    ON le.linkedin_installation_id = li.id AND le.state = 'active'
 WHERE li.id = $1 AND li.tenant_id = $2 AND li.disabled_at IS NULL
 GROUP BY li.id
"""


async def load_linkedin_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(
        _LINKEDIN_INSTALL_SQL,
        installation_id,
        tenant_id,
    )


_FACEBOOK_PAGES_INSTALL_SQL = """
SELECT id, tenant_id, page_id, page_name, page_access_token_ref,
       user_access_token_ref, user_token_expires_at, connection_state,
       reauthorization_required_at,
       page_token_recovery_next_attempt_at, page_token_recovery_attempts,
       page_token_recovery_last_attempt_at,
       page_recovery_last_error_code
           AS page_token_recovery_last_error_code,
       app_secret_ref, verify_token_ref, granted_scopes, subscribed_fields,
       webhook_subscribed_at, enabled, oldest_message_at,
       backfill_exhausted_at, backfill_exhausted_reason,
       conversation_count, message_count
  FROM facebook_page_installations
 WHERE id = $1 AND tenant_id = $2 AND enabled = TRUE
"""


async def load_facebook_pages_installation(
    executor: Any, *, tenant_id: UUID, installation_id: UUID,
) -> Any | None:
    return await executor.fetchrow(
        _FACEBOOK_PAGES_INSTALL_SQL,
        installation_id,
        tenant_id,
    )


async def build_slack_planner_client(pool: Any, install: Any) -> Any:
    from services.ingest.ingestion.fetchers._clients import build_slack_client

    return await build_slack_client(install, pool=pool)


async def build_github_planner_client(pool: Any, install: Any) -> Any:
    from services.ingest.ingestion.fetchers._clients import build_github_client

    return await build_github_client(install, pool=pool)


async def build_discord_planner_client(pool: Any, install: Any) -> Any:
    from services.ingest.ingestion.fetchers._clients import build_discord_client

    return await build_discord_client(install, pool=pool)


async def build_notion_planner_client(pool: Any, install: Any) -> Any:
    from services.ingest.ingestion.fetchers._clients import build_notion_client

    return await build_notion_client(install, pool=pool)


__all__ = [
    "build_discord_planner_client",
    "build_github_planner_client",
    "build_notion_planner_client",
    "build_slack_planner_client",
    "load_ashby_installation",
    "load_aws_installation",
    "load_brex_installation",
    "load_carta_installation",
    "load_deel_installation",
    "load_discord_installation",
    "load_facebook_pages_installation",
    "load_figma_installation",
    "load_fireflies_installation",
    "load_github_installation",
    "load_gmail_installation",
    "load_google_calendar_installation",
    "load_google_drive_installation",
    "load_grafana_installation",
    "load_gusto_installation",
    "load_hibob_installation",
    "load_jira_installation",
    "load_linkedin_installation",
    "load_mercury_installation",
    "load_miro_installation",
    "load_notion_installation",
    "load_quickbooks_installation",
    "load_ramp_installation",
    "load_signal_installation",
    "load_slack_installation",
    "load_source_installation",
    "load_telegram_installation",
    "require_installation_row_id",
    "InstallationIdentityError",
    "record_figma_onboarding_failure",
]
