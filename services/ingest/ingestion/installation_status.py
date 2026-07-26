"""Contract-owned installation status reads.

Status and control-plane callers either request one exact Fyralis installation
row UUID or receive the complete tenant/source collection. Provider-native
identifiers remain useful metadata, but never substitute for the row UUID.
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from uuid import UUID

from lib.shared.secrets import load_app_secret_text_from_env
from services.ingest.source_contract.catalog import source_definition
from services.ingest.source_contract.models import (
    InstallationManagementDefinition,
)


def _unique(values: Iterable[str | None]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value is None or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return tuple(result)


def _credential_groups(
    management: InstallationManagementDefinition,
) -> tuple[tuple[str, ...], ...]:
    if management.status_credential_column_groups:
        return management.status_credential_column_groups
    if management.ref_columns:
        # The first ref is the primary provider credential. Refresh-token and
        # webhook refs are normally optional status detail, not prerequisites.
        return ((management.ref_columns[0],),)
    return ()


def _has_credentials(
    row: Any,
    groups: tuple[tuple[str, ...], ...],
) -> bool:
    return any(all(row[column] is not None for column in group) for group in groups)


async def load_managed_installation_status_rows(
    executor: Any,
    *,
    tenant_id: UUID,
    source: str,
    installation_row_id: UUID | None = None,
    include_disabled: bool = True,
) -> list[dict[str, Any]]:
    """Load a dedicated installation table through immutable contract metadata."""

    definition = source_definition(source)
    adapter = definition.installation_adapter
    management = adapter.management if adapter is not None else None
    if management is None:
        raise ValueError(f"source {definition.source_id!r} has no managed install table")

    credential_groups = _credential_groups(management)
    detail_columns = _unique(
        (
            management.scope_column,
            management.base_url_column,
            *management.extra_output_columns,
            *management.status_detail_columns,
        )
    )
    internal_columns = _unique(
        (
            *detail_columns,
            *(column for _, column in management.status_presence_columns),
            *(column for group in credential_groups for column in group),
        )
    )
    enabled_expression = (
        f"i.{management.enabled_column}"
        if management.enabled_column is not None
        else "i.disabled_at IS NULL"
    )
    selected_columns = ", ".join(
        (
            "i.id",
            "i.created_at AS installed_at",
            f"({enabled_expression}) AS enabled",
            *(f"i.{column}" for column in internal_columns),
        )
    )
    predicates = ["i.tenant_id = $1"]
    arguments: list[Any] = [tenant_id]
    if installation_row_id is not None:
        arguments.append(installation_row_id)
        predicates.append(f"i.id = ${len(arguments)}")
    if not include_disabled:
        predicates.append(f"({enabled_expression})")
    order_column = management.updated_at_column or "created_at"
    rows = await executor.fetch(
        f"""
        SELECT {selected_columns}
          FROM {management.table} AS i
         WHERE {' AND '.join(predicates)}
         ORDER BY ({enabled_expression}) DESC, i.{order_column} DESC, i.id
        """,
        *arguments,
    )

    output: list[dict[str, Any]] = []
    for row in rows:
        details = {column: row[column] for column in detail_columns}
        for output_name, column_name in management.status_presence_columns:
            details[output_name] = row[column_name] is not None
        output.append(
            {
                "id": row["id"],
                "installation_id": row["id"],
                "external_installation_id": row[management.scope_column],
                "enabled": bool(row["enabled"]),
                "has_secret": _has_credentials(row, credential_groups),
                "installed_at": row["installed_at"],
                "details": details,
            }
        )
    return output


async def load_provider_installation_status_rows(
    executor: Any,
    *,
    tenant_id: UUID,
    source: str,
    installation_row_id: UUID | None = None,
    include_disabled: bool = True,
) -> list[dict[str, Any]]:
    """Load all or one exact generic provider installation."""

    definition = source_definition(source)
    predicates = ["tenant_id = $1", "provider = $2"]
    arguments: list[Any] = [tenant_id, definition.source_id]
    if installation_row_id is not None:
        arguments.append(installation_row_id)
        predicates.append(f"id = ${len(arguments)}")
    if not include_disabled:
        predicates.append("enabled = TRUE")
    rows = await executor.fetch(
        f"""
        SELECT id, installation_id AS external_installation_id, enabled,
               secret_ref, installed_at
          FROM provider_installations
         WHERE {' AND '.join(predicates)}
         ORDER BY enabled DESC, installed_at DESC, id
        """,
        *arguments,
    )
    return [
        {
            "id": row["id"],
            "installation_id": row["id"],
            "external_installation_id": row["external_installation_id"],
            "enabled": bool(row["enabled"]),
            "has_secret": row["secret_ref"] is not None,
            "installed_at": row["installed_at"],
            "details": {
                "external_installation_id": row["external_installation_id"],
            },
        }
        for row in rows
    ]


async def load_github_installation_status_rows(
    executor: Any,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """GitHub App credentials are deployment-owned, not stored per row."""

    rows = await load_provider_installation_status_rows(executor, **kwargs)
    for row in rows:
        if row["enabled"]:
            row["has_secret"] = True
            row["details"]["credential_scope"] = (
                "github_app_level_private_key_and_webhook_secret"
            )
    return rows


async def load_discord_installation_status_rows(
    executor: Any,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """Discord's bot token is deployment-owned while guild rows are exact."""

    rows = await load_provider_installation_status_rows(executor, **kwargs)
    has_bot_token = bool(load_app_secret_text_from_env("DISCORD_BOT_TOKEN"))
    for row in rows:
        if row["enabled"]:
            row["has_secret"] = has_bot_token
            row["details"]["credential_scope"] = (
                "discord_app_level_bot_token"
            )
    return rows


async def load_facebook_pages_installation_status_rows(
    executor: Any,
    *,
    tenant_id: UUID,
    source: str,
    installation_row_id: UUID | None = None,
    include_disabled: bool = True,
) -> list[dict[str, Any]]:
    """Load Meta Page installations while retaining Page ID as metadata."""

    if source_definition(source).source_id != "facebook_pages":
        raise ValueError("Facebook Pages status loader received another source")
    predicates = ["tenant_id = $1"]
    arguments: list[Any] = [tenant_id]
    if installation_row_id is not None:
        arguments.append(installation_row_id)
        predicates.append(f"id = ${len(arguments)}")
    if not include_disabled:
        predicates.append("enabled = TRUE")
    rows = await executor.fetch(
        f"""
        SELECT id, page_id, enabled,
               (page_access_token_ref IS NOT NULL
                AND app_secret_ref IS NOT NULL
                AND verify_token_ref IS NOT NULL) AS has_secret,
               created_at AS installed_at, page_name, oldest_message_at,
               backfill_exhausted_at, backfill_exhausted_reason,
               conversation_count, message_count
          FROM facebook_page_installations
         WHERE {' AND '.join(predicates)}
         ORDER BY enabled DESC, updated_at DESC, id
        """,
        *arguments,
    )
    return [
        {
            "id": row["id"],
            "installation_id": row["id"],
            "external_installation_id": row["page_id"],
            "enabled": bool(row["enabled"]),
            "has_secret": bool(row["has_secret"]),
            "installed_at": row["installed_at"],
            "details": {
                "page_id": row["page_id"],
                "page_name": row["page_name"],
                "coverage": "All available history",
                "oldest_message_at": row["oldest_message_at"],
                "backfill_exhausted_at": row["backfill_exhausted_at"],
                "backfill_exhausted_reason": row["backfill_exhausted_reason"],
                "conversation_count": row["conversation_count"],
                "message_count": row["message_count"],
            },
        }
        for row in rows
    ]


__all__ = [
    "load_discord_installation_status_rows",
    "load_facebook_pages_installation_status_rows",
    "load_github_installation_status_rows",
    "load_managed_installation_status_rows",
    "load_provider_installation_status_rows",
]
