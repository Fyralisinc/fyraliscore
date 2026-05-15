"""services/integrations/github/lifecycle.py — handle `installation`
and `installation_repositories` webhook events.

Routed from `services/webhooks/router.py` (T058) AFTER signature
verification, replay-cache pass-through, and tenant resolution. The
existing ingestion handler at `services/ingestion/handlers/github.py`
is NOT invoked for these event types — they are pure installation-
lifecycle state changes, not Observations.

Dispatch table (FR-009, FR-010):

  installation.created      → no-op if row exists; raise unknown
                              installation if not (FR-009 final clause)
  installation.deleted      → _disable_installation_github
  installation.suspend      → _disable_installation_github (audit='suspend')
  installation.unsuspend    → re-enable row (audit='unsuspend')
  installation_repositories.added    → merge into selected_repositories
  installation_repositories.removed  → remove from selected_repositories

Idempotency: every action is safe to replay. Adding a repo already
present is a no-op (audit row still written for forensic traceability).

Logging redaction (FR-016 / SC-008): never log raw `installation_id`,
`account.login`, or `account.id`.
"""
from __future__ import annotations

import json
from typing import Any, Mapping
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7

from services.integrations.github import metrics
from services.integrations.github.uninstall import (
    _disable_installation_github,
    _short_installation_hash,
)


log = structlog.get_logger("integrations.github.lifecycle")


_SUPPORTED_INSTALLATION_ACTIONS = {
    "created",
    "deleted",
    "suspend",
    "unsuspend",
}
_SUPPORTED_REPO_ACTIONS = {"added", "removed"}


async def dispatch(
    *,
    event_type: str,
    payload: Mapping[str, Any],
    tenant_id: UUID,
    installation_row_id: UUID,
    installation_id: str,
    pool: asyncpg.Pool,
    installation_token_cache: dict[str, Any] | None = None,
    tenant_resolver: Any | None = None,
) -> dict[str, Any]:
    """Dispatch a verified, tenant-resolved lifecycle event.

    Returns the JSON body the webhook router emits with HTTP 200.
    Raises `ValidationError` for unsupported (event_type, action) pairs
    so the router maps them to 400.
    """
    action = payload.get("action") if isinstance(payload, Mapping) else None

    if event_type == "installation":
        return await _dispatch_installation(
            action=action,
            payload=payload,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            installation_id=installation_id,
            pool=pool,
            installation_token_cache=installation_token_cache,
            tenant_resolver=tenant_resolver,
        )
    if event_type == "installation_repositories":
        return await _dispatch_installation_repositories(
            action=action,
            payload=payload,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            installation_id=installation_id,
            pool=pool,
        )
    raise ValidationError(
        f"unsupported lifecycle event_type: {event_type!r}",
        event_type=event_type,
    )


# ---------------------------------------------------------------------
# installation.*
# ---------------------------------------------------------------------

async def _dispatch_installation(
    *,
    action: str | None,
    payload: Mapping[str, Any],
    tenant_id: UUID,
    installation_row_id: UUID,
    installation_id: str,
    pool: asyncpg.Pool,
    installation_token_cache: dict[str, Any] | None,
    tenant_resolver: Any | None,
) -> dict[str, Any]:
    if action not in _SUPPORTED_INSTALLATION_ACTIONS:
        raise ValidationError(
            f"unsupported installation action: {action!r}",
            event_type="installation",
            action=action,
        )

    metrics.record_lifecycle(event="installation", action=str(action))

    short_hash = _short_installation_hash(installation_id)
    log.info(
        "github_lifecycle_installation",
        installation_row_id=str(installation_row_id),
        tenant_id=str(tenant_id),
        installation_id_hash=short_hash,
        action=action,
    )

    if action == "created":
        # Row already exists (verified by tenant resolution at the
        # router layer); the webhook arrived AFTER the OAuth callback.
        # Audit it as a no-op for forensic traceability.
        await _audit(
            pool=pool,
            tenant_id=tenant_id,
            installation_row_id=installation_row_id,
            action="installation_created_noop",
            status="ok",
            context={
                "installation_id_hash": short_hash,
                "reason": "lifecycle_arrived_after_oauth",
            },
        )
        return {"handled": "installation_created"}

    if action == "deleted":
        await _disable_installation_github(
            pool=pool,
            installation_row_id=installation_row_id,
            tenant_id=tenant_id,
            installation_id=installation_id,
            reason="installation_deleted_webhook",
            audit_action="uninstall",
            installation_token_cache=installation_token_cache,
            tenant_resolver=tenant_resolver,
        )
        return {"handled": "installation_deleted"}

    if action == "suspend":
        await _disable_installation_github(
            pool=pool,
            installation_row_id=installation_row_id,
            tenant_id=tenant_id,
            installation_id=installation_id,
            reason="installation_suspend_webhook",
            audit_action="suspend",
            installation_token_cache=installation_token_cache,
            tenant_resolver=tenant_resolver,
        )
        return {"handled": "installation_suspend"}

    # action == "unsuspend"
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                UPDATE provider_installations
                   SET enabled = TRUE
                 WHERE id = $1
                   AND provider = 'github'
                RETURNING id
                """,
                installation_row_id,
            )
            if row is None:
                raise ValidationError(
                    "installation_row not found for unsuspend",
                    event_type="installation",
                    action="unsuspend",
                )
            await conn.execute(
                """
                INSERT INTO installation_audit_log
                    (id, tenant_id, installation_row_id, provider,
                     action, status, context)
                VALUES ($1, $2, $3, 'github', 'unsuspend', 'ok',
                        $4::jsonb)
                """,
                uuid7(),
                tenant_id,
                installation_row_id,
                json.dumps({"installation_id_hash": short_hash}),
            )

    if tenant_resolver is not None:
        try:
            cache = getattr(tenant_resolver, "_cache", None)
            if cache is not None:
                cache.invalidate(("github", installation_id))
        except Exception:  # noqa: BLE001
            pass

    return {"handled": "installation_unsuspend"}


# ---------------------------------------------------------------------
# installation_repositories.*
# ---------------------------------------------------------------------

async def _dispatch_installation_repositories(
    *,
    action: str | None,
    payload: Mapping[str, Any],
    tenant_id: UUID,
    installation_row_id: UUID,
    installation_id: str,
    pool: asyncpg.Pool,
) -> dict[str, Any]:
    if action not in _SUPPORTED_REPO_ACTIONS:
        raise ValidationError(
            f"unsupported installation_repositories action: {action!r}",
            event_type="installation_repositories",
            action=action,
        )

    metrics.record_lifecycle(
        event="installation_repositories", action=str(action),
    )

    short_hash = _short_installation_hash(installation_id)

    # repository_selection at the payload root indicates the install's
    # current mode (all vs explicit selection).
    repo_selection = payload.get("repository_selection")  # 'all' | 'selected'

    added = _extract_full_names(payload.get("repositories_added"))
    removed = _extract_full_names(payload.get("repositories_removed"))

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT selected_repositories
                  FROM provider_installations
                 WHERE id = $1
                   AND provider = 'github'
                """,
                installation_row_id,
            )
            if row is None:
                raise ValidationError(
                    "installation_row not found for repo_change",
                    event_type="installation_repositories",
                )
            current_raw = row["selected_repositories"]
            if current_raw is None:
                current: list[str] | None = None
            else:
                try:
                    parsed = (
                        current_raw
                        if isinstance(current_raw, list)
                        else json.loads(current_raw)
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    parsed = None
                current = parsed if isinstance(parsed, list) else None

            new_value = _merge_selection(
                current=current,
                repo_selection=repo_selection,
                added=added,
                removed=removed,
            )

            if new_value is None:
                serialized: str | None = None
            else:
                serialized = json.dumps(new_value)

            await conn.execute(
                """
                UPDATE provider_installations
                   SET selected_repositories = $2::jsonb
                 WHERE id = $1
                """,
                installation_row_id,
                serialized,
            )

            await conn.execute(
                """
                INSERT INTO installation_audit_log
                    (id, tenant_id, installation_row_id, provider,
                     action, status, context)
                VALUES ($1, $2, $3, 'github', 'repo_change', 'ok',
                        $4::jsonb)
                """,
                uuid7(),
                tenant_id,
                installation_row_id,
                json.dumps(
                    {
                        "installation_id_hash": short_hash,
                        "subaction": action,
                        "added": added,
                        "removed": removed,
                        "repository_selection": repo_selection,
                    }
                ),
            )

    return {
        "handled": f"installation_repositories_{action}",
        "count": len(added) + len(removed),
    }


def _extract_full_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for entry in value:
        if isinstance(entry, dict):
            full = entry.get("full_name")
            if isinstance(full, str) and full:
                out.append(full)
    return out


def _merge_selection(
    *,
    current: list[str] | None,
    repo_selection: Any,
    added: list[str],
    removed: list[str],
) -> list[str] | None:
    """Apply the added/removed merge against `current`, honoring the
    install's current `repository_selection` mode.

    Modes:
      - 'all'      → return None (NULL means "all-repositories")
      - 'selected' → return a list (possibly empty)
      - missing    → leave current mode unchanged when feasible
    """
    if repo_selection == "all":
        return None
    if repo_selection == "selected" and current is None:
        # Seed an empty selection then apply the merge.
        current = []
    if current is None:
        # Still in "all" mode and no selection signal — adding repos
        # under all-mode is semantically a no-op; preserve None.
        return None

    selection: list[str] = list(current)
    for name in added:
        if name not in selection:
            selection.append(name)
    selection = [name for name in selection if name not in set(removed)]
    return selection


# ---------------------------------------------------------------------
# Audit helper
# ---------------------------------------------------------------------

async def _audit(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    installation_row_id: UUID | None,
    action: str,
    status: str,
    context: dict[str, Any],
) -> None:
    try:
        await pool.execute(
            """
            INSERT INTO installation_audit_log
                (id, tenant_id, installation_row_id, provider,
                 action, status, context)
            VALUES ($1, $2, $3, 'github', $4, $5, $6::jsonb)
            """,
            uuid7(),
            tenant_id,
            installation_row_id,
            action,
            status,
            json.dumps(context),
        )
    except Exception:  # noqa: BLE001 — audit is best-effort
        log.error(
            "github_lifecycle_audit_failed",
            tenant_id=str(tenant_id),
            action=action,
        )


__all__ = ["dispatch"]
