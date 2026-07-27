"""Contract-owned GitHub webhook policy.

The shared webhook router verifies signatures and resolves tenants.  GitHub's
provider-specific decisions live here and are referenced directly by the
canonical ``WebhookIngressDefinition``:

* bootstrap ``ping`` and replay handling run after signature verification but
  before an unresolved installation is rejected;
* installation lifecycle events and selected-repository filtering run after
  exact tenant and installation resolution.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

import structlog
from fastapi import Request
from fastapi.responses import JSONResponse

from services.ingest.integrations.github import lifecycle, metrics


log = structlog.get_logger("integrations.github.webhook_ingress")

_LIFECYCLE_EVENTS = frozenset({"installation", "installation_repositories"})


def _header(headers: Mapping[str, str], name: str) -> str | None:
    return headers.get(name) or headers.get(name.casefold())


def _event_type(headers: Mapping[str, str]) -> str | None:
    return _header(headers, "X-GitHub-Event")


def _delivery_id(headers: Mapping[str, str]) -> str | None:
    return _header(headers, "X-GitHub-Delivery")


def _installation_id(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    installation = payload.get("installation")
    if not isinstance(installation, Mapping):
        return None
    value = installation.get("id")
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    normalized = str(value).strip()
    return normalized or None


def _repository_full_name(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    repository = payload.get("repository")
    if not isinstance(repository, Mapping):
        return None
    value = repository.get("full_name")
    return value if isinstance(value, str) and value else None


async def _selected_repositories(
    pool: Any,
    installation_row_id: Any,
) -> list[str] | None:
    """Return the exact installation's repository selection.

    ``None`` means all-repositories mode, while ``[]`` means the installation
    explicitly selects no repositories.
    """

    if pool is None or installation_row_id is None:
        return None
    row = await pool.fetchrow(
        """
        SELECT selected_repositories
          FROM provider_installations
         WHERE id = $1
        """,
        installation_row_id,
    )
    if row is None:
        return None
    raw = row["selected_repositories"]
    if raw is None:
        return None
    if isinstance(raw, list):
        return [value for value in raw if isinstance(value, str)]
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if isinstance(parsed, list):
        return [value for value in parsed if isinstance(value, str)]
    return None


async def handle_verified_pre_tenant(
    *,
    request: Request,
    runtime: Any,
    payload: Mapping[str, Any] | None,
) -> JSONResponse | None:
    """Handle verified bootstrap pings and replayed deliveries.

    This hook deliberately runs before tenant-outcome rejection: a GitHub App
    bootstrap ping can arrive before an installation row exists.  Every other
    event continues into exact tenant resolution.
    """

    event_type = _event_type(request.headers)
    delivery_id = _delivery_id(request.headers)
    if event_type == "ping":
        metrics.record_webhook_verified(result="ok")
        log.info(
            "github_webhook_ping",
            event_type=event_type,
            delivery_id=delivery_id,
        )
        return JSONResponse({"handled": "ping"}, status_code=200)

    replay_cache = runtime.github_replay_cache
    installation_id = _installation_id(payload)
    if replay_cache is None or installation_id is None or delivery_id is None:
        return None
    if not replay_cache.seen(installation_id, delivery_id):
        return None

    metrics.record_replay_dropped()
    log.info("github_webhook_replay_dropped", delivery_id=delivery_id)
    return JSONResponse({"handled": "replay"}, status_code=200)


async def handle_verified_tenant(
    *,
    request: Request,
    runtime: Any,
    outcome: Any,
    tenant_id: Any,
    payload: Mapping[str, Any] | None,
    verified: Any,
    process_unit: Any | None = None,
) -> JSONResponse | None:
    """Handle lifecycle state and repository selection for one installation."""

    del tenant_id, verified, process_unit
    # Exact identities are carried by ``outcome``.
    event_type = _event_type(request.headers)
    installation_id = _installation_id(payload)
    if event_type in _LIFECYCLE_EVENTS:
        return await _handle_lifecycle(
            runtime=runtime,
            outcome=outcome,
            payload=payload or {},
            event_type=event_type,
            installation_id=installation_id,
        )

    selected = await _selected_repositories(
        runtime.pool,
        outcome.installation_row_id,
    )
    if selected is None:
        return None
    repository = _repository_full_name(payload)
    if repository is not None and repository in selected:
        return None

    metrics.record_filtered_repo(reason="not_selected")
    log.info(
        "github_webhook_filtered_repo",
        event_type=event_type,
        repo_full_name=repository,
    )
    return JSONResponse({"handled": "filtered_repo"}, status_code=200)


async def _handle_lifecycle(
    *,
    runtime: Any,
    outcome: Any,
    payload: Mapping[str, Any],
    event_type: str,
    installation_id: str | None,
) -> JSONResponse:
    pool = runtime.pool
    if pool is None or installation_id is None:
        log.error(
            "github_lifecycle_deps_missing",
            has_pool=pool is not None,
            has_installation_id=installation_id is not None,
        )
        return JSONResponse({"handled": event_type}, status_code=200)

    github_client = runtime.github_client
    token_cache = (
        getattr(github_client, "_installation_tokens", None)
        if github_client is not None
        else None
    )
    try:
        body = await lifecycle.dispatch(
            event_type=event_type,
            payload=payload,
            tenant_id=outcome.tenant_id,
            installation_row_id=outcome.installation_row_id,
            installation_id=installation_id,
            pool=pool,
            installation_token_cache=token_cache,
            tenant_resolver=runtime.tenant_resolver,
        )
    except Exception as exc:  # noqa: BLE001 - preserve provider ACK semantics
        log.error(
            "github_lifecycle_dispatch_failed",
            event_type=event_type,
            error_type=type(exc).__name__,
        )
        return JSONResponse(
            {"handled": event_type, "error": "dispatch_failed"},
            status_code=200,
        )
    return JSONResponse(body, status_code=200)


__all__ = [
    "handle_verified_pre_tenant",
    "handle_verified_tenant",
]
