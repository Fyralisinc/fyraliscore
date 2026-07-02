"""Hosted-portal onboarding routes for Design Partner BYOC."""
from __future__ import annotations

import json
import os
from datetime import timedelta
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, status

from services.app.gateway.auth import create_session
from services.platform.runtime.byoc_onboarding_intents import (
    CreateOnboardingIntentRequest,
    InMemoryOnboardingIntentStore,
    OnboardingIntentNotFound,
    OnboardingIntentRecord,
    OnboardingIntentStore,
    PostgresOnboardingIntentStore,
    SubmitDesignPartnerIntakeRequest,
    UnsupportedOnboardingPlan,
)


def build_byoc_onboarding_router(
    *,
    store: OnboardingIntentStore | None = None,
) -> APIRouter:
    router = APIRouter(
        prefix="/platform/onboarding",
        tags=["platform-onboarding"],
    )

    @router.post(
        "/intents",
        status_code=status.HTTP_201_CREATED,
    )
    async def create_onboarding_intent(
        request: Request,
        payload: CreateOnboardingIntentRequest,
    ) -> OnboardingIntentRecord:
        try:
            return await (store or _store_from_state(request)).create_intent(payload)
        except UnsupportedOnboardingPlan as exc:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail={
                    "error": "unsupported_onboarding_plan",
                    "message": str(exc),
                },
            ) from exc

    @router.post("/intents/{intent_id}/design-partner-intake")
    async def submit_design_partner_intake(
        request: Request,
        intent_id: str,
        payload: SubmitDesignPartnerIntakeRequest,
    ) -> OnboardingIntentRecord:
        try:
            return await (
                store or _store_from_state(request)
            ).submit_design_partner_intake(intent_id, payload)
        except OnboardingIntentNotFound as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "onboarding_intent_not_found"},
            ) from exc
        except UnsupportedOnboardingPlan as exc:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail={
                    "error": "unsupported_onboarding_plan",
                    "message": str(exc),
                },
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "invalid_onboarding_intake", "message": str(exc)},
            ) from exc

    @router.post("/slack/rehearsal/prepare")
    async def prepare_slack_rehearsal(request: Request) -> dict[str, Any]:
        _require_slack_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, actor_id = _rehearsal_actor_ids()
        await _ensure_rehearsal_actor(pool, tenant_id=tenant_id, actor_id=actor_id)

        from services.ingest.integrations.slack import oauth as slack_oauth

        client_id = os.environ.get("SLACK_CLIENT_ID", "").strip()
        redirect_uri = os.environ.get("SLACK_REDIRECT_URI", "").strip()
        public_url = _public_url_from_env_or_request(request)
        if not client_id or not redirect_uri:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "slack_oauth_unconfigured",
                    "message": "SLACK_CLIENT_ID and SLACK_REDIRECT_URI are required.",
                },
            )

        token, ctx = await create_session(
            pool,
            actor_id=actor_id,
            tenant_id=tenant_id,
            ttl=timedelta(hours=24),
        )
        state_token = await slack_oauth.issue_state_token(tenant_id, pool)
        install_url = (
            f"{slack_oauth._SLACK_AUTHORIZE_URL}?"  # noqa: SLF001
            + urlencode(
                {
                    "client_id": client_id,
                    "scope": slack_oauth._SLACK_BOT_SCOPES,  # noqa: SLF001
                    "user_scope": slack_oauth._SLACK_USER_SCOPES,  # noqa: SLF001
                    "redirect_uri": redirect_uri,
                    "state": state_token,
                }
            )
        )
        status_payload = await _slack_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            bearer_token=token,
            session_expires_at=ctx.expires_at.isoformat(),
        )
        return {
            "enabled": True,
            "tenant_id": str(tenant_id),
            "actor_id": str(actor_id),
            "gateway_api_base": str(request.base_url).rstrip("/"),
            "provider_ingress_url": public_url,
            "oauth_redirect_url": redirect_uri,
            "events_request_url": f"{public_url}/webhooks/slack/events",
            "install_url": install_url,
            "bearer_token": token,
            "session_expires_at": ctx.expires_at.isoformat(),
            "state_expires_in_seconds": 600,
            "status": status_payload,
        }

    @router.get("/slack/rehearsal/status")
    async def slack_rehearsal_status(request: Request) -> dict[str, Any]:
        _require_slack_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, _actor_id = _rehearsal_actor_ids()
        return await _slack_rehearsal_status_payload(pool, tenant_id=tenant_id)

    return router


def _store_from_state(request: Request) -> OnboardingIntentStore:
    existing = getattr(request.app.state, "byoc_onboarding_intent_store", None)
    if existing is not None:
        return existing
    deps = getattr(request.app.state, "deps", None)
    pool = getattr(deps, "pool", None)
    if pool is not None:
        created = PostgresOnboardingIntentStore(pool)
    else:
        created = InMemoryOnboardingIntentStore()
    request.app.state.byoc_onboarding_intent_store = created
    return created


def _pool_from_state(request: Request) -> Any:
    deps = getattr(request.app.state, "deps", None)
    pool = getattr(deps, "pool", None)
    if pool is None:
        pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "gateway_pool_unavailable"},
        )
    return pool


def _require_slack_rehearsal_enabled(request: Request) -> None:
    settings = getattr(request.app.state, "gateway_settings", None)
    env_name = (
        getattr(settings, "environment", None)
        or os.environ.get("FYRALIS_ENV")
        or os.environ.get("COMPANY_OS_ENV")
        or ""
    )
    is_production = bool(getattr(settings, "is_production", False)) or (
        str(env_name).strip().lower() in {"prod", "production"}
    )
    if is_production or not _truthy(os.environ.get("FYRALIS_SLACK_REHEARSAL_ENABLED")):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "slack_rehearsal_not_enabled"},
        )


def _truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _rehearsal_actor_ids() -> tuple[UUID, UUID]:
    tenant_id = os.environ.get("COMPANY_OS_TENANT_ID", "").strip()
    actor_id = os.environ.get("COMPANY_OS_CEO_ACTOR_ID", "").strip()
    if not tenant_id or not actor_id:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "slack_rehearsal_tenant_unconfigured",
                "message": "COMPANY_OS_TENANT_ID and COMPANY_OS_CEO_ACTOR_ID are required.",
            },
        )
    try:
        return UUID(tenant_id), UUID(actor_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "slack_rehearsal_tenant_invalid"},
        ) from exc


def _public_url_from_env_or_request(request: Request) -> str:
    public_url = (
        os.environ.get("SANDBOX_PUBLIC_URL")
        or os.environ.get("FYRALIS_PROVIDER_INGRESS_URL")
        or str(request.base_url).rstrip("/")
    )
    return public_url.rstrip("/")


async def _ensure_rehearsal_actor(
    pool: Any,
    *,
    tenant_id: UUID,
    actor_id: UUID,
) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO tenants (id, name)
                VALUES ($1, 'sandbox')
                ON CONFLICT (id) DO NOTHING
                """,
                tenant_id,
            )
            await conn.execute(
                """
                INSERT INTO actors
                    (id, tenant_id, type, display_name, email, status, metadata, created_at)
                VALUES ($1, $2, 'human_internal', 'Fyralis setup owner',
                        'operator@fyralis.test', 'active', $3::jsonb, now())
                ON CONFLICT (id) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    email = EXCLUDED.email
                """,
                actor_id,
                tenant_id,
                json.dumps(
                    {
                        "role": "setup_owner",
                        "synthetic_persona": False,
                        "created_by": "slack_rehearsal_automation",
                    }
                ),
            )


async def _slack_rehearsal_status_payload(
    pool: Any,
    *,
    tenant_id: UUID,
    bearer_token: str | None = None,
    session_expires_at: str | None = None,
) -> dict[str, Any]:
    install = await pool.fetchrow(
        """
        SELECT installation_id, enabled, (secret_ref IS NOT NULL) AS has_secret,
               installed_at
          FROM provider_installations
         WHERE tenant_id = $1 AND provider = 'slack'
         ORDER BY installed_at DESC
         LIMIT 1
        """,
        tenant_id,
    )
    triggers = await pool.fetchrow(
        """
        SELECT count(*)::int AS total,
               count(*) FILTER (WHERE consumed_at IS NOT NULL)::int AS consumed
          FROM onboarding_triggers
         WHERE tenant_id = $1 AND source = 'slack'
        """,
        tenant_id,
    )
    runs = await pool.fetch(
        """
        SELECT status, count(*)::int AS count
          FROM onboarding_runs
         WHERE tenant_id = $1 AND 'slack' = ANY(sources_enabled)
         GROUP BY status
         ORDER BY status
        """,
        tenant_id,
    )
    shards = await pool.fetch(
        """
        SELECT state, count(*)::int AS count,
               coalesce(sum(observations_seen), 0)::int AS observations_seen
          FROM onboarding_shards
         WHERE tenant_id = $1 AND source = 'slack'
         GROUP BY state
         ORDER BY state
        """,
        tenant_id,
    )
    observation_rows = await pool.fetch(
        """
        SELECT id, kind, source_channel, occurred_at, content_text
          FROM observations
         WHERE tenant_id = $1 AND source_channel LIKE 'slack:%'
         ORDER BY occurred_at DESC
         LIMIT 25
        """,
        tenant_id,
    )
    observation_count = await pool.fetchval(
        """
        SELECT count(*)::int
          FROM observations
         WHERE tenant_id = $1 AND source_channel LIKE 'slack:%'
        """,
        tenant_id,
    )
    failures = await pool.fetchrow(
        """
        SELECT count(*)::int AS total
          FROM ingestion_failures
         WHERE tenant_id = $1 AND source = 'slack' AND resolved_at IS NULL
        """,
        tenant_id,
    )
    installed = install is not None and bool(install["enabled"])
    trigger_total = int(triggers["total"] if triggers else 0)
    observations = [
        {
            "id": str(row["id"]),
            "kind": row["kind"],
            "source_channel": row["source_channel"],
            "occurred_at": row["occurred_at"].isoformat(),
            "content_text": row["content_text"],
        }
        for row in observation_rows
    ]
    return {
        "installed": installed,
        "installation": (
            {
                "installation_id": install["installation_id"],
                "enabled": bool(install["enabled"]),
                "has_secret": bool(install["has_secret"]),
                "installed_at": install["installed_at"].isoformat(),
            }
            if install
            else None
        ),
        "trigger_count": trigger_total,
        "consumed_trigger_count": int(triggers["consumed"] if triggers else 0),
        "run_status_counts": {
            row["status"]: int(row["count"])
            for row in runs
        },
        "shard_state_counts": {
            row["state"]: {
                "count": int(row["count"]),
                "observations_seen": int(row["observations_seen"]),
            }
            for row in shards
        },
        "observation_count": int(observation_count or 0),
        "observations": observations,
        "unresolved_failure_count": int(failures["total"] if failures else 0),
        "bearer_token": bearer_token,
        "session_expires_at": session_expires_at,
        "next_action": _slack_rehearsal_next_action(
            installed=installed,
            trigger_count=trigger_total,
            observation_count=int(observation_count or 0),
        ),
    }


def _slack_rehearsal_next_action(
    *,
    installed: bool,
    trigger_count: int,
    observation_count: int,
) -> str:
    if not installed:
        return "Approve Slack OAuth in the browser."
    if trigger_count == 0:
        return "Slack installed; waiting for onboarding trigger."
    if observation_count == 0:
        return "Slack installed; waiting for historical backfill or a live message."
    return "Slack observations are landing in Fyralis."


__all__ = ["build_byoc_onboarding_router"]
