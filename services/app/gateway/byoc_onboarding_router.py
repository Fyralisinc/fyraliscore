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

_OAUTH_REHEARSAL_SOURCES = {"slack", "github", "discord", "notion"}
_FORM_REHEARSAL_SOURCES = {"jira", "telegram"}
_REHEARSAL_SOURCES = _OAUTH_REHEARSAL_SOURCES | _FORM_REHEARSAL_SOURCES

_SOURCE_CALLBACK_PATHS = {
    "slack": "/integrations/slack/callback",
    "discord": "/integrations/discord/callback",
    "github": "/integrations/github/callback",
    "notion": "/integrations/notion/callback",
}

_SOURCE_LIVE_INGRESS_PATHS = {
    "slack": "/webhooks/slack/events",
    "discord": "/webhooks/discord",
    "github": "/webhooks/github",
    "notion": "/webhooks/notion/events",
    "jira": "/webhooks/jira/events",
    "telegram": "customer-cloud MTProto gateway worker",
}

_SOURCE_REQUIRED_INPUTS = {
    "jira": [
        "base_url",
        "account_email",
        "api_token",
        "webhook_secret",
    ],
    "telegram": [
        "account_label",
        "api_id",
        "api_hash",
        "live_session",
        "backfill_session",
    ],
}


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

    @router.post("/sources/{source_id}/rehearsal/prepare")
    async def prepare_source_rehearsal(
        request: Request,
        source_id: str,
    ) -> dict[str, Any]:
        source = _normalize_rehearsal_source(source_id)
        return await _prepare_source_rehearsal_response(request, source)

    @router.get("/sources/{source_id}/rehearsal/status")
    async def source_rehearsal_status(
        request: Request,
        source_id: str,
    ) -> dict[str, Any]:
        source = _normalize_rehearsal_source(source_id)
        _require_source_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, _actor_id = _rehearsal_actor_ids()
        return await _source_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            source=source,
        )

    @router.post("/sources/jira/rehearsal/finalize")
    async def finalize_jira_rehearsal(request: Request) -> dict[str, Any]:
        _require_source_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, actor_id = _rehearsal_actor_ids()
        await _ensure_rehearsal_actor(pool, tenant_id=tenant_id, actor_id=actor_id)

        from lib.shared.errors import JiraApiError
        from services.ingest.integrations.jira.client import JiraClient
        from services.ingest.integrations.jira.oauth import (
            _auth_failure_response,  # noqa: PLC2701
            _enumerate_projects,  # noqa: PLC2701
            _require_credentials,  # noqa: PLC2701
        )
        from services.ingest.integrations.jira.onboarding import (
            finalize_install,
            register_webhook_installation,
            site_host,
        )

        body = await request.json()
        base_url, account_email, api_token = _require_credentials(body)
        requested_keys = body.get("project_keys")
        if requested_keys is not None and not isinstance(requested_keys, list):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "project_keys_must_be_list"},
            )
        webhook_secret = (body.get("webhook_secret") or "").strip() or None

        client = JiraClient(
            base_url=base_url,
            account_email=account_email,
            api_token=api_token,
        )
        try:
            await client.myself()
            if requested_keys:
                project_keys = [
                    str(key).strip()
                    for key in requested_keys
                    if str(key).strip()
                ]
                project_meta: dict[str, dict] = {}
            else:
                project_keys, project_meta = await _enumerate_projects(client)
        except JiraApiError as exc:
            response = _auth_failure_response(exc)
            raise HTTPException(
                status_code=response.status_code,
                detail=json.loads(response.body.decode("utf-8")),
            ) from exc
        finally:
            await client.aclose()

        secret_store = _secret_store_from_state(request, pool)
        secret_ref = await secret_store.put(
            api_token,
            label=f"jira_api_token:{base_url}",
            tenant_id=tenant_id,
        )
        webhook_secret_ref = None
        if webhook_secret:
            webhook_secret_ref = await secret_store.put(
                webhook_secret,
                label=f"jira_webhook_secret:{base_url}",
                tenant_id=tenant_id,
            )

        install_id = await finalize_install(
            pool,
            tenant_id=tenant_id,
            base_url=base_url,
            account_email=account_email,
            project_keys=project_keys,
            secret_ref=secret_ref,
            webhook_secret_ref=webhook_secret_ref,
            project_meta=project_meta,
        )
        if webhook_secret_ref:
            await register_webhook_installation(
                pool,
                tenant_id=tenant_id,
                base_url=base_url,
                webhook_secret_ref=webhook_secret_ref,
            )

        status_payload = await _source_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            source="jira",
        )
        return {
            "ok": True,
            "source": "jira",
            "installation_id": str(install_id),
            "site_host": site_host(base_url),
            "project_count": len(project_keys),
            "webhook_registered": webhook_secret_ref is not None,
            "status": status_payload,
        }

    @router.post("/sources/telegram/rehearsal/finalize")
    async def finalize_telegram_rehearsal(request: Request) -> dict[str, Any]:
        _require_source_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, actor_id = _rehearsal_actor_ids()
        await _ensure_rehearsal_actor(pool, tenant_id=tenant_id, actor_id=actor_id)

        body = await request.json()
        account_label = (body.get("account_label") or "").strip()
        api_id = str(body.get("api_id") or "").strip()
        api_hash = (body.get("api_hash") or "").strip()
        live_session = (body.get("live_session") or body.get("session") or "").strip()
        backfill_session = (
            body.get("backfill_session") or live_session
        ).strip()
        if not (account_label and api_id and api_hash and live_session):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "telegram_missing_required_inputs",
                    "message": (
                        "account_label, api_id, api_hash, and live_session "
                        "are required."
                    ),
                },
            )

        from lib.shared.errors import TelegramApiError
        from services.ingest.integrations.telegram.client import TelegramClient
        from services.ingest.integrations.telegram.onboarding import finalize_install

        requested_dialogs = body.get("dialogs")
        if requested_dialogs is not None and not isinstance(requested_dialogs, list):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "telegram_dialogs_must_be_list"},
            )

        client = TelegramClient(
            api_id=api_id,
            api_hash=api_hash,
            session=backfill_session,
        )
        try:
            account = await client.me()
            dialogs = (
                requested_dialogs
                if requested_dialogs
                else await client.iter_dialogs(limit=75)
            )
        except TelegramApiError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": getattr(exc, "code", "telegram_connect_failed"),
                    "message": str(exc),
                },
            ) from exc
        finally:
            await client.aclose()

        if not dialogs:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "telegram_no_dialogs",
                    "message": "No Telegram dialogs were available to scope.",
                },
            )

        secret_store = _secret_store_from_state(request, pool)
        api_hash_ref = await secret_store.put(
            api_hash,
            label=f"telegram_api_hash:{account_label}",
            tenant_id=tenant_id,
        )
        live_session_ref = await secret_store.put(
            live_session,
            label=f"telegram_live_session:{account_label}",
            tenant_id=tenant_id,
        )
        backfill_session_ref = await secret_store.put(
            backfill_session,
            label=f"telegram_backfill_session:{account_label}",
            tenant_id=tenant_id,
        )

        install_id = await finalize_install(
            pool,
            tenant_id=tenant_id,
            account_label=account_label,
            dialogs=dialogs,
            api_id=api_id,
            api_hash_secret_ref=api_hash_ref,
            session_secret_ref=live_session_ref,
            backfill_session_secret_ref=backfill_session_ref,
        )
        status_payload = await _source_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            source="telegram",
        )
        return {
            "ok": True,
            "source": "telegram",
            "installation_id": str(install_id),
            "account": account,
            "dialog_count": len(dialogs),
            "status": status_payload,
        }

    @router.post("/slack/rehearsal/prepare")
    async def prepare_slack_rehearsal(request: Request) -> dict[str, Any]:
        return await _prepare_source_rehearsal_response(request, "slack")

    @router.get("/slack/rehearsal/status")
    async def slack_rehearsal_status(request: Request) -> dict[str, Any]:
        _require_source_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, _actor_id = _rehearsal_actor_ids()
        return await _source_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            source="slack",
        )

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


def _normalize_rehearsal_source(source_id: str) -> str:
    source = source_id.strip().lower().replace("-", "_")
    if source not in _REHEARSAL_SOURCES:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "source_rehearsal_not_supported",
                "source": source_id,
            },
        )
    return source


async def _prepare_source_rehearsal_response(
    request: Request,
    source: str,
) -> dict[str, Any]:
    _require_source_rehearsal_enabled(request)
    pool = _pool_from_state(request)
    tenant_id, actor_id = _rehearsal_actor_ids()
    await _ensure_rehearsal_actor(pool, tenant_id=tenant_id, actor_id=actor_id)

    token, ctx = await create_session(
        pool,
        actor_id=actor_id,
        tenant_id=tenant_id,
        ttl=timedelta(hours=24),
    )
    handoff = await _source_provider_handoff(
        source,
        pool=pool,
        tenant_id=tenant_id,
        request=request,
    )
    status_payload = await _source_rehearsal_status_payload(
        pool,
        tenant_id=tenant_id,
        source=source,
        bearer_token=token,
        session_expires_at=ctx.expires_at.isoformat(),
    )
    public_url = _public_url_from_env_or_request(request)
    callback_path = _SOURCE_CALLBACK_PATHS.get(source)
    live_path = _SOURCE_LIVE_INGRESS_PATHS.get(source)
    return {
        "enabled": True,
        "source": source,
        "tenant_id": str(tenant_id),
        "actor_id": str(actor_id),
        "gateway_api_base": str(request.base_url).rstrip("/"),
        "provider_ingress_url": public_url,
        "oauth_redirect_url": (
            handoff.get("oauth_redirect_url")
            or (f"{public_url}{callback_path}" if callback_path else None)
        ),
        "events_request_url": (
            f"{public_url}{live_path}"
            if live_path and live_path.startswith("/")
            else live_path
        ),
        "install_url": handoff.get("install_url"),
        "provider_console_url": handoff.get("provider_console_url"),
        "authorization_mode": handoff["authorization_mode"],
        "missing_configuration": handoff["missing_configuration"],
        "required_inputs": _SOURCE_REQUIRED_INPUTS.get(source, []),
        "bearer_token": token,
        "session_expires_at": ctx.expires_at.isoformat(),
        "state_expires_in_seconds": 600 if source in _OAUTH_REHEARSAL_SOURCES else None,
        "status": status_payload,
    }


async def _source_provider_handoff(
    source: str,
    *,
    pool: Any,
    tenant_id: UUID,
    request: Request,
) -> dict[str, Any]:
    public_url = _public_url_from_env_or_request(request)
    if source == "slack":
        from services.ingest.integrations.slack import oauth as slack_oauth

        client_id = os.environ.get("SLACK_CLIENT_ID", "").strip()
        redirect_uri = os.environ.get("SLACK_REDIRECT_URI", "").strip()
        missing = [
            name
            for name, value in {
                "SLACK_CLIENT_ID": client_id,
                "SLACK_REDIRECT_URI": redirect_uri,
                "SLACK_CLIENT_SECRET": os.environ.get("SLACK_CLIENT_SECRET", ""),
                "SLACK_SIGNING_SECRET": os.environ.get("SLACK_SIGNING_SECRET", ""),
            }.items()
            if not str(value).strip()
        ]
        install_url = None
        if client_id and redirect_uri:
            state_token = await slack_oauth.issue_state_token(tenant_id, pool)
            install_url = f"{slack_oauth._SLACK_AUTHORIZE_URL}?" + urlencode(  # noqa: SLF001
                {
                    "client_id": client_id,
                    "scope": slack_oauth._SLACK_BOT_SCOPES,  # noqa: SLF001
                    "user_scope": slack_oauth._SLACK_USER_SCOPES,  # noqa: SLF001
                    "redirect_uri": redirect_uri,
                    "state": state_token,
                }
            )
        return {
            "authorization_mode": "oauth",
            "install_url": install_url,
            "oauth_redirect_url": redirect_uri or f"{public_url}/integrations/slack/callback",
            "provider_console_url": "https://api.slack.com/apps",
            "missing_configuration": missing,
        }

    if source == "discord":
        from services.ingest.integrations.discord import oauth as discord_oauth

        client_id = os.environ.get("DISCORD_CLIENT_ID", "").strip()
        redirect_uri = os.environ.get("DISCORD_REDIRECT_URI", "").strip()
        missing = [
            name
            for name, value in {
                "DISCORD_CLIENT_ID": client_id,
                "DISCORD_REDIRECT_URI": redirect_uri,
                "DISCORD_CLIENT_SECRET": os.environ.get("DISCORD_CLIENT_SECRET", ""),
                "WEBHOOK_SECRET_DISCORD": os.environ.get("WEBHOOK_SECRET_DISCORD", ""),
            }.items()
            if not str(value).strip()
        ]
        install_url = None
        if client_id and redirect_uri:
            state_token = await discord_oauth.issue_state_token(tenant_id, pool)
            install_url = f"{discord_oauth._DISCORD_AUTHORIZE_URL}?" + urlencode(  # noqa: SLF001
                {
                    "client_id": client_id,
                    "scope": discord_oauth._DISCORD_SCOPES,  # noqa: SLF001
                    "permissions": discord_oauth._DISCORD_PERMISSIONS,  # noqa: SLF001
                    "redirect_uri": redirect_uri,
                    "response_type": "code",
                    "state": state_token,
                }
            )
        return {
            "authorization_mode": "oauth",
            "install_url": install_url,
            "oauth_redirect_url": redirect_uri or f"{public_url}/integrations/discord/callback",
            "provider_console_url": "https://discord.com/developers/applications",
            "missing_configuration": missing,
        }

    if source == "github":
        from services.ingest.integrations.github import oauth as github_oauth

        app_slug = os.environ.get("GITHUB_APP_SLUG", "").strip()
        missing = ["GITHUB_APP_SLUG"] if not app_slug else []
        install_url = None
        if app_slug:
            state_token = await github_oauth.issue_state_token(tenant_id, pool)
            install_url = (
                f"{github_oauth._GITHUB_INSTALL_BASE}/{app_slug}/installations/new?"  # noqa: SLF001
                + urlencode({"state": state_token})
            )
        return {
            "authorization_mode": "github_app",
            "install_url": install_url,
            "oauth_redirect_url": f"{public_url}/integrations/github/callback",
            "provider_console_url": "https://github.com/settings/apps",
            "missing_configuration": missing,
        }

    if source == "notion":
        from services.ingest.integrations.notion import oauth as notion_oauth

        client_id = os.environ.get("NOTION_CLIENT_ID", "").strip()
        redirect_uri = os.environ.get("NOTION_REDIRECT_URI", "").strip()
        missing = [
            name
            for name, value in {
                "NOTION_CLIENT_ID": client_id,
                "NOTION_REDIRECT_URI": redirect_uri,
                "NOTION_CLIENT_SECRET": os.environ.get("NOTION_CLIENT_SECRET", ""),
            }.items()
            if not str(value).strip()
        ]
        install_url = None
        if client_id and redirect_uri:
            state_token = await notion_oauth.issue_state_token(  # type: ignore[attr-defined]
                tenant_id,
                pool,
                provider="notion",
            )
            install_url = f"{notion_oauth._NOTION_AUTHORIZE_URL}?" + urlencode(  # noqa: SLF001
                {
                    "client_id": client_id,
                    "response_type": "code",
                    "owner": "user",
                    "redirect_uri": redirect_uri,
                    "state": state_token,
                }
            )
        return {
            "authorization_mode": "oauth",
            "install_url": install_url,
            "oauth_redirect_url": redirect_uri or f"{public_url}/integrations/notion/callback",
            "provider_console_url": "https://www.notion.so/my-integrations",
            "missing_configuration": missing,
        }

    if source == "jira":
        return {
            "authorization_mode": "customer_api_token",
            "install_url": None,
            "oauth_redirect_url": None,
            "provider_console_url": "https://id.atlassian.com/manage-profile/security/api-tokens",
            "missing_configuration": [],
        }

    if source == "telegram":
        return {
            "authorization_mode": "customer_mtproto_session",
            "install_url": None,
            "oauth_redirect_url": None,
            "provider_console_url": "https://my.telegram.org/apps",
            "missing_configuration": [],
        }

    raise AssertionError(f"unsupported rehearsal source {source!r}")


def _secret_store_from_state(request: Request, pool: Any) -> Any:
    runtime = getattr(request.app.state, "integration_runtime", None)
    store = getattr(runtime, "secret_store", None) if runtime is not None else None
    if store is None:
        store = getattr(request.app.state, "secret_store", None)
    if store is None:
        deps = getattr(request.app.state, "deps", None)
        store = getattr(deps, "secret_store", None) if deps is not None else None
    if store is None:
        from lib.shared.secrets import build_secret_store

        store = build_secret_store(pool)
        request.app.state.secret_store = store
    return store


def _require_source_rehearsal_enabled(request: Request) -> None:
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
    enabled = _truthy(os.environ.get("FYRALIS_SOURCE_REHEARSAL_ENABLED")) or _truthy(
        os.environ.get("FYRALIS_SLACK_REHEARSAL_ENABLED")
    )
    if is_production or not enabled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "source_rehearsal_not_enabled"},
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
                        "created_by": "source_rehearsal_automation",
                    }
                ),
            )


async def _source_rehearsal_status_payload(
    pool: Any,
    *,
    tenant_id: UUID,
    source: str,
    bearer_token: str | None = None,
    session_expires_at: str | None = None,
) -> dict[str, Any]:
    install = await _source_installation_row(pool, tenant_id=tenant_id, source=source)
    triggers = await pool.fetchrow(
        """
        SELECT count(*)::int AS total,
               count(*) FILTER (WHERE consumed_at IS NOT NULL)::int AS consumed
          FROM onboarding_triggers
         WHERE tenant_id = $1 AND source = $2
        """,
        tenant_id,
        source,
    )
    runs = await pool.fetch(
        """
        SELECT status, count(*)::int AS count
          FROM onboarding_runs
         WHERE tenant_id = $1 AND $2 = ANY(sources_enabled)
         GROUP BY status
         ORDER BY status
        """,
        tenant_id,
        source,
    )
    shards = await pool.fetch(
        """
        SELECT state, count(*)::int AS count,
               coalesce(sum(observations_seen), 0)::int AS observations_seen
          FROM onboarding_shards
         WHERE tenant_id = $1 AND source = $2
         GROUP BY state
         ORDER BY state
        """,
        tenant_id,
        source,
    )
    observation_rows = await pool.fetch(
        """
        SELECT id, kind, source_channel, occurred_at, content_text
          FROM observations
         WHERE tenant_id = $1 AND source_channel LIKE $2
         ORDER BY occurred_at DESC
         LIMIT 25
        """,
        tenant_id,
        f"{source}:%",
    )
    observation_count = await pool.fetchval(
        """
        SELECT count(*)::int
          FROM observations
         WHERE tenant_id = $1 AND source_channel LIKE $2
        """,
        tenant_id,
        f"{source}:%",
    )
    failures = await pool.fetchrow(
        """
        SELECT count(*)::int AS total
          FROM ingestion_failures
         WHERE tenant_id = $1 AND source = $2 AND resolved_at IS NULL
        """,
        tenant_id,
        source,
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
        "source": source,
        "installed": installed,
        "installation": (
            {
                "installation_id": install["installation_id"],
                "enabled": bool(install["enabled"]),
                "has_secret": bool(install["has_secret"]),
                "installed_at": install["installed_at"].isoformat(),
                "details": install.get("details", {}),
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
        "next_action": _source_rehearsal_next_action(
            source=source,
            installed=installed,
            trigger_count=trigger_total,
            observation_count=int(observation_count or 0),
        ),
    }


async def _source_installation_row(
    pool: Any,
    *,
    tenant_id: UUID,
    source: str,
) -> dict[str, Any] | None:
    if source in _OAUTH_REHEARSAL_SOURCES:
        row = await pool.fetchrow(
            """
            SELECT installation_id, enabled, (secret_ref IS NOT NULL) AS has_secret,
                   installed_at
              FROM provider_installations
             WHERE tenant_id = $1 AND provider = $2
             ORDER BY installed_at DESC
             LIMIT 1
            """,
            tenant_id,
            source,
        )
        return dict(row, details={}) if row else None

    if source == "jira":
        row = await pool.fetchrow(
            """
            SELECT base_url AS installation_id,
                   (disabled_at IS NULL) AS enabled,
                   (secret_ref IS NOT NULL) AS has_secret,
                   created_at AS installed_at,
                   account_email,
                   cloud_id,
                   (webhook_secret_ref IS NOT NULL) AS webhook_registered
              FROM jira_installations
             WHERE tenant_id = $1
             ORDER BY created_at DESC
             LIMIT 1
            """,
            tenant_id,
        )
        if row is None:
            return None
        data = dict(row)
        return {
            "installation_id": data["installation_id"],
            "enabled": data["enabled"],
            "has_secret": data["has_secret"],
            "installed_at": data["installed_at"],
            "details": {
                "account_email": data.get("account_email"),
                "cloud_id": data.get("cloud_id"),
                "webhook_registered": data.get("webhook_registered"),
            },
        }

    if source == "telegram":
        row = await pool.fetchrow(
            """
            SELECT account_label AS installation_id,
                   (disabled_at IS NULL) AS enabled,
                   (session_secret_ref IS NOT NULL) AS has_secret,
                   created_at AS installed_at,
                   api_id,
                   (backfill_session_secret_ref IS NOT NULL) AS has_backfill_session
              FROM telegram_installations
             WHERE tenant_id = $1
             ORDER BY created_at DESC
             LIMIT 1
            """,
            tenant_id,
        )
        if row is None:
            return None
        data = dict(row)
        return {
            "installation_id": data["installation_id"],
            "enabled": data["enabled"],
            "has_secret": data["has_secret"],
            "installed_at": data["installed_at"],
            "details": {
                "api_id": data.get("api_id"),
                "has_backfill_session": data.get("has_backfill_session"),
            },
        }

    return None


def _source_rehearsal_next_action(
    *,
    source: str,
    installed: bool,
    trigger_count: int,
    observation_count: int,
) -> str:
    if not installed:
        if source in _OAUTH_REHEARSAL_SOURCES:
            return f"Approve {source.title()} in the provider browser window."
        return f"Submit the required {source.title()} connection details."
    if trigger_count == 0:
        return f"{source.title()} installed; waiting for onboarding trigger."
    if observation_count == 0:
        return (
            f"{source.title()} installed; waiting for historical backfill "
            "or live signals."
        )
    return f"{source.title()} observations are landing in Fyralis."


__all__ = ["build_byoc_onboarding_router"]
