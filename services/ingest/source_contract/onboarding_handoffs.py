"""Contract-resolved BYOC provider handoffs.

The gateway invokes one callable selected by ``OnboardingDefinition``.  This
module contains shared declarative behavior plus the small provider-specific
OAuth preparations that previously lived in a source switch in the gateway.
Provider modules are imported lazily so importing the source catalog remains
dependency-light.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

from services.ingest.source_contract.models import SourceDefinition


def _env_or_secret_ref_configured(name: str) -> bool:
    return bool(
        os.environ.get(name, "").strip()
        or os.environ.get(f"{name}_SECRET_REF", "").strip()
    )


def aws_source_approval_url(
    region: str | None,
    *,
    default_url: str,
) -> str:
    """Return the regional CloudFormation handoff without trusting raw input."""

    clean_region = str(region or "").strip()[:200]
    if clean_region:
        return (
            f"https://{clean_region}.console.aws.amazon.com/cloudformation/home"
            f"?region={clean_region}#/stacks/create/template"
        )
    return default_url


async def generic_provider_handoff(
    *,
    source_definition: SourceDefinition,
    pool: Any,
    tenant_id: UUID,
    public_url: str,
    callback_path: str | None,
    request_payload: dict[str, Any],
    deployment_context: dict[str, str],
) -> dict[str, Any]:
    """Build the contract-only handoff for customer-owned provider refs."""

    del pool, tenant_id, request_payload, deployment_context
    onboarding = source_definition.onboarding
    return {
        "authorization_mode": (
            onboarding.generic_authorization_mode
            or "customer_local_provider_refs"
        ),
        "install_url": None,
        "oauth_redirect_url": (
            f"{public_url}{callback_path}" if callback_path else None
        ),
        "provider_console_url": onboarding.provider_console_url,
        "missing_configuration": [],
    }


async def aws_provider_handoff(
    *,
    source_definition: SourceDefinition,
    pool: Any,
    tenant_id: UUID,
    public_url: str,
    callback_path: str | None,
    request_payload: dict[str, Any],
    deployment_context: dict[str, str],
) -> dict[str, Any]:
    """Build the regional AWS console handoff declared by the AWS contract."""

    payload = await generic_provider_handoff(
        source_definition=source_definition,
        pool=pool,
        tenant_id=tenant_id,
        public_url=public_url,
        callback_path=callback_path,
        request_payload=request_payload,
        deployment_context=deployment_context,
    )
    payload["provider_console_url"] = aws_source_approval_url(
        deployment_context.get("aws_region"),
        default_url=source_definition.onboarding.provider_console_url,
    )
    return payload


async def slack_provider_handoff(
    *,
    source_definition: SourceDefinition,
    pool: Any,
    tenant_id: UUID,
    public_url: str,
    callback_path: str | None,
    request_payload: dict[str, Any],
    deployment_context: dict[str, str],
) -> dict[str, Any]:
    """Prepare the Slack OAuth URL and report deployment-owned config gaps."""

    del request_payload, deployment_context
    from services.ingest.integrations.slack import oauth as slack_oauth

    client_id = os.environ.get("SLACK_CLIENT_ID", "").strip()
    redirect_uri = os.environ.get("SLACK_REDIRECT_URI", "").strip()
    missing = [
        name
        for name, value in {
            "SLACK_CLIENT_ID": client_id,
            "SLACK_REDIRECT_URI": redirect_uri,
            "SLACK_CLIENT_SECRET": _env_or_secret_ref_configured(
                "SLACK_CLIENT_SECRET"
            ),
            "SLACK_SIGNING_SECRET": _env_or_secret_ref_configured(
                "SLACK_SIGNING_SECRET"
            ),
        }.items()
        if not value
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
        "oauth_redirect_url": redirect_uri
        or (f"{public_url}{callback_path}" if callback_path else None),
        "provider_console_url": source_definition.onboarding.provider_console_url,
        "missing_configuration": missing,
    }


async def discord_provider_handoff(
    *,
    source_definition: SourceDefinition,
    pool: Any,
    tenant_id: UUID,
    public_url: str,
    callback_path: str | None,
    request_payload: dict[str, Any],
    deployment_context: dict[str, str],
) -> dict[str, Any]:
    """Prepare Discord OAuth/Gateway authorization for the selected mode."""

    del deployment_context
    from services.ingest.integrations.discord import oauth as discord_oauth

    client_id = os.environ.get("DISCORD_CLIENT_ID", "").strip()
    redirect_uri = os.environ.get("DISCORD_REDIRECT_URI", "").strip()
    access_mode = discord_oauth.discord_access_mode(
        request_payload.get("access_mode")
        or request_payload.get("discord_access_mode")
    )
    missing = [
        name
        for name, value in {
            "DISCORD_CLIENT_ID": client_id,
            "DISCORD_REDIRECT_URI": redirect_uri,
            "DISCORD_CLIENT_SECRET": _env_or_secret_ref_configured(
                "DISCORD_CLIENT_SECRET"
            ),
            "DISCORD_APPLICATION_ID": os.environ.get(
                "DISCORD_APPLICATION_ID",
                "",
            ),
            "DISCORD_BOT_TOKEN": _env_or_secret_ref_configured(
                "DISCORD_BOT_TOKEN"
            ),
            "WEBHOOK_SECRET_DISCORD": _env_or_secret_ref_configured(
                "WEBHOOK_SECRET_DISCORD"
            ),
        }.items()
        if not value
    ]
    install_url = None
    if client_id and redirect_uri:
        state_token = await discord_oauth.issue_state_token(tenant_id, pool)
        install_url = discord_oauth.discord_authorize_url(
            client_id=client_id,
            redirect_uri=redirect_uri,
            state_token=state_token,
            access_mode=access_mode,
        )
    return {
        "authorization_mode": "oauth_plus_gateway",
        "install_url": install_url,
        "discord_access_mode": access_mode,
        "discord_permissions": (
            discord_oauth.discord_permissions_for_access_mode(access_mode)
        ),
        "oauth_redirect_url": redirect_uri
        or (f"{public_url}{callback_path}" if callback_path else None),
        "provider_console_url": source_definition.onboarding.provider_console_url,
        "missing_configuration": missing,
    }


async def figma_provider_handoff(
    *,
    source_definition: SourceDefinition,
    pool: Any,
    tenant_id: UUID,
    public_url: str,
    callback_path: str | None,
    request_payload: dict[str, Any],
    deployment_context: dict[str, str],
) -> dict[str, Any]:
    """Report the deployment-owned, file-scoped Figma OAuth handoff."""

    del pool, tenant_id, request_payload, deployment_context
    from services.ingest.integrations.figma import oauth as figma_oauth

    configured_redirect = ""
    try:
        configured_redirect = figma_oauth._figma_redirect_uri()  # noqa: SLF001
    except figma_oauth.FigmaOAuthError:
        pass
    ready = figma_oauth._deployment_oauth_ready()  # noqa: SLF001
    return {
        "authorization_mode": "oauth",
        "install_url": None,
        "oauth_redirect_url": configured_redirect
        or (f"{public_url}{callback_path}" if callback_path else None),
        "provider_console_url": source_definition.onboarding.provider_console_url,
        "missing_configuration": (
            [] if ready else ["deployment_figma_oauth_app"]
        ),
        "setup_owner": "deployment_admin",
        "deployment_model": "customer_owned_byoc_oauth_app",
    }


async def github_provider_handoff(
    *,
    source_definition: SourceDefinition,
    pool: Any,
    tenant_id: UUID,
    public_url: str,
    callback_path: str | None,
    request_payload: dict[str, Any],
    deployment_context: dict[str, str],
) -> dict[str, Any]:
    """Prepare the GitHub App installation URL and exact config diagnostics."""

    del request_payload, deployment_context
    from services.ingest.integrations.github import oauth as github_oauth

    app_slug = os.environ.get("GITHUB_APP_SLUG", "").strip()
    private_key_sources = [
        name
        for name in (
            "GITHUB_APP_PRIVATE_KEY_SECRET_REF",
            "GITHUB_APP_PRIVATE_KEY",
            "GITHUB_APP_PRIVATE_KEY_PATH",
        )
        if os.environ.get(name, "").strip()
    ]
    missing = [
        name
        for name, configured in {
            "GITHUB_APP_SLUG": bool(app_slug),
            "GITHUB_APP_ID": bool(
                os.environ.get("GITHUB_APP_ID", "").strip()
            ),
            "WEBHOOK_SECRET_GITHUB": _env_or_secret_ref_configured(
                "WEBHOOK_SECRET_GITHUB"
            ),
        }.items()
        if not configured
    ]
    if not private_key_sources:
        missing.append("GITHUB_APP_PRIVATE_KEY_SOURCE")
    elif len(private_key_sources) > 1:
        missing.append("GITHUB_APP_PRIVATE_KEY_SOURCE_CONFLICT")
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
        "oauth_redirect_url": (
            f"{public_url}{callback_path}" if callback_path else None
        ),
        "provider_console_url": source_definition.onboarding.provider_console_url,
        "missing_configuration": missing,
    }


async def notion_provider_handoff(
    *,
    source_definition: SourceDefinition,
    pool: Any,
    tenant_id: UUID,
    public_url: str,
    callback_path: str | None,
    request_payload: dict[str, Any],
    deployment_context: dict[str, str],
) -> dict[str, Any]:
    """Prepare the Notion OAuth URL and deployment configuration report."""

    del request_payload, deployment_context
    from services.ingest.integrations.notion import oauth as notion_oauth

    client_id = os.environ.get("NOTION_CLIENT_ID", "").strip()
    redirect_uri = os.environ.get("NOTION_REDIRECT_URI", "").strip()
    missing = [
        name
        for name, value in {
            "NOTION_CLIENT_ID": client_id,
            "NOTION_REDIRECT_URI": redirect_uri,
            "NOTION_CLIENT_SECRET": _env_or_secret_ref_configured(
                "NOTION_CLIENT_SECRET"
            ),
        }.items()
        if not value
    ]
    install_url = None
    if client_id and redirect_uri:
        state_token = await notion_oauth.issue_state_token(
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
        "oauth_redirect_url": redirect_uri
        or (f"{public_url}{callback_path}" if callback_path else None),
        "provider_console_url": source_definition.onboarding.provider_console_url,
        "missing_configuration": missing,
    }


__all__ = [
    "aws_provider_handoff",
    "aws_source_approval_url",
    "discord_provider_handoff",
    "figma_provider_handoff",
    "generic_provider_handoff",
    "github_provider_handoff",
    "notion_provider_handoff",
    "slack_provider_handoff",
]
