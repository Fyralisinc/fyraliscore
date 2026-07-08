"""Provider setup bundles for customer-cloud source browser agents.

The bundle is safe to move through hosted control planes: it contains only
URLs, scope names, non-secret provider metadata, and generated templates. Raw
provider credentials stay in the customer's browser and secret manager.
"""
from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse


SLACK_BOT_SCOPES = (
    "channels:read",
    "channels:history",
    "groups:read",
    "groups:history",
    "users:read",
    "team:read",
)
SLACK_USER_SCOPES = (
    "im:read",
    "im:history",
    "mpim:read",
    "mpim:history",
)
SLACK_BOT_EVENTS = ("message.channels", "message.groups")

GOOGLE_DWD_SOURCES = {"gmail", "google_calendar", "google_drive"}
API_TOKEN_SOURCES = {
    "ashby",
    "brex",
    "deel",
    "figma",
    "fireflies",
    "grafana",
    "hibob",
    "mercury",
    "miro",
    "ramp",
}
OAUTH_SOURCES = {"carta", "gusto", "linkedin", "quickbooks"}
LOCAL_SESSION_SOURCES = {"signal", "telegram"}

GOOGLE_DWD_SCOPES = {
    "gmail": [
        "https://www.googleapis.com/auth/gmail.metadata",
        "https://www.googleapis.com/auth/admin.directory.user.readonly",
        "https://www.googleapis.com/auth/admin.directory.group.readonly",
    ],
    "google_calendar": [
        "https://www.googleapis.com/auth/calendar.readonly",
        "https://www.googleapis.com/auth/admin.directory.user.readonly",
        "https://www.googleapis.com/auth/admin.directory.group.readonly",
    ],
    "google_drive": [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/admin.directory.user.readonly",
        "https://www.googleapis.com/auth/admin.directory.group.readonly",
    ],
}


def build_source_provider_setup_bundle(
    *,
    source: str,
    recipe: dict[str, Any],
    provider_console_url: str | None = None,
    oauth_redirect_url: str | None = None,
    events_request_url: str | None = None,
    install_url: str | None = None,
    native_connect: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a provider-specific setup bundle for a browser-agent run."""
    normalized = _normalize_source(source)
    if normalized == "slack":
        return _slack_setup_bundle(
            provider_console_url=provider_console_url or "https://api.slack.com/apps",
            oauth_redirect_url=oauth_redirect_url,
            events_request_url=events_request_url,
            native_connect=native_connect,
        )
    if normalized == "github":
        return _github_setup_bundle(
            recipe=recipe,
            provider_console_url=provider_console_url,
            oauth_redirect_url=oauth_redirect_url,
            events_request_url=events_request_url,
            native_connect=native_connect,
        )
    if normalized == "discord":
        return _discord_setup_bundle(
            recipe=recipe,
            provider_console_url=provider_console_url,
            oauth_redirect_url=oauth_redirect_url,
            events_request_url=events_request_url,
            native_connect=native_connect,
        )
    if normalized == "notion":
        return _notion_setup_bundle(
            recipe=recipe,
            provider_console_url=provider_console_url,
            oauth_redirect_url=oauth_redirect_url,
            events_request_url=events_request_url,
            native_connect=native_connect,
        )
    if normalized == "jira":
        return _jira_setup_bundle(
            recipe=recipe,
            provider_console_url=provider_console_url,
            events_request_url=events_request_url,
            native_connect=native_connect,
        )
    if normalized in GOOGLE_DWD_SOURCES:
        return _google_dwd_setup_bundle(
            source=normalized,
            recipe=recipe,
            provider_console_url=provider_console_url,
            events_request_url=events_request_url,
            native_connect=native_connect,
        )
    if normalized == "aws":
        return _aws_setup_bundle(
            recipe=recipe,
            provider_console_url=provider_console_url,
            native_connect=native_connect,
        )
    if normalized in LOCAL_SESSION_SOURCES:
        return _local_session_setup_bundle(
            source=normalized,
            recipe=recipe,
            provider_console_url=provider_console_url,
            native_connect=native_connect,
        )
    if normalized == "whatsapp":
        return _whatsapp_setup_bundle(
            recipe=recipe,
            provider_console_url=provider_console_url,
            events_request_url=events_request_url,
            native_connect=native_connect,
        )
    if normalized in API_TOKEN_SOURCES:
        return _api_token_setup_bundle(
            source=normalized,
            recipe=recipe,
            provider_console_url=provider_console_url,
            events_request_url=events_request_url,
            native_connect=native_connect,
        )
    if normalized in OAUTH_SOURCES:
        return _oauth_setup_bundle(
            source=normalized,
            recipe=recipe,
            provider_console_url=provider_console_url,
            oauth_redirect_url=oauth_redirect_url,
            events_request_url=events_request_url,
            install_url=install_url,
            native_connect=native_connect,
        )
    return _recipe_setup_bundle(
        source=normalized,
        recipe=recipe,
        provider_console_url=provider_console_url,
        oauth_redirect_url=oauth_redirect_url,
        events_request_url=events_request_url,
        install_url=install_url,
        native_connect=native_connect,
    )


def provider_setup_bundle_actions(
    bundle: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not isinstance(bundle, dict):
        return []
    actions = bundle.get("agent_actions")
    if not isinstance(actions, list):
        return []
    normalized: list[dict[str, Any]] = []
    for action in actions:
        if not isinstance(action, dict):
            continue
        action_id = str(action.get("id") or "").strip()
        if not action_id:
            continue
        normalized.append(
            {
                "id": action_id,
                "owner": "fyralis_agent",
                "status": str(action.get("status") or "ready"),
                "label": str(action.get("label") or action_id.replace("_", " ")),
                "kind": str(action.get("kind") or "provider_setup"),
            }
        )
    return normalized


def _slack_setup_bundle(
    *,
    provider_console_url: str,
    oauth_redirect_url: str | None,
    events_request_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    redirect = oauth_redirect_url or "https://fyralis-ingress.customer.example/integrations/slack/callback"
    event_url = events_request_url or "https://fyralis-ingress.customer.example/webhooks/slack/events"
    base_manifest, events_manifest = slack_manifest_text(
        oauth_redirect_url=redirect,
        events_request_url=event_url,
    )
    return {
        "schema_version": "fyralis.byoc.source.provider_setup_bundle.v1",
        "source": "slack",
        "kind": "slack_app_manifest",
        "provider_console_url": provider_console_url,
        "oauth_redirect_url": redirect,
        "events_request_url": event_url,
        "settings_targets": [
            "Slack app manifest",
            "OAuth scopes",
            "event subscriptions",
        ],
        "collected_non_secret_fields": [
            "workspace id",
            "team domain",
            "approved channel ids",
        ],
        "generated_refs": [
            "oauth client ref",
            "bot token ref",
            "signing secret ref",
            "OAuth state HMAC key ref",
        ],
        "browser_tasks": [
            {
                "id": "open_provider_settings",
                "target": provider_console_url,
                "agent_role": "open Slack app settings in customer BYOC browser",
            },
            {
                "id": "collect_non_secret_configuration",
                "fields": [
                    "workspace id",
                    "team domain",
                    "approved channel ids",
                ],
                "agent_role": "read Slack workspace IDs and approved channel scope only",
            },
            {
                "id": "generate_customer_cloud_refs",
                "refs": [
                    "oauth client ref",
                    "bot token ref",
                    "signing secret ref",
                    "OAuth state HMAC key ref",
                ],
                "agent_role": "write Slack manifest and verifier refs locally",
            },
        ],
        "browser_dom_plan": _browser_dom_plan(
            source="slack",
            kind="slack_app_manifest",
            provider_console_url=provider_console_url,
            oauth_redirect_url=redirect,
            events_request_url=event_url,
            settings_targets=[
                "Slack app manifest",
                "OAuth scopes",
                "event subscriptions",
            ],
            collected_non_secret_fields=[
                "workspace id",
                "team domain",
                "approved channel ids",
            ],
            generated_refs=[
                "oauth client ref",
                "bot token ref",
                "signing secret ref",
                "OAuth state HMAC key ref",
            ],
            primary_artifacts=[
                "fyralis-slack-app-manifest.yaml",
                "fyralis-slack-app-events-manifest.yaml",
            ],
        ),
        "bot_scopes": list(SLACK_BOT_SCOPES),
        "user_scopes": list(SLACK_USER_SCOPES),
        "bot_events": list(SLACK_BOT_EVENTS),
        "native_connect": native_connect,
        "artifacts": [
            {
                "name": "slack_app_manifest",
                "filename": "fyralis-slack-app-manifest.yaml",
                "media_type": "application/yaml",
                "content": base_manifest,
            },
            {
                "name": "slack_events_manifest",
                "filename": "fyralis-slack-app-events-manifest.yaml",
                "media_type": "application/yaml",
                "content": events_manifest,
            },
            {
                "name": "slack_setup_summary",
                "filename": "fyralis-slack-provider-setup.json",
                "media_type": "application/json",
                "json": {
                    "source": "slack",
                    "oauth_redirect_url": redirect,
                    "events_request_url": event_url,
                    "bot_scopes": list(SLACK_BOT_SCOPES),
                    "user_scopes": list(SLACK_USER_SCOPES),
                    "bot_events": list(SLACK_BOT_EVENTS),
                    "native_connect": native_connect,
                    "raw_secret_values_included": False,
                },
            },
        ],
        "agent_actions": [
            {
                "id": "generate_slack_app_manifest",
                "kind": "materialize_provider_setup_bundle",
                "label": "Generate Slack app manifest and event subscription bundle.",
            },
            {
                "id": "prepare_slack_oauth_redirects",
                "kind": "provider_setup",
                "label": "Prepare Slack OAuth redirect URL.",
            },
            {
                "id": "prepare_slack_event_subscriptions",
                "kind": "provider_setup",
                "label": "Prepare Slack event subscription request URL.",
            },
            {
                "id": "prepare_slack_browser_dom_plan",
                "kind": "materialize_browser_dom_plan",
                "label": "Prepare Slack browser DOM action plan.",
            },
            {
                "id": "execute_slack_browser_dom_plan",
                "kind": "execute_browser_dom_plan",
                "label": "Run the admin-present Slack browser agent.",
            },
        ],
        "human_gates": [
            "Slack admin signs in and completes MFA when prompted",
            "Slack admin imports or approves the app manifest",
            "Slack admin approves workspace OAuth scopes",
        ],
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_provider_setup_bundle_only",
    }


def _github_setup_bundle(
    *,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    oauth_redirect_url: str | None,
    events_request_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    redirect = oauth_redirect_url or "https://fyralis-ingress.customer.example/integrations/github/callback"
    webhook = events_request_url or "https://fyralis-ingress.customer.example/webhooks/github"
    manifest = {
        "name": "Fyralis BYOC",
        "url": _origin_from_url(redirect, "https://fyralis-ingress.customer.example"),
        "hook_attributes": {"url": webhook},
        "redirect_url": redirect,
        "callback_urls": [redirect],
        "public": False,
        "default_permissions": {
            "contents": "read",
            "issues": "read",
            "metadata": "read",
            "pull_requests": "read",
        },
        "default_events": [
            "issues",
            "pull_request",
            "pull_request_review",
            "push",
        ],
    }
    setup = {
        "manifest": manifest,
        "app_private_key_ref": "customer-cloud secret ref generated after GitHub App creation",
        "webhook_secret_ref": "customer-cloud verifier ref generated by Fyralis",
        "repository_scope_contract": "selected repositories or approved organization scope",
        "native_connect": native_connect,
    }
    return _provider_bundle(
        source="github",
        kind="github_app_manifest",
        recipe=recipe,
        provider_console_url=provider_console_url or "https://github.com/settings/apps",
        oauth_redirect_url=redirect,
        events_request_url=webhook,
        setup_payload=setup,
        primary_filename="fyralis-github-app-manifest.json",
        primary_artifact_name="github_app_manifest",
        primary_payload=manifest,
        action_label="Generate GitHub App manifest and webhook contract.",
    )


def _discord_setup_bundle(
    *,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    oauth_redirect_url: str | None,
    events_request_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    redirect = oauth_redirect_url or "https://fyralis-ingress.customer.example/integrations/discord/callback"
    webhook = events_request_url or "https://fyralis-ingress.customer.example/webhooks/discord"
    setup = {
        "application": {
            "redirect_uris": [redirect],
            "bot_permissions": [
                "read message history",
                "view channels",
                "use slash commands",
            ],
            "gateway_intents": [
                "guilds",
                "guild messages",
                "message content when approved",
            ],
        },
        "webhook_url": webhook,
        "generated_refs": [
            "bot token ref",
            "webhook verifier ref",
            "gateway session contract",
        ],
        "native_connect": native_connect,
    }
    return _provider_bundle(
        source="discord",
        kind="discord_application_setup",
        recipe=recipe,
        provider_console_url=provider_console_url
        or "https://discord.com/developers/applications",
        oauth_redirect_url=redirect,
        events_request_url=webhook,
        setup_payload=setup,
        primary_filename="fyralis-discord-app-setup.json",
        primary_artifact_name="discord_application_setup",
        primary_payload=setup,
        action_label="Generate Discord application and gateway setup contract.",
    )


def _notion_setup_bundle(
    *,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    oauth_redirect_url: str | None,
    events_request_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    redirect = oauth_redirect_url or "https://fyralis-ingress.customer.example/integrations/notion/callback"
    webhook = events_request_url or "https://fyralis-ingress.customer.example/webhooks/notion/events"
    setup = {
        "integration": {
            "redirect_uris": [redirect],
            "capabilities": [
                "read content",
                "read user information including email addresses",
                "read comments when enabled",
            ],
            "workspace_share_scope": "approved pages and databases only",
        },
        "webhook_url": webhook,
        "generated_refs": [
            "integration token ref",
            "webhook verification token ref when Notion supplies one",
        ],
        "native_connect": native_connect,
    }
    return _provider_bundle(
        source="notion",
        kind="notion_integration_setup",
        recipe=recipe,
        provider_console_url=provider_console_url or "https://www.notion.so/my-integrations",
        oauth_redirect_url=redirect,
        events_request_url=webhook,
        setup_payload=setup,
        primary_filename="fyralis-notion-app-setup.json",
        primary_artifact_name="notion_integration_setup",
        primary_payload=setup,
        action_label="Generate Notion integration and workspace sharing contract.",
    )


def _jira_setup_bundle(
    *,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    events_request_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    webhook = events_request_url or "https://fyralis-ingress.customer.example/webhooks/jira/events"
    connect_payload = {
        "base_url": "${JIRA_BASE_URL}",
        "account_email": "${JIRA_ACCOUNT_EMAIL}",
        "api_token_ref": "customer-cloud secret ref",
        "project_keys": "${JIRA_PROJECT_KEYS comma-separated or blank for all approved projects}",
        "webhook_secret_ref": "customer-cloud verifier ref generated by Fyralis",
        "webhook_url": webhook,
        "native_connect": native_connect,
    }
    return _provider_bundle(
        source="jira",
        kind="jira_api_token_webhook_setup",
        recipe=recipe,
        provider_console_url=provider_console_url or "https://admin.atlassian.com/",
        events_request_url=webhook,
        setup_payload=connect_payload,
        primary_filename="jira-connect-payload.example.json",
        primary_artifact_name="jira_connect_payload",
        primary_payload=connect_payload,
        action_label="Generate Jira token preflight and webhook setup payload.",
    )


def _google_dwd_setup_bundle(
    *,
    source: str,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    events_request_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    scopes = GOOGLE_DWD_SCOPES[source]
    setup = {
        "workspace_admin_url": provider_console_url
        or "https://admin.google.com/ac/owl/domainwidedelegation",
        "domain_wide_delegation": {
            "client_id_ref": "Fyralis service account OAuth client ID ref",
            "scopes": scopes,
            "inclusion_spec": {
                "mode": "approved_users_groups_org_units_or_resources",
                "source": source,
            },
        },
        "native_connect": native_connect,
        "events_request_url": events_request_url,
        "raw_secret_values_included": False,
    }
    filename_source = source.replace("_", "-")
    return _provider_bundle(
        source=source,
        kind="google_workspace_dwd_setup",
        recipe=recipe,
        provider_console_url=provider_console_url
        or "https://admin.google.com/ac/owl/domainwidedelegation",
        events_request_url=events_request_url,
        setup_payload=setup,
        primary_filename=f"fyralis-{filename_source}-dwd-preflight.json",
        primary_artifact_name="google_workspace_dwd_preflight",
        primary_payload=setup,
        action_label="Generate Google Workspace DWD preflight and finalize payload.",
    )


def _aws_setup_bundle(
    *,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    setup = {
        "iam_role": {
            "role_name": "fyralis-source-readonly",
            "external_id_ref": "customer-cloud generated external ID",
            "trust_boundary": "customer-approved Fyralis BYOC source agent only",
        },
        "readonly_policy_outline": [
            "List/read inventory metadata",
            "Read CloudTrail/EventBridge metadata when approved",
            "No write/delete permissions",
        ],
        "generated_refs": [
            "role ARN ref",
            "external ID ref",
            "account and region scope contract",
        ],
        "native_connect": native_connect,
    }
    return _provider_bundle(
        source="aws",
        kind="aws_iam_role_setup",
        recipe=recipe,
        provider_console_url=provider_console_url or "https://console.aws.amazon.com/iam/",
        setup_payload=setup,
        primary_filename="fyralis-aws-iam-role-setup.json",
        primary_artifact_name="aws_iam_role_setup",
        primary_payload=setup,
        action_label="Generate AWS read-only role trust and policy setup contract.",
    )


def _local_session_setup_bundle(
    *,
    source: str,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    if source == "telegram":
        setup = {
            "provider_app_url": provider_console_url or "https://my.telegram.org/apps",
            "session_type": "MTProto StringSession",
            "generated_refs": [
                "api id ref",
                "api hash ref",
                "live session ref",
                "optional backfill session ref",
            ],
            "scope_contract": "approved dialogs, groups, and channels only",
            "native_connect": native_connect,
        }
    else:
        setup = {
            "provider_app_url": provider_console_url or "Customer-cloud linked-device setup",
            "session_type": "linked device session",
            "generated_refs": [
                "linked-device session ref",
                "approved contacts contract",
                "approved groups contract",
            ],
            "scope_contract": "approved Signal contacts and groups only",
            "native_connect": native_connect,
        }
    filename_source = source.replace("_", "-")
    return _provider_bundle(
        source=source,
        kind="local_gateway_session_setup",
        recipe=recipe,
        provider_console_url=provider_console_url,
        setup_payload=setup,
        primary_filename=f"fyralis-{filename_source}-session-plan.json",
        primary_artifact_name="local_gateway_session_plan",
        primary_payload=setup,
        action_label=f"Generate {source.replace('_', ' ').title()} local gateway session plan.",
    )


def _whatsapp_setup_bundle(
    *,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    events_request_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    webhook = events_request_url or "https://fyralis-ingress.customer.example/integrations/whatsapp/webhook"
    setup = {
        "meta_app": {
            "webhook_callback_url": webhook,
            "verify_token_ref": "customer-cloud verifier ref generated by Fyralis",
            "app_secret_ref": "customer-cloud app secret ref",
        },
        "business_scope": {
            "business_account_id": "${WHATSAPP_BUSINESS_ACCOUNT_ID}",
            "phone_number_ids": "${APPROVED_PHONE_NUMBER_IDS}",
        },
        "native_connect": native_connect,
    }
    return _provider_bundle(
        source="whatsapp",
        kind="whatsapp_webhook_setup",
        recipe=recipe,
        provider_console_url=provider_console_url or "https://developers.facebook.com/apps/",
        events_request_url=webhook,
        setup_payload=setup,
        primary_filename="fyralis-whatsapp-webhook-setup.json",
        primary_artifact_name="whatsapp_webhook_setup",
        primary_payload=setup,
        action_label="Generate WhatsApp webhook verification and business scope contract.",
    )


def _api_token_setup_bundle(
    *,
    source: str,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    events_request_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    setup = {
        "provider_console_url": provider_console_url,
        "api_token_ref": "customer-cloud secret ref created after admin approval",
        "webhook_url": events_request_url,
        "webhook_verifier_ref": (
            "customer-cloud verifier ref generated by Fyralis"
            if events_request_url
            else None
        ),
        "approved_scope_contract": list(recipe.get("settings_targets") or []),
        "non_secret_discovery_fields": list(recipe.get("agent_collects") or []),
        "native_connect": native_connect,
    }
    filename_source = source.replace("_", "-")
    return _provider_bundle(
        source=source,
        kind="api_token_provider_setup",
        recipe=recipe,
        provider_console_url=provider_console_url,
        events_request_url=events_request_url,
        setup_payload=setup,
        primary_filename=f"fyralis-{filename_source}-api-token-setup.json",
        primary_artifact_name="api_token_provider_setup",
        primary_payload=setup,
        action_label=f"Generate {source.replace('_', ' ').title()} token and webhook setup contract.",
    )


def _oauth_setup_bundle(
    *,
    source: str,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    oauth_redirect_url: str | None,
    events_request_url: str | None,
    install_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    setup = {
        "provider_console_url": provider_console_url,
        "install_url": install_url,
        "oauth_redirect_url": oauth_redirect_url,
        "webhook_url": events_request_url,
        "oauth_client_ref": "customer-cloud OAuth client ref",
        "token_ref": "customer-cloud token/refresh ref after admin consent",
        "approved_scope_contract": list(recipe.get("settings_targets") or []),
        "non_secret_discovery_fields": list(recipe.get("agent_collects") or []),
        "native_connect": native_connect,
    }
    filename_source = source.replace("_", "-")
    return _provider_bundle(
        source=source,
        kind="oauth_provider_setup",
        recipe=recipe,
        provider_console_url=provider_console_url,
        oauth_redirect_url=oauth_redirect_url,
        events_request_url=events_request_url,
        install_url=install_url,
        setup_payload=setup,
        primary_filename=f"fyralis-{filename_source}-oauth-setup.json",
        primary_artifact_name="oauth_provider_setup",
        primary_payload=setup,
        action_label=f"Generate {source.replace('_', ' ').title()} OAuth setup contract.",
    )


def slack_manifest_text(
    *,
    oauth_redirect_url: str,
    events_request_url: str | None = None,
) -> tuple[str, str]:
    bot_scopes = "\n".join(f"      - {scope}" for scope in SLACK_BOT_SCOPES)
    user_scopes = "\n".join(f"      - {scope}" for scope in SLACK_USER_SCOPES)
    bot_events = "\n".join(f"      - {event}" for event in SLACK_BOT_EVENTS)
    base_manifest = f"""display_information:
  name: Fyralis BYOC
  description: Slack ingestion app for Fyralis BYOC.
  background_color: "#0b1020"
features:
  bot_user:
    display_name: Fyralis
    always_online: false
oauth_config:
  redirect_urls:
    - {oauth_redirect_url}
  scopes:
    bot:
{bot_scopes}
    user:
{user_scopes}
settings:
  org_deploy_enabled: false
  socket_mode_enabled: false
  token_rotation_enabled: false
"""
    if not events_request_url:
        return base_manifest, base_manifest
    events_manifest = base_manifest.rstrip() + f"""
  event_subscriptions:
    request_url: {events_request_url}
    bot_events:
{bot_events}
"""
    return base_manifest, events_manifest


def _provider_bundle(
    *,
    source: str,
    kind: str,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    setup_payload: dict[str, Any],
    primary_filename: str,
    primary_artifact_name: str,
    primary_payload: dict[str, Any],
    action_label: str,
    oauth_redirect_url: str | None = None,
    events_request_url: str | None = None,
    install_url: str | None = None,
) -> dict[str, Any]:
    settings_targets = list(recipe.get("settings_targets") or [])
    collected_non_secret_fields = list(recipe.get("agent_collects") or [])
    generated_refs = list(recipe.get("agent_generates") or [])
    artifacts = [
        _json_artifact(
            primary_artifact_name,
            primary_filename,
            primary_payload,
        )
    ]
    summary_filename = f"fyralis-{source.replace('_', '-')}-provider-setup.json"
    if summary_filename != primary_filename:
        artifacts.append(
            _json_artifact(
                "provider_setup_summary",
                summary_filename,
                setup_payload
                | {
                    "source": source,
                    "kind": kind,
                    "raw_secret_values_included": False,
                },
            )
        )
    browser_dom_plan = _browser_dom_plan(
        source=source,
        kind=kind,
        provider_console_url=provider_console_url,
        oauth_redirect_url=oauth_redirect_url,
        events_request_url=events_request_url,
        settings_targets=settings_targets,
        collected_non_secret_fields=collected_non_secret_fields,
        generated_refs=generated_refs,
        primary_artifacts=[artifact["filename"] for artifact in artifacts],
    )
    return {
        "schema_version": "fyralis.byoc.source.provider_setup_bundle.v1",
        "source": source,
        "kind": kind,
        "provider_console_url": provider_console_url,
        "install_url": install_url,
        "oauth_redirect_url": oauth_redirect_url,
        "events_request_url": events_request_url,
        "settings_targets": settings_targets,
        "collected_non_secret_fields": collected_non_secret_fields,
        "generated_refs": generated_refs,
        "browser_tasks": [
            {
                "id": "open_provider_settings",
                "target": provider_console_url,
                "agent_role": "open provider settings in customer BYOC browser",
            },
            {
                "id": "collect_non_secret_configuration",
                "fields": list(recipe.get("agent_collects") or []),
                "agent_role": "read IDs, scopes, object names, and URLs only",
            },
            {
                "id": "generate_customer_cloud_refs",
                "refs": list(recipe.get("agent_generates") or []),
                "agent_role": "write generated contracts and verifier refs locally",
            },
        ],
        "browser_dom_plan": browser_dom_plan,
        "artifacts": artifacts,
        "agent_actions": [
            {
                "id": f"generate_{source}_provider_setup_bundle",
                "kind": "materialize_provider_setup_bundle",
                "label": action_label,
            },
            {
                "id": f"prepare_{source}_provider_handoff",
                "kind": "provider_setup",
                "label": "Prepare provider settings handoff and non-secret collection plan.",
            },
            {
                "id": f"prepare_{source}_browser_dom_plan",
                "kind": "materialize_browser_dom_plan",
                "label": "Prepare provider browser DOM action plan.",
            },
            {
                "id": f"execute_{source}_browser_dom_plan",
                "kind": "execute_browser_dom_plan",
                "label": "Run the admin-present provider browser agent.",
            },
        ],
        "human_gates": list(recipe.get("human_gates") or []),
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_provider_setup_bundle_only",
    }


def _json_artifact(
    name: str,
    filename: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "name": name,
        "filename": filename,
        "media_type": "application/json",
        "json": payload | {"raw_secret_values_included": False},
    }


def _browser_dom_plan(
    *,
    source: str,
    kind: str,
    provider_console_url: str | None,
    oauth_redirect_url: str | None = None,
    events_request_url: str | None = None,
    settings_targets: list[str],
    collected_non_secret_fields: list[str],
    generated_refs: list[str],
    primary_artifacts: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.source.browser_dom_plan.v1",
        "source": source,
        "kind": kind,
        "runtime": "customer_cloud_admin_present_browser_dom_v1",
        "provider_console_url": provider_console_url,
        "oauth_redirect_url": oauth_redirect_url,
        "events_request_url": events_request_url,
        "settings_targets": settings_targets,
        "primary_artifacts": primary_artifacts,
        "steps": _browser_dom_steps(
            source=source,
            kind=kind,
            provider_console_url=provider_console_url,
            oauth_redirect_url=oauth_redirect_url,
            events_request_url=events_request_url,
            collected_non_secret_fields=collected_non_secret_fields,
            generated_refs=generated_refs,
            primary_artifacts=primary_artifacts,
        ),
        "completion_evidence": [
            "provider settings page accepted generated callback/webhook/scope configuration",
            "customer-cloud secret refs exist for generated provider material",
            "Fyralis source install status can be polled without raw provider secrets",
            "sanitized connection-proof observation is readable from customer-cloud gateway",
        ],
        "human_pause_policy": {
            "pause_for": [
                "provider sign-in",
                "MFA challenge",
                "credential creation or reveal",
                "scope consent",
                "final provider admin approval",
            ],
            "agent_may_complete": [
                "open provider settings",
                "paste generated manifests or URLs",
                "select least-privilege scopes from the generated contract",
                "collect non-secret IDs and resource names",
                "write sanitized local setup artifacts",
            ],
        },
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_browser_dom_plan_only",
    }


def _browser_dom_steps(
    *,
    source: str,
    kind: str,
    provider_console_url: str | None,
    oauth_redirect_url: str | None,
    events_request_url: str | None,
    collected_non_secret_fields: list[str],
    generated_refs: list[str],
    primary_artifacts: list[str],
) -> list[dict[str, Any]]:
    steps = [
        _dom_step(
            "goto_provider_console",
            "goto",
            target_url=provider_console_url,
            selectors=_selectors("a", "button", "input"),
            text_targets=_text_targets("Sign in", "Log in", "Admin", "Settings"),
        ),
        _dom_step(
            "wait_for_admin_session",
            "human_pause",
            text_targets=_text_targets("Sign in", "MFA", "Verify", "Approve"),
            human_reason="Provider authentication and MFA must be completed by the customer admin.",
        ),
    ]
    if kind == "slack_app_manifest":
        steps.extend(_slack_dom_steps(primary_artifacts, oauth_redirect_url, events_request_url))
    elif kind == "github_app_manifest":
        steps.extend(_github_dom_steps(primary_artifacts, oauth_redirect_url, events_request_url))
    elif kind == "google_workspace_dwd_setup":
        steps.extend(_google_dwd_dom_steps(source, primary_artifacts))
    elif kind == "aws_iam_role_setup":
        steps.extend(_aws_dom_steps(primary_artifacts))
    elif kind == "jira_api_token_webhook_setup":
        steps.extend(_jira_dom_steps(primary_artifacts, events_request_url))
    elif kind == "whatsapp_webhook_setup":
        steps.extend(_whatsapp_dom_steps(primary_artifacts, events_request_url))
    elif kind == "discord_application_setup":
        steps.extend(_oauth_app_dom_steps("discord", primary_artifacts, oauth_redirect_url, events_request_url))
    elif kind == "notion_integration_setup":
        steps.extend(_oauth_app_dom_steps("notion", primary_artifacts, oauth_redirect_url, events_request_url))
    elif kind == "local_gateway_session_setup":
        steps.extend(_local_session_dom_steps(source, primary_artifacts))
    elif kind == "api_token_provider_setup":
        steps.extend(_api_token_dom_steps(source, primary_artifacts, events_request_url))
    elif kind == "oauth_provider_setup":
        steps.extend(_oauth_app_dom_steps(source, primary_artifacts, oauth_redirect_url, events_request_url))
    else:
        steps.append(
            _dom_step(
                "apply_generated_provider_setup",
                "apply_generated_artifact",
                artifacts=primary_artifacts,
                selectors=_selectors("textarea", "input", "button[type=submit]"),
                text_targets=_text_targets("Create", "Save", "Update", "Install"),
            )
        )
    steps.extend(
        [
            _dom_step(
                "collect_non_secret_configuration",
                "collect_text",
                fields=collected_non_secret_fields,
                selectors=_selectors("[data-testid]", "code", "input[readonly]", "dd"),
                text_targets=_text_targets("ID", "Workspace", "Organization", "Team", "Scope"),
            ),
            _dom_step(
                "prepare_customer_cloud_refs",
                "generate_refs",
                refs=generated_refs,
                selectors=_selectors("input", "textarea", "code"),
                text_targets=_text_targets("Client ID", "Secret", "Token", "Signing secret", "Webhook"),
                human_reason="Raw provider secret values stay in the customer-cloud secret manager.",
            ),
            _dom_step(
                "pause_for_final_provider_approval",
                "human_pause",
                text_targets=_text_targets("Approve", "Authorize", "Install", "Allow", "Create"),
                human_reason="Final provider approval belongs to the customer's accountable admin.",
            ),
            _dom_step(
                "verify_provider_configuration",
                "verify",
                selectors=_selectors("[role=alert]", ".success", ".notice", "text=Installed"),
                text_targets=_text_targets("Installed", "Saved", "Enabled", "Verified", "Active"),
            ),
        ]
    )
    return steps


def _slack_dom_steps(
    primary_artifacts: list[str],
    oauth_redirect_url: str | None,
    events_request_url: str | None,
) -> list[dict[str, Any]]:
    return [
        _dom_step(
            "create_slack_app_from_manifest",
            "paste_or_upload_manifest",
            artifacts=primary_artifacts,
            selectors=_selectors("textarea[name=manifest]", "input[type=file]", "button"),
            text_targets=_text_targets("Create New App", "From an app manifest", "Next", "Create"),
        ),
        _dom_step(
            "configure_slack_oauth_redirect",
            "set_url",
            value=oauth_redirect_url,
            selectors=_selectors("input[name=redirect_url]", "input[type=url]", "textarea"),
            text_targets=_text_targets("Redirect URLs", "OAuth", "Save URLs"),
        ),
        _dom_step(
            "configure_slack_events_request_url",
            "set_url",
            value=events_request_url,
            selectors=_selectors("input[name=request_url]", "input[type=url]", "textarea"),
            text_targets=_text_targets("Event Subscriptions", "Request URL", "Subscribe to bot events"),
        ),
    ]


def _github_dom_steps(
    primary_artifacts: list[str],
    oauth_redirect_url: str | None,
    events_request_url: str | None,
) -> list[dict[str, Any]]:
    return [
        _dom_step(
            "create_github_app_from_manifest",
            "paste_or_upload_manifest",
            artifacts=primary_artifacts,
            selectors=_selectors("textarea", "input[name=name]", "button[type=submit]"),
            text_targets=_text_targets("New GitHub App", "Manifest", "Register GitHub App"),
        ),
        _dom_step(
            "configure_github_callback_and_webhook",
            "set_urls",
            values={"callback_url": oauth_redirect_url, "webhook_url": events_request_url},
            selectors=_selectors("input[name=callback_url]", "input[name=hook_url]", "input[type=url]"),
            text_targets=_text_targets("Callback URL", "Webhook URL", "Permissions", "Subscribe to events"),
        ),
    ]


def _google_dwd_dom_steps(source: str, primary_artifacts: list[str]) -> list[dict[str, Any]]:
    return [
        _dom_step(
            "open_google_dwd_add_client",
            "click",
            selectors=_selectors("button", "[role=button]", "input"),
            text_targets=_text_targets("Add new", "Add client", "Domain-wide delegation"),
        ),
        _dom_step(
            "fill_google_dwd_client_and_scopes",
            "fill_from_artifact",
            artifacts=primary_artifacts,
            selectors=_selectors("input[type=text]", "textarea", "button[type=submit]"),
            text_targets=_text_targets("Client ID", "OAuth scopes", "Authorize", "Save"),
        ),
        _dom_step(
            "confirm_google_inclusion_scope",
            "collect_text",
            fields=[f"{source} inclusion scope", "workspace domain", "admin email"],
            selectors=_selectors("input", "textarea", "table", "[role=row]"),
            text_targets=_text_targets("Users", "Groups", "Organizational units", "Shared drives"),
        ),
    ]


def _aws_dom_steps(primary_artifacts: list[str]) -> list[dict[str, Any]]:
    return [
        _dom_step(
            "create_or_update_aws_role",
            "fill_from_artifact",
            artifacts=primary_artifacts,
            selectors=_selectors("textarea", "input[name=roleName]", "button"),
            text_targets=_text_targets("Create role", "Trust policy", "Permissions policy", "External ID"),
        ),
        _dom_step(
            "collect_aws_role_arn",
            "collect_text",
            fields=["role ARN", "account id", "regions"],
            selectors=_selectors("code", "input[readonly]", "[data-testid]", "dd"),
            text_targets=_text_targets("ARN", "Account ID", "Role summary"),
        ),
    ]


def _jira_dom_steps(primary_artifacts: list[str], events_request_url: str | None) -> list[dict[str, Any]]:
    return [
        _dom_step(
            "prepare_jira_api_token_ref",
            "human_pause",
            artifacts=primary_artifacts,
            text_targets=_text_targets("Create API token", "API tokens", "Copy"),
            human_reason="Atlassian token creation/reveal must be completed by the customer admin.",
        ),
        _dom_step(
            "configure_jira_webhook",
            "set_url",
            value=events_request_url,
            selectors=_selectors("input[type=url]", "textarea", "button"),
            text_targets=_text_targets("Webhooks", "URL", "Events", "Create webhook"),
        ),
    ]


def _whatsapp_dom_steps(primary_artifacts: list[str], events_request_url: str | None) -> list[dict[str, Any]]:
    return [
        _dom_step(
            "prepare_whatsapp_verify_token",
            "generate_refs",
            refs=["verify token ref"],
            human_reason="Fyralis generates the webhook verify token inside the customer BYOC browser session.",
        ),
        _dom_step(
            "configure_meta_webhook",
            "set_config_values",
            fields=[
                {
                    "name": "callback_url",
                    "value": events_request_url,
                    "selectors": _selectors(
                        "input[type=url]",
                        "input[name=callback_url]",
                        "input[placeholder*='Callback']",
                    ),
                },
                {
                    "name": "verify_token",
                    "generated_secret_field": "verify_token",
                    "selectors": _selectors(
                        "input[name=verify_token]",
                        "input[placeholder*='Verify']",
                        "input[aria-label*='Verify']",
                        "input[type=text]",
                    ),
                },
            ],
            artifacts=primary_artifacts,
            selectors=_selectors("input[type=url]", "input[name=callback_url]", "input[name=verify_token]"),
            text_targets=_text_targets("Webhooks", "Callback URL", "Verify token", "Verify and save"),
        ),
        _dom_step(
            "subscribe_whatsapp_events",
            "click",
            selectors=_selectors("button", "input[type=checkbox]", "[role=checkbox]"),
            text_targets=_text_targets("messages", "Subscribe", "WhatsApp Business Account"),
        ),
    ]


def _api_token_dom_steps(
    source: str,
    primary_artifacts: list[str],
    events_request_url: str | None,
) -> list[dict[str, Any]]:
    steps = [
        _dom_step(
            f"create_{source}_least_privilege_token",
            "human_pause",
            artifacts=primary_artifacts,
            selectors=_selectors("button", "input", "textarea"),
            text_targets=_text_targets("API token", "Create token", "Service account", "Read only"),
            human_reason="Provider credential creation/reveal must be completed by the customer admin.",
        )
    ]
    if events_request_url:
        steps.append(
            _dom_step(
                f"configure_{source}_webhook",
                "set_url",
                value=events_request_url,
                selectors=_selectors("input[type=url]", "textarea", "button"),
                text_targets=_text_targets("Webhook", "Callback URL", "Signing secret", "Save"),
            )
        )
    return steps


def _oauth_app_dom_steps(
    source: str,
    primary_artifacts: list[str],
    oauth_redirect_url: str | None,
    events_request_url: str | None,
) -> list[dict[str, Any]]:
    steps = [
        _dom_step(
            f"configure_{source}_oauth_app",
            "set_url",
            value=oauth_redirect_url,
            artifacts=primary_artifacts,
            selectors=_selectors("input[type=url]", "textarea", "button"),
            text_targets=_text_targets("Redirect URI", "Callback URL", "OAuth", "Save"),
        )
    ]
    if events_request_url:
        steps.append(
            _dom_step(
                f"configure_{source}_webhook",
                "set_url",
                value=events_request_url,
                selectors=_selectors("input[type=url]", "textarea", "button"),
                text_targets=_text_targets("Webhook", "Event", "Callback URL", "Save"),
            )
        )
    return steps


def _local_session_dom_steps(source: str, primary_artifacts: list[str]) -> list[dict[str, Any]]:
    if source == "signal":
        auth_targets = _text_targets(
            "Link device",
            "QR code",
            "Signal Desktop",
            "Linked devices",
        )
        scope_fields = ["approved contacts", "approved groups", "account label"]
        scope_targets = _text_targets("Contacts", "Groups", "Linked devices", "Chats")
    else:
        auth_targets = _text_targets("API ID", "API hash", "Link device", "QR code", "Login code")
        scope_fields = ["approved contacts", "approved groups", "approved channels", "account label"]
        scope_targets = _text_targets("Contacts", "Groups", "Channels", "Dialogs")
    return [
        _dom_step(
            f"prepare_{source}_local_session",
            "human_pause",
            artifacts=primary_artifacts,
            selectors=_selectors("input", "textarea", "button", "code"),
            text_targets=auth_targets,
            human_reason="Local session/device authorization must be completed by the customer account owner.",
        ),
        _dom_step(
            f"collect_{source}_approved_scope",
            "collect_text",
            fields=scope_fields,
            selectors=_selectors("input", "textarea", "[role=listitem]", "table"),
            text_targets=scope_targets,
        ),
    ]


def _dom_step(
    step_id: str,
    action: str,
    **kwargs: Any,
) -> dict[str, Any]:
    payload = {
        "id": step_id,
        "action": action,
        "status": "ready",
        "raw_secret_values_included": False,
    }
    payload.update({key: value for key, value in kwargs.items() if value not in (None, [], {})})
    return payload


def _selectors(*values: str) -> list[str]:
    return [value for value in values if value]


def _text_targets(*values: str) -> list[str]:
    return [value for value in values if value]


def _origin_from_url(value: str, fallback: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return fallback


def _recipe_setup_bundle(
    *,
    source: str,
    recipe: dict[str, Any],
    provider_console_url: str | None,
    oauth_redirect_url: str | None,
    events_request_url: str | None,
    install_url: str | None,
    native_connect: dict[str, Any] | None,
) -> dict[str, Any]:
    setup_summary = {
        "source": source,
        "provider_console_url": provider_console_url,
        "install_url": install_url,
        "oauth_redirect_url": oauth_redirect_url,
        "events_request_url": events_request_url,
        "settings_targets": list(recipe.get("settings_targets") or []),
        "agent_collects": list(recipe.get("agent_collects") or []),
        "agent_generates": list(recipe.get("agent_generates") or []),
        "native_connect": native_connect,
        "raw_secret_values_included": False,
    }
    filename_source = source.replace("_", "-")
    browser_dom_plan = _browser_dom_plan(
        source=source,
        kind="generic_provider_setup_contract",
        provider_console_url=provider_console_url,
        oauth_redirect_url=oauth_redirect_url,
        events_request_url=events_request_url,
        settings_targets=setup_summary["settings_targets"],
        collected_non_secret_fields=setup_summary["agent_collects"],
        generated_refs=setup_summary["agent_generates"],
        primary_artifacts=[f"fyralis-{filename_source}-provider-setup.json"],
    )
    return {
        "schema_version": "fyralis.byoc.source.provider_setup_bundle.v1",
        "source": source,
        "kind": "generic_provider_setup_contract",
        "provider_console_url": provider_console_url,
        "oauth_redirect_url": oauth_redirect_url,
        "events_request_url": events_request_url,
        "settings_targets": setup_summary["settings_targets"],
        "collected_non_secret_fields": setup_summary["agent_collects"],
        "generated_refs": setup_summary["agent_generates"],
        "browser_dom_plan": browser_dom_plan,
        "artifacts": [
            {
                "name": "provider_setup_summary",
                "filename": f"fyralis-{filename_source}-provider-setup.json",
                "media_type": "application/json",
                "content": json.dumps(setup_summary, indent=2, sort_keys=True) + "\n",
            }
        ],
        "agent_actions": [
            {
                "id": "materialize_provider_setup_bundle",
                "kind": "materialize_provider_setup_bundle",
                "label": "Generate provider setup bundle.",
            },
            {
                "id": "materialize_browser_dom_plan",
                "kind": "materialize_browser_dom_plan",
                "label": "Prepare provider browser DOM action plan.",
            }
        ],
        "human_gates": list(recipe.get("human_gates") or []),
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_provider_setup_bundle_only",
    }


def _normalize_source(source: str) -> str:
    return source.strip().lower().replace("-", "_")


__all__ = [
    "SLACK_BOT_EVENTS",
    "SLACK_BOT_SCOPES",
    "SLACK_USER_SCOPES",
    "build_source_provider_setup_bundle",
    "provider_setup_bundle_actions",
    "slack_manifest_text",
]
