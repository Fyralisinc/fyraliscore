"""Contract-bound provider notes and local rehearsal artifacts.

The source catalog owns which callable applies to a source.  These callables
keep provider-specific artifact formats out of the shared CLI dispatcher while
returning data-only descriptors that the CLI can write safely.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from services.ingest.source_contract.catalog import source_definition
from services.platform.runtime.source_browser_agent_setup import (
    SLACK_BOT_EVENTS,
    SLACK_BOT_SCOPES,
    SLACK_USER_SCOPES,
)


RehearsalArtifactDescriptor = dict[str, Any]


def generic_provider_setup_notes(source_id: str) -> list[str]:
    """Return the shared setup notes using source-owned ingress metadata."""

    source = source_definition(source_id)
    notes = [
        (
            f"Use the customer-owned {source.source_id} admin console to "
            "approve the connection."
        ),
        (
            "Store credentials in the customer-cloud secret manager or local "
            "env file only."
        ),
        "Run the generated Fyralis rehearsal command after the env file is filled.",
    ]
    if source.onboarding.ingress_paths:
        notes.append(
            "Register the generated webhook URL with the provider when required."
        )
    if source.onboarding.no_ingress_reason:
        notes.append(source.onboarding.no_ingress_reason)
    return notes


def discord_provider_setup_notes(_source_id: str) -> list[str]:
    return [
        "Use the Discord Developer Portal to create an application and bot.",
        "Set the OAuth redirect URL to the callback URL.",
        "Set the interactions endpoint to the webhook URL.",
        (
            "Copy client ID, client secret, application ID, app public key, "
            "and bot token into the local env file."
        ),
    ]


def facebook_pages_provider_setup_notes(_source_id: str) -> list[str]:
    return [
        "Use Meta for Developers to create or update the Facebook app.",
        "Set the OAuth redirect URL to the callback URL.",
        "Set the Messenger webhook callback URL and verify token.",
        (
            "Copy app ID, app secret, redirect URI, and webhook verify token "
            "into the local env file."
        ),
    ]


def notion_provider_setup_notes(_source_id: str) -> list[str]:
    return [
        "Use Notion integrations settings to create an OAuth integration.",
        "Set redirect URI to the callback URL.",
        "Configure webhook subscription after gateway env is applied.",
        (
            "Copy the webhook verification token into "
            "NOTION_WEBHOOK_VERIFICATION_TOKEN when Notion provides it."
        ),
    ]


def figma_provider_setup_notes(_source_id: str) -> list[str]:
    return [
        (
            "Create one private Figma OAuth app owned by this customer BYOC "
            "deployment."
        ),
        "Register the exact callback URL under Figma OAuth credentials.",
        (
            "Store the Client Secret through FIGMA_CLIENT_SECRET_SECRET_REF; "
            "never enter it in Fyralis onboarding."
        ),
        (
            "After the deployment readiness check passes, users connect "
            "explicitly selected design file URLs from the Figma card."
        ),
    ]


def build_slack_rehearsal_artifacts(
    *,
    source_id: str,
    profile: Mapping[str, Any],
    public_url: str,
) -> tuple[RehearsalArtifactDescriptor, ...]:
    """Build the two Slack manifests plus the legacy env-file alias."""

    del source_id, profile
    bot_scopes = "\n".join(f"      - {scope}" for scope in SLACK_BOT_SCOPES)
    user_scopes = "\n".join(f"      - {scope}" for scope in SLACK_USER_SCOPES)
    bot_events = "\n".join(f"      - {event}" for event in SLACK_BOT_EVENTS)
    base_manifest = f"""display_information:
  name: Fyralis Local Rehearsal
  description: Slack ingestion test app for the local Fyralis BYOC rehearsal.
  background_color: "#0b1020"
features:
  bot_user:
    display_name: Fyralis
    always_online: false
oauth_config:
  redirect_urls:
    - {public_url}/integrations/slack/callback
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
    events_manifest = (
        base_manifest.rstrip()
        + f"""
  event_subscriptions:
    request_url: {public_url}/webhooks/slack/events
    bot_events:
{bot_events}
"""
    )
    return (
        {
            "name": "manifest",
            "filename": "fyralis-slack-app-manifest.yaml",
            "text": base_manifest,
        },
        {
            "name": "events_manifest",
            "filename": "fyralis-slack-app-events-manifest.yaml",
            "text": events_manifest,
        },
        {
            "name": "legacy_env_example",
            "filename": "slack-app.env.example",
            "copy_from": "env_example",
        },
    )


def build_jira_rehearsal_artifacts(
    *,
    source_id: str,
    profile: Mapping[str, Any],
    public_url: str,
) -> tuple[RehearsalArtifactDescriptor, ...]:
    """Build the Jira token-connect payload consumed by local rehearsal."""

    del source_id, profile
    return (
        {
            "name": "connect_payload",
            "filename": "jira-connect-payload.example.json",
            "json": {
                "base_url": "${JIRA_BASE_URL}",
                "account_email": "${JIRA_ACCOUNT_EMAIL}",
                "api_token_ref": "${JIRA_API_TOKEN_REF}",
                "project_keys": (
                    "${JIRA_PROJECT_KEYS comma-separated or blank for all}"
                ),
                "webhook_secret_ref": "${JIRA_WEBHOOK_SECRET_REF optional}",
                "webhook_url": f"{public_url}/webhooks/jira/events",
                "note": (
                    "Resolve refs inside customer cloud before submitting "
                    "finalize; do not export raw token values."
                ),
            },
        },
    )


def build_telegram_rehearsal_artifacts(
    *,
    source_id: str,
    profile: Mapping[str, Any],
    public_url: str,
) -> tuple[RehearsalArtifactDescriptor, ...]:
    """Build the customer-local Telegram MTProto session plan."""

    del public_url
    required_env = profile.get("required_env", ())
    return (
        {
            "name": "session_plan",
            "filename": "telegram-session-plan.json",
            "json": {
                "source": source_id,
                "provider_kind": "local_gateway_session",
                "steps": [
                    "Create a Telegram API ID and API hash at my.telegram.org.",
                    (
                        "Run customer-cloud MTProto login to produce a "
                        "StringSession."
                    ),
                    (
                        "Store live and optional backfill sessions in the "
                        "customer secret manager."
                    ),
                    (
                        "Select approved dialogs/chats and finalize the "
                        "installation locally."
                    ),
                ],
                "required_env": list(required_env),
                "raw_secret_values_included": False,
            },
        },
    )


__all__ = [
    "RehearsalArtifactDescriptor",
    "build_jira_rehearsal_artifacts",
    "build_slack_rehearsal_artifacts",
    "build_telegram_rehearsal_artifacts",
    "discord_provider_setup_notes",
    "facebook_pages_provider_setup_notes",
    "figma_provider_setup_notes",
    "generic_provider_setup_notes",
    "notion_provider_setup_notes",
]
