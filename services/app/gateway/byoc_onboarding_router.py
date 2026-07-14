"""Hosted-portal onboarding routes for Design Partner BYOC."""

from __future__ import annotations

import asyncio
import copy
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, status

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
from services.platform.runtime.source_browser_agent_recipes import (
    browser_agent_recipe_for_source,
)
from services.platform.runtime.source_browser_agent_runner import (
    SourceBrowserAgentRunnerInputs,
    run_source_browser_agent,
)
from services.platform.runtime.source_browser_agent_workflow import (
    source_browser_agent_run_for_payload,
)

from lib.shared.errors import DiscordApiError
from lib.shared.ids import uuid7
from lib.shared.secrets import load_app_secret_text_from_env
from services.ingest.integrations.discord.client import DiscordClient

_DISCORD_TEXT_CHANNEL_TYPE = 0
_DISCORD_CATEGORY_CHANNEL_TYPE = 4
_DISCORD_ANNOUNCEMENT_CHANNEL_TYPE = 5
_DISCORD_ANNOUNCEMENT_THREAD_TYPE = 10
_DISCORD_PUBLIC_THREAD_TYPE = 11
_DISCORD_PRIVATE_THREAD_TYPE = 12
_DISCORD_FORUM_CHANNEL_TYPE = 15
_DISCORD_MEDIA_CHANNEL_TYPE = 16
_DISCORD_OVERWRITE_ROLE_TYPE = 0
_DISCORD_VIEW_CHANNEL_PERMISSION = 1 << 10
_DISCORD_MESSAGE_CHANNEL_TYPES = {
    _DISCORD_TEXT_CHANNEL_TYPE,
    _DISCORD_ANNOUNCEMENT_CHANNEL_TYPE,
    _DISCORD_ANNOUNCEMENT_THREAD_TYPE,
    _DISCORD_PUBLIC_THREAD_TYPE,
    _DISCORD_PRIVATE_THREAD_TYPE,
}
_DISCORD_THREAD_PARENT_CHANNEL_TYPES = {
    _DISCORD_TEXT_CHANNEL_TYPE,
    _DISCORD_ANNOUNCEMENT_CHANNEL_TYPE,
    _DISCORD_FORUM_CHANNEL_TYPE,
    _DISCORD_MEDIA_CHANNEL_TYPE,
}

_ALL_REHEARSAL_SOURCES = {
    "ashby",
    "aws",
    "brex",
    "carta",
    "deel",
    "discord",
    "facebook_pages",
    "figma",
    "fireflies",
    "github",
    "gmail",
    "google_calendar",
    "google_drive",
    "grafana",
    "gusto",
    "hibob",
    "jira",
    "linkedin",
    "mercury",
    "miro",
    "notion",
    "quickbooks",
    "ramp",
    "signal",
    "slack",
    "telegram",
    "whatsapp",
}
_OAUTH_REHEARSAL_SOURCES = {
    "slack",
    "github",
    "discord",
    "figma",
    "notion",
    "facebook_pages",
}
_FORM_REHEARSAL_SOURCES = {"jira", "telegram", "whatsapp"}
_SOURCE_SPECIFIC_FINALIZE_SOURCES = {"jira", "telegram", "whatsapp"}
_GENERIC_FINALIZE_BLOCKED_SOURCES = (
    _OAUTH_REHEARSAL_SOURCES
    | _SOURCE_SPECIFIC_FINALIZE_SOURCES
    | {
        "ashby",
        "aws",
        "brex",
        "carta",
        "deel",
        "fireflies",
        "gmail",
        "google_calendar",
        "google_drive",
        "grafana",
        "gusto",
        "hibob",
        "linkedin",
        "mercury",
        "miro",
        "quickbooks",
        "ramp",
        "signal",
    }
)
_REHEARSAL_SOURCES = _ALL_REHEARSAL_SOURCES
_SAFE_PROVIDER_ERROR_CODES = {
    "telegram_connect_failed",
    "telegram_dialogs_must_be_list",
    "telegram_missing_api_credentials",
}
_SOURCE_ACCESS_PERMISSION_STATUSES = {
    "ready",
    "missing_access",
    "needs_admin",
    "not_selected",
    "unknown",
}
_SOURCE_ACCESS_READY_REPLAY_FROM_STATUSES = {
    "missing_access",
    "needs_admin",
    "not_selected",
    "unknown",
}
_DISCORD_ACCESS_STATUS_CACHE_SECONDS = 45
_DISCORD_ACCESS_STATUS_ERROR_CACHE_SECONDS = 15
_DISCORD_ACCESS_STATUS_CACHE: dict[
    tuple[str, str, str],
    tuple[datetime, dict[str, Any]],
] = {}
_DISCORD_ACCESS_STATUS_LOCKS: dict[tuple[str, str, str], asyncio.Lock] = {}

_SOURCE_CALLBACK_PATHS = {
    "slack": "/integrations/slack/callback",
    "discord": "/integrations/discord/callback",
    "github": "/integrations/github/callback",
    "figma": "/integrations/figma/oauth/callback",
    "notion": "/integrations/notion/callback",
    "facebook_pages": "/integrations/facebook_pages/callback",
}

_SOURCE_LIVE_INGRESS_PATHS = {
    "ashby": "/webhooks/ashby/{install-id}",
    "brex": "/webhooks/brex",
    "deel": "/webhooks/deel",
    "slack": "/webhooks/slack/events",
    "discord": "/webhooks/discord",
    "facebook_pages": "/integrations/facebook_pages/webhook",
    "fireflies": "/webhooks/fireflies",
    "github": "/webhooks/github",
    "gmail": "/webhooks/gmail/pubsub",
    "google_drive": "/webhooks/google_drive/push",
    "grafana": "/webhooks/grafana/events",
    "gusto": "/webhooks/gusto",
    "hibob": "/webhooks/hibob",
    "notion": "/webhooks/notion/events",
    "jira": "/webhooks/jira/events",
    "mercury": "/webhooks/mercury/events",
    "quickbooks": "/webhooks/quickbooks/events",
    "ramp": "/webhooks/ramp",
    "telegram": "customer-cloud MTProto gateway worker",
    "whatsapp": "/integrations/whatsapp/webhook",
}

_SOURCE_REQUIRED_INPUTS = {
    "ashby": [
        "api_token",
    ],
    "aws": [
        "role_arn",
    ],
    "brex": [
        "api_token",
    ],
    "carta": [
        "token_ref",
    ],
    "deel": [
        "api_token",
    ],
    "facebook_pages": [
        "page_id",
    ],
    # The native Figma OAuth card accepts only file-scoped URLs.  The
    # deployment-owned client secret is never an end-user onboarding field.
    "figma": [
        "file_urls",
    ],
    "fireflies": [
        "api_token",
    ],
    "gmail": [
        "workspace_domain",
        "admin_email",
        "dwd_grant",
    ],
    "google_calendar": [
        "workspace_domain",
        "admin_email",
        "dwd_grant",
    ],
    "google_drive": [
        "workspace_domain",
        "admin_email",
        "dwd_grant",
    ],
    "grafana": [
        "base_url",
        "service_account_token",
    ],
    "gusto": [
        "token_ref",
    ],
    "hibob": [
        "service_user_id",
        "service_user_token",
    ],
    "jira": [
        "base_url",
        "account_email",
        "api_token",
    ],
    "linkedin": [
        "token_ref",
    ],
    "mercury": [
        "api_token",
    ],
    "miro": [
        "api_token",
    ],
    "quickbooks": [
        "realm_id",
        "token_ref",
    ],
    "ramp": [
        "access_token_or_client_credentials",
    ],
    "signal": [
        "linked_device_session",
    ],
    "slack": [
        "slack_app_config_token",
    ],
    "telegram": [
        "account_label",
        "api_id",
        "api_hash",
        "live_session",
    ],
    "whatsapp": [
        "phone_number_id",
        "verify_token",
        "app_secret",
    ],
}

_SOURCE_OPTIONAL_INPUTS = {
    "ashby": ["org_id", "base_url", "webhook_secret"],
    "aws": ["account_id", "region"],
    "brex": ["organization_id", "base_url", "account_ids", "webhook_secret"],
    "carta": ["firm_id", "oauth_client", "base_url", "refresh_token_ref"],
    "deel": ["organization_id", "base_url", "contract_ids", "webhook_secret"],
    "facebook_pages": ["oauth_redirect_url", "events_request_url"],
    "figma": [],
    "fireflies": ["workspace_id", "base_url", "webhook_secret"],
    "gmail": ["scope", "inclusion_spec", "pubsub_topic", "watch_channel_id"],
    "google_calendar": ["scope", "inclusion_spec"],
    "google_drive": [
        "scope",
        "inclusion_spec",
        "include_shared_drives",
        "watch_channel_id",
    ],
    "grafana": ["webhook_secret"],
    "gusto": [
        "company_uuid",
        "oauth_client",
        "base_url",
        "refresh_token_ref",
        "webhook_verifier_token",
    ],
    "hibob": ["company_id", "base_url", "webhook_secret"],
    "jira": ["project_keys", "webhook_secret"],
    "linkedin": ["organization_urn", "oauth_client", "base_url", "refresh_token_ref"],
    "mercury": ["organization_id", "base_url", "account_ids", "webhook_secret"],
    "miro": ["base_url", "board_ids"],
    "quickbooks": [
        "oauth_client",
        "base_url",
        "refresh_token_ref",
        "webhook_verifier_token",
    ],
    "ramp": ["business_id", "base_url", "entity_scope", "webhook_verifier_token"],
    "signal": ["account_label", "backfill_session", "thread_scope"],
    "telegram": ["backfill_session", "dialogs"],
    "whatsapp": ["business_account_id", "display_phone_number", "access_token"],
}

_GENERIC_PROVIDER_CONSOLES = {
    "ashby": "https://app.ashbyhq.com/admin/api",
    "aws": "https://console.aws.amazon.com/cloudformation/home#/stacks/create/template",
    "brex": "https://developer.brex.com/",
    "carta": "https://developers.app.carta.com/",
    "deel": "https://app.deel.com/",
    "facebook_pages": "https://developers.facebook.com/apps/",
    "figma": "https://www.figma.com/developers/apps",
    "fireflies": "https://app.fireflies.ai/integrations",
    "gmail": "https://admin.google.com/ac/owl/domainwidedelegation",
    "google_calendar": "https://admin.google.com/ac/owl/domainwidedelegation",
    "google_drive": "https://admin.google.com/ac/owl/domainwidedelegation",
    "grafana": "https://grafana.com/auth/sign-in/",
    "gusto": "https://dev.gusto.com/",
    "hibob": "https://app.hibob.com/",
    "linkedin": "https://www.linkedin.com/developers/apps",
    "mercury": "https://app.mercury.com/settings/tokens",
    "miro": "https://developers.miro.com/",
    "quickbooks": "https://developer.intuit.com/app/developer/myapps",
    "ramp": "https://developers.ramp.com/",
    "signal": "https://signal.org/download/",
    "whatsapp": "https://developers.facebook.com/apps/",
}

_AWS_RUNTIME_ROLE_ENV_KEYS = (
    "FYRALIS_BYOC_SOURCE_RUNTIME_ROLE_ARN",
    "FYRALIS_BYOC_RUNTIME_ROLE_ARN",
    "FYRALIS_AWS_SOURCE_RUNTIME_ROLE_ARN",
    "FYRALIS_AWS_RUNTIME_ROLE_ARN",
)
_AWS_RUNTIME_ROLE_CONTEXT_KEYS = (
    "aws_assuming_principal_arn",
    "aws_source_runtime_role_arn",
    "source_runtime_role_arn",
    "byoc_runtime_role_arn",
    "runtime_role_arn",
    "setup_role_arn",
    "setupRoleArn",
)
_AWS_RUNTIME_ROLE_OUTPUT_KEYS = (
    "SourceRuntimeRoleArn",
    "source_runtime_role_arn",
    "ByocRuntimeRoleArn",
    "RuntimeRoleArn",
    "SetupRoleArn",
    "EksNodeRoleArn",
)
_AWS_SOURCE_RUNTIME_TEMPLATE_PATH = (
    ".fyralis/sources/aws/byoc-runtime/"
    "fyralis-byoc-source-runtime-role-cloudformation.json"
)
_AWS_SOURCE_EXTERNAL_ID_PATH = (
    ".fyralis/sources/aws/browser-agent-provider-setup/external-id.txt"
)

_GENERIC_AUTHORIZATION_MODES = {
    "aws": "customer_iam_role_ref",
    "signal": "customer_linked_device_session",
    "telegram": "customer_mtproto_session",
    "whatsapp": "customer_webhook_app",
}

_SOURCE_METHODS = {
    "ashby": "api_token",
    "aws": "iam_role",
    "brex": "api_token",
    "carta": "oauth",
    "deel": "api_token",
    "discord": "oauth_plus_gateway",
    "facebook_pages": "oauth",
    "figma": "oauth",
    "fireflies": "api_token",
    "github": "oauth",
    "gmail": "dwd",
    "google_calendar": "dwd",
    "google_drive": "dwd",
    "grafana": "api_token",
    "gusto": "oauth",
    "hibob": "api_token",
    "jira": "api_token",
    "linkedin": "poll",
    "mercury": "api_token",
    "miro": "api_token",
    "notion": "oauth",
    "quickbooks": "oauth",
    "ramp": "oauth_client_credentials",
    "signal": "gateway",
    "slack": "oauth",
    "telegram": "gateway",
    "whatsapp": "webhook",
}

_SOURCE_DISCOVERY_TARGETS = {
    "ashby": "jobs, candidates, interviews, and organization metadata",
    "aws": "account, region, CloudTrail, and inventory scope",
    "brex": "cash accounts, cards, and transaction scopes",
    "carta": "issuer, securities, and stakeholder scopes",
    "deel": "contracts, workers, and payment scopes",
    "discord": "guilds, message channels, private channels, forum/media posts, and threads",
    "facebook_pages": "Facebook Pages, Messenger conversations, Page message history, and webhooks",
    "figma": (
        "explicitly selected Figma design files, document structure, comments, "
        "and version history"
    ),
    "fireflies": "workspace, meetings, and transcripts",
    "github": "installations, repositories, pull requests, issues, and webhooks",
    "gmail": "mailboxes, labels, watch channels, and Pub/Sub topic readiness",
    "google_calendar": "calendars and shared calendar inclusion scope",
    "google_drive": "shared drives, folders, files, and change tokens",
    "grafana": "folders, dashboards, alert rules, and org metadata",
    "gusto": "company, employees, and payroll scopes",
    "hibob": "people fields, reports, and company metadata",
    "jira": "projects, issue types, comments, and webhook registration",
    "linkedin": "organization/page scope and polling windows",
    "mercury": "organization, accounts, and transaction scopes",
    "miro": "teams, boards, and board items",
    "notion": "shared pages, databases, users, and webhook eligibility",
    "quickbooks": "company realm, accounting entities, and webhook verifier",
    "ramp": "business scope, transactions, reimbursements, cards, and users",
    "signal": "linked account, approved contacts, groups, and threads",
    "slack": "workspace, public/private channels the app can access, users, and events",
    "telegram": "account, dialogs, channels, groups, and live update cursor",
    "whatsapp": "business account, phone numbers, webhook verification, and message events",
}

_SOURCE_NATIVE_CONNECT_CONTRACTS = {
    "ashby": {
        "kind": "api_token_native_connect",
        "preflight_path": "/integrations/ashby/connect/preflight",
        "finalize_path": "/integrations/ashby/connect/finalize",
        "preflight_payload_fields": ["api_token", "base_url", "org_id"],
        "payload_fields": [
            "api_token",
            "base_url",
            "org_id",
            "entities",
            "webhook_secret",
        ],
    },
    "aws": {
        "kind": "aws_iam_native_connect",
        "preflight_path": "/integrations/aws/connect/preflight",
        "finalize_path": "/integrations/aws/connect/finalize",
        "payload_fields": [
            "account_id",
            "region",
            "credential_kind",
            "role_arn",
            "external_id",
            "backfill_window_days",
        ],
    },
    "brex": {
        "kind": "api_token_native_connect",
        "preflight_path": "/integrations/brex/connect/preflight",
        "finalize_path": "/integrations/brex/connect/finalize",
        "preflight_payload_fields": ["api_token", "base_url"],
        "payload_fields": [
            "api_token",
            "base_url",
            "account_ids",
            "organization_id",
            "webhook_secret",
        ],
    },
    "carta": {
        "kind": "access_token_native_connect",
        "preflight_path": "/integrations/carta/connect/preflight",
        "finalize_path": "/integrations/carta/connect/finalize",
        "payload_fields": [
            "access_token",
            "base_url",
            "issuer_id",
            "firm_id",
            "client_secret",
            "refresh_token",
            "entities",
        ],
    },
    "deel": {
        "kind": "api_token_native_connect",
        "preflight_path": "/integrations/deel/connect/preflight",
        "finalize_path": "/integrations/deel/connect/finalize",
        "preflight_payload_fields": ["api_token", "base_url"],
        "payload_fields": [
            "api_token",
            "base_url",
            "contract_ids",
            "organization_id",
            "webhook_secret",
        ],
    },
    "discord": {
        "kind": "oauth_gateway_native_connect",
        "preflight_path": "/integrations/discord/connect/preflight",
        "finalize_path": "/integrations/discord/connect/finalize",
        "payload_fields": [
            "guild_id",
            "application_id",
            "access_mode",
            "approved_channel_ids",
            "oauth_redirect_url",
            "events_request_url",
        ],
    },
    "figma": {
        # This is intentionally not a preflight/finalize contract.  The
        # browser starts a file-scoped OAuth authorization, and Figma's public
        # callback finalizes the install server-side.  Legacy PAT endpoints
        # stay isolated from this normal BYOC onboarding path.
        "kind": "figma_oauth_file_scoped_connect",
        "start_path": "/integrations/figma/oauth/start",
        "status_path": "/integrations/figma/connect/status",
        "retry_path": "/integrations/figma/connect/retry",
        "disconnect_path": "/integrations/figma/connect",
        "payload_fields": ["file_urls", "return_path"],
    },
    "fireflies": {
        "kind": "api_token_native_connect",
        "preflight_path": "/integrations/fireflies/connect/preflight",
        "finalize_path": "/integrations/fireflies/connect/finalize",
        "preflight_payload_fields": ["api_token", "base_url"],
        "payload_fields": [
            "api_token",
            "base_url",
            "workspace_id",
            "webhook_secret",
        ],
    },
    "github": {
        "kind": "github_app_native_connect",
        "preflight_path": "/integrations/github/connect/preflight",
        "finalize_path": "/integrations/github/connect/finalize",
        "payload_fields": [
            "installation_id",
            "organization",
            "repository_selection",
            "oauth_redirect_url",
            "events_request_url",
        ],
    },
    "gmail": {
        "kind": "google_workspace_dwd",
        "preflight_path": "/integrations/gmail/connect/preflight",
        "finalize_path": "/integrations/gmail/connect/finalize",
        "preflight_payload_fields": ["workspace_domain", "admin_email", "scope"],
        "payload_fields": [
            "workspace_domain",
            "admin_email",
            "scope",
            "inclusion_spec",
        ],
        "scope_aliases": ["gmail.metadata"],
    },
    "google_calendar": {
        "kind": "google_workspace_dwd",
        "preflight_path": "/integrations/google_calendar/connect/preflight",
        "finalize_path": "/integrations/google_calendar/connect/finalize",
        "preflight_payload_fields": ["workspace_domain", "admin_email", "scope"],
        "payload_fields": [
            "workspace_domain",
            "admin_email",
            "scope",
            "inclusion_spec",
        ],
        "scope_aliases": ["calendar.readonly"],
    },
    "google_drive": {
        "kind": "google_workspace_dwd",
        "preflight_path": "/integrations/google_drive/connect/preflight",
        "finalize_path": "/integrations/google_drive/connect/finalize",
        "preflight_payload_fields": ["workspace_domain", "admin_email", "scope"],
        "payload_fields": [
            "workspace_domain",
            "admin_email",
            "scope",
            "inclusion_spec",
            "include_shared_drives",
        ],
        "scope_aliases": ["drive.readonly"],
    },
    "grafana": {
        "kind": "api_token_native_connect",
        "preflight_path": "/integrations/grafana/connect/preflight",
        "finalize_path": "/integrations/grafana/connect/finalize",
        "preflight_payload_fields": ["base_url", "service_account_token", "org_id"],
        "payload_fields": [
            "base_url",
            "service_account_token",
            "org_id",
            "webhook_secret",
        ],
    },
    "gusto": {
        "kind": "access_token_native_connect",
        "preflight_path": "/integrations/gusto/connect/preflight",
        "finalize_path": "/integrations/gusto/connect/finalize",
        "payload_fields": [
            "company_uuid",
            "access_token",
            "base_url",
            "refresh_token",
            "webhook_verifier_token",
            "entities",
        ],
    },
    "hibob": {
        "kind": "api_token_native_connect",
        "preflight_path": "/integrations/hibob/connect/preflight",
        "finalize_path": "/integrations/hibob/connect/finalize",
        "preflight_payload_fields": [
            "company_id",
            "service_user_id",
            "service_user_token",
            "base_url",
        ],
        "payload_fields": [
            "company_id",
            "service_user_id",
            "service_user_token",
            "base_url",
            "entities",
            "webhook_secret",
        ],
    },
    "linkedin": {
        "kind": "access_token_native_connect",
        "preflight_path": "/integrations/linkedin/connect/preflight",
        "finalize_path": "/integrations/linkedin/connect/finalize",
        "preflight_payload_fields": ["organization_urn", "access_token", "base_url"],
        "payload_fields": [
            "organization_urn",
            "access_token",
            "base_url",
            "refresh_token",
            "entities",
        ],
    },
    "jira": {
        "kind": "jira_api_token_native_connect",
        "preflight_path": "/integrations/jira/connect/preflight",
        "finalize_path": "/integrations/jira/connect/finalize",
        "preflight_payload_fields": ["base_url", "account_email", "api_token"],
        "payload_fields": [
            "base_url",
            "account_email",
            "api_token",
            "project_keys",
            "webhook_secret",
        ],
    },
    "mercury": {
        "kind": "api_token_native_connect",
        "preflight_path": "/integrations/mercury/connect/preflight",
        "finalize_path": "/integrations/mercury/connect/finalize",
        "preflight_payload_fields": ["api_token", "base_url"],
        "payload_fields": [
            "api_token",
            "base_url",
            "account_ids",
            "organization_id",
            "webhook_secret",
        ],
    },
    "miro": {
        "kind": "api_token_native_connect",
        "preflight_path": "/integrations/miro/connect/preflight",
        "finalize_path": "/integrations/miro/connect/finalize",
        "preflight_payload_fields": ["api_token", "base_url"],
        "payload_fields": [
            "api_token",
            "base_url",
            "board_ids",
        ],
    },
    "notion": {
        "kind": "oauth_callback_native_connect",
        "preflight_path": "/integrations/notion/connect/preflight",
        "finalize_path": "/integrations/notion/connect/finalize",
        "payload_fields": [
            "workspace_id",
            "shared_page_ids",
            "shared_database_ids",
            "oauth_redirect_url",
            "events_request_url",
            "installation_id",
        ],
    },
    "quickbooks": {
        "kind": "access_token_native_connect",
        "preflight_path": "/integrations/quickbooks/connect/preflight",
        "finalize_path": "/integrations/quickbooks/connect/finalize",
        "preflight_payload_fields": ["realm_id", "access_token", "base_url"],
        "payload_fields": [
            "realm_id",
            "access_token",
            "base_url",
            "refresh_token",
            "webhook_verifier_token",
            "entities",
        ],
    },
    "signal": {
        "kind": "local_session_native_connect",
        "preflight_path": "/integrations/signal/connect/preflight",
        "finalize_path": "/integrations/signal/connect/finalize",
        "payload_fields": [
            "account_label",
            "linked_device_session",
            "backfill_session",
            "threads",
        ],
    },
    "slack": {
        "kind": "oauth_callback_native_connect",
        "preflight_path": "/integrations/slack/connect/preflight",
        "finalize_path": "/integrations/slack/connect/finalize",
        "payload_fields": [
            "workspace_id",
            "approved_channel_ids",
            "oauth_redirect_url",
            "events_request_url",
            "installation_id",
        ],
    },
    "telegram": {
        "kind": "local_session_native_connect",
        "preflight_path": "/integrations/telegram/connect/preflight",
        "finalize_path": "/integrations/telegram/connect/finalize",
        "payload_fields": [
            "account_label",
            "api_id",
            "api_hash",
            "live_session",
            "backfill_session",
            "dialogs",
        ],
    },
    "ramp": {
        "kind": "ramp_native_connect",
        "preflight_path": "/integrations/ramp/connect/preflight",
        "finalize_path": "/integrations/ramp/connect/finalize",
        "preflight_payload_fields": [
            "access_token",
            "client_id",
            "client_secret",
            "scopes",
            "base_url",
        ],
        "payload_fields": [
            "access_token",
            "client_id",
            "client_secret",
            "base_url",
            "business_id",
            "entities",
            "webhook_verifier_token",
        ],
    },
    "whatsapp": {
        "kind": "whatsapp_native_connect",
        "preflight_path": "/integrations/whatsapp/connect/preflight",
        "finalize_path": "/integrations/whatsapp/connect/finalize",
        "preflight_payload_fields": ["phone_number_id", "app_secret", "verify_token"],
        "payload_fields": [
            "phone_number_id",
            "business_account_id",
            "display_phone_number",
            "app_secret",
            "verify_token",
            "access_token",
        ],
    },
    "facebook_pages": {
        "kind": "oauth_native_connect",
        "preflight_path": "/integrations/facebook_pages/connect/preflight",
        "finalize_path": "/integrations/facebook_pages/connect/finalize",
        "preflight_payload_fields": [
            "page_id",
            "oauth_redirect_url",
            "events_request_url",
        ],
        "payload_fields": [
            "page_id",
            "installation_id",
            "oauth_redirect_url",
            "events_request_url",
        ],
    },
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
                detail={"error": "unsupported_onboarding_plan"},
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
                detail={"error": "unsupported_onboarding_plan"},
            ) from exc
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "invalid_onboarding_intake"},
            ) from exc

    @router.post("/sources/{source_id}/rehearsal/prepare")
    async def prepare_source_rehearsal(
        request: Request,
        source_id: str,
    ) -> dict[str, Any]:
        source = _normalize_rehearsal_source(source_id)
        request_payload = await _optional_json_body(request)
        return await _prepare_source_rehearsal_response(
            request,
            source,
            request_payload=request_payload,
        )

    @router.get("/sources/{source_id}/rehearsal/status")
    async def source_rehearsal_status(
        request: Request,
        source_id: str,
    ) -> dict[str, Any]:
        source = _normalize_rehearsal_source(source_id)
        _require_source_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, _actor_id = _rehearsal_actor_ids()
        payload = await _source_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            source=source,
        )
        run_record = _source_auto_connect_run_store(request).get(
            _source_auto_connect_run_key(source)
        ) or _source_auto_connect_persisted_run_record(source)
        if run_record:
            _source_auto_connect_run_store(request)[
                _source_auto_connect_run_key(source)
            ] = dict(run_record)
            payload["auto_connect_run"] = dict(run_record)
        return payload

    @router.post("/sources/{source_id}/rehearsal/auto-connect")
    async def auto_connect_source_rehearsal(
        request: Request,
        background_tasks: BackgroundTasks,
        source_id: str,
    ) -> dict[str, Any]:
        source = _normalize_rehearsal_source(source_id)
        request_payload = await _optional_json_body(request)
        payload = await _prepare_source_rehearsal_response(
            request,
            source,
            request_payload=request_payload,
        )
        payload["auto_connect"] = _source_auto_connect_state(source, payload)
        payload["browser_agent_run"] = source_browser_agent_run_for_payload(
            source,
            payload,
            auto_state=payload["auto_connect"],
        )
        payload["auto_connect"]["browser_agent_run"] = payload["browser_agent_run"]
        payload["auto_connect"]["automation_run"] = _source_auto_connect_run_descriptor(
            source,
            payload,
            payload["browser_agent_run"],
        )
        run_record = _materialize_source_auto_connect_run(
            source,
            payload["browser_agent_run"],
            payload["auto_connect"]["automation_run"],
        )
        payload["auto_connect"]["automation_run"].update(run_record)
        _source_auto_connect_run_store(request)[
            _source_auto_connect_run_key(source)
        ] = run_record
        background_tasks.add_task(
            _execute_source_auto_connect_background_run,
            source,
            Path(run_record["run_artifact_path_hint"]),
            Path(run_record["receipt_path_hint"]),
            payload["gateway_api_base"],
            _source_auto_connect_run_store(request),
            _source_auto_connect_run_key(source),
        )
        return payload

    @router.post("/sources/slack/rehearsal/browser-agent/configuration")
    async def consume_slack_browser_agent_config_token(
        request: Request,
    ) -> dict[str, Any]:
        body = await _optional_json_body(request)
        provider_inputs = _source_finalize_inputs(body) if body else {}
        config_token = provider_inputs.get("slack_app_config_token", "").strip()
        if not config_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "slack_app_config_token_required"},
            )
        payload = await _prepare_source_rehearsal_response(
            request,
            "slack",
            provider_inputs=provider_inputs,
        )
        payload["auto_connect"] = _source_auto_connect_state("slack", payload)
        payload["browser_agent_run"] = source_browser_agent_run_for_payload(
            "slack",
            payload,
            auto_state=payload["auto_connect"],
        )
        payload["auto_connect"]["browser_agent_run"] = payload["browser_agent_run"]
        return {
            "schema_version": "fyralis.byoc.source.slack_config_token_consume.v1",
            "ok": bool(
                payload.get("install_url")
                or (payload.get("status") or {}).get("installed")
            ),
            "source": "slack",
            "install_url": payload.get("install_url"),
            "oauth_redirect_url": payload.get("oauth_redirect_url"),
            "provider_console_url": payload.get("provider_console_url"),
            "status": payload.get("status") or {},
            "auto_connect": {
                "state": payload["auto_connect"].get("state"),
                "label": payload["auto_connect"].get("label"),
                "message": payload["auto_connect"].get("message"),
                "install_url": payload["auto_connect"].get("install_url"),
            },
            "raw_secret_values_included": False,
            "raw_payloads_exported": False,
            "stored_scope": "sanitized_slack_config_token_handoff_metadata_only",
        }

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
                    str(key).strip() for key in requested_keys if str(key).strip()
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
        async with pool.acquire() as conn:
            await _emit_source_connection_proof(
                conn,
                tenant_id=tenant_id,
                actor_id=actor_id,
                source="jira",
                installation_id=base_url,
                visible_inputs={
                    "base_url": base_url,
                    "account_email": account_email,
                    "project_count": str(len(project_keys)),
                },
                secret_ref_names=(
                    ["api_token", "webhook_secret"]
                    if webhook_secret_ref
                    else ["api_token"]
                ),
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
        backfill_session = (body.get("backfill_session") or live_session).strip()
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
                detail={"error": _bounded_telegram_error_code(exc)},
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
        async with pool.acquire() as conn:
            await _emit_source_connection_proof(
                conn,
                tenant_id=tenant_id,
                actor_id=actor_id,
                source="telegram",
                installation_id=account_label,
                visible_inputs={
                    "account_label": account_label,
                    "api_id": api_id,
                    "dialog_count": str(len(dialogs)),
                },
                secret_ref_names=[
                    "api_hash",
                    "live_session",
                    "backfill_session",
                ],
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

    @router.post("/sources/whatsapp/rehearsal/finalize")
    async def finalize_whatsapp_rehearsal(request: Request) -> dict[str, Any]:
        _require_source_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, actor_id = _rehearsal_actor_ids()
        await _ensure_rehearsal_actor(pool, tenant_id=tenant_id, actor_id=actor_id)

        body = await _json_body(request)
        inputs = _source_finalize_inputs(body)
        missing_inputs = [
            name
            for name in _SOURCE_REQUIRED_INPUTS["whatsapp"]
            if not str(inputs.get(name, "")).strip()
        ]
        if missing_inputs:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "source_rehearsal_missing_required_inputs",
                    "source": "whatsapp",
                    "missing_inputs": missing_inputs,
                },
            )

        phone_number_id = inputs["phone_number_id"].strip()
        waba_id = (
            inputs.get("business_account_id") or inputs.get("waba_id") or ""
        ).strip() or None
        display_phone_number = inputs.get("display_phone_number", "").strip() or None

        secret_store = _secret_store_from_state(request, pool)
        app_secret_ref = await secret_store.put(
            inputs["app_secret"],
            label=f"whatsapp_app_secret:{phone_number_id}",
            tenant_id=tenant_id,
        )
        verify_token_ref = await secret_store.put(
            inputs["verify_token"],
            label=f"whatsapp_verify_token:{phone_number_id}",
            tenant_id=tenant_id,
        )
        access_token_ref = None
        if inputs.get("access_token"):
            access_token_ref = await secret_store.put(
                inputs["access_token"],
                label=f"whatsapp_access_token:{phone_number_id}",
                tenant_id=tenant_id,
            )

        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO whatsapp_installations
                        (tenant_id, phone_number_id, waba_id, display_phone_number,
                         app_secret_ref, verify_token_ref, access_token_ref,
                         app_secret, verify_token, access_token, enabled, updated_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,NULL,NULL,NULL,true, now())
                    ON CONFLICT (phone_number_id) DO UPDATE SET
                        tenant_id            = EXCLUDED.tenant_id,
                        waba_id              = COALESCE(
                                                 EXCLUDED.waba_id,
                                                 whatsapp_installations.waba_id
                                               ),
                        display_phone_number = COALESCE(
                                                 EXCLUDED.display_phone_number,
                                                 whatsapp_installations.display_phone_number
                                               ),
                        app_secret_ref       = EXCLUDED.app_secret_ref,
                        verify_token_ref     = EXCLUDED.verify_token_ref,
                        access_token_ref     = COALESCE(
                                                 EXCLUDED.access_token_ref,
                                                 whatsapp_installations.access_token_ref
                                               ),
                        app_secret           = NULL,
                        verify_token         = NULL,
                        access_token         = CASE
                                                 WHEN EXCLUDED.access_token_ref IS NOT NULL THEN NULL
                                                 ELSE whatsapp_installations.access_token
                                               END,
                        enabled              = true,
                        updated_at           = now()
                    RETURNING id, phone_number_id
                    """,
                    tenant_id,
                    phone_number_id,
                    waba_id,
                    display_phone_number,
                    app_secret_ref,
                    verify_token_ref,
                    access_token_ref,
                )
                await _emit_source_connection_proof(
                    conn,
                    tenant_id=tenant_id,
                    actor_id=actor_id,
                    source="whatsapp",
                    installation_id=phone_number_id,
                    visible_inputs={
                        key: value
                        for key, value in {
                            "phone_number_id": phone_number_id,
                            "business_account_id": waba_id,
                            "display_phone_number": display_phone_number,
                        }.items()
                        if value
                    },
                    secret_ref_names=[
                        name
                        for name, ref in {
                            "app_secret": app_secret_ref,
                            "verify_token": verify_token_ref,
                            "access_token": access_token_ref,
                        }.items()
                        if ref
                    ],
                )

        status_payload = await _source_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            source="whatsapp",
        )
        return {
            "ok": True,
            "source": "whatsapp",
            "installation_id": str(row["phone_number_id"]),
            "installation_row_id": str(row["id"]),
            "status": status_payload,
        }

    @router.post("/sources/aws/rehearsal/finalize")
    async def finalize_aws_rehearsal(request: Request) -> dict[str, Any]:
        _require_source_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, actor_id = _rehearsal_actor_ids()
        await _ensure_rehearsal_actor(pool, tenant_id=tenant_id, actor_id=actor_id)

        from services.ingest.integrations.aws.onboarding import finalize_install

        body = await _json_body(request)
        role_arn = _valid_aws_role_arn(body.get("role_arn"))
        if not role_arn:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "aws_role_arn_required"},
            )
        account_id = _aws_account_id_from_role_arn(role_arn)
        if not account_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "aws_account_id_required"},
            )
        region = _bounded_context_value(body.get("region")) or "us-east-1"
        external_id = _aws_source_external_id(body.get("external_id"))
        if not external_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "aws_external_id_missing",
                    "message": (
                        "Fyralis could not find the generated AWS ExternalId. "
                        "Run AWS Connect again or pass external_id explicitly."
                    ),
                },
            )
        secret_store = _secret_store_from_state(request, pool)
        secret_ref = await secret_store.put(
            json.dumps(
                {
                    "role_arn": role_arn,
                    "external_id": external_id,
                },
                sort_keys=True,
            ),
            label=f"aws_assume_role:{account_id}:{region}",
            tenant_id=tenant_id,
        )
        install_id = await finalize_install(
            pool,
            tenant_id=tenant_id,
            account_id=account_id,
            region=region,
            credential_kind="assume_role",
            secret_ref=secret_ref,
            backfill_window_days=int(body.get("backfill_window_days") or 90),
        )
        status_payload = await _source_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            source="aws",
        )
        return {
            "ok": True,
            "source": "aws",
            "installation_id": str(install_id),
            "account_id": account_id,
            "region": region,
            "status": status_payload,
        }

    @router.post("/sources/aws/rehearsal/retry")
    async def retry_aws_rehearsal(request: Request) -> dict[str, Any]:
        _require_source_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, actor_id = _rehearsal_actor_ids()
        await _ensure_rehearsal_actor(pool, tenant_id=tenant_id, actor_id=actor_id)

        install = await _source_installation_row(
            pool, tenant_id=tenant_id, source="aws"
        )
        if not install or not install["enabled"] or not install["has_secret"]:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "aws_install_not_ready"},
            )
        details = install.get("details", {})
        trigger_id = await _enqueue_source_manual_replay(
            pool,
            tenant_id=tenant_id,
            source="aws",
            payload={
                "reason": "ui_retry_first_sync",
                "account_id": details.get("account_id"),
                "region": details.get("region"),
                "installation_id": install.get("installation_id"),
            },
        )
        status_payload = await _source_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            source="aws",
        )
        return {
            "ok": True,
            "source": "aws",
            "trigger_id": str(trigger_id),
            "status": status_payload,
        }

    @router.post("/sources/{source_id}/rehearsal/finalize")
    async def finalize_generic_source_rehearsal(
        request: Request,
        source_id: str,
    ) -> dict[str, Any]:
        source = _normalize_rehearsal_source(source_id)
        if source in _SOURCE_SPECIFIC_FINALIZE_SOURCES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error": "source_specific_finalize_required"},
            )
        if source in _OAUTH_REHEARSAL_SOURCES:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "provider_callback_finalize_required",
                    "source": source,
                    "message": (
                        f"{_source_display_name(source)} must be finalized by "
                        "the provider callback after customer approval."
                    ),
                },
            )
        if source in _GENERIC_FINALIZE_BLOCKED_SOURCES:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail={
                    "error": "source_native_finalize_required",
                    "source": source,
                    "message": (
                        f"{_source_display_name(source)} needs a source-native "
                        "finalizer before the ingestion workers can consume "
                        "the install."
                    ),
                },
            )
        _require_source_rehearsal_enabled(request)
        pool = _pool_from_state(request)
        tenant_id, actor_id = _rehearsal_actor_ids()
        await _ensure_rehearsal_actor(pool, tenant_id=tenant_id, actor_id=actor_id)

        body = await _json_body(request)
        input_values = _source_finalize_inputs(body)
        required_inputs = _SOURCE_REQUIRED_INPUTS.get(source, [])
        missing_inputs = [
            name
            for name in required_inputs
            if not str(input_values.get(name, "")).strip()
        ]
        if missing_inputs:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "source_rehearsal_missing_required_inputs",
                    "source": source,
                    "missing_inputs": missing_inputs,
                },
            )

        secret_store = _secret_store_from_state(request, pool)
        secret_refs: dict[str, str] = {}
        visible_inputs: dict[str, str] = {}
        for name, value in sorted(input_values.items()):
            cleaned = str(value).strip()
            if not cleaned:
                continue
            if _source_input_is_secret(name):
                secret_refs[name] = await secret_store.put(
                    cleaned,
                    label=f"{source}_{name}",
                    tenant_id=tenant_id,
                )
            else:
                visible_inputs[name] = cleaned

        primary_secret_ref = _primary_secret_ref(secret_refs)
        installation_id = _source_installation_id(
            source,
            input_values,
            fallback=body.get("installation_id"),
        )
        trigger_payload = {
            "source": source,
            "authorization_mode": _GENERIC_AUTHORIZATION_MODES.get(
                source,
                "customer_local_provider_refs",
            ),
            "installation_id": installation_id,
            "inputs": visible_inputs,
            "secret_refs": secret_refs,
            "prepared_by": "fyralis_source_rehearsal_ui",
        }

        from lib.shared.errors import InstallationCollisionError
        from lib.shared.provider_installations import (
            upsert_provider_installation_for_tenant,
        )

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    installation_row_id = await upsert_provider_installation_for_tenant(
                        conn,
                        provider=source,
                        tenant_id=tenant_id,
                        installation_id=installation_id,
                        secret_ref=primary_secret_ref,
                    )
                    await conn.execute(
                        """
                        INSERT INTO onboarding_triggers (
                            id, tenant_id, source, trigger_kind,
                            installation_row_id, payload
                        )
                        VALUES ($1, $2, $3, 'install', $4, $5::jsonb)
                        """,
                        uuid7(),
                        tenant_id,
                        source,
                        installation_row_id,
                        json.dumps(trigger_payload),
                    )
                    await _emit_source_connection_proof(
                        conn,
                        tenant_id=tenant_id,
                        actor_id=actor_id,
                        source=source,
                        installation_id=installation_id,
                        visible_inputs=visible_inputs,
                        secret_ref_names=sorted(secret_refs),
                    )
        except InstallationCollisionError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "source_installation_already_bound"},
            ) from exc

        status_payload = await _source_rehearsal_status_payload(
            pool,
            tenant_id=tenant_id,
            source=source,
        )
        return {
            "ok": True,
            "source": source,
            "installation_id": installation_id,
            "stored_secret_ref_count": len(secret_refs),
            "stored_metadata_keys": sorted(visible_inputs),
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
            detail={"error": "onboarding_store_unavailable"},
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


def _bounded_telegram_error_code(exc: Exception) -> str:
    code = str(getattr(exc, "code", "") or "").strip()
    if code in _SAFE_PROVIDER_ERROR_CODES:
        return code
    return "telegram_connect_failed"


async def _prepare_source_rehearsal_response(
    request: Request,
    source: str,
    *,
    request_payload: dict[str, Any] | None = None,
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
    deployment_context = _source_deployment_context(request_payload or {})
    handoff = await _source_provider_handoff(
        source,
        pool=pool,
        tenant_id=tenant_id,
        request=request,
        request_payload=request_payload or {},
    )
    if source == "aws":
        handoff["provider_console_url"] = _aws_source_approval_url(
            deployment_context.get("aws_region")
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
    payload = {
        "enabled": True,
        "source": source,
        "tenant_id": str(tenant_id),
        "actor_id": str(actor_id),
        # Browser polling must use the same trusted public HTTPS origin as
        # provider callbacks. request.base_url can be downgraded to http when
        # the gateway sits behind an untrusted reverse-proxy hop (for example
        # host-side ngrok forwarding into Docker).
        "gateway_api_base": public_url,
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
        "discord_access_mode": handoff.get("discord_access_mode"),
        "discord_permissions": handoff.get("discord_permissions"),
        "provider_console_url": handoff.get("provider_console_url"),
        "authorization_mode": handoff["authorization_mode"],
        "missing_configuration": handoff["missing_configuration"],
        "setup_owner": handoff.get("setup_owner"),
        "deployment_model": handoff.get("deployment_model"),
        "required_inputs": _SOURCE_REQUIRED_INPUTS.get(source, []),
        "optional_inputs": _SOURCE_OPTIONAL_INPUTS.get(source, []),
        "finalize_mode": _source_finalize_mode(source),
        "automation_profile": _source_automation_profile(source),
        "browser_agent": browser_agent_recipe_for_source(source),
        "native_connect": _SOURCE_NATIVE_CONNECT_CONTRACTS.get(source),
        "deployment_context": deployment_context,
        "bearer_token": token,
        "session_expires_at": ctx.expires_at.isoformat(),
        "state_expires_in_seconds": 600 if source in _OAUTH_REHEARSAL_SOURCES else None,
        "status": status_payload,
    }
    payload["browser_agent_run"] = source_browser_agent_run_for_payload(
        source,
        payload,
    )
    return payload


async def _source_provider_handoff(
    source: str,
    *,
    pool: Any,
    tenant_id: UUID,
    request: Request,
    request_payload: dict[str, Any],
) -> dict[str, Any]:
    public_url = _public_url_from_env_or_request(request)
    provider_inputs = (
        _source_finalize_inputs(request_payload) if request_payload else {}
    )
    if source == "slack":
        from services.ingest.integrations.slack import byoc_app
        from services.ingest.integrations.slack import oauth as slack_oauth

        redirect_uri = (
            os.environ.get("SLACK_REDIRECT_URI", "").strip()
            or f"{public_url}/integrations/slack/callback"
        )
        events_request_url = f"{public_url}/webhooks/slack/events"
        config_token = byoc_app.configuration_token_from_inputs(provider_inputs)
        app_refs = None
        if config_token:
            secret_store = _secret_store_from_state(request, pool)
            try:
                created_app = await byoc_app.create_app_from_manifest(
                    configuration_token=config_token,
                    oauth_redirect_url=redirect_uri,
                    events_request_url=events_request_url,
                )
            except byoc_app.SlackManifestCreateError as exc:
                raise HTTPException(
                    status_code=exc.status_code,
                    detail={
                        "error": "slack_manifest_create_failed",
                        "slack_error": exc.slack_error,
                    },
                ) from exc
            app_refs = await byoc_app.store_app_credentials(
                pool=pool,
                secret_store=secret_store,
                tenant_id=tenant_id,
                credentials=created_app,
            )
        if app_refs is None:
            app_refs = await byoc_app.fetch_app_credentials(pool, tenant_id=tenant_id)

        client_id = (
            app_refs.client_id
            if app_refs
            else os.environ.get("SLACK_CLIENT_ID", "").strip()
        )
        env_ready = bool(
            os.environ.get("SLACK_CLIENT_ID", "").strip()
            and _env_or_secret_ref_configured("SLACK_CLIENT_SECRET")
            and _env_or_secret_ref_configured("SLACK_SIGNING_SECRET")
        )
        missing = [] if app_refs or env_ready else ["slack_app_config_token"]
        install_url = None
        if client_id and redirect_uri and not missing:
            state_payload = {"slack_app_id": app_refs.app_id} if app_refs else None
            state_token = await slack_oauth.issue_state_token(
                tenant_id,
                pool,
                extra_payload=state_payload,
            )
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
            or f"{public_url}/integrations/slack/callback",
            "provider_console_url": "https://api.slack.com/apps",
            "missing_configuration": missing,
        }

    if source == "discord":
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
                "DISCORD_APPLICATION_ID": os.environ.get("DISCORD_APPLICATION_ID", ""),
                "DISCORD_BOT_TOKEN": _env_or_secret_ref_configured("DISCORD_BOT_TOKEN"),
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
            "discord_permissions": discord_oauth.discord_permissions_for_access_mode(
                access_mode
            ),
            "oauth_redirect_url": redirect_uri
            or f"{public_url}/integrations/discord/callback",
            "provider_console_url": "https://discord.com/developers/applications",
            "missing_configuration": missing,
        }

    if source == "figma":
        # Figma is deliberately deployment-app OAuth, not a PAT form.  The
        # one-time administrator setup is reported as a single safe category;
        # ordinary users must never see configuration gaps or credential refs.
        from services.ingest.integrations.figma import oauth as figma_oauth

        configured_redirect = ""
        try:
            configured_redirect = figma_oauth._figma_redirect_uri()  # noqa: SLF001
        except figma_oauth.FigmaOAuthError:
            pass
        ready = figma_oauth._deployment_oauth_ready()  # noqa: SLF001
        return {
            "authorization_mode": "oauth",
            # OAuth state must be created through POST /oauth/start with the
            # selected file URLs, so there is no generic install URL to open.
            "install_url": None,
            "oauth_redirect_url": configured_redirect
            or f"{public_url}{_SOURCE_CALLBACK_PATHS['figma']}",
            "provider_console_url": "https://www.figma.com/developers/apps",
            "missing_configuration": ([] if ready else ["deployment_figma_oauth_app"]),
            "setup_owner": "deployment_admin",
            "deployment_model": "customer_owned_byoc_oauth_app",
        }

    if source == "github":
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
                "GITHUB_APP_ID": bool(os.environ.get("GITHUB_APP_ID", "").strip()),
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
                "NOTION_CLIENT_SECRET": _env_or_secret_ref_configured(
                    "NOTION_CLIENT_SECRET"
                ),
            }.items()
            if not value
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
            "oauth_redirect_url": redirect_uri
            or f"{public_url}/integrations/notion/callback",
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

    if source in _ALL_REHEARSAL_SOURCES:
        callback_path = _SOURCE_CALLBACK_PATHS.get(source)
        return {
            "authorization_mode": _GENERIC_AUTHORIZATION_MODES.get(
                source,
                "customer_local_provider_refs",
            ),
            "install_url": None,
            "oauth_redirect_url": (
                f"{public_url}{callback_path}" if callback_path else None
            ),
            "provider_console_url": _GENERIC_PROVIDER_CONSOLES.get(
                source,
                "Customer provider admin console",
            ),
            "missing_configuration": [],
        }

    raise AssertionError(f"unsupported rehearsal source {source!r}")


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "invalid_json_body"},
        ) from exc
    if not isinstance(body, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "json_object_required"},
        )
    return body


async def _optional_json_body(request: Request) -> dict[str, Any]:
    raw_body = await request.body()
    if not raw_body or not raw_body.strip():
        return {}
    try:
        body = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "invalid_json_body"},
        ) from exc
    if not isinstance(body, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "json_object_required"},
        )
    return body


def _source_deployment_context(body: dict[str, Any]) -> dict[str, str]:
    raw = body.get("deployment_context") or body.get("deploymentContext") or {}
    if not isinstance(raw, dict):
        raw = {}
    context: dict[str, str] = {}
    region = _bounded_context_value(raw.get("aws_region") or raw.get("region"))
    if region:
        context["aws_region"] = region
    assuming_principal_arn = _known_aws_source_runtime_role_arn(raw)
    if assuming_principal_arn:
        context["aws_assuming_principal_arn"] = assuming_principal_arn
        context["setup_role_arn"] = assuming_principal_arn
        context["source_runtime_role_source"] = _aws_runtime_role_source(raw)
    return context


def _aws_source_approval_url(region: str | None) -> str:
    clean_region = _bounded_context_value(region)
    if clean_region:
        return (
            f"https://{clean_region}.console.aws.amazon.com/cloudformation/home"
            f"?region={clean_region}#/stacks/create/template"
        )
    return _GENERIC_PROVIDER_CONSOLES["aws"]


def _materialize_aws_source_runtime_bootstrap() -> Path:
    path = Path(_AWS_SOURCE_RUNTIME_TEMPLATE_PATH)
    _write_json_file(path, _aws_source_runtime_role_cloudformation_template())
    return path


def _aws_source_runtime_role_cloudformation_template() -> dict[str, Any]:
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": (
            "Fyralis BYOC source runtime identity bootstrap. Creates the IAM "
            "role that AWS source roles trust; no data-plane resources are created."
        ),
        "Parameters": {
            "RuntimeRoleName": {
                "Type": "String",
                "Default": "fyralis-source-runtime",
                "Description": "Name for the Fyralis source runtime IAM role.",
            },
            "SourceReadRoleName": {
                "Type": "String",
                "Default": "fyralis-source-readonly",
                "Description": (
                    "Name of the AWS source read role this runtime is allowed to assume."
                ),
            },
            "PermissionsBoundaryArn": {
                "Type": "String",
                "Default": "",
                "Description": "Optional customer-owned IAM permissions boundary ARN.",
            },
        },
        "Conditions": {
            "HasPermissionsBoundary": {
                "Fn::Not": [{"Fn::Equals": [{"Ref": "PermissionsBoundaryArn"}, ""]}]
            }
        },
        "Resources": {
            "FyralisSourceRuntimeRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": {"Ref": "RuntimeRoleName"},
                    "PermissionsBoundary": {
                        "Fn::If": [
                            "HasPermissionsBoundary",
                            {"Ref": "PermissionsBoundaryArn"},
                            {"Ref": "AWS::NoValue"},
                        ]
                    },
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {
                                    "Service": [
                                        "ec2.amazonaws.com",
                                        "pods.eks.amazonaws.com",
                                    ]
                                },
                                "Action": "sts:AssumeRole",
                            }
                        ],
                    },
                    "Policies": [
                        {
                            "PolicyName": "fyralis-source-runtime-assume-source",
                            "PolicyDocument": {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Sid": "AssumeFyralisSourceReadRole",
                                        "Effect": "Allow",
                                        "Action": "sts:AssumeRole",
                                        "Resource": {
                                            "Fn::Sub": (
                                                "arn:${AWS::Partition}:iam::"
                                                "${AWS::AccountId}:role/"
                                                "${SourceReadRoleName}"
                                            )
                                        },
                                    }
                                ],
                            },
                        }
                    ],
                    "Tags": [
                        {"Key": "fyralis:managed", "Value": "true"},
                        {"Key": "fyralis:purpose", "Value": "source-runtime"},
                    ],
                },
            }
        },
        "Outputs": {
            "SourceRuntimeRoleArn": {
                "Description": (
                    "Role ARN Fyralis AWS source stacks use as "
                    "FyralisAssumingPrincipalArn."
                ),
                "Value": {"Fn::GetAtt": ["FyralisSourceRuntimeRole", "Arn"]},
            }
        },
    }


def _bounded_context_value(value: Any) -> str:
    return str(value or "").strip()[:300]


def _known_aws_source_runtime_role_arn(raw_context: dict[str, Any]) -> str | None:
    candidate = _aws_runtime_role_from_mapping(raw_context)
    if candidate:
        return candidate
    for env_key in _AWS_RUNTIME_ROLE_ENV_KEYS:
        candidate = _valid_aws_role_arn(os.environ.get(env_key))
        if candidate:
            return candidate
    for path in _aws_runtime_role_context_paths():
        candidate = _aws_runtime_role_from_json_file(path)
        if candidate:
            return candidate
    return None


def _aws_runtime_role_source(raw_context: dict[str, Any]) -> str:
    if _aws_runtime_role_from_mapping(raw_context):
        return "request_deployment_context"
    for env_key in _AWS_RUNTIME_ROLE_ENV_KEYS:
        if _valid_aws_role_arn(os.environ.get(env_key)):
            return f"env:{env_key}"
    for path in _aws_runtime_role_context_paths():
        if _aws_runtime_role_from_json_file(path):
            return str(path)
    return "missing"


def _aws_runtime_role_context_paths() -> list[Path]:
    paths = [
        ".fyralis/deployment-context.json",
        ".fyralis/byoc-deployment-context.json",
        ".fyralis/local-rehearsal/provider/aws-cloudformation/provider-executor-report.json",
    ]
    configured = os.environ.get("FYRALIS_BYOC_DEPLOYMENT_CONTEXT_PATH", "").strip()
    if configured:
        paths.insert(0, configured)
    return [Path(path) for path in paths]


def _aws_runtime_role_from_json_file(path: Path) -> str | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    return _aws_runtime_role_from_mapping(payload)


def _aws_runtime_role_from_mapping(payload: dict[str, Any]) -> str | None:
    for key in _AWS_RUNTIME_ROLE_CONTEXT_KEYS:
        candidate = _valid_aws_role_arn(payload.get(key))
        if candidate:
            return candidate
    for key in ("deployment_outputs", "outputs", "stack_outputs"):
        candidate = _aws_runtime_role_from_outputs(payload.get(key))
        if candidate:
            return candidate
    for key in ("aws", "byoc", "deployment", "runtime"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidate = _aws_runtime_role_from_mapping(nested)
            if candidate:
                return candidate
    return None


def _aws_runtime_role_from_outputs(outputs: Any) -> str | None:
    if isinstance(outputs, dict):
        for key in _AWS_RUNTIME_ROLE_OUTPUT_KEYS:
            candidate = _valid_aws_role_arn(outputs.get(key))
            if candidate:
                return candidate
        for value in outputs.values():
            if isinstance(value, dict):
                candidate = _aws_runtime_role_from_outputs(value)
                if candidate:
                    return candidate
        return None
    if isinstance(outputs, list):
        for item in outputs:
            if not isinstance(item, dict):
                continue
            output_key = str(item.get("OutputKey") or item.get("key") or "").strip()
            if output_key not in _AWS_RUNTIME_ROLE_OUTPUT_KEYS:
                continue
            candidate = _valid_aws_role_arn(
                item.get("OutputValue") or item.get("value")
            )
            if candidate:
                return candidate
    return None


def _valid_aws_role_arn(value: Any) -> str | None:
    candidate = _bounded_context_value(value)
    if not candidate.startswith("arn:aws:iam::"):
        return None
    parts = candidate.split(":", 5)
    if len(parts) < 6 or not parts[4].isdigit() or len(parts[4]) != 12:
        return None
    if parts[4] == "123456789012":
        return None
    if not parts[5].startswith("role/"):
        return None
    return candidate


def _aws_account_id_from_role_arn(role_arn: str) -> str | None:
    candidate = _valid_aws_role_arn(role_arn)
    if not candidate:
        return None
    parts = candidate.split(":", 5)
    return parts[4] if len(parts) >= 5 else None


def _aws_source_external_id(value: Any) -> str | None:
    direct = _bounded_context_value(value)
    if direct:
        return direct
    try:
        from_file = (
            Path(_AWS_SOURCE_EXTERNAL_ID_PATH).read_text(encoding="utf-8").strip()
        )
    except OSError:
        return None
    return from_file[:300] if from_file else None


def _source_finalize_inputs(body: dict[str, Any]) -> dict[str, str]:
    raw_inputs = body.get("inputs", body)
    if not isinstance(raw_inputs, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "source_inputs_must_be_object"},
        )
    inputs: dict[str, str] = {}
    for key, value in raw_inputs.items():
        if value is None:
            continue
        name = str(key).strip()
        if not name:
            continue
        if isinstance(value, (dict, list)):
            cleaned = json.dumps(value, sort_keys=True)
        else:
            cleaned = str(value).strip()
        if cleaned:
            inputs[name] = cleaned
    return inputs


def _source_input_is_secret(name: str) -> bool:
    lowered = name.strip().lower()
    if lowered.endswith("_ref") or lowered in {"role_arn", "region"}:
        return False
    secret_terms = (
        "token",
        "secret",
        "hash",
        "session",
        "password",
        "private_key",
        "credential",
        "oauth_client",
        "api_key",
    )
    return any(term in lowered for term in secret_terms)


def _primary_secret_ref(secret_refs: dict[str, str]) -> str | None:
    priority_terms = (
        "webhook_secret",
        "signing_secret",
        "verify_token",
        "api_token",
        "service_user_token",
        "service_account_token",
        "access_token",
        "bot_token",
        "linked_device_session",
        "session",
        "oauth_client",
        "api_hash",
    )
    for term in priority_terms:
        for name, ref in secret_refs.items():
            if term in name:
                return ref
    return next(iter(secret_refs.values()), None)


def _source_installation_id(
    source: str,
    inputs: dict[str, str],
    *,
    fallback: Any = None,
) -> str:
    candidate_fields = (
        "installation_id",
        "company_id",
        "organization_id",
        "organization_urn",
        "org_id",
        "workspace_id",
        "business_id",
        "business_account_id",
        "realm_id",
        "account_id",
        "account_label",
        "phone_number_id",
        "team_id",
        "firm_id",
        "instance_url",
        "base_url",
        "site_url",
        "mailbox_scope",
        "calendar_scope",
        "drive_scope",
    )
    for field in candidate_fields:
        value = str(inputs.get(field, "")).strip()
        if value:
            return value[:240]
    fallback_value = str(fallback or "").strip()
    if fallback_value:
        return fallback_value[:240]
    return f"{source}:customer-local-refs"


async def _emit_source_connection_proof(
    conn: Any,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    source: str,
    installation_id: str,
    visible_inputs: dict[str, str],
    secret_ref_names: list[str],
) -> None:
    source_name = _source_display_name(source)
    content = {
        "source": source,
        "installation_id": installation_id,
        "proof_type": "source_connection_rehearsal",
        "visible_input_keys": sorted(visible_inputs),
        "secret_ref_names": secret_ref_names,
        "raw_secret_values_included": False,
        "next_stage": "historical_backfill_or_live_signal_worker",
    }
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, actor_id, occurred_at, kind, source_channel,
            source_actor_ref, content, content_text, trust_tier, external_id
        ) VALUES (
            $1, $2, $3, now(), 'source_connection',
            $4, $5, $6::jsonb, $7, 'system', $8
        )
        """,
        uuid7(),
        tenant_id,
        actor_id,
        f"{source}:connection",
        "fyralis:onboarding",
        json.dumps(content, sort_keys=True),
        (
            f"{source_name} connection finalized in the customer-cloud "
            "onboarding flow. Sanitized proof only; raw secrets were not stored "
            "in this observation."
        ),
        f"source-connection:{source}:{installation_id}",
    )


def _source_finalize_mode(source: str) -> str:
    if source in _OAUTH_REHEARSAL_SOURCES:
        return "provider_callback"
    if source in _SOURCE_SPECIFIC_FINALIZE_SOURCES:
        return "source_specific"
    if source in _GENERIC_FINALIZE_BLOCKED_SOURCES:
        return "native_finalizer_required"
    return "generic_customer_refs"


def _source_automation_profile(source: str) -> dict[str, Any]:
    method = _SOURCE_METHODS.get(source, "api_token")
    source_name = _source_display_name(source)
    human_steps = _source_human_steps(source, method)
    required_inputs = list(_SOURCE_REQUIRED_INPUTS.get(source, []))
    optional_inputs = list(_SOURCE_OPTIONAL_INPUTS.get(source, []))
    return {
        "automation_level": _source_automation_level(method),
        "method": method,
        "minimum_human_inputs": required_inputs,
        "optional_hints": optional_inputs,
        "automated_actions": _source_automated_actions(source, method),
        "human_steps": human_steps,
        "agent_discovery_target": _SOURCE_DISCOVERY_TARGETS.get(
            source,
            f"{source_name} approved workspace and source scope",
        ),
        "post_connect_actions": [
            "store encrypted customer-cloud refs",
            "register source installation metadata",
            "emit onboarding trigger",
            "write sanitized connection-proof observation",
            "poll for historical backfill and live observations",
        ],
        "human_step_count": len(human_steps),
    }


def _source_auto_connect_state(source: str, payload: dict[str, Any]) -> dict[str, Any]:
    source_name = _source_display_name(source)
    status_payload = payload.get("status") or {}
    automation_profile = payload.get("automation_profile") or {}
    browser_agent = payload.get("browser_agent") or browser_agent_recipe_for_source(
        source
    )
    browser_agent_run = payload.get(
        "browser_agent_run"
    ) or source_browser_agent_run_for_payload(
        source,
        payload,
    )
    missing_configuration = list(payload.get("missing_configuration") or [])
    human_steps = list(automation_profile.get("human_steps") or [])
    human_step_count = int(
        automation_profile.get("human_step_count") or len(human_steps)
    )
    installed = bool(status_payload.get("installed"))
    observation_count = int(status_payload.get("observation_count") or 0)
    install_url = payload.get("install_url")
    finalize_mode = payload.get("finalize_mode")

    if installed:
        return {
            "state": "connected",
            "label": "Connected",
            "message": (
                f"{source_name} is connected; {observation_count} sanitized "
                f"observation{' has' if observation_count == 1 else 's have'} landed."
                if observation_count
                else f"{source_name} install exists; Fyralis is waiting for first sync proof."
            ),
            "human_step_count": 0,
            "human_steps": [],
            "automated_actions": automation_profile.get("automated_actions") or [],
            "browser_agent": browser_agent,
            "browser_agent_run": browser_agent_run,
            "install_url": install_url,
        }

    if source == "aws" and not _aws_source_runtime_role_from_payload(payload):
        bootstrap_template_path = _materialize_aws_source_runtime_bootstrap()
        bootstrap_url = _aws_source_approval_url(
            (payload.get("deployment_context") or {}).get("aws_region")
            if isinstance(payload.get("deployment_context"), dict)
            else None
        )
        return {
            "state": "blocked",
            "label": "BYOC runtime missing",
            "message": (
                "Create the Fyralis BYOC source runtime role first. Use the "
                f"generated CloudFormation template at {bootstrap_template_path}; "
                "its SourceRuntimeRoleArn output becomes the trusted runtime "
                "principal for the AWS source role."
            ),
            "human_step_count": 1,
            "human_steps": [
                {
                    "id": "deploy_fyralis_byoc_runtime",
                    "label": "Create Fyralis BYOC source runtime role.",
                    "reason": (
                        "AWS source connection needs SourceRuntimeRoleArn before "
                        "the read-only source role can be created."
                    ),
                    "can_agent_complete": False,
                }
            ],
            "automated_actions": automation_profile.get("automated_actions") or [],
            "browser_agent": browser_agent,
            "browser_agent_run": browser_agent_run,
            "install_url": bootstrap_url,
        }

    if missing_configuration:
        if _source_missing_configuration_agent_assisted(source, missing_configuration):
            assisted_steps = _source_agent_assisted_missing_configuration_steps(
                source,
                source_name,
                human_steps,
            )
            if _source_auto_connect_runner_mode() == "artifact_materialization":
                return {
                    "state": "blocked",
                    "label": "Not connected",
                    "message": (
                        "Slack was not connected. App creation needs the "
                        "customer-cloud browser agent to run, or a Slack app "
                        "configuration token must be provided as fallback."
                    ),
                    "human_step_count": len(assisted_steps),
                    "human_steps": assisted_steps,
                    "automated_actions": automation_profile.get("automated_actions")
                    or [],
                    "browser_agent": browser_agent,
                    "browser_agent_run": browser_agent_run,
                    "install_url": install_url,
                }
            return {
                "state": "admin_gate",
                "label": "Admin assist",
                "message": (
                    "Fyralis can open the Slack app dashboard, generate the app "
                    "configuration token in an admin-present browser, create the "
                    "BYOC Slack app, then ask for OAuth approval."
                ),
                "human_step_count": len(assisted_steps),
                "human_steps": assisted_steps,
                "automated_actions": automation_profile.get("automated_actions") or [],
                "browser_agent": browser_agent,
                "browser_agent_run": browser_agent_run,
                "install_url": install_url,
            }
        return {
            "state": "blocked",
            "label": "Needs config",
            "message": (
                "Add "
                + ", ".join(missing_configuration)
                + " in the customer-cloud runtime, then connect again."
            ),
            "human_step_count": len(missing_configuration),
            "human_steps": [
                {
                    "id": f"configure_{name.lower()}",
                    "label": f"Configure {name}.",
                    "reason": "The customer-cloud gateway needs this runtime value.",
                    "can_agent_complete": False,
                }
                for name in missing_configuration
            ],
            "automated_actions": automation_profile.get("automated_actions") or [],
            "browser_agent": browser_agent,
            "browser_agent_run": browser_agent_run,
            "install_url": install_url,
        }

    if install_url or finalize_mode == "provider_callback":
        return {
            "state": "admin_gate",
            "label": "Admin gate",
            "message": (
                f"{source_name} approval is ready. Fyralis opened the provider "
                "handoff and will poll in the background after approval."
            ),
            "human_step_count": max(1, human_step_count),
            "human_steps": human_steps,
            "automated_actions": automation_profile.get("automated_actions") or [],
            "browser_agent": browser_agent,
            "browser_agent_run": browser_agent_run,
            "install_url": install_url,
        }

    if (
        finalize_mode in {"source_specific", "native_finalizer_required"}
        or human_step_count
    ):
        return {
            "state": "admin_gate",
            "label": "Admin gate",
            "message": (
                f"Fyralis prepared {source_name}. Only provider-required "
                "approval or credential creation remains."
            ),
            "human_step_count": max(1, human_step_count),
            "human_steps": human_steps,
            "automated_actions": automation_profile.get("automated_actions") or [],
            "browser_agent": browser_agent,
            "browser_agent_run": browser_agent_run,
            "install_url": install_url,
        }

    return {
        "state": "running",
        "label": "Running",
        "message": (
            f"Fyralis prepared {source_name} and is polling for install proof."
        ),
        "human_step_count": human_step_count,
        "human_steps": human_steps,
        "automated_actions": automation_profile.get("automated_actions") or [],
        "browser_agent": browser_agent,
        "browser_agent_run": browser_agent_run,
        "install_url": install_url,
    }


def _aws_source_runtime_role_from_payload(payload: dict[str, Any]) -> str | None:
    deployment_context = payload.get("deployment_context")
    if not isinstance(deployment_context, dict):
        return None
    return _valid_aws_role_arn(
        deployment_context.get("aws_assuming_principal_arn")
        or deployment_context.get("setup_role_arn")
    )


def _source_missing_configuration_agent_assisted(
    source: str,
    missing_configuration: list[str],
) -> bool:
    normalized_missing = {
        str(name).strip().lower() for name in missing_configuration if str(name).strip()
    }
    return source == "slack" and normalized_missing == {"slack_app_config_token"}


def _source_agent_assisted_missing_configuration_steps(
    source: str,
    source_name: str,
    fallback_steps: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if source != "slack":
        return fallback_steps
    return [
        {
            "id": "generate_slack_app_config_token",
            "label": "Generate a Slack app configuration token in the admin browser.",
            "reason": (
                "Slack requires an authenticated workspace admin session before "
                "a configuration token can be created."
            ),
            "can_agent_complete": True,
        },
        {
            "id": "provider_admin_approval",
            "label": f"Approve the {source_name} OAuth scopes.",
            "reason": "Slack requires workspace admin consent for the created app.",
            "can_agent_complete": False,
        },
    ]


def _source_auto_connect_run_descriptor(
    source: str,
    payload: dict[str, Any],
    browser_agent_run: dict[str, Any],
) -> dict[str, Any]:
    source_cli = source.replace("_", "-")
    native_connect = payload.get("native_connect")
    missing_configuration = list(payload.get("missing_configuration") or [])
    include_native_execution = bool(native_connect) and not (
        _source_missing_configuration_agent_assisted(source, missing_configuration)
    )
    gateway_api_base = str(payload.get("gateway_api_base") or "").strip()
    command_args = [
        "fyralis",
        "byoc",
        "source",
        "browser-agent",
        "--workdir",
        str(_source_auto_connect_workdir()),
        "--source",
        source_cli,
        "--execute-browser-dom",
        "--interactive-admin",
    ]
    if gateway_api_base:
        command_args.extend(["--gateway-api-base", gateway_api_base])
    if include_native_execution:
        command_args.append("--execute-native")
    action_queue = [
        item
        for item in browser_agent_run.get("action_queue") or []
        if isinstance(item, dict)
    ]
    provider_admin_actions = [
        item for item in action_queue if item.get("owner") == "provider_admin"
    ]
    agent_actions = [
        item for item in action_queue if item.get("owner") == "fyralis_agent"
    ]
    return {
        "schema_version": "fyralis.byoc.source.auto_connect_run.v1",
        "source": source,
        "status": browser_agent_run.get("state") or "running",
        "launch_mode": browser_agent_run.get("launch_mode")
        or "customer_cloud_admin_present_browser",
        "can_start": bool(browser_agent_run.get("can_start", True)),
        "handoff_url": browser_agent_run.get("handoff_url"),
        "current_action_id": (
            (browser_agent_run.get("current_action") or {}).get("id")
            if isinstance(browser_agent_run.get("current_action"), dict)
            else None
        ),
        "automated_action_count": len(agent_actions),
        "human_action_count": len(provider_admin_actions),
        "native_connect_kind": (
            native_connect.get("kind") if isinstance(native_connect, dict) else None
        ),
        "native_payload_template_path_hint": (
            f".fyralis/sources/{source_cli}/browser-agent-provider-setup/"
            "native-payload-template.json"
            if native_connect
            else None
        ),
        "provider_setup_output_dir_hint": (
            f".fyralis/sources/{source_cli}/browser-agent-provider-setup"
        ),
        "receipt_path_hint": f".fyralis/sources/{source_cli}/browser-agent-receipt.json",
        "command_preview": " ".join(command_args),
        "command_args": command_args,
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_auto_connect_run_metadata_only",
    }


def _source_auto_connect_workdir() -> Path:
    return Path(os.environ.get("FYRALIS_SOURCE_AUTO_CONNECT_WORKDIR") or ".fyralis")


def _source_auto_connect_run_key(source: str) -> str:
    return source.replace("-", "_")


def _source_auto_connect_run_store(request: Request) -> dict[str, dict[str, Any]]:
    existing = getattr(request.app.state, "source_auto_connect_runs", None)
    if isinstance(existing, dict):
        return existing
    created: dict[str, dict[str, Any]] = {}
    request.app.state.source_auto_connect_runs = created
    return created


def _source_auto_connect_source_dir(source: str) -> Path:
    return _source_auto_connect_workdir() / "sources" / source.replace("_", "-")


def _materialize_source_auto_connect_run(
    source: str,
    browser_agent_run: dict[str, Any],
    descriptor: dict[str, Any],
) -> dict[str, Any]:
    generated_at = datetime.now(UTC).isoformat()
    source_dir = _source_auto_connect_source_dir(source)
    run_artifact_path = source_dir / "connection.json"
    receipt_path = source_dir / "browser-agent-receipt.json"
    run_record = {
        **descriptor,
        "background_status": "queued",
        "background_queued_at": generated_at,
        "background_runner_mode": _source_auto_connect_runner_mode(),
        "run_artifact_path_hint": str(run_artifact_path),
        "receipt_path_hint": str(receipt_path),
    }
    payload = {
        "schema_version": "fyralis.byoc.source.connection_artifact.v1",
        "source": source,
        "generated_at": generated_at,
        "auto_connect_run": dict(run_record),
        "browser_agent_run": browser_agent_run,
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_source_auto_connect_artifact_only",
    }
    _write_json_file(run_artifact_path, payload)
    return run_record


def _source_auto_connect_persisted_run_record(source: str) -> dict[str, Any] | None:
    source_dir = _source_auto_connect_source_dir(source)
    run_artifact_path = source_dir / "connection.json"
    if not run_artifact_path.is_file():
        return None
    try:
        artifact = json.loads(run_artifact_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    descriptor = artifact.get("auto_connect_run")
    if not isinstance(descriptor, dict):
        descriptor = {}
    record = dict(descriptor)
    record["run_artifact_path_hint"] = str(run_artifact_path)
    receipt_path = Path(
        str(
            record.get("receipt_path_hint") or source_dir / "browser-agent-receipt.json"
        )
    )
    record["receipt_path_hint"] = str(receipt_path)
    record.setdefault("background_queued_at", artifact.get("generated_at"))
    if receipt_path.is_file():
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            receipt = {}
        receipt_status = str(receipt.get("status") or "").strip()
        if receipt_status:
            record["background_status"] = receipt_status
            record["status"] = receipt_status
        receipt_generated_at = receipt.get("generated_at")
        if isinstance(receipt_generated_at, str):
            record["background_finished_at"] = receipt_generated_at
    else:
        record.setdefault("background_status", "queued")
    record.setdefault("background_runner_mode", _source_auto_connect_runner_mode())
    return record


async def _execute_source_auto_connect_background_run(
    source: str,
    run_artifact_path: Path,
    receipt_path: Path,
    gateway_api_base: str,
    store: dict[str, dict[str, Any]],
    store_key: str,
) -> None:
    record = store.setdefault(store_key, {})
    record.update(
        {
            "background_status": "running",
            "background_started_at": datetime.now(UTC).isoformat(),
        }
    )
    try:
        receipt = await run_source_browser_agent(
            SourceBrowserAgentRunnerInputs(
                run_path=run_artifact_path,
                gateway_api_base=gateway_api_base,
                execute_native=_truthy(
                    os.environ.get("FYRALIS_SOURCE_AUTO_CONNECT_EXECUTE_NATIVE")
                ),
                open_browser=_truthy(
                    os.environ.get("FYRALIS_SOURCE_AUTO_CONNECT_OPEN_BROWSER")
                ),
                execute_browser_dom=_truthy(
                    os.environ.get("FYRALIS_SOURCE_AUTO_CONNECT_EXECUTE_BROWSER_DOM")
                ),
                browser_headless=_truthy(
                    os.environ.get("FYRALIS_SOURCE_AUTO_CONNECT_HEADLESS_BROWSER")
                ),
                interactive_admin=_truthy(
                    os.environ.get("FYRALIS_SOURCE_AUTO_CONNECT_INTERACTIVE_ADMIN")
                ),
            )
        )
        receipt_payload = receipt.as_json()
        _write_json_file(receipt_path, receipt_payload)
        record.update(
            {
                "background_status": receipt.status,
                "background_finished_at": datetime.now(UTC).isoformat(),
                "receipt_path_hint": str(receipt_path),
            }
        )
    except Exception as exc:  # noqa: BLE001 - background receipt must be bounded
        failed_payload = {
            "schema_version": "fyralis.byoc.source.browser_agent_runner_receipt.v1",
            "source": source,
            "generated_at": datetime.now(UTC).isoformat(),
            "status": "failed",
            "run_state": "failed",
            "handoff_url": None,
            "handoff_opened": False,
            "native_connect_kind": None,
            "automated_action_count": 0,
            "human_action_count": 0,
            "completed_action_count": 0,
            "waiting_action_count": 0,
            "generated_artifacts": {},
            "action_results": [
                {
                    "id": "source_auto_connect_background_run",
                    "owner": "fyralis_agent",
                    "status": "failed",
                    "detail": f"Background source auto-connect failed: {type(exc).__name__}",
                    "endpoint": str(run_artifact_path),
                    "http_status": None,
                }
            ],
            "raw_secret_values_included": False,
            "raw_payloads_exported": False,
            "stored_scope": "sanitized_browser_agent_runner_metadata_only",
        }
        _write_json_file(receipt_path, failed_payload)
        record.update(
            {
                "background_status": "failed",
                "background_finished_at": datetime.now(UTC).isoformat(),
                "receipt_path_hint": str(receipt_path),
            }
        )


def _source_auto_connect_runner_mode() -> str:
    if _truthy(os.environ.get("FYRALIS_SOURCE_AUTO_CONNECT_EXECUTE_BROWSER_DOM")):
        return "admin_present_browser_dom"
    return "artifact_materialization"


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _source_automation_level(method: str) -> str:
    if method in {"api_token", "oauth_client_credentials", "iam_role", "poll"}:
        return "fully_automated_after_customer_ref"
    if method == "dwd":
        return "automated_after_workspace_dwd_authorization"
    if method in {"oauth", "oauth_plus_gateway", "webhook"}:
        return "automated_after_provider_authorization"
    if method == "gateway":
        return "automated_after_local_session_authorization"
    return "automated_after_customer_authorization"


def _source_automated_actions(source: str, method: str) -> list[str]:
    actions = [
        "prepare provider handoff and gateway routes",
        "validate required customer-owned refs are present",
        f"discover {_SOURCE_DISCOVERY_TARGETS.get(source, 'approved source scope')}",
        "generate least-privilege connection contract",
        "create encrypted secret refs in the customer cloud",
        "register install metadata and source trigger",
    ]
    if source == "figma":
        actions.insert(1, "prepare deployment-owned Figma OAuth app contract")
        actions.insert(
            2,
            "prepare file-scoped Figma OAuth start, status, retry, and disconnect flow",
        )
    elif method == "oauth":
        actions.insert(1, "mint OAuth state and open provider approval")
    elif method == "oauth_plus_gateway":
        actions.insert(1, "mint OAuth state and open provider approval")
        actions.insert(2, "prepare local gateway runner contract")
    elif method == "dwd":
        actions.insert(
            1, "prepare Google Workspace DWD preflight and finalize contract"
        )
        actions.insert(2, "open Workspace Admin DWD authorization target")
    elif method == "oauth_client_credentials":
        actions.insert(1, "prepare OAuth client-credentials or access-token contract")
    elif method == "webhook":
        actions.insert(1, "prepare webhook verifier and callback route")
    elif method == "gateway":
        actions.insert(1, "prepare local session/gateway runner contract")
    elif method == "iam_role":
        actions.insert(1, "validate role ARN shape and generate read-only policy")
    elif method == "poll":
        actions.insert(1, "prepare local polling schedule and rate-limit guard")
    return actions


def _source_human_steps(source: str, method: str) -> list[dict[str, Any]]:
    source_name = _source_display_name(source)
    if source == "figma":
        steps = [
            (
                "configure_deployment_figma_oauth_app",
                "Create or update the private Figma OAuth app owned by this BYOC deployment.",
                "This is a one-time deployment administrator setup; no individual user creates a Figma token.",
            ),
            (
                "store_deployment_figma_oauth_secret",
                "Store the Figma app Client Secret in the deployment secret manager.",
                "The client secret must remain in the customer cloud and is never entered in the onboarding UI.",
            ),
            (
                "approve_file_scoped_figma_oauth",
                "Select Figma file URLs in Fyralis and approve Figma OAuth consent.",
                "Each user connection is limited to the explicitly selected design files.",
            ),
        ]
    elif method == "oauth":
        steps = [
            (
                "provider_admin_approval",
                f"Approve the {source_name} app or provide a preauthorized token ref.",
                "Providers require a workspace/org admin consent screen.",
            )
        ]
    elif method == "oauth_plus_gateway":
        steps = [
            (
                "provider_admin_approval",
                f"Approve the {source_name} app or provide a preauthorized token ref.",
                "Providers require a workspace/org admin consent screen.",
            ),
            (
                "authorize_local_gateway",
                f"Authorize the local {source_name} gateway or bot session.",
                "Gateway-backed providers need a customer-approved runtime to receive events.",
            ),
        ]
    elif method == "api_token":
        steps = [
            (
                "create_provider_token",
                f"Create a least-privilege {source_name} token or service user.",
                "Fyralis cannot mint provider-owned credentials without customer approval.",
            )
        ]
    elif method == "oauth_client_credentials":
        steps = [
            (
                "create_provider_client_credentials",
                f"Create a least-privilege {source_name} OAuth client credential or access token.",
                "The provider requires customer-approved credential material before Fyralis can verify access.",
            )
        ]
    elif method == "dwd":
        steps = [
            (
                "authorize_workspace_dwd",
                f"Approve {source_name} Domain-Wide Delegation scopes in Google Workspace.",
                "Google Workspace requires an admin to authorize the service account client ID and scopes.",
            ),
            (
                "approve_workspace_scope",
                f"Confirm {source_name} workspace domain and inclusion scope.",
                "The admin owns which users, calendars, drives, or org units are in scope.",
            ),
        ]
    elif method == "webhook":
        steps = [
            (
                "approve_webhook_app",
                f"Approve the {source_name} webhook app and verify token.",
                "The provider must accept the customer-cloud callback endpoint.",
            )
        ]
    elif method == "gateway":
        steps = [
            (
                "authorize_local_session",
                f"Authorize the local {source_name} gateway session.",
                "Linked-device and MTProto sessions require a human login or device approval.",
            )
        ]
    elif method == "iam_role":
        steps = [
            (
                "approve_iam_role",
                f"Approve the read-only {source_name} role ref.",
                "Cloud access must be granted by the customer account owner.",
            )
        ]
    elif method == "poll":
        steps = [
            (
                "approve_polling_scope",
                f"Approve {source_name} polling scope and rate-limit posture.",
                "Some providers do not expose a webhook/OAuth callback for fully silent setup.",
            )
        ]
    else:
        steps = [
            (
                "approve_source_connection",
                f"Approve the {source_name} connection.",
                "The source owner must approve the boundary and scope.",
            )
        ]
    if _SOURCE_LIVE_INGRESS_PATHS.get(source, "").startswith("/"):
        steps.append(
            (
                "provider_webhook_enablement",
                f"Provider admin approval is required to enable {source_name} events to the customer-cloud ingress when live sync is needed.",
                "Providers only deliver live events after an admin-approved callback/webhook target is configured.",
            )
        )
    return [
        {
            "id": step_id,
            "label": label,
            "reason": reason,
            "can_agent_complete": False,
        }
        for step_id, label, reason in steps
    ]


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


def _env_or_secret_ref_configured(name: str) -> bool:
    return bool(
        os.environ.get(name, "").strip()
        or os.environ.get(f"{name}_SECRET_REF", "").strip()
    )


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
            await conn.execute(
                """
                INSERT INTO actor_roles (
                    tenant_id, actor_id, entity_type, entity_id, role,
                    granted_by, granted_at, revoked_at
                )
                VALUES ($1, $2, 'tenant', NULL, 'admin', $2, now(), NULL)
                ON CONFLICT ON CONSTRAINT actor_roles_dedup
                DO NOTHING
                """,
                tenant_id,
                actor_id,
            )


async def _source_rehearsal_status_payload(
    pool: Any,
    *,
    tenant_id: UUID,
    source: str,
    bearer_token: str | None = None,
    session_expires_at: str | None = None,
) -> dict[str, Any]:
    installations = await _source_installation_rows(
        pool,
        tenant_id=tenant_id,
        source=source,
    )
    install = installations[0] if installations else None
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
    sync_run = await pool.fetchrow(
        """
        SELECT min(coalesce(started_at, created_at)) AS sync_started_at
          FROM source_onboarding_runs
         WHERE tenant_id = $1
           AND source = $2
           AND status IN ('pending', 'in_progress')
        """,
        tenant_id,
        source,
    )
    observation_rows = await pool.fetch(
        """
        SELECT id, kind, source_channel, occurred_at, content_text
          FROM observations
         WHERE tenant_id = $1
           AND (source_channel LIKE $2 OR source_channel LIKE $3)
         ORDER BY occurred_at DESC
         LIMIT 25
        """,
        tenant_id,
        f"{source}:%",
        f"gateway:{source}:%",
    )
    observation_count = await pool.fetchval(
        """
        SELECT count(*)::int
          FROM observations
         WHERE tenant_id = $1
           AND (source_channel LIKE $2 OR source_channel LIKE $3)
        """,
        tenant_id,
        f"{source}:%",
        f"gateway:{source}:%",
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
    latest_failure = await pool.fetchrow(
        """
        SELECT last_error AS failure
          FROM onboarding_shards
         WHERE tenant_id = $1
           AND source = $2
           AND state = 'failed'
           AND last_error IS NOT NULL
         ORDER BY completed_at DESC NULLS LAST, created_at DESC
         LIMIT 1
        """,
        tenant_id,
        source,
    )
    installed = any(bool(row["enabled"]) for row in installations)
    has_secret = install is not None and bool(install["has_secret"])
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
    access_payload = await _source_access_status_payload(
        pool,
        tenant_id=tenant_id,
        source=source,
        install=install,
        installations=installations,
        installed=installed,
    )
    installation_payloads = [_source_installation_payload(row) for row in installations]
    return {
        "source": source,
        "installed": installed,
        "installation": installation_payloads[0] if installation_payloads else None,
        "installations": installation_payloads,
        "trigger_count": trigger_total,
        "consumed_trigger_count": int(triggers["consumed"] if triggers else 0),
        "run_status_counts": {row["status"]: int(row["count"]) for row in runs},
        "shard_state_counts": {
            row["state"]: {
                "count": int(row["count"]),
                "observations_seen": int(row["observations_seen"]),
            }
            for row in shards
        },
        "observation_count": int(observation_count or 0),
        "sync_started_at": _iso_or_none(
            sync_run["sync_started_at"] if sync_run else None
        ),
        "observations": observations,
        "unresolved_failure_count": int(failures["total"] if failures else 0),
        "latest_failure": latest_failure["failure"] if latest_failure else None,
        "access_summary": access_payload["access_summary"],
        "access_resources": access_payload["access_resources"],
        "access_next_actions": access_payload["access_next_actions"],
        "bearer_token": bearer_token,
        "session_expires_at": session_expires_at,
        "next_action": _source_rehearsal_next_action(
            source=source,
            installed=installed,
            has_secret=has_secret,
            trigger_count=trigger_total,
            observation_count=int(observation_count or 0),
            latest_failure=latest_failure["failure"] if latest_failure else None,
        ),
    }


async def _enqueue_source_manual_replay(
    pool: Any,
    *,
    tenant_id: UUID,
    source: str,
    payload: dict[str, Any],
) -> UUID:
    trigger_id = uuid7()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO onboarding_triggers (
                    id, tenant_id, source, trigger_kind, payload
                ) VALUES ($1, $2, $3, 'manual_replay', $4::jsonb)
                """,
                trigger_id,
                tenant_id,
                source,
                json.dumps(payload, sort_keys=True),
            )
    return trigger_id


def _empty_source_access_status() -> dict[str, Any]:
    return {
        "access_summary": {
            "total": 0,
            "ready": 0,
            "missing_access": 0,
            "needs_admin": 0,
            "not_selected": 0,
            "unknown": 0,
            "selected": 0,
            "observed": 0,
        },
        "access_resources": [],
        "access_next_actions": [],
    }


async def _source_access_status_payload(
    pool: Any,
    *,
    tenant_id: UUID,
    source: str,
    install: dict[str, Any] | None,
    installations: list[dict[str, Any]],
    installed: bool,
) -> dict[str, Any]:
    if source != "discord" or not installed or install is None:
        return _empty_source_access_status()
    return await _discord_source_access_payload_for_installations(
        pool,
        tenant_id=tenant_id,
        installations=installations,
    )


async def _discord_source_access_payload_for_installations(
    pool: Any,
    *,
    tenant_id: UUID,
    installations: list[dict[str, Any]],
) -> dict[str, Any]:
    resources: list[dict[str, Any]] = []
    actions: list[str] = []
    for install in installations:
        if not bool(install.get("enabled")):
            continue
        payload = await _discord_source_access_payload(
            pool,
            tenant_id=tenant_id,
            install=install,
        )
        resources.extend(payload["access_resources"])
        actions.extend(payload["access_next_actions"])
    return {
        "access_summary": _source_access_summary(resources),
        "access_resources": resources,
        "access_next_actions": _dedupe_source_access_actions(actions),
    }


async def _discord_source_access_payload(
    pool: Any,
    *,
    tenant_id: UUID,
    install: dict[str, Any],
) -> dict[str, Any]:
    guild_id = str(install.get("installation_id") or "").strip()
    installation_row_id = _coerce_uuid(install.get("id"))
    if not guild_id or installation_row_id is None:
        status = _empty_source_access_status()
        status["access_next_actions"] = [
            "Reconnect Discord so Fyralis can review channel access."
        ]
        return status

    cache_key = (str(tenant_id), guild_id, str(installation_row_id))
    now = datetime.now(UTC)
    cached = _DISCORD_ACCESS_STATUS_CACHE.get(cache_key)
    if cached is not None and cached[0] > now:
        return copy.deepcopy(cached[1])

    lock = _DISCORD_ACCESS_STATUS_LOCKS.setdefault(cache_key, asyncio.Lock())
    async with lock:
        now = datetime.now(UTC)
        cached = _DISCORD_ACCESS_STATUS_CACHE.get(cache_key)
        if cached is not None and cached[0] > now:
            return copy.deepcopy(cached[1])

        payload = await _discord_source_access_payload_uncached(
            pool,
            tenant_id=tenant_id,
            install=install,
        )
        ttl_seconds = _DISCORD_ACCESS_STATUS_CACHE_SECONDS
        actions = payload.get("access_next_actions") or []
        if _discord_probe_error_action("discord_api_rate_limited") in actions:
            ttl_seconds = _DISCORD_ACCESS_STATUS_ERROR_CACHE_SECONDS
            if cached is not None:
                payload = copy.deepcopy(cached[1])
                payload["access_next_actions"] = _dedupe_source_access_actions(
                    [
                        _discord_probe_error_action("discord_api_rate_limited"),
                        *list(payload.get("access_next_actions") or []),
                    ]
                )
        _DISCORD_ACCESS_STATUS_CACHE[cache_key] = (
            now + timedelta(seconds=ttl_seconds),
            copy.deepcopy(payload),
        )
        return payload


async def _discord_source_access_payload_uncached(
    pool: Any,
    *,
    tenant_id: UUID,
    install: dict[str, Any],
) -> dict[str, Any]:
    status = _empty_source_access_status()
    guild_id = str(install.get("installation_id") or "").strip()
    installation_row_id = _coerce_uuid(install.get("id"))
    if not guild_id or installation_row_id is None:
        status["access_next_actions"] = [
            "Reconnect Discord so Fyralis can review channel access."
        ]
        return status

    probe_at = datetime.now(UTC).isoformat()
    client = DiscordClient(
        pool=pool,
        secret_store=None,
        tenant_id=tenant_id,
        installation_row_id=installation_row_id,
        guild_id=guild_id,
    )
    try:
        guild_name = await _discord_installation_name(client, install, guild_id)
        if guild_name:
            details = dict(install.get("details") or {})
            details.setdefault("server_name", guild_name)
            details.setdefault("guild_name", guild_name)
            install["details"] = details
        channels = await client.list_guild_channels(guild_id)
        observation_stats = await _discord_observation_stats_by_channel(
            pool,
            tenant_id=tenant_id,
        )
        previous_access_state = await _discord_existing_access_state_by_channel(
            pool,
            tenant_id=tenant_id,
            guild_id=guild_id,
        )
        resources = []
        category_names = {
            str(channel.get("id")): str(channel.get("name") or "Uncategorized")
            for channel in channels
            if _discord_channel_type(channel) == _DISCORD_CATEGORY_CHANNEL_TYPE
            and channel.get("id") is not None
        }
        category_private = {
            str(channel.get("id")): _discord_channel_has_private_gate(
                channel,
                guild_id=guild_id,
            )
            for channel in channels
            if _discord_channel_type(channel) == _DISCORD_CATEGORY_CHANNEL_TYPE
            and channel.get("id") is not None
        }
        message_channels = await _discord_message_channels_for_access(
            client,
            guild_id=guild_id,
            channels=channels,
            include_archived_threads=False,
        )
        channel_names = {
            str(channel.get("id")): str(channel.get("name") or channel.get("id") or "")
            for channel in channels + message_channels
            if channel.get("id") is not None
        }
        message_channels.sort(
            key=lambda channel: (
                str(channel.get("parent_id") or ""),
                _discord_channel_position(channel),
                str(channel.get("name") or channel.get("id") or ""),
            )
        )
        for channel in message_channels:
            channel_id = str(channel.get("id") or "").strip()
            if not channel_id:
                continue
            stats = observation_stats.get(channel_id, {})
            permission_status = "unknown"
            diagnostics: dict[str, Any] = {}
            previous_status = previous_access_state.get(channel_id)
            if previous_status == "ready":
                permission_status = "ready"
            else:
                try:
                    await client.get_messages(channel_id=channel_id, limit=1)
                    permission_status = "ready"
                except DiscordApiError as exc:
                    error_code = getattr(exc, "code", "discord_api_error")
                    if error_code == "discord_channel_forbidden":
                        permission_status = "missing_access"
                        diagnostics = {
                            "issue_code": "discord_channel_missing_access",
                            "message": (
                                "Reconnect Discord with Full Server Sync so Fyralis "
                                "can read private channels without per-channel setup."
                            ),
                        }
                    elif error_code == "discord_secret_unavailable":
                        permission_status = "needs_admin"
                        diagnostics = {
                            "issue_code": "discord_bot_token_missing",
                            "message": "Configure the Discord bot token in the customer data plane.",
                        }
                    else:
                        diagnostics = {
                            "issue_code": _safe_provider_error_code(error_code),
                            "message": "Fyralis could not complete this channel access check.",
                        }
            parent_id = (
                str(channel.get("parent_id"))
                if channel.get("parent_id") is not None
                else None
            )
            parent_private = category_private.get(parent_id or "", False)
            if _discord_channel_type(channel) in {
                _DISCORD_ANNOUNCEMENT_THREAD_TYPE,
                _DISCORD_PUBLIC_THREAD_TYPE,
                _DISCORD_PRIVATE_THREAD_TYPE,
            }:
                parent_channel = next(
                    (
                        item
                        for item in channels
                        if str(item.get("id") or "") == str(parent_id or "")
                    ),
                    None,
                )
                parent_private = (
                    _discord_channel_type(channel) == _DISCORD_PRIVATE_THREAD_TYPE
                    or _discord_channel_has_private_gate(
                        parent_channel or {},
                        guild_id=guild_id,
                    )
                    or category_private.get(
                        str((parent_channel or {}).get("parent_id") or ""),
                        False,
                    )
                )
            visibility = _discord_channel_visibility(
                channel,
                guild_id=guild_id,
                parent_private=parent_private,
                permission_status=permission_status,
            )
            resources.append(
                {
                    "source": "discord",
                    "installation_id": guild_id,
                    "installation_name": guild_name
                    or _discord_installation_fallback_name(guild_id),
                    "resource_kind": "channel",
                    "resource_id": channel_id,
                    "display_name": str(channel.get("name") or channel_id),
                    "parent_id": parent_id,
                    "parent_name": (
                        channel_names.get(parent_id or "")
                        or category_names.get(parent_id or "")
                    ),
                    "visibility": visibility,
                    "permission_status": permission_status,
                    "selected": permission_status == "ready",
                    "can_backfill": permission_status == "ready",
                    "can_receive_live": permission_status == "ready",
                    "last_probe_at": probe_at,
                    "last_observation_at": _iso_or_none(
                        stats.get("last_observation_at")
                    ),
                    "observation_count": int(stats.get("observation_count") or 0),
                    "diagnostics": diagnostics,
                }
            )
        status["access_resources"] = resources
        status["access_summary"] = _source_access_summary(resources)
        replay_actions = await _sync_discord_access_state_and_enqueue_replay(
            pool,
            tenant_id=tenant_id,
            install=install,
            resources=resources,
        )
        status["access_next_actions"] = _dedupe_source_access_actions(
            _source_access_next_actions(resources) + replay_actions
        )
        return status
    except DiscordApiError as exc:
        error_code = getattr(exc, "code", "discord_api_error")
        status["access_next_actions"] = [_discord_probe_error_action(error_code)]
        return status
    finally:
        await client.aclose()


async def _sync_discord_access_state_and_enqueue_replay(
    pool: Any,
    *,
    tenant_id: UUID,
    install: dict[str, Any],
    resources: list[dict[str, Any]],
) -> list[str]:
    guild_id = str(install.get("installation_id") or "").strip()
    installation_row_id = _coerce_uuid(install.get("id"))
    if not guild_id or installation_row_id is None:
        return []

    channel_resources = [
        resource
        for resource in resources
        if resource.get("resource_kind") == "channel"
        and str(resource.get("resource_id") or "").strip()
    ]
    if not channel_resources:
        return []

    transitioned_channel_ids: list[str] = []
    async with pool.acquire() as conn:
        async with conn.transaction():
            for resource in channel_resources:
                resource_id = str(resource.get("resource_id") or "").strip()
                permission_status = _source_access_permission_status(
                    resource.get("permission_status")
                )
                observation_count = max(
                    0,
                    int(resource.get("observation_count") or 0),
                )
                metadata_json = json.dumps(
                    {
                        "display_name": str(
                            resource.get("display_name") or resource_id
                        ),
                        "installation_name": str(
                            resource.get("installation_name")
                            or _discord_installation_fallback_name(guild_id)
                        ),
                        "parent_id": resource.get("parent_id"),
                        "parent_name": str(resource.get("parent_name") or ""),
                        "visibility": str(resource.get("visibility") or "unknown"),
                    },
                    sort_keys=True,
                )
                if permission_status == "ready":
                    transitioned = await conn.fetch(
                        """
                        UPDATE source_resource_access_state
                           SET permission_status = 'ready',
                               observation_count = $5,
                               last_probe_at = now(),
                               last_ready_replay_at = now(),
                               metadata = $6::jsonb,
                               updated_at = now()
                         WHERE tenant_id = $1
                           AND source = 'discord'
                           AND installation_id = $2
                           AND resource_kind = 'channel'
                           AND resource_id = $3
                           AND permission_status = ANY($4::text[])
                         RETURNING resource_id
                        """,
                        tenant_id,
                        guild_id,
                        resource_id,
                        sorted(_SOURCE_ACCESS_READY_REPLAY_FROM_STATUSES),
                        observation_count,
                        metadata_json,
                    )
                    transitioned_channel_ids.extend(
                        str(row["resource_id"]) for row in transitioned
                    )
                await conn.execute(
                    """
                    INSERT INTO source_resource_access_state (
                        tenant_id,
                        source,
                        installation_id,
                        resource_kind,
                        resource_id,
                        permission_status,
                        observation_count,
                        last_probe_at,
                        metadata
                    )
                    VALUES (
                        $1,
                        'discord',
                        $2,
                        'channel',
                        $3,
                        $4,
                        $5,
                        now(),
                        $6::jsonb
                    )
                    ON CONFLICT (
                        tenant_id,
                        source,
                        installation_id,
                        resource_kind,
                        resource_id
                    )
                    DO UPDATE SET
                        permission_status = EXCLUDED.permission_status,
                        observation_count = EXCLUDED.observation_count,
                        last_probe_at = EXCLUDED.last_probe_at,
                        metadata = EXCLUDED.metadata,
                        updated_at = now()
                    """,
                    tenant_id,
                    guild_id,
                    resource_id,
                    permission_status,
                    observation_count,
                    metadata_json,
                )

            if transitioned_channel_ids:
                await conn.execute(
                    """
                    INSERT INTO onboarding_triggers (
                        id,
                        tenant_id,
                        source,
                        trigger_kind,
                        installation_row_id,
                        payload
                    )
                    VALUES (
                        $1,
                        $2,
                        'discord',
                        'manual_replay',
                        NULL,
                        $3::jsonb
                    )
                    """,
                    uuid7(),
                    tenant_id,
                    json.dumps(
                        {
                            "reason": "discord_channel_access_granted",
                            "installation_row_id": str(installation_row_id),
                            "guild_id": guild_id,
                            "channel_ids": transitioned_channel_ids,
                            "channel_count": len(transitioned_channel_ids),
                            "queued_by": "discord_access_probe",
                        },
                        sort_keys=True,
                    ),
                )

    if not transitioned_channel_ids:
        return []
    channel_word = "channel" if len(transitioned_channel_ids) == 1 else "channels"
    return [
        "Fyralis detected newly granted Discord access and queued "
        f"backfill for {len(transitioned_channel_ids)} {channel_word}."
    ]


def _source_access_permission_status(value: Any) -> str:
    status = str(value or "unknown").strip()
    if status in _SOURCE_ACCESS_PERMISSION_STATUSES:
        return status
    return "unknown"


async def _discord_observation_stats_by_channel(
    pool: Any,
    *,
    tenant_id: UUID,
) -> dict[str, dict[str, Any]]:
    rows = await pool.fetch(
        """
        SELECT content #>> '{metadata,channel_id}' AS channel_id,
               count(*)::int AS observation_count,
               max(ingested_at) AS last_observation_at
          FROM observations
         WHERE tenant_id = $1
           AND source_channel = 'discord:message'
           AND content #>> '{metadata,channel_id}' IS NOT NULL
         GROUP BY 1
        """,
        tenant_id,
    )
    return {
        str(row["channel_id"]): {
            "observation_count": int(row["observation_count"] or 0),
            "last_observation_at": row["last_observation_at"],
        }
        for row in rows
        if row["channel_id"]
    }


async def _discord_existing_access_state_by_channel(
    pool: Any,
    *,
    tenant_id: UUID,
    guild_id: str,
) -> dict[str, str]:
    rows = await pool.fetch(
        """
        SELECT resource_id, permission_status
          FROM source_resource_access_state
         WHERE tenant_id = $1
           AND source = 'discord'
           AND installation_id = $2
           AND resource_kind = 'channel'
        """,
        tenant_id,
        guild_id,
    )
    return {
        str(row["resource_id"]): _source_access_permission_status(
            row["permission_status"]
        )
        for row in rows
        if row["resource_id"]
    }


async def _discord_message_channels_for_access(
    client: DiscordClient,
    *,
    guild_id: str,
    channels: list[dict[str, Any]],
    include_archived_threads: bool = True,
) -> list[dict[str, Any]]:
    streams: dict[str, dict[str, Any]] = {
        str(channel["id"]): channel
        for channel in channels
        if channel.get("id") is not None
        and _discord_channel_type(channel) in _DISCORD_MESSAGE_CHANNEL_TYPES
    }

    for thread in await _discord_safe_list_active_threads(client, guild_id):
        thread_id = str(thread.get("id") or "").strip()
        if (
            thread_id
            and _discord_channel_type(thread) in _DISCORD_MESSAGE_CHANNEL_TYPES
        ):
            streams[thread_id] = thread

    if include_archived_threads:
        for parent in channels:
            parent_id = str(parent.get("id") or "").strip()
            if (
                not parent_id
                or _discord_channel_type(parent)
                not in _DISCORD_THREAD_PARENT_CHANNEL_TYPES
            ):
                continue
            for archive_kind in ("public", "private"):
                threads = await _discord_safe_list_archived_threads(
                    client,
                    parent_id,
                    archive_kind=archive_kind,
                )
                for thread in threads:
                    thread_id = str(thread.get("id") or "").strip()
                    if (
                        thread_id
                        and _discord_channel_type(thread)
                        in _DISCORD_MESSAGE_CHANNEL_TYPES
                    ):
                        streams[thread_id] = thread

    return list(streams.values())


async def _discord_safe_list_active_threads(
    client: DiscordClient,
    guild_id: str,
) -> list[dict[str, Any]]:
    try:
        return await client.list_active_guild_threads(guild_id)
    except (AttributeError, DiscordApiError, NotImplementedError):
        return []


async def _discord_safe_list_archived_threads(
    client: DiscordClient,
    channel_id: str,
    *,
    archive_kind: str,
) -> list[dict[str, Any]]:
    try:
        return await client.list_channel_archived_threads(
            channel_id,
            archive_kind=archive_kind,
        )
    except (AttributeError, DiscordApiError, NotImplementedError):
        return []


def _source_access_summary(resources: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "total": len(resources),
        "ready": 0,
        "missing_access": 0,
        "needs_admin": 0,
        "not_selected": 0,
        "unknown": 0,
        "selected": 0,
        "observed": 0,
    }
    for resource in resources:
        permission_status = str(resource.get("permission_status") or "unknown")
        if permission_status in summary:
            summary[permission_status] += 1
        else:
            summary["unknown"] += 1
        if resource.get("selected"):
            summary["selected"] += 1
        if int(resource.get("observation_count") or 0) > 0:
            summary["observed"] += 1
    return summary


def _source_access_next_actions(resources: list[dict[str, Any]]) -> list[str]:
    actions: list[str] = []
    missing_access = [
        resource
        for resource in resources
        if resource.get("permission_status") == "missing_access"
    ]
    needs_admin = [
        resource
        for resource in resources
        if resource.get("permission_status") == "needs_admin"
    ]
    if missing_access:
        channel_names = ", ".join(
            f"#{resource['display_name']}" for resource in missing_access[:3]
        )
        suffix = " and more" if len(missing_access) > 3 else ""
        actions.append(
            "Reconnect Discord with Full Server Sync so Fyralis can read "
            f"{channel_names}{suffix} without per-channel role setup."
        )
    if needs_admin:
        actions.append(
            "Configure the Discord bot token before Fyralis can read channels."
        )
    if (
        not actions
        and resources
        and not any(
            resource.get("permission_status") == "ready" for resource in resources
        )
    ):
        actions.append("Review Discord bot permissions, then refresh channel access.")
    return actions


def _dedupe_source_access_actions(actions: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for action in actions:
        if action in seen:
            continue
        seen.add(action)
        deduped.append(action)
    return deduped


def _discord_probe_error_action(error_code: str) -> str:
    if error_code == "discord_secret_unavailable":
        return (
            "Configure the Discord bot token before Fyralis can review channel access."
        )
    if error_code == "discord_api_unauthorized":
        return "Reinstall Discord or restore bot access to this server."
    if error_code == "discord_api_rate_limited":
        return "Discord rate-limited the access review. Refresh after the retry window."
    return "Refresh Discord access after provider connectivity is healthy."


async def _discord_installation_name(
    client: Any,
    install: dict[str, Any],
    guild_id: str,
) -> str | None:
    details = install.get("details") or {}
    for key in ("server_name", "guild_name", "name"):
        value = details.get(key) if isinstance(details, dict) else None
        if isinstance(value, str) and value.strip():
            return value.strip()

    list_guilds = getattr(client, "list_guilds", None)
    if not callable(list_guilds):
        return None
    try:
        guilds = await list_guilds()
    except Exception:  # noqa: BLE001 - channel probes still provide useful status.
        return None
    for guild in guilds:
        if not isinstance(guild, dict):
            continue
        if str(guild.get("id") or "") == guild_id:
            name = guild.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
    return None


def _discord_installation_fallback_name(guild_id: str) -> str:
    suffix = guild_id[-6:] if len(guild_id) > 6 else guild_id
    return f"Server {suffix}" if suffix else "Discord server"


def _discord_channel_visibility(
    channel: dict[str, Any],
    *,
    guild_id: str,
    parent_private: bool,
    permission_status: str,
) -> str:
    if _discord_channel_has_private_gate(channel, guild_id=guild_id) or parent_private:
        return "private"
    if permission_status == "missing_access":
        return "private"
    if permission_status == "ready":
        return "public"
    return "unknown"


def _discord_channel_has_private_gate(
    channel: dict[str, Any],
    *,
    guild_id: str,
) -> bool:
    overwrites = channel.get("permission_overwrites")
    if not isinstance(overwrites, list):
        return False
    for overwrite in overwrites:
        if not isinstance(overwrite, dict):
            continue
        if str(overwrite.get("id") or "") != guild_id:
            continue
        if _discord_overwrite_type(overwrite) != _DISCORD_OVERWRITE_ROLE_TYPE:
            continue
        if (
            _discord_permission_bits(overwrite.get("deny"))
            & _DISCORD_VIEW_CHANNEL_PERMISSION
        ):
            return True
    return False


def _discord_overwrite_type(overwrite: dict[str, Any]) -> int | None:
    raw_type = overwrite.get("type")
    if isinstance(raw_type, int):
        return raw_type
    try:
        return int(str(raw_type))
    except (TypeError, ValueError):
        return None


def _discord_permission_bits(value: Any) -> int:
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


def _discord_channel_type(channel: dict[str, Any]) -> int | None:
    raw_type = channel.get("type")
    if isinstance(raw_type, int):
        return raw_type
    try:
        return int(str(raw_type))
    except (TypeError, ValueError):
        return None


def _discord_channel_position(channel: dict[str, Any]) -> int:
    raw_position = channel.get("position")
    if isinstance(raw_position, int):
        return raw_position
    try:
        return int(str(raw_position))
    except (TypeError, ValueError):
        return 0


def _coerce_uuid(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _safe_provider_error_code(error_code: Any) -> str:
    if not isinstance(error_code, str) or not error_code:
        return "discord_api_error"
    safe = "".join(
        char if char.isalnum() or char == "_" else "_"
        for char in error_code.strip().lower()
    )
    return safe[:80] or "discord_api_error"


def _source_installation_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "installation_id": row["installation_id"],
        "enabled": bool(row["enabled"]),
        "has_secret": bool(row["has_secret"]),
        "installed_at": row["installed_at"].isoformat(),
        "details": row.get("details", {}),
    }


async def _source_installation_rows(
    pool: Any,
    *,
    tenant_id: UUID,
    source: str,
) -> list[dict[str, Any]]:
    if source == "facebook_pages":
        rows = await pool.fetch(
            """
            SELECT page_id AS installation_id,
                   enabled,
                   (page_access_token_ref IS NOT NULL
                    AND app_secret_ref IS NOT NULL
                    AND verify_token_ref IS NOT NULL) AS has_secret,
                   created_at AS installed_at,
                   page_name,
                   oldest_message_at,
                   backfill_exhausted_at,
                   backfill_exhausted_reason,
                   conversation_count,
                   message_count
              FROM facebook_page_installations
             WHERE tenant_id = $1
             ORDER BY enabled DESC, updated_at DESC
            """,
            tenant_id,
        )
        return [
            {
                "installation_id": row["installation_id"],
                "enabled": row["enabled"],
                "has_secret": row["has_secret"],
                "installed_at": row["installed_at"],
                "details": {
                    "page_name": row["page_name"],
                    "coverage": "All available history",
                    "oldest_message_at": _iso_or_none(row["oldest_message_at"]),
                    "backfill_exhausted_at": _iso_or_none(row["backfill_exhausted_at"]),
                    "backfill_exhausted_reason": row["backfill_exhausted_reason"],
                    "conversation_count": row["conversation_count"],
                    "message_count": row["message_count"],
                },
            }
            for row in rows
        ]
    if source in _OAUTH_REHEARSAL_SOURCES:
        rows = await pool.fetch(
            """
            SELECT id, installation_id, enabled,
                   (secret_ref IS NOT NULL) AS has_secret, installed_at
             FROM provider_installations
             WHERE tenant_id = $1 AND provider = $2
             ORDER BY enabled DESC, installed_at DESC
            """,
            tenant_id,
            source,
        )
        out = [dict(row, details={}) for row in rows]
        if source == "github":
            for item in out:
                if item["enabled"]:
                    item["has_secret"] = True
                    item["details"] = {
                        "credential_scope": "github_app_level_private_key_and_webhook_secret",
                    }
        if source == "discord":
            has_bot_token = bool(load_app_secret_text_from_env("DISCORD_BOT_TOKEN"))
            for item in out:
                if item["enabled"]:
                    item["has_secret"] = has_bot_token
                    item["details"] = {
                        "credential_scope": "discord_app_level_bot_token",
                    }
        return out

    row = await _source_installation_row(pool, tenant_id=tenant_id, source=source)
    return [row] if row else []


async def _source_installation_row(
    pool: Any,
    *,
    tenant_id: UUID,
    source: str,
) -> dict[str, Any] | None:
    if source == "facebook_pages":
        rows = await _source_installation_rows(
            pool,
            tenant_id=tenant_id,
            source=source,
        )
        return rows[0] if rows else None

    if source in _OAUTH_REHEARSAL_SOURCES:
        row = await pool.fetchrow(
            """
            SELECT id, installation_id, enabled,
                   (secret_ref IS NOT NULL) AS has_secret, installed_at
             FROM provider_installations
             WHERE tenant_id = $1 AND provider = $2
             ORDER BY enabled DESC, installed_at DESC
             LIMIT 1
            """,
            tenant_id,
            source,
        )
        if row is None:
            return None
        data = dict(row, details={})
        if source == "github" and data["enabled"]:
            data["has_secret"] = True
            data["details"] = {
                "credential_scope": "github_app_level_private_key_and_webhook_secret",
            }
        return data

    if source == "gmail":
        row = await pool.fetchrow(
            """
            SELECT workspace_domain AS installation_id,
                   (disabled_at IS NULL) AS enabled,
                   (service_account_email IS NOT NULL) AS has_secret,
                   created_at AS installed_at,
                   service_account_email,
                   scope,
                   resolved_user_count,
                   resolved_at
              FROM gmail_installations
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
                "service_account_email": data.get("service_account_email"),
                "scope": data.get("scope"),
                "resolved_user_count": data.get("resolved_user_count"),
                "resolved_at": data.get("resolved_at"),
            },
        }

    if source == "google_calendar":
        row = await pool.fetchrow(
            """
            SELECT workspace_domain AS installation_id,
                   (disabled_at IS NULL) AS enabled,
                   (service_account_email IS NOT NULL) AS has_secret,
                   created_at AS installed_at,
                   service_account_email,
                   scope,
                   resolved_calendar_count,
                   resolved_at
              FROM google_calendar_installations
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
                "service_account_email": data.get("service_account_email"),
                "scope": data.get("scope"),
                "resolved_calendar_count": data.get("resolved_calendar_count"),
                "resolved_at": data.get("resolved_at"),
            },
        }

    if source == "google_drive":
        row = await pool.fetchrow(
            """
            SELECT workspace_domain AS installation_id,
                   (disabled_at IS NULL) AS enabled,
                   (service_account_email IS NOT NULL) AS has_secret,
                   created_at AS installed_at,
                   service_account_email,
                   scope,
                   include_shared_drives,
                   resolved_target_count,
                   resolved_at
              FROM google_drive_installations
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
                "service_account_email": data.get("service_account_email"),
                "scope": data.get("scope"),
                "include_shared_drives": data.get("include_shared_drives"),
                "resolved_target_count": data.get("resolved_target_count"),
                "resolved_at": data.get("resolved_at"),
            },
        }

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

    if source == "aws":
        row = await pool.fetchrow(
            """
            SELECT account_id || ':' || region AS installation_id,
                   (disabled_at IS NULL) AS enabled,
                   (secret_ref IS NOT NULL) AS has_secret,
                   created_at AS installed_at,
                   account_id,
                   region,
                   credential_kind,
                   backfill_window_days
              FROM aws_installations
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
                "account_id": data.get("account_id"),
                "region": data.get("region"),
                "credential_kind": data.get("credential_kind"),
                "backfill_window_days": data.get("backfill_window_days"),
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

    if source == "whatsapp":
        row = await pool.fetchrow(
            """
            SELECT phone_number_id AS installation_id,
                   enabled,
                   (app_secret_ref IS NOT NULL AND verify_token_ref IS NOT NULL)
                       AS has_secret,
                   updated_at AS installed_at,
                   waba_id,
                   display_phone_number,
                   (access_token_ref IS NOT NULL) AS has_access_token
              FROM whatsapp_installations
             WHERE tenant_id = $1
             ORDER BY updated_at DESC
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
                "business_account_id": data.get("waba_id"),
                "display_phone_number": data.get("display_phone_number"),
                "has_access_token": data.get("has_access_token"),
            },
        }

    if source == "ramp":
        row = await pool.fetchrow(
            """
            SELECT business_id AS installation_id,
                   (disabled_at IS NULL) AS enabled,
                   (secret_ref IS NOT NULL OR refresh_secret_ref IS NOT NULL)
                       AS has_secret,
                   created_at AS installed_at,
                   base_url,
                   token_expires_at,
                   (webhook_secret_ref IS NOT NULL) AS webhook_registered
              FROM ramp_installations
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
                "base_url": data.get("base_url"),
                "token_expires_at": data.get("token_expires_at"),
                "webhook_registered": data.get("webhook_registered"),
            },
        }

    return None


def _source_rehearsal_next_action(
    *,
    source: str,
    installed: bool,
    has_secret: bool,
    trigger_count: int,
    observation_count: int,
    latest_failure: str | None = None,
) -> str:
    source_name = _source_display_name(source)
    if not installed:
        if source in _OAUTH_REHEARSAL_SOURCES:
            return f"Approve {source_name} in the provider browser window."
        return f"Submit the required {source_name} connection details."
    if not has_secret:
        return f"{source_name} install is present but required secret refs are missing."
    if source == "whatsapp":
        if observation_count == 0:
            return f"{source_name} install is present; send webhook events to the customer-cloud ingress."
        return f"{source_name} observations are landing in Fyralis."
    if trigger_count == 0:
        return f"{source_name} installed; waiting for onboarding trigger."
    if latest_failure and observation_count == 0:
        return f"{source_name} fetch failed: {latest_failure}"
    if observation_count == 0:
        return (
            f"{source_name} installed; waiting for historical backfill "
            "or live signals."
        )
    return f"{source_name} observations are landing in Fyralis."


def _source_display_name(source: str) -> str:
    names = {
        "aws": "AWS",
        "brex": "Brex",
        "figma": "Figma",
        "facebook_pages": "Facebook Page Messages",
        "gmail": "Gmail",
        "github": "GitHub",
        "google_calendar": "Google Calendar",
        "google_drive": "Google Drive",
        "grafana": "Grafana",
        "hibob": "HiBob",
        "jira": "Jira",
        "miro": "Miro",
        "notion": "Notion",
        "quickbooks": "QuickBooks",
        "slack": "Slack",
        "whatsapp": "WhatsApp",
    }
    return names.get(
        source,
        " ".join(part.capitalize() for part in source.replace("_", "-").split("-")),
    )


__all__ = ["build_byoc_onboarding_router"]
