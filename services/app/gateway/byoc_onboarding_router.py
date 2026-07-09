"""Hosted-portal onboarding routes for Design Partner BYOC."""
from __future__ import annotations

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

from lib.shared.ids import uuid7

_ALL_REHEARSAL_SOURCES = {
    "ashby",
    "aws",
    "brex",
    "carta",
    "deel",
    "discord",
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
_OAUTH_REHEARSAL_SOURCES = {"slack", "github", "discord", "notion"}
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
        "figma",
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

_SOURCE_CALLBACK_PATHS = {
    "slack": "/integrations/slack/callback",
    "discord": "/integrations/discord/callback",
    "github": "/integrations/github/callback",
    "notion": "/integrations/notion/callback",
}

_SOURCE_LIVE_INGRESS_PATHS = {
    "ashby": "/webhooks/ashby/{install-id}",
    "brex": "/webhooks/brex",
    "deel": "/webhooks/deel",
    "slack": "/webhooks/slack/events",
    "discord": "/webhooks/discord",
    "figma": "/webhooks/figma",
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
    "figma": [
        "api_token",
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
    "figma": ["team_id", "base_url", "file_keys", "webhook_secret"],
    "fireflies": ["workspace_id", "base_url", "webhook_secret"],
    "gmail": ["scope", "inclusion_spec", "pubsub_topic", "watch_channel_id"],
    "google_calendar": ["scope", "inclusion_spec"],
    "google_drive": ["scope", "inclusion_spec", "include_shared_drives", "watch_channel_id"],
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
    "quickbooks": ["oauth_client", "base_url", "refresh_token_ref", "webhook_verifier_token"],
    "ramp": ["business_id", "base_url", "entity_scope", "webhook_verifier_token"],
    "signal": ["account_label", "backfill_session", "thread_scope"],
    "telegram": ["backfill_session", "dialogs"],
    "whatsapp": ["business_account_id", "display_phone_number", "access_token"],
}

_GENERIC_PROVIDER_CONSOLES = {
    "ashby": "https://app.ashbyhq.com/admin/api",
    "aws": "https://console.aws.amazon.com/iam/",
    "brex": "https://developer.brex.com/",
    "carta": "https://developers.app.carta.com/",
    "deel": "https://app.deel.com/",
    "figma": "https://www.figma.com/developers/api",
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
    "figma": "api_token",
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
    "discord": "guilds, text channels, private channels the bot can access, and threads",
    "figma": "teams, projects, files, and webhook-capable file scopes",
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
            "approved_channel_ids",
            "oauth_redirect_url",
            "events_request_url",
        ],
    },
    "figma": {
        "kind": "api_token_native_connect",
        "preflight_path": "/integrations/figma/connect/preflight",
        "finalize_path": "/integrations/figma/connect/finalize",
        "preflight_payload_fields": ["api_token", "base_url", "team_id"],
        "payload_fields": [
            "api_token",
            "team_id",
            "base_url",
            "file_keys",
            "webhook_id",
            "webhook_secret",
        ],
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
        "payload_fields": ["workspace_domain", "admin_email", "scope", "inclusion_spec"],
        "scope_aliases": ["gmail.metadata"],
    },
    "google_calendar": {
        "kind": "google_workspace_dwd",
        "preflight_path": "/integrations/google_calendar/connect/preflight",
        "finalize_path": "/integrations/google_calendar/connect/finalize",
        "preflight_payload_fields": ["workspace_domain", "admin_email", "scope"],
        "payload_fields": ["workspace_domain", "admin_email", "scope", "inclusion_spec"],
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
        body = await _optional_json_body(request)
        provider_inputs = _source_finalize_inputs(body) if body else {}
        payload = await _prepare_source_rehearsal_response(
            request,
            source,
            provider_inputs=provider_inputs,
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
    async def consume_slack_browser_agent_config_token(request: Request) -> dict[str, Any]:
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
            inputs.get("business_account_id")
            or inputs.get("waba_id")
            or ""
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
    provider_inputs: dict[str, str] | None = None,
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
        provider_inputs=provider_inputs or {},
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
        "optional_inputs": _SOURCE_OPTIONAL_INPUTS.get(source, []),
        "finalize_mode": _source_finalize_mode(source),
        "automation_profile": _source_automation_profile(source),
        "browser_agent": browser_agent_recipe_for_source(source),
        "native_connect": _SOURCE_NATIVE_CONNECT_CONTRACTS.get(source),
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
    provider_inputs: dict[str, str],
) -> dict[str, Any]:
    public_url = _public_url_from_env_or_request(request)
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
                        "message": str(exc),
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

        client_id = app_refs.client_id if app_refs else os.environ.get(
            "SLACK_CLIENT_ID", ""
        ).strip()
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
                "DISCORD_CLIENT_SECRET": _env_or_secret_ref_configured("DISCORD_CLIENT_SECRET"),
                "DISCORD_APPLICATION_ID": os.environ.get("DISCORD_APPLICATION_ID", ""),
                "DISCORD_BOT_TOKEN": _env_or_secret_ref_configured("DISCORD_BOT_TOKEN"),
                "WEBHOOK_SECRET_DISCORD": _env_or_secret_ref_configured("WEBHOOK_SECRET_DISCORD"),
            }.items()
            if not value
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
            "authorization_mode": "oauth_plus_gateway",
            "install_url": install_url,
            "oauth_redirect_url": redirect_uri or f"{public_url}/integrations/discord/callback",
            "provider_console_url": "https://discord.com/developers/applications",
            "missing_configuration": missing,
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
                "NOTION_CLIENT_SECRET": _env_or_secret_ref_configured("NOTION_CLIENT_SECRET"),
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
    browser_agent = payload.get("browser_agent") or browser_agent_recipe_for_source(source)
    browser_agent_run = payload.get("browser_agent_run") or source_browser_agent_run_for_payload(
        source,
        payload,
    )
    missing_configuration = list(payload.get("missing_configuration") or [])
    human_steps = list(automation_profile.get("human_steps") or [])
    human_step_count = int(automation_profile.get("human_step_count") or len(human_steps))
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
                    "automated_actions": automation_profile.get("automated_actions") or [],
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

    if finalize_mode in {"source_specific", "native_finalizer_required"} or human_step_count:
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
        item for item in browser_agent_run.get("action_queue") or [] if isinstance(item, dict)
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
        "auto_connect_run": {**descriptor, **run_record},
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
        str(record.get("receipt_path_hint") or source_dir / "browser-agent-receipt.json")
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
    if (
        record.get("background_runner_mode") == "artifact_materialization"
        and record.get("background_status") in {"admin_gate", "waiting_for_admin"}
    ):
        record["background_status"] = "blocked"
        record["status"] = "blocked"
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
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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
    if method == "oauth":
        actions.insert(1, "mint OAuth state and open provider approval")
    elif method == "oauth_plus_gateway":
        actions.insert(1, "mint OAuth state and open provider approval")
        actions.insert(2, "prepare local gateway runner contract")
    elif method == "dwd":
        actions.insert(1, "prepare Google Workspace DWD preflight and finalize contract")
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
    if method == "oauth":
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
    installed = install is not None and bool(install["enabled"])
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
            has_secret=has_secret,
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
