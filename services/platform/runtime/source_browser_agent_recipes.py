"""Customer-cloud browser-agent recipes for BYOC source auto-connect.

The UI intentionally stays small: source cards and a Connect button. These
recipes are the background contract for the customer-cloud browser agent that
opens provider settings, collects non-secret configuration, generates Fyralis
owned material, and pauses only at provider-enforced admin gates.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BrowserAgentRecipe:
    source: str
    provider_console_url: str
    settings_targets: tuple[str, ...]
    agent_collects: tuple[str, ...]
    agent_generates: tuple[str, ...]
    human_gates: tuple[str, ...]
    completion_checks: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "provider_console_url": self.provider_console_url,
            "settings_targets": list(self.settings_targets),
            "agent_collects": list(self.agent_collects),
            "agent_generates": list(self.agent_generates),
            "human_gates": list(self.human_gates),
            "completion_checks": list(self.completion_checks),
        }


COMMON_COMPLETION_CHECKS = (
    "provider handoff prepared",
    "customer-cloud secret refs created or confirmed",
    "source install status is pollable",
    "onboarding trigger or source-native install row is present",
    "sanitized connection proof can be read from observations",
)


def _recipe(
    source: str,
    provider_console_url: str,
    settings_targets: tuple[str, ...],
    agent_collects: tuple[str, ...],
    agent_generates: tuple[str, ...],
    human_gates: tuple[str, ...],
    completion_checks: tuple[str, ...] = COMMON_COMPLETION_CHECKS,
) -> BrowserAgentRecipe:
    return BrowserAgentRecipe(
        source=source,
        provider_console_url=provider_console_url,
        settings_targets=settings_targets,
        agent_collects=agent_collects,
        agent_generates=agent_generates,
        human_gates=human_gates,
        completion_checks=completion_checks,
    )


OAUTH_GATES = (
    "admin signs in and completes MFA when prompted",
    "admin approves provider app scopes",
)

TOKEN_GATES = (
    "admin signs in and completes MFA when prompted",
    "admin creates or approves a least-privilege service credential",
)

LOCAL_SESSION_GATES = (
    "admin signs in and completes MFA when prompted",
    "admin authorizes the local customer-cloud session or device link",
)

DWD_GATES = (
    "Google Workspace admin signs in and completes MFA when prompted",
    "Google Workspace admin authorizes the Fyralis service account client ID and scopes",
    "admin approves workspace inclusion scope",
)


BROWSER_AGENT_RECIPES: dict[str, BrowserAgentRecipe] = {
    "ashby": _recipe(
        "ashby",
        "https://app.ashbyhq.com/admin/api",
        ("API settings", "webhook settings", "organization settings"),
        ("organization id", "API base URL", "available recruiting scopes"),
        ("webhook signing secret ref", "jobs/candidates/interviews scope contract"),
        TOKEN_GATES,
    ),
    "aws": _recipe(
        "aws",
        "https://console.aws.amazon.com/iam/",
        ("IAM roles", "CloudTrail", "EventBridge", "AWS Organizations"),
        ("account id", "region list", "role ARN", "event source availability"),
        ("read-only IAM policy template", "external id", "role trust contract"),
        (
            "cloud admin signs in and completes MFA when prompted",
            "cloud admin creates or approves the read-only Fyralis role",
        ),
    ),
    "brex": _recipe(
        "brex",
        "https://developer.brex.com/",
        ("developer API settings", "webhook settings", "organization settings"),
        ("organization id", "account ids", "supported card and transaction scopes"),
        ("webhook verifier ref", "finance entity scope contract"),
        TOKEN_GATES,
    ),
    "carta": _recipe(
        "carta",
        "https://developers.app.carta.com/",
        ("developer app settings", "issuer or firm settings", "token settings"),
        ("issuer id", "firm id", "OAuth/client-credentials availability"),
        ("equity entity scope contract", "token refresh ref contract"),
        OAUTH_GATES,
    ),
    "deel": _recipe(
        "deel",
        "https://app.deel.com/",
        ("developer API settings", "webhook settings", "organization settings"),
        ("organization id", "contract ids", "worker/payment scope availability"),
        ("webhook signing secret ref", "workforce scope contract"),
        TOKEN_GATES,
    ),
    "discord": _recipe(
        "discord",
        "https://discord.com/developers/applications",
        ("application OAuth2 settings", "bot settings", "guild settings"),
        ("application id", "guild ids", "channel ids", "enabled intents"),
        ("bot/gateway session contract", "webhook verifier ref"),
        (
            "server admin signs in and completes MFA when prompted",
            "server admin approves bot install and gateway intents",
        ),
    ),
    "figma": _recipe(
        "figma",
        "https://www.figma.com/developers/api",
        ("developer token settings", "team settings", "webhook settings"),
        ("team id", "file keys", "webhook-capable file scopes"),
        ("webhook secret ref", "design file scope contract"),
        TOKEN_GATES,
    ),
    "fireflies": _recipe(
        "fireflies",
        "https://app.fireflies.ai/integrations",
        ("integration settings", "workspace settings", "webhook settings"),
        ("workspace id", "transcript scope", "meeting history availability"),
        ("webhook signing secret ref", "meeting transcript scope contract"),
        TOKEN_GATES,
    ),
    "github": _recipe(
        "github",
        "https://github.com/settings/apps",
        ("GitHub App settings", "organization installations", "webhook settings"),
        ("installation id", "repository scope", "webhook delivery URL"),
        ("webhook secret ref", "repository scope contract"),
        (
            "org admin signs in and completes MFA when prompted",
            "org admin approves the GitHub App installation and repositories",
        ),
    ),
    "gmail": _recipe(
        "gmail",
        "https://admin.google.com/ac/owl/domainwidedelegation",
        ("Domain-wide delegation", "API controls", "mailbox inclusion scope", "Pub/Sub topic"),
        ("workspace domain", "admin email", "mailbox/user/group/org-unit scope"),
        ("DWD preflight payload", "mailbox inclusion contract", "watch verifier ref"),
        DWD_GATES,
    ),
    "google_calendar": _recipe(
        "google_calendar",
        "https://admin.google.com/ac/owl/domainwidedelegation",
        ("Domain-wide delegation", "Calendar API controls", "calendar inclusion scope"),
        ("workspace domain", "admin email", "calendar/user/group/org-unit scope"),
        ("DWD preflight payload", "calendar inclusion contract"),
        DWD_GATES,
    ),
    "google_drive": _recipe(
        "google_drive",
        "https://admin.google.com/ac/owl/domainwidedelegation",
        ("Domain-wide delegation", "Drive API controls", "Drive inclusion scope"),
        ("workspace domain", "admin email", "shared-drive/user/group/org-unit scope"),
        ("DWD preflight payload", "Drive inclusion contract", "drive watch verifier ref"),
        DWD_GATES,
    ),
    "grafana": _recipe(
        "grafana",
        "https://grafana.com/auth/sign-in/",
        ("service account settings", "folder settings", "alerting settings"),
        ("instance URL", "folder ids", "dashboard ids", "alert scope"),
        ("service account token ref", "webhook secret ref", "dashboard/alert scope contract"),
        TOKEN_GATES,
    ),
    "gusto": _recipe(
        "gusto",
        "https://dev.gusto.com/",
        ("developer app settings", "company settings", "webhook settings"),
        ("company uuid", "employee/payroll scope", "webhook availability"),
        ("webhook verifier token ref", "company scope contract"),
        OAUTH_GATES,
    ),
    "hibob": _recipe(
        "hibob",
        "https://app.hibob.com/",
        ("service user settings", "reports", "people fields"),
        ("company id", "service user id", "people fields", "report ids", "directory scope"),
        ("service user token ref", "people field scope contract"),
        TOKEN_GATES,
    ),
    "jira": _recipe(
        "jira",
        "https://admin.atlassian.com/",
        ("API token page", "Jira project settings", "webhook settings"),
        ("site URL", "project keys", "issue/comment scope"),
        ("webhook secret ref", "Jira project scope contract"),
        TOKEN_GATES,
    ),
    "linkedin": _recipe(
        "linkedin",
        "https://www.linkedin.com/developers/apps",
        ("developer app settings", "organization/page settings", "rate limit posture"),
        ("organization URN", "page scope", "polling window"),
        ("polling contract", "rate-limit guard"),
        OAUTH_GATES,
    ),
    "mercury": _recipe(
        "mercury",
        "https://app.mercury.com/settings/tokens",
        ("API token settings", "account settings", "webhook settings"),
        ("organization id", "account ids", "transaction scope"),
        ("webhook secret ref", "bank account scope contract"),
        TOKEN_GATES,
    ),
    "miro": _recipe(
        "miro",
        "https://developers.miro.com/",
        ("developer app settings", "team settings", "board settings"),
        ("team id", "board ids", "polling scope"),
        ("board scope contract", "polling cadence guard"),
        TOKEN_GATES,
    ),
    "notion": _recipe(
        "notion",
        "https://www.notion.so/my-integrations",
        ("integration settings", "workspace sharing", "database/page settings"),
        ("workspace id", "shared pages", "shared databases"),
        ("workspace/page scope contract", "webhook eligibility contract"),
        OAUTH_GATES,
    ),
    "quickbooks": _recipe(
        "quickbooks",
        "https://developer.intuit.com/app/developer/myapps",
        ("developer app settings", "company realm", "webhook settings"),
        ("realm id", "accounting entity scope", "webhook verifier status"),
        ("webhook verifier ref", "realm scope contract"),
        OAUTH_GATES,
    ),
    "ramp": _recipe(
        "ramp",
        "https://developers.ramp.com/",
        ("OAuth app settings", "business settings", "webhook settings"),
        ("business id", "transaction/reimbursement/card/user streams", "API base URL", "webhook target"),
        ("webhook verifier token ref", "Ramp spend scope contract"),
        TOKEN_GATES,
    ),
    "signal": _recipe(
        "signal",
        "https://signal.org/download/",
        ("local device-link session", "contact/group scope", "gateway runner settings"),
        ("account label", "approved contacts", "approved groups", "thread scope"),
        ("linked-device session ref", "Signal gateway runner contract"),
        LOCAL_SESSION_GATES,
    ),
    "slack": _recipe(
        "slack",
        "https://api.slack.com/apps",
        ("Slack app configuration tokens", "OAuth scopes", "event subscriptions"),
        ("workspace id", "channel scope", "OAuth callback URL"),
        ("Slack app manifest contract", "Slack event scope contract"),
        OAUTH_GATES,
    ),
    "telegram": _recipe(
        "telegram",
        "https://my.telegram.org/apps",
        ("Telegram API app", "local MTProto session", "dialog discovery"),
        ("api id", "account label", "dialogs", "channel/group scope"),
        ("api hash ref", "MTProto session refs", "Telegram gateway runner contract"),
        LOCAL_SESSION_GATES,
    ),
    "whatsapp": _recipe(
        "whatsapp",
        "https://developers.facebook.com/apps/",
        ("Meta app settings", "WhatsApp business settings", "webhook settings"),
        ("business account id", "phone number id", "webhook verify target"),
        ("verify token ref", "app secret ref", "WhatsApp webhook contract"),
        (
            "Meta admin signs in and completes MFA when prompted",
            "Meta admin approves business phone and webhook subscriptions",
        ),
    ),
}


def browser_agent_recipe_for_source(source: str) -> dict[str, Any]:
    recipe = BROWSER_AGENT_RECIPES.get(source)
    if recipe is None:
        raise KeyError(source)
    return recipe.as_dict()


def missing_browser_agent_recipe_sources(sources: set[str]) -> set[str]:
    return set(sources) - set(BROWSER_AGENT_RECIPES)


__all__ = [
    "BROWSER_AGENT_RECIPES",
    "BrowserAgentRecipe",
    "browser_agent_recipe_for_source",
    "missing_browser_agent_recipe_sources",
]
