#!/usr/bin/env python3
"""Validate the production environment template contract.

The runtime already fails closed for several missing production settings. This
static check keeps `.env.production.example` aligned with those fail-closed
paths so operators see the required keys before deploy time.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_TEMPLATE = REPO_ROOT / ".env.production.example"

REQUIRED_KEYS = frozenset(
    {
        "COMPANY_OS_ENV",
        "FYRALIS_ENV",
        "FYRALIS_DEPLOYMENT_MODE",
        "DATABASE_URL",
        "REDIS_URL",
        "POSTGRES_PGBOUNCER_COMPATIBLE",
        "DB_STATEMENT_TIMEOUT_MS",
        "DB_LOCK_TIMEOUT_MS",
        "DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS",
        "OLLAMA_URL",
        "OLLAMA_EMBED_MODEL",
        "GRT_RENDERING_BASE_URL",
        "QUERY_RENDERING_BASE_URL",
        "QUERY_CACHE_BACKEND",
        "LLM_PROVIDER",
        "LLM_STRICT_CONFIG",
        "CODEX_API_KEY_SECRET_REF",
        "CODEX_API_KEY",
        "CODEX_TRANSPORT",
        "CODEX_MODEL",
        "CODEX_REASONING_EFFORT",
        "FYRALIS_BYOC_EVIDENCE_INTAKE_KEY_REF",
        "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY_SECRET_REF",
        "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY",
        "FYRALIS_BYOC_EVIDENCE_READ_KEY_REF",
        "FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY_SECRET_REF",
        "FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY",
        "INQUIRY_CODEX_QUESTION_MODEL",
        "THINK_NARROW_INFERENTIAL_TX",
        "SECRET_STORE_BACKEND",
        "MASTER_KEK_PROVIDER",
        "MASTER_KEK_SECRET_REF",
        "SECRET_PROVIDER_REGION",
        "SECRET_PROVIDER_TIMEOUT_SECONDS",
        "OAUTH_STATE_HMAC_KEY_SECRET_REF",
        "OAUTH_STATE_HMAC_KEY",
        "AUTH_BOOTSTRAP_SECRET_SECRET_REF",
        "AUTH_BOOTSTRAP_SECRET",
        "DEBUG_ENDPOINTS_ENABLED",
        "FINANCE_PANEL_ENABLED",
        "SLACK_DM_PANEL_ENABLED",
        "SPEC_DEMO_ROUTES_ENABLED",
        "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED",
        "WEBSOCKET_SESSION_COOKIE_NAME",
        "VIEW_CEO_STATIC_TOKENS_ENABLED",
        "GATEWAY_MOUNT_SIM",
        "GATEWAY_START_GRT_SCHEDULER",
        "WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW",
        "KAFKA_BOOTSTRAP_SERVERS",
        "GATEWAY_REQUIRE_REALTIME",
        "GATEWAY_REQUIRE_GITHUB_INTEGRATION",
        "GATEWAY_REQUIRE_INGESTION_DATA_PLANE",
        "S3_RAW_BUCKET",
        "S3_BLOB_BUCKET",
        "S3_REGION_NAME",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "EMBEDDER_BACKEND",
        "GITHUB_APP_ID",
        "GITHUB_APP_SLUG",
        "GITHUB_APP_PRIVATE_KEY_SECRET_REF",
        "GITHUB_APP_PRIVATE_KEY",
        "WEBHOOK_SECRET_GITHUB_SECRET_REF",
        "WEBHOOK_SECRET_GITHUB",
        "FIGMA_OAUTH_ENABLED",
        "FIGMA_CLIENT_ID",
        "FIGMA_CLIENT_SECRET_SECRET_REF",
        "FIGMA_CLIENT_SECRET",
        "FIGMA_REDIRECT_URI",
        "FIGMA_OAUTH_UI_BASE_URL",
        "FIGMA_OAUTH_ALLOW_HTTP_LOOPBACK",
        "FIGMA_OAUTH_SCOPES",
        "SLACK_CLIENT_ID",
        "SLACK_CLIENT_SECRET_SECRET_REF",
        "SLACK_CLIENT_SECRET",
        "SLACK_REDIRECT_URI",
        "SLACK_SIGNING_SECRET_SECRET_REF",
        "SLACK_SIGNING_SECRET",
        "DISCORD_BOT_TOKEN_SECRET_REF",
        "DISCORD_BOT_TOKEN",
        "DISCORD_APPLICATION_ID",
        "DISCORD_CLIENT_ID",
        "DISCORD_CLIENT_SECRET_SECRET_REF",
        "DISCORD_CLIENT_SECRET",
        "DISCORD_REDIRECT_URI",
        "WEBHOOK_SECRET_DISCORD_SECRET_REF",
        "WEBHOOK_SECRET_DISCORD",
        "PYTHONHASHSEED",
        "GMAIL_SERVICE_ACCOUNT_JSON_FILE",
        "GMAIL_SERVICE_ACCOUNT_CLIENT_ID",
        "GMAIL_PUBSUB_PROJECT_ID",
        "GMAIL_PUBSUB_PUSH_ENDPOINT",
        "GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE",
        "GMAIL_PUBSUB_PUSH_OIDC_SA",
        "GRAFANA_ADMIN_PASSWORD",
        "GATEWAY_POSTGRES_POOL_SIZE",
        "WRITER_POSTGRES_POOL_SIZE",
        "POSTGRES_POOL_SIZE",
        "THINK_POSTGRES_POOL_SIZE",
        "POST_COMMIT_POSTGRES_POOL_SIZE",
        "HOUSEKEEPER_POSTGRES_POOL_SIZE",
        "MAINTENANCE_POSTGRES_POOL_SIZE",
        "SOURCE_GATEWAY_POSTGRES_POOL_SIZE",
        "SOURCE_SCHEDULER_POSTGRES_POOL_SIZE",
        "EXTENSION_WORKERS_POOL_MAX",
        "THINK_MAX_CONCURRENCY_PER_TENANT",
        "ANOMALY_T3_BUDGET_PER_MIN",
        "ENTITY_RESOLVER_LLM_BUDGET_PER_MIN",
        "TOPOLOGY_SWEEPER_LIMIT_PER_TENANT",
        "RELATIONSHIP_ONTOLOGY_PROPOSALS_LIMIT_PER_TENANT",
        "SAGE_TOPOLOGY_OPTIMIZER_LIMIT",
        "HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS",
        "WEBHOOK_TENANT_DEFAULT_ALLOW",
        "LOG_LEVEL",
        "DEBUG_ARTIFACT_CAPTURE",
        "SHARD_FETCH_RATE_LIMIT",
        "SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC",
        "SOURCE_CONNECTOR_REQUIRE_SIGNED_ARTIFACTS",
        "SOURCE_CONNECTOR_TRUSTED_SIGNERS_JSON",
        "SOURCE_CONNECTOR_ALLOWED_BUILDERS",
    }
)

REQUIRED_EXACT_VALUES = {
    "COMPANY_OS_ENV": "prod",
    "FYRALIS_ENV": "prod",
    "QUERY_CACHE_BACKEND": "pg",
    "LLM_PROVIDER": "codex",
    "LLM_STRICT_CONFIG": "1",
    "CODEX_TRANSPORT": "responses",
    "DEBUG_ENDPOINTS_ENABLED": "0",
    "FINANCE_PANEL_ENABLED": "false",
    "SLACK_DM_PANEL_ENABLED": "false",
    "SPEC_DEMO_ROUTES_ENABLED": "0",
    "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED": "0",
    "VIEW_CEO_STATIC_TOKENS_ENABLED": "0",
    "THINK_NARROW_INFERENTIAL_TX": "1",
    "GATEWAY_MOUNT_SIM": "0",
    "WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW": "0",
    "GATEWAY_REQUIRE_INGESTION_DATA_PLANE": "1",
    "PYTHONHASHSEED": "0",
    "WEBHOOK_TENANT_DEFAULT_ALLOW": "0",
    "DEBUG_ARTIFACT_CAPTURE": "0",
    "SHARD_FETCH_RATE_LIMIT": "1",
    "HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS": "0",
    "SOURCE_CONNECTOR_REQUIRE_SIGNED_ARTIFACTS": "1",
}
REQUIRED_ALLOWED_VALUES = {
    "FYRALIS_DEPLOYMENT_MODE": {"single-tenant", "byoc"},
    "SECRET_STORE_BACKEND": {"fernet"},
    "MASTER_KEK_PROVIDER": {
        "aws-secrets-manager",
        "gcp-secret-manager",
        "hashicorp-vault",
    },
    "POSTGRES_PGBOUNCER_COMPATIBLE": {"0", "1"},
    "GATEWAY_REQUIRE_REALTIME": {"0", "1"},
    "GATEWAY_REQUIRE_GITHUB_INTEGRATION": {"0", "1"},
    "GATEWAY_START_GRT_SCHEDULER": {"0", "1"},
    "FIGMA_OAUTH_ENABLED": {"0", "1"},
    "FIGMA_OAUTH_ALLOW_HTTP_LOOPBACK": {"0", "1"},
}
FORBIDDEN_EXACT_VALUES = {
    "GRAFANA_ADMIN_PASSWORD": {"", "admin", "password", "fyralis-admin"},
}
REQUIRED_BLANK_SECRET_PLACEHOLDER_KEYS = frozenset(
    {
        "AUTH_BOOTSTRAP_SECRET",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "CODEX_API_KEY",
        "DISCORD_BOT_TOKEN",
        "DISCORD_CLIENT_SECRET",
        "FIGMA_CLIENT_SECRET",
        "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY",
        "FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY",
        "GITHUB_APP_PRIVATE_KEY",
        "OAUTH_STATE_HMAC_KEY",
        "SLACK_CLIENT_SECRET",
        "SLACK_SIGNING_SECRET",
        "WEBHOOK_SECRET_DISCORD",
        "WEBHOOK_SECRET_GITHUB",
    }
)
REQUIRED_NONEMPTY_SECRET_REF_KEYS = frozenset(
    {
        "AUTH_BOOTSTRAP_SECRET_SECRET_REF",
        "CODEX_API_KEY_SECRET_REF",
        "DISCORD_BOT_TOKEN_SECRET_REF",
        "DISCORD_CLIENT_SECRET_SECRET_REF",
        "FIGMA_CLIENT_SECRET_SECRET_REF",
        "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY_SECRET_REF",
        "FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY_SECRET_REF",
        "GITHUB_APP_PRIVATE_KEY_SECRET_REF",
        "OAUTH_STATE_HMAC_KEY_SECRET_REF",
        "SLACK_CLIENT_SECRET_SECRET_REF",
        "SLACK_SIGNING_SECRET_SECRET_REF",
        "WEBHOOK_SECRET_DISCORD_SECRET_REF",
        "WEBHOOK_SECRET_GITHUB_SECRET_REF",
    }
)
FORBIDDEN_KEYS = frozenset(
    {
        "DEFAULT_ACTOR_ID",
        "DEFAULT_TENANT_ID",
        "COMPANY_OS_TENANT_ID",
        "FYRALIS_BYOC_INSTALL_TOKEN",
        "FYRALIS_DATA_PLANE_AGENT_PRIVATE_KEY",
        "GMAIL_SERVICE_ACCOUNT_JSON",
        "MASTER_KEK",
        "WHATSAPP_APP_SECRET",
        "WHATSAPP_VERIFY_TOKEN",
        "WHATSAPP_ALLOW_UNSIGNED",
    }
)
REQUIRED_POSITIVE_INTEGER_KEYS = frozenset(
    {
        "DB_STATEMENT_TIMEOUT_MS",
        "DB_LOCK_TIMEOUT_MS",
        "DB_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS",
        "GATEWAY_POSTGRES_POOL_SIZE",
        "WRITER_POSTGRES_POOL_SIZE",
        "POSTGRES_POOL_SIZE",
        "THINK_POSTGRES_POOL_SIZE",
        "POST_COMMIT_POSTGRES_POOL_SIZE",
        "HOUSEKEEPER_POSTGRES_POOL_SIZE",
        "MAINTENANCE_POSTGRES_POOL_SIZE",
        "SOURCE_GATEWAY_POSTGRES_POOL_SIZE",
        "SOURCE_SCHEDULER_POSTGRES_POOL_SIZE",
        "EXTENSION_WORKERS_POOL_MAX",
        "THINK_MAX_CONCURRENCY_PER_TENANT",
        "ANOMALY_T3_BUDGET_PER_MIN",
        "ENTITY_RESOLVER_LLM_BUDGET_PER_MIN",
        "TOPOLOGY_SWEEPER_LIMIT_PER_TENANT",
        "RELATIONSHIP_ONTOLOGY_PROPOSALS_LIMIT_PER_TENANT",
        "SAGE_TOPOLOGY_OPTIMIZER_LIMIT",
    }
)
REQUIRED_POSITIVE_NUMBER_KEYS = frozenset(
    {
        "SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC",
    }
)
BYOC_REQUIRED_KEYS = frozenset(
    {
        "FYRALIS_BYOC_DEPLOYMENT_ID",
        "FYRALIS_BYOC_CUSTOMER_ID",
        "FYRALIS_BYOC_CLOUD_PROVIDER",
        "FYRALIS_BYOC_REGION",
        "FYRALIS_CONTROL_PLANE_URL",
        "FYRALIS_CONTROL_PLANE_CONNECTIVITY",
        "FYRALIS_DATA_PLANE_AGENT_ENABLED",
        "FYRALIS_DATA_PLANE_AGENT_AUTH",
        "FYRALIS_DATA_PLANE_AGENT_INSTALL_TOKEN_SECRET_REF",
        "FYRALIS_DATA_PLANE_AGENT_CLIENT_CERT_SECRET_REF",
        "FYRALIS_BYOC_EVIDENCE_INTAKE_KEY_REF",
        "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY_SECRET_REF",
        "FYRALIS_BYOC_EVIDENCE_READ_KEY_REF",
        "FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY_SECRET_REF",
        "FYRALIS_TELEMETRY_MODE",
        "FYRALIS_TELEMETRY_RAW_LOGS_ALLOWED",
        "FYRALIS_TELEMETRY_RAW_PAYLOADS_ALLOWED",
        "FYRALIS_CONTROL_PLANE_INBOUND_ALLOWED",
    }
)
BYOC_REQUIRED_EXACT_VALUES = {
    "FYRALIS_CONTROL_PLANE_CONNECTIVITY": "egress_only",
    "FYRALIS_DATA_PLANE_AGENT_ENABLED": "1",
    "FYRALIS_DATA_PLANE_AGENT_AUTH": "mtls",
    "FYRALIS_TELEMETRY_RAW_LOGS_ALLOWED": "0",
    "FYRALIS_TELEMETRY_RAW_PAYLOADS_ALLOWED": "0",
    "FYRALIS_CONTROL_PLANE_INBOUND_ALLOWED": "0",
}
BYOC_REQUIRED_ALLOWED_VALUES = {
    "FYRALIS_BYOC_CLOUD_PROVIDER": {
        "aws",
        "gcp",
        "azure",
        "customer-managed-kubernetes",
    },
    "FYRALIS_TELEMETRY_MODE": {"aggregate-only", "disabled"},
}
BYOC_REQUIRED_NONEMPTY_KEYS = frozenset(
    {
        "FYRALIS_BYOC_DEPLOYMENT_ID",
        "FYRALIS_BYOC_CUSTOMER_ID",
        "FYRALIS_BYOC_REGION",
        "FYRALIS_DATA_PLANE_AGENT_INSTALL_TOKEN_SECRET_REF",
        "FYRALIS_DATA_PLANE_AGENT_CLIENT_CERT_SECRET_REF",
        "FYRALIS_BYOC_EVIDENCE_INTAKE_KEY_REF",
        "FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY_SECRET_REF",
        "FYRALIS_BYOC_EVIDENCE_READ_KEY_REF",
        "FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY_SECRET_REF",
    }
)

_FIGMA_OAUTH_CALLBACK_PATH = "/integrations/figma/oauth/callback"
_FIGMA_OAUTH_REQUIRED_SCOPES = frozenset(
    {
        "current_user:read",
        "file_metadata:read",
        "file_content:read",
        "file_comments:read",
        "file_versions:read",
    }
)


def _figma_oauth_url_violation(
    *,
    path: Path,
    entry: "EnvEntry | None",
    key: str,
    callback: bool = False,
) -> EnvContractViolation | None:
    """Validate non-secret BYOC OAuth URLs without normalizing them.

    Figma compares redirect URLs exactly, so a trailing slash or query string
    must be caught in deployment configuration instead of after a user reaches
    the provider consent screen.
    """
    if entry is None or not entry.value:
        return EnvContractViolation(
            path=path,
            key=key,
            line_number=entry.line_number if entry else None,
            message="must be configured when FIGMA_OAUTH_ENABLED=1 in BYOC production",
        )
    parsed = urlparse(entry.value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.params
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        return EnvContractViolation(
            path=path,
            key=key,
            line_number=entry.line_number,
            message="must be an absolute https URL without credentials, query, or fragment",
        )
    if callback and parsed.path != _FIGMA_OAUTH_CALLBACK_PATH:
        return EnvContractViolation(
            path=path,
            key=key,
            line_number=entry.line_number,
            message=(
                "must use the exact Figma callback path "
                f"{_FIGMA_OAUTH_CALLBACK_PATH!r}"
            ),
        )
    return None


def _append_byoc_figma_oauth_violations(
    *,
    path: Path,
    values_by_key: dict[str, "EnvEntry"],
    violations: list["EnvContractViolation"],
) -> None:
    """Enforce the one-app-per-BYOC-deployment Figma contract when enabled."""
    enabled = values_by_key.get("FIGMA_OAUTH_ENABLED")
    if enabled is None or enabled.value != "1":
        return

    client_id = values_by_key.get("FIGMA_CLIENT_ID")
    if client_id is None or not client_id.value:
        violations.append(
            EnvContractViolation(
                path=path,
                key="FIGMA_CLIENT_ID",
                line_number=client_id.line_number if client_id else None,
                message="must be configured when FIGMA_OAUTH_ENABLED=1 in BYOC production",
            )
        )

    for key, callback in (
        ("FIGMA_REDIRECT_URI", True),
        ("FIGMA_OAUTH_UI_BASE_URL", False),
    ):
        violation = _figma_oauth_url_violation(
            path=path,
            entry=values_by_key.get(key),
            key=key,
            callback=callback,
        )
        if violation is not None:
            violations.append(violation)

    scopes_entry = values_by_key.get("FIGMA_OAUTH_SCOPES")
    scopes = (
        {value for value in re.split(r"[\s,]+", scopes_entry.value) if value}
        if scopes_entry is not None
        else set()
    )
    if scopes != _FIGMA_OAUTH_REQUIRED_SCOPES:
        violations.append(
            EnvContractViolation(
                path=path,
                key="FIGMA_OAUTH_SCOPES",
                line_number=scopes_entry.line_number if scopes_entry else None,
                message=(
                    "must contain exactly the enabled snapshot scopes: "
                    + ", ".join(sorted(_FIGMA_OAUTH_REQUIRED_SCOPES))
                ),
            )
        )


@dataclass(frozen=True)
class EnvEntry:
    key: str
    value: str
    line_number: int


@dataclass(frozen=True)
class EnvContractViolation:
    path: Path
    key: str
    message: str
    line_number: int | None = None

    def render(self) -> str:
        location = f":{self.line_number}" if self.line_number is not None else ""
        return f"{self.path}{location}: {self.key}: {self.message}"


def _strip_inline_comment(value: str) -> str:
    quote: str | None = None
    for idx, char in enumerate(value):
        if char in {"'", '"'}:
            quote = None if quote == char else char
        elif char == "#" and quote is None:
            return value[:idx].rstrip()
    return value.rstrip()


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def parse_env_template(path: Path) -> list[EnvEntry]:
    entries: list[EnvEntry] = []
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = _unquote(_strip_inline_comment(raw_value.strip()))
        if key:
            entries.append(EnvEntry(key=key, value=value, line_number=line_number))
    return entries


def check_env_contract(path: Path = DEFAULT_ENV_TEMPLATE) -> list[EnvContractViolation]:
    entries = parse_env_template(path)
    values_by_key: dict[str, EnvEntry] = {}
    duplicates: dict[str, list[EnvEntry]] = {}

    for entry in entries:
        if entry.key in values_by_key:
            duplicates.setdefault(entry.key, [values_by_key[entry.key]]).append(entry)
        else:
            values_by_key[entry.key] = entry

    violations: list[EnvContractViolation] = []
    for key in sorted(REQUIRED_KEYS - values_by_key.keys()):
        violations.append(
            EnvContractViolation(
                path=path,
                key=key,
                message="required production key is missing from the template",
            )
        )

    for key in sorted(FORBIDDEN_KEYS & values_by_key.keys()):
        entry = values_by_key[key]
        violations.append(
            EnvContractViolation(
                path=path,
                key=key,
                line_number=entry.line_number,
                message="must not be present in the production template",
            )
        )

    for key, entries_for_key in sorted(duplicates.items()):
        line_numbers = ", ".join(str(entry.line_number) for entry in entries_for_key)
        violations.append(
            EnvContractViolation(
                path=path,
                key=key,
                line_number=entries_for_key[-1].line_number,
                message=f"duplicate definition; first/duplicate lines: {line_numbers}",
            )
        )

    for key, expected in sorted(REQUIRED_EXACT_VALUES.items()):
        exact_entry = values_by_key.get(key)
        if exact_entry is None:
            continue
        if exact_entry.value != expected:
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    line_number=exact_entry.line_number,
                    message=f"expected {expected!r}, found {exact_entry.value!r}",
                )
            )

    for key, allowed_values in sorted(REQUIRED_ALLOWED_VALUES.items()):
        allowed_entry = values_by_key.get(key)
        if allowed_entry is None:
            continue
        if allowed_entry.value not in allowed_values:
            allowed = ", ".join(repr(value) for value in sorted(allowed_values))
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    line_number=allowed_entry.line_number,
                    message=(
                        f"expected one of {allowed}; found {allowed_entry.value!r}"
                    ),
                )
            )

    for key, forbidden_values in sorted(FORBIDDEN_EXACT_VALUES.items()):
        forbidden_entry = values_by_key.get(key)
        if forbidden_entry is None:
            continue
        if forbidden_entry.value in forbidden_values:
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    line_number=forbidden_entry.line_number,
                    message=(
                        f"must not use known unsafe value " f"{forbidden_entry.value!r}"
                    ),
                )
            )

    for key in sorted(REQUIRED_BLANK_SECRET_PLACEHOLDER_KEYS):
        blank_secret_entry = values_by_key.get(key)
        if blank_secret_entry is None:
            continue
        if blank_secret_entry.value:
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    line_number=blank_secret_entry.line_number,
                    message=(
                        "must stay blank in the checked-in production "
                        "template; inject the real secret through the runtime "
                        "secret mechanism"
                    ),
                )
            )

    for key in sorted(REQUIRED_NONEMPTY_SECRET_REF_KEYS):
        secret_ref_entry = values_by_key.get(key)
        if secret_ref_entry is None:
            continue
        if not secret_ref_entry.value:
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    line_number=secret_ref_entry.line_number,
                    message=(
                        "must point at a managed secret reference in the "
                        "checked-in production template; keep the raw "
                        "companion secret key blank"
                    ),
                )
            )

    for key in sorted(REQUIRED_POSITIVE_INTEGER_KEYS):
        integer_entry = values_by_key.get(key)
        if integer_entry is None:
            continue
        try:
            int_value = int(integer_entry.value)
        except ValueError:
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    line_number=integer_entry.line_number,
                    message=(
                        "must be a positive integer number of milliseconds; "
                        f"found {integer_entry.value!r}"
                    ),
                )
            )
            continue
        if int_value <= 0:
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    line_number=integer_entry.line_number,
                    message=(
                        "must be a positive integer number of milliseconds; "
                        f"found {integer_entry.value!r}"
                    ),
                )
            )

    for key in sorted(REQUIRED_POSITIVE_NUMBER_KEYS):
        number_entry = values_by_key.get(key)
        if number_entry is None:
            continue
        try:
            number_value = float(number_entry.value)
        except ValueError:
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    line_number=number_entry.line_number,
                    message=f"must be a positive number; found {number_entry.value!r}",
                )
            )
            continue
        if number_value <= 0:
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    line_number=number_entry.line_number,
                    message=f"must be a positive number; found {number_entry.value!r}",
                )
            )

    deployment_mode = values_by_key.get("FYRALIS_DEPLOYMENT_MODE")
    if deployment_mode is not None and deployment_mode.value == "byoc":
        for key in sorted(BYOC_REQUIRED_KEYS - values_by_key.keys()):
            violations.append(
                EnvContractViolation(
                    path=path,
                    key=key,
                    message=(
                        "required BYOC production key is missing from the template"
                    ),
                )
            )

        for key, expected in sorted(BYOC_REQUIRED_EXACT_VALUES.items()):
            exact_entry = values_by_key.get(key)
            if exact_entry is None:
                continue
            if exact_entry.value != expected:
                violations.append(
                    EnvContractViolation(
                        path=path,
                        key=key,
                        line_number=exact_entry.line_number,
                        message=f"expected {expected!r}, found {exact_entry.value!r}",
                    )
                )

        for key, allowed_values in sorted(BYOC_REQUIRED_ALLOWED_VALUES.items()):
            allowed_entry = values_by_key.get(key)
            if allowed_entry is None:
                continue
            if allowed_entry.value not in allowed_values:
                allowed = ", ".join(repr(value) for value in sorted(allowed_values))
                violations.append(
                    EnvContractViolation(
                        path=path,
                        key=key,
                        line_number=allowed_entry.line_number,
                        message=(
                            f"expected one of {allowed}; found {allowed_entry.value!r}"
                        ),
                    )
                )

        for key in sorted(BYOC_REQUIRED_NONEMPTY_KEYS):
            nonempty_entry = values_by_key.get(key)
            if nonempty_entry is None:
                continue
            if not nonempty_entry.value:
                violations.append(
                    EnvContractViolation(
                        path=path,
                        key=key,
                        line_number=nonempty_entry.line_number,
                        message="must not be blank for BYOC production",
                    )
                )

        control_plane_url = values_by_key.get("FYRALIS_CONTROL_PLANE_URL")
        if control_plane_url is not None:
            parsed = urlparse(control_plane_url.value)
            if parsed.scheme != "https" or not parsed.netloc:
                violations.append(
                    EnvContractViolation(
                        path=path,
                        key="FYRALIS_CONTROL_PLANE_URL",
                        line_number=control_plane_url.line_number,
                        message="must be an https URL for BYOC production",
                    )
                )
            elif parsed.username or parsed.password:
                violations.append(
                    EnvContractViolation(
                        path=path,
                        key="FYRALIS_CONTROL_PLANE_URL",
                        line_number=control_plane_url.line_number,
                        message="must not contain credentials",
                    )
                )

        _append_byoc_figma_oauth_violations(
            path=path,
            values_by_key=values_by_key,
            violations=violations,
        )

    return violations


def _render_violations(violations: Iterable[EnvContractViolation]) -> str:
    lines = ["Production environment contract violations:"]
    lines.extend(f"  {violation.render()}" for violation in violations)
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-template",
        type=Path,
        default=DEFAULT_ENV_TEMPLATE,
        help="Production env template to validate.",
    )
    args = parser.parse_args(argv)

    violations = check_env_contract(args.env_template)
    if violations:
        print(_render_violations(violations), file=sys.stderr)
        return 1
    print("Production environment contract passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
