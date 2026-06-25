#!/usr/bin/env python3
"""Validate the production environment template contract.

The runtime already fails closed for several missing production settings. This
static check keeps `.env.production.example` aligned with those fail-closed
paths so operators see the required keys before deploy time.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_TEMPLATE = REPO_ROOT / ".env.production.example"

REQUIRED_KEYS = frozenset(
    {
        "COMPANY_OS_ENV",
        "FYRALIS_ENV",
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
        "CODEX_API_KEY",
        "CODEX_TRANSPORT",
        "CODEX_MODEL",
        "CODEX_REASONING_EFFORT",
        "INQUIRY_CODEX_QUESTION_MODEL",
        "THINK_NARROW_INFERENTIAL_TX",
        "SECRET_STORE_BACKEND",
        "MASTER_KEK_PROVIDER",
        "MASTER_KEK_SECRET_REF",
        "SECRET_PROVIDER_REGION",
        "SECRET_PROVIDER_TIMEOUT_SECONDS",
        "OAUTH_STATE_HMAC_KEY",
        "AUTH_BOOTSTRAP_SECRET",
        "DEBUG_ENDPOINTS_ENABLED",
        "FINANCE_PANEL_ENABLED",
        "SLACK_DM_PANEL_ENABLED",
        "SPEC_DEMO_ROUTES_ENABLED",
        "WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED",
        "WEBSOCKET_SESSION_COOKIE_NAME",
        "VIEW_CEO_STATIC_TOKENS_ENABLED",
        "GATEWAY_MOUNT_SIM",
        "WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW",
        "KAFKA_BOOTSTRAP_SERVERS",
        "GATEWAY_REQUIRE_INGESTION_DATA_PLANE",
        "S3_RAW_BUCKET",
        "S3_REGION_NAME",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "EMBEDDER_BACKEND",
        "GITHUB_APP_ID",
        "GITHUB_APP_SLUG",
        "GITHUB_APP_PRIVATE_KEY",
        "WEBHOOK_SECRET_GITHUB",
        "SLACK_CLIENT_ID",
        "SLACK_CLIENT_SECRET",
        "SLACK_REDIRECT_URI",
        "SLACK_SIGNING_SECRET",
        "DISCORD_BOT_TOKEN",
        "DISCORD_APPLICATION_ID",
        "DISCORD_CLIENT_ID",
        "DISCORD_CLIENT_SECRET",
        "DISCORD_REDIRECT_URI",
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
        "WEBHOOK_TENANT_DEFAULT_ALLOW",
        "LOG_LEVEL",
        "DEBUG_ARTIFACT_CAPTURE",
        "SHARD_FETCH_RATE_LIMIT",
        "SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC",
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
}
REQUIRED_ALLOWED_VALUES = {
    "SECRET_STORE_BACKEND": {"fernet"},
    "MASTER_KEK_PROVIDER": {
        "aws-secrets-manager",
        "gcp-secret-manager",
        "hashicorp-vault",
    },
    "POSTGRES_PGBOUNCER_COMPATIBLE": {"0", "1"},
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
        "GITHUB_APP_PRIVATE_KEY",
        "OAUTH_STATE_HMAC_KEY",
        "SLACK_CLIENT_SECRET",
        "SLACK_SIGNING_SECRET",
        "WEBHOOK_SECRET_DISCORD",
        "WEBHOOK_SECRET_GITHUB",
    }
)
FORBIDDEN_KEYS = frozenset(
    {
        "DEFAULT_ACTOR_ID",
        "DEFAULT_TENANT_ID",
        "COMPANY_OS_TENANT_ID",
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
    }
)
REQUIRED_POSITIVE_NUMBER_KEYS = frozenset(
    {
        "SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC",
    }
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
                        f"must not use known unsafe value "
                        f"{forbidden_entry.value!r}"
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
