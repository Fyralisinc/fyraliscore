from __future__ import annotations

from pathlib import Path

from scripts.check_production_env_contract import (
    DEFAULT_ENV_TEMPLATE,
    REQUIRED_ALLOWED_VALUES,
    REQUIRED_BLANK_SECRET_PLACEHOLDER_KEYS,
    FORBIDDEN_EXACT_VALUES,
    REQUIRED_POSITIVE_INTEGER_KEYS,
    REQUIRED_EXACT_VALUES,
    REQUIRED_KEYS,
    REQUIRED_POSITIVE_NUMBER_KEYS,
    check_env_contract,
)


def _write_template(path: Path, *, overrides: dict[str, str] | None = None) -> None:
    overrides = overrides or {}
    lines: list[str] = []
    for key in sorted(REQUIRED_KEYS):
        if key in REQUIRED_POSITIVE_INTEGER_KEYS:
            default = "1000"
        elif key in REQUIRED_POSITIVE_NUMBER_KEYS:
            default = "30"
        else:
            default = "placeholder"
        if key == "SECRET_STORE_BACKEND":
            default = "fernet"
        elif key == "MASTER_KEK_PROVIDER":
            default = "aws-secrets-manager"
        elif key in REQUIRED_BLANK_SECRET_PLACEHOLDER_KEYS:
            default = ""
        elif key in FORBIDDEN_EXACT_VALUES:
            default = "safe-placeholder-value"
        elif key == "SECRET_PROVIDER_TIMEOUT_SECONDS":
            default = "5"
        elif key in REQUIRED_ALLOWED_VALUES:
            default = sorted(REQUIRED_ALLOWED_VALUES[key])[0]
        value = overrides.get(key, REQUIRED_EXACT_VALUES.get(key, default))
        lines.append(f"{key}={value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_checked_in_production_env_template_satisfies_contract() -> None:
    assert check_env_contract(DEFAULT_ENV_TEMPLATE) == []


def test_env_contract_reports_missing_required_keys(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template)
    lines = [
        line
        for line in template.read_text(encoding="utf-8").splitlines()
        if not line.startswith("QUERY_RENDERING_BASE_URL=")
    ]
    template.write_text("\n".join(lines) + "\n", encoding="utf-8")

    violations = check_env_contract(template)

    assert [violation.key for violation in violations] == ["QUERY_RENDERING_BASE_URL"]


def test_env_contract_reports_unsafe_exact_values(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template, overrides={"QUERY_CACHE_BACKEND": "memory"})

    violations = check_env_contract(template)

    assert [(violation.key, violation.message) for violation in violations] == [
        ("QUERY_CACHE_BACKEND", "expected 'pg', found 'memory'")
    ]


def test_env_contract_reports_invalid_positive_integer_values(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template, overrides={"DB_STATEMENT_TIMEOUT_MS": "0"})

    violations = check_env_contract(template)

    assert [(violation.key, violation.message) for violation in violations] == [
        (
            "DB_STATEMENT_TIMEOUT_MS",
            "must be a positive integer number of milliseconds; found '0'",
        )
    ]


def test_env_contract_reports_invalid_positive_number_values(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(
        template,
        overrides={"SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC": "0"},
    )

    violations = check_env_contract(template)

    assert [(violation.key, violation.message) for violation in violations] == [
        (
            "SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC",
            "must be a positive number; found '0'",
        )
    ]


def test_env_contract_reports_invalid_allowed_values(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template, overrides={"MASTER_KEK_PROVIDER": "local-file"})

    violations = check_env_contract(template)

    assert len(violations) == 1
    assert violations[0].key == "MASTER_KEK_PROVIDER"
    assert "expected one of" in violations[0].message


def test_env_contract_reports_forbidden_exact_values(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template, overrides={"GRAFANA_ADMIN_PASSWORD": "fyralis-admin"})

    violations = check_env_contract(template)

    assert len(violations) == 1
    assert violations[0].key == "GRAFANA_ADMIN_PASSWORD"
    assert "known unsafe value" in violations[0].message


def test_env_contract_reports_nonempty_raw_secret_placeholder(
    tmp_path: Path,
) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template, overrides={"SLACK_CLIENT_SECRET": "raw-secret"})

    violations = check_env_contract(template)

    assert len(violations) == 1
    assert violations[0].key == "SLACK_CLIENT_SECRET"
    assert "must stay blank" in violations[0].message


def test_env_contract_reports_invalid_pgbouncer_flag(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(
        template,
        overrides={"POSTGRES_PGBOUNCER_COMPATIBLE": "true"},
    )

    violations = check_env_contract(template)

    assert len(violations) == 1
    assert violations[0].key == "POSTGRES_PGBOUNCER_COMPATIBLE"
    assert "expected one of" in violations[0].message


def test_env_contract_reports_duplicate_keys(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template)
    with template.open("a", encoding="utf-8") as fh:
        fh.write("LLM_PROVIDER=codex\n")

    violations = check_env_contract(template)

    assert len(violations) == 1
    assert violations[0].key == "LLM_PROVIDER"
    assert "duplicate definition" in violations[0].message


def test_env_contract_reports_forbidden_tenant_fallback(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template)
    with template.open("a", encoding="utf-8") as fh:
        fh.write("DEFAULT_TENANT_ID=00000000-0000-0000-0000-000000000001\n")

    violations = check_env_contract(template)

    assert len(violations) == 1
    assert violations[0].key == "DEFAULT_TENANT_ID"
    assert "must not be present" in violations[0].message


def test_env_contract_reports_forbidden_actor_fallback(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template)
    with template.open("a", encoding="utf-8") as fh:
        fh.write("DEFAULT_ACTOR_ID=00000000-0000-0000-0000-000000000001\n")

    violations = check_env_contract(template)

    assert len(violations) == 1
    assert violations[0].key == "DEFAULT_ACTOR_ID"
    assert "must not be present" in violations[0].message


def test_env_contract_reports_forbidden_whatsapp_debug_secrets(
    tmp_path: Path,
) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template)
    with template.open("a", encoding="utf-8") as fh:
        fh.write("WHATSAPP_VERIFY_TOKEN=debug-token\n")
        fh.write("WHATSAPP_ALLOW_UNSIGNED=1\n")

    violations = check_env_contract(template)

    assert [violation.key for violation in violations] == [
        "WHATSAPP_ALLOW_UNSIGNED",
        "WHATSAPP_VERIFY_TOKEN",
    ]
    assert all("must not be present" in violation.message for violation in violations)


def test_env_contract_reports_forbidden_raw_master_kek(tmp_path: Path) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template)
    with template.open("a", encoding="utf-8") as fh:
        fh.write("MASTER_KEK=raw-fernet-key-must-live-in-managed-provider\n")

    violations = check_env_contract(template)

    assert len(violations) == 1
    assert violations[0].key == "MASTER_KEK"
    assert "must not be present" in violations[0].message


def test_env_contract_reports_forbidden_inline_gmail_service_account_json(
    tmp_path: Path,
) -> None:
    template = tmp_path / ".env.production.example"
    _write_template(template)
    with template.open("a", encoding="utf-8") as fh:
        fh.write('GMAIL_SERVICE_ACCOUNT_JSON={"private_key":"raw-key"}\n')

    violations = check_env_contract(template)

    assert len(violations) == 1
    assert violations[0].key == "GMAIL_SERVICE_ACCOUNT_JSON"
    assert "must not be present" in violations[0].message
