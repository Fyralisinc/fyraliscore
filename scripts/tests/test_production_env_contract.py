from __future__ import annotations

from pathlib import Path

from scripts.check_production_env_contract import (
    DEFAULT_ENV_TEMPLATE,
    REQUIRED_EXACT_VALUES,
    REQUIRED_KEYS,
    check_env_contract,
)


def _write_template(path: Path, *, overrides: dict[str, str] | None = None) -> None:
    overrides = overrides or {}
    lines: list[str] = []
    for key in sorted(REQUIRED_KEYS):
        value = overrides.get(key, REQUIRED_EXACT_VALUES.get(key, "placeholder"))
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
