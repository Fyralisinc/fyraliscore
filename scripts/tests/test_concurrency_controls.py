from __future__ import annotations

from pathlib import Path

from scripts.check_concurrency_controls import validate_concurrency_controls
from scripts.check_production_env_contract import (
    DEFAULT_ENV_TEMPLATE,
    REQUIRED_EXACT_VALUES,
    REQUIRED_KEYS,
    REQUIRED_POSITIVE_INTEGER_KEYS,
)
from services.platform.performance.concurrency_controls import (
    CONCURRENCY_CONTROLS,
    EXPENSIVE_WORKER_GATES,
)


def test_checked_in_concurrency_controls_match_env_contract() -> None:
    assert validate_concurrency_controls() == []


def test_every_registered_concurrency_control_has_unique_env_key() -> None:
    keys = [control.env_key for control in CONCURRENCY_CONTROLS]

    assert len(keys) == len(set(keys))


def test_concurrency_control_check_flags_missing_required_key() -> None:
    first = CONCURRENCY_CONTROLS[0]

    violations = validate_concurrency_controls(
        required_keys=frozenset(REQUIRED_KEYS - {first.env_key}),
    )

    assert f"{first.env_key}: missing from REQUIRED_KEYS" in violations


def test_concurrency_control_check_flags_nonpositive_template_value(
    tmp_path: Path,
) -> None:
    template = tmp_path / ".env.production.example"
    template.write_text(DEFAULT_ENV_TEMPLATE.read_text(encoding="utf-8"))
    first = CONCURRENCY_CONTROLS[0]
    template.write_text(
        template.read_text(encoding="utf-8").replace(
            f"{first.env_key}={first.default_value}",
            f"{first.env_key}=0",
        ),
        encoding="utf-8",
    )

    violations = validate_concurrency_controls(env_template=template)

    assert f"{first.env_key}: env value must be positive" in violations


def test_expensive_worker_gate_defaults_closed() -> None:
    for env_key, expected in EXPENSIVE_WORKER_GATES.items():
        assert REQUIRED_EXACT_VALUES[env_key] == expected
        assert env_key not in REQUIRED_POSITIVE_INTEGER_KEYS
