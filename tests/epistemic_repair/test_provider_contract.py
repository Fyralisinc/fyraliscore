from __future__ import annotations

import pytest

from lib.evaluation.epistemic_repair.provider_contract import (
    require_codex_cli_environment,
)


def test_active_proof_provider_contract_accepts_only_explicit_codex_cli() -> None:
    assert require_codex_cli_environment({
        "LLM_PROVIDER": "codex",
        "CODEX_TRANSPORT": "cli",
    }) == {"LLM_PROVIDER": "codex", "CODEX_TRANSPORT": "cli"}


@pytest.mark.parametrize("environment", [
    {},
    {"LLM_PROVIDER": "deepseek", "CODEX_TRANSPORT": "cli"},
    {"LLM_PROVIDER": "codex", "CODEX_TRANSPORT": "app_server"},
])
def test_active_proof_provider_contract_fails_closed(
    environment: dict[str, str],
) -> None:
    with pytest.raises(RuntimeError, match="requires exact environment"):
        require_codex_cli_environment(environment)
