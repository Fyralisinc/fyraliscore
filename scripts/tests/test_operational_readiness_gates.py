from __future__ import annotations

import argparse

from scripts.run_operational_readiness_gates import (
    PASS,
    _production_env_contract_gate,
)


def test_production_env_contract_gate_passes_for_checked_in_template() -> None:
    args = argparse.Namespace(command_timeout_s=30)

    result = _production_env_contract_gate(args)

    assert result.status == PASS
    assert result.command is not None
    assert result.command[-1] == "scripts/check_production_env_contract.py"
