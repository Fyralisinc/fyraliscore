from __future__ import annotations

import pytest

from lib.shared.errors import InvariantViolation
from services.reasoning.think.execution_policy import (
    NORMAL_EXECUTION_POLICY,
    STAGE1_COMPANY_MEMORY_POLICY,
    ThinkExecutionPolicy,
    issue_evaluation_validate_only_policy,
)


def test_validate_only_requires_unforgeable_explicit_capability() -> None:
    NORMAL_EXECUTION_POLICY.assert_authorized()
    issue_evaluation_validate_only_policy().assert_authorized()
    with pytest.raises(InvariantViolation, match="explicit evaluation-control"):
        ThinkExecutionPolicy(
            mode="validate_only", authority="evaluation_control"
        ).assert_authorized()


def test_no_environment_switch_can_enable_validate_only(monkeypatch) -> None:
    monkeypatch.setenv("THINK_VALIDATE_ONLY", "1")
    assert NORMAL_EXECUTION_POLICY.mode == "normal"


def test_stage1_policy_is_applying_but_restricts_composition() -> None:
    STAGE1_COMPANY_MEMORY_POLICY.assert_authorized()
    assert STAGE1_COMPANY_MEMORY_POLICY.mode == "normal"
    assert STAGE1_COMPANY_MEMORY_POLICY.is_stage1_company_memory is True
    assert NORMAL_EXECUTION_POLICY.is_stage1_company_memory is False
