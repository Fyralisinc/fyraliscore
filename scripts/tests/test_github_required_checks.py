from __future__ import annotations

import json
from pathlib import Path

from scripts.check_github_required_checks import (
    ProtectionState,
    RequiredChecksPolicy,
    RequiredCheck,
    _extract_required_status_checks,
    _extract_ruleset_required_checks,
    load_policy,
    merge_protection_states,
    validate_local_policy,
    validate_protection_state,
)


def test_checked_in_required_checks_match_workflow_job_names() -> None:
    policy = load_policy()

    assert validate_local_policy(policy) == []


def test_local_policy_flags_missing_workflow_context(tmp_path: Path) -> None:
    workflow = tmp_path / ".github" / "workflows" / "ci.yml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text(
        """
name: CI
jobs:
  tests:
    name: Tests
""".lstrip(),
        encoding="utf-8",
    )
    policy = RequiredChecksPolicy(
        branch="main",
        require_strict_status_checks=True,
        required_checks=(
            RequiredCheck(
                context="Security scan + SBOM",
                workflow_file=Path(".github/workflows/ci.yml"),
            ),
        ),
    )

    violations = validate_local_policy(policy, repo_root=tmp_path)

    assert len(violations) == 1
    assert "Security scan + SBOM" in violations[0]


def test_load_policy_rejects_duplicate_contexts(tmp_path: Path) -> None:
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "branch": "main",
                "required_checks": [
                    {
                        "workflow_file": ".github/workflows/ci.yml",
                        "context": "Tests",
                    },
                    {
                        "workflow_file": ".github/workflows/ci.yml",
                        "context": "Tests",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    try:
        load_policy(policy_path)
    except ValueError as exc:
        assert "duplicate" in str(exc)
    else:  # pragma: no cover - makes the assertion message clearer.
        raise AssertionError("duplicate context policy was accepted")


def test_classic_branch_protection_missing_required_check_is_violation() -> None:
    policy = RequiredChecksPolicy(
        branch="main",
        require_strict_status_checks=True,
        required_checks=(
            RequiredCheck(context="Tests", workflow_file=Path("ci.yml")),
            RequiredCheck(context="Security scan + SBOM", workflow_file=Path("ci.yml")),
        ),
    )
    state = _extract_required_status_checks(
        {
            "strict": True,
            "contexts": ["Tests"],
            "checks": [],
        },
        source="classic",
    )

    violations = validate_protection_state(policy, state)

    assert violations == ["missing required status checks for main: Security scan + SBOM"]


def test_ruleset_required_checks_cover_default_branch() -> None:
    policy = RequiredChecksPolicy(
        branch="main",
        require_strict_status_checks=True,
        required_checks=(
            RequiredCheck(context="Tests", workflow_file=Path("ci.yml")),
        ),
    )
    state = _extract_ruleset_required_checks(
        [
            {
                "target": "branch",
                "enforcement": "active",
                "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"]}},
                "rules": [
                    {
                        "type": "required_status_checks",
                        "parameters": {
                            "strict_required_status_checks_policy": True,
                            "required_status_checks": [{"context": "Tests"}],
                        },
                    }
                ],
            }
        ],
        branch="main",
    )

    assert validate_protection_state(policy, state) == []


def test_protection_state_requires_strict_status_checks() -> None:
    policy = RequiredChecksPolicy(
        branch="main",
        require_strict_status_checks=True,
        required_checks=(
            RequiredCheck(context="Tests", workflow_file=Path("ci.yml")),
        ),
    )
    state = ProtectionState(
        contexts={"Tests"},
        strict_status_checks=False,
        source="classic",
    )

    violations = validate_protection_state(policy, state)

    assert violations == [
        "main: strict required-status-check updates are not enabled",
    ]


def test_merge_protection_states_accepts_split_classic_and_ruleset() -> None:
    policy = RequiredChecksPolicy(
        branch="main",
        require_strict_status_checks=True,
        required_checks=(
            RequiredCheck(context="Tests", workflow_file=Path("ci.yml")),
            RequiredCheck(context="Security scan + SBOM", workflow_file=Path("ci.yml")),
        ),
    )
    merged = merge_protection_states(
        (
            ProtectionState({"Tests"}, False, "classic"),
            ProtectionState({"Security scan + SBOM"}, True, "rulesets"),
        )
    )

    assert validate_protection_state(policy, merged) == []
