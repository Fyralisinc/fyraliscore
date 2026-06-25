#!/usr/bin/env python3
"""Verify Fyralis' required GitHub checks policy.

The local check keeps the checked-in required-check policy aligned with the
workflow job names that GitHub exposes as status-check contexts. The optional
live check verifies that GitHub branch protection or an active repository
ruleset requires those contexts for the target branch.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY_PATH = REPO_ROOT / ".github" / "main-required-checks.json"
GITHUB_API_VERSION = "2022-11-28"
DEFAULT_TIMEOUT_SECONDS = 20


@dataclass(frozen=True)
class RequiredCheck:
    context: str
    workflow_file: Path


@dataclass(frozen=True)
class RequiredChecksPolicy:
    branch: str
    require_strict_status_checks: bool
    required_checks: tuple[RequiredCheck, ...]

    @property
    def required_contexts(self) -> set[str]:
        return {check.context for check in self.required_checks}


@dataclass(frozen=True)
class ProtectionState:
    contexts: set[str]
    strict_status_checks: bool
    source: str


def _load_json_file(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _clean_yaml_scalar(value: str) -> str:
    value = value.strip()
    if (
        len(value) >= 2
        and value[0] == value[-1]
        and value[0] in {"'", '"'}
    ):
        return value[1:-1]
    return value


def _job_names_from_workflow(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    names: set[str] = set()
    for line in text.splitlines():
        match = re.match(r"^ {4}name:\s*(?P<name>.+?)\s*$", line)
        if match:
            names.add(_clean_yaml_scalar(match.group("name")))
    return names


def load_policy(path: Path = DEFAULT_POLICY_PATH) -> RequiredChecksPolicy:
    data = _load_json_file(path)
    if not isinstance(data, dict):
        raise ValueError("required-check policy must be a JSON object")
    branch = data.get("branch")
    if not isinstance(branch, str) or not branch:
        raise ValueError("required-check policy must set a non-empty branch")
    require_strict = data.get("require_strict_status_checks", True)
    if not isinstance(require_strict, bool):
        raise ValueError("require_strict_status_checks must be a boolean")
    raw_checks = data.get("required_checks")
    if not isinstance(raw_checks, list) or not raw_checks:
        raise ValueError("required_checks must be a non-empty list")

    checks: list[RequiredCheck] = []
    seen: set[str] = set()
    for index, raw_check in enumerate(raw_checks, start=1):
        if not isinstance(raw_check, dict):
            raise ValueError(f"required_checks[{index}] must be an object")
        context = raw_check.get("context")
        workflow_file = raw_check.get("workflow_file")
        if not isinstance(context, str) or not context:
            raise ValueError(f"required_checks[{index}].context must be non-empty")
        if not isinstance(workflow_file, str) or not workflow_file:
            raise ValueError(
                f"required_checks[{index}].workflow_file must be non-empty"
            )
        if context in seen:
            raise ValueError(f"duplicate required check context: {context}")
        seen.add(context)
        checks.append(RequiredCheck(context=context, workflow_file=Path(workflow_file)))
    return RequiredChecksPolicy(
        branch=branch,
        require_strict_status_checks=require_strict,
        required_checks=tuple(checks),
    )


def validate_local_policy(
    policy: RequiredChecksPolicy,
    *,
    repo_root: Path = REPO_ROOT,
) -> list[str]:
    """Return policy/workflow alignment violations."""

    violations: list[str] = []
    for check in policy.required_checks:
        workflow_path = repo_root / check.workflow_file
        if not workflow_path.exists():
            violations.append(f"{check.workflow_file}: workflow file does not exist")
            continue
        declared_names = _job_names_from_workflow(workflow_path)
        if check.context not in declared_names:
            available = ", ".join(sorted(declared_names)) or "<none>"
            violations.append(
                f"{check.workflow_file}: required context {check.context!r} is "
                f"not declared as a workflow job name; available: {available}"
            )
    return violations


def _extract_required_status_checks(data: Any, *, source: str) -> ProtectionState:
    if not isinstance(data, dict):
        return ProtectionState(contexts=set(), strict_status_checks=False, source=source)
    section = data.get("required_status_checks")
    if isinstance(section, dict):
        data = section
    contexts = {
        str(context)
        for context in data.get("contexts", [])
        if isinstance(context, str) and context
    }
    for check in data.get("checks", []):
        if isinstance(check, dict) and isinstance(check.get("context"), str):
            contexts.add(check["context"])
    return ProtectionState(
        contexts=contexts,
        strict_status_checks=bool(data.get("strict")),
        source=source,
    )


def _ruleset_targets_branch(ruleset: dict[str, Any], branch: str) -> bool:
    conditions = ruleset.get("conditions")
    if not isinstance(conditions, dict):
        return False
    ref_name = conditions.get("ref_name")
    if not isinstance(ref_name, dict):
        return False
    includes = ref_name.get("include") or []
    if not isinstance(includes, list):
        return False
    accepted = {
        "*",
        "~DEFAULT_BRANCH",
        branch,
        f"refs/heads/{branch}",
        f"heads/{branch}",
    }
    return any(isinstance(item, str) and item in accepted for item in includes)


def _extract_ruleset_required_checks(data: Any, *, branch: str) -> ProtectionState:
    rulesets: Iterable[Any]
    if isinstance(data, list):
        rulesets = data
    elif isinstance(data, dict):
        rulesets = [data]
    else:
        rulesets = []

    contexts: set[str] = set()
    strict = False
    for item in rulesets:
        if not isinstance(item, dict):
            continue
        if item.get("enforcement") != "active":
            continue
        if item.get("target") not in {None, "branch"}:
            continue
        if not _ruleset_targets_branch(item, branch):
            continue
        rules = item.get("rules") or []
        if not isinstance(rules, list):
            continue
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            if rule.get("type") != "required_status_checks":
                continue
            parameters = rule.get("parameters") or {}
            if not isinstance(parameters, dict):
                continue
            strict = strict or bool(
                parameters.get("strict_required_status_checks_policy")
            )
            required = parameters.get("required_status_checks") or []
            if not isinstance(required, list):
                continue
            for check in required:
                if isinstance(check, dict) and isinstance(check.get("context"), str):
                    contexts.add(check["context"])
    return ProtectionState(
        contexts=contexts,
        strict_status_checks=strict,
        source="repository rulesets",
    )


def merge_protection_states(states: Sequence[ProtectionState]) -> ProtectionState:
    contexts: set[str] = set()
    strict = False
    sources: list[str] = []
    for state in states:
        contexts.update(state.contexts)
        strict = strict or state.strict_status_checks
        if state.contexts:
            sources.append(state.source)
    return ProtectionState(
        contexts=contexts,
        strict_status_checks=strict,
        source=", ".join(sources) if sources else "no live protection source",
    )


def validate_protection_state(
    policy: RequiredChecksPolicy,
    state: ProtectionState,
) -> list[str]:
    violations: list[str] = []
    missing = sorted(policy.required_contexts - state.contexts)
    if missing:
        violations.append(
            "missing required status checks for "
            f"{policy.branch}: {', '.join(missing)}"
        )
    if policy.require_strict_status_checks and not state.strict_status_checks:
        violations.append(
            f"{policy.branch}: strict required-status-check updates are not enabled"
        )
    return violations


def _github_request_json(url: str, *, token: str, timeout_s: int) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": GITHUB_API_VERSION,
            "User-Agent": "fyralis-required-checks-verifier",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:
        return json.loads(response.read().decode("utf-8"))


def _optional_github_json(url: str, *, token: str, timeout_s: int) -> Any:
    try:
        return _github_request_json(url, token=token, timeout_s=timeout_s)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def _repo_from_git_remote(repo_root: Path) -> str | None:
    config = repo_root / ".git" / "config"
    if not config.exists():
        return None
    text = config.read_text(encoding="utf-8", errors="ignore")
    match = re.search(r"github\.com[:/](?P<repo>[^/\s]+/[^/\s.]+)(?:\.git)?", text)
    if match:
        return match.group("repo")
    return None


def fetch_live_protection_state(
    *,
    repo: str,
    branch: str,
    token: str,
    timeout_s: int = DEFAULT_TIMEOUT_SECONDS,
) -> ProtectionState:
    base = f"https://api.github.com/repos/{repo}"
    states: list[ProtectionState] = []

    classic = _optional_github_json(
        f"{base}/branches/{branch}/protection/required_status_checks",
        token=token,
        timeout_s=timeout_s,
    )
    if classic is not None:
        states.append(_extract_required_status_checks(classic, source="branch protection"))

    raw_rulesets = _optional_github_json(
        f"{base}/rulesets?targets=branch",
        token=token,
        timeout_s=timeout_s,
    )
    detailed_rulesets: list[Any] = []
    if isinstance(raw_rulesets, list):
        for ruleset in raw_rulesets:
            if not isinstance(ruleset, dict) or ruleset.get("enforcement") != "active":
                continue
            ruleset_id = ruleset.get("id")
            if ruleset_id is None:
                detailed_rulesets.append(ruleset)
                continue
            detailed = _optional_github_json(
                f"{base}/rulesets/{ruleset_id}",
                token=token,
                timeout_s=timeout_s,
            )
            detailed_rulesets.append(detailed or ruleset)
    if detailed_rulesets:
        states.append(
            _extract_ruleset_required_checks(detailed_rulesets, branch=branch)
        )
    return merge_protection_states(states)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument(
        "--live",
        action="store_true",
        help="Verify live GitHub branch protection/rulesets as well.",
    )
    parser.add_argument(
        "--repo",
        help="GitHub repository in owner/name form. Defaults to GITHUB_REPOSITORY.",
    )
    parser.add_argument(
        "--token-env",
        default="GITHUB_TOKEN",
        help="Environment variable that contains a GitHub token.",
    )
    parser.add_argument("--timeout-s", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--classic-protection-json",
        type=Path,
        help="Fixture JSON for classic branch protection required_status_checks.",
    )
    parser.add_argument(
        "--rulesets-json",
        type=Path,
        help="Fixture JSON for repository rulesets.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        policy = load_policy(args.policy)
    except Exception as exc:
        print(f"Invalid required-check policy: {exc}", file=sys.stderr)
        return 1

    violations = validate_local_policy(policy, repo_root=args.repo_root)
    fixture_states: list[ProtectionState] = []
    if args.classic_protection_json:
        fixture_states.append(
            _extract_required_status_checks(
                _load_json_file(args.classic_protection_json),
                source=str(args.classic_protection_json),
            )
        )
    if args.rulesets_json:
        fixture_states.append(
            _extract_ruleset_required_checks(
                _load_json_file(args.rulesets_json),
                branch=policy.branch,
            )
        )
    if fixture_states:
        violations.extend(
            validate_protection_state(policy, merge_protection_states(fixture_states))
        )

    if args.live:
        token = os.environ.get(args.token_env) or os.environ.get("GH_TOKEN")
        repo = args.repo or os.environ.get("GITHUB_REPOSITORY") or _repo_from_git_remote(
            args.repo_root
        )
        if not token:
            violations.append(
                f"--live requires {args.token_env} or GH_TOKEN with repository "
                "metadata/administration read access"
            )
        if not repo:
            violations.append(
                "--live requires --repo, GITHUB_REPOSITORY, or a github.com remote"
            )
        if token and repo:
            try:
                live_state = fetch_live_protection_state(
                    repo=repo,
                    branch=policy.branch,
                    token=token,
                    timeout_s=args.timeout_s,
                )
            except Exception as exc:
                violations.append(f"could not read GitHub branch protection: {exc}")
            else:
                violations.extend(validate_protection_state(policy, live_state))

    if violations:
        print("Required-check policy violations:", file=sys.stderr)
        for violation in violations:
            print(f"  - {violation}", file=sys.stderr)
        return 1

    print(
        "Required-check policy passed "
        f"({len(policy.required_checks)} checks for {policy.branch})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
