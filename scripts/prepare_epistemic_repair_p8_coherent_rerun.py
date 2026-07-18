#!/usr/bin/env python3
"""Prepare, but never execute, one-head P8 evidence rerun commands."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.contracts.kernel import canonical_sha256


def build_plan(*, repository: Path, output_dir: Path, expected_head: str | None) -> dict[str, object]:
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    if expected_head is not None and head != expected_head:
        raise ValueError(f"HEAD mismatch: expected {expected_head}, observed {head}")
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=repository, text=True,
    ).strip()
    if dirty:
        raise ValueError("coherent P8 rerun requires a clean tracked worktree")
    fault = output_dir / "p8-production-fault-evidence.json"
    characterization = output_dir / "p8-component-characterization.json"
    scale = output_dir / "p8-postgres-scale-matrix.json"
    contention = output_dir / "p8-shared-contention.json"
    exit_artifact = output_dir / "epistemic-repair-p8-fault-scale.json"
    # Commands are intentionally data, not subprocess calls. The coordinator
    # launches them only after exclusive database ownership is confirmed.
    commands = [
        [".venv/bin/python", "scripts/run_epistemic_repair_p8_production_faults.py",
         "--database-url", "${P8_DATABASE_URL}", "--output", str(fault)],
        [".venv/bin/python", "scripts/run_epistemic_repair_p8_characterization.py",
         "--database-url", "${P8_DATABASE_URL}", "--output", str(characterization)],
        [".venv/bin/python", "scripts/run_epistemic_repair_p8_isolated_scale_matrix.py",
         "--admin-database-url", "${P8_DATABASE_URL}",
         "--template-database", "${P8_TEMPLATE_DATABASE}",
         "--expected-head", "${P8_EXPECTED_HEAD}", "--output", str(scale),
         "--contention-output", str(contention)],
        [".venv/bin/python", "scripts/build_epistemic_repair_p8_exit.py",
         "--fault", str(fault), "--scale", str(scale),
         "--characterization", str(characterization), "--contention", str(contention),
         "--output", str(exit_artifact)],
    ]
    plan: dict[str, object] = {
        "schema_version": "p8-coherent-rerun-plan-v1",
        "prepared_at": datetime.now(timezone.utc).isoformat(),
        "repository": str(repository), "commit_sha": head,
        "requirements": {
            "exclusive_database_ownership": True,
            "clean_tracked_worktree": True,
            "same_head_for_every_stage": True,
            "provider": "codex-cli-only",
            "separate_provider_canary_authorized": False,
            "commands_are_not_executed_by_this_script": True,
        },
        "commands": commands,
        "expected_artifacts": [str(fault), str(characterization), str(scale),
                               str(contention), str(exit_artifact)],
    }
    plan["plan_digest"] = canonical_sha256(plan)
    return plan


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--expected-head")
    parser.add_argument("--plan-output", type=Path, required=True)
    args = parser.parse_args()
    plan = build_plan(
        repository=args.repository.resolve(), output_dir=args.output_dir.resolve(),
        expected_head=args.expected_head,
    )
    args.plan_output.parent.mkdir(parents=True, exist_ok=True)
    args.plan_output.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
    print(f"prepared_only=true commit_sha={plan['commit_sha']} output={args.plan_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
