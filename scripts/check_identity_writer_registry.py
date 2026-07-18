#!/usr/bin/env python3
"""Ratchet canonical identity mutation and command-capability ownership."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "docs/plans/epistemic-repair/p3/identity-writer-registry-v1.json"


def violations() -> list[str]:
    registry = json.loads(REGISTRY.read_text())
    files = [
        path for path in (ROOT / "services").rglob("*.py")
        if "/tests/" not in path.relative_to(ROOT).as_posix()
        and not path.relative_to(ROOT).as_posix().startswith("services/evaluation/")
        and not path.name.startswith("test_")
    ]
    mutation = re.compile(
        r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+entity_aliases\b", re.I
    )
    writers = {
        path.relative_to(ROOT).as_posix()
        for path in files if mutation.search(path.read_text(errors="replace"))
    }
    minters = {
        path.relative_to(ROOT).as_posix()
        for path in files
        if registry["authority_setting"] in path.read_text(errors="replace")
    }
    errors: list[str] = []
    if writers != {registry["mutation_module"]}:
        errors.append(f"identity writers differ from registry: {sorted(writers)}")
    if minters != {registry["authority_minter"]}:
        errors.append(f"identity authority minters differ from registry: {sorted(minters)}")
    return errors


if __name__ == "__main__":
    problems = violations()
    print("identity writer registry: " + ("FAIL" if problems else "PASS"))
    for problem in problems:
        print(f"- {problem}")
    raise SystemExit(bool(problems))
