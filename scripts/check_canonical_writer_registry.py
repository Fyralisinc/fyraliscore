#!/usr/bin/env python3
"""Ratchet canonical truth authority and freeze known legacy Model bypasses."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = (
    ROOT
    / "docs/plans/epistemic-repair/p2/canonical-writer-registry-v1.json"
)
MUTATION = r"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)"


def load_registry() -> dict[str, Any]:
    return json.loads(REGISTRY.read_text())


def production_files() -> list[Path]:
    return [
        path
        for path in (ROOT / "services").rglob("*.py")
        if "/tests/" not in path.relative_to(ROOT).as_posix()
        and not path.name.startswith("test_")
    ]


def writer_modules(tables: set[str]) -> set[str]:
    table_pattern = "|".join(re.escape(table) for table in sorted(tables))
    pattern = re.compile(
        rf"{MUTATION}\s+(?:public\.)?(?:{table_pattern})\b",
        re.IGNORECASE,
    )
    return {
        path.relative_to(ROOT).as_posix()
        for path in production_files()
        if pattern.search(path.read_text(errors="replace"))
    }


def violations() -> list[str]:
    registry = load_registry()
    canonical = set(registry["canonical_truth_tables"])
    actual_canonical = writer_modules(canonical)
    registered_canonical = set(registry["canonical_truth_writer_modules"])
    errors: list[str] = []
    unknown = actual_canonical - registered_canonical
    stale = registered_canonical - actual_canonical
    if unknown:
        errors.append(f"unregistered canonical truth writers: {sorted(unknown)}")
    if stale:
        errors.append(f"stale canonical truth writers: {sorted(stale)}")

    capability_setting = registry["command_authority_minter"]["setting"]
    capability_minters = {
        path.relative_to(ROOT).as_posix()
        for path in production_files()
        if capability_setting in path.read_text(errors="replace")
    }
    expected_minter = {registry["command_authority_minter"]["module"]}
    if capability_minters != expected_minter:
        errors.append(
            "truth command authority minters differ from registry: "
            f"actual={sorted(capability_minters)} expected={sorted(expected_minter)}"
        )

    forbidden_roots = tuple(registry["forbidden_direct_writer_roots"])
    forbidden = sorted(
        module for module in actual_canonical if module.startswith(forbidden_roots)
    )
    if forbidden:
        errors.append(f"reasoning/projection planes write canonical truth: {forbidden}")

    legacy_writers = writer_modules({"models"})
    projector = registry["legacy_models_projection_writer"]["module"]
    frozen = set(registry["frozen_legacy_models_bypasses"])
    allowed_legacy = frozen | {projector}
    unknown_legacy = legacy_writers - allowed_legacy
    stale_legacy = frozen - legacy_writers
    if unknown_legacy:
        errors.append(f"unregistered legacy models writers: {sorted(unknown_legacy)}")
    if stale_legacy:
        errors.append(
            "remove retired entries from frozen_legacy_models_bypasses: "
            f"{sorted(stale_legacy)}"
        )
    return errors


def main() -> int:
    errors = violations()
    if errors:
        print("canonical writer registry: FAIL")
        for error in errors:
            print(f"- {error}")
        return 1
    print("canonical writer registry: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
