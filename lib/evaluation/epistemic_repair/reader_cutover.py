"""Machine-checkable accepted-truth reader cutover census."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ReaderResult:
    module: str
    classification: str
    authority: str
    compliant: bool
    missing_tokens: tuple[str, ...]
    reason: str


@dataclass(frozen=True, slots=True)
class ReaderCutoverReport:
    manifest_version: str
    results: tuple[ReaderResult, ...]

    @property
    def consequential(self) -> tuple[ReaderResult, ...]:
        return tuple(
            item for item in self.results if item.classification == "consequential"
        )

    @property
    def coverage(self) -> float:
        population = self.consequential
        return (
            sum(item.compliant for item in population) / len(population)
            if population
            else 1.0
        )

    @property
    def remaining_debt(self) -> tuple[str, ...]:
        return tuple(item.module for item in self.consequential if not item.compliant)


def scan_reader_cutover(repo_root: Path, manifest_path: Path) -> ReaderCutoverReport:
    manifest: dict[str, Any] = json.loads(manifest_path.read_text(encoding="utf-8"))
    inventory_path = repo_root / manifest["source_inventory"]
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    entries = manifest["entries"]
    by_module = {entry["module"]: entry for entry in entries}
    inventoried = {entry["module"] for entry in inventory["reader_modules"]}
    if set(by_module) != inventoried:
        missing = sorted(inventoried - set(by_module))
        extra = sorted(set(by_module) - inventoried)
        raise ValueError(f"reader manifest drift: missing={missing}, extra={extra}")

    results: list[ReaderResult] = []
    for module in sorted(by_module):
        entry = by_module[module]
        path = repo_root / module
        if not path.is_file():
            raise ValueError(f"reader manifest path does not exist: {module}")
        source = path.read_text(encoding="utf-8")
        required = tuple(entry["required_all"])
        missing_tokens = tuple(token for token in required if token not in source)
        compliant = (
            entry["classification"] == "exempt"
            or (entry["authority"] in {"accepted_direct", "accepted_delegate"}
                and not missing_tokens)
        )
        results.append(
            ReaderResult(
                module,
                entry["classification"],
                entry["authority"],
                compliant,
                missing_tokens,
                entry["reason"],
            )
        )
    # The P0 direct SQL census is broader than its curated reader summaries.
    # Until each additional module earns an explicit manifest disposition it
    # remains uncovered consequential debt; silence can never raise coverage.
    direct_census = set(inventory["direct_canonical_reader_modules"])
    for module in sorted(direct_census - set(by_module)):
        path = repo_root / module
        if not path.is_file():
            raise ValueError(f"P0 direct-reader path does not exist: {module}")
        results.append(
            ReaderResult(
                module,
                "consequential",
                "uncovered",
                False,
                (),
                "P0 direct reader has not yet received an explicit P2 disposition",
            )
        )
    return ReaderCutoverReport(manifest["schema_version"], tuple(results))


__all__ = ["ReaderCutoverReport", "ReaderResult", "scan_reader_cutover"]
