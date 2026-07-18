"""Post-execution gold for the core fast-path 4x25 population.

This module is evaluator-only.  Production-shaped runners and scripted provider
adapters must not import it.
"""

from __future__ import annotations

from dataclasses import dataclass

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.core_fast_path_population import (
    CORE_FAST_PATH_SIGNAL_COUNT,
    build_core_fast_path_population,
)


@dataclass(frozen=True, slots=True)
class CoreFastPathSignalGold:
    signal_id: str
    storyline_id: str | None
    role: str
    canonical_ref: str | None
    expected_surface: str | None
    expected_authority: str | None


@dataclass(frozen=True, slots=True)
class CoreFastPathGold:
    population_digest: str
    signals: tuple[CoreFastPathSignalGold, ...]
    synthesis_signal_id: str
    correction_signal_id: str
    expected_scope_ref: str
    expected_thesis: str
    expected_corrected_thesis: str
    expected_relation_kind: str
    expected_post_correction_relation_lifecycle: str
    gold_digest: str


def _role(signal_id: str) -> str:
    if signal_id == "cf2-harbor-b03-o03":
        return "synthesis_conclusion"
    if signal_id == "cf2-harbor-b04-o03":
        return "authoritative_correction"
    if signal_id.startswith("cf2-harbor-"):
        return "storyline_evidence"
    if signal_id.startswith("cf2-noise-"):
        return "noise"
    if signal_id.startswith("cf2-distractor-"):
        return "high_similarity_distractor"
    return "background_storyline"


def build_core_fast_path_gold() -> CoreFastPathGold:
    population = build_core_fast_path_population()
    rows: list[CoreFastPathSignalGold] = []
    for signal in population.signals:
        parts = signal.signal_id.split("-")
        storyline = parts[1] if len(parts) > 2 and parts[1] in {
            "harbor", "northstar", "access", "delta"
        } else None
        canonical_ref = {
            "harbor": "workstream:harbor-release",
            "northstar": "workstream:northstar-pilot",
            "access": "workstream:access-review",
            "delta": "workstream:delta-handoff",
        }.get(storyline)
        surface = {
            "harbor": "Harbor release",
            "northstar": "Northstar pilot",
            "access": "Access review",
            "delta": "Delta handoff",
        }.get(storyline)
        rows.append(CoreFastPathSignalGold(
            signal_id=signal.signal_id,
            storyline_id=storyline,
            role=_role(signal.signal_id),
            canonical_ref=canonical_ref,
            expected_surface=surface,
            expected_authority=(
                "resolved_for_consumer" if storyline is not None else None
            ),
        ))
    if len(rows) != CORE_FAST_PATH_SIGNAL_COUNT:
        raise AssertionError("gold must cover every core fast-path signal")
    body = {
        "population_digest": population.population_digest,
        "signals": [
            {
                "signal_id": row.signal_id,
                "storyline_id": row.storyline_id,
                "role": row.role,
                "canonical_ref": row.canonical_ref,
                "expected_surface": row.expected_surface,
                "expected_authority": row.expected_authority,
            }
            for row in rows
        ],
        "synthesis_signal_id": "cf2-harbor-b03-o03",
        "correction_signal_id": "cf2-harbor-b04-o03",
        "expected_scope_ref": "workstream:harbor-release",
        "expected_thesis": (
            "Harbor release is blocked by incomplete certificate renewal."
        ),
        "expected_corrected_thesis": (
            "Harbor release is no longer blocked after certificate renewal completed."
        ),
        "expected_relation_kind": "dependency_constraint",
        "expected_post_correction_relation_lifecycle": "retired",
    }
    return CoreFastPathGold(
        population_digest=population.population_digest,
        signals=tuple(rows),
        synthesis_signal_id=str(body["synthesis_signal_id"]),
        correction_signal_id=str(body["correction_signal_id"]),
        expected_scope_ref=str(body["expected_scope_ref"]),
        expected_thesis=str(body["expected_thesis"]),
        expected_corrected_thesis=str(body["expected_corrected_thesis"]),
        expected_relation_kind=str(body["expected_relation_kind"]),
        expected_post_correction_relation_lifecycle=str(
            body["expected_post_correction_relation_lifecycle"]
        ),
        gold_digest=canonical_sha256(body),
    )


__all__ = [
    "CoreFastPathGold",
    "CoreFastPathSignalGold",
    "build_core_fast_path_gold",
]
