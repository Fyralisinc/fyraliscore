"""P0 characterization of truth authority and illegal epistemic states.

These tests freeze the discovered surface without changing production behavior.
They intentionally say that the illegal fixtures are currently representable;
P2 must add independent rejection/read-isolation tests before changing that fact.
"""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
P0 = ROOT / "docs" / "plans" / "epistemic-repair" / "p0"


def _load(name: str) -> dict[str, object]:
    return json.loads((P0 / name).read_text())


def _production_python_files() -> list[Path]:
    files: list[Path] = []
    for path in (ROOT / "services").rglob("*.py"):
        relative = path.relative_to(ROOT).as_posix()
        if "/tests/" in relative or relative.endswith("_vertical.py"):
            continue
        if Path(relative).name.startswith("company_physics"):
            continue
        files.append(path)
    return files


def test_every_direct_canonical_sql_writer_file_is_in_authority_inventory() -> None:
    inventory = _load("authority-writer-reader-inventory.json")
    canonical_tables = set(inventory["canonical_tables"])
    mutation = re.compile(
        rf"(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+"
        rf"(?:public\.)?(?:{'|'.join(sorted(canonical_tables))})\b",
        re.IGNORECASE,
    )
    discovered = {
        path.relative_to(ROOT).as_posix()
        for path in _production_python_files()
        if mutation.search(path.read_text(errors="replace"))
    }
    registered = {record["module"] for record in inventory["writer_modules"]}
    assert discovered <= registered, f"Unregistered canonical writers: {sorted(discovered - registered)}"


def test_every_canonical_writer_family_has_a_named_owner_and_risk() -> None:
    inventory = _load("authority-writer-reader-inventory.json")
    required_families = {
        "identity",
        "model",
        "model_event",
        "lifecycle",
        "legacy_edge",
        "relation_claim",
        "relation_instance",
        "relation_participant",
    }
    records = inventory["writer_modules"]
    observed = {family for record in records for family in record["families"]}
    assert required_families <= observed
    assert all(record["authority"] and record["risk"] for record in records)


def test_every_direct_canonical_sql_reader_file_is_in_authority_inventory() -> None:
    inventory = _load("authority-writer-reader-inventory.json")
    canonical_tables = set(inventory["canonical_tables"])
    read = re.compile(
        rf"(?:FROM|JOIN)\s+(?:public\.)?"
        rf"(?:{'|'.join(sorted(canonical_tables))})\b",
        re.IGNORECASE,
    )
    discovered = {
        path.relative_to(ROOT).as_posix()
        for path in _production_python_files()
        if read.search(path.read_text(errors="replace"))
    }
    registered = set(inventory["direct_canonical_reader_modules"])
    assert discovered == registered, (
        f"Unregistered readers: {sorted(discovered - registered)}; "
        f"stale registrations: {sorted(registered - discovered)}"
    )


def test_derived_and_projection_writers_do_not_declare_canonical_tables() -> None:
    inventory = _load("authority-writer-reader-inventory.json")
    canonical = set(inventory["canonical_tables"])
    violations = []
    for record in inventory["writer_modules"]:
        if record["classification"] in {"derived_only", "projection_only"}:
            overlap = canonical.intersection(record["tables"])
            if overlap:
                violations.append((record["module"], sorted(overlap)))
    assert violations == []


def test_all_registered_illegal_truth_classes_are_reproduced_and_owned() -> None:
    inventory = _load("truth-state-inventory.json")
    rules = inventory["state_rules"]
    fixtures = {fixture["id"]: fixture for fixture in inventory["illegal_fixtures"]}

    assert len(rules) == 15
    assert len(fixtures) == len(rules)
    assert {rule["illegal_fixture"] for rule in rules} == set(fixtures)
    assert all(rule["gate"].startswith("HG-") for rule in rules)
    assert all(rule["p2_owner"].startswith(("P2-", "P3-")) for rule in rules)
    assert all(fixture["expected"] == "illegal" for fixture in fixtures.values())
    assert all(fixture["currently_representable"] is True for fixture in fixtures.values())


def test_legal_controls_are_distinct_and_nonempty() -> None:
    inventory = _load("truth-state-inventory.json")
    illegal_ids = {fixture["id"] for fixture in inventory["illegal_fixtures"]}
    controls = inventory["legal_control_fixtures"]
    assert len(controls) >= 4
    assert illegal_ids.isdisjoint(fixture["id"] for fixture in controls)
    assert all(fixture["expected"] == "legal" for fixture in controls)


def test_inventory_does_not_claim_p0_repaired_production_behavior() -> None:
    inventory = _load("truth-state-inventory.json")
    verdict = inventory["current_verdict"].lower()
    assert "remain representable" in verdict
    assert "not a p2 repair claim" in verdict
