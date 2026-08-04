from __future__ import annotations

import json
from pathlib import Path

import pytest

from services.domain.identity.capabilities import (
    SOURCE_IDENTITY_CAPABILITIES,
    canonical_admission,
    capability_for,
    capability_snapshot,
    reference_kind_for_hint,
)


def test_capability_registry_covers_the_current_source_contract_catalog() -> None:
    root = Path(__file__).resolve().parents[4]
    index = json.loads(
        (root / "services/ingest/source_contract/source-index.json").read_text()
    )
    assert set(SOURCE_IDENTITY_CAPABILITIES) == set(index["sources"])


def test_only_source_grounded_person_is_initially_canonical() -> None:
    assert canonical_admission("person") == "canonical"
    assert canonical_admission("document") == "conditional"
    for contextual in ("audit", "goal", "project", "team", "software_system"):
        assert canonical_admission(contextual) == "contextual_only"


def test_rich_connectors_declare_only_evidence_they_emit() -> None:
    assert capability_for("slack").semantic_maturity == "rich"
    assert "channel" in capability_for("slack").native_reference_types
    assert capability_for("notion").native_reference_types == (
        "page",
        "block",
        "comment",
        "database",
        "user",
    )
    assert reference_kind_for_hint("notion_page") == "artifact"
    assert reference_kind_for_hint("slack_user") == "principal"
    assert reference_kind_for_hint("unknown_hint") is None


def test_capability_snapshot_is_deterministic() -> None:
    assert capability_snapshot() == capability_snapshot()
    assert list(capability_snapshot()["sources"]) == sorted(
        SOURCE_IDENTITY_CAPABILITIES
    )


def test_unknown_source_is_rejected() -> None:
    with pytest.raises(ValueError, match="no declared identity capability"):
        capability_for("imaginary")
