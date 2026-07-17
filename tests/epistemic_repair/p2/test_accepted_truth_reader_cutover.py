"""Ratchets for accepted-truth retrieval and SAGE reader boundaries."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from services.domain.models.read_shapes import (
    ACCEPTED_MODEL_ROWS_SQL,
    ACCEPTED_PROJECTED_MODEL_EDGES_SQL,
)


ROOT = Path(__file__).resolve().parents[3]
READER_FILES = (
    ROOT / "services/reasoning/retrieval/pathways.py",
    ROOT / "services/reasoning/sage/reader.py",
)
RAW_MODEL_READ = re.compile(r"\b(?:FROM|JOIN)\s+models\b", re.IGNORECASE)
RAW_EDGE_READ = re.compile(r"\b(?:FROM|JOIN)\s+model_edges\b", re.IGNORECASE)


def test_model_row_compatibility_adapter_is_admission_gated() -> None:
    normalized = " ".join(ACCEPTED_MODEL_ROWS_SQL.split()).lower()
    assert "from accepted_current_models accepted" in normalized
    assert "join models legacy" in normalized
    assert "legacy.tenant_id = accepted.tenant_id" in normalized
    assert "legacy.id = accepted.id" in normalized


def test_projected_edge_adapter_is_accepted_relation_gated() -> None:
    normalized = " ".join(ACCEPTED_PROJECTED_MODEL_EDGES_SQL.split()).lower()
    assert "from accepted_current_relations accepted_relation" in normalized
    assert "join relation_edge_projections projection" in normalized
    assert "projection.status = 'active'" in normalized
    assert "join model_edges edge" in normalized


def test_production_readers_have_no_direct_legacy_truth_reads() -> None:
    for path in READER_FILES:
        source = path.read_text(encoding="utf-8")
        assert not RAW_MODEL_READ.search(source), path
        assert not RAW_EDGE_READ.search(source), path
        assert "accepted_current_models" not in source
        # Readers consume the shared adapter instead of reimplementing its join.
        assert "ACCEPTED_MODEL_ROWS_SQL" in source


def test_adapter_placeholders_are_always_interpolated() -> None:
    for path in READER_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        leaked_literals = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and (
                "{_ACCEPTED_MODEL_ROWS_SQL}" in node.value
                or "{_ACCEPTED_PROJECTED_MODEL_EDGES_SQL}" in node.value
            )
        ]
        assert leaked_literals == [], f"unexpanded SQL adapter at {path}:{leaked_literals}"


def test_sage_candidates_remain_explicitly_noncanonical() -> None:
    source = (ROOT / "services/reasoning/sage/reader.py").read_text(encoding="utf-8")
    assert "'accepted_projection'::text AS _truth_class" in source
    assert "'candidate'::text AS _truth_class" in source
    assert "'edge_type_candidate'::text AS _truth_class" in source
    assert "review_status IN ('candidate', 'needs_review', 'accepted')" not in source
