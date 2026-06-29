from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FLOW_DIR = REPO_ROOT / "docs" / "ingestion" / "flows"
CLASSIFICATION_DOC = (
    REPO_ROOT / "docs" / "operations" / "integration-data-classification.md"
)


def test_every_ingestion_flow_has_data_classification_row() -> None:
    doc = CLASSIFICATION_DOC.read_text()
    classified_sources = set(
        re.findall(r"^\| `([^`]+)` \|", doc, flags=re.MULTILINE)
    )
    flow_sources = {
        path.name.removesuffix("-ingestion.md")
        for path in FLOW_DIR.glob("*-ingestion.md")
    }

    missing = sorted(flow_sources - classified_sources)
    assert not missing, (
        "integration data classification is missing source rows: "
        + ", ".join(missing)
    )
