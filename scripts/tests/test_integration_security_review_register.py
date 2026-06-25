from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FLOW_DIR = REPO_ROOT / "docs" / "ingestion" / "flows"
REGISTER_DOC = (
    REPO_ROOT / "docs" / "operations" / "integration-security-review-register.md"
)


def _register_sources(text: str) -> set[str]:
    return set(re.findall(r"^\| `([^`]+)` \|", text, flags=re.MULTILINE))


def test_every_ingestion_flow_has_security_review_row() -> None:
    doc = REGISTER_DOC.read_text()
    registered_sources = _register_sources(doc)
    flow_sources = {
        path.name.removesuffix("-ingestion.md")
        for path in FLOW_DIR.glob("*-ingestion.md")
    }

    missing = sorted(flow_sources - registered_sources)
    assert not missing, (
        "integration security review register is missing source rows: "
        + ", ".join(missing)
    )


def test_security_review_register_contains_enablement_gate() -> None:
    doc = REGISTER_DOC.read_text()
    required_phrases = [
        "Production Enablement Rule",
        "approval artifact is linked",
        "Secrets are stored as opaque refs",
        "uninstall tests pass in CI",
        "blocked",
    ]
    missing = [phrase for phrase in required_phrases if phrase not in doc]
    assert not missing, "security review register missing: " + ", ".join(missing)
