from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
EVIDENCE_DIR = REPO_ROOT / "docs" / "operations" / "customer-launch-evidence"


def test_customer_launch_evidence_templates_exist() -> None:
    required_files = [
        EVIDENCE_DIR / "README.md",
        EVIDENCE_DIR / "customer-launch-evidence-template.md",
        EVIDENCE_DIR / "integration-evidence-template.md",
        EVIDENCE_DIR / ".gitignore",
    ]
    missing = [path.relative_to(REPO_ROOT).as_posix() for path in required_files if not path.exists()]
    assert not missing, "missing customer launch evidence files: " + ", ".join(missing)


def test_customer_launch_evidence_keeps_customer_artifacts_out_of_repo() -> None:
    readme = (EVIDENCE_DIR / "README.md").read_text()
    ignore = (EVIDENCE_DIR / ".gitignore").read_text()
    required_phrases = [
        "Real customer evidence",
        "must not be committed",
        "signed customer contract and DPA reference",
        "completed security questionnaire",
        "integration security review approval",
    ]
    missing = [phrase for phrase in required_phrases if phrase not in readme]
    assert not missing, "evidence README missing: " + ", ".join(missing)
    assert "customers/" in ignore
