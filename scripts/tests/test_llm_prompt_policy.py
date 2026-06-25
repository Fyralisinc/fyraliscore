from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_DOC = REPO_ROOT / "docs" / "operations" / "llm-prompt-content-use-policy.md"


def test_llm_prompt_policy_covers_required_controls() -> None:
    text = POLICY_DOC.read_text()
    required_phrases = [
        "Authorization must happen before prompt assembly",
        "Provider calls must happen outside database transactions",
        "must not include raw prompts",
        "daily per-tenant spend, token, and request budgets",
        "Provider Enablement Checklist",
        "Incident Response",
    ]
    missing = [phrase for phrase in required_phrases if phrase not in text]
    assert not missing, "LLM prompt/content policy missing: " + ", ".join(missing)
