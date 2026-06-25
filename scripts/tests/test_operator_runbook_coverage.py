from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNBOOK_INDEX = REPO_ROOT / "docs" / "operations" / "runbook-index.md"

REQUIRED_SCENARIOS = (
    "deploy",
    "rollback",
    "migration failure",
    "queue backlog",
    "DLQ replay/quarantine",
    "webhook verification spike",
    "source API outage",
    "LLM provider outage",
    "DB saturation",
    "Redis/broker/object storage outage",
    "tenant isolation incident",
    "secret rotation",
    "backup restore",
    "customer support diagnostics",
)


def test_operator_runbook_index_covers_required_scenarios() -> None:
    text = RUNBOOK_INDEX.read_text(encoding="utf-8")

    missing = [
        scenario
        for scenario in REQUIRED_SCENARIOS
        if f"| {scenario} |" not in text
    ]

    assert missing == []
