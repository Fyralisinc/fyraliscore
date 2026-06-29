from __future__ import annotations

from pathlib import Path


def test_source_onboarding_does_not_reassign_provider_installation_tenant() -> None:
    root = Path("services/ingest/integrations")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if (
            "INSERT INTO provider_installations" in text
            and "SET tenant_id = EXCLUDED.tenant_id" in text
        ):
            offenders.append(str(path))

    assert offenders == []
