from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
DEPENDABOT_CONFIG = REPO_ROOT / ".github" / "dependabot.yml"


def test_dependabot_covers_release_dependency_surfaces() -> None:
    config = yaml.safe_load(DEPENDABOT_CONFIG.read_text(encoding="utf-8"))

    assert config["version"] == 2
    updates = config["updates"]
    by_ecosystem = {entry["package-ecosystem"]: entry for entry in updates}

    assert {"pip", "github-actions", "docker"} <= set(by_ecosystem)
    for ecosystem in ("pip", "github-actions", "docker"):
        entry = by_ecosystem[ecosystem]
        assert entry["directory"] == "/"
        assert entry["schedule"]["interval"] == "weekly"
        assert entry["open-pull-requests-limit"] >= 1
