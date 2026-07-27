from __future__ import annotations

import tomllib
from pathlib import Path


def test_certification_runtime_assets_are_declared_as_package_data() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    project = tomllib.loads((repo_root / "pyproject.toml").read_text("utf-8"))
    package_data = project["tool"]["setuptools"]["package-data"][
        "services.ingest.source_certification"
    ]

    assert {
        "EXECUTION_BINDINGS.md",
        "evidence/*.json",
        "evidence/README.md",
        "execution_bindings/*.json",
        "surfaces/*.json",
        "surfaces/README.md",
    } <= set(package_data)
