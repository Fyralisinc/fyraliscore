from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "docs/plans/epistemic-repair/p2/canonical-writer-registry-v1.json"
CHECKER = ROOT / "scripts/check_canonical_writer_registry.py"


def _checker():
    spec = importlib.util.spec_from_file_location("canonical_writer_registry", CHECKER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_registry_is_versioned_and_explicit_about_frozen_debt() -> None:
    registry = json.loads(REGISTRY.read_text())
    assert registry["schema_version"] == "canonical-writer-registry-v1"
    assert registry["status"] == "cutover_in_progress"
    assert registry["legacy_models_projection_writer"] == {
        "module": "services/domain/truth_kernel/repository.py",
        "method": "_insert_legacy_read_projection",
        "classification": "registered_compatibility_projector",
    }
    assert "services/reasoning/think/applier.py" in registry[
        "frozen_legacy_models_bypasses"
    ]
    assert registry["command_authority_minter"] == {
        "module": "services/domain/truth_kernel/service.py",
        "setting": "app.truth_kernel_command",
        "scope": "transaction_local",
        "same_role_security_boundary": False,
    }


def test_no_unregistered_or_forbidden_canonical_writer_exists() -> None:
    assert _checker().violations() == []
