"""Test bootstrap for the summarization unit tests.

These tests exercise pure logic in ``summarization/llm.py`` (schema parsing,
rendering, map-reduce merge/dedup). Importing it through the normal package path
pulls in ``services.ingest.ingestion.__init__`` -> handlers -> heavy runtime deps
(confluent_kafka, aiokafka, ...) that aren't needed for these pure-unit tests.

When those heavy deps ARE installed (CI), the normal import works and this
conftest is a no-op. When they are NOT, we register lightweight namespace
placeholders for the parent packages and load ``llm.py`` directly from its file,
binding it under its real dotted name so the tests' ``from
services.ingest.ingestion.summarization.llm import ...`` continues to resolve.
"""
from __future__ import annotations

import importlib
import importlib.util
import sys
import types
from pathlib import Path

_REAL_NAME = "services.ingest.ingestion.summarization.llm"


def _ensure_namespace_package(dotted: str, path: Path) -> None:
    if dotted in sys.modules:
        return
    module = types.ModuleType(dotted)
    module.__path__ = [str(path)]  # mark as a (namespace) package
    sys.modules[dotted] = module


def _load_llm_standalone() -> None:
    repo_root = Path(__file__).resolve().parents[6]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    # Register empty placeholders for the parent packages so loading the leaf
    # module does not execute their (heavy) __init__.py side effects.
    pkg_paths = {
        "services": repo_root / "services",
        "services.ingest": repo_root / "services" / "ingest",
        "services.ingest.ingestion": repo_root / "services" / "ingest" / "ingestion",
        "services.ingest.ingestion.summarization": (
            repo_root / "services" / "ingest" / "ingestion" / "summarization"
        ),
    }
    for dotted, path in pkg_paths.items():
        _ensure_namespace_package(dotted, path)

    llm_path = pkg_paths["services.ingest.ingestion.summarization"] / "llm.py"
    spec = importlib.util.spec_from_file_location(_REAL_NAME, llm_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Register BEFORE exec so pydantic's deferred-annotation resolution sees the
    # module namespace under its real name.
    sys.modules[_REAL_NAME] = module
    spec.loader.exec_module(module)


try:  # Prefer the real import when all runtime deps are available (CI).
    importlib.import_module(_REAL_NAME)
except Exception:  # noqa: BLE001 - any import failure -> fall back to standalone load.
    _load_llm_standalone()
