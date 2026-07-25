from __future__ import annotations

from pathlib import Path

from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS


_REPO_ROOT = Path(__file__).resolve().parents[4]


def test_transport_enforced_sources_cannot_use_process_wide_builder_breaker() -> None:
    """Keep scoped ProviderTransport from being wrapped by a source-global gate.

    The remaining proxy exists only for not-yet-migrated sources. Once those
    sources move, this assertion makes each newly enforced declaration retire
    its old wrapper at the same time.
    """

    builders = (
        _REPO_ROOT
        / "services/ingest/ingestion/fetchers/_clients.py"
    ).read_text(encoding="utf-8")
    violations = [
        source.source_id
        for source in SOURCE_DEFINITIONS
        if source.provider_transport_enforced
        and f'_wrap_source_client("{source.source_id}"' in builders
    ]
    assert violations == []
