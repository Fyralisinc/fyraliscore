from __future__ import annotations

import pytest

from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.runtime import (
    CertificationHistoryUnsupportedError,
    resolve_fixture_count_oracle,
    resolve_fixture_factory,
    resolve_installation_seeder,
    resolve_live_fixture_factory,
    validate_certification_bindings,
)
from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    source_definition,
)


def test_every_history_source_binding_resolves_and_matches_source() -> None:
    history_sources = tuple(
        source_id
        for source_id in CANONICAL_SOURCE_IDS
        if source_definition(source_id).history is not None
    )

    assert validate_certification_bindings() == history_sources
    assert len(history_sources) == 26

    for source_id in history_sources:
        spec = SOURCE_CERTIFICATION_CATALOG[source_id]
        fixture = spec.fixture_factory_binding
        count_oracle = spec.fixture_count_oracle_binding
        installation = spec.installation_seeder_binding
        assert fixture is not None
        assert count_oracle is not None
        assert installation is not None
        assert (
            fixture.source_id
            == count_oracle.source_id
            == installation.source_id
            == source_id
        )
        assert resolve_fixture_factory(source_id).__name__ == (
            f"build_{source_id}_fixture"
        )
        assert resolve_fixture_count_oracle(source_id).__name__ == (
            f"count_{source_id}_fixture_observations"
        )
        assert callable(resolve_installation_seeder(source_id))


def test_whatsapp_history_is_explicitly_unsupported() -> None:
    spec = SOURCE_CERTIFICATION_CATALOG["whatsapp"]

    assert source_definition("whatsapp").history is None
    assert spec.fixture_factory_binding is None
    assert spec.live_fixture_factory_binding is not None
    assert spec.live_fixture_factory_binding.role == "live_fixture_factory"
    assert spec.fixture_count_oracle_binding is None
    assert spec.installation_seeder_binding is None
    fixture = resolve_live_fixture_factory("whatsapp")()
    assert fixture["object"] == "whatsapp_business_account"
    with pytest.raises(
        CertificationHistoryUnsupportedError,
        match="explicitly does not support history",
    ):
        resolve_fixture_factory("whatsapp")
    with pytest.raises(
        CertificationHistoryUnsupportedError,
        match="explicitly does not support history",
    ):
        resolve_fixture_count_oracle("whatsapp")
