from __future__ import annotations

import pytest

from services.ingest.connector_conformance import (
    ConnectorConformanceSuite,
    assert_connector_conforms,
)
from services.ingest.connector_runtime.registry import ConnectorCandidate
from services.ingest.connector_runtime.tests.helpers import (
    ExampleConnector,
    make_candidate,
    make_manifest,
)
from services.ingest.source_contract.capabilities import IDENTITY_V1
from services.ingest.source_contract.connector import BindingContext, BoundConnector


def test_conforming_candidate_gets_reproducible_fingerprint() -> None:
    candidate, _ = make_candidate()
    suite = ConnectorConformanceSuite()

    first = suite.run(candidate)
    second = suite.run(candidate)

    assert first.passed
    assert first.fingerprint == second.fingerprint
    assert len(first.fingerprint) == 64
    assert {check.name for check in first.checks} >= {
        "registry.snapshot",
        "connector.binding",
        "capability.semantic.identity.v1",
    }
    assert_connector_conforms(first)


def test_registry_validation_failure_is_a_conformance_failure() -> None:
    manifest = make_manifest(contract=">=2.0,<3.0")
    candidate = ConnectorCandidate(
        manifest,
        lambda: ExampleConnector(manifest),
        (IDENTITY_V1,),
    )

    report = ConnectorConformanceSuite().run(candidate)

    assert not report.passed
    assert any(
        check.diagnostic_code == "contract_incompatible"
        for check in report.failures
    )
    with pytest.raises(AssertionError, match="contract_incompatible"):
        assert_connector_conforms(report)


def test_binding_failure_is_reported_instead_of_escaping_harness() -> None:
    manifest = make_manifest()

    class BrokenConnector(ExampleConnector):
        def bind(self, context: BindingContext) -> BoundConnector:
            raise RuntimeError("binding side effect detected")

    candidate = ConnectorCandidate(
        manifest,
        lambda: BrokenConnector(manifest),
        (IDENTITY_V1,),
    )

    report = ConnectorConformanceSuite().run(candidate)

    assert not report.passed
    assert any(
        check.name == "connector.binding"
        and "failed while binding" in check.message
        for check in report.failures
    )
