"""Meta-tests for the real-provider contract framework + the coverage checklist.

Run the whole contract layer and show the outstanding-fixture checklist with:

    pytest -m contract -rs

Every `AWAITING FIXTURE` skip line names the exact provider payload still needed
and what it must let us confirm.
"""
from __future__ import annotations

import pytest

from tests.contract.framework import (
    FIXTURES_DIR,
    has_fixture,
    iter_fixtures,
    load_fixture,
    validate_fixture,
)
from tests.contract.registry import REGISTRY, ContractNeed

pytestmark = pytest.mark.contract


def test_framework_selftest_fixture_loads() -> None:
    """The loader/validator work end-to-end against the committed self-test
    fixture — no real provider data required to prove the harness is sound."""
    fx = load_fixture("_selftest", "api_response", "example")
    assert fx.provider == "_selftest"
    assert fx.kind == "api_response"
    assert fx.response["status"] == 200
    assert fx.response_body == {"ok": True}


def test_all_committed_fixtures_are_valid() -> None:
    """Every fixture file on disk must satisfy the strict schema (provenance +
    sanitized attestation + kind-appropriate envelope). A malformed or
    un-sanitized fixture fails the build rather than silently 'passing'."""
    paths = list(FIXTURES_DIR.rglob("*.json"))
    assert paths, "expected at least the _selftest fixture on disk"
    for fx in iter_fixtures():
        validate_fixture(fx.path, fx.data)  # raises ContractFixtureError on bad shape


@pytest.mark.parametrize(
    "need", REGISTRY, ids=lambda n: f"{n.provider}.{n.kind}.{n.fixture}"
)
def test_contract_coverage(need: ContractNeed) -> None:
    """Live coverage checklist. When the real fixture is provided this asserts
    it validates; until then it SKIPS with the precise ask, so the suite output
    is the authoritative list of fixtures still owed by the provider."""
    if not has_fixture(need.provider, need.kind, need.fixture):
        pytest.skip(
            f"AWAITING FIXTURE [{need.finding}] "
            f"fixtures/{need.provider}/{need.kind}/{need.fixture}.json — "
            f"today we read: {need.we_currently_read}; "
            f"fixture must confirm: {need.must_confirm}"
        )
    fx = load_fixture(need.provider, need.kind, need.fixture)
    validate_fixture(fx.path, fx.data)
