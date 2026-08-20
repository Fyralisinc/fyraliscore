from __future__ import annotations

import pytest

from services.ingest.source_contract.identity import (
    connector_name,
    connector_namespace,
    validate_capability_id,
    validate_connector_id,
    validate_source_id,
)


def test_connector_identity_is_namespaced() -> None:
    assert validate_connector_id("fyralis/stripe") == "fyralis/stripe"
    assert connector_namespace("fyralis/stripe") == "fyralis"
    assert connector_name("fyralis/stripe") == "stripe"


@pytest.mark.parametrize(
    "value",
    ("stripe", "Fyralis/stripe", "fyralis/Stripe", "/stripe", "fyralis/"),
)
def test_invalid_connector_identity_is_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="invalid connector ID"):
        validate_connector_id(value)


def test_source_and_capability_identifiers_have_distinct_grammars() -> None:
    assert validate_source_id("google_calendar") == "google_calendar"
    assert validate_capability_id("ingestion.historical_pull") == (
        "ingestion.historical_pull"
    )
    with pytest.raises(ValueError, match="invalid source ID"):
        validate_source_id("google.calendar")
    with pytest.raises(ValueError, match="invalid capability ID"):
        validate_capability_id("webhook")
