from __future__ import annotations

from dataclasses import replace

from services.ingest.source_contract.catalog import (
    PROVIDER_TRANSPORT_OPERATION_CATALOG,
    SOURCE_DEFINITIONS,
)
from services.ingest.source_contract.quota_contract import (
    PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION,
    PROVIDER_QUOTA_CONTRACT,
    build_provider_quota_contract,
)


def test_quota_contract_is_an_exact_projection_of_operation_policies() -> None:
    actual = {
        (identity.source_id, identity.operation_id)
        for identity in PROVIDER_QUOTA_CONTRACT.operations
    }
    expected = {
        (source_id, operation_id)
        for source_id, operation_ids in PROVIDER_TRANSPORT_OPERATION_CATALOG.items()
        for operation_id in operation_ids
    }

    assert actual == expected
    assert len(actual) == len(PROVIDER_QUOTA_CONTRACT.operations)
    assert len(PROVIDER_QUOTA_CONTRACT.operations_by_reference) == len(
        PROVIDER_QUOTA_CONTRACT.operations
    )
    assert PROVIDER_QUOTA_CONTRACT.schema_version == (
        PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION
    )
    assert len(PROVIDER_QUOTA_CONTRACT.catalog_sha256) == 64


def test_operation_references_are_opaque_and_resolve_through_contract() -> None:
    reference = PROVIDER_QUOTA_CONTRACT.reference_for(
        "slack",
        "users.info",
    )

    assert reference.startswith("qop_v1_")
    assert "slack" not in reference
    assert "users" not in reference
    assert (
        PROVIDER_QUOTA_CONTRACT.operations_by_reference[reference].source_id == "slack"
    )


def test_catalog_hash_tracks_request_policy_without_renaming_operation() -> None:
    slack = next(source for source in SOURCE_DEFINITIONS if source.source_id == "slack")
    first_operation = slack.operation_policies[0]
    changed_operation = replace(
        first_operation,
        request_policy=replace(
            first_operation.request_policy,
            timeout_seconds=first_operation.request_policy.timeout_seconds + 1,
        ),
    )
    changed_slack = replace(
        slack,
        operation_policies=(changed_operation, *slack.operation_policies[1:]),
    )
    changed_sources = tuple(
        changed_slack if source is slack else source for source in SOURCE_DEFINITIONS
    )

    changed = build_provider_quota_contract(changed_sources)

    assert changed.catalog_sha256 != PROVIDER_QUOTA_CONTRACT.catalog_sha256
    assert changed.reference_for(
        "slack",
        first_operation.operation_id,
    ) == PROVIDER_QUOTA_CONTRACT.reference_for(
        "slack",
        first_operation.operation_id,
    )
