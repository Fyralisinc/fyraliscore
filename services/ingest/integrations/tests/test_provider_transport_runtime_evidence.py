from __future__ import annotations

import json
from datetime import date

import pytest

from services.ingest.integrations.provider_transport_runtime import (
    _parse_declarations,
    _parse_rule,
    get_provider_transport_runtime,
    reset_provider_transport_runtime_for_tests,
)
from services.ingest.source_contract.quota_contract import (
    PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION,
    PROVIDER_QUOTA_CONTRACT,
)


def _quota_rule(*, include_evidence: bool) -> dict[str, object]:
    rule: dict[str, object] = {
        "scope": "global",
        "identity": "global",
        "capacity": 1,
        "refill_per_second": 1,
        "cost": 1,
    }
    if include_evidence:
        rule.update(
            evidence_ref="evidence://provider/quota/v1",
            verified_on="2025-01-01",
        )
    return rule


def _complete_payload(*, include_evidence: bool) -> dict[str, object]:
    return {
        "schema_version": PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION,
        "catalog_sha256": PROVIDER_QUOTA_CONTRACT.catalog_sha256,
        "limits": {
            identity.reference: [_quota_rule(include_evidence=include_evidence)]
            for identity in PROVIDER_QUOTA_CONTRACT.operations
        },
    }


def test_production_runtime_fails_closed_without_quota_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_provider_transport_runtime_for_tests()
    monkeypatch.setenv("FYRALIS_ENV", "production")
    monkeypatch.setenv("REDIS_URL", "redis://provider-transport.invalid/0")
    monkeypatch.setenv(
        "FYRALIS_PROVIDER_QUOTAS_JSON",
        json.dumps(_complete_payload(include_evidence=False)),
    )

    with pytest.raises(RuntimeError, match="missing verified quota evidence"):
        get_provider_transport_runtime()


async def test_optional_local_runtime_uses_contract_linked_quota_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_provider_transport_runtime_for_tests()
    for name in ("FYRALIS_ENV", "COMPANY_OS_ENV", "APP_ENV", "ENVIRONMENT"):
        monkeypatch.setenv(name, "test")
    monkeypatch.setenv("REDIS_URL", "redis://provider-transport.invalid/0")
    monkeypatch.setenv(
        "FYRALIS_PROVIDER_QUOTAS_JSON",
        json.dumps(_complete_payload(include_evidence=False)),
    )

    runtime = get_provider_transport_runtime(required=False)

    assert runtime is not None
    await runtime.aclose()
    reset_provider_transport_runtime_for_tests()


def test_quota_payload_rejects_legacy_source_operation_registry() -> None:
    with pytest.raises(RuntimeError, match="Legacy source/operation maps"):
        _parse_declarations(
            json.dumps({"slack": {"users.info": [_quota_rule(include_evidence=False)]}})
        )


def test_quota_payload_rejects_contract_hash_drift() -> None:
    payload = _complete_payload(include_evidence=False)
    payload["catalog_sha256"] = "0" * 64

    with pytest.raises(RuntimeError, match="operation-policy catalog"):
        _parse_declarations(json.dumps(payload))


def test_quota_payload_rejects_missing_and_unknown_operation_references() -> None:
    payload = _complete_payload(include_evidence=False)
    limits = payload["limits"]
    assert isinstance(limits, dict)
    missing_identity = PROVIDER_QUOTA_CONTRACT.operations[0]
    rule = limits.pop(missing_identity.reference)

    with pytest.raises(RuntimeError, match="missing required contract operations"):
        _parse_declarations(json.dumps(payload))

    limits[missing_identity.reference] = rule
    limits[f"qop_v1_{'0' * 64}"] = rule
    with pytest.raises(RuntimeError, match="undeclared operation references"):
        _parse_declarations(json.dumps(payload))


def test_quota_payload_rejects_duplicate_json_keys() -> None:
    with pytest.raises(RuntimeError, match="duplicate key 'schema_version'"):
        _parse_declarations(
            (
                '{"schema_version":"1","schema_version":"1",'
                '"catalog_sha256":"unused","limits":{}}'
            )
        )


def test_quota_payload_does_not_own_source_or_operation_names() -> None:
    payload = _complete_payload(include_evidence=False)

    assert set(payload) == {"schema_version", "catalog_sha256", "limits"}
    limits = payload["limits"]
    assert isinstance(limits, dict)
    assert set(limits) == set(PROVIDER_QUOTA_CONTRACT.operations_by_reference)
    assert not (set(limits) & {"slack", "github", "users.info"})


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"evidence_ref": "evidence://provider/quota/v1"}, "together"),
        ({"verified_on": "2025-01-01"}, "together"),
        (
            {
                "evidence_ref": "evidence://provider/quota/v1",
                "verified_on": "January 1, 2025",
            },
            "YYYY-MM-DD",
        ),
        (
            {
                "evidence_ref": "evidence://provider/quota/v1",
                "verified_on": "2027-01-01",
            },
            "future",
        ),
    ],
)
def test_quota_evidence_validation(
    updates: dict[str, object],
    message: str,
) -> None:
    rule = _quota_rule(include_evidence=False)
    rule.update(updates)

    with pytest.raises(RuntimeError, match=message):
        _parse_rule(
            "provider",
            "objects.list",
            0,
            rule,
            require_evidence=False,
            today=date(2026, 7, 25),
        )


def test_verified_quota_rule_retains_audit_metadata() -> None:
    rule = _parse_rule(
        "provider",
        "objects.list",
        0,
        _quota_rule(include_evidence=True),
        require_evidence=True,
        today=date(2026, 7, 25),
    )

    assert rule.evidence_ref == "evidence://provider/quota/v1"
    assert rule.verified_on == date(2025, 1, 1)
