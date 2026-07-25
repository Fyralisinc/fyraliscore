from __future__ import annotations

import json
from datetime import date

import pytest

from services.ingest.integrations.provider_transport_runtime import (
    _parse_rule,
    get_provider_transport_runtime,
    reset_provider_transport_runtime_for_tests,
)
from services.ingest.source_contract.catalog import (
    PROVIDER_TRANSPORT_OPERATION_CATALOG,
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
        source: {
            operation: [_quota_rule(include_evidence=include_evidence)]
            for operation in operations
        }
        for source, operations in PROVIDER_TRANSPORT_OPERATION_CATALOG.items()
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


async def test_optional_local_runtime_retains_legacy_quota_shape(
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
