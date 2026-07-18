from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.domain.company_identity_bootstrap import FounderIdentityBootstrapEntry
from services.evaluation.epistemic_repair import founder_bootstrap


pytestmark = pytest.mark.asyncio


_ZERO_COUNTS = {
    "models": 0,
    "accepted_relation_instances": 0,
    "active_model_edges": 0,
    "observations": 25,
    "resources": 0,
}


class _FakeConnection:
    def __init__(self, *snapshots: dict[str, int]) -> None:
        self._snapshots = list(snapshots or (_ZERO_COUNTS, _ZERO_COUNTS))

    async def fetchrow(self, _query: str, _tenant_id):
        return self._snapshots.pop(0)


def _preparer() -> founder_bootstrap.FounderBootstrapBatchPreparer:
    return founder_bootstrap.build_founder_bootstrap_batch_preparer(
        manifest_ref="founder-manifest:acme:v1",
        authority_ref="company-founder:ada",
        asserted_by_ref="actor:ada",
        provenance_refs=("founder-onboarding:acme:2026-07-18",),
        entries=(
            FounderIdentityBootstrapEntry(
                canonical_ref={
                    "type": "workstream",
                    "id": "workstream:atlas-release",
                    "version": 1,
                },
                canonical_name="Atlas release",
                aliases=("Atlas launch",),
            ),
        ),
        effective_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
    )


async def test_preparer_applies_explicit_manifest_once_across_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    calls: list[dict] = []
    expected_result = SimpleNamespace(
        manifest_ref="founder-manifest:acme:v1", alias_count=2,
    )

    async def apply(_conn, **kwargs):
        calls.append(kwargs)
        return expected_result

    monkeypatch.setattr(founder_bootstrap, "apply_founder_identity_bootstrap", apply)
    preparer = _preparer()

    conn = _FakeConnection()
    await preparer(conn, tenant_id, SimpleNamespace(batch_number=1), {})
    await preparer(conn, tenant_id, SimpleNamespace(batch_number=2), {})

    assert len(calls) == 1
    assert calls[0]["tenant_id"] == tenant_id
    assert calls[0]["entries"] == preparer.entries
    assert calls[0]["entries"][0].canonical_name == "Atlas release"
    assert preparer.result is expected_result
    assert preparer.receipt == {
        "manifest_ref": "founder-manifest:acme:v1",
        "alias_count": 2,
        "applied_before_enqueue": True,
        "semantic_truth_unchanged": True,
        "counts_before": _ZERO_COUNTS,
        "counts_after": _ZERO_COUNTS,
        "semantic_deltas": {key: 0 for key in _ZERO_COUNTS},
    }


async def test_preparer_retries_after_failed_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    async def apply(_conn, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient failure")
        return SimpleNamespace(
            manifest_ref="founder-manifest:acme:v1", alias_count=2,
        )

    monkeypatch.setattr(founder_bootstrap, "apply_founder_identity_bootstrap", apply)
    preparer = _preparer()
    tenant_id = uuid4()

    with pytest.raises(RuntimeError, match="transient failure"):
        await preparer(_FakeConnection(), tenant_id, object(), {})
    await preparer(_FakeConnection(), tenant_id, object(), {})

    assert attempts == 2
    assert preparer.result is not None


async def test_preparer_cannot_leak_one_manifest_across_tenants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def apply(_conn, **_kwargs):
        return SimpleNamespace(
            manifest_ref="founder-manifest:acme:v1", alias_count=2,
        )

    monkeypatch.setattr(founder_bootstrap, "apply_founder_identity_bootstrap", apply)
    preparer = _preparer()
    await preparer(_FakeConnection(), uuid4(), object(), {})

    with pytest.raises(ValueError, match="cannot be shared across tenants"):
        await preparer(_FakeConnection(), uuid4(), object(), {})


async def test_receipt_exposes_nonzero_semantic_delta_without_hiding_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def apply(_conn, **_kwargs):
        return SimpleNamespace(
            manifest_ref="founder-manifest:acme:v1", alias_count=2,
        )

    monkeypatch.setattr(founder_bootstrap, "apply_founder_identity_bootstrap", apply)
    after = {**_ZERO_COUNTS, "models": 1}
    preparer = _preparer()

    await preparer(
        _FakeConnection(_ZERO_COUNTS, after), uuid4(), object(), {},
    )

    assert preparer.receipt is not None
    assert preparer.receipt["semantic_truth_unchanged"] is False
    assert preparer.receipt["semantic_deltas"]["models"] == 1


async def test_receipt_property_returns_a_defensive_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def apply(_conn, **_kwargs):
        return SimpleNamespace(
            manifest_ref="founder-manifest:acme:v1", alias_count=2,
        )

    monkeypatch.setattr(founder_bootstrap, "apply_founder_identity_bootstrap", apply)
    preparer = _preparer()
    await preparer(_FakeConnection(), uuid4(), object(), {})

    receipt = preparer.receipt
    assert receipt is not None
    receipt["semantic_deltas"]["models"] = 99

    assert preparer.receipt is not None
    assert preparer.receipt["semantic_deltas"]["models"] == 0


async def test_helper_source_does_not_import_sealed_gold_or_model_writers() -> None:
    source = founder_bootstrap.__file__
    assert source is not None
    text = open(source, encoding="utf-8").read()

    assert "P6Gold" not in text
    assert "p6_gold" not in text
    assert "services.domain.models" not in text
    assert "model_constructor" not in text
