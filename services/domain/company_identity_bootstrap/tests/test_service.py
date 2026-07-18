from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from lib.shared.errors import ValidationError
from services.domain.company_identity_bootstrap.service import (
    FounderIdentityBootstrapEntry,
    apply_founder_identity_bootstrap,
)


class _Connection:
    def __init__(self, rows=()):
        self.rows = rows

    @asynccontextmanager
    async def transaction(self):
        yield

    async def fetch(self, *_args):
        return self.rows


@pytest.mark.asyncio
async def test_bootstrap_writes_exact_authoritative_aliases_only(monkeypatch) -> None:
    calls: list[dict] = []

    async def insert(_conn, **kwargs):
        calls.append(kwargs)
        return SimpleNamespace(
            resolved_entity_ref=kwargs["resolved_entity_ref"],
            entity_metadata={**kwargs["extra_metadata"], "source": kwargs["source"]},
        )

    monkeypatch.setattr(
        "services.domain.company_identity_bootstrap.service.insert_alias_with_connection",
        insert,
    )
    result = await apply_founder_identity_bootstrap(
        _Connection(),  # type: ignore[arg-type]
        tenant_id=uuid4(),
        manifest_ref="founder-map:2026-07-18:v1",
        authority_ref="company-founder-assertion:alice",
        asserted_by_ref="founder:alice",
        provenance_refs=("founder-workshop:recording-1",),
        effective_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
        entries=(
            FounderIdentityBootstrapEntry(
                canonical_ref={
                    "type": "workstream",
                    "id": "workstream:atlas-release",
                    "version": 1,
                },
                canonical_name="Atlas Release",
                aliases=("Atlas",),
            ),
        ),
    )

    assert result.alias_count == 2
    assert [call["phrase"] for call in calls] == ["Atlas Release", "Atlas"]
    assert [call["is_canonical"] for call in calls] == [True, False]
    assert all(call["source"] == "ingestion" for call in calls)
    assert all(call["confidence"] == 1.0 for call in calls)
    metadata = calls[0]["extra_metadata"]
    assert metadata["founder_bootstrap_contract"] == {"version": "v1"}
    assert metadata["identity_basis_class"] == "source_authoritative"
    assert metadata["identity_basis_ref"] == "founder-map:2026-07-18:v1"
    assert metadata["resolution_scope"] == "tenant_global_exact"
    assert metadata["canonical_identity_authority"] is True
    assert metadata["behavioral_model_authority"] is False


@pytest.mark.asyncio
async def test_bootstrap_rejects_ambiguous_exact_name_before_writing(
    monkeypatch,
) -> None:
    called = False

    async def insert(_conn, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "services.domain.company_identity_bootstrap.service.insert_alias_with_connection",
        insert,
    )
    with pytest.raises(ValidationError, match="multiple referents"):
        await apply_founder_identity_bootstrap(
            _Connection(),  # type: ignore[arg-type]
            tenant_id=uuid4(),
            manifest_ref="founder-map:v1",
            authority_ref="founder-assertion:alice",
            asserted_by_ref="founder:alice",
            provenance_refs=("workshop:1",),
            effective_at=datetime.now(timezone.utc),
            entries=(
                FounderIdentityBootstrapEntry(
                    {"type": "workstream", "id": "atlas", "version": 1},
                    "Atlas",
                ),
                FounderIdentityBootstrapEntry(
                    {"type": "customer", "id": "atlas", "version": 1},
                    " atlas ",
                ),
            ),
        )
    assert called is False


@pytest.mark.asyncio
async def test_bootstrap_requires_evidence_and_timezone() -> None:
    entry = FounderIdentityBootstrapEntry(
        {"type": "workstream", "id": "atlas", "version": 1},
        "Atlas",
    )
    common = dict(
        conn=_Connection(),
        tenant_id=uuid4(),
        manifest_ref="founder-map:v1",
        authority_ref="founder-assertion:alice",
        asserted_by_ref="founder:alice",
        entries=(entry,),
    )
    with pytest.raises(ValidationError, match="provenance_refs"):
        await apply_founder_identity_bootstrap(
            **common,  # type: ignore[arg-type]
            provenance_refs=(),
            effective_at=datetime.now(timezone.utc),
        )
    with pytest.raises(ValidationError, match="timezone-aware"):
        await apply_founder_identity_bootstrap(
            **common,  # type: ignore[arg-type]
            provenance_refs=("workshop:1",),
            effective_at=datetime.now(),
        )


@pytest.mark.asyncio
async def test_bootstrap_rejects_existing_normalized_collision(monkeypatch) -> None:
    called = False

    async def insert(_conn, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "services.domain.company_identity_bootstrap.service.insert_alias_with_connection",
        insert,
    )
    conn = _Connection(
        rows=(
            {
                "normalized": "atlas",
                "resolved_entity_ref": {
                    "type": "customer",
                    "id": "customer:atlas",
                    "version": 1,
                },
            },
        )
    )
    with pytest.raises(ValidationError, match="already maps"):
        await apply_founder_identity_bootstrap(
            conn,  # type: ignore[arg-type]
            tenant_id=uuid4(),
            manifest_ref="founder-map:v1",
            authority_ref="founder-assertion:alice",
            asserted_by_ref="founder:alice",
            provenance_refs=("workshop:1",),
            effective_at=datetime.now(timezone.utc),
            entries=(
                FounderIdentityBootstrapEntry(
                    {"type": "workstream", "id": "atlas", "version": 1},
                    " Atlas ",
                ),
            ),
        )
    assert called is False
