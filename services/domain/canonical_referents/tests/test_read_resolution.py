from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import pytest

from services.domain.canonical_referents.repo import CanonicalReferentLineage
from services.domain.canonical_referents.service import (
    CanonicalReferentRegistryService,
)
from services.domain.canonical_referents.types import (
    CanonicalReferentVersionRef,
)


TENANT_ID = UUID("11111111-1111-4111-8111-111111111111")
AT = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def _ref(name: str) -> CanonicalReferentVersionRef:
    return CanonicalReferentVersionRef(
        type="resource",
        id=f"resource:{name}",
        version=1,
    )


class _LineageRepo:
    def __init__(self, lineage: CanonicalReferentLineage) -> None:
        self.lineage = lineage
        self.calls = []

    async def lineage_at(self, **kwargs):
        self.calls.append(kwargs)
        return self.lineage


@pytest.mark.asyncio
async def test_resolve_at_returns_requested_lineage_and_effective_head() -> None:
    requested = _ref("root")
    head = _ref("head")
    lineage = CanonicalReferentLineage(
        tenant_id=TENANT_ID,
        valid_at=AT,
        known_at=AT,
        members=(requested, head),
    )
    repo = _LineageRepo(lineage)
    service = CanonicalReferentRegistryService(None, repo=repo)  # type: ignore[arg-type]

    resolution = await service.resolve_at(
        tenant_id=TENANT_ID,
        referent=requested,
        valid_at=AT,
        known_at=AT,
    )

    assert resolution.requested_ref == requested
    assert resolution.effective_ref == head
    assert resolution.lineage is lineage
    assert resolution.replaced is True
    assert repo.calls == [
        {
            "tenant_id": TENANT_ID,
            "referent": requested,
            "valid_at": AT,
            "known_at": AT,
            "conn": None,
        }
    ]


@pytest.mark.asyncio
async def test_resolve_at_marks_unreplaced_singleton() -> None:
    requested = _ref("root")
    lineage = CanonicalReferentLineage(
        tenant_id=TENANT_ID,
        valid_at=AT,
        known_at=AT,
        members=(requested,),
    )
    service = CanonicalReferentRegistryService(
        None,
        repo=_LineageRepo(lineage),  # type: ignore[arg-type]
    )

    resolution = await service.resolve_at(
        tenant_id=TENANT_ID,
        referent=requested,
        valid_at=AT,
        known_at=AT,
    )

    assert resolution.effective_ref == requested
    assert resolution.replaced is False
