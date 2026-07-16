from __future__ import annotations

from inspect import signature

from lib.shared.ids import uuid7
from services.workers.entity_resolver.context import (
    KnownEntityCandidate,
    RecentAlias,
    ResolverContext,
)
from services.workers.entity_resolver.worker import EntityResolverWorker


def _recent_alias(
    *,
    alias_text: str,
    identity_basis_ref: str,
) -> RecentAlias:
    return RecentAlias(
        alias_id=uuid7(),
        alias_text=alias_text,
        resolved_entity_ref={"type": "customer", "id": alias_text.casefold()},
        confidence=0.99,
        source="manual",
        identity_basis_class="independently_adjudicated",
        identity_basis_ref=identity_basis_ref,
    )


def _known_candidate(
    *,
    alias_text: str,
    identity_basis_ref: str,
) -> KnownEntityCandidate:
    return KnownEntityCandidate(
        alias_id=uuid7(),
        alias_text=alias_text,
        resolved_entity_ref={"type": "customer", "id": alias_text.casefold()},
        confidence=0.99,
        source="manual",
        identity_basis_class="independently_adjudicated",
        identity_basis_ref=identity_basis_ref,
    )


def test_corrective_memory_reuse_is_production_enabled_by_default() -> None:
    parameter = signature(EntityResolverWorker).parameters[
        "corrective_memory_reuse_enabled"
    ]

    assert parameter.default is True


def test_frozen_control_hides_only_clarification_learned_aliases() -> None:
    learned_recent = _recent_alias(
        alias_text="NBI",
        identity_basis_ref=f"clarification-request:{uuid7()}",
    )
    manual_recent = _recent_alias(
        alias_text="Nimbus Bank",
        identity_basis_ref="manual-review:nimbus-bank",
    )
    learned_candidate = _known_candidate(
        alias_text="NBI",
        identity_basis_ref=f"clarification-request:{uuid7()}",
    )
    manual_candidate = _known_candidate(
        alias_text="Nimbus Bank",
        identity_basis_ref="manual-review:nimbus-bank",
    )
    ctx = ResolverContext(
        observation_id=uuid7(),
        phrase="NBI",
        tenant_id=uuid7(),
        recent_aliases=[learned_recent, manual_recent],
        known_entity_candidates=[learned_candidate, manual_candidate],
    )

    EntityResolverWorker._hide_clarification_learned_aliases(ctx)

    assert ctx.recent_aliases == [manual_recent]
    assert ctx.known_entity_candidates == [manual_candidate]
