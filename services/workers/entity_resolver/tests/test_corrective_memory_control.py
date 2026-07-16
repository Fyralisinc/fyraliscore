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


def test_candidate_inputs_exclude_inactive_uuid_backed_targets() -> None:
    inactive_actor = RecentAlias(
        alias_id=uuid7(),
        alias_text="Former Owner",
        resolved_entity_ref={"type": "actor", "id": str(uuid7())},
        confidence=0.99,
        source="manual",
        identity_basis_class="source_authoritative",
        identity_basis_ref="directory:former-owner",
        canonical_target_valid=False,
    )
    archived_resource = KnownEntityCandidate(
        alias_id=uuid7(),
        alias_text="Legacy Gateway",
        resolved_entity_ref={"type": "resource", "id": str(uuid7())},
        confidence=0.99,
        source="manual",
        identity_basis_class="independently_adjudicated",
        identity_basis_ref="manual-review:legacy-gateway",
        canonical_target_valid=False,
    )
    active_customer = RecentAlias(
        alias_id=uuid7(),
        alias_text="Nimbus Bank",
        resolved_entity_ref={"type": "customer", "id": str(uuid7())},
        confidence=0.99,
        source="manual",
        identity_basis_class="independently_adjudicated",
        identity_basis_ref="manual-review:nimbus-bank",
        canonical_target_valid=True,
    )
    ctx = ResolverContext(
        observation_id=uuid7(),
        phrase="owner",
        tenant_id=uuid7(),
        recent_aliases=[inactive_actor, active_customer],
        known_entity_candidates=[archived_resource],
    )

    inputs = EntityResolverWorker._candidate_inputs(ctx)

    assert [item.canonical_ref for item in inputs] == [
        active_customer.resolved_entity_ref
    ]


def test_candidate_inputs_preserve_non_lifecycle_and_legacy_refs() -> None:
    source_object = RecentAlias(
        alias_id=uuid7(),
        alias_text="ENG-123",
        resolved_entity_ref={"type": "jira_issue", "id": "ENG-123"},
        confidence=0.99,
        source="ingestion",
        identity_basis_class="source_authoritative",
        identity_basis_ref="jira:issue:ENG-123",
        canonical_target_valid=False,
    )
    legacy_customer = KnownEntityCandidate(
        alias_id=uuid7(),
        alias_text="Acme",
        resolved_entity_ref={"type": "customer", "id": "salesforce:acct-123"},
        confidence=0.99,
        source="ingestion",
        identity_basis_class="source_authoritative",
        identity_basis_ref="salesforce:acct-123",
        canonical_target_valid=False,
    )
    ctx = ResolverContext(
        observation_id=uuid7(),
        phrase="Acme",
        tenant_id=uuid7(),
        recent_aliases=[source_object],
        known_entity_candidates=[legacy_customer],
    )

    inputs = EntityResolverWorker._candidate_inputs(ctx)

    assert {item.canonical_ref["type"] for item in inputs} == {
        "jira_issue",
        "customer",
    }
    by_type = {item.canonical_ref["type"]: item for item in inputs}
    assert by_type["jira_issue"].exact_mention_match is True
    assert by_type["jira_issue"].decisive_authority_refs == (
        "jira:issue:ENG-123",
    )
    assert by_type["customer"].exact_mention_match is False


def test_generic_source_hint_is_conflicting_but_never_decisive() -> None:
    learned = RecentAlias(
        alias_id=uuid7(),
        alias_text="NBI",
        resolved_entity_ref={"type": "customer", "id": "customer:nimbus"},
        confidence=0.99,
        source="manual",
        identity_basis_class="independently_adjudicated",
        identity_basis_ref="clarification-request:nbi",
        canonical_target_valid=False,
    )
    source_ref = {"type": "customer", "id": "customer:other"}
    ctx = ResolverContext(
        observation_id=uuid7(),
        phrase="NBI",
        tenant_id=uuid7(),
        recent_aliases=[learned],
        source_entities_mentioned=[source_ref],
    )

    inputs = EntityResolverWorker._candidate_inputs(ctx)
    source_input = next(
        item for item in inputs if item.candidate_source == "source_mentions"
    )

    assert source_input.exact_mention_match is True
    assert source_input.decisive_authority_refs == ()


def test_multiple_source_refs_form_a_conservative_exact_conflict() -> None:
    ctx = ResolverContext(
        observation_id=uuid7(),
        phrase="Cafe Ops",
        tenant_id=uuid7(),
        source_entities_mentioned=[
            {"type": "team", "id": "team:cafe-primary"},
            {"type": "team", "id": "team:cafe-conflict"},
        ],
    )

    inputs = EntityResolverWorker._candidate_inputs(ctx)

    assert len(inputs) == 2
    assert all(item.exact_mention_match for item in inputs)
    assert all(item.decisive_authority_refs == () for item in inputs)
