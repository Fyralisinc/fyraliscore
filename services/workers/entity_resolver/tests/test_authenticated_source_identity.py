from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from lib.contracts.kernel import BitemporalInterval
from lib.contracts.perception import SourceIdentityBinding
from services.domain.source_identity_bindings import ResolvedSourceIdentityBinding
from services.workers.entity_resolver.context import (
    KnownEntityCandidate,
    RecentAlias,
    ResolverContext,
)
from services.workers.entity_resolver.worker import EntityResolverWorker


NOW = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)


def _context() -> ResolverContext:
    tenant_id = uuid4()
    target_id = uuid4()
    binding = SourceIdentityBinding(
        binding_id="binding:pagerduty:mercury",
        binding_version=3,
        binding_lineage_id="binding-lineage:pagerduty:mercury",
        tenant_id=tenant_id,
        source_system="pagerduty",
        source_native_identifier="pagerduty:service:mercury",
        source_identity_authority_ref="pagerduty-service-contract-v2",
        canonical_referent_type="resource",
        canonical_referent_id=str(target_id),
        canonical_referent_version=2,
        temporal_scope=BitemporalInterval(
            valid_from=NOW - timedelta(days=1),
            transaction_from=NOW - timedelta(hours=1),
        ),
        evidence_refs=("pagerduty:service:mercury",),
    )
    return ResolverContext(
        observation_id=uuid4(),
        phrase="  MERCURY  ",
        tenant_id=tenant_id,
        source_identity_binding=ResolvedSourceIdentityBinding(
            binding=binding,
            canonical_ref={
                "type": "resource",
                "id": str(target_id),
                "version": 2,
            },
            attachment_authority_ref="pagerduty-envelope-v1",
            source_surface="Mercury",
        ),
    )


def test_authenticated_source_binding_becomes_deterministic_resolution() -> None:
    ctx = _context()

    resolution = EntityResolverWorker._authenticated_source_identity_resolution(ctx)

    assert resolution is not None
    assert resolution.canonical_ref == ctx.source_identity_binding.canonical_ref
    assert resolution.confidence == 1.0
    assert resolution.decision_source == "authenticated_source_identity_binding"
    assert resolution.identity_basis_ref == (
        "source-identity-binding:binding:pagerduty:mercury:version:3"
    )
    assert resolution.resolution_scope == "observation_source_surface_exact"


def test_authenticated_source_binding_fails_closed_on_lineage_mismatch() -> None:
    ctx = _context()
    assert ctx.source_identity_binding is not None
    ctx.source_identity_binding.canonical_ref["id"] = str(uuid4())

    assert (
        EntityResolverWorker._authenticated_source_identity_resolution(ctx)
        is None
    )


def test_authenticated_source_binding_fails_closed_off_attached_surface() -> None:
    ctx = _context()
    ctx.phrase = "Mercury Billing"

    assert (
        EntityResolverWorker._authenticated_source_identity_resolution(ctx)
        is None
    )


def test_context_only_adjudication_is_not_reused_as_global_candidate() -> None:
    ctx = ResolverContext(
        observation_id=uuid4(),
        phrase="the project",
        tenant_id=uuid4(),
        recent_aliases=[RecentAlias(
            alias_id=uuid4(),
            alias_text="the project",
            resolved_entity_ref={"type": "goal", "id": "project-northstar"},
            confidence=0.99,
            source="manual",
            identity_basis_class="independently_adjudicated",
            identity_basis_ref="clarification-request:local-only",
            adjudication_state="active",
            resolution_scope="source_context_only",
            canonical_target_valid=True,
        )],
        known_entity_candidates=[KnownEntityCandidate(
            alias_id=uuid4(),
            alias_text="the project",
            resolved_entity_ref={"type": "goal", "id": "project-northstar"},
            confidence=0.99,
            source="manual",
            identity_basis_class="independently_adjudicated",
            identity_basis_ref="clarification-request:local-only",
            adjudication_state="active",
            resolution_scope="source_context_only",
            canonical_target_valid=True,
        )],
    )

    assert EntityResolverWorker._candidate_inputs(ctx) == ()
