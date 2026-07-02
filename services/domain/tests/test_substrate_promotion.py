from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from lib.shared.errors import ValidationError
import services.domain.substrate_promotion as substrate_promotion
from services.domain.substrate_candidates import SubstrateCandidate
from services.domain.substrate_promotion import (
    apply_candidate_resolution_answer,
    auto_promote_candidate,
    backfill_promoted_candidate_scopes,
    open_candidate_clarification,
    plan_candidate_promotion,
    promote_actor_candidate,
    promote_commitment_candidate,
    promote_pattern_substrate_candidate,
    promote_resource_candidate,
)


def _candidate(
    *,
    kind: str = "actor",
    confidence: float = 0.86,
    aliases: list[dict] | None = None,
    metadata: dict | None = None,
    related_candidate_ids: list | None = None,
    evidence_model_ids: list | None = None,
) -> SubstrateCandidate:
    return SubstrateCandidate(
        id=uuid4(),
        tenant_id=uuid4(),
        kind=kind,
        label="Alpen Ops",
        status="proposed",
        confidence=confidence,
        fingerprint=f"{kind}:alpen-ops",
        aliases=aliases or [],
        evidence_observation_ids=[uuid4(), uuid4()],
        evidence_model_ids=evidence_model_ids or [],
        related_candidate_ids=related_candidate_ids or [],
        metadata=metadata or {},
    )


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.fetch_results: list[list[dict]] = []
        self.fetchval_results: list = []
        self.fetchvals: list[tuple[str, tuple]] = []
        self.fetchrows: list[tuple[str, tuple]] = []

    async def execute(self, query: str, *args):
        self.executed.append((query, args))
        return "UPDATE 1"

    async def fetch(self, query: str, *args):
        if self.fetch_results:
            return self.fetch_results.pop(0)
        return []

    async def fetchval(self, query: str, *args):
        self.fetchvals.append((query, args))
        if self.fetchval_results:
            return self.fetchval_results.pop(0)
        if "INSERT INTO clarification_requests" in query:
            return args[0]
        return None

    async def fetchrow(self, query: str, *args):
        self.fetchrows.append((query, args))
        return {"id": args[0]}


@pytest.mark.asyncio
async def test_pattern_review_enqueue_payload_requires_semantic_review() -> None:
    tenant_id = uuid4()
    candidate_id = uuid4()
    observation_id = uuid4()
    constituent_id = uuid4()

    class _PatternReviewConn(_FakeConn):
        async def fetchrow(self, query: str, *args):
            self.fetchrows.append((query, args))
            assert "FROM pattern_candidates" in query
            assert args == (tenant_id, candidate_id)
            return {
                "proposed_signature": {"kind": "substrate_recurrence"},
                "observed_tendency": {"summary": "review loop recurs"},
                "constituent_model_ids": [constituent_id],
                "cluster_size": 1,
                "density": 0.72,
            }

    conn = _PatternReviewConn()
    conn.fetchval_results.append(True)

    trigger_id = await substrate_promotion._enqueue_pattern_review_if_possible(
        conn,
        tenant_id=tenant_id,
        pattern_candidate_id=candidate_id,
        observation_id=observation_id,
    )

    assert trigger_id is not None
    query, args = conn.executed[-1]
    assert "INSERT INTO think_trigger_queue" in query
    payload = json.loads(args[6])
    assert payload["pattern_candidate_id"] == str(candidate_id)
    assert payload["source"] == "substrate_promotion"
    assert payload["review_mode"] == "semantic_required"
    assert payload["constituent_model_ids"] == [str(constituent_id)]
    assert payload["proposed_signature"]["kind"] == "substrate_recurrence"


def test_high_confidence_actor_plan_preserves_cross_source_aliases() -> None:
    candidate = _candidate(
        aliases=[
            {
                "source_channel": "slack:message",
                "source_actor_ref": "U_ALPEN",
                "confidence": 0.91,
            },
            {
                "source_channel": "github:webhook",
                "source_actor_ref": "alpen-ops",
                "confidence": 0.82,
            },
            {
                "source_channel": "github:webhook",
                "source_actor_ref": "alpen-ops",
                "confidence": 0.82,
            },
        ],
        metadata={"company_domains": ["alpen.example"], "email": "ops@alpen.example"},
    )

    plan = plan_candidate_promotion(candidate)

    assert plan.action == "promote_actor"
    assert plan.actor is not None
    assert plan.actor["type"] == "human_internal"
    assert [m["source_channel"] for m in plan.alias_mappings] == [
        "slack:message",
        "github:webhook",
    ]
    assert [m["source_actor_ref"] for m in plan.alias_mappings] == [
        "U_ALPEN",
        "alpen-ops",
    ]


def test_low_confidence_or_ambiguous_actor_requires_user_resolution() -> None:
    candidate = _candidate(
        confidence=0.58,
        aliases=[
            {"source_channel": "slack:message", "source_actor_ref": "sam"},
        ],
        metadata={"same_label_candidate_ids": [str(uuid4())]},
    )

    plan = plan_candidate_promotion(candidate)

    assert plan.action == "ask_user"
    assert plan.needs_user is True
    assert plan.clarification is not None
    assert plan.clarification["priority"] == "normal"
    assert plan.clarification["payload"]["ambiguity"]["same_label_candidate_ids"]


def test_customer_vendor_system_candidates_plan_semantic_resources() -> None:
    customer = plan_candidate_promotion(_candidate(kind="customer"))
    vendor = plan_candidate_promotion(_candidate(kind="vendor"))
    system = plan_candidate_promotion(_candidate(kind="system"))

    assert customer.action == "promote_resource"
    assert customer.resource["kind"] == "relational"
    assert customer.resource["current_value"]["semantic_kind"] == "customer"
    assert vendor.resource["kind"] == "relational"
    assert vendor.resource["current_value"]["semantic_kind"] == "vendor"
    assert system.resource["kind"] == "infrastructure"
    assert system.resource["current_value"]["semantic_kind"] == "system"


def test_commitment_candidate_plan_uses_explicit_promotion_floor() -> None:
    promotable = plan_candidate_promotion(_candidate(kind="commitment", confidence=0.80))
    below_commitment_floor = plan_candidate_promotion(
        _candidate(kind="commitment", confidence=0.74)
    )
    weak = plan_candidate_promotion(_candidate(kind="commitment", confidence=0.62))

    assert promotable.action == "promote_commitment"
    assert promotable.reason == "high_confidence_commitment_candidate"
    assert below_commitment_floor.action == "keep_provisional"
    assert below_commitment_floor.reason == "commitment_confidence_below_promotion_floor"
    assert weak.action == "ask_user"


@pytest.mark.asyncio
async def test_open_candidate_clarification_marks_candidate_waiting() -> None:
    conn = _FakeConn()
    candidate = _candidate(confidence=0.41)

    request_id = await open_candidate_clarification(conn, candidate=candidate)

    assert request_id == conn.fetchvals[0][1][0]
    assert "INSERT INTO clarification_requests" in conn.fetchvals[0][0]
    assert "UPDATE substrate_candidates" in conn.executed[0][0]
    assert conn.executed[0][1][2] == "needs_clarification"


@pytest.mark.asyncio
async def test_apply_candidate_resolution_answer_handles_reject_merge_and_link() -> None:
    conn = _FakeConn()
    candidate = _candidate()
    merge_target_id = uuid4()
    canonical_ref = {"type": "actor", "id": str(uuid4())}

    rejected = await apply_candidate_resolution_answer(
        conn,
        candidate=candidate,
        answer={"action": "reject", "reason": "test duplicate"},
    )
    merged = await apply_candidate_resolution_answer(
        conn,
        candidate=candidate,
        answer={
            "action": "merge",
            "merge_target_id": str(merge_target_id),
            "canonical_ref": canonical_ref,
        },
    )
    linked = await apply_candidate_resolution_answer(
        conn,
        candidate=candidate,
        answer={"action": "link_existing", "canonical_ref": canonical_ref},
    )

    assert rejected.action == "reject"
    assert merged.action == "merge"
    assert linked.action == "link_existing"
    statuses = [args[2] for _query, args in conn.executed]
    assert statuses == ["rejected", "merged", "promoted"]


@pytest.mark.asyncio
async def test_apply_candidate_resolution_promote_actor_creates_canonical_actor() -> None:
    conn = _FakeConn()
    candidate = _candidate(
        aliases=[{"source_channel": "slack:message", "source_actor_ref": "U1"}],
        metadata={"actor_type": "human_internal"},
    )

    plan = await apply_candidate_resolution_answer(
        conn,
        candidate=candidate,
        answer={"action": "promote_actor"},
    )

    assert plan.action == "promote_actor"
    assert plan.canonical_ref is not None
    assert plan.canonical_ref["type"] == "actor"
    assert any("INSERT INTO actors" in query for query, _args in conn.fetchrows)
    assert sum(
        "UPDATE substrate_candidates" in query and args[2] == "promoted"
        for query, args in conn.executed
    ) >= 1


@pytest.mark.asyncio
async def test_apply_candidate_resolution_link_existing_actor_backfills_scope() -> None:
    conn = _FakeConn()
    actor_id = uuid4()
    model_id = uuid4()
    candidate = _candidate()
    conn.fetch_results.append(
        [
            {
                "id": model_id,
                "scope_actors": [],
                "scope_entities": [candidate.scope_ref],
            }
        ]
    )

    plan = await apply_candidate_resolution_answer(
        conn,
        candidate=candidate,
        answer={
            "action": "link_existing",
            "canonical_ref": {"type": "actor", "id": str(actor_id)},
        },
    )

    assert plan.action == "link_existing"
    assert any(
        "INSERT INTO model_scope_actors" in query and args[2] == actor_id
        for query, args in conn.executed
    )


@pytest.mark.asyncio
async def test_apply_candidate_resolution_promote_resource_executes_resource_creation(
    monkeypatch,
) -> None:
    conn = _FakeConn()
    resource_id = uuid4()
    candidate = _candidate(kind="customer", confidence=0.64)
    conn.fetchval_results.append(None)
    created_calls: list[dict] = []

    async def fake_create(**kwargs):
        created_calls.append(kwargs)
        return SimpleNamespace(id=resource_id)

    monkeypatch.setattr(substrate_promotion.resources_repo, "create", fake_create)

    plan = await apply_candidate_resolution_answer(
        conn,
        candidate=candidate,
        answer={"action": "promote_resource"},
    )

    assert plan.action == "promote_resource"
    assert plan.canonical_ref == {
        "type": "customer",
        "id": str(resource_id),
        "resource_id": str(resource_id),
    }
    assert created_calls
    assert created_calls[0]["metadata"]["promoted_from_candidate_id"] == str(
        candidate.id
    )


@pytest.mark.asyncio
async def test_apply_candidate_resolution_promote_commitment_executes_commitment_creation(
    monkeypatch,
) -> None:
    conn = _FakeConn()
    commitment_id = uuid4()
    candidate = _candidate(kind="commitment", confidence=0.61)
    conn.fetchval_results.append(None)
    created_calls: list[dict] = []

    async def fake_create(**kwargs):
        created_calls.append(kwargs)
        return SimpleNamespace(id=commitment_id)

    monkeypatch.setattr(substrate_promotion.commitments_svc, "create", fake_create)

    plan = await apply_candidate_resolution_answer(
        conn,
        candidate=candidate,
        answer={"action": "promote_commitment"},
    )

    assert plan.action == "promote_commitment"
    assert plan.canonical_ref == {"type": "commitment", "id": str(commitment_id)}
    assert created_calls
    assert created_calls[0]["initial_state"] == "proposed"


@pytest.mark.asyncio
async def test_apply_candidate_resolution_requires_merge_target_or_canonical_ref() -> None:
    with pytest.raises(ValidationError):
        await apply_candidate_resolution_answer(
            _FakeConn(),
            candidate=_candidate(),
            answer={"action": "merge"},
        )


@pytest.mark.asyncio
async def test_promote_actor_candidate_creates_actor_and_alias_mappings() -> None:
    conn = _FakeConn()
    candidate = _candidate(
        aliases=[
            {"source_channel": "slack:message", "source_actor_ref": "U1"},
            {"source_channel": "email:message", "source_actor_ref": "ops@alpen.test"},
        ],
        metadata={"actor_type": "human_external"},
    )
    plan = plan_candidate_promotion(candidate)

    result = await promote_actor_candidate(conn, candidate=candidate, plan=plan)

    assert result["canonical_ref"]["type"] == "actor"
    assert result["alias_mapping_count"] == 2
    assert "INSERT INTO actors" in conn.fetchrows[0][0]
    assert sum("actor_identity_mappings" in query for query, _args in conn.executed) == 2
    assert conn.executed[-1][1][2] == "promoted"


@pytest.mark.asyncio
async def test_promote_actor_candidate_backfills_model_scope_actor() -> None:
    conn = _FakeConn()
    model_id = uuid4()
    candidate = _candidate(
        aliases=[{"source_channel": "slack:message", "source_actor_ref": "U1"}],
        metadata={"actor_type": "human_internal"},
    )
    conn.fetch_results.append(
        [
            {
                "id": model_id,
                "scope_actors": [],
                "scope_entities": [candidate.scope_ref],
            }
        ]
    )

    result = await promote_actor_candidate(
        conn,
        candidate=candidate,
        plan=plan_candidate_promotion(candidate),
    )

    actor_id = result["actor_id"]
    assert result["backfilled_models"] == 1
    assert any(
        "UPDATE models" in query and args[2] == [actor_id]
        for query, args in conn.executed
    )
    assert any(
        "INSERT INTO model_scope_actors" in query and args[2] == actor_id
        for query, args in conn.executed
    )


@pytest.mark.asyncio
async def test_promote_resource_candidate_creates_customer_and_backfills_scopes(
    monkeypatch,
) -> None:
    conn = _FakeConn()
    resource_id = uuid4()
    model_id = uuid4()
    created_calls: list[dict] = []
    candidate = _candidate(
        kind="customer",
        confidence=0.88,
        aliases=[{"alias_text": "Alpen Customer"}],
    )
    conn.fetchval_results.append(None)
    conn.fetch_results.append(
        [
            {
                "id": model_id,
                "scope_actors": [],
                "scope_entities": [candidate.scope_ref],
            }
        ]
    )

    async def fake_create(**kwargs):
        created_calls.append(kwargs)
        return SimpleNamespace(id=resource_id)

    monkeypatch.setattr(substrate_promotion.resources_repo, "create", fake_create)

    result = await promote_resource_candidate(
        conn,
        candidate=candidate,
        plan=plan_candidate_promotion(candidate),
    )

    assert result["canonical_ref"] == {
        "type": "customer",
        "id": str(resource_id),
        "resource_id": str(resource_id),
    }
    assert created_calls[0]["kind"] == "relational"
    assert created_calls[0]["created_by_event_id"] == candidate.evidence_observation_ids[0]
    assert created_calls[0]["metadata"]["promoted_from_candidate_id"] == str(candidate.id)
    assert result["backfilled_models"] == 1
    entity_types = [
        args[2]
        for query, args in conn.executed
        if "INSERT INTO model_scope_entities" in query
    ]
    assert entity_types == ["resource", "customer"]
    assert any("INSERT INTO entity_aliases" in query for query, _args in conn.executed)
    assert any(
        "UPDATE substrate_candidates" in query and args[2] == "promoted"
        for query, args in conn.executed
    )


@pytest.mark.asyncio
async def test_promote_commitment_candidate_creates_proposed_commitment_and_backfills(
    monkeypatch,
) -> None:
    conn = _FakeConn()
    commitment_id = uuid4()
    model_id = uuid4()
    created_calls: list[dict] = []
    candidate = _candidate(kind="commitment", confidence=0.81)
    conn.fetchval_results.append(None)
    conn.fetch_results.append(
        [
            {
                "id": model_id,
                "scope_actors": [],
                "scope_entities": [candidate.scope_ref],
            }
        ]
    )

    async def fake_create(**kwargs):
        created_calls.append(kwargs)
        return SimpleNamespace(id=commitment_id)

    monkeypatch.setattr(substrate_promotion.commitments_svc, "create", fake_create)

    result = await promote_commitment_candidate(conn, candidate=candidate)

    assert result["canonical_ref"] == {"type": "commitment", "id": str(commitment_id)}
    assert created_calls[0]["initial_state"] == "proposed"
    assert created_calls[0]["is_maintenance"] is True
    assert created_calls[0]["created_by_event_id"] == candidate.evidence_observation_ids[0]
    assert created_calls[0]["estimated_capacity"]["promoted_from_candidate_id"] == str(
        candidate.id
    )
    assert result["backfilled_models"] == 1
    assert any(
        "INSERT INTO model_scope_entities" in query
        and args[2] == "commitment"
        and args[3] == commitment_id
        for query, args in conn.executed
    )


@pytest.mark.asyncio
async def test_promote_commitment_links_promoted_customer_candidate(
    monkeypatch,
) -> None:
    conn = _FakeConn()
    customer_id = uuid4()
    commitment_id = uuid4()
    customer_candidate_id = uuid4()
    candidate = _candidate(
        kind="commitment",
        confidence=0.81,
        related_candidate_ids=[customer_candidate_id],
    )
    conn.fetchval_results.append(None)
    conn.fetch_results.append([])
    conn.fetch_results.append(
        [
            {
                "id": customer_candidate_id,
                "kind": "customer",
                "label": "Acme Corp",
                "promotion_ref": {"type": "customer", "id": str(customer_id)},
                "proposed_canonical_ref": None,
            }
        ]
    )
    linked_calls: list[dict] = []

    async def fake_create(**kwargs):
        return SimpleNamespace(id=commitment_id)

    async def fake_link(customer_resource_id, linked_commitment_id, **kwargs):
        linked_calls.append(
            {
                "customer_resource_id": customer_resource_id,
                "commitment_id": linked_commitment_id,
                **kwargs,
            }
        )
        return SimpleNamespace(id=uuid4())

    monkeypatch.setattr(substrate_promotion.commitments_svc, "create", fake_create)
    monkeypatch.setattr(
        substrate_promotion.customer_commitments_svc,
        "link_commitment",
        fake_link,
    )

    result = await promote_commitment_candidate(conn, candidate=candidate)

    assert result["linked_customer_commitments"] == 1
    assert linked_calls == [
        {
            "customer_resource_id": customer_id,
            "commitment_id": commitment_id,
            "tenant_id": candidate.tenant_id,
            "relationship_kind": "delivers",
            "criticality": "medium",
            "served_description": (
                "Linked from shared substrate evidence: Alpen Ops / Acme Corp"
            ),
            "conn": conn,
        }
    ]


@pytest.mark.asyncio
async def test_promote_pattern_substrate_candidate_creates_review_candidate() -> None:
    conn = _FakeConn()
    model_ids = [uuid4(), uuid4(), uuid4()]
    candidate = _candidate(
        kind="pattern",
        confidence=0.72,
        metadata={
            "basis": "contextual_recurrence",
            "signature": "action:slack:blocked:release",
            "count_in_context": 4,
            "actor_fingerprints": ["actor:sam"],
        },
    )
    conn.fetch_results.append([{"id": model_id} for model_id in model_ids])
    conn.fetchval_results.extend([None, False])

    result = await promote_pattern_substrate_candidate(conn, candidate=candidate)

    assert result is not None
    assert result["canonical_ref"]["type"] == "pattern_candidate"
    assert result["constituent_model_count"] == 3
    insert_query, insert_args = next(
        (query, args)
        for query, args in conn.executed
        if "INSERT INTO pattern_candidates" in query
    )
    assert "cluster_size" in insert_query
    assert insert_args[4] == model_ids
    assert insert_args[5] == 3
    assert any(
        "UPDATE substrate_candidates" in query and args[2] == "promoted"
        for query, args in conn.executed
    )


@pytest.mark.asyncio
async def test_auto_promote_pattern_skips_until_constituent_floor() -> None:
    conn = _FakeConn()
    candidate = _candidate(kind="pattern", confidence=0.72)
    conn.fetch_results.append([{"id": uuid4()}, {"id": uuid4()}])

    result = await auto_promote_candidate(conn, candidate=candidate)

    assert result is None
    assert not any("INSERT INTO pattern_candidates" in query for query, _args in conn.executed)


@pytest.mark.asyncio
async def test_auto_promote_candidate_skips_weak_or_unresolved_candidates(
    monkeypatch,
) -> None:
    conn = _FakeConn()
    weak_commitment = _candidate(kind="commitment", confidence=0.77)
    ambiguous_actor = _candidate(
        confidence=0.9,
        aliases=[{"source_channel": "slack", "source_actor_ref": "sam"}],
        metadata={"same_label_candidate_ids": [str(uuid4())]},
    )
    called = False

    async def fake_create(**kwargs):
        nonlocal called
        called = True
        return SimpleNamespace(id=uuid4())

    monkeypatch.setattr(substrate_promotion.commitments_svc, "create", fake_create)

    assert await auto_promote_candidate(conn, candidate=weak_commitment) is None
    assert await auto_promote_candidate(conn, candidate=ambiguous_actor) is None
    assert called is False


@pytest.mark.asyncio
async def test_backfill_promoted_candidate_scopes_is_idempotent() -> None:
    conn = _FakeConn()
    model_id = uuid4()
    resource_id = uuid4()
    candidate = _candidate(kind="system", confidence=0.86)
    conn.fetch_results.append(
        [
            {
                "id": model_id,
                "scope_actors": [],
                "scope_entities": [
                    candidate.scope_ref,
                    {"type": "resource", "id": str(resource_id)},
                ],
            }
        ]
    )

    updated = await backfill_promoted_candidate_scopes(
        conn,
        candidate=candidate,
        canonical_refs=[{"type": "resource", "id": str(resource_id)}],
    )

    assert updated == 1
    update_args = next(args for query, args in conn.executed if "UPDATE models" in query)
    scope_entities_json = update_args[3]
    assert scope_entities_json.count(str(resource_id)) == 1
