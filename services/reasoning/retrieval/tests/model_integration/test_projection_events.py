from __future__ import annotations

import json
import uuid
from typing import Any

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
import services.domain.projections.catalog as projection_catalog
from services.domain.models.repo import ModelsRepo
from services.domain.observations.events import notify_scope
from services.domain.projections import (
    ConstraintProjector,
    ProjectionRepo,
    ProjectionRunner,
    ProjectionSnapshot,
    ResourceProjector,
)
from services.reasoning.retrieval.projection_context import (
    load_constraint_context,
    load_resource_context,
)


pytestmark = [pytest.mark.integration]


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _model_create(
    *,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
    assertion: str,
    confidence: float = 0.66,
    claim_role: str = "concern",
    domain_tags: list[str] | None = None,
    scope_entities: list[dict[str, Any]] | None = None,
) -> ModelCreate:
    tags = domain_tags or []
    falsifier = None
    if confidence > 0.7:
        falsifier = {
            "kind": "observation_pattern",
            "pattern": "authoritative operating data contradicts this claim within the stated window",
            "within_window": "30 days",
        }
    proposition = {
        "kind": "belief",
        "claim_role": claim_role,
        "assertion": assertion,
        "domain_tags": tags,
    }
    if claim_role == "capability":
        proposition.update(
            {
                "abstraction_level": "atomic",
                "capability_id": (tags[0] if tags else "operating-capability"),
                "subject": "company",
                "assessment": assertion,
            }
        )
    return ModelCreate(
        tenant_id=tenant,
        born_from_event_id=born_from_event,
        proposition=proposition,
        natural=assertion,
        embedding=embedding,
        scope_actors=[],
        scope_entities=scope_entities or [{"type": "company", "id": str(tenant)}],
        scope_temporal={"type": "current"},
        confidence=confidence,
        confidence_at_assertion=confidence,
        falsifier=falsifier,
        domain_tags=tags,
    )


async def _snapshot(
    conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    subject_key: str,
    projection_name: str = "constraints",
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        SELECT *
        FROM projection_snapshots
        WHERE tenant_id = $1
          AND projection_name = $2
          AND projection_version = 'v1'
          AND subject_key = $3
        """,
        tenant,
        projection_name,
        subject_key,
    )


async def test_model_insert_emits_neutral_model_created_event(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Runway pressure is increasing.",
                domain_tags=["runway", "financial_capacity"],
            ),
            conn=tx_conn,
        )

    event = await tx_conn.fetchrow(
        """
        SELECT event_type, changed_fields, claim_role, domain_tags,
               semantic_snapshot, source_event_id
        FROM model_events
        WHERE tenant_id = $1 AND model_id = $2
        """,
        tenant,
        row.id,
    )
    assert event is not None
    assert event["event_type"] == "model.created"
    assert event["claim_role"] == "concern"
    assert set(event["domain_tags"]) >= {"runway", "financial_capacity"}
    assert "projection" not in _json(event["semantic_snapshot"])
    assert "proposition" in event["changed_fields"]
    assert event["source_event_id"] == born_from_event


async def test_constraint_projector_materializes_projection_from_model_event(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Current burn implies runway pressure.",
                confidence=0.84,
                domain_tags=["runway", "financial_capacity", "constraint"],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([ConstraintProjector()])
    processed = await runner.run_once(tx_conn, tenant_id=tenant)

    assert processed == 1
    snap = await _snapshot(tx_conn, tenant=tenant, subject_key="company:runway")
    assert snap is not None
    payload = _json(snap["payload"])
    assert payload["status"] == "active"
    assert payload["severity"] == "high"
    assert str(row.id) in {str(mid) for mid in snap["source_model_ids"]}
    assert payload["constraints"][0]["model_id"] == str(row.id)

    context = await load_constraint_context(
        tx_conn,
        tenant_id=tenant,
        subject_key="company:runway",
    )
    assert context is not None
    assert context.payload["status"] == "active"
    assert context.source_model_ids == (row.id,)
    assert [model.id for model in context.source_models] == [row.id]


async def test_projection_repo_loads_context_and_detects_staleness(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    projection_repo = ProjectionRepo()
    with notify_scope():
        row = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Runway pressure constrains hiring plans.",
                confidence=0.81,
                domain_tags=["runway", "financial_capacity", "constraint"],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([ConstraintProjector()])
    await runner.run_once(tx_conn, tenant_id=tenant)

    snapshot = await projection_repo.get_snapshot(
        tx_conn,
        tenant_id=tenant,
        projection_name="constraints",
        subject_key="company:runway",
    )
    assert snapshot is not None
    assert snapshot.payload["status"] == "active"
    assert snapshot.source_model_ids == (row.id,)

    subjects = await projection_repo.list_subjects(
        tx_conn,
        tenant_id=tenant,
        projection_name="constraints",
    )
    assert "company:runway" in subjects

    snapshots = await projection_repo.list_snapshots_for_subjects(
        tx_conn,
        tenant_id=tenant,
        subjects=[
            ("resources", "company:financial"),
            ("constraints", "company:runway"),
        ],
        require_source_models=True,
    )
    assert [record.subject_key for record in snapshots] == ["company:runway"]

    context = await projection_repo.get_context(
        tx_conn,
        tenant_id=tenant,
        projection_name="constraints",
        subject_key="company:runway",
    )
    assert context is not None
    assert context.source_model_ids == (row.id,)
    assert [model.id for model in context.source_models] == [row.id]

    current = await projection_repo.is_stale(
        tx_conn,
        tenant_id=tenant,
        projection_name="constraints",
    )
    assert current.is_stale is False
    assert current.reason == "current"

    batched = await projection_repo.list_staleness(
        tx_conn,
        tenant_id=tenant,
        projection_names=["constraints", "resources", "constraints"],
    )
    assert [entry.projection_name for entry in batched] == ["constraints", "resources"]
    assert [(entry.is_stale, entry.reason) for entry in batched] == [
        (False, "current"),
        (True, "no_snapshot"),
    ]

    with notify_scope():
        await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="The weekly reporting checklist was completed.",
                claim_role="fact",
                domain_tags=["reporting"],
            ),
            conn=tx_conn,
        )

    stale = await projection_repo.is_stale(
        tx_conn,
        tenant_id=tenant,
        projection_name="constraints",
    )
    assert stale.is_stale is True
    assert stale.reason == "pending_model_events"

    batched_after_new_event = await projection_repo.list_staleness(
        tx_conn,
        tenant_id=tenant,
        projection_names=["constraints", "resources"],
    )
    assert [(entry.is_stale, entry.reason) for entry in batched_after_new_event] == [
        (True, "pending_model_events"),
        (True, "no_snapshot"),
    ]


class _CustomerHealthProjector:
    name = "customer_health"
    version = "v1"

    def matches(self, event) -> bool:
        return "customer" in {tag.casefold() for tag in event.domain_tags}

    async def affected_subjects(self, conn, event):
        del conn
        subjects = []
        for entity in event.scope_entities:
            if entity.get("type") == "customer" and entity.get("id"):
                subjects.append(f"customer:{entity['id']}:health")
        return subjects

    async def project_subject(
        self,
        conn,
        *,
        tenant_id,
        subject_key: str,
        source_event_ids,
    ) -> ProjectionSnapshot:
        rows = await conn.fetch(
            """
            SELECT model_id
            FROM model_events
            WHERE tenant_id = $1 AND id = ANY($2::uuid[])
            ORDER BY created_at ASC, id ASC
            """,
            tenant_id,
            list(source_event_ids),
        )
        return ProjectionSnapshot(
            tenant_id=tenant_id,
            projection_name=self.name,
            projection_version=self.version,
            subject_key=subject_key,
            payload={
                "kind": "customer_health_projection",
                "subject_key": subject_key,
                "status": "active",
            },
            confidence=0.7,
            severity="medium",
            source_model_ids=tuple(row["model_id"] for row in rows),
            source_event_ids=tuple(source_event_ids),
        )


async def test_registered_extension_projector_materializes_from_model_events(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    customer_id = uuid7()
    projection_catalog.reset_for_tests()
    projection_catalog.register_projector_factory(
        "customer_health",
        _CustomerHealthProjector,
    )
    try:
        with notify_scope():
            row = await repo.insert(
                _model_create(
                    tenant=tenant,
                    born_from_event=born_from_event,
                    embedding=embedding,
                    assertion="Customer renewal health is improving.",
                    domain_tags=["customer", "renewal"],
                    scope_entities=[{"type": "customer", "id": str(customer_id)}],
                ),
                conn=tx_conn,
            )

        runner = ProjectionRunner(
            projection_catalog.build_projection_registry(["customer_health"])
        )
        processed = await runner.run_once(tx_conn, tenant_id=tenant)
    finally:
        projection_catalog.reset_for_tests()

    assert processed == 1
    snap = await _snapshot(
        tx_conn,
        tenant=tenant,
        projection_name="customer_health",
        subject_key=f"customer:{customer_id}:health",
    )
    assert snap is not None
    assert snap["source_model_ids"] == [row.id]
    assert _json(snap["payload"])["status"] == "active"


async def test_model_retrieval_does_not_emit_projection_events_or_stale_projections(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    projection_repo = ProjectionRepo()
    with notify_scope():
        row = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Runway pressure constrains the operating plan.",
                confidence=0.83,
                domain_tags=["runway", "financial_capacity", "constraint"],
            ),
            conn=tx_conn,
        )

    await ProjectionRunner([ConstraintProjector()]).run_once(tx_conn, tenant_id=tenant)
    event_count_before = await tx_conn.fetchval(
        """
        SELECT count(*)
        FROM model_events
        WHERE tenant_id = $1
        """,
        tenant,
    )

    retrieved = await repo.retrieve([row.id], conn=tx_conn)
    staleness = await projection_repo.is_stale(
        tx_conn,
        tenant_id=tenant,
        projection_name="constraints",
    )
    event_count_after = await tx_conn.fetchval(
        """
        SELECT count(*)
        FROM model_events
        WHERE tenant_id = $1
        """,
        tenant,
    )

    assert [model.id for model in retrieved] == [row.id]
    assert retrieved[0].retrieval_count == row.retrieval_count + 1
    assert event_count_after == event_count_before
    assert staleness.is_stale is False
    assert staleness.reason == "current"


async def test_non_constraint_event_advances_checkpoint_without_snapshot(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Alice completed the weekly reporting checklist.",
                claim_role="fact",
                domain_tags=["reporting"],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([ConstraintProjector()])
    processed = await runner.run_once(tx_conn, tenant_id=tenant)

    assert processed == 1
    snapshot_count = await tx_conn.fetchval(
        """
        SELECT count(*)
        FROM projection_snapshots
        WHERE tenant_id = $1 AND projection_name = 'constraints'
        """,
        tenant,
    )
    checkpoint = await tx_conn.fetchrow(
        """
        SELECT last_processed_event_id
        FROM projection_checkpoints
        WHERE tenant_id = $1
          AND projection_name = 'constraints'
          AND projection_version = 'v1'
        """,
        tenant,
    )
    assert snapshot_count == 0
    assert checkpoint is not None
    assert checkpoint["last_processed_event_id"] is not None


async def test_constraint_projection_recomputes_after_confidence_update(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Hiring expansion is increasing cash burn.",
                confidence=0.61,
                domain_tags=["runway", "financial_capacity", "hiring"],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([ConstraintProjector()])
    await runner.run_once(tx_conn, tenant_id=tenant)

    with notify_scope():
        await repo.bulk_confidence_update({row.id: 0.95}, conn=tx_conn)
    processed = await runner.run_once(tx_conn, tenant_id=tenant)

    assert processed == 1
    snap = await _snapshot(tx_conn, tenant=tenant, subject_key="company:runway")
    assert snap is not None
    payload = _json(snap["payload"])
    assert snap["confidence"] == pytest.approx(0.95)
    assert payload["severity"] == "high"


async def test_constraint_projection_clears_subject_when_model_is_archived(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Cash runway is the dominant planning constraint.",
                confidence=0.86,
                domain_tags=["runway", "financial_capacity", "constraint"],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([ConstraintProjector()])
    await runner.run_once(tx_conn, tenant_id=tenant)

    with notify_scope():
        await repo.archive(row.id, "manual", conn=tx_conn)
    processed = await runner.run_once(tx_conn, tenant_id=tenant)

    assert processed == 1
    snap = await _snapshot(tx_conn, tenant=tenant, subject_key="company:runway")
    assert snap is not None
    payload = _json(snap["payload"])
    assert payload["status"] == "empty"
    assert payload["severity"] == "none"
    assert snap["source_model_ids"] == []


async def test_resource_projector_materializes_projection_from_model_event(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Cash runway gives the company operating flexibility.",
                claim_role="capability",
                confidence=0.74,
                domain_tags=["runway", "financial_capacity", "resource"],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([ResourceProjector()])
    processed = await runner.run_once(tx_conn, tenant_id=tenant)

    assert processed == 1
    snap = await _snapshot(
        tx_conn,
        tenant=tenant,
        projection_name="resources",
        subject_key="company:financial",
    )
    assert snap is not None
    payload = _json(snap["payload"])
    assert payload["status"] == "active"
    assert payload["resource_kind"] == "financial"
    assert payload["state"] == "available"
    assert str(row.id) in {str(mid) for mid in snap["source_model_ids"]}
    assert payload["resources"][0]["model_id"] == str(row.id)

    context = await load_resource_context(
        tx_conn,
        tenant_id=tenant,
        subject_key="company:financial",
    )
    assert context is not None
    assert context.payload["status"] == "active"
    assert context.payload["resource_kind"] == "financial"
    assert context.source_model_ids == (row.id,)
    assert [model.id for model in context.source_models] == [row.id]


async def test_resource_projection_clears_subject_when_model_is_archived(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Available hiring capacity is a current resource.",
                claim_role="capability",
                confidence=0.79,
                domain_tags=["hiring", "capacity", "resource"],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([ResourceProjector()])
    await runner.run_once(tx_conn, tenant_id=tenant)

    with notify_scope():
        await repo.archive(row.id, "manual", conn=tx_conn)
    processed = await runner.run_once(tx_conn, tenant_id=tenant)

    assert processed == 1
    snap = await _snapshot(
        tx_conn,
        tenant=tenant,
        projection_name="resources",
        subject_key="company:capacity",
    )
    assert snap is not None
    payload = _json(snap["payload"])
    assert payload["status"] == "empty"
    assert payload["state"] == "empty"
    assert payload["severity"] == "none"
    assert snap["source_model_ids"] == []


async def test_resource_tenant_fallback_only_uses_resource_tagged_models(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        unrelated = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="Reporting quality is a current concern.",
                confidence=0.93,
                domain_tags=["reporting"],
            ),
            conn=tx_conn,
        )
        resource = await repo.insert(
            _model_create(
                tenant=tenant,
                born_from_event=born_from_event,
                embedding=embedding,
                assertion="A general operating resource needs follow-up.",
                confidence=0.72,
                domain_tags=["resource"],
            ),
            conn=tx_conn,
        )

    runner = ProjectionRunner([ResourceProjector()])
    processed = await runner.run_once(tx_conn, tenant_id=tenant)

    assert processed == 2
    snap = await _snapshot(
        tx_conn,
        tenant=tenant,
        projection_name="resources",
        subject_key=f"tenant:{tenant}:resources",
    )
    assert snap is not None
    payload = _json(snap["payload"])
    assert payload["status"] == "active"
    assert {entry["model_id"] for entry in payload["resources"]} == {str(resource.id)}
    assert str(unrelated.id) not in {str(mid) for mid in snap["source_model_ids"]}
