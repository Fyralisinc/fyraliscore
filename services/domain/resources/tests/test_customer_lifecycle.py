"""End-to-end customer identity lifecycle on resource-backed customers."""
from __future__ import annotations

import json
from uuid import UUID

import pytest

from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.domain.resources import repo
from services.domain.resources.tests.conftest import TENANT_A, make_observation


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _insert_model_scope(resources_db, *, event_id, customer_ref) -> UUID:
    model_id = uuid7()
    zero_vector = "[" + ",".join(["0"] * 768) + "]"
    await resources_db.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, falsifier, signal_readings,
            supporting_event_ids, supporting_model_ids,
            contributing_models, status, confidence_at_assertion
        ) VALUES (
            $1, $2, $3,
            '{"kind":"state","subject":"customer","assertion":"active"}'::jsonb,
            'Customer is active', $4::vector,
            '{}'::uuid[], $5::jsonb,
            '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
            0.6, NULL, '[]'::jsonb,
            ARRAY[$3]::uuid[], '{}'::uuid[],
            '{}'::uuid[], 'active', 0.6
        )
        """,
        model_id,
        TENANT_A,
        event_id,
        zero_vector,
        json.dumps([customer_ref]),
    )
    return model_id


async def test_customer_rename_archive_and_name_reuse_preserve_history(
    resources_db,
    event_id,
) -> None:
    customer = await repo.create(
        kind="relational",
        identity="Acme",
        description="Original customer",
        current_value={"arr_usd": 100_000},
        metadata={"semantic_kind": "customer"},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    old_ref = {"type": "customer", "id": str(customer.id)}
    evidence_event_id = await make_observation(resources_db)
    await resources_db.execute(
        """
        UPDATE observations
        SET entities_mentioned = $2::jsonb
        WHERE id = $1
        """,
        evidence_event_id,
        json.dumps([old_ref]),
    )
    model_id = await _insert_model_scope(
        resources_db,
        event_id=evidence_event_id,
        customer_ref=old_ref,
    )

    rename_event_id = await make_observation(resources_db)
    renamed = await repo.rename_customer(
        customer.id,
        new_identity="Acme Holdings",
        cause_event_id=rename_event_id,
    )
    assert renamed.id == customer.id
    assert renamed.identity == "Acme Holdings"

    aliases = EntityAliasRepo(resources_db)
    old_history = await aliases.list_history("Acme", TENANT_A)
    new_history = await aliases.list_history("Acme Holdings", TENANT_A)
    assert len(old_history) == 1
    assert len(new_history) == 1
    rename_at = old_history[0]["valid_until"]
    assert rename_at == new_history[0]["valid_from"]
    before_rename = customer.created_at + (rename_at - customer.created_at) / 2

    assert await aliases.fast_path_resolve(
        "Acme",
        TENANT_A,
        as_of=before_rename,
    ) == old_ref
    assert await aliases.fast_path_resolve("Acme", TENANT_A) is None
    assert await aliases.fast_path_resolve(
        "Acme Holdings",
        TENANT_A,
    ) == old_ref

    archive_event_id = await make_observation(resources_db)
    archived = await repo.archive(
        customer.id,
        reason="customer relationship ended",
        cause_event_id=archive_event_id,
    )
    assert archived.archived_at is not None
    new_history = await aliases.list_history("Acme Holdings", TENANT_A)
    assert new_history[0]["valid_until"] == archived.archived_at
    before_archive = rename_at + (archived.archived_at - rename_at) / 2
    assert await aliases.fast_path_resolve(
        "Acme Holdings",
        TENANT_A,
        as_of=before_archive,
    ) == old_ref
    assert await aliases.fast_path_resolve(
        "Acme Holdings",
        TENANT_A,
    ) is None

    second_event_id = await make_observation(resources_db)
    second_customer = await repo.create(
        kind="relational",
        identity="Acme",
        description="A later company using the historical name",
        current_value={"arr_usd": 25_000},
        metadata={"semantic_kind": "customer"},
        tenant_id=TENANT_A,
        created_by_event_id=second_event_id,
    )
    second_ref = {"type": "customer", "id": str(second_customer.id)}

    assert await aliases.fast_path_resolve("Acme", TENANT_A) == second_ref
    assert await aliases.fast_path_resolve(
        "Acme",
        TENANT_A,
        as_of=before_rename,
    ) == old_ref
    reused_history = await aliases.list_history("Acme", TENANT_A)
    assert [item["resolved_entity_ref"] for item in reused_history] == [
        old_ref,
        second_ref,
    ]

    preserved_observation = await resources_db.fetchval(
        "SELECT entities_mentioned FROM observations WHERE id = $1",
        evidence_event_id,
    )
    preserved_model = await resources_db.fetchval(
        "SELECT scope_entities FROM models WHERE id = $1",
        model_id,
    )
    assert preserved_observation == [old_ref]
    assert preserved_model == [old_ref]


async def test_rename_rejects_non_customer_resource(
    resources_db,
    event_id,
) -> None:
    resource = await repo.create(
        kind="relational",
        identity="Vendor One",
        current_value={},
        metadata={"semantic_kind": "vendor"},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    with pytest.raises(InvariantViolation) as exc_info:
        await repo.rename_customer(
            resource.id,
            new_identity="Vendor Two",
            cause_event_id=event_id,
        )
    assert "only relational resources" in str(exc_info.value)


async def test_customer_birth_and_rename_reject_normalized_name_collision(
    resources_db,
    event_id,
) -> None:
    acme = await repo.create(
        kind="relational",
        identity="Acme",
        current_value={},
        metadata={"semantic_kind": "customer"},
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )
    acme_ref = {"type": "customer", "id": str(acme.id)}

    with pytest.raises(InvariantViolation):
        await repo.create(
            kind="relational",
            identity="  ACME  ",
            current_value={},
            metadata={"semantic_kind": "customer"},
            tenant_id=TENANT_A,
            created_by_event_id=event_id,
        )
    customer_count = await resources_db.fetchval(
        """
        SELECT count(*)
        FROM resources
        WHERE tenant_id = $1
          AND metadata ->> 'semantic_kind' = 'customer'
        """,
        TENANT_A,
    )
    assert customer_count == 1

    globex_event_id = await make_observation(resources_db)
    globex = await repo.create(
        kind="relational",
        identity="Globex",
        current_value={},
        metadata={"semantic_kind": "customer"},
        tenant_id=TENANT_A,
        created_by_event_id=globex_event_id,
    )
    globex_ref = {"type": "customer", "id": str(globex.id)}
    rename_event_id = await make_observation(resources_db)
    with pytest.raises(InvariantViolation):
        await repo.rename_customer(
            globex.id,
            new_identity=" acme ",
            cause_event_id=rename_event_id,
        )

    preserved = await repo.get(globex.id)
    assert preserved is not None
    assert preserved.identity == "Globex"
    aliases = EntityAliasRepo(resources_db)
    assert await aliases.fast_path_resolve("ACME", TENANT_A) == acme_ref
    assert await aliases.fast_path_resolve("globex", TENANT_A) == globex_ref
    assert len(await aliases.list_history("Acme", TENANT_A)) == 1
