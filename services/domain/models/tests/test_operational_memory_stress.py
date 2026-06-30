from __future__ import annotations

import json
import uuid
from typing import Any

import asyncpg
import pytest

from lib.shared.types import ModelCreate
from services.domain.models.repo import ModelsRepo
from services.domain.models.tests.conftest import make_embedding, state_proposition
from services.domain.observations.events import notify_scope
from services.reasoning.synthesis.operational_facets import infer_operational_query_plan


pytestmark = [pytest.mark.integration]

_NOISE_MODEL_COUNT = 144


def _mc(
    *,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    actor_id: uuid.UUID,
    natural: str,
    proposition: dict[str, Any] | None = None,
    scope_entities: list[dict[str, Any]] | None = None,
) -> ModelCreate:
    return ModelCreate(
        tenant_id=tenant,
        born_from_event_id=born_from_event,
        proposition=proposition or state_proposition(
            subject="operational memory stress",
            assertion="captures explicit UI state",
        ),
        natural=natural,
        embedding=make_embedding(natural),
        scope_actors=[actor_id],
        scope_entities=scope_entities or [],
        scope_temporal={"type": "now"},
        confidence=0.5,
        confidence_at_assertion=0.5,
    )


def _jsonb(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


@pytest.mark.timeout(180)
async def test_operational_memory_model_insert_and_search_projection_stress(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    target_natural = (
        "Operational memory record: catalog item Development Laptop (PC), "
        "product Dell XPS 13, developer laptop. Form controls visible: "
        "radio 500 GB [add $300.00] checked=false; "
        "radio Windows 8 [add $100.00] checked=false; "
        "radio Ubuntu checked=true; "
        "'Quantity' value='2'; 'Catalog item' value='Development Laptop (PC)'"
    )
    target_scope = [
        {"type": "workflow", "id": "order dell xps developer laptop"},
        {"type": "form_control", "id": "radio 500 gb add 300 dollars"},
        {"type": "route_from", "id": "route-noise-" + ("x" * 2400)},
        {"type": "ui_label_added", "id": "ui-label-noise-" + ("y" * 2400)},
    ]

    inserted_ids: list[uuid.UUID] = []
    with notify_scope():
        target = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                natural=target_natural,
                scope_entities=target_scope,
            ),
            conn=tx_conn,
        )
        inserted_ids.append(target.id)

        for idx in range(_NOISE_MODEL_COUNT):
            natural = _noise_natural(idx)
            row = await repo.insert(
                _mc(
                    tenant=tenant,
                    born_from_event=born_from_event,
                    actor_id=actor_id,
                    natural=natural,
                    scope_entities=[
                        {"type": "workflow", "id": f"noise workflow {idx}"},
                        {"type": "route_to", "id": "noise-route-" + str(idx) * 300},
                    ],
                ),
                conn=tx_conn,
            )
            inserted_ids.append(row.id)

    assert len(inserted_ids) == _NOISE_MODEL_COUNT + 1

    target_prop = _jsonb(await tx_conn.fetchval(
        "SELECT proposition FROM models WHERE id = $1",
        target.id,
    ))
    assert target_prop["kind"] != "choice_price_delta"
    assert target_prop["operational_facet_schema"] == "operational_facets_v1"
    assert "delta" in target_prop["operational_roles"]
    assert "choice_price_delta" in target_prop["operational_roles"]
    assert any(
        facet.get("role") == "delta"
        and facet.get("value") == "500 GB"
        and facet.get("attributes", {}).get("amount") == 300
        for facet in target_prop["operational_facets"]
    )
    assert any(
        facet.get("role") == "delta"
        and facet.get("value") == "Windows 8"
        and facet.get("attributes", {}).get("amount") == 100
        for facet in target_prop["operational_facets"]
    )

    sidecar = await tx_conn.fetchrow(
        """
        SELECT
          msd.search_text,
          model_search_document_text(m."natural", m.proposition, m.scope_entities)
            AS recomputed_search_text,
          length(msd.search_text)::int AS compact_len,
          length(lower(
            coalesce(m."natural", '')
            || ' '
            || coalesce(m.proposition::text, '')
            || ' '
            || coalesce(m.scope_entities::text, '')
          ))::int AS old_full_len
        FROM models m
        JOIN model_search_documents msd ON msd.model_id = m.id
        WHERE m.id = $1
        """,
        target.id,
    )
    assert sidecar is not None
    search_text = str(sidecar["search_text"])
    assert search_text == sidecar["recomputed_search_text"]
    assert "500 gb" in search_text
    assert "windows 8" in search_text
    assert "workflow order dell xps developer laptop" in search_text
    assert "route-noise" not in search_text
    assert "ui-label-noise" not in search_text
    assert sidecar["compact_len"] < sidecar["old_full_len"] * 0.65

    role_counts = {
        row["role"]: row["count"]
        for row in await tx_conn.fetch(
            """
            SELECT role, count(*)::int AS count
            FROM models m
            CROSS JOIN LATERAL jsonb_array_elements_text(
              coalesce(m.proposition->'operational_roles', '[]'::jsonb)
            ) AS role
            WHERE m.tenant_id = $1
              AND m.id = ANY($2::uuid[])
            GROUP BY role
            """,
            tenant,
            inserted_ids,
        )
    }
    assert role_counts["delta"] >= 30
    assert role_counts["sequence"] >= 30
    assert role_counts["property"] >= 60
    posting_counts = {
        row["role"]: row["count"]
        for row in await tx_conn.fetch(
            """
            SELECT role, count(*)::int AS count
            FROM model_operational_role_postings
            WHERE tenant_id = $1
              AND model_id = ANY($2::uuid[])
            GROUP BY role
            """,
            tenant,
            inserted_ids,
        )
    }
    assert posting_counts["delta"] == role_counts["delta"]
    assert posting_counts["sequence"] == role_counts["sequence"]
    assert posting_counts["property"] == role_counts["property"]

    ssd_matches = await _operational_matches(
        tx_conn,
        tenant=tenant,
        question=(
            "When we order a Dell XPS as the developer laptop, what is the "
            "extra dollar amount if we choose the largest SSD option?"
        ),
    )
    assert ssd_matches[0]["id"] == target.id
    assert ssd_matches[0]["lexical_match_count"] >= 6

    windows_matches = await _operational_matches(
        tx_conn,
        tenant=tenant,
        question=(
            "When we order a Dell XPS as the developer laptop, what is the "
            "extra dollar amount if we choose a Windows operating system?"
        ),
    )
    assert windows_matches[0]["id"] == target.id
    assert windows_matches[0]["lexical_match_count"] >= 8


def _noise_natural(idx: int) -> str:
    if idx % 4 == 0:
        return (
            f"Operational memory record: catalog item iPad Pro {idx}. "
            "Form controls visible: radio 256 GB [add $100.00] checked=false; "
            "radio 512 GB [add $300.00] checked=false; "
            "radio Space Gray checked=true"
        )
    if idx % 4 == 1:
        return (
            f"Relevant structured UI facts: field list Assets {idx} option order: "
            "for text; Asset tag; Model; Assigned to; State; "
            "bottom_option = State"
        )
    if idx % 4 == 2:
        return (
            f"After action pipeline stage chains: Intake {idx} (In progress); "
            "Approval (Pending - has not started); Fulfillment "
            "(Pending - has not started); remaining_excluding_in_progress_count=2"
        )
    return (
        f"Operational memory record: Problem table {idx}. "
        "'Problem statement' value='Laptop is slow'; "
        "'Assignment group' value=''; 'Subcategory' value='-- None --'; "
        "option Hardware selected=false; option Software selected=true"
    )


async def _operational_matches(
    conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    question: str,
) -> list[asyncpg.Record]:
    plan = infer_operational_query_plan(question)
    seed_roles = [
        role for role in plan.roles
        if role in {"action", "count", "delta", "invariant", "sequence"}
    ]
    assert seed_roles == ["delta"]
    return list(await conn.fetch(
        """
        WITH seed_roles AS MATERIALIZED (
          SELECT role::text,
                 ord::int AS role_ord
          FROM unnest($3::text[]) WITH ORDINALITY AS r(role, ord)
        ),
        query_terms AS MATERIALIZED (
          SELECT term::text
          FROM unnest($4::text[]) AS term(value)
          WHERE nullif(term.value, '') IS NOT NULL
        ),
        role_hits AS MATERIALIZED (
          SELECT sr.role,
                 sr.role_ord,
                 morp.model_id,
                 coalesce(lexical.lexical_match_count, 0)::int
                   AS lexical_match_count
          FROM seed_roles sr
          JOIN model_operational_role_postings morp
            ON morp.tenant_id = $1
           AND morp.status = 'active'
           AND morp.role = sr.role
          JOIN models m
            ON m.id = morp.model_id
           AND m.tenant_id = $1
           AND m.status = 'active'
          LEFT JOIN model_search_documents msd
            ON msd.model_id = morp.model_id
           AND msd.tenant_id = $1
           AND msd.status = 'active'
          LEFT JOIN LATERAL (
            SELECT count(*)::int AS lexical_match_count
            FROM query_terms term
            WHERE strpos(coalesce(msd.search_text, ''), term.term) > 0
          ) lexical ON TRUE
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 array_agg(DISTINCT role ORDER BY role) AS matched_roles,
                 count(DISTINCT role)::int AS role_match_count,
                 min(role_ord)::int AS first_role_ord,
                 max(lexical_match_count)::int AS lexical_match_count
          FROM role_hits
          GROUP BY model_id
        )
        SELECT m.id,
               m."natural",
               scored.matched_roles,
               scored.role_match_count,
               scored.lexical_match_count
        FROM scored
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scored.role_match_count DESC,
                 scored.lexical_match_count DESC,
                 scored.first_role_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant,
        12,
        seed_roles,
        [term.casefold() for term in plan.terms],
    ))
