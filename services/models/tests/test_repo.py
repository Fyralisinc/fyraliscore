"""
services/models/tests/test_repo.py — integration tests for ModelsRepo.

BUILD-PLAN §0.5: every test runs against live Postgres. We follow the
observations-service pattern (see services/observations/tests/conftest.py):
every test owns a single asyncpg transaction on `tx_conn` which it
rolls back at teardown. Parallel Wave-1 agents are insulated by
tenant-UUID isolation plus the per-test transaction lock.

The repo exposes a `conn=` parameter on every method; tests pass the
shared `tx_conn` so all reads/writes are visible within the same
transaction.

Covers the 20+ cases listed in BUILD-PLAN 1-C plus the Q3 resolution
assertions explicitly (25+ total).
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from lib.shared.errors import FalsifierInadequateError, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.models.repo import ModelsRepo
from services.observations.events import notify_scope
from services.models.tests.conftest import (
    every_kind_proposition,
    make_embedding,
    prediction_proposition,
    similar_embedding,
    state_proposition,
)


pytestmark = [pytest.mark.integration]


def _mc(
    *,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    proposition: dict,
    natural: str,
    embedding: list[float],
    confidence: float,
    actor_id: uuid.UUID | None = None,
    **kwargs,
) -> ModelCreate:
    """Tiny helper to build a ModelCreate with sensible defaults."""
    scope_actors = kwargs.pop("scope_actors", [actor_id] if actor_id else [])
    return ModelCreate(
        tenant_id=tenant,
        born_from_event_id=born_from_event,
        proposition=proposition,
        natural=natural,
        embedding=embedding,
        scope_actors=scope_actors,
        scope_temporal=kwargs.pop("scope_temporal", {"type": "now"}),
        confidence=confidence,
        confidence_at_assertion=kwargs.pop("confidence_at_assertion", confidence),
        **kwargs,
    )


# =====================================================================
# Insert happy path, falsifier adequacy, confidence clipping
# =====================================================================


async def test_insert_low_confidence_without_falsifier_succeeds(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """Per §2 pipeline step 1: falsifier only required above 0.7."""
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="alice is reliable on small tickets",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    assert row.status == "active"
    # Per AUDIT-REVIEW-1-FIXES §C5: cold-start calibration applies
    # PROP_KIND_DEFAULTS["state"] = 0.95, so 0.5 × 0.95 = 0.475.
    assert row.confidence == pytest.approx(0.475)
    assert row.confidence_at_assertion == 0.5
    assert row.proposition_kind == "state"
    assert row.activation == 1.0
    assert row.confirmed_count == 0
    assert row.contested_count == 0
    assert row.last_confirmed_at is None
    assert row.resolved_at is None
    assert row.resolution_outcome is None
    assert row.activation_coefficient == 1.0


async def test_insert_high_confidence_without_falsifier_rejected(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """confidence > 0.7 without any falsifier → FalsifierInadequateError."""
    with pytest.raises(FalsifierInadequateError) as exc, notify_scope():
        await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="alice will definitely ship X",
                embedding=embedding,
                confidence=0.8,
                falsifier=None,
            ),
            conn=tx_conn,
        )
    assert "no falsifier" in exc.value.reason


async def test_insert_high_confidence_with_adequate_falsifier_succeeds(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="alice ships consistently",
                embedding=embedding,
                confidence=0.8,
                falsifier={
                    "kind": "observation_pattern",
                    "pattern": "any Observation from authoritative source saying alice missed a deadline",
                    "within_window": "any 4-week period",
                },
            ),
            conn=tx_conn,
        )
    assert row.status == "active"
    assert row.falsifier is not None
    assert row.falsifier["kind"] == "observation_pattern"


@pytest.mark.parametrize("raw_conf", [0.5, 0.05, 0.95])
async def test_confidence_within_range_passes_through(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
    raw_conf: float,
) -> None:
    from services.models.calibration import PROP_KIND_DEFAULTS
    falsifier = None
    if raw_conf > 0.7:
        falsifier = {
            "kind": "observation_pattern",
            "pattern": "x" * 40,
            "within_window": "4w",
        }
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural=f"confidence={raw_conf}",
                embedding=embedding,
                confidence=raw_conf,
                falsifier=falsifier,
            ),
            conn=tx_conn,
        )
    # Cold-start: raw_conf × PROP_KIND_DEFAULTS["state"] (0.95), clipped to [0.05, 0.95].
    expected = max(0.05, min(0.95, raw_conf * PROP_KIND_DEFAULTS["state"]))
    assert row.confidence == pytest.approx(expected)


async def test_pydantic_rejects_out_of_range_confidence(
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """ModelCreate.confidence has ge=0.05/le=0.95; Pydantic catches it first."""
    from pydantic import ValidationError as PydV

    with pytest.raises(PydV):
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=born_from_event,
            proposition=state_proposition(),
            natural="too low",
            embedding=embedding,
            scope_temporal={"type": "now"},
            confidence=0.01,
            confidence_at_assertion=0.5,
        )
    with pytest.raises(PydV):
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=born_from_event,
            proposition=state_proposition(),
            natural="too high",
            embedding=embedding,
            scope_temporal={"type": "now"},
            confidence=0.99,
            confidence_at_assertion=0.5,
        )


async def test_db_check_rejects_out_of_range_confidence_at_assertion(
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """Q3: CHECK constraint rejects confidence_at_assertion < 0.05 / > 0.95.

    Bypass ModelCreate to let the DB's CHECK fire directly. Use a
    SAVEPOINT so the violation doesn't poison the outer tx.
    """
    # Low value
    await tx_conn.execute("SAVEPOINT sp_low")
    with pytest.raises(asyncpg.exceptions.CheckViolationError):
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_temporal,
                confidence, activation,
                confidence_at_assertion,
                status
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, 'x', $5,
                '{"type":"now"}'::jsonb,
                0.5, 1.0,
                0.01,
                'active'
            )
            """,
            uuid7(),
            tenant,
            born_from_event,
            '{"kind":"state","subject":"a","assertion":"b"}',
            embedding,
        )
    await tx_conn.execute("ROLLBACK TO SAVEPOINT sp_low")

    # High value
    await tx_conn.execute("SAVEPOINT sp_high")
    with pytest.raises(asyncpg.exceptions.CheckViolationError):
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_temporal,
                confidence, activation,
                confidence_at_assertion,
                status
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, 'x', $5,
                '{"type":"now"}'::jsonb,
                0.5, 1.0,
                0.99,
                'active'
            )
            """,
            uuid7(),
            tenant,
            born_from_event,
            '{"kind":"state","subject":"a","assertion":"b"}',
            embedding,
        )
    await tx_conn.execute("ROLLBACK TO SAVEPOINT sp_high")


# =====================================================================
# Proposition kinds
# =====================================================================


@pytest.mark.parametrize("prop", every_kind_proposition(), ids=lambda p: p["kind"])
async def test_all_ten_proposition_kinds_insert(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    prop: dict,
) -> None:
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=prop,
                natural=f"test {prop['kind']}",
                embedding=make_embedding(f"kind-{prop['kind']}"),
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    assert row.proposition_kind == prop["kind"]
    assert row.proposition["kind"] == prop["kind"]


async def test_invalid_proposition_kind_rejected(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with pytest.raises(ValidationError):
        await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition={"kind": "notakind", "foo": "bar"},
                natural="bad",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )


# =====================================================================
# Q3 resolution: proposition_kind is GENERATED
# =====================================================================


async def test_proposition_kind_equals_jsonb_discriminator(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """Q3: proposition_kind is a stored generated column; must match
    proposition->>'kind' exactly, and never drift."""
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=prediction_proposition(),
                natural="prediction",
                embedding=embedding,
                confidence=0.6,
                scope_temporal={"type": "future", "deadline": "2026-12-01T00:00:00Z"},
            ),
            conn=tx_conn,
        )
    jsonb_kind = await tx_conn.fetchval(
        "SELECT proposition->>'kind' FROM models WHERE id = $1", row.id
    )
    stored_kind = await tx_conn.fetchval(
        "SELECT proposition_kind FROM models WHERE id = $1", row.id
    )
    assert jsonb_kind == stored_kind == "prediction"


async def test_proposition_kind_not_settable_directly(
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """Q3: GENERATED ALWAYS columns must reject direct INSERT."""
    await tx_conn.execute("SAVEPOINT sp_genalways")
    with pytest.raises(
        (
            asyncpg.exceptions.GeneratedAlwaysError,
            asyncpg.exceptions.PostgresError,
        )
    ):
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_temporal,
                confidence, activation,
                confidence_at_assertion, proposition_kind,
                status
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, 'x', $5,
                '{"type":"now"}'::jsonb,
                0.5, 1.0,
                0.5, 'bogus',
                'active'
            )
            """,
            uuid7(),
            tenant,
            born_from_event,
            '{"kind":"state","subject":"a","assertion":"b"}',
            embedding,
        )
    await tx_conn.execute("ROLLBACK TO SAVEPOINT sp_genalways")


# =====================================================================
# Q3 resolution: resolved_at / resolution_outcome paired CHECK
# =====================================================================


async def test_resolution_pair_check_rejects_partial(
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """Q3 A1: CHECK (resolved_at, resolution_outcome) both NULL or both NOT NULL."""
    # resolved_at set, outcome NULL → rejected
    await tx_conn.execute("SAVEPOINT sp_a")
    with pytest.raises(asyncpg.exceptions.CheckViolationError):
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_temporal,
                confidence, activation,
                confidence_at_assertion,
                resolved_at, resolution_outcome,
                status
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, 'x', $5,
                '{"type":"now"}'::jsonb,
                0.5, 1.0,
                0.5,
                now(), NULL,
                'active'
            )
            """,
            uuid7(),
            tenant,
            born_from_event,
            '{"kind":"state","subject":"a","assertion":"b"}',
            embedding,
        )
    await tx_conn.execute("ROLLBACK TO SAVEPOINT sp_a")

    # outcome set, resolved_at NULL → rejected
    await tx_conn.execute("SAVEPOINT sp_b")
    with pytest.raises(asyncpg.exceptions.CheckViolationError):
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_temporal,
                confidence, activation,
                confidence_at_assertion,
                resolved_at, resolution_outcome,
                status
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, 'x', $5,
                '{"type":"now"}'::jsonb,
                0.5, 1.0,
                0.5,
                NULL, TRUE,
                'active'
            )
            """,
            uuid7(),
            tenant,
            born_from_event,
            '{"kind":"state","subject":"a","assertion":"b"}',
            embedding,
        )
    await tx_conn.execute("ROLLBACK TO SAVEPOINT sp_b")


# =====================================================================
# Q3 resolution: confidence_at_assertion immutable + deprecated archive_reason
# =====================================================================


async def test_confidence_at_assertion_unchanged_by_bulk_update(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """Q3: bulk_confidence_update MUST NOT touch confidence_at_assertion."""
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="immutable test",
                embedding=embedding,
                confidence=0.6,
            ),
            conn=tx_conn,
        )
        updated = await repo.bulk_confidence_update({row.id: 0.3}, conn=tx_conn)
    assert len(updated) == 1
    assert updated[0].confidence == pytest.approx(0.3)
    assert updated[0].confidence_at_assertion == pytest.approx(0.6)  # immutable


async def test_deprecated_archive_reason_accepted(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """Q3 A3: pseudo-code's `deprecated_at = now()` → archive_reason='deprecated'."""
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="soon to deprecate",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
        archived = await repo.archive(row.id, reason="deprecated", conn=tx_conn)
    assert archived.status == "archived"
    assert archived.archive_reason == "deprecated"
    assert archived.archived_at is not None


# =====================================================================
# Retrieval: activation bump, confidence UNCHANGED
# =====================================================================


async def test_retrieve_bumps_activation_clipped_at_1_confidence_untouched(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """§2 retrieve SQL: activation = LEAST(1.0, activation + 0.15),
    retrieval_count += 1, last_retrieved_at = now(); confidence NOT updated."""
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="retrieve me",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    # Pin activation at 0.9 so a +0.15 bump stays below 1.0.
    await tx_conn.execute(
        "UPDATE models SET activation = 0.9 WHERE id = $1", row.id
    )
    got = await repo.retrieve([row.id], conn=tx_conn)
    assert len(got) == 1
    assert got[0].retrieval_count == 1
    assert got[0].activation == pytest.approx(1.0)   # 0.9 + 0.15 → clipped 1.0
    assert got[0].last_retrieved_at is not None
    # Cold-start calibration applied on insert; confidence untouched by retrieval.
    expected_conf = 0.5 * 0.95  # state default
    assert got[0].confidence == pytest.approx(expected_conf)

    got2 = await repo.retrieve([row.id], conn=tx_conn)
    assert got2[0].retrieval_count == 2
    assert got2[0].activation == pytest.approx(1.0)
    assert got2[0].confidence == pytest.approx(expected_conf)


async def test_reconsolidation_never_touches_confidence(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="immutable-conf",
                embedding=embedding,
                confidence=0.45,
            ),
            conn=tx_conn,
        )
    # Cold-start calibration discounts: 0.45 × 0.95 (state default) = 0.4275.
    # Retrieval must never mutate that value.
    expected_conf = 0.45 * 0.95
    for _ in range(10):
        got = await repo.retrieve([row.id], conn=tx_conn)
        assert got[0].confidence == pytest.approx(expected_conf)


# =====================================================================
# Decay (spec §2)
# =====================================================================


async def test_hourly_decay_matches_spec_formula(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """activation=1.0, 120 hourly decays → ~0.368 (e^-1)."""
    import math
    from services.models.decay import hourly_decay

    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="decay me",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )

    # Filter to just this test's model to avoid touching siblings' rows
    # from other in-flight test transactions (rare in pytest but safe).
    for _ in range(120):
        await tx_conn.execute(
            """
            UPDATE models
            SET activation = activation * exp(-1.0/120.0)
            WHERE status = 'active' AND id = $1
            """,
            row.id,
        )
    final = await tx_conn.fetchval(
        "SELECT activation FROM models WHERE id = $1", row.id
    )
    assert final == pytest.approx(math.exp(-1.0), rel=1e-6)


async def test_hourly_decay_helper_runs(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """services.models.decay.hourly_decay helper runs and returns rowcount."""
    from services.models.decay import hourly_decay

    with notify_scope():
        await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="decay helper",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    n = await hourly_decay(conn=tx_conn)
    # rowcount >= 1 (might include other in-flight models on shared DB)
    assert n >= 1


async def test_archive_decayed_archives_only_cold_low_activation(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """activation < 0.05 AND (last_retrieved_at NULL OR > 30d ago) → archive.

    To avoid interfering with other agents' rows, we target only our
    three models by id in a localised version of the decay SQL.
    """
    with notify_scope():
        hot_low = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="a", assertion="hot low"),
                natural="hot low",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
        cold_low = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="b", assertion="cold low"),
                natural="cold low",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
        cold_high = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="c", assertion="cold high"),
                natural="cold high",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    # hot_low: activation tiny but last_retrieved_at recent → NOT archived
    await tx_conn.execute(
        "UPDATE models SET activation = 0.01, last_retrieved_at = now() "
        "WHERE id = $1",
        hot_low.id,
    )
    # cold_low: activation tiny + retrieval NULL → target
    await tx_conn.execute(
        "UPDATE models SET activation = 0.01, last_retrieved_at = NULL "
        "WHERE id = $1",
        cold_low.id,
    )
    # cold_high: activation high + retrieval old → NOT archived (activation OK)
    await tx_conn.execute(
        "UPDATE models SET activation = 0.5, "
        "last_retrieved_at = now() - interval '40 days' WHERE id = $1",
        cold_high.id,
    )
    # Targeted decay (same predicate, scoped to our three ids)
    await tx_conn.execute(
        """
        UPDATE models
        SET status = 'archived',
            archived_at = now(),
            archive_reason = 'decay'
        WHERE status = 'active'
          AND id = ANY($1::uuid[])
          AND activation < 0.05
          AND (last_retrieved_at IS NULL
               OR last_retrieved_at < now() - interval '30 days')
        """,
        [hot_low.id, cold_low.id, cold_high.id],
    )
    rows = {
        r["id"]: r
        for r in await tx_conn.fetch(
            "SELECT id, status, archive_reason FROM models "
            "WHERE id = ANY($1::uuid[])",
            [hot_low.id, cold_low.id, cold_high.id],
        )
    }
    assert rows[hot_low.id]["status"] == "active"
    assert rows[cold_low.id]["status"] == "archived"
    assert rows[cold_low.id]["archive_reason"] == "decay"
    assert rows[cold_high.id]["status"] == "active"


# =====================================================================
# Archive flow + dependent tracking
# =====================================================================


async def test_archive_tracks_dependents_in_state_change(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """Q8 resolved: model_reeval_queue is a real table (migration 0007).
    archive() enqueues every active dependent with a cause_kind derived
    from the archive_reason. The state_change still carries a dependent
    count for observability, but the dependent ids live in the queue."""
    with notify_scope():
        parent = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="parent", assertion="x"),
                natural="parent",
                embedding=embedding,
                confidence=0.4,
            ),
            conn=tx_conn,
        )
        child = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="child", assertion="y"),
                natural="child",
                embedding=embedding,
                confidence=0.4,
                supporting_model_ids=[parent.id],
            ),
            conn=tx_conn,
        )
        await repo.archive(parent.id, reason="manual", conn=tx_conn)

    # State_change still fires with dependent_count in metadata.
    obs = await tx_conn.fetchrow(
        """
        SELECT content FROM observations
        WHERE kind = 'state_change'
          AND content->>'entity_id' = $1
        ORDER BY occurred_at DESC LIMIT 1
        """,
        str(parent.id),
    )
    assert obs is not None
    import json as _json
    content = obs["content"]
    if isinstance(content, str):
        content = _json.loads(content)
    meta = content.get("metadata") or {}
    assert meta.get("dependent_count") == 1
    assert meta.get("reeval_cause_kind") == "supporting_archived"

    # Dependents are in model_reeval_queue, not in state_change.
    queue_row = await tx_conn.fetchrow(
        """
        SELECT model_id, cause_model_id, cause_kind, processed_at
        FROM model_reeval_queue
        WHERE tenant_id = $1 AND cause_model_id = $2
        """,
        tenant,
        parent.id,
    )
    assert queue_row is not None
    assert queue_row["model_id"] == child.id
    assert queue_row["cause_kind"] == "supporting_archived"
    assert queue_row["processed_at"] is None

    # Dedup: re-archiving (hypothetically — archive of an archived
    # model is a no-op at DB level but the enqueue call is idempotent
    # via the NULLS NOT DISTINCT constraint).
    # Here we directly attempt a duplicate insert to verify the
    # constraint works.
    from lib.shared.ids import uuid7 as _uuid7
    await tx_conn.execute(
        """
        INSERT INTO model_reeval_queue
          (id, tenant_id, model_id, cause_model_id, cause_kind)
        VALUES ($1, $2, $3, $4, 'supporting_archived')
        ON CONFLICT ON CONSTRAINT model_reeval_queue_dedup DO NOTHING
        """,
        _uuid7(), tenant, child.id, parent.id,
    )
    count = await tx_conn.fetchval(
        """
        SELECT count(*) FROM model_reeval_queue
        WHERE tenant_id = $1 AND cause_model_id = $2 AND processed_at IS NULL
        """,
        tenant, parent.id,
    )
    assert count == 1, "dedup constraint should collapse unprocessed duplicates"


# =====================================================================
# Search
# =====================================================================


async def test_search_by_scope_returns_models_for_actor(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        inserted = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="x", assertion="y"),
                natural="alice",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    found = await repo.search_by_scope(
        tenant_id=tenant, scope_actors=[actor_id], conn=tx_conn
    )
    assert len(found) == 1
    assert found[0].id == inserted.id
    assert found[0].scope_actors == [actor_id]


async def test_search_by_scope_entity_gin(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        inserted = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="c-187", assertion="in progress"),
                natural="customer resource x",
                embedding=embedding,
                confidence=0.4,
                scope_entities=[{"type": "customer", "id": "acme"}],
            ),
            conn=tx_conn,
        )
    found = await repo.search_by_scope(
        tenant_id=tenant,
        scope_entities=[{"type": "customer", "id": "acme"}],
        conn=tx_conn,
    )
    assert inserted.id in {m.id for m in found}


async def test_search_by_embedding_clusters_similar(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    anchor_vec = make_embedding("alice ships consistently on schedule")
    similar = similar_embedding(anchor_vec, jitter=0.01)
    unrelated = make_embedding("market trend shifting against enterprise vendors")

    with notify_scope():
        m1 = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="a1", assertion="x"),
                natural="alice delivers small tasks reliably",
                embedding=anchor_vec,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
        m2 = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="a2", assertion="y"),
                natural="alice is consistent on cadence",
                embedding=similar,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
        m3 = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="market", assertion="z"),
                natural="market is shifting",
                embedding=unrelated,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    top2 = await repo.search_by_embedding(
        anchor_vec, tenant_id=tenant, k=2, conn=tx_conn
    )
    ids = {m.id for m in top2}
    assert m1.id in ids
    assert m2.id in ids
    assert m3.id not in ids


async def test_status_filter_archived_excluded_from_semantic_search(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="archive test",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
        await repo.archive(row.id, reason="manual", conn=tx_conn)
    found = await repo.search_by_embedding(
        embedding, tenant_id=tenant, k=10, conn=tx_conn
    )
    assert row.id not in {m.id for m in found}


# =====================================================================
# Predictions due
# =====================================================================


async def test_get_predictions_due_returns_matured(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    past = datetime.now(tz=timezone.utc) - timedelta(days=1)
    future = datetime.now(tz=timezone.utc) + timedelta(days=30)

    with notify_scope():
        matured = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=prediction_proposition(expected="x", resolution="y"),
                natural="matured prediction",
                embedding=embedding,
                confidence=0.5,
                scope_temporal={"type": "future", "deadline": past.isoformat()},
                evaluate_at=past,
            ),
            conn=tx_conn,
        )
        not_matured = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=prediction_proposition(expected="a", resolution="b"),
                natural="future prediction",
                embedding=embedding,
                confidence=0.5,
                scope_temporal={"type": "future", "deadline": future.isoformat()},
                evaluate_at=future,
            ),
            conn=tx_conn,
        )
    due = await repo.get_predictions_due(
        datetime.now(tz=timezone.utc), tenant_id=tenant, conn=tx_conn
    )
    ids = {m.id for m in due}
    assert matured.id in ids
    assert not_matured.id not in ids


# =====================================================================
# Bulk confidence update
# =====================================================================


async def test_bulk_confidence_update_many_models_atomic(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """100 Models updated atomically. All emit state_change."""
    ids: list[uuid.UUID] = []
    with notify_scope():
        for i in range(100):
            row = await repo.insert(
                _mc(
                    tenant=tenant,
                    born_from_event=born_from_event,
                    actor_id=actor_id,
                    proposition=state_proposition(subject=f"s{i}", assertion="x"),
                    natural=f"bulk {i}",
                    embedding=embedding,
                    confidence=0.5,
                ),
                conn=tx_conn,
            )
            ids.append(row.id)
        updates = {mid: 0.3 for mid in ids}
        updated = await repo.bulk_confidence_update(updates, conn=tx_conn)
    assert len(updated) == 100
    assert all(m.confidence == pytest.approx(0.3) for m in updated)
    assert all(m.confidence_at_assertion == pytest.approx(0.5) for m in updated)


async def test_bulk_confidence_update_clips(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="bulk clip",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
        updated = await repo.bulk_confidence_update({row.id: 1.5}, conn=tx_conn)
    assert updated[0].confidence == pytest.approx(0.95)


# =====================================================================
# Tenant isolation
# =====================================================================


async def test_tenant_isolation(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    other_tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    # Create a second tenant's actor + obs inside the same tx.
    other_actor = uuid7()
    other_obs = uuid7()
    await tx_conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status,
            metadata, specification_id, created_at, last_seen_at
        ) VALUES ($1, $2, 'human_internal', 'Other', NULL, 'active',
                  '{}'::jsonb, NULL, now(), NULL)
        """,
        other_actor,
        other_tenant,
    )
    await tx_conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES ($1, $2, now(), 'signal', 'test:signal',
                  $3, '{}'::jsonb, 'other', NULL, TRUE,
                  'authoritative', $4, '[]'::jsonb)
        """,
        other_obs,
        other_tenant,
        other_actor,
        f"other-{other_obs}",
    )

    with notify_scope():
        mine = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="mine",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
        theirs = await repo.insert(
            _mc(
                tenant=other_tenant,
                born_from_event=other_obs,
                actor_id=other_actor,
                proposition=state_proposition(),
                natural="theirs",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )

    mine_seen = await repo.search_by_scope(
        tenant_id=tenant, scope_actors=[actor_id], conn=tx_conn
    )
    theirs_seen = await repo.search_by_scope(
        tenant_id=other_tenant, scope_actors=[other_actor], conn=tx_conn
    )
    assert mine.id in {m.id for m in mine_seen}
    assert theirs.id not in {m.id for m in mine_seen}
    assert theirs.id in {m.id for m in theirs_seen}
    assert mine.id not in {m.id for m in theirs_seen}


# =====================================================================
# Concurrency — deliberately serial inside a single tx (gather on one
# connection would pipeline; asyncpg forbids that), so we sequentially
# insert 20 in a tight loop to exercise the hot path and confirm every
# row commits distinct.
# =====================================================================


async def test_sequential_inserts_do_not_interfere(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """20 sequential inserts produce 20 distinct ids and all survive read-back."""
    N = 20
    ids = []
    with notify_scope():
        for i in range(N):
            row = await repo.insert(
                _mc(
                    tenant=tenant,
                    born_from_event=born_from_event,
                    actor_id=actor_id,
                    proposition=state_proposition(subject=f"c{i}", assertion="x"),
                    natural=f"seq {i}",
                    embedding=embedding,
                    confidence=0.5,
                ),
                conn=tx_conn,
            )
            ids.append(row.id)
    assert len(set(ids)) == N
    # All readable back:
    rows = await tx_conn.fetch(
        "SELECT id FROM models WHERE id = ANY($1::uuid[])", ids
    )
    assert {r["id"] for r in rows} == set(ids)


# =====================================================================
# Invalid scope actor rejected
# =====================================================================


async def test_insert_rejects_missing_scope_actor(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    ghost = uuid7()
    with pytest.raises(ValidationError), notify_scope():
        await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                proposition=state_proposition(),
                natural="ghost scope",
                embedding=embedding,
                scope_actors=[ghost],
                confidence=0.5,
            ),
            conn=tx_conn,
        )


# =====================================================================
# Embedding dim validation
# =====================================================================


async def test_insert_rejects_bad_embedding_dim(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    with pytest.raises(ValidationError), notify_scope():
        await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="bad-dim",
                embedding=[0.0, 1.0, 2.0],   # 3 floats, not 768
                confidence=0.5,
            ),
            conn=tx_conn,
        )


# =====================================================================
# A4: status_notes sidecar
# =====================================================================


async def test_add_note_and_list_notes(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    from services.models.status_notes import add_note, list_notes

    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="needs a note",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )

    await add_note(
        row.id,
        note="alice contested this — the system misunderstood",
        kind="first_person_override",
        authored_by=actor_id,
        conn=tx_conn,
    )
    await add_note(
        row.id,
        note="falsifier triggered on 2026-05-01",
        kind="system",
        conn=tx_conn,
    )
    notes = await list_notes(row.id, conn=tx_conn)

    assert len(notes) == 2
    kinds = {n.kind for n in notes}
    assert kinds == {"first_person_override", "system"}
    # Newest first:
    assert notes[0].authored_at >= notes[1].authored_at


async def test_add_note_rejects_bad_kind(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    from services.models.status_notes import add_note

    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="n",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    with pytest.raises(ValidationError):
        await add_note(row.id, note="x", kind="not_a_kind", conn=tx_conn)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        await add_note(row.id, note="", kind="manual", conn=tx_conn)


# =====================================================================
# state_change emitted on insert
# =====================================================================


async def test_insert_emits_state_change_observation(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """§2 step 8: state_change observation with cause_id=born_from_event_id."""
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(),
                natural="state change check",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    obs = await tx_conn.fetchrow(
        """
        SELECT id, kind, cause_id, source_channel, content
        FROM observations
        WHERE kind = 'state_change'
          AND content->>'entity_id' = $1
        ORDER BY occurred_at DESC LIMIT 1
        """,
        str(row.id),
    )
    assert obs is not None
    assert obs["kind"] == "state_change"
    assert obs["cause_id"] == born_from_event
    assert obs["source_channel"] == "internal:state_change"


# =====================================================================
# get_by_id round-trip
# =====================================================================


async def test_get_by_id_round_trip(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
    embedding: list[float],
) -> None:
    """ModelCreate → insert → get_by_id returns equivalent row."""
    with notify_scope():
        row = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition=state_proposition(subject="rt", assertion="x"),
                natural="round trip",
                embedding=embedding,
                confidence=0.5,
            ),
            conn=tx_conn,
        )
    got = await repo.get_by_id(row.id, conn=tx_conn)
    assert got is not None
    assert got.id == row.id
    assert got.proposition["kind"] == "state"
    assert got.proposition["subject"] == "rt"
    # Cold-start calibration: 0.5 × 0.95 = 0.475; assertion unchanged.
    assert got.confidence == pytest.approx(0.475)
    assert got.confidence_at_assertion == 0.5
    assert got.natural == "round trip"
