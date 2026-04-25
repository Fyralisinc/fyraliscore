"""
services/actors/tests/test_repo.py — integration tests for ActorRepo.

Every test uses the real `fresh_db` fixture from the root conftest.py.
No mocks for Postgres (BUILD-PLAN §0.5). Tests cover:
  - three-value type enum (Q5 resolution)
  - identity mapping happy path / multi-channel / duplicate rejection
  - resolve_by_source_actor_ref (happy, unknown, bad format)
  - list_active_actors tenant isolation + status filter
  - deactivate invariant: active commitment blocks deactivation
  - deactivate happy path: metadata carries the reason
  - property test on type validation (hypothesis)
  - concurrent insert on (source_channel, source_actor_ref)
"""
from __future__ import annotations

import asyncio
import uuid

import asyncpg
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ActorRow
from services.actors.repo import ActorRepo


pytestmark = [pytest.mark.integration]


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def tenant() -> uuid.UUID:
    return uuid7()


@pytest.fixture
def other_tenant() -> uuid.UUID:
    return uuid7()


@pytest.fixture
def repo(fresh_db: asyncpg.Pool) -> ActorRepo:
    return ActorRepo(fresh_db)


# ---------------------------------------------------------------------
# create_actor — every type value
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "actor_type", ["human_internal", "human_external", "ai_agent"]
)
async def test_create_actor_accepts_all_three_types(
    repo: ActorRepo, tenant: uuid.UUID, actor_type: str
) -> None:
    """Q5 resolution: three-value enum, all three accepted."""
    row = await repo.create_actor(
        email=f"{actor_type}@example.com",
        display_name=actor_type.replace("_", " ").title(),
        type=actor_type,  # type: ignore[arg-type]
        tenant_id=tenant,
    )
    assert isinstance(row, ActorRow)
    assert row.type == actor_type
    assert row.status == "active"
    assert row.tenant_id == tenant
    # uuid7 is time-sortable — verify the version bits:
    assert row.id.version == 7


@pytest.mark.parametrize(
    "bad_type", ["human", "agent", "HUMAN_INTERNAL", "", "robot", None]
)
async def test_create_actor_rejects_unknown_type(
    repo: ActorRepo, tenant: uuid.UUID, bad_type
) -> None:
    """Q5 resolution: the two-value paraphrase is rejected."""
    with pytest.raises(ValidationError) as exc:
        await repo.create_actor(
            email=None,
            display_name="Tester",
            type=bad_type,  # type: ignore[arg-type]
            tenant_id=tenant,
        )
    assert "actor type" in exc.value.message or "type" in exc.value.context


async def test_create_actor_requires_non_empty_display_name(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    with pytest.raises(ValidationError):
        await repo.create_actor(
            email=None,
            display_name="   ",
            type="human_internal",
            tenant_id=tenant,
        )


async def test_create_actor_records_nexus_attested_in_metadata(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    row = await repo.create_actor(
        email="ai@example.com",
        display_name="Ally Agent",
        type="ai_agent",
        tenant_id=tenant,
        nexus_attested=True,
    )
    assert row.metadata is not None
    assert row.metadata.get("nexus_attested") is True


async def test_create_actor_allows_null_email(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    row = await repo.create_actor(
        email=None,
        display_name="Email-less",
        type="human_external",
        tenant_id=tenant,
    )
    assert row.email is None


# ---------------------------------------------------------------------
# add_identity_mapping
# ---------------------------------------------------------------------


async def test_add_identity_mapping_happy_path(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    alice = await repo.create_actor(
        email="alice@example.com",
        display_name="Alice",
        type="human_internal",
        tenant_id=tenant,
    )
    mapping = await repo.add_identity_mapping(
        actor_id=alice.id,
        source_channel="slack",
        source_actor_ref="U01ALICE",
    )
    assert mapping.actor_id == alice.id
    assert mapping.source_channel == "slack"
    assert mapping.source_actor_ref == "U01ALICE"
    assert mapping.confidence == 1.0


async def test_one_actor_many_channels(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    """BUILD-PLAN 1-B: "Same actor can have multiple mappings (slack + github + email)."""
    alice = await repo.create_actor(
        email="alice@example.com",
        display_name="Alice",
        type="human_internal",
        tenant_id=tenant,
    )
    for channel, ref in [
        ("slack", "U01ALICE"),
        ("github", "alice-gh"),
        ("email", "alice@example.com"),
    ]:
        await repo.add_identity_mapping(
            actor_id=alice.id, source_channel=channel, source_actor_ref=ref
        )

    for channel, ref in [
        ("slack", "U01ALICE"),
        ("github", "alice-gh"),
        ("email", "alice@example.com"),
    ]:
        resolved = await repo.resolve_by_source_actor_ref(f"{channel}:{ref}")
        assert resolved == alice.id


async def test_duplicate_identity_mapping_rejected(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    """(source_channel, source_actor_ref) PK must reject duplicates cleanly."""
    alice = await repo.create_actor(
        email="alice@example.com",
        display_name="Alice",
        type="human_internal",
        tenant_id=tenant,
    )
    bob = await repo.create_actor(
        email="bob@example.com",
        display_name="Bob",
        type="human_internal",
        tenant_id=tenant,
    )
    await repo.add_identity_mapping(
        actor_id=alice.id, source_channel="slack", source_actor_ref="U01ALICE"
    )
    with pytest.raises(ValidationError) as exc:
        # Same pair, different actor — must reject.
        await repo.add_identity_mapping(
            actor_id=bob.id, source_channel="slack", source_actor_ref="U01ALICE"
        )
    assert "already exists" in exc.value.message


async def test_identity_mapping_rejects_unknown_actor(
    repo: ActorRepo,
) -> None:
    """FK to actors(id); asyncpg error must surface as ValidationError."""
    ghost = uuid7()
    with pytest.raises(ValidationError):
        await repo.add_identity_mapping(
            actor_id=ghost, source_channel="slack", source_actor_ref="U_ghost"
        )


# ---------------------------------------------------------------------
# resolve_by_source_actor_ref
# ---------------------------------------------------------------------


async def test_resolve_unknown_returns_none(
    repo: ActorRepo,
) -> None:
    assert await repo.resolve_by_source_actor_ref("slack:U_NOBODY") is None


async def test_resolve_rejects_malformed_ref(repo: ActorRepo) -> None:
    for bad in ["", "slackonly", ":no-channel", "no-ref:"]:
        with pytest.raises(ValidationError):
            await repo.resolve_by_source_actor_ref(bad)


async def test_resolve_tolerates_colon_in_ref(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    """GitHub node IDs contain colons; split must be on the first one."""
    alice = await repo.create_actor(
        email="alice@example.com",
        display_name="Alice",
        type="human_internal",
        tenant_id=tenant,
    )
    await repo.add_identity_mapping(
        actor_id=alice.id,
        source_channel="github",
        source_actor_ref="MDQ6VXNlcjE6MjM=",  # realistic base64-with-colons
    )
    assert (
        await repo.resolve_by_source_actor_ref("github:MDQ6VXNlcjE6MjM=")
        == alice.id
    )


# ---------------------------------------------------------------------
# list_active_actors
# ---------------------------------------------------------------------


async def test_list_active_actors_filters_by_status(
    repo: ActorRepo, tenant: uuid.UUID, fresh_db: asyncpg.Pool
) -> None:
    active = await repo.create_actor(
        email="a@example.com",
        display_name="Active",
        type="human_internal",
        tenant_id=tenant,
    )
    inactive = await repo.create_actor(
        email="i@example.com",
        display_name="Inactive",
        type="human_internal",
        tenant_id=tenant,
    )
    # Flip to inactive via raw SQL so we don't invoke deactivate's
    # invariant check here.
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "UPDATE actors SET status='inactive' WHERE id=$1", inactive.id
        )

    rows = await repo.list_active_actors(tenant)
    ids = {r.id for r in rows}
    assert active.id in ids
    assert inactive.id not in ids


async def test_list_active_actors_tenant_isolation(
    repo: ActorRepo, tenant: uuid.UUID, other_tenant: uuid.UUID
) -> None:
    """Actors in tenant A must not leak into tenant B's listing."""
    me = await repo.create_actor(
        email="me@example.com",
        display_name="Me",
        type="human_internal",
        tenant_id=tenant,
    )
    other = await repo.create_actor(
        email="o@example.com",
        display_name="Other",
        type="human_internal",
        tenant_id=other_tenant,
    )
    mine = await repo.list_active_actors(tenant)
    theirs = await repo.list_active_actors(other_tenant)
    assert {r.id for r in mine} == {me.id}
    assert {r.id for r in theirs} == {other.id}


# ---------------------------------------------------------------------
# deactivate — invariant check
# ---------------------------------------------------------------------


async def _insert_stub_commitment(
    pool: asyncpg.Pool,
    *,
    owner_id: uuid.UUID,
    tenant_id: uuid.UUID,
    state: str,
) -> uuid.UUID:
    """
    Insert a commitment owned by `owner_id` in the given state.

    We must not build the commitments repo (that is Agent 1-D's
    surface). BUILD-PLAN 1-B explicitly allows a direct INSERT for the
    invariant test. `created_by_event_id` is NOT NULL but has no FK
    enforcement (partitioned-observations adaptation — see
    0001_foundation.sql comment), so any UUID is acceptable here.
    """
    cid = uuid7()
    fake_event_id = uuid7()
    await pool.execute(
        """
        INSERT INTO commitments (
            id, tenant_id, title, description,
            state, owner_id, due_date, ambition_level, priority,
            created_at, last_state_change_at, created_by_event_id
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, now() + interval '7 days',
            'base', 5,
            now(), now(), $7
        )
        """,
        cid,
        tenant_id,
        f"stub-{cid}",
        "stub commitment for actor deactivation test",
        state,
        owner_id,
        fake_event_id,
    )
    return cid


@pytest.mark.parametrize(
    "blocking_state",
    ["active", "blocked", "paused", "doneunverified"],
)
async def test_deactivate_blocked_by_active_commitment(
    repo: ActorRepo,
    fresh_db: asyncpg.Pool,
    tenant: uuid.UUID,
    blocking_state: str,
) -> None:
    alice = await repo.create_actor(
        email="alice@example.com",
        display_name="Alice",
        type="human_internal",
        tenant_id=tenant,
    )
    cid = await _insert_stub_commitment(
        fresh_db,
        owner_id=alice.id,
        tenant_id=tenant,
        state=blocking_state,
    )
    with pytest.raises(InvariantViolation) as exc:
        await repo.deactivate(alice.id, reason="offboarded")
    assert exc.value.invariant == "actor_deactivation_no_active_commitments"
    # Commitment id is surfaced in context for UI.
    blockers = exc.value.context["active_commitments"]
    assert any(b["id"] == str(cid) and b["state"] == blocking_state for b in blockers)

    # Actor must still be active.
    actives = await repo.list_active_actors(tenant)
    assert alice.id in {a.id for a in actives}


@pytest.mark.parametrize(
    "terminal_state", ["proposed", "doneverified", "closed"]
)
async def test_deactivate_allowed_when_commitments_terminal(
    repo: ActorRepo,
    fresh_db: asyncpg.Pool,
    tenant: uuid.UUID,
    terminal_state: str,
) -> None:
    alice = await repo.create_actor(
        email="alice@example.com",
        display_name="Alice",
        type="human_internal",
        tenant_id=tenant,
    )
    await _insert_stub_commitment(
        fresh_db,
        owner_id=alice.id,
        tenant_id=tenant,
        state=terminal_state,
    )
    row = await repo.deactivate(alice.id, reason="graduated")
    assert row.status == "inactive"
    assert row.metadata["deactivation_reason"] == "graduated"


async def test_deactivate_records_reason_in_metadata(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    actor = await repo.create_actor(
        email=None,
        display_name="Test",
        type="ai_agent",
        tenant_id=tenant,
    )
    row = await repo.deactivate(actor.id, reason="retired model")
    assert row.status == "inactive"
    assert row.metadata["deactivation_reason"] == "retired model"
    # nexus_attested flag must be preserved through the jsonb merge.
    assert "nexus_attested" in row.metadata


async def test_deactivate_requires_reason(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    actor = await repo.create_actor(
        email=None,
        display_name="X",
        type="human_internal",
        tenant_id=tenant,
    )
    with pytest.raises(ValidationError):
        await repo.deactivate(actor.id, reason="")


async def test_deactivate_unknown_actor(repo: ActorRepo) -> None:
    with pytest.raises(ValidationError):
        await repo.deactivate(uuid7(), reason="because")


# ---------------------------------------------------------------------
# Property test — type enum safety
# ---------------------------------------------------------------------


@given(
    bad=st.text(min_size=1, max_size=30).filter(
        lambda s: s not in {"human_internal", "human_external", "ai_agent"}
    )
)
@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
async def test_property_random_type_string_rejected(
    bad: str, repo: ActorRepo, tenant: uuid.UUID
) -> None:
    with pytest.raises(ValidationError):
        await repo.create_actor(
            email=None,
            display_name="fuzz",
            type=bad,  # type: ignore[arg-type]
            tenant_id=tenant,
        )


# ---------------------------------------------------------------------
# Concurrency — identity-mapping unique constraint races
# ---------------------------------------------------------------------


async def test_concurrent_identity_mapping_exactly_one_wins(
    repo: ActorRepo, tenant: uuid.UUID
) -> None:
    alice = await repo.create_actor(
        email="a@example.com",
        display_name="Alice",
        type="human_internal",
        tenant_id=tenant,
    )

    errors: list[str] = []

    async def attempt() -> bool:
        try:
            await repo.add_identity_mapping(
                actor_id=alice.id,
                source_channel="slack",
                source_actor_ref="U01RACE",
            )
            return True
        except ValidationError:
            return False
        except Exception as e:  # pragma: no cover - diagnostic on failure
            errors.append(f"{type(e).__name__}: {e}")
            return False

    results = await asyncio.gather(*[attempt() for _ in range(10)])
    wins = sum(results)
    assert wins == 1, (
        f"exactly one insert must win; got {wins}. "
        f"errors: {errors[:3]}"
    )

    # Mapping is resolvable.
    assert (
        await repo.resolve_by_source_actor_ref("slack:U01RACE") == alice.id
    )
