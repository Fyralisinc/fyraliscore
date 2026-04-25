"""
Wave 5-A access control tests — 20+ cases.

Covers all five layers, materialized views, tenant isolation, admin
override, first-person override, realtime revocation, property test.
"""
from __future__ import annotations

import json
import random
import uuid

import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from services.access_control.checks import (
    AccessDecision,
    can_read,
    can_read_by_id,
)
from services.access_control.hierarchy import (
    is_hr_channel,
    is_in_manager_chain,
    is_shared_channel,
    manager_chain_of,
    register_shared_channel,
)
from services.access_control.materialized import (
    MATERIALIZED_VIEWS,
    enqueue_refresh,
    is_commitment_visible_to,
    refresh_all,
    refresh_one,
)
from services.access_control.roles import (
    grant_role,
    has_role,
    revoke_role,
    roles_for_actor,
)

from .conftest import (
    insert_actor,
    insert_commitment,
    insert_contributes_to,
    insert_contributor,
    insert_decision,
    insert_deployment,
    insert_goal,
    insert_model,
    insert_observation,
    insert_resource,
)


pytestmark = pytest.mark.asyncio


# =====================================================================
# Test 1 — each role grant round-trips through has_role
# =====================================================================


@pytest.mark.parametrize(
    "role,entity_type",
    [
        ("owner", "commitment"),
        ("contributor", "commitment"),
        ("viewer", "goal"),
        ("admin", "tenant"),
        ("finance", "tenant"),
        ("legal", "tenant"),
        ("leadership", "tenant"),
    ],
)
async def test_grant_and_has_role(tx_conn, tenant, role, entity_type):
    actor = await insert_actor(tx_conn, tenant)
    entity_id: uuid.UUID | None = None
    if entity_type == "commitment":
        entity_id = await insert_commitment(tx_conn, tenant, owner_id=actor)
    elif entity_type == "goal":
        entity_id = await insert_goal(tx_conn, tenant)
    await grant_role(
        actor, entity_type, entity_id, role, actor,
        conn=tx_conn, tenant_id=tenant,
    )
    got = await has_role(
        actor, role,
        conn=tx_conn, tenant_id=tenant,
        entity_id=entity_id, entity_type=entity_type if entity_id else None,
    )
    assert got is True


# =====================================================================
# Test 2 — revoke → has_role False; re-grant → True again
# =====================================================================


async def test_revoke_then_regrant(tx_conn, tenant):
    actor = await insert_actor(tx_conn, tenant)
    await grant_role(
        actor, "tenant", None, "finance", actor,
        conn=tx_conn, tenant_id=tenant,
    )
    assert await has_role(actor, "finance", conn=tx_conn, tenant_id=tenant)
    revoked = await revoke_role(
        actor, "tenant", None, "finance",
        conn=tx_conn, tenant_id=tenant,
    )
    assert revoked is True
    assert not await has_role(
        actor, "finance", conn=tx_conn, tenant_id=tenant,
    )
    # Re-grant creates a brand-new active row.
    await grant_role(
        actor, "tenant", None, "finance", actor,
        conn=tx_conn, tenant_id=tenant,
    )
    assert await has_role(actor, "finance", conn=tx_conn, tenant_id=tenant)
    # roles_for_actor returns only the active one.
    active = await roles_for_actor(actor, conn=tx_conn, tenant_id=tenant)
    finance_active = [r for r in active if r["role"] == "finance"]
    assert len(finance_active) == 1


# =====================================================================
# Test 3 — Commitment owner can_read
# =====================================================================


async def test_commitment_owner_can_read(tx_conn, tenant):
    actor = await insert_actor(tx_conn, tenant)
    cid = await insert_commitment(tx_conn, tenant, owner_id=actor)
    decision = await can_read_by_id(
        actor, "commitment", cid, conn=tx_conn, tenant_id=tenant,
    )
    assert decision.allowed
    assert decision.reason == "commitment_owner"


# =====================================================================
# Test 4 — Commitment contributor can_read
# =====================================================================


async def test_commitment_contributor_can_read(tx_conn, tenant):
    owner = await insert_actor(tx_conn, tenant)
    contrib = await insert_actor(tx_conn, tenant)
    cid = await insert_commitment(tx_conn, tenant, owner_id=owner)
    await insert_contributor(tx_conn, cid, contrib)
    decision = await can_read_by_id(
        contrib, "commitment", cid, conn=tx_conn, tenant_id=tenant,
    )
    assert decision.allowed
    assert decision.reason == "commitment_contributor"


# =====================================================================
# Test 5 — Goal viewer: reads Goal, NOT child Commitments unless shared
# =====================================================================


async def test_goal_viewer_does_not_see_unshared_commitment(tx_conn, tenant):
    viewer = await insert_actor(tx_conn, tenant)
    owner = await insert_actor(tx_conn, tenant)
    gid = await insert_goal(tx_conn, tenant)
    cid = await insert_commitment(tx_conn, tenant, owner_id=owner)
    await insert_contributes_to(tx_conn, cid, gid)

    await grant_role(
        viewer, "goal", gid, "viewer", viewer,
        conn=tx_conn, tenant_id=tenant,
    )
    # Goal visible (role grant).
    g_decision = await can_read_by_id(
        viewer, "goal", gid, conn=tx_conn, tenant_id=tenant,
    )
    assert g_decision.allowed, g_decision.reason

    # Commitment NOT visible — viewer has no stake in it.
    c_decision = await can_read_by_id(
        viewer, "commitment", cid, conn=tx_conn, tenant_id=tenant,
    )
    assert not c_decision.allowed


# =====================================================================
# Test 6 — Private Model: scope_actors members can read, others cannot
# =====================================================================


async def test_private_model_scope_actors(tx_conn, tenant):
    scoped = await insert_actor(tx_conn, tenant)
    outsider = await insert_actor(tx_conn, tenant)
    obs = await insert_observation(tx_conn, tenant, scoped)
    mid = await insert_model(
        tx_conn,
        tenant=tenant,
        born_from_event_id=obs,
        scope_actors=[scoped],
        visible_to_subjects=False,
        natural="secret about scoped",
    )
    in_scope = await can_read_by_id(
        scoped, "model", mid, conn=tx_conn, tenant_id=tenant,
    )
    assert in_scope.allowed
    assert in_scope.reason == "model_self_scope"

    out_of_scope = await can_read_by_id(
        outsider, "model", mid, conn=tx_conn, tenant_id=tenant,
    )
    assert not out_of_scope.allowed


# =====================================================================
# Test 7 — Public Model is visible to any tenant actor
# =====================================================================


async def test_public_model_visible_to_anyone(tx_conn, tenant):
    author = await insert_actor(tx_conn, tenant)
    reader = await insert_actor(tx_conn, tenant)
    obs = await insert_observation(tx_conn, tenant, author)
    mid = await insert_model(
        tx_conn,
        tenant=tenant,
        born_from_event_id=obs,
        visible_to_subjects=True,
        natural="public fact",
    )
    decision = await can_read_by_id(
        reader, "model", mid, conn=tx_conn, tenant_id=tenant,
    )
    assert decision.allowed and decision.reason == "model_public"


# =====================================================================
# Test 8 — Pattern Model visible via scope_entities.commitment
# =====================================================================


async def test_pattern_model_via_scope_entity(committed_conn, tenant):
    """Pattern Model with scope_entities=[{type=commitment, id=<cid>}]
    — anyone visible on the commitment sees the Model.

    Uses committed_conn because the matview must see the rows + we
    trigger a refresh mid-test.
    """
    conn = committed_conn
    try:
        owner = await insert_actor(conn, tenant)
        cid = await insert_commitment(conn, tenant, owner_id=owner)
        obs = await insert_observation(conn, tenant, owner)
        mid = await insert_model(
            conn,
            tenant=tenant,
            born_from_event_id=obs,
            visible_to_subjects=False,   # private; fall through to pattern
            scope_entities=[{"type": "commitment", "id": str(cid)}],
            natural="pattern over commitment",
        )
        # Refresh matviews so actor_visible_commitments knows about cid.
        await refresh_all(conn=conn, concurrently=False)

        decision = await can_read_by_id(
            owner, "model", mid, conn=conn, tenant_id=tenant,
        )
        assert decision.allowed
        assert decision.reason == "model_via_commitment_scope"
    finally:
        # Clean up committed rows for this tenant.
        await _cleanup_tenant(conn, tenant)


# =====================================================================
# Test 9 — Financial resource: finance / leadership / outsider
# =====================================================================


async def test_financial_resource_access_rules(tx_conn, tenant):
    fin_actor = await insert_actor(tx_conn, tenant)
    leader = await insert_actor(tx_conn, tenant)
    outsider = await insert_actor(tx_conn, tenant)
    rid = await insert_resource(tx_conn, tenant, kind="financial")

    await grant_role(
        fin_actor, "tenant", None, "finance", fin_actor,
        conn=tx_conn, tenant_id=tenant,
    )
    await grant_role(
        leader, "tenant", None, "leadership", leader,
        conn=tx_conn, tenant_id=tenant,
    )

    d = await can_read_by_id(
        fin_actor, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert d.allowed, d.reason

    d = await can_read_by_id(
        leader, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert d.allowed

    d = await can_read_by_id(
        outsider, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert not d.allowed


# =====================================================================
# Test 10 — IP resource: legal / leadership only
# =====================================================================


async def test_ip_resource_legal_leadership(tx_conn, tenant):
    legal_actor = await insert_actor(tx_conn, tenant)
    outsider = await insert_actor(tx_conn, tenant)
    rid = await insert_resource(tx_conn, tenant, kind="ip")

    await grant_role(
        legal_actor, "tenant", None, "legal", legal_actor,
        conn=tx_conn, tenant_id=tenant,
    )
    d = await can_read_by_id(
        legal_actor, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert d.allowed

    d = await can_read_by_id(
        outsider, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert not d.allowed

    # Even finance role does NOT grant IP access.
    await grant_role(
        outsider, "tenant", None, "finance", outsider,
        conn=tx_conn, tenant_id=tenant,
    )
    d = await can_read_by_id(
        outsider, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert not d.allowed


# =====================================================================
# Test 11 — Customer resource: account owner + leadership
# =====================================================================


async def test_customer_resource_account_owner(tx_conn, tenant):
    acct_owner = await insert_actor(tx_conn, tenant)
    outsider = await insert_actor(tx_conn, tenant)
    rid = await insert_resource(
        tx_conn, tenant, kind="relational",
        metadata={"account_owner_id": str(acct_owner)},
    )
    d = await can_read_by_id(
        acct_owner, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert d.allowed
    assert d.reason == "resource_customer_account_owner"

    d = await can_read_by_id(
        outsider, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert not d.allowed


# =====================================================================
# Test 12 — Capacity resource: team member + manager
# =====================================================================


async def test_capacity_resource_team_and_manager(tx_conn, tenant):
    manager = await insert_actor(tx_conn, tenant)
    team_member = await insert_actor(
        tx_conn, tenant, metadata={"manager_id": str(manager)},
    )
    outsider = await insert_actor(tx_conn, tenant)

    rid = await insert_resource(
        tx_conn, tenant, kind="capacity",
        metadata={"team_ids": [str(team_member)]},
    )
    d = await can_read_by_id(
        team_member, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert d.allowed
    assert d.reason == "resource_capacity_team"

    # Capacity manager flow: deploy resource to a commitment the
    # team_member owns → manager is in the chain and can read.
    cid = await insert_commitment(tx_conn, tenant, owner_id=team_member)
    await insert_deployment(tx_conn, rid, cid)
    # Manager in the team member's chain — CAPACITY via deployment only
    # grants access to owner/contrib of the commitment. Manager access
    # for capacity flows through the commitment instead; we verify via
    # commitment visibility.
    cm = await can_read_by_id(
        manager, "commitment", cid, conn=tx_conn, tenant_id=tenant,
    )
    assert cm.allowed and cm.reason == "commitment_manager_chain"

    # Outsider can't read capacity.
    d = await can_read_by_id(
        outsider, "resource", rid, conn=tx_conn, tenant_id=tenant,
    )
    assert not d.allowed


# =====================================================================
# Test 13 — Materialized view: owner + contributor + mgr in chain
# =====================================================================


async def test_actor_visible_commitments_matview(committed_conn, tenant):
    conn = committed_conn
    try:
        manager = await insert_actor(conn, tenant)
        owner = await insert_actor(
            conn, tenant, metadata={"manager_id": str(manager)},
        )
        contrib = await insert_actor(conn, tenant)
        outsider = await insert_actor(conn, tenant)
        cid = await insert_commitment(conn, tenant, owner_id=owner)
        await insert_contributor(conn, cid, contrib)

        await refresh_all(conn=conn, concurrently=False)

        assert await is_commitment_visible_to(
            owner, cid, conn=conn, tenant_id=tenant,
        )
        assert await is_commitment_visible_to(
            contrib, cid, conn=conn, tenant_id=tenant,
        )
        assert await is_commitment_visible_to(
            manager, cid, conn=conn, tenant_id=tenant,
        )
        assert not await is_commitment_visible_to(
            outsider, cid, conn=conn, tenant_id=tenant,
        )
    finally:
        await _cleanup_tenant(conn, tenant)


# =====================================================================
# Test 14 — Matview matches live can_read over 50 random pairs
# =====================================================================


async def test_matview_matches_can_read(committed_conn, tenant):
    conn = committed_conn
    try:
        actors = [await insert_actor(conn, tenant) for _ in range(8)]
        # Wire a light manager chain: a[1] reports to a[0], etc.
        for i in range(1, 4):
            await conn.execute(
                """
                UPDATE actors
                SET metadata = jsonb_build_object('manager_id', $1::text)
                WHERE id = $2
                """,
                str(actors[i - 1]), actors[i],
            )
        commits = []
        for i in range(6):
            owner = actors[(i + 1) % len(actors)]
            commits.append(
                await insert_commitment(conn, tenant, owner_id=owner)
            )
        # Random contributors.
        rng = random.Random(42)
        for cid in commits:
            for a in actors:
                if rng.random() < 0.2:
                    await insert_contributor(conn, cid, a)

        await refresh_all(conn=conn, concurrently=False)

        pairs = [(rng.choice(actors), rng.choice(commits)) for _ in range(50)]
        for actor, cid in pairs:
            live = await can_read_by_id(
                actor, "commitment", cid, conn=conn, tenant_id=tenant,
            )
            mat = await is_commitment_visible_to(
                actor, cid, conn=conn, tenant_id=tenant,
            )
            # Live implies matview OR vice versa is the contract when
            # no admin override exists (no admin granted in this test).
            assert live.allowed == mat, (
                f"mismatch for actor={actor} cid={cid} "
                f"live={live} mat={mat}"
            )
    finally:
        await _cleanup_tenant(conn, tenant)


# =====================================================================
# Test 15 — Newly-granted role picked up after manual refresh
# =====================================================================


async def test_refresh_after_role_grant(committed_conn, tenant):
    conn = committed_conn
    try:
        leader = await insert_actor(conn, tenant)
        cid = await insert_commitment(conn, tenant)

        await refresh_all(conn=conn, concurrently=False)
        assert not await is_commitment_visible_to(
            leader, cid, conn=conn, tenant_id=tenant,
        )

        await grant_role(
            leader, "tenant", None, "leadership", leader,
            conn=conn, tenant_id=tenant,
        )
        enqueue_refresh()
        await refresh_all(conn=conn, concurrently=False)
        assert await is_commitment_visible_to(
            leader, cid, conn=conn, tenant_id=tenant,
        )
    finally:
        await _cleanup_tenant(conn, tenant)


# =====================================================================
# Test 16 — Tenant isolation is absolute (even with admin role)
# =====================================================================


async def test_tenant_isolation_absolute(tx_conn, tenant, other_tenant):
    admin = await insert_actor(tx_conn, tenant)
    await grant_role(
        admin, "tenant", None, "admin", admin,
        conn=tx_conn, tenant_id=tenant,
    )
    # Create a commitment in a DIFFERENT tenant.
    foreign_owner = await insert_actor(tx_conn, other_tenant)
    foreign_cid = await insert_commitment(
        tx_conn, other_tenant, owner_id=foreign_owner,
    )
    decision = await can_read(
        admin,
        {"kind": "commitment", "id": foreign_cid, "tenant_id": other_tenant},
        conn=tx_conn, tenant_id=tenant,
    )
    assert not decision.allowed
    assert decision.reason == "tenant_mismatch"


# =====================================================================
# Test 17 — Realtime: revocation drops subscription
# =====================================================================


async def test_realtime_revoke_drops_subscription(tx_conn, tenant):
    from services.realtime.dispatcher import Dispatcher

    # Real dispatcher with a throwaway pool — we only need
    # revoke_for_entity, which doesn't talk to the DB.
    import asyncpg
    import os
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        dispatcher = Dispatcher(pool)
        actor_id = uuid7()
        goal_id = uuid7()
        state = dispatcher.register_client(
            tenant_id=tenant, actor_id=actor_id,
            initial_topics={f"goal:{goal_id}", "tenant:other"},
        )
        assert f"goal:{goal_id}" in state.sub.topics
        dropped = await dispatcher.revoke_for_entity(
            actor_id=actor_id, entity_kind="goal", entity_id=goal_id,
        )
        assert dropped == 1
        assert f"goal:{goal_id}" not in state.sub.topics
        await dispatcher.unregister_client(state.connection_id)
    finally:
        await pool.close()


# =====================================================================
# Test 18 — First-person override: subject reads their private Model
# =====================================================================


async def test_first_person_override(tx_conn, tenant):
    subject = await insert_actor(tx_conn, tenant)
    obs = await insert_observation(tx_conn, tenant, subject)
    mid = await insert_model(
        tx_conn,
        tenant=tenant,
        born_from_event_id=obs,
        visible_to_subjects=False,
        scope_actors=[subject],
        natural="private belief about subject",
    )
    decision = await can_read_by_id(
        subject, "model", mid, conn=tx_conn, tenant_id=tenant,
    )
    assert decision.allowed
    assert decision.reason == "model_self_scope"

    # Standing check (contestability) must also succeed.
    from services.contestability.standing import actor_has_standing_on_model

    standing = await actor_has_standing_on_model(
        tx_conn, actor_id=subject, model_id=mid,
    )
    assert standing.granted
    assert standing.basis == "scope"


# =====================================================================
# Test 19 — Admin override works + writes audit row
# =====================================================================


async def test_admin_override_with_audit(tx_conn, tenant):
    from services.access_control.audit import record_override

    admin = await insert_actor(tx_conn, tenant)
    owner = await insert_actor(tx_conn, tenant)
    cid = await insert_commitment(tx_conn, tenant, owner_id=owner)
    await grant_role(
        admin, "tenant", None, "admin", admin,
        conn=tx_conn, tenant_id=tenant,
    )
    decision = await can_read_by_id(
        admin, "commitment", cid, conn=tx_conn, tenant_id=tenant,
    )
    assert decision.allowed
    assert decision.reason == "admin_override"
    assert decision.override_applied

    # Record audit row.
    log_id = await record_override(
        admin, "commitment", cid, "admin",
        conn=tx_conn, tenant_id=tenant,
        reason="test_admin_override",
    )
    assert log_id is not None
    row = await tx_conn.fetchrow(
        """
        SELECT override_kind, reason FROM access_override_log
        WHERE id = $1
        """,
        log_id,
    )
    assert row is not None
    assert row["override_kind"] == "admin"
    assert row["reason"] == "test_admin_override"

    # Admin override still denies cross-tenant (Layer 1 > override).
    other = uuid7()
    decision = await can_read(
        admin,
        {"kind": "commitment", "id": uuid7(), "tenant_id": other},
        conn=tx_conn, tenant_id=tenant,
    )
    assert not decision.allowed


# =====================================================================
# Test 20 — Property test: random role assignments → no leaks
# =====================================================================


async def test_property_no_tenant_leaks(tx_conn, tenant, other_tenant):
    """Random role / entity assignments should never let a tenant-A
    actor read a tenant-B entity."""
    rng = random.Random(123)
    tenant_a_actors = [await insert_actor(tx_conn, tenant) for _ in range(4)]
    tenant_b_actors = [
        await insert_actor(tx_conn, other_tenant) for _ in range(3)
    ]
    # Seed tenant-B entities.
    b_commits = [
        await insert_commitment(
            tx_conn, other_tenant, owner_id=rng.choice(tenant_b_actors),
        )
        for _ in range(5)
    ]
    b_goals = [
        await insert_goal(tx_conn, other_tenant) for _ in range(3)
    ]
    obs_b = await insert_observation(
        tx_conn, other_tenant, rng.choice(tenant_b_actors),
    )
    b_models = [
        await insert_model(
            tx_conn,
            tenant=other_tenant,
            born_from_event_id=obs_b,
            visible_to_subjects=True,
            natural=f"public model {i}",
        )
        for i in range(3)
    ]
    # Give tenant-A actors every role we know about (still no access).
    for a in tenant_a_actors:
        for role in ("admin", "leadership", "finance", "legal"):
            await grant_role(
                a, "tenant", None, role, a,
                conn=tx_conn, tenant_id=tenant,
            )

    for actor in tenant_a_actors:
        for cid in b_commits:
            d = await can_read(
                actor,
                {"kind": "commitment", "id": cid, "tenant_id": other_tenant},
                conn=tx_conn, tenant_id=tenant,
            )
            assert not d.allowed
        for gid in b_goals:
            d = await can_read(
                actor,
                {"kind": "goal", "id": gid, "tenant_id": other_tenant},
                conn=tx_conn, tenant_id=tenant,
            )
            assert not d.allowed
        for mid in b_models:
            d = await can_read(
                actor,
                {
                    "kind": "model",
                    "id": mid,
                    "tenant_id": other_tenant,
                    "visible_to_subjects": True,
                    "scope_actors": [],
                    "scope_entities": [],
                },
                conn=tx_conn, tenant_id=tenant,
            )
            assert not d.allowed


# =====================================================================
# Test 21 — shared_channels gives tenant-wide visibility
# =====================================================================


async def test_shared_channel_grants_observation_visibility(tx_conn, tenant):
    author = await insert_actor(tx_conn, tenant)
    reader = await insert_actor(tx_conn, tenant)
    obs = await insert_observation(
        tx_conn, tenant, author,
        source_channel="slack:public-announcements",
    )
    # Without the registration, reader has no access.
    d = await can_read_by_id(
        reader, "observation", obs, conn=tx_conn, tenant_id=tenant,
    )
    assert not d.allowed

    await register_shared_channel(
        "slack:public-announcements",
        conn=tx_conn, tenant_id=tenant, audience_role="all",
    )
    d = await can_read_by_id(
        reader, "observation", obs, conn=tx_conn, tenant_id=tenant,
    )
    assert d.allowed
    assert d.reason == "observation_shared_channel"


# =====================================================================
# Test 22 — HR channel NEVER leaks through manager chain
# =====================================================================


async def test_hr_channel_blocks_manager_chain(tx_conn, tenant):
    manager = await insert_actor(tx_conn, tenant)
    report = await insert_actor(
        tx_conn, tenant, metadata={"manager_id": str(manager)},
    )
    # A regular (non-HR) observation — manager CAN read.
    normal = await insert_observation(
        tx_conn, tenant, report, source_channel="slack:eng",
    )
    d = await can_read_by_id(
        manager, "observation", normal, conn=tx_conn, tenant_id=tenant,
    )
    assert d.allowed
    assert d.reason == "observation_manager_chain"

    # An HR-channel observation — manager must NOT read.
    hr = await insert_observation(
        tx_conn, tenant, report, source_channel="hr:review",
    )
    d = await can_read_by_id(
        manager, "observation", hr, conn=tx_conn, tenant_id=tenant,
    )
    assert not d.allowed
    # Even if we try to share the HR channel, it's rejected.
    await register_shared_channel(
        "hr:review", conn=tx_conn, tenant_id=tenant, audience_role="all",
    )
    d = await can_read_by_id(
        manager, "observation", hr, conn=tx_conn, tenant_id=tenant,
    )
    assert not d.allowed


# =====================================================================
# Test 23 — manager_chain_of returns correct order
# =====================================================================


async def test_manager_chain_of(tx_conn, tenant):
    ceo = await insert_actor(tx_conn, tenant)
    director = await insert_actor(
        tx_conn, tenant, metadata={"manager_id": str(ceo)},
    )
    manager = await insert_actor(
        tx_conn, tenant, metadata={"manager_id": str(director)},
    )
    ic = await insert_actor(
        tx_conn, tenant, metadata={"manager_id": str(manager)},
    )
    chain = await manager_chain_of(
        ic, conn=tx_conn, tenant_id=tenant,
    )
    assert chain == [manager, director, ceo]
    assert await is_in_manager_chain(
        ic, ceo, conn=tx_conn, tenant_id=tenant,
    )
    assert not await is_in_manager_chain(
        ic, ic, conn=tx_conn, tenant_id=tenant,
    )


# =====================================================================
# Test 24 — idempotent grant + dedup
# =====================================================================


async def test_idempotent_grant(tx_conn, tenant):
    actor = await insert_actor(tx_conn, tenant)
    # Granting the same role twice should produce a single active row.
    await grant_role(
        actor, "tenant", None, "admin", actor,
        conn=tx_conn, tenant_id=tenant,
    )
    await grant_role(
        actor, "tenant", None, "admin", actor,
        conn=tx_conn, tenant_id=tenant,
    )
    roles = await roles_for_actor(actor, conn=tx_conn, tenant_id=tenant)
    admin_rows = [r for r in roles if r["role"] == "admin"]
    assert len(admin_rows) == 1


# =====================================================================
# Helpers
# =====================================================================


async def _cleanup_tenant(conn, tenant: uuid.UUID) -> None:
    """Delete every row scoped to this tenant (test-only). Order
    matters because of FK dependencies."""
    # Truncate in dependency-safe order.
    await conn.execute(
        "DELETE FROM actor_roles WHERE tenant_id = $1", tenant,
    )
    await conn.execute(
        "DELETE FROM access_override_log WHERE tenant_id = $1", tenant,
    )
    await conn.execute(
        "DELETE FROM shared_channels WHERE tenant_id = $1", tenant,
    )
    await conn.execute(
        "DELETE FROM resource_deployments "
        "WHERE resource_id IN (SELECT id FROM resources WHERE tenant_id = $1)",
        tenant,
    )
    await conn.execute(
        "DELETE FROM contributes_to "
        "WHERE goal_id IN (SELECT id FROM goals WHERE tenant_id = $1)",
        tenant,
    )
    await conn.execute(
        "DELETE FROM commitment_contributors "
        "WHERE commitment_id IN (SELECT id FROM commitments WHERE tenant_id = $1)",
        tenant,
    )
    await conn.execute(
        "DELETE FROM models WHERE tenant_id = $1", tenant,
    )
    await conn.execute(
        "DELETE FROM resources WHERE tenant_id = $1", tenant,
    )
    await conn.execute(
        "DELETE FROM commitments WHERE tenant_id = $1", tenant,
    )
    await conn.execute(
        "DELETE FROM goals WHERE tenant_id = $1", tenant,
    )
    await conn.execute(
        "DELETE FROM decisions WHERE tenant_id = $1", tenant,
    )
    await conn.execute(
        "DELETE FROM observations WHERE tenant_id = $1", tenant,
    )
    await conn.execute(
        "DELETE FROM actors WHERE tenant_id = $1", tenant,
    )
    # Refresh matviews so they reflect the cleanup.
    try:
        await refresh_all(conn=conn, concurrently=False)
    except Exception:
        pass
