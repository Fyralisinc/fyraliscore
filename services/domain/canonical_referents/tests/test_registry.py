from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.errors import InvariantViolation
from lib.shared.tenant_context import bind_tenant
from services.domain.canonical_referents.service import (
    CanonicalReferentRegistryService,
)
from services.domain.canonical_referents.types import (
    CanonicalReferentReplacementCommand,
    CanonicalReferentVersionRef,
)


pytestmark = pytest.mark.integration

TENANT_A = UUID("11111111-1111-1111-1111-111111111111")
TENANT_B = UUID("22222222-2222-2222-2222-222222222222")
JAN_10 = datetime(2026, 1, 10, 12, 0, tzinfo=timezone.utc)
JAN_20 = datetime(2026, 1, 20, 12, 0, tzinfo=timezone.utc)


def _ref(name: str, *, version: int = 1) -> CanonicalReferentVersionRef:
    return CanonicalReferentVersionRef(
        type="resource",
        id=f"resource:{name}",
        version=version,
    )


def _command(
    *,
    tenant_id: UUID = TENANT_A,
    operation_ref: str,
    predecessor: CanonicalReferentVersionRef,
    successor: CanonicalReferentVersionRef,
    effective_at: datetime,
) -> CanonicalReferentReplacementCommand:
    return CanonicalReferentReplacementCommand(
        tenant_id=tenant_id,
        operation_ref=operation_ref,
        predecessor=predecessor,
        successor=successor,
        expected_predecessor_version=predecessor.version,
        effective_at=effective_at,
        authority_ref="authority:canonical-review:1",
        reason="Evidence establishes a governed canonical successor.",
        evidence_refs=("observation:1", "review:1"),
    )


@pytest.fixture
def registry(fresh_db: asyncpg.Pool) -> CanonicalReferentRegistryService:
    return CanonicalReferentRegistryService(fresh_db)


@pytest.mark.asyncio
async def test_apply_and_replay_are_exact_and_idempotent(
    registry: CanonicalReferentRegistryService,
    fresh_db: asyncpg.Pool,
) -> None:
    predecessor = _ref("northstar-draft", version=2)
    successor = _ref("northstar")
    command = _command(
        operation_ref="replace:northstar",
        predecessor=predecessor,
        successor=successor,
        effective_at=JAN_10,
    )

    applied = await registry.apply_replacement(command)
    replayed = await registry.apply_replacement(command)

    assert applied.applied is True
    assert replayed.applied is False
    assert replayed.transition_id == applied.transition_id
    assert replayed.request_fingerprint == applied.request_fingerprint
    assert replayed.predecessor == predecessor
    assert replayed.successor == successor

    async with fresh_db.acquire() as conn:
        transition_count = await conn.fetchval(
            """
            SELECT count(*)
            FROM canonical_referent_transitions
            WHERE tenant_id=$1
            """,
            TENANT_A,
        )
        members = await conn.fetch(
            """
            SELECT member_role, member_ordinal
            FROM canonical_referent_transition_members
            WHERE tenant_id=$1 AND transition_id=$2
            ORDER BY member_role
            """,
            TENANT_A,
            applied.transition_id,
        )

    assert transition_count == 1
    assert [(row["member_role"], row["member_ordinal"]) for row in members] == [
        ("predecessor", 0),
        ("successor", 0),
    ]


@pytest.mark.asyncio
async def test_operation_ref_conflict_fails_before_lineage_mutation(
    registry: CanonicalReferentRegistryService,
) -> None:
    predecessor = _ref("northstar-draft")
    first = _command(
        operation_ref="replace:conflict",
        predecessor=predecessor,
        successor=_ref("northstar"),
        effective_at=JAN_10,
    )
    conflicting = _command(
        operation_ref=first.operation_ref,
        predecessor=predecessor,
        successor=_ref("other-project"),
        effective_at=JAN_10,
    )
    await registry.apply_replacement(first)

    with pytest.raises(InvariantViolation) as exc:
        await registry.apply_replacement(conflicting)

    assert exc.value.invariant == "CANONICAL_REFERENT_OPERATION_CONFLICT"


@pytest.mark.asyncio
async def test_operation_ref_cannot_cross_reserved_transition_kinds(
    registry: CanonicalReferentRegistryService,
    fresh_db: asyncpg.Pool,
) -> None:
    operation_ref = "transition:reserved-kind"
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO canonical_referent_transitions (
                id, tenant_id, operation_ref, request_fingerprint,
                transition_kind, effective_at, expected_predecessor_version,
                authority_ref, reason, evidence_refs
            ) VALUES (
                $1, $2, $3, $4, 'merge', $5, 1,
                'authority:test', 'reserved future protocol', ARRAY['test:1']
            )
            """,
            uuid4(),
            TENANT_A,
            operation_ref,
            "a" * 64,
            JAN_10,
        )

    with pytest.raises(InvariantViolation) as exc:
        await registry.apply_replacement(
            _command(
                operation_ref=operation_ref,
                predecessor=_ref("kind-root"),
                successor=_ref("kind-successor"),
                effective_at=JAN_10,
            )
        )

    assert (
        exc.value.invariant
        == "CANONICAL_REFERENT_OPERATION_KIND_CONFLICT"
    )


@pytest.mark.asyncio
async def test_stale_predecessor_head_and_temporal_reversal_are_fenced(
    registry: CanonicalReferentRegistryService,
) -> None:
    root = _ref("root")
    middle = _ref("middle")
    await registry.apply_replacement(
        _command(
            operation_ref="replace:root-middle",
            predecessor=root,
            successor=middle,
            effective_at=JAN_10,
        )
    )

    with pytest.raises(InvariantViolation) as stale:
        await registry.apply_replacement(
            _command(
                operation_ref="replace:stale-root",
                predecessor=root,
                successor=_ref("branch"),
                effective_at=JAN_20,
            )
        )
    assert stale.value.invariant == "CANONICAL_REFERENT_STALE_HEAD"

    with pytest.raises(InvariantViolation) as reversed_time:
        await registry.apply_replacement(
            _command(
                operation_ref="replace:middle-too-early",
                predecessor=middle,
                successor=_ref("head"),
                effective_at=JAN_10 - timedelta(seconds=1),
            )
        )
    assert (
        reversed_time.value.invariant
        == "CANONICAL_REFERENT_EFFECTIVE_ORDER"
    )


@pytest.mark.asyncio
async def test_successor_must_be_fresh_to_keep_replacement_one_to_one(
    registry: CanonicalReferentRegistryService,
) -> None:
    first_root = _ref("first-root")
    occupied = _ref("occupied")
    await registry.apply_replacement(
        _command(
            operation_ref="replace:first-occupied",
            predecessor=first_root,
            successor=occupied,
            effective_at=JAN_10,
        )
    )

    with pytest.raises(InvariantViolation) as exc:
        await registry.apply_replacement(
            _command(
                operation_ref="replace:second-occupied",
                predecessor=_ref("second-root"),
                successor=occupied,
                effective_at=JAN_20,
            )
        )

    assert exc.value.invariant == "CANONICAL_REFERENT_SUCCESSOR_NOT_FRESH"


@pytest.mark.asyncio
async def test_current_adjacency_and_bitemporal_lineage_reads(
    registry: CanonicalReferentRegistryService,
) -> None:
    root = _ref("root")
    middle = _ref("middle")
    head = _ref("head")
    first = await registry.apply_replacement(
        _command(
            operation_ref="replace:root-middle",
            predecessor=root,
            successor=middle,
            effective_at=JAN_10,
        )
    )
    await asyncio.sleep(0.01)
    second = await registry.apply_replacement(
        _command(
            operation_ref="replace:middle-head",
            predecessor=middle,
            successor=head,
            effective_at=JAN_20,
        )
    )

    assert await registry.current_successor(
        tenant_id=TENANT_A,
        referent=root,
    ) == middle
    assert await registry.current_predecessor(
        tenant_id=TENANT_A,
        referent=head,
    ) == middle

    before_second_effective = await registry.lineage_at(
        tenant_id=TENANT_A,
        referent=middle,
        valid_at=JAN_20 - timedelta(seconds=1),
        known_at=second.transaction_at,
    )
    before_second_known = await registry.lineage_at(
        tenant_id=TENANT_A,
        referent=middle,
        valid_at=JAN_20 + timedelta(seconds=1),
        known_at=first.transaction_at,
    )
    fully_visible = await registry.lineage_at(
        tenant_id=TENANT_A,
        referent=middle,
        valid_at=JAN_20 + timedelta(seconds=1),
        known_at=second.transaction_at,
    )

    assert before_second_effective.members == (root, middle)
    assert before_second_known.members == (root, middle)
    assert fully_visible.members == (root, middle, head)
    assert fully_visible.root == root
    assert fully_visible.head == head


@pytest.mark.asyncio
async def test_registry_reads_and_heads_are_tenant_isolated(
    registry: CanonicalReferentRegistryService,
) -> None:
    predecessor = _ref("shared-native-key")
    tenant_a_successor = _ref("tenant-a")
    tenant_b_successor = _ref("tenant-b")
    await registry.apply_replacement(
        _command(
            tenant_id=TENANT_A,
            operation_ref="replace:shared",
            predecessor=predecessor,
            successor=tenant_a_successor,
            effective_at=JAN_10,
        )
    )
    await registry.apply_replacement(
        _command(
            tenant_id=TENANT_B,
            operation_ref="replace:shared",
            predecessor=predecessor,
            successor=tenant_b_successor,
            effective_at=JAN_10,
        )
    )

    assert await registry.current_successor(
        tenant_id=TENANT_A,
        referent=predecessor,
    ) == tenant_a_successor
    assert await registry.current_successor(
        tenant_id=TENANT_B,
        referent=predecessor,
    ) == tenant_b_successor


@pytest.mark.asyncio
async def test_rls_hides_other_tenant_lineage_even_with_a_cross_tenant_key(
    db_pool: asyncpg.Pool,
    rls_app_pool: asyncpg.Pool,
) -> None:
    assert db_pool is not None  # Ensures schema migration precedes RLS setup.
    predecessor = _ref("rls-shared-key")
    tenant_a_successor = _ref("rls-tenant-a")
    tenant_b_successor = _ref("rls-tenant-b")
    rls_registry = CanonicalReferentRegistryService(rls_app_pool)
    async with rls_app_pool.acquire() as conn:
        transaction = conn.transaction()
        await transaction.start()
        try:
            await conn.execute(
                """
                INSERT INTO tenants (id, name, is_demo)
                VALUES ($1, 'referent-rls-a', FALSE),
                       ($2, 'referent-rls-b', FALSE)
                ON CONFLICT (id) DO NOTHING
                """,
                TENANT_A,
                TENANT_B,
            )
            await rls_registry.apply_replacement(
                _command(
                    tenant_id=TENANT_A,
                    operation_ref="replace:rls-a",
                    predecessor=predecessor,
                    successor=tenant_a_successor,
                    effective_at=JAN_10,
                ),
                conn=conn,
            )
            await rls_registry.apply_replacement(
                _command(
                    tenant_id=TENANT_B,
                    operation_ref="replace:rls-b",
                    predecessor=predecessor,
                    successor=tenant_b_successor,
                    effective_at=JAN_10,
                ),
                conn=conn,
            )
            async with bind_tenant(conn, TENANT_A) as context:
                assert await rls_registry.current_successor(
                    tenant_id=TENANT_A,
                    referent=predecessor,
                    conn=context.conn,
                ) == tenant_a_successor
                assert (
                    await rls_registry.current_successor(
                        tenant_id=TENANT_B,
                        referent=predecessor,
                        conn=context.conn,
                    )
                    is None
                )
        finally:
            await transaction.rollback()


@pytest.mark.asyncio
async def test_concurrent_replacements_serialize_on_the_predecessor_head(
    registry: CanonicalReferentRegistryService,
) -> None:
    predecessor = _ref("contended-root")
    commands = (
        _command(
            operation_ref="replace:contended-a",
            predecessor=predecessor,
            successor=_ref("winner-a"),
            effective_at=JAN_10,
        ),
        _command(
            operation_ref="replace:contended-b",
            predecessor=predecessor,
            successor=_ref("winner-b"),
            effective_at=JAN_10,
        ),
    )

    outcomes = await asyncio.gather(
        *(registry.apply_replacement(command) for command in commands),
        return_exceptions=True,
    )

    successes = [outcome for outcome in outcomes if not isinstance(outcome, Exception)]
    failures = [outcome for outcome in outcomes if isinstance(outcome, Exception)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], InvariantViolation)
    assert failures[0].invariant == "CANONICAL_REFERENT_STALE_HEAD"
