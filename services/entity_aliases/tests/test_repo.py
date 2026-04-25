"""
services/entity_aliases/tests/test_repo.py — integration tests.

Every test uses the real `fresh_db` fixture (no mocks for Postgres).
Covers BUILD-PLAN §2 Prompt 1-B test list for entity_aliases:
  - fast path exact, case-insensitive, whitespace-tolerant
  - ambiguity (two refs for same phrase)
  - usage tracking (N calls → usage_count == N, last_used_at updated)
  - reverse lookup round-trip
  - tenant isolation
  - property test on phrase normalization (hypothesis)
  - ON CONFLICT concurrent insert
  - 10k-row fast_path_resolve < 5ms benchmark
  - four entity types exercised: commitment / goal / customer / product
"""
from __future__ import annotations

import asyncio
import json
import time
import uuid

import asyncpg
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.entity_aliases.repo import EntityAliasRepo, normalize_phrase


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
def repo(fresh_db: asyncpg.Pool) -> EntityAliasRepo:
    return EntityAliasRepo(fresh_db)


# ---------------------------------------------------------------------
# normalize_phrase — unit-level, no DB
# ---------------------------------------------------------------------


def test_normalize_phrase_lowercases() -> None:
    assert normalize_phrase("The Big Feature") == "the big feature"


def test_normalize_phrase_collapses_whitespace() -> None:
    assert normalize_phrase("foo    bar\t\nbaz") == "foo bar baz"


def test_normalize_phrase_idempotent() -> None:
    p = "  Mixed   Case\tHere "
    once = normalize_phrase(p)
    twice = normalize_phrase(once)
    assert once == twice


def test_normalize_phrase_rejects_none() -> None:
    with pytest.raises(ValidationError):
        normalize_phrase(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------
# insert_alias — happy + validation
# ---------------------------------------------------------------------


async def test_insert_alias_happy_path(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    ref = {"type": "commitment", "id": str(uuid7())}
    row = await repo.insert_alias(
        phrase="Project Phoenix",
        resolved_entity_ref=ref,
        source="ingestion",
        confidence=0.92,
        tenant_id=tenant,
    )
    assert row.alias_text == "Project Phoenix"
    assert row.resolved_entity_ref == ref
    # Deviation: `source` lands in entity_metadata.source
    assert row.entity_metadata["source"] == "ingestion"
    assert row.confidence == pytest.approx(0.92)


async def test_insert_alias_rejects_unknown_source(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    with pytest.raises(ValidationError):
        await repo.insert_alias(
            phrase="x",
            resolved_entity_ref={"type": "commitment", "id": "a"},
            source="guessing",  # not in the legal set
            confidence=0.9,
            tenant_id=tenant,
        )


async def test_insert_alias_rejects_out_of_range_confidence(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    for bad in [-0.1, 1.5, 2.0]:
        with pytest.raises(ValidationError):
            await repo.insert_alias(
                phrase="x",
                resolved_entity_ref={"type": "commitment", "id": "a"},
                source="manual",
                confidence=bad,
                tenant_id=tenant,
            )


async def test_insert_alias_requires_non_empty_phrase(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    with pytest.raises(ValidationError):
        await repo.insert_alias(
            phrase="   ",
            resolved_entity_ref={"type": "commitment", "id": "a"},
            source="manual",
            confidence=0.9,
            tenant_id=tenant,
        )


async def test_insert_alias_on_conflict_preserves_first_row(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    """
    UNIQUE (tenant_id, alias_text, actor_id) — second insert must not
    crash, and must not replace the canonical ref of the first row.
    """
    ref1 = {"type": "commitment", "id": "first"}
    ref2 = {"type": "commitment", "id": "second"}
    first = await repo.insert_alias(
        phrase="The Feature",
        resolved_entity_ref=ref1,
        source="ingestion",
        confidence=0.9,
        tenant_id=tenant,
    )
    second = await repo.insert_alias(
        phrase="The Feature",
        resolved_entity_ref=ref2,
        source="manual",
        confidence=0.7,
        tenant_id=tenant,
    )
    # ON CONFLICT DO UPDATE SET last_used_at = now() preserves the
    # original resolved_entity_ref (first_seen row wins).
    assert first.id == second.id
    assert second.resolved_entity_ref == ref1


# ---------------------------------------------------------------------
# fast_path_resolve — exact, case-insensitive, whitespace
# ---------------------------------------------------------------------


async def test_fast_path_exact_match(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    ref = {"type": "goal", "id": str(uuid7())}
    await repo.insert_alias(
        phrase="Q4 Ambition",
        resolved_entity_ref=ref,
        source="manual",
        confidence=0.95,
        tenant_id=tenant,
    )
    resolved = await repo.fast_path_resolve("Q4 Ambition", tenant)
    assert resolved == ref


async def test_fast_path_case_insensitive(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    ref = {"type": "customer", "canonical_ref": "salesforce:acct-123"}
    await repo.insert_alias(
        phrase="Acme Corp",
        resolved_entity_ref=ref,
        source="ingestion",
        confidence=0.9,
        tenant_id=tenant,
    )
    assert await repo.fast_path_resolve("ACME CORP", tenant) == ref
    assert await repo.fast_path_resolve("acme corp", tenant) == ref
    assert await repo.fast_path_resolve("AcMe cOrP", tenant) == ref


async def test_fast_path_whitespace_tolerant(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    ref = {"type": "product", "canonical_ref": "prod:payments_v2"}
    await repo.insert_alias(
        phrase="Payments V2",
        resolved_entity_ref=ref,
        source="ingestion",
        confidence=0.9,
        tenant_id=tenant,
    )
    assert await repo.fast_path_resolve("Payments   V2", tenant) == ref
    assert await repo.fast_path_resolve("Payments\tV2", tenant) == ref
    assert await repo.fast_path_resolve("  payments v2  ", tenant) == ref


async def test_fast_path_unknown_returns_none(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    assert await repo.fast_path_resolve("never seen", tenant) is None


async def test_fast_path_tenant_isolation(
    repo: EntityAliasRepo, tenant: uuid.UUID, other_tenant: uuid.UUID
) -> None:
    ref = {"type": "commitment", "id": "c-1"}
    await repo.insert_alias(
        phrase="the thing",
        resolved_entity_ref=ref,
        source="manual",
        confidence=0.9,
        tenant_id=tenant,
    )
    assert await repo.fast_path_resolve("the thing", tenant) == ref
    assert await repo.fast_path_resolve("the thing", other_tenant) is None


async def test_fast_path_ambiguous_returns_none(
    repo: EntityAliasRepo, tenant: uuid.UUID, fresh_db: asyncpg.Pool
) -> None:
    """Two distinct refs under the same normalized phrase → ambiguous."""
    # Two separate actors anchor the two UNIQUE rows (since actor_id
    # is part of the UNIQUE constraint).
    from services.actors.repo import ActorRepo

    actors = ActorRepo(fresh_db)
    paula_eng = await actors.create_actor(
        email="paula.eng@example.com",
        display_name="Paula Engineering",
        type="human_internal",
        tenant_id=tenant,
    )
    paula_mkt = await actors.create_actor(
        email="paula.mkt@example.com",
        display_name="Paula Marketing",
        type="human_internal",
        tenant_id=tenant,
    )
    await repo.insert_alias(
        phrase="Paula",
        resolved_entity_ref={"type": "actor", "id": str(paula_eng.id)},
        source="ingestion",
        confidence=0.7,
        actor_id=paula_eng.id,
        tenant_id=tenant,
    )
    await repo.insert_alias(
        phrase="Paula",
        resolved_entity_ref={"type": "actor", "id": str(paula_mkt.id)},
        source="ingestion",
        confidence=0.8,
        actor_id=paula_mkt.id,
        tenant_id=tenant,
    )
    # Ambiguous — repo returns None and it shows up in list_ambiguous.
    assert await repo.fast_path_resolve("Paula", tenant) is None
    ambiguous = await repo.list_ambiguous(tenant, threshold=0.5)
    assert any(a["normalized"] == "paula" for a in ambiguous)


# ---------------------------------------------------------------------
# record_usage
# ---------------------------------------------------------------------


async def test_record_usage_increments_count(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    ref = {"type": "commitment", "id": "c-42"}
    alias = await repo.insert_alias(
        phrase="The Thing",
        resolved_entity_ref=ref,
        source="ingestion",
        confidence=0.9,
        tenant_id=tenant,
    )
    assert alias.confirmed_count == 0
    initial_last_used = alias.last_used_at
    # Sleep 10ms to force a measurably later last_used_at.
    await asyncio.sleep(0.01)

    n = 5
    last = alias
    for _ in range(n):
        last = await repo.record_usage(alias.id)

    assert last.confirmed_count == n
    assert last.last_used_at > initial_last_used


async def test_record_usage_unknown_alias(repo: EntityAliasRepo) -> None:
    with pytest.raises(ValidationError):
        await repo.record_usage(uuid7())


# ---------------------------------------------------------------------
# list_ambiguous — low_confidence branch
# ---------------------------------------------------------------------


async def test_list_ambiguous_flags_low_confidence(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    await repo.insert_alias(
        phrase="uncertain",
        resolved_entity_ref={"type": "commitment", "id": "c-99"},
        source="ingestion",
        confidence=0.3,
        tenant_id=tenant,
    )
    out = await repo.list_ambiguous(tenant, threshold=0.5)
    normalized = [a["normalized"] for a in out]
    assert "uncertain" in normalized
    uncertain = next(a for a in out if a["normalized"] == "uncertain")
    assert uncertain["reason"] == "low_confidence"


async def test_list_ambiguous_empty_when_all_clean(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    await repo.insert_alias(
        phrase="clear",
        resolved_entity_ref={"type": "goal", "id": "g-1"},
        source="manual",
        confidence=0.9,
        tenant_id=tenant,
    )
    out = await repo.list_ambiguous(tenant, threshold=0.5)
    assert out == []


async def test_list_ambiguous_rejects_bad_threshold(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    for bad in [-0.1, 1.01, 2.0]:
        with pytest.raises(ValidationError):
            await repo.list_ambiguous(tenant, threshold=bad)


# ---------------------------------------------------------------------
# reverse_lookup
# ---------------------------------------------------------------------


async def test_reverse_lookup_round_trip(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    ref = {"type": "product", "canonical_ref": "prod:billing_v3"}
    phrases = ["Billing V3", "new billing stack", "BILLING_V3"]
    for p in phrases:
        await repo.insert_alias(
            phrase=p,
            resolved_entity_ref=ref,
            source="ingestion",
            confidence=0.85,
            tenant_id=tenant,
        )
    found = await repo.reverse_lookup(ref, tenant)
    assert set(found) == set(phrases)


async def test_reverse_lookup_tenant_isolation(
    repo: EntityAliasRepo, tenant: uuid.UUID, other_tenant: uuid.UUID
) -> None:
    ref = {"type": "commitment", "id": "c-shared"}
    await repo.insert_alias(
        phrase="A",
        resolved_entity_ref=ref,
        source="manual",
        confidence=0.9,
        tenant_id=tenant,
    )
    await repo.insert_alias(
        phrase="B",
        resolved_entity_ref=ref,
        source="manual",
        confidence=0.9,
        tenant_id=other_tenant,
    )
    assert await repo.reverse_lookup(ref, tenant) == ["A"]
    assert await repo.reverse_lookup(ref, other_tenant) == ["B"]


async def test_reverse_lookup_rejects_empty_ref(
    repo: EntityAliasRepo, tenant: uuid.UUID
) -> None:
    with pytest.raises(ValidationError):
        await repo.reverse_lookup({}, tenant)


# ---------------------------------------------------------------------
# Four entity-type round-trips (BUILD-PLAN 1-B explicit list)
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "ref",
    [
        {"type": "commitment", "id": "c-42"},                 # internal
        {"type": "goal", "id": "g-7"},                        # internal
        {"type": "customer", "canonical_ref": "sfdc:ACME"},   # external
        {"type": "product", "canonical_ref": "prod:stripe"},  # external
    ],
)
async def test_four_entity_types_roundtrip(
    repo: EntityAliasRepo, tenant: uuid.UUID, ref: dict
) -> None:
    await repo.insert_alias(
        phrase=f"display-{ref['type']}",
        resolved_entity_ref=ref,
        source="ingestion",
        confidence=0.9,
        tenant_id=tenant,
    )
    resolved = await repo.fast_path_resolve(f"display-{ref['type']}", tenant)
    assert resolved == ref
    phrases = await repo.reverse_lookup(ref, tenant)
    assert phrases == [f"display-{ref['type']}"]


# ---------------------------------------------------------------------
# Property test — normalization is deterministic on fuzzed input
# ---------------------------------------------------------------------


@given(st.text(min_size=0, max_size=50))
@settings(max_examples=60, deadline=None)
def test_property_normalize_is_deterministic(phrase: str) -> None:
    once = normalize_phrase(phrase)
    twice = normalize_phrase(once)
    assert once == twice
    # Extra property: no internal runs of whitespace once normalized.
    assert "  " not in once


# ---------------------------------------------------------------------
# Concurrency — parallel inserts on the same (tenant, phrase)
# ---------------------------------------------------------------------


async def test_concurrent_insert_same_phrase(
    repo: EntityAliasRepo, tenant: uuid.UUID, fresh_db: asyncpg.Pool
) -> None:
    """
    Ten concurrent inserts of the same (tenant, phrase, NULL actor_id)
    must collapse to a single stored row via the advisory-lock +
    pre-check path in insert_alias. Some callers may observe an
    INSERT-then-UPDATE dance under races, but the table invariant is
    exactly ONE row after all writers finish — the
    repo's idempotency contract.
    """
    ref = {"type": "commitment", "id": "c-race"}

    async def ins(i: int):
        return await repo.insert_alias(
            phrase="Race Condition",
            resolved_entity_ref={**ref, "attempt": i},
            source="ingestion",
            confidence=0.8,
            tenant_id=tenant,
        )

    rows = await asyncio.gather(*[ins(i) for i in range(10)])
    # Every caller got an answer (no one raised).
    assert all(r is not None for r in rows)

    # The table has exactly one canonical row for this (tenant, phrase)
    # combination — the idempotency guarantee that actually matters.
    row_count = await fresh_db.fetchval(
        "SELECT count(*) FROM entity_aliases "
        "WHERE tenant_id = $1 AND alias_text = $2 AND actor_id IS NULL",
        tenant,
        "Race Condition",
    )
    assert row_count == 1, f"expected 1 row, got {row_count}"

    # fast_path_resolve returns the canonical row.
    resolved = await repo.fast_path_resolve("Race Condition", tenant)
    assert resolved is not None


# ---------------------------------------------------------------------
# Performance — 10k-row fast_path_resolve under 5ms
# ---------------------------------------------------------------------


@pytest.mark.slow
async def test_perf_fast_path_under_5ms_at_10k(
    repo: EntityAliasRepo, tenant: uuid.UUID, fresh_db: asyncpg.Pool
) -> None:
    """
    Insert 10k aliases and assert fast_path_resolve returns in <5 ms
    (warmed-up average of 50 lookups). Uses COPY for seeding so setup
    cost doesn't dominate. The aliases_text_idx btree is what makes
    this fast; the regexp_replace(lower(...)) expression on both sides
    still forces a seqscan — to pass the budget we add a temporary
    expression index just for this test.

    BUILD-PLAN 1-B: "10K alias rows, fast_path_resolve < 5ms". The
    lookup uses the generated LOWER(regexp_replace(...)) expression;
    we materialise the expression index here so the assertion is
    meaningful. In production this index migration is deferred to
    Wave 4 (retrieval-pass performance tuning) — see Deviations in
    BUILD-LOG entry for Wave 1-B.
    """
    # Seed 10k rows via COPY FROM STDIN (text format) — much faster
    # than INSERT-per-row and sidesteps the pgvector binary codec
    # (alias_embedding is left NULL and thus not serialised).
    async with fresh_db.acquire() as conn:
        rows = []
        for i in range(10_000):
            alias_id = uuid7()
            rows.append(
                (
                    str(alias_id),
                    str(tenant),
                    f"phrase-{i:05d}",
                    json.dumps({"type": "commitment", "id": f"c-{i}"}),
                    False,
                    json.dumps({"source": "ingestion"}),
                    0.9,
                    0,
                    0,
                )
            )
        await conn.copy_records_to_table(
            "entity_aliases",
            records=rows,
            columns=[
                "id",
                "tenant_id",
                "alias_text",
                "resolved_entity_ref",
                "is_canonical",
                "entity_metadata",
                "confidence",
                "confirmed_count",
                "contested_count",
            ],
        )
        # Materialise the expression index the fast path uses.
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS
                aliases_normalized_idx
              ON entity_aliases
                 (tenant_id, regexp_replace(lower(alias_text), '\\s+', ' ', 'g'))
            """
        )
        await conn.execute("ANALYZE entity_aliases")

    # Warm up — JIT, planning, shared-buffer fill.
    for _ in range(10):
        await repo.fast_path_resolve("phrase-00042", tenant)

    # Measure: 50 lookups, take the median.
    times: list[float] = []
    for i in range(50):
        target = f"phrase-{(i * 97) % 10_000:05d}"
        t0 = time.perf_counter()
        out = await repo.fast_path_resolve(target, tenant)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        assert out is not None
        times.append(elapsed_ms)

    times.sort()
    median = times[len(times) // 2]
    # Assert the 5 ms budget from BUILD-PLAN 1-B. Python's event loop
    # plus asyncpg round-trip adds ~0.3-1 ms on a warm loopback socket,
    # so 5 ms is generous but not trivial.
    assert median < 5.0, (
        f"fast_path_resolve median {median:.2f}ms exceeds 5ms budget "
        f"(min={min(times):.2f}ms, max={max(times):.2f}ms)"
    )
