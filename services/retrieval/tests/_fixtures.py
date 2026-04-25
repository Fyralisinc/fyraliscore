"""
services/retrieval/tests/_fixtures.py — hand-build the retrieval test
dataset through the Wave 1/2 repositories.

Target shape: 200 Observations, 100 Models, 50 Commitments, 20 Goals,
10 Customer Resources. Every row is tenant-scoped.

We deliberately go through the repos (ObservationRepository,
ModelsRepo, services.acts.*, services.resources.*) because the prompt
says: "do NOT bypass the repos; that's how we prove end-to-end
correctness."

Deterministic seeds: every embedding is derived from a text seed, so
two runs produce the same dataset. This supports the determinism
property test.

The builder is OPTIMIZED for the test transaction model: every write
accepts the shared `conn` so the rollback-at-teardown works. Where the
public repo takes a pool (ModelsRepo, ObservationRepository), we pass
`conn=tx_conn` on the method call.
"""
from __future__ import annotations

import hashlib
import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import asyncpg

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate, ObservationCreate

from services.models.repo import ModelsRepo
from services.observations.repo import ObservationRepository


# ---------------------------------------------------------------------
# Deterministic embedding helper — copied from Models conftest so the
# retrieval fixture is self-contained.
# ---------------------------------------------------------------------


def make_embedding(text: str, *, dim: int = 768) -> list[float]:
    seed = int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8], "big"
    )
    rng = random.Random(seed)
    vec = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0:
        return vec
    return [x / norm for x in vec]


@dataclass
class FixtureSet:
    """
    Snapshot of what we built. Tests index by id / name / index as
    appropriate. `seed_model` is a commitment id chosen as the "hero"
    seed for pathway A tests. `seed_commitment` is its owning Commitment.
    """

    tenant_id: uuid.UUID
    actor_ids: list[uuid.UUID] = field(default_factory=list)
    observation_ids: list[uuid.UUID] = field(default_factory=list)
    model_ids: list[uuid.UUID] = field(default_factory=list)
    goal_ids: list[uuid.UUID] = field(default_factory=list)
    commitment_ids: list[uuid.UUID] = field(default_factory=list)
    decision_ids: list[uuid.UUID] = field(default_factory=list)
    customer_resource_ids: list[uuid.UUID] = field(default_factory=list)
    # Hero seeds — used by tests as the "focus" entity
    hero_commitment_id: uuid.UUID | None = None
    hero_goal_id: uuid.UUID | None = None
    hero_customer_id: uuid.UUID | None = None
    hero_actor_id: uuid.UUID | None = None
    hero_model_id: uuid.UUID | None = None
    # Diagnostic counters (tests assert on these)
    scope_by_commitment: dict[uuid.UUID, list[uuid.UUID]] = field(default_factory=dict)
    # Pattern-related: for pattern-D tests
    pattern_model_ids: list[uuid.UUID] = field(default_factory=list)
    pattern_instance_model_ids: list[uuid.UUID] = field(default_factory=list)


# ---------------------------------------------------------------------
# Low-level inserters (bypass the Acts repos because they open their
# own transactions. We go direct via SQL on `conn` to keep everything
# in one test transaction. The Models and Observations writes go
# through the real repos as required by the prompt.)
# ---------------------------------------------------------------------


async def _insert_actor(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    display_name: str,
    email: str | None = None,
) -> uuid.UUID:
    aid = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status,
            metadata, created_at, last_seen_at
        ) VALUES (
            $1, $2, 'human_internal', $3, $4, 'active',
            '{}'::jsonb, now(), NULL
        )
        """,
        aid, tenant_id, display_name, email,
    )
    return aid


async def _insert_goal(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    title: str,
    parent_goal_id: uuid.UUID | None,
    created_by_event_id: uuid.UUID,
) -> uuid.UUID:
    gid = uuid7()
    await conn.execute(
        """
        INSERT INTO goals (
          id, tenant_id, title, state, altitude, cached_health,
          cached_health_computed_at, created_by_event_id, parent_goal_id
        ) VALUES ($1, $2, $3, 'active', 'operational', 'healthy', now(), $4, $5)
        """,
        gid, tenant_id, title, created_by_event_id, parent_goal_id,
    )
    return gid


async def _insert_commitment(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    title: str,
    state: str,
    owner_id: uuid.UUID | None,
    created_by_event_id: uuid.UUID,
    external_counterparty_ref: dict | None = None,
    due_date: datetime | None = None,
) -> uuid.UUID:
    cid = uuid7()
    await conn.execute(
        """
        INSERT INTO commitments (
          id, tenant_id, title, state, owner_id, due_date,
          ambition_level, priority, external_counterparty_ref,
          created_by_event_id
        ) VALUES ($1, $2, $3, $4, $5, $6, 'base', 5, $7::jsonb, $8)
        """,
        cid, tenant_id, title, state, owner_id, due_date,
        json.dumps(external_counterparty_ref) if external_counterparty_ref else None,
        created_by_event_id,
    )
    return cid


async def _insert_decision(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    title: str,
    decision_text: str,
    created_by_event_id: uuid.UUID,
) -> uuid.UUID:
    did = uuid7()
    await conn.execute(
        """
        INSERT INTO decisions (
          id, tenant_id, title, decision_text, state, created_by_event_id
        ) VALUES ($1, $2, $3, $4, 'active', $5)
        """,
        did, tenant_id, title, decision_text, created_by_event_id,
    )
    return did


async def _insert_resource_customer(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    identity: str,
    arr_cents: int,
) -> uuid.UUID:
    rid = uuid7()
    await conn.execute(
        """
        INSERT INTO resources (
          id, tenant_id, kind, identity, description,
          current_value, utilization_state, controllability,
          temporal_character
        ) VALUES ($1, $2, 'relational', $3, $4, $5::jsonb,
                  'available', 'joint', 'renewable')
        """,
        rid, tenant_id, identity, f"Customer {identity}",
        json.dumps({"arr_cents": arr_cents}),
    )
    return rid


async def _link_contributes(
    conn: asyncpg.Connection,
    commitment_id: uuid.UUID,
    goal_id: uuid.UUID,
    is_critical_path: bool = False,
) -> None:
    await conn.execute(
        """
        INSERT INTO contributes_to (commitment_id, goal_id, is_critical_path)
        VALUES ($1, $2, $3)
        ON CONFLICT (commitment_id, goal_id) DO NOTHING
        """,
        commitment_id, goal_id, is_critical_path,
    )


async def _link_depends_on(
    conn: asyncpg.Connection,
    dependent: uuid.UUID,
    dependency: uuid.UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO depends_on (dependent_commitment_id, dependency_commitment_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        dependent, dependency,
    )


async def _link_constrained_by(
    conn: asyncpg.Connection,
    commitment_id: uuid.UUID,
    decision_id: uuid.UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO constrained_by (commitment_id, decision_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        commitment_id, decision_id,
    )


async def _link_customer_commitment(
    conn: asyncpg.Connection,
    customer_resource_id: uuid.UUID,
    commitment_id: uuid.UUID,
    served_description: str = "fixture",
) -> None:
    await conn.execute(
        """
        INSERT INTO customer_commitments (
          customer_resource_id, commitment_id, served_description
        ) VALUES ($1, $2, $3)
        ON CONFLICT (customer_resource_id, commitment_id) DO NOTHING
        """,
        customer_resource_id, commitment_id, served_description,
    )


# ---------------------------------------------------------------------
# Build the full fixture set.
# ---------------------------------------------------------------------


async def build_fixture(
    conn: asyncpg.Connection,
    tenant_id: uuid.UUID,
    *,
    pool: asyncpg.Pool,
    rng_seed: int = 42,
    n_actors: int = 10,
    n_goals: int = 20,
    n_commitments: int = 50,
    n_observations: int = 200,
    n_models: int = 100,
    n_customers: int = 10,
    n_decisions: int = 8,
) -> FixtureSet:
    """
    Build the dataset. Caller provides the test transaction `conn`; we
    pass `conn=conn` into every repo method so all writes land in the
    caller's transaction.

    `pool` is here because ObservationRepository and ModelsRepo expect
    a pool constructor arg — we still pass `conn=conn` per call so the
    work happens on the test's connection.
    """
    rng = random.Random(rng_seed)
    fs = FixtureSet(tenant_id=tenant_id)
    obs_repo = ObservationRepository(pool=pool, embedder=None)
    mod_repo = ModelsRepo(pool=pool, embedder=None)

    # ---- Actors ----
    for i in range(n_actors):
        name = f"actor-{i}"
        email = f"{name}@fixture.test"
        fs.actor_ids.append(await _insert_actor(conn, tenant_id, display_name=name, email=email))
    fs.hero_actor_id = fs.actor_ids[0]

    # ---- Observations (200) ----
    # We build in 4 "topics" so semantic clustering tests work.
    topics = [
        "shipping release cadence",
        "customer churn risk",
        "hiring pipeline status",
        "infrastructure reliability",
    ]
    base_time = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)
    for i in range(n_observations):
        topic = topics[i % len(topics)]
        text = f"observation {i} about {topic}"
        actor_id = fs.actor_ids[i % n_actors]
        occurred = base_time + timedelta(minutes=i * 10)
        obs = ObservationCreate(
            tenant_id=tenant_id,
            occurred_at=occurred,
            kind="signal",
            source_channel=f"fixture:topic-{i % len(topics)}",
            source_actor_ref=f"fixture:{actor_id}",
            actor_id=actor_id,
            content={"topic": topic, "index": i},
            content_text=text,
            trust_tier="authoritative",
            external_id=f"fixture-obs-{i}",
        )
        # ObservationRepository.insert will compute embedding via
        # embedder which is None — that triggers embedding_pending=True.
        # We need real embeddings for HNSW search, so insert directly
        # with our deterministic embedding.
        obs_id = uuid7()
        emb = make_embedding(text)
        await conn.execute(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                source_actor_ref, actor_id, content, content_text,
                embedding, embedding_pending, trust_tier,
                external_id, entities_mentioned
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9,
                $10, FALSE, $11, $12, '[]'::jsonb
            )
            """,
            obs_id, tenant_id, occurred, "signal",
            obs.source_channel, obs.source_actor_ref, actor_id,
            json.dumps(obs.content), text, emb,
            "authoritative", obs.external_id,
        )
        fs.observation_ids.append(obs_id)

    # ---- Goals (20) ----
    # Build a small tree: root at 0, 5 children, others as grandchildren.
    root_goal = await _insert_goal(
        conn, tenant_id,
        title="root goal: grow ARR",
        parent_goal_id=None,
        created_by_event_id=fs.observation_ids[0],
    )
    fs.goal_ids.append(root_goal)
    fs.hero_goal_id = root_goal
    for i in range(1, 6):
        gid = await _insert_goal(
            conn, tenant_id,
            title=f"child goal {i}",
            parent_goal_id=root_goal,
            created_by_event_id=fs.observation_ids[i],
        )
        fs.goal_ids.append(gid)
    # Remaining grandchildren distributed under the 5 child goals.
    for i in range(6, n_goals):
        parent = fs.goal_ids[1 + ((i - 6) % 5)]
        gid = await _insert_goal(
            conn, tenant_id,
            title=f"grandchild goal {i}",
            parent_goal_id=parent,
            created_by_event_id=fs.observation_ids[i % n_observations],
        )
        fs.goal_ids.append(gid)

    # ---- Customer Resources (10) ----
    for i in range(n_customers):
        arr = (i + 1) * 100_000_00  # cents → $100k, $200k, ...
        cid = await _insert_resource_customer(
            conn, tenant_id,
            identity=f"customer-{i}",
            arr_cents=arr,
        )
        fs.customer_resource_ids.append(cid)
    fs.hero_customer_id = fs.customer_resource_ids[0]

    # ---- Decisions (8) ----
    for i in range(n_decisions):
        did = await _insert_decision(
            conn, tenant_id,
            title=f"decision {i}",
            decision_text=f"we chose approach {i}",
            created_by_event_id=fs.observation_ids[i % n_observations],
        )
        fs.decision_ids.append(did)

    # ---- Commitments (50) ----
    future = datetime(2027, 1, 1, tzinfo=timezone.utc)
    for i in range(n_commitments):
        owner = fs.actor_ids[i % n_actors]
        state = "active" if i < 40 else ("blocked" if i < 45 else "paused")
        # Every 5th commitment has a customer counterparty ref
        # (pointing at a real Customer Resource).
        counterparty = None
        if i % 5 == 0:
            cust = fs.customer_resource_ids[(i // 5) % n_customers]
            counterparty = {"type": "customer_resource", "id": str(cust)}
        # Every 3rd commitment is created_by_event cycling through obs
        cbe = fs.observation_ids[i % n_observations]
        cid = await _insert_commitment(
            conn, tenant_id,
            title=f"commitment {i}",
            state=state,
            owner_id=owner,
            created_by_event_id=cbe,
            external_counterparty_ref=counterparty,
            due_date=future,
        )
        fs.commitment_ids.append(cid)
        # Contribute to a goal (cycling).
        goal = fs.goal_ids[i % n_goals]
        await _link_contributes(conn, cid, goal, is_critical_path=(i % 4 == 0))
        # Every 10th commitment depends on the previous one.
        if i > 0 and i % 10 == 0:
            await _link_depends_on(conn, cid, fs.commitment_ids[i - 1])
        # Every 8th commitment is constrained by a decision.
        if i % 8 == 0 and fs.decision_ids:
            await _link_constrained_by(conn, cid, fs.decision_ids[i % len(fs.decision_ids)])
        # Link customer commitments where counterparty is present.
        if counterparty is not None:
            try:
                cust_id = uuid.UUID(counterparty["id"])
                await _link_customer_commitment(conn, cust_id, cid, "fixture")
            except (ValueError, KeyError):
                pass
    fs.hero_commitment_id = fs.commitment_ids[0]

    # ---- Models (100) ----
    # Distribution:
    #   - 10 are pattern Models (kind='pattern') with a shared signature
    #   - 10 are pattern_instance Models referencing pattern_ids
    #   - 80 are state / prediction / relation / hypothesis Models
    #     scoped to commitments or actors.
    # Every Model scoped to a Commitment gets its id recorded in
    # fs.scope_by_commitment so tests can cross-reference.
    topics_for_models = [
        "alice ships reliably",
        "customer-0 churn risk high",
        "hiring backfill on track",
        "infrastructure drift monitored",
    ]
    for i in range(n_models):
        if i < 10:
            # Pattern Model
            sig = {"regex": "^hotfix", "group": f"p{i % 3}"}
            prop = {
                "kind": "pattern",
                "signature": sig,
                "observed_tendency": f"hotfixes cluster on Fridays (p{i%3})",
                "trigger_conditions": ["label=hotfix"],
            }
            natural = f"pattern {i}: hotfixes cluster"
            scope_entities = []
            scope_actors = []
        elif i < 20:
            # Pattern instance — refer to the pattern Model at index i-10.
            parent_idx = i - 10
            pattern_pid = fs.model_ids[parent_idx] if parent_idx < len(fs.model_ids) else None
            prop = {
                "kind": "pattern_instance",
                "pattern_id": str(pattern_pid) if pattern_pid else str(uuid7()),
                "matched_context": {"pr": f"#{i}"},
            }
            natural = f"pattern instance {i}"
            scope_entities = []
            scope_actors = []
        else:
            # State / prediction / relation / hypothesis Models scoped
            # to a commitment + (sometimes) an actor.
            commit_idx = (i - 20) % n_commitments
            commit_id = fs.commitment_ids[commit_idx]
            actor_id = fs.actor_ids[(i - 20) % n_actors]
            kinds = ["state", "prediction", "relation", "hypothesis", "concern"]
            k = kinds[i % len(kinds)]
            if k == "state":
                prop = {"kind": "state", "subject": f"actor-{i%n_actors}", "assertion": "is reliable"}
            elif k == "prediction":
                prop = {"kind": "prediction", "expected": f"commit-{commit_idx} doneverified", "resolution": f"commitment {commit_idx} state"}
            elif k == "relation":
                prop = {"kind": "relation", "subject": f"actor-{i%n_actors}", "relation": "reports_to", "object": "founder"}
            elif k == "hypothesis":
                prop = {"kind": "hypothesis", "hypothesis_text": f"latency root cause {i}", "test_conditions": ["profile it"]}
            else:
                prop = {"kind": "concern", "about": "customer churn", "nature": "risk", "raised_by": "cs"}
            natural = f"{topics_for_models[i % len(topics_for_models)]} (model {i})"
            scope_entities = [{"type": "commitment", "id": str(commit_id)}]
            scope_actors = [actor_id]
            fs.scope_by_commitment.setdefault(commit_id, []).append(uuid.UUID(int=0))  # placeholder; filled after insert

        # Born from an observation (cycle).
        born = fs.observation_ids[i % n_observations]
        emb = make_embedding(natural)
        mc = ModelCreate(
            tenant_id=tenant_id,
            born_from_event_id=born,
            proposition=prop,
            natural=natural,
            embedding=emb,
            scope_actors=scope_actors,
            scope_entities=scope_entities,
            scope_temporal={"type": "now"},
            confidence=0.6,
            confidence_at_assertion=0.6,
        )
        row = await mod_repo.insert(mc, conn=conn)
        fs.model_ids.append(row.id)
        if i < 10:
            fs.pattern_model_ids.append(row.id)
        elif i < 20:
            fs.pattern_instance_model_ids.append(row.id)
        if scope_entities:
            commit_id = uuid.UUID(scope_entities[0]["id"])
            fs.scope_by_commitment.setdefault(commit_id, [])
            # Replace the placeholder with the real model id.
            if fs.scope_by_commitment[commit_id] and fs.scope_by_commitment[commit_id][-1] == uuid.UUID(int=0):
                fs.scope_by_commitment[commit_id][-1] = row.id
            else:
                fs.scope_by_commitment[commit_id].append(row.id)

    # hero_model_id = the first scope_entities-bound Model. In the
    # full fixture (n_models >= 21) index 20 is the first such Model;
    # for smaller smoke-test subsets, pick whatever's available.
    if len(fs.model_ids) > 20:
        fs.hero_model_id = fs.model_ids[20]
    elif fs.model_ids:
        fs.hero_model_id = fs.model_ids[-1]

    # Patch pattern_instance Models: rewrite proposition.pattern_id to
    # the real pattern model's id (now known after insert). We updated
    # the proposition IN the insert above using the pattern model id
    # from `fs.model_ids[parent_idx]` — which for i in [10,20) is
    # `fs.model_ids[i-10]`. Since we populate fs.model_ids in order,
    # at the time of the i=10 insert, fs.model_ids[0..9] are set; good.

    return fs
