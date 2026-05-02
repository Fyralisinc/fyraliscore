"""services/demo/snapshot.py — load + wipe demo tenant snapshots.

Two paths:

  * SQL snapshot file (Session 2 produces these): plain .sql or .sql.zst
    in `demo/snapshots/<company>-v1.sql.zst`. We read it, swap the
    placeholder tenant id (00000000-0000-0000-0000-000000000000) with
    the real tenant_id, and execute it inside the caller's transaction.

  * Synthetic fallback: when the SQL file is absent (which is the case
    until Session 2 runs full LLM generation), we materialize a small,
    deterministic in-process snapshot — enough actors, goals,
    commitments, customers, and recommendations that the action list
    surfaces something meaningful. The synthetic snapshot is not as
    rich as the LLM-generated one but is sufficient for end-to-end
    smoke and for demos that don't require the full company depth.

Both paths return the CEO actor's id so the session orchestrator can
mint a token bound to that actor.
"""
from __future__ import annotations

import gzip
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


REPO_ROOT = Path(__file__).resolve().parents[2]
PLACEHOLDER_TENANT_ID = "00000000-0000-0000-0000-000000000000"

# UUID detection — case-insensitive, requires the canonical 8-4-4-4-12
# hex form. Used by load_snapshot to rewrite deterministic snapshot
# UUIDs to per-tenant unique UUIDs at load time, so the same snapshot
# can be loaded into N tenants without PRIMARY KEY collisions.
import re
_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
import uuid as _uuid_mod


def _remap_snapshot_uuids(sql: str, tenant_id: UUID) -> str:
    """Rewrite every entity UUID in the snapshot SQL to a fresh
    per-tenant UUID — so two demo sessions for the same company load
    cleanly without PRIMARY KEY collisions on `actors.id` etc.

    Strategy: collect every UUID in the text, build a mapping
    `original → uuid5(tenant_id, original)`, then do a single
    text substitution. The PLACEHOLDER_TENANT_ID is excluded — that
    one is replaced with the real tenant id, not remapped.
    """
    # Use the tenant id itself as the uuid5 namespace seed.
    ns = _uuid_mod.UUID(str(tenant_id))
    seen: dict[str, str] = {PLACEHOLDER_TENANT_ID: str(tenant_id)}
    for match in _UUID_RE.findall(sql):
        if match in seen:
            continue
        seen[match] = str(_uuid_mod.uuid5(ns, match))

    # Substitute longest-first to avoid prefix shadowing — but UUIDs
    # are fixed length, so no risk: a single-pass dict-driven
    # replacement is fine.
    def _sub(m: re.Match) -> str:
        return seen.get(m.group(0), m.group(0))

    return _UUID_RE.sub(_sub, sql)


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


async def load_snapshot(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    snapshot_uri: str,
    company_id: str,
    preserve_ceo_actor_id: UUID | None = None,
) -> UUID:
    """Load `snapshot_uri` into `tenant_id`. Returns the CEO actor id.

    Resolution: try the SQL file at `<repo>/<snapshot_uri>` first; if
    absent, materialize the synthetic fallback for `company_id`.

    `preserve_ceo_actor_id` is set on reset to keep the existing auth
    token valid across resets.
    """
    sql_path = REPO_ROOT / snapshot_uri
    if sql_path.exists():
        sql = _read_snapshot_file(sql_path)
        sql = _remap_snapshot_uuids(sql, tenant_id)
        await conn.execute(sql)
        return await _find_ceo_actor(
            conn, tenant_id=tenant_id,
        )

    return await _materialize_synthetic(
        conn, tenant_id=tenant_id, company_id=company_id,
        preserve_ceo_actor_id=preserve_ceo_actor_id,
    )


async def _find_ceo_actor(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> UUID:
    """After loading a SQL snapshot, find the actor flagged as CEO via
    metadata.is_ceo (set by sql_emit). Falls back to the first
    'founder' role, then to any actor in the tenant."""
    row = await conn.fetchrow(
        """
        SELECT id FROM actors
        WHERE tenant_id = $1 AND metadata->>'is_ceo' = 'true'
        LIMIT 1
        """,
        tenant_id,
    )
    if row is not None:
        return row["id"]
    row = await conn.fetchrow(
        """
        SELECT id FROM actors
        WHERE tenant_id = $1 AND metadata->>'role' = 'founder'
        LIMIT 1
        """,
        tenant_id,
    )
    if row is not None:
        return row["id"]
    row = await conn.fetchrow(
        "SELECT id FROM actors WHERE tenant_id = $1 LIMIT 1",
        tenant_id,
    )
    if row is None:
        raise RuntimeError(
            f"snapshot loaded for tenant {tenant_id} but no actors found"
        )
    return row["id"]


async def wipe_tenant(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    preserve_actor_ids: list[UUID] | None = None,
) -> None:
    """Delete every row tied to `tenant_id` across all demo-relevant
    tables. Order respects FK direction. Used by reset.

    `preserve_actor_ids`: when provided, the listed actors are kept
    intact along with their actor_sessions rows, so an open auth
    token (e.g., the demo CEO's tab) survives the wipe. The reload
    step's ON CONFLICT DO NOTHING then skips re-inserting them.
    """
    preserve_actor_ids = preserve_actor_ids or []
    tables = [
        "demo_session_costs",       # FK on demo_sessions; sessions kept
        # Edge tables first — they FK into goals/commitments/decisions/resources.
        "customer_commitments",
        "constrained_by",
        "depends_on",
        "contributes_to",
        "commitment_contributors",
        # Models reference observations; observations reference models'
        # born_from_event. Order matters.
        "models",
        "commitments",
        "goals",
        "decisions",
        "resource_transactions",
        "resources",
        "observations",
        "actor_identity_mappings",
        "actors",
        # actor_sessions deliberately omitted: the CEO's auth token must
        # survive a reset so the user stays logged in to the same tab.
    ]
    # Per-table delete strategy. Edge tables that lack a tenant_id
    # column are scrubbed via a join to whatever parent table they
    # reference — different schemas use different column names.
    # The actor-side joins also exclude `preserve_actor_ids` so we
    # never strand a live auth session.
    preserved = list(preserve_actor_ids)
    edge_join_sql: dict[str, str] = {
        "contributes_to":
            "DELETE FROM contributes_to WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)",
        "depends_on":
            "DELETE FROM depends_on WHERE dependent_commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)",
        "constrained_by":
            "DELETE FROM constrained_by WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)",
        "commitment_contributors":
            "DELETE FROM commitment_contributors WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)",
        "actor_identity_mappings":
            "DELETE FROM actor_identity_mappings WHERE actor_id IN "
            "(SELECT id FROM actors WHERE tenant_id = $1 "
            "AND id <> ALL($2::uuid[]))",
    }
    for tbl in tables:
        exists = await conn.fetchval(
            "SELECT to_regclass($1)", f"public.{tbl}"
        )
        if exists is None:
            continue
        col = await conn.fetchval(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
              AND column_name = 'tenant_id'
            """,
            tbl,
        )
        if col is None:
            sql = edge_join_sql.get(tbl)
            if sql is None:
                continue
            if "$2::uuid[]" in sql:
                await conn.execute(sql, tenant_id, preserved)
            else:
                await conn.execute(sql, tenant_id)
            continue
        if tbl == "actors" and preserved:
            # Keep the CEO (and any other preserved actors) so their
            # actor_sessions FK survives the wipe.
            await conn.execute(
                "DELETE FROM actors WHERE tenant_id = $1 "
                "AND id <> ALL($2::uuid[])",
                tenant_id, preserved,
            )
        else:
            await conn.execute(
                f"DELETE FROM {tbl} WHERE tenant_id = $1",
                tenant_id,
            )


# ---------------------------------------------------------------------
# Snapshot file I/O
# ---------------------------------------------------------------------


def _read_snapshot_file(path: Path) -> str:
    if str(path).endswith(".zst"):
        try:
            import zstandard as zstd
        except ImportError as e:
            raise RuntimeError(
                "zstandard package required to read .zst snapshots; "
                "pip install zstandard"
            ) from e
        with open(path, "rb") as fh:
            data = zstd.ZstdDecompressor().decompress(fh.read())
        return data.decode("utf-8")
    if str(path).endswith(".gz"):
        with gzip.open(path, "rt") as fh:
            return fh.read()
    return path.read_text()


# ---------------------------------------------------------------------
# Synthetic fallback — small, deterministic, sufficient for demos
# ---------------------------------------------------------------------


# Per-company shape parameters. Headcount + customer counts per spec.
_COMPANY_SHAPE: dict[str, dict[str, Any]] = {
    "truss": {
        "ceo_name": "Maya Patel", "ceo_email": "maya@truss.dev",
        "actor_count": 40, "customer_count": 35,
        "company_name": "Truss",
        "tagline": "Series A founder at full cognitive load",
        "recommendations": [
            ("Engineering capacity hitting saturation — pause new commitments",
             "capacity", 95000.0),
            ("3 design partners requested SSO in past 60 days — $280K ARR exposure",
             "customer_pressure", 280000.0),
            ("Lead engineer Sarah on incident rotation 4 of 6 weeks — burnout risk",
             "personnel", 50000.0),
            ("API redesign commitment predates 3 customer requests for stable v1 — re-scope",
             "decision_revisit", 120000.0),
            ("Roadmap has 8 active workstreams; 3 lack customer demand signal",
             "strategic", 200000.0),
            ("3 weeks since founder-VP Eng sync; 2 open roles blocking critical path",
             "founder_context", 75000.0),
        ],
    },
    "northwind": {
        "ceo_name": "Jordan Reyes", "ceo_email": "jordan@northwind.io",
        "actor_count": 60, "customer_count": 50,
        "company_name": "Northwind Software",
        "tagline": "Series B, healthy growth, normal Tuesday",
        "recommendations": [
            ("Engineering at 91% utilization — reallocate before Q3 push",
             "capacity", 60000.0),
            ("Postgres-only architecture decision (14 mo old) — conditions changed",
             "decision_revisit", 90000.0),
            ("Manager has gone 6 weeks without 1:1s with direct report — attention",
             "personnel", 30000.0),
            ("3 customers requested SAML SSO in past 60 days — $410K ARR",
             "customer_pressure", 410000.0),
            ("Acme Corp commitment showing slip risk — mid-priority, watch",
             "slip_warning", 80000.0),
            ("Pipeline lean on enterprise — $2M Series B target needs deeper top-of-funnel",
             "strategic", 1500000.0),
        ],
    },
    "meridian": {
        "ceo_name": "Sam Whitfield", "ceo_email": "sam@meridianindustrial.com",
        "actor_count": 80, "customer_count": 70,
        "company_name": "Meridian Industrial",
        "tagline": "Series C, $4.2M ARR customer escalating",
        "recommendations": [
            ("Industrium ($4.2M ARR) — 3 critical-path commitments in slip risk",
             "bridge_alert", 4200000.0),
            ("Cross-team allocation needed for Industrium recovery this week",
             "capacity", 350000.0),
            ("VP Engineering has not been engaged on Industrium issue — needs visibility",
             "personnel", 120000.0),
            ("Original Industrium commitment scope grew 3x — re-scope before retry",
             "decision_revisit", 280000.0),
            ("Past 4 enterprise customers all hit the same scope-growth pattern",
             "pattern", 800000.0),
            ("Q4 pipeline composition shifting to mid-market — strategic check",
             "strategic", 1200000.0),
            ("Renewal risk on Acme Co. ($380K ARR) — health drift over 30 days",
             "customer_pressure", 380000.0),
        ],
    },
}


async def _materialize_synthetic(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    company_id: str,
    preserve_ceo_actor_id: UUID | None = None,
) -> UUID:
    shape = _COMPANY_SHAPE.get(company_id)
    if shape is None:
        raise ValueError(f"no synthetic snapshot for company_id={company_id!r}")

    now = datetime.now(timezone.utc)
    ceo_id = preserve_ceo_actor_id or uuid7()

    # 1) CEO actor (target of every recommendation)
    await conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status, metadata,
            created_at, last_seen_at
        ) VALUES ($1, $2, 'human_internal', $3, $4, 'active',
                  $5::jsonb, $6, $6)
        """,
        ceo_id, tenant_id, shape["ceo_name"], shape["ceo_email"],
        '{"role":"ceo","title":"Founder & CEO"}', now,
    )

    # 2) Surrounding cast — small, deterministic, enough to back recs
    surrounding = await _create_surrounding_actors(
        conn, tenant_id=tenant_id, count=12, company_id=company_id, base_now=now,
    )

    # 3) Seed observation (every Model needs born_from_event_id)
    seed_obs_id = await _insert_seed_observation(
        conn, tenant_id=tenant_id, actor_id=surrounding[0], now=now,
        company_id=company_id,
    )

    # 4) A handful of customer Resources
    customer_ids = await _create_customer_resources(
        conn, tenant_id=tenant_id, count=min(5, shape["customer_count"]),
        company_id=company_id, seed_obs_id=seed_obs_id, now=now,
    )

    # 5) A backbone goal so recommendations have a real target_act_ref.
    backbone_goal_id = await _create_backbone_goal(
        conn, tenant_id=tenant_id, owner_id=ceo_id,
        seed_obs_id=seed_obs_id, company_name=shape["company_name"],
        now=now,
    )

    # 6) Recommendations — the substance the action list surfaces.
    # Round-robin target_act_ref over backbone_goal + customer resources
    # so the action-list ranker's denormalization step finds real
    # entities for every card.
    targets = [("goal", backbone_goal_id)] + [
        ("resource", cid) for cid in customer_ids
    ]
    for i, (proposition_text, kind_label, impact_usd) in enumerate(shape["recommendations"]):
        ref_type, ref_id = targets[i % len(targets)]
        await _insert_recommendation(
            conn,
            tenant_id=tenant_id,
            target_actor_id=ceo_id,
            seed_obs_id=seed_obs_id,
            proposition_text=proposition_text,
            kind_label=kind_label,
            impact_usd=impact_usd,
            target_ref_type=ref_type,
            target_ref_id=ref_id,
            now=now,
        )

    return ceo_id


async def _create_backbone_goal(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    owner_id: UUID,
    seed_obs_id: UUID,
    company_name: str,
    now: datetime,
) -> UUID:
    gid = uuid7()
    await conn.execute(
        """
        INSERT INTO goals (
            id, tenant_id, title, description, state, altitude,
            cached_health, cached_health_computed_at,
            created_at, last_state_change_at, created_by_event_id
        ) VALUES (
            $1, $2, $3, $4, 'active', 'strategic',
            'healthy', $5,
            $5, $5, $6
        )
        """,
        gid, tenant_id,
        f"{company_name} — operating cadence",
        f"Backbone goal for the {company_name} demo. Recommendations "
        f"hang off this goal so they have a concrete target_act_ref.",
        now, seed_obs_id,
    )
    return gid


async def _create_surrounding_actors(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    count: int,
    company_id: str,
    base_now: datetime,
) -> list[UUID]:
    names = [
        ("Sarah Chen", "sarah@example.com", "engineer"),
        ("Marcus Lee", "marcus@example.com", "engineer"),
        ("Priya Shah", "priya@example.com", "pm"),
        ("Diego Rivera", "diego@example.com", "sales"),
        ("Riley Kim", "riley@example.com", "sales"),
        ("Avery Nakamura", "avery@example.com", "cs"),
        ("Tom Bishop", "tom@example.com", "vp_eng"),
        ("Grace Liu", "grace@example.com", "design"),
        ("Imani Black", "imani@example.com", "ops"),
        ("Jules Park", "jules@example.com", "founder"),
        ("Noor Hassan", "noor@example.com", "marketing"),
        ("Theo Schmidt", "theo@example.com", "engineer"),
    ]
    ids: list[UUID] = []
    for name, email, role in names[:count]:
        aid = uuid7()
        await conn.execute(
            """
            INSERT INTO actors (
                id, tenant_id, type, display_name, email, status,
                metadata, created_at, last_seen_at
            ) VALUES ($1, $2, 'human_internal', $3, $4, 'active',
                      $5::jsonb, $6, $6)
            """,
            aid, tenant_id, name, email,
            f'{{"role":"{role}"}}', base_now,
        )
        ids.append(aid)
    return ids


async def _insert_seed_observation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    now: datetime,
    company_id: str,
) -> UUID:
    """Synthetic seed observation. Leaves embedding NULL —
    `embedding_pending=TRUE` lets the embedder backfill on demand and
    sidesteps the asyncpg vector-codec registration that the seed-only
    path doesn't otherwise need."""
    obs_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, ingested_at, kind, source_channel,
            source_actor_ref, actor_id, content, content_text,
            embedding, embedding_pending,
            trust_tier, external_id, entities_mentioned, sequence_num
        ) VALUES (
            $1, $2, $3, $3, 'signal', 'system:demo_seed',
            $4, $5, $6::jsonb, $7,
            NULL, TRUE,
            'authoritative', $8, '[]'::jsonb,
            (SELECT COALESCE(MAX(sequence_num), 0) + 1 FROM observations
             WHERE tenant_id = $2)
        )
        """,
        obs_id, tenant_id, now,
        actor_id.hex[:12], actor_id,
        '{"event":"demo_seed"}',
        f"Demo seed for {company_id}",
        f"demo_seed_{obs_id}",
    )
    return obs_id


async def _create_customer_resources(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    count: int,
    company_id: str,
    seed_obs_id: UUID,
    now: datetime,
) -> list[UUID]:
    customer_pool = {
        "truss": [
            ("Linear", 65000.0), ("Vercel", 88000.0), ("Replit", 42000.0),
            ("Cursor Labs", 110000.0), ("Modal", 71000.0),
        ],
        "northwind": [
            ("Acme Corp", 240000.0), ("Wayfair", 380000.0),
            ("Drift", 195000.0), ("Pendo", 220000.0), ("Notion", 410000.0),
        ],
        "meridian": [
            ("Industrium Corp", 4200000.0), ("Acme Co.", 380000.0),
            ("Globex Manufacturing", 920000.0), ("Sirius Logistics", 1100000.0),
            ("Helios Heavy Industries", 1850000.0),
        ],
    }
    customers = customer_pool.get(company_id, [])[:count]
    ids: list[UUID] = []
    for name, arr in customers:
        rid = uuid7()
        await conn.execute(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, description,
                current_value, utilization_state, controllability,
                temporal_character, metadata, created_at,
                last_updated_at, last_updated_by_event_id
            ) VALUES (
                $1, $2, 'relational', $3, $4,
                $5::jsonb, 'deployed', 'owned',
                'time_limited', $6::jsonb, $7, $7, $8
            )
            """,
            rid, tenant_id, f"customer:{name.lower().replace(' ', '_')}",
            f"{name} — paying customer (synthetic demo data)",
            f'{{"arr_usd":{arr}}}',
            '{"segment":"demo","source":"synthetic_snapshot"}',
            now, seed_obs_id,
        )
        ids.append(rid)
    return ids


async def _insert_recommendation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    target_actor_id: UUID,
    seed_obs_id: UUID,
    proposition_text: str,
    kind_label: str,
    impact_usd: float,
    target_ref_type: str,
    target_ref_id: UUID,
    now: datetime,
) -> UUID:
    """Insert a single recommendation Model. Uses a synthetic vector
    of zeros so the NOT NULL embedding constraint is satisfied without
    registering the pgvector codec on the connection."""
    mid = uuid7()
    op = "update" if target_ref_type == "resource" else "transition"
    payload: dict[str, Any] = {"description": proposition_text}
    if op == "transition":
        payload["new_state"] = "active"
    proposition = {
        "kind": "recommendation",
        "natural": proposition_text,
        "target_actor_id": str(target_actor_id),
        "target_act_ref": {
            "type": target_ref_type,
            "id": str(target_ref_id),
        },
        "proposed_change": {
            "operation": op,
            "payload": payload,
        },
        "expected_impact": impact_usd,
        "qualitative_impact": kind_label,
        "supporting_observation_ids": [str(seed_obs_id)],
        "supporting_model_ids": [],
    }
    import json
    embedding_literal = "[" + ",".join("0" for _ in range(768)) + "]"
    await conn.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id, proposition, "natural",
            embedding, scope_temporal, confidence, activation,
            confidence_at_assertion, status, created_at,
            visible_to_subjects
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5,
            $6::vector, $7::jsonb, $8, 1.0,
            $8, 'active', $9, TRUE
        )
        """,
        mid, tenant_id, seed_obs_id, json.dumps(proposition), proposition_text,
        embedding_literal, '{"window":"current"}',
        0.78, now,
    )
    return mid


__all__ = ["load_snapshot", "wipe_tenant", "REPO_ROOT"]
