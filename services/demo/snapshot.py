"""services/demo/snapshot.py — load + wipe demo tenant snapshots.

Loads a .sql or .sql.zst from `demo/snapshots/<company>-v1.sql.zst`,
swaps the placeholder tenant id (00000000-0000-0000-0000-000000000000)
for the real tenant_id, and executes it inside the caller's
transaction. Returns the CEO actor's id so the session orchestrator
can mint a token bound to that actor.
"""
from __future__ import annotations

import gzip
from pathlib import Path
from uuid import UUID

import asyncpg


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

    Reads the SQL file at `<repo>/<snapshot_uri>` and executes it.
    Raises FileNotFoundError if the snapshot is missing — every
    registered demo company is expected to ship a snapshot file.

    `preserve_ceo_actor_id` is accepted for API compatibility with the
    reset flow, but the snapshot path doesn't need it; the CEO actor id
    is recovered by querying the loaded tenant rows.
    """
    del company_id, preserve_ceo_actor_id
    sql_path = REPO_ROOT / snapshot_uri
    if not sql_path.exists():
        raise FileNotFoundError(
            f"demo snapshot not found at {sql_path}; ensure the snapshot "
            f"file is committed under demo/snapshots/"
        )
    sql = _read_snapshot_file(sql_path)
    sql = _remap_snapshot_uuids(sql, tenant_id)
    await conn.execute(sql)
    return await _find_ceo_actor(conn, tenant_id=tenant_id)


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
        "resource_deployments",
        "constrained_by",
        "depends_on",
        "contributes_to",
        "commitment_contributors",
        # Order matters across these FKs:
        #   - commitments.last_confidence_basis → models.id
        #   - commitments.created_by_event_id   → observations.id
        #   - models.born_from_event_id         → observations.id
        # so commitments must be wiped before models, and models before
        # observations. (Earlier order put models first which broke
        # reset whenever a Commitment had a last_confidence_basis set
        # by a Think run that picked up an augmented Model.)
        "commitments",
        "models",
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
        "resource_deployments":
            "DELETE FROM resource_deployments WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)",
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

