"""services/code_intel/graph.py — code-graph storage + blast-radius queries.

`CodeGraphRepo` reads/writes the 0063 tables. It takes any tenant-bound
connection (a `TenantContext` or an asyncpg connection with app.current_tenant
already set) — every method just uses execute/fetch/fetchrow/fetchval.

Blast radius = reverse traversal over `code_edges`:
  - file-level via `imports` edges (who imports the changed file?), transitive,
    bounded by max_hops — the reliable backbone.
  - symbol-level via `references` edges (who calls the changed symbol?), 1 hop.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from lib.shared.ids import uuid7


@dataclass
class SnapshotRow:
    id: UUID
    repo_full_name: str
    branch: str
    commit_sha: str
    status: str
    file_count: int
    symbol_count: int
    edge_count: int


class CodeGraphRepo:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    # ---- snapshot lifecycle ----------------------------------------
    async def create_snapshot(
        self,
        *,
        tenant_id: UUID,
        repo_full_name: str,
        branch: str,
        commit_sha: str,
        index_kind: str = "full",
        indexer: str = "python_ast",
        parent_snapshot_id: UUID | None = None,
    ) -> UUID:
        """Create (or return existing) snapshot for (tenant, repo, sha)."""
        existing = await self.conn.fetchrow(
            "SELECT id, status FROM code_snapshots "
            "WHERE tenant_id=$1 AND repo_full_name=$2 AND commit_sha=$3",
            tenant_id, repo_full_name, commit_sha,
        )
        if existing is not None:
            return existing["id"]
        sid = uuid7()
        await self.conn.execute(
            "INSERT INTO code_snapshots "
            "(id, tenant_id, repo_full_name, branch, commit_sha, status, "
            " index_kind, indexer, parent_snapshot_id) "
            "VALUES ($1,$2,$3,$4,$5,'indexing',$6,$7,$8)",
            sid, tenant_id, repo_full_name, branch, commit_sha,
            index_kind, indexer, parent_snapshot_id,
        )
        return sid

    async def mark_ready(
        self, snapshot_id: UUID, *, files: int, symbols: int, edges: int, parse_errors: int
    ) -> None:
        await self.conn.execute(
            "UPDATE code_snapshots SET status='ready', completed_at=now(), "
            "file_count=$2, symbol_count=$3, edge_count=$4, parse_error_files=$5 "
            "WHERE id=$1",
            snapshot_id, files, symbols, edges, parse_errors,
        )

    async def mark_failed(self, snapshot_id: UUID, error: str) -> None:
        await self.conn.execute(
            "UPDATE code_snapshots SET status='failed', completed_at=now(), last_error=$2 "
            "WHERE id=$1",
            snapshot_id, error[:2000],
        )

    async def latest_ready_snapshot(
        self, tenant_id: UUID, repo_full_name: str
    ) -> SnapshotRow | None:
        row = await self.conn.fetchrow(
            "SELECT id, repo_full_name, branch, commit_sha, status, "
            "file_count, symbol_count, edge_count FROM code_snapshots "
            "WHERE tenant_id=$1 AND repo_full_name=$2 AND status='ready' "
            "ORDER BY created_at DESC LIMIT 1",
            tenant_id, repo_full_name,
        )
        return _snap(row)

    async def get_snapshot(self, snapshot_id: UUID) -> SnapshotRow | None:
        row = await self.conn.fetchrow(
            "SELECT id, repo_full_name, branch, commit_sha, status, "
            "file_count, symbol_count, edge_count FROM code_snapshots WHERE id=$1",
            snapshot_id,
        )
        return _snap(row)

    # ---- bulk writes ------------------------------------------------
    async def insert_file(
        self, *, tenant_id: UUID, snapshot_id: UUID, path: str, language: str | None,
        blob_sha: str, size_bytes: int, line_count: int, is_generated: bool = False,
    ) -> UUID:
        fid = uuid7()
        await self.conn.execute(
            "INSERT INTO code_files "
            "(id, tenant_id, snapshot_id, path, language, blob_sha, size_bytes, "
            " line_count, is_generated) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            fid, tenant_id, snapshot_id, path, language, blob_sha,
            size_bytes, line_count, is_generated,
        )
        return fid

    async def insert_symbols(self, rows: list[tuple]) -> None:
        if rows:
            await self.conn.executemany(
                "INSERT INTO code_symbols "
                "(id, tenant_id, snapshot_id, file_id, kind, name, qualified_name, "
                " parent_symbol_id, start_line, end_line, signature, docstring, symbol_hash) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                rows,
            )

    async def insert_edges(self, rows: list[tuple]) -> None:
        if rows:
            await self.conn.executemany(
                "INSERT INTO code_edges "
                "(id, tenant_id, snapshot_id, edge_kind, src_symbol_id, src_file_id, "
                " dst_symbol_id, dst_file_id, dst_unresolved, precision) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                rows,
            )

    async def insert_embeddings_pending(self, rows: list[tuple]) -> None:
        if rows:
            await self.conn.executemany(
                "INSERT INTO code_embeddings "
                "(id, tenant_id, snapshot_id, symbol_id, file_id, chunk_index, "
                " content_text, embedding_pending) VALUES ($1,$2,$3,$4,$5,$6,$7,TRUE) "
                "ON CONFLICT (snapshot_id, symbol_id, chunk_index) DO NOTHING",
                rows,
            )

    # ---- reads for blast radius / lookups --------------------------
    async def file_ids_for_paths(
        self, snapshot_id: UUID, paths: list[str]
    ) -> dict[str, UUID]:
        if not paths:
            return {}
        rows = await self.conn.fetch(
            "SELECT id, path FROM code_files WHERE snapshot_id=$1 AND path = ANY($2::text[])",
            snapshot_id, paths,
        )
        return {r["path"]: r["id"] for r in rows}

    async def blast_radius(
        self, snapshot_id: UUID, changed_paths: list[str], *, max_hops: int = 3
    ) -> dict[str, Any]:
        """Given changed file paths, return the dependency blast radius."""
        path_to_fid = await self.file_ids_for_paths(snapshot_id, changed_paths)
        changed_fids = list(path_to_fid.values())
        result: dict[str, Any] = {
            "changed_files": list(path_to_fid.keys()),
            "unknown_files": [p for p in changed_paths if p not in path_to_fid],
            "dependent_files": [],
            "dependent_symbols": [],
            "changed_symbols": [],
            "max_hops": max_hops,
        }
        if not changed_fids:
            return result

        # changed symbols (symbols defined in changed files)
        sym_rows = await self.conn.fetch(
            "SELECT id, qualified_name, kind FROM code_symbols "
            "WHERE snapshot_id=$1 AND file_id = ANY($2::uuid[]) AND kind <> 'module'",
            snapshot_id, changed_fids,
        )
        changed_sids = [r["id"] for r in sym_rows]
        result["changed_symbols"] = [
            {"qualified_name": r["qualified_name"], "kind": r["kind"]} for r in sym_rows
        ]

        # file-level transitive: who imports the changed files?
        dep_files = await self.conn.fetch(
            """
            WITH RECURSIVE impacted AS (
                SELECT f AS file_id, 0 AS depth
                  FROM unnest($2::uuid[]) AS f
                UNION
                SELECT e.src_file_id, i.depth + 1
                  FROM code_edges e
                  JOIN impacted i ON e.dst_file_id = i.file_id
                 WHERE e.snapshot_id = $1
                   AND e.edge_kind = 'imports'
                   AND e.src_file_id IS NOT NULL
                   AND i.depth < $3
            )
            SELECT cf.path, MIN(impacted.depth) AS depth
              FROM impacted
              JOIN code_files cf ON cf.id = impacted.file_id
             WHERE impacted.depth > 0
             GROUP BY cf.path
             ORDER BY depth, cf.path
            """,
            snapshot_id, changed_fids, max_hops,
        )
        result["dependent_files"] = [
            {"path": r["path"], "hops": r["depth"]} for r in dep_files
        ]

        # symbol-level: who references the changed symbols? (1 hop)
        if changed_sids:
            dep_syms = await self.conn.fetch(
                """
                SELECT DISTINCT s.qualified_name, s.kind, cf.path
                  FROM code_edges e
                  JOIN code_symbols s ON s.id = e.src_symbol_id
                  JOIN code_files cf ON cf.id = s.file_id
                 WHERE e.snapshot_id = $1
                   AND e.edge_kind = 'references'
                   AND e.dst_symbol_id = ANY($2::uuid[])
                 ORDER BY cf.path, s.qualified_name
                 LIMIT 200
                """,
                snapshot_id, changed_sids,
            )
            result["dependent_symbols"] = [
                {"qualified_name": r["qualified_name"], "kind": r["kind"], "path": r["path"]}
                for r in dep_syms
            ]
        return result

    async def search_code(
        self, snapshot_id: UUID, embedding: list[float], *, k: int = 5
    ) -> list[dict[str, Any]]:
        """Semantic code-RAG: nearest symbols to an embedding within a snapshot."""
        # Bind the query vector as text and cast in SQL ($2::text::vector) so the
        # read works whether or not the pgvector codec is registered.
        rows = await self.conn.fetch(
            """
            SELECT s.qualified_name, s.kind, cf.path, s.signature,
                   1 - (ce.embedding <=> $2::text::vector) AS score
              FROM code_embeddings ce
              JOIN code_symbols s ON s.id = ce.symbol_id
              JOIN code_files cf ON cf.id = ce.file_id
             WHERE ce.snapshot_id = $1
               AND ce.embedding_pending = FALSE
               AND ce.embedding IS NOT NULL
             ORDER BY ce.embedding <=> $2::text::vector
             LIMIT $3
            """,
            snapshot_id, _vec(embedding), k,
        )
        return [
            {
                "qualified_name": r["qualified_name"], "kind": r["kind"],
                "path": r["path"], "signature": r["signature"],
                "score": round(float(r["score"]), 4),
            }
            for r in rows
        ]


def _snap(row: Any) -> SnapshotRow | None:
    if row is None:
        return None
    return SnapshotRow(
        id=row["id"], repo_full_name=row["repo_full_name"], branch=row["branch"],
        commit_sha=row["commit_sha"], status=row["status"], file_count=row["file_count"],
        symbol_count=row["symbol_count"], edge_count=row["edge_count"],
    )


def _vec(embedding: list[float]) -> str:
    """pgvector text literal '[1,2,3]' (works without the vector codec)."""
    return "[" + ",".join(repr(float(x)) for x in embedding) + "]"
