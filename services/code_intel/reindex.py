"""services/code_intel/reindex.py — drain reindex triggers (self-update loop).

When github_intel advances the default branch (push/merge), it writes a
`code_intel_index_triggers` row. This consumer re-indexes the working copy at
the new commit sha, producing a fresh `ready` snapshot whose `parent_snapshot_id`
links to the prior one — so the code model stays live with the codebase.

In production the working copy comes from a `git fetch` of the new sha on the
cached clone; here it's a local path (the same bytes, tagged with the new sha).
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from lib.shared.tenant_context import tenant_transaction

from services.code_intel.graph import CodeGraphRepo
from services.code_intel.indexer import index_working_copy


async def drain_reindex_triggers(
    pool: Any, tenant_id: UUID, *, root_path: str, max_items: int = 100
) -> list[dict[str, Any]]:
    """Process pending reindex triggers. Returns a summary per processed trigger."""
    out: list[dict[str, Any]] = []
    for _ in range(max_items):
        async with tenant_transaction(tenant_id, pool=pool) as ctx:
            row = await ctx.fetchrow(
                """
                SELECT id, repo_full_name, branch, commit_sha, kind
                  FROM code_intel_index_triggers
                 WHERE tenant_id=$1 AND status='pending'
                 ORDER BY created_at ASC
                 FOR UPDATE SKIP LOCKED LIMIT 1
                """,
                tenant_id,
            )
            if row is None:
                break
            await ctx.execute(
                "UPDATE code_intel_index_triggers SET status='claimed', claimed_at=now() "
                "WHERE id=$1",
                row["id"],
            )
            repo = CodeGraphRepo(ctx)
            parent = await repo.latest_ready_snapshot(tenant_id, row["repo_full_name"])
            parent_id = parent.id if parent else None

        try:
            stats = await index_working_copy(
                pool=pool, tenant_id=tenant_id, repo_full_name=row["repo_full_name"],
                root_path=root_path, commit_sha=row["commit_sha"],
                branch=row["branch"] or "main", index_kind="incremental",
                parent_snapshot_id=parent_id,
            )
            async with tenant_transaction(tenant_id, pool=pool) as ctx:
                await ctx.execute(
                    "UPDATE code_intel_index_triggers SET status='done', completed_at=now() "
                    "WHERE id=$1",
                    row["id"],
                )
            out.append({
                "repo": row["repo_full_name"], "commit_sha": row["commit_sha"],
                "kind": row["kind"], "snapshot_id": str(stats.snapshot_id),
                "files": stats.files, "symbols": stats.symbols, "edges": stats.edges,
            })
        except Exception as exc:  # noqa: BLE001
            async with tenant_transaction(tenant_id, pool=pool) as ctx:
                await ctx.execute(
                    "UPDATE code_intel_index_triggers SET status='failed', "
                    "last_error=$2 WHERE id=$1",
                    row["id"], f"{type(exc).__name__}: {exc}"[:500],
                )
            out.append({"repo": row["repo_full_name"], "commit_sha": row["commit_sha"],
                        "error": str(exc)})
    return out
