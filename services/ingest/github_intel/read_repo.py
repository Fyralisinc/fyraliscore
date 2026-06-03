"""services/ingest/github_intel/read_repo.py — read-side queries for the API surface.

`GithubIntelReadRepo` wraps a tenant-bound connection (a `TenantContext` or an
asyncpg connection with app.current_tenant already set) and exposes one method
per gateway endpoint. All reads are RLS-scoped by the surrounding
`tenant_transaction`; queries also filter `repo` explicitly so a single tenant's
multiple repos stay separated.

`authorize_repo` is the allowlist gate: a repo is visible to a tenant if it is
covered by a github `provider_installations` row (NULL/empty selected_repositories
= all repos), OR we already hold intelligence for it (the dogfood/test path where
no formal OAuth install exists). Callers translate False -> 404 (no existence leak).
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt is not None else None


def _loads(v: Any) -> Any:
    """JSONB columns arrive as dict/list with the gateway codec, str without."""
    if isinstance(v, (str, bytes)):
        try:
            return json.loads(v)
        except (ValueError, TypeError):
            return v
    return v


class GithubIntelReadRepo:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    # ---- authorization --------------------------------------------------
    async def authorize_repo(self, tenant_id: UUID, repo: str) -> bool:
        # (a) covered by a github provider_installations allowlist
        inst = await self.conn.fetch(
            "SELECT selected_repositories FROM provider_installations "
            "WHERE tenant_id=$1 AND provider='github'",
            tenant_id,
        )
        for row in inst:
            sel = _loads(row["selected_repositories"])
            if not sel:  # NULL or empty list = all repos
                return True
            if isinstance(sel, list) and repo in sel:
                return True
        # (b) fallback: we already hold intelligence for this repo
        held = await self.conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM github_repo_state WHERE tenant_id=$1 AND repo=$2
                UNION ALL
                SELECT 1 FROM code_snapshots WHERE tenant_id=$1 AND repo_full_name=$2
                UNION ALL
                SELECT 1 FROM github_signal_enrichment WHERE tenant_id=$1 AND repo=$2
                LIMIT 1
            )
            """,
            tenant_id, repo,
        )
        return bool(held)

    # ---- discovery ------------------------------------------------------
    async def list_repos(self, tenant_id: UUID, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = await self.conn.fetch(
            """
            SELECT e.repo,
                   count(*) AS signal_count,
                   max(e.enriched_at) AS last_signal_at
              FROM github_signal_enrichment e
             WHERE e.tenant_id = $1 AND e.repo IS NOT NULL
             GROUP BY e.repo
             ORDER BY last_signal_at DESC NULLS LAST
             LIMIT $2
            """,
            tenant_id, limit,
        )
        out = []
        for r in rows:
            snap = await self.conn.fetchrow(
                "SELECT commit_sha, symbol_count, file_count FROM code_snapshots "
                "WHERE tenant_id=$1 AND repo_full_name=$2 AND status='ready' "
                "ORDER BY created_at DESC LIMIT 1",
                tenant_id, r["repo"],
            )
            out.append({
                "repo": r["repo"],
                "signal_count": r["signal_count"],
                "last_signal_at": _iso(r["last_signal_at"]),
                "indexed": snap is not None,
                "head_commit_sha": snap["commit_sha"] if snap else None,
                "symbol_count": snap["symbol_count"] if snap else 0,
                "file_count": snap["file_count"] if snap else 0,
            })
        return out

    # ---- current state --------------------------------------------------
    async def repo_state(self, tenant_id: UUID, repo: str, *, pr_limit: int = 50) -> dict[str, Any]:
        rs = await self.conn.fetchrow(
            "SELECT default_branch, head_sha, head_sha_at, updated_at "
            "FROM github_repo_state WHERE tenant_id=$1 AND repo=$2",
            tenant_id, repo,
        )
        prs = await self.conn.fetch(
            "SELECT pr_number, lifecycle, ci_state, merged, base_ref, head_ref, "
            "title, author, updated_at FROM github_pr_state "
            "WHERE tenant_id=$1 AND repo=$2 ORDER BY pr_number DESC LIMIT $3",
            tenant_id, repo, pr_limit,
        )
        issues = await self.conn.fetch(
            "SELECT issue_number, status, title, author, updated_at "
            "FROM github_issue_state WHERE tenant_id=$1 AND repo=$2 "
            "ORDER BY issue_number DESC LIMIT $3",
            tenant_id, repo, pr_limit,
        )
        branches = await self.conn.fetch(
            "SELECT branch, head_sha, is_deleted, last_push_by, updated_at "
            "FROM github_branch_state WHERE tenant_id=$1 AND repo=$2 "
            "ORDER BY updated_at DESC LIMIT 100",
            tenant_id, repo,
        )
        snap = await self.conn.fetchrow(
            "SELECT commit_sha, symbol_count, file_count, edge_count, created_at "
            "FROM code_snapshots WHERE tenant_id=$1 AND repo_full_name=$2 AND status='ready' "
            "ORDER BY created_at DESC LIMIT 1",
            tenant_id, repo,
        )
        return {
            "repo": repo,
            "default_branch": rs["default_branch"] if rs else None,
            "head_sha": rs["head_sha"] if rs else None,
            "head_sha_at": _iso(rs["head_sha_at"]) if rs else None,
            "code_index": {
                "indexed": snap is not None,
                "commit_sha": snap["commit_sha"] if snap else None,
                "symbol_count": snap["symbol_count"] if snap else 0,
                "file_count": snap["file_count"] if snap else 0,
                "edge_count": snap["edge_count"] if snap else 0,
                "indexed_at": _iso(snap["created_at"]) if snap else None,
            },
            "pull_requests": [
                {
                    "pr_number": p["pr_number"], "lifecycle": p["lifecycle"],
                    "ci_state": p["ci_state"], "merged": p["merged"],
                    "base_ref": p["base_ref"], "head_ref": p["head_ref"],
                    "title": p["title"], "author": p["author"],
                    "updated_at": _iso(p["updated_at"]),
                } for p in prs
            ],
            "issues": [
                {
                    "issue_number": i["issue_number"], "status": i["status"],
                    "title": i["title"], "author": i["author"],
                    "updated_at": _iso(i["updated_at"]),
                } for i in issues
            ],
            "branches": [
                {
                    "branch": b["branch"], "head_sha": b["head_sha"],
                    "is_deleted": b["is_deleted"], "last_push_by": b["last_push_by"],
                } for b in branches
            ],
        }

    # ---- signal feed ----------------------------------------------------
    async def signals(
        self, tenant_id: UUID, repo: str, *, limit: int = 50,
        before_ts: datetime | None = None, before_id: UUID | None = None,
        event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        # Keyset pagination on a TOTAL order. enriched_at is not unique, so the
        # cursor must include observation_id as a tiebreaker — a strict `<` on
        # enriched_at alone would silently drop sibling rows that share the
        # boundary timestamp (and make inter-page order nondeterministic).
        clauses = ["e.tenant_id = $1", "e.repo = $2"]
        params: list[Any] = [tenant_id, repo]
        if before_ts is not None and before_id is not None:
            params.append(before_ts)
            params.append(before_id)
            clauses.append(
                f"(e.enriched_at, e.observation_id) < (${len(params) - 1}, ${len(params)})"
            )
        if event_type:
            params.append(event_type)
            clauses.append(f"e.event_type = ${len(params)}")
        params.append(limit)
        rows = await self.conn.fetch(
            f"""
            SELECT e.observation_id, e.event_type, e.action, e.entity_kind, e.entity_ref,
                   e.state_before, e.state_after, e.state_changed, e.cause, e.effect,
                   e.confidence, e.reasoning_path, e.blast_radius, e.enriched_at,
                   o.content_text, o.occurred_at
              FROM github_signal_enrichment e
              LEFT JOIN observations o ON o.id = e.observation_id
             WHERE {' AND '.join(clauses)}
             ORDER BY e.enriched_at DESC, e.observation_id DESC
             LIMIT ${len(params)}
            """,
            *params,
        )
        out = []
        for r in rows:
            blast = _loads(r["blast_radius"]) or {}
            dep_files = blast.get("dependent_files", []) if isinstance(blast, dict) else []
            out.append({
                "observation_id": str(r["observation_id"]),
                "event_type": r["event_type"], "action": r["action"],
                "entity_kind": r["entity_kind"], "entity_ref": r["entity_ref"],
                "state_before": _loads(r["state_before"]),
                "state_after": _loads(r["state_after"]),
                "state_changed": r["state_changed"],
                "cause": r["cause"], "effect": r["effect"],
                "confidence": r["confidence"], "reasoning_path": r["reasoning_path"],
                "blast_radius_count": len(dep_files),
                "content_text": r["content_text"],
                "occurred_at": _iso(r["occurred_at"]),
                "enriched_at": _iso(r["enriched_at"]),
            })
        return out

    # ---- single-signal explain -----------------------------------------
    async def explain_signal(self, tenant_id: UUID, observation_id: UUID) -> dict[str, Any] | None:
        enr = await self.conn.fetchrow(
            "SELECT * FROM github_signal_enrichment WHERE tenant_id=$1 AND observation_id=$2",
            tenant_id, observation_id,
        )
        obs = await self.conn.fetchrow(
            "SELECT content, content_text, occurred_at, source_channel "
            "FROM observations WHERE tenant_id=$1 AND id=$2",
            tenant_id, observation_id,
        )
        if enr is None and obs is None:
            return None
        content = _loads(obs["content"]) if obs else {}
        intelligence = content.get("intelligence") if isinstance(content, dict) else None
        result: dict[str, Any] = {
            "observation_id": str(observation_id),
            "content_text": obs["content_text"] if obs else None,
            "occurred_at": _iso(obs["occurred_at"]) if obs else None,
            "intelligence": intelligence,  # the inline view (raw signal -> None)
            "enrichment": None,
        }
        if enr is not None:
            result["repo"] = enr["repo"]
            result["enrichment"] = {
                "event_type": enr["event_type"], "action": enr["action"],
                "entity_kind": enr["entity_kind"], "entity_ref": enr["entity_ref"],
                "state_before": _loads(enr["state_before"]),
                "state_after": _loads(enr["state_after"]),
                "state_changed": enr["state_changed"],
                "affected_files": _loads(enr["affected_files"]),
                "affected_symbols": _loads(enr["affected_symbols"]),
                "blast_radius": _loads(enr["blast_radius"]),
                "code_snapshot_sha": enr["code_snapshot_sha"],
                "related_entities": _loads(enr["related_entities"]),
                "cause": enr["cause"], "effect": enr["effect"],
                "explanation": enr["explanation"], "confidence": enr["confidence"],
                "reasoning_path": enr["reasoning_path"],
                "enriched_at": _iso(enr["enriched_at"]),
            }
        return result

    async def repo_for_observation(self, tenant_id: UUID, observation_id: UUID) -> str | None:
        """Repo an observation belongs to (for the allowlist check on /explain)."""
        repo = await self.conn.fetchval(
            "SELECT repo FROM github_signal_enrichment WHERE tenant_id=$1 AND observation_id=$2",
            tenant_id, observation_id,
        )
        if repo:
            return repo
        content = await self.conn.fetchval(
            "SELECT content->>'repo' FROM observations "
            "WHERE tenant_id=$1 AND id=$2 AND source_channel='github:webhook'",
            tenant_id, observation_id,
        )
        return content

    # ---- PR list / detail ----------------------------------------------
    async def prs(
        self, tenant_id: UUID, repo: str, *, lifecycle: str | None = None,
        ci_state: str | None = None, limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses = ["tenant_id = $1", "repo = $2"]
        params: list[Any] = [tenant_id, repo]
        if lifecycle:
            params.append(lifecycle)
            clauses.append(f"lifecycle = ${len(params)}")
        if ci_state:
            params.append(ci_state)
            clauses.append(f"ci_state = ${len(params)}")
        params.append(limit)
        rows = await self.conn.fetch(
            f"SELECT pr_number, lifecycle, ci_state, merged, base_ref, head_ref, "
            f"head_sha, title, author, opened_at, closed_at, updated_at "
            f"FROM github_pr_state WHERE {' AND '.join(clauses)} "
            f"ORDER BY pr_number DESC LIMIT ${len(params)}",
            *params,
        )
        return [
            {
                "pr_number": r["pr_number"], "lifecycle": r["lifecycle"],
                "ci_state": r["ci_state"], "merged": r["merged"],
                "base_ref": r["base_ref"], "head_ref": r["head_ref"],
                "head_sha": r["head_sha"], "title": r["title"], "author": r["author"],
                "opened_at": _iso(r["opened_at"]), "closed_at": _iso(r["closed_at"]),
                "updated_at": _iso(r["updated_at"]),
            } for r in rows
        ]

    async def pr_detail(self, tenant_id: UUID, repo: str, pr_number: int) -> dict[str, Any] | None:
        pr = await self.conn.fetchrow(
            "SELECT pr_number, pr_node_id, lifecycle, ci_state, merged, merge_commit_sha, "
            "base_ref, head_ref, head_sha, title, author, opened_at, closed_at, "
            "last_event_at, state_version, updated_at "
            "FROM github_pr_state WHERE tenant_id=$1 AND repo=$2 AND pr_number=$3",
            tenant_id, repo, pr_number,
        )
        if pr is None:
            return None
        ref = f"{repo}#{pr_number}"
        timeline = await self.conn.fetch(
            "SELECT event_type, action, state_before, state_after, state_changed, "
            "cause, effect, reasoning_path, confidence, enriched_at "
            "FROM github_signal_enrichment WHERE tenant_id=$1 AND repo=$2 AND entity_ref=$3 "
            "ORDER BY enriched_at ASC",
            tenant_id, repo, ref,
        )
        return {
            "repo": repo, "pr_number": pr["pr_number"], "pr_node_id": pr["pr_node_id"],
            "lifecycle": pr["lifecycle"], "ci_state": pr["ci_state"], "merged": pr["merged"],
            "merge_commit_sha": pr["merge_commit_sha"], "base_ref": pr["base_ref"],
            "head_ref": pr["head_ref"], "head_sha": pr["head_sha"], "title": pr["title"],
            "author": pr["author"], "opened_at": _iso(pr["opened_at"]),
            "closed_at": _iso(pr["closed_at"]), "state_version": pr["state_version"],
            "timeline": [
                {
                    "event_type": t["event_type"], "action": t["action"],
                    "state_before": _loads(t["state_before"]),
                    "state_after": _loads(t["state_after"]),
                    "state_changed": t["state_changed"], "cause": t["cause"],
                    "effect": t["effect"], "reasoning_path": t["reasoning_path"],
                    "confidence": t["confidence"], "enriched_at": _iso(t["enriched_at"]),
                } for t in timeline
            ],
        }
