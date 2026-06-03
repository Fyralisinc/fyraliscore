"""services/ingest/github_intel/api.py — read-only GitHub Intelligence HTTP surface.

Mounted by services/app/gateway/main.py via `app.include_router(build_github_intel_router())`.
All routes are Bearer-scoped (the gateway's BearerAuthMiddleware sets
`request.state.auth`) and additionally gated by a per-tenant repo allowlist
(`GithubIntelReadRepo.authorize_repo`) — an unauthorized repo returns 404 so the
endpoint never leaks whether a repo exists for another tenant.

Stateless: the pool is read from `request.app.state.deps.pool`; every DB read
runs inside `tenant_transaction` (RLS-scoped). Repos are addressed as
`{owner}/{repo}` path segments and reassembled to "owner/name".
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from lib.shared.tenant_context import tenant_transaction
from services.ingest.code_intel.graph import CodeGraphRepo
from services.ingest.github_intel.read_repo import GithubIntelReadRepo

log = logging.getLogger("github_intel.api")

# Sorts strictly after any real uuid7, so a bare-timestamp cursor (no id) is
# paired with this to include ALL rows at that timestamp via the row-value `<`.
_MAX_UUID = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")


def _clip(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(x)))


def _err(code: str, status: int, **extra: Any) -> JSONResponse:
    return JSONResponse({"error": code, **extra}, status_code=status)


def _auth_tenant(request: Request) -> UUID | None:
    auth = getattr(request.state, "auth", None)
    return getattr(auth, "tenant_id", None) if auth is not None else None


def _pool(request: Request) -> Any:
    return request.app.state.deps.pool


async def _embed_and_search(
    request: Request, graph: Any, snapshot_id: Any, q: str, k: int
) -> list[dict[str, Any]]:
    """Embed `q` and run code-RAG. Prefer the gateway's shared embedder (app-managed,
    not closed here); only build + close a throwaway when none is configured. Degrades
    to [] if the embedder is unavailable so a down Ollama never 500s the request."""
    shared = getattr(getattr(request.app.state, "deps", None), "embedder", None)
    embedder = shared
    own = False
    try:
        if embedder is None:
            from lib.embeddings.factory import make_embedder
            embedder = make_embedder()
            own = True
        vec = await embedder.embed(q)
        return await graph.search_code(snapshot_id, vec, k=k)
    except Exception:  # noqa: BLE001 — embedder down / no results: degrade to empty
        # Log so operators can tell "broken embedder/search" from "no matches"
        # (both otherwise return an empty list with HTTP 200).
        log.warning("code-search degraded to empty result", exc_info=True)
        return []
    finally:
        if own and embedder is not None:
            try:
                await embedder.close()
            except Exception:  # noqa: BLE001
                pass


def _parse_ts(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _parse_cursor(before: str | None) -> tuple[datetime | None, UUID | None]:
    """Decode the signals cursor into (enriched_at, observation_id).

    Accepts the compound form "<iso>|<observation_id>" (what /signals emits as
    `next_before`), or a bare "<iso>" which is paired with _MAX_UUID so the
    row-value comparison includes every row at that exact timestamp.
    Raises ValueError on a malformed value (handler maps it to 400).
    """
    if not before:
        return None, None
    s = before.strip()
    if "|" in s:
        ts_part, id_part = s.rsplit("|", 1)
        return _parse_ts(ts_part), UUID(id_part)
    return _parse_ts(s), _MAX_UUID


def build_github_intel_router() -> APIRouter:
    router = APIRouter(prefix="/github-intel", tags=["github-intel"])

    @router.get("/repos")
    async def list_repos(request: Request, limit: int = 100) -> Any:
        tenant_id = _auth_tenant(request)
        if tenant_id is None:
            return _err("unauthorized", 401)
        async with tenant_transaction(tenant_id, pool=_pool(request)) as ctx:
            repos = await GithubIntelReadRepo(ctx).list_repos(
                tenant_id, limit=_clip(limit, 1, 500)
            )
        return {"repos": repos, "count": len(repos)}

    @router.get("/repos/{owner}/{repo}/state")
    async def repo_state(request: Request, owner: str, repo: str) -> Any:
        tenant_id = _auth_tenant(request)
        if tenant_id is None:
            return _err("unauthorized", 401)
        full = f"{owner}/{repo}"
        async with tenant_transaction(tenant_id, pool=_pool(request)) as ctx:
            r = GithubIntelReadRepo(ctx)
            if not await r.authorize_repo(tenant_id, full):
                return _err("repo_not_found", 404, repo=full)
            return await r.repo_state(tenant_id, full)

    @router.get("/repos/{owner}/{repo}/signals")
    async def repo_signals(
        request: Request, owner: str, repo: str,
        limit: int = 50, before: str | None = None, event_type: str | None = None,
    ) -> Any:
        tenant_id = _auth_tenant(request)
        if tenant_id is None:
            return _err("unauthorized", 401)
        full = f"{owner}/{repo}"
        lim = _clip(limit, 1, 200)
        try:
            before_ts, before_id = _parse_cursor(before)
        except ValueError:
            return _err("bad_request", 400, reason="invalid 'before' cursor")
        async with tenant_transaction(tenant_id, pool=_pool(request)) as ctx:
            r = GithubIntelReadRepo(ctx)
            if not await r.authorize_repo(tenant_id, full):
                return _err("repo_not_found", 404, repo=full)
            signals = await r.signals(
                tenant_id, full, limit=lim,
                before_ts=before_ts, before_id=before_id, event_type=event_type,
            )
        # Compound cursor (enriched_at + observation_id) so a non-unique
        # enriched_at never drops sibling rows across the page boundary.
        # Emit the timestamp in 'Z' form (not '+00:00') so the cursor survives
        # an unencoded URL query, where '+' would otherwise decode to a space.
        next_before = None
        if len(signals) == lim:
            last = signals[-1]
            ts = (last["enriched_at"] or "").replace("+00:00", "Z")
            next_before = f"{ts}|{last['observation_id']}"
        return {"signals": signals, "count": len(signals), "next_before": next_before}

    @router.get("/repos/{owner}/{repo}/prs")
    async def repo_prs(
        request: Request, owner: str, repo: str,
        state: str | None = None, ci: str | None = None, limit: int = 50,
    ) -> Any:
        tenant_id = _auth_tenant(request)
        if tenant_id is None:
            return _err("unauthorized", 401)
        full = f"{owner}/{repo}"
        async with tenant_transaction(tenant_id, pool=_pool(request)) as ctx:
            r = GithubIntelReadRepo(ctx)
            if not await r.authorize_repo(tenant_id, full):
                return _err("repo_not_found", 404, repo=full)
            prs = await r.prs(
                tenant_id, full, lifecycle=state, ci_state=ci, limit=_clip(limit, 1, 200)
            )
        return {"pull_requests": prs, "count": len(prs)}

    @router.get("/repos/{owner}/{repo}/prs/{pr_number}")
    async def repo_pr_detail(request: Request, owner: str, repo: str, pr_number: int) -> Any:
        tenant_id = _auth_tenant(request)
        if tenant_id is None:
            return _err("unauthorized", 401)
        full = f"{owner}/{repo}"
        async with tenant_transaction(tenant_id, pool=_pool(request)) as ctx:
            r = GithubIntelReadRepo(ctx)
            if not await r.authorize_repo(tenant_id, full):
                return _err("repo_not_found", 404, repo=full)
            detail = await r.pr_detail(tenant_id, full, pr_number)
        if detail is None:
            return _err("pr_not_found", 404, repo=full, pr_number=pr_number)
        return detail

    @router.get("/repos/{owner}/{repo}/blast-radius")
    async def repo_blast_radius(
        request: Request, owner: str, repo: str,
        path: list[str] = Query(default=[]), max_hops: int = 3,
    ) -> Any:
        tenant_id = _auth_tenant(request)
        if tenant_id is None:
            return _err("unauthorized", 401)
        full = f"{owner}/{repo}"
        # `?path=` yields [''] (a non-empty list of a blank), which would slip
        # past a bare `if not path` and return a misleading 200. Strip blanks.
        paths = [p for p in path if p.strip()]
        if not paths:
            return _err("bad_request", 400,
                        reason="at least one non-empty 'path' is required")
        async with tenant_transaction(tenant_id, pool=_pool(request)) as ctx:
            r = GithubIntelReadRepo(ctx)
            if not await r.authorize_repo(tenant_id, full):
                return _err("repo_not_found", 404, repo=full)
            graph = CodeGraphRepo(ctx)
            snap = await graph.latest_ready_snapshot(tenant_id, full)
            if snap is None:
                return {"repo": full, "indexed": False,
                        "reason": "no ready code snapshot for repo"}
            br = await graph.blast_radius(snap.id, paths, max_hops=_clip(max_hops, 1, 6))
        br["indexed"] = True
        br["repo"] = full
        br["commit_sha"] = snap.commit_sha
        return br

    @router.get("/repos/{owner}/{repo}/code-search")
    async def repo_code_search(
        request: Request, owner: str, repo: str, q: str = "", k: int = 5,
    ) -> Any:
        tenant_id = _auth_tenant(request)
        if tenant_id is None:
            return _err("unauthorized", 401)
        full = f"{owner}/{repo}"
        if not q.strip():
            return _err("bad_request", 400, reason="query 'q' is required")
        async with tenant_transaction(tenant_id, pool=_pool(request)) as ctx:
            r = GithubIntelReadRepo(ctx)
            if not await r.authorize_repo(tenant_id, full):
                return _err("repo_not_found", 404, repo=full)
            graph = CodeGraphRepo(ctx)
            snap = await graph.latest_ready_snapshot(tenant_id, full)
            if snap is None:
                return {"repo": full, "query": q, "results": [], "indexed": False}
            results = await _embed_and_search(request, graph, snap.id, q, _clip(k, 1, 25))
        return {"repo": full, "query": q, "results": results, "count": len(results)}

    @router.get("/signals/{observation_id}/explain")
    async def explain_signal(request: Request, observation_id: str) -> Any:
        tenant_id = _auth_tenant(request)
        if tenant_id is None:
            return _err("unauthorized", 401)
        try:
            obs_uuid = UUID(observation_id)
        except ValueError:
            return _err("bad_request", 400, reason="invalid observation_id")
        async with tenant_transaction(tenant_id, pool=_pool(request)) as ctx:
            r = GithubIntelReadRepo(ctx)
            repo = await r.repo_for_observation(tenant_id, obs_uuid)
            if repo is None:
                return _err("signal_not_found", 404, observation_id=observation_id)
            if not await r.authorize_repo(tenant_id, repo):
                return _err("signal_not_found", 404, observation_id=observation_id)
            result = await r.explain_signal(tenant_id, obs_uuid)
        if result is None:
            return _err("signal_not_found", 404, observation_id=observation_id)
        return result

    return router


__all__ = ["build_github_intel_router"]
