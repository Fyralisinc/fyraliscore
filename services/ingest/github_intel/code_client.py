"""services/ingest/github_intel/code_client.py — bridge to the code_intel subsystem.

Thin adapter the enrichment uses to answer "what code does this signal touch?"
without knowing code_intel internals: extract changed paths from the event,
compute the blast radius against the repo's latest ready snapshot, and
(best-effort) pull semantically-relevant code via code-RAG.
"""
from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from services.ingest.code_intel.graph import CodeGraphRepo

log = logging.getLogger("github_intel.code_client")


def extract_changed_paths(
    content: dict[str, Any], raw_payload: dict[str, Any] | None
) -> list[str]:
    """Best-effort changed-file paths from a github event.

    push        -> commits[].{added,modified,removed} (authoritative; real payload)
    pull_request-> pull_request._changed_files when present (demo/test provides it;
                   production resolves these via git diff on the clone — deferred)
    """
    raw = raw_payload or content.get("_raw") or {}
    et = content.get("event_type")
    paths: set[str] = set()

    # Self-contained fast path: the handler stashes changed paths on content
    # (content["files"] for push, content["changed_files"] for PR) so the worker
    # — which reads the persisted observation, not the raw payload — can compute
    # blast radius without the raw body.
    for key in ("files", "changed_files"):
        for p in (content.get(key) or []):
            if isinstance(p, str):
                paths.add(p)

    if et == "push":
        commits = raw.get("commits") or []
        if isinstance(commits, list):
            for c in commits:
                if isinstance(c, dict):
                    for key in ("added", "modified", "removed"):
                        for p in (c.get(key) or []):
                            if isinstance(p, str):
                                paths.add(p)
        head = raw.get("head_commit") or {}
        if isinstance(head, dict):
            for key in ("added", "modified", "removed"):
                for p in (head.get(key) or []):
                    if isinstance(p, str):
                        paths.add(p)
    elif et == "pull_request":
        pr = raw.get("pull_request") or {}
        for p in (pr.get("_changed_files") or []):
            if isinstance(p, str):
                paths.add(p)

    return sorted(paths)


async def blast_radius_for(
    ctx: Any, *, tenant_id: UUID, repo: str, changed_paths: list[str], max_hops: int = 3
) -> tuple[str | None, dict[str, Any]]:
    """Return (snapshot_commit_sha, blast_radius). Empty when no ready snapshot."""
    graph = CodeGraphRepo(ctx)
    snap = await graph.latest_ready_snapshot(tenant_id, repo)
    if snap is None:
        return None, {"indexed": False, "reason": "no ready code snapshot for repo"}
    if not changed_paths:
        return snap.commit_sha, {
            "indexed": True, "changed_files": [], "dependent_files": [],
            "dependent_symbols": [], "changed_symbols": [],
        }
    br = await graph.blast_radius(snap.id, changed_paths, max_hops=max_hops)
    br["indexed"] = True
    return snap.commit_sha, br


async def code_rag_for(
    ctx: Any, *, tenant_id: UUID, repo: str, query_text: str, k: int = 5,
    embedder: Any | None = None,
) -> list[dict[str, Any]]:
    """Best-effort semantic retrieval of code relevant to a signal."""
    if not query_text:
        return []
    if embedder is None:
        try:
            from lib.embeddings.factory import make_embedder
            embedder = make_embedder()
        except Exception:  # noqa: BLE001 — embedder unconfigurable: skip code-RAG
            log.warning("code-RAG skipped: embedder unavailable", exc_info=True)
            return []
    try:
        vec = await embedder.embed(query_text)
    except Exception:  # noqa: BLE001 — embedder down: skip code-RAG gracefully
        log.warning("code-RAG skipped: embed failed", exc_info=True)
        return []
    graph = CodeGraphRepo(ctx)
    snap = await graph.latest_ready_snapshot(tenant_id, repo)
    if snap is None:
        return []
    try:
        return await graph.search_code(snap.id, vec, k=k)
    except Exception:  # noqa: BLE001 — search failed: degrade to empty
        log.warning("code-RAG skipped: search_code failed", exc_info=True)
        return []
