"""services/github_intel/state_store.py — FSM state persistence + ordering guard.

Two access modes over the 0064 tables:
  - `read_state_snapshot(ctx, ev)` — READ-ONLY current state, used by the inline
    enrichment path (which must not write on the parallel normalize stage).
  - `apply_event(ctx, tenant_id, ev, occurred_at)` — the authoritative, ordered
    write used by the worker. Transitions apply ONLY when
    occurred_at >= last_event_at (ordering guard); late/replayed events return
    state_changed=False without mutating live state.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from services.github_intel import fsm
from services.github_intel.fsm import GithubEvent

# Branches treated as a repo's default when none has been established yet.
_DEFAULT_BRANCHES = {"main", "master"}


# ---- read-only snapshot (inline path) --------------------------------
async def read_state_snapshot(ctx: Any, ev: GithubEvent) -> dict[str, Any]:
    """Current state for the event's entity (no lock, no write)."""
    repo = ev.repo
    if ev.entity_kind == "pr":
        n = ev.fields.get("pr_number")
        row = await ctx.fetchrow(
            "SELECT lifecycle, ci_state FROM github_pr_state "
            "WHERE repo=$1 AND pr_number=$2", repo, n,
        )
        return {"lifecycle": row["lifecycle"] if row else None,
                "ci_state": row["ci_state"] if row else "unknown"} if True else {}
    if ev.entity_kind == "issue":
        n = ev.fields.get("issue_number")
        row = await ctx.fetchrow(
            "SELECT status FROM github_issue_state WHERE repo=$1 AND issue_number=$2",
            repo, n,
        )
        return {"status": row["status"] if row else None}
    if ev.entity_kind == "branch":
        br = ev.fields.get("branch")
        row = await ctx.fetchrow(
            "SELECT head_sha FROM github_branch_state WHERE repo=$1 AND branch=$2",
            repo, br,
        )
        return {"head_sha": row["head_sha"] if row else None}
    if ev.entity_kind == "check":
        return {"head_sha": ev.fields.get("head_sha")}
    return {}


# ---- authoritative ordered write (worker path) -----------------------
async def apply_event(
    ctx: Any, *, tenant_id: UUID, ev: GithubEvent, occurred_at: datetime
) -> dict[str, Any]:
    """Apply the FSM transition for ev. Returns
    {before, after, state_changed, entity_kind, entity_ref}."""
    kind = ev.entity_kind
    if kind == "pr":
        return await _apply_pr(ctx, tenant_id, ev, occurred_at)
    if kind == "issue":
        return await _apply_issue(ctx, tenant_id, ev, occurred_at)
    if kind == "branch":
        return await _apply_branch(ctx, tenant_id, ev, occurred_at)
    if kind == "check":
        return await _apply_check(ctx, tenant_id, ev, occurred_at)
    # comment / unknown — no state change
    return {"before": {}, "after": {}, "state_changed": False,
            "entity_kind": kind, "entity_ref": ev.entity_ref}


def _guard(last_event_at: datetime | None, occurred_at: datetime) -> bool:
    """True when this event is in-order (safe to mutate state)."""
    return last_event_at is None or occurred_at >= last_event_at


async def _apply_pr(ctx, tenant_id, ev, occurred_at) -> dict[str, Any]:
    repo = ev.repo
    n = ev.fields.get("pr_number")
    row = await ctx.fetchrow(
        "SELECT lifecycle, ci_state, last_event_at, state_version, merged "
        "FROM github_pr_state WHERE repo=$1 AND pr_number=$2 FOR UPDATE", repo, n,
    )
    cur_life = row["lifecycle"] if row else None
    cur_ci = row["ci_state"] if row else "unknown"
    before = {"lifecycle": cur_life, "ci_state": cur_ci}
    in_order = _guard(row["last_event_at"] if row else None, occurred_at)

    new_life = fsm.pr_lifecycle_next(cur_life, ev) if in_order else cur_life
    merged = bool(ev.fields.get("merged")) or (row["merged"] if row else False)
    after = {"lifecycle": new_life, "ci_state": cur_ci}
    changed = in_order and (new_life != (cur_life or "open") or row is None)

    if in_order:
        if row is None:
            await ctx.execute(
                "INSERT INTO github_pr_state "
                "(tenant_id, repo, pr_number, pr_node_id, title, author, base_ref, "
                " head_ref, head_sha, lifecycle, merged, opened_at, last_event_at, state_version) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12,1) "
                "ON CONFLICT (tenant_id, repo, pr_number) DO NOTHING",
                tenant_id, repo, n, ev.fields.get("pr_node_id"), ev.fields.get("title"),
                ev.author, ev.fields.get("base_ref"), ev.fields.get("head_ref"),
                ev.fields.get("head_sha"), new_life or "open", merged, occurred_at,
            )
        else:
            await ctx.execute(
                "UPDATE github_pr_state SET lifecycle=$3, merged=$4, "
                "head_sha=COALESCE($6, head_sha), head_ref=COALESCE($7, head_ref), "
                "merge_commit_sha=CASE WHEN $3='merged' THEN COALESCE($8, merge_commit_sha) "
                "  ELSE merge_commit_sha END, "
                "last_event_at=$5, state_version=state_version+1, updated_at=now(), "
                "closed_at=CASE WHEN $3 IN ('merged','closed') THEN $5 ELSE closed_at END "
                "WHERE repo=$1 AND pr_number=$2",
                repo, n, new_life or "open", merged, occurred_at,
                ev.fields.get("head_sha"), ev.fields.get("head_ref"),
                ev.fields.get("merge_commit_sha") or ev.fields.get("head_sha"),
            )
    return {"before": before, "after": after, "state_changed": changed,
            "entity_kind": "pr", "entity_ref": ev.entity_ref}


async def _apply_issue(ctx, tenant_id, ev, occurred_at) -> dict[str, Any]:
    repo = ev.repo
    n = ev.fields.get("issue_number")
    row = await ctx.fetchrow(
        "SELECT status, last_event_at FROM github_issue_state "
        "WHERE repo=$1 AND issue_number=$2 FOR UPDATE", repo, n,
    )
    cur = row["status"] if row else None
    before = {"status": cur}
    in_order = _guard(row["last_event_at"] if row else None, occurred_at)
    new = fsm.issue_status_next(cur, ev) if in_order else cur
    after = {"status": new}
    changed = in_order and (new != (cur or "open") or row is None)
    if in_order:
        await ctx.execute(
            "INSERT INTO github_issue_state "
            "(tenant_id, repo, issue_number, issue_node_id, title, author, status, "
            " last_event_at, state_version) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1) "
            "ON CONFLICT (tenant_id, repo, issue_number) DO UPDATE SET "
            "status=EXCLUDED.status, last_event_at=EXCLUDED.last_event_at, "
            "state_version=github_issue_state.state_version+1, updated_at=now()",
            tenant_id, repo, n, ev.fields.get("issue_node_id"), ev.fields.get("title"),
            ev.author, new or "open", occurred_at,
        )
    return {"before": before, "after": after, "state_changed": changed,
            "entity_kind": "issue", "entity_ref": ev.entity_ref}


async def _apply_branch(ctx, tenant_id, ev, occurred_at) -> dict[str, Any]:
    repo = ev.repo
    br = ev.fields.get("branch")
    after_sha = ev.fields.get("after")
    row = await ctx.fetchrow(
        "SELECT head_sha, last_event_at FROM github_branch_state "
        "WHERE repo=$1 AND branch=$2 FOR UPDATE", repo, br,
    )
    before = {"head_sha": row["head_sha"] if row else None}
    in_order = _guard(row["last_event_at"] if row else None, occurred_at)
    after = {"head_sha": after_sha if in_order else before["head_sha"]}
    changed = in_order and after_sha is not None and after_sha != before["head_sha"]
    if in_order and after_sha:
        await ctx.execute(
            "INSERT INTO github_branch_state "
            "(tenant_id, repo, branch, head_sha, last_push_by, last_event_at, state_version) "
            "VALUES ($1,$2,$3,$4,$5,$6,1) "
            "ON CONFLICT (tenant_id, repo, branch) DO UPDATE SET "
            "head_sha=EXCLUDED.head_sha, last_push_by=EXCLUDED.last_push_by, "
            "last_event_at=EXCLUDED.last_event_at, "
            "state_version=github_branch_state.state_version+1, updated_at=now()",
            tenant_id, repo, br, after_sha, ev.author, occurred_at,
        )
        # The repo's HEAD only advances when the DEFAULT branch moves. We don't
        # always know the default a priori, so treat main/master as default, or
        # an already-established default_branch. A push to a feature branch must
        # NOT hijack the repo head (else it sticks on the first branch seen).
        if br in _DEFAULT_BRANCHES:
            await ctx.execute(
                "INSERT INTO github_repo_state "
                "(tenant_id, repo, default_branch, head_sha, head_sha_at, last_event_at, state_version) "
                "VALUES ($1,$2,$3,$4,$5,$5,1) "
                "ON CONFLICT (tenant_id, repo) DO UPDATE SET "
                "head_sha=EXCLUDED.head_sha, head_sha_at=EXCLUDED.head_sha_at, "
                "default_branch=COALESCE(github_repo_state.default_branch, EXCLUDED.default_branch), "
                "last_event_at=EXCLUDED.last_event_at, "
                "state_version=github_repo_state.state_version+1, updated_at=now()",
                tenant_id, repo, br, after_sha, occurred_at,
            )
    return {"before": before, "after": after, "state_changed": changed,
            "entity_kind": "branch", "entity_ref": br}


async def _apply_check(ctx, tenant_id, ev, occurred_at) -> dict[str, Any]:
    repo = ev.repo
    head_sha = ev.fields.get("head_sha")
    name = ev.fields.get("check_name")
    status = ev.fields.get("status")
    conclusion = ev.fields.get("conclusion")
    await ctx.execute(
        "INSERT INTO github_check_state "
        "(tenant_id, repo, head_sha, check_name, status, conclusion, last_event_at, state_version) "
        "VALUES ($1,$2,$3,$4,$5,$6,$7,1) "
        "ON CONFLICT (tenant_id, repo, head_sha, check_name) DO UPDATE SET "
        "status=EXCLUDED.status, conclusion=EXCLUDED.conclusion, "
        "last_event_at=EXCLUDED.last_event_at, "
        "state_version=github_check_state.state_version+1, updated_at=now()",
        tenant_id, repo, head_sha, name, status, conclusion, occurred_at,
    )
    # roll all checks for this head_sha into the matching PR's ci_state
    checks = await ctx.fetch(
        "SELECT status, conclusion FROM github_check_state "
        "WHERE repo=$1 AND head_sha=$2", repo, head_sha,
    )
    new_ci = fsm.ci_rollup([dict(r) for r in checks])
    pr_rows = await ctx.fetch(
        "SELECT pr_number, ci_state FROM github_pr_state WHERE repo=$1 AND head_sha=$2",
        repo, head_sha,
    )
    before_ci = pr_rows[0]["ci_state"] if pr_rows else "unknown"
    changed = False
    pr_ref = ev.entity_ref
    for pr in pr_rows:
        if pr["ci_state"] != new_ci:
            changed = True
        await ctx.execute(
            "UPDATE github_pr_state SET ci_state=$3, state_version=state_version+1, "
            "updated_at=now() WHERE repo=$1 AND pr_number=$2",
            repo, pr["pr_number"], new_ci,
        )
        pr_ref = f"{repo}#{pr['pr_number']}"
    return {
        "before": {"ci_state": before_ci}, "after": {"ci_state": new_ci},
        "state_changed": changed or not pr_rows, "entity_kind": "check",
        "entity_ref": pr_ref,
    }
