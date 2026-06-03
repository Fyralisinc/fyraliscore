"""services/ingest/github_intel/fsm.py — GitHub event classification + state machines.

Pure, side-effect-free logic:
  - `classify(content)` normalizes a github:webhook observation's `content` dict
    into a `GithubEvent` (entity identity + intent).
  - `pr_lifecycle_next` / `issue_status_next` / `ci_rollup` are the transition
    functions.
  - `rule_reasoning` is the deterministic fast-path that explains the bulk of
    events (merge, close, check completion, push) with no LLM call.

State writes + the ordering guard live in `state_store.py`; this module never
touches the DB.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ---- PR lifecycle FSM -------------------------------------------------
PR_LIFECYCLE = (
    "open", "draft", "review_requested", "changes_requested",
    "approved", "merged", "closed",
)
PR_TERMINAL = {"merged", "closed"}
CI_STATES = ("unknown", "pending", "passing", "failing", "error")


@dataclass
class GithubEvent:
    event_type: str
    action: str | None
    repo: str | None
    entity_kind: str | None          # pr|issue|branch|repo|check|comment
    entity_ref: str | None           # "repo#number" or branch name
    author: str | None = None
    fields: dict[str, Any] = field(default_factory=dict)


def classify(content: dict[str, Any]) -> GithubEvent:
    et = content.get("event_type") or "unknown"
    action = content.get("action")
    repo = content.get("repo")
    author = content.get("author")

    if et == "pull_request":
        n = content.get("pr_number")
        return GithubEvent(
            et, action, repo, "pr", f"{repo}#{n}", author,
            fields={
                "pr_number": n, "pr_node_id": content.get("pr_node_id"),
                "title": content.get("pr_title"), "base_ref": content.get("base_ref"),
                "merged": bool(content.get("merged")),
                "head_sha": content.get("head_sha"), "head_ref": content.get("head_ref"),
                "draft": bool(content.get("draft")),
            },
        )
    if et == "pull_request_review":
        n = content.get("pr_number")
        return GithubEvent(
            et, action, repo, "pr", f"{repo}#{n}", author,
            fields={"pr_number": n, "review_state": content.get("review_state")},
        )
    if et == "issues":
        n = content.get("issue_number")
        return GithubEvent(
            et, action, repo, "issue", f"{repo}#{n}", author,
            fields={
                "issue_number": n, "issue_node_id": content.get("issue_node_id"),
                "title": content.get("issue_title"),
            },
        )
    if et == "issue_comment":
        n = content.get("issue_number")
        return GithubEvent(
            et, action, repo, "comment", f"{repo}#{n}", author,
            fields={"issue_number": n, "body": content.get("body")},
        )
    if et == "push":
        branch = content.get("branch")
        return GithubEvent(
            et, action, repo, "branch", branch, author,
            fields={
                "branch": branch, "after": content.get("after"),
                "commits_count": content.get("commits_count"),
            },
        )
    if et == "check_run":
        return GithubEvent(
            et, action, repo, "check", content.get("head_sha"), author,
            fields={
                "check_name": content.get("check_name"),
                "status": content.get("status"),
                "conclusion": content.get("conclusion"),
                "head_sha": content.get("head_sha"),
            },
        )
    return GithubEvent(et, action, repo, None, None, author, fields=dict(content))


# ---- transition functions --------------------------------------------
def pr_lifecycle_next(current: str | None, ev: GithubEvent) -> str | None:
    """Return the next PR lifecycle state, or None for no change."""
    cur = current or "open"
    if ev.event_type == "pull_request":
        action = ev.action
        merged = ev.fields.get("merged")
        if action == "opened":
            return "draft" if ev.fields.get("draft") else "open"
        if action == "reopened":
            return "open"
        if action == "ready_for_review":
            return "open"
        if action == "review_requested":
            return "review_requested" if cur not in PR_TERMINAL else cur
        if action == "closed":
            return "merged" if merged else "closed"
        return cur
    if ev.event_type == "pull_request_review":
        if cur in PR_TERMINAL:
            return cur
        state = (ev.fields.get("review_state") or "").lower()
        if state == "approved":
            return "approved"
        if state == "changes_requested":
            return "changes_requested"
        return cur
    return cur


def issue_status_next(current: str | None, ev: GithubEvent) -> str | None:
    cur = current or "open"
    if ev.event_type == "issues":
        if ev.action == "closed":
            return "closed"
        if ev.action in ("reopened", "opened"):
            return "open"
    return cur


def ci_rollup(check_rows: list[dict[str, Any]]) -> str:
    """Roll a set of check rows for one head_sha into a single ci_state."""
    if not check_rows:
        return "unknown"
    concl = [(r.get("conclusion") or "").lower() for r in check_rows]
    status = [(r.get("status") or "").lower() for r in check_rows]
    if any(c in ("failure", "timed_out", "action_required") for c in concl):
        return "failing"
    if any(c in ("cancelled", "stale") for c in concl):
        return "error"
    completed = [s == "completed" for s in status]
    if all(completed) and all(c == "success" for c in concl):
        return "passing"
    if any(s in ("queued", "in_progress") for s in status):
        return "pending"
    return "pending"


# ---- rule-based reasoning (the LLM-free fast path) --------------------
def is_obvious(ev: GithubEvent) -> bool:
    """True when the cause/effect is deterministic (no LLM needed)."""
    if ev.event_type == "pull_request":
        return ev.action in ("opened", "reopened", "closed", "ready_for_review")
    return ev.event_type in (
        "issues", "issue_comment", "push", "check_run", "pull_request_review"
    )


def rule_reasoning(
    ev: GithubEvent, *, before: str | None, after: str | None
) -> dict[str, Any]:
    """Deterministic cause/effect/explanation for an event."""
    et = ev.event_type
    repo = ev.repo or "the repo"
    who = ev.author or "someone"
    transition = f"{before}->{after}" if (before or after) and before != after else "none"

    if et == "pull_request":
        n = ev.fields.get("pr_number")
        base = ev.fields.get("base_ref")
        if ev.action == "closed" and ev.fields.get("merged"):
            return _r(
                cause=f"{who} merged PR #{n} into {base}",
                effect=f"PR #{n} reached terminal state 'merged'; {base} now includes its changes "
                       f"and a re-index of the affected code is triggered",
                explanation=f"A merge to {base} changes the codebase state of {repo}; downstream "
                            f"code that depends on the merged files is in the blast radius.",
                transition=transition, confidence=1.0,
            )
        if ev.action == "closed":
            return _r(
                cause=f"{who} closed PR #{n} without merging",
                effect=f"PR #{n} reached terminal state 'closed'; no code change landed",
                explanation="The proposed changes were abandoned; no blast radius.",
                transition=transition, confidence=1.0,
            )
        if ev.action in ("opened", "reopened"):
            return _r(
                cause=f"{who} {ev.action} PR #{n} against {base}",
                effect=f"PR #{n} entered review; its changes are candidate modifications to {base}",
                explanation="An open PR proposes changes; the affected code indicates review focus.",
                transition=transition, confidence=0.9,
            )
    if et == "pull_request_review":
        n = ev.fields.get("pr_number")
        st = ev.fields.get("review_state")
        return _r(
            cause=f"{who} submitted a '{st}' review on PR #{n}",
            effect=f"PR #{n} review state advanced to '{after}'",
            explanation="Reviews gate merge readiness; an approval unblocks merge, "
                        "changes_requested blocks it.",
            transition=transition, confidence=0.95,
        )
    if et == "issues":
        n = ev.fields.get("issue_number")
        return _r(
            cause=f"{who} {ev.action} issue #{n}",
            effect=f"Issue #{n} status is now '{after}'",
            explanation="Issue lifecycle reflects whether the tracked work is open or resolved.",
            transition=transition, confidence=1.0,
        )
    if et == "issue_comment":
        n = ev.fields.get("issue_number")
        return _r(
            cause=f"{who} commented on issue #{n}",
            effect="Discussion signal; no state change",
            explanation="A comment is a discussion signal, not a state transition.",
            transition="none", confidence=0.8,
        )
    if et == "push":
        br = ev.fields.get("branch")
        cnt = ev.fields.get("commits_count")
        return _r(
            cause=f"{who} pushed {cnt} commit(s) to {br}",
            effect=f"Branch '{br}' head advanced; pushed files and their dependents are affected",
            explanation=f"A push mutates {br}; the changed files' blast radius is the code at risk.",
            transition=transition, confidence=1.0,
        )
    if et == "check_run":
        name = ev.fields.get("check_name")
        concl = ev.fields.get("conclusion")
        return _r(
            cause=f"check '{name}' completed with conclusion '{concl}'",
            effect=f"CI signal for commit {(ev.fields.get('head_sha') or '')[:8]}: {concl}",
            explanation="Check conclusions roll up into the PR's ci_state (passing/failing), "
                        "gating merge readiness.",
            transition=transition, confidence=1.0,
        )
    return _r(
        cause=f"{who} {ev.action or ''} {et}".strip(),
        effect="recorded",
        explanation="Signal recorded; no specific causal rule matched.",
        transition=transition, confidence=0.5,
    )


def _r(*, cause: str, effect: str, explanation: str, transition: str, confidence: float) -> dict[str, Any]:
    return {
        "cause": cause, "effect": effect, "explanation": explanation,
        "state_change": transition, "confidence": confidence, "reasoning_path": "rule",
    }
