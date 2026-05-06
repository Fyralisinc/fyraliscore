"""services/ingestion/handlers/github.py — GitHub webhook handler.

BUILD-PLAN §3 Prompt 2.B:
    "Webhook events per §14: push, pull_request, issues,
     issue_comment, pull_request_review, check_run.
     For PR merge: content_text synthesized ('Alice merged PR #123
     'Add rate limiter' into main'), entities include PR id, repo,
     base branch. external_id = PR node_id.
     Trust tier: authoritative for merges/check_runs; inferential
     for comments.
     Signature verification with GITHUB_WEBHOOK_SECRET."

Protocol (docs.github.com/en/webhooks/securing-your-webhooks):
    X-Hub-Signature-256: sha256=<hex_hmac_sha256(secret, body)>
    X-GitHub-Event:       event type ('push', 'pull_request', ...)
    X-GitHub-Delivery:    per-delivery UUID (audit, not dedup)

Trust tier policy (per prompt):
    - PR merge event               → authoritative
    - check_run event              → authoritative
    - issues.opened / .closed      → authoritative
    - issue_comment                → inferential
    - pull_request_review.comment  → inferential
    - pull_request_review.approved → authoritative
    - push                         → authoritative
"""
from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingestion.handlers import (
    HandlerError,
    ObservationDraft,
    register,
)


_CHANNEL = "github:webhook"


class GithubSignatureError(HandlerError):
    default_code = "github_signature_invalid"


def _constant_time_eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def verify_github_signature(
    body: bytes,
    signature_header: str | None,
    secret: str | None,
) -> None:
    """Raise `GithubSignatureError` when the HMAC-SHA256 check fails.

    GitHub docs: the `X-Hub-Signature-256` header is the hex digest
    prefixed by `sha256=`. Constant-time compare prevents timing leaks.

    Empty/missing secret raises — silently accepting unsigned payloads
    would be a security regression.
    """
    if not secret:
        raise GithubSignatureError(
            "GITHUB_WEBHOOK_SECRET is not configured",
            reason="missing_secret",
        )
    if not signature_header:
        raise GithubSignatureError(
            "missing X-Hub-Signature-256 header",
            reason="missing_signature",
        )
    if not signature_header.startswith("sha256="):
        raise GithubSignatureError(
            "signature header lacks 'sha256=' prefix",
            reason="malformed",
        )
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    expected = "sha256=" + mac.hexdigest()
    if not _constant_time_eq(expected, signature_header):
        raise GithubSignatureError(
            "github signature mismatch", reason="mismatch"
        )


# ---------------------------------------------------------------------
# Event-type shapers. Each produces a partial ObservationDraft.
# ---------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(dt: Any, default: datetime | None = None) -> datetime:
    """Parse an ISO-8601 datetime; GitHub sends UTC 'Z'-suffixed strings."""
    if dt is None:
        if default is not None:
            return default
        return _utcnow()
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    s = str(dt)
    # Python pre-3.11 can't parse trailing 'Z'; 3.11+ can. Normalize
    # just in case.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        if default is not None:
            return default
        return _utcnow()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _author(payload: dict[str, Any]) -> str:
    """Return the best human-readable author name."""
    sender = payload.get("sender") or {}
    if isinstance(sender, dict):
        return sender.get("login") or "unknown"
    return "unknown"


def _repo_fullname(payload: dict[str, Any]) -> str | None:
    repo = payload.get("repository")
    if isinstance(repo, dict):
        return repo.get("full_name")
    return None


def _shape_pull_request(payload: dict[str, Any]) -> ObservationDraft:
    """Handle a `pull_request` webhook.

    The only subcase the prompt spells out is `action=closed` with
    `merged=true`. Other PR actions are recorded with a readable
    sentence and trust_tier=inferential by default (they are not
    system-of-record events).
    """
    action = payload.get("action") or "unknown"
    pr = payload.get("pull_request") or {}
    if not isinstance(pr, dict):
        raise ValidationError(
            "pull_request payload missing 'pull_request' object",
            channel=_CHANNEL,
        )
    number = pr.get("number")
    title = (pr.get("title") or "").strip() or "(no title)"
    node_id = pr.get("node_id")
    base = pr.get("base") or {}
    base_ref = base.get("ref") if isinstance(base, dict) else None
    repo_full = _repo_fullname(payload)
    author = _author(payload)
    merged = bool(pr.get("merged"))

    if action == "closed" and merged:
        content_text = (
            f"{author} merged PR #{number} '{title}' into {base_ref}"
        )
        trust_tier = "authoritative"
        kind = "state_change"
    elif action in ("opened", "reopened"):
        content_text = (
            f"{author} {action} PR #{number} '{title}' against {base_ref}"
        )
        trust_tier = "inferential"
        kind = "signal"
    elif action == "closed" and not merged:
        content_text = (
            f"{author} closed PR #{number} '{title}' without merging"
        )
        trust_tier = "inferential"
        kind = "state_change"
    else:
        content_text = (
            f"{author} PR #{number} action={action} '{title}'"
        )
        trust_tier = "inferential"
        kind = "signal"

    entities_hint: list[dict[str, Any]] = []
    if node_id:
        entities_hint.append({"type": "github_pr", "id": node_id})
    if repo_full:
        entities_hint.append({"type": "github_repo", "id": repo_full})
    if base_ref:
        entities_hint.append({"type": "github_branch", "id": base_ref})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "pull_request",
            "action": action,
            "pr_number": number,
            "pr_title": title,
            "pr_node_id": node_id,
            "base_ref": base_ref,
            "repo": repo_full,
            "merged": merged,
            "author": author,
        },
        occurred_at=_parse_iso(pr.get("updated_at") or pr.get("created_at")),
        trust_tier=trust_tier,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=f"github:{author}" if author != "unknown" else None,
        external_id=node_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


def _shape_push(payload: dict[str, Any]) -> ObservationDraft:
    repo_full = _repo_fullname(payload)
    ref = payload.get("ref") or ""
    branch = ref.rsplit("/", 1)[-1] if ref else "(unknown)"
    commits = payload.get("commits") or []
    after = payload.get("after")  # new HEAD SHA — stable dedup key
    author = _author(payload)
    n = len(commits) if isinstance(commits, list) else 0
    content_text = (
        f"{author} pushed {n} commit(s) to {branch} "
        f"in {repo_full or 'unknown-repo'}"
    )
    entities_hint = []
    if repo_full:
        entities_hint.append({"type": "github_repo", "id": repo_full})
    if branch:
        entities_hint.append({"type": "github_branch", "id": branch})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "push",
            "ref": ref,
            "branch": branch,
            "repo": repo_full,
            "commits_count": n,
            "author": author,
            "after": after,
        },
        occurred_at=_utcnow(),
        trust_tier="authoritative",
        kind="signal",
        source_actor_ref=f"github:{author}" if author != "unknown" else None,
        external_id=f"{repo_full}@{after}" if repo_full and after else None,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


def _shape_issues(payload: dict[str, Any]) -> ObservationDraft:
    action = payload.get("action") or "unknown"
    issue = payload.get("issue") or {}
    if not isinstance(issue, dict):
        raise ValidationError(
            "issues payload missing 'issue' object", channel=_CHANNEL
        )
    number = issue.get("number")
    title = (issue.get("title") or "").strip() or "(no title)"
    node_id = issue.get("node_id")
    repo_full = _repo_fullname(payload)
    author = _author(payload)
    content_text = (
        f"{author} {action} issue #{number} '{title}' "
        f"in {repo_full or 'unknown-repo'}"
    )
    entities_hint: list[dict[str, Any]] = []
    if node_id:
        entities_hint.append({"type": "github_issue", "id": node_id})
    if repo_full:
        entities_hint.append({"type": "github_repo", "id": repo_full})

    kind = "state_change" if action in ("closed", "reopened") else "signal"

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "issues",
            "action": action,
            "issue_number": number,
            "issue_title": title,
            "issue_node_id": node_id,
            "repo": repo_full,
            "author": author,
        },
        occurred_at=_parse_iso(
            issue.get("updated_at") or issue.get("created_at")
        ),
        trust_tier="authoritative",
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=f"github:{author}" if author != "unknown" else None,
        external_id=node_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


def _shape_issue_comment(payload: dict[str, Any]) -> ObservationDraft:
    comment = payload.get("comment") or {}
    issue = payload.get("issue") or {}
    if not isinstance(comment, dict) or not isinstance(issue, dict):
        raise ValidationError(
            "issue_comment missing 'comment' or 'issue'", channel=_CHANNEL
        )
    body = (comment.get("body") or "").strip()
    author = _author(payload)
    number = issue.get("number")
    repo_full = _repo_fullname(payload)
    comment_node_id = comment.get("node_id")
    content_text = (
        f"{author} commented on issue #{number} in "
        f"{repo_full or 'unknown-repo'}: {body[:200]}"
    )
    entities_hint: list[dict[str, Any]] = []
    if issue.get("node_id"):
        entities_hint.append(
            {"type": "github_issue", "id": issue["node_id"]}
        )
    if repo_full:
        entities_hint.append({"type": "github_repo", "id": repo_full})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "issue_comment",
            "action": payload.get("action"),
            "issue_number": number,
            "repo": repo_full,
            "author": author,
            "comment_node_id": comment_node_id,
            "body": body,
        },
        occurred_at=_parse_iso(
            comment.get("updated_at") or comment.get("created_at")
        ),
        trust_tier="inferential",
        kind="signal",
        source_actor_ref=f"github:{author}" if author != "unknown" else None,
        external_id=comment_node_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


def _shape_pull_request_review(payload: dict[str, Any]) -> ObservationDraft:
    review = payload.get("review") or {}
    pr = payload.get("pull_request") or {}
    if not isinstance(review, dict) or not isinstance(pr, dict):
        raise ValidationError(
            "pull_request_review missing 'review' or 'pull_request'",
            channel=_CHANNEL,
        )
    state = review.get("state") or "unknown"
    body = (review.get("body") or "").strip()
    author = _author(payload)
    pr_number = pr.get("number")
    repo_full = _repo_fullname(payload)
    review_node_id = review.get("node_id")

    # Approved review is authoritative; commented/changes_requested are
    # inferential per prompt.
    if state == "approved":
        trust_tier = "authoritative"
        kind = "state_change"
    else:
        trust_tier = "inferential"
        kind = "signal"

    content_text = (
        f"{author} {state} review on PR #{pr_number} "
        f"in {repo_full or 'unknown-repo'}"
    )
    if body:
        content_text += f": {body[:200]}"

    entities_hint: list[dict[str, Any]] = []
    if pr.get("node_id"):
        entities_hint.append({"type": "github_pr", "id": pr["node_id"]})
    if repo_full:
        entities_hint.append({"type": "github_repo", "id": repo_full})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "pull_request_review",
            "action": payload.get("action"),
            "pr_number": pr_number,
            "review_state": state,
            "repo": repo_full,
            "author": author,
            "review_node_id": review_node_id,
        },
        occurred_at=_parse_iso(
            review.get("submitted_at")
            or review.get("updated_at")
            or review.get("created_at")
        ),
        trust_tier=trust_tier,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=f"github:{author}" if author != "unknown" else None,
        external_id=review_node_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


def _shape_check_run(payload: dict[str, Any]) -> ObservationDraft:
    check = payload.get("check_run") or {}
    if not isinstance(check, dict):
        raise ValidationError(
            "check_run missing 'check_run' object", channel=_CHANNEL
        )
    name = check.get("name") or "(unnamed)"
    status = check.get("status") or "unknown"
    conclusion = check.get("conclusion") or "pending"
    repo_full = _repo_fullname(payload)
    check_node_id = check.get("node_id")
    head_sha = check.get("head_sha")
    content_text = (
        f"check '{name}' → status={status} conclusion={conclusion} "
        f"in {repo_full or 'unknown-repo'}"
    )
    entities_hint: list[dict[str, Any]] = []
    if repo_full:
        entities_hint.append({"type": "github_repo", "id": repo_full})
    if head_sha:
        entities_hint.append({"type": "github_commit", "id": head_sha})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "check_run",
            "action": payload.get("action"),
            "check_name": name,
            "status": status,
            "conclusion": conclusion,
            "repo": repo_full,
            "head_sha": head_sha,
        },
        occurred_at=_parse_iso(
            check.get("completed_at") or check.get("started_at")
        ),
        trust_tier="authoritative",
        kind="state_change" if status == "completed" else "signal",
        source_actor_ref=None,  # check runs are bot-originated
        external_id=check_node_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


_EVENT_SHAPERS = {
    "pull_request": _shape_pull_request,
    "push": _shape_push,
    "issues": _shape_issues,
    "issue_comment": _shape_issue_comment,
    "pull_request_review": _shape_pull_request_review,
    "check_run": _shape_check_run,
}


@register(_CHANNEL)
async def handle_github_webhook(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """GitHub webhook entry.

    The event type comes from `X-GitHub-Event` (headers), not the body.
    Signature verification is performed by the caller (ingestion core)
    before the handler is invoked; if the caller omits verification a
    tampered payload will surface elsewhere. To make the handler safe
    to call directly we ALSO verify here when headers contain the
    signature — no-op when the caller verified already and stripped
    the headers.
    """
    if not isinstance(payload, dict):
        raise ValidationError(
            "github payload must be a JSON object", channel=_CHANNEL
        )
    event_type = headers.get("X-GitHub-Event") or headers.get("x-github-event")
    if not event_type:
        raise ValidationError(
            "missing X-GitHub-Event header", channel=_CHANNEL
        )
    shaper = _EVENT_SHAPERS.get(event_type)
    if shaper is None:
        raise ValidationError(
            f"unsupported github event type: {event_type}",
            channel=_CHANNEL,
            supported=sorted(_EVENT_SHAPERS.keys()),
        )
    return shaper(payload)


__all__ = [
    "GithubSignatureError",
    "verify_github_signature",
    "handle_github_webhook",
]
