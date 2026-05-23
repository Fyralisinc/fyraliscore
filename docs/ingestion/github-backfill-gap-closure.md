# GitHub backfill gap-closure — spec & tasks

> Goal: bring the **backfill** path up to parity with the **live** path for the
> mandatory CompanyOS GitHub signal set. Live already handles 6 event types;
> backfill today reconstructs only 2 (issues, pull_requests). This closes the
> other 4 — **issue/PR comments, commits, PR reviews, check results** — with
> dedup-key alignment so a backfilled record and its live webhook twin collapse
> to one observation.

## Background: the two-path contract

- **Live**: `services/ingestion/handlers/github.py` — `_EVENT_SHAPERS` dispatches
  6 webhook event types on the `X-GitHub-Event` header. Each shaper derives an
  `external_id` (almost always the object's `node_id`).
- **Backfill**: `services/ingestion/planners/github.py` enumerates one shard per
  `(repo, event_type)`; `services/ingestion/fetchers/github.py` pages the REST
  list endpoint and **reshapes each REST item into the webhook event-body shape**
  (`{action, <obj>, repository, sender, webhook_metadata:{X-GitHub-Event}}`) so it
  flows through the *same* live handler. Dedup is enforced by
  `UNIQUE(source_channel, external_id)` on `observations`.

The reshape must mint the **same `external_id`** the live handler would, or the
two paths double-count.

## Signal coverage & dedup-key alignment

| Signal | Live event / external_id | REST backfill endpoint | Endpoint shape | Status |
|---|---|---|---|---|
| Issues | `issues` / `issue.node_id` | `GET /repos/{o}/{r}/issues?state=all` | repo-level list | **done** |
| Pull requests | `pull_request` / `pr.node_id` | `GET /repos/{o}/{r}/pulls?state=all` | repo-level list | **done** |
| **Issue/PR comments** | `issue_comment` / `comment.node_id` | `GET /repos/{o}/{r}/issues/comments?sort=updated` | **repo-level list** | **done (Class A)** |
| **Commits** | `push` / `{repo}@{after}` (tip SHA) | `GET /repos/{o}/{r}/commits` | **repo-level list** | **done (Class A)** |
| **PR reviews** | `pull_request_review` / `review.node_id` | `GET /repos/{o}/{r}/pulls/{n}/reviews` | **per-PR fan-out** | **done (Class B)** |
| **Check results** | `check_run` / `check.node_id` | `GET /repos/{o}/{r}/commits/{sha}/check-runs` | **per-commit fan-out** | **done (Class B, opt-in)** |

All six mandatory signals are now backfilled at dedup parity with live.
`check_runs` is opt-in via `GITHUB_BACKFILL_CHECK_RUNS=1` (default off) and fans
out over **PR-head SHAs** (bounded by PR count), not all commits.

### Two classes of endpoint

- **Class A (repo-level list)** — `issue_comments`, `commits`. Fit the existing
  fetcher model exactly: plain offset/Link paging, one shard per `(repo,
  event_type)`, no fan-out. Low risk.
- **Class B (nested fan-out)** — `pr_reviews`, `check_runs`. No repo-level list
  endpoint exists; you must enumerate a parent collection (PRs / commits) and
  page each parent's children. Needs a resumable two-level cursor.

## Dedup notes (load-bearing)

- **Comments**: the repo-level `/issues/comments` endpoint returns the comment
  object with `node_id` (parity key) but **no embedded issue object** — only an
  `issue_url`. The live `_shape_issue_comment` uses `issue.node_id` *only* for an
  optional `entities_hint`; `external_id` is `comment.node_id`. So reshape with
  `issue = {"number": <parsed from issue_url>}` — parity holds, the issue
  entity-hint is simply absent on backfill. Acceptable.
- **Commits ↔ push**: live emits **one** observation per push, keyed by the push
  *tip* SHA (`{repo}@{after}`). Backfill emits **one per commit**, keyed
  `{repo}@{sha}`. The tip commit's key collides with the live push → dedups;
  intermediate commits are backfill-only (no live twin), which makes backfill
  *more* complete, not double-counted. To preserve this, reshape each commit as a
  single-commit `push` body with `after = commit.sha`.
  - **Timestamp fix required**: `_shape_push` currently stamps `occurred_at =
    now()` (the live push event carries no top-level time in our shaper). For
    backfill that destroys the real commit date. Fix: have `_shape_push` read
    `head_commit.timestamp` (present on real push events *and* injectable by the
    backfill reshape from `commit.commit.author.date`), falling back to `now()`.
- **Reviews / checks**: `review.node_id` / `check.node_id` are present in their
  REST objects and identical to the webhook payload → direct parity.

## GitHub App permission deltas

Current App scope: `issues`, `pull_requests`, `metadata` (read). Add:

| Signal | Permission needed | Notes |
|---|---|---|
| Issue/PR comments | (covered by `issues` + `pull_requests` read) | no new perm |
| Commits | **`contents: read`** | required for `/commits` |
| PR reviews | (covered by `pull_requests` read) | no new perm |
| Check results | **`checks: read`** | required for `/commits/{sha}/check-runs` |

## Tasks

### Class A — this PR (issue_comments + commits)

1. **Client** (`services/integrations/github/client.py`)
   - Extend `_GH_EVENT_PATH`: add `issue_comments → "issues/comments"`,
     `commits → "commits"`.
   - Generalize `list_repo_events` query string per event_type (issues/pulls keep
     `state=all&sort=updated&direction=asc`; comments use `sort=updated&
     direction=asc`; commits take no state/sort). Keep `head_repo_events`
     unchanged (reconciler only probes issues/pull_requests).
2. **Handler** (`services/ingestion/handlers/github.py`)
   - `_shape_push`: read `head_commit.timestamp` → `commits[-1].timestamp` →
     `now()` for `occurred_at`. No behavior change for live (falls back to now()).
3. **Fetcher** (`services/ingestion/fetchers/github.py`)
   - Expand the accepted `event_type` set + `_GH_EVENT_NAME`
     (`issue_comments→issue_comment`, `commits→push`).
   - `_build_record`: branch per event_type to build the right body
     (comment+parsed-issue-number; commit→single-commit push w/ `after=sha` and
     injected `head_commit.timestamp`).
   - Generalize the `last_seen_updated_at` extraction (`updated_at` for
     issues/pulls/comments; `commit.author.date` for commits).
4. **Planner** (`services/ingestion/planners/github.py`)
   - Extend `EVENT_TYPES` → `("issues", "pull_requests", "issue_comments",
     "commits")`. (~4 shards/repo; ~80/tenant at 20 repos — within the ~250 target.)
5. **Tests** — fetcher reshape per new type, planner shard count, push timestamp.

### Class B — DONE (pr_reviews + check_runs, fan-out)

Implemented as shipped:

6. **Two-level resumable cursor** `GithubFanoutCursor`
   (`services/ingestion/fetchers/github.py`):
   `{parent_page, parents_exhausted, parent_queue:[entries], current_parent,
   child_page, last_seen_updated_at}`. Each `_fetch_page_fanout` call does
   **one HTTP fetch** — advance PR enumeration (fill `parent_queue`, return `[]`)
   or drain one child page for `current_parent` (return records) — fully
   restorable from the cursor dict for the N1 invariant.
7. **Parents are always PRs** (reusing `list_repo_events(event_type=
   "pull_requests")`). For `pr_reviews` the parent entry carries `{number,
   node_id}`; for `check_runs` it carries `{sha}` from `pr.head.sha`.
8. **Client** read methods added: `list_pr_reviews(pull_number, page)`,
   `list_check_runs(ref, page)` (unwraps the `{check_runs:[...]}` envelope).
9. **Reshape**: `_build_review_record` → `{action:"submitted", review,
   pull_request:{number,node_id}, repository, sender, webhook_metadata}`;
   `_build_check_run_record` → `{action:"completed", check_run, repository,
   webhook_metadata}`.
10. **check_runs bounded** to PR-head SHAs (O(PRs), not O(commits)) and gated by
    `GITHUB_BACKFILL_CHECK_RUNS=1` (default off). `pr_reviews` is always on.
11. **Reconciler guard**: `_RECONCILABLE_EVENT_TYPES = {issues, pull_requests,
    issue_comments}` — commits (no `updated_at`) and fan-out shards (no
    repo-level endpoint) are skipped rather than KeyError'd; their backfill is
    one-shot.
12. **Mock client** gained `list_pr_reviews` / `list_check_runs`; parity +
    fan-out + gating tests added (fetcher, planner, normalizer parity suites).

### Follow-up (not in scope)

- `pull_request_review_comment` (inline review comments) and `commit_comment`
  remain tier-2 — they need *new live shapers* (`_EVENT_SHAPERS`) before backfill
  parity is meaningful.
- App permission deltas still required operationally: `contents:read` (commits),
  `checks:read` (check_runs).
