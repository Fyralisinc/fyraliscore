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
| **Issue/PR comments** | `issue_comment` / `comment.node_id` | `GET /repos/{o}/{r}/issues/comments?sort=updated` | **repo-level list** | **Class A — this PR** |
| **Commits** | `push` / `{repo}@{after}` (tip SHA) | `GET /repos/{o}/{r}/commits` | **repo-level list** | **Class A — this PR** |
| **PR reviews** | `pull_request_review` / `review.node_id` | `GET /repos/{o}/{r}/pulls/{n}/reviews` | **per-PR fan-out** | **Class B — next** |
| **Check results** | `check_run` / `check.node_id` | `GET /repos/{o}/{r}/commits/{sha}/check-runs` | **per-commit fan-out** | **Class B — next** |

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

### Class B — next PR (pr_reviews + check_runs, fan-out)

6. **Two-level resumable cursor** `GithubFanoutCursor`:
   `{phase, parent_page, parent_queue: [refs], current_parent, child_page,
   last_seen_updated_at}`. Each `fetch_page` call does **one unit**: either
   advance parent enumeration (fetch next page of PRs/commits → fill
   `parent_queue`, return `[]`), or drain one child page for `current_parent`
   (fetch reviews/check-runs → return records). Fully restorable from the cursor
   dict for the N1 invariant.
7. **Client** read methods: `list_pull_numbers(page)`, `list_pr_reviews(n, page)`,
   `list_commit_shas(page)`, `list_check_runs(sha, page)`.
8. **Reshape**: review → `{action:"submitted", review, pull_request:{number,
   node_id}, repository, sender, webhook_metadata}`; check → `{action:"completed",
   check_run, repository, webhook_metadata}`.
9. **Bound check_runs** — per-commit over *all* history is O(commits) API calls
   and will exhaust rate limits. Restrict to PR-head SHAs or commits within the
   backfill window; check_runs is the highest-cost / lowest-ROI signal — consider
   gating it behind an env flag, default off.
10. **Mock client + tests** for the fan-out paths and dedup parity.
