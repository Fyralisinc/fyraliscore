# GitHub Ingestion — How Fyralis Pulls GitHub Data

This document explains, in detail, **how GitHub data enters Fyralis**: which
GitHub REST APIs are called, with which token, and how the GitHub signal set —
**issues, pull requests, issue comments, commits/pushes, PR reviews, and check
runs** — is each ingested.

It deliberately stops at the point where a GitHub event becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope. (The GitHub Intelligence layer
that consumes the resulting observations has been extracted to a separate repo,
`Fyralisinc/github-intel`, and is also out of scope here.)

---

## 1. The two ways data arrives

GitHub data reaches Fyralis through **two independent paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* history via the GitHub **REST API** (`/repos/{owner}/{repo}/…` list endpoints) | `services/ingestion/planners/github.py`, `services/ingestion/fetchers/github.py` |
| **Live (real‑time)** | New activity in a repo | GitHub *pushes* a **webhook** delivery to Fyralis | `services/webhooks/router.py`, `services/webhooks/signatures/github.py`, `services/ingestion/handlers/github.py` |

Crucially, **both paths produce the exact same record shape** — a GitHub webhook
**event body** (`{action, issue|pull_request|…, repository, sender}`) paired with
the event type in an `X-GitHub-Event` header — and both are parsed by the
**single** `github:webhook` handler
([handlers/github.py](../../../services/ingestion/handlers/github.py)). Both
derive the **same** dedup key per event type:

```
issues / pull_request / issue_comment / pull_request_review / check_run
    external_id = <object node_id>          # GitHub's global node id
push
    external_id = "{repo_full_name}@{after}" # repo + new HEAD sha
```

Because GitHub's `node_id` is identical in the REST item and the webhook
payload, a commit/issue/PR that is both backfilled *and* delivered live collapses
into **one** observation. This is the central design invariant of GitHub
ingestion — the backfill fetcher exists precisely to **reshape REST list items
into the webhook event-body shape** so the one handler treats both identically
([fetchers/github.py:7‑20](../../../services/ingestion/fetchers/github.py#L7-L20)).

> GitHub uses **REST only** here (`X-GitHub-Api-Version: 2022-11-28`). No
> GraphQL. Real‑time is HTTP **webhooks**; history is the **REST API**.

---

## 2. Authentication — one token type (GitHub App)

Unlike Slack (which needs two token kinds), GitHub ingestion uses a **single
credential model: the GitHub App**. There are no per‑user OAuth tokens and no
PATs. Everything is read with a **per‑installation access token** minted from
the App's identity.

### 2.1 The App JWT → installation access token flow

1. **App JWT (RS256).** `mint_app_jwt` signs a short‑lived JWT with the App's
   PEM private key
   ([jwt.py:73‑125](../../../services/integrations/github/jwt.py#L73-L125)). The
   payload is GitHub's contract: `{iat: now-30, exp: now+ttl, iss: app_id}`,
   `ttl` capped at GitHub's **600 s** max, `iat` back‑dated 30 s for clock skew.
   The key is read on **every** mint (no cache) so rotation is a no‑op deploy,
   and is **never logged**.
2. **Installation token.** The JWT is sent as `Authorization: Bearer {jwt}` to
   **`POST /app/installations/{installation_id}/access_tokens`**; the `201`
   response yields `{token, expires_at}`
   ([client.py:206‑301](../../../services/integrations/github/client.py#L206-L301)).
3. **In‑process cache.** Tokens are cached per `installation_id` and re‑minted
   when within **60 s** of expiry, guarded by a per‑installation `asyncio.Lock`
   to prevent a mint stampede
   ([client.py:738‑745](../../../services/integrations/github/client.py#L738-L745)).
4. **Read calls** then carry `Authorization: token {installation_token}` against
   the repo endpoints.

### 2.2 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| App ID | `GITHUB_APP_ID` (env) | numeric, string‑typed |
| App private key | `GITHUB_APP_PRIVATE_KEY` (inline PEM) **or** `GITHUB_APP_PRIVATE_KEY_PATH` (file) | exactly one must be set; conflicting/missing → `GithubJWTError` ([jwt.py:46‑57](../../../services/integrations/github/jwt.py#L46-L57)) |
| App slug | `GITHUB_APP_SLUG` (env) | used to build the install‑consent URL |
| Webhook secret | `WEBHOOK_SECRET_GITHUB` (+ `…_PREV` for rotation) | **single App‑level** secret, not per‑install |

> **Contrast with Slack.** Slack stores per‑team / per‑user tokens in the secret
> store. GitHub stores **no token per installation** — the
> `provider_installations.secret_ref` column is explicitly `NULL` for GitHub
> ([oauth.py:347‑349](../../../services/integrations/github/oauth.py#L347-L349)).
> The App private key + the single webhook secret are the only secrets, and
> they are App‑global.

### 2.3 The App‑install flow (how an installation gets registered)

`services/integrations/github/oauth.py` implements GitHub's App‑install
handshake (routes wired in
[integrations/router.py:48‑54](../../../services/integrations/router.py#L48-L54)):

1. **`GET /integrations/github/install`** (Bearer‑authed) — issues an
   HMAC‑signed `state` token bound to the session's `tenant_id` (never a
   client‑supplied param; the state helpers are shared with Slack), then `302`s
   to `https://github.com/apps/{GITHUB_APP_SLUG}/installations/new?state=…`
   ([oauth.py:92‑140](../../../services/integrations/github/oauth.py#L92-L140)).
2. **`GET /integrations/github/callback`** (public, state‑authed) — verifies the
   HMAC, atomically consumes the nonce, then **in one transaction** upserts a
   `provider_installations` row keyed on `(provider='github', installation_id)`
   and emits an `onboarding_triggers` row (install vs reinstall)
   ([oauth.py:147‑233](../../../services/integrations/github/oauth.py#L147-L233)).
   A cross‑tenant rebind is rejected with `installation_collision`, and the
   foreign `tenant_id` never appears in the response, redirect, or logs
   ([oauth.py:369‑397](../../../services/integrations/github/oauth.py#L369-L397)).
3. The callback then seeds **`selected_repositories`** by calling
   `GET /installation/repositories` (`list_installation_repositories`); a `NULL`
   value means **all‑repositories mode**. A fetch failure is non‑fatal — the row
   stays with `selected_repositories=NULL` and an audit row records the unknown
   flag ([oauth.py:235‑310](../../../services/integrations/github/oauth.py#L235-L310)).
4. There is **no `code` exchange** — GitHub Apps don't return a user token here;
   the installation token is minted on demand (§2.1) whenever Fyralis reads.

---

## 3. The GitHub REST API surface that is actually called

All read calls funnel through `GithubClient._get_with_rl_retry`
([client.py:160‑183](../../../services/integrations/github/client.py#L160-L183)),
which:

- sets `Authorization: token {installation_token}` and
  `X-GitHub-Api-Version: 2022-11-28`,
- honours `Retry-After` on **`429`** and **`403` + Retry-After** (secondary /
  abuse limits) within a bounded budget (`GITHUB_RL_MAX_ATTEMPTS`=4,
  `GITHUB_RL_MAX_SLEEP_SEC`=30),
- lets transport errors propagate and maps any non‑2xx to `GithubApiError`.

The endpoints invoked for ingestion:

| GitHub endpoint | Wrapper | Purpose | Code |
|-----------------|---------|---------|------|
| `POST /app/installations/{id}/access_tokens` | `mint_installation_token()` | App JWT → installation token | [client.py:206‑301](../../../services/integrations/github/client.py#L206-L301) |
| `GET /installation/repositories` | `_paginate_installation_repositories()` | enumerate accessible repos (selected **or** all‑repos) | [client.py:303‑439](../../../services/integrations/github/client.py#L303-L439) |
| `GET /repos/{o}/{r}/issues` | `list_repo_events(event_type="issues")` | issue list page | [client.py:457‑516](../../../services/integrations/github/client.py#L457-L516) |
| `GET /repos/{o}/{r}/pulls` | `list_repo_events(event_type="pull_requests")` | PR list page | ″ |
| `GET /repos/{o}/{r}/issues/comments` | `list_repo_events(event_type="issue_comments")` | repo‑wide comment list page | ″ |
| `GET /repos/{o}/{r}/commits` | `list_repo_events(event_type="commits")` | commit list page | ″ |
| `GET /repos/{o}/{r}/pulls/{n}/reviews` | `list_pr_reviews()` | reviews for one PR (fan‑out) | [client.py:571‑620](../../../services/integrations/github/client.py#L571-L620) |
| `GET /repos/{o}/{r}/commits/{ref}/check-runs` | `list_check_runs()` | check‑runs for a commit (fan‑out) | [client.py:622‑675](../../../services/integrations/github/client.py#L622-L675) |
| `GET …` (conditional, `per_page=1`) | `head_repo_events()` | reconciler "did anything change?" probe | [client.py:518‑563](../../../services/integrations/github/client.py#L518-L563) |

The REST `event_type` → collection‑path map lives in `_GH_EVENT_PATH`
([client.py:54‑60](../../../services/integrations/github/client.py#L54-L60)).

### 3.1 Pagination — `page` + Link header

Every list endpoint pages the same way: request `?per_page=100&page=K`, then read
the next page number from the `Link` header's `rel="next"` entry (regex
[client.py:78](../../../services/integrations/github/client.py#L78), parser
[client.py:759‑765](../../../services/integrations/github/client.py#L759-L765)).
When the `Link` header is absent but a full page came back, the client falls back
to `page+1` ([client.py:514‑515](../../../services/integrations/github/client.py#L514-L515)).
End‑of‑data is a short page (`< per_page`) or no `next`.

- `list_repositories_for_backfill` loops **to completion** internally and returns
  the concrete repo list even in org‑wide all‑repos mode, so a large install is
  never silently truncated (bounded only by `GITHUB_MAX_BACKFILL_REPOS`; a cap
  hit is logged, never silent —
  [client.py:379‑392](../../../services/integrations/github/client.py#L379-L392)).
- The list/history endpoints return **one page** plus the next page number to the
  fetcher, which persists it in the shard cursor and resumes next invocation.

Query ordering is chosen for a stable forward scan: issues/pulls use
`state=all&sort=updated&direction=asc`; `issues/comments` uses
`sort=updated&direction=asc`; `commits` takes plain paging (no `state`, default
reverse‑chronological) ([client.py:63‑77](../../../services/integrations/github/client.py#L63-L77)).

### 3.2 ETags

Every list call returns the response `ETag` to the caller and accepts an
`If-None-Match` request header. A `304 Not Modified` short‑circuits to an empty
page. The reconciler relies on this for its fast‑path (§9).

### 3.3 Rate limits

A single client‑side token bucket covers the whole App, sized conservatively for
GitHub's primary 5000/h limit plus secondary (abuse) limits
([rate_limit/buckets.py:88](../../../services/ingestion/rate_limit/buckets.py#L88)):

```
("github", "rest_authenticated"): capacity 4000, refill 1.11/s
```

There is **no per‑method tier** for GitHub (contrast Slack's `SLACK_API_TIER`):
one bucket per app, plus the `Retry-After`‑aware retry in the client.

---

## 4. Backfill scope — the shard families

The planner decomposes one install into **one shard per `(repo, event_type)`**,
all of `shard_kind = "github_repo_events"`
([planners/github.py:79‑125](../../../services/ingestion/planners/github.py#L79-L125)).

Event types split into two fetch classes:

| Class | Event types | REST shape | Fetch path |
|-------|-------------|-----------|------------|
| **A** (repo‑level list) | `issues`, `pull_requests`, `issue_comments`, `commits` | one list endpoint per type | `fetch_page_github` |
| **B** (parent fan‑out) | `pr_reviews` (always on), `check_runs` (opt‑in) | no repo‑level list — enumerate PR parents, drain each parent's children | `_fetch_page_fanout` |

`EVENT_TYPES` is `issues, pull_requests, issue_comments, commits, pr_reviews`;
`check_runs` is opt‑in via **`GITHUB_BACKFILL_CHECK_RUNS=1`** (the highest‑cost /
lowest‑ROI signal — per‑PR‑head fan‑out)
([planners/github.py:63‑76](../../../services/ingestion/planners/github.py#L63-L76)).

### 4.1 Enumerate repos

```python
repos = await ctx.source_client.list_repositories_for_backfill(installation_id)
```

This fully paginates `GET /installation/repositories` and returns the concrete
repo set in **both** selected and all‑repos (org‑wide) mode — so an org‑wide
grant or a >90‑repo selection backfills completely
([planners/github.py:94‑100](../../../services/ingestion/planners/github.py#L94-L100)).
With ~20 repos × 5 always‑on event types ≈ ~100 shards/tenant.

Each shard carries `repo_full_name`, `owner`, `repo`, `event_type`,
`installation_id`, at a baseline `recency_score=1.0`
([planners/github.py:112‑124](../../../services/ingestion/planners/github.py#L112-L124)).

---

## 5. Class A — repo‑level lists (issues, PRs, comments, commits)

`fetch_page_github` ([fetchers/github.py:399‑474](../../../services/ingestion/fetchers/github.py#L399-L474))
fetches one page, advances the cursor, and reshapes each REST item into a webhook
event body.

### 5.1 Cursor

```python
class GithubCursor:
    page: int = 1                       # 1-indexed; advances per page
    etag: str | None = None             # response ETag (used by reconciler)
    last_seen_updated_at: str | None    # newest updated_at seen (gap baseline)
```

([fetchers/github.py:83‑88](../../../services/ingestion/fetchers/github.py#L83-L88)).
`last_seen_updated_at` advances over the **full** page (PRs included) so
pagination and the reconciler baseline stay correct even when items are filtered
out ([fetchers/github.py:453‑457](../../../services/ingestion/fetchers/github.py#L453-L457)).

### 5.2 Reshape REST item → webhook event body (`_build_record`)

The handler consumes the webhook event body and reads the event TYPE from the
`X-GitHub-Event` header — so the fetcher reshapes each REST item into that shape
and injects the header under the reserved **`webhook_metadata`** key (lifted into
the RawEnvelope by the producer and replayed to the handler by the normalizer)
([fetchers/github.py:158‑210](../../../services/ingestion/fetchers/github.py#L158-L210)).
The REST `event_type` → webhook event‑name map is `_GH_EVENT_NAME`
([fetchers/github.py:117‑125](../../../services/ingestion/fetchers/github.py#L117-L125)):
`pull_requests`→`pull_request`, `issue_comments`→`issue_comment`,
`commits`→`push`, etc.

Two reshapes are non‑trivial:

- **`commits` → `push`.** Each commit becomes a single‑commit push body with
  `after=sha` (so `external_id="{repo}@{sha}"`) and `head_commit.timestamp` set
  from the commit's author date — so the backfilled observation keeps its true
  time. The live push's tip SHA collides with the backfilled tip commit (dedups);
  intermediate commits are backfill‑only
  ([fetchers/github.py:169‑188](../../../services/ingestion/fetchers/github.py#L169-L188)).
- **`issue_comments`.** The repo‑wide comments endpoint omits the parent issue
  object, so the fetcher parses the issue number out of the comment's
  `issue_url`; the handler only needs the number for the content sentence, never
  for the dedup key (which is the comment's own `node_id`)
  ([fetchers/github.py:147‑199](../../../services/ingestion/fetchers/github.py#L147-L199)).

### 5.3 The issues‑vs‑PRs guard

GitHub returns **pull requests in the `/issues` stream** (every PR is an issue),
each carrying a `pull_request` key. The `issues` shard filters these out via
`_is_pull_request` so PRs aren't double‑ingested as bogus `issues` observations —
their issue‑endpoint `node_id` is a distinct *issue* id that dedup can't collapse
against the `PullRequest_*` id the `pull_requests` shard sees. The cursor still
advances over the full page so paging stays correct
([fetchers/github.py:432‑445](../../../services/ingestion/fetchers/github.py#L432-L445)).

---

## 6. Class B — fan‑out signals (PR reviews, check runs)

`pr_reviews` and `check_runs` have **no repo‑level list endpoint** — they hang
off a parent. The fan‑out walker `_fetch_page_fanout`
([fetchers/github.py:298‑396](../../../services/ingestion/fetchers/github.py#L298-L396))
enumerates PR parents (via the `pull_requests` list) and drains each parent's
children, doing **exactly one HTTP fetch per call** so the whole walk is
restorable under the N1 invariant.

The cursor is two‑level
([fetchers/github.py:234‑251](../../../services/ingestion/fetchers/github.py#L234-L251)):

```python
class GithubFanoutCursor:
    parent_page: int = 1
    parents_exhausted: bool = False
    parent_queue: list[dict] = []       # pending PR parents
    current_parent: dict | None = None  # parent currently draining
    child_page: int = 1
    last_seen_updated_at: str | None = None
```

- **`pr_reviews`** — parent = PR `number`; children fetched via
  `list_pr_reviews`, reshaped to a `pull_request_review` body with
  `external_id = review.node_id`, timestamp field `submitted_at`
  ([fetchers/github.py:266‑282](../../../services/ingestion/fetchers/github.py#L266-L282)).
- **`check_runs`** — parent = the PR's head `sha`; children fetched via
  `list_check_runs` (a *wrapped* `{total_count, check_runs:[…]}` response the
  client unwraps), reshaped to a `check_run` body with
  `external_id = check.node_id`, no sender (bot‑originated), timestamp field
  `completed_at`. Requires the App's `checks: read` permission
  ([fetchers/github.py:285‑295](../../../services/ingestion/fetchers/github.py#L285-L295)).

---

## 7. The handler — shaping events into `ObservationDraft`

`handle_github_webhook` ([handlers/github.py:516‑546](../../../services/ingestion/handlers/github.py#L516-L546))
reads the event type from `X-GitHub-Event` (**headers, not body**) and dispatches
to one of six shapers ([handlers/github.py:506‑513](../../../services/ingestion/handlers/github.py#L506-L513)).
Each shaper derives `content_text`, `occurred_at`, `source_actor_ref`,
`external_id`, `entities_hint`, a `kind`, and a **trust tier**:

| Event (`X-GitHub-Event`) | Shaper | `external_id` | `occurred_at` | Trust tier |
|--------------------------|--------|---------------|---------------|------------|
| `pull_request` | `_shape_pull_request` | PR `node_id` | PR `updated_at`/`created_at` | **authoritative** if `closed`+`merged`; else inferential |
| `push` | `_shape_push` | `{repo}@{after}` | `head_commit.timestamp` or now | authoritative |
| `issues` | `_shape_issues` | issue `node_id` | issue `updated_at`/`created_at` | authoritative |
| `issue_comment` | `_shape_issue_comment` | comment `node_id` | comment `updated_at`/`created_at` | inferential |
| `pull_request_review` | `_shape_pull_request_review` | review `node_id` | review `submitted_at`/… | **authoritative** if `approved`; else inferential |
| `check_run` | `_shape_check_run` | check `node_id` | `completed_at`/`started_at` | authoritative |

Highlights:

- **PR merge** synthesizes the canonical sentence
  `"{author} merged PR #{n} '{title}' into {base_ref}"` and emits
  `kind=state_change`, `trust_tier=authoritative`
  ([handlers/github.py:174‑232](../../../services/ingestion/handlers/github.py#L174-L232)).
- **`source_actor_ref`** is `github:{login}` (from `sender.login`), or `None`
  for bot‑originated check runs
  ([handlers/github.py:126‑131](../../../services/ingestion/handlers/github.py#L126-L131), [496‑500](../../../services/ingestion/handlers/github.py#L496-L500)).
- **`entities_hint`** carries typed refs — `github_pr` / `github_issue`,
  `github_repo`, `github_branch`, `github_commit` — using `node_id`s and the
  repo full name (e.g. [handlers/github.py:199‑205](../../../services/ingestion/handlers/github.py#L199-L205)).
- **Changed files** for PRs/pushes are gathered into `content.changed_files` /
  `content.files` to drive the GitHub Intelligence blast‑radius layer (now a
  separate repo, `Fyralisinc/github-intel`)
  ([handlers/github.py:172](../../../services/ingestion/handlers/github.py#L172), [253‑267](../../../services/ingestion/handlers/github.py#L253-L267)).
- An unknown `X-GitHub-Event` is rejected with a `ValidationError` listing the
  supported set ([handlers/github.py:539‑545](../../../services/ingestion/handlers/github.py#L539-L545)).

---

## 8. Live (real‑time) ingestion via webhooks

When activity occurs in an installed repo, GitHub **POSTs a webhook delivery** to
Fyralis's webhook edge. Backfill and live both land on the **same** `github:webhook`
handler — the webhook router maps provider `github` → channel `github:webhook`
([webhooks/router.py:350](../../../services/webhooks/router.py#L350)).

### 8.1 Signature verification (HMAC‑SHA256, no timestamp)

The inbound body is verified against GitHub's `X-Hub-Signature-256` header —
`sha256=` + hex `HMAC-SHA256(secret, raw_body)`, constant‑time compared. The
deprecated SHA‑1 `X-Hub-Signature` is **not** accepted. Each active secret is
tried in turn (1–2 during rotation) ([signatures/github.py:35‑83](../../../services/webhooks/signatures/github.py#L35-L83)).
There is **one** implementation; the handler module also exposes
`verify_github_signature` for direct‑call safety
([handlers/github.py:56‑89](../../../services/ingestion/handlers/github.py#L56-L89)).

> **No replay window.** GitHub's signature is over the body alone — there is no
> timestamp envelope (contrast Slack's `v0:{ts}:{body}` + 300 s window). GitHub's
> at‑least‑once retry semantics are made idempotent at the **ingestion layer**
> via `external_id`, not here ([signatures/github.py:8‑11](../../../services/webhooks/signatures/github.py#L8-L11)).

### 8.2 Replay cache + ping (defense in depth)

Two checks run after signature verification, before tenant‑outcome enforcement:

- **`ping`** is answered `200 {"handled":"ping"}` *before* unknown‑installation
  enforcement, since the App's bootstrap ping can precede any customer install
  ([webhooks/router.py:749‑763](../../../services/webhooks/router.py#L749-L763)).
- A **replay cache** keyed on `(installation_id, X-GitHub-Delivery)` — TTL 300 s,
  max 4096 entries
  ([replay_cache.py:19‑20](../../../services/integrations/github/replay_cache.py#L19-L20)) —
  drops duplicate deliveries with `200 {"handled":"replay"}`. This is
  defense‑in‑depth; observation dedup remains the correctness backstop
  ([webhooks/router.py:765‑796](../../../services/webhooks/router.py#L765-L796)).

### 8.3 Tenant resolution

The tenant is resolved from `payload.installation.id` → the
`provider_installations` row for `(provider='github', installation_id=…)`
([tenant_resolver.py:262‑266](../../../services/webhooks/tenant_resolver.py#L262-L266)).
Unknown/disabled installations get `401 unknown_installation` — deferred until
*after* signature verification so a tenant‑id prober sees signature failures
first ([webhooks/router.py:798‑807](../../../services/webhooks/router.py#L798-L807)).

### 8.4 Lifecycle events are not observations

`installation` and `installation_repositories` deliveries are **not** ingested.
They are dispatched (after verification + tenant resolution) to
`services/integrations/github/lifecycle.py` and return `200`
([webhooks/router.py:862‑879](../../../services/webhooks/router.py#L862-L879),
[lifecycle.py:57‑99](../../../services/integrations/github/lifecycle.py#L57-L99)):

| Event / action | Effect |
|----------------|--------|
| `installation.created` | no‑op audit (row already exists from OAuth callback) |
| `installation.deleted` / `.suspend` | disable the install row (`enabled=FALSE`) |
| `installation.unsuspend` | re‑enable the row + invalidate tenant cache |
| `installation_repositories.added` / `.removed` | merge/subtract `selected_repositories` (JSONB; `all` mode → `NULL`) |

### 8.5 Per‑installation repo allowlist

For non‑lifecycle events, if the install pinned an explicit repo list
(`selected_repositories` non‑NULL), an event whose `repository.full_name` isn't
in the list is dropped with `200 {"handled":"filtered_repo"}`. `NULL` = all
repos = no filter ([webhooks/router.py:881‑904](../../../services/webhooks/router.py#L881-L904)).

---

## 9. Reconciliation — gap detection

`reconcile_github` ([reconcilers/github.py:178‑216](../../../services/ingestion/reconcilers/github.py#L178-L216))
re‑checks completed shards for new activity using a **two‑tier** probe per shard
([reconcilers/github.py:100‑175](../../../services/ingestion/reconcilers/github.py#L100-L175)):

1. **ETag fast‑path** — `head_repo_events` issues a conditional `per_page=1` GET
   with the stored ETag; a `304` means nothing changed → clean.
2. **Cursor‑based** — fetch page 1 and compare the newest `updated_at` against
   the stored `last_seen_updated_at`; if newer, there's a gap.

On a gap it reshares a `github_repo_events` shard at **`recency_score=1.5`** with
the cursor reset to `page=1` and the old `last_seen_updated_at` as the baseline.

Only `issues`, `pull_requests`, `issue_comments` are gap‑checkable — they have a
stable ETag/HEAD probe **and** an `updated_at` ordering. `commits` carries no
`updated_at`, and the fan‑out signals (`pr_reviews`, `check_runs`) have no
repo‑level endpoint, so those shards are one‑shot and skipped (not errored)
([reconcilers/github.py:50‑56](../../../services/ingestion/reconcilers/github.py#L50-L56)).

---

## 10. Revocation chokepoint

The outbound client is the single chokepoint that disables an installation on the
documented revocation signals, via `_maybe_disable_on_revocation`
([client.py:681‑731](../../../services/integrations/github/client.py#L681-L731)):

- **`401` `{"message":"Bad credentials"}`**, or
- **`404`** whose `documentation_url` points at `/rest/apps/(apps|installations)`.

Either fires `_disable_installation_github` (idempotent on the row), provided the
`(tenant_id, installation_row_id)` context was registered — by the OAuth callback
([oauth.py:227‑233](../../../services/integrations/github/oauth.py#L227-L233)) or
lazily by the webhook router. Other 4xx/5xx is a plain `GithubApiError` and does
**not** trip the chokepoint (preserves the retry budget). The minted JWT and
installation token are **never logged**; only an `installation_id_hash` is.

---

## 11. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  App JWT (RS256) ─► POST /app/installations/{id}/access_tokens   │
                          │     └─► per-installation access token (cached, 60s pre-expiry)   │
   ALL ACCESSIBLE REPOS   │  planner: GET /installation/repositories (selected OR all-repos) │
                          │     └─► one github_repo_events shard per (repo, event_type)      │
   Class A (lists)        │  fetcher: GET /repos/{o}/{r}/{issues|pulls|issues/comments|commits}
                          │     └─► reshape REST item → webhook event body + X-GitHub-Event  │
   Class B (fan-out)      │  fetcher: per PR → /pulls/{n}/reviews ; per head sha → check-runs │
                          │     └─► reshape review/check → webhook event body                │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
                          ┌──────────────────────── LIVE (push) ──────────────────────────┐│
   ANY repo activity ─────►  GitHub webhook ──HTTP POST──► /webhooks/github                ││
                          │     verify X-Hub-Signature-256 (HMAC-SHA256, no ts)            ││
                          │     ping → 200 ; replay (inst,delivery) → 200 ; repo allowlist ││
                          │     installation* events → lifecycle (NOT observations)        ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_github_webhook           │
                                                            │  event type from X-GitHub-Event  │
                                                            │  external_id = node_id            │
                                                            │     (push → {repo}@{after})       │
                                                            │  → ObservationDraft               │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill reshapes REST items into the
   webhook event‑body shape so `github:webhook` treats backfilled and live events
   identically. A backfilled event and its live twin dedup to one observation via
   `external_id` (`node_id`, or `{repo}@{after}` for pushes).
2. **One credential model.** A single GitHub **App** — JWT → per‑installation
   token — reads everything. No per‑user tokens, no PATs, and `secret_ref` is
   `NULL` for GitHub installs.
3. **Two backfill fetch classes.** Class A (repo‑level lists: issues, PRs,
   comments, commits) pages directly; Class B (PR reviews, check runs) fans out
   over PR parents with a resumable two‑level cursor.
4. **`page` + Link pagination, ETag conditional, bounded retries**, with
   `Retry-After` honoured on 429/secondary‑limit 403.
5. **No webhook replay window** (GitHub signs the body only); idempotency is the
   `external_id` dedup plus a defense‑in‑depth `(installation, delivery)` cache.

---

## 12. Configuration & compliance

Verified against GitHub's official docs (App auth, webhooks, REST pagination,
rate limits).

### 12.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `GITHUB_APP_ID` | — (required) | numeric App ID |
| `GITHUB_APP_SLUG` | — (required for install) | URL‑safe App slug for the consent URL |
| `GITHUB_APP_PRIVATE_KEY` / `…_PATH` | — (exactly one) | RS256 PEM key for the App JWT |
| `WEBHOOK_SECRET_GITHUB` (+ `…_PREV`) | — | App‑level HMAC secret(s) for webhook verification |
| `GITHUB_BACKFILL_CHECK_RUNS` | `0` | `1` adds the opt‑in `check_runs` fan‑out shard family |
| `GITHUB_MAX_BACKFILL_REPOS` | `0` (no cap) | bound on repo enumeration; a cap hit is logged, never silent |
| `GITHUB_RL_MAX_ATTEMPTS` | `4` | rate‑limit retry budget (429 / secondary‑limit 403) |
| `GITHUB_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` |
| `SHARD_FETCH_CONCURRENCY` | `auto` | concurrent independent shard loops; pages within each shard remain serial |
| `SHARD_FETCH_AUTO_CONCURRENCY_MAX` | `32` | backlog-adaptive concurrency ceiling in auto mode; `0` restores unbounded fan-out |

### 12.2 Verified compliant

- **App auth** — RS256 JWT (`iat-30`, `exp` ≤ 600 s, `iss=app_id`) →
  `POST /app/installations/{id}/access_tokens`; token cached + re‑minted pre‑expiry. ✅
- **Webhook signing** — HMAC‑SHA256 `X-Hub-Signature-256`, constant‑time compare,
  SHA‑1 variant rejected. ✅
- **Pagination** — `Link` `rel="next"` everywhere, `per_page=100`. ✅
- **API version pinned** — `X-GitHub-Api-Version: 2022-11-28` on every call. ✅
- **Secondary‑rate‑limit etiquette** — `Retry-After` honoured on 429 and on
  `403 + Retry-After` within a bounded budget. ✅
- **Least secret surface** — single App‑level webhook secret + App private key;
  `provider_installations.secret_ref` is `NULL` for GitHub. ✅

### 12.3 Dev / spammer mode

For local testing against the mock source servers, `build_github_client` detects
spammer mode and **preseeds** the token cache with `spam-gh::{installation_id}`,
skipping the real App‑JWT mint entirely
([_clients.py:110‑132](../../../services/ingestion/fetchers/_clients.py#L110-L132)).
The client's API base then points at the local spammer rather than
`api.github.com`.

> **Known mock gotcha.** The mock GitHub server (`:7003`) returns **422** on
> `/repos/{owner}/{repo}/events`, which fails github backfill runs that hit that
> endpoint; the supported backfill collections are the four Class‑A list paths in
> §3, not `/events`.
```
