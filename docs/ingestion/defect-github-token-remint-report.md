# Defect Report — GitHub backfill re-mints an installation token on every fetch

| | |
|---|---|
| **ID** | INGEST-GH-TOKEN-REMINT |
| **Severity** | Medium → **must-fix-before-scale** — secondary-rate-limit risk that scales with PR/commit count via the pr_reviews fan-out (not a correctness bug) |
| **Component** | `services/ingestion` — GitHub backfill fetch + reconcile path |
| **Status** | ✅ **RESOLVED** 2026-06-03 — `services/ingestion/fetchers/_clients.py` now memoizes one `GithubClient` per `installation_id` process-wide |
| **Found during** | GitHub backfill+live ingestion test against the local mock (spammer), 2026-06-03 |

---

## Resolution (2026-06-03)

Fixed in `services/ingestion/fetchers/_clients.py`: `build_github_client()` now
returns a process-wide memoized `GithubClient` per `installation_id` (double-checked
lock, mirroring the shared `_get_http()` httpx client), so the in-process
installation-token cache survives across fetches. The planner factory's
explicit-`pool` calls still get a fresh client. The chokepoint / revocation logic
inside `GithubClient` is unchanged.

**Verified (acceptance criteria met):** a fresh real-auth backfill issued
**1 token mint for 671+ repo GETs** (and a single mint for the whole worker
process across the full run), versus ~1 mint per GET before. Backfill output
unchanged (still exact-fidelity counts).

---

## Summary

During GitHub backfill, Fyralis mints a fresh GitHub App **installation access
token** (`POST /app/installations/{id}/access_tokens`) before **nearly every**
REST call, instead of reusing a cached token. In the logs this shows up as a
`POST .../access_tokens` immediately preceding almost every `GET /repos/...` —
hundreds of redundant mints for a ~12.6k-object backfill across 32 repos.

Backfill output is **correct and complete** (verified exact-fidelity against the
source per event type); this is purely an efficiency / load defect.

---

## Root cause

`GithubClient` already has a correct in-process token cache, keyed by
`installation_id`, that only re-mints when the token is near expiry:

- `services/integrations/github/client.py` → `mint_installation_token()` (~L206–301)
- freshness check `_is_fresh()` (~L738–745), re-mint window 60 s before expiry

**But the backfill fetcher builds a NEW `GithubClient` per fetch call**, so the
cache is empty every time → every fetch is a cache miss → re-mint:

- `services/ingestion/fetchers/github.py` → `_open_github_client()` (~L93–99)
- → `services/ingestion/fetchers/_clients.py` → `open_github_client()` /
  `build_github_client()` (~L110–132) constructs a fresh `GithubClient` on each call

The HTTP connection pool **is** shared process-wide (`_get_http()`), but the
`GithubClient` wrapper — which owns the token cache (`_installation_tokens`) — is
**not**. So the expensive-to-create, cheap-to-reuse thing (the token) is the one
thing thrown away on each fetch.

The same per-call construction exists on the reconcile path:
`services/ingestion/reconcilers/github.py` → `_open_github_client()`.

> **Note on environments:** in spammer-mode (`SYNTHETIC_SOURCE_API_BASE` set) the
> token is preset to `spam-gh::<inst>`, so no mint happens and the storm is
> hidden. It only occurs under **real App-JWT auth** — i.e. production, and
> real-auth runs against the mock.

---

## Impact

- Hundreds of unnecessary installation-token mints per backfill run.
- Each mint is an extra HTTP round-trip on the critical fetch path → slower backfill.
- Heavier load on GitHub's installation-token endpoint. Under that volume the
  local mock intermittently returned a **404 on the mint endpoint**; the more
  the endpoint is hammered, the more likely a transient blip. Real GitHub has its
  own limits on this endpoint.
- Self-healing but wasteful: the shard fetch loop catches per-shard errors, marks
  the shard `failed`, and the orphan-scan retries it — so a transient mint failure
  does not lose data, but the redundant mint traffic is the underlying amplifier
  of those transient failures.

---

## Reproduction / evidence

1. Run a real-auth GitHub backfill (not spammer-mode) over multiple repos.
2. Tail the `shard_fetch` worker log.
3. Observe a `POST .../app/installations/{id}/access_tokens` line immediately
   before almost every `GET /repos/{owner}/{repo}/...` line.

Observed in the 2026-06-03 run: a `201 access_tokens` mint preceding nearly
every repo GET, across the full 32-repo / ~12.6k-object backfill.

---

## Recommended fix

Share the installation-token cache (or the whole `GithubClient`) **process-wide**
across backfill fetches/reconciles, keyed by `installation_id` — mirroring how the
HTTP client is already shared via `_get_http()`.

Simplest concrete option: in `services/ingestion/fetchers/_clients.py`, memoize a
single `GithubClient` per `installation_id` (or hold one process-wide client and
reuse it) instead of building a new one on each `open_github_client()` call. Apply
the same to the reconciler opener.

---

## Trade-off to decide consciously

Sharing the token cache slightly **slows revocation propagation**:

- **Today:** per-fetch re-mint means a suspended/revoked install is noticed almost
  immediately.
- **After fix:** a revoked install could keep using a cached token until it expires
  — bounded by GitHub's **~1 h installation-token TTL**.

This is backstopped and **safe**: the client's chokepoint disables the install row
the moment any call returns `401 "Bad credentials"` or `404 apps-not-found`
(`services/integrations/github/client.py` → `_maybe_disable_on_revocation`,
~L681–731). Worst case is "one install keeps reading for up to its token TTL," not
"revocation ignored." **Keep the chokepoint exactly as-is.**

---

## Acceptance criteria

- A multi-repo backfill issues **~1 token mint per installation per ~hour** (token
  TTL), not one per fetch. Verify by counting `POST .../access_tokens` in the
  `shard_fetch` logs for a multi-shard run — should drop from hundreds to single
  digits.
- Revocation still disables the install promptly via the chokepoint (existing
  behavior unchanged). Keep/add a test asserting a 401/404 on a shared-client call
  still fires `_disable_installation_github`.
- No change to backfill output (still exact-fidelity counts per event type).

---

## Risk / rollback

**Low.** The change is isolated to client construction in the fetch/reconcile
openers; the token cache + chokepoint logic inside `GithubClient` is unchanged.
Rollback = revert the memoization.

Watch for: token-cache async-safety across concurrent fetches in one worker.
`GithubClient` already uses per-installation `asyncio.Lock`s for minting, so this
is safe within a single event loop; each worker process keeps its own cache.

---

## Related (context, not part of this defect)

A separate, already-fixed packaging defect in the same area: the generic
`WORKFLOW_SERVICE=shard_fetch` dispatcher did not wire the raw-tier S3 client
(only the dedicated `python -m services.ingestion.workflows.shard_fetch`
entrypoint did), so backfill crashed with `requires an S3Client (A27.1)`. Fixed in
`services/ingestion/workflows/__main__.py` (the generic branch now builds, connects,
and tears down the S3 client). Mentioned here only so the two are not conflated.
