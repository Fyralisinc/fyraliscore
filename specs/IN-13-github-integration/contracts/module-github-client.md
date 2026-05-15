# Module Contract: `services/integrations/github/client.py` (`GithubClient`) and `jwt.py`

Outbound GitHub REST surface. Two distinct modules; the client depends on the JWT minter.

## `services/integrations/github/jwt.py`

### Public surface

```python
def mint_app_jwt(*, app_id: str | None = None, now: float | None = None) -> str:
    """Mint a 10-minute App JWT signed RS256 with the App's private key.

    Reads from env on EVERY call (no in-process cache):
      - GITHUB_APP_ID (or `app_id` parameter override for tests)
      - Exactly one of GITHUB_APP_PRIVATE_KEY (multi-line PEM) OR
        GITHUB_APP_PRIVATE_KEY_PATH (file path)

    Returns the encoded JWT string. The payload:
      {iat: now-30, exp: now+600, iss: app_id}
      (-30s skew tolerance per GitHub recommendation)

    Raises:
      GithubJWTError(reason='no_app_id')         — GITHUB_APP_ID missing
      GithubJWTError(reason='no_private_key')    — neither env var set
      GithubJWTError(reason='conflicting_keys')  — both env vars set
      GithubJWTError(reason='malformed_key')     — PEM parse failure
      GithubJWTError(reason='io_error')          — file path unreadable
    """
```

### Error class

```python
class GithubJWTError(CompanyOSError):
    default_code = "github_jwt_error"
    # context: {reason: str}
```

### Invariants

- The function MUST NOT print, log, or otherwise emit the private key material at any level. Test `test_jwt_github.py::test_private_key_never_logged` asserts this.
- Rotation is transparent: changing `GITHUB_APP_PRIVATE_KEY` between calls (without process restart) means the next call mints with the new key. Test `test_jwt_github.py::test_rotation_transparent` asserts this.
- Parse cost ≤ 5 ms per call (P50; CPython 3.12 on dev hardware). Performance ceiling, not a tight budget — exceeded performance triggers a research-task in a follow-up.

## `services/integrations/github/client.py`

### Public surface

```python
@dataclass(frozen=True)
class GithubClientDeps:
    pool: asyncpg.Pool
    secret_store: SecretStore       # from lib/shared/secrets — used for the chokepoint's secret-store invalidation calls
    http: httpx.AsyncClient
    clock: Callable[[], float] = time.time
    api_base_url: str = "https://api.github.com"  # GHES override is a future-work param


class GithubClient:
    def __init__(self, deps: GithubClientDeps) -> None: ...

    async def mint_installation_token(self, installation_id: str) -> str:
        """Return a valid installation access token for `installation_id`.

        Cache hit (current entry not within 60 s of expiry): return cached.
        Cache miss / near-expiry: mint via POST /app/installations/{id}/access_tokens,
        cache the (token, expires_at), return it.

        On HTTP 401 with body.message='Bad credentials' OR
        HTTP 404 with body.documentation_url matching the apps-not-found pattern,
        invokes the uninstall chokepoint (FR-012) and raises GithubApiError.

        On other 4xx/5xx, raises GithubApiError without chokepoint.
        """

    async def list_installation_repositories(self, installation_id: str) -> list[str] | None:
        """Return the `<owner>/<repo>` list for the installation, OR None
        if the installation is in 'all-repos' mode (response's
        `total_count` matches the org's full repo count AND the API
        signals `repository_selection='all'`).

        Reads up to 3 pages (90 repos) per R8; truncation is signalled
        via a structured warning log and a return-value annotation
        (the caller can inspect `self.last_repos_truncated`).

        Same chokepoint trigger conditions as mint_installation_token.
        """

    async def close(self) -> None:
        """Close the underlying httpx client. Called by the app lifespan."""
```

### Token cache

- Per-instance `dict[str, CachedInstallationToken]` keyed on `installation_id`.
- Eviction:
  - On TTL expiry (next call sees expiry-within-60-s and re-mints).
  - On `_disable_installation_github` invocation (chokepoint clears the entry).
- Concurrent miss for the same `installation_id`: serialized via per-installation `asyncio.Lock` (R risk #3). One Lock per active installation_id; evicted with the cache entry.

### Error class

```python
class GithubApiError(CompanyOSError):
    default_code = "github_api_error"
    # context: {reason, status, github_code, installation_id_hash}
    # reason ∈ {'unauthorized', 'not_found', 'rate_limited', 'server_error', 'bad_request', 'malformed_response'}
```

### Chokepoint integration

The client is the ONLY outbound call site for v1 (per Out of Scope (b) — no product-feature outbound). The chokepoint trigger is co-located here:

```python
async def _maybe_disable_on_revocation(
    self,
    installation_id: str,
    response: httpx.Response,
) -> None:
    """Check the response shape; if it matches the documented
    revocation signals (R2), invoke _disable_installation_github
    exactly once for this Python coroutine (idempotent on the DB row).
    """
```

The check matches **exactly two** response patterns per R2:
- `status_code == 401 AND body.message == 'Bad credentials'`
- `status_code == 404 AND body.documentation_url matches /rest/apps/(apps|installations)`

Any other 4xx/5xx is a regular `GithubApiError`; no chokepoint, no state change.

### Metrics

- `github_installation_token_mint_total{result}` per FR-017.
- `github_outbound_request_total{path,status}` — low-cardinality (path is the GitHub endpoint pattern, not the full URL).
- `github_outbound_chokepoint_total{reason}` where reason ∈ `bad_credentials|installation_not_found`.

### Logging

- `installation_id_hash` (BLAKE2b 8-byte) instead of raw `installation_id`.
- The minted JWT and installation access token are **NEVER** logged at any level (FR-016 / SC-008).
- The private key is **NEVER** logged at any level.
- Verified by `test_client_github.py::test_no_secrets_in_logs`.

### Concurrency

- One `GithubClient` instance per gateway pod, instantiated in the app lifespan.
- Internal state is `dict + asyncio.Lock` per-installation; no module-level globals.
- `close()` is called from the lifespan teardown.

### Tests

Co-located in `services/integrations/tests/test_client_github.py`. Use `respx` to mock `api.github.com`. Each test runs against real Postgres for the chokepoint's row-update path.

Test coverage matrix:

| Test | Scenario |
|---|---|
| `test_mint_caches_within_ttl` | Two mint calls within TTL → 1 POST |
| `test_mint_remints_near_expiry` | Mint at T0; mock returns expiry T0+30s; call at T0+10s → re-mint |
| `test_401_bad_credentials_triggers_chokepoint` | Outbound 401 + body → chokepoint fires once + GithubApiError raised |
| `test_404_apps_documentation_triggers_chokepoint` | Outbound 404 with doc_url → chokepoint + GithubApiError |
| `test_404_other_documentation_does_not_trigger` | Outbound 404 with unrelated doc_url → GithubApiError but NO chokepoint |
| `test_429_retry_within_budget` | (Future-work) — 429 path is not exercised in v1 because no product-feature outbound; documented as deferred |
| `test_list_repos_pagination` | 3 pages, 90 repos; 4th page → truncation flag set |
| `test_list_repos_all_mode` | response says `repository_selection='all'` → return None |
| `test_concurrent_mint_serialized` | Two concurrent `mint_installation_token` for same installation_id → 1 POST |
| `test_no_secrets_in_logs` | Capture structlog; assert no PEM, no JWT, no token strings appear |
