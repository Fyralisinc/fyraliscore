# GitHub Intelligence — Read API

Read-only HTTP surface over the GitHub Intelligence Layer, mounted on the gateway
at `/github-intel/*` (`services/github_intel/api.py`, registered in
`services/gateway/main.py`). It exposes FSM state, the enriched-signal feed,
per-signal causal explanations, PR detail, code blast-radius, and code-RAG search.

## Auth & authorization
- **Bearer token** (same as all gateway reads): `Authorization: Bearer <session-token>`.
  Missing/invalid → `401 {"error":"unauthorized"}`.
- **Per-tenant repo allowlist**: a repo is visible to a tenant if it is covered by a
  github `provider_installations` row (NULL/empty `selected_repositories` = all repos),
  OR the tenant already holds intelligence for it. An unauthorized repo →
  `404 {"error":"repo_not_found"}` (never leaks existence).
- All reads are RLS-scoped (`tenant_transaction`). Repos are addressed as
  `{owner}/{repo}` path segments.

## Endpoints

| Method & path | Description |
|---|---|
| `GET /github-intel/repos?limit=` | Repos this tenant has intelligence for (+ signal/symbol counts). |
| `GET /github-intel/repos/{owner}/{repo}/state` | Current FSM state: repo HEAD/default branch, code-index summary, PRs, issues, branches. |
| `GET /github-intel/repos/{owner}/{repo}/signals?limit=&before=&event_type=` | Enriched signal feed, newest-first; `before` (ISO ts) cursor → response `next_before`. |
| `GET /github-intel/repos/{owner}/{repo}/prs?state=&ci=&limit=` | PR list; `state` filters lifecycle, `ci` filters ci_state. |
| `GET /github-intel/repos/{owner}/{repo}/prs/{pr_number}` | One PR: state + enrichment timeline (`404 pr_not_found` if absent). |
| `GET /github-intel/repos/{owner}/{repo}/blast-radius?path=...&max_hops=` | Dependent files/symbols for changed `path`(s) (repeatable). `indexed:false` if no snapshot. |
| `GET /github-intel/repos/{owner}/{repo}/code-search?q=&k=` | Semantic code-RAG; `results:[]` if embedder unavailable or repo not indexed. |
| `GET /github-intel/signals/{observation_id}/explain` | Full per-signal context: inline `intelligence` block + structured `enrichment` row. |

Envelopes: lists return `{"<plural>": [...], "count": N}`; singles return the object;
errors return `{"error": "<code>", ...}` with the matching status.

## Examples (against the demo tenant after `scripts/demo_github_intel.py`)

```bash
TOKEN=<bearer>        # POST /auth/session, or a demo session token
BASE=http://localhost:8000
REPO=acme/intelligence-demo

curl -s -H "Authorization: Bearer $TOKEN" "$BASE/github-intel/repos"
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/github-intel/repos/$REPO/state"
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/github-intel/repos/$REPO/prs?state=merged"
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/github-intel/repos/$REPO/blast-radius?path=app/db.py"
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/github-intel/repos/$REPO/code-search?q=verify+token"
```

Sample `/state` (abridged):
```json
{
  "repo": "acme/intelligence-demo",
  "default_branch": "main", "head_sha": "m2m2m2m2",
  "code_index": {"indexed": true, "symbol_count": 12, "file_count": 5, "edge_count": 18},
  "pull_requests": [{"pr_number": 42, "lifecycle": "merged", "ci_state": "passing", "merged": true}],
  "issues": [{"issue_number": 12, "status": "closed"}]
}
```

Sample `/blast-radius?path=app/db.py`:
```json
{
  "repo": "acme/intelligence-demo", "indexed": true, "commit_sha": "m2m2m2m2",
  "changed_files": ["app/db.py"],
  "dependent_files": [
    {"path": "app/auth.py", "hops": 1}, {"path": "app/api.py", "hops": 1},
    {"path": "app/ratelimit.py", "hops": 1}, {"path": "app/main.py", "hops": 2}
  ]
}
```

## Tests
`services/gateway/tests/test_github_intel_endpoints.py` — integration tests over the
real gateway app (`client` + `valid_session` fixtures): every endpoint, the
`{owner}/{repo}` reassembly, 401 unauth, 404 unauthorized-repo / unknown PR / unknown
signal, 400 bad params, and the blast-radius / code-search paths.
