# GitHub Intelligence — Browser UI

A read-only React panel over the [read API](API.md), at the route **`/github`**.
It's a single self-contained page (`ui/src/pages/GitHubIntel.tsx`) with internal
view state — no react-router sub-routes — so it's easy to run and test.

## What it shows
- **Repos** — every repo the tenant has intelligence for (signal count, indexed sha, symbol count).
- **Repo → State** — summary cards (default branch/HEAD, code-index size, open PRs) + PR table
  (lifecycle + CI pills), issues, branches.
- **Repo → Signals** — the enriched signal feed; click a signal → **explain panel**: cause / effect /
  why, before→after state, confidence, changed files, and the **blast radius** (dependents).
- **Repo → Blast radius** — type a changed file path → dependent files + symbols.
- **Repo → Code search** — semantic code-RAG over the indexed snapshot.

## How to run it in the browser

The browser **demo picker provisions a fresh per-session tenant**, which does NOT see data seeded
under the dogfood tenant. So you authenticate the panel with a token bound to the tenant that holds
the data (a one-time paste in the page's token bar).

```bash
# 0. infra up: postgres (:5434), ollama (:11434)
export DATABASE_URL=postgresql://company_os:company_os@localhost:5434/company_os
export COMPANY_OS_TENANT_ID=00000000-0000-0000-0000-000000000001
export PYTHONPATH=$PWD

# 1. seed the demo repo + signals (once)
python scripts/demo_github_intel.py

# 2. start the gateway FROM SOURCE so it has the /github-intel routes.
#    CAVEAT: the docker-compose `company_os_gateway` container holds host :8000
#    with a PREBUILT image — if it predates this feature it 404s these routes.
#    Either rebuild it (docker compose build gateway && docker compose up -d gateway)
#    or run from source on another port and point the UI at it (shown here):
MASTER_KEK=dev-kek KAFKA_BOOTSTRAP_SERVERS="" \
  .venv/bin/uvicorn services.app.gateway.main:app --host 127.0.0.1 --port 8011

# 3. start the UI dev server, proxied at that gateway:
cd ui && npm install
GATEWAY_URL=http://127.0.0.1:8011 npm run dev      # http://localhost:5173
#   (omit GATEWAY_URL to use the default http://localhost:8000)

# 4. mint a token bound to the seeded tenant
python scripts/github_intel_dev_session.py
#    -> prints the URL (http://localhost:5173/github) + a bearer token

# 5. open http://localhost:5173/github, paste the token in the top bar, click Connect.
```

In production the panel ships in the same `ui` bundle (`Dockerfile.ui` → nginx), and `/api` is
routed to the gateway; a normally-authenticated session needs no token paste.

## How it's wired (reuses existing conventions)
- `ui/src/api/github-intel-client.ts` — typed wrappers over `/github-intel/*`, mirroring the
  private `request<T>()` pattern in `ui/src/api/client.ts`: BASE `/api` → vite proxy strips `/api`
  → gateway; bearer token from `localStorage["demoAuthToken"]`; 401 → redirect to `/demo`.
- `setDemoAuthToken()` added to `ui/src/api/auth.ts`. Pasting a token sets it and clears
  `demoTenantId` so `authHeaders()` doesn't send a stale `X-Tenant-Id` (the gateway 403s on mismatch).
- Route added in `ui/src/main.tsx`; `gi-*` styles appended to `ui/src/index.css` using the existing
  design tokens; `ui/vite.config.ts` reads `GATEWAY_URL` for the dev proxy.

## Verified
`npm run build` (tsc strict + vite) is green and the panel is bundled. Driven headless (Playwright,
chromium) against a live vite → source-gateway with a real token — all assertions pass and
screenshots confirm: repos list (acme/intelligence-demo); State (PR #42 **merged**/CI **passing**,
issue #12 **closed**); signal **explain** (cause/effect + blast radius); Blast-radius for
`app/db.py` → `app/auth.py`, `app/api.py`, `app/ratelimit.py`, `app/main.py`.
