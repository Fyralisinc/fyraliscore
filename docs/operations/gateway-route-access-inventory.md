# Gateway Route Access Inventory

This inventory records the current transport-level access boundary for Fyralis
gateway routes. It is the production-readiness baseline for deciding which
routes are public, self-authenticated, provider-signed, gateway bearer-auth,
extension-authenticated, debug-only, or internal-only.

Authoritative generated inventory:

```bash
.venv/bin/python scripts/audit_gateway_route_access.py --check
.venv/bin/python scripts/audit_gateway_route_access.py --format markdown
.venv/bin/python scripts/audit_gateway_route_access.py --format json
```

The audit command builds the static gateway route table without running lifespan
startup. As of this inventory slice, the production-default static route table
contains 126 routes:

| Access class | Count | Boundary |
| --- | ---: | --- |
| `bearer-auth` | 101 | Gateway actor-session bearer token, tenant bound in middleware. |
| `admin-only` | 3 | Gateway actor-session bearer token plus tenant-scoped admin/operator authorization in route code. |
| `extension-auth` | 7 | Extension OAuth bearer token plus `X-Fyralis-Tenant` grant checks. |
| `self-authenticated` | 5 | Endpoint-specific auth such as bootstrap secret or OAuth state token. |
| `provider-signed` | 4 | Provider webhook signature, verification token, or equivalent check. |
| `internal-only` | 3 | Internal API; currently gateway bearer plus deployment boundary. |
| `public` | 3 | Health, readiness, and metrics only. |

## Route Family Classification

| Route family | Current access | Production requirement |
| --- | --- | --- |
| `/healthz`, `/readyz`, `/metrics` | `public` | Must not return tenant data, raw errors, secrets, or per-tenant labels. |
| `/auth/session` | `self-authenticated` | Must require `AUTH_BOOTSTRAP_SECRET` in production and reject tenant header/body mismatch. |
| `/api/admin/dead-letters*` | `admin-only` plus operator audit | Must require gateway actor auth, tenant-scoped admin role, sanitized output, and `operator_action_log` for list/retry/quarantine actions. |
| `/integrations/*/install` | `bearer-auth` | Must require an authenticated actor before creating an OAuth install flow. |
| `/integrations/*/callback`, `/integrations/*/installed`, `/integrations/*/install-error` | `self-authenticated` | Must validate provider state token and never rely on a default tenant in production. |
| `/integrations/whatsapp/webhook` | `provider-signed` | Must validate Meta verification token or `X-Hub-Signature-256`; no gateway bearer token required. |
| `/webhooks/{provider}` and `/webhooks/{provider}/{subpath}` | `provider-signed` | Must validate source-specific webhook signatures before resolving tenant or ingesting payloads. |
| `/ext/*` | `extension-auth` | Must bypass gateway actor bearer and enforce extension OAuth token, tenant grant, capability, RLS, and audit checks in the extension router. |
| `/observations`, `/models`, `/commitments`, `/goals`, `/decisions`, `/resources` | `bearer-auth` plus per-row `can_read` filtering | Must remain actor-scoped and continue auditing admin/leadership override reads. |
| `/model/*`, `/v1/model/*` | `bearer-auth` plus model `can_read` filtering | Must gate seed models, aggregate lists, relationship neighbors, synthesized neighbors, and trace chains by actor-visible model scope. |
| `/map/*` | `bearer-auth` plus model `can_read` filtering; projection refresh requires admin/leadership | Must gate map nodes, edges, neighborhoods, topology events, model stories, and activity rows by actor-visible model scope. Tenant-wide projection refresh must be restricted to admin/leadership actors. |
| `/dashboard/*` | `bearer-auth` plus resource/goal/commitment `can_read` filtering | Must gate customer, capacity, goal, served commitment, deployment, and aggregate dashboard data by actor-visible substrate scope. |
| `/contest/*` | `bearer-auth` plus model `can_read` filtering | Must gate target models before contestation mutations and continue enforcing contestability standing. |
| `/today/*` | `bearer-auth` plus target-entity `can_read` filtering | Must gate target-scoped decision deltas, evidence, summary counts, next-card pointers, and mutations by actor-visible target scope. |
| `/v1/artifacts/*` | `bearer-auth` plus artifact `can_read` filtering | Must gate direct artifact drawer reads, actor drawer access, nested drawer links, and override audit behavior by actor-visible substrate scope. |
| `/v1/structure/*` | `bearer-auth` plus commitment/resource `can_read` filtering | Must gate structure overlays, recent graph payloads, resource aggregates, resource overlays, visible-only deployment counts, and nested evidence by actor-visible substrate scope. |
| `/v1/history*` | `bearer-auth` plus target/model/observation `can_read` filtering | Must gate ledger events, predictions, arcs, calibration, and summary counters by actor-visible substrate scope. |
| `/v1/forecasts*` | `bearer-auth` plus target-entity `can_read` filtering and targetless prediction actor scope | Must gate prediction lists, details, page payloads, patterns, ask context, summary counters, upcoming resolutions, risk exposure, accuracy bins, recent resolutions, calibration, and target-linked create requests by actor-visible target scope or explicit targetless forecast actor scope. |
| `/v1/decision_deltas/*` | `bearer-auth` plus target-entity/model `can_read` filtering | Must filter target-linked lists, gate detail and mutation endpoints by actor-visible target scope, gate recommendation promotion by model scope, and audit admin/leadership override reads. |
| `/v1/recommendations/*` | `bearer-auth` plus recommendation model/target `can_read` filtering | Must filter recommendation and hypothesis lists, gate act/dismiss/ratify/watch/triage mutations by actor-visible model and target scope, and audit admin/leadership override reads. |
| `/v1/resolution_threads/*` | `bearer-auth` plus target-entity `can_read` filtering and creator scope for targetless threads | Must filter operational tracker lists, gate create/read/update/evaluate paths by actor-visible target scope, and audit admin/leadership override reads. |
| `/v1/clarifications/*` | `bearer-auth` plus source/model/object/candidate-evidence `can_read` filtering | Must filter clarification lists, gate answer/dismiss mutations by actor-visible evidence scope, fail closed on unanchored rows, and audit admin/leadership override reads. |
| `/v1/spec/*` | `bearer-auth` demo routes gated by `SPEC_DEMO_ROUTES_ENABLED` | Must stay unmounted in production (`SPEC_DEMO_ROUTES_ENABLED=0`) until these seed-payload routes are replaced by substrate-backed, actor-scoped queries. |
| `/v1/today/brand` | `bearer-auth` plus tenant admin/leadership role check | Must restrict tenant-wide brand resource mutation to admin/leadership actors and keep the write tenant-scoped. |
| `/v1/*` | `bearer-auth` | Must require actor session and add explicit substrate access checks for every route that reads user/customer data. |
| `/internal/*` | `internal-only` | Must not be internet-addressable. Current code still uses gateway bearer; production target is private network plus service identity. |
| `/debug/*`, `/api/debug/*` | `debug` | Must not mount in production. `DEBUG_ENDPOINTS_ENABLED=0` is required in the production env contract. |

## Startup-Mounted Route Families

These route families are mounted during lifespan startup when the corresponding
runtime components are available, so they do not appear in the static audit
table above:

| Route family | Current access | Production requirement |
| --- | --- | --- |
| `/rendering/*` | `internal-only` bypass of gateway bearer | Must only be reachable from in-process adapters or private service network. |
| `/view/ceo/*` | `self-authenticated` view/session token plus actor-session cookie support | Query-string token auth defaults disabled and must stay disabled in production via `WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED=0`; Ask/query routes must resolve `request.state.auth` in production and must not accept raw tenant headers or default tenant fallbacks. |
| `/view/ceo/stream` | stream-specific auth with actor-session cookie support | Query-string tokens default disabled and must stay disabled in production via `WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED=0`; browser clients should authenticate with the configured session cookie. |
| `/stream` | actor-session WebSocket auth with bearer-header or session-cookie support | Query-string tokens default disabled and must stay disabled in production via `WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED=0`; browser clients should authenticate with the configured session cookie. |
| `/v1/cards/{card_id}/conversation`, `/v1/cards/{card_id}/probe` | `bearer-auth` plus card model `can_read` filtering | Must gate card conversation reads, probe creation, and conversation clearing by actor-visible recommendation/model scope. |
| Google/Gmail push webhook routes under `/webhooks/*` | `provider-signed` | Must validate provider push identity and tenant/source mapping. |
| Google/Gmail OAuth management routes | mixed `bearer-auth` and `self-authenticated` | Install routes must require actor bearer; callbacks must validate OAuth state. |
| Central `/debug/*` inspector router | `debug` | Must only mount when `DEBUG_ENDPOINTS_ENABLED=1`; production env forbids it. |

## Regression Guards

- `services.app.gateway.route_access` is the source of truth for gateway
  bearer-bypass paths, route access classes, and inventory generation.
- `scripts/audit_gateway_route_access.py --check` fails if debug routes are
  mounted under production defaults or if a route is classified as fully public
  outside health/readiness/metrics, or if `/api/admin/*` routes regress to a
  non-admin-only classification.
- `services/app/gateway/tests/test_route_access_policy.py` verifies key route
  family classifications and proves `/ext/*` is enforced by extension auth
  rather than being intercepted by gateway actor bearer auth.
- `services/product/ask/tests/test_api.py` and
  `services/product/query/tests/test_api.py` verify production Ask/query
  surfaces fail closed without gateway-authenticated actor context even when
  development defaults or raw tenant headers are present.

## Remaining Hardening Work

- Add a generated inventory artifact to CI once the docs build environment is
  stable, so route additions require an access-classification review.
- Strengthen `/internal/*` from bearer-authenticated internal routes to private
  service-to-service identity enforced by network policy or mTLS.
- Apply `can_read` or equivalent access-control checks to every remaining
  substrate-reading product route beyond the legacy substrate list endpoints,
  model-page/model-trace/map/dashboard/contest/today surfaces, and legacy
  artifact/structure/history drawers plus target-linked forecasts, decision
  deltas, recommendations, resolution threads, clarifications, and card
  conversations.
- Add lifespan-route inventory coverage for CEO view, rendering, realtime, and
  Google/Gmail startup-mounted routes.
