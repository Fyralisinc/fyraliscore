# Known Limitations And Feature Flags

Owner: Engineering Leadership.
Last reviewed: 2026-06-24.

This page records launch limitations and production feature flags that
operators must understand before enabling customer traffic.

## Production Flags

| Flag | Production default | Purpose | Launch rule |
| --- | --- | --- | --- |
| `FYRALIS_ENV` | `production` | Enables production fail-closed settings | Must be set on production hosts. |
| `DEBUG_ENDPOINTS_ENABLED` | `0` | Mounts debug routes | Must remain disabled. |
| `SPEC_DEMO_ROUTES_ENABLED` | `0` | Mounts spec/demo payload routes | Must remain disabled. |
| `WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED` | `0` | Allows websocket auth via query string | Must remain disabled in production; local/dev must explicitly opt in. |
| `VIEW_CEO_STATIC_TOKENS_ENABLED` | `0` | Enables static view tokens | Must remain disabled. |
| `FINANCE_PANEL_ENABLED` | `false` | Enables local/demo finance panel | Must remain disabled until production source lifecycle is complete. |
| `SLACK_DM_PANEL_ENABLED` | `false` | Enables local/demo Slack DM panel | Must remain disabled. |
| `GATEWAY_CEO_VIEW_ENABLED` | deployment-specific | Enables CEO view routes | Enable only after product workflow checks pass. |
| `KAFKA_PATH_ENABLED` | tenant flag | Uses Kafka-first ingestion path | Enable per tenant after topic provisioning and source lane health. |
| `QUERY_CACHE_BACKEND` | `pg` or production-approved cache | Query result cache backend | Local/noop backends must fail production contract. |

## Hard Launch Limitations

The repo is not production-ready until these are closed:

- strict tenant isolation and RLS coverage are proven across app and DB paths
- permissive no-tenant RLS policy branches are removed from production
- production source install/uninstall/refresh flows are verified per source
- credentials are stored and rotated through a secret provider only
- frontend overlay removes localStorage/query-token auth patterns
- staging load/soak tests prove launch tenant sizes and cost budgets
- SLO rollback thresholds are tuned with staging soak evidence rather than
  first-pass launch defaults
- governance evidence exists for data classification, subprocessors, LLM data
  use, DPA/security questionnaire, and integration security review

## Current Safe Defaults

- Demo/spec/debug routes are unmounted in production.
- Gateway route access audit can run in production mode.
- Product workflow metrics use bounded labels.
- Schema/RLS drift monitor exposes bounded status metrics.
- Signed CI artifacts are verified before staging and production deploys.
- Staging and production deploy workflows roll back if `/healthz` fails.
- Role grant/revoke CLI actions now write `operator_action_log`.

## Enabling A Feature In Production

Before enabling any non-default production flag:

1. Confirm the feature has owner, rollback, and customer impact statement.
2. Confirm route access and tenant isolation tests pass.
3. Confirm metrics and alerts exist.
4. Confirm safe degraded behavior exists.
5. Confirm release notes call out the flag and rollback plan.
6. Enable in staging first and observe the product SLO dashboard.
7. Enable for production only through the approved deploy/config process.

## Disabling A Feature

When disabling a feature for incident mitigation:

1. Record the tenant or global scope.
2. Record the feature flag/config key and previous value.
3. Disable new work first, then drain or quarantine in-flight work.
4. Verify customer-facing routes show safe degraded states.
5. Add follow-up work before re-enabling.
