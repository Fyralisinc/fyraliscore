# Fyralis UI

Modern Next.js UI split into customer-facing and host/operator surfaces.

## Customer-Facing Surfaces

- `/` - Public Fyralis landing page for customers.
- `/onboarding/get-fyralis` - Customer BYOC workspace setup flow.

The public landing page explains Fyralis, the BYOC model, and the hosted-portal to customer-cloud handoff. It does not expose host control-panel, API-surface, or observability navigation.

## Host Surfaces

- `/host/control-panel` - Internal BYOC control panel using sanitized metadata-only contracts.
- `/host/surfaces` - Internal UI-facing backend API surface map.
- `/host/observability` - Internal Grafana, Prometheus, and dashboard-as-code operator view.

Legacy top-level paths redirect into host routes:

- `/control-panel` -> `/host/control-panel`
- `/surfaces` -> `/host/surfaces`
- `/observability` -> `/host/observability`

## Preserved Contracts

The host control panel continues to use:

- `GET /byoc/control-panel/deployments`
- `GET /byoc/control-panel/state`

The UI rejects responses whose `stored_scope` values do not match the expected sanitized metadata scopes.

## Run

```bash
cd ui
npm test
npm run build
npm run dev
```

Set `NEXT_PUBLIC_FYRALIS_API_BASE` when the gateway is not served from the same origin:

```bash
NEXT_PUBLIC_FYRALIS_API_BASE=http://localhost:8000 npm run dev
```

Bearer tokens entered into the host control panel are kept in component memory only and are not written to browser storage.
