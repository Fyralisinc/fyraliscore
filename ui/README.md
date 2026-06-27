# Fyralis BYOC Control Panel UI

This is the browser-facing BYOC control-panel integration for the core repo.
It uses the backend routes in this branch as the source of truth:

- `GET /byoc/control-panel/deployments`
- `GET /byoc/control-panel/state`

The UI intentionally does not call signed BYOC read endpoints and does not
handle BYOC HMAC material. For local testing, paste a gateway bearer token into
the in-memory token field. The token is not written to browser storage.

## Run

```bash
cd ui
npm test
npm run build
npm run dev
```

Set `VITE_FYRALIS_API_BASE` when the gateway is not served from the same origin:

```bash
VITE_FYRALIS_API_BASE=http://localhost:8000 npm run dev
```

The teammate `feat/byoc-control-plane-mvp` branch should be treated as a
prototype/reference. It adds a separate control-plane stack and Grafana-oriented
operator console. This UI integrates directly with the metadata-only backend
contracts implemented in core.
