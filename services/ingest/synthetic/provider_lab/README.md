# Fyralis Provider Lab

Provider Lab is the deterministic, loopback-only simulator for the exact
provider API surface used by Fyralis's 27 canonical sources. It is test
infrastructure, not a production service.

Start it from the repository root with an explicitly non-production
environment:

```bash
COMPANY_OS_ENV=test FYRALIS_ENV=test APP_ENV=test ENVIRONMENT=test \
  .venv/bin/python -m services.ingest.synthetic.provider_lab \
  --host 127.0.0.1 --port 8787
```

The data plane is available below `http://127.0.0.1:8787/{source}/...`.
Set `PROVIDER_LAB_URL=http://127.0.0.1:8787` to enable deterministic lab
credentials in shared client builders, and pass the matching per-source base
URL explicitly. `lib.integrations.provider_lab.provider_lab_endpoint_overrides`
builds the complete subprocess environment when a multi-source run needs it.
The production endpoint resolver never treats the lab origin as a fallback.
Requests outside the registered finite surface receive a strict error.

Provider Lab startup also validates operation ownership against the production
source catalog. Every `SourceDefinition.operation_policy_id` must have exactly
one declared route or non-HTTP protocol surface, and an adapter may not name an
operation absent from that source contract. Multi-operation provider
boundaries (GraphQL, JSON-RPC, AWS SigV4) declare their complete operation set;
Discord Gateway and Telegram's finite injected transport appear separately in
the adapter inventory.

The control plane provides:

- `POST /_lab/reset`
- `GET|PUT|DELETE /_lab/sources/{source}/state`
- `GET|PUT /_lab/clock` and `POST /_lab/clock/advance`
- `GET|POST|DELETE /_lab/quotas...`
- `GET|POST|DELETE /_lab/faults...`
- `GET|DELETE /_lab/ledger`
- `GET /_lab/adapters`

`GET /_lab/adapters` includes the expected and owned operation IDs, route
transport kinds, and non-HTTP protocol surfaces for all 27 sources.

The process and URL helper refuse a production environment and reject
non-loopback addresses. Before a throughput certification, run the calibration
helper and require at least 2× the intended Fyralis request rate with Provider
Lab p99 below 10% of the client timeout.

Production-client parity coverage is intentionally kept beside the lab:

- `tests/test_production_clients.py` covers the used Slack, GitHub, and Gmail
  HTTP surfaces with production clients.
- `tests/test_core_outbound_parity.py` covers Slack per-user DMs, Discord REST
  snowflake pagination, and real-client 429 retry.
- `tests/test_discord_gateway.py` covers HELLO, IDENTIFY, READY, dispatch,
  heartbeat, and resume through the production Discord Gateway client.

The scoped quota and deterministic fault control planes replace the retired
simulator's process-wide periodic rate limiter.
