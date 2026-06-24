# Fyralis BYOC — Fleet Console (WS-CONSOLE, P4)

The **operator console**: a FastAPI service over the fleet registry — the
one-row-per-deployment store of C4 `DeploymentRecord`s. It is the operator's
read/write surface on the BYOC fleet and the place where a deployment's
**health is derived** from heartbeat freshness so a deployment that goes silent
visibly degrades green → yellow → red without anyone touching its row.

```
control-plane/console/
├── app.py                 # FastAPI app: the P4 REST API + the HTML rollup at GET /
├── store.py               # DeploymentStore: in-memory registry + JSON persistence
│                          #   (console/data/), health derived ON READ (NFR-5)
├── Dockerfile             # build context = control-plane root (needs lib/)
├── service.compose.yml    # compose fragment (console on :8080, cp-net) — merged at integrate
├── requirements.txt       # fastapi / uvicorn / pydantic
├── data/                  # JSON persistence dir (created at runtime; gitignored)
└── tests/
    ├── conftest.py        # front-loads control-plane root on sys.path
    └── test_console_api.py
```

It **reuses** `control-plane/lib/deployment.py` for the C4 `DeploymentRecord`
model and the `derive_health` math — it does **not** redefine the record (that
is the cross-component contract every component honors).

## The REST API contract (P4)

| Method & path | Body | Returns |
|---|---|---|
| `POST /api/v1/register` | `{tenant_id?, region, plan}` | `{tenant_id, deployment_id}` — mints a `deployment_id` (and a `tenant_id` if absent), stamps an initial heartbeat (starts **green**) |
| `POST /api/v1/heartbeat` | a `DeploymentRecord` JSON | the stored record with **health recomputed**; **upsert** keyed by `deployment_id` (a heartbeat replaces the row, never appends) |
| `GET  /api/v1/deployments` | — | `[DeploymentRecord]`, each with **health derived on read**, worst-health first |
| `GET  /api/v1/deployments/{deployment_id}` | — | one `DeploymentRecord` (404 if unknown) |
| `GET  /` | — | minimal **HTML fleet rollup** (table: tenant, deployment, version, region, tier, health badge, last-heartbeat age, license expiry) |
| `GET  /healthz` | — | `{status, fleet_size}` liveness probe |

`DeploymentRecord` wire shape (C4, RFC-3339 UTC, `health ∈ {green,yellow,red}`,
`telemetry_tier ∈ {T1,T2,T3}`):

```json
{
  "tenant_id": "acme",
  "deployment_id": "acme-use1-7f3a",
  "version": "1.4.2",
  "region": "us-east-1",
  "last_heartbeat_ts": "2026-06-24T00:00:00Z",
  "health": "green",
  "license_expiry": "2027-06-24T00:00:00Z",
  "telemetry_tier": "T1"
}
```

## Health derivation (NFR-5 / C4)

Health is **never trusted off the wire** — `store.py` re-derives it on every read
via the shared `lib.deployment.derive_health`, so the console reflects a
deployment that went silent *after* its last heartbeat:

| Condition | Health |
|---|---|
| heartbeat age ≤ 90 s | **green** |
| 90 s < age ≤ 300 s (stale) | **yellow** |
| age > 300 s (missing) | **red** |
| reported fleet-SLI burn flag | green → **yellow** |
| license expired | forced **red** (regardless of freshness) |

Thresholds are `CP_HEARTBEAT_YELLOW_AFTER_S` (default 90, the NFR-5 staleness
bound) and `CP_HEARTBEAT_RED_AFTER_S` (default 300). The worst applicable
condition wins.

## Run it

### Locally (uvicorn)

```bash
# from control-plane/console/  (uses the repo virtualenv)
CP_CONSOLE_PORT=8080 /path/to/.venv/bin/python app.py
# then:
curl -s -XPOST localhost:8080/api/v1/register \
     -H content-type:application/json \
     -d '{"tenant_id":"acme","region":"us-east-1","plan":"enterprise"}'
curl -s localhost:8080/api/v1/deployments | python -m json.tool
open http://localhost:8080/        # the HTML rollup
```

Environment knobs:

| var | default | meaning |
|---|---|---|
| `CP_CONSOLE_HOST` | `0.0.0.0` | bind host |
| `CP_CONSOLE_PORT` | `8080` | listen port (P4 contract) |
| `CP_CONSOLE_DATA_DIR` | `console/data/` | JSON persistence dir |
| `CP_HEARTBEAT_YELLOW_AFTER_S` | `90` | stale → yellow threshold |
| `CP_HEARTBEAT_RED_AFTER_S` | `300` | missing → red threshold |

### Docker / compose

The Dockerfile's build context is the **control-plane root** (so the image can
copy `lib/`). The fragment is merged into the master compose at integrate time:

```bash
docker build -f console/Dockerfile -t fyralis/console .          # from control-plane/
# standalone bring-up of just this fragment:
docker compose --project-directory . -f console/service.compose.yml up
```

In the master `docker-compose.control-plane.yml` it replaces the commented
`console:` stub in the "Phase 4 — console" section: service `console`, port
`8080:8080`, on `cp-net`, with a `console-data` volume.

## Tests

```bash
# from control-plane/console/
/path/to/.venv/bin/python -m pytest tests/ -q
```

Covers: register mints ids (+ tenant when absent) and shows green; heartbeat is
an upsert (not append); health derivation on read (fresh→green, stale→yellow,
missing→red, expired-license→red); GET / renders the rollup; JSON persistence
round-trips across a restart.

## Caveats

- **`plan` is accepted but not stored on the C4 record.** Per the P4 register
  contract the body carries `{tenant_id?, region, plan}`, but the C4
  `DeploymentRecord` has no `plan` field — the **signed license bundle** carries
  the plan (and is authoritative). `register` uses `plan` only as a hint and
  defaults `license_expiry` to a 1-year window; the agent's first **heartbeat**
  (which carries the real `license_expiry` off its verified license) corrects it.
- **The console does not verify the license signature.** Verifying signed
  license/config bundles (`control-plane/signing` verify-before-use, I6) is the
  **agent's** job before it operates; the console only reads back the
  `license_expiry` the heartbeat reports and uses it to derive health. Treat the
  expiry shown here as advisory until the licensing service is wired in.
- **No auth on the console API itself.** In the deployed topology the console
  sits behind the control-plane perimeter on `cp-net`; per I2 it never dials into
  a customer VPC. The agent reaches it **outbound** (dial-home). Do not publish
  `:8080` to an untrusted network without fronting it with the auth proxy /
  operator SSO — `register`/`heartbeat` are unauthenticated as written.
- **Persistence is best-effort, single-node.** The JSON file under
  `console/data/` is written atomically (temp-file + `os.replace`) and reloaded on
  start, but it is a single-writer local store, not an HA database. A corrupt or
  unreadable file starts the console empty rather than crashing it. Multiple
  console replicas would each keep their own file — run one, or swap
  `DeploymentStore`'s backend for a shared DB when the fleet registry graduates
  from MVP.
- **Health is wall-clock relative.** Derivation uses `utcnow()` on read, so a
  badly skewed console clock skews health. A heartbeat *from the future* is
  clamped to age 0 (treated fresh) so agent clock-skew never falsely reds a live
  deployment.
- **`deployment_id` minting is console-side.** Ids are `<tenant>-<region>-<rand>`
  (4 hex chars), retried on the astronomically unlikely in-process collision.
  This is the registry's id authority for the MVP; if onboarding later mints ids
  at cert-issuance time, make that the single source and have `register` accept a
  caller-supplied id.
