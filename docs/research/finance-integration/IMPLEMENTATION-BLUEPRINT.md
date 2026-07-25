All seams confirmed against the live tree. I now have everything needed. Here is the complete blueprint.

---

> Historical blueprint: the `synthetic/mock_servers` paths below were the
> original implementation targets. They are retired and deleted; executable
> HTTP conformance now lives in `services/ingest/synthetic/provider_lab/`.

# IMPLEMENTATION BLUEPRINT — Wiring Brex / Deel (Bearer archetype) + Ramp / Gusto (OAuth archetype) into Fyralis

**Archetype assignment**
- **Brex**, **Deel** → clone **Mercury** (long-lived Bearer token; dedicated `<src>_installations` + child table keyed on `(tenant_id, base_url)`; no token refresh).
- **Ramp**, **Gusto** → clone **QuickBooks** (OAuth2 access+refresh token; scope id `realm_id`-equivalent; `(tenant_id, <scope_id>)` identity; refresh is a documented-but-unbuilt seam).

**Confirmed facts (verified against the live tree, branch `feat/telegram-mtproto-ingestion`):**
- Newest migration on disk = `db/migrations/0094_telegram.sql`. **Next four numbers: `0095`, `0096`, `0097`, `0098`.**
- Current source superset = **12**, byte-identical across all 4 source-CHECK tables and all enums: `'slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram'`.
- Error base class `CompanyOSError` lives at `lib/shared/errors.py` (Mercury at :521, QuickBooks at :551).
- `HMAC_PROVIDERS = ("jira", "mercury", "quickbooks", "grafana")` at `services/ingest/synthetic/live_generators/hmac_webhook.py:55`.

A new source touches **22 distinct seams per source** (10 new-file groups + 12 shared-file edits). All shared lists are in identical source-order; everywhere a new source **appends after `"telegram"`**, and the four new sources append in the order **brex → ramp → gusto → deel** (matching the migration sequence so each migration's CHECK is a strict superset of the prior).

---

## 1. Per-source file manifest (NEW files to create)

For each source `<s>` ∈ {brex, ramp, gusto, deel}, create the following. "Clone" = copy the named archetype file, then apply the §4 per-source delta.

### 1a. Brex (clone **Mercury**) — 16 new files
| New file | Clones |
|---|---|
| `services/ingest/integrations/brex/__init__.py` | `…/mercury/__init__.py` |
| `services/ingest/integrations/brex/client.py` | `…/mercury/client.py` (`BrexClient`, `_api_token`, Bearer) |
| `services/ingest/integrations/brex/oauth.py` | `…/mercury/oauth.py` (static-token connect wizard) |
| `services/ingest/integrations/brex/onboarding.py` | `…/mercury/onboarding.py` |
| `services/ingest/integrations/brex/metrics.py` | `…/mercury/metrics.py` (prefix `brex.`) |
| `services/ingest/ingestion/fetchers/brex.py` | `…/fetchers/mercury.py` |
| `services/ingest/ingestion/planners/brex.py` | `…/planners/mercury.py` |
| `services/ingest/ingestion/handlers/brex.py` | `…/handlers/mercury.py` |
| `services/ingest/ingestion/reconcilers/brex.py` | `…/reconcilers/mercury.py` |
| `services/app/webhooks/signatures/brex.py` | `…/signatures/mercury.py` |
| `services/ingest/synthetic/fixtures/brex_generator.py` | `…/fixtures/mercury_generator.py` |
| `services/ingest/synthetic/mock_clients/brex.py` | `…/mock_clients/mercury.py` |
| `services/ingest/synthetic/mock_servers/brex.py` | `…/mock_servers/mercury.py` |
| `services/ingest/ingestion/fetchers/tests/test_brex.py` | `…/fetchers/tests/test_mercury.py` |
| `services/ingest/ingestion/handlers/tests/test_brex.py` | `…/handlers/tests/test_mercury.py` |
| `db/migrations/0095_brex.sql` | `db/migrations/0094_telegram.sql` (DDL blocks per §3) |

Plus a sandbox driver (optional but recommended, telegram precedent): `scripts/sandbox_brex.py`.

### 1b. Deel (clone **Mercury**) — same 16-file set
Identical layout to Brex with `brex`→`deel` everywhere; migration `0098_deel.sql`.

### 1c. Ramp (clone **QuickBooks**) — 16 new files
| New file | Clones |
|---|---|
| `services/ingest/integrations/ramp/__init__.py` | `…/quickbooks/__init__.py` |
| `services/ingest/integrations/ramp/client.py` | `…/quickbooks/client.py` (`RampClient`, `access_token`, scope id `business_id`) |
| `services/ingest/integrations/ramp/oauth.py` | `…/quickbooks/oauth.py` (operator-paste preflight+finalize) |
| `services/ingest/integrations/ramp/onboarding.py` | `…/quickbooks/onboarding.py` |
| `services/ingest/integrations/ramp/metrics.py` | `…/quickbooks/metrics.py` (prefix `ramp.`) |
| `services/ingest/ingestion/fetchers/ramp.py` | `…/fetchers/quickbooks.py` |
| `services/ingest/ingestion/planners/ramp.py` | `…/planners/quickbooks.py` |
| `services/ingest/ingestion/handlers/ramp.py` | `…/handlers/quickbooks.py` |
| `services/ingest/ingestion/reconcilers/ramp.py` | `…/reconcilers/quickbooks.py` |
| `services/app/webhooks/signatures/ramp.py` | `…/signatures/quickbooks.py` |
| `services/ingest/synthetic/fixtures/ramp_generator.py` | `…/fixtures/quickbooks_generator.py` |
| `services/ingest/synthetic/mock_clients/ramp.py` | `…/mock_clients/quickbooks.py` |
| `services/ingest/synthetic/mock_servers/ramp.py` | `…/mock_servers/quickbooks.py` |
| `services/ingest/ingestion/fetchers/tests/test_ramp.py` | `…/fetchers/tests/test_quickbooks.py` |
| `services/ingest/ingestion/handlers/tests/test_ramp.py` | `…/handlers/tests/test_quickbooks.py` |
| `db/migrations/0096_ramp.sql` | `db/migrations/0075_quickbooks.sql` |

Plus `scripts/sandbox_ramp.py`.

### 1d. Gusto (clone **QuickBooks**) — same 16-file set
Identical layout to Ramp with `ramp`→`gusto`, scope id `company_uuid`; migration `0097_gusto.sql`.

**Also add a typed error class per source** in `lib/shared/errors.py` (clone the `MercuryApiError` block at :521 for Brex/Deel; `QuickBooksApiError` at :551 for Ramp/Gusto): `BrexApiError`, `RampApiError`, `GustoApiError`, `DeelApiError`, each with code strings `<src>_api_unauthorized / <src>_api_rate_limited / <src>_api_not_found / <src>_api_error`.

---

## 2. Shared-file edit manifest (exact additive lines)

Every edit appends after the current last entry (`telegram` / `grafana`), in the order **brex, ramp, gusto, deel**. All edits are additive supersets — no existing line is removed or reordered.

### 2.1 `services/ingest/ingestion/fetchers/__init__.py`
**Dict** — after `"telegram": _not_implemented_fetcher("telegram", "IN-TELEGRAM"),` (line 157):
```python
    "brex":  _not_implemented_fetcher("brex",  "IN-FIN2"),
    "ramp":  _not_implemented_fetcher("ramp",  "IN-FIN2"),
    "gusto": _not_implemented_fetcher("gusto", "IN-FIN2"),
    "deel":  _not_implemented_fetcher("deel",  "IN-FIN2"),
```
**Import block** — after `from services.ingest.ingestion.fetchers import telegram as _telegram  # noqa: E402,F401` (line 181):
```python
from services.ingest.ingestion.fetchers import brex as _brex  # noqa: E402,F401
from services.ingest.ingestion.fetchers import ramp as _ramp  # noqa: E402,F401
from services.ingest.ingestion.fetchers import gusto as _gusto  # noqa: E402,F401
from services.ingest.ingestion.fetchers import deel as _deel  # noqa: E402,F401
```

### 2.2 `services/ingest/ingestion/planners/__init__.py`
**Dict** — after `"telegram": …` (line 135) and **import block** — after line 162: identical 4-line additions (`_not_implemented_planner("<s>", "IN-FIN2")` + `from …planners import <s> as _<s>`).

### 2.3 `services/ingest/ingestion/reconcilers/__init__.py`
**Dict** — after `"telegram": _not_implemented_reconciler("telegram", "IN-TELEGRAM"),` (line 181):
```python
    "brex":  _not_implemented_reconciler("brex",  "IN-FIN2"),
    "ramp":  _not_implemented_reconciler("ramp",  "IN-FIN2"),
    "gusto": _not_implemented_reconciler("gusto", "IN-FIN2"),
    "deel":  _not_implemented_reconciler("deel",  "IN-FIN2"),
```
**Import block** — after line 206: 4 `from …reconcilers import <s> as _<s>` lines. (Reconciler default is clean `has_gaps=False`, not NotImplementedError — so even an un-wired reconciler is safe.)

### 2.4 `services/ingest/ingestion/handlers/__init__.py`
**Import block** — after `from services.ingest.ingestion.handlers import telegram  # noqa: E402,F401` (line 172):
```python
from services.ingest.ingestion.handlers import brex  # noqa: E402,F401
from services.ingest.ingestion.handlers import ramp  # noqa: E402,F401
from services.ingest.ingestion.handlers import gusto  # noqa: E402,F401
from services.ingest.ingestion.handlers import deel  # noqa: E402,F401
```
`CHANNEL_TRUST_MAP` (lines 41–69): **no edit required** — finance handlers supply `trust_tier` inline on `ObservationDraft` (Mercury/QBO precedent). Trust is registered by each handler module via `CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)` at module bottom.

### 2.5 `services/app/webhooks/signatures/__init__.py`
**Import tuple** (lines 20–31) — add `brex,`, `deel,`, `gusto,`, `ramp,` (alpha-ish). **`VERIFIERS`** — after `"grafana": grafana.verifier,` (line 45):
```python
    "brex": brex.verifier,
    "ramp": ramp.verifier,
    "gusto": gusto.verifier,
    "deel": deel.verifier,
```

### 2.6 `services/ingest/ingestion/fetchers/_clients.py`
Add **8 builder/opener functions** (clone `build_mercury_client`/`open_mercury_client` at :309/:476 for brex+deel; `build_quickbooks_client`/`open_quickbooks_client` at :366/:480 for ramp+gusto). Then extend `__all__` (lines 492–503):
```python
    "build_brex_client", "build_ramp_client", "build_gusto_client", "build_deel_client",
    "open_brex_client", "open_ramp_client", "open_gusto_client", "open_deel_client",
```
Brex/Deel builders read `install["base_url"]`, `install["secret_ref"]`, `install["tenant_id"]`; Provider Lab runs preset the `"spam-brex"`/`"spam-deel"` fixture tokens and pass explicit `BREX_API_BASE_URL`/`DEEL_API_BASE_URL` overrides. Ramp/Gusto builders additionally read `install["realm_id"]` (the scope id — `business_id`/`company_uuid`) and use their explicit per-source endpoint overrides in Provider Lab runs.

### 2.7 Provider endpoint URLs — production resolver + Provider Lab contract
After the `grafana_api` lines (56/85/104):
```python
# default-host dict (after line 56):
    "brex_api": "https://platform.brexapis.com",      # TODO(human): confirm host
    "ramp_api": "https://api.ramp.com/developer/v1",   # TODO(human): confirm host
    "gusto_api": "https://api.gusto.com",              # TODO(human): confirm host
    "deel_api": "https://api.letsdeel.com",            # TODO(human): confirm host
# env-override dict (after line 87):
    "brex_api": "BREX_API_BASE_URL",  "ramp_api": "RAMP_API_BASE_URL",
    "gusto_api": "GUSTO_API_BASE_URL", "deel_api": "DEEL_API_BASE_URL",
# Provider Lab path map in lib/integrations/provider_lab.py:
    "brex_api": "/brex", "ramp_api": "/ramp", "gusto_api": "/gusto", "deel_api": "/deel",
```

`PROVIDER_LAB_URL` is a non-production, loopback-only origin. Test harnesses
materialize its path map into the explicit per-source environment variables;
`endpoint()` never treats the lab origin as an implicit fallback.

### 2.8 `services/ingest/ingestion/idempotency/__init__.py`
Add constructors after the telegram block (line ~191), and append names to `__all__` (alphabetical, lines 194–214):
```python
def brex_transaction(account_id: str, txn_id: str, status: str) -> str:
    return f"brex:{account_id}:txn:{txn_id}:{status}"
def brex_balance(account_id: str, as_of_date: str) -> str:
    return f"brex:{account_id}:balance:{as_of_date}"
def deel_payment(contract_id: str, payment_id: str, status: str) -> str:
    return f"deel:{contract_id}:payment:{payment_id}:{status}"
def deel_contract(contract_id: str, updated: str) -> str:
    return f"deel:{contract_id}:contract:{updated}"
def ramp_transaction(business_id: str, txn_id: str, state: str) -> str:
    return f"ramp:{business_id}:txn:{txn_id}:{state}"
def gusto_entity(company_uuid: str, entity_kind: str, entity_id: str, version: str) -> str:
    return f"gusto:{company_uuid}:{entity_kind}:{entity_id}:{version}"
def gusto_change(company_uuid: str, entity_kind: str, entity_id: str, ver: str) -> str:
    return f"gusto:{company_uuid}:{entity_kind}:{entity_id}:chg:{ver}"
```
(Ramp/Gusto follow the QBO two-constructor pattern — a versioned full key + a thin-change key — **only if** their webhooks are body-less; see §4.)

### 2.9 `services/ingest/ingestion/workflows/shard_fetch.py`
**SQL constants** — after `_LOAD_TELEGRAM_INSTALL_SQL` (line 463), add 4 constants. Brex/Deel clone `_LOAD_MERCURY_INSTALL_SQL` (:424); Ramp/Gusto clone `_LOAD_QUICKBOOKS_INSTALL_SQL` (:435, selecting `realm_id, refresh_secret_ref`):
```python
_LOAD_BREX_INSTALL_SQL = """
SELECT id, tenant_id, base_url, secret_ref, disabled_at
  FROM brex_installations WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1
"""
_LOAD_RAMP_INSTALL_SQL = """
SELECT id, tenant_id, business_id, base_url, secret_ref, refresh_secret_ref, disabled_at
  FROM ramp_installations WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1
"""
_LOAD_GUSTO_INSTALL_SQL = """
SELECT id, tenant_id, company_uuid, base_url, secret_ref, refresh_secret_ref, disabled_at
  FROM gusto_installations WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1
"""
_LOAD_DEEL_INSTALL_SQL = """
SELECT id, tenant_id, base_url, secret_ref, disabled_at
  FROM deel_installations WHERE tenant_id = $1 AND disabled_at IS NULL LIMIT 1
"""
```
**`_load_install` branches** — after `if source == "telegram":` (lines 564–565), before the `provider_installations` fallback (line 566):
```python
    if source == "brex":
        return await pool.fetchrow(_LOAD_BREX_INSTALL_SQL, tenant_id)
    if source == "ramp":
        return await pool.fetchrow(_LOAD_RAMP_INSTALL_SQL, tenant_id)
    if source == "gusto":
        return await pool.fetchrow(_LOAD_GUSTO_INSTALL_SQL, tenant_id)
    if source == "deel":
        return await pool.fetchrow(_LOAD_DEEL_INSTALL_SQL, tenant_id)
```
**Load-bearing**: omitting any branch → the shard falls through to `_LOAD_PROVIDER_INSTALL_SQL`, finds nothing, and **silently parks `in_progress` forever** (not a visible failure). The same `_load_install`-style branch set must be replicated in `services/ingest/ingestion/workflows/source_onboarding.py` (constants after line 404; branches after line 553) — that file has its own copy with `conn.fetchrow`.

### 2.10 `services/app/webhooks/router.py` — three dicts
**`_PROVIDER_TO_SHADOW_SOURCE`** — after `"grafana": "grafana",` (line 134): `"brex": "brex", "ramp": "ramp", "gusto": "gusto", "deel": "deel",`
**`_CUTOVER_ENABLED_PROVIDERS`** — after line 157: same four `"<s>": "<s>",` entries.
**`_PROVIDER_CHANNEL`** — after `"grafana": "grafana:alert",` (line 419):
```python
    "brex": "brex:transaction",
    "ramp": "ramp:transaction",
    "gusto": "gusto:object",
    "deel": "deel:payment",
```
(The channel string must match each handler's `@register(...)`; line 1045 does a hard `_PROVIDER_CHANNEL[provider]` index → `KeyError` if missing.)

### 2.11 `services/app/webhooks/tenant_resolver.py`
**`ResolverProvider` Literal** (lines 76–79) — append `"brex", "ramp", "gusto", "deel",`.
**Extractor fns** — after `_extract_grafana` (line 346) add `_extract_brex`/`_extract_ramp`/`_extract_gusto`/`_extract_deel` (clone `_extract_mercury` :316 / `_extract_quickbooks` :330). Each returns the installation_id used to key `provider_installations` via `_str_or_none(...)`.
**`PROVIDER_EXTRACTORS`** — after `"grafana": _extract_grafana,` (line 377): `"brex": _extract_brex, "ramp": _extract_ramp, "gusto": _extract_gusto, "deel": _extract_deel,`

### 2.12 `services/ingest/ingestion/raw_tier/envelope.py`
**`SourceLiteral`** (lines 24–27) — append after `"telegram"`:
```python
    "brex", "ramp", "gusto", "deel",
```
This single edit propagates automatically to `INGESTION_SOURCES` (`kafka/topics.py:33`), the DLQ allowlist, normalizer invariants, Kafka topic provisioning, and the per-source compose generator. **No separate edit needed for those.**

### 2.13 `services/ingest/ingestion/normalizer/channel_mapping.py`
**`_CHANNEL_MAP`** (after line 161) — add `(source, ingress)` → channel for each. `resolve_channel` returns `None` → skip if missing:
```python
    ("brex", "backfill"): "brex:transaction", ("brex", "poll"): "brex:transaction", ("brex", "webhook"): "brex:transaction",
    ("ramp", "backfill"): "ramp:transaction", ("ramp", "poll"): "ramp:transaction", ("ramp", "webhook"): "ramp:transaction",
    ("gusto", "backfill"): "gusto:object", ("gusto", "poll"): "gusto:object", ("gusto", "webhook"): "gusto:object",
    ("deel", "backfill"): "deel:payment", ("deel", "poll"): "deel:payment", ("deel", "webhook"): "deel:payment",
```

### 2.14 `services/ingest/ingestion/workflows/tenant_onboarding.py`
**`VALID_SOURCES`** (line 174) — append `"brex", "ramp", "gusto", "deel"`.
**`_LOAD_ACTIVE_SOURCES_SQL` UNION** — after the telegram arm (lines 236–238), add four arms:
```sql
UNION
SELECT 'brex' AS source FROM brex_installations WHERE disabled_at IS NULL
UNION
SELECT 'ramp' AS source FROM ramp_installations WHERE disabled_at IS NULL
UNION
SELECT 'gusto' AS source FROM gusto_installations WHERE disabled_at IS NULL
UNION
SELECT 'deel' AS source FROM deel_installations WHERE disabled_at IS NULL
```
**This is one of the two documented drift bugs the all-source overlap gate catches — do not skip the UNION arm.** Also append to `source_onboarding.py::VALID_SOURCES` (line 202).

### 2.15 `services/ingest/ingestion/workflows/reconciler.py`
**Imports** — after `telegram as telegram_reconciler_mod` (line 769) add 4 imports. **Pool wiring** — after `telegram_reconciler_mod.set_pool_provider(pool)` (line 781):
```python
    brex_reconciler_mod.set_pool_provider(pool)
    ramp_reconciler_mod.set_pool_provider(pool)
    gusto_reconciler_mod.set_pool_provider(pool)
    deel_reconciler_mod.set_pool_provider(pool)
```
**This is the other of the two drift bugs the gate catches.** A missing `set_pool_provider` call → reconciler raises `RuntimeError` at first gap probe.

### 2.16 `services/ingest/ingestion/workflows/source_onboarding.py::_build_source_client`
The current finance dispatch (lines 578–586) only covers github/slack/discord/notion and returns `None` otherwise. Add branches dispatching to the new builders (clone the pattern; finance sources may legitimately return `None` here because the planner reads child rows from DB, not the API — but add the branch for parity with the loader). At minimum confirm the install loader branch (§2.9 source_onboarding copy) exists.

### 2.17 `services/app/gateway/finance_router.py` — the finance UI testing console
**`_SOURCES`** (line 56):
```python
_SOURCES = ("mercury", "quickbooks", "brex", "ramp", "gusto", "deel")
```
**`_CHANNEL`** (line 57) — add `"brex": "brex:transaction", "ramp": "ramp:transaction", "gusto": "gusto:object", "deel": "deel:payment"`.
Add per-source API-base constants alongside `_MERCURY_BASE`/`_QBO_BASE` (lines 58–59), and add install/backfill branches mirroring the mercury branch (lines 405–425) and qbo branch. This is what gives each source its `/finance/{source}/install|backfill|live/emit|status` dev console.

### 2.18 Validation — Provider Lab + source certification
**`live_generators/hmac_webhook.py:55`**:
```python
HMAC_PROVIDERS = ("jira", "mercury", "quickbooks", "grafana", "brex", "ramp", "gusto", "deel")
```
and teach `HmacWebhookGenerator.simulate_event` how to build each provider's payload + external_id from the `LiveTarget` fields.
Add each source once to the canonical source contract, implement its Provider
Lab adapter, and attach its count/live-status/overlap evidence to the
source-certification artifact. The retired manual all-source runner must not
regain copied `_EXPECTED`, live-status, or source tables.
**`composition.py`**: `SigningSecrets` (line 87) add 4 fields + 4 `WEBHOOK_SECRET_<P>` env exports (after line 111); `LiveTarget` (line 130) add `brex_org/brex_account`, `ramp_business`, `gusto_company`, `deel_org`; `live_target_for` (line 169) add 4 branches; `_hmac_secret` map (line 409) add 4 entries; `seed_live_installs` `inst` chain (lines 502–509) add 4 `elif`; `dispatch_live_concurrent` tuple (line 848) append the four.
**`preflight.py`**: add `_<s>_records` helper + `_SOURCE_SPECS` entry per source (finance sources are not yet covered there — additive).
Register new fixtures in `fixtures/__init__.py` (after line 41) and mock clients in `mock_clients/__init__.py` (after line 42).

---

## 3. Migration plan — 0095_brex / 0096_ramp / 0097_gusto / 0098_deel

Each wrapped in `BEGIN; … COMMIT;`, all DDL `CREATE TABLE IF NOT EXISTS` (additive/idempotent), all CHECK widenings `DROP CONSTRAINT IF EXISTS` then `ADD … NOT VALID`. RLS = `ENABLE` + `FORCE` + `<table>_tenant_isolation` policy on `current_setting('app.current_tenant', true)::uuid` via the standard DO-loop (identical template across sources — only the table-name array varies).

### Install-table column matrix
| Column | Brex (0095) | Ramp (0096) | Gusto (0097) | Deel (0098) |
|---|---|---|---|---|
| `id UUID PK` | ✓ | ✓ | ✓ | ✓ |
| `tenant_id UUID FK tenants(id)` | ✓ | ✓ | ✓ | ✓ |
| scope id | — | `business_id TEXT NOT NULL` | `company_uuid TEXT NOT NULL` | — |
| `base_url TEXT NOT NULL` | ✓ | ✓ | ✓ | ✓ |
| `secret_ref TEXT` (access/Bearer token) | ✓ | ✓ | ✓ | ✓ |
| `refresh_secret_ref TEXT` (OAuth) | — | ✓ | ✓ | — |
| `token_expires_at TIMESTAMPTZ` (OAuth) | — | ✓ | ✓ | — |
| `organization_id TEXT` (webhook key, nullable) | ✓ | — | — | ✓ |
| `webhook_secret_ref TEXT` | ✓ | ✓ | ✓ | ✓ |
| `created_at TIMESTAMPTZ DEFAULT now()` | ✓ | ✓ | ✓ | ✓ |
| `disabled_at TIMESTAMPTZ` (LOAD-BEARING filter) | ✓ | ✓ | ✓ | ✓ |
| natural key UNIQUE | `(tenant_id, base_url)` | `(tenant_id, business_id)` | `(tenant_id, company_uuid)` | `(tenant_id, base_url)` |

Indexes: `<t>_installations_tenant_idx` on `tenant_id` (all). Brex/Deel: partial `…_org_idx ON (organization_id) WHERE organization_id IS NOT NULL` (webhook hot path). Ramp/Gusto: `…_scope_idx ON (business_id)` / `(company_uuid)` (unconditional, webhook hot path).

### Child resource tables (one row per shard target)
| Source | Child table | Resource id col | Cursor col | UNIQUE |
|---|---|---|---|---|
| Brex | `brex_accounts` | `account_id TEXT` | `txn_cursor TEXT` | `(brex_installation_id, account_id)` |
| Ramp | `ramp_entities` | `entity_type TEXT` | `updated_cursor TEXT` | `(ramp_installation_id, entity_type)` |
| Gusto | `gusto_entities` | `entity_type TEXT` | `updated_cursor TEXT` | `(gusto_installation_id, entity_type)` |
| Deel | `deel_contracts` | `contract_id TEXT` | `payment_cursor TEXT` | `(deel_installation_id, contract_id)` |

Each child table: `id UUID PK`, `tenant_id UUID FK`, `<t>_installation_id UUID NOT NULL REFERENCES <t>_installations(id) ON DELETE CASCADE`, descriptive cols, `last_synced_at`, `state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('pending','active','paused','errored'))`, `last_error`, `created_at`, plus `<t>_<child>_install_idx` index. (A Ramp/Gusto/Deel/Brex backfill that is org-wide rather than per-resource could omit the child table — but all four here shard per resource, so keep it.)

### Source-CHECK widening — the four substrate tables, strict-superset chain
Each migration widens `source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`, `onboarding_triggers` (and `provider_installations.provider` CHECK if present) with byte-identical IN-lists. The chain (each is a strict superset of the prior):

- **0095_brex** IN-list (13): `'slack','github','discord','gmail','notion','google_calendar','google_drive','jira','mercury','quickbooks','grafana','telegram','brex'`
- **0096_ramp** IN-list (14): … `'telegram','brex','ramp'`
- **0097_gusto** IN-list (15): … `'brex','ramp','gusto'`
- **0098_deel** IN-list (16): … `'brex','ramp','gusto','deel'`

**Exact final 16-source list (0098_deel, all four CHECK blocks byte-identical):**
```sql
CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel')) NOT VALID
```
**Source-CHECK-rerun landmine** (from project memory): the newest migration (`0098`) poisons each prior widening migration's isolated test re-run because a row written with `'deel'` violates the narrower earlier CHECK. Integration tests that re-run a single migration in isolation **must clean up** `'brex'/'ramp'/'gusto'/'deel'` rows first.

---

## 4. Per-source delta spec

### Brex (Bearer / Mercury archetype)
- **Auth**: long-lived API token, `Authorization: Bearer {token}`, **no refresh**. Static connect wizard (`oauth.py` clones Mercury's preflight/finalize). Secret label `brex_api_token:{base_url}`, webhook secret `brex_webhook_secret:{base_url}`.
- **Backfill cursor**: offset/limit (clone Mercury) — `BrexCursor(extra="forbid")` with `offset:int`, `high_water_created:str|None`, `incremental_floor`, `txns_seen`, `seeded`. **TODO(human): confirm Brex transactions API supports offset pagination + a `created`/`posted` filter; Brex v2 may be cursor-based.**
- **Shard on**: cash/card accounts → `brex_accounts.account_id`; one shard per account, `SHARD_KIND_ACCOUNT_TXNS = "brex_account_txns"`.
- **Channel / kind / trust**: `brex:transaction`; `trust_tier="authoritative"` (bank/card system-of-record); `kind="state_change"` iff status ∈ `{failed,cancelled,canceled,declined}` else `signal`.
- **external_id**: `brex:{account_id}:txn:{txn_id}:{status}` (versioned by status); `brex:{account_id}:balance:{YYYY-MM-DD}`.
- **Webhook signature**: **TODO(human): confirm Brex webhook signature scheme.** Default safe impl = clone Mercury HMAC-SHA256-hex with header name + prefix made configurable (see §5). Resolver `_extract_brex` reads `organizationId`/`accountId` (configurable). **TODO(human): confirm Brex webhook tenant-id field.**

### Ramp (OAuth / QuickBooks archetype)
- **Auth**: OAuth2 `access_token` (~hours) + rotating `refresh_token`. Operator-paste preflight/finalize (clone QBO). Secrets `ramp_access_token:{business_id}`, `ramp_refresh_token:{business_id}`, `ramp_webhook_verifier:{business_id}`. **Refresh is the documented-but-unbuilt seam** (QBO has none) — see §5 TODO.
- **Backfill cursor**: incremental floor on an "updated"/"modified" timestamp. **TODO(human): confirm Ramp supports a `from_date`/`since` filter on transactions; if no SQL-query API, use Mercury-style REST list + `start` filter instead of QBO's SELECT string.** `RampCursor` with `start_position`/`offset`, `high_water_updated`, `incremental_floor`, `rows_seen`, `seeded`.
- **Shard on**: `ramp_entities.entity_type` (e.g. `transaction`, `card`, `reimbursement`) — or per-business if org-wide. `realm_id`-equivalent = `business_id`, path-scoped.
- **Channel / kind / trust**: `ramp:transaction`; `trust_tier="authoritative"`; `kind` per state predicate (declined/disputed → state_change).
- **external_id**: `ramp:{business_id}:txn:{txn_id}:{state}` (versioned by state). Add a thin-change ctor only if webhooks are body-less.
- **Webhook signature**: **TODO(human): confirm Ramp webhook signature (scheme + header + base64 vs hex).** Default = configurable HMAC-SHA256. Resolver `_extract_ramp` digs `business_id` from the event envelope — **TODO(human): confirm field path.**

### Gusto (OAuth / QuickBooks archetype)
- **Auth**: OAuth2 access+refresh (clone QBO). Scope id `company_uuid`. Secrets labelled `gusto_access_token:{company_uuid}` etc.
- **Backfill cursor**: incremental floor on `updated_at`. Entities: payrolls, employees, contractor payments. **TODO(human): confirm Gusto pagination (page/per_page params) + the per-entity "updated since" filter.**
- **Shard on**: `gusto_entities.entity_type`; path-scoped `/v1/companies/{company_uuid}/…`.
- **Channel / kind / trust**: `gusto:object`; `trust_tier="authoritative"`; payroll-failed/cancelled → state_change.
- **external_id**: `gusto:{company_uuid}:{entity_kind}:{entity_id}:{version}` (versioned by version/updated_at); thin-change `gusto:{company_uuid}:{entity_kind}:{entity_id}:chg:{ver}` if webhooks are id-only.
- **Webhook signature**: Gusto uses an HMAC over the body with a per-subscription signing secret. **TODO(human): confirm Gusto signature header name + digest encoding.** Resolver `_extract_gusto` from the event envelope — **TODO(human): confirm the company-uuid field path.**

### Deel (Bearer / Mercury archetype)
- **Auth**: long-lived API token, Bearer, no refresh (clone Mercury static wizard). Secrets `deel_api_token:{base_url}`, `deel_webhook_secret:{base_url}`.
- **Backfill cursor**: offset/limit (clone Mercury). **TODO(human): confirm Deel pagination + "created since" filter on payments/contracts.**
- **Shard on**: `deel_contracts.contract_id`; `SHARD_KIND_CONTRACT_PAYMENTS = "deel_contract_payments"`. Snapshot record-type = contract state; child = payments.
- **Channel / kind / trust**: `deel:payment`; `trust_tier="authoritative"`; payment status failed/rejected → state_change.
- **external_id**: `deel:{contract_id}:payment:{payment_id}:{status}` (versioned by status); `deel:{contract_id}:contract:{updated}` (versioned by updated).
- **Webhook signature**: **TODO(human): confirm Deel webhook signature scheme.** Default = configurable HMAC-SHA256. `_extract_deel` from the webhook envelope — **TODO(human): confirm tenant/org field.**

---

## 5. UNVERIFIED / RISK register

The research flagged several external-API details as UNVERIFIED. **Do NOT fabricate a specific verified scheme.** For each, the SAFE implementation choice is: **clone the archetype faithfully, make the unverified bit configurable via env var and/or an install-row column, default it to the archetype's scheme, and leave a visible `TODO(human): confirm <X> against <source> docs` marker** (per CLAUDE.md "Never fabricate rationale").

| # | Unverified detail | Source(s) | SAFE choice | Marker location |
|---|---|---|---|---|
| 1 | Webhook **signature scheme** (HMAC algo, digest encoding hex vs base64, header name, prefix) | Brex, Ramp, **Deel**, Gusto | Make `_PREFIX`, header name, and `base64_vs_hex` **module constants** in `signatures/<s>.py`, defaulting to Mercury's `sha256=`+hex. Loop over all active secrets (rotation). | `TODO(human): confirm <src> webhook signature` atop each `signatures/<s>.py` |
| 2 | Webhook **tenant/installation-id field** in the payload | Brex, Ramp, Gusto, Deel | `_extract_<s>` reads a primary field with a documented fallback, both via `_str_or_none`. Keep the field name in one place. | inside each `_extract_<s>` in `tenant_resolver.py` |
| 3 | **Pagination scheme** (offset/limit vs cursor vs page-token; query-language vs REST list) | all four (esp. Brex v2, Ramp) | Default to archetype (Mercury offset/limit; QBO query string). Cap + page-size env knob `<SRC>_BACKFILL_PAGE_SIZE`. | atop each `fetchers/<s>.py` |
| 4 | **Incremental "updated/created since" filter** support + field name | all four | Cursor freezes whatever monotonic field the API exposes into `incremental_floor`; field name configurable. If no filter exists, fall back to full re-walk (idempotent via external_id). | `<s>Cursor` docstring + `_bump_high_water` |
| 5 | **OAuth token-refresh** (endpoint, grant flow, rotation) | Ramp, Gusto | Persist `refresh_secret_ref` + `token_expires_at` (columns exist) but **the refresh loop is the documented-but-unbuilt seam** — exactly as QBO ships. Either implement refresh-on-401 in the client (exchange → persist rotated token → retry once) OR a refresh poller. Do NOT silently assume tokens never expire. | `TODO(human): implement <src> OAuth refresh (none exists; QBO seam)` in `integrations/<s>/client.py` + `oauth.py` |
| 6 | Exact **prod API host** / base path | all four | Set a plausible default in `endpoints.py` AND make it overridable per-install (`base_url` column) and per-env (`<SRC>_API_BASE_URL`). | `TODO(human): confirm host` inline in `endpoints.py` (see §2.7) |
| 7 | **Endpoints / scopes** beyond the verified read surface | all four | Implement only the verified read surface; tag speculative endpoints. | `TODO(human): confirm <src> read endpoints + OAuth scopes` in `client.py` |
| 8 | **Rate-limit signalling** (429+Retry-After vs X-RateLimit-Reset) | all four | Default to 429+`Retry-After` (Mercury). Env knobs `<SRC>_RL_MAX_ATTEMPTS`/`<SRC>_RL_MAX_SLEEP_SEC`. | atop `_request` in `client.py` |
| 9 | **Resource taxonomy** (which entities to shard; Gusto payroll vs employees; Deel contracts vs payments; Brex card vs cash) | all four | Make the entity list a constant; start with the cash/payment-flow entity (highest signal value), add others later. | planner `_decode_*` + child-table seed |

**Full TODO(human) list to emit in code (9 categories × up to 4 sources):** one signature-scheme TODO per `signatures/<s>.py` (4); one tenant-id-field TODO per `_extract_<s>` (4); one pagination + one incremental-filter TODO per `fetchers/<s>.py` (8); one OAuth-refresh TODO per Ramp/Gusto `client.py` + `oauth.py` (4); one host TODO per source in `endpoints.py` (4); one endpoints/scopes TODO per `client.py` (4); one resource-taxonomy TODO per planner (4). Total ≈ 32 visible markers.

---

## 6. Build order recommendation

**Recommendation: build ONE proven vertical slice first (Brex, the Mercury clone), land it through the all-13 gate, then replicate the remaining three — but stage the four migrations and shared-file edits in a single coordinated pass at the end.**

Rationale: the shared files are a **serialization point** — 12 of the ~22 seams are edits to the *same* shared files (`fetchers/__init__.py`, `planners/__init__.py`, etc.). Four people editing `router.py`'s three dicts or the `0095…0098` CHECK chain in parallel guarantees merge conflicts and risks a non-strict-superset CHECK ordering. Prove the pattern once end-to-end (Brex), then the per-source work is mechanical.

**Per-source-ISOLATED files (safe to write fully in parallel — no collision):**
- `services/ingest/integrations/<s>/*` (all 5 files)
- `services/ingest/ingestion/{fetchers,planners,handlers,reconcilers}/<s>.py`
- `services/app/webhooks/signatures/<s>.py`
- `services/ingest/synthetic/{fixtures/<s>_generator,mock_clients/<s>,mock_servers/<s>}.py`
- `services/ingest/ingestion/{fetchers,handlers}/tests/test_<s>.py`
- `scripts/sandbox_<s>.py`
- `db/migrations/009N_<s>.sql` (per-file, but the CHECK IN-list must be coordinated — see below)

**SHARED files (must be consolidated / edited serially, single owner):**
- `fetchers/__init__.py`, `planners/__init__.py`, `reconcilers/__init__.py`, `handlers/__init__.py` (4 registries)
- `signatures/__init__.py`, `tenant_resolver.py`, `router.py`, `envelope.py`, `idempotency/__init__.py`, `channel_mapping.py`, `_clients.py`, `endpoints.py`, `lib/shared/errors.py`
- `workflows/{shard_fetch.py, source_onboarding.py, tenant_onboarding.py, reconciler.py}` (the two drift-bug surfaces live here: `tenant_onboarding` UNION + `reconciler.py` `set_pool_provider`)
- `gateway/finance_router.py`
- validation: Provider Lab adapters, source-certification evidence, `composition.py`, `preflight.py`, `live_generators/hmac_webhook.py`
- The **migration CHECK chain** — `0095…0098` must be authored together so each IN-list is a strict superset (a parallel author who writes `0097_gusto` without `'ramp'` breaks the chain).

**Suggested sequence**: (1) Brex slice end-to-end incl. all shared edits + `0095` → green all-13 gate. (2) Replicate Deel (second Bearer) → `0096`-equivalent slot but **renumber to 0098** so OAuth pair stays contiguous; recommend keeping numeric order = build order: do Brex(0095), Ramp(0096), Gusto(0097), Deel(0098) even if Bearer pair is built first conceptually — the migration NUMBER must match disk order. (3) Ramp + Gusto (OAuth pair, share the refresh-seam decision). (4) Final consolidation pass on all shared files + the 4-migration CHECK chain + harness in one commit per source-pair to keep diffs reviewable.

---

## 7. Acceptance / verification plan

**"Done" =** all four sources pass the all-16 overlap gate (concurrent backfill + live ingestion across every source, per-tenant-isolated, no drift) AND per-source unit tests are green AND `mkdocs build --strict` passes with the architecture/ingest page + an ADR updated in the same PR (CLAUDE.md rule).

**Commands (run against a throwaway pgvector + `fyralis_test` DB per the running-signal-source-tests memory; superuser-RLS-bypass + 0059-cascade gotchas apply):**

1. **Migrations apply cleanly + idempotently:**
```bash
psql "$DATABASE_URL" -f db/migrations/0095_brex.sql
psql "$DATABASE_URL" -f db/migrations/0096_ramp.sql
psql "$DATABASE_URL" -f db/migrations/0097_gusto.sql
psql "$DATABASE_URL" -f db/migrations/0098_deel.sql
# re-run each to prove idempotency (must be no-op)
```

2. **Per-source unit tests (isolated, fast):**
```bash
.venv/bin/pytest services/ingest/ingestion/fetchers/tests/test_brex.py \
  services/ingest/ingestion/handlers/tests/test_brex.py \
  services/ingest/ingestion/fetchers/tests/test_ramp.py \
  services/ingest/ingestion/handlers/tests/test_ramp.py \
  services/ingest/ingestion/fetchers/tests/test_gusto.py \
  services/ingest/ingestion/handlers/tests/test_gusto.py \
  services/ingest/ingestion/fetchers/tests/test_deel.py \
  services/ingest/ingestion/handlers/tests/test_deel.py -q
```
Assertion sets (clone Mercury/QBO tests): full backfill → `1 snapshot + N children`, `end_of_data is True`, cursor seeded with high-water; warm start → incremental `start=`/WHERE only; empty resource → snapshot only; missing id → `records == []`; registration assert (`get_handler("<s>:<grain>") is handle_<s>_*`); versioned external_id; state-change status flips `kind`; **backfill↔webhook dedup parity** (same external_id both paths).

3. **HMAC generator parity** (now parametrized over the widened `HMAC_PROVIDERS`):
```bash
.venv/bin/pytest services/ingest/synthetic/live_generators/tests/test_hmac_webhook.py -q
```

4. **Signature-verifier round-trip:** add `signatures/tests/test_<s>.py` asserting `verifier.verify` accepts a correctly-signed body and rejects a tampered one; rotation across multiple active secrets.

5. **The headline gate — catalog-complete source certification:**
```bash
.venv/bin/python -m services.ingest.source_certification inventory --require-ready
```
Provider Lab registry parity must cover every canonical source. Per-source
certification evidence carries backfill counts, cross-tenant uniqueness, live
ingress status, and HMAC coverage without a parallel source list.

6. **Preflight gate** (handler-real, partition-coverage): `run_preflight` over the extended `_SOURCE_SPECS` — asserts each fetched record runs through the REAL handler without raising, `draft.external_id` non-null, `draft.occurred_at` inside the live `observations` partition window (timestamps anchored 2026-01 in the generators).

7. **Static checks:**
```bash
.venv/bin/python -c "from services.ingest.ingestion.fetchers import FETCHER_DISPATCH; \
  assert {'brex','ramp','gusto','deel'} <= set(FETCHER_DISPATCH)"
.venv/bin/python -c "from services.ingest.ingestion.raw_tier.envelope import SourceLiteral; \
  from typing import get_args; s=set(get_args(SourceLiteral)); assert {'brex','ramp','gusto','deel'} <= s and len(s)==16"
.venv/bin/python -c "from services.app.webhooks.signatures import VERIFIERS; \
  assert {'brex','ramp','gusto','deel'} <= set(VERIFIERS)"
mkdocs build --strict
```

8. **Drift-bug guards explicitly** (the two the gate historically catches): assert `tenant_onboarding._LOAD_ACTIVE_SOURCES_SQL` contains a UNION arm per new source, and that `reconciler.py` calls `set_pool_provider` for each new reconciler module — a missing UNION arm = source never discovered (silent); a missing `set_pool_provider` = `RuntimeError` on first gap probe.

---

### Key absolute paths referenced
- Migrations dir: `/home/prajwal-adhikari/Desktop/v2/fyraliscore/db/migrations/` (newest `0094_telegram.sql`; new `0095_brex.sql` … `0098_deel.sql`)
- Archetypes: `services/ingest/integrations/mercury/` and `services/ingest/integrations/quickbooks/`
- Shared registries: `services/ingest/ingestion/{fetchers,planners,handlers,reconcilers}/__init__.py`
- `services/ingest/ingestion/raw_tier/envelope.py:24` (`SourceLiteral`)
- `services/ingest/ingestion/workflows/{shard_fetch.py:546,source_onboarding.py:539,tenant_onboarding.py:174,reconciler.py:752}`
- `services/app/webhooks/{router.py:124,tenant_resolver.py:76,signatures/__init__.py:35}`
- `services/app/gateway/finance_router.py:56`
- `lib/integrations/endpoints.py:56` ; `lib/shared/errors.py:521`
- Validation: `services/ingest/source_certification/`,
  `services/ingest/synthetic/provider_lab/`, and
  `services/ingest/synthetic/validation_runs/{composition.py,preflight.py}`
