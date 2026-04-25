# CompanyOS baseline — real-integration notes (Phase 2.1)

`CompanyOSBaseline` ships with two client implementations:

| Client              | Default? | External services                              |
| ------------------- | -------- | ---------------------------------------------- |
| `MockCompanyOSClient`  | yes   | none                                           |
| `LocalCompanyOSClient` | no    | Postgres + pgvector, Ollama (nomic-embed-text), an LLM provider (Anthropic / DeepSeek) |

Select at construction time:

```python
CompanyOSBaseline()                      # mock (default)
CompanyOSBaseline(client="mock")         # explicit mock
CompanyOSBaseline(client="local")        # real local integration
CompanyOSBaseline(client=my_client)      # inject your own
```

Or through the registry factory params (`SUTConfig.params["client"]`)
or the `LSOB_COMPANY_OS_CLIENT` environment variable
(`mock` | `local`).

---

## Repo layout assumptions (verified 2026-04-23)

The parent Company OS workspace lives at
`/Users/rachinkalakheti/fyraliscore/` and ships a valid
`pyproject.toml` (name `company-os`, Python >= 3.11). Its packages are
importable as:

- `services.*` — `ingestion`, `bridge`, `think`, `query`, `models`, …
- `lib.shared.*` — `db`, `errors`, `ids`, `types`, `trust`
- `lib.embeddings.ollama` — embedding client
- `lib.llm.provider` — LLM fanout

The local-path install is wired in `packages/lsob-baselines/pyproject.toml`
under the `heavy` optional-dependency group so baseline installs stay
lightweight by default.

## SUT protocol → Company OS mapping

| SUT method                         | Company OS touch-point                                                                 |
| ---------------------------------- | -------------------------------------------------------------------------------------- |
| `startup(config)`                  | `lib.shared.db.init_pool(DATABASE_URL)`                                                |
| `apply_ablation(a)`                | Env-var overrides (see below) on `services.retrieval.config.CONFIG`                    |
| `ingest_signal(sig)`               | `services.ingestion.core` via the `internal:state_change` handler                      |
| `query_beliefs_at(q)`              | `services.query.api.answer_query` / `services.models.repo`                             |
| `query_at_risk_at(ts)`             | `services.bridge.queries.revenue_at_risk(tenant_id, horizon_days=90)`                  |
| `produce_diff_for_trigger(t)`      | `services.think.reason.think` wrapped as `produce_diff_for_trigger`                    |
| `shutdown()`                       | close pool                                                                             |

### Tenant isolation

Company OS scopes every table on `tenant_id: UUID`. `LocalCompanyOSClient`
takes the tenant from `SUTConfig.tenant_id`, else from the instance
constructor, else mints a fresh UUID v4 at `startup()` time so each
benchmark run is isolated.

### Ablation → feature-flag translation

The parent repo does not (yet) have a cross-cutting per-tenant
feature-flag table. `LocalCompanyOSClient.apply_ablation` translates
each `disable_*` flag into a process-level env var:

| AblationConfig flag              | Env var                              |
| -------------------------------- | ------------------------------------ |
| `disable_bridge`                 | `LSOB_DISABLE_BRIDGE=1`              |
| `disable_calibration`            | `LSOB_DISABLE_CALIBRATION=1`         |
| `disable_second_pass`            | `RETRIEVAL_SECOND_PASS_ENABLED=false` (existing RA-5 knob) |
| `disable_activation`             | `RETRIEVAL_ACTIVATION_ENABLED=false` (existing RA-5 knob)  |
| `disable_entity_resolver`        | `LSOB_DISABLE_ENTITY_RESOLVER=1`     |
| `disable_pattern_precipitation`  | `LSOB_DISABLE_PATTERN_PRECIPITATION=1` |
| `disable_model_composition`      | `LSOB_DISABLE_MODEL_COMPOSITION=1`   |

The `RETRIEVAL_*` knobs already exist in
`services/retrieval/config.py`. The `LSOB_DISABLE_*` knobs are **not
yet honoured** by the parent; making them effective requires the
follow-up changes in the next section.

---

## Gaps — what needs to land in the parent repo for full integration

The `LocalCompanyOSClient` boots, connects, and allocates tenants
correctly. What it does **not** do (yet) end-to-end:

1. **Ingest payload shaping.** `services.ingestion.core.UniformIngestPath`
   expects a handler-shaped dict with trust tier + entity hints. We
   need a helper (e.g. `lsob_bridge.build_internal_payload(signal)`)
   that translates a `Signal` into that shape with
   `channel="internal:state_change"`.

2. **Belief query translation.** `services.query.api.answer_query`
   returns the parent's `QueryResult` Pydantic shape. We need a small
   adapter that projects it onto `lsob_contracts.Belief`. Fields line
   up 1:1 except `proposition_kind` (needs a string enum round-trip).

3. **At-risk mapping.** `services.bridge.queries.revenue_at_risk`
   returns `RevenueAtRiskReport` with per-customer rows. The LSOB
   `AtRiskReport` wants a flat list of `AtRiskItem` with
   `entity_ref=EntityRef(kind="customer", ...)` — straightforward
   projection, just not implemented yet.

4. **Think diff translation.** `services.think.reason.think` emits a
   `ThinkRunOutcome` containing a diff schema with `claim_ops` /
   `act_ops`. Shapes are similar to `lsob_contracts.DiffOp`; a field
   renamer is needed.

5. **Ablation enforcement.** The `LSOB_DISABLE_*` env vars have no
   readers in the parent. Each disabled module needs a feature-flag
   check at its entry point, e.g. in
   `services/bridge/queries.py:revenue_at_risk` an early return when
   `os.getenv("LSOB_DISABLE_BRIDGE") == "1"`.

6. **Schema migrations.** The parent expects migrations applied
   (`db/migrations/` — 0001–0004+). `LocalCompanyOSClient.startup`
   does not currently run them; callers must set up the schema out of
   band (docker-compose at the repo root does this).

Until items 1-5 are done, `LocalCompanyOSClient` connects but its
query/ingest/diff methods are intentionally no-ops with a structured
rationale (`"local: no-op diff (integration wiring in progress)"`).
The mock path remains the fully-functional default.

---

## Running the integration test

The integration test under
`packages/lsob-baselines/tests/test_company_os_real_integration.py` is
skipped unless **both**:

- `LSOB_COMPANY_OS_REAL=1`
- the parent Company OS packages import cleanly

are true. When it runs, it performs two `lsob run` invocations:

```
lsob run --corpus fixtures/mini_corpus_a.json --sut company-os --ablation none
lsob run --corpus fixtures/mini_corpus_a.json --sut company-os --ablation no-calibration
```

and asserts at least one Layer-3 metric differs between them.

---

## Quickstart for a local-backed run

```bash
cd /Users/rachinkalakheti/fyraliscore
docker-compose up -d postgres ollama
export DATABASE_URL=postgresql://company_os:company_os@localhost:5432/company_os
export OLLAMA_URL=http://localhost:11434
# apply migrations
python scripts/apply_migrations.py

cd /Users/rachinkalakheti/fyraliscore/lsob
export LSOB_COMPANY_OS_REAL=1
export LSOB_COMPANY_OS_CLIENT=local
uv run lsob run \
  --corpus fixtures/mini_corpus_a.json \
  --sut company-os \
  --ablation no-calibration
```
