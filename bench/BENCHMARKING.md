# Benchmarking Fyraliscore

End-to-end guide to the `bench/` system: what each dimension measures, how
it works, and how to run a meaningful benchmark from scratch.

The bench answers two questions:

1. **Did the system get faster, slower, better, worse, or cheaper?** Across
   five dimensions, against a committed baseline, with direction-aware
   thresholds.
2. **Why?** Through opt-in profilers that capture flame graphs, EXPLAIN
   ANALYZE plans, and async execution timelines alongside the metrics.

The primary interface is the UI at **http://localhost:5173/bench**. The
CLI (`python -m bench`) is a thin wrapper around the same orchestrator
for scripted use.

---

## TL;DR

```bash
# 1. Bring up the stack.
docker run -d --name fyralis_bench_pg -p 5433:5432 \
  -e POSTGRES_USER=company_os -e POSTGRES_PASSWORD=company_os \
  -e POSTGRES_DB=company_os pgvector/pgvector:pg16
DATABASE_URL=postgresql://company_os:company_os@localhost:5433/company_os \
  .venv/bin/python -c "import asyncio,asyncpg; from pathlib import Path; from lib.shared.migrations import apply_migrations_dir; \
    asyncio.run((lambda: (lambda c: (c.execute('CREATE EXTENSION IF NOT EXISTS vector'), apply_migrations_dir(c, Path('db/migrations'))))() )())"

# 2. Load real seed data (Pelago demo snapshot — committed in the repo).
cat demo/snapshots/pelago-v1.sql | \
  docker exec -i fyralis_bench_pg psql -U company_os -d company_os

# 3. Start gateway + UI.
DATABASE_URL=postgresql://company_os:company_os@localhost:5433/company_os \
  .venv/bin/uvicorn services.gateway.main:app --host 127.0.0.1 --port 8000 &
cd ui && npm run dev &

# 4. Trigger a run from the UI: http://localhost:5173/bench → "+ New benchmark".
#    Or CLI:
.venv/bin/python -m bench all --runs 5 --profile cpu,db

# 5. Save the baseline. On the run-detail page, click "Save as baseline",
#    then `git add bench/baselines/ && git commit`.

# 6. Make a code change on a feature branch and re-run. The verdict chips
#    show what moved.
```

---

## What "performance" means here

Fyraliscore is an organizational-intelligence runtime — signals flow in,
get embedded, trigger LLM-driven reasoning, and produce Models / Acts.
A change can make the system faster *but* drop retrieval quality, or
improve calibration *but* double LLM cost. So the bench measures **five
orthogonal axes** in parallel; any one regression is reported.

| Dimension | Question it answers |
|---|---|
| **latency** | Did the system get slower? |
| **throughput** | Can we still handle the same QPS? |
| **retrieval_quality** | Are we still surfacing the right Models? |
| **reasoning_quality** | Is the LLM still well-calibrated? |
| **cost** | Did $/Think run change? |

Each dimension produces metrics. Each metric is diffed against a committed
baseline JSON file. Each diff produces a verdict (`ok` / `regression` /
`improvement`) based on per-metric thresholds. The dashboard surfaces
verdicts as colored chips and dimension-specific charts.

---

## How each dimension is benchmarked

### `latency` — per-stage wall-clock percentiles

**Code:** [bench/dimensions/latency.py](dimensions/latency.py)

**What it measures.** Four stages of the Think pipeline, each represented
by a real operation against the local Postgres:

| Stage | Operation timed |
|---|---|
| `ingest` | `pg_notify` round-trip — write-path overhead surrogate |
| `retrieve` | `SELECT count(*)` over `observations` / `models` / `actors` / `commitments` — read-path overhead |
| `think` | Tight in-process Python loop (50k iter, `i*i % 7`) — interpreter / GC overhead |
| `apply` | `BEGIN + SAVEPOINT + ROLLBACK` cycle — transaction-machinery overhead |

**How it runs.** For `n_runs × scenarios` (default 5 × 4 = 20 samples per
stage), times each operation with `time.perf_counter()`. Then computes
`p50 / p95 / p99 / mean` per stage from the sample list using nearest-rank
percentiles (no interpolation — we have ample N).

**Why these operations.** They're cheap, deterministic, and exercise the
same machinery the real Think pipeline uses. The bench is a regression
detector, not an absolute-load test — measuring fixed lightweight
operations is what makes runs reproducible enough for paired comparison.

**Metrics emitted:** `ingest_p50/p95/p99/mean`, `retrieve_*`, `think_*`,
`apply_*` (16 total).

**Chart:** Grouped bar chart — 4 stages × 3 percentiles, with baseline
bars adjacent when a baseline exists.

**Threshold default:** `delta_pct: 0.15` (15% slower → regression).

---

### `throughput` — concurrency sweep

**Code:** [bench/dimensions/throughput.py](dimensions/throughput.py)

**What it measures.** Sustained signals-per-second under three
concurrency levels: 8 / 16 / 32. For each level, dispatches N async
tasks where each task simulates a signal:

```
pg_notify('bench_throughput_probe', '')   # write equivalent
sleep(0.001–0.003)                         # think-time
SELECT 1                                   # read equivalent
```

**How it runs.** `asyncio.Semaphore(concurrency)` caps in-flight tasks.
Each task acquires a connection from the asyncpg pool. After dispatch
completes, computes `signals_per_sec = n_signals / wall_clock` and
`p95 = nearest-rank percentile of per-task latency`.

A level is marked **saturated** if `p95 > 200 ms` — the bench's SLO
ceiling. The lowest saturated concurrency is reported as
`saturation_concurrency`.

**Why this shape.** Real ingest is dominated by an Ollama embed call
(~10–50 ms) + a PG insert. The simulator here is faster — measuring
*relative* improvement under contention is what matters, not absolute
production load. Concurrency 8 → 16 → 32 is enough to expose pool size
limits, lock contention, or GIL hot spots if any are introduced.

**Metrics emitted:** `signals_per_sec_at_c8/16/32`, `p95_latency_at_c*`,
`saturated_at_c*`, `saturation_concurrency` (10 total).

**Chart:** Dual-axis line chart — teal `signals_per_sec` on left axis,
red dashed `p95 latency` on right axis, vs concurrency. Yellow rings
on saturated points.

**Threshold default:** `delta_pct: 0.10` (10% throughput drop →
regression).

---

### `retrieval_quality` — recall@k + per-pathway share

**Code:** [bench/dimensions/retrieval_quality.py](dimensions/retrieval_quality.py)

This dim has **two modes** depending on whether a labeled set exists.

#### Labeled mode (when `bench/fixtures/labeled_retrieval.jsonl` is populated)

Each line of the JSONL file:

```json
{
  "query_text": "Maya pushed a hotfix to main at 3am",
  "tenant_id": "00000000-...",
  "relevant_model_ids": ["uuid-1", "uuid-2", ...]
}
```

For each labeled row:

1. Build a synthetic trigger context from `query_text` + `tenant_id`.
2. Call into `services/retrieval/primary.py:primary_retrieve()` to get
   the top-N model_ids the retriever surfaces (across 4 pathways:
   structural / semantic / temporal / topological).
3. Compute:
   - **recall@k = |retrieved[:k] ∩ relevant| / |relevant|** for k=10/20/80
   - **NDCG@k** using log-2 position discount, with ideal-DCG normalization
   - **per-pathway share** — how many of the top-10 came from each pathway

Reports `recall_at_10/20/80`, `ndcg_at_10`, `pathway_a/b/c/f_share`.

#### Surrogate mode (when labels missing)

If the JSONL file is empty (the default at install), labels can't be
checked. Falls back to timing three representative pathway queries
against the `models` table:

```sql
SELECT id FROM models ORDER BY created_at DESC LIMIT 10               -- pathway A surrogate
SELECT id FROM models ORDER BY last_retrieved_at DESC NULLS LAST LIMIT 10  -- pathway B surrogate
SELECT id FROM models WHERE confidence > 0.5 ORDER BY confidence DESC LIMIT 10  -- pathway C surrogate
```

Reports `pathway_a_ms`, `pathway_b_ms`, `pathway_c_ms`, `labels_in_set=0`.
Useful as a regression smoke — if the GIN index disappears, these
timings jump — but doesn't measure ranking quality.

**Why two modes.** Hand-labeling is the highest-leverage human effort
in the bench (a 50-row label set takes ~2 hours and pays off forever
after). The surrogate mode lets the dim ship usable from day one while
that investment is pending.

**Chart:** Labeled mode → recall@k line chart + pathway-share donut.
Surrogate mode → horizontal bar chart of `pathway_*_ms`.

**Threshold default:** `delta_abs: 0.03` (absolute drop of 0.03 on
recall@10 → regression).

---

### `reasoning_quality` — ECE + pass rate

**Code:** [bench/dimensions/reasoning_quality.py](dimensions/reasoning_quality.py)

**What it measures.** Reads the committed calibration baseline at
[tests/synthesis_harness/baselines/calibration.json](../tests/synthesis_harness/baselines/calibration.json)
— a snapshot from the existing synthesis-harness test suite — and
extracts:

- **ECE** (Expected Calibration Error): weighted mean of
  `|avg_stated_confidence - empirical_correctness|` across 10
  confidence buckets, weighted by bucket population. Lower is better.
  Below 0.05 = well-calibrated.
- **pass_rate**: population-weighted mean of `empirical_correctness`
  across all buckets — how often the engine is right when it makes a
  claim. Higher is better.
- **scenarios_labeled**: how many synthesis-harness cases contributed
  ground-truth labels.

**Why a wrapper, not a fresh run.** Running the 377-case synthesis
harness inline takes minutes and requires a fully seeded DB. The
existing harness already produces this artifact on every CI run; the
bench wraps it so a regression there shows up in `/bench` alongside
the systems metrics.

**Future extension:** shell out to `pytest tests/synthesis_harness` and
parse its calibration emission for live measurement instead of reading
the static baseline. Tracked but not yet wired.

**Metrics emitted:** `ece`, `pass_rate`, `scenarios_labeled` (3 total).

**Chart:** Three KPI cards (ECE / pass rate / scenarios labeled) above
a horizontal **ECE gauge** with shaded zones:
green (<0.05) → yellow (0.05–0.10) → red (>0.10).
Baseline shown as a dashed vertical line for direct comparison.

**Threshold defaults:** `ece: delta_abs: 0.05`, `pass_rate: delta_abs: 0.02`.

---

### `cost` — $ and tokens per Think run

**Code:** [bench/dimensions/cost.py](dimensions/cost.py)

**What it measures.** Reads recent rows from the `think_run_costs`
table — populated post-commit by every Think run via
[services/think/observability.py:record_think_run_cost](../services/think/observability.py).
Each row holds:

```
llm_calls_count, llm_input_tokens_total, llm_output_tokens_total,
llm_cost_usd, latency_total_ms
```

The dim looks at the last `max(50, n_runs * 10)` rows and aggregates:

- **mean_usd_per_run** — average $/Think across the window
- **p95_input_tokens** — 95th-percentile input-token count per run
- **p95_output_tokens** — 95th-percentile output-token count per run
- **mean_llm_calls** — average LLM call count per run
- **total_runs_observed** — how many rows the dim saw

**Why this matters.** A change that speeds up Think by restructuring
the prompt may silently double cost. A change to context-window
trimming may help cost but hurt retrieval quality. Cost has to be on
the dashboard alongside latency.

**Caveat for fresh databases.** Until real Think runs have executed
against the DB, `think_run_costs` is empty and the dim returns zeros
with an error note. The plumbing works; it just needs data.

**Metrics emitted:** 5 total (above).

**Chart:** 4 KPI cards (mean $/run, p95 input/output tokens, mean calls)
plus a dual-bar "current vs baseline" view of token usage. Amber banner
when `think_run_costs` is empty.

**Threshold default:** `delta_pct: 0.20` (20% cost increase → regression).

---

## Profiling: capturing the *why*

When a metric regresses, the verdict tells you *which* one. The four
opt-in profilers tell you *why*. Enable via the form (chips on
`/bench/new`) or `--profile cpu,db,trace,memory` on the CLI.

| Profiler | What it captures | Output | Viewer |
|---|---|---|---|
| **cpu** | cProfile of the whole dimension loop. Converted to speedscope JSON. | `bench/artifacts/<run_id>/cpu.speedscope.json` | [FlameGraph.tsx](../ui/src/components/FlameGraph.tsx) — SVG icicle chart, click frames to inspect duration |
| **db** | Every SQL statement via `asyncpg.Connection.add_query_logger`. Top-50 unique queries replayed with `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`. | `bench/artifacts/<run_id>/db_plans.json` | [QueryPlan.tsx](../ui/src/components/QueryPlan.tsx) — expandable plan-tree per query |
| **trace** | structlog `think.*` events converted to Chrome Trace Event Format. Each Think run is a thread; phases (retrieve / validate / apply / commit) are spans. | `bench/artifacts/<run_id>/trace.json` | [TraceTimeline.tsx](../ui/src/components/TraceTimeline.tsx) — Gantt timeline |
| **memory** | `tracemalloc` snapshot before + after; top-50 allocator deltas. | `bench/artifacts/<run_id>/memory.json` | Table view in `BenchProfile.tsx` |

Profilers add overhead — never include them in the run that establishes
the baseline. Use them on the *current* run when investigating a
suspected regression.

---

## How regression detection works

**Per-metric flow** (see [bench/stats.py](stats.py)):

```
metric.value (this run)
  vs
baseline_payload["metrics"][metric.name]  (from bench/baselines/<dim>.json)
  ↓
delta_abs = current - baseline
delta_pct = delta_abs / |baseline|
  ↓
threshold_cfg = bench/thresholds.json[dim][metric] or bench/thresholds.json[dim]["default"]
  ↓
direction-aware:
  if metric.higher_is_better:
    bad_delta = -delta              # e.g. recall@10 dropping
  else:
    bad_delta = +delta              # e.g. p95 latency rising
  ↓
verdict = "regression" if bad_delta > threshold
        | "improvement" if bad_delta < -threshold
        | "ok"
```

**No baseline → `ok` for everything.** First run, fresh repo, or
brand-new metric not yet in the baseline file. Save the run as the
baseline, commit it, then future runs have something to diff against.

**Improvement is never a regression.** If `recall@10` rises by 0.10 or
latency drops by 30%, the verdict is `improvement` and you should
consider snapshotting the new baseline.

**Thresholds are configured per metric** in
[bench/thresholds.json](thresholds.json). Either `delta_pct` (relative —
good for things that scale, like latency or cost) or `delta_abs`
(absolute — good for bounded ratios like recall@k or ECE). The metric's
own entry wins; otherwise the dimension's `"default"` entry is used.

Tuning advice: run the bench against itself 5–10 times on the same code
and observe the natural variance, then set the threshold to ~2× that.
Otherwise you'll get false positives on noise.

---

## End-to-end workflow

### 1. Bring up dependencies

**Postgres** (idempotent — skip if already running):

```bash
docker run -d --name fyralis_bench_pg -p 5433:5432 \
  -e POSTGRES_USER=company_os -e POSTGRES_PASSWORD=company_os \
  -e POSTGRES_DB=company_os pgvector/pgvector:pg16
```

**Migrations:**

```bash
DATABASE_URL=postgresql://company_os:company_os@localhost:5433/company_os \
  .venv/bin/python -c "
import asyncio, asyncpg
from pathlib import Path
from lib.shared.migrations import apply_migrations_dir

async def main():
    c = await asyncpg.connect('postgresql://company_os:company_os@localhost:5433/company_os')
    await c.execute('CREATE EXTENSION IF NOT EXISTS vector')
    await apply_migrations_dir(c, Path('db/migrations'), on_error='warn')
    await c.close()

asyncio.run(main())
"
```

### 2. Seed realistic data

The repo ships with a committed snapshot of the Pelago demo company —
**877 Models, 240 Observations, 35 Actors, 141 Commitments, 6 Goals,
5 Decisions**. Load it once:

```bash
cat demo/snapshots/pelago-v1.sql | \
  docker exec -i fyralis_bench_pg psql -U company_os -d company_os
```

Other snapshots in [demo/snapshots/](../demo/snapshots/):
`meridian-v1.sql.zst`, `northwind-v1.sql.zst`, `truss-v1.sql.zst`. Use
`zstd -d` to decompress if needed.

For populating `think_run_costs` (the cost dimension's data source),
the system needs real Think runs to fire. Drive them via the demo flow:

```bash
TOKEN=$(curl -s -X POST http://127.0.0.1:8000/v1/demo/sessions/start \
  -H 'Content-Type: application/json' \
  -d '{"company_id":"pelago"}' | jq -r .token)

for i in $(seq 1 50); do
  curl -s -X POST http://127.0.0.1:8000/v1/demo/simulator/inject \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -d '{"signal_id":"suggested_1"}'
  sleep 0.3
done
sleep 60   # let the Think worker drain
```

This requires `DEEPSEEK_API_KEY` to be set in `.env` and an Ollama
instance running for embeddings.

### 3. Start the bench surface

```bash
DATABASE_URL=postgresql://company_os:company_os@localhost:5433/company_os \
  .venv/bin/uvicorn services.gateway.main:app --host 127.0.0.1 --port 8000 &
cd ui && npm run dev &
```

Open **http://localhost:5173/bench**.

### 4. Capture the baseline

On the baseline branch (typically `demo-deploy`):

**Via UI:** click `+ New benchmark` → leave all dimensions / N=5 / no
profiles → `Start benchmark`. When done, click `Save as baseline` on
the run-detail page.

**Via CLI:**

```bash
DATABASE_URL=postgresql://company_os:company_os@localhost:5433/company_os \
  .venv/bin/python -m bench all --update-baseline --runs 5
```

Both paths write JSON files to `bench/baselines/<dim>.json`. Commit them:

```bash
git add bench/baselines/ && git commit -m "bench: refresh baselines on demo-deploy"
```

### 5. Measure a change

Cut a feature branch off `demo-deploy`, make your changes, then:

**Via UI:** `/bench/new` → enable the profiles you want (cpu / db / trace)
→ add a note describing the change → `Start benchmark`. The detail page
shows live progress via WebSocket, then auto-flips to the results view.

**Via CLI:**

```bash
.venv/bin/python -m bench all --runs 5 --profile cpu,db --note "HNSW ef_search=80"
# → exits 0 if no regressions, 2 if any metric regressed
```

### 6. Inspect

Every chart on `/bench/runs/<id>` shows current vs baseline with
verdict-colored bars/lines:

- 🔴 red → regression (delta exceeded threshold in the bad direction)
- 🟢 green → improvement (delta exceeded threshold in the good direction)
- ⚫ black → ok (within threshold)

Click any captured profile card to drill into the flame graph / DB plans
/ trace timeline. For diagnosing a regression, the typical path is:

1. See red bar on `retrieve_p95`.
2. Click the run's **db** profile card.
3. Top slow query is `SELECT … FROM models … ORDER BY embedding <=> $1`.
4. Plan shows `Seq Scan` instead of `Index Scan`.
5. Root cause identified — fix the missing/disabled HNSW index.

### 7. Decide

- **All green.** Change is safe to merge from a performance standpoint.
- **Improvements + no regressions.** Consider clicking `Save as baseline`
  to lock in the wins.
- **Regression that's intentional** (e.g. trading latency for recall). Bump
  the threshold in [bench/thresholds.json](thresholds.json), document why
  in the commit message, and commit both.

---

## File map

| Path | Role |
|---|---|
| [bench/runner.py](runner.py) | Orchestrator. Runs dimensions, persists metrics, emits NOTIFY for live progress |
| [bench/store.py](store.py) | `bench_runs` / `bench_metrics` / `bench_profiles` DB access + LISTEN/NOTIFY channels |
| [bench/stats.py](stats.py) | Percentiles, paired-delta, direction-aware regression decision |
| [bench/report.py](report.py) | Markdown + JSON report writer |
| [bench/cli.py](cli.py) | `python -m bench` entry point |
| [bench/dimensions/](dimensions/) | One module per axis (latency, throughput, retrieval_quality, reasoning_quality, cost) |
| [bench/profiling/](profiling/) | cpu / db / trace / memory profilers |
| [bench/baselines/](baselines/) | Committed baseline JSON files (one per dimension) |
| [bench/thresholds.json](thresholds.json) | Per-metric regression thresholds |
| [bench/config.json](config.json) | Default baseline branch + run limits |
| [bench/fixtures/labeled_retrieval.jsonl](fixtures/labeled_retrieval.jsonl) | Hand-labeled retrieval ground truth |
| [bench/artifacts/](artifacts/) | Profile artifacts (gitignored — local only) |
| [bench/reports/](reports/) | Markdown reports per run (gitignored — local only) |
| [db/migrations/0035_bench_runs.sql](../db/migrations/0035_bench_runs.sql) | Schema for `bench_runs` / `bench_metrics` / `bench_profiles` |
| [services/gateway/bench_routes.py](../services/gateway/bench_routes.py) | REST endpoints under `/v1/bench/*` |
| [services/gateway/bench_ws.py](../services/gateway/bench_ws.py) | WebSocket at `/stream/bench/runs/<id>` for live progress |
| [ui/src/pages/Bench*.tsx](../ui/src/pages/) | Bench dashboard, new-run form, run detail, profile viewer, compare, trends, baselines |
| [ui/src/components/bench/](../ui/src/components/bench/) | Dimension-specific chart components |
| [ui/src/components/FlameGraph.tsx](../ui/src/components/FlameGraph.tsx), [QueryPlan.tsx](../ui/src/components/QueryPlan.tsx), [TraceTimeline.tsx](../ui/src/components/TraceTimeline.tsx) | Profile viewer components |

---

## Things to keep in mind

- **Only one benchmark runs at a time per instance.** Enforced by the
  POST `/v1/bench/runs` handler (returns 409 if any row in `bench_runs`
  has `status='running'`) and by a partial unique index in the migration.
  The UI's submit button is disabled with a tooltip when this guard
  fires.

- **The dev branch baseline is `demo-deploy`, not `main`.** See
  [bench/config.json](config.json). Switch when `main` catches up.

- **Profilers should never be on for the baseline run.** Their overhead
  distorts the latency numbers you're trying to measure. Use them on
  the *current* run, when you already suspect something regressed.

- **Threshold tuning matters.** Defaults are conservative
  (latency +15%, cost +20%, recall@10 −0.03). Run 5–10 same-code rounds
  to learn natural variance before trusting any verdict.

- **The bench measures relative regression, not absolute load.** It's
  designed to detect "this PR made things worse," not to certify
  production capacity. For load testing, use a different tool against
  staging.

- **Stop the stack when done:**
  ```bash
  docker stop fyralis_bench_pg && docker rm fyralis_bench_pg
  pkill -f 'uvicorn services.gateway.main'
  pkill -f 'vite'
  ```
