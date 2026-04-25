# Simulation harness

Authoring tool for Company OS dogfood. Lets you (Rachin) drive the
substrate by typing messages as different personas — either through
a simulated Slack UI or YAML-authored scenarios — and watch Think
turn them into Models, Acts, and Resources.

Everything here feeds the existing `services/synthetic` bypass.
Nothing in this directory modifies `services/synthetic/*` or the
ingestion path.

## Prerequisites

- `COMPANY_OS_ENV=dev|staging|test` (the synthetic bypass refuses to
  import in production).
- `DATABASE_URL` pointing at your local Postgres with the Company OS
  migrations applied.
- `OLLAMA_URL` pointing at a running Ollama with
  `nomic-embed-text:v1.5` pulled.

## Quickstart

```bash
# 1. Author scenario replay — produces 38 signals for the Acme
#    Tuesday demonstration. Each signal enqueues a Think trigger;
#    Model / Act / Resource generation happens once the Think
#    worker drains the queue (see services/think/worker.py).
COMPANY_OS_ENV=dev DATABASE_URL=... \
  python -m simulation.scenarios.replay acme_tuesday

# 2. See what Think made of it. NOTE: inspect.py collides with the
#    stdlib `inspect` module, so always run it via `-m`.
COMPANY_OS_ENV=dev DATABASE_URL=... \
  python -m simulation.inspect

# 3. Reset when you want to re-run.
COMPANY_OS_ENV=dev DATABASE_URL=... \
  python -m simulation.reset --confirm

# 4. Interactive authoring via the simulated Slack UI.
COMPANY_OS_ENV=dev DATABASE_URL=... \
  uvicorn simulation.server:app --port 8765
# -> open http://localhost:8765/
```

## Layout

```
simulation/
  personas.yaml              # 12 persona definitions (stable UUIDs)
  personas.py                # load_personas / switch_active_persona / voice_hints_for
  server.py                  # FastAPI app: /simulation/inject, /messages, /personas, /channels + static UI
  slack_ui/                  # React-via-ESM single-page app served by server.py
    index.html
    app.mjs
    styles.css
  workers/                   # Channel CLIs
    github_pr_worker.py
    github_issue_worker.py
    email_worker.py
    calendar_worker.py
    linear_worker.py
    _common.py               # shared bootstrap (pool, run_id, actor seeding)
  scenarios/
    replay.py                # scenario loader + runner
    acme_tuesday.yaml        # 38 events across 7 days ending Tue morning
    quiet_week.yaml          # 9 events; tests "nothing consequential" render
    two_fires.yaml           # 13 events, two concurrent situations
  reset.py                   # purge synthetic observations + dependent Models / Acts
  inspect.py                 # summary of tenant state (observations, Models, Acts, Resources)
  tests/
    test_personas.py         # persona registry unit tests
    test_worker_signal_shape.py
```

## Personas

`simulation/personas.yaml` ships with 12 personas covering the roles
needed for the Acme Tuesday narrative:

| handle | role | title |
| ----- | ---- | ----- |
| alice  | engineer          | Staff Engineer, Payments |
| marcus | head_of_engineering | Head of Engineering |
| monica | head_of_sales     | Head of Sales |
| priya  | customer_success  | CS Lead (Acme) |
| david  | cfo               | CFO |
| nora   | engineer          | Senior Engineer (Rate Limiter) |
| jakob  | head_of_product   | Head of Product |
| sara   | designer          | Staff Designer |
| tomas  | account_executive | Enterprise AE (Acme) |
| evelyn | legal_counsel     | General Counsel |
| ben    | support_engineer  | Support Engineer |
| rachin | ceo               | Founder / CEO |

Each persona has a `voice_style_notes` hint that surfaces above the
composer in the UI. The hint is a reminder to the human author; it
is never fed to an LLM.

Persona UUIDs are hand-authored and stable across re-runs. The
harness seeds them as `actors` + `actor_identity_mappings` rows on
every worker / server boot (idempotent ON CONFLICT).

## Simulated Slack UI

`simulation/server.py` exposes:

- `GET /simulation/health` — tenant + run id + channel count.
- `GET /simulation/personas` — the persona registry.
- `GET /simulation/channels` — the fixed channel list.
- `GET /simulation/messages?channel=<handle>` — last 20 messages.
- `POST /simulation/inject` — composes a `SyntheticSignal` and calls
  `services.synthetic.core.inject()`.

The UI in `slack_ui/`:

- Left sidebar: persona switcher + channel list.
- Top bar: active persona, voice hints, `occurred_at` input (supports
  `now`, `-3h`, `2026-04-22T09:00Z`). When not `now`, the label turns
  red and reads **SIMULATION TIME**.
- Main: last 20 messages in the current channel.
- Composer: textarea; `Cmd/Ctrl+Enter` submits.

No build step. React comes in via an ESM CDN (`esm.sh`) so the UI
only requires the running FastAPI process.

## Channel workers

Every worker is a thin CLI around `services.synthetic.core.inject()`:

```bash
# GitHub PR
python simulation/workers/github_pr_worker.py \
  --persona alice --event merged --pr "refactor billing service" \
  --repo payments --number 847

# GitHub issue
python simulation/workers/github_issue_worker.py \
  --persona ben --event opened --title "acme 429s" --repo payments \
  --number 312 --labels "bug,acme"

# Email
python simulation/workers/email_worker.py --direction outbound \
  --persona tomas --to rachin \
  --subject "Acme renewal update" --body "Quick note."

# Calendar
python simulation/workers/calendar_worker.py \
  --persona monica --event meeting_scheduled \
  --title "Acme decision" --when "+2d 14:00" \
  --attendees rachin,marcus,monica,priya

# Linear
python simulation/workers/linear_worker.py \
  --persona nora --event status_change \
  --ticket ENG-412 --title "rate-limiter refactor" \
  --from-state in_progress --to-state blocked
```

All workers accept `--tenant`, `--run-id`, `--scenario`, and
`--occurred-at` flags (see `_common.py`).

## Scenarios

YAML files in `simulation/scenarios/`. Event schema:

```yaml
name: scenario_id
description: ...
narrative:
  - t: "-7d 15:30"         # relative days + UTC clock
    actor: alice           # persona handle
    channel: eng           # (for kind=slack) channel handle
    content: "..."

  - t: "-6d 09:15"
    actor: marcus
    kind: github_pr        # slack | github_pr | github_issue | email | calendar | linear
    event: opened
    repo: payments
    number: 847
    title: "..."
```

Run:

```bash
python -m simulation.scenarios.replay acme_tuesday
python -m simulation.scenarios.replay acme_tuesday --dry-run
python -m simulation.scenarios.replay acme_tuesday --speed 60
python -m simulation.scenarios.replay acme_tuesday --skip-t1  # backfill
```

- `--dry-run` — prints the parsed events without DB writes.
- `--speed N` — wall-clock speedup; `N=60` means a 24h scenario
  replays in 24 min.
- `--skip-t1` — ingests signals but does not enqueue Think triggers,
  useful when priming historical state.

Shipped scenarios:

- **acme_tuesday** — 38 events across 7 days, ends with Tuesday
  morning's "structurally unsafe" framing; matches design doc §10.2.
- **quiet_week** — 9 low-stakes events; substrate should produce a
  short quiet-day greeting.
- **two_fires** — 13 events, two concurrent situations (Acme renewal
  risk and a third-party CVE). Tests multi-card ranking.

## Reset / inspect

```bash
# Dry-run: show what would be purged
python simulation/reset.py --dry-run

# Purge the default dev tenant's synthetic signals
python simulation/reset.py --confirm

# Scoped purge — only this scenario
python simulation/reset.py --scenario-id acme_tuesday --confirm

# State snapshot
python simulation/inspect.py               # text
python simulation/inspect.py --json        # machine-readable
python simulation/inspect.py --run-id sim-<uuid>
```

`reset.py` removes only observations tagged `content.synthetic=true`
and their dependent Models / Acts / Resources. Personas (actors +
identity mappings) are kept so the next scenario re-run reuses the
same actor_ids. Out-of-scope: production data — the env guard
prevents it.

## Tests

```bash
# Fast unit tests (no DB, no Ollama).
COMPANY_OS_ENV=test pytest simulation/tests/ -v
```

Integration testing of the full ingestion pipeline lives in the
existing `services/ingestion/tests/` suite; the simulation harness
piggybacks on that infrastructure at runtime by calling
`services.synthetic.core.inject()`.
