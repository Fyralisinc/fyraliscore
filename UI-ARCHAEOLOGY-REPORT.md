# Fyralis UI — Archaeology Report

> **What this is:** a reconstruction of the front-end ("CEO view") web app that
> used to live in this repo, written for both humans and AI agents. Every claim
> is sourced from git history at the last commit that contained the UI.
> **Reconstructed from:** `git show 82f65ac:ui/...` (the commit immediately
> before removal). **Report generated:** 2026-06-10.

---

## 1. TL;DR / Provenance

| Fact | Value |
|---|---|
| **When removed** | Commit `3a43d17`, **2026-06-05** (Fri Jun 5 2026), authored by Prajwal Adhikari |
| **Removal PR** | #56 — *"Split demo overlay into its own repo + productionize core (backend-only)"* |
| **Last commit WITH the UI** | `82f65ac` (parent of `3a43d17`) |
| **Where it lived** | `ui/` (plus `simulation/`, `demo/`, `mocks/`, `nginx-ui.conf`, `Dockerfile.ui`) |
| **Where it went** | Extracted to a separate overlay repo: **`fyraliscore-demo`** |
| **Why removed** | Make core a pure, demo-free, **backend-only** runtime. UI + demo + simulation became an *overlay* that plugs back in via Python entry-point seams; core imports nothing from it. Enforced by an `import-linter` contract ("core never imports the demo / simulation overlays"). |
| **Scale of removal** | `3a43d17` deleted **611 files / ~109,613 lines**; the `ui/` slice alone was **225 files**, **~30,620 LOC** of `.ts`/`.tsx`. |
| **DB side-effect** | Migration `0093` dropped demo tables (the `tenants` table + `is_demo` flag stayed). |

**Core seams left behind** (the plug-back-in points the overlay UI/demo used):
- `services/app/gateway/extensions.py` — gateway route/extension registration
- `lib/shared/events.py` — event bus
- `services/reasoning/think/hooks.py` — reasoning augmentors

---

## 2. Identity & Naming

- **Internal package name:** `company-os-ui` (`ui/package.json` → `"name"`). The
  product/codename in code is **"Company OS" / "CEO view"**; the user-facing brand
  is **Fyralis**.
- **Browser title:** `Fyralis · Today` (`ui/index.html`).
- **Persona baked into the demo:** founder **"Diana", role "CEO"** (hard-coded in
  the sidebar user card).
- **Anchor tenant:** **Pelago** — the demo always boots the founder *inside* the
  Pelago tenant; there is no company picker.

---

## 3. Tech Stack

### Runtime / framework
| Concern | Choice | Version |
|---|---|---|
| UI library | **React** | 18.3.1 |
| Routing | **react-router-dom** (SPA, `BrowserRouter`) | ^6.26.2 |
| State | **Zustand** | ^5.0.13 |
| Language | **TypeScript** | 5.5.4 |
| Build / dev server | **Vite** | 5.4.8 (`@vitejs/plugin-react` 4.3.2) |
| Styling | **Tailwind CSS** + PostCSS + Autoprefixer + hand-written CSS | tailwind ^3.4.13 |
| Graph visualization | **Cytoscape** + `react-cytoscapejs` + `cytoscape-cose-bilkent` layout | cytoscape ^3.33.3 |
| Color scales | **d3-scale-chromatic**, **d3-color** | ^3.x |

### Tooling
| Concern | Choice |
|---|---|
| Unit/component tests | **Vitest** 2.1.2 + Testing Library (react/dom/jest-dom/user-event) + **jsdom** 25 |
| End-to-end tests | **Playwright** 1.47.2 (`USE_MOCK=1 playwright test`) |
| Mock backend | In-process Vite plugin (`ui/mock-server.ts`) + `ws` 8.18 for streaming |
| Type-check gate | `tsc --noEmit` (run before `vite build`) |
| Path alias | `@/*` → `ui/src/*` |

> **Not used:** no Next.js, no Redux, no component library (MUI/Chakra/etc.), no
> CSS-in-JS runtime. It was a hand-built Vite SPA with Tailwind tokens + bespoke CSS.

### npm scripts (`ui/package.json`)
- `dev` — Vite against the real FastAPI gateway
- `dev:mock` / `USE_MOCK=1` — Vite with the in-process mock backend (zero external deps)
- `build` — `tsc --noEmit && vite build`
- `test` / `test:watch` — Vitest
- `test:e2e` — Playwright (mock mode)
- `typecheck` — `tsc --noEmit`

---

## 4. How the UI Talked to the Backend

Two run modes, selected by the `USE_MOCK` env var (`ui/vite.config.ts`):

1. **Mock mode** (`USE_MOCK=1`): a Vite middleware plugin (`mock-server.ts`)
   serves `/api/*` and `/stream/*` locally from `src/api/*-mock.ts` fixtures.
   Used for development + all Playwright e2e.
2. **Live mode** (default `npm run dev`): Vite **proxies**
   - `/api/*` → `http://localhost:8000` (FastAPI gateway), path rewritten to strip `/api`
   - `/stream/*` → `ws://localhost:8000` (WebSocket), strip `/stream`

**Demo session model** (`src/shell/AutoDemoSession.tsx`): on first load, the app
`POST`s to start a **Pelago** demo session, stores the returned auth token in
`localStorage`, and only then renders the page. Every primary route is wrapped in
`<AutoDemoSession>` so the four pages always render against a real tenant.

**API surface** (`src/api/routes.ts`) — the backend contract the UI depended on,
grouped by feature:
- `ceo.*` — `/view/ceo/home`, `/view/ceo/ask`, `/view/ceo/turn-action`
- `decisionDeltas.*` — list / detail / **accept / delegate / contest / add_context**
- `today.*` + `todayPage.*` — briefing payload, deltas, evidence, apply, delegate, correction; recommendation act/dismiss/triage/watch
- `forecasts.*` — page (by horizon days), detail, patterns, ask, accuracy, create scenario
- `map.*` + `modelPage.*` + `modelTrace.*` — graph snapshot, topology events, overview/categories/relationships/items, **trace (supports / depends_on)**
- `spec.*` — operating threads, decision deltas, forecasts, ledger events
- `structure.*` — commitment/resource overlays, recent changes
- `demo.*` — companies, sessions (start/end/reset), **simulator suggested / inject**
- `recommendationStream` — `/v1/recommendations/stream` (live recommendations)

---

## 5. Architecture & Folder Organization

The UI was a layered Vite SPA. Directory map at `82f65ac` (file counts):

```
ui/
├── index.html               # SPA root, font preconnects, mounts /src/main.tsx
├── package.json             # company-os-ui
├── vite.config.ts           # mock vs proxy modes
├── tailwind.config.js       # design tokens (palette/typography/easing)
├── playwright.config.ts
├── mock-server.ts           # in-process mock API + WS
├── scripts/                 # screenshot-today.mjs, spec-smoke.mjs
├── e2e/                     (11 files)  Playwright specs
└── src/
    ├── main.tsx             # router + global CSS imports (entry)
    ├── shell/        (3)    # AppShell, Sidebar, AutoDemoSession
    ├── pages/        (23)   # today-v2, model-v2, forecasts, ledger-v2 (+ legacy model/, today-v2/)
    ├── components/   (98)   # feature components, grouped per surface
    ├── api/          (40)   # typed clients + mock fixtures + route map
    ├── hooks/        (4)    # useTodayPage, useForecastsPage, useSpecData, useDecisionDeltas
    ├── lib/                 # store.ts (zustand), time.ts
    ├── styles/       (10)   # global + per-surface CSS
    ├── debug/        (12)   # internal operator console (separate layout)
    └── tests/        (7)    # Vitest unit/component tests
```

### Layering model
- **Shell layer** (`src/shell/`): `AppShell` = a 3-slot CSS-grid frame
  (`sidebar | main | optional inspector`) with an `expanded`/`collapsed` sidebar
  mode. `Sidebar` = primary nav + shortcuts + utilities + a **Model-health card**
  (live status + sparkline) + user card. `AutoDemoSession` = the demo bootstrap gate.
- **Page layer** (`src/pages/`): one folder per primary surface; each page owns a
  small local state machine and composes the shell + its feature components.
- **Component layer** (`src/components/`): grouped by surface — `today-v2/`,
  `forecasts/`, `ledger-v2/`, `model/`, `spec/`, `primitives/`, `SignalSimulator/`.
- **API layer** (`src/api/`): every surface has a typed `*-client.ts`, a
  `*-types.ts`, and a `*-mock.ts` fixture; `routes.ts` centralizes URLs.
- **Hooks layer** (`src/hooks/`): data-fetching hooks bridging API → pages.

> **Note — two UI generations coexisted in the tree.** `main.tsx` routes
> *exclusively* to the **"v2" generation** (`today-v2`, `model-v2`, `forecasts`,
> `ledger-v2`). An earlier **"spec" generation** (`components/spec/` — `Shell`,
> `Sidebar`, `CommandPalette`, `LensBar`, `CausalSpine`…) and a v1 `pages/model/`
> remained in the tree as superseded layers, no longer mounted by the router.

---

## 6. Routing Map

Defined in `src/main.tsx`. SPA via `BrowserRouter`. `/` → redirect to `/today`.

| Route | Surface | Wrapped in demo session? |
|---|---|---|
| `/today` | **Today** briefing | ✅ |
| `/model` | **Model** graph | ✅ |
| `/forecasts` | **Forecasts** | ✅ |
| `/ledger` | **Ledger** | ✅ |
| `/debug/*` | Internal operator console (own layout) | ❌ |

**Legacy redirects** (all collapse into the four primary surfaces): `/structure`,
`/map`, `/commitments`, `/customers`, `/risks`, `/decisions`, `/owners`, `/teams`,
`/sources` → `/model`; `/history` → `/ledger`; `/mind`, `/demo`, `/ask`,
`/settings` → `/today`; `/today/review/:deltaId` → `/today?expand=:deltaId`
(deep-link compatibility for Slack messages / bookmarks).

The sidebar still *showed* the richer nomenclature (Commitments, Customers, Risks,
Decisions, Owners, Teams, Ask Fyralis, Sources, Settings) — those were
forward-looking shortcuts that redirected into the four built surfaces.

---

## 7. The Surfaces (UX Flow per Page)

### 7.1 Today — `pages/today-v2/Briefing.tsx`
The default landing surface. **Single combined layout, no mode switching.** ASCII
from the source header:

```
┌────────────────────────────────────────────────────────────┐
│ Sidebar │ Today header (briefing line + Ask Fyralis search) │
│         │ Fyralis Brief (synthesis · what changed · handled)│
│         │ ┌──────────────┬─────────────────────────────────┐│
│         │ │ Review queue │ Focused review sheet            ││
│         │ │ rail         │   (selected proposed change)    ││
│         │ └──────────────┴─────────────────────────────────┘│
│         │ Action bar (Accept · Delegate · Request · Report) │
└────────────────────────────────────────────────────────────┘
```

- The unit of work is a **Decision Delta** (a proposed change). A delta is always
  selected; its **Focused Review sheet** renders beside the **Review Queue Rail**.
- **Flow:** read the *Fyralis Brief* (AI synthesis: what changed / what was handled
  without you) → pick a delta in the rail → review evidence in the
  **Evidence Drawer** → act via the **Action Bar**: **Accept / Delegate / Request
  more / Report**. Delegation and corrections open dedicated sheets
  (`DelegationSheet`, `CorrectionSheet`). A `Toast` confirms outcomes.
- URL stays in sync via `?review=<id>` / `?expand=<id>` for deep-linking.
- Key components: `BriefingHeader`, `FyralisBrief`, `ReviewQueueRail`,
  `FocusedReviewCard`, `ReviewActionBar`, `EvidenceDrawer`, `HandledWithoutYou`,
  `ChangeDiff`, `PrimaryJudgmentPreview`, `AskFyralisStrip`.

### 7.2 Model — `pages/model-v2/ModelPage.tsx`
An interactive **graph/knowledge canvas** of the company model (rendered with
Cytoscape).
- **Local state machine** with a **back stack**: one active focus at a time;
  `Esc`/browser-back pops to the previous view.
- **Canvas views** (`canvas/`): `OverviewMap` → `NodeNeighborhood` →
  `RelationshipCorridor` → `TracePath` (provenance: supports / depends-on, with
  direction + depth). Custom edge geometry (`edgeGeometry.ts`, `ArrowDefs`).
- **Relationship modes** (e.g. `impact`) chosen via a `RelationshipModeBar`.
- Overlays/sheets: `CategorySheet`, `SearchOverlay`, `FullDetailSheet`,
  breadcrumb navigation, `ModelMetricsStrip`, `NodeInspector`, `GraphLegend`.

### 7.3 Forecasts — `pages/forecasts/ForecastsPage.tsx`
Implements *"fyralis_forecasts_page_implementation_complete_spec_v1"*.
- Page shape: `ForecastsHeader` → `ForesightBrief` → `ModeSelector` → mode body →
  `AccuracyStrip`.
- **Four modes** (URL `?mode=`, default `horizon`): **Horizon / Patterns /
  Scenarios / Accuracy**. Horizon is driven by a `?horizon=<days>` window
  (default 90, min 14).
- Components: `HorizonMatrix`, `HorizonMode`, `PatternField`, `PatternsMode`,
  `ScenariosMode`, `AccuracyMode`, `ForecastCard`, `ForesightInspector`,
  `ForesightBrief`, `AskFyralis`.

### 7.4 Ledger — `pages/ledger-v2/LedgerPage.tsx`
Implements *"fyralis_ledger_page_implementation_spec_v1"* — the memory/audit trail.
- Page shape: `LedgerHeader` → `LedgerBrief` → `ModeSelector` → mode body.
- **Four modes:** **timeline** (`MemoryRiver` ▸ `ChainInspector`), **resolutions**
  (outcome-bucketed chains), **accuracy** (calibration), **audit** (raw event table).
- Components: `MemoryRiver`, `ChainCard`, `ChainInspector`, `EventTimeline`,
  `StageSequence`, `BeforeAfter`, `EvidenceAtTime`, `ForecastAccuracyBlock`,
  `OutcomeImpactBlock`, `RelatedContext`, `StatusChip`.

### 7.5 Signal Simulator (demo control) — `components/SignalSimulator/`
A floating panel to **inject synthetic signals** into the running demo (maps to
`/v1/demo/simulator/inject` + `…/suggested`). Tabs per source: **Email, Slack,
GitHub, Calendar, Stripe, Custom**, plus `SuggestedSignals`. This is how a
demo-driver fired events that then flowed through the real reasoning pipeline and
surfaced as decision deltas on Today.

### 7.6 Debug console — `src/debug/` (route `/debug/*`)
A separate operator/inspection layout (`DebugLayout`, own `debug.css`, `JsonView`)
for looking *inside* the pipeline. Pages: **Signals** (list + detail),
**Think-Runs** (list + detail), **Models** (list + detail), **Acts**, **Renders**,
**Cache**. Not part of the demo-session wrapper.

---

## 8. Design System / Visual Language

Source of truth: CSS variables in `src/styles/app/*.css`, mirrored into
`tailwind.config.js` so utility classes resolve to the same palette.

### Typography (Google Fonts, preconnected in `index.html`)
- **Sans:** Inter (400–700) — UI text
- **Serif:** Newsreader + Source Serif Pro — editorial/briefing copy
- **Mono:** JetBrains Mono — code/IDs/numbers

### Palette (warm-paper, teal accent, semantic severity)
- **Base/paper:** `#F4F5F7` (base), recess `#E9EBEF`, deep `#DCDFE5`; surfaces white/`#F9FAFB`
- **Ink ramp:** `#0A0A0F` → `#2C2D33` → `#5A5C66` → `#8B8D96` → `#C5C7D0`
- **Accent:** teal `#0F766E` (hover `#115E59`, deep `#134E4A`, soft `#99F6E4`)
- **Semantic severity tokens** (each with bg/bg-2/text variants):
  - **critical** — red `#991B1B`
  - **strategic** — purple `#5B21B6`
  - **high** — amber `#854D0E`
  - **med** — slate `#334155`
- **Radii:** 4 / 6 / 8 / 10 / 14 px
- **Motion easings:** `out` (decelerate), `out-soft`, `spring` (overshoot), `organic`

The aesthetic was a calm, editorial "paper" surface with a single teal accent and
a forest-silhouette SVG decoration in the sidebar — deliberately restrained,
document-like, with serif briefs reading like a newspaper.

---

## 9. Testing

| Layer | Tooling | Location | Count |
|---|---|---|---|
| Component / unit | Vitest + Testing Library + jsdom | `src/tests/` | 7 specs (today-v2, ledger, model, spec-pages, SignalSimulator, api-contract) |
| End-to-end | Playwright (mock backend) | `e2e/` | 11 specs |

**E2E spec names** reveal the headline demo flows that were guaranteed:
`demo-ask-and-glow`, `demo-live`, `demo-live-all`, `demo-reaffirm`,
`demo-reasoning-depth`, `demo-simulator-minimize`, plus per-surface
`today-v2`, `forecasts`, `ledger`, `model`, and a `visual-smoke`. Helper scripts:
`scripts/screenshot-today.mjs`, `scripts/spec-smoke.mjs`.

---

## 10. How to Retrieve the Real Code

The UI is gone from `HEAD` but fully recoverable from history:

```bash
# Last commit that contained the UI:
git show 82f65ac:ui/package.json

# Browse the whole tree:
git ls-tree -r --name-only 82f65ac -- ui/

# Restore the entire ui/ folder into the working tree (does not commit):
git checkout 82f65ac -- ui/

# See exactly what the removal deleted:
git show 3a43d17 --stat
```

The live, maintained version moved to the **`fyraliscore-demo`** overlay repo;
this report describes the snapshot as of `82f65ac` and may diverge from whatever
the overlay repo has done since.

---

## 11. One-Paragraph Summary (for embedding / quick recall)

> Fyralis once shipped a hand-built **React 18 + Vite + TypeScript** single-page
> "CEO view" (internal pkg `company-os-ui`) styled with **Tailwind** design tokens
> (warm-paper palette, teal accent, Inter/Newsreader/JetBrains-Mono type) and
> **Cytoscape** for graph visualization. It had four primary surfaces — **Today**
> (decision-delta review queue + AI "Fyralis Brief" + Accept/Delegate/Request/Report
> action bar), **Model** (interactive knowledge-graph canvas with trace/provenance),
> **Forecasts** (horizon/patterns/scenarios/accuracy modes), and **Ledger** (Memory
> River + chain inspector audit trail) — plus a **Signal Simulator** to inject demo
> events and a `/debug` operator console. It ran either against an in-process mock
> backend or proxied to the FastAPI gateway on :8000, auto-booting a **Pelago** demo
> tenant for founder "Diana, CEO". It was **removed on 2026-06-05 in commit `3a43d17`
> (PR #56)** to make the core a pure backend-only runtime, with the UI/demo/simulation
> extracted into the separate **`fyraliscore-demo`** overlay repo that re-attaches via
> Python entry-point seams. ~30.6k LOC, 225 files, last present at commit `82f65ac`.
