# Latest-System Component Cleanup Handoff

**Date:** 2026-07-19

**Worktree:** `/Users/rachinkalakheti/fyraliscore-autonomous-learning`

**Branch:** `codex/autonomous-company-learning`

**Starting HEAD:** `eec044a5`
**Scope:** component separation and cleanup foundation only

## Outcome

The latest Physics–Brain–Intent system is now separated at the architecture
registry level into C0, P1–P10, and E0. The registry records current owned paths,
shared legacy hotspots, test roots, writers, contracts, dependencies, forbidden
responsibilities, and the next component gate. The checker validates component
and test paths and rejects an `implemented` writer whose package is absent.

No production feature, provider experiment, TI3 run, CF3-C run, database schema,
or external effect was executed or changed.

## Sources Reconciled

- User-supplied July 15 revised system document:
  `/Users/rachinkalakheti/Downloads/revised-reality-belief-intent-system-implementation.md`
  (`b55a0616ae361d772ea51e9c91017d1b3e0c61d81558fb05fd6da0014ff1a970`).
- Repository July 16 normative projection
  `docs/plans/revised-reality-belief-intent-system-implementation.md`
  (`a3e75c96080101cc547b5ea144c51a57988b6ff5f054aa32b57d1d234daecf4a`).
- July 19 TI3 recovery handoff and LOG-071 through LOG-078.
- Current code, tests, migrations, import ratchets, runtime process manifest,
  and existing architecture registry.
- Historical status maps were consulted as leads only; several still describe
  removed demo/UI or older worker/graph states and are not current truth.

## What Was Achieved

1. Added a checked `RegistryComponent` model to
   `lib/architecture_registry.py`.
2. Added C0, P1–P10, and E0 to `architecture/registry.yaml`.
3. Made sole ownership of `owned_paths` enforceable while allowing deliberately
   explicit `shared_legacy_paths`.
4. Added path checks for component implementation and test roots.
5. Added a check that `implemented` writer package paths exist.
6. Corrected the stale digest for the normative evaluation framework.
7. Downgraded `PolicyRegistryApplier`, `RepairLedgerApplier`, and
   `WriterEpochApplier` from implemented to partial because their claimed
   packages do not exist on this branch.
8. Added missing latest-system writer identities, including EpistemicApplier,
   InquiryRecorder, PhysicalStateApplier, CriteriaProjector, projection writers,
   authority/trace writers, and TransactionKernel, with honest planned/partial
   status where the target package or writer is not materialized.
9. Published `docs/reference/LATEST-SYSTEM-COMPONENTS.md` with the human map,
   P1 subcomponents, mixed seams, proof ladder, and clean-component definition.
10. Published `docs/plans/latest-system-component-cleanup-plan.md` with debt
    metrics, risks, ten cleanup waves, exit gates, and metrics.
11. Updated the architecture reference to distinguish current physical runtime
    from target semantic ownership.
12. Added a durable learning-log entry: end-to-end runs consume component proof;
    they must not be the first test of component contracts.
13. Added the component-registry checker to the static architecture CI job.

## Failures And Lessons

### End-to-end testing was debugging lower layers

TI3 exposed exact provider-schema identity drift and missing durability for
schema-invalid/non-JSON outcomes. Those are L0–L2 defects, not discoveries that
should require a 21-call experiment.

### The registry was structurally valid but physically optimistic

It checked contracts/invariants/projection digests but not component boundaries
or writer package existence. A registry can therefore be internally well-typed
while describing code that is absent. Physical path validation is now part of
internal validity.

### Package names are not component boundaries

`outcomes`, `execution`, `intent`, SAGE, Models/Bridge/topology, and platform
execution each mix multiple latest-system responsibilities. Moving these whole
directories would preserve the confusion under new names. They need ports,
writer ownership, and characterization before physical splitting.

### Old-looking is not dead

Prior cleanup work proved that situation/operating/model-edge and worker paths
often remain live through imports, tests, manifests, migrations, or product
readers. Retirement requires a liveness and replay proof, not naming judgment.

### Later implementation evidence does not redefine the target

The July 16 repo projection is newer than the supplied July 15 file and adds
adopted implementation amendments, but live code remains a hybrid. Conflicts
must be recorded as gaps, not resolved by assuming the implementation is right.

## Validation

Passed:

- `13 passed` in `lib/contracts/tests/test_registry.py` after the final writer
  inventory expansion.
- `scripts/check_architecture_registry.py` is internally valid with registry
  digest `82b29566cbaf31bc37368fd70a4fa6e939f53d75fc175d803c6dc8cf0b2a1511`.
- `scripts/check_architecture_ratchets.py` passed.
- `compileall` passed for `lib/architecture_registry.py`.
- `git diff --check` passed.

Unavailable:

- Ruff is not installed as an executable or Python module in this worktree.

Expected red baseline:

- `scripts/check_tech_debt_budget.py` remains red: 61 files versus budget 29,
  156 functions versus budget 16, and 36 classes versus budget 21, plus named
  inquiry/Think/retrieval/ingestion/gateway hotspots. This pass organized the
  debt and did not claim to reduce it.

## What Remains

The registry-level separation is foundation work, not the cleanup itself.

1. Finish Wave 0 by requiring component ownership for new production paths.
2. Build the Wave 1 per-component physical inventories: imports, symbols,
   migrations/tables, readers/writers, routes, workers, config, tests, replay,
   rollback, compatibility, and retirement candidates.
3. Start with C0/P10/P9/E0 boundaries, then prove P1A–P1I before a broad
   ingest-to-reasoning run.
4. Resolve P3's frozen schema identity mismatch provider-free:
   `SynthesisSemanticDecision` / `think-synthesis-semantic-decision-v1` versus
   the implemented provider-v2 name.
5. Separate canonical relations from Models/model_edges/topology/Bridge derived
   or compatibility roles.
6. Split P2/P6/P9 ownership inside intent/outcomes/execution.
7. Decompose SAGE across P3/P4/P5/P7/P8 or retire duplicate responsibilities.
8. Make P8 projections rebuildable and epistemically explicit.
9. Prove P7 learning only after independent outcomes and settlement are clean.
10. Retire compatibility code and stale documentation only with measured
    removal gates.

## Exact Next Action

1. Review and commit this component-separation checkpoint.
2. Begin Wave 1 with C0, because every later inventory needs stable contract,
   writer, compatibility, and evidence-manifest rules.
3. In parallel only where files do not overlap, inventory P10, P9, and E0.
4. Do not restructure or delete shared legacy hotspots until their component
   inventories are complete.

```bash
.venv/bin/python -m pytest lib/contracts/tests/test_registry.py -q
.venv/bin/python scripts/check_architecture_registry.py
.venv/bin/python scripts/check_architecture_ratchets.py
.venv/bin/python -m compileall -q lib/architecture_registry.py
git diff --check
```

## Stop Rules

- Do not run TI3 or CF3-C from this cleanup checkpoint.
- Do not use a provider or broad E2E run to validate component separation.
- Do not delete migrations or shared legacy packages from the registry map.
- Do not mark planned/partial writers implemented without physical package,
  focused tests, and the required component evidence.
- Do not raise technical-debt budgets to make the baseline green.
- Do not treat an architecture-registry pass as production-freeze readiness;
  the checker must continue to report `production_freeze_ready = false` while
  contracts remain partial.
