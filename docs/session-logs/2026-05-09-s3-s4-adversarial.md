# Session log — 2026-05-09 — S3 + S4 + adversarial

**Branch:** `demo-deploy`
**Commits this session:** `4caba1d`, `dea0439`, `aecf077`, `cd2f8a1` (auto-snapshots)
**Final test count:** 377 passing across S1-S4 + adversarial

This log captures the work of one continuous session: implementing
stages S3 and S4 of the **Self-Organizing Substrate** plan, then
running an aggressive adversarial test sweep across both new stages.

---

## What landed

### S3 — Pathway F + topology_events + T6 trigger + neighborhood naming

Arrangement becomes consequential. Phase events of the substrate's
emergent neighborhoods now flow into reasoning end-to-end.

| Component | File |
|---|---|
| Migration: `topology_events` table | [db/migrations/0033_topology_events.sql](../../db/migrations/0033_topology_events.sql) |
| Heuristic naming | [lib/topology/naming.py](../../lib/topology/naming.py) |
| Phase-event log + pure detector | [services/topology/events_repo.py](../../services/topology/events_repo.py) |
| Event emission + named_signature wiring | [services/topology/neighborhoods_repo.py](../../services/topology/neighborhoods_repo.py) |
| T6 enqueue in detector worker | [services/workers/neighborhood_detector/worker.py](../../services/workers/neighborhood_detector/worker.py) |
| Pathway F (HNSW + neighborhood expansion) | [services/retrieval/pathways.py](../../services/retrieval/pathways.py) |
| `DIMENSION_TOPOLOGICAL` + RRF mapping | [services/retrieval/scoring.py](../../services/retrieval/scoring.py) |
| F wired into `primary_retrieve` + T6 weights | [services/retrieval/primary.py](../../services/retrieval/primary.py) |
| F config knobs | [services/retrieval/config.py](../../services/retrieval/config.py) |
| `topology_context` on `ContextBundle` + assembler | [services/retrieval/assembler.py](../../services/retrieval/assembler.py) |
| `<topology_context>` section + T6 instructions | [services/think/prompt.py](../../services/think/prompt.py) |
| T6 payload rehydration | [services/think/worker.py](../../services/think/worker.py) |

### S4 — `relocate` claim_op + bounded topological cascade

Closes the loop. Reasoning can now deliberately reposition a Model in
topology space, with bounded fan-out so a single relocate doesn't
tsunami-propagate.

| Component | File |
|---|---|
| Migration: extend `topology_events.kind` with `'relocate'` | [db/migrations/0034_topology_relocate.sql](../../db/migrations/0034_topology_relocate.sql) |
| `RelocateTarget`, `parse_relocate_target`, `blend_topo`, `select_bounded_neighbors`, `damped_magnitude` | [lib/topology/relocate.py](../../lib/topology/relocate.py) |
| `TopoRepo.relocate()` + `TopoRepo.bounded_cascade()` | [services/topology/topo_repo.py](../../services/topology/topo_repo.py) |
| `ClaimOp.op = "relocate"` literal + `relocate_target` field | [services/think/diff_schema.py](../../services/think/diff_schema.py) |
| Validator `relocate` branch | [services/think/validator.py](../../services/think/validator.py) |
| Applier `relocate` dispatch | [services/think/applier.py](../../services/think/applier.py) |
| System prompt `relocate` documentation | [services/think/prompt.py](../../services/think/prompt.py) |

### Architecture doc

Both stages reflected in [CODEBASE-ARCHITECTURE.md](../../CODEBASE-ARCHITECTURE.md):
- New `lib/topology/relocate.py` row in libraries table
- `services/topology` per-directory entry extended with `events_repo` + S3 hooks + S4 relocate methods
- `T6` added to trigger types matrix; updated weights for T1-T4
- `topology_events` row added to Supporting Tables (relocate kind documented)
- Migration list extended with 0033 (S3) and 0034 (S4)
- Substrate-evolution narrative now ends at "Loop closes — `relocate` claim_op + bounded cascade"

---

## Test counts

### S3 + S4 baseline (added during implementation)
| Suite | Tests |
|---|---|
| `lib/topology/tests/test_naming.py` | 13 |
| `lib/topology/tests/test_relocate.py` | 24 |
| `services/topology/tests/test_phase_events.py` | 9 |
| `services/topology/tests/test_events_repo.py` | 5 |
| `services/topology/tests/test_relocate.py` | 8 |
| `services/retrieval/tests/test_pathway_f.py` | 4 |
| `services/think/tests/test_t6_prompt.py` | 5 |
| `services/think/tests/test_relocate_applier.py` | 4 |

### Adversarial (added in second pass)
| Suite | Tests |
|---|---|
| `lib/topology/tests/test_adversarial.py` | 27 |
| `services/topology/tests/test_adversarial.py` | 20 |
| `services/topology/tests/test_adversarial_extra.py` | 6 |
| `services/retrieval/tests/test_pathway_f_adversarial.py` | 14 |
| `services/workers/neighborhood_detector/tests/test_t6_adversarial.py` | 7 |
| `services/think/tests/test_relocate_adversarial.py` | 16 |
| `services/think/tests/test_t6_prompt_adversarial.py` | 7 |

**Final sweep:** 377 passing across `lib/topology/`, `lib/shared/tests/test_edge_registry.py`,
`services/topology/`, `services/retrieval/`, `services/workers/edge_drift/`,
`services/workers/topology_updater/`, `services/workers/neighborhood_detector/`,
`services/think/tests/test_t6_prompt*.py`, `services/think/tests/test_relocate*.py`,
`services/models/tests/test_edges_repo.py`. Runtime ~95s.

Pre-existing flaky tests outside scope (not S3/S4 regressions, confirmed
via `git stash` baseline): 6 think worker pgvector-codec tests, 4
`lib/shared/tests/test_db.py` migration-ownership errors.

---

## Bugs surfaced and fixed

### Production fixes
1. **NaN/Inf in `parse_relocate_target` vector input** — accepted at parse time, then crashed pgvector at INSERT with an opaque error. Fixed in [lib/topology/relocate.py](../../lib/topology/relocate.py): `parse_relocate_target` now raises `ValidationError("...non-finite...")` early. Tests:
   - `test_parse_target_vector_rejects_nan_component`
   - `test_parse_target_vector_rejects_inf_component`
   - `test_parse_target_vector_rejects_negative_inf_component`

### Test-setup gotchas (caught and fixed during writing)
1. Generator-with-await comprehension in tuple unpack — Python doesn't auto-iterate. Fixed across two test files.
2. `models.proposition_kind` is a Postgres generated column (derived from `proposition->>'kind'`). Cannot be set explicitly in INSERT statements. Test seeders updated.
3. The `supports` edge_kind is DAG-only with cycle scope `{supports, instance_of}`. Cannot construct a 4-node directed cycle for the cascade-loop stress test. Reshaped to a triangle (DAG-legal, undirected-cyclic) which is the correct stress for the visited-set guard.

---

## Behavior gaps documented (not bugs)

These are intentional or accepted today; tests pin the behavior so any
future change is deliberate.

| Gap | Where | Mitigation |
|---|---|---|
| Relocate of an `archived` Model succeeds | [TopoRepo.relocate](../../services/topology/topo_repo.py) | Documented by `test_relocate_archived_model_succeeds_currently` |
| `member_summaries_from_rows` doesn't auto-parse JSON-string `scope_entities` | [lib/topology/naming.py](../../lib/topology/naming.py) | Hot path in `recompute_for_tenant` does its own JSON parsing — only affects standalone use |
| `ClaimOp` accepts nonsensical field combinations (e.g. `op="insert"` with a `relocate_target`) | [services/think/diff_schema.py](../../services/think/diff_schema.py) | Pydantic `extra="forbid"` only protects against unknown keys; validator/applier discard irrelevant ones |
| `parse_relocate_target` permissive about extra keys in target dict | [lib/topology/relocate.py](../../lib/topology/relocate.py) | By design — keep the LLM-facing API forgiving |
| `select_bounded_neighbors` doesn't dedup duplicate input candidates | [lib/topology/relocate.py](../../lib/topology/relocate.py) | Caller / queue UNIQUE constraint dedup |

---

## Surfaces verified solid under adversarial attack

- **Tenant isolation** — Pathway F + relocate target lookup both reject cross-tenant references with `ValidationError`.
- **Cycle prevention in bounded cascade** — visited-set guard handles undirected triangles even at `max_depth=10`.
- **Inert edges excluded from cascade walk** — the active-only filter in `bounded_cascade` SQL is correct.
- **Per-kind T6 cap** — fires correctly at `NEIGHBORHOOD_DETECTOR_T6_LIMIT_PER_KIND=10`; over-cap events still get `processed_at=now()` so they don't re-emit on the next sweep.
- **Phase-event detector** — handles 50-way splits, 5-way merges, drift-at-threshold, and 50-emergence-events-at-once without misclassifying.
- **Naming** — tolerates unicode (CJK, emoji, mixed), 100+ members, very long labels with truncation that doesn't split codepoints.
- **HNSW path resilience** — Pathway F works both with and without the pgvector codec on the connection (string-literal binding fallback verified).
- **Recompute idempotency** — back-to-back recomputes in the same transaction emit zero events on the second pass.
- **Relocate audit trail** — every relocate produces a `topology_events` row with `kind='relocate'`, magnitude = L2 delta, payload carrying `target_kind`, `alpha`, `reason`, `applied_by_diff_id`.

---

## Reproducing the test sweep

```bash
DATABASE_URL=postgresql://company_os:company_os@localhost:5432/company_os \
  .venv/bin/pytest \
    lib/topology/ \
    lib/shared/tests/test_edge_registry.py \
    services/topology/ \
    services/retrieval/ \
    services/workers/edge_drift/ \
    services/workers/topology_updater/ \
    services/workers/neighborhood_detector/ \
    services/think/tests/test_t6_prompt.py \
    services/think/tests/test_t6_prompt_adversarial.py \
    services/think/tests/test_relocate_applier.py \
    services/think/tests/test_relocate_adversarial.py \
    services/models/tests/test_edges_repo.py \
    -q
```

Expected: `377 passed in ~95s`.

---

## What's NOT in this session

These were explicitly out of scope (per the staging plan; separate
follow-up plans):

- `contradicts` producer (NLI/polarity gate writing `contradicts` edges that pull topo embeddings apart with negative weight)
- A/B harness measuring diff quality with vs. without F + neighborhood context
- Synthesis harness scenarios for structural belief revision (S4 acceptance gate calls for these — should land before considering the substrate's positional layer feature-complete)
- LLM-driven naming overwrites via T6 (the prompt allows it, but no end-to-end harness verifies the LLM actually emits a name-update claim_op)
- Pathway F integration with `second_pass_expand`
- CEO view UI surfacing `topology_events`
