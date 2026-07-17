# P2 Truth Kernel — Reuse And Implementation Gap Matrix

**Scope:** implementation-readiness audit for coordinator phase P2 only

**Evidence basis:** live code, migrations, and P0 inventories on the current worktree
**Non-authority:** this file does not amend the coordinator or system architecture

## 1. Executive finding

P2 should be a controlled cutover around existing machinery, not a new model
system. The repository already contains useful implementations for Model storage,
append-only Model events, source-semantic admission, pre-truth relationship
candidates, n-ary relation instances, binary projections, correction census, and
SAGE/retrieval reads. Those components are reusable once they are placed behind
one admission/lifecycle authority and accepted-version views.

The central missing object is an immutable, compare-and-swap **truth head and
version contract**. Today `models` is simultaneously the semantic object, its
mutable current state, and a retrieval-activity record; `relation_instances` is
mutable and replaces its participants in place; `model_edges` remains directly
writable truth; and most readers equate `models.status='active'` with accepted
truth. Therefore the existing components are substrates and adapters, not yet
the P2 truth kernel.

## 2. Requirement-to-gap matrix

| Package | Existing component to reuse | Exact seam | What is already true | Missing invariant / required change | Primary proof |
| --- | --- | --- | --- | --- | --- |
| P2-A admission | Source semantic append-only decisions | `services/domain/source_semantics/processor.py:SourceSemanticProcessor.process`; `services/domain/source_semantics/repo.py:SourceSemanticRepo.append_admission`; `db/migrations/0206_source_semantic_belief_vertical.sql` | Source interpretations and admission decisions are immutable; `belief_applied` and `no_admission` are distinct. | Admission currently creates the Model before appending its decision, and this path covers only asserted reports. Introduce a general candidate record plus immutable `AdmissionDecision`; accepted readers must require an admitted immutable Model version, not merely `models.status='active'`. Candidate/review/rejected records must never become truth by status mutation. | Ten nonaccepted attempts absent from accepted view; wrapper/control candidates noncanonical; command replay idempotent. |
| P2-A admission | Existing narrow application adapter | `services/domain/models/epistemic_applier.py:EpistemicApplier.apply_asserted_report`; `services/domain/models/repo.py:ModelsRepo.insert` | There is already a narrow point that converts a validated proposal to `ModelCreate`. | Make the truth command handler the only canonical admission writer. `EpistemicApplier` should call that handler, not raw `ModelsRepo.insert`. Register and reject all bypass writers found in `p0/authority-writer-reader-inventory.json`. | HG-02/HG-04/HG-10 evaluator plus static writer registry. |
| P2-B lineage | Existing Model evidence arrays and source semantics | `ModelsRepo._INSERT_COLS` (`supporting_event_ids`, `supporting_model_ids`, `born_from_event_id`); `ProposedBeliefAssertion`; source interpretation/grounding tables | Direct observation and Model IDs can be stored; source semantics preserves grounding continuity. | Arrays do not encode evidence kind, role, exact coordinate, authority, cutoff, or a transitive lineage closure. Add immutable typed evidence references bound to a Model version. Validate tenant, existence, authority and `occurred_at/created_at <= cutoff`; never select the first generic event ID as source. | Evidence-lineage coverage 1.0, exact direct/transitive citations, cross-tenant and future-evidence attempts rejected. |
| P2-B scope | Existing scope fields and grounding selection | `ModelsRepo._INSERT_COLS` (`scope_actors`, `scope_entities`, `scope_temporal`); `EpistemicApplier.apply_asserted_report(selected_scope_entity=...)` | A claim can carry actor/entity/time scope; asserted-report vertical uses one selected grounded entity. | Scope JSON is untyped and other writers can copy batch/retrieval scope. Add version-bound role-bearing scope bindings derived only from cited claim-local evidence. Same UUID with conflicting entity type must fail. | Scope precision 1.0 on sealed fixtures; five conflicting entity-type attempts rejected. |
| P2-C representation | Model event snapshot machinery | `services/domain/models/events.py:model_semantic_snapshot`, `emit_model_event`; `db/migrations/0160_model_events_and_projection_snapshots.sql:model_events` | Events preserve semantic snapshots and previous snapshots and already feed rebuildable projections. | `models` has no semantic version/head CAS and snapshots have no canonical semantic digest binding proposition, natural rendering, scope and provenance. Create immutable Model versions with digest; event must reference exact from/to versions and digest. Natural/proposition divergence must be rejected before head advancement. | Five divergence attempts rejected; each event and current head resolve to one digest/version. |
| P2-D lifecycle | Model repository and edge cascade concepts | `ModelsRepo.archive`, `ModelsRepo.bulk_confidence_update`; `lib/shared/edge_registry.py`; `services/domain/models/events.py` | Archive emits an event and existing edge registry describes dependent reevaluation. | Lifecycle is spread across direct updates and is not CAS-based. Implement one transaction command for falsify/supersede/contest/archive; terminal heads cannot reactivate. Fence stale accepted reads, incompatible support, relation evidence and projections atomically. Treat confirm/unchanged/deduplicate/touch as events, not states. | Failure after third of five fences rolls back fully; retry creates one lifecycle event; confirm/falsify race has one winner. |
| P2-D correction | Existing correction dependency census/service | `services/domain/correction_propagation/service.py:CorrectionPropagationService`; `lib/evaluation/correction_propagation.py:evaluate_correction_propagation` | Existing code can enumerate downstream Models, edges, relation evidence and projection snapshots and track repair. | Current correction is largely post-hoc fencing/repair and cannot prove same semantic commit. Reuse its census and obligation vocabulary, but invoke a synchronous truth-critical fence inside lifecycle transaction; background repair follows the fence. | No unsafe descendant readable after commit; historical rows preserved; exactly one version-bound repair obligation. |
| P2-E relation truth | N-ary relation frame storage | `db/migrations/0150_relation_instances.sql`; `services/reasoning/edge_intelligence/repo.py:EdgeIntelligenceRepo.insert_relation_frame`, `insert_relation_participant`, `insert_relation_edge_projection` | Relation instances already separate semantic frames, participants and derived edge projections. | `insert_relation_frame` upserts the frame and deletes/replaces participants; status and binding completeness can disagree. Introduce immutable relation versions plus CAS head. Participant bindings belong to a version and cannot be updated/deleted in place. Accepted relation requires complete typed roles and accepted endpoints/evidence. | Valid relation cases accepted; wrong role/endpoint/direction/rationale and reciprocal invalidity rejected; endpoint supersession does not rebind. |
| P2-E relation candidates | Relationship candidate/adjudication pipeline | `services/reasoning/relationships/repo.py:RelationshipCandidatesRepo`; `services/reasoning/relationships/adjudication.py:_adjudicate`; `services/reasoning/relationships/promoter.py:promote_high_confidence_edges` | Candidate history and review states already exist separately from Model rows. | Promotion currently writes binary edges and marks candidates accepted. Replace promotion target with AdmissionDecision + relation version. Unknown kinds remain candidates/ontology gaps. Candidate disposition must not mutate into canonical truth. | Unknown kinds never coerced; candidate history preserved and references admitted version. |
| P2-E relation semantics | Relation extraction/frame validators | `services/reasoning/edge_intelligence/relation_frames.py`; `EdgeIntelligenceRepo._validate_relation_frame`; `lib/shared/edge_registry.py` | Role-bearing frames and edge-kind specifications provide a useful validation base. | Enforce initial accepted business vocabulary (`causal_influence`, `dependency_constraint`, `enablement`, `predictive_indicator`) separately from epistemic/lifecycle links. Add rationale polarity/direction validation and unique signed evidence. | Relation joint accuracy 1.0; all self-negating and reverse-direction cases rejected. |
| P2-E confidence | Existing evidence and pair observation storage | `EdgeIntelligenceRepo.insert_relation_evidence`, `record_pair_observation`; `model_pair_evidence` update path | Evidence is separately recordable and pair orientation is normalized. | `EdgesRepo._insert_one` uses `GREATEST` and increments `confirmed_count` on conflict. Replace accepted relation confidence with a deterministic projection of unique signed evidence IDs; retries/rebuilds are idempotent and counterevidence can lower/dispute. | Duplicate evidence and projection replay leave confidence/count unchanged. |
| P2-F evaluator | Existing independent correction evaluator and P0 registries | `lib/evaluation/correction_propagation.py`; `lib/evaluation/epistemic_repair/*`; P0 truth/writer inventories | Evaluators already use typed result models and database census patterns. | Add an independent P2 evaluator that queries accepted views and raw tables, runs HG-04..HG-10, race/failure fixtures, digests, provenance closure and reader cutover census. It must not import truth command internals or reuse their validators as its oracle. | `epistemic-repair-p2-truth-kernel-v1.json` contains member-level results, receipts, snapshots and compatibility debt. |
| P2-G accepted reads | Existing active-only retrieval filters | `services/reasoning/retrieval/pathways.py:hydrate_active_models_by_ids`, all Model pathways; `services/reasoning/sage/reader.py:_load_models`; `ModelsRepo.search_by_embedding/search_by_scope` | Most primary retrieval paths already consistently filter `status='active'`, making a centralized view cutover feasible. | `active` is not proof of admission. Create an accepted-current Model view with version/digest and replace all canonical Model reads, including joins in SAGE sparse/address paths. Add a ratchet prohibiting raw active truth reads outside truth kernel/evaluator/migration code. | Reader census reports 100% accepted-view coverage and nonaccepted fixtures never appear. |
| P2-G relation reads | Existing graph traversal and projection mapping | `services/reasoning/retrieval/pathways.py:pathway_g_model_edges`; `services/reasoning/sage/reader.py:_load_candidate_edges`; `relation_edge_projections` | Binary edges are already an efficient derived traversal surface and relation-to-edge mapping exists. | Rebuild `model_edges` only from admitted relation versions for business semantics. Current/consequential readers must exclude disputed/retired relations and invalidated evidence immediately. Candidate edges remain explicitly pre-truth and cannot merge into accepted graph results. | HG-08/HG-10; every accepted business edge maps to one current accepted relation version. |
| P2-G activity separation | Existing SAGE policy/activation sidecars | `services/reasoning/sage/reader.py:SynthesisReader`; SAGE affordance, shortcut, negative-memory and activation-trace repositories | SAGE is already largely a derived adaptive reader and should be reused rather than folded into canonical truth. | `ModelsRepo.retrieve` mutates `models.retrieval_count`, `last_retrieved_at`, and `activation`. Move those to an activity sidecar. SAGE/topology/projections may rank or materialize but must have no canonical truth grants. | 100 repeated retrievals leave Model head/digest/confidence/lifecycle identical; static writer audit finds no derived truth write. |

## 3. Reuse decisions

### Reuse directly

- `model_events` and projection checkpoints/snapshots as the neutral outbox and
  rebuild substrate. Extend their references; do not create a second event bus.
- Source-semantic interpretations, grounding continuity, and immutable admission
  decisions as the first concrete admission pattern.
- `relationship_candidates` and relation claims as immutable pre-truth inputs.
- `relation_instances`/participants as the conceptual n-ary relation shape, but
  migrate their mutable implementation to version/head storage.
- `relation_edge_projections` and `model_edges` as derived compatibility/read
  projections, never as independent business truth.
- SAGE reader, affordances, shortcuts, negative memory, structural features and
  topology as adaptive/derived consumers. They should consume accepted views
  and emit candidates or policy memory only.
- Correction propagation census and projection dependency records for finding
  what must be fenced or repaired.

### Reuse behind adapters

- `ModelsRepo` remains useful for hydration, embedding search mechanics and
  compatibility, but its semantic mutations must become private adapters behind
  the truth command owner.
- `EdgesRepo` remains useful for projection traversal/cycle mechanics; `link`
  cannot remain an accepted business-truth API.
- Relationship adjudication gates can propose decisions, but cannot directly
  admit a relation or edge.

### Do not reuse as authority

- `models.status='active'` as an admission test.
- `relationship_candidates.review_status='accepted'` as canonical relation truth.
- `model_edges` confidence/confirmation conflict updates as epistemic aggregation.
- SAGE activation, retrieval frequency, topology, or projection state as evidence
  that changes canonical semantic confidence.

## 4. Exact writer cutover seams

| Current writer | Current mutation | P2 disposition |
| --- | --- | --- |
| `ModelsRepo.insert` | Inserts active canonical Model | Call only from truth admission command after immutable decision/version validation. |
| `EpistemicApplier.apply_asserted_report` | Converts proposal then calls `ModelsRepo.insert` | Preserve adapter API; redirect to admission command. |
| Think applier (`services/reasoning/think/applier.py`) | Inserts/updates/archives Models and edges | Emit typed candidates/commands; no raw canonical SQL or direct repo lifecycle. |
| `ModelsRepo.archive` and contestability/decay paths | Mutate lifecycle fields | Redirect to CAS lifecycle command with expected head version. |
| `ModelsRepo.bulk_confidence_update` | Mutates semantic confidence | Replace with evidence/adjudication command or calibration sidecar; no unversioned update. |
| `ModelsRepo.retrieve` | Mutates activity fields on `models` | Write activity sidecar only. |
| `EdgesRepo.link/unlink/retire` | Writes/deletes accepted binary edges | Restrict to projector identity; business relation admission owns semantic version. |
| `promote_high_confidence_edges` | Promotes candidate to edge | Promote to relation admission request; projector derives edges after acceptance. |
| `EdgeIntelligenceRepo.insert_relation_frame` | Upserts relation and replaces participant set | Candidate-only insert remains; accepted truth uses immutable version command. |
| `mark_relation_frame_decided` | Mutates relation status in place | Candidate disposition only, or replace with immutable admission decision plus head CAS. |

The complete bypass list remains the machine-readable P0 inventory at
`docs/plans/epistemic-repair/p0/authority-writer-reader-inventory.json`; P2 must
ratchet that list down rather than treating this table as exhaustive.

## 5. Reader cutover seams

1. Introduce one compatibility relation such as `accepted_current_models` and
   one accepted relation view. The migration owner defines their exact names.
2. Cut over the shared hydration seams first:
   `services/domain/models/read_shapes.py`,
   `services/reasoning/retrieval/pathways.py:hydrate_active_models_by_ids`, and
   `services/reasoning/sage/reader.py:_load_models`.
3. Cut over every remaining raw `FROM/JOIN models` in `retrieval/pathways.py`,
   `retrieval/primary.py`, and `sage/reader.py`; many sparse/address queries
   bypass the shared hydrator.
4. Cut over `pathway_g_model_edges` and `_load_candidate_edges` so accepted
   binary projections and candidates are returned in different typed buckets.
5. Add a static ratchet that rejects new raw canonical reads outside registered
   kernel, migration, evaluator and projector owners.

## 6. Database invariants still absent

- Immutable Model version rows and a tenant-scoped current-head row with CAS.
- A foreign-key-verifiable AdmissionDecision -> admitted ModelVersion link.
- A semantic digest covering proposition, natural, typed scope and provenance.
- Typed evidence references with evidence role, coordinates, authority and
  observation cutoff; transitive lineage closure must be queryable.
- Typed scope bindings owned by a ModelVersion.
- Terminal lifecycle constraints and valid `old superseded_by new` ordering.
- Version-bound repair obligations with uniqueness for one invalidation cause.
- Immutable relation versions and relation heads; version-owned participants.
- Accepted-relation completeness checks for vocabulary, roles, endpoints,
  evidence and participant binding.
- Signed unique relation evidence aggregation; no retry-sensitive counters.
- Accepted-current compatibility views and writer/read grants or triggers that
  make bypass mechanically difficult.
- A Model activity sidecar for retrieval count, activation and timestamps.

These are migration-owned per coordinator step 17. Production agents should
first publish contracts and tests; the migration owner then chooses the minimum
schema additions after checking whether existing sidecars can satisfy them.

## 7. Parallel agent ownership

The packages below are deliberately file-disjoint. Shared migrations and shared
view names are coordinator-owned and land only after contract review.

| Lane | Own files | May read, must not edit | Deliverable / dependency |
| --- | --- | --- | --- |
| A — admission contracts | New `lib/contracts/truth_admission.py`; new unit tests under `lib/contracts/tests/` | Model repo, source semantics, migrations | Candidate, AdmissionDecision, ModelVersion/head command contracts; no DB writes. Starts immediately. |
| B — evidence/scope contracts | New `lib/contracts/truth_evidence.py`; new contract tests | Grounding/source semantics, Model types | Typed reference/coordinate/role/cutoff and scope-binding contracts. Starts immediately. |
| C — lifecycle service | New package `services/domain/truth_kernel/` excluding relation files; focused unit tests | `ModelsRepo`, correction service, events | CAS command handler and transaction plan after A/B contract freeze. No direct edits to readers. |
| D — relation kernel | New relation files under `services/domain/truth_kernel/relations/`; relation tests | Edge intelligence, relationship candidates, edge registry | Versioned relation admission/lifecycle and signed-evidence derivation after B. |
| E — compatibility readers | `services/domain/models/read_shapes.py`, `services/reasoning/retrieval/pathways.py`, `services/reasoning/retrieval/primary.py` | Kernel services, SAGE reader | Accepted-view retrieval cutover after migration/view contract. |
| F — SAGE cutover/activity | `services/reasoning/sage/reader.py`; new Model activity repo; SAGE tests | Retrieval paths, Model repo | Accepted-only SAGE loads and activity sidecar; cannot edit truth commands. |
| G — independent evaluator | New `lib/evaluation/epistemic_repair/p2_*`; `tests/epistemic_repair/p2/`; report builder | All production code read-only | HG-04..HG-10 oracle, fixtures, races, rollback injection, JSON artifact. Starts with contracts; stays independent. |
| M — coordinator migration | New next-numbered files under `db/migrations/`; schema tests | All lanes | Versions/heads/evidence/scope/relations/views/activity/grants. Sole migration owner; resolves names and FK order. |
| R — authority ratchets | Architecture scripts/config and new focused tests only | All production writers/readers read-only | Writer registry and raw-read prohibition after cutover. |

Suggested merge order: `A+B+G fixture schema` -> `M` -> `C+D` -> `E+F` ->
`R` -> `G database/race execution`. C and D run in parallel after the contract
freeze; E and F run in parallel after accepted views exist.

## 8. Highest-risk integration traps

1. **False reuse of active status:** changing a few shared hydrators leaves many
   raw SAGE/retrieval joins that still read active-but-unadmitted rows.
2. **Admission atomicity inversion:** the current source-semantic path creates a
   Model before its decision. The new command must commit decision, version,
   head and event as one semantic transaction.
3. **Mutable relation overwrite:** reusing `insert_relation_frame` unchanged
   destroys immutable participant history through delete-and-replace.
4. **Projection promoted to truth:** `model_edges` is useful and fast, but its
   present direct write API, `GREATEST` confidence and incrementing confirmation
   count violate epistemic idempotency.
5. **Asynchronous correction gap:** background repair is valuable only after the
   consequential/read fence is synchronous. A queued repair is not a fence.
6. **SAGE authority creep:** SAGE should influence selection and candidate
   generation. Its learned scores, retrieval counts and topology must never
   become admission or confidence evidence by themselves.
7. **Versionless events:** extending `model_events` without exact from/to version
   and semantic digest would preserve snapshots but not prove representation
   coherence.

## 9. Definition of implementation-ready

P2 coding may proceed without architectural invention once:

- lanes A and B freeze the command, version, evidence and scope contracts;
- migration lane M publishes exact table/view names and transaction/FK order;
- evaluator lane G preregisters the sealed fixtures and independent SQL oracles;
- every direct writer in the P0 inventory has a named redirect or projector-only
  disposition; and
- every raw accepted reader has a named cutover owner.

Until then, SAGE, ModelsRepo, relation frames and projections should be treated
as reusable mechanisms—not proof that P2 invariants already exist.
