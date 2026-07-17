# P2 Canonical Writer Cutover — Remaining Bypasses

**Registry:** `canonical-writer-registry-v1.json`

**State:** exact frozen debt after the asserted-report cutover

The grounded asserted/report vertical no longer inserts a canonical-looking
Model through `ModelsRepo.insert`. It builds one immutable admission command
and executes it through `build_default_truth_kernel()` inside the transaction
owned by `GroundedBeliefProcessor`. The `models` row produced by
`AsyncpgTruthKernelStorage._insert_legacy_read_projection` is explicitly a
compatibility projection, not an independent truth assertion.

The following direct legacy `models` writers remain. They are registered only
to make the ratchet exact and to prevent the bypass surface from growing. Their
registration does **not** make them approved canonical authorities.

| Remaining module | Existing mutation family | Required cutover fate |
| --- | --- | --- |
| `services/domain/models/repo.py` | legacy create/update/archive/calibration | split read projection and sidecars; route semantic admission/lifecycle to truth kernel |
| `services/domain/models/decay.py` | archive/activation decay | lifecycle command or rebuildable activity sidecar |
| `services/domain/substrate_promotion.py` | canonical scope update | typed claim-local scope command |
| `services/reasoning/think/applier.py` | semantic, lifecycle, scope updates | candidate compiler plus truth-kernel commands |
| `services/reasoning/contestability/service.py` | confidence and contest state | evidence/lifecycle command plus derived counters |
| `services/product/recommendations/handlers.py` | archive | lifecycle command |
| `services/product/recommendations/feedback.py` | confirmation counters | derived feedback sidecar; lifecycle command when semantic |
| `services/product/today/triage.py` | archive | lifecycle command |

Other P0 authority bypasses intentionally remain outside this narrow cutover:

1. maintenance can delete canonical aliases outside the identity applier;
2. accepted `model_edges` remain directly writable;
3. relation truth and relation-edge projections remain co-located;
4. correction propagation has a separate relation/edge lifecycle path; and
5. database constraints do not yet encode every admission and fencing rule.

The executable ratchet fails on any new canonical truth-table writer, on any
canonical truth write from Think, SAGE, topology, or projections, and on any
new direct `models` writer. A later P2 package must remove each frozen bypass
from both code and registry; it must not broaden the registry to make a failure
green.
