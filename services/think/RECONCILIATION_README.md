# Reconciliation — operator guide

This is the operator-facing guide. The design rationale lives in
[RECONCILIATION_DESIGN.md](RECONCILIATION_DESIGN.md); read that
first if you want the *why*.

## What it does

Between Think's validate and apply stages, the reconciler examines
every `claim_op.insert` the LLM emits and asks: "is this Model
already in our store?" Three outcomes:

* **`auto_merge`** — yes, with high confidence. The insert is
  rewritten to a confidence update against the matched Model. One
  row in `models`, not two.
* **`human_review`** — maybe. The insert proceeds normally, but a
  row lands in `reconciliation_events` for a human to triage.
* **`no_match`** — no. The insert proceeds; an audit row records
  the closest near-miss for tuning data.

The reconciler runs *inside* the apply transaction, so its
decisions are atomic with the rest of Think's apply.

## Configuration

All knobs are environment variables. Defaults are conservative
starting points that need empirical tuning — see the design doc.

| Env var                              | Type   | Default | Effect |
|--------------------------------------|--------|---------|--------|
| `RECONCILE_ENABLED`                  | bool   | `true`  | Master kill switch. `false` short-circuits every decision. |
| `RECONCILE_AUTO_MERGE_COSINE`        | float  | `0.85`  | Cosine ≥ this AND all other signals match → auto_merge. |
| `RECONCILE_HUMAN_REVIEW_COSINE`      | float  | `0.70`  | Cosine in `[0.70, RECONCILE_AUTO_MERGE_COSINE)` → human_review. |
| `RECONCILE_RECENCY_WINDOW_DAYS`      | int    | `30`    | Existing Models older than this don't qualify as candidates. |
| `RECONCILE_LOG_NO_MATCH`             | bool   | `true`  | Whether to write an audit row for `no_match` decisions (tuning data; one row per insert). |

Boolean parsing accepts `1/true/yes/on/y/t` (case-insensitive) for
true; everything else is false.

The config is read on every reconciler call, not at module
import. Flipping `RECONCILE_ENABLED=false` in the environment of
a running worker takes effect on the next Think run — no restart
needed.

## When to use the kill switch

Turn off the reconciler (`RECONCILE_ENABLED=false`) when:

* The auto-merge rate looks pathological (e.g. > 30% of inserts
  auto-merge for a tenant that legitimately produces lots of
  unique Models). This usually means the cosine threshold needs
  raising; the kill switch buys time while you tune.
* `pending_reconciliation` queue depth balloons past what
  ops can process. Better to take more rows than to lose audit
  trail by silently merging.
* You're investigating a calibration regression and want to
  isolate whether the reconciler's confidence-update path
  contributed. Re-run the harness with the switch off and
  compare ECE.

## Reading the audit table

```sql
-- The pending review queue, oldest first.
SELECT id, tenant_id, occurred_at, cosine_similarity,
       matched_model_id, original_claim_op->>'natural' AS proposed
FROM reconciliation_events
WHERE decision = 'human_review' AND resolved_at IS NULL
ORDER BY occurred_at ASC;

-- Today's auto-merge rate per tenant.
SELECT tenant_id, decision, COUNT(*)
FROM reconciliation_events
WHERE occurred_at >= now() - interval '24 hours'
GROUP BY tenant_id, decision
ORDER BY tenant_id, decision;

-- Near-miss distribution: how close were no_match decisions to
-- the human_review threshold? Useful for retuning.
SELECT
  width_bucket(cosine_similarity, 0.0, 0.7, 7) AS bucket_low,
  COUNT(*)
FROM reconciliation_events
WHERE decision = 'no_match' AND cosine_similarity IS NOT NULL
GROUP BY 1 ORDER BY 1;
```

## Resolving a `human_review` row

When a reviewer decides:

```sql
UPDATE reconciliation_events
SET resolved_at = now(),
    resolved_decision = 'merge',           -- or 'keep_separate' / 'reject'
    resolved_by_actor_id = '<reviewer-uuid>'
WHERE id = '<event-uuid>';
```

The schema records the decision; it does NOT execute it. If
`resolved_decision='merge'`, an operator currently has to
manually issue a `claim_op.update` against the matched Model
(via the Think queue or the `/contest/{model_id}` API) — there's
no auto-resolver yet. Out-of-scope for T5; design doc lists this
as deferred.

`resolved_decision='reject'` means the original insert should not
have happened either. Archive the inserted Model with reason
`'manual'`.

## Metrics

Counters in `services.think.observability.METRICS`:

* `reconcile_decisions_total{decision}` — rate of each outcome.
* `cascade_invariant_violations` — unrelated, kept here as a
  reminder that the same `Metrics` instance feeds dashboards.

There is no Prometheus endpoint yet. `METRICS.snapshot()` returns
a dict that can be serialized; expose it however your environment
expects.

## Failure modes

If the reconciler itself raises (DB error, codec issue, anything),
it logs `reconcile.error` and returns `decision="skipped"` — apply
proceeds with the original insert. The reconciler never aborts
apply on its own account. This is by design: a regression in the
reconciler must not be able to break the upstream pipeline.

If you observe `reconcile.error` events in production, the
reconciler is degrading silently to "no dedup." That's an
incident-grade signal: fix the underlying issue or flip the kill
switch.
