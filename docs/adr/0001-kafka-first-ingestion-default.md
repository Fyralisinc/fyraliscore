# ADR-0001: Kafka full pipeline is the default ingestion path; inline ingest is the fallback

- **Status:** Accepted
- **Date:** 2026-06-02
- **Deciders:** Ingestion / data-plane
- **Related:** `services/ingest/ingestion/feature_flags/client.py`,
  `services/app/webhooks/router.py`,
  `services/ingest/ingestion/writers/observation_writer.py`;
  [Ingest architecture](../architecture/ingest.md),
  [Data plane](../architecture/data-plane.md)

## Context

Every external signal can be persisted into an `observations` row by **two
convergent paths** that share `core.ingest_from_draft()`:

- **Inline** — the webhook/gateway handler calls `core.ingest()` *synchronously*
  and the provider's HTTP request blocks until the observation is committed
  (handler extract → actor/entity resolve → embed → INSERT).
- **Kafka full pipeline** — ingress publishes the raw body to
  `ingestion.raw.{source}` (S3 + a `RawEnvelope`), returns `202`, and the
  **normalizer** → **observation_writer** workers persist asynchronously.

Both are gated per tenant by the `ingestion.kafka_path_enabled` flag. The flag
was originally **opt-in (default FALSE)**: the writer read it per envelope and,
for any tenant without an explicit row, *shadow-logged* the normalized envelope
to an in-process list and wrote nothing — the M2 "zero-divergence soak" that
validated the async lane against the inline source of truth.

That default outlived its purpose. With the soak long complete and tenant
onboarding already auto-enabling the flag, the opt-in default meant the full
pipeline was **shadow/no-op for every un-onboarded or legacy tenant**: the
normalizer and writer consumed end-to-end and then dropped the result, while the
synchronous inline path silently carried production — coupling each provider's
webhook latency to our DB-write + embedding time and forfeiting the
pipeline's asynchronicity, durability buffering, and backfill-at-scale benefits.

A second hazard was **split-brain**: the flag default lived as a hand-passed
`default=` argument at every read site (four ingress readers + the writer). If
ingress defaulted one way and the writer the other, a tenant's ingress would
publish to Kafka while the writer shadow-logged — silently dropping the
observation. Nothing structurally prevented that drift.

## Decision

**We will make the Kafka full pipeline the default and treat inline ingest as
the fallback + kill-switch.**

1. **Invert the default.** A new `KAFKA_PATH_ENABLED_DEFAULT` (default `True`,
   overridable fleet-wide via `INGESTION_KAFKA_PATH_DEFAULT=false`) means a tenant
   with **no flag row is kafka-first**. An explicit `FALSE` — set by an operator
   or `auto:circuit_breaker` — is now the **kill-switch** that forces a tenant
   back onto inline.
2. **Single source of truth.** Every reader — the webhook router, gmail fetcher,
   discord gateway dispatch, the circuit breaker, **and** the observation writer
   — resolves the flag through one helper, `TenantFlags.kafka_path_enabled()`.
   Ingress and the writer therefore can never drift. *(Rejected: leaving the
   `default=` hand-passed at each site — it preserved the split-brain hazard.)*
3. **Inline stays — it is the fallback and the dev/test path.** When the Kafka
   publish fails (broker/S3 down, or simply not wired in dev/test/demo), ingress
   degrades to inline; the safety is real because `ingest_from_draft` is
   idempotent on `(source_channel, external_id, occurred_at)`, so a
   late-delivered duplicate collapses to one row. *(Rejected: removing inline —
   it is also the synchronous-result API for admin/backfill endpoints and the
   only path that runs without a Kafka broker.)*
4. **Bound the synchronous flush.** The request-path flush is capped by
   `CUTOVER_FLUSH_TIMEOUT_SEC` (default 2.0s) so a slow broker trips the inline
   fallback quickly instead of stacking a long wait on every fallback.
5. **Keep the circuit breaker.** It remains the coarse-grained "pipeline is sick
   → force this tenant to inline" control, complementing the per-request
   fallback. Its bookkeeping was already consistent with a kafka-first default.

The cutover keeps both ends in lockstep: when a tenant is kafka-first, ingress
returns `202` and **skips** inline, and the writer **persists**; when killed,
ingress runs inline and the writer shadow-logs.

## Consequences

**Easier / now true.** New and un-flagged tenants get the asynchronous pipeline
by default — fast `202` acks, Kafka as a durability buffer that absorbs DB/Ollama
outages, backfill at scale through the same machinery, and replay from the raw
lane. Embedding moves fully off the request path. The split-brain failure mode is
structurally impossible (one helper).

**Harder / new constraints.** Kafka + S3 become a hard dependency for the
*default* ingestion path in production; the inline fallback and the
`INGESTION_KAFKA_PATH_DEFAULT=false` global switch are the escape hatches, and
the per-tenant kill-switch still wins. Synchronous-result endpoints
(`gateway/main.py` debug ingest, slack/finance backfill consoles) are
deliberately **exempt** — they call `ingest()` directly and return the
observation, which a `202` cannot.

**Follow-up created.** A kill-switched tenant still runs the M2
shadow-write-after-inline, which keeps publishing raw events into the pipeline it
was just pulled off of. It is harmless (gated by `ingestion.shadow_write_enabled`;
the writer no-ops them) but, if the kill-switch fired *because* the pipeline is
sick, we may later want to suppress shadow-write for tripped tenants too. Left
out of this decision deliberately.

**Falsification / rollout.** Revert globally with
`INGESTION_KAFKA_PATH_DEFAULT=false` (no redeploy) or per tenant with an explicit
`kafka_path_enabled=FALSE`. If the async lane proves unable to carry default
traffic safely, this ADR should be superseded rather than silently re-flipped.
