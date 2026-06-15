# Architecture Decision Records

An **Architecture Decision Record (ADR)** captures a single significant
architectural decision: the context that forced it, the decision itself, and
the consequences. ADRs are how we make the *reasoning* behind the system
durable — so the next engineer (or AI agent) can see *why* something is the
way it is, not just *what* it is.

> **Scope.** Write an ADR when a choice is hard to reverse, affects more than
> one subsystem, or encodes a trade-off future-you will question — e.g. a new
> data store, a queue/transport choice, a security boundary, a layering rule,
> or deprecating a subsystem. Do **not** write one for routine, easily-reversed
> changes.

## How to add an ADR

1. Copy [`0000-template.md`](0000-template.md) to `NNNN-kebab-case-title.md`,
   using the next free number (zero-padded, monotonically increasing).
2. Fill in every section. Keep it short — one page is ideal.
3. Set **Status** to `Proposed` while under discussion, then `Accepted` once
   agreed. Never edit an Accepted ADR's decision; instead write a new ADR and
   mark the old one `Superseded by ADR-XXXX`.
4. Add a row to the index table below.
5. Commit the ADR in the **same PR** as the change it describes, where possible.

**Status values:** `Proposed` · `Accepted` · `Superseded by ADR-XXXX` · `Deprecated`

## Index

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [0001](0001-kafka-first-ingestion-default.md) | Kafka full pipeline is the default ingestion path; inline ingest is the fallback | Accepted | 2026-06-02 |
| [0002](0002-main-is-the-single-integration-trunk.md) | Main is the single integration trunk | Accepted | 2026-06-03 |
| [0003](0003-telegram-mtproto-user-account-ingestion.md) | Telegram ingestion uses the MTProto user-account API, with a two-session backfill+live topology | Proposed | 2026-06-07 |
| [0004](0004-keep-model-predictions-and-outcome-evaluator.md) | Keep Model Predictions and Outcome Evaluator | Accepted | 2026-06-12 |

## Related: existing decision records

This ADR log is the place for **new, forward-looking** decisions. Several
already-made decisions are documented narratively elsewhere in the repo and
have **not** been retro-fitted into ADRs (doing so would be inventing history):

- `CODEBASE-MANAGEMENT.md` — the decision record for the monorepo model and the
  layered `services/` structure (single-monorepo vs. polyrepo, layer boundaries,
  enforcement). This is the closest thing to existing ADRs.
- `CONTRIBUTING.md` — the enforced import-discipline rules that flow from those
  decisions.
- `specs/` — per-feature specs and plans (Spec-Driven Development artifacts).

> **TODO(human):** If the team wants the major past decisions (monorepo,
> layering, choice of Postgres+pgvector as the substrate, durable DB queues vs.
> a broker, the Think pipeline design) captured as first-class ADRs, port them
> from `CODEBASE-MANAGEMENT.md` into numbered records here. Until then, treat
> `CODEBASE-MANAGEMENT.md` as the authoritative decision narrative.
