# Ticket #46 — Pre-existing writer permanent-error branch uses invalid failure_kind

**Status:** Filed during M6.7 verification work. Not blocking; pre-existing.

**Trigger:** During A2/A28 implementation (writer permanent-error
classification for missing partition), an incidental observation
surfaced: the pre-existing writer permanent-error branch passes
failure_kind="writer.full_mode_permanent_failure", which isn't a
valid WireFailureKind. publish_dlq silently skips these — meaning
existing permanent failures aren't actually reaching the DLQ.

**Impact:** Observable today in production. Any pre-existing permanent
failure routed through that branch never reaches the DLQ; the message
processes nothing visible. New A28 branch (writer.invariant_failure)
works correctly.

**Fix shape:** One-string change in the existing permanent-error branch
to use a valid WireFailureKind. Plus a regression test asserting
publish_dlq accepts the failure_kind.

**Why not folded into M6.7 verification work-unit:** Pre-existing;
not introduced by M6.7. Fixing would expand branch scope after the
verification gate already closed. Better as its own focused commit.

**Scope:** ~5 lines production + 1 test. Standalone work-unit.

**Cross-references:** A28 (writer permanent-error classification —
this ticket addresses the parallel pre-existing branch that A28's
new branch was modeled on).
