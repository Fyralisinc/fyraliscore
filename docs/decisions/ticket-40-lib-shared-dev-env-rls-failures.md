# Ticket #40 — `lib/shared` test failures under SUPERUSER dev environment

**Status:** Won't fix unless dev-env conventions change. Filed during post-M-Load tracker hygiene.
**Target milestone:** n/a.
**Filed:** 2026-05-19.

## Symptom

The following tests fail under the local dev environment, where the Postgres connecting role has `SUPERUSER`:

- [`lib/shared/tests/test_db.py::test_integration_*`](../../lib/shared/tests/test_db.py) — 3 tests.
- [`lib/shared/tests/test_rls_isolation.py::*`](../../lib/shared/tests/test_rls_isolation.py) — 4 tests.

Failures have been present at every merge from M6.1 onward without blocking. The skip-message text in [`services/ingestion/tests/test_migrations.py:190`](../../services/ingestion/tests/test_migrations.py) and [`services/ingestion/writers/tests/test_dlq_writer.py:243`](../../services/ingestion/writers/tests/test_dlq_writer.py) already documents the underlying cause; this ticket consolidates that into a single referenceable entry.

## Diagnosis

`SUPERUSER` role bypasses Row-Level Security regardless of the `FORCE` setting. The tests assert RLS isolation behavior, which holds for non-super roles but not for `SUPERUSER`. CI runs as a non-super role and the tests pass there.

The two-line summary:

- Test assertion: "tenant A cannot SELECT tenant B's rows after `SET LOCAL app.tenant_id`."
- Dev environment: SUPERUSER bypasses RLS, sees everything, assertion fails.
- CI: non-super role respects RLS, assertion passes.

The production correctness this test verifies is intact in production (which runs as non-super); the dev-env failure is a role-mismatch artifact, not a regression.

## Status

Documented "won't fix" — the dev-environment role assumption is fixed by the project's existing dev-env convention (Postgres `company_os` role created as `SUPERUSER` for ease of schema management, per `docker-compose.yml`). Switching dev to a non-super role would require:

- Coordinated migration across all contributors' local databases.
- A second role with explicit grants for dev-time schema ops.
- Updates to onboarding docs + setup scripts.

The benefit (one fewer footnote per merge confirmation) does not justify the coordination cost.

## Decision

**Accept as dev-env-only failures.** CI is the authoritative test surface for these RLS assertions. Future merge confirmations should reference this ticket rather than re-explaining the failures.

## Action

**Won't fix unless dev-env conventions change.** This ticket exists so the "pre-existing dev-env failures" footnote in merge confirmations has a permanent reference.

If dev-env conventions ever change (e.g., the project moves to a containerized dev DB with a non-super default role), this ticket can be closed and the tests reactivated locally.
