-- =====================================================================
-- 0049_entity_aliases_normalized_index.sql
--   Ingestion LLD §1.6 — functional index for batched alias lookups.
-- =====================================================================
-- The Path-B writer (LLD §5) resolves entity aliases via a single
-- batched ANY($1::text[]) lookup per observation batch. The lookup
-- predicate is:
--
--     WHERE tenant_id = $1
--       AND regexp_replace(lower(alias_text), '\s+', ' ', 'g')
--             = ANY($2::text[])
--
-- The expression here MUST match the application's normalize_phrase()
-- exactly (see services/workers/entity_resolver/context.py:217-218
-- and services/entity_aliases/repo.py). A mismatch produces a silent
-- table-scan fallback; the paired EXPLAIN-based test in
-- services/ingestion/tests/test_migrations.py is the guard.
--
-- ---------------------------------------------------------------------
-- IMPORTANT — non-transactional migration.
-- ---------------------------------------------------------------------
-- `CREATE INDEX CONCURRENTLY` is mandatory: the table may be large in
-- some tenants and the migration must not block writers. Postgres
-- forbids CONCURRENTLY inside an explicit transaction block.
--
-- The project's migration runner (lib/shared/migrations.py;
-- scripts/docker-migrate.sh) detects the keyword `CONCURRENTLY` and
-- dispatches this file OUTSIDE the usual atomic-rollback wrapper.
-- This is the only file in db/migrations/ that opts out as of M1.
--
-- Deliberate deviation from LLD §1.6: the LLD shows `BEGIN/COMMIT`
-- around the statement "for file structure"; this file omits them
-- because the runner no longer wraps the file in a txn and an
-- inline `BEGIN; CREATE INDEX CONCURRENTLY; COMMIT;` would still
-- raise SQLSTATE 25001 (CONCURRENTLY cannot run inside an explicit
-- transaction, regardless of nesting source).
--
-- The existing aliases_text_idx on (tenant_id, alias_text) stays —
-- it serves the by-raw-text retrieval path.
-- ---------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS entity_aliases_normalized_idx
    ON entity_aliases (
        tenant_id,
        (regexp_replace(lower(alias_text), '\s+', ' ', 'g'))
    );
