-- =====================================================================
-- 0051_ingestion_failures_upsert_key.sql
--   Ingestion LLD §1.3 / §5.5 — fix UPSERT key + add embedding kind.
-- =====================================================================
-- Two related changes to `ingestion_failures` (migration 0046):
--
--   (1) Add a DB-enforced UNIQUE constraint on the LLD §5.5 UPSERT key
--       `(tenant_id, source, raw_s3_key, failure_kind)`, so the DLQ
--       writer can use `INSERT ... ON CONFLICT DO UPDATE` and rely on
--       the DB to serialise concurrent inserts of the same logical
--       failure. Migration 0046 deferred this to app-level on the
--       reasoning that a UNIQUE would over-restrict the NULL-
--       raw_s3_key cases; that reasoning was incorrect — Postgres
--       treats NULLs as DISTINCT in unique indexes by default, so
--       NULL-raw_s3_key rows (genuinely-distinct occurrences like
--       `reconciliation_gap_unresolved`) remain allowed to multiply.
--       The constraint only dedupes when raw_s3_key is non-NULL,
--       which IS the case the UPSERT semantics are written for.
--
--   (2) Extend the failure_kind CHECK enum with
--       `embedding_ollama_failure` so M3.2's embedding worker can
--       publish that kind to ingestion.dlq without a follow-up
--       migration. Wire name will be `embedding.ollama_failure`
--       (dot-separated; the producer-namespaced convention from
--       services/ingestion/dlq/models.py) and the DLQ writer's
--       `_WIRE_TO_DB_FAILURE_KIND` map bridges to the underscore-
--       separated DB enum value.
--
-- Constitution alignment:
--   §I — bounded to ingestion's DLQ surface.
--   §II — additive (no destructive ALTERs to existing rows); the
--         UNIQUE add will fail loudly if violated by existing data,
--         which is the correct behaviour (a pre-existing duplicate
--         means the prior app-level dedup raced and the migration
--         must be re-run after manual cleanup).
--   §III — no RLS change.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- (1) UPSERT key UNIQUE constraint.
-- ---------------------------------------------------------------------
-- Adding via CREATE UNIQUE INDEX (not ALTER TABLE ADD CONSTRAINT)
-- because that's the form `INSERT ... ON CONFLICT (col, ...)` matches
-- against; a named CONSTRAINT also works but the index form keeps the
-- conflict target column list explicit at the call site.
--
-- NULL-handling: Postgres default (NULLS DISTINCT) is what we want —
-- two rows with raw_s3_key IS NULL never collide on this index, so
-- the LLD §1.3 carve-out for raw_s3_key-less failure modes
-- (rate_limit_exhausted, reconciliation_gap_unresolved, etc.) still
-- holds.
CREATE UNIQUE INDEX IF NOT EXISTS
    ingestion_failures_upsert_key_idx
    ON ingestion_failures (tenant_id, source, raw_s3_key, failure_kind);

-- ---------------------------------------------------------------------
-- (2) Extend failure_kind CHECK to include the embedding worker.
-- ---------------------------------------------------------------------
-- Postgres has no ALTER CHECK; drop + recreate is the standard form.
-- Validated immediately (no NOT VALID dance) — the new enum value is
-- a superset of the old, so existing rows can't fail validation.
ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_failure_kind_check;

ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_failure_kind_check
    CHECK (failure_kind IN (
        'normalizer_parse_error',
        'observation_insert_error',
        'rate_limit_exhausted',
        's3_put_failure',
        'kafka_publish_failure',
        'fetcher_terminal_error',
        'reconciliation_gap_unresolved',
        'oauth_revoked_mid_run',
        -- M3.2 (migration 0051): embedding worker's terminal-after-retry
        -- failure. Wire format: "embedding.ollama_failure".
        'embedding_ollama_failure'
    ));

COMMIT;
