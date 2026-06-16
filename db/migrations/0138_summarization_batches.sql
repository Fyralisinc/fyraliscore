-- 0138_summarization_batches.sql
--
-- Durable OpenAI Batch API queue for summarize-on-ingest backfills.
--
-- Live/poll lanes summarize synchronously so T1 stays fresh. Backfill lanes
-- can tolerate the Batch API's 24h completion window, so the summarization
-- worker parks those requests here and commits the Kafka offset. A separate
-- batch worker groups queued items into provider batches, polls completion,
-- and applies the resulting summaries through the same post-summary path as
-- the live worker.

BEGIN;

CREATE TABLE IF NOT EXISTS summarization_batch_jobs (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    provider          TEXT        NOT NULL DEFAULT 'openai',
    provider_batch_id TEXT        NOT NULL UNIQUE,
    input_file_id     TEXT,
    output_file_id    TEXT,
    error_file_id     TEXT,
    endpoint          TEXT        NOT NULL DEFAULT '/v1/responses',
    status            TEXT        NOT NULL CHECK (status IN (
                         'submitted', 'validating', 'in_progress',
                         'finalizing', 'completed', 'failed', 'expired',
                         'cancelling', 'cancelled'
                       )),
    item_count        INTEGER     NOT NULL DEFAULT 0 CHECK (item_count >= 0),
    metadata          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    error_context     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    submitted_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_polled_at    TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS summarization_batch_items (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      UUID        NOT NULL REFERENCES tenants(id),
    source         TEXT        NOT NULL,
    observation_id UUID        NOT NULL,
    raw_s3_key     TEXT,
    ingress_kind   TEXT,
    custom_id      TEXT        NOT NULL UNIQUE,
    job_id         UUID        REFERENCES summarization_batch_jobs(id),
    status         TEXT        NOT NULL CHECK (status IN (
                      'queued', 'submitting', 'submitted',
                      'completed', 'failed'
                    )),
    source_chars   INTEGER     CHECK (source_chars IS NULL OR source_chars >= 0),
    attempts       INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error     TEXT,
    queued_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at   TIMESTAMPTZ,
    completed_at   TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, observation_id)
);

CREATE INDEX IF NOT EXISTS summarization_batch_items_status_idx
  ON summarization_batch_items (status, queued_at);

CREATE INDEX IF NOT EXISTS summarization_batch_items_job_idx
  ON summarization_batch_items (job_id)
  WHERE job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS summarization_batch_jobs_status_idx
  ON summarization_batch_jobs (status, submitted_at);

ALTER TABLE summarization_batch_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE summarization_batch_items FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON summarization_batch_items;
CREATE POLICY tenant_isolation ON summarization_batch_items
    USING (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    )
    WITH CHECK (
        current_setting('app.current_tenant', true) IS NULL
        OR tenant_id = current_setting('app.current_tenant', true)::uuid
    );

COMMIT;
