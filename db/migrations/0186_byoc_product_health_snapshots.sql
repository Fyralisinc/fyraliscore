-- 0186_byoc_product_health_snapshots.sql
--
-- Metadata-only BYOC product-health snapshots. These tables deliberately store
-- only scalar counters, bounded status/code fields, and timestamps. They must
-- never store raw customer records, metric document blobs, logs, prompts, vectors,
-- model contents, credential material, URLs, signatures, or request bodies.

CREATE TABLE IF NOT EXISTS byoc_product_health_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_version TEXT NOT NULL,
    artifact_revision TEXT NOT NULL,
    snapshot_digest TEXT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL,
    overall_status TEXT NOT NULL,
    pipeline_status TEXT NOT NULL,
    queue_lag_count BIGINT NOT NULL CHECK (queue_lag_count >= 0),
    dead_letter_count BIGINT NOT NULL CHECK (dead_letter_count >= 0),
    retry_backlog_count BIGINT NOT NULL CHECK (retry_backlog_count >= 0),
    dropped_item_count BIGINT NOT NULL CHECK (dropped_item_count >= 0),
    think_status TEXT NOT NULL,
    think_run_count BIGINT NOT NULL CHECK (think_run_count >= 0),
    think_failed_run_count BIGINT NOT NULL CHECK (think_failed_run_count >= 0),
    think_queued_run_count BIGINT NOT NULL CHECK (think_queued_run_count >= 0),
    think_latest_run_at TIMESTAMPTZ,
    think_breaker_status TEXT NOT NULL,
    model_status TEXT NOT NULL,
    model_count BIGINT NOT NULL CHECK (model_count >= 0),
    model_build_count BIGINT NOT NULL CHECK (model_build_count >= 0),
    model_failed_build_count BIGINT NOT NULL CHECK (model_failed_build_count >= 0),
    model_relation_count BIGINT NOT NULL CHECK (model_relation_count >= 0),
    orphan_model_count BIGINT NOT NULL CHECK (orphan_model_count >= 0),
    stale_relation_count BIGINT NOT NULL CHECK (stale_relation_count >= 0),
    model_latest_build_at TIMESTAMPTZ,
    model_graph_status TEXT NOT NULL,
    vector_status TEXT NOT NULL,
    vector_count BIGINT NOT NULL CHECK (vector_count >= 0),
    vector_backlog_count BIGINT NOT NULL CHECK (vector_backlog_count >= 0),
    vector_failed_job_count BIGINT NOT NULL CHECK (vector_failed_job_count >= 0),
    vector_latest_job_at TIMESTAMPTZ,
    vector_retrieval_status TEXT NOT NULL,
    source_count INTEGER NOT NULL CHECK (source_count >= 0),
    open_issue_count INTEGER NOT NULL CHECK (open_issue_count >= 0),
    raw_payloads_included BOOLEAN NOT NULL DEFAULT FALSE CHECK (raw_payloads_included = FALSE),
    raw_prompts_included BOOLEAN NOT NULL DEFAULT FALSE CHECK (raw_prompts_included = FALSE),
    raw_logs_included BOOLEAN NOT NULL DEFAULT FALSE CHECK (raw_logs_included = FALSE),
    pii_included BOOLEAN NOT NULL DEFAULT FALSE CHECK (pii_included = FALSE),
    source_records_included BOOLEAN NOT NULL DEFAULT FALSE CHECK (source_records_included = FALSE),
    model_contents_included BOOLEAN NOT NULL DEFAULT FALSE CHECK (model_contents_included = FALSE),
    vector_values_included BOOLEAN NOT NULL DEFAULT FALSE CHECK (vector_values_included = FALSE),
    stored_scope TEXT NOT NULL DEFAULT 'sanitized_product_health_metadata_only'
        CHECK (stored_scope = 'sanitized_product_health_metadata_only'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS byoc_product_health_snapshots_lookup_idx
    ON byoc_product_health_snapshots (
        deployment_id,
        customer_id,
        collected_at DESC,
        accepted_at DESC
    );

CREATE TABLE IF NOT EXISTS byoc_product_health_sources (
    snapshot_id TEXT NOT NULL REFERENCES byoc_product_health_snapshots(snapshot_id)
        ON DELETE CASCADE,
    deployment_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    status TEXT NOT NULL,
    auth_status TEXT NOT NULL,
    backfill_status TEXT NOT NULL,
    items_ingested_count BIGINT NOT NULL CHECK (items_ingested_count >= 0),
    items_failed_count BIGINT NOT NULL CHECK (items_failed_count >= 0),
    queue_depth_count BIGINT NOT NULL CHECK (queue_depth_count >= 0),
    lag_seconds BIGINT CHECK (lag_seconds IS NULL OR lag_seconds >= 0),
    last_success_at TIMESTAMPTZ,
    stored_scope TEXT NOT NULL DEFAULT 'sanitized_product_health_metadata_only'
        CHECK (stored_scope = 'sanitized_product_health_metadata_only'),
    PRIMARY KEY (snapshot_id, source_name)
);

CREATE INDEX IF NOT EXISTS byoc_product_health_sources_lookup_idx
    ON byoc_product_health_sources (deployment_id, customer_id, source_name);

CREATE TABLE IF NOT EXISTS byoc_product_health_issues (
    snapshot_id TEXT NOT NULL REFERENCES byoc_product_health_snapshots(snapshot_id)
        ON DELETE CASCADE,
    deployment_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    issue_code TEXT NOT NULL,
    severity TEXT NOT NULL,
    component TEXT NOT NULL,
    observed_count BIGINT NOT NULL CHECK (observed_count >= 1),
    first_observed_at TIMESTAMPTZ,
    latest_observed_at TIMESTAMPTZ,
    stored_scope TEXT NOT NULL DEFAULT 'sanitized_product_health_metadata_only'
        CHECK (stored_scope = 'sanitized_product_health_metadata_only'),
    PRIMARY KEY (snapshot_id, issue_code, component)
);

CREATE INDEX IF NOT EXISTS byoc_product_health_issues_lookup_idx
    ON byoc_product_health_issues (deployment_id, customer_id, severity, component);
