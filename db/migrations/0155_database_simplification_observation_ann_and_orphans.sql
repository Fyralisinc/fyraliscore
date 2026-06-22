-- 0155_database_simplification_observation_ann_and_orphans.sql
--
-- The hot semantic retrieval path searches Model embeddings, not raw
-- Observation embeddings. Keeping a partitioned HNSW index on every
-- observations partition adds catalog/index surface without serving the live
-- retrieval path. Preserve the column for now because T1 retrieval can still use
-- a stored observation vector as a seed; retire only the direct observation ANN
-- index.

DROP INDEX IF EXISTS obs_embedding_idx;

-- Orphan tables from 0021_review1_remediation.sql. They have no live
-- services/lib references and are already documented as dead legacy.
DROP TABLE IF EXISTS anomaly_thresholds;
DROP TABLE IF EXISTS dedup_keys_seen;
