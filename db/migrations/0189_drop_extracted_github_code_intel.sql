-- 0189_drop_extracted_github_code_intel.sql
-- destructive-migration-approved: backup=pre-deploy-schema-snapshot rollback=docs/operations/migration-release-runbook.md owner=platform
--
-- GitHub/code intelligence was extracted out of core into the extension-owned
-- interface. These host-owned tables have no live services/lib references in
-- core and keep an unused code HNSW index plus several FSM/read-model tables in
-- the primary schema.

DROP TABLE IF EXISTS github_intel_queue;
DROP TABLE IF EXISTS github_signal_enrichment;
DROP TABLE IF EXISTS github_check_state;
DROP TABLE IF EXISTS github_issue_state;
DROP TABLE IF EXISTS github_pr_state;
DROP TABLE IF EXISTS github_branch_state;
DROP TABLE IF EXISTS github_repo_state;

DROP TABLE IF EXISTS code_intel_index_triggers;
DROP TABLE IF EXISTS code_embeddings;
DROP TABLE IF EXISTS code_edges;
DROP TABLE IF EXISTS code_symbols;
DROP TABLE IF EXISTS code_files;
DROP TABLE IF EXISTS code_snapshots;
