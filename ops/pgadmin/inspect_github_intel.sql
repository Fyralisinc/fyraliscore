-- inspect_github_intel.sql — observe the github-intel extension working.
--
-- Run against the `fyralis_ext_demo` database (populated by
-- scripts/demo_extension_e2e.py) in pgAdmin's Query Tool. The demo's installed
-- tenant is A; the control tenant B is intentionally NOT installed.
--
-- NOTE on RLS: the `company_os` role is SUPERUSER + BYPASSRLS, so these plain
-- SELECTs see rows across tenants — scope with `WHERE tenant_id = …`. Under a
-- non-superuser role you would first run, in the same transaction:
--   SELECT set_config('app.current_tenant','aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',true);

\set tenant_a '''aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'''
\set tenant_b '''bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'''

-- 1) The capability GRANT the tenant received at install (the lifecycle wrote it).
SELECT tenant_id, extension_id, granted_version, trust_ceiling, granted_by,
       capabilities, granted_at, revoked_at
FROM extension_grants
ORDER BY tenant_id, extension_id;

-- 2) The enablement feature flags the install flipped (per tenant).
SELECT tenant_id, flag_name, flag_value, set_by, set_at
FROM tenant_flags
WHERE flag_name IN ('github_intel.enabled', 'github_intel.llm_enabled', 'code_intel.enabled')
ORDER BY tenant_id, flag_name;

-- 3) ENFORCEMENT proof: enriched vs raw github observations, per tenant.
--    Tenant A (installed) → enriched=true; tenant B (control) → enriched=false.
SELECT tenant_id,
       count(*)                                              AS github_observations,
       count(*) FILTER (WHERE content ? 'intelligence')      AS inline_enriched
FROM observations
WHERE source_channel = 'github:webhook'
GROUP BY tenant_id
ORDER BY tenant_id;

-- 4) The inline intelligence riding on the SAME observation row (tenant A).
SELECT content->>'event_type'                       AS event,
       content->'intelligence'->>'state_change'     AS state_change,
       content->'intelligence'->>'effect'           AS effect,
       (content->'intelligence'->'affected'->>'blast_radius_count') AS blast_radius
FROM observations
WHERE tenant_id = :tenant_a AND source_channel = 'github:webhook'
ORDER BY occurred_at;

-- 5) The system-of-record the ordered worker wrote (authoritative enrichment).
SELECT repo, event_type, action, entity_ref, state_changed,
       cause, effect, confidence, reasoning_path, enriched_at
FROM github_signal_enrichment
WHERE tenant_id = :tenant_a
ORDER BY enriched_at;

-- 6) The FSM state the worker maintains.
SELECT 'pr'    AS kind, pr_number::text AS ref, lifecycle AS state, ci_state, merged::text AS extra
  FROM github_pr_state    WHERE tenant_id = :tenant_a
UNION ALL
SELECT 'issue', issue_number::text, status, NULL, NULL
  FROM github_issue_state WHERE tenant_id = :tenant_a
UNION ALL
SELECT 'repo',  repo, default_branch, left(head_sha,8), NULL
  FROM github_repo_state  WHERE tenant_id = :tenant_a
ORDER BY kind, ref;

-- 7) The code graph (code_intel) the indexer built + the self-update reindex.
SELECT left(commit_sha,8) AS sha, branch, status, file_count, symbol_count, edge_count, created_at
FROM code_snapshots
WHERE tenant_id = :tenant_a
ORDER BY created_at;

-- 8) The reindex outbox the worker emitted on default-branch advances.
SELECT repo_full_name, branch, left(commit_sha,8) AS sha, kind, status, attempts
FROM code_intel_index_triggers
WHERE tenant_id = :tenant_a
ORDER BY created_at;
