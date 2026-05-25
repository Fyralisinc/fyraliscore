-- =====================================================================
-- 0045_situation_compositional_fields.sql
--
-- Documents the extended `situation` proposition shape introduced in the
-- spec-revamp phase that makes situations genuinely compositional.
--
-- Situations live in `models.proposition` JSONB with `kind='situation'`;
-- there is NO separate situations table and there is NO generated column
-- for the new fields. Pydantic (`SituationProposition` in
-- services/models/propositions.py) is the authoritative schema. This
-- migration exists to:
--
--   1. Record the new field shape for future readers reviewing the
--      migration log without diving into Python.
--   2. Add a CHECK constraint that NEW situations must carry the two
--      load-bearing compositional fields (`pressure_type`,
--      `shared_mechanism`). The constraint is added with NOT VALID so
--      existing rows from earlier (less structured) situation emissions
--      are not retroactively rejected; future inserts/updates are
--      validated normally.
--
-- New situation JSONB shape (additive to the original five fields):
--
--   {
--     "kind": "situation",
--     "situation": "<named composite condition>",
--     "summary": "<what is jointly true>",
--     "member_model_ids": ["<model uuid>", ...],
--     "relationship_summary": "<how the member claims interact>",
--     "status": "forming|active|resolved|contested|null",
--
--     -- NEW (this migration):
--     "pressure_type": "capacity|trust|revenue|compliance|decision|execution|market|resource",
--     "shared_mechanism": "<one sentence: why these members belong together>",
--     "judgment_change": "<one sentence: what becomes clear only when seen together>",
--     "affected_decisions": ["<string>", ...],
--     "affected_customers": ["<entity name or actor id string>", ...],
--     "affected_teams": ["<string>", ...],
--     "evidence_event_ids": ["<observation uuid>", ...],
--     "open_falsifier": "<sentence: under what observation would this be invalid>"
--   }
--
-- Pressure type enum is mirrored from `SituationPressureType` in
-- services/models/propositions.py — keep both in sync.
-- =====================================================================

BEGIN;

ALTER TABLE models
  DROP CONSTRAINT IF EXISTS models_situation_compositional_fields;

-- NOT VALID — only NEW or UPDATED rows are checked; pre-existing rows
-- written before the prompt change are grandfathered. Use a follow-up
-- VALIDATE CONSTRAINT migration after backfill if we ever want to
-- enforce on the whole table.
ALTER TABLE models
  ADD CONSTRAINT models_situation_compositional_fields
  CHECK (
    proposition_kind <> 'situation'
    OR (
      proposition ? 'pressure_type'
      AND proposition ? 'shared_mechanism'
      AND proposition->>'pressure_type' IN (
        'capacity',
        'trust',
        'revenue',
        'compliance',
        'decision',
        'execution',
        'market',
        'resource'
      )
      AND length(coalesce(proposition->>'shared_mechanism', '')) > 0
    )
  )
  NOT VALID;

COMMIT;
