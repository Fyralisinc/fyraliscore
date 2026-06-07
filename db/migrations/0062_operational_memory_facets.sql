-- 0062_operational_memory_facets.sql
-- Index universal operational facet roles stored inside Model propositions.
--
-- Facets are not a new proposition kind. They annotate ordinary Models with
-- evidence-backed operational roles such as property, value, action, sequence,
-- state, delta, count, and invariant so SAGE can choose where to look without
-- learning benchmark-specific shortcuts.

CREATE INDEX IF NOT EXISTS models_operational_roles_gin_idx
  ON models USING gin ((coalesce(proposition->'operational_roles', '[]'::jsonb)))
  WHERE status = 'active';
