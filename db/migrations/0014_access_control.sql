-- =====================================================================
-- 0014_access_control.sql — Wave 5-A Access Control
-- =====================================================================
-- BUILD-PLAN §6 Prompt 5.A: "role model per §26, materialized views,
-- refresh triggers".
--
-- Tables added by Wave 5-A (all outside SCHEMA-LOCK.md S1-S6):
--   * actor_roles      — per-entity role grants with idempotent dedup.
--   * shared_channels  — channels that broadcast Observations to role
--                        audiences (audience_role='all' for Wave 5-A).
--   * access_override_log — audit trail for admin + first-person
--                        overrides (spec §26 + §11 cross-cuts).
--
-- Materialized views (refreshed from Wave 4-D daily maintenance):
--   * actor_visible_commitments  — owner + contributor + mgr-chain.
--   * actor_visible_goals        — contributor-via-commitment + mgr-chain.
--   * actor_visible_models       — public-or-in-scope.
--
-- Note on migration ordering: Q2 (customer_commitments §27 superset)
-- will land in its own dedicated slot under Wave 5-B; it does NOT
-- overlap with this migration, so 0014 is safe to claim here. If the
-- ordering is later reshuffled, this migration is idempotent.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- actor_roles — per-entity role grants
-- ---------------------------------------------------------------------
-- entity_type is one of 'goal' | 'commitment' | 'decision' |
-- 'resource' | 'tenant'. entity_id is NULL when role is tenant-scoped
-- (e.g. finance / legal / leadership / admin).
-- role is one of 'owner' | 'contributor' | 'viewer' | 'admin' |
-- 'finance' | 'legal' | 'leadership'.
-- revoked_at is NULL while the grant is active; setting it marks the
-- grant as retired without losing audit history.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS actor_roles (
  tenant_id UUID NOT NULL,
  actor_id UUID NOT NULL REFERENCES actors(id),
  entity_type TEXT NOT NULL,
  entity_id UUID,
  role TEXT NOT NULL,
  granted_by UUID REFERENCES actors(id),
  granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at TIMESTAMPTZ,
  CONSTRAINT actor_roles_entity_type_check CHECK (
    entity_type IN ('goal', 'commitment', 'decision', 'resource', 'tenant')
  ),
  CONSTRAINT actor_roles_role_check CHECK (
    role IN (
      'owner', 'contributor', 'viewer', 'admin',
      'finance', 'legal', 'leadership'
    )
  ),
  -- tenant-scoped roles must have NULL entity_id; entity-scoped roles
  -- must have a non-NULL entity_id. A CHECK keeps the invariant local.
  CONSTRAINT actor_roles_scope_check CHECK (
    (entity_type = 'tenant' AND entity_id IS NULL)
    OR (entity_type <> 'tenant' AND entity_id IS NOT NULL)
  ),
  -- NULLS NOT DISTINCT is PG15+. Lets us treat tenant-scoped grants
  -- (entity_id=NULL) as a real uniqueness value. Re-grant after revoke
  -- works because revoked_at participates in the uniqueness tuple.
  CONSTRAINT actor_roles_dedup UNIQUE NULLS NOT DISTINCT
    (tenant_id, actor_id, entity_type, entity_id, role, revoked_at)
);

CREATE INDEX IF NOT EXISTS actor_roles_actor_idx
  ON actor_roles (tenant_id, actor_id)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS actor_roles_entity_idx
  ON actor_roles (tenant_id, entity_type, entity_id)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS actor_roles_role_idx
  ON actor_roles (tenant_id, role)
  WHERE revoked_at IS NULL;


-- ---------------------------------------------------------------------
-- shared_channels — source_channel → audience_role mapping
-- ---------------------------------------------------------------------
-- Wave 5-A exercises audience_role='all' (tenant-wide shared channel).
-- The column exists so future waves can target 'team:<id>' or similar
-- without a schema change.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS shared_channels (
  tenant_id UUID NOT NULL,
  source_channel TEXT NOT NULL,
  audience_role TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, source_channel, audience_role)
);

CREATE INDEX IF NOT EXISTS shared_channels_tenant_idx
  ON shared_channels (tenant_id);


-- ---------------------------------------------------------------------
-- access_override_log — audit trail for admin + first-person overrides
-- ---------------------------------------------------------------------
-- Spec §26 "Admin can override but audit trail captures" + §11 first-
-- person override interaction. Every time can_read returns True via an
-- override path, we append a row here for post-hoc audit.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS access_override_log (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  actor_id UUID NOT NULL REFERENCES actors(id),
  entity_type TEXT NOT NULL,
  entity_id UUID,
  override_kind TEXT NOT NULL,
  reason TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT access_override_log_kind_check CHECK (
    override_kind IN (
      'admin', 'first_person', 'leadership', 'system'
    )
  )
);

CREATE INDEX IF NOT EXISTS access_override_log_tenant_time_idx
  ON access_override_log (tenant_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS access_override_log_actor_idx
  ON access_override_log (tenant_id, actor_id, occurred_at DESC);


-- ---------------------------------------------------------------------
-- Materialized view: actor_visible_commitments
-- ---------------------------------------------------------------------
-- Per spec §26 "Materialized access views".
--
-- An actor sees a Commitment when ANY of:
--   (a) actor owns the commitment
--   (b) actor is a contributor (commitment_contributors)
--   (c) actor is an ancestor in the owner's manager chain
--       (actors.metadata.manager_id climb)
--   (d) actor has tenant-scoped 'admin' role
--   (e) actor has tenant-scoped 'leadership' role
--   (f) actor has an entity-scoped 'viewer' role on this commitment
--
-- The recursive CTE follows the metadata.manager_id chain upward;
-- a 32-level guard keeps pathological cycles from blowing up.
-- ---------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_visible_commitments AS
WITH RECURSIVE manager_chain AS (
  -- depth 0: every active actor starts as self
  SELECT
    a.id AS start_actor,
    a.tenant_id,
    a.id AS current_actor,
    (a.metadata->>'manager_id')::UUID AS next_manager,
    0 AS depth
  FROM actors a
  WHERE a.status = 'active'

  UNION ALL

  -- climb one level: the NEXT manager becomes current_actor
  SELECT
    mc.start_actor,
    mc.tenant_id,
    mc.next_manager AS current_actor,
    (a.metadata->>'manager_id')::UUID AS next_manager,
    mc.depth + 1
  FROM manager_chain mc
  JOIN actors a ON a.id = mc.next_manager AND a.tenant_id = mc.tenant_id
  WHERE mc.next_manager IS NOT NULL
    AND mc.depth < 32
)
SELECT DISTINCT a.id AS actor_id, c.id AS commitment_id, c.tenant_id
FROM actors a
JOIN commitments c ON c.tenant_id = a.tenant_id
WHERE a.status = 'active'
  AND (
    -- (a) owner
    c.owner_id = a.id
    -- (b) contributor
    OR EXISTS (
      SELECT 1 FROM commitment_contributors cc
      WHERE cc.commitment_id = c.id AND cc.actor_id = a.id
    )
    -- (c) manager chain: `a` is the viewer, `c.owner_id` is the report.
    -- The owner's chain climbs upward through their managers; if the
    -- viewer appears in that chain, the viewer is the owner's manager.
    OR EXISTS (
      SELECT 1 FROM manager_chain mc
      WHERE mc.start_actor = c.owner_id
        AND mc.current_actor = a.id
        AND mc.depth > 0
    )
    -- (d)+(e) tenant-wide admin/leadership
    OR EXISTS (
      SELECT 1 FROM actor_roles ar
      WHERE ar.tenant_id = a.tenant_id
        AND ar.actor_id = a.id
        AND ar.entity_type = 'tenant'
        AND ar.role IN ('admin', 'leadership')
        AND ar.revoked_at IS NULL
    )
    -- (f) entity-scoped viewer on this specific commitment
    OR EXISTS (
      SELECT 1 FROM actor_roles ar
      WHERE ar.tenant_id = a.tenant_id
        AND ar.actor_id = a.id
        AND ar.entity_type = 'commitment'
        AND ar.entity_id = c.id
        AND ar.role IN ('viewer', 'contributor', 'owner')
        AND ar.revoked_at IS NULL
    )
  )
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS actor_visible_commitments_uniq
  ON actor_visible_commitments (actor_id, commitment_id);

CREATE INDEX IF NOT EXISTS actor_visible_commitments_tenant_idx
  ON actor_visible_commitments (tenant_id, actor_id);


-- ---------------------------------------------------------------------
-- Materialized view: actor_visible_goals
-- ---------------------------------------------------------------------
-- An actor sees a Goal when ANY of:
--   (a) actor owns or contributes to a Commitment that contributes_to
--       the goal
--   (b) actor is in owner's manager chain (for any contributing cmt)
--   (c) admin / leadership tenant-wide
--   (d) entity-scoped viewer on this Goal
-- ---------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_visible_goals AS
SELECT DISTINCT
  avc.actor_id,
  ct.goal_id,
  g.tenant_id
FROM actor_visible_commitments avc
JOIN contributes_to ct ON ct.commitment_id = avc.commitment_id
JOIN goals g ON g.id = ct.goal_id AND g.tenant_id = avc.tenant_id

UNION

-- Admin/leadership gets all tenant goals.
SELECT DISTINCT a.id AS actor_id, g.id AS goal_id, g.tenant_id
FROM actors a
JOIN goals g ON g.tenant_id = a.tenant_id
WHERE a.status = 'active'
  AND EXISTS (
    SELECT 1 FROM actor_roles ar
    WHERE ar.tenant_id = a.tenant_id
      AND ar.actor_id = a.id
      AND ar.entity_type = 'tenant'
      AND ar.role IN ('admin', 'leadership')
      AND ar.revoked_at IS NULL
  )

UNION

-- Entity-scoped viewer on the Goal itself.
SELECT DISTINCT ar.actor_id, ar.entity_id AS goal_id, g.tenant_id
FROM actor_roles ar
JOIN goals g ON g.id = ar.entity_id AND g.tenant_id = ar.tenant_id
WHERE ar.entity_type = 'goal'
  AND ar.role IN ('viewer', 'contributor', 'owner')
  AND ar.revoked_at IS NULL
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS actor_visible_goals_uniq
  ON actor_visible_goals (actor_id, goal_id);

CREATE INDEX IF NOT EXISTS actor_visible_goals_tenant_idx
  ON actor_visible_goals (tenant_id, actor_id);


-- ---------------------------------------------------------------------
-- Materialized view: actor_visible_models
-- ---------------------------------------------------------------------
-- An actor sees a Model when ANY of:
--   (a) Model.visible_to_subjects=TRUE (public within tenant)
--   (b) actor_id in Model.scope_actors
--   (c) admin / leadership tenant-wide
--
-- Pattern Models: the pattern's scope entities are Goals/Commitments;
-- anyone visible on the scoped entity inherits visibility. We don't
-- materialize (c′) "pattern-scope" directly — it's expressed at query
-- time by joining actor_visible_commitments / actor_visible_goals. The
-- materialized view captures the three hot-path clauses; live can_read
-- handles pattern-scope via live joins.
-- ---------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_visible_models AS
-- (a) public Models — every tenant actor sees them
SELECT a.id AS actor_id, m.id AS model_id, m.tenant_id
FROM actors a
JOIN models m ON m.tenant_id = a.tenant_id
WHERE a.status = 'active'
  AND m.visible_to_subjects = TRUE
  AND m.status = 'active'

UNION

-- (b) private Models — actor in scope_actors
SELECT a.id AS actor_id, m.id AS model_id, m.tenant_id
FROM actors a
JOIN models m ON m.tenant_id = a.tenant_id
WHERE a.status = 'active'
  AND m.status = 'active'
  AND a.id = ANY(m.scope_actors)

UNION

-- (c) admin / leadership sees every Model in tenant
SELECT a.id AS actor_id, m.id AS model_id, m.tenant_id
FROM actors a
JOIN models m ON m.tenant_id = a.tenant_id
JOIN actor_roles ar
  ON ar.tenant_id = a.tenant_id
 AND ar.actor_id = a.id
 AND ar.entity_type = 'tenant'
 AND ar.role IN ('admin', 'leadership')
 AND ar.revoked_at IS NULL
WHERE a.status = 'active'
  AND m.status = 'active'
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS actor_visible_models_uniq
  ON actor_visible_models (actor_id, model_id);

CREATE INDEX IF NOT EXISTS actor_visible_models_tenant_idx
  ON actor_visible_models (tenant_id, actor_id);


COMMIT;
