-- =====================================================================
-- 0196_exact_installation_scope_uniqueness.sql
--   Exact provider-scope identity for legacy dedicated install tables.
--
-- Six early connectors used UNIQUE (tenant_id, base_url).  Their canonical
-- API host is shared by every organization/workspace/team, so that key merged
-- sibling installations belonging to the same Fyralis tenant.  The source
-- contract instead identifies these installs by their provider scope:
--
--   mercury.organization_id     brex.organization_id
--   deel.organization_id        fireflies.workspace_id
--   miro.org_id                 figma.team_id
--
-- Each replacement index uses the same two-mode identity expression:
--
--   scope present  -> (tenant_id, false, scope)
--   scope missing  -> (tenant_id, true,  base_url)
--
-- The boolean discriminator prevents an unresolved base URL from colliding
-- with a provider scope that happens to contain the same text.  The base-URL
-- branch preserves idempotence for legacy/operator flows that cannot resolve a
-- scope before persistence, while resolved installations sharing a canonical
-- API host remain distinct.
--
-- Existing blank scope values are semantically unresolved and are normalized
-- to NULL before the indexes are created.  A preflight rejects duplicate
-- non-empty scopes instead of guessing which credentials/resources should win;
-- the transaction then rolls back without dropping the legacy constraint or
-- changing data.  This is deliberately fail-closed and non-destructive.
-- =====================================================================

BEGIN;

UPDATE mercury_installations
   SET organization_id = NULL
 WHERE organization_id IS NOT NULL
   AND btrim(organization_id) = '';
UPDATE brex_installations
   SET organization_id = NULL
 WHERE organization_id IS NOT NULL
   AND btrim(organization_id) = '';
UPDATE deel_installations
   SET organization_id = NULL
 WHERE organization_id IS NOT NULL
   AND btrim(organization_id) = '';
UPDATE fireflies_installations
   SET workspace_id = NULL
 WHERE workspace_id IS NOT NULL
   AND btrim(workspace_id) = '';
UPDATE miro_installations
   SET org_id = NULL
 WHERE org_id IS NOT NULL
   AND btrim(org_id) = '';
UPDATE figma_installations
   SET team_id = NULL
 WHERE team_id IS NOT NULL
   AND btrim(team_id) = '';

DO $$
DECLARE
    relation_name TEXT;
    scope_column TEXT;
    duplicate_exists BOOLEAN;
BEGIN
    FOR relation_name, scope_column IN
        SELECT *
          FROM (VALUES
            ('mercury_installations',   'organization_id'),
            ('brex_installations',      'organization_id'),
            ('deel_installations',      'organization_id'),
            ('fireflies_installations', 'workspace_id'),
            ('miro_installations',       'org_id'),
            ('figma_installations',      'team_id')
          ) AS scoped_install(relation_name, scope_column)
    LOOP
        EXECUTE format(
            'SELECT EXISTS ('
            '  SELECT 1 FROM %I'
            '   WHERE %I IS NOT NULL'
            '   GROUP BY tenant_id, %I'
            '  HAVING count(*) > 1'
            ')',
            relation_name,
            scope_column,
            scope_column
        )
        INTO duplicate_exists;

        IF duplicate_exists THEN
            RAISE EXCEPTION
                'cannot enable exact installation identity on %.%: duplicate tenant/provider scopes exist',
                relation_name,
                scope_column
                USING HINT =
                    'Reconcile the duplicate credentials and child resources before re-running migration 0196; no migration changes were committed.';
        END IF;
    END LOOP;
END
$$;

ALTER TABLE mercury_installations
    DROP CONSTRAINT IF EXISTS mercury_installations_tenant_id_base_url_key;
ALTER TABLE brex_installations
    DROP CONSTRAINT IF EXISTS brex_installations_tenant_id_base_url_key;
ALTER TABLE deel_installations
    DROP CONSTRAINT IF EXISTS deel_installations_tenant_id_base_url_key;
ALTER TABLE fireflies_installations
    DROP CONSTRAINT IF EXISTS fireflies_installations_tenant_id_base_url_key;
ALTER TABLE miro_installations
    DROP CONSTRAINT IF EXISTS miro_installations_tenant_id_base_url_key;
ALTER TABLE figma_installations
    DROP CONSTRAINT IF EXISTS figma_installations_tenant_id_base_url_key;

CREATE UNIQUE INDEX IF NOT EXISTS mercury_installations_exact_scope_unique
    ON mercury_installations (
        tenant_id,
        (organization_id IS NULL),
        (COALESCE(organization_id, base_url))
    );
CREATE UNIQUE INDEX IF NOT EXISTS brex_installations_exact_scope_unique
    ON brex_installations (
        tenant_id,
        (organization_id IS NULL),
        (COALESCE(organization_id, base_url))
    );
CREATE UNIQUE INDEX IF NOT EXISTS deel_installations_exact_scope_unique
    ON deel_installations (
        tenant_id,
        (organization_id IS NULL),
        (COALESCE(organization_id, base_url))
    );
CREATE UNIQUE INDEX IF NOT EXISTS fireflies_installations_exact_scope_unique
    ON fireflies_installations (
        tenant_id,
        (workspace_id IS NULL),
        (COALESCE(workspace_id, base_url))
    );
CREATE UNIQUE INDEX IF NOT EXISTS miro_installations_exact_scope_unique
    ON miro_installations (
        tenant_id,
        (org_id IS NULL),
        (COALESCE(org_id, base_url))
    );
CREATE UNIQUE INDEX IF NOT EXISTS figma_installations_exact_scope_unique
    ON figma_installations (
        tenant_id,
        (team_id IS NULL),
        (COALESCE(team_id, base_url))
    );

COMMENT ON INDEX mercury_installations_exact_scope_unique IS
    'Exact tenant+organization identity; falls back to tenant+base_url only while organization_id is unresolved.';
COMMENT ON INDEX brex_installations_exact_scope_unique IS
    'Exact tenant+organization identity; falls back to tenant+base_url only while organization_id is unresolved.';
COMMENT ON INDEX deel_installations_exact_scope_unique IS
    'Exact tenant+organization identity; falls back to tenant+base_url only while organization_id is unresolved.';
COMMENT ON INDEX fireflies_installations_exact_scope_unique IS
    'Exact tenant+workspace identity; falls back to tenant+base_url only while workspace_id is unresolved.';
COMMENT ON INDEX miro_installations_exact_scope_unique IS
    'Exact tenant+organization identity; falls back to tenant+base_url only while org_id is unresolved.';
COMMENT ON INDEX figma_installations_exact_scope_unique IS
    'Exact tenant+team identity; falls back to tenant+base_url only while team_id is unresolved.';

COMMIT;
