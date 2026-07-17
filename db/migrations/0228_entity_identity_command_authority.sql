-- Fail closed on canonical alias mutation outside the governed repository path.
BEGIN;

CREATE OR REPLACE FUNCTION require_entity_identity_command_authority()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  capability text := NULLIF(
    current_setting('app.entity_identity_command', true), ''
  );
BEGIN
  IF capability IS NULL OR capability !~
     '^identity:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
    RAISE EXCEPTION 'canonical entity identity mutation requires governed identity command authority'
      USING ERRCODE = '42501';
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS entity_aliases_identity_command_authority
  ON entity_aliases;
CREATE TRIGGER entity_aliases_identity_command_authority
  BEFORE INSERT OR UPDATE OR DELETE ON entity_aliases
  FOR EACH ROW EXECUTE FUNCTION require_entity_identity_command_authority();

COMMENT ON FUNCTION require_entity_identity_command_authority() IS
  'Accidental same-role write fence. Only services/domain/entity_aliases/authority.py may mint the transaction-local capability.';

COMMIT;
