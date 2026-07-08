-- 0188_ashby_intelligence_entities.sql
--   Expand Ashby from the ATS spine into an organization-level recruiting
--   intelligence read model.
--
-- The original Ashby source sharded on five core objects:
-- candidate / application / job / interview / offer.
--
-- This additive migration seeds the additional read-only list endpoints that
-- the Ashby client now supports for every existing install. New installs get
-- the same set from services.ingest.integrations.ashby.client.DEFAULT_ENTITIES.

BEGIN;

WITH intelligence_entities(entity_type) AS (
  VALUES
    ('application_feedback'),
    ('approval'),
    ('candidate_tag'),
    ('department'),
    ('feedback_form_definition'),
    ('interview_plan'),
    ('interview_schedule'),
    ('interview_stage_group'),
    ('job_posting'),
    ('location'),
    ('opening'),
    ('project'),
    ('source'),
    ('source_tracking_link'),
    ('survey_form_definition'),
    ('survey_request'),
    ('survey_submission_candidate_experience'),
    ('survey_submission_questionnaire'),
    ('user')
)
INSERT INTO ashby_entities (
  id, tenant_id, ashby_installation_id, entity_type, state
)
SELECT
  gen_random_uuid(),
  ai.tenant_id,
  ai.id,
  ie.entity_type,
  'active'
FROM ashby_installations ai
CROSS JOIN intelligence_entities ie
ON CONFLICT (ashby_installation_id, entity_type)
  DO NOTHING;

COMMIT;
