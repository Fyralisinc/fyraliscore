-- =====================================================================
-- 0108_sage_outcome_quality_event.sql
--
-- Adds an outcome-quality diagnosis event for the SAGE reader/writer
-- feedback loop. This is Discovery Utility Layer telemetry: it records
-- how useful a retrieval/writer run was and which bottleneck dominated;
-- it is not canonical truth about Models, edges, observations, or users.
-- =====================================================================

BEGIN;

ALTER TABLE inquiry_outcome_events
  DROP CONSTRAINT IF EXISTS inquiry_outcome_events_event_type_check;

ALTER TABLE inquiry_outcome_events
  ADD CONSTRAINT inquiry_outcome_events_event_type_check CHECK (
    event_type IN (
      'retrieved_evidence_used_in_packet',
      'retrieved_evidence_omitted',
      'omitted_evidence_later_requested',
      'node_used_in_valid_diff',
      'path_used_in_valid_diff',
      'reader_decision_used_in_valid_diff',
      'reader_decision_low_value',
      'outcome_quality_assessed',
      'validation_failed_due_to_missing_evidence',
      'validation_failed_due_to_bad_reference',
      'user_accepted_node',
      'user_contested_node',
      'model_later_confirmed',
      'model_later_falsified',
      'recommendation_acted_on',
      'recommendation_ignored'
    )
  );

COMMENT ON COLUMN inquiry_outcome_events.payload IS
  'JSON payload for SAGE Discovery Utility Layer events. outcome_quality_assessed payloads carry quality axes, objective alignment, failure modes, and evidence counts; they are optimization telemetry, not canonical truth.';

COMMIT;
