-- =====================================================================
-- 0118_sage_low_value_reader_decision_event.sql
--
-- Adds a negative credit-assignment event for selected reader decisions
-- that were packetized but did not contribute to a valid writer outcome.
-- This remains Discovery Utility Layer telemetry, not canonical truth.
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

COMMIT;
