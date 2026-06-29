-- =====================================================================
-- 0177_clarification_product_action_audit.sql
-- =====================================================================
-- Extend product_action_audit_log to cover clarification answer/dismiss
-- actions where users accept, reject, or otherwise resolve autonomous output.
-- =====================================================================

BEGIN;

ALTER TABLE product_action_audit_log
  DROP CONSTRAINT IF EXISTS product_action_audit_log_action_check;

ALTER TABLE product_action_audit_log
  ADD CONSTRAINT product_action_audit_log_action_check CHECK (
    action IN (
      'decision_delta.accept',
      'decision_delta.delegate',
      'decision_delta.contest',
      'decision_delta.add_context',
      'decision_delta.promote_from_recommendation',
      'recommendation.act',
      'recommendation.dismiss',
      'recommendation.ratify',
      'recommendation.triage',
      'recommendation.watch',
      'recommendation.unwatch',
      'clarification.answer',
      'clarification.dismiss'
    )
  );

COMMIT;
