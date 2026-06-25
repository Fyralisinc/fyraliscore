-- =====================================================================
-- 0159_customer_commitments_revenue_precision.sql
-- =====================================================================
-- Preserve cent precision for the customer/commitment bridge revenue field.
-- The original superset migration added this as unbounded NUMERIC; production
-- money values need an explicit precision/scale contract.
-- =====================================================================

BEGIN;

ALTER TABLE customer_commitments
  ALTER COLUMN revenue_at_risk_usd TYPE NUMERIC(14,2)
  USING ROUND(revenue_at_risk_usd, 2);

COMMIT;
