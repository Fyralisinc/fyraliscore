CREATE TABLE IF NOT EXISTS company_learning_barrier_heads (
  tenant_id UUID PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
  barrier_id UUID NULL,
  barrier_version BIGINT NOT NULL DEFAULT 0 CHECK (barrier_version >= 0)
);

INSERT INTO company_learning_barrier_heads (tenant_id, barrier_id, barrier_version)
SELECT DISTINCT ON (tenant_id) tenant_id, barrier_id, barrier_version
FROM company_learning_barriers
ORDER BY tenant_id, barrier_version DESC
ON CONFLICT (tenant_id) DO UPDATE
SET barrier_id=EXCLUDED.barrier_id, barrier_version=EXCLUDED.barrier_version;

DROP FUNCTION IF EXISTS complete_company_learning_barrier_common(UUID,TEXT,UUID,UUID[],TIMESTAMPTZ,TEXT,TEXT);

CREATE OR REPLACE FUNCTION complete_company_learning_barrier_common(
  p_tenant_id UUID, p_batch_id TEXT, p_barrier_id UUID,
  p_model_versions UUID[], p_completed_at TIMESTAMPTZ
) RETURNS SETOF company_learning_barriers
LANGUAGE plpgsql AS $$
DECLARE
  v_head company_learning_barrier_heads%ROWTYPE;
  v_receipt company_learning_barriers%ROWTYPE;
  v_digest TEXT;
  v_completed_iso TEXT;
  v_models_json TEXT;
BEGIN
  INSERT INTO company_learning_barrier_heads(tenant_id)
  VALUES (p_tenant_id) ON CONFLICT (tenant_id) DO NOTHING;

  SELECT * INTO v_head FROM company_learning_barrier_heads
  WHERE tenant_id=p_tenant_id FOR UPDATE;

  SELECT * INTO v_receipt FROM company_learning_barriers
  WHERE tenant_id=p_tenant_id AND batch_id=p_batch_id;
  IF FOUND THEN
    RETURN NEXT v_receipt;
    RETURN;
  END IF;

  IF (SELECT count(*) FROM accepted_current_models
      WHERE tenant_id=p_tenant_id
        AND truth_version_id=ANY(p_model_versions)) <> cardinality(p_model_versions) THEN
    RETURN;
  END IF;

  v_completed_iso := to_char(p_completed_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS') ||
    CASE WHEN (extract(microseconds FROM p_completed_at)::bigint % 1000000)=0 THEN ''
         ELSE '.'||to_char(p_completed_at AT TIME ZONE 'UTC','US') END || '+00:00';
  SELECT '['||COALESCE(string_agg(to_jsonb(item::text)::text,',' ORDER BY item::text),'')||']'
  INTO v_models_json FROM unnest(p_model_versions) item;
  v_digest := encode(sha256(convert_to(
    '{"barrier_id":'||to_jsonb(p_barrier_id::text)::text||
    ',"barrier_version":'||(v_head.barrier_version+1)::text||
    ',"batch_id":'||to_jsonb(p_batch_id)::text||
    ',"completed_at":'||to_jsonb(v_completed_iso)::text||
    ',"expected_model_version_ids":'||v_models_json||
    ',"expected_relation_version_ids":[]'||
    ',"invalidated_model_version_ids":[]'||
    ',"prior_barrier_id":'||COALESCE(to_jsonb(v_head.barrier_id::text)::text,'null')||
    ',"tenant_id":'||to_jsonb(p_tenant_id::text)::text||
    ',"truth_critical_pending_count":0}', 'UTF8')), 'hex');

  INSERT INTO company_learning_barriers (
    barrier_id,tenant_id,batch_id,barrier_version,prior_barrier_id,
    expected_model_version_ids,expected_relation_version_ids,
    invalidated_model_version_ids,truth_critical_pending_count,status,
    receipt_digest,completed_at
  ) VALUES (
    p_barrier_id,p_tenant_id,p_batch_id,v_head.barrier_version+1,v_head.barrier_id,
    p_model_versions,'{}','{}',0,'complete',v_digest,p_completed_at
  ) RETURNING * INTO v_receipt;

  UPDATE company_learning_barrier_heads
  SET barrier_id=v_receipt.barrier_id, barrier_version=v_receipt.barrier_version
  WHERE tenant_id=p_tenant_id;
  RETURN NEXT v_receipt;
END;
$$;
