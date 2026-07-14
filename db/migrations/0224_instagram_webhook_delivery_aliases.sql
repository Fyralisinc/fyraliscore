-- Meta can deliver Instagram webhooks under the OAuth user_id while Graph
-- account discovery returns a different canonical account id. Retain that
-- delivery identifier solely for pre-tenant webhook routing.

BEGIN;

ALTER TABLE instagram_webhook_routes
  ADD COLUMN IF NOT EXISTS webhook_delivery_account_id TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS instagram_webhook_routes_delivery_account_idx
  ON instagram_webhook_routes (webhook_delivery_account_id)
  WHERE webhook_delivery_account_id IS NOT NULL;

COMMIT;
