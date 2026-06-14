-- =====================================================================
-- 0129_extension_egress.sql — the egress delivery read-model (E3.1)
-- =====================================================================
-- The Kafka projection (ext.egress.v1) is the transport; this is the host-managed
-- DELIVERY state that backs the two ways a developer-hosted extension consumes it:
--   1. `extension_egress` — an append-only outbox the cursor PULL endpoint reads
--      (seq is the monotonic cursor). Payload is the ALREADY-REDACTED ObservationView.
--   2. `extension_webhook_delivery` — per-item push attempts for the opt-in HMAC
--      webhook (status/attempts/last_error → retry + dead-letter).
--
-- Host-managed (no RLS): rows are keyed by extension_id and the pull endpoint
-- additionally filters by tenant_id after verifying an active grant. Not granted
-- to fyralis_ext_readonly — extensions never query these tables directly.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS extension_egress (
  seq             BIGSERIAL PRIMARY KEY,        -- the pull cursor (monotonic)
  extension_id    TEXT NOT NULL,
  tenant_id       UUID NOT NULL,
  observation_id  UUID NOT NULL,
  source_channel  TEXT NOT NULL,
  payload         JSONB NOT NULL,               -- redacted ObservationView wire dict
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Pull path: WHERE extension_id=? AND tenant_id=? AND seq > cursor ORDER BY seq.
CREATE INDEX IF NOT EXISTS extension_egress_pull_idx
  ON extension_egress (extension_id, tenant_id, seq);

-- Idempotent projection: one row per (extension, observation).
CREATE UNIQUE INDEX IF NOT EXISTS extension_egress_uniq
  ON extension_egress (extension_id, observation_id);

CREATE TABLE IF NOT EXISTS extension_webhook_delivery (
  id            UUID PRIMARY KEY,
  egress_seq    BIGINT NOT NULL REFERENCES extension_egress(seq) ON DELETE CASCADE,
  extension_id  TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'delivered', 'failed')),
  attempts      INT NOT NULL DEFAULT 0,
  last_error    TEXT,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  delivered_at  TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS extension_webhook_delivery_pending_idx
  ON extension_webhook_delivery (next_attempt_at)
  WHERE status = 'pending';

-- Single-row cursor the projector advances as it tails `observations` (by the
-- (occurred_at, id) order). Lets the projection resume without rescanning.
CREATE TABLE IF NOT EXISTS extension_egress_progress (
  id                   INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  last_occurred_at     TIMESTAMPTZ,
  last_observation_id  UUID,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO extension_egress_progress (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Dedicated webhook signing secret (plaintext, host-readable): the host signs each
-- push body with it (HMAC) and the extension verifies. Distinct from the
-- client_secret (which is only stored hashed). Returned once at registration.
ALTER TABLE extension_oauth_clients ADD COLUMN IF NOT EXISTS webhook_secret TEXT;

COMMIT;
