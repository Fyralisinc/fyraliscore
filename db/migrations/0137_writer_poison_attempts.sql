-- 0137_writer_poison_attempts.sql
--
-- F3 — durable poison-attempt counter for the observation writer.
--
-- The writer retries a transient/unknown error in-process up to
-- WRITER_TRANSIENT_MAX_ATTEMPTS, then re-raises so the supervisor restarts the
-- process and Kafka redelivers the (uncommitted) message. That is correct for a
-- genuine brief outage, but a DETERMINISTIC poison message (a code bug that
-- fails identically every time) is redelivered FOREVER — head-of-line-blocking
-- the partition, and with it any backfill records sharing the per-tenant key.
--
-- The in-process retry counter resets on every restart, so it cannot detect a
-- cross-restart loop. This table is that missing durable counter: one row per
-- stuck Kafka coordinate (topic, partition, offset). After
-- WRITER_POISON_MAX_DURABLE_ATTEMPTS cross-restart give-ups the writer routes
-- the message to the DLQ and commits, so the partition advances.
--
-- Infra bookkeeping, NOT tenant data: it tracks a Kafka log position, has no
-- tenant_id, and carries no RLS — same pattern as workflow_states /
-- workflow_signals (0065). "offset" is a reserved word and is double-quoted.

BEGIN;

CREATE TABLE IF NOT EXISTS writer_poison_attempts (
    topic         TEXT        NOT NULL,
    partition     INTEGER     NOT NULL,
    "offset"      BIGINT      NOT NULL,
    attempts      INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error    TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition, "offset")
);

COMMENT ON TABLE writer_poison_attempts IS
  'F3 — durable, restart-surviving give-up counter for the observation writer, keyed by Kafka coordinates. After WRITER_POISON_MAX_DURABLE_ATTEMPTS the message is DLQ-parked so a deterministic poison cannot head-of-line-block the partition (and backfill behind it). Infra bookkeeping; no tenant_id / RLS.';

-- Janitor index: lets an operator / a future sweep reclaim rows for
-- coordinates that long since committed and will never recur.
CREATE INDEX IF NOT EXISTS writer_poison_attempts_last_seen_idx
  ON writer_poison_attempts (last_seen_at);

COMMIT;
