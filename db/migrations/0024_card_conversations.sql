-- 0024_card_conversations.sql
--
-- DRIFTWOOD_TODAY_CARD_REVISION: per-card conversation persistence.
--
-- A conversation is a sequence of probe→response exchanges scoped to a
-- single recommendation card. Exchanges persist across sessions; when
-- the card is resolved (act/hold/dismiss), the conversation is archived
-- in place (read-only after archival; enforced at the application layer
-- by `archived_at IS NOT NULL`).

CREATE TABLE IF NOT EXISTS card_conversations (
    id              UUID        PRIMARY KEY,
    tenant_id       UUID        NOT NULL,
    actor_id        UUID        NOT NULL,
    card_id         UUID        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_probed_at  TIMESTAMPTZ,
    archived_at     TIMESTAMPTZ,
    archive_reason  TEXT,
    -- Phrases the user has clicked at least once in this conversation.
    -- Stored as a JSONB array of probe ids so the UI can mark them
    -- ".probed" across sessions without a join.
    probed_phrase_ids JSONB     NOT NULL DEFAULT '[]'::jsonb,
    -- Probe chips the user has used (and which therefore should not
    -- reappear in the main probe row). Same format.
    used_chip_ids   JSONB       NOT NULL DEFAULT '[]'::jsonb,
    UNIQUE (tenant_id, actor_id, card_id)
);

CREATE INDEX IF NOT EXISTS card_conversations_tenant_card_idx
    ON card_conversations (tenant_id, card_id);

CREATE TABLE IF NOT EXISTS card_exchanges (
    id              UUID        PRIMARY KEY,
    conversation_id UUID        NOT NULL
                    REFERENCES card_conversations(id) ON DELETE CASCADE,
    tenant_id       UUID        NOT NULL,
    -- "phrase" | "chip" | "ask" — what the user did.
    probe_kind      TEXT        NOT NULL,
    -- For phrase/chip clicks: the probe id (e.g. "three-contradictions").
    -- For ask: NULL.
    probe_id        TEXT,
    -- Display text shown in the exchange header. Substrate-emitted so
    -- the frontend doesn't construct it.
    probe_action    TEXT        NOT NULL,
    probe_text      TEXT        NOT NULL,
    -- Substrate response HTML, may contain <probe> markup.
    response_html   TEXT        NOT NULL,
    -- 0–3 follow-up chips: [{id, text}, ...].
    follow_ups      JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Optional latency for telemetry.
    latency_ms      INTEGER
);

CREATE INDEX IF NOT EXISTS card_exchanges_conv_idx
    ON card_exchanges (conversation_id, created_at);
